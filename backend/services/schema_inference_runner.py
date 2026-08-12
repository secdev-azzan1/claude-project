"""
Schema Inference Runner.

Orchestrates the full schema inference workflow:
  1. Deploy a temporary NiFi inference flow (same source, JSON output, inference topic)
  2. Wait for NiFi to start producing messages
  3. Consume messages from the inference Kafka topic
  4. Run Avro schema inference on the collected messages
  5. Save the generated schema as 'Needs Verification'
  6. Link schema to the flow
  7. Stop the inference NiFi process group
"""
from __future__ import annotations

import asyncio
import copy
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from services.http_tls import tls_verify_enabled
from services.runtime_recovery import APP_INSTANCE_ID
from services.rest_stream_value_mapping import substitute_runtime_values

logger = logging.getLogger(__name__)

# Poll interval (seconds) between Kafka reads while collecting messages
POLL_INTERVAL = 5
# Seconds to wait after NiFi is deployed before polling Kafka
NIFI_WARMUP_SECONDS = 35
# Max seconds to wait for messages (after warmup)
COLLECT_TIMEOUT_SECONDS = 180
# Hard timeout for the full inference job (deploy + warmup + collect + infer)
INFERENCE_TOTAL_TIMEOUT_SECONDS = 120
WEBHOOK_INFERENCE_WINDOW_SECONDS = 90
INFERENCE_SAMPLE_LIMIT = 100
INFERENCE_STABLE_POLLS = 2


def _inference_collection_complete(
    *,
    collected: int,
    requested_minimum: int,
    stable_polls: int,
) -> bool:
    # The inference target is the minimum sample size needed to infer a schema.
    # Once that target is reached, continuing to poll can time out active streams
    # that keep producing more data, even though enough samples were already read.
    if collected >= requested_minimum:
        return True
    if collected >= INFERENCE_SAMPLE_LIMIT:
        return True
    return collected >= requested_minimum and stable_polls >= INFERENCE_STABLE_POLLS


def _substitute_runtime_templates(template: str, values: Dict[str, Any]) -> str:
    if not isinstance(template, str) or not template:
        return template
    return substitute_runtime_values(template, values or {})


def _normalize_attr_token(value: str) -> str:
    import re

    return re.sub(r"[^A-Za-z0-9_]+", "_", (value or "").strip()).strip("_").lower()


def _stream_identity(stream: dict) -> str:
    return str(stream.get("id") or stream.get("name") or "").strip()


def _parent_stream_id(stream: dict) -> str:
    fan_out = stream.get("fan_out") if isinstance(stream.get("fan_out"), dict) else {}
    if fan_out.get("enabled") and fan_out.get("parent_stream_id"):
        return str(fan_out.get("parent_stream_id") or "").strip()
    route_source = stream.get("route_source") if isinstance(stream.get("route_source"), dict) else {}
    if route_source.get("parent_stream_id"):
        return str(route_source.get("parent_stream_id") or "").strip()
    return ""


def _parent_extracted_tokens(source: dict, stream: dict) -> set[str]:
    parent_id = _parent_stream_id(stream)
    if not parent_id:
        return set()
    streams = source.get("streams") or []
    parent = None
    for candidate in streams:
        if not isinstance(candidate, dict):
            continue
        if _stream_identity(candidate) == parent_id:
            parent = candidate
            break
    if not parent:
        return set()
    tokens: set[str] = set()
    for rule in parent.get("extraction_rules") or []:
        if not isinstance(rule, dict):
            continue
        attr = str(rule.get("attribute_name") or "").strip()
        if attr:
            tokens.add(_normalize_attr_token(attr))
    return tokens


def _substitute_inference_template(
    template: str,
    values: Dict[str, Any],
    parent_tokens: set[str],
) -> str:
    if not isinstance(template, str) or not template:
        return template
    effective_values = {}
    for key, value in (values or {}).items():
        if _normalize_attr_token(str(key)) in parent_tokens:
            continue
        effective_values[key] = value
    return substitute_runtime_values(template, effective_values)


def _extract_required_runtime_bindings(stream: dict) -> List[dict]:
    required: List[dict] = []
    for binding in stream.get("parameter_bindings") or []:
        if not isinstance(binding, dict):
            continue
        if not bool(binding.get("enabled", True)):
            continue
        if not bool(binding.get("required", False)):
            continue
        if str(binding.get("value_source") or "").strip().lower() != "manual_runtime":
            continue
        required.append(binding)
    return required


def _compile_rest_source_for_inference(source: dict, runtime_values: Dict[str, Any]) -> dict:
    """
    Build an inference-only copy of a REST source with manual runtime placeholders resolved.
    """
    compiled = copy.deepcopy(source)
    provided = runtime_values or {}
    unresolved_messages: List[str] = []

    for stream in compiled.get("streams") or []:
        stream_name = (stream.get("name") or stream.get("id") or "stream").strip()
        parent_tokens = _parent_extracted_tokens(compiled, stream)
        value_map: Dict[str, Any] = {}
        stream_defaults = stream.get("path_param_defaults") or {}
        if isinstance(stream_defaults, dict):
            for k, v in stream_defaults.items():
                key = str(k or "").strip()
                text = str(v).strip() if v is not None else ""
                if key and text and _normalize_attr_token(key) not in parent_tokens:
                    value_map[key] = text

        for binding in stream.get("parameter_bindings") or []:
            if not isinstance(binding, dict):
                continue
            if not bool(binding.get("enabled", True)):
                continue
            value_source = str(binding.get("value_source") or "").strip().lower()
            if value_source != "manual_runtime":
                continue
            param_name = str(binding.get("param_name") or "").strip()
            runtime_key = str(binding.get("runtime_var_name") or param_name).strip()
            default_value = binding.get("default_value")
            chosen = provided.get(runtime_key)
            if (chosen is None or not str(chosen).strip()) and default_value is not None and str(default_value).strip():
                chosen = str(default_value).strip()
            if chosen is None or not str(chosen).strip():
                if isinstance(stream_defaults, dict):
                    fallback_default = str(stream_defaults.get(runtime_key) or stream_defaults.get(param_name) or "").strip()
                    if fallback_default:
                        chosen = fallback_default
            if chosen is None or not str(chosen).strip():
                continue
            value_map[runtime_key] = str(chosen).strip()
            if param_name:
                value_map[param_name] = str(chosen).strip()

        endpoint_path = stream.get("endpoint_path")
        if isinstance(endpoint_path, str):
            stream["endpoint_path"] = _substitute_inference_template(endpoint_path, value_map, parent_tokens)

        query_params = stream.get("query_params") or {}
        if isinstance(query_params, dict):
            stream["query_params"] = {
                k: _substitute_inference_template(v, value_map, parent_tokens) if isinstance(v, str) else v
                for k, v in query_params.items()
            }

        headers = stream.get("headers") or {}
        if isinstance(headers, dict):
            stream["headers"] = {
                k: _substitute_inference_template(v, value_map, parent_tokens) if isinstance(v, str) else v
                for k, v in headers.items()
            }

        body_template = stream.get("body_template")
        if isinstance(body_template, str):
            stream["body_template"] = _substitute_inference_template(body_template, value_map, parent_tokens)

        required = _extract_required_runtime_bindings(stream)
        unresolved_for_stream: List[str] = []
        for binding in required:
            param_name = str(binding.get("param_name") or "").strip()
            runtime_key = str(binding.get("runtime_var_name") or param_name).strip()
            candidates = [runtime_key] + ([param_name] if param_name and param_name != runtime_key else [])
            placeholders = [f"${{{k}}}" for k in candidates if k]
            values_to_check: List[str] = []
            for field in ("endpoint_path", "body_template"):
                raw = stream.get(field)
                if isinstance(raw, str):
                    values_to_check.append(raw)
            for mapping_field in ("query_params", "headers"):
                mapping = stream.get(mapping_field)
                if isinstance(mapping, dict):
                    values_to_check.extend(str(v) for v in mapping.values() if isinstance(v, str))
            if any(token in text for text in values_to_check for token in placeholders):
                unresolved_for_stream.append(runtime_key or param_name)

        if unresolved_for_stream:
            unresolved_messages.append(f"{stream_name}: {', '.join(sorted(set(unresolved_for_stream)))}")

    if unresolved_messages:
        raise ValueError("Missing required runtime params for inference. " + "; ".join(unresolved_messages))
    return compiled


def _normalize_inferred_schema_root(
    schema: Dict[str, Any],
    fallback_name: str,
    fallback_namespace: str,
) -> Dict[str, Any]:
    """
    Ensure saved inferred schema has a record root.

    Some sources (notably Mongo JSON batches) may infer as:
      {"type":"array","items":["null", {"type":"record", ...}]}
    Schema Manager editor expects a top-level record.
    """
    if not isinstance(schema, dict):
        return {
            "type": "record",
            "name": fallback_name,
            "namespace": fallback_namespace,
            "fields": [],
        }

    stype = schema.get("type")
    if stype == "record":
        if not schema.get("name") or str(schema.get("name")).strip("_") == "":
            schema["name"] = fallback_name
        if not schema.get("namespace"):
            schema["namespace"] = fallback_namespace
        return schema

    if stype == "array":
        items = schema.get("items")
        item_candidates = items if isinstance(items, list) else [items]
        for item in item_candidates:
            if isinstance(item, dict) and item.get("type") == "record":
                record = dict(item)
                if not record.get("name") or str(record.get("name")).strip("_") == "":
                    record["name"] = fallback_name
                if not record.get("namespace"):
                    record["namespace"] = fallback_namespace
                return record

    # Fallback wrapper for other root types.
    return {
        "type": "record",
        "name": fallback_name,
        "namespace": fallback_namespace,
        "fields": [
            {
                "name": "value",
                "type": ["null", "string"],
                "default": None,
            }
        ],
    }


async def _update_job(
    db,
    job_id: str,
    updates: Dict[str, Any],
) -> None:
    """Update a schema inference job in MongoDB."""
    now = datetime.utcnow()
    updates["updated_at"] = now
    updates["heartbeat_at"] = now
    updates["worker_instance_id"] = APP_INSTANCE_ID
    await db.schema_inference_jobs.update_one(
        {"id": job_id},
        {"$set": updates},
    )


async def _stop_inference_nifi_pg(
    nifi_conn: dict,
    pg_id: str,
) -> None:
    """Stop the inference NiFi process group (best-effort)."""
    if not pg_id or not nifi_conn:
        return
    try:
        from services.nifi_flow_manager import stop_process_group
        await stop_process_group(
            url=nifi_conn["endpoint"],
            process_group_id=pg_id,
            auth_type=nifi_conn.get("auth_type", "NONE"),
            username=nifi_conn.get("username"),
            password=nifi_conn.get("password"),
        )
        logger.info(f"Stopped inference NiFi PG {pg_id}")
    except Exception as e:
        logger.warning(f"Could not stop inference NiFi PG {pg_id}: {e}")


async def _delete_inference_nifi_pg(
    nifi_conn: dict,
    pg_id: str,
) -> None:
    """Delete the inference NiFi process group (best-effort)."""
    if not pg_id or not nifi_conn:
        return
    try:
        from services.nifi_flow_generator import _delete_old_pg
        await _delete_old_pg(
            nifi_conn["endpoint"],
            pg_id,
            auth_type=nifi_conn.get("auth_type", "NONE"),
            username=nifi_conn.get("username"),
            password=nifi_conn.get("password"),
        )
        logger.info(f"Deleted inference NiFi PG {pg_id}")
    except Exception as e:
        logger.warning(f"Could not delete inference NiFi PG {pg_id}: {e}")


async def _create_kafka_inference_topic(
    inference_topic: str,
    kafka_conn: dict,
) -> Optional[str]:
    """Create the Kafka inference topic.

    Primary path: Kafbat API (if configured)
    Fallback: direct Kafka Admin API via kafka-python
    Returns None on success, error string on failure.
    """
    kafka_mode = (kafka_conn.get("kafka_connection_mode") or "native").strip().lower()
    kafbat_url = ((kafka_conn.get("kafbat_url") or kafka_conn.get("endpoint") or "") if kafka_mode == "kafbat" else "").rstrip("/")
    kafbat_username = kafka_conn.get("kafbat_username") if kafka_mode == "kafbat" else ""
    kafbat_password = kafka_conn.get("kafbat_password") if kafka_mode == "kafbat" else ""
    bootstrap = "" if kafka_mode == "kafbat" else (kafka_conn.get("endpoint") or "").strip()

    async def _create_via_kafka_admin() -> Optional[str]:
        if not bootstrap:
            return "No Kafka bootstrap endpoint configured."
        try:
            from kafka.admin import KafkaAdminClient, NewTopic
            from kafka.errors import TopicAlreadyExistsError
        except Exception as e:
            return f"kafka-python admin unavailable: {e}"

        def _create_topic():
            admin = KafkaAdminClient(
                bootstrap_servers=bootstrap,
                request_timeout_ms=10000,
                api_version_auto_timeout_ms=8000,
            )
            try:
                topic = NewTopic(name=inference_topic, num_partitions=1, replication_factor=1)
                admin.create_topics(new_topics=[topic], validate_only=False)
            except TopicAlreadyExistsError:
                pass
            finally:
                admin.close()

        try:
            await asyncio.to_thread(_create_topic)
            logger.info(f"Created inference topic '{inference_topic}' via Kafka Admin API")
            return None
        except Exception as e:
            return f"Kafka Admin topic creation failed: {str(e)[:200]}"

    if not kafbat_url:
        return await _create_via_kafka_admin()

    try:
        import httpx
        async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=20.0, follow_redirects=True) as client:
            if kafbat_username:
                await client.post(
                    f"{kafbat_url}/login",
                    data={"username": kafbat_username, "password": kafbat_password or ""},
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )
            # Get cluster name
            clusters = await client.get(f"{kafbat_url}/api/clusters")
            cluster = None
            if clusters.status_code == 200:
                cdata = clusters.json()
                if isinstance(cdata, list) and cdata:
                    cluster = cdata[0].get("name") or cdata[0].get("id")
            if not cluster:
                return "Cannot determine Kafbat cluster name"

            # Check if topic already exists
            topics_resp = await client.get(f"{kafbat_url}/api/clusters/{cluster}/topics")
            if topics_resp.status_code == 200:
                tdata = topics_resp.json()
                existing = tdata.get("topics", tdata) if isinstance(tdata, dict) else tdata
                existing_names = [t.get("name") if isinstance(t, dict) else t for t in existing]
                if inference_topic in existing_names:
                    logger.info(f"Inference topic '{inference_topic}' already exists")
                    return None

            # Create the topic
            create_resp = await client.post(
                f"{kafbat_url}/api/clusters/{cluster}/topics",
                json={
                    "name": inference_topic,
                    "partitions": 3,
                    "replicationFactor": 1,
                    "configs": {},
                },
            )
            if create_resp.status_code in (200, 201):
                logger.info(f"Created inference Kafka topic '{inference_topic}'")
                return None
            return f"Kafbat topic creation returned {create_resp.status_code}: {create_resp.text[:200]}"
    except Exception as e:
        fallback_err = await _create_via_kafka_admin()
        if fallback_err is None:
            return None
        return f"Topic creation error: {str(e)[:200]}; fallback failed: {fallback_err}"


async def _delete_kafka_inference_topic(
    inference_topic: str,
    kafka_conn: Optional[dict],
) -> Optional[str]:
    """Delete the Kafka inference topic after an inference attempt.

    Best-effort cleanup: failures are logged by caller and must not change the
    inference result because schema generation may already have completed.
    """
    if not inference_topic or not kafka_conn:
        return "No Kafka connection or inference topic configured."

    kafka_mode = (kafka_conn.get("kafka_connection_mode") or "native").strip().lower()
    kafbat_url = ((kafka_conn.get("kafbat_url") or kafka_conn.get("endpoint") or "") if kafka_mode == "kafbat" else "").rstrip("/")
    kafbat_username = kafka_conn.get("kafbat_username") if kafka_mode == "kafbat" else ""
    kafbat_password = kafka_conn.get("kafbat_password") if kafka_mode == "kafbat" else ""
    bootstrap = "" if kafka_mode == "kafbat" else (kafka_conn.get("endpoint") or "").strip()

    async def _delete_via_kafka_admin() -> Optional[str]:
        if not bootstrap:
            return "No Kafka bootstrap endpoint configured."
        try:
            from kafka.admin import KafkaAdminClient
            from kafka.errors import UnknownTopicOrPartitionError
        except Exception as e:
            return f"kafka-python admin unavailable: {e}"

        def _delete_topic():
            admin = KafkaAdminClient(
                bootstrap_servers=bootstrap,
                request_timeout_ms=10000,
                api_version_auto_timeout_ms=8000,
            )
            try:
                admin.delete_topics(topics=[inference_topic], timeout_ms=10000)
            except UnknownTopicOrPartitionError:
                pass
            finally:
                admin.close()

        try:
            await asyncio.to_thread(_delete_topic)
            logger.info(f"Deleted inference topic '{inference_topic}' via Kafka Admin API")
            return None
        except Exception as e:
            return f"Kafka Admin topic deletion failed: {str(e)[:200]}"

    if not kafbat_url:
        return await _delete_via_kafka_admin()

    try:
        import httpx
        async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=20.0, follow_redirects=True) as client:
            if kafbat_username:
                await client.post(
                    f"{kafbat_url}/login",
                    data={"username": kafbat_username, "password": kafbat_password or ""},
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )

            clusters = await client.get(f"{kafbat_url}/api/clusters")
            cluster = None
            if clusters.status_code == 200:
                cdata = clusters.json()
                if isinstance(cdata, list) and cdata:
                    cluster = cdata[0].get("name") or cdata[0].get("id")
            if not cluster:
                return "Cannot determine Kafbat cluster name"

            delete_resp = await client.delete(
                f"{kafbat_url}/api/clusters/{quote(cluster, safe='')}/topics/{quote(inference_topic, safe='')}"
            )
            if delete_resp.status_code in (200, 202, 204, 404):
                logger.info(f"Deleted inference Kafka topic '{inference_topic}' via Kafbat")
                return None
            if delete_resp.status_code == 500 and "UnknownTopicOrPartition" in delete_resp.text:
                logger.info(f"Inference Kafka topic '{inference_topic}' was already absent in Kafbat")
                return None
            return f"Kafbat topic deletion returned {delete_resp.status_code}: {delete_resp.text[:200]}"
    except Exception as e:
        fallback_err = await _delete_via_kafka_admin()
        if fallback_err is None:
            return None
        return f"Topic deletion error: {str(e)[:200]}; fallback failed: {fallback_err}"


async def run_inference_background(
    job_id: str,
    flow_id: str,
    source: dict,
    flow: dict,
    nifi_conn: Optional[dict],
    kafka_conn: Optional[dict],
    inference_topic: str,
    target_messages: int,
    runtime_values: Optional[Dict[str, Any]],
    db,
    entity_stream_id: Optional[str] = None,
    entity_kafka_topic: Optional[str] = None,
) -> None:
    """
    Background task: runs the full schema inference workflow.
    This function is called as an asyncio background task.
    """
    pg_id: Optional[str] = None
    messages_collected: List[Any] = []
    started_at = asyncio.get_event_loop().time()
    source_type = source.get("type")

    def _stream_name_for_id(stream_id: Optional[str]) -> str:
        wanted = str(stream_id or "").strip()
        if not wanted:
            return ""
        for stream in source.get("streams") or []:
            if not isinstance(stream, dict):
                continue
            if str(stream.get("id") or "").strip() == wanted:
                return str(stream.get("name") or wanted).strip()
        return wanted

    def _timed_out() -> bool:
        return (asyncio.get_event_loop().time() - started_at) >= INFERENCE_TOTAL_TIMEOUT_SECONDS

    try:
        # ── Webhook branch: collect from locally captured webhook samples ─────
        if source_type == "Webhook":
            await _update_job(db, job_id, {"status": "collecting", "messages_collected": 0, "error": None})
            started_dt = datetime.utcnow()
            deadline = asyncio.get_event_loop().time() + WEBHOOK_INFERENCE_WINDOW_SECONDS
            samples: List[Any] = []

            while asyncio.get_event_loop().time() < deadline and len(samples) < target_messages:
                docs = await db.webhook_samples.find(
                    {
                        "flow_id": flow_id,
                        "created_at": {"$gte": started_dt},
                        "topic": {"$ne": None},
                    },
                    {"_id": 0, "payload": 1},
                ).sort("created_at", 1).limit(target_messages).to_list(target_messages)
                payloads = [d.get("payload") for d in docs if d.get("payload") is not None]
                if len(payloads) > len(samples):
                    samples = payloads[:target_messages]
                    await _update_job(db, job_id, {"messages_collected": len(samples)})
                if len(samples) >= target_messages:
                    break
                await asyncio.sleep(2)

            if not samples:
                await _update_job(
                    db,
                    job_id,
                    {
                        "status": "failed",
                        "error": (
                            f"No webhook messages captured within {WEBHOOK_INFERENCE_WINDOW_SECONDS}s. "
                            "Start the webhook flow and send events, then retry inference."
                        ),
                    },
                )
                return

            await _update_job(db, job_id, {"status": "inferring", "messages_collected": len(samples)})

            from services.schema_inferencer import infer_avro_schema
            flow_name = flow.get("name") or f"flow-{flow_id[:8]}"
            schema_name = flow_name.replace("-", "_").replace(" ", "_").replace(".", "_")
            namespace = "com.nif"
            generated_schema = infer_avro_schema(samples=samples, name=schema_name, namespace=namespace)
            generated_schema = _normalize_inferred_schema_root(
                generated_schema,
                fallback_name=schema_name,
                fallback_namespace=namespace,
            )

            topic_for_schema = (flow.get("kafka_topic") or "").strip()
            artifact_id = f"{topic_for_schema}-value" if topic_for_schema else f"{flow_name}-value"
            import json as _json
            now = datetime.utcnow()

            existing = await db.schema_artifacts.find_one({"artifact_id": artifact_id})
            if existing:
                versions = existing.get("versions", [])
                new_ver_num = max((v.get("version", 0) for v in versions), default=0) + 1
                new_version = {
                    "version": new_ver_num,
                    "status": "Needs Verification",
                    "avro_schema": generated_schema,
                    "raw_avro": _json.dumps(generated_schema, indent=2),
                    "created_at": now,
                    "updated_at": now,
                    "verified_at": None,
                    "verified_by": None,
                    "apicurio_global_id": None,
                }
                versions.append(new_version)
                await db.schema_artifacts.update_one(
                    {"artifact_id": artifact_id},
                    {"$set": {"versions": versions, "updated_at": now}},
                )
                schema_version = new_ver_num
            else:
                artifact_doc = {
                    "id": str(uuid.uuid4()),
                    "artifact_id": artifact_id,
                    "stream": _stream_name_for_id(flow.get("primary_stream_id")),
                    "namespace": namespace,
                    "versions": [
                        {
                            "version": 1,
                            "status": "Needs Verification",
                            "avro_schema": generated_schema,
                            "raw_avro": _json.dumps(generated_schema, indent=2),
                            "created_at": now,
                            "updated_at": now,
                            "verified_at": None,
                            "verified_by": None,
                            "apicurio_global_id": None,
                        }
                    ],
                    "created_at": now,
                    "updated_at": now,
                }
                await db.schema_artifacts.insert_one(artifact_doc)
                schema_version = 1

            await db.flows.update_one(
                {"id": flow_id},
                {"$set": {
                    "schema_artifact_id": artifact_id,
                    "schema_version": schema_version,
                    "updated_at": now,
                }},
            )

            await _update_job(
                db,
                job_id,
                {
                    "status": "complete",
                    "generated_schema": generated_schema,
                    "schema_artifact_id": artifact_id,
                    "schema_version": schema_version,
                    "messages_collected": len(samples),
                    "error": None,
                },
            )
            return


        # ── Step 0: Create Kafka inference topic ──────────────────────────────
        if not kafka_conn or not nifi_conn:
            await _update_job(db, job_id, {"status": "failed", "error": "Inference requires NiFi and Kafka connections for this source type."})
            return
        await _update_job(db, job_id, {"status": "deploying_nifi"})
        logger.info(f"[Inference {job_id}] Creating Kafka inference topic '{inference_topic}'...")
        topic_err = await _create_kafka_inference_topic(inference_topic, kafka_conn)
        if topic_err:
            logger.warning(f"[Inference {job_id}] Topic creation note: {topic_err}")
            await _update_job(db, job_id, {"error": f"Topic creation note: {topic_err}"})

        # ── Step 1: Deploy inference NiFi flow ─────────────────────────────
        await _update_job(db, job_id, {"status": "deploying_nifi"})
        logger.info(f"[Inference {job_id}] Deploying inference NiFi flow...")

        inference_source = source
        if source_type == "REST API":
            try:
                inference_source = _compile_rest_source_for_inference(source, runtime_values or {})
            except Exception as compile_err:
                await _update_job(
                    db,
                    job_id,
                    {
                        "status": "failed",
                        "error": str(compile_err)[:500],
                    },
                )
                return

        try:
            from services.nifi_flow_generator import generate_and_deploy_inference_flow
            from services.nifi_global_services import resolve_flow_global_service_ids
            global_service_ids = await resolve_flow_global_service_ids(db, inference_source)
            deploy_result = await generate_and_deploy_inference_flow(
                source=inference_source,
                flow_id=flow_id,
                inference_topic=inference_topic,
                nifi_conn=nifi_conn,
                kafka_conn=kafka_conn,
                target_stream_id=entity_stream_id,
                global_service_ids=global_service_ids,
            )
        except ImportError:
            deploy_result = {"ok": False, "error": "nifi_flow_generator not available"}
        except Exception as service_err:
            deploy_result = {"ok": False, "error": str(service_err)[:500]}

        if not deploy_result.get("ok"):
            err = deploy_result.get("error", "Unknown NiFi deploy error")
            logger.warning(f"[Inference {job_id}] NiFi deploy failed: {err}. Will still poll Kafka.")
            await _update_job(db, job_id, {
                "status": "nifi_running",
                "error": f"NiFi deploy warning: {err}. Polling Kafka anyway — topic may already have data.",
            })
        else:
            pg_id = deploy_result.get("process_group_id")
            await _update_job(db, job_id, {"status": "nifi_running", "nifi_pg_id": pg_id, "error": None})
            logger.info(f"[Inference {job_id}] NiFi inference PG deployed: {pg_id}")

            # ── Step 2: Start the inference PG ─────────────────────────────
            if pg_id:
                try:
                    from services.nifi_flow_manager import start_process_group
                    await start_process_group(
                        url=nifi_conn["endpoint"],
                        process_group_id=pg_id,
                        auth_type=nifi_conn.get("auth_type", "NONE"),
                        username=nifi_conn.get("username"),
                        password=nifi_conn.get("password"),
                    )
                    logger.info(f"[Inference {job_id}] Started inference NiFi PG {pg_id}")
                except Exception as e:
                    logger.warning(f"[Inference {job_id}] Could not start NiFi PG: {e}")

        if _timed_out():
            if pg_id:
                await _stop_inference_nifi_pg(nifi_conn, pg_id)
            await _update_job(db, job_id, {
                "status": "failed",
                "error": (
                    f"Schema inference timed out after {INFERENCE_TOTAL_TIMEOUT_SECONDS}s while preparing NiFi/Kafka. "
                    "Please retry inference."
                ),
            })
            return

        # ── Step 3: Wait for NiFi warmup ───────────────────────────────────
        logger.info(f"[Inference {job_id}] Waiting {NIFI_WARMUP_SECONDS}s for NiFi warmup...")
        warmup_left = INFERENCE_TOTAL_TIMEOUT_SECONDS - (asyncio.get_event_loop().time() - started_at)
        if warmup_left <= 0:
            if pg_id:
                await _stop_inference_nifi_pg(nifi_conn, pg_id)
            await _update_job(db, job_id, {
                "status": "failed",
                "error": f"Schema inference timed out after {INFERENCE_TOTAL_TIMEOUT_SECONDS}s. Please retry inference.",
            })
            return
        await asyncio.sleep(min(NIFI_WARMUP_SECONDS, max(0, warmup_left)))

        if _timed_out():
            if pg_id:
                await _stop_inference_nifi_pg(nifi_conn, pg_id)
            await _update_job(db, job_id, {
                "status": "failed",
                "error": f"Schema inference timed out after {INFERENCE_TOTAL_TIMEOUT_SECONDS}s. Please retry inference.",
            })
            return

        # ── Step 4: Collect messages from inference topic ──────────────────
        await _update_job(db, job_id, {"status": "collecting"})
        logger.info(f"[Inference {job_id}] Starting Kafka collection from '{inference_topic}'...")

        kafka_mode = (kafka_conn.get("kafka_connection_mode") or "native").strip().lower()
        kafka_bootstrap = "" if kafka_mode == "kafbat" else kafka_conn.get("endpoint", "")
        kafbat_url = ((kafka_conn.get("kafbat_url") or kafka_conn.get("endpoint") or "") if kafka_mode == "kafbat" else "").rstrip("/")
        kafbat_username = kafka_conn.get("kafbat_username") if kafka_mode == "kafbat" else ""
        kafbat_password = kafka_conn.get("kafbat_password") if kafka_mode == "kafbat" else ""

        from services.kafka_schema_consumer import consume_messages_for_inference

        deadline = min(
            asyncio.get_event_loop().time() + COLLECT_TIMEOUT_SECONDS,
            started_at + INFERENCE_TOTAL_TIMEOUT_SECONDS,
        )
        messages_collected = []
        stable_polls = 0

        while (
            asyncio.get_event_loop().time() < deadline
            and not _inference_collection_complete(
                collected=len(messages_collected),
                requested_minimum=target_messages,
                stable_polls=stable_polls,
            )
        ):
            try:
                new_msgs, method, error = await consume_messages_for_inference(
                    bootstrap_servers=kafka_bootstrap,
                    topic=inference_topic,
                    max_messages=INFERENCE_SAMPLE_LIMIT,
                    kafbat_url=kafbat_url,
                    kafbat_username=kafbat_username,
                    kafbat_password=kafbat_password,
                    timeout_ms=8000,
                )
                if error:
                    logger.warning(f"[Inference {job_id}] Kafka read error ({method}): {error}")
                elif new_msgs:
                    # Use the max set we've seen so far (Kafka is read from beginning each time)
                    if len(new_msgs) > len(messages_collected):
                        messages_collected = new_msgs[:INFERENCE_SAMPLE_LIMIT]
                        stable_polls = 0
                        await _update_job(db, job_id, {"messages_collected": len(messages_collected)})
                        logger.info(
                            f"[Inference {job_id}] Collected {len(messages_collected)} messages "
                            f"(minimum {target_messages}, limit {INFERENCE_SAMPLE_LIMIT}) via {method}"
                        )
                    elif len(messages_collected) >= target_messages:
                        stable_polls += 1
                else:
                    if len(messages_collected) >= target_messages:
                        stable_polls += 1
                    logger.debug(f"[Inference {job_id}] No messages yet via {method}")
            except Exception as e:
                logger.warning(f"[Inference {job_id}] Collection error: {e}")

            # Wait before next poll
            await asyncio.sleep(POLL_INTERVAL)

        # ── Step 5: Stop the inference NiFi PG ────────────────────────────
        if pg_id:
            await _stop_inference_nifi_pg(nifi_conn, pg_id)
            await _delete_inference_nifi_pg(nifi_conn, pg_id)
            await _update_job(db, job_id, {"nifi_pg_id": None})
            pg_id = None

        if _timed_out():
            await _update_job(db, job_id, {
                "status": "failed",
                "error": f"Schema inference timed out after {INFERENCE_TOTAL_TIMEOUT_SECONDS}s. Please retry inference.",
            })
            return

        if not messages_collected:
            await _update_job(db, job_id, {
                "status": "failed",
                "error": (
                    f"No messages found in inference topic '{inference_topic}' after waiting {NIFI_WARMUP_SECONDS + COLLECT_TIMEOUT_SECONDS}s. "
                    "Check: (1) NiFi inference flow deployed successfully, (2) Kafka topic auto-creation is enabled, "
                    "(3) Kafka is reachable from NiFi."
                ),
            })
            return

        # ── Step 6: Run schema inference ───────────────────────────────────
        await _update_job(db, job_id, {"status": "inferring", "messages_collected": len(messages_collected)})
        logger.info(f"[Inference {job_id}] Running schema inference on {len(messages_collected)} messages...")

        from services.schema_inferencer import infer_avro_schema
        flow_name = flow.get("name") or f"flow-{flow_id[:8]}"
        topic_for_schema = (
            (entity_kafka_topic or "").strip()
            if entity_stream_id
            else (flow.get("kafka_topic") or "").strip()
        )
        schema_name_source = topic_for_schema or flow_name
        schema_name = schema_name_source.replace("-", "_").replace(" ", "_").replace(".", "_")
        namespace = "com.nif"

        generated_schema = infer_avro_schema(
            samples=messages_collected,
            name=schema_name,
            namespace=namespace,
        )
        generated_schema = _normalize_inferred_schema_root(
            generated_schema,
            fallback_name=schema_name,
            fallback_namespace=namespace,
        )

        logger.info(f"[Inference {job_id}] Schema inference complete. Root type: {type(generated_schema)}")

        # ── Step 7: Save schema artifact to MongoDB ────────────────────────
        artifact_id = f"{topic_for_schema}-value" if topic_for_schema else f"{flow_name}-value"
        import json as _json
        now = datetime.utcnow()

        # Check if artifact already exists
        existing = await db.schema_artifacts.find_one({"artifact_id": artifact_id})
        if existing:
            # Fork a new version
            versions = existing.get("versions", [])
            new_ver_num = max((v.get("version", 0) for v in versions), default=0) + 1
            new_version = {
                "version": new_ver_num,
                "status": "Needs Verification",
                "avro_schema": generated_schema,
                "raw_avro": _json.dumps(generated_schema, indent=2),
                "created_at": now,
                "updated_at": now,
                "verified_at": None,
                "verified_by": None,
                "apicurio_global_id": None,
            }
            versions.append(new_version)
            await db.schema_artifacts.update_one(
                {"artifact_id": artifact_id},
                {"$set": {"versions": versions, "updated_at": now}},
            )
            schema_version = new_ver_num
        else:
            # Create new artifact with v1
            artifact_doc = {
                "id": str(uuid.uuid4()),
                "artifact_id": artifact_id,
                "stream": _stream_name_for_id(entity_stream_id or flow.get("primary_stream_id")),
                "namespace": namespace,
                "versions": [
                    {
                        "version": 1,
                        "status": "Needs Verification",
                        "avro_schema": generated_schema,
                        "raw_avro": _json.dumps(generated_schema, indent=2),
                        "created_at": now,
                        "updated_at": now,
                        "verified_at": None,
                        "verified_by": None,
                        "apicurio_global_id": None,
                    }
                ],
                "created_at": now,
                "updated_at": now,
            }
            await db.schema_artifacts.insert_one(artifact_doc)
            schema_version = 1

        # ── Step 8: Link schema to flow ────────────────────────────────────
        await db.flows.update_one(
            {"id": flow_id},
            {"$set": {
                "schema_artifact_id": artifact_id,
                "schema_version": schema_version,
                "updated_at": now,
            }},
        )

        # ── Step 9: Mark job complete ──────────────────────────────────────
        await _update_job(db, job_id, {
            "status": "complete",
            "generated_schema": generated_schema,
            "schema_artifact_id": artifact_id,
            "schema_version": schema_version,
            "messages_collected": len(messages_collected),
            "error": None,
        })
        logger.info(f"[Inference {job_id}] Complete! Schema '{artifact_id}' v{schema_version} saved.")

        # Audit
        try:
            from models.audit import AuditEvent
            event = AuditEvent(
                action="Schema inference completed",
                object_type="Schema",
                target=artifact_id,
                status="Success",
                details=f"Inferred from {len(messages_collected)} messages on topic '{inference_topic}'",
            )
            await db.audit_events.insert_one(event.dict())
        except Exception:
            pass

    except Exception as e:
        logger.error(f"[Inference {job_id}] Unexpected error: {e}", exc_info=True)
        # Clean up NiFi PG if possible
        if pg_id:
            await _stop_inference_nifi_pg(nifi_conn, pg_id)
        await _update_job(db, job_id, {
            "status": "failed",
            "error": str(e)[:500],
        })
        # Audit
        try:
            from models.audit import AuditEvent
            event = AuditEvent(
                action="Schema inference failed",
                object_type="Schema",
                target=flow.get("name", flow_id),
                status="Failed",
                details=str(e)[:300],
            )
            await db.audit_events.insert_one(event.dict())
        except Exception:
            pass
    finally:
        if pg_id and nifi_conn:
            try:
                await _delete_inference_nifi_pg(nifi_conn, pg_id)
                await _update_job(db, job_id, {"nifi_pg_id": None})
            except Exception as cleanup_exc:
                logger.warning(f"[Inference {job_id}] Inference NiFi PG cleanup failed for '{pg_id}': {cleanup_exc}")
        if kafka_conn and inference_topic:
            try:
                cleanup_err = await _delete_kafka_inference_topic(inference_topic, kafka_conn)
                if cleanup_err:
                    logger.warning(f"[Inference {job_id}] Could not delete inference topic '{inference_topic}': {cleanup_err}")
            except Exception as cleanup_exc:
                logger.warning(f"[Inference {job_id}] Inference topic cleanup failed for '{inference_topic}': {cleanup_exc}")
        try:
            await db.flows.update_one(
                {"id": flow_id},
                {"$set": {"schema_inference_active": False, "updated_at": datetime.utcnow()}},
            )
        except Exception as lock_err:
            logger.warning(f"[Inference {job_id}] Could not release schema inference lock: {lock_err}")
