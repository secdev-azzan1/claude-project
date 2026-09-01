"""V2 schema inference runner.

V1's important guarantee is that inference observes the records after the
runtime has fetched and transformed them.  V2 now does the same thing: compile
the selected source-to-writer path, deploy it under a unique temporary name,
publish JSON to a unique temporary Kafka topic, consume those records here,
infer Avro, and tear the temporary runtime down in all cases.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

from models.adapter import Flow, FlowBlock, PlatformConnection
from services import kafka_schema_consumer
from services.adapter.common import COLLECTIONS, audit, now_iso
from services.adapter.compiler import CompileContext, CompileError
from services.adapter.compiler.inference import build_inference_plan
from services.adapter.deployer import lifecycle, nifi_apply, topics
from services.schema_inferencer import infer_avro_schema

logger = logging.getLogger(__name__)

MAX_SAMPLE_RECORDS = 100
DEFAULT_TARGET_MESSAGES = 10
INFERENCE_TIMEOUT_SECONDS = 120
POLL_SECONDS = 2

ACTIVE_STATUSES = {"queued", "deploying", "running", "collecting", "inferring", "cleaning_up", "stopping"}
TERMINAL_STATUSES = {"complete", "failed", "stopped"}


class InferenceCancelled(RuntimeError):
    pass


def public_job(document: Dict[str, Any]) -> Dict[str, Any]:
    """Return a browser-safe job document.

    The job intentionally contains no connection configuration or credentials;
    this function is kept as a boundary so future runner diagnostics cannot
    accidentally become API output.
    """
    allowed = {
        "id", "flowId", "targetBlockId", "flowName", "targetTopic", "inferenceTopic",
        "status", "messagesCollected", "targetMessages", "nifiProcessGroupId",
        "generatedSchema", "schemaStatus", "error", "cleanupError", "createdAt", "updatedAt",
    }
    return {key: value for key, value in document.items() if key in allowed}


async def get_job(db, job_id: str) -> Optional[Dict[str, Any]]:
    return await db[COLLECTIONS.schema_inference_jobs].find_one({"id": job_id}, {"_id": 0})


async def update_job(db, job_id: str, **fields: Any) -> Dict[str, Any]:
    fields["updatedAt"] = now_iso()
    await db[COLLECTIONS.schema_inference_jobs].update_one({"id": job_id}, {"$set": fields})
    return (await get_job(db, job_id)) or {"id": job_id, **fields}


def target_block_or_error(flow: Flow, block_id: str) -> FlowBlock:
    block = next((candidate for candidate in flow.blocks if candidate.id == block_id), None)
    if block is None:
        raise CompileError(f"Schema inference target block {block_id!r} was not found.")
    if block.adapter == "kafka_kc":
        return block
    if block.adapter == "kafka" and block.mode == "write":
        return block
    raise CompileError("Schema inference must target a Kafka publisher (kafka+connect governed write or kafka write).")


def _safe_avro_name(block: FlowBlock) -> str:
    from services.adapter.naming import tokenize

    value = tokenize(block.entity or block.name) or "record"
    return value if not value[0].isdigit() else f"record_{value}"


def _schema_identity(flow: Flow, block: FlowBlock) -> tuple[str, str, str]:
    from services.adapter.naming import derive_topic_name

    topic = derive_topic_name(flow, block).value
    parts = topic.split(".")
    namespace = ".".join(parts[:-1]) or "raw"
    return topic, _safe_avro_name(block), namespace


def _temporary_topic(flow: Flow, block: FlowBlock, job_id: str) -> str:
    from services.adapter.naming import tokenize

    flow_token = (tokenize(flow.name) or "flow")[:48]
    entity_token = (tokenize(block.entity or block.name) or "record")[:40]
    job_token = (tokenize(job_id) or "job")[:30]
    return f"dmp.schema_inference.{flow_token}.{entity_token}.{job_token}"


async def _connections_and_context(db, flow: Flow) -> tuple[Dict[str, Any], Dict[str, Any], CompileContext, PlatformConnection, PlatformConnection]:
    services = await lifecycle._load_services(db)
    schemas = await lifecycle._load_schemas(db)
    connections = await lifecycle._load_connections(db)
    gateway = await lifecycle._load_gateway(db)
    nifi_doc = lifecycle._active_connection(connections, "nifi")
    kafka_doc = lifecycle._active_connection(connections, "kafka")
    if not nifi_doc or not kafka_doc:
        raise RuntimeError("Schema inference needs active NiFi and Kafka connections.")
    context = lifecycle._build_compile_context(
        services,
        connections,
        gateway,
        {schema.blockId: schema for schema in schemas if schema.flowId == flow.id},
    )
    return (
        lifecycle._nifi_conn_dict(nifi_doc),
        lifecycle._kafka_conn_dict(kafka_doc),
        context,
        nifi_doc,
        kafka_doc,
    )


async def _cleanup(
    db,
    *,
    nifi_conn: Optional[Dict[str, Any]],
    process_group_id: Optional[str],
    parameter_context_id: Optional[str],
    parameter_context_created: bool,
    kafka_conn: Optional[Dict[str, Any]],
    plan_topics: List[str],
) -> List[str]:
    errors: List[str] = []
    if nifi_conn and process_group_id:
        result = await nifi_apply.delete_flow_pg(nifi_conn, process_group_id)
        if not result.get("ok"):
            errors.append(f"Temporary NiFi process group cleanup failed: {result.get('error') or 'unknown error'}")
    if nifi_conn and parameter_context_id and parameter_context_created:
        result = await nifi_apply.delete_parameter_context(nifi_conn, parameter_context_id)
        if not result.get("ok"):
            errors.append(f"Temporary NiFi parameter context cleanup failed: {result.get('error') or 'unknown error'}")
    if kafka_conn:
        for topic_name in reversed(list(dict.fromkeys(plan_topics))):
            result = await topics.delete_topic(kafka_conn, topic_name)
            if not result.get("ok"):
                errors.append(f"Temporary Kafka topic {topic_name!r} cleanup failed: {result.get('error') or 'unknown error'}")
    return errors


async def run_inference_background(db, job_id: str, flow_doc: Dict[str, Any]) -> None:
    """Run one V2 inference job and always perform external cleanup."""
    job = await get_job(db, job_id)
    if not job:
        return

    nifi_conn: Optional[Dict[str, Any]] = None
    kafka_conn: Optional[Dict[str, Any]] = None
    process_group_id: Optional[str] = None
    parameter_context_id: Optional[str] = None
    parameter_context_created = False
    plan_topics: List[str] = []
    final_status = "failed"
    final_fields: Dict[str, Any] = {}

    try:
        if job.get("cancelRequested") or job.get("status") == "stopping":
            raise InferenceCancelled("Schema inference was stopped by the user.")
        flow = Flow(**flow_doc)
        target = target_block_or_error(flow, str(job["targetBlockId"]))
        nifi_conn, kafka_conn, ctx, nifi_doc, kafka_doc = await _connections_and_context(db, flow)
        inference_topic = str(job["inferenceTopic"])

        await update_job(db, job_id, status="deploying", error=None)
        plan = build_inference_plan(
            flow,
            ctx,
            target_block_id=target.id,
            inference_topic=inference_topic,
            job_id=job_id,
        )
        plan_topics = [spec.name for spec in plan.topics]
        topic_results = await topics.ensure_topics(kafka_conn, plan.topics)
        failed_topics = [result for result in topic_results if not result.get("ok")]
        if failed_topics:
            raise RuntimeError(
                "Could not create temporary Kafka topic(s): "
                + ", ".join(str(result.get("name")) for result in failed_topics)
            )

        await update_job(db, job_id, status="deploying")
        applied = await nifi_apply.apply_plan(nifi_conn, plan)
        process_group_id = applied.process_group_id
        parameter_context_id = applied.parameter_context_id
        parameter_context_created = applied.parameter_context_created
        await update_job(db, job_id, status="running", nifiProcessGroupId=process_group_id)

        started = await nifi_apply.start_pg(nifi_conn, process_group_id)
        if not started.get("ok"):
            raise RuntimeError(f"Temporary NiFi process group could not start: {started.get('error') or 'unknown error'}")

        await update_job(db, job_id, status="collecting", messagesCollected=0)
        target_messages = int(job.get("targetMessages") or DEFAULT_TARGET_MESSAGES)
        target_messages = max(1, min(MAX_SAMPLE_RECORDS, target_messages))
        deadline = time.monotonic() + INFERENCE_TIMEOUT_SECONDS
        samples: List[Any] = []
        last_consumer_error: Optional[str] = None
        while len(samples) < target_messages:
            current = await get_job(db, job_id)
            if current and current.get("cancelRequested"):
                raise InferenceCancelled("Schema inference was stopped by the user.")
            if time.monotonic() >= deadline:
                detail = f" Last consumer detail: {last_consumer_error}" if last_consumer_error else ""
                raise RuntimeError(f"No {target_messages} records were produced before the inference timeout.{detail}")

            records, _method, consumer_error = await kafka_schema_consumer.consume_messages_for_inference(
                bootstrap_servers=kafka_conn.get("endpoint") or "",
                topic=inference_topic,
                max_messages=MAX_SAMPLE_RECORDS,
                kafbat_url=kafka_conn.get("kafbat_url"),
                kafbat_username=kafka_conn.get("kafbat_username"),
                kafbat_password=kafka_conn.get("kafbat_password"),
                timeout_ms=min(5000, max(1000, int((deadline - time.monotonic()) * 1000))),
            )
            if consumer_error:
                last_consumer_error = consumer_error
            if records:
                samples = records[:MAX_SAMPLE_RECORDS]
                await update_job(db, job_id, messagesCollected=len(samples))
                if len(samples) >= target_messages:
                    break
            await asyncio.sleep(POLL_SECONDS)

        if not samples:
            raise RuntimeError("The temporary flow produced no records to infer from.")

        await update_job(db, job_id, status="inferring", messagesCollected=len(samples))
        _production_topic, schema_name, namespace = _schema_identity(flow, target)
        # The requested target is the minimum number needed before moving on,
        # while the consumer can return up to the V1-compatible 100-record
        # sample window in one poll. Infer from every record obtained so a
        # field that appears after the first ten records is not lost.
        generated = infer_avro_schema(samples, schema_name, namespace)
        final_status = "complete"
        final_fields = {
            "generatedSchema": generated,
            "schemaStatus": "Needs Verification",
            "messagesCollected": len(samples),
            "error": None,
        }
        await audit(
            db,
            action="V2 schema inference completed",
            target=str(job.get("flowName") or flow.name),
            status="Success",
            details=f"Inferred from {len(samples)} record(s) through temporary NiFi/Kafka runtime.",
            object="Schema",
        )
    except InferenceCancelled as exc:
        final_status = "stopped"
        final_fields = {"error": str(exc)}
    except Exception as exc:
        logger.exception("V2 schema inference job %s failed", job_id)
        final_status = "failed"
        final_fields = {"error": str(exc)[:1000]}
        await audit(db, "V2 schema inference failed", str(job.get("flowName") or job_id), status="Failed", details=str(exc)[:1000], object="Schema")
    finally:
        if process_group_id or plan_topics or parameter_context_id:
            await update_job(db, job_id, status="cleaning_up")
        try:
            cleanup_errors = await _cleanup(
                db,
                nifi_conn=nifi_conn,
                process_group_id=process_group_id,
                parameter_context_id=parameter_context_id,
                parameter_context_created=parameter_context_created,
                kafka_conn=kafka_conn,
                plan_topics=plan_topics,
            )
        except Exception as exc:  # defensive: cleanup must never kill the task
            cleanup_errors = [f"Temporary resource cleanup raised an error: {str(exc)[:500]}"]
            logger.exception("V2 inference cleanup failed for job %s", job_id)
        if cleanup_errors:
            final_fields["cleanupError"] = "; ".join(cleanup_errors)
        final_fields["nifiProcessGroupId"] = None
        final_fields["cancelRequested"] = False
        await update_job(db, job_id, status=final_status, **final_fields)
