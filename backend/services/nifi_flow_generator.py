"""NiFi Flow Generator — Phase 4.

Translates a Source configuration (REST API / SMB / PostgreSQL / MongoDB)
into a real NiFi process group via the NiFi 2.x REST API.

Public entry-point:
    generate_and_deploy_flow(source, flow, flow_id, nifi_conn, kafka_conn, apicurio_conn)
    → {"ok": True, "process_group_id": str}
    → {"ok": False, "error": str}
"""
import asyncio
import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import httpx

from services.http_tls import tls_verify_enabled
from services.nifi_client import nifi_api_request, get_nifi_root_process_group_id, _get_nifi_token
from services.schema_inferencer import sanitize_name
from services.rest_stream_request_resolver import BODY_CONTENT_TYPES, RESPONSE_ACCEPTS
from services.rest_stream_behavior import infer_rest_stream_behaviors
from services.rest_stream_value_mapping import substitute_runtime_values, to_nifi_runtime_template

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schedule helper
# ---------------------------------------------------------------------------

def _schedule_period(source: dict) -> str:
    """Convert source schedule config → NiFi scheduling period string."""
    sched = source.get("schedule") or {}
    val = sched.get("interval_value", 15)
    unit = (sched.get("interval_unit") or "minutes").lower()
    unit_map = {
        "second": "sec", "seconds": "sec",
        "minute": "min", "minutes": "min",
        "hour": "hour", "hours": "hour",
        "day": "day", "days": "day",
    }
    nifi_unit = unit_map.get(unit, "min")
    return f"{val} {nifi_unit}"


# ---------------------------------------------------------------------------
# Primary stream extractor
# ---------------------------------------------------------------------------

def _primary_stream(source: dict) -> Optional[dict]:
    streams = source.get("streams") or []
    for s in streams:
        if s.get("is_primary"):
            return s
    return streams[0] if streams else None


# ---------------------------------------------------------------------------
# Low-level NiFi helpers
# ---------------------------------------------------------------------------

async def _delete_old_pg(
    nifi_url: str, pg_id: str,
    auth_type: str, username: Optional[str], password: Optional[str],
    strict: bool = False,
) -> None:
    """Stop all processors in a process group, disable CSes, purge queues, then delete it (best-effort)."""
    # 1. Stop all processors
    await nifi_api_request(
        nifi_url, "PUT", f"/nifi-api/flow/process-groups/{pg_id}",
        auth_type=auth_type, username=username, password=password,
        json_body={"id": pg_id, "state": "STOPPED", "disconnectedNodeAcknowledged": False},
    )
    await asyncio.sleep(3)

    # 2. Disable all controller services in the PG
    cs_resp = await nifi_api_request(
        nifi_url, "GET", f"/nifi-api/flow/process-groups/{pg_id}/controller-services",
        auth_type=auth_type, username=username, password=password,
    )
    if cs_resp.get("ok"):
        for cs in cs_resp["data"].get("controllerServices", []):
            component = cs.get("component", {})
            service_pg_id = component.get("parentGroupId") or component.get("processGroupId") or component.get("groupId")
            if service_pg_id and service_pg_id != pg_id:
                continue
            cs_id = component.get("id") or cs.get("id")
            cs_rev = cs.get("revision", {}).get("version", 0)
            if cs_id:
                await nifi_api_request(
                    nifi_url, "PUT", f"/nifi-api/controller-services/{cs_id}/run-status",
                    auth_type=auth_type, username=username, password=password,
                    json_body={"revision": {"version": cs_rev}, "state": "DISABLED"},
                )
    await asyncio.sleep(1)

    # 3. Empty queues (flowfiles) inside the process group
    await nifi_api_request(
        nifi_url, "POST", f"/nifi-api/process-groups/{pg_id}/empty-all-connections-requests",
        auth_type=auth_type, username=username, password=password,
        json_body={},
    )
    await asyncio.sleep(2)

    # 4. Get current revision
    r = await nifi_api_request(
        nifi_url, "GET", f"/nifi-api/process-groups/{pg_id}",
        auth_type=auth_type, username=username, password=password,
    )
    if not r.get("ok"):
        logger.info(f"Process group {pg_id} not found — skipping delete")
        if strict:
            raise RuntimeError(f"NiFi process group {pg_id} not found.")
        return

    version = r["data"].get("revision", {}).get("version", 0)

    # 5. Delete (retry once if 409)
    for attempt in range(2):
        del_r = await nifi_api_request(
            nifi_url, "DELETE", f"/nifi-api/process-groups/{pg_id}",
            auth_type=auth_type, username=username, password=password,
            params={"version": str(version), "disconnectedNodeAcknowledged": "false"},
        )
        if del_r.get("ok"):
            logger.info(f"Deleted process group {pg_id}")
            return
        if del_r.get("status_code") == 409:
            logger.warning(f"409 on delete attempt {attempt+1}, waiting and retrying...")
            await asyncio.sleep(3)
            r2 = await nifi_api_request(
                nifi_url, "GET", f"/nifi-api/process-groups/{pg_id}",
                auth_type=auth_type, username=username, password=password,
            )
            if r2.get("ok"):
                version = r2["data"].get("revision", {}).get("version", 0)
        else:
            logger.warning(f"Failed to delete process group {pg_id}: {del_r.get('error')}")
            if strict:
                raise RuntimeError(del_r.get("error") or f"Failed to delete process group {pg_id}")
            return
    logger.warning(f"Could not fully delete process group {pg_id} after retries — proceeding anyway")
    if strict:
        raise RuntimeError(f"Could not fully delete process group {pg_id} after retries.")


async def _create_process_group(
    nifi_url: str, parent_pg_id: str, name: str,
    auth_type: str, username: Optional[str], password: Optional[str],
    x: float = 200.0, y: float = 200.0,
) -> Optional[str]:
    """Create a child process group and return its ID."""
    body = {
        "revision": {"version": 0},
        "component": {"name": name, "position": {"x": x, "y": y}},
    }
    r = await nifi_api_request(
        nifi_url, "POST", f"/nifi-api/process-groups/{parent_pg_id}/process-groups",
        auth_type=auth_type, username=username, password=password,
        json_body=body,
    )
    if r.get("ok"):
        data = r["data"]
        pg_id = data.get("id") or (data.get("component") or {}).get("id")
        logger.info(f"Created process group '{name}': {pg_id}")
        return pg_id
    logger.error(f"Failed to create process group '{name}': {r.get('error')}")
    return None


async def _create_controller_service(
    nifi_url: str, pg_id: str,
    svc_type: str, name: str, properties: Dict[str, Any],
    auth_type: str, username: Optional[str], password: Optional[str],
) -> Tuple[Optional[str], int]:
    """Create a controller service. Returns (id, revision_version)."""
    body = {
        "revision": {"version": 0},
        "component": {"type": svc_type, "name": name, "properties": properties},
    }
    r = await nifi_api_request(
        nifi_url, "POST", f"/nifi-api/process-groups/{pg_id}/controller-services",
        auth_type=auth_type, username=username, password=password,
        json_body=body,
    )
    if r.get("ok"):
        data = r["data"]
        cs_id = data.get("id") or (data.get("component") or {}).get("id")
        rev = (data.get("revision") or {}).get("version", 0)
        logger.info(f"Created controller service '{name}': {cs_id}")
        return cs_id, rev
    logger.error(f"Failed to create controller service '{name}': {r.get('error')}")
    return None, 0


async def _enable_controller_service(
    nifi_url: str, cs_id: str, revision_version: int,
    auth_type: str, username: Optional[str], password: Optional[str],
) -> bool:
    """Enable a controller service (triggers async state transition in NiFi)."""
    r = await nifi_api_request(
        nifi_url, "PUT", f"/nifi-api/controller-services/{cs_id}/run-status",
        auth_type=auth_type, username=username, password=password,
        json_body={
            "revision": {"version": revision_version},
            "state": "ENABLED",
            "disconnectedNodeAcknowledged": False,
        },
    )
    return r.get("ok", False)


async def _wait_cs_enabled(
    nifi_url: str, cs_id: str,
    auth_type: str, username: Optional[str], password: Optional[str],
    timeout: int = 30,
) -> bool:
    """Poll until the controller service reaches ENABLED state."""
    deadline = asyncio.get_event_loop().time() + timeout
    last_state = ""
    last_validation_errors: List[str] = []
    while asyncio.get_event_loop().time() < deadline:
        r = await nifi_api_request(
            nifi_url, "GET", f"/nifi-api/controller-services/{cs_id}",
            auth_type=auth_type, username=username, password=password,
        )
        if r.get("ok"):
            component = r["data"].get("component") or {}
            last_state = component.get("state", "")
            last_validation_errors = component.get("validationErrors") or []
            if last_state == "ENABLED":
                return True
        await asyncio.sleep(2)
    details = f"state={last_state or 'unknown'}"
    if last_validation_errors:
        details += f", validation_errors={last_validation_errors}"
    logger.warning(f"Controller service {cs_id} did not reach ENABLED within {timeout}s ({details})")
    return False


async def _create_processor(
    nifi_url: str, pg_id: str,
    proc_type: str, name: str,
    properties: Dict[str, Any],
    auto_terminate: List[str],
    auth_type: str, username: Optional[str], password: Optional[str],
    scheduling_strategy: str = "TIMER_DRIVEN",
    scheduling_period: str = "0 sec",
    x: float = 400.0, y: float = 200.0,
) -> Optional[str]:
    """Create a processor and return its ID."""
    # Strip None values from properties (NiFi rejects null values for some props)
    clean_props = {k: v for k, v in properties.items() if v is not None}
    body = {
        "revision": {"version": 0},
        "component": {
            "type": proc_type,
            "name": name,
            "position": {"x": x, "y": y},
            "config": {
                "properties": clean_props,
                "schedulingStrategy": scheduling_strategy,
                "schedulingPeriod": scheduling_period,
                "autoTerminatedRelationships": auto_terminate,
            },
        },
    }
    r = await nifi_api_request(
        nifi_url, "POST", f"/nifi-api/process-groups/{pg_id}/processors",
        auth_type=auth_type, username=username, password=password,
        json_body=body,
    )
    if r.get("ok"):
        data = r["data"]
        proc_id = data.get("id") or (data.get("component") or {}).get("id")
        logger.info(f"Created processor '{name}': {proc_id}")
        return proc_id
    logger.error(f"Failed to create processor '{name}' ({proc_type}): {r.get('error')}")
    return None


async def _create_connection(
    nifi_url: str, pg_id: str,
    src_id: str, dst_id: str,
    relationships: List[str],
    auth_type: str, username: Optional[str], password: Optional[str],
    src_type: str = "PROCESSOR",
    dst_type: str = "PROCESSOR",
    src_group_id: Optional[str] = None,
    dst_group_id: Optional[str] = None,
) -> Optional[str]:
    """Create a connection between two components (processors or ports)."""
    effective_src_group = src_group_id or pg_id
    effective_dst_group = dst_group_id or pg_id
    # Ports have no outgoing relationship labels; NiFi requires empty list.
    effective_rels = relationships if src_type == "PROCESSOR" else []
    body = {
        "revision": {"version": 0},
        "component": {
            "source": {"id": src_id, "groupId": effective_src_group, "type": src_type},
            "destination": {"id": dst_id, "groupId": effective_dst_group, "type": dst_type},
            "selectedRelationships": effective_rels,
        },
    }
    r = await nifi_api_request(
        nifi_url, "POST", f"/nifi-api/process-groups/{pg_id}/connections",
        auth_type=auth_type, username=username, password=password,
        json_body=body,
    )
    if r.get("ok"):
        return r["data"].get("id") or (r["data"].get("component") or {}).get("id")
    logger.error(f"Failed to create connection {src_id}->{dst_id}: {r.get('error')}")
    return None


async def _create_input_port(
    nifi_url: str, pg_id: str, name: str,
    auth_type: str, username: Optional[str], password: Optional[str],
    x: float = 200.0, y: float = 100.0,
) -> Optional[str]:
    """Create an input port in a process group and return its ID."""
    body = {
        "revision": {"version": 0},
        "component": {"name": name, "position": {"x": x, "y": y}},
    }
    r = await nifi_api_request(
        nifi_url, "POST", f"/nifi-api/process-groups/{pg_id}/input-ports",
        auth_type=auth_type, username=username, password=password,
        json_body=body,
    )
    if r.get("ok"):
        data = r["data"]
        port_id = data.get("id") or (data.get("component") or {}).get("id")
        logger.info(f"Created input port '{name}': {port_id}")
        return port_id
    logger.error(f"Failed to create input port '{name}': {r.get('error')}")
    return None


async def _create_output_port(
    nifi_url: str, pg_id: str, name: str,
    auth_type: str, username: Optional[str], password: Optional[str],
    x: float = 200.0, y: float = 900.0,
) -> Optional[str]:
    """Create an output port in a process group and return its ID."""
    body = {
        "revision": {"version": 0},
        "component": {"name": name, "position": {"x": x, "y": y}},
    }
    r = await nifi_api_request(
        nifi_url, "POST", f"/nifi-api/process-groups/{pg_id}/output-ports",
        auth_type=auth_type, username=username, password=password,
        json_body=body,
    )
    if r.get("ok"):
        data = r["data"]
        port_id = data.get("id") or (data.get("component") or {}).get("id")
        logger.info(f"Created output port '{name}': {port_id}")
        return port_id
    logger.error(f"Failed to create output port '{name}': {r.get('error')}")
    return None


# ---------------------------------------------------------------------------
# Controller service builders
# ---------------------------------------------------------------------------

def _normalize_apicurio_ccompat_url(endpoint: str) -> str:
    """Return a valid Apicurio ccompat base URL from any configured endpoint."""
    raw = (endpoint or "").rstrip("/")
    if not raw:
        return ""
    if "/apis/ccompat/" in raw:
        return raw

    root = raw
    for marker in ("/apis/registry/v3", "/apis/registry/v2", "/api/groups", "/api/artifacts"):
        idx = root.find(marker)
        if idx != -1:
            root = root[:idx]
            break
    root = root.rstrip("/")
    return f"{root}/apis/ccompat/v7"


async def _create_standard_controller_services(
    nifi_url: str, pg_id: str,
    kafka_bootstrap: str,
    apicurio_base_url: str,
    schema_name: Optional[str],
    auth_type: str, username: Optional[str], password: Optional[str],
    global_service_ids: Optional[Dict[str, str]] = None,
) -> Dict[str, Optional[str]]:
    """
    Create the standard controller services for any flow:
      - Kafka3ConnectionService
      - ConfluentSchemaRegistry (pointing at Apicurio ccompat endpoint)
      - ConfluentEncodedSchemaReferenceWriter
      - JsonTreeReader
      - JsonRecordSetWriter (intermediate transform writer)
      - AvroRecordSetWriter (transform-safe, embedded schema)
      - AvroRecordSetWriter

    Returns dict keyed by role:
      kafka_svc, schema_reg, schema_ref_writer, json_reader, json_writer,
      avro_transform_writer, avro_writer.
    All IDs may be None if creation failed.
    """
    schema_registry_url = _normalize_apicurio_ccompat_url(apicurio_base_url)
    inherited = dict(global_service_ids or {})
    inherited_schema_reg_type = str(inherited.get("schema_registry_type") or "")
    inherited_schema_reg_is_confluent = (
        not inherited.get("schema_registry")
        or not inherited_schema_reg_type
        or inherited_schema_reg_type.endswith("ConfluentSchemaRegistry")
    )

    # 1. Kafka3ConnectionService
    kafka_svc_id = inherited.get("kafka") or None
    kafka_rev = 0
    if not kafka_svc_id:
        kafka_svc_id, kafka_rev = await _create_controller_service(
            nifi_url, pg_id,
            "org.apache.nifi.kafka.service.Kafka3ConnectionService",
            "KafkaConnectionService",
            properties={
                "bootstrap.servers": kafka_bootstrap,
                "security.protocol": "PLAINTEXT",
                "ack.wait.time": "60 sec",
                "default.api.timeout.ms": "120 sec",
            },
            auth_type=auth_type, username=username, password=password,
        )

    # 2. ConfluentSchemaRegistry. Only create schema-registry services when
    # this flow actually has a linked schema; otherwise NiFi validates an empty
    # registry URL even for non-publishing REST proof flows.
    schema_reg_id = inherited.get("schema_registry") or None
    schema_reg_rev = 0
    needs_schema_registry = bool(schema_name and schema_registry_url)
    if not schema_reg_id and needs_schema_registry:
        schema_reg_id, schema_reg_rev = await _create_controller_service(
            nifi_url, pg_id,
            "org.apache.nifi.confluent.schemaregistry.ConfluentSchemaRegistry",
            "ApicurioSchemaRegistry",
            properties={
                "Schema Registry URLs": schema_registry_url,
                "Communications Timeout": "30 secs",
                "Cache Size": "1000",
                "Cache Expiration": "1 hour",
            },
            auth_type=auth_type, username=username, password=password,
        )

    # Kafka consumers such as Kafka Connect's Apicurio AvroConverter expect a
    # schema id in the Kafka payload. If the shared global registry is NiFi's
    # native Apicurio service, keep it for readers but create a local
    # Confluent-compatible registry service for Kafka serialization.
    kafka_schema_reg_id = schema_reg_id
    kafka_schema_reg_rev = schema_reg_rev
    if inherited.get("schema_registry") and not inherited_schema_reg_is_confluent and needs_schema_registry:
        kafka_schema_reg_id, kafka_schema_reg_rev = await _create_controller_service(
            nifi_url, pg_id,
            "org.apache.nifi.confluent.schemaregistry.ConfluentSchemaRegistry",
            "KafkaSchemaRegistry",
            properties={
                "Schema Registry URLs": schema_registry_url,
                "Communications Timeout": "30 secs",
                "Cache Size": "1000",
                "Cache Expiration": "1 hour",
            },
            auth_type=auth_type, username=username, password=password,
        )

    # 3. Confluent-encoded schema reference writer.
    # This writes the registry id prefix expected by Confluent-compatible
    # consumers, including Apicurio's ccompat API.
    schema_ref_writer_id = None
    schema_ref_writer_rev = 0
    if kafka_schema_reg_id and schema_name:
        schema_ref_writer_id, schema_ref_writer_rev = await _create_controller_service(
            nifi_url, pg_id,
            "org.apache.nifi.confluent.schemaregistry.ConfluentEncodedSchemaReferenceWriter",
            "ConfluentEncodedSchemaReferenceWriter",
            properties={},
            auth_type=auth_type, username=username, password=password,
        )

    # 4. JsonTreeReader  (infer schema from flow content)
    reader_props: Dict[str, Any] = {
        "Schema Access Strategy": "infer-schema",
        "Starting Field Strategy": "ROOT_NODE",
        "Max String Length": "20 MB",
    }
    if schema_reg_id and schema_name:
        reader_props["Schema Access Strategy"] = "schema-name"
        reader_props["Schema Registry"] = schema_reg_id
        reader_props["Schema Name"] = schema_name

    json_reader_id, json_reader_rev = await _create_controller_service(
        nifi_url, pg_id,
        "org.apache.nifi.json.JsonTreeReader",
        "JsonReader",
        properties=reader_props,
        auth_type=auth_type, username=username, password=password,
    )

    # 5. JsonRecordSetWriter (for transformation chain intermediate output)
    json_writer_id, json_writer_rev = await _create_controller_service(
        nifi_url, pg_id,
        "org.apache.nifi.json.JsonRecordSetWriter",
        "JsonTransformWriter",
        properties={
            "Schema Access Strategy": "inherit-record-schema",
            "Pretty Print JSON": "false",
            "Suppress Null Values": "never-suppress",
            "Output Grouping": "output-oneline",
            "Compression Format": "none",
        },
        auth_type=auth_type, username=username, password=password,
    )

    # 6. AvroRecordSetWriter for transformed records that change schema.
    # Uses embedded schema to avoid schema-reference lookup errors on
    # dynamically transformed schemas.
    avro_transform_writer_id, avro_transform_writer_rev = await _create_controller_service(
        nifi_url, pg_id,
        "org.apache.nifi.avro.AvroRecordSetWriter",
        "AvroTransformWriter",
        properties={
            "Schema Access Strategy": "inherit-record-schema",
            "Schema Write Strategy": "avro-embedded",
            "Compression Format": "NONE",
        },
        auth_type=auth_type, username=username, password=password,
    )

    # 7. AvroRecordSetWriter
    writer_props: Dict[str, Any] = {
        # NiFi 2.x AvroRecordSetWriter valid values: inherit-record-schema, schema-name, schema-text-property
        "Schema Access Strategy": "inherit-record-schema",
        "Compression Format": "NONE",
    }
    if schema_reg_id and schema_name:
        writer_props["Schema Access Strategy"] = "schema-name"
        writer_props["Schema Registry"] = kafka_schema_reg_id or schema_reg_id
        writer_props["Schema Name"] = schema_name
    if schema_ref_writer_id:
        writer_props["Schema Write Strategy"] = "schema-reference-writer"
        writer_props["Schema Reference Writer"] = schema_ref_writer_id
    elif schema_reg_id and schema_name:
        writer_props["Schema Write Strategy"] = "avro-embedded"

    avro_writer_id, avro_writer_rev = await _create_controller_service(
        nifi_url, pg_id,
        "org.apache.nifi.avro.AvroRecordSetWriter",
        "AvroWriter",
        properties=writer_props,
        auth_type=auth_type, username=username, password=password,
    )

    # Enable all services (order matters — schema registry must enable before reader/writer)
    enable_list = [
        (None if inherited.get("kafka") else kafka_svc_id, kafka_rev, "KafkaConnectionService"),
        (None if inherited.get("schema_registry") else schema_reg_id, schema_reg_rev, "ApicurioSchemaRegistry"),
        (kafka_schema_reg_id if kafka_schema_reg_id != schema_reg_id else None, kafka_schema_reg_rev, "KafkaSchemaRegistry"),
        (schema_ref_writer_id, schema_ref_writer_rev, "ConfluentEncodedSchemaReferenceWriter"),
        (json_reader_id, json_reader_rev, "JsonReader"),
        (json_writer_id, json_writer_rev, "JsonTransformWriter"),
        (avro_transform_writer_id, avro_transform_writer_rev, "AvroTransformWriter"),
        (avro_writer_id, avro_writer_rev, "AvroWriter"),
    ]
    for cs_id, rev, label in enable_list:
        if cs_id:
            ok = await _enable_controller_service(
                nifi_url, cs_id, rev,
                auth_type=auth_type, username=username, password=password,
            )
            if ok:
                await _wait_cs_enabled(
                    nifi_url, cs_id,
                    auth_type=auth_type, username=username, password=password,
                )
            else:
                logger.warning(f"Could not initiate enable for {label} ({cs_id})")

    return {
        "kafka_svc": kafka_svc_id,
        "schema_reg": schema_reg_id,
        "kafka_schema_reg": kafka_schema_reg_id,
        "schema_ref_writer": schema_ref_writer_id,
        "json_reader": json_reader_id,
        "json_writer": json_writer_id,
        "avro_transform_writer": avro_transform_writer_id,
        "avro_writer": avro_writer_id,
    }


async def _create_schema_avro_writer(
    nifi_url: str,
    pg_id: str,
    name: str,
    schema_name: str,
    schema_version: Optional[Any],
    schema_reg_id: Optional[str],
    schema_ref_writer_id: Optional[str],
    auth_type: str,
    username: Optional[str],
    password: Optional[str],
) -> Optional[str]:
    """Create an Avro writer for one Kafka entity schema."""
    if not schema_name:
        return None
    props: Dict[str, Any] = {
        "Schema Access Strategy": "schema-name",
        "Schema Name": schema_name,
        "Compression Format": "NONE",
    }
    if schema_version not in (None, ""):
        props["Schema Version"] = str(schema_version)
    if schema_reg_id:
        props["Schema Registry"] = schema_reg_id
    if schema_ref_writer_id:
        props["Schema Write Strategy"] = "schema-reference-writer"
        props["Schema Reference Writer"] = schema_ref_writer_id
    else:
        props["Schema Write Strategy"] = "avro-embedded"

    writer_id, writer_rev = await _create_controller_service(
        nifi_url, pg_id,
        "org.apache.nifi.avro.AvroRecordSetWriter",
        name,
        properties=props,
        auth_type=auth_type, username=username, password=password,
    )
    if not writer_id:
        return None
    ok = await _enable_controller_service(
        nifi_url, writer_id, writer_rev,
        auth_type=auth_type, username=username, password=password,
    )
    if ok:
        await _wait_cs_enabled(
            nifi_url, writer_id,
            auth_type=auth_type, username=username, password=password,
        )
    return writer_id


async def _create_schema_json_reader(
    nifi_url: str,
    pg_id: str,
    name: str,
    schema_name: str,
    schema_version: Optional[Any],
    schema_reg_id: Optional[str],
    auth_type: str,
    username: Optional[str],
    password: Optional[str],
) -> Optional[str]:
    """Create a JSON reader for one entity schema before Avro conversion."""
    if not schema_name:
        return None
    props: Dict[str, Any] = {
        "Schema Access Strategy": "schema-name" if schema_reg_id else "infer-schema",
        "Starting Field Strategy": "ROOT_NODE",
        "Max String Length": "20 MB",
    }
    if schema_reg_id:
        props["Schema Registry"] = schema_reg_id
        props["Schema Name"] = schema_name
        if schema_version not in (None, ""):
            props["Schema Version"] = str(schema_version)

    reader_id, reader_rev = await _create_controller_service(
        nifi_url, pg_id,
        "org.apache.nifi.json.JsonTreeReader",
        name,
        properties=props,
        auth_type=auth_type, username=username, password=password,
    )
    if not reader_id:
        return None
    ok = await _enable_controller_service(
        nifi_url, reader_id, reader_rev,
        auth_type=auth_type, username=username, password=password,
    )
    if ok:
        await _wait_cs_enabled(
            nifi_url, reader_id,
            auth_type=auth_type, username=username, password=password,
        )
    return reader_id


async def _create_xml_reader_service(
    nifi_url: str, pg_id: str,
    schema_reg_id: Optional[str],
    schema_name: Optional[str],
    auth_type: str, username: Optional[str], password: Optional[str],
) -> Tuple[Optional[str], int]:
    """Create an XMLReader controller service for XML-response streams.

    Uses schema-name strategy when a schema registry + name are available;
    otherwise falls back to infer-schema.
    """
    props: Dict[str, Any] = {
        "Parse XML Attributes": "true",
        "Attribute Prefix": "_",
        "Expect Records as Array": "false",
        "Field Name for Content": "original_content",
    }
    if schema_reg_id and schema_name:
        props["Schema Access Strategy"] = "schema-name"
        props["Schema Registry"] = schema_reg_id
        props["Schema Name"] = schema_name
    else:
        props["Schema Access Strategy"] = "infer-schema"

    return await _create_controller_service(
        nifi_url, pg_id,
        "org.apache.nifi.xml.XMLReader",
        "XMLReader",
        properties=props,
        auth_type=auth_type, username=username, password=password,
    )


def _normalize_csv_option_char(value: Any, fallback: str) -> str:
    text = str(value if value is not None else "").strip()
    if not text:
        return fallback
    aliases = {
        r"\t": "\t",
        "tab": "\t",
        r"\n": "\n",
        r"\r": "\r",
    }
    if text in aliases:
        return aliases[text]
    return text[0]


async def _create_rest_csv_reader_service(
    nifi_url: str,
    pg_id: str,
    stream: Dict[str, Any],
    *,
    auth_type: str,
    username: Optional[str],
    password: Optional[str],
) -> Tuple[Optional[str], int]:
    """Create a CSVReader controller service scoped to a single REST stream."""
    delimiter = _normalize_csv_option_char(stream.get("csv_delimiter"), ",")
    quote_raw = stream.get("csv_quote_char")
    quote_char = _normalize_csv_option_char(quote_raw, '"') if quote_raw not in (None, "") else '"'
    escape_raw = stream.get("csv_escape_char")
    escape_char = _normalize_csv_option_char(escape_raw, "\\") if escape_raw not in (None, "") else "\\"
    has_header = stream.get("csv_has_header_row")
    has_header = True if has_header is None else bool(has_header)
    # Keep REST CSV parsing consistent and simple in v1.
    charset = "utf-8"
    stream_name = sanitize_name(str(stream.get("name") or stream.get("id") or "stream")) or "stream"

    props: Dict[str, Any] = {
        "Treat First Line as Header": "true" if has_header else "false",
        "Trim Fields": "true",
        "CSV Format": "custom",
        "Value Separator": delimiter,
        "CSV Parser": "commons-csv",
        "Quote Character": quote_char,
        "Escape Character": escape_char,
        "Character Set": charset,
        "Schema Access Strategy": "infer-schema",
        "Ignore CSV Header Column Names": "false" if has_header else "true",
    }

    return await _create_controller_service(
        nifi_url, pg_id,
        "org.apache.nifi.csv.CSVReader",
        f"{stream_name}__CSVReader",
        properties=props,
        auth_type=auth_type, username=username, password=password,
    )


async def _create_smb_xlsx_controller_services(
    nifi_url: str, pg_id: str,
    schema_reg_id: Optional[str],
    schema_name: Optional[str],
    auth_type: str, username: Optional[str], password: Optional[str],
) -> Dict[str, Optional[str]]:
    """Create ExcelReader + CSVWriter + CSVReader controller services for SMB XLSX flows."""

    # ExcelReader — reads XLSX header row as schema
    excel_reader_id, excel_reader_rev = await _create_controller_service(
        nifi_url, pg_id,
        "org.apache.nifi.excel.ExcelReader",
        "ExcelReader",
        properties={
            "Schema Access Strategy": "Use Starting Row",
            "Input File Type": "XLSX",
            "Starting Row": "1",
            "Row Evaluation Strategy": "STANDARD",
        },
        auth_type=auth_type, username=username, password=password,
    )

    # CSVWriter — writes intermediate CSV (inherits schema from reader)
    csv_writer_id, csv_writer_rev = await _create_controller_service(
        nifi_url, pg_id,
        "org.apache.nifi.csv.CSVRecordSetWriter",
        "CSVWriter",
        properties={
            "Schema Access Strategy": "inherit-record-schema",
            "Include Header Line": "true",
            "Value Separator": ",",
            "Quote Mode": "MINIMAL",
            "Record Separator": "\\n",
            "Trim Fields": "true",
            "Character Set": "UTF-8",
        },
        auth_type=auth_type, username=username, password=password,
    )

    # CSVReader — reads the intermediate CSV.
    # When a linked schema is available, map columns by configured schema
    # (ignore header names) so output fields align with the schema names used
    # by the Avro writer. Otherwise fall back to header-derived mode.
    csv_reader_props: Dict[str, Any] = {
        "Treat First Line as Header": "true",
        "Trim Fields": "true",
        "Value Separator": ",",
        "Quote Character": "\"",
        "Character Set": "UTF-8",
        "CSV Format": "custom",
        "CSV Parser": "commons-csv",
    }
    if schema_reg_id and schema_name:
        csv_reader_props["Schema Access Strategy"] = "schema-name"
        csv_reader_props["Schema Registry"] = schema_reg_id
        csv_reader_props["Schema Name"] = schema_name
        csv_reader_props["Ignore CSV Header Column Names"] = "true"
    else:
        csv_reader_props["Schema Access Strategy"] = "csv-header-derived"
        csv_reader_props["Ignore CSV Header Column Names"] = "false"

    csv_reader_id, csv_reader_rev = await _create_controller_service(
        nifi_url, pg_id,
        "org.apache.nifi.csv.CSVReader",
        "CSVReader",
        properties=csv_reader_props,
        auth_type=auth_type, username=username, password=password,
    )

    # Enable all (schema_reg already enabled; Excel/CSV services have no deps)
    enable_list = [
        (excel_reader_id, excel_reader_rev, "ExcelReader"),
        (csv_writer_id, csv_writer_rev, "CSVWriter"),
        (csv_reader_id, csv_reader_rev, "CSVReader"),
    ]
    for cs_id, rev, label in enable_list:
        if cs_id:
            ok = await _enable_controller_service(
                nifi_url, cs_id, rev,
                auth_type=auth_type, username=username, password=password,
            )
            if ok:
                await _wait_cs_enabled(
                    nifi_url, cs_id,
                    auth_type=auth_type, username=username, password=password,
                )
            else:
                logger.warning(f"Could not initiate enable for {label} ({cs_id})")

    return {
        "excel_reader": excel_reader_id,
        "csv_writer": csv_writer_id,
        "csv_reader": csv_reader_id,
    }


# ---------------------------------------------------------------------------
# Per-source-type processor chains
# ---------------------------------------------------------------------------

async def _build_rest_api_flow(
    nifi_url: str, pg_id: str, source: dict,
    kafka_svc_id: Optional[str],
    json_reader_id: Optional[str],
    schema_reg_id: Optional[str],
    avro_writer_id: Optional[str],
    kafka_schema_reg_id: Optional[str],
    schema_ref_writer_id: Optional[str],
    kafka_topic: str,
    schedule_str: str,
    auth_type_nifi: str, username_nifi: Optional[str], password_nifi: Optional[str],
    xml_reader_id: Optional[str] = None,
    json_writer_id: Optional[str] = None,
    avro_transform_writer_id: Optional[str] = None,
    target_stream_id: Optional[str] = None,
) -> bool:
    """Build advanced REST API processor chain (Airbyte-inspired).

    Topology for each stream:
        [Trigger →] InitPagination → InvokeHTTP → SplitJson(records) ─┬→ Extract → [Filter] → [Enrich] → ─┐
                       ↑                          original branch ↓                                       ↓
                       └────────── NextPage ←── HasMore ←── PageMeta              [child stream] OR PublishKafka

    Multi-stream fan-out: a child stream (with `fan_out.parent_stream_id` set) is appended
    to its parent's "tail" relationship — its InvokeHTTP URL can interpolate parent attributes
    via NiFi EL (e.g. ${site_id}).
    """
    nargs = dict(auth_type=auth_type_nifi, username=username_nifi, password=password_nifi)
    src_auth = (source.get("auth_type") or "NONE").upper()
    base_url = (source.get("base_url") or "").rstrip("/")
    streams = source.get("streams") or []
    if not streams:
        # Fall back to a single empty stream so we still produce a valid PG
        streams = [{"id": "default", "name": "default", "endpoint_path": "/", "is_primary": True}]
    for stream in streams:
        method = str(stream.get("method") or "GET").strip().upper()
        body_format = _body_format_for_stream(stream)
        body_template = _body_template_for_stream(stream)
        if method == "DELETE" and body_format != "empty" and body_template.strip():
            error = (
                f"Stream {stream.get('name') or stream.get('id') or 'stream'}: "
                "DELETE request bodies are not supported by NiFi InvokeHTTP; "
                "use DELETE without a body or defer this endpoint until a non-InvokeHTTP compiler is added."
            )
            logger.error(error)
            source["__nifi_build_error"] = error
            return False

    # ── 1. Single Trigger for the whole flow ─────────────────────────────
    trigger_id = await _create_processor(
        nifi_url, pg_id,
        "org.apache.nifi.processors.standard.GenerateFlowFile",
        "trigger",
        properties={"File Size": "0 B", "Batch Size": "1", "Data Format": "Text", "Custom Text": "trigger"},
        auto_terminate=[],
        scheduling_period=schedule_str,
        x=400.0, y=100.0,
        **nargs,
    )
    if not trigger_id:
        return False

    # ── 2. Auth header attribute holder ──────────────────────────────────
    auth_attr_props: Dict[str, Any] = {}
    auth_header_pattern_keys: List[str] = []
    if src_auth == "BEARER" and source.get("bearer_token"):
        auth_attr_props["Authorization"] = f"Bearer {source['bearer_token']}"
        auth_header_pattern_keys.append("Authorization")
    elif src_auth == "API_KEY" and (source.get("api_key_location") or "").upper() == "HEADER":
        api_key_name = source.get("api_key_name") or "X-Api-Key"
        auth_attr_props[api_key_name] = source.get("api_key_value") or ""
        auth_header_pattern_keys.append(api_key_name)

    set_auth_id = await _create_processor(
        nifi_url, pg_id,
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        "set_request_meta",
        properties=auth_attr_props,
        auto_terminate=[],
        x=400.0, y=300.0,
        **nargs,
    )
    if not set_auth_id:
        return False

    await _create_connection(nifi_url, pg_id, trigger_id, set_auth_id, ["success"], **nargs)

    # Dicts populated after position assignment (below)
    stream_child_pgs: Dict[str, str] = {}
    stream_entry_ports: Dict[str, str] = {}
    stream_proxy_procs: Dict[str, str] = {}

    # ── 3. Order streams: parents before children for fan-out wiring ─────
    ordered = _topological_order(streams)

    # Each entry stores per-stream processor IDs so children can attach
    stream_results: Dict[str, Dict[str, Any]] = {}
    avro_writer_cache: Dict[str, str] = {}
    json_reader_cache: Dict[str, str] = {}
    xml_reader_cache: Dict[str, str] = {}

    def _sid(stream_obj: dict, fallback_idx: int = 0) -> str:
        return str(stream_obj.get("id") or stream_obj.get("name") or f"stream_{fallback_idx}")

    children_by_parent: Dict[str, List[str]] = {}
    child_ids: set[str] = set()
    for stream_idx, stream_obj in enumerate(ordered):
        sid = _sid(stream_obj, stream_idx)
        fan = stream_obj.get("fan_out") or {}
        route_src = stream_obj.get("route_source") or {}
        parent_id = ""
        if fan.get("enabled") and fan.get("parent_stream_id"):
            parent_id = str(fan.get("parent_stream_id"))
        elif isinstance(route_src, dict) and route_src.get("parent_stream_id"):
            parent_id = str(route_src.get("parent_stream_id"))
        if parent_id:
            children_by_parent.setdefault(parent_id, []).append(sid)
            child_ids.add(sid)

    roots = [
        _sid(stream_obj, stream_idx)
        for stream_idx, stream_obj in enumerate(ordered)
        if _sid(stream_obj, stream_idx) not in child_ids
    ]
    positions: Dict[str, Tuple[float, float]] = {}
    root_x = 700.0
    root_gap_x = 1100.0
    branch_gap_x = 900.0
    stream_gap_y = 1800.0
    start_y = 500.0

    def assign_positions(stream_id: str, x_pos: float, y_pos: float) -> None:
        if stream_id in positions:
            return
        positions[stream_id] = (x_pos, y_pos)
        children = children_by_parent.get(stream_id) or []
        if not children:
            return
        child_y = y_pos + stream_gap_y
        if len(children) == 1:
            assign_positions(children[0], x_pos, child_y)
            return
        center = (len(children) - 1) / 2.0
        for child_idx, child_id in enumerate(children):
            assign_positions(child_id, x_pos + (child_idx - center) * branch_gap_x, child_y)

    for root_idx, root_id in enumerate(roots):
        assign_positions(root_id, root_x + root_idx * root_gap_x, start_y)

    # ── Pre-create per-stream child PGs, entry input ports, and proxy processors ──
    for _pre_idx, _pre_stream in enumerate(ordered):
        _pre_sid = _sid(_pre_stream, _pre_idx)
        _pre_x, _pre_y = positions.get(_pre_sid, (root_x + _pre_idx * root_gap_x, start_y))
        _child_pg = await _create_process_group(
            nifi_url, pg_id, f"{_pre_sid}",
            auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
            x=_pre_x, y=_pre_y,
        )
        if not _child_pg:
            return False
        stream_child_pgs[_pre_sid] = _child_pg

        _in_port = await _create_input_port(
            nifi_url, _child_pg, f"{_pre_sid}__entry",
            auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
            x=200.0, y=100.0,
        )
        if not _in_port:
            return False
        stream_entry_ports[_pre_sid] = _in_port

        _proxy = await _create_processor(
            nifi_url, _child_pg,
            "org.apache.nifi.processors.attributes.UpdateAttribute",
            f"{_pre_sid}__entry_proxy",
            properties={},
            auto_terminate=[],
            x=200.0, y=280.0,
            auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
        )
        if not _proxy:
            return False
        stream_proxy_procs[_pre_sid] = _proxy

        # Wire input_port → proxy inside child PG (INPUT_PORT as source)
        conn = await _create_connection(
            nifi_url, _child_pg, _in_port, _proxy, [],
            auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
            src_type="INPUT_PORT",
        )
        if not conn:
            return False

    # ── Wire root-stream entries from set_auth at flow PG level ──
    for _root_sid in roots:
        _root_child_pg = stream_child_pgs[_root_sid]
        _root_in_port = stream_entry_ports[_root_sid]
        conn = await _create_connection(
            nifi_url, pg_id, set_auth_id, _root_in_port, ["success"],
            auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
            dst_type="INPUT_PORT",
            dst_group_id=_root_child_pg,
        )
        if not conn:
            return False

    for idx, stream in enumerate(ordered):
        sid = _sid(stream, idx)
        child_pg = stream_child_pgs[sid]
        proxy_id = stream_proxy_procs[sid]

        result = await _build_stream_chain(
            nifi_url, child_pg, source, stream,
            entry_processor_id=proxy_id,
            entry_relationship="success",
            stream_results=stream_results,
            kafka_svc_id=kafka_svc_id,
            json_reader_id=json_reader_id,
            schema_reg_id=schema_reg_id,
            xml_reader_id=xml_reader_id,
            avro_writer_id=avro_writer_id,
            kafka_schema_reg_id=kafka_schema_reg_id,
            schema_ref_writer_id=schema_ref_writer_id,
            avro_writer_cache=avro_writer_cache,
            json_reader_cache=json_reader_cache,
            xml_reader_cache=xml_reader_cache,
            json_writer_id=json_writer_id,
            avro_transform_writer_id=avro_transform_writer_id,
            target_stream_id=target_stream_id,
            kafka_topic=kafka_topic,
            base_url=base_url,
            src_auth=src_auth,
            auth_header_pattern_keys=auth_header_pattern_keys,
            source_username=source.get("rest_username") or "",
            source_password=source.get("rest_password") or "",
            connection_timeout=source.get("connection_timeout") or 30,
            read_timeout=source.get("read_timeout") or 30,
            x=400.0, y=460.0,
            auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
            external_entry=(proxy_id, "success"),
            shared_pg_id=pg_id,
        )
        if not result:
            return False
        stream_results[sid] = result

        # ── Post-process: wire cross-PG connections from this stream to its children ──

        # A) Route children: each RouteOnAttribute in this stream's PG connects to the
        #    matched child stream's PG via an output port → input port pair.
        for route_ep in result.get("route_entry_points") or []:
            child_sid = (route_ep.get("child_stream_id") or "").strip()
            target_child_pg = stream_child_pgs.get(child_sid)
            target_in_port = stream_entry_ports.get(child_sid)
            target_proxy = stream_proxy_procs.get(child_sid)
            if not (target_child_pg and target_in_port and target_proxy):
                continue
            roa_id = route_ep["roa_id"]
            route_rel = route_ep["rel"]

            out_port = await _create_output_port(
                nifi_url, child_pg,
                f"{sid}__to__{child_sid}",
                auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
                x=400.0, y=9000.0,
            )
            if not out_port:
                return False

            # roa → output port within parent's child PG
            conn = await _create_connection(
                nifi_url, child_pg, roa_id, out_port, [route_rel],
                auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
                dst_type="OUTPUT_PORT",
            )
            if not conn:
                return False
            # output port → input port at top-level flow PG level (cross-PG)
            conn = await _create_connection(
                nifi_url, pg_id, out_port, target_in_port, [],
                auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
                src_type="OUTPUT_PORT", src_group_id=child_pg,
                dst_type="INPUT_PORT", dst_group_id=target_child_pg,
            )
            if not conn:
                return False
            # Update route_ep so fallback resolution (external_entry=None path) still works
            route_ep["roa_id"] = target_proxy
            route_ep["rel"] = "success"

        # B) Fan-out children: parent's tail processor connects to each fan-out child's PG.
        fan_tail_id = result.get("tail_id")
        fan_tail_rel = result.get("tail_relationship", "success")
        for child_sid in children_by_parent.get(sid) or []:
            # Skip route children — already handled above via route_entry_points.
            child_obj = next(
                (s for s_idx, s in enumerate(ordered) if _sid(s, s_idx) == child_sid),
                None,
            )
            if child_obj is None:
                continue
            c_route_src = child_obj.get("route_source") or {}
            if isinstance(c_route_src, dict) and c_route_src.get("parent_stream_id"):
                continue  # route child — handled above

            target_child_pg = stream_child_pgs.get(child_sid)
            target_in_port = stream_entry_ports.get(child_sid)
            if not (target_child_pg and target_in_port and fan_tail_id):
                continue

            completion_mode = str((child_obj.get("fan_out") or {}).get("completion_mode") or "per_record").strip().lower()
            wait_for_all = bool(stream.get("wait_for_all_records_before_downstream")) or completion_mode == "after_all"
            branch_src_id = fan_tail_id
            branch_src_rel = fan_tail_rel
            if wait_for_all:
                barrier_id = await _create_processor(
                    nifi_url, child_pg,
                    "org.apache.nifi.processors.standard.MergeContent",
                    f"{sid}__after_all__{child_sid}",
                    properties={
                        "Merge Strategy": "Defragment",
                        "Merge Format": "Binary Concatenation",
                        "Attribute Strategy": "Keep Only Common Attributes",
                        "Maximum Number of Bins": "100",
                        "Max Bin Age": "5 min",
                    },
                    auto_terminate=["failure", "original"],
                    auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
                    x=400.0, y=9100.0,
                )
                if not barrier_id:
                    return False
                conn = await _create_connection(
                    nifi_url, child_pg, fan_tail_id, barrier_id, [fan_tail_rel],
                    auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
                )
                if not conn:
                    return False
                branch_src_id = barrier_id
                branch_src_rel = "merged"

            out_port = await _create_output_port(
                nifi_url, child_pg,
                f"{sid}__fanout__{child_sid}",
                auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
                x=400.0, y=9200.0,
            )
            if not out_port:
                return False

            conn = await _create_connection(
                nifi_url, child_pg, branch_src_id, out_port, [branch_src_rel],
                auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
                dst_type="OUTPUT_PORT",
            )
            if not conn:
                return False
            conn = await _create_connection(
                nifi_url, pg_id, out_port, target_in_port, [],
                auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
                src_type="OUTPUT_PORT", src_group_id=child_pg,
                dst_type="INPUT_PORT", dst_group_id=target_child_pg,
            )
            if not conn:
                return False

    return True


def _topological_order(streams: List[dict]) -> List[dict]:
    """Order streams parent-before-child based on fan_out.parent_stream_id or route_source.parent_stream_id.

    Stable: independent streams keep their input order. Streams referencing
    an unknown parent fall back to root level.
    """
    by_id: Dict[str, dict] = {}
    for s in streams:
        sid = s.get("id") or s.get("name")
        if sid:
            by_id[sid] = s

    visited: set = set()
    out: List[dict] = []

    def visit(stream: dict) -> None:
        sid = stream.get("id") or stream.get("name")
        if sid in visited:
            return
        # Resolve parent: fan_out OR route_source
        fan = stream.get("fan_out") or {}
        fanout_parent = fan.get("parent_stream_id") if fan.get("enabled") else None
        route_src = stream.get("route_source") or {}
        # route_source can be a dict (from DB) or None
        if isinstance(route_src, dict):
            route_parent = route_src.get("parent_stream_id")
        else:
            route_parent = None
        parent_id = fanout_parent or route_parent
        if parent_id and parent_id in by_id and parent_id != sid and parent_id not in visited:
            visit(by_id[parent_id])
        visited.add(sid)
        out.append(stream)

    for s in streams:
        visit(s)
    return out


def _inject_body_pagination(template: str, pag: dict) -> str:
    """Inject pagination values into a JSON request body template.

    ${page} is not valid JSON, so a sentinel string is placed, the object is serialised, and the
    quoted sentinel is then swapped for the bare NiFi EL token.
    """
    ptype = _normalize_pagination_type(pag.get("type"))
    text = (template or "").strip()
    try:
        obj = json.loads(text) if text else {}
    except (ValueError, TypeError) as exc:
        raise ValueError(
            "Request-body pagination requires a valid JSON body template; could not parse it."
        ) from exc
    if not isinstance(obj, dict):
        raise ValueError("Request-body pagination requires the body template to be a JSON object.")

    subs: Dict[str, str] = {}
    if ptype == "page":
        obj[pag.get("page_param") or "page"] = "__PAG_PAGE__"
        subs['"__PAG_PAGE__"'] = "${page}"
        if pag.get("page_size_param") and pag.get("page_size"):
            obj[pag["page_size_param"]] = pag["page_size"]
    elif ptype == "offset":
        obj[pag.get("offset_param") or "offset"] = "__PAG_OFFSET__"
        subs['"__PAG_OFFSET__"'] = "${offset}"
        if pag.get("limit_param") and pag.get("page_size"):
            obj[pag["limit_param"]] = pag["page_size"]
    elif ptype == "cursor":
        obj[pag.get("cursor_param") or "cursor"] = "__PAG_CURSOR__"
        subs['"__PAG_CURSOR__"'] = '"${cursor}"'
        if pag.get("page_size_param") and pag.get("page_size"):
            obj[pag["page_size_param"]] = pag["page_size"]
    else:
        return template

    out = json.dumps(obj)
    for sentinel, replacement in subs.items():
        out = out.replace(sentinel, replacement)
    return out


def _pagination_header_expr(name: str) -> str:
    """Read a response header into an attribute, defending against header-name casing."""
    text = str(name or "").strip()
    return "${'" + text + "':replaceEmpty(${'" + text.lower() + "'})}"


def _pagination_link_next_expr(name: str) -> str:
    """Extract rel="next" from an RFC 5988 Link header.

    The find()/ifElse() guard is required: a bare replaceAll returns the input unchanged when
    there is no rel="next", which would make has_more true forever on the last page.
    """
    text = str(name or "").strip()
    return (
        "${'" + text + "':find('rel=\"next\"'):ifElse("
        "${'" + text + "':replaceAll('(?s).*<([^>]*)>;[^,]*rel=\"next\".*','$1')}, '')}"
    )


def _normalize_stop_condition(value: Optional[str]) -> str:
    """Normalize the UI stop_condition enum to an internal token."""
    text = str(value or "").strip().upper().replace("-", "_").replace(" ", "_")
    aliases = {
        "EMPTY_ARRAY": "empty",
        "EMPTY": "empty",
        "EMPTY_PAGE": "empty",
        "TOTAL_COUNT": "total_count",
        "TOTAL": "total_count",
        "MAX_PAGES": "max_pages",
        "MAX": "max_pages",
        "HAS_MORE": "has_more",
        "API_FLAG": "has_more",
    }
    return aliases.get(text, "")


def _normalize_pagination_type(t: Optional[str]) -> str:
    if not t:
        return "none"
    t = t.lower()
    if t in ("page", "page_increment", "page-increment"):
        return "page"
    if t in ("cursor",):
        return "cursor"
    if t in ("offset", "offset_limit", "offset-limit"):
        return "offset"
    if t in ("next_url", "next-url", "next"):
        return "next_url"
    return "none"


def _augment_url_with_pagination(base_api_url: str, pag: dict) -> str:
    """Append pagination query params with NiFi EL placeholders."""
    ptype = _normalize_pagination_type(pag.get("type"))
    if ptype == "none":
        return base_api_url
    if str(pag.get("location") or "query").strip().lower() == "body" and ptype != "next_url":
        # Values are injected into the request body instead; the URL must stay clean.
        return base_api_url
    sep = "&" if "?" in base_api_url else "?"
    parts: List[str] = []
    if ptype == "page":
        page_param = pag.get("page_param") or "page"
        parts.append(f"{page_param}=${{page}}")
        if pag.get("page_size_param") and pag.get("page_size"):
            parts.append(f"{pag['page_size_param']}={pag['page_size']}")
    elif ptype == "cursor":
        cursor_param = pag.get("cursor_param") or "cursor"
        parts.append(f"{cursor_param}=${{cursor}}")
        if pag.get("page_size_param") and pag.get("page_size"):
            parts.append(f"{pag['page_size_param']}={pag['page_size']}")
    elif ptype == "offset":
        offset_param = pag.get("offset_param") or "offset"
        limit_param = pag.get("limit_param") or "limit"
        parts.append(f"{offset_param}=${{offset}}")
        if pag.get("page_size"):
            parts.append(f"{limit_param}={pag['page_size']}")
    elif ptype == "next_url":
        return "${next_url:replaceEmpty('" + base_api_url.replace("'", "\\'") + "')}"
    if not parts:
        return base_api_url
    return base_api_url + sep + "&".join(parts)


def _augment_url_with_stream_query(base_api_url: str, query_params: Optional[Dict[str, Any]]) -> str:
    """Append stream-level query params while preserving NiFi EL placeholders.

    Values are intentionally not URL-encoded so expressions like `${site_id}`
    remain evaluable by NiFi at runtime.
    """
    if not isinstance(query_params, dict) or not query_params:
        return base_api_url
    parts: List[str] = []
    for raw_key, raw_value in query_params.items():
        key = str(raw_key or "").strip()
        if not key:
            continue
        if raw_value is None:
            continue
        value = str(raw_value).strip()
        if value == "":
            continue
        parts.append(f"{key}={value}")
    if not parts:
        return base_api_url
    sep = "&" if "?" in base_api_url else "?"
    return base_api_url + sep + "&".join(parts)


def _rest_failure_topic(source: dict) -> str:
    cfg = source.get("error_handling") if isinstance(source.get("error_handling"), dict) else {}
    topic = str((cfg or {}).get("failure_topic") or "").strip()
    return topic or "rest-flow-failures"


def _failure_event_json_template() -> str:
    return (
        '{"event_type":"rest_flow_failure",'
        '"flow_id":"${rest.error.flow_id}",'
        '"source_id":"${rest.error.source_id}",'
        '"stream_id":"${rest.error.stream_id}",'
        '"stream_name":"${rest.error.stream_name}",'
        '"stage":"${rest.error.stage}",'
        '"processor_name":"${rest.error.processor}",'
        '"http_status":"${rest.http.status}",'
        '"message":"${rest.error.message}",'
        '"error_category":"${rest.error.category}",'
        '"pagination_page_count":"${rest.pagination.page_count}",'
        '"request_method":"${rest.operation.method}",'
        '"request_url":"${rest.operation.url}",'
        '"timestamp":"${now()}",'
        '"attributes":{}}'
    )


async def _create_rest_failure_event_path(
    nifi_url: str,
    pg_id: str,
    source: dict,
    stream: dict,
    stream_name: str,
    stage: str,
    processor_name: str,
    category: str,
    message: str,
    kafka_svc_id: Optional[str],
    x: float,
    y: float,
    auth_type: str,
    username: Optional[str],
    password: Optional[str],
    extra_properties: Optional[Dict[str, Any]] = None,
) -> Optional[Tuple[str, str, str]]:
    stream_id = str(stream.get("id") or stream.get("name") or stream_name)
    marker_props: Dict[str, Any] = {
        "rest.error.flow_id": str(source.get("__flow_id") or ""),
        "rest.error.source_id": str(source.get("id") or source.get("_id") or ""),
        "rest.error.stream_id": stream_id,
        "rest.error.stream_name": stream_name,
        "rest.error.stage": stage,
        "rest.error.processor": processor_name,
        "rest.error.category": category,
        "rest.error.message": message,
        "rest.operation.method": str(stream.get("method") or "GET").upper(),
        "rest.operation.url": "${invokehttp.request.url}",
        "rest.http.status": "${invokehttp.status.code}",
        "rest.pagination.page_count": "${page_count}",
    }
    if extra_properties:
        marker_props.update(extra_properties)

    marker_id = await _create_processor(
        nifi_url, pg_id,
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        f"{stream_name}__mark_{stage}_failure",
        properties=marker_props,
        auto_terminate=[],
        x=x, y=y,
        auth_type=auth_type, username=username, password=password,
    )
    if not marker_id:
        return None

    builder_id = await _create_processor(
        nifi_url, pg_id,
        "org.apache.nifi.processors.standard.ReplaceText",
        f"{stream_name}__build_failure_event",
        properties={
            "Search Value": "(?s)^.*$",
            "Replacement Value": _failure_event_json_template(),
            "Replacement Strategy": "Always Replace",
            "Evaluation Mode": "Entire text",
        },
        auto_terminate=["failure"],
        x=x + 350.0, y=y,
        auth_type=auth_type, username=username, password=password,
    )
    if not builder_id:
        return None
    await _create_connection(
        nifi_url, pg_id, marker_id, builder_id, ["success"],
        auth_type=auth_type, username=username, password=password,
    )

    publish_props: Dict[str, Any] = {
        "Topic Name": _rest_failure_topic(source),
        "acks": "all",
        "compression.type": "gzip",
        "Failure Strategy": "Route to Failure",
        "Transactions Enabled": "false",
        "Publish Strategy": "USE_VALUE",
    }
    if kafka_svc_id:
        publish_props["Kafka Connection Service"] = kafka_svc_id
    publisher_id = await _create_processor(
        nifi_url, pg_id,
        "org.apache.nifi.kafka.processors.PublishKafka",
        f"{stream_name}__publish_failure_event",
        properties=publish_props,
        auto_terminate=["success", "failure"],
        x=x + 700.0, y=y,
        auth_type=auth_type, username=username, password=password,
    )
    if not publisher_id:
        return None
    await _create_connection(
        nifi_url, pg_id, builder_id, publisher_id, ["success"],
        auth_type=auth_type, username=username, password=password,
    )
    return marker_id, builder_id, publisher_id


def _has_header_key(headers: Dict[str, Any], header_name: str) -> bool:
    target = header_name.lower()
    return any(str(key or "").strip().lower() == target for key in headers)


def _body_format_for_stream(stream: dict) -> str:
    text = str(stream.get("body_format") or "empty").strip().lower()
    return text if text in {"empty", "json", "xml", "csv"} else "empty"


def _body_template_for_stream(stream: dict) -> str:
    value = stream.get("body_template")
    return str(value) if value is not None else ""


def _compile_template_for_nifi(template: str, defaults: Optional[Dict[str, Any]], *, is_child_stream: bool) -> str:
    if is_child_stream:
        return to_nifi_runtime_template(template, defaults or {})
    try:
        return substitute_runtime_values(template, defaults or {})
    except Exception:
        return str(template or "")


def _compile_mapping_for_nifi(mapping: Optional[Dict[str, Any]], defaults: Optional[Dict[str, Any]], *, is_child_stream: bool) -> Dict[str, str]:
    compiled: Dict[str, str] = {}
    if not isinstance(mapping, dict):
        return compiled
    for raw_key, raw_value in mapping.items():
        key = str(raw_key or "").strip()
        if not key or raw_value is None:
            continue
        value = str(raw_value).strip()
        if not value:
            continue
        compiled[key] = _compile_template_for_nifi(value, defaults, is_child_stream=is_child_stream)
    return compiled


def _apply_root_path_param_defaults(endpoint_path: str, path_param_defaults: Optional[Dict[str, Any]], *, is_child_stream: bool) -> str:
    """Apply configured defaults only for root streams.

    Child streams must keep `${var}` placeholders so NiFi can resolve them from
    parent FlowFile attributes at runtime.
    """
    if is_child_stream:
        return endpoint_path
    if not endpoint_path:
        return endpoint_path
    if not isinstance(path_param_defaults, dict) or not path_param_defaults:
        return endpoint_path

    resolved = endpoint_path
    for match in re.findall(r"\$\{([^}]+)\}", endpoint_path):
        key = (match or "").strip()
        if not key:
            continue
        raw_value = path_param_defaults.get(key)
        if raw_value is None:
            continue
        text = str(raw_value).strip()
        if text == "":
            continue
        resolved = resolved.replace(f"${{{key}}}", text)
    return resolved


def _apply_child_path_param_defaults(endpoint_path: str, path_param_defaults: Optional[Dict[str, Any]], *, is_child_stream: bool) -> str:
    """Apply fallback defaults for child path placeholders.

    For child streams, convert `${var}` into `${var:replaceEmpty('default')}` when
    a fallback default is configured. Parent attribute values still win; default is
    used only when the parent value is missing/empty.
    """
    if not is_child_stream:
        return endpoint_path
    if not endpoint_path:
        return endpoint_path
    if not isinstance(path_param_defaults, dict) or not path_param_defaults:
        return endpoint_path

    return to_nifi_runtime_template(endpoint_path, path_param_defaults)


def _to_record_path(field_name: str) -> str:
    """Normalize UI field names to NiFi RecordPath syntax for record processors.

    Accepted user formats:
      - "field"                -> "/field"
      - "a.b.c"                -> "/a/b/c"
      - "/a/b/c"               -> "/a/b/c"
      - "$.a.b[0].c"           -> "/a/b[0]/c"
    """
    raw = (field_name or "").strip()
    if not raw:
        return ""

    if raw.startswith("$."):
        raw = raw[2:]
    elif raw == "$":
        return "/"

    if raw.startswith("/"):
        return raw

    if "/" in raw:
        return "/" + raw.lstrip("/")

    parts = [segment.strip() for segment in raw.split(".") if segment.strip()]
    if not parts:
        return ""
    return "/" + "/".join(parts)


def _normalize_smb_extraction_path(path: str, attribute_name: str) -> str:
    """Normalize SMB extraction paths generated from clicked tabular headers.

    Converts bracket-quoted JSONPath like $["Sl. No"] to $.Sl__No when the
    sanitized header token matches the configured attribute name.
    """
    raw_path = (path or "").strip()
    attr = (attribute_name or "").strip()
    if not raw_path or not attr:
        return raw_path
    m = re.fullmatch(r'\$\["(.+)"\]', raw_path)
    if not m:
        return raw_path
    clicked = sanitize_name(m.group(1))
    if clicked != attr:
        return raw_path
    return f"$.{attr}"


def _escape_el_single_quote(value: str) -> str:
    """Escape a literal for single-quoted NiFi Expression Language arguments."""
    return (value or "").replace("\\", "\\\\").replace("'", "\\'")


def _build_default_attribute_props(extraction_rules: List[dict]) -> Dict[str, str]:
    """Build UpdateAttribute props to apply extraction-rule defaults."""
    props: Dict[str, str] = {}
    for rule in extraction_rules or []:
        if not isinstance(rule, dict):
            continue
        attr = (rule.get("attribute_name") or "").strip()
        default_raw = rule.get("default_value")
        if not attr or default_raw is None:
            continue
        default_text = str(default_raw)
        if default_text == "":
            continue
        escaped_default = _escape_el_single_quote(default_text)
        props[attr] = "${" + attr + ":replaceNull(''):replaceEmpty('" + escaped_default + "')}"
    return props


def _compile_mongo_query_template_for_nifi(value: Any) -> str:
    """Compile Mongo JSON templates to NiFi-safe EL for runtime substitution.

    UI now encourages raw placeholders (example: {"asset_id":${asset_id}}).
    At runtime, parent attributes are strings, so raw insertion would produce
    invalid JSON (`{"asset_id":asset-1001}`).

    This helper converts raw placeholders outside JSON strings to quoted,
    JSON-escaped NiFi expressions:
      ${asset_id} -> ${asset_id:escapeJson():prepend('"'):append('"')}

    Placeholders already inside JSON strings are preserved.
    """
    if value is None:
        return "{}"
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    text = str(value).strip()
    if not text:
        return "{}"

    out: List[str] = []
    i = 0
    in_string = False
    escaped = False
    while i < len(text):
        ch = text[i]
        if in_string:
            out.append(ch)
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            i += 1
            continue

        if ch == '"':
            in_string = True
            out.append(ch)
            i += 1
            continue

        if ch == "$" and i + 1 < len(text) and text[i + 1] == "{":
            end = text.find("}", i + 2)
            if end != -1:
                variable = text[i + 2:end].strip()
                if variable:
                    out.append("${" + variable + ":escapeJson():prepend('\"'):append('\"')}")
                    i = end + 1
                    continue
        out.append(ch)
        i += 1

    return "".join(out)


async def _build_stream_chain(
    nifi_url: str, pg_id: str, source: dict, stream: dict,
    *,
    entry_processor_id: str,
    entry_relationship: str,
    stream_results: Dict[str, Dict[str, Any]],
    kafka_svc_id: Optional[str],
    json_reader_id: Optional[str],
    schema_reg_id: Optional[str] = None,
    xml_reader_id: Optional[str] = None,
    avro_writer_id: Optional[str],
    kafka_schema_reg_id: Optional[str] = None,
    schema_ref_writer_id: Optional[str] = None,
    avro_writer_cache: Optional[Dict[str, str]] = None,
    json_reader_cache: Optional[Dict[str, str]] = None,
    xml_reader_cache: Optional[Dict[str, str]] = None,
    json_writer_id: Optional[str] = None,
    avro_transform_writer_id: Optional[str] = None,
    target_stream_id: Optional[str] = None,
    kafka_topic: str,
    base_url: str,
    src_auth: str,
    auth_header_pattern_keys: List[str],
    source_username: str, source_password: str,
    connection_timeout: int, read_timeout: int,
    x: float, y: float,
    auth_type: str, username: Optional[str], password: Optional[str],
    external_entry: Optional[Tuple[str, str]] = None,
    shared_pg_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Build a single stream chain. Returns dict of processor IDs / tail rel for fan-out.

    For XML streams (response_format == 'xml'):
      - Uses SplitXml (configurable depth) instead of SplitJson
      - Uses EvaluateXPath instead of EvaluateJsonPath for attribute extraction and pagination
      - Uses XMLReader instead of JsonReader for UpdateRecord

    For CSV streams (response_format == 'csv'):
      - Uses SplitRecord/ConvertRecord with CSVReader + JsonRecordSetWriter
      - Uses EvaluateJsonPath downstream on parsed JSON records
    """
    nargs = dict(auth_type=auth_type, username=username, password=password)
    sname = (stream.get("name") or "stream").replace(" ", "_").lower()
    stream_id = str(stream.get("id") or stream.get("name") or "").strip()
    pag = stream.get("pagination") or {}
    pag_type = _normalize_pagination_type(pag.get("type")) if pag.get("enabled") else "none"
    fan = stream.get("fan_out") or {}
    fan_enabled = bool(fan.get("enabled") and fan.get("parent_stream_id"))

    # Determine response format: json (default), xml, or csv
    is_xml = (stream.get("response_format") or "json").lower() == "xml"
    is_csv = (stream.get("response_format") or "json").lower() == "csv"
    # REST polling has been removed from the application. Keep this false so
    # legacy saved stream payloads cannot generate a NiFi polling loop.
    polling_enabled = False
    xml_split_depth = stream.get("xml_split_depth") or 1
    extraction_rules = stream.get("extraction_rules") or []
    # Existing saved streams may not have this flag yet. Preserve legacy behavior
    # for those records, while letting the UI explicitly disable splitting for
    # detail endpoints where one response should remain one record.
    split_setting = stream.get("split_response_records")
    split_response_records = bool(split_setting) if split_setting is not None else (is_xml or is_csv or bool(stream.get("response_data_path")))

    csv_reader_id: Optional[str] = None
    if is_csv:
        csv_reader_id, csv_reader_rev = await _create_rest_csv_reader_service(
            nifi_url,
            pg_id,
            stream,
            auth_type=auth_type,
            username=username,
            password=password,
        )
        if not csv_reader_id:
            logger.error(f"Stream {sname}: failed to create CSVReader controller service")
            return None
        enabled = await _enable_controller_service(
            nifi_url, csv_reader_id, csv_reader_rev,
            auth_type=auth_type, username=username, password=password,
        )
        if not enabled:
            logger.error(f"Stream {sname}: failed to enable CSVReader controller service")
            return None
        ready = await _wait_cs_enabled(
            nifi_url, csv_reader_id,
            auth_type=auth_type, username=username, password=password,
            timeout=90,
        )
        if not ready:
            logger.error(f"Stream {sname}: CSVReader controller service did not reach ENABLED state")
            return None

    step = 180.0
    cy = y

    # ── A. Resolve entry source: parent's tail, route entry point, or top-level set_auth ────
    entry_id = entry_processor_id
    entry_rel = entry_relationship
    is_route_child = False  # True when this stream receives records from a parent route
    route_child_request_enabled = False
    route_src = stream.get("route_source") or {}

    if external_entry is not None:
        # Caller has already resolved the entry and pre-wired any cross-PG ports.
        # Still detect is_route_child so skip_http_request logic stays correct.
        entry_id, entry_rel = external_entry
        if isinstance(route_src, dict) and route_src.get("parent_stream_id"):
            is_route_child = True
            route_child_request_enabled = bool(stream.get("request_enabled"))
    else:
        if isinstance(route_src, dict) and route_src.get("parent_stream_id"):
            # This stream is a route-child; look up the RouteOnAttribute entry point.
            # Route-children can either process routed records directly or make a
            # child HTTP request using the routed record's attributes as context.
            is_route_child = True
            route_child_request_enabled = bool(stream.get("request_enabled"))
            route_parent_id = route_src.get("parent_stream_id")
            route_id_ref = route_src.get("route_id")
            parent_result = stream_results.get(route_parent_id)
            if not parent_result:
                logger.error(f"Stream {sname}: route parent {route_parent_id} was not built before route child")
                return None
            route_entry_points_lookup = parent_result.get("route_entry_points") or []
            stream_id_self = stream.get("id") or stream.get("name")
            matched_entry = None
            for ep in route_entry_points_lookup:
                if ep.get("child_stream_id") == stream_id_self:
                    matched_entry = ep
                    break
            if not matched_entry and route_id_ref:
                for ep in route_entry_points_lookup:
                    if ep.get("route_id") == route_id_ref:
                        matched_entry = ep
                        break
            if not matched_entry:
                logger.error(f"Stream {sname}: no route entry point found from parent {route_parent_id}")
                return None
            entry_id = matched_entry["roa_id"]
            entry_rel = matched_entry["rel"]
        elif fan_enabled:
            parent = stream_results.get(fan.get("parent_stream_id"))
            if parent and parent.get("tail_id"):
                entry_id = parent["tail_id"]
                entry_rel = parent.get("tail_relationship", "success")

    # ── B-E. HTTP fetch chain (skipped only for passive route-children) ─────
    # Request-enabled route-children start from the routed FlowFile and invoke HTTP.
    skip_http_request = is_route_child and not route_child_request_enabled

    init_id: Optional[str] = None
    fetch_id: Optional[str] = None
    split_id: Optional[str] = None
    split_rel = "split"
    convert_id: Optional[str] = None
    stream_header_keys: List[str] = []

    if not skip_http_request:
        # ── B. Init pagination attributes ────────────────────────────────
        init_props: Dict[str, Any] = {}
        if pag_type == "page":
            init_props["page"] = str(pag.get("first_page_number") if pag.get("first_page_number") is not None else 0)
        elif pag_type == "cursor":
            init_props["cursor"] = ""
        elif pag_type == "offset":
            init_props["offset"] = "0"
        elif pag_type == "next_url":
            init_props["next_url"] = ""
        if pag_type != "none":
            init_props["page_count"] = "1"
        request_uses_parent_context = fan_enabled or is_route_child
        template_defaults = stream.get("path_param_defaults") or {}
        stream_headers = _compile_mapping_for_nifi(
            stream.get("headers") or {},
            template_defaults,
            is_child_stream=request_uses_parent_context,
        )
        for key, value in stream_headers.items():
            init_props[key] = value
            stream_header_keys.append(key)
        body_format = _body_format_for_stream(stream)
        if body_format != "empty" and not _has_header_key(init_props, "Content-Type"):
            init_props["Content-Type"] = BODY_CONTENT_TYPES[body_format]
            stream_header_keys.append("Content-Type")
        response_format = str(stream.get("response_format") or "json").strip().lower()
        if not _has_header_key(init_props, "Accept"):
            init_props["Accept"] = RESPONSE_ACCEPTS.get(response_format, "application/json")
            stream_header_keys.append("Accept")
        init_id = await _create_processor(
            nifi_url, pg_id,
            "org.apache.nifi.processors.attributes.UpdateAttribute",
            f"{sname}__init_page",
            properties=init_props or {f"{sname}_init": "true"},
            auto_terminate=[],
            x=x, y=cy,
            **nargs,
        )
        if not init_id:
            return None
        cy += step
        await _create_connection(nifi_url, pg_id, entry_id, init_id, [entry_rel], **nargs)

    # ── C-E. HTTP fetch, split, and pagination (skipped for passive route-children) ──
    if not skip_http_request:
      # ── C. InvokeHTTP fetch ─────────────────────────────────────────────
      endpoint_path = stream.get("endpoint_path") or "/"
      endpoint_path = _apply_root_path_param_defaults(
          endpoint_path,
          stream.get("path_param_defaults") or {},
          is_child_stream=fan_enabled or is_route_child,
      )
      endpoint_path = _apply_child_path_param_defaults(
          endpoint_path,
          stream.get("path_param_defaults") or {},
          is_child_stream=fan_enabled or is_route_child,
      )
      http_method = (stream.get("method") or "GET").upper()
      api_url = base_url + endpoint_path
      compiled_query_params = _compile_mapping_for_nifi(
          stream.get("query_params") or {},
          stream.get("path_param_defaults") or {},
          is_child_stream=fan_enabled or is_route_child,
      )
      api_url = _augment_url_with_stream_query(api_url, compiled_query_params)
      if src_auth == "API_KEY" and (source.get("api_key_location") or "").upper() == "QUERY":
          sep = "&" if "?" in api_url else "?"
          api_url += f"{sep}{source.get('api_key_name') or 'api_key'}={source.get('api_key_value') or ''}"

      api_url = _augment_url_with_pagination(api_url, pag)
      body_format = _body_format_for_stream(stream)
      body_template = _compile_template_for_nifi(
          _body_template_for_stream(stream),
          stream.get("path_param_defaults") or {},
          is_child_stream=fan_enabled or is_route_child,
      )
      if (
          pag_type != "none"
          and str(pag.get("location") or "query").strip().lower() == "body"
          and body_format == "json"
      ):
          body_template = _inject_body_pagination(body_template, pag)
      has_request_body = body_format != "empty" and body_template.strip() != ""

      http_props: Dict[str, Any] = {
          "HTTP URL": api_url,
          "HTTP Method": http_method,
          "Connection Timeout": f"{connection_timeout} secs",
          "Socket Read Timeout": f"{read_timeout} secs",
          "Response Redirects Enabled": "True",
          "HTTP/2 Disabled": "True",
      }
      if http_method in {"POST", "PUT", "PATCH"}:
          http_props["Request Body Enabled"] = "true" if has_request_body else "false"
      if has_request_body:
          http_props["Request Content-Type"] = "${Content-Type}"
      if src_auth == "BASIC":
          http_props["Request Username"] = source_username
          http_props["Request Password"] = source_password
      request_header_keys = list(dict.fromkeys([*auth_header_pattern_keys, *stream_header_keys]))
      if request_header_keys:
          http_props["Request Header Attributes Pattern"] = "|".join(re.escape(item) for item in request_header_keys)

      fetch_entry_id = init_id
      if has_request_body:
          body_id = await _create_processor(
              nifi_url, pg_id,
              "org.apache.nifi.processors.standard.ReplaceText",
              f"{sname}__build_request_body",
              properties={
                  "Search Value": "(?s)^.*$",
                  "Replacement Value": body_template,
                  "Replacement Strategy": "Always Replace",
                  "Evaluation Mode": "Entire text",
              },
              auto_terminate=["failure"],
              x=x, y=cy,
              **nargs,
          )
          if not body_id:
              return None
          cy += step
          await _create_connection(nifi_url, pg_id, init_id, body_id, ["success"], **nargs)
          fetch_entry_id = body_id

      fetch_id = await _create_processor(
          nifi_url, pg_id,
          "org.apache.nifi.processors.standard.InvokeHTTP",
          f"{sname}__fetch",
          properties=http_props,
          auto_terminate=["Original"],
          x=x, y=cy,
          **nargs,
      )
      if not fetch_id:
          return None
      cy += step
      await _create_connection(nifi_url, pg_id, fetch_entry_id, fetch_id, ["success"], **nargs)

      http_failure_path = await _create_rest_failure_event_path(
          nifi_url=nifi_url,
          pg_id=pg_id,
          source=source,
          stream=stream,
          stream_name=sname,
          stage="invoke_http",
          processor_name=f"{sname}__fetch",
          category="http",
          message="API request failed",
          kafka_svc_id=kafka_svc_id,
          x=x + 900.0,
          y=cy - step,
          auth_type=auth_type,
          username=username,
          password=password,
      )
      if not http_failure_path:
          return None
      await _create_connection(nifi_url, pg_id, fetch_id, http_failure_path[0], ["Failure", "No Retry", "Retry"], **nargs)

      response_src_id = fetch_id
      response_src_rels = ["Response"]
      if polling_enabled:
          if is_xml:
              status_props: Dict[str, Any] = {
                  "Destination": "flowfile-attribute",
                  "Return Type": "auto-detect",
                  "Allow DTD": "false",
                  "Path Not Found Behavior": "ignore",
                  "poll_status": polling.get("status_path") or "",
              }
              status_proc_type = "org.apache.nifi.processors.standard.EvaluateXPath"
          else:
              status_props = {
                  "Destination": "flowfile-attribute",
                  "Max String Length": "20 MB",
                  "Return Type": "auto-detect",
                  "Null Value Representation": "empty string",
                  "Path Not Found Behavior": "ignore",
                  "poll_status": polling.get("status_path") or "",
              }
              status_proc_type = "org.apache.nifi.processors.standard.EvaluateJsonPath"

          poll_status_id = await _create_processor(
              nifi_url, pg_id,
              status_proc_type,
              f"{sname}__poll_status",
              properties=status_props,
              auto_terminate=["failure", "unmatched"],
              x=x, y=cy,
              **nargs,
          )
          if not poll_status_id:
              return None
          cy += step
          await _create_connection(nifi_url, pg_id, fetch_id, poll_status_id, ["Response"], **nargs)

          pending_values = _polling_values(polling.get("pending_values"))
          success_values = _polling_values(polling.get("success_values"))
          failure_values = _polling_values(polling.get("failure_values"))
          route_props = {
              "Routing Strategy": "Route to Property name",
              "poll_success": _polling_match_expr("poll_status", success_values),
              "poll_pending": _polling_match_expr("poll_status", pending_values),
          }
          if failure_values:
              route_props["poll_failure"] = _polling_match_expr("poll_status", failure_values)
          if str(polling.get("missing_status_behavior") or "fail").strip().lower() == "fail":
              route_props["poll_missing"] = "${poll_status:trim():isEmpty()}"

          poll_route_id = await _create_processor(
              nifi_url, pg_id,
              "org.apache.nifi.processors.standard.RouteOnAttribute",
              f"{sname}__poll_route",
              properties=route_props,
              auto_terminate=["unmatched"],
              x=x, y=cy,
              **nargs,
          )
          if not poll_route_id:
              return None
          cy += step
          await _create_connection(nifi_url, pg_id, poll_status_id, poll_route_id, ["matched"], **nargs)

          max_attempts = int(polling.get("max_attempts") or 1)
          poll_check_id = await _create_processor(
              nifi_url, pg_id,
              "org.apache.nifi.processors.standard.RouteOnAttribute",
              f"{sname}__poll_check_attempt",
              properties={
                  "Routing Strategy": "Route to Property name",
                  "attempts_remaining": f"${{poll_attempt:toNumber():lt({max_attempts})}}",
              },
              auto_terminate=[],
              x=x, y=cy,
              **nargs,
          )
          if not poll_check_id:
              return None
          cy += step
          await _create_connection(nifi_url, pg_id, poll_route_id, poll_check_id, ["poll_pending"], **nargs)

          poll_failed_id = await _create_processor(
              nifi_url, pg_id,
              "org.apache.nifi.processors.attributes.UpdateAttribute",
              f"{sname}__poll_failed",
              properties={
                  "rest.error.flow_id": str(source.get("__flow_id") or ""),
                  "rest.error.source_id": str(source.get("id") or source.get("_id") or ""),
                  "rest.error.stream_id": str(stream.get("id") or stream.get("name") or sname),
                  "rest.error.stream_name": sname,
                  "rest.error.stage": "polling_failed",
                  "rest.error.processor": f"{sname}__poll_failed",
                  "rest.error.category": "polling",
                  "rest.error.message": "Unsupported legacy polling configuration reached flow generation",
                  "rest.operation.method": str(stream.get("method") or "GET").upper(),
                  "rest.operation.url": "${invokehttp.request.url}",
                  "rest.http.status": "${invokehttp.status.code}",
                  "rest.pagination.page_count": "${page_count}",
              },
              auto_terminate=[],
              x=x + 350.0, y=cy,
              **nargs,
          )
          if not poll_failed_id:
              return None
          poll_failure_builder_id = await _create_processor(
              nifi_url, pg_id,
              "org.apache.nifi.processors.standard.ReplaceText",
              f"{sname}__build_polling_failure_event",
              properties={
                  "Search Value": "(?s)^.*$",
                  "Replacement Value": _failure_event_json_template(),
                  "Replacement Strategy": "Always Replace",
                  "Evaluation Mode": "Entire text",
              },
              auto_terminate=["failure"],
              x=x + 700.0, y=cy,
              **nargs,
          )
          if not poll_failure_builder_id:
              return None
          await _create_connection(nifi_url, pg_id, poll_failed_id, poll_failure_builder_id, ["success"], **nargs)

          poll_failure_pub_props: Dict[str, Any] = {
              "Topic Name": _rest_failure_topic(source),
              "acks": "all",
              "compression.type": "gzip",
              "Failure Strategy": "Route to Failure",
              "Transactions Enabled": "false",
              "Publish Strategy": "USE_VALUE",
          }
          if kafka_svc_id:
              poll_failure_pub_props["Kafka Connection Service"] = kafka_svc_id
          poll_failure_pub_id = await _create_processor(
              nifi_url, pg_id,
              "org.apache.nifi.kafka.processors.PublishKafka",
              f"{sname}__publish_polling_failure_event",
              properties=poll_failure_pub_props,
              auto_terminate=["success", "failure"],
              x=x + 1050.0, y=cy,
              **nargs,
          )
          if not poll_failure_pub_id:
              return None
          await _create_connection(nifi_url, pg_id, poll_failure_builder_id, poll_failure_pub_id, ["success"], **nargs)
          for rel in ("poll_failure", "poll_missing"):
              if rel in route_props:
                  await _create_connection(nifi_url, pg_id, poll_route_id, poll_failed_id, [rel], **nargs)
          await _create_connection(nifi_url, pg_id, poll_check_id, poll_failed_id, ["unmatched"], **nargs)

          interval_seconds = int(polling.get("interval_seconds") or 1)
          poll_delay_id = await _create_processor(
              nifi_url, pg_id,
              "org.apache.nifi.processors.standard.ControlRate",
              f"{sname}__poll_delay",
              properties={
                  "Rate Control Criteria": "flowfile count",
                  "Maximum Rate": "1",
                  "Time Duration": f"{interval_seconds} sec",
              },
              auto_terminate=["failure"],
              x=x, y=cy,
              **nargs,
          )
          if not poll_delay_id:
              return None
          cy += step
          await _create_connection(nifi_url, pg_id, poll_check_id, poll_delay_id, ["attempts_remaining"], **nargs)

          poll_next_id = await _create_processor(
              nifi_url, pg_id,
              "org.apache.nifi.processors.attributes.UpdateAttribute",
              f"{sname}__poll_next_attempt",
              properties={"poll_attempt": "${poll_attempt:toNumber():plus(1)}"},
              auto_terminate=[],
              x=x, y=cy,
              **nargs,
          )
          if not poll_next_id:
              return None
          cy += step
          await _create_connection(nifi_url, pg_id, poll_delay_id, poll_next_id, ["success"], **nargs)
          await _create_connection(nifi_url, pg_id, poll_next_id, fetch_id, ["success"], **nargs)

          response_src_id = poll_route_id
          response_src_rels = ["poll_success"]

      # ── D. Split response records ───────────────────────────────────────
      data_path = stream.get("response_data_path")

    if not skip_http_request and is_csv and split_response_records:
        if not csv_reader_id:
            logger.error(f"Stream {sname}: CSV split requested but CSVReader is unavailable")
            return None
        if not json_writer_id:
            logger.error(f"Stream {sname}: CSV split requested but JsonRecordSetWriter is unavailable")
            return None
        csv_auto_term = ["failure"] if pag_type != "none" else ["original", "failure"]
        split_id = await _create_processor(
            nifi_url, pg_id,
            "org.apache.nifi.processors.standard.SplitRecord",
            f"{sname}__split",
            properties={
                "Record Reader": csv_reader_id,
                "Record Writer": json_writer_id,
                "Records Per Split": "1",
            },
            auto_terminate=csv_auto_term,
            x=x, y=cy,
            **nargs,
        )
        if not split_id:
            return None
        split_rel = "splits"
        cy += step
        await _create_connection(nifi_url, pg_id, response_src_id, split_id, response_src_rels, **nargs)
    elif not skip_http_request and is_xml and split_response_records:
        # XML streams can optionally split list responses into record FlowFiles.
        xml_auto_term = ["failure"] if pag_type != "none" else ["original", "failure"]
        split_id = await _create_processor(
            nifi_url, pg_id,
            "org.apache.nifi.processors.standard.SplitXml",
            f"{sname}__split",
            properties={"Split Depth": str(xml_split_depth)},
            auto_terminate=xml_auto_term,
            x=x, y=cy,
            **nargs,
        )
        if not split_id:
            return None
        cy += step
        await _create_connection(nifi_url, pg_id, response_src_id, split_id, response_src_rels, **nargs)
    elif not skip_http_request and not is_xml and not is_csv and split_response_records and data_path:
        # JSON list streams with an explicit record path use SplitJson.
        jsonpath = data_path
        json_auto_term = ["failure"] if pag_type != "none" else ["original", "failure"]
        split_id = await _create_processor(
            nifi_url, pg_id,
            "org.apache.nifi.processors.standard.SplitJson",
            f"{sname}__split",
            properties={"JsonPath Expression": jsonpath, "Null Value Representation": "empty string"},
            auto_terminate=json_auto_term,
            x=x, y=cy,
            **nargs,
        )
        if not split_id:
            return None
        cy += step
        await _create_connection(nifi_url, pg_id, response_src_id, split_id, response_src_rels, **nargs)
    elif not skip_http_request and is_csv and not split_response_records:
        if not csv_reader_id:
            logger.error(f"Stream {sname}: CSV conversion requested but CSVReader is unavailable")
            return None
        if not json_writer_id:
            logger.error(f"Stream {sname}: CSV conversion requested but JsonRecordSetWriter is unavailable")
            return None
        convert_id = await _create_processor(
            nifi_url, pg_id,
            "org.apache.nifi.processors.standard.ConvertRecord",
            f"{sname}__convert_csv_json",
            properties={
                "Record Reader": csv_reader_id,
                "Record Writer": json_writer_id,
            },
            auto_terminate=["failure"],
            x=x, y=cy,
            **nargs,
        )
        if not convert_id:
            return None
        cy += step
        await _create_connection(nifi_url, pg_id, response_src_id, convert_id, response_src_rels, **nargs)

    # ── E. Pagination loop: page_meta → has_more → next_page ──────────────
    if not skip_http_request and pag_type != "none":
        stop_cond = _normalize_stop_condition(pag.get("stop_condition"))
        if not stop_cond:
            stop_cond = "total_count" if pag.get("total_count_path") else "empty"
        # PageMeta — pagination metadata comes from the response BODY (JSONPath/XPath) and/or
        # from response HEADERS (already present as FlowFile attributes, verified on NiFi 2.9.0).
        body_props: Dict[str, Any] = {}
        header_props: Dict[str, Any] = {}

        if pag_type == "cursor":
            if pag.get("cursor_path"):
                body_props["next_cursor"] = pag["cursor_path"]
            elif pag.get("cursor_header"):
                header_props["next_cursor"] = _pagination_header_expr(pag["cursor_header"])
        elif pag_type == "next_url":
            if pag.get("next_url_path"):
                body_props["next_url_candidate"] = pag["next_url_path"]
            elif pag.get("next_url_header"):
                if str(pag.get("next_url_header_format") or "raw").strip().lower() == "link":
                    header_props["next_url_candidate"] = _pagination_link_next_expr(pag["next_url_header"])
                else:
                    header_props["next_url_candidate"] = _pagination_header_expr(pag["next_url_header"])

        if stop_cond == "total_count":
            if pag.get("total_count_path"):
                body_props["total_count"] = pag["total_count_path"]
            elif pag.get("total_count_header"):
                header_props["total_count"] = _pagination_header_expr(pag["total_count_header"])
        elif stop_cond == "has_more":
            if pag.get("has_more_path"):
                body_props["has_more_flag"] = pag["has_more_path"]
            elif pag.get("has_more_header"):
                header_props["has_more_flag"] = _pagination_header_expr(pag["has_more_header"])

        if is_xml:
            meta_props: Dict[str, Any] = {
                "Destination": "flowfile-attribute",
                "Return Type": "auto-detect",
                "Allow DTD": "false",
                "Path Not Found Behavior": "ignore",
            }
            page_meta_proc_type = "org.apache.nifi.processors.standard.EvaluateXPath"
        else:
            meta_props = {
                "Destination": "flowfile-attribute",
                "Max String Length": "20 MB",
                "Return Type": "auto-detect",
                "Null Value Representation": "empty string",
                "Path Not Found Behavior": "ignore",
            }
            page_meta_proc_type = "org.apache.nifi.processors.standard.EvaluateJsonPath"
        meta_props.update(body_props)

        # Emit the body extractor when there are body paths, OR when there are no header props at
        # all (the `empty` stop condition needs neither, and a property-less Evaluate* is valid and
        # is the existing measured-working behaviour).
        use_body_extractor = bool(body_props) or not header_props

        page_meta_id = None
        meta_head_id = None
        meta_tail_id = None
        meta_tail_rel = "success"

        if use_body_extractor:
            page_meta_id = await _create_processor(
                nifi_url, pg_id,
                page_meta_proc_type,
                f"{sname}__page_meta",
                properties=meta_props,
                auto_terminate=["failure", "unmatched"],
                x=x + 350.0, y=cy - step,
                **nargs,
            )
            if not page_meta_id:
                return None
            meta_head_id = page_meta_id
            meta_tail_id = page_meta_id
            meta_tail_rel = "matched"

        if header_props:
            # UpdateAttribute copies response headers (already FlowFile attributes) into the
            # pagination attributes. It emits ONLY "success" — never "matched".
            header_meta_id = await _create_processor(
                nifi_url, pg_id,
                "org.apache.nifi.processors.attributes.UpdateAttribute",
                f"{sname}__page_meta_headers",
                properties=header_props,
                auto_terminate=[],
                x=x + 350.0, y=cy,
                **nargs,
            )
            if not header_meta_id:
                return None
            if meta_tail_id:
                await _create_connection(nifi_url, pg_id, meta_tail_id, header_meta_id, [meta_tail_rel], **nargs)
            else:
                meta_head_id = header_meta_id
            meta_tail_id = header_meta_id
            meta_tail_rel = "success"
            page_meta_id = header_meta_id

        if meta_head_id:
            # Original (full response) goes into the metadata chain — split-original branch
            if split_id:
                await _create_connection(nifi_url, pg_id, split_id, meta_head_id, ["original"], **nargs)
            else:
                # No split — pagination requires a split/SplitXml upstream to work correctly
                logger.info(f"Stream {sname}: pagination requires an upstream split processor; skipping loop wiring")
                page_meta_id = None
                meta_tail_id = None

        if page_meta_id:
            # has_more route
            # max_pages is now OPTIONAL. When absent/blank/non-positive, emit NO bound term
            # (previously it silently defaulted to 1, truncating a blank-Max-Pages stream to one page).
            try:
                max_pages = int(pag.get("max_pages"))
            except (TypeError, ValueError):
                max_pages = 0
            has_max = max_pages > 0
            max_pages_expr = f"${{page_count:toNumber():lt({max_pages})}}" if has_max else ""
            page_size = int(pag.get("page_size") or 100)

            if pag_type == "cursor":
                progress_expr = "${next_cursor:trim():isEmpty():not()}"
            elif pag_type == "next_url":
                progress_expr = "${next_url_candidate:trim():isEmpty():not()}"
            elif stop_cond == "total_count":
                if pag_type == "offset":
                    total_cmp_expr = f"${{offset:toNumber():plus({page_size}):lt(${{total_count:toNumber()}})}}"
                else:
                    total_cmp_expr = f"${{page_count:toNumber():multiply({page_size}):lt(${{total_count:toNumber()}})}}"
                # If the total count never arrives (header absent, wrong name, wrong casing), do NOT
                # silently stop after one page. Keep going, bounded by max_pages.
                progress_expr = f"${{total_count:isEmpty():or({total_cmp_expr})}}"
            elif stop_cond == "has_more":
                # Stop only when the API explicitly says false. A missing flag must not truncate.
                progress_expr = "${has_more_flag:equals('false'):not()}"
            elif stop_cond == "empty":
                progress_expr = "${fragment.count:replaceEmpty('0'):gt(0)}"
            else:
                progress_expr = ""

            # Combine the progress signal and the optional max-pages bound.
            if progress_expr and max_pages_expr:
                has_more_expr = progress_expr[:-1] + f":and({max_pages_expr})}}"
            elif progress_expr:
                has_more_expr = progress_expr
            elif max_pages_expr:
                has_more_expr = max_pages_expr
            else:
                # Degenerate legacy config (stop_condition="max_pages" with a blank value, only
                # reachable by bypassing validation). Fall back to empty-page detection so the loop
                # can still terminate rather than run forever.
                has_more_expr = "${fragment.count:replaceEmpty('0'):gt(0)}"

            has_more_id = await _create_processor(
                nifi_url, pg_id,
                "org.apache.nifi.processors.standard.RouteOnAttribute",
                f"{sname}__has_more",
                properties={
                    "Routing Strategy": "Route to Property name",
                    "has_more": has_more_expr,
                },
                auto_terminate=["unmatched"],
                x=x + 700.0, y=cy - step,
                **nargs,
            )
            if has_more_id:
                await _create_connection(nifi_url, pg_id, meta_tail_id, has_more_id, [meta_tail_rel], **nargs)

                # next_page processor — increments pagination attribute, loops back to fetch
                next_props: Dict[str, Any] = {}
                if pag_type == "page":
                    next_props["page"] = "${page:toNumber():plus(1)}"
                elif pag_type == "cursor":
                    next_props["cursor"] = "${next_cursor}"
                elif pag_type == "offset":
                    page_size = pag.get("page_size") or 100
                    next_props["offset"] = f"${{offset:toNumber():plus({page_size})}}"
                elif pag_type == "next_url":
                    next_props["next_url"] = "${next_url_candidate}"
                if pag_type != "none":
                    next_props["page_count"] = "${page_count:toNumber():plus(1)}"

                next_id = await _create_processor(
                    nifi_url, pg_id,
                    "org.apache.nifi.processors.attributes.UpdateAttribute",
                    f"{sname}__next_page",
                    properties=next_props,
                    auto_terminate=[],
                    x=x + 1050.0, y=cy - step,
                    **nargs,
                )
                if next_id:
                    await _create_connection(nifi_url, pg_id, has_more_id, next_id, ["has_more"], **nargs)
                    loop_target = body_id if has_request_body else fetch_id
                    await _create_connection(nifi_url, pg_id, next_id, loop_target, ["success"], **nargs)

    # ── F. Build the post-split (per-record) pipeline ─────────────────────
    # Passive route-children skip HTTP fetch/split entirely and receive already-routed records
    # from the parent's RouteOnAttribute output (entry_id/entry_rel).
    if skip_http_request:
        record_src_id = entry_id
        record_src_rel = entry_rel
        record_src_rels = [entry_rel]
    else:
        # Source for downstream is split/convert output when present; otherwise
        # the current response source.
        if split_id:
            record_src_id = split_id
            record_src_rels = [split_rel]
        elif convert_id:
            record_src_id = convert_id
            record_src_rels = ["success"]
        else:
            record_src_id = response_src_id
            record_src_rels = response_src_rels
        record_src_rel = record_src_rels[0]
    record_content_format = "xml" if is_xml and not is_csv else "json"

    # F.1 Extract attributes (EvaluateJsonPath for JSON; EvaluateXPath for XML)
    extract_id: Optional[str] = None
    if extraction_rules:
        if is_xml:
            extract_props: Dict[str, Any] = {
                "Destination": "flowfile-attribute",
                "Return Type": "auto-detect",
                "Allow DTD": "false",
                "Path Not Found Behavior": "ignore",
            }
            for rule in extraction_rules:
                attr = rule.get("attribute_name")
                path = rule.get("path")
                if attr and path:
                    extract_props[attr] = path
            extract_proc_type = "org.apache.nifi.processors.standard.EvaluateXPath"
        else:
            extract_props = {
                "Destination": "flowfile-attribute",
                "Max String Length": "20 MB",
                "Return Type": "scalar",
                "Null Value Representation": "empty string",
                "Path Not Found Behavior": "ignore",
            }
            for rule in extraction_rules:
                attr = rule.get("attribute_name")
                path = rule.get("path")
                if attr and path:
                    extract_props[attr] = path
            extract_proc_type = "org.apache.nifi.processors.standard.EvaluateJsonPath"

        extract_id = await _create_processor(
            nifi_url, pg_id,
            extract_proc_type,
            f"{sname}__extract",
            properties=extract_props,
            auto_terminate=["failure", "unmatched"],
            x=x, y=cy,
            **nargs,
        )
        if extract_id:
            await _create_connection(nifi_url, pg_id, record_src_id, extract_id, record_src_rels, **nargs)
            cy += step
            record_src_id = extract_id
            record_src_rel = "matched"
            record_src_rels = ["matched"]

    # F.1b Apply extraction defaults on missing/empty attributes.
    default_props = _build_default_attribute_props(extraction_rules)
    if default_props:
        defaults_id = await _create_processor(
            nifi_url, pg_id,
            "org.apache.nifi.processors.attributes.UpdateAttribute",
            f"{sname}__apply_defaults",
            properties=default_props,
            auto_terminate=["failure"],
            x=x, y=cy,
            **nargs,
        )
        if defaults_id:
            await _create_connection(nifi_url, pg_id, record_src_id, defaults_id, record_src_rels, **nargs)
            cy += step
            record_src_id = defaults_id
            record_src_rel = "success"
            record_src_rels = ["success"]

    # F.2a Apply filters and legacy topic-routing rules.
    # Legacy routing_rules must stay in one ordered chain so route_to_topic,
    # include, and exclude keep first-match behavior.
    filters = stream.get("filters") or []
    legacy_routing_rules = [
        r for r in (stream.get("routing_rules") or [])
        if isinstance(r, dict) and r.get("destination") in ("include", "exclude", "route_to_topic")
    ]
    # Convert new FilterRule format to legacy format for _apply_stream_routing
    combined_filter_rules = []
    for f in filters:
        if isinstance(f, dict):
            if f.get("action") in ("include", "exclude"):
                combined_filter_rules.append({
                    "attribute": f.get("attribute", ""),
                    "condition": f.get("condition", "equals"),
                    "value": f.get("value", ""),
                    "destination": f.get("action", "include"),
                })
    combined_filter_rules.extend(legacy_routing_rules)

    pk_fields = stream.get("primary_key_fields") or []
    kafka_key_attr = None
    if pk_fields and any(r.get("attribute_name") == pk_fields[0] for r in extraction_rules):
        kafka_key_attr = pk_fields[0]

    if combined_filter_rules:
        filter_res = await _apply_stream_routing(
            nifi_url=nifi_url,
            pg_id=pg_id,
            stream_name=sname,
            routing_rules=combined_filter_rules,
            kafka_topic=kafka_topic,
            kafka_svc_id=kafka_svc_id,
            kafka_key_attr=kafka_key_attr,
            input_id=record_src_id,
            input_rels=record_src_rels,
            x=x,
            y=cy,
            step=step,
            scheduling_period="0 sec",
            auth_type=auth_type,
            username=username,
            password=password,
        )
        record_src_id = filter_res["tail_id"]
        record_src_rels = filter_res["tail_rels"]
        record_src_rel = record_src_rels[0]
        cy = filter_res["y"]

    # F.3 Apply configured record transformations
    transformation_rules = stream.get("transformation_rules") or []
    # Choose reader: XML stream uses XMLReader when available; fallback to JsonReader
    active_reader_id = xml_reader_id if (is_xml and xml_reader_id) else json_reader_id
    add_set_rules: List[dict] = []
    remove_rules: List[dict] = []
    for rule in transformation_rules:
        action = (rule.get("type") or "").lower()
        if action in ("add_field", "set_from_attribute"):
            add_set_rules.append(rule)
        elif action == "remove_field":
            remove_rules.append(rule)

    # RemoveRecordField mutates schema. For remove flows, use a dedicated Avro
    # writer that embeds schema in-content instead of schema-reference writing.
    # This avoids registry-id lookup failures for transformed schemas.
    use_transform_writer = bool(remove_rules and avro_transform_writer_id)
    transform_writer_id = avro_transform_writer_id if use_transform_writer else avro_writer_id

    # F.3a add/set transformations via UpdateRecord
    if active_reader_id and transform_writer_id and add_set_rules:
        ur_props: Dict[str, Any] = {
            "Replacement Value Strategy": "literal-value",
            "Record Reader": active_reader_id,
            "Record Writer": transform_writer_id,
        }
        for r in add_set_rules:
            t = (r.get("type") or "").lower()
            field = r.get("field_name")
            if not field:
                continue
            path = _to_record_path(field)
            if not path:
                continue
            if t == "add_field":
                ur_props[path] = r.get("value") or ""
            elif t == "set_from_attribute":
                src_attr = r.get("source_attribute")
                if src_attr:
                    ur_props[path] = "${" + src_attr + "}"

        enriched_id = await _create_processor(
            nifi_url, pg_id,
            "org.apache.nifi.processors.standard.UpdateRecord",
            f"{sname}__set_metadata",
            properties=ur_props,
            auto_terminate=["failure"],
            x=x, y=cy,
            **nargs,
        )
        if not enriched_id:
            logger.error(f"Stream {sname}: failed to create UpdateRecord for add/set transformations")
            return None
        await _create_connection(nifi_url, pg_id, record_src_id, enriched_id, record_src_rels, **nargs)
        cy += step
        record_src_id = enriched_id
        record_src_rel = "success"
        record_src_rels = ["success"]
        record_content_format = "avro"

    # F.3b remove transformations via RemoveRecordField (true schema/content field deletion)
    if remove_rules:
        if not (active_reader_id and transform_writer_id):
            logger.error(f"Stream {sname}: remove_field transformation requires active record reader/writer")
            return None

        remove_props: Dict[str, Any] = {
            "Record Reader": active_reader_id,
            "Record Writer": transform_writer_id,
        }
        remove_index = 1
        for rule in remove_rules:
            field = (rule.get("field_name") or "").strip()
            if not field:
                continue
            record_path = _to_record_path(field)
            if not record_path:
                continue
            # RemoveRecordField uses dynamic properties; property name is ignored by processor.
            remove_props[f"field_to_remove_{remove_index}"] = record_path
            remove_index += 1

        if remove_index == 1:
            logger.warning(f"Stream {sname}: remove_field rules present but no valid field_name values")
        else:
            remove_id = await _create_processor(
                nifi_url, pg_id,
                "org.apache.nifi.processors.standard.RemoveRecordField",
                f"{sname}__remove_fields",
                properties=remove_props,
                auto_terminate=["failure"],
                x=x, y=cy,
                **nargs,
            )
            if not remove_id:
                logger.error(f"Stream {sname}: failed to create RemoveRecordField for remove_field transformations")
                return None
            await _create_connection(nifi_url, pg_id, record_src_id, remove_id, record_src_rels, **nargs)
            cy += step
            record_src_id = remove_id
            record_src_rel = "success"
            record_src_rels = ["success"]
            record_content_format = "avro"

    # F.4 New-style routes after the stream's own processing.
    # Routing is first-match-wins: each RouteOnAttribute passes only unmatched
    # records to the next route, so branches diverge and never merge.
    routes = sorted(
        [r for r in (stream.get("routes") or []) if isinstance(r, dict)],
        key=lambda r: int(r.get("ordinal") or 0),
    )
    route_entry_points: List[Dict[str, Any]] = []  # [{roa_id, rel, child_stream_id, route_id}]
    nargs_local = dict(auth_type=auth_type, username=username, password=password)
    route_chain_id = record_src_id
    route_chain_rels = list(record_src_rels)
    default_route_action = str(stream.get("default_route_action") or "drop").strip().lower()
    default_route_child_id = str(stream.get("default_route_child_stream_id") or "").strip()

    for route_idx, route_def in enumerate(routes):
        cond = route_def.get("condition") or {}
        if isinstance(cond, dict):
            attr = (cond.get("attribute") or "").strip()
            condition_type = (cond.get("condition") or "equals").strip().lower()
            value_str = str(cond.get("value") or "")
        else:
            continue
        child_id = (route_def.get("child_stream_id") or "").strip()
        route_id_label = str(route_def.get("ordinal", route_idx) or route_idx)
        if not attr or not child_id:
            continue

        route_rel = f"route_{route_id_label}"
        route_props = {
            "Routing Strategy": "Route to Property name",
            route_rel: _build_route_expr(attr, condition_type, value_str),
        }
        is_last_route = route_idx == len(routes) - 1
        unmatched_is_terminal = is_last_route and default_route_action not in ("route_to_stream", "keep")

        roa_id = await _create_processor(
            nifi_url, pg_id,
            "org.apache.nifi.processors.standard.RouteOnAttribute",
            f"{sname}__route_{route_id_label}",
            properties=route_props,
            auto_terminate=["unmatched"] if unmatched_is_terminal else [],
            scheduling_period="0 sec",
            x=x, y=cy,
            **nargs_local,
        )
        if not roa_id:
            logger.warning(f"Stream {sname}: failed to create RouteOnAttribute for route {route_id_label}")
            continue

        await _create_connection(nifi_url, pg_id, route_chain_id, roa_id, route_chain_rels, **nargs_local)
        cy += step
        route_entry_points.append({
            "roa_id": roa_id,
            "rel": route_rel,
            "child_stream_id": child_id,
            "route_id": route_def.get("id") or "",
            "ordinal": route_idx,
        })
        route_chain_id = roa_id
        route_chain_rels = ["unmatched"]

    if routes and default_route_action == "route_to_stream" and default_route_child_id:
        route_entry_points.append({
            "roa_id": route_chain_id,
            "rel": "unmatched",
            "child_stream_id": default_route_child_id,
            "route_id": "default",
        })

    has_fanout_children = any(
        (s.get("fan_out") or {}).get("enabled") and (s.get("fan_out") or {}).get("parent_stream_id") == stream_id
        for s in (source.get("streams") or [])
    )
    has_route_source_children = any(
        isinstance(s.get("route_source"), dict) and (s.get("route_source") or {}).get("parent_stream_id") == stream_id
        for s in (source.get("streams") or [])
    )
    is_parent = has_fanout_children or has_route_source_children or bool(route_entry_points)

    entity_cfg = stream.get("entity") or {}
    behavior = infer_rest_stream_behaviors(source, target_stream_id=target_stream_id).get(stream_id)
    should_publish = bool(behavior and behavior.publishes_to_kafka)
    publish_id: Optional[str] = None
    fanout_tail_id = route_chain_id if default_route_action == "keep" and routes else record_src_id
    fanout_tail_relationship = "unmatched" if default_route_action == "keep" and routes else record_src_rel

    if should_publish:
        effective_kafka_topic = kafka_topic
        entity_kafka: Dict[str, Any] = {}
        if not target_stream_id and isinstance(entity_cfg, dict):
            ek = entity_cfg.get("kafka") or {}
            if isinstance(ek, dict):
                entity_kafka = ek
                if ek.get("topic"):
                    effective_kafka_topic = ek["topic"]

        source_kafka_output = source.get("kafka_output") if isinstance(source.get("kafka_output"), dict) else {}
        fallback_schema = str((source_kafka_output or {}).get("schema_artifact_id") or "").strip()
        fallback_schema_version = (source_kafka_output or {}).get("schema_version")
        stream_schema_name = ""
        stream_schema_version: Optional[Any] = fallback_schema_version
        if isinstance(entity_cfg, dict):
            stream_schema_name = str(
                entity_cfg.get("schema_artifact_id")
                or entity_kafka.get("schema_artifact_id")
                or fallback_schema
                or ""
            ).strip()
            stream_schema_version = (
                entity_cfg.get("schema_version")
                or entity_kafka.get("schema_version")
                or fallback_schema_version
            )
        else:
            stream_schema_name = fallback_schema
        value_format = str(entity_kafka.get("value_format") or ("avro" if stream_schema_name else "")).strip().lower()
        publish_as_avro = not target_stream_id and value_format == "avro" and bool(stream_schema_name)

        if publish_as_avro:
            writer_id = avro_writer_id
            if stream_schema_name:
                cache = avro_writer_cache if avro_writer_cache is not None else {}
                schema_cache_key = (
                    stream_schema_name,
                    str(stream_schema_version) if stream_schema_version not in (None, "") else "",
                )
                writer_id = cache.get(schema_cache_key)
                if not writer_id:
                    writer_id = await _create_schema_avro_writer(
                        nifi_url=nifi_url,
                        pg_id=shared_pg_id or pg_id,
                        name=f"{sname}__AvroWriter",
                        schema_name=stream_schema_name,
                        schema_version=stream_schema_version,
                        schema_reg_id=kafka_schema_reg_id,
                        schema_ref_writer_id=schema_ref_writer_id,
                        auth_type=auth_type,
                        username=username,
                        password=password,
                    )
                    if writer_id:
                        cache[schema_cache_key] = writer_id
            if not writer_id:
                logger.error(f"Stream {sname}: Avro Kafka output requires an AvroRecordSetWriter for schema {stream_schema_name}")
                return None
            if record_content_format != "avro":
                active_publish_reader_id = xml_reader_id if record_content_format == "xml" else json_reader_id
                if record_content_format == "xml" and stream_schema_name:
                    reader_cache = xml_reader_cache if xml_reader_cache is not None else {}
                    active_publish_reader_id = reader_cache.get(stream_schema_name)
                    if not active_publish_reader_id:
                        active_publish_reader_id, active_publish_reader_rev = await _create_xml_reader_service(
                            nifi_url=nifi_url,
                            pg_id=shared_pg_id or pg_id,
                            schema_reg_id=schema_reg_id,
                            schema_name=stream_schema_name,
                            auth_type=auth_type,
                            username=username,
                            password=password,
                        )
                        if active_publish_reader_id:
                            ok_cs = await _enable_controller_service(
                                nifi_url,
                                active_publish_reader_id,
                                active_publish_reader_rev,
                                auth_type=auth_type,
                                username=username,
                                password=password,
                            )
                            xml_reader_enabled = False
                            if ok_cs:
                                xml_reader_enabled = await _wait_cs_enabled(
                                    nifi_url,
                                    active_publish_reader_id,
                                    auth_type=auth_type,
                                    username=username,
                                    password=password,
                                    timeout=90,
                                )
                            else:
                                logger.warning(
                                    f"Stream {sname}: could not initiate enable for XMLReader "
                                    f"{active_publish_reader_id}"
                                )
                            if not xml_reader_enabled:
                                logger.error(
                                    f"Stream {sname}: XML Avro publishing requires an enabled XMLReader "
                                    f"for schema {stream_schema_name}"
                                )
                                return None
                            reader_cache[stream_schema_name] = active_publish_reader_id
                    if not active_publish_reader_id:
                        logger.error(
                            f"Stream {sname}: XML Avro publishing requires an XMLReader for schema {stream_schema_name}"
                        )
                        return None
                elif stream_schema_name:
                    reader_cache = json_reader_cache if json_reader_cache is not None else {}
                    schema_cache_key = (
                        stream_schema_name,
                        str(stream_schema_version) if stream_schema_version not in (None, "") else "",
                    )
                    active_publish_reader_id = reader_cache.get(schema_cache_key)
                    if not active_publish_reader_id:
                        active_publish_reader_id = await _create_schema_json_reader(
                            nifi_url=nifi_url,
                            pg_id=shared_pg_id or pg_id,
                            name=f"{sname}__JsonReader",
                            schema_name=stream_schema_name,
                            schema_version=stream_schema_version,
                            schema_reg_id=schema_reg_id,
                            auth_type=auth_type,
                            username=username,
                            password=password,
                        )
                        if active_publish_reader_id:
                            reader_cache[schema_cache_key] = active_publish_reader_id
                if not active_publish_reader_id:
                    logger.error(f"Stream {sname}: Avro Kafka output requires a record reader before PublishKafka")
                    return None
                convert_avro_id = await _create_processor(
                    nifi_url, pg_id,
                    "org.apache.nifi.processors.standard.ConvertRecord",
                    f"{sname}__convert_avro",
                    properties={
                        "Record Reader": active_publish_reader_id,
                        "Record Writer": writer_id,
                    },
                    auto_terminate=["failure"],
                    x=x, y=cy,
                    **nargs,
                )
                if not convert_avro_id:
                    return None
                await _create_connection(nifi_url, pg_id, record_src_id, convert_avro_id, record_src_rels, **nargs)
                cy += step
                record_src_id = convert_avro_id
                record_src_rel = "success"
                record_src_rels = ["success"]
                record_content_format = "avro"

        pub_props: Dict[str, Any] = {
            "Topic Name": effective_kafka_topic,
            "acks": "all",
            "compression.type": "gzip",
            "Failure Strategy": "Route to Failure",
            "Transactions Enabled": "false",
            "Publish Strategy": "USE_VALUE",
        }
        if kafka_svc_id:
            pub_props["Kafka Connection Service"] = kafka_svc_id
        if kafka_key_attr:
            pub_props["Kafka Key"] = "${" + kafka_key_attr + "}"

        pub_id = await _create_processor(
            nifi_url, pg_id,
            "org.apache.nifi.kafka.processors.PublishKafka",
            f"{sname}__publish",
            properties=pub_props,
            auto_terminate=["success", "failure"],
            x=x, y=cy,
            **nargs,
        )
        if not pub_id:
            return None
        await _create_connection(nifi_url, pg_id, record_src_id, pub_id, record_src_rels, **nargs)
        publish_id = pub_id
        cy += step

    if is_parent:
        return {
            "tail_id": fanout_tail_id,
            "tail_relationship": fanout_tail_relationship,
            "is_parent": True,
            "route_entry_points": route_entry_points,
            "publish_id": publish_id,
        }

    if target_stream_id and stream_id != target_stream_id:
        drop_id = await _create_processor(
            nifi_url, pg_id,
            "org.apache.nifi.processors.attributes.UpdateAttribute",
            f"{sname}__skip_inference_target",
            properties={"inference_skip": "non_target_entity"},
            auto_terminate=["success"],
            x=x, y=cy,
            **nargs,
        )
        if not drop_id:
            return None
        await _create_connection(nifi_url, pg_id, record_src_id, drop_id, record_src_rels, **nargs)
        return {
            "tail_id": drop_id,
            "tail_relationship": "success",
            "is_parent": False,
        }

    if not publish_id and record_src_id:
        complete_id = await _create_processor(
            nifi_url, pg_id,
            "org.apache.nifi.processors.attributes.UpdateAttribute",
            f"{sname}__complete",
            properties={"rest_stream_complete": "true"},
            auto_terminate=["success"],
            x=x, y=cy,
            **nargs,
        )
        if not complete_id:
            return None
        await _create_connection(nifi_url, pg_id, record_src_id, complete_id, record_src_rels, **nargs)
        return {
            "tail_id": complete_id,
            "tail_relationship": "success",
            "is_parent": False,
        }

    return {
        "tail_id": publish_id or record_src_id,
        "tail_relationship": "success" if publish_id else record_src_rel,
        "is_parent": False,
    }


def _build_route_expr(attr: str, condition: str, value: str) -> str:
    """Build a NiFi Expression Language expression for RouteOnAttribute."""
    safe_attr = (attr or "value").strip() or "value"
    safe_value = _escape_el_single_quote(str(value or ""))
    cond = condition.lower()
    if cond == "equals":
        return "${" + safe_attr + ":equals('" + safe_value + "')}"
    if cond == "contains":
        return "${" + safe_attr + ":contains('" + safe_value + "')}"
    if cond == "starts_with":
        return "${" + safe_attr + ":startsWith('" + safe_value + "')}"
    if cond == "ends_with":
        return "${" + safe_attr + ":endsWith('" + safe_value + "')}"
    if cond == "regex":
        return "${" + safe_attr + ":matches('" + safe_value + "')}"
    if cond == "is_empty":
        return "${" + safe_attr + ":isEmpty()}"
    return "${" + safe_attr + ":equals('" + safe_value + "')}"


def _normalize_routing_rule(rule: dict) -> Optional[Dict[str, str]]:
    if not isinstance(rule, dict):
        return None
    attr = (rule.get("attribute") or "").strip()
    cond = (rule.get("condition") or "equals").strip().lower()
    dest = (rule.get("destination") or "include").strip().lower()
    value = str(rule.get("value") or "")
    if not attr:
        return None
    if cond != "is_empty" and value == "":
        return None

    # Backward-compatible topic resolution:
    # - preferred: topic_name (absolute topic)
    # - legacy: topic_suffix (used as suffix under main topic)
    topic_name = (rule.get("topic_name") or "").strip()
    topic_suffix = (rule.get("topic_suffix") or "").strip()
    return {
        "attribute": attr,
        "condition": cond,
        "value": value,
        "destination": dest,
        "topic_name": topic_name,
        "topic_suffix": topic_suffix,
    }


async def _apply_stream_routing(
    *,
    nifi_url: str,
    pg_id: str,
    stream_name: str,
    routing_rules: List[dict],
    kafka_topic: str,
    kafka_svc_id: Optional[str],
    kafka_key_attr: Optional[str],
    input_id: str,
    input_rels: List[str],
    x: float,
    y: float,
    step: float,
    scheduling_period: str,
    auth_type: str,
    username: Optional[str],
    password: Optional[str],
) -> Dict[str, Any]:
    """Apply include/exclude/topic routing and return main-path tail.

    First-match-wins behavior is implemented by chaining RouteOnAttribute
    processors on the unmatched relationship.
    """
    normalized: List[Dict[str, str]] = []
    for r in (routing_rules or []):
        nr = _normalize_routing_rule(r)
        if nr:
            normalized.append(nr)

    if not normalized:
        return {"tail_id": input_id, "tail_rels": list(input_rels), "y": y}

    nargs = dict(auth_type=auth_type, username=username, password=password)
    cur_id = input_id
    cur_rels = list(input_rels)
    cy = y
    include_mode = any(r["destination"] == "include" for r in normalized)
    include_branches: List[Tuple[str, List[str]]] = []

    for idx, rule in enumerate(normalized, start=1):
        route_rel = f"route_{idx}"
        route_props: Dict[str, Any] = {
            "Routing Strategy": "Route to Property name",
            route_rel: _build_route_expr(rule["attribute"], rule["condition"], rule["value"]),
        }

        auto_terms: List[str] = []
        dest = rule["destination"]
        if dest in ("exclude", "drop", "blocked"):
            auto_terms.append(route_rel)

        route_id = await _create_processor(
            nifi_url, pg_id,
            "org.apache.nifi.processors.standard.RouteOnAttribute",
            f"{stream_name}__route_{idx}",
            properties=route_props,
            auto_terminate=auto_terms,
            scheduling_period=scheduling_period,
            x=x, y=cy,
            **nargs,
        )
        if not route_id:
            return {"tail_id": cur_id, "tail_rels": cur_rels, "y": cy}
        await _create_connection(nifi_url, pg_id, cur_id, route_id, cur_rels, **nargs)
        cy += step

        if dest == "include":
            include_branches.append((route_id, [route_rel]))
        elif dest in ("separate_topic", "topic", "route_to_topic"):
            raw_topic_name = (rule.get("topic_name") or "").strip()
            if raw_topic_name:
                topic_for_route = raw_topic_name
            else:
                legacy_suffix = (rule.get("topic_suffix") or "").strip()
                topic_for_route = f"{kafka_topic}.{legacy_suffix}" if legacy_suffix else ""
            if topic_for_route:
                route_pub_props: Dict[str, Any] = {
                    "Topic Name": topic_for_route,
                    "acks": "all",
                    "compression.type": "gzip",
                    "Failure Strategy": "Route to Failure",
                    "Transactions Enabled": "false",
                    "Publish Strategy": "USE_VALUE",
                }
                if kafka_svc_id:
                    route_pub_props["Kafka Connection Service"] = kafka_svc_id
                if kafka_key_attr:
                    route_pub_props["Kafka Key"] = "${" + kafka_key_attr + "}"
                route_pub_id = await _create_processor(
                    nifi_url, pg_id,
                    "org.apache.nifi.kafka.processors.PublishKafka",
                    f"{stream_name}__route_topic_{idx}",
                    properties=route_pub_props,
                    auto_terminate=["success", "failure"],
                    scheduling_period=scheduling_period,
                    x=x + 360.0, y=cy - step,
                    **nargs,
                )
                if route_pub_id:
                    await _create_connection(
                        nifi_url, pg_id, route_id, route_pub_id, [route_rel], **nargs
                    )

        # First-match-wins: keep routing only unmatched records through the chain.
        cur_id = route_id
        cur_rels = ["unmatched"]

    if include_mode:
        # Whitelist behavior: non-matching records are dropped.
        drop_id = await _create_processor(
            nifi_url, pg_id,
            "org.apache.nifi.processors.attributes.UpdateAttribute",
            f"{stream_name}__route_drop_unmatched",
            properties={"route_drop": "include_non_match"},
            auto_terminate=["success"],
            scheduling_period=scheduling_period,
            x=x, y=cy,
            **nargs,
        )
        if drop_id:
            await _create_connection(nifi_url, pg_id, cur_id, drop_id, cur_rels, **nargs)
            cy += step

        if not include_branches:
            return {"tail_id": cur_id, "tail_rels": cur_rels, "y": cy}
        if len(include_branches) == 1:
            return {
                "tail_id": include_branches[0][0],
                "tail_rels": include_branches[0][1],
                "y": cy,
            }

        merge_id = await _create_processor(
            nifi_url, pg_id,
            "org.apache.nifi.processors.attributes.UpdateAttribute",
            f"{stream_name}__route_merge",
            properties={"route_merge": "true"},
            auto_terminate=["failure"],
            scheduling_period=scheduling_period,
            x=x, y=cy,
            **nargs,
        )
        if not merge_id:
            return {"tail_id": include_branches[0][0], "tail_rels": include_branches[0][1], "y": cy}
        for branch_id, branch_rels in include_branches:
            await _create_connection(
                nifi_url, pg_id, branch_id, merge_id, branch_rels, **nargs
            )
        cy += step
        return {"tail_id": merge_id, "tail_rels": ["success"], "y": cy}

    return {"tail_id": cur_id, "tail_rels": cur_rels, "y": cy}


def _derive_smb_target(source: dict) -> Tuple[str, str, str]:
    """Resolve SMB host/share/input-dir from flexible source inputs.

    Supports:
      - smb_hostname as plain host or smb://host/share/path
      - smb_share explicit field
      - smb_base_directory as /share/path (share inferred when missing)
    Returns: (hostname, share, input_directory)
    """
    raw_host = (source.get("smb_hostname") or "").strip()
    raw_share = (source.get("smb_share") or "").strip()
    raw_base = (source.get("smb_base_directory") or "").strip()

    host = raw_host
    share = raw_share

    if host.lower().startswith("smb://"):
        host = host[6:]

    # If host contains /share/path, split that out.
    host_path = ""
    if "/" in host:
        host, host_path = host.split("/", 1)

    host_segs = [seg for seg in host_path.replace("\\", "/").split("/") if seg]
    if host_segs and not share:
        share = host_segs[0]

    base_segs = [seg for seg in raw_base.replace("\\", "/").split("/") if seg]
    if not share and base_segs:
        share = base_segs.pop(0)
    elif share and base_segs and base_segs[0].lower() == share.lower():
        # Avoid duplicated share in input directory when UI sends /<share>/...
        base_segs = base_segs[1:]

    # If no explicit base path but host had /share/path, use the host path tail.
    if not base_segs and host_segs and len(host_segs) > 1:
        base_segs = host_segs[1:]

    input_directory = "/" + "/".join(base_segs) if base_segs else "/"
    return host.strip(), share.strip(), input_directory


async def _build_smb_flow(
    nifi_url: str, pg_id: str, source: dict,
    kafka_svc_id: Optional[str],
    json_reader_id: Optional[str],
    json_writer_id: Optional[str],
    avro_writer_id: Optional[str],
    schema_reg_id: Optional[str],
    schema_name: Optional[str],
    kafka_topic: str,
    schedule_str: str,
    auth_type_nifi: str, username_nifi: Optional[str], password_nifi: Optional[str],
) -> bool:
    """Build SMB processor chain.

    For XLSX files (smb_file_format in {XLS, XLSX, EXCEL}):
      ListSmb → FetchSmb → ConvertRecord(ExcelReader→CSVWriter)
        → SplitRecord(CSVReader→CSVWriter, 1 rec/split)
        → UpdateRecord(CSVReader→AvroWriter, apply user transformations + optional org injection)
        → PublishKafka

    For CSV:
      ListSmb → FetchSmb → SplitRecord(CSVReader→CSVWriter, 1 rec/split)
        → UpdateRecord(CSVReader→AvroWriter, apply user transformations + optional org injection)
        → PublishKafka

    For JSONL:
      ListSmb → FetchSmb → SplitText(line-by-line)
        → UpdateRecord(JsonReader→AvroWriter, apply transformations + metadata)
        → PublishKafka

    For JSON:
      ListSmb → FetchSmb
        → UpdateRecord(JsonReader→AvroWriter, apply transformations + metadata)
        → PublishKafka

    For BINARY:
      ListSmb → FetchSmb → PublishKafka  (raw bytes)
    """
    x = 400.0
    y_base = 200.0
    step = 200.0
    nargs = dict(auth_type=auth_type_nifi, username=username_nifi, password=password_nifi)

    # ── SMB Client Provider controller service ────────────────────────────
    smb_host, smb_share, smb_input_dir = _derive_smb_target(source)
    if not smb_host:
        logger.error("SMB source is missing host (smb_hostname)")
        return False
    if not smb_share:
        logger.error("SMB source is missing share (smb_share or smb_base_directory must include share)")
        return False

    smb_props: Dict[str, Any] = {
        "Hostname": smb_host,
        "Share": smb_share,
        "Username": source.get("smb_username") or "",
        "Password": source.get("smb_password") or "",
    }
    if source.get("smb_domain"):
        smb_props["Domain"] = source["smb_domain"]

    smb_svc_id, smb_svc_rev = await _create_controller_service(
        nifi_url, pg_id,
        "org.apache.nifi.services.smb.SmbjClientProviderService",
        "SmbClientProvider",
        properties=smb_props,
        **nargs,
    )
    if smb_svc_id:
        ok = await _enable_controller_service(nifi_url, smb_svc_id, smb_svc_rev, **nargs)
        if ok:
            await _wait_cs_enabled(nifi_url, smb_svc_id, **nargs)

    smb_format = (source.get("smb_file_format") or "").upper()
    is_xlsx = smb_format in {"XLS", "XLSX", "EXCEL"}
    is_binary = smb_format == "BINARY"
    is_csv = smb_format == "CSV"
    is_jsonl = smb_format == "JSONL"
    inject_org = bool(source.get("smb_inject_org_from_filename"))
    org_delimiter = source.get("smb_org_filename_delimiter") or "__"
    primary_stream = _primary_stream(source) or {}
    smb_stream_name = (primary_stream.get("name") or "stream").replace(" ", "_").lower()
    # Guard: normalize auto-generated SMB extraction paths to schema-safe tokens.
    # This keeps clicked UI paths like $["Sl. No"] aligned with schema fields
    # such as $.Sl__No without touching unrelated/manual path expressions.
    extraction_rules = primary_stream.get("extraction_rules") or []
    for rule in extraction_rules:
        if not isinstance(rule, dict):
            continue
        rule["path"] = _normalize_smb_extraction_path(
            rule.get("path") or "",
            rule.get("attribute_name") or "",
        )
    smb_default_props = _build_default_attribute_props(extraction_rules)
    smb_transform_rules = primary_stream.get("transformation_rules") or []
    smb_routing_rules = primary_stream.get("routing_rules") or []
    pk_fields = primary_stream.get("primary_key_fields") or []
    pk_attr = (pk_fields[0] or "").strip() if pk_fields else ""
    add_set_rules: List[dict] = []
    remove_paths: List[str] = []
    for rule in smb_transform_rules:
        action = (rule.get("type") or "").lower()
        if action in ("add_field", "set_from_attribute"):
            add_set_rules.append(rule)
        elif action == "remove_field":
            path = _to_record_path((rule.get("field_name") or "").strip())
            if path and path not in remove_paths:
                remove_paths.append(path)

    def _build_smb_update_record_props(
        *,
        record_reader_id: Optional[str],
        record_writer_id: Optional[str],
    ) -> Dict[str, Any]:
        props: Dict[str, Any] = {
            "Replacement Value Strategy": "literal-value",
        }
        if record_reader_id:
            props["Record Reader"] = record_reader_id
        if record_writer_id:
            props["Record Writer"] = record_writer_id
        if inject_org:
            props["/organization_name"] = "${filename:substringBefore('" + org_delimiter + "')}"

        for rule in add_set_rules:
            action = (rule.get("type") or "").lower()
            field_name = (rule.get("field_name") or "").strip()
            if not field_name:
                continue
            record_path = _to_record_path(field_name)
            if not record_path:
                continue
            if action == "add_field":
                props[record_path] = rule.get("value") or ""
            elif action == "set_from_attribute":
                source_attr = (rule.get("source_attribute") or "").strip()
                if source_attr:
                    props[record_path] = "${" + source_attr + "}"
        return props

    def _has_update_record_paths(props: Dict[str, Any]) -> bool:
        return any(isinstance(k, str) and k.startswith("/") for k in props.keys())

    # ── Create XLSX-specific controller services ──────────────────────────
    excel_reader_id: Optional[str] = None
    csv_writer_id: Optional[str] = None
    csv_reader_id: Optional[str] = None
    if is_xlsx:
        xlsx_cs = await _create_smb_xlsx_controller_services(
            nifi_url, pg_id,
            schema_reg_id=schema_reg_id,
            schema_name=schema_name,
            **nargs,
        )
        excel_reader_id = xlsx_cs.get("excel_reader")
        csv_writer_id = xlsx_cs.get("csv_writer")
        csv_reader_id = xlsx_cs.get("csv_reader")
    elif is_csv:
        # CSV-specific reader/writer services (native CSV files, non-XLSX)
        csv_delimiter = source.get("smb_csv_delimiter") or ","
        has_header = source.get("smb_has_header_row")
        treat_header = "true" if has_header is not False else "false"
        csv_reader_props: Dict[str, Any] = {
            "Treat First Line as Header": treat_header,
            "Trim Double Quote": "true",
            "CSV Format": "RFC 4180",
            "Value Separator": csv_delimiter,
            "CSV Parser": "commons-csv",
        }
        if schema_reg_id and schema_name:
            # Schema-aligned mapping (position-based) keeps runtime field names
            # consistent with the linked schema artifact expected by AvroWriter.
            csv_reader_props["Schema Access Strategy"] = "schema-name"
            csv_reader_props["Schema Registry"] = schema_reg_id
            csv_reader_props["Schema Name"] = schema_name
            csv_reader_props["Ignore CSV Header Column Names"] = "true"
        else:
            # No linked schema available; infer from CSV headers at runtime.
            csv_reader_props["Schema Access Strategy"] = "csv-header-derived"
            csv_reader_props["Ignore CSV Header Column Names"] = "false"
        csv_writer_props: Dict[str, Any] = {
            "Schema Access Strategy": "inherit-record-schema",
            "Include Header Line": "false",
            "Value Separator": csv_delimiter,
        }
        csv_writer_id, csv_writer_rev = await _create_controller_service(
            nifi_url, pg_id,
            "org.apache.nifi.csv.CSVRecordSetWriter",
            "CSVWriter",
            properties=csv_writer_props,
            **nargs,
        )
        csv_reader_id, csv_reader_rev = await _create_controller_service(
            nifi_url, pg_id,
            "org.apache.nifi.csv.CSVReader",
            "CSVReader",
            properties=csv_reader_props,
            **nargs,
        )
        for cs_id, cs_rev, label in (
            (csv_writer_id, csv_writer_rev, "CSVWriter"),
            (csv_reader_id, csv_reader_rev, "CSVReader"),
        ):
            if not cs_id:
                logger.warning(f"Failed to create SMB {label} controller service")
                continue
            ok = await _enable_controller_service(nifi_url, cs_id, cs_rev, **nargs)
            if not ok:
                logger.warning(f"Could not initiate enable for SMB {label} ({cs_id})")
                continue
            enabled = await _wait_cs_enabled(nifi_url, cs_id, timeout_sec=30, **nargs)
            if not enabled:
                logger.error(f"SMB {label} did not reach ENABLED (id={cs_id})")
                return False

    # ── 1. ListSmb ────────────────────────────────────────────────────────
    list_props: Dict[str, Any] = {
        "Input Directory": smb_input_dir,
        "Listing Strategy": "none",
        "Initial Listing Strategy": "ALL_FILES",
        "Minimum File Age": "5 secs",
    }
    if smb_svc_id:
        list_props["SMB Client Provider Service"] = smb_svc_id
    if source.get("smb_file_filter"):
        list_props["File Filter"] = source["smb_file_filter"]

    list_id = await _create_processor(
        nifi_url, pg_id,
        "org.apache.nifi.processors.smb.ListSmb",
        "ListFiles",
        properties=list_props,
        auto_terminate=[],
        scheduling_period=schedule_str,
        x=x, y=y_base,
        **nargs,
    )
    if not list_id:
        return False

    # ── 2. FetchSmb ───────────────────────────────────────────────────────
    fetch_props: Dict[str, Any] = {
        "Remote File": "${path}/${filename}",
        "Completion Strategy": "NONE",
    }
    if smb_svc_id:
        fetch_props["SMB Client Provider Service"] = smb_svc_id

    fetch_id = await _create_processor(
        nifi_url, pg_id,
        "org.apache.nifi.processors.smb.FetchSmb",
        "FetchFile",
        properties=fetch_props,
        auto_terminate=["not-found", "permission-denied", "failure"],
        scheduling_period="0 sec",
        x=x, y=y_base + step,
        **nargs,
    )
    if not fetch_id:
        return False

    await _create_connection(nifi_url, pg_id, list_id, fetch_id, ["success"], **nargs)

    if is_binary:
        route_res = await _apply_stream_routing(
            nifi_url=nifi_url,
            pg_id=pg_id,
            stream_name=smb_stream_name,
            routing_rules=smb_routing_rules,
            kafka_topic=kafka_topic,
            kafka_svc_id=kafka_svc_id,
            kafka_key_attr=pk_attr or None,
            input_id=fetch_id,
            input_rels=["success"],
            x=x,
            y=y_base + step * 2,
            step=step,
            scheduling_period="0 sec",
            auth_type=auth_type_nifi,
            username=username_nifi,
            password=password_nifi,
        )
        publish_input_id = route_res["tail_id"]
        publish_input_rel = route_res["tail_rels"][0]
        y_pub = route_res["y"]

        # ── BINARY: publish raw bytes directly ────────────────────────────
        pub_props: Dict[str, Any] = {
            "Topic Name": kafka_topic,
            "acks": "all",
            "compression.type": "gzip",
            "Failure Strategy": "Route to Failure",
            "Transactions Enabled": "false",
            "Publish Strategy": "USE_VALUE",
        }
        if kafka_svc_id:
            pub_props["Kafka Connection Service"] = kafka_svc_id

        pub_id = await _create_processor(
            nifi_url, pg_id,
            "org.apache.nifi.kafka.processors.PublishKafka",
            "PublishToKafka",
            properties=pub_props,
            auto_terminate=["success", "failure"],
            scheduling_period="0 sec",
            x=x, y=y_pub,
            **nargs,
        )
        if not pub_id:
            return False
        await _create_connection(nifi_url, pg_id, publish_input_id, pub_id, [publish_input_rel], **nargs)
        return True

    if is_xlsx:
        # ── XLSX path: Excel→CSV → SplitRecord → UpdateRecord → PublishKafka ──
        y_cur = y_base + step * 2

        # 3a. ConvertRecord: ExcelReader → CSVWriter (XLSX → CSV)
        conv_props: Dict[str, Any] = {"Include Zero Record FlowFiles": "true"}
        if excel_reader_id:
            conv_props["Record Reader"] = excel_reader_id
        if csv_writer_id:
            conv_props["Record Writer"] = csv_writer_id

        conv_id = await _create_processor(
            nifi_url, pg_id,
            "org.apache.nifi.processors.standard.ConvertRecord",
            "ExcelToCSV",
            properties=conv_props,
            auto_terminate=["failure"],
            scheduling_period="0 sec",
            x=x, y=y_cur,
            **nargs,
        )
        if not conv_id:
            return False
        await _create_connection(nifi_url, pg_id, fetch_id, conv_id, ["success"], **nargs)
        y_cur += step

        # 3b. SplitRecord: CSVReader → CSVWriter, 1 record per flowfile
        split_props: Dict[str, Any] = {"Records Per Split": "1"}
        if csv_reader_id:
            split_props["Record Reader"] = csv_reader_id
        if csv_writer_id:
            split_props["Record Writer"] = csv_writer_id

        split_id = await _create_processor(
            nifi_url, pg_id,
            "org.apache.nifi.processors.standard.SplitRecord",
            "SplitCSVRows",
            properties=split_props,
            auto_terminate=["original", "failure"],
            scheduling_period="0 sec",
            x=x, y=y_cur,
            **nargs,
        )
        if not split_id:
            return False
        await _create_connection(nifi_url, pg_id, conv_id, split_id, ["success"], **nargs)
        y_cur += step

        transform_input_id = split_id
        transform_input_rel = "splits"
        transform_input_rels = [transform_input_rel]

        if remove_paths and csv_reader_id and csv_writer_id:
            remove_props: Dict[str, Any] = {
                "Record Reader": csv_reader_id,
                "Record Writer": csv_writer_id,
            }
            for idx, record_path in enumerate(remove_paths, start=1):
                remove_props[f"field_to_remove_{idx}"] = record_path

            remove_id = await _create_processor(
                nifi_url, pg_id,
                "org.apache.nifi.processors.standard.RemoveRecordField",
                "RemoveFields",
                properties=remove_props,
                auto_terminate=["failure"],
                scheduling_period="0 sec",
                x=x, y=y_cur,
                **nargs,
            )
            if not remove_id:
                return False
            await _create_connection(nifi_url, pg_id, split_id, remove_id, ["splits"], **nargs)
            y_cur += step
            transform_input_id = remove_id
            transform_input_rel = "success"
            transform_input_rels = [transform_input_rel]
        elif remove_paths:
            logger.warning("SMB XLSX flow: remove_field rules provided but CSV reader/writer were not available.")

        # 3c. Optional Kafka key extraction from first CSV column (primary key field).
        # For SMB CSV/XLSX in this runtime, EvaluateRecordPath is unavailable; use
        # ExtractText on split CSV row content and map to the selected key attribute.
        if pk_attr:
            key_extract_props: Dict[str, Any] = {
                "Include Capture Group 0": "false",
                # If split output contains header+row, this captures first column from
                # second line; if it is a single row, it captures first column from line 1.
                pk_attr: r"(?s)^(?:[^\r\n]*\r?\n)?\s*\"?([^\",\r\n]*)\"?(?:,|$)",
            }
            key_extract_id = await _create_processor(
                nifi_url, pg_id,
                "org.apache.nifi.processors.standard.ExtractText",
                "ExtractKafkaKey",
                properties=key_extract_props,
                auto_terminate=["failure"],
                scheduling_period="0 sec",
                x=x, y=y_cur,
                **nargs,
            )
            if not key_extract_id:
                return False
            await _create_connection(
                nifi_url,
                pg_id,
                transform_input_id,
                key_extract_id,
                transform_input_rels,
                **nargs,
            )
            y_cur += step
            transform_input_id = key_extract_id
            transform_input_rels = ["matched", "unmatched"]

        if smb_default_props:
            defaults_id = await _create_processor(
                nifi_url, pg_id,
                "org.apache.nifi.processors.attributes.UpdateAttribute",
                "ApplyDefaultAttributes",
                properties=smb_default_props,
                auto_terminate=["failure"],
                scheduling_period="0 sec",
                x=x, y=y_cur,
                **nargs,
            )
            if not defaults_id:
                return False
            await _create_connection(
                nifi_url,
                pg_id,
                transform_input_id,
                defaults_id,
                transform_input_rels,
                **nargs,
            )
            y_cur += step
            transform_input_id = defaults_id
            transform_input_rels = ["success"]

        # 3d. Apply add/set transformations and metadata via UpdateRecord.
        enrich_props = _build_smb_update_record_props(
            record_reader_id=csv_reader_id,
            record_writer_id=avro_writer_id,
        )
        publish_input_id = transform_input_id
        publish_input_rel = "success"
        if _has_update_record_paths(enrich_props):
            enrich_id = await _create_processor(
                nifi_url, pg_id,
                "org.apache.nifi.processors.standard.UpdateRecord",
                "ApplyTransformations",
                properties=enrich_props,
                auto_terminate=["failure"],
                scheduling_period="0 sec",
                x=x, y=y_cur,
                **nargs,
            )
            if not enrich_id:
                return False
            await _create_connection(
                nifi_url,
                pg_id,
                transform_input_id,
                enrich_id,
                transform_input_rels,
                **nargs,
            )
            y_cur += step
            publish_input_id = enrich_id
            publish_input_rel = "success"
        else:
            # No UpdateRecord paths configured; still convert CSV records to Avro.
            if csv_reader_id and avro_writer_id:
                convert_id = await _create_processor(
                    nifi_url, pg_id,
                    "org.apache.nifi.processors.standard.ConvertRecord",
                    "ConvertToAvro",
                    properties={
                        "Record Reader": csv_reader_id,
                        "Record Writer": avro_writer_id,
                    },
                    auto_terminate=["failure"],
                    scheduling_period="0 sec",
                    x=x, y=y_cur,
                    **nargs,
                )
                if not convert_id:
                    return False
                await _create_connection(
                    nifi_url,
                    pg_id,
                    transform_input_id,
                    convert_id,
                    transform_input_rels,
                    **nargs,
                )
                y_cur += step
                publish_input_id = convert_id
                publish_input_rel = "success"

        route_res = await _apply_stream_routing(
            nifi_url=nifi_url,
            pg_id=pg_id,
            stream_name=smb_stream_name,
            routing_rules=smb_routing_rules,
            kafka_topic=kafka_topic,
            kafka_svc_id=kafka_svc_id,
            kafka_key_attr=pk_attr or None,
            input_id=publish_input_id,
            input_rels=[publish_input_rel],
            x=x,
            y=y_cur,
            step=step,
            scheduling_period="0 sec",
            auth_type=auth_type_nifi,
            username=username_nifi,
            password=password_nifi,
        )
        publish_input_id = route_res["tail_id"]
        publish_input_rel = route_res["tail_rels"][0]
        y_cur = route_res["y"]

        # 3e. PublishKafka
        pub_props = {
            "Topic Name": kafka_topic,
            "acks": "all",
            "compression.type": "gzip",
            "Failure Strategy": "Route to Failure",
            "Transactions Enabled": "false",
            "Publish Strategy": "USE_VALUE",
        }
        if kafka_svc_id:
            pub_props["Kafka Connection Service"] = kafka_svc_id
        if pk_attr:
            pub_props["Kafka Key"] = "${" + pk_attr + "}"

        pub_id = await _create_processor(
            nifi_url, pg_id,
            "org.apache.nifi.kafka.processors.PublishKafka",
            "PublishToKafka",
            properties=pub_props,
            auto_terminate=["success", "failure"],
            scheduling_period="0 sec",
            x=x, y=y_cur,
            **nargs,
        )
        if not pub_id:
            return False
        await _create_connection(nifi_url, pg_id, publish_input_id, pub_id, [publish_input_rel], **nargs)
        return True

    if is_csv:
        # ── CSV path: Split row-by-row → inject metadata → publish ─────────
        y_cur = y_base + step * 2
        split_props: Dict[str, Any] = {"Records Per Split": "1"}
        if csv_reader_id:
            split_props["Record Reader"] = csv_reader_id
        if csv_writer_id:
            split_props["Record Writer"] = csv_writer_id
        split_id = await _create_processor(
            nifi_url, pg_id,
            "org.apache.nifi.processors.standard.SplitRecord",
            "SplitCSVRows",
            properties=split_props,
            auto_terminate=["original", "failure"],
            scheduling_period="0 sec",
            x=x, y=y_cur,
            **nargs,
        )
        if not split_id:
            return False
        await _create_connection(nifi_url, pg_id, fetch_id, split_id, ["success"], **nargs)
        y_cur += step

        transform_input_id = split_id
        transform_input_rel = "splits"
        transform_input_rels = [transform_input_rel]

        if remove_paths and csv_reader_id and csv_writer_id:
            remove_props: Dict[str, Any] = {
                "Record Reader": csv_reader_id,
                "Record Writer": csv_writer_id,
            }
            for idx, record_path in enumerate(remove_paths, start=1):
                remove_props[f"field_to_remove_{idx}"] = record_path
            remove_id = await _create_processor(
                nifi_url, pg_id,
                "org.apache.nifi.processors.standard.RemoveRecordField",
                "RemoveFields",
                properties=remove_props,
                auto_terminate=["failure"],
                scheduling_period="0 sec",
                x=x, y=y_cur,
                **nargs,
            )
            if not remove_id:
                return False
            await _create_connection(nifi_url, pg_id, split_id, remove_id, ["splits"], **nargs)
            y_cur += step
            transform_input_id = remove_id
            transform_input_rel = "success"
            transform_input_rels = [transform_input_rel]
        elif remove_paths:
            logger.warning("SMB CSV flow: remove_field rules provided but CSV reader/writer were not available.")

        if pk_attr:
            key_extract_props: Dict[str, Any] = {
                "Include Capture Group 0": "false",
                pk_attr: r"(?s)^(?:[^\r\n]*\r?\n)?\s*\"?([^\",\r\n]*)\"?(?:,|$)",
            }
            key_extract_id = await _create_processor(
                nifi_url, pg_id,
                "org.apache.nifi.processors.standard.ExtractText",
                "ExtractKafkaKey",
                properties=key_extract_props,
                auto_terminate=["failure"],
                scheduling_period="0 sec",
                x=x, y=y_cur,
                **nargs,
            )
            if not key_extract_id:
                return False
            await _create_connection(
                nifi_url,
                pg_id,
                transform_input_id,
                key_extract_id,
                transform_input_rels,
                **nargs,
            )
            y_cur += step
            transform_input_id = key_extract_id
            transform_input_rels = ["matched", "unmatched"]

        if smb_default_props:
            defaults_id = await _create_processor(
                nifi_url, pg_id,
                "org.apache.nifi.processors.attributes.UpdateAttribute",
                "ApplyDefaultAttributes",
                properties=smb_default_props,
                auto_terminate=["failure"],
                scheduling_period="0 sec",
                x=x, y=y_cur,
                **nargs,
            )
            if not defaults_id:
                return False
            await _create_connection(
                nifi_url,
                pg_id,
                transform_input_id,
                defaults_id,
                transform_input_rels,
                **nargs,
            )
            y_cur += step
            transform_input_id = defaults_id
            transform_input_rels = ["success"]

        enrich_props = _build_smb_update_record_props(
            record_reader_id=csv_reader_id,
            record_writer_id=avro_writer_id,
        )
        publish_input_id = transform_input_id
        publish_input_rel = "success"
        if _has_update_record_paths(enrich_props):
            enrich_id = await _create_processor(
                nifi_url, pg_id,
                "org.apache.nifi.processors.standard.UpdateRecord",
                "ApplyTransformations",
                properties=enrich_props,
                auto_terminate=["failure"],
                scheduling_period="0 sec",
                x=x, y=y_cur,
                **nargs,
            )
            if not enrich_id:
                return False
            await _create_connection(
                nifi_url,
                pg_id,
                transform_input_id,
                enrich_id,
                transform_input_rels,
                **nargs,
            )
            y_cur += step
            publish_input_id = enrich_id
            publish_input_rel = "success"
        else:
            # No UpdateRecord paths configured; still convert CSV records to Avro.
            if csv_reader_id and avro_writer_id:
                convert_id = await _create_processor(
                    nifi_url, pg_id,
                    "org.apache.nifi.processors.standard.ConvertRecord",
                    "ConvertToAvro",
                    properties={
                        "Record Reader": csv_reader_id,
                        "Record Writer": avro_writer_id,
                    },
                    auto_terminate=["failure"],
                    scheduling_period="0 sec",
                    x=x, y=y_cur,
                    **nargs,
                )
                if not convert_id:
                    return False
                await _create_connection(
                    nifi_url,
                    pg_id,
                    transform_input_id,
                    convert_id,
                    transform_input_rels,
                    **nargs,
                )
                y_cur += step
                publish_input_id = convert_id
                publish_input_rel = "success"

        route_res = await _apply_stream_routing(
            nifi_url=nifi_url,
            pg_id=pg_id,
            stream_name=smb_stream_name,
            routing_rules=smb_routing_rules,
            kafka_topic=kafka_topic,
            kafka_svc_id=kafka_svc_id,
            kafka_key_attr=pk_attr or None,
            input_id=publish_input_id,
            input_rels=[publish_input_rel],
            x=x,
            y=y_cur,
            step=step,
            scheduling_period="0 sec",
            auth_type=auth_type_nifi,
            username=username_nifi,
            password=password_nifi,
        )
        publish_input_id = route_res["tail_id"]
        publish_input_rel = route_res["tail_rels"][0]
        y_cur = route_res["y"]

        pub_props = {
            "Topic Name": kafka_topic,
            "acks": "all",
            "compression.type": "gzip",
            "Failure Strategy": "Route to Failure",
            "Transactions Enabled": "false",
            "Publish Strategy": "USE_VALUE",
        }
        if kafka_svc_id:
            pub_props["Kafka Connection Service"] = kafka_svc_id
        if pk_attr:
            pub_props["Kafka Key"] = "${" + pk_attr + "}"
        pub_id = await _create_processor(
            nifi_url, pg_id,
            "org.apache.nifi.kafka.processors.PublishKafka",
            "PublishToKafka",
            properties=pub_props,
            auto_terminate=["success", "failure"],
            scheduling_period="0 sec",
            x=x, y=y_cur,
            **nargs,
        )
        if not pub_id:
            return False
        await _create_connection(nifi_url, pg_id, publish_input_id, pub_id, [publish_input_rel], **nargs)
        return True

    # ── JSON/JSONL structured formats: apply optional remove rules, then UpdateRecord ──
    transform_input_id = fetch_id
    transform_input_rel = "success"
    transform_input_rels = [transform_input_rel]
    transform_y = y_base + step * 2

    if is_jsonl:
        split_text_id = await _create_processor(
            nifi_url, pg_id,
            "org.apache.nifi.processors.standard.SplitText",
            "SplitJSONLLines",
            properties={"Line Split Count": "1", "Remove Trailing Newlines": "true"},
            auto_terminate=["original", "failure"],
            scheduling_period="0 sec",
            x=x, y=transform_y,
            **nargs,
        )
        if not split_text_id:
            return False
        await _create_connection(nifi_url, pg_id, fetch_id, split_text_id, ["success"], **nargs)
        transform_input_id = split_text_id
        transform_input_rel = "splits"
        transform_input_rels = [transform_input_rel]
        transform_y += step

    if remove_paths and json_reader_id and json_writer_id:
        remove_props: Dict[str, Any] = {
            "Record Reader": json_reader_id,
            "Record Writer": json_writer_id,
        }
        for idx, record_path in enumerate(remove_paths, start=1):
            remove_props[f"field_to_remove_{idx}"] = record_path
        remove_id = await _create_processor(
            nifi_url, pg_id,
            "org.apache.nifi.processors.standard.RemoveRecordField",
            "RemoveFields",
            properties=remove_props,
            auto_terminate=["failure"],
            scheduling_period="0 sec",
            x=x, y=transform_y,
            **nargs,
        )
        if not remove_id:
            return False
        await _create_connection(nifi_url, pg_id, transform_input_id, remove_id, transform_input_rels, **nargs)
        transform_input_id = remove_id
        transform_input_rel = "success"
        transform_input_rels = [transform_input_rel]
        transform_y += step
    elif remove_paths:
        logger.warning("SMB JSON/JSONL flow: remove_field rules provided but JSON writer service is unavailable.")

    enrich_props = _build_smb_update_record_props(
        record_reader_id=json_reader_id,
        record_writer_id=avro_writer_id,
    )
    publish_input_id = transform_input_id
    publish_input_rel = transform_input_rel
    if _has_update_record_paths(enrich_props):
        enrich_id = await _create_processor(
            nifi_url, pg_id,
            "org.apache.nifi.processors.standard.UpdateRecord",
            "ApplyTransformations",
            properties=enrich_props,
            auto_terminate=["failure"],
            scheduling_period="0 sec",
            x=x, y=transform_y,
            **nargs,
        )
        if not enrich_id:
            return False
        await _create_connection(nifi_url, pg_id, transform_input_id, enrich_id, transform_input_rels, **nargs)
        publish_input_id = enrich_id
        publish_input_rel = "success"
    else:
        # No UpdateRecord paths configured; still convert JSON/JSONL records to Avro.
        if json_reader_id and avro_writer_id:
            convert_id = await _create_processor(
                nifi_url, pg_id,
                "org.apache.nifi.processors.standard.ConvertRecord",
                "ConvertToAvro",
                properties={
                    "Record Reader": json_reader_id,
                    "Record Writer": avro_writer_id,
                },
                auto_terminate=["failure"],
                scheduling_period="0 sec",
                x=x, y=transform_y,
                **nargs,
            )
            if not convert_id:
                return False
            await _create_connection(nifi_url, pg_id, transform_input_id, convert_id, transform_input_rels, **nargs)
            publish_input_id = convert_id
            publish_input_rel = "success"

    route_res = await _apply_stream_routing(
        nifi_url=nifi_url,
        pg_id=pg_id,
        stream_name=smb_stream_name,
        routing_rules=smb_routing_rules,
        kafka_topic=kafka_topic,
        kafka_svc_id=kafka_svc_id,
        kafka_key_attr=pk_attr or None,
        input_id=publish_input_id,
        input_rels=[publish_input_rel],
        x=x,
        y=transform_y,
        step=step,
        scheduling_period="0 sec",
        auth_type=auth_type_nifi,
        username=username_nifi,
        password=password_nifi,
    )
    publish_input_id = route_res["tail_id"]
    publish_input_rel = route_res["tail_rels"][0]

    pub_props = {
        "Topic Name": kafka_topic,
        "acks": "all",
        "compression.type": "gzip",
        "Failure Strategy": "Route to Failure",
        "Transactions Enabled": "false",
        "Publish Strategy": "USE_VALUE",
    }
    if kafka_svc_id:
        pub_props["Kafka Connection Service"] = kafka_svc_id

    pub_id = await _create_processor(
        nifi_url, pg_id,
        "org.apache.nifi.kafka.processors.PublishKafka",
        "PublishToKafka",
        properties=pub_props,
        auto_terminate=["success", "failure"],
        scheduling_period="0 sec",
        x=x, y=transform_y + step,
        **nargs,
    )
    if not pub_id:
        return False
    await _create_connection(nifi_url, pg_id, publish_input_id, pub_id, [publish_input_rel], **nargs)
    return True


def _trino_sql_literal(value: Any) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def _trino_identifier(value: str) -> str:
    text = str(value or "").strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", text):
        raise ValueError(f"Invalid Trino identifier: {text}")
    return f'"{text}"'


def _trino_table_ref(catalog: str, schema: str, table: str) -> str:
    return ".".join(_trino_identifier(part) for part in (catalog, schema, table))


def _trino_dotted_ref(value: str) -> str:
    parts = [part.strip() for part in str(value or "").split(".") if part.strip()]
    if len(parts) < 2:
        raise ValueError(f"Invalid Trino table reference: {value}")
    return ".".join(_trino_identifier(part) for part in parts)


def _trino_checkpoint_ref(source: dict) -> str:
    catalog = str(source.get("trino_checkpoint_catalog") or "").strip()
    schema = str(source.get("trino_checkpoint_schema") or "").strip()
    table = str(source.get("trino_checkpoint_table_name") or "").strip()
    if catalog and schema and table:
        return _trino_table_ref(catalog, schema, table)
    return _trino_dotted_ref(str(source.get("trino_checkpoint_table") or "").strip())


def _trino_json_value_expr(column: str, data_type: Optional[str]) -> str:
    ident = _trino_identifier(column)
    type_text = str(data_type or "").strip().lower()
    if type_text.startswith(("row(", "array(", "map(")) or type_text == "json":
        return f"json_format(CAST({ident} AS JSON)) FORMAT JSON"
    if type_text.startswith(("timestamp", "time", "date")):
        return f"CAST({ident} AS varchar)"
    return ident


def _trino_json_object_expr(columns: List[str], column_types: Optional[Dict[str, str]] = None) -> str:
    parts = []
    types = {str(k).lower(): v for k, v in (column_types or {}).items()}
    for column in columns:
        name = str(column or "").strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError(f"Invalid Trino column name: {name}")
        parts.append(f"'{name}' VALUE {_trino_json_value_expr(name, types.get(name.lower()))}")
    return "json_object(" + ", ".join(parts) + ")"


def _trino_column_expr_builder_sql() -> str:
    identifier_expr = "'\"' || replace(column_name, '\"', '\"\"') || '\"'"
    return (
        "'''' || replace(column_name, '''', '''''') || ''' VALUE ' || "
        "CASE "
        "WHEN lower(data_type) LIKE 'row(%' OR lower(data_type) LIKE 'array(%' "
        "OR lower(data_type) LIKE 'map(%' OR lower(data_type) = 'json' "
        "THEN 'CAST(NULL AS varchar)' "
        "WHEN lower(data_type) LIKE 'timestamp%' OR lower(data_type) LIKE 'time%' OR lower(data_type) LIKE 'date%' "
        f"THEN 'CAST(' || {identifier_expr} || ' AS varchar)' "
        f"ELSE {identifier_expr} END"
    )


def _trino_all_columns_query_builder_sql(catalog: str, schema: str, table: str, payload_sql_template: str) -> str:
    marker = "__TRINO_ROW_JSON_EXPR__"
    if marker not in payload_sql_template:
        raise ValueError("All-column payload template is missing row JSON marker")
    prefix, suffix = payload_sql_template.split(marker, 1)
    return (
        "WITH column_exprs AS ("
        "SELECT ordinal_position, "
        f"{_trino_column_expr_builder_sql()} AS expr "
        f"FROM {_trino_identifier(catalog)}.information_schema.columns "
        f"WHERE table_schema = {_trino_sql_literal(schema)} "
        f"AND table_name = {_trino_sql_literal(table)}"
        "), row_expr AS ("
        "SELECT 'json_object(' || array_join(array_agg(expr ORDER BY ordinal_position), ', ') || ')' AS row_json_expr "
        "FROM column_exprs"
        ") "
        f"SELECT {_trino_sql_literal(prefix)} || row_json_expr || {_trino_sql_literal(suffix)} FROM row_expr"
    )


async def _trino_statement_data(
    endpoint: str,
    user: str,
    catalog: str,
    schema: str,
    sql: str,
    max_polls: int = 30,
) -> List[List[Any]]:
    headers = {
        "X-Trino-User": user,
        "X-Trino-Source": "nifi-flow-designer",
        "X-Trino-Catalog": catalog,
        "X-Trino-Schema": schema,
        "Accept": "application/json",
    }
    async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=30.0, follow_redirects=True) as client:
        response = await client.post(f"{endpoint.rstrip('/')}/v1/statement", headers=headers, content=sql)
        response.raise_for_status()
        payload = response.json()
        data: List[List[Any]] = []
        if payload.get("data"):
            data.extend(payload["data"])
        polls = 0
        while payload.get("nextUri") and polls < max_polls:
            polls += 1
            response = await client.get(payload["nextUri"], headers=headers)
            response.raise_for_status()
            payload = response.json()
            if payload.get("error"):
                raise RuntimeError(payload["error"].get("message") or "Trino query failed")
            if payload.get("data"):
                data.extend(payload["data"])
        return data


async def _trino_column_types(
    endpoint: str,
    user: str,
    catalog: str,
    schema: str,
    table: str,
    columns: List[str],
) -> Dict[str, str]:
    if not columns:
        return {}
    column_list = ", ".join(_trino_sql_literal(column) for column in columns)
    sql = (
        "SELECT column_name, data_type "
        f"FROM {_trino_identifier(catalog)}.information_schema.columns "
        f"WHERE table_schema = {_trino_sql_literal(schema)} "
        f"AND table_name = {_trino_sql_literal(table)} "
        f"AND column_name IN ({column_list})"
    )
    rows = await _trino_statement_data(endpoint, user, catalog, schema, sql)
    return {str(row[0]): str(row[1]) for row in rows if len(row) >= 2}


async def _build_trino_iceberg_flow(
    nifi_url: str,
    pg_id: str,
    source: dict,
    kafka_svc_id: Optional[str],
    kafka_topic: str,
    schedule_str: str,
    auth_type_nifi: str,
    username_nifi: Optional[str],
    password_nifi: Optional[str],
) -> bool:
    """Build a Trino HTTP API flow that exports one Kafka message per selected Iceberg row."""
    nargs = {"auth_type": auth_type_nifi, "username": username_nifi, "password": password_nifi}

    endpoint = str(source.get("trino_endpoint") or "").rstrip("/")
    user = str(source.get("trino_user") or "admin").strip() or "admin"
    catalog = str(source.get("trino_catalog") or "").strip()
    schema = str(source.get("trino_schema") or "").strip()
    table = str(source.get("trino_table") or "").strip()
    checkpoint_table = str(source.get("trino_checkpoint_table") or "").strip()
    auto_create_checkpoint_table = source.get("trino_auto_create_checkpoint_table", True) is not False
    has_split_checkpoint = all(
        str(source.get(key) or "").strip()
        for key in ("trino_checkpoint_catalog", "trino_checkpoint_schema", "trino_checkpoint_table_name")
    )
    column_mode = str(source.get("trino_column_mode") or "manual").strip().lower()
    columns = [str(c).strip() for c in (source.get("trino_columns") or []) if str(c or "").strip()]
    if column_mode not in {"manual", "all"}:
        logger.error("Trino source has invalid column mode.")
        return False
    if not endpoint or not catalog or not schema or not table or (not checkpoint_table and not has_split_checkpoint):
        logger.error("Trino source is missing endpoint, table, or checkpoint table.")
        return False
    if column_mode == "manual" and not columns:
        logger.error("Trino source is missing endpoint, table, checkpoint table, or columns.")
        return False

    try:
        table_ref = _trino_table_ref(catalog, schema, table)
        snapshots_ref = _trino_table_ref(catalog, schema, f"{table}$snapshots")
        checkpoint_ref = _trino_checkpoint_ref(source)
        if column_mode == "manual":
            column_types = await _trino_column_types(endpoint, user, catalog, schema, table, columns)
            row_json = _trino_json_object_expr(columns, column_types)
        else:
            row_json = "__TRINO_ROW_JSON_EXPR__"
    except ValueError as exc:
        logger.error(str(exc))
        return False

    full_table_name = f"{catalog}.{schema}.{table}"
    statement_url = f"{endpoint}/v1/statement"
    topic = kafka_topic or ((source.get("kafka_output") or {}).get("topic")) or f"{catalog}.{schema}.{table}"

    trino_pg_id = await _create_process_group(
        nifi_url, pg_id, "trino_iceberg_snapshot_export",
        x=950.0, y=220.0, **nargs,
    )
    if not trino_pg_id:
        return False

    x = 260.0
    y = 120.0
    step = 115.0

    async def proc(
        proc_type: str,
        name: str,
        properties: Dict[str, Any],
        auto: List[str],
        dx: float = 0.0,
        scheduling_period: str = "0 sec",
    ) -> Optional[str]:
        nonlocal y
        proc_id = await _create_processor(
            nifi_url, trino_pg_id, proc_type, name,
            properties=properties,
            auto_terminate=auto,
            scheduling_period=scheduling_period,
            x=x + dx, y=y,
            **nargs,
        )
        y += step
        return proc_id

    trigger_id = await proc(
        "org.apache.nifi.processors.standard.GenerateFlowFile",
        "trigger_snapshot_check",
        {"File Size": "0 B", "Custom Text": "trino snapshot check", "Batch Size": "1"},
        ["success"],
        scheduling_period=schedule_str,
    )
    headers_id = await proc(
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        "set_trino_headers",
        {
            "mime.type": "text/plain",
            "trino.statement.url": statement_url,
            "trino.user": user,
            "X-Trino-User": user,
            "X-Trino-Source": "nifi-flow-designer",
            "X-Trino-Catalog": catalog,
            "X-Trino-Schema": schema,
            "Accept": "application/json",
        },
        ["success"],
    )
    checkpoint_create_sql_id = None
    checkpoint_create_http_id = None
    checkpoint_create_eval_id = None
    checkpoint_create_route_id = None
    checkpoint_create_poll_id = None
    if auto_create_checkpoint_table:
        checkpoint_create_sql = (
            f"CREATE TABLE IF NOT EXISTS {checkpoint_ref} ("
            "table_name varchar, "
            "last_snapshot_id bigint, "
            "processed_at timestamp, "
            "row_count bigint"
            ")"
        )
        checkpoint_create_sql_id = await proc(
            "org.apache.nifi.processors.standard.ReplaceText",
            "write_checkpoint_create_sql",
            {
                "Search Value": "(?s)^.*$",
                "Replacement Value": checkpoint_create_sql,
                "Replacement Strategy": "Always Replace",
                "Evaluation Mode": "Entire text",
                "Maximum Buffer Size": "10 MB",
            },
            ["failure"],
        )
        checkpoint_create_http_id = await proc(
            "org.apache.nifi.processors.standard.InvokeHTTP",
            "submit_checkpoint_create_query",
            {
                "HTTP URL": "${trino.statement.url}",
                "HTTP Method": "POST",
                "Request Header Attributes Pattern": "X-Trino-User|X-Trino-Source|X-Trino-Catalog|X-Trino-Schema|Accept",
                "Connection Timeout": "30 secs",
                "Socket Read Timeout": "60 secs",
                "HTTP/2 Disabled": "True",
            },
            ["Original", "Failure", "No Retry", "Retry", "Response"],
        )
        checkpoint_create_eval_id = await proc(
            "org.apache.nifi.processors.standard.EvaluateJsonPath",
            "extract_checkpoint_create_status",
            {
                "Destination": "flowfile-attribute",
                "Return Type": "auto-detect",
                "Path Not Found Behavior": "ignore",
                "Max String Length": "20 MB",
                "trino.nextUri": "$.nextUri",
            },
            ["failure", "unmatched"],
        )
        checkpoint_create_route_id = await proc(
            "org.apache.nifi.processors.standard.RouteOnAttribute",
            "route_checkpoint_create_state",
            {
                "Routing Strategy": "Route to Property name",
                "done": "${trino.nextUri:isEmpty()}",
                "needs_poll": "${trino.nextUri:isEmpty():not()}",
            },
            ["unmatched"],
        )
        checkpoint_create_poll_id = await proc(
            "org.apache.nifi.processors.standard.InvokeHTTP",
            "poll_checkpoint_create_query",
            {
                "HTTP URL": "${trino.nextUri}",
                "HTTP Method": "GET",
                "Request Header Attributes Pattern": "X-Trino-User|X-Trino-Source|X-Trino-Catalog|X-Trino-Schema|Accept",
                "Connection Timeout": "30 secs",
                "Socket Read Timeout": "60 secs",
                "HTTP/2 Disabled": "True",
            },
            ["Original", "Failure", "No Retry", "Retry"],
        )
    checkpoint_sql = (
        "SELECT CAST(last_snapshot_id AS varchar) "
        f"FROM {checkpoint_ref} "
        f"WHERE table_name = {_trino_sql_literal(full_table_name)} "
        "ORDER BY processed_at DESC LIMIT 1"
    )
    checkpoint_sql_id = await proc(
        "org.apache.nifi.processors.standard.ReplaceText",
        "write_checkpoint_lookup_sql",
        {
            "Search Value": "(?s)^.*$",
            "Replacement Value": checkpoint_sql,
            "Replacement Strategy": "Always Replace",
            "Evaluation Mode": "Entire text",
            "Maximum Buffer Size": "10 MB",
        },
        ["failure"],
    )
    checkpoint_http_id = await proc(
        "org.apache.nifi.processors.standard.InvokeHTTP",
        "submit_checkpoint_lookup_query",
        {
            "HTTP URL": "${trino.statement.url}",
            "HTTP Method": "POST",
            "Request Header Attributes Pattern": "X-Trino-User|X-Trino-Source|X-Trino-Catalog|X-Trino-Schema|Accept",
            "Connection Timeout": "30 secs",
            "Socket Read Timeout": "60 secs",
            "HTTP/2 Disabled": "True",
        },
        ["Original", "Failure", "No Retry", "Retry"],
    )
    checkpoint_eval_id = await proc(
        "org.apache.nifi.processors.standard.EvaluateJsonPath",
        "extract_checkpoint_snapshot",
        {
            "Destination": "flowfile-attribute",
            "Return Type": "auto-detect",
            "Path Not Found Behavior": "ignore",
            "Max String Length": "20 MB",
            "checkpoint.snapshot.id": "$.data[0][0]",
            "trino.nextUri": "$.nextUri",
        },
        ["failure", "unmatched"],
    )
    checkpoint_route_id = await proc(
        "org.apache.nifi.processors.standard.RouteOnAttribute",
        "route_checkpoint_state",
        {
            "Routing Strategy": "Route to Property name",
            "has_checkpoint": "${checkpoint.snapshot.id:isEmpty():not()}",
            "no_checkpoint": "${checkpoint.snapshot.id:isEmpty():and(${trino.nextUri:isEmpty()})}",
            "needs_poll": "${checkpoint.snapshot.id:isEmpty():and(${trino.nextUri:isEmpty():not()})}",
        },
        ["unmatched"],
    )
    checkpoint_poll_id = await proc(
        "org.apache.nifi.processors.standard.InvokeHTTP",
        "poll_checkpoint_lookup_query",
        {
            "HTTP URL": "${trino.nextUri}",
            "HTTP Method": "GET",
            "Request Header Attributes Pattern": "X-Trino-User|X-Trino-Source|X-Trino-Catalog|X-Trino-Schema|Accept",
            "Connection Timeout": "30 secs",
            "Socket Read Timeout": "60 secs",
            "HTTP/2 Disabled": "True",
        },
        ["Original", "Failure", "No Retry", "Retry"],
    )

    snapshot_sql = (
        "WITH checkpoint AS ("
        "SELECT TRY_CAST(NULLIF('${checkpoint.snapshot.id}', '') AS bigint) AS checkpoint_id"
        "), checkpoint_time AS ("
        f"SELECT committed_at FROM {snapshots_ref} WHERE snapshot_id = (SELECT checkpoint_id FROM checkpoint)"
        "), next_snapshot AS ("
        "SELECT committed_at, snapshot_id, parent_id, operation "
        f"FROM {snapshots_ref} "
        "WHERE (SELECT checkpoint_id FROM checkpoint) IS NULL "
        "OR committed_at > (SELECT committed_at FROM checkpoint_time) "
        "ORDER BY committed_at DESC LIMIT 1"
        ") "
        "SELECT CAST(committed_at AS varchar), CAST(snapshot_id AS varchar), "
        "CAST(parent_id AS varchar), operation FROM next_snapshot"
    )
    snapshot_sql_id = await proc(
        "org.apache.nifi.processors.standard.ReplaceText",
        "write_snapshot_payload_sql",
        {
            "Search Value": "(?s)^.*$",
            "Replacement Value": snapshot_sql,
            "Replacement Strategy": "Always Replace",
            "Evaluation Mode": "Entire text",
            "Maximum Buffer Size": "10 MB",
        },
        ["failure"],
    )
    snapshot_http_id = await proc(
        "org.apache.nifi.processors.standard.InvokeHTTP",
        "submit_snapshot_payload_query",
        {
            "HTTP URL": "${trino.statement.url}",
            "HTTP Method": "POST",
            "Request Header Attributes Pattern": "X-Trino-User|X-Trino-Source|X-Trino-Catalog|X-Trino-Schema|Accept",
            "Connection Timeout": "30 secs",
            "Socket Read Timeout": "60 secs",
            "HTTP/2 Disabled": "True",
        },
        ["Original", "Failure", "No Retry", "Retry"],
    )
    snapshot_eval_id = await proc(
        "org.apache.nifi.processors.standard.EvaluateJsonPath",
        "extract_snapshot_payload",
        {
            "Destination": "flowfile-attribute",
            "Return Type": "auto-detect",
            "Path Not Found Behavior": "ignore",
            "Max String Length": "20 MB",
            "snapshot.id": "$.data[0][1]",
            "snapshot.parent.id": "$.data[0][2]",
            "snapshot.operation": "$.data[0][3]",
            "trino.nextUri": "$.nextUri",
        },
        ["failure", "unmatched"],
    )
    snapshot_route_id = await proc(
        "org.apache.nifi.processors.standard.RouteOnAttribute",
        "route_snapshot_state",
        {
            "Routing Strategy": "Route to Property name",
            "has_snapshot": "${snapshot.id:isEmpty():not()}",
            "needs_poll": "${snapshot.id:isEmpty():and(${trino.nextUri:isEmpty():not()})}",
        },
        ["unmatched"],
    )
    snapshot_poll_id = await proc(
        "org.apache.nifi.processors.standard.InvokeHTTP",
        "poll_snapshot_payload_query",
        {
            "HTTP URL": "${trino.nextUri}",
            "HTTP Method": "GET",
            "Request Header Attributes Pattern": "X-Trino-User|X-Trino-Source|X-Trino-Catalog|X-Trino-Schema|Accept",
            "Connection Timeout": "30 secs",
            "Socket Read Timeout": "60 secs",
            "HTTP/2 Disabled": "True",
        },
        ["Original", "Failure", "No Retry", "Retry"],
    )
    rows_mode_route_id = await proc(
        "org.apache.nifi.processors.standard.RouteOnAttribute",
        "route_rows_query_mode",
        {
            "Routing Strategy": "Route to Property name",
            "bootstrap": "${checkpoint.snapshot.id:isEmpty()}",
            "incremental": "${checkpoint.snapshot.id:isEmpty():not()}",
        },
        ["unmatched"],
    )
    bootstrap_row_payload_sql = (
        "WITH rows AS ("
        "SELECT "
        "json_object("
        "'snapshot_id' VALUE CAST(${snapshot.id} AS varchar), "
        "'parent_snapshot_id' VALUE NULLIF('${snapshot.parent.id}', ''), "
        "'operation' VALUE '${snapshot.operation}', "
        "'change_type' VALUE 'bootstrap', "
        "'table' VALUE " + _trino_sql_literal(full_table_name) + ", "
        f"'row' VALUE {row_json}"
        ") AS row_json "
        f"FROM {table_ref} FOR VERSION AS OF ${{snapshot.id}}"
        ") "
        "SELECT "
        "'{\"snapshot_id\":\"${snapshot.id}\",\"parent_snapshot_id\":' || "
        "COALESCE('\"' || NULLIF('${snapshot.parent.id}', '') || '\"', 'null') || "
        "',\"operation\":\"${snapshot.operation}\",\"table\":\"" + full_table_name + "\",\"rows\":' || "
        "json_format(CAST(array_agg(json_parse(row_json)) AS JSON)) || "
        "'}' FROM rows"
    )
    incremental_row_payload_sql = (
        "WITH rows AS ("
        "SELECT "
        "json_object("
        "'snapshot_id' VALUE CAST(_change_version_id AS varchar), "
        "'parent_snapshot_id' VALUE CAST(${checkpoint.snapshot.id} AS varchar), "
        "'operation' VALUE '${snapshot.operation}', "
        "'change_type' VALUE _change_type, "
        "'change_timestamp' VALUE CAST(_change_timestamp AS varchar), "
        "'change_ordinal' VALUE _change_ordinal, "
        "'table' VALUE " + _trino_sql_literal(full_table_name) + ", "
        f"'row' VALUE {row_json}"
        ") AS row_json "
        "FROM TABLE(system.table_changes("
        f"schema_name => {_trino_sql_literal(schema)}, "
        f"table_name => {_trino_sql_literal(table)}, "
        "start_snapshot_id => ${checkpoint.snapshot.id}, "
        "end_snapshot_id => ${snapshot.id}"
        "))"
        ") "
        "SELECT "
        "'{\"snapshot_id\":\"${snapshot.id}\",\"parent_snapshot_id\":\"${checkpoint.snapshot.id}\","
        "\"operation\":\"${snapshot.operation}\",\"table\":\"" + full_table_name + "\",\"rows\":' || "
        "json_format(CAST(array_agg(json_parse(row_json)) AS JSON)) || "
        "'}' FROM rows"
    )
    if column_mode == "all":
        bootstrap_row_payload_sql = (
            "SELECT "
            "json_object("
            "'snapshot_id' VALUE CAST(${snapshot.id} AS varchar), "
            "'parent_snapshot_id' VALUE NULLIF('${snapshot.parent.id}', ''), "
            "'operation' VALUE '${snapshot.operation}', "
            "'change_type' VALUE 'bootstrap', "
            "'table' VALUE " + _trino_sql_literal(full_table_name) + ", "
            f"'row' VALUE {row_json}"
            ") "
            f"FROM {table_ref} FOR VERSION AS OF ${{snapshot.id}}"
        )
        incremental_row_payload_sql = (
            "SELECT "
            "json_object("
            "'snapshot_id' VALUE CAST(_change_version_id AS varchar), "
            "'parent_snapshot_id' VALUE CAST(${checkpoint.snapshot.id} AS varchar), "
            "'operation' VALUE '${snapshot.operation}', "
            "'change_type' VALUE _change_type, "
            "'change_timestamp' VALUE CAST(_change_timestamp AS varchar), "
            "'change_ordinal' VALUE _change_ordinal, "
            "'table' VALUE " + _trino_sql_literal(full_table_name) + ", "
            f"'row' VALUE {row_json}"
            ") "
            "FROM TABLE(system.table_changes("
            f"schema_name => {_trino_sql_literal(schema)}, "
            f"table_name => {_trino_sql_literal(table)}, "
            "start_snapshot_id => ${checkpoint.snapshot.id}, "
            "end_snapshot_id => ${snapshot.id}"
            "))"
        )
        try:
            bootstrap_row_payload_sql = _trino_all_columns_query_builder_sql(
                catalog, schema, table, bootstrap_row_payload_sql
            )
            incremental_row_payload_sql = _trino_all_columns_query_builder_sql(
                catalog, schema, table, incremental_row_payload_sql
            )
        except ValueError as exc:
            logger.error(str(exc))
            return False
    bootstrap_payload_sql_id = await proc(
        "org.apache.nifi.processors.standard.ReplaceText",
        "write_bootstrap_rows_query_builder_sql" if column_mode == "all" else "write_bootstrap_rows_query_sql",
        {
            "Search Value": "(?s)^.*$",
            "Replacement Value": bootstrap_row_payload_sql,
            "Replacement Strategy": "Always Replace",
            "Evaluation Mode": "Entire text",
            "Maximum Buffer Size": "10 MB",
        },
        ["failure"],
    )
    incremental_payload_sql_id = await proc(
        "org.apache.nifi.processors.standard.ReplaceText",
        "write_incremental_rows_query_builder_sql" if column_mode == "all" else "write_incremental_rows_query_sql",
        {
            "Search Value": "(?s)^.*$",
            "Replacement Value": incremental_row_payload_sql,
            "Replacement Strategy": "Always Replace",
            "Evaluation Mode": "Entire text",
            "Maximum Buffer Size": "10 MB",
        },
        ["failure"],
    )
    rows_builder_http_id = None
    rows_builder_eval_id = None
    rows_builder_route_id = None
    rows_builder_poll_id = None
    generated_query_content_id = None
    trino_row_extract_id = None
    trino_row_content_id = None
    if column_mode == "all":
        rows_builder_http_id = await proc(
            "org.apache.nifi.processors.standard.InvokeHTTP",
            "submit_rows_query_builder",
            {
                "HTTP URL": "${trino.statement.url}",
                "HTTP Method": "POST",
                "Request Header Attributes Pattern": "X-Trino-User|X-Trino-Source|X-Trino-Catalog|X-Trino-Schema|Accept",
                "Connection Timeout": "30 secs",
                "Socket Read Timeout": "120 secs",
                "HTTP/2 Disabled": "True",
            },
            ["Original", "Failure", "No Retry", "Retry"],
        )
        rows_builder_eval_id = await proc(
            "org.apache.nifi.processors.standard.EvaluateJsonPath",
            "extract_generated_rows_query",
            {
                "Destination": "flowfile-attribute",
                "Return Type": "auto-detect",
                "Path Not Found Behavior": "ignore",
                "Max String Length": "100 MB",
                "trino.generated.rows.sql": "$.data[0][0]",
                "trino.nextUri": "$.nextUri",
            },
            ["failure", "unmatched"],
        )
        rows_builder_route_id = await proc(
            "org.apache.nifi.processors.standard.RouteOnAttribute",
            "route_rows_query_builder_state",
            {
                "Routing Strategy": "Route to Property name",
                "has_query": "${trino.generated.rows.sql:isEmpty():not()}",
                "needs_poll": "${trino.generated.rows.sql:isEmpty():and(${trino.nextUri:isEmpty():not()})}",
            },
            ["unmatched"],
        )
        rows_builder_poll_id = await proc(
            "org.apache.nifi.processors.standard.InvokeHTTP",
            "poll_rows_query_builder",
            {
                "HTTP URL": "${trino.nextUri}",
                "HTTP Method": "GET",
                "Request Header Attributes Pattern": "X-Trino-User|X-Trino-Source|X-Trino-Catalog|X-Trino-Schema|Accept",
                "Connection Timeout": "30 secs",
                "Socket Read Timeout": "120 secs",
                "HTTP/2 Disabled": "True",
            },
            ["Original", "Failure", "No Retry", "Retry"],
        )
        generated_query_content_id = await proc(
            "org.apache.nifi.processors.standard.ReplaceText",
            "write_generated_rows_query_to_content",
            {
                "Search Value": "(?s)^.*$",
                "Replacement Value": "${trino.generated.rows.sql}",
                "Replacement Strategy": "Always Replace",
                "Evaluation Mode": "Entire text",
                "Maximum Buffer Size": "100 MB",
            },
            ["failure"],
        )
    payload_http_id = await proc(
        "org.apache.nifi.processors.standard.InvokeHTTP",
        "submit_rows_query",
        {
            "HTTP URL": "${trino.statement.url}",
            "HTTP Method": "POST",
            "Request Header Attributes Pattern": "X-Trino-User|X-Trino-Source|X-Trino-Catalog|X-Trino-Schema|Accept",
            "Connection Timeout": "30 secs",
            "Socket Read Timeout": "120 secs",
            "HTTP/2 Disabled": "True",
        },
        ["Original", "Failure", "No Retry", "Retry"],
    )
    rows_eval_id = await proc(
        "org.apache.nifi.processors.standard.EvaluateJsonPath",
        "extract_rows_payload",
        {
            "Destination": "flowfile-attribute",
            "Return Type": "auto-detect",
            "Path Not Found Behavior": "ignore",
            "Max String Length": "100 MB",
            "trino.payload": "$.data[0][0]",
            "trino.nextUri": "$.nextUri",
        },
        ["failure", "unmatched"],
    )
    rows_route_id = await proc(
        "org.apache.nifi.processors.standard.RouteOnAttribute",
        "route_rows_payload_state",
        {
            "Routing Strategy": "Route to Property name",
            "has_payload": "${trino.payload:isEmpty():not()}",
            "needs_poll": "${trino.nextUri:isEmpty():not()}",
        },
        ["unmatched"],
    )
    rows_poll_id = await proc(
        "org.apache.nifi.processors.standard.InvokeHTTP",
        "poll_rows_query",
        {
            "HTTP URL": "${trino.nextUri}",
            "HTTP Method": "GET",
            "Request Header Attributes Pattern": "X-Trino-User|X-Trino-Source|X-Trino-Catalog|X-Trino-Schema|Accept",
            "Connection Timeout": "30 secs",
            "Socket Read Timeout": "120 secs",
            "HTTP/2 Disabled": "True",
        },
        ["Original", "Failure", "No Retry", "Retry"],
    )
    payload_content_id = None
    if column_mode != "all":
        payload_content_id = await proc(
            "org.apache.nifi.processors.standard.ReplaceText",
            "write_rows_payload_to_content",
            {
                "Search Value": "(?s)^.*$",
                "Replacement Value": "${trino.payload}",
                "Replacement Strategy": "Always Replace",
                "Evaluation Mode": "Entire text",
                "Maximum Buffer Size": "100 MB",
            },
            ["failure"],
        )
    if column_mode == "all":
        trino_row_extract_id = await proc(
            "org.apache.nifi.processors.standard.EvaluateJsonPath",
            "extract_trino_row_json",
            {
                "Destination": "flowfile-attribute",
                "Return Type": "auto-detect",
                "Path Not Found Behavior": "ignore",
                "Max String Length": "100 MB",
                "trino.row.json": "$[0]",
            },
            ["failure", "unmatched"],
        )
        trino_row_content_id = await proc(
            "org.apache.nifi.processors.standard.ReplaceText",
            "write_trino_row_json_to_content",
            {
                "Search Value": "(?s)^.*$",
                "Replacement Value": "${trino.row.json}",
                "Replacement Strategy": "Always Replace",
                "Evaluation Mode": "Entire text",
                "Maximum Buffer Size": "100 MB",
            },
            ["failure"],
        )
    split_id = await proc(
        "org.apache.nifi.processors.standard.SplitJson",
        "split_trino_response_rows_to_messages" if column_mode == "all" else "split_snapshot_rows_to_messages",
        {"JsonPath Expression": "$.data[*]" if column_mode == "all" else "$.rows[*]", "Null Value Representation": "empty string"},
        ["failure", "original"],
    )
    publish_props: Dict[str, Any] = {
        "Topic Name": topic,
        "acks": "all",
        "compression.type": "gzip",
        "Failure Strategy": "Route to Failure",
        "Transactions Enabled": "false",
        "Publish Strategy": "USE_VALUE",
        "Kafka Key": "${snapshot.id}",
    }
    if kafka_svc_id:
        publish_props["Kafka Connection Service"] = kafka_svc_id
    publish_id = await proc(
        "org.apache.nifi.kafka.processors.PublishKafka",
        "publish_one_row_to_kafka",
        publish_props,
        ["success", "failure"],
    )
    checkpoint_update_sql = (
        f"INSERT INTO {checkpoint_ref} (table_name, last_snapshot_id, processed_at, row_count) "
        f"VALUES ({_trino_sql_literal(full_table_name)}, ${{snapshot.id}}, current_timestamp, ${{fragment.count}})"
    )
    checkpoint_update_id = await proc(
        "org.apache.nifi.processors.standard.ReplaceText",
        "write_checkpoint_update_sql",
        {
            "Search Value": "(?s)^.*$",
            "Replacement Value": checkpoint_update_sql,
            "Replacement Strategy": "Always Replace",
            "Evaluation Mode": "Entire text",
            "Maximum Buffer Size": "10 MB",
        },
        ["failure"],
    )
    checkpoint_update_http_id = await proc(
        "org.apache.nifi.processors.standard.InvokeHTTP",
        "submit_checkpoint_update_query",
        {
            "HTTP URL": "${trino.statement.url}",
            "HTTP Method": "POST",
            "Request Header Attributes Pattern": "X-Trino-User|X-Trino-Source|X-Trino-Catalog|X-Trino-Schema|Accept",
            "Connection Timeout": "30 secs",
            "Socket Read Timeout": "60 secs",
            "HTTP/2 Disabled": "True",
        },
        ["Original", "Response", "Failure", "No Retry", "Retry"],
    )
    checkpoint_update_eval_id = await proc(
        "org.apache.nifi.processors.standard.EvaluateJsonPath",
        "extract_checkpoint_update_status",
        {
            "Destination": "flowfile-attribute",
            "Return Type": "auto-detect",
            "Path Not Found Behavior": "ignore",
            "Max String Length": "20 MB",
            "trino.nextUri": "$.nextUri",
        },
        ["failure", "unmatched"],
    )
    checkpoint_update_route_id = await proc(
        "org.apache.nifi.processors.standard.RouteOnAttribute",
        "route_checkpoint_update_state",
        {
            "Routing Strategy": "Route to Property name",
            "done": "${trino.nextUri:isEmpty()}",
            "needs_poll": "${trino.nextUri:isEmpty():not()}",
        },
        ["unmatched", "done"],
    )
    checkpoint_update_poll_id = await proc(
        "org.apache.nifi.processors.standard.InvokeHTTP",
        "poll_checkpoint_update_query",
        {
            "HTTP URL": "${trino.nextUri}",
            "HTTP Method": "GET",
            "Request Header Attributes Pattern": "X-Trino-User|X-Trino-Source|X-Trino-Catalog|X-Trino-Schema|Accept",
            "Connection Timeout": "30 secs",
            "Socket Read Timeout": "60 secs",
            "HTTP/2 Disabled": "True",
        },
        ["Original", "Failure", "No Retry", "Retry"],
    )

    ids = [
        trigger_id, headers_id,
        checkpoint_sql_id, checkpoint_http_id, checkpoint_eval_id,
        checkpoint_route_id, checkpoint_poll_id, snapshot_sql_id, snapshot_http_id, snapshot_eval_id,
        snapshot_route_id, snapshot_poll_id, rows_mode_route_id, bootstrap_payload_sql_id,
        incremental_payload_sql_id, payload_http_id, rows_eval_id,
        rows_route_id, rows_poll_id, split_id, publish_id,
        checkpoint_update_id, checkpoint_update_http_id, checkpoint_update_eval_id,
        checkpoint_update_route_id, checkpoint_update_poll_id,
    ]
    if column_mode != "all":
        ids.append(payload_content_id)
    if auto_create_checkpoint_table:
        ids.extend([
            checkpoint_create_sql_id,
            checkpoint_create_http_id,
            checkpoint_create_eval_id,
            checkpoint_create_route_id,
            checkpoint_create_poll_id,
        ])
    if column_mode == "all":
        ids.extend([
            rows_builder_http_id,
            rows_builder_eval_id,
            rows_builder_route_id,
            rows_builder_poll_id,
            generated_query_content_id,
            trino_row_extract_id,
            trino_row_content_id,
        ])
    if any(item is None for item in ids):
        return False

    row_query_connections = [
        (rows_mode_route_id, bootstrap_payload_sql_id, ["bootstrap"]),
        (rows_mode_route_id, incremental_payload_sql_id, ["incremental"]),
    ]
    if column_mode == "all":
        row_query_connections.extend([
            (bootstrap_payload_sql_id, rows_builder_http_id, ["success"]),
            (incremental_payload_sql_id, rows_builder_http_id, ["success"]),
            (rows_builder_http_id, rows_builder_eval_id, ["Response"]),
            (rows_builder_eval_id, rows_builder_route_id, ["matched"]),
            (rows_builder_route_id, rows_builder_poll_id, ["needs_poll"]),
            (rows_builder_poll_id, rows_builder_eval_id, ["Response"]),
            (rows_builder_route_id, generated_query_content_id, ["has_query"]),
            (generated_query_content_id, payload_http_id, ["success"]),
        ])
    else:
        row_query_connections.extend([
            (bootstrap_payload_sql_id, payload_http_id, ["success"]),
            (incremental_payload_sql_id, payload_http_id, ["success"]),
        ])

    row_payload_connections = [
        (payload_http_id, rows_eval_id, ["Response"]),
        (rows_eval_id, rows_route_id, ["matched"]),
        (rows_route_id, rows_poll_id, ["needs_poll"]),
        (rows_poll_id, rows_eval_id, ["Response"]),
    ]
    if column_mode == "all":
        row_payload_connections.extend([
            (rows_route_id, split_id, ["has_payload"]),
            (split_id, trino_row_extract_id, ["split"]),
            (trino_row_extract_id, trino_row_content_id, ["matched"]),
            (trino_row_content_id, publish_id, ["success"]),
            (split_id, checkpoint_update_id, ["original"]),
        ])
    else:
        row_payload_connections.extend([
            (rows_route_id, payload_content_id, ["has_payload"]),
            (payload_content_id, split_id, ["success"]),
            (split_id, publish_id, ["split"]),
            (split_id, checkpoint_update_id, ["original"]),
        ])

    connections = [
        (trigger_id, headers_id, ["success"]),
        (checkpoint_sql_id, checkpoint_http_id, ["success"]),
        (checkpoint_http_id, checkpoint_eval_id, ["Response"]),
        (checkpoint_eval_id, checkpoint_route_id, ["matched"]),
        (checkpoint_route_id, checkpoint_poll_id, ["needs_poll"]),
        (checkpoint_poll_id, checkpoint_eval_id, ["Response"]),
        (checkpoint_route_id, snapshot_sql_id, ["has_checkpoint", "no_checkpoint"]),
        (snapshot_sql_id, snapshot_http_id, ["success"]),
        (snapshot_http_id, snapshot_eval_id, ["Response"]),
        (snapshot_eval_id, snapshot_route_id, ["matched"]),
        (snapshot_route_id, snapshot_poll_id, ["needs_poll"]),
        (snapshot_poll_id, snapshot_eval_id, ["Response"]),
        (snapshot_route_id, rows_mode_route_id, ["has_snapshot"]),
        *row_query_connections,
        *row_payload_connections,
        (checkpoint_update_id, checkpoint_update_http_id, ["success"]),
        (checkpoint_update_http_id, checkpoint_update_eval_id, ["Response"]),
        (checkpoint_update_eval_id, checkpoint_update_route_id, ["matched"]),
        (checkpoint_update_route_id, checkpoint_update_poll_id, ["needs_poll"]),
        (checkpoint_update_poll_id, checkpoint_update_eval_id, ["Response"]),
    ]
    if checkpoint_create_sql_id and checkpoint_create_http_id:
        connections[1:1] = [
            (headers_id, checkpoint_create_sql_id, ["success"]),
            (checkpoint_create_sql_id, checkpoint_create_http_id, ["success"]),
            (checkpoint_create_http_id, checkpoint_create_eval_id, ["Response"]),
            (checkpoint_create_eval_id, checkpoint_create_route_id, ["matched"]),
            (checkpoint_create_route_id, checkpoint_create_poll_id, ["needs_poll"]),
            (checkpoint_create_poll_id, checkpoint_create_eval_id, ["Response"]),
            (checkpoint_create_route_id, checkpoint_sql_id, ["done"]),
        ]
    else:
        connections.insert(1, (headers_id, checkpoint_sql_id, ["success"]))
    for src_id, dst_id, rels in connections:
        conn_id = await _create_connection(nifi_url, trino_pg_id, src_id, dst_id, rels, **nargs)
        if not conn_id:
            return False
    return True


async def _build_postgres_flow(
    nifi_url: str, pg_id: str, source: dict,
    kafka_svc_id: Optional[str],
    avro_writer_id: Optional[str],
    kafka_topic: str,
    schedule_str: str,
    auth_type_nifi: str, username_nifi: Optional[str], password_nifi: Optional[str],
) -> bool:
    """Build PostgreSQL processor chain.

    DBCPConnectionPool (ctrl-svc) → QueryDatabaseTableRecord → PublishKafka
    """
    x = 400.0
    y_base = 200.0
    step = 200.0

    db_host = source.get("db_host") or "localhost"
    db_port = source.get("db_port") or 5432
    db_name = source.get("db_database") or "postgres"
    db_user = source.get("db_username") or ""
    db_pass = source.get("db_password") or ""
    ssl_mode = source.get("db_ssl_mode") or "disable"
    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}?sslmode={ssl_mode}"

    # ── DBCPConnectionPool controller service ─────────────────────────────
    dbcp_id, dbcp_rev = await _create_controller_service(
        nifi_url, pg_id,
        "org.apache.nifi.dbcp.DBCPConnectionPool",
        "PostgreSQLConnectionPool",
        properties={
            "Database Connection URL": jdbc_url,
            "Database Driver Class Name": "org.postgresql.Driver",
            "database-driver-locations": "",
            "Database User": db_user,
            "Password": db_pass,
            "Max Wait Time": "500 millis",
            "Max Total Connections": "8",
        },
        auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
    )
    if dbcp_id:
        ok = await _enable_controller_service(
            nifi_url, dbcp_id, dbcp_rev,
            auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
        )
        if ok:
            await _wait_cs_enabled(nifi_url, dbcp_id,
                                   auth_type=auth_type_nifi, username=username_nifi, password=password_nifi)

    # Table name derived from primary stream name or endpoint_path
    stream = _primary_stream(source)
    table_name = ""
    if stream:
        table_name = (stream.get("name") or stream.get("endpoint_path") or "").strip("/")
    if not table_name:
        table_name = source.get("name", "data_table").replace(" ", "_").lower()

    # ── 1. QueryDatabaseTableRecord ───────────────────────────────────────
    qdt_props: Dict[str, Any] = {
        "Database Type": "PostgreSQL",
        "Table Name": table_name,
        "Fetch Size": "1000",
    }
    if dbcp_id:
        qdt_props["Database Connection Pooling Service"] = dbcp_id
    if avro_writer_id:
        qdt_props["Return Type"] = "Use Record Writer"
        qdt_props["Record Writer"] = avro_writer_id

    qdt_id = await _create_processor(
        nifi_url, pg_id,
        "org.apache.nifi.processors.standard.QueryDatabaseTableRecord",
        "QueryPostgresTable",
        properties=qdt_props,
        auto_terminate=[],
        scheduling_period=schedule_str,
        auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
        x=x, y=y_base,
    )
    if not qdt_id:
        return False

    # ── 2. PublishKafka ───────────────────────────────────────────────────
    pub_props: Dict[str, Any] = {
        "Topic Name": kafka_topic,
        "acks": "all",
        "compression.type": "gzip",
        "Failure Strategy": "Route to Failure",
        "Transactions Enabled": "false",
        "Publish Strategy": "USE_VALUE",
    }
    if kafka_svc_id:
        pub_props["Kafka Connection Service"] = kafka_svc_id

    pub_id = await _create_processor(
        nifi_url, pg_id,
        "org.apache.nifi.kafka.processors.PublishKafka",
        "PublishToKafka",
        properties=pub_props,
        auto_terminate=["success", "failure"],
        scheduling_period="0 sec",
        auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
        x=x, y=y_base + step,
    )
    if not pub_id:
        return False

    await _create_connection(nifi_url, pg_id, qdt_id, pub_id, ["success"],
                             auth_type=auth_type_nifi, username=username_nifi, password=password_nifi)
    return True


async def _build_mongodb_flow(
    nifi_url: str, pg_id: str, source: dict,
    kafka_svc_id: Optional[str],
    json_reader_id: Optional[str],
    json_writer_id: Optional[str],
    schema_reg_id: Optional[str],
    schema_name: Optional[str],
    avro_writer_id: Optional[str],
    avro_transform_writer_id: Optional[str],
    kafka_topic: str,
    schedule_str: str,
    auth_type_nifi: str, username_nifi: Optional[str], password_nifi: Optional[str],
    mongodb_svc_id: Optional[str] = None,
) -> bool:
    """Build MongoDB processor chain with parent-child stream support.

    Streams are ordered parent-before-child using fan_out.parent_stream_id.
    Child streams can reference parent-extracted attributes in mongo_query using
    NiFi Expression Language (example: {"asset_id":"${asset_id}"}).
    """
    db_host = source.get("db_host") or "localhost"
    db_port = source.get("db_port") or 27017
    db_name = source.get("db_database") or "admin"
    db_user = source.get("db_username")
    db_pass = source.get("db_password")
    mongo_schema_name = (
        schema_name
        or (source.get("kafka_output") or {}).get("schema_artifact_id")
        or ""
    )
    conn_str = source.get("db_connection_uri") or f"mongodb://{db_host}:{db_port}"
    nifi_refs = source.get("nifi_service_refs") if isinstance(source.get("nifi_service_refs"), dict) else {}
    mongo_svc_id = mongodb_svc_id or (nifi_refs or {}).get("mongodb")

    # ── MongoDBControllerService ──────────────────────────────────────────
    if not mongo_svc_id:
        mongo_props: Dict[str, Any] = {
            "Mongo URI": conn_str,
            "Write Concern": "ACKNOWLEDGED",
        }
        if db_user:
            mongo_props["Database User"] = db_user
        if db_pass:
            mongo_props["Password"] = db_pass

        mongo_svc_id, mongo_svc_rev = await _create_controller_service(
            nifi_url, pg_id,
            "org.apache.nifi.mongodb.MongoDBControllerService",
            "MongoDBConnectionService",
            properties=mongo_props,
            auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
        )
        if mongo_svc_id:
            ok = await _enable_controller_service(
                nifi_url, mongo_svc_id, mongo_svc_rev,
                auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
            )
            if ok:
                await _wait_cs_enabled(
                    nifi_url, mongo_svc_id,
                    auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
                )

    # In inference mode (no schema registry), GetMongoRecord cannot reliably emit record schema.
    # Use GetMongo (JSON output) and publish those JSON documents directly.
    use_getmongo_json = schema_reg_id is None

    # Stream preparation: support multi-stream Mongo flows (parent -> child).
    streams = list(source.get("streams") or [])
    if not streams:
        fallback_stream = _primary_stream(source)
        if fallback_stream:
            streams = [fallback_stream]
    if not streams:
        logger.error("Mongo flow has no streams to build.")
        return False

    ordered = _topological_order(streams)
    stream_results: Dict[str, Dict[str, Any]] = {}
    streams_by_id: Dict[str, dict] = {}
    for idx, stream in enumerate(ordered):
        sid = stream.get("id") or stream.get("name") or f"stream_{idx}"
        streams_by_id[sid] = stream

    # GetMongoRecord + JSON output needs a writer that does not depend on
    # "inherit-record-schema" because Mongo records may not carry an embedded
    # incoming schema at this stage.
    mongo_json_writer_id: Optional[str] = None
    if not use_getmongo_json:
        mongo_json_writer_props: Dict[str, Any]
        if schema_reg_id and mongo_schema_name:
            mongo_json_writer_props = {
                "Schema Access Strategy": "schema-name",
                "Schema Registry": schema_reg_id,
                "Schema Name": mongo_schema_name,
                "Schema Write Strategy": "no-schema",
                "Output Grouping": "output-array",
                "Pretty Print JSON": "false",
                "Suppress Null Values": "suppress-missing",
                "Compression Format": "none",
            }
        else:
            mongo_json_writer_props = {
                "Schema Access Strategy": "inherit-record-schema",
                "Output Grouping": "output-array",
                "Pretty Print JSON": "false",
                "Suppress Null Values": "suppress-missing",
                "Compression Format": "none",
            }

        mongo_json_writer_id, mongo_json_writer_rev = await _create_controller_service(
            nifi_url, pg_id,
            "org.apache.nifi.json.JsonRecordSetWriter",
            "MongoJsonRecordSetWriter",
            properties=mongo_json_writer_props,
            auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
        )
        if mongo_json_writer_id:
            ok = await _enable_controller_service(
                nifi_url, mongo_json_writer_id, mongo_json_writer_rev,
                auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
            )
            if ok:
                enabled = await _wait_cs_enabled(
                    nifi_url, mongo_json_writer_id,
                    auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
                    timeout=90,
                )
                if not enabled:
                    logger.warning("MongoJsonRecordSetWriter did not reach ENABLED in time; using fallback JsonTransformWriter.")
                    mongo_json_writer_id = None
            else:
                logger.warning("Could not initiate enable for MongoJsonRecordSetWriter; using fallback JsonTransformWriter.")
                mongo_json_writer_id = None

    get_json_writer_id = mongo_json_writer_id or json_writer_id
    step_y = 170.0
    base_x = 400.0
    col_gap = 430.0
    base_y = 200.0

    for idx, stream in enumerate(ordered):
        sid = stream.get("id") or stream.get("name") or f"stream_{idx}"
        sname = (stream.get("name") or sid).replace(" ", "_").lower()
        col_x = base_x + idx * col_gap

        fan = stream.get("fan_out") or {}
        parent_id = fan.get("parent_stream_id") if fan.get("enabled") else None
        parent_result = stream_results.get(parent_id) if parent_id else None
        has_parent = bool(parent_result and parent_result.get("tail_id"))

        # Collection name
        collection = (stream.get("endpoint_path") or stream.get("name") or "").strip("/")
        if not collection:
            collection = source.get("name", "collection").replace(" ", "_").lower()

        # Stream-level query controls
        stream_query = stream.get("mongo_query")
        if stream_query is None:
            stream_query = source.get("db_query")
        stream_query = _compile_mongo_query_template_for_nifi(stream_query)

        stream_projection_raw = stream.get("mongo_projection")
        if stream_projection_raw is None:
            stream_projection_raw = source.get("db_projection")
        stream_projection = (
            _compile_mongo_query_template_for_nifi(stream_projection_raw)
            if stream_projection_raw not in (None, "")
            else None
        )

        stream_sort_raw = stream.get("mongo_sort")
        if stream_sort_raw is None:
            stream_sort_raw = source.get("db_sort")
        stream_sort = (
            _compile_mongo_query_template_for_nifi(stream_sort_raw)
            if stream_sort_raw not in (None, "")
            else None
        )
        stream_limit = stream.get("mongo_limit")
        if stream_limit is None:
            stream_limit = source.get("db_limit")
        try:
            stream_limit_int = int(stream_limit) if stream_limit is not None else None
        except Exception:
            stream_limit_int = None

        split_setting = stream.get("split_response_records")
        split_records = True if split_setting is None else bool(split_setting)
        effective_split_records = split_records and not use_getmongo_json

        extraction_rules = stream.get("extraction_rules") or []
        transformation_rules = stream.get("transformation_rules") or []
        routing_rules = stream.get("routing_rules") or []
        add_set_rules: List[dict] = []
        remove_rules: List[dict] = []
        for rule in transformation_rules:
            action = (rule.get("type") or "").lower()
            if action in ("add_field", "set_from_attribute"):
                add_set_rules.append(rule)
            elif action == "remove_field":
                remove_rules.append(rule)

        needs_json_post_process = bool(extraction_rules or routing_rules or add_set_rules or remove_rules)

        # Entry source:
        # - root streams: scheduled trigger
        # - child streams: parent tail (attributes available for ${...} query templates)
        entry_id: Optional[str] = None
        entry_rel = "success"
        if has_parent:
            entry_id = parent_result["tail_id"]
            entry_rel = parent_result.get("tail_relationship", "success")
        else:
            trigger_id = await _create_processor(
                nifi_url, pg_id,
                "org.apache.nifi.processors.standard.GenerateFlowFile",
                f"{sname}__trigger",
                properties={"File Size": "0 B", "Batch Size": "1", "Data Format": "Text", "Custom Text": "{}"},
                auto_terminate=[],
                scheduling_period=schedule_str,
                auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
                x=col_x, y=base_y,
            )
            if not trigger_id:
                return False
            entry_id = trigger_id

        get_props: Dict[str, Any] = {
            "Mongo Database Name": db_name,
            "Mongo Collection Name": collection,
            "Query": stream_query,
            "Batch Size": "100",
        }
        if not use_getmongo_json:
            get_props["Schema Name"] = mongo_schema_name
        if stream_projection:
            get_props["Projection"] = stream_projection
        if stream_sort:
            get_props["Sort"] = stream_sort
        if isinstance(stream_limit_int, int) and stream_limit_int > 0:
            get_props["Limit"] = str(stream_limit_int)
        if mongo_svc_id:
            get_props["Client Service"] = mongo_svc_id

        get_writes_json = False
        if use_getmongo_json:
            get_props["Results Per FlowFile"] = "1"
            get_props["JSON Type"] = "Standard"
            get_props["Pretty Print Results JSON"] = "false"
            get_writes_json = True
        else:
            if effective_split_records or needs_json_post_process:
                if not get_json_writer_id:
                    logger.error(f"Mongo stream {sname}: JsonRecordSetWriter required for split/transform/extract path.")
                    return False
                get_props["Record Writer"] = get_json_writer_id
                get_writes_json = True
            elif avro_writer_id:
                get_props["Record Writer"] = avro_writer_id

        get_proc_type = (
            "org.apache.nifi.processors.mongodb.GetMongo"
            if use_getmongo_json
            else "org.apache.nifi.processors.mongodb.GetMongoRecord"
        )
        get_proc_name = f"{sname}__get_mongo_docs" if use_getmongo_json else f"{sname}__get_mongo_records"
        get_id = await _create_processor(
            nifi_url, pg_id,
            get_proc_type,
            get_proc_name,
            properties=get_props,
            auto_terminate=["original", "failure"],
            scheduling_period="0 sec",
            auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
            x=col_x, y=base_y + step_y,
        )
        if not get_id:
            return False

        await _create_connection(
            nifi_url, pg_id, entry_id, get_id, [entry_rel],
            auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
        )

        split_id = None
        split_writes_json = False
        if effective_split_records:
            if not json_reader_id:
                logger.error(f"Mongo stream {sname}: split_response_records requires JsonReader.")
                return False
            split_props: Dict[str, Any] = {
                "Records Per Split": "1",
                "Record Reader": json_reader_id,
            }
            if needs_json_post_process:
                if not json_writer_id:
                    logger.error(f"Mongo stream {sname}: split + transform/extract path requires JsonRecordSetWriter.")
                    return False
                split_props["Record Writer"] = json_writer_id
                split_writes_json = True
            elif avro_writer_id:
                split_props["Record Writer"] = avro_writer_id

            split_id = await _create_processor(
                nifi_url, pg_id,
                "org.apache.nifi.processors.standard.SplitRecord",
                f"{sname}__split",
                properties=split_props,
                auto_terminate=["original", "failure"],
                scheduling_period="0 sec",
                auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
                x=col_x, y=base_y + step_y * 2,
            )
            if not split_id:
                return False
            await _create_connection(
                nifi_url, pg_id, get_id, split_id, ["success"],
                auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
            )

        record_src_id = split_id if (effective_split_records and split_id) else get_id
        record_src_rel = "splits" if (effective_split_records and split_id) else "success"
        record_src_rels: List[str] = [record_src_rel]
        record_is_json = split_writes_json if (effective_split_records and split_id) else get_writes_json
        cy = base_y + step_y * 3

        if extraction_rules:
            if not record_is_json:
                logger.error(f"Mongo stream {sname}: extraction_rules require JSON record content before EvaluateJsonPath.")
                return False
            extract_props: Dict[str, Any] = {
                "Destination": "flowfile-attribute",
                "Max String Length": "20 MB",
                "Return Type": "auto-detect",
                "Null Value Representation": "empty string",
                "Path Not Found Behavior": "ignore",
            }
            for rule in extraction_rules:
                attr = rule.get("attribute_name")
                path = rule.get("path")
                if attr and path:
                    extract_props[attr] = path
            extract_id = await _create_processor(
                nifi_url, pg_id,
                "org.apache.nifi.processors.standard.EvaluateJsonPath",
                f"{sname}__extract",
                properties=extract_props,
                auto_terminate=["failure", "unmatched"],
                scheduling_period="0 sec",
                auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
                x=col_x, y=cy,
            )
            if not extract_id:
                return False
            await _create_connection(
                nifi_url, pg_id, record_src_id, extract_id, record_src_rels,
                auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
            )
            record_src_id = extract_id
            record_src_rel = "matched"
            record_src_rels = ["matched"]
            cy += step_y

        default_props = _build_default_attribute_props(extraction_rules)
        if default_props:
            defaults_id = await _create_processor(
                nifi_url, pg_id,
                "org.apache.nifi.processors.attributes.UpdateAttribute",
                f"{sname}__apply_defaults",
                properties=default_props,
                auto_terminate=["failure"],
                scheduling_period="0 sec",
                auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
                x=col_x, y=cy,
            )
            if not defaults_id:
                return False
            await _create_connection(
                nifi_url, pg_id, record_src_id, defaults_id, record_src_rels,
                auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
            )
            record_src_id = defaults_id
            record_src_rel = "success"
            record_src_rels = ["success"]
            cy += step_y

        pk_fields = stream.get("primary_key_fields") or []
        kafka_key_attr = None
        if pk_fields and any(r.get("attribute_name") == pk_fields[0] for r in extraction_rules):
            kafka_key_attr = pk_fields[0]
        route_res = await _apply_stream_routing(
            nifi_url=nifi_url,
            pg_id=pg_id,
            stream_name=sname,
            routing_rules=routing_rules,
            kafka_topic=kafka_topic,
            kafka_svc_id=kafka_svc_id,
            kafka_key_attr=kafka_key_attr,
            input_id=record_src_id,
            input_rels=record_src_rels,
            x=col_x,
            y=cy,
            step=step_y,
            scheduling_period="0 sec",
            auth_type=auth_type_nifi,
            username=username_nifi,
            password=password_nifi,
        )
        record_src_id = route_res["tail_id"]
        record_src_rels = route_res["tail_rels"]
        record_src_rel = record_src_rels[0]
        cy = route_res["y"]

        if remove_rules:
            if not (record_is_json and json_reader_id):
                logger.error(f"Mongo stream {sname}: remove_field rules require JSON record content and JsonReader.")
                return False
            remove_props: Dict[str, Any] = {"Record Reader": json_reader_id}
            remove_writer_id: Optional[str] = None
            if add_set_rules:
                if not json_writer_id:
                    logger.error(f"Mongo stream {sname}: remove_field + add/set path requires JsonRecordSetWriter.")
                    return False
                remove_writer_id = json_writer_id
            else:
                remove_writer_id = avro_transform_writer_id or avro_writer_id
                if not remove_writer_id:
                    logger.error(f"Mongo stream {sname}: remove_field rules require an Avro writer controller service.")
                    return False
            remove_props["Record Writer"] = remove_writer_id
            remove_index = 1
            for rule in remove_rules:
                field = (rule.get("field_name") or "").strip()
                record_path = _to_record_path(field)
                if not record_path:
                    continue
                remove_props[f"field_to_remove_{remove_index}"] = record_path
                remove_index += 1
            if remove_index > 1:
                remove_id = await _create_processor(
                    nifi_url, pg_id,
                    "org.apache.nifi.processors.standard.RemoveRecordField",
                    f"{sname}__remove_fields",
                    properties=remove_props,
                    auto_terminate=["failure"],
                    scheduling_period="0 sec",
                    auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
                    x=col_x, y=cy,
                )
                if not remove_id:
                    return False
                await _create_connection(
                    nifi_url, pg_id, record_src_id, remove_id, record_src_rels,
                    auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
                )
                record_src_id = remove_id
                record_src_rel = "success"
                record_src_rels = ["success"]
                record_is_json = (remove_writer_id == json_writer_id)
                cy += step_y

        if add_set_rules:
            if not (record_is_json and json_reader_id):
                logger.error(f"Mongo stream {sname}: add/set transformations require JSON record content and JsonReader.")
                return False
            transform_writer_id = avro_transform_writer_id if remove_rules else avro_writer_id
            if not transform_writer_id:
                logger.error(f"Mongo stream {sname}: add/set transformations require an Avro writer controller service.")
                return False
            ur_props: Dict[str, Any] = {
                "Replacement Value Strategy": "literal-value",
                "Record Reader": json_reader_id,
                "Record Writer": transform_writer_id,
            }
            for rule in add_set_rules:
                action = (rule.get("type") or "").lower()
                field = (rule.get("field_name") or "").strip()
                record_path = _to_record_path(field)
                if not record_path:
                    continue
                if action == "add_field":
                    ur_props[record_path] = rule.get("value") or ""
                elif action == "set_from_attribute":
                    src_attr = (rule.get("source_attribute") or "").strip()
                    if src_attr:
                        ur_props[record_path] = "${" + src_attr + "}"

            update_id = await _create_processor(
                nifi_url, pg_id,
                "org.apache.nifi.processors.standard.UpdateRecord",
                f"{sname}__apply_transformations",
                properties=ur_props,
                auto_terminate=["failure"],
                scheduling_period="0 sec",
                auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
                x=col_x, y=cy,
            )
            if not update_id:
                return False
            await _create_connection(
                nifi_url, pg_id, record_src_id, update_id, record_src_rels,
                auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
            )
            record_src_id = update_id
            record_src_rel = "success"
            record_src_rels = ["success"]
            record_is_json = False
            cy += step_y

        # Parent streams keep their processed record tail available to children.
        # If also marked as an entity, PublishKafka is a side branch from that same tail.
        is_parent = any(
            (s.get("fan_out") or {}).get("enabled")
            and (s.get("fan_out") or {}).get("parent_stream_id") == sid
            for s in ordered
        )
        has_explicit_entities = any(bool(s.get("entity")) for s in ordered)
        entity_cfg = stream.get("entity") or {}
        is_explicit_entity_stream = isinstance(entity_cfg, dict) and bool(entity_cfg)
        should_publish = is_explicit_entity_stream or (not has_explicit_entities and not is_parent)

        if record_is_json and should_publish:
            if not (json_reader_id and avro_writer_id):
                logger.error(f"Mongo stream {sname}: JsonReader + AvroWriter required to convert JSON records before publish.")
                return False
            convert_id = await _create_processor(
                nifi_url, pg_id,
                "org.apache.nifi.processors.standard.ConvertRecord",
                f"{sname}__convert_avro",
                properties={
                    "Record Reader": json_reader_id,
                    "Record Writer": avro_writer_id,
                },
                auto_terminate=["failure"],
                scheduling_period="0 sec",
                auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
                x=col_x, y=cy,
            )
            if not convert_id:
                return False
            await _create_connection(
                nifi_url, pg_id, record_src_id, convert_id, record_src_rels,
                auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
            )
            record_src_id = convert_id
            record_src_rel = "success"
            record_src_rels = ["success"]
            record_is_json = False
            cy += step_y

        publish_id: Optional[str] = None
        if should_publish:
            effective_kafka_topic = kafka_topic
            entity_kafka = entity_cfg.get("kafka") if isinstance(entity_cfg, dict) else {}
            if isinstance(entity_kafka, dict) and entity_kafka.get("topic"):
                effective_kafka_topic = entity_kafka["topic"]

            pub_props: Dict[str, Any] = {
                "Topic Name": effective_kafka_topic,
                "acks": "all",
                "compression.type": "gzip",
                "Failure Strategy": "Route to Failure",
                "Transactions Enabled": "false",
                "Publish Strategy": "USE_VALUE",
            }
            if kafka_svc_id:
                pub_props["Kafka Connection Service"] = kafka_svc_id
            if kafka_key_attr:
                pub_props["Kafka Key"] = "${" + kafka_key_attr + "}"

            publish_id = await _create_processor(
                nifi_url, pg_id,
                "org.apache.nifi.kafka.processors.PublishKafka",
                f"{sname}__publish",
                properties=pub_props,
                auto_terminate=["success", "failure"],
                scheduling_period="0 sec",
                auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
                x=col_x, y=cy,
            )
            if not publish_id:
                return False
            await _create_connection(
                nifi_url, pg_id, record_src_id, publish_id, record_src_rels,
                auth_type=auth_type_nifi, username=username_nifi, password=password_nifi,
            )

        stream_results[sid] = {
            "tail_id": record_src_id if is_parent or not publish_id else publish_id,
            "tail_relationship": record_src_rel if is_parent or not publish_id else "success",
            "is_parent": is_parent,
            "publish_id": publish_id,
        }

    return True


# ---------------------------------------------------------------------------
# Main entry-point
# ---------------------------------------------------------------------------

async def generate_and_deploy_flow(
    source: dict,
    flow_id: str,
    nifi_conn: dict,
    kafka_conn: Optional[dict] = None,
    apicurio_conn: Optional[dict] = None,
    flow: Optional[dict] = None,
    global_service_ids: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Translate a Source config into a live NiFi process group.

    Steps:
    1. Get NiFi root process group
    2. If re-deploying (source or flow already has nifi_process_group_id),
       stop + delete the old process group
    3. Create new process group
    4. Create + enable controller services
    5. Create processors for the source type
    6. Wire connections
    7. Return {ok: True, process_group_id: str}
    """
    nifi_url = (nifi_conn.get("endpoint") or "").rstrip("/")
    auth_type = nifi_conn.get("auth_type", "NONE")
    username = nifi_conn.get("username")
    password = nifi_conn.get("password")
    inherited_services = dict(global_service_ids or {})

    flow_name = source.get("name") or f"flow-{flow_id[:8]}"

    # Pre-warm JWT token cache — avoids per-call re-authentication latency
    if auth_type == "BASIC" and username and password:
        await _get_nifi_token(nifi_url, username, password)
        logger.info("NiFi JWT token warmed in cache for deploy session")

    # ── Resolve Kafka / Apicurio connection details ───────────────────────
    kafka_bootstrap = ""
    if kafka_conn:
        kafka_bootstrap = kafka_conn.get("endpoint") or ""

    apicurio_base_url = ""
    if apicurio_conn:
        apicurio_base_url = apicurio_conn.get("endpoint") or ""

    # ── Resolve Kafka topic ───────────────────────────────────────────────
    kafka_topic = ""
    if flow:
        kafka_topic = flow.get("kafka_topic") or ""
    if not kafka_topic:
        kafka_output = source.get("kafka_output") or {}
        kafka_topic = kafka_output.get("topic") or flow_name.lower().replace(" ", "-")

    # ── Resolve schema name (if linked) ───────────────────────────────────
    schema_name: Optional[str] = None
    if flow:
        schema_name = flow.get("schema_artifact_id") or None
    if not schema_name:
        kafka_output = source.get("kafka_output") or {}
        schema_name = kafka_output.get("schema_artifact_id") or None

    # ── 1. Get root process group ID ──────────────────────────────────────
    root_pg_id = await get_nifi_root_process_group_id(
        nifi_url, auth_type=auth_type, username=username, password=password
    )
    if not root_pg_id:
        return {"ok": False, "error": "Could not retrieve NiFi root process group ID. Check NiFi connection."}

    # ── 2. Delete old process group on re-deploy ──────────────────────────
    old_pg_id = source.get("nifi_process_group_id")
    if not old_pg_id and flow:
        old_pg_id = flow.get("nifi_process_group_id")
    if old_pg_id:
        logger.info(f"Re-deploy: deleting old process group {old_pg_id}")
        await _delete_old_pg(nifi_url, old_pg_id,
                             auth_type=auth_type, username=username, password=password)

    # ── 3. Create new process group ───────────────────────────────────────
    pg_id = await _create_process_group(
        nifi_url, root_pg_id, flow_name,
        auth_type=auth_type, username=username, password=password,
    )
    if not pg_id:
        return {"ok": False, "error": "Failed to create NiFi process group. Check NiFi permissions."}

    # ── 4. Create + enable standard controller services ───────────────────
    services = await _create_standard_controller_services(
        nifi_url, pg_id,
        kafka_bootstrap=kafka_bootstrap,
        apicurio_base_url=apicurio_base_url,
        schema_name=schema_name,
        auth_type=auth_type, username=username, password=password,
        global_service_ids=inherited_services,
    )

    kafka_svc_id = services["kafka_svc"]
    json_reader_id = services["json_reader"]
    json_writer_id = services.get("json_writer")
    avro_transform_writer_id = services.get("avro_transform_writer")
    avro_writer_id = services["avro_writer"]
    schema_reg_id = services["schema_reg"]
    kafka_schema_reg_id = services.get("kafka_schema_reg")
    schema_ref_writer_id = services.get("schema_ref_writer")

    schedule_str = _schedule_period(source)
    source["__flow_id"] = flow_id

    # ── 5 + 6. Build source-type-specific processor chain ─────────────────
    source_type_raw = (source.get("type") or "").strip()
    source_type_key = source_type_raw.upper().replace(" ", "_")

    # ── 4b. Create XMLReader if any REST stream uses XML response format ───
    xml_reader_id: Optional[str] = None
    streams = source.get("streams") or []
    has_xml_streams = any(
        (s.get("response_format") or "json").lower() == "xml"
        for s in streams
    )
    if has_xml_streams and source_type_key in ("REST_API", "REST"):
        xml_reader_id, xml_reader_rev = await _create_xml_reader_service(
            nifi_url, pg_id,
            schema_reg_id=schema_reg_id,
            schema_name=schema_name,
            auth_type=auth_type, username=username, password=password,
        )
        xml_reader_enabled = False
        if xml_reader_id:
            ok_cs = await _enable_controller_service(
                nifi_url, xml_reader_id, xml_reader_rev,
                auth_type=auth_type, username=username, password=password,
            )
            if ok_cs:
                xml_reader_enabled = await _wait_cs_enabled(
                    nifi_url, xml_reader_id,
                    auth_type=auth_type, username=username, password=password,
                    timeout=90,
                )
            else:
                logger.warning(f"Could not initiate enable for XMLReader ({xml_reader_id})")
        else:
            logger.warning("Failed to create XMLReader CS")

        if not xml_reader_enabled:
            error = (
                "XMLReader controller service could not be enabled; refusing to create REST XML flow "
                "because XML UpdateRecord processors would reference a disabled Record Reader."
            )
            logger.error(error)
            await _delete_old_pg(
                nifi_url, pg_id,
                auth_type=auth_type, username=username, password=password,
            )
            return {"ok": False, "error": error}

    # ── 5 + 6. Build source-type-specific processor chain ─────────────────
    ok = False
    if source_type_key in ("REST_API", "REST"):
        ok = await _build_rest_api_flow(
            nifi_url, pg_id, source,
            kafka_svc_id=kafka_svc_id,
            json_reader_id=json_reader_id,
            schema_reg_id=schema_reg_id,
            xml_reader_id=xml_reader_id,
            avro_writer_id=avro_writer_id,
            kafka_schema_reg_id=kafka_schema_reg_id,
            schema_ref_writer_id=schema_ref_writer_id,
            json_writer_id=json_writer_id,
            avro_transform_writer_id=avro_transform_writer_id,
            kafka_topic=kafka_topic,
            schedule_str=schedule_str,
            auth_type_nifi=auth_type, username_nifi=username, password_nifi=password,
        )
    elif source_type_key == "SMB":
        ok = await _build_smb_flow(
            nifi_url, pg_id, source,
            kafka_svc_id=kafka_svc_id,
            json_reader_id=json_reader_id,
            json_writer_id=json_writer_id,
            avro_writer_id=avro_writer_id,
            schema_reg_id=schema_reg_id,
            schema_name=schema_name,
            kafka_topic=kafka_topic,
            schedule_str=schedule_str,
            auth_type_nifi=auth_type, username_nifi=username, password_nifi=password,
        )
    elif source_type_key == "POSTGRESQL":
        ok = await _build_postgres_flow(
            nifi_url, pg_id, source,
            kafka_svc_id=kafka_svc_id,
            avro_writer_id=avro_writer_id,
            kafka_topic=kafka_topic,
            schedule_str=schedule_str,
            auth_type_nifi=auth_type, username_nifi=username, password_nifi=password,
        )
    elif source_type_key == "MONGODB":
        ok = await _build_mongodb_flow(
            nifi_url, pg_id, source,
            kafka_svc_id=kafka_svc_id,
            json_reader_id=json_reader_id,
            json_writer_id=json_writer_id,
            schema_reg_id=schema_reg_id,
            schema_name=schema_name,
            avro_writer_id=avro_writer_id,
            avro_transform_writer_id=avro_transform_writer_id,
            kafka_topic=kafka_topic,
            schedule_str=schedule_str,
            auth_type_nifi=auth_type, username_nifi=username, password_nifi=password,
            mongodb_svc_id=inherited_services.get("mongodb"),
        )
    elif source_type_key == "TRINO":
        ok = await _build_trino_iceberg_flow(
            nifi_url, pg_id, source,
            kafka_svc_id=kafka_svc_id,
            kafka_topic=kafka_topic,
            schedule_str=schedule_str,
            auth_type_nifi=auth_type, username_nifi=username, password_nifi=password,
        )
    else:
        # Unknown type — create a minimal stub flow (GenerateFlowFile only) so the PG exists
        logger.warning(f"Unsupported source type '{source_type_raw}' — creating placeholder process group")
        gff_id = await _create_processor(
            nifi_url, pg_id,
            "org.apache.nifi.processors.standard.GenerateFlowFile",
            "Placeholder",
            properties={"File Size": "0 B", "Custom Text": f"unsupported source type: {source_type_raw}"},
            auto_terminate=["success"],
            scheduling_period="1 hour",
            auth_type=auth_type, username=username, password=password,
        )
        ok = gff_id is not None

    if not ok:
        # Attempt cleanup — delete the partially-built PG
        r = await nifi_api_request(
            nifi_url, "GET", f"/nifi-api/process-groups/{pg_id}",
            auth_type=auth_type, username=username, password=password,
        )
        if r.get("ok"):
            version = (r["data"].get("revision") or {}).get("version", 0)
            await nifi_api_request(
                nifi_url, "DELETE", f"/nifi-api/process-groups/{pg_id}",
                auth_type=auth_type, username=username, password=password,
                params={"version": str(version), "disconnectedNodeAcknowledged": "false"},
            )
        return {"ok": False, "error": source.get("__nifi_build_error") or f"Failed to build processor chain for source type '{source_type_raw}'"}

    logger.info(f"Successfully deployed flow '{flow_name}' as NiFi process group {pg_id}")
    return {"ok": True, "process_group_id": pg_id}


# ---------------------------------------------------------------------------
# Inference mode helpers
# ---------------------------------------------------------------------------

async def _create_inference_controller_services(
    nifi_url: str,
    pg_id: str,
    kafka_bootstrap: str,
    kafka_security_protocol: Optional[str],
    auth_type: str,
    username: Optional[str],
    password: Optional[str],
    global_service_ids: Optional[Dict[str, str]] = None,
    include_xml_reader: bool = False,
) -> Dict[str, Any]:
    """
    Create controller services for a schema inference NiFi flow.
    - No schema registry (Avro not enforced)
    - Uses JsonRecordSetWriter (plain JSON output) instead of AvroRecordSetWriter
    - Uses JsonTreeReader with infer-schema (no registry)
    """
    # 1. Kafka3ConnectionService
    # Prefer explicit Kafka connection values from backend DB for inference flows.
    # Fall back to NiFi parameter context only if bootstrap is not provided.
    kafka_bootstrap_value = (kafka_bootstrap or "").strip() or "#{KAFKA_BOOTSTRAP_SERVERS}"
    kafka_protocol_value = (kafka_security_protocol or "").strip() or "#{KAFKA_SECURITY_PROTOCOL}"

    inherited = dict(global_service_ids or {})
    kafka_svc_id = inherited.get("kafka") or None
    kafka_rev = 0
    if not kafka_svc_id:
        kafka_svc_id, kafka_rev = await _create_controller_service(
            nifi_url, pg_id,
            "org.apache.nifi.kafka.service.Kafka3ConnectionService",
            "KafkaConnectionService",
            properties={
                "bootstrap.servers": kafka_bootstrap_value,
                "security.protocol": kafka_protocol_value,
            },
            auth_type=auth_type, username=username, password=password,
        )

    # 2. JsonTreeReader (infer schema from content — no registry)
    json_reader_id, json_reader_rev = await _create_controller_service(
        nifi_url, pg_id,
        "org.apache.nifi.json.JsonTreeReader",
        "JsonReaderInference",
        properties={
            "Schema Access Strategy": "infer-schema",
            "Starting Field Strategy": "ROOT_NODE",
            "Max String Length": "20 MB",
        },
        auth_type=auth_type, username=username, password=password,
    )

    # 3. JsonRecordSetWriter (output plain JSON, no schema enforcement)
    json_writer_id, json_writer_rev = await _create_controller_service(
        nifi_url, pg_id,
        "org.apache.nifi.json.JsonRecordSetWriter",
        "JsonWriterInference",
        properties={
            "Schema Access Strategy": "inherit-record-schema",
            "Pretty Print JSON": "false",
            "Suppress Null Values": "never-suppress",
            "Output Grouping": "output-oneline",
        },
        auth_type=auth_type, username=username, password=password,
    )

    xml_reader_id: Optional[str] = None
    xml_reader_rev = 0
    if include_xml_reader:
        xml_reader_id, xml_reader_rev = await _create_xml_reader_service(
            nifi_url, pg_id,
            schema_reg_id=None,
            schema_name=None,
            auth_type=auth_type, username=username, password=password,
        )

    enable_list = [
        (None if inherited.get("kafka") else kafka_svc_id, kafka_rev, "KafkaConnectionService"),
        (json_reader_id, json_reader_rev, "JsonReaderInference"),
        (json_writer_id, json_writer_rev, "JsonWriterInference"),
        (xml_reader_id, xml_reader_rev, "XMLReaderInference"),
    ]
    for cs_id, rev, label in enable_list:
        if cs_id:
            ok_cs = await _enable_controller_service(
                nifi_url, cs_id, rev,
                auth_type=auth_type, username=username, password=password,
            )
            if ok_cs:
                await _wait_cs_enabled(
                    nifi_url, cs_id,
                    auth_type=auth_type, username=username, password=password,
                )
            else:
                logger.warning(f"Could not enable {label} ({cs_id}) for inference flow")

    return {
        "kafka_svc": kafka_svc_id,
        "schema_reg": None,
        "schema_ref_writer": None,
        "json_reader": json_reader_id,
        "json_writer": json_writer_id,
        "xml_reader": xml_reader_id,
        # Pass json_writer as avro_writer so existing flow builders use it transparently
        "avro_writer": json_writer_id,
    }


async def generate_and_deploy_inference_flow(
    source: dict,
    flow_id: str,
    inference_topic: str,
    nifi_conn: dict,
    kafka_conn: Optional[dict] = None,
    target_stream_id: Optional[str] = None,
    global_service_ids: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Deploy a temporary schema-inference NiFi process group.

    This flow is identical to the production flow in terms of source fetching,
    extraction, and transformation — but it writes plain JSON records to
    ``inference_topic`` instead of Avro records to the production topic.

    The intent is that the backend reads the JSON messages from the inference
    topic and infers an Avro schema from them.
    """
    nifi_url = (nifi_conn.get("endpoint") or "").rstrip("/")
    auth_type = nifi_conn.get("auth_type", "NONE")
    username = nifi_conn.get("username")
    password = nifi_conn.get("password")

    flow_name = source.get("name") or f"flow-{flow_id[:8]}"
    # Process group named with '-schema-inference' suffix
    pg_name = f"{flow_name}-schema-inference"

    if auth_type == "BASIC" and username and password:
        await _get_nifi_token(nifi_url, username, password)

    kafka_bootstrap = ""
    kafka_security_protocol = "PLAINTEXT"
    if kafka_conn:
        kafka_bootstrap = kafka_conn.get("endpoint") or ""
        kafka_security_protocol = kafka_conn.get("security_protocol") or kafka_security_protocol

    # ── 1. Get root process group ID ──────────────────────────────────────
    root_pg_id = await get_nifi_root_process_group_id(
        nifi_url, auth_type=auth_type, username=username, password=password
    )
    if not root_pg_id:
        return {"ok": False, "error": "Could not retrieve NiFi root process group ID."}

    # ── 2. Delete any previous inference PG with the same name ────────────
    # (best-effort — ignore errors)
    try:
        r = await nifi_api_request(
            nifi_url, "GET",
            f"/nifi-api/process-groups/{root_pg_id}/process-groups",
            auth_type=auth_type, username=username, password=password,
        )
        if r.get("ok"):
            for pg in (r["data"].get("processGroups") or []):
                comp = pg.get("component") or {}
                if comp.get("name") == pg_name:
                    old_id = comp.get("id") or pg.get("id")
                    logger.info(f"Removing stale inference PG '{pg_name}' ({old_id})")
                    await _delete_old_pg(nifi_url, old_id,
                                        auth_type=auth_type, username=username, password=password)
    except Exception as e:
        logger.debug(f"Could not clean up stale inference PG: {e}")

    # ── 3. Create new process group ───────────────────────────────────────
    pg_id = await _create_process_group(
        nifi_url, root_pg_id, pg_name,
        auth_type=auth_type, username=username, password=password,
    )
    if not pg_id:
        return {"ok": False, "error": "Failed to create inference NiFi process group."}

    # ── 3b. Assign global parameter context (for #{KAFKA_BOOTSTRAP_SERVERS} etc.) ──
    # Look up the global-infra context (the one used by all production flows)
    try:
        pc_resp = await nifi_api_request(
            nifi_url, "GET", "/nifi-api/flow/parameter-contexts",
            auth_type=auth_type, username=username, password=password,
        )
        global_pc_id = None
        if pc_resp.get("ok"):
            for pc in pc_resp["data"].get("parameterContexts", []):
                comp = pc.get("component", {})
                params = {p.get("parameter", {}).get("name"): True for p in comp.get("parameters", [])}
                if "KAFKA_BOOTSTRAP_SERVERS" in params:
                    global_pc_id = comp.get("id")
                    logger.info(f"Found global parameter context: {comp.get('name')} ({global_pc_id})")
                    break
        if global_pc_id:
            # Assign parameter context to the new PG
            pg_info = await nifi_api_request(
                nifi_url, "GET", f"/nifi-api/process-groups/{pg_id}",
                auth_type=auth_type, username=username, password=password,
            )
            if pg_info.get("ok"):
                rev = pg_info["data"].get("revision", {})
                await nifi_api_request(
                    nifi_url, "PUT", f"/nifi-api/process-groups/{pg_id}",
                    auth_type=auth_type, username=username, password=password,
                    json_body={
                        "revision": rev,
                        "component": {
                            "id": pg_id,
                            "parameterContext": {"id": global_pc_id},
                        },
                    },
                )
                logger.info(f"Assigned global parameter context {global_pc_id} to inference PG {pg_id}")
    except Exception as e:
        logger.warning(f"Could not assign parameter context to inference PG: {e}")

    # ── 4. Create inference controller services (JSON output, no schema registry)
    source_type_raw = (source.get("type") or "").strip()
    source_type_key = source_type_raw.upper().replace(" ", "_")
    streams = source.get("streams") or []
    has_xml_streams = any(
        (s.get("response_format") or "json").lower() == "xml"
        for s in streams
    )

    services = await _create_inference_controller_services(
        nifi_url, pg_id,
        kafka_bootstrap=kafka_bootstrap,
        kafka_security_protocol=kafka_security_protocol,
        auth_type=auth_type, username=username, password=password,
        global_service_ids=global_service_ids,
        include_xml_reader=has_xml_streams,
    )

    kafka_svc_id = services["kafka_svc"]
    json_reader_id = services["json_reader"]
    json_writer_id = services.get("json_writer") or services["avro_writer"]
    xml_reader_id = services.get("xml_reader")

    if has_xml_streams and not xml_reader_id:
        error = (
            "XMLReader controller service could not be created for schema inference; "
            "refusing to build REST XML inference flow."
        )
        logger.error(error)
        await _delete_old_pg(nifi_url, pg_id, auth_type=auth_type, username=username, password=password)
        return {"ok": False, "error": error}

    schedule_str = _schedule_period(source)

    # ── 5. Build source-type-specific processor chain ─────────────────────
    ok = False
    if source_type_key in ("REST_API", "REST"):
        ok = await _build_rest_api_flow(
            nifi_url, pg_id, source,
            kafka_svc_id=kafka_svc_id,
            json_reader_id=json_reader_id,
            schema_reg_id=None,
            xml_reader_id=xml_reader_id,
            avro_writer_id=json_writer_id,
            kafka_schema_reg_id=None,
            schema_ref_writer_id=None,
            json_writer_id=json_writer_id,
            target_stream_id=target_stream_id,
            kafka_topic=inference_topic,
            schedule_str=schedule_str,
            auth_type_nifi=auth_type, username_nifi=username, password_nifi=password,
        )
    elif source_type_key == "SMB":
        ok = await _build_smb_flow(
            nifi_url, pg_id, source,
            kafka_svc_id=kafka_svc_id,
            json_reader_id=json_reader_id,
            json_writer_id=json_writer_id,
            avro_writer_id=json_writer_id,
            schema_reg_id=None,
            schema_name=None,
            kafka_topic=inference_topic,
            schedule_str=schedule_str,
            auth_type_nifi=auth_type, username_nifi=username, password_nifi=password,
        )
    elif source_type_key == "POSTGRESQL":
        ok = await _build_postgres_flow(
            nifi_url, pg_id, source,
            kafka_svc_id=kafka_svc_id,
            avro_writer_id=json_writer_id,
            kafka_topic=inference_topic,
            schedule_str=schedule_str,
            auth_type_nifi=auth_type, username_nifi=username, password_nifi=password,
        )
    elif source_type_key == "MONGODB":
        ok = await _build_mongodb_flow(
            nifi_url, pg_id, source,
            kafka_svc_id=kafka_svc_id,
            json_reader_id=json_reader_id,
            json_writer_id=json_writer_id,
            schema_reg_id=None,
            schema_name=None,
            avro_writer_id=json_writer_id,
            avro_transform_writer_id=None,
            kafka_topic=inference_topic,
            schedule_str=schedule_str,
            auth_type_nifi=auth_type, username_nifi=username, password_nifi=password,
        )
    else:
        logger.warning(f"Unsupported source type '{source_type_raw}' for inference — creating placeholder")
        gff_id = await _create_processor(
            nifi_url, pg_id,
            "org.apache.nifi.processors.standard.GenerateFlowFile",
            "InferencePlaceholder",
            properties={"File Size": "0 B"},
            auto_terminate=["success"],
            scheduling_period="1 hour",
            auth_type=auth_type, username=username, password=password,
        )
        ok = gff_id is not None

    if not ok:
        await _delete_old_pg(nifi_url, pg_id, auth_type=auth_type, username=username, password=password)
        return {"ok": False, "error": f"Failed to build inference processor chain for source type '{source_type_raw}'"}

    logger.info(f"Deployed inference flow '{pg_name}' as NiFi process group {pg_id}")
    return {"ok": True, "process_group_id": pg_id}
    if smb_default_props:
        defaults_id = await _create_processor(
            nifi_url, pg_id,
            "org.apache.nifi.processors.attributes.UpdateAttribute",
            "ApplyDefaultAttributes",
            properties=smb_default_props,
            auto_terminate=["failure"],
            scheduling_period="0 sec",
            x=x, y=transform_y,
            **nargs,
        )
        if not defaults_id:
            return False
        await _create_connection(
            nifi_url, pg_id, transform_input_id, defaults_id, transform_input_rels, **nargs
        )
        transform_input_id = defaults_id
        transform_input_rel = "success"
        transform_input_rels = [transform_input_rel]
        transform_y += step
