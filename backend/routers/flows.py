import logging
import os
import uuid
import re
from copy import deepcopy
from datetime import datetime
from typing import Optional, Dict, Any, List
from fastapi import APIRouter, HTTPException, Depends, Body, Query, Response
from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import BaseModel, ConfigDict, Field
from pymongo import ReturnDocument

from db import get_db
from models.flow import Flow, FlowCreate
from services import content_store, flow_export, iceberg_sinks
from services.application_services import resolve_source_application_service
from services.runtime_connections import require_runtime_connections
from services.flow_operation_claims import claim_flow_state, release_flow_state
from services.rest_stream_branch_validation import RestBranchValidationError, validate_rest_branch_graph
from services.rest_stream_pagination_validation import RestPaginationValidationError, validate_rest_stream_pagination
from services.rest_stream_polling_validation import RestPollingValidationError, validate_rest_stream_polling
from services.connection_resolver import resolve_connection
from services.connection_fingerprint import probe_nifi_fingerprint, probe_kafka_fingerprint
from services.lifecycle_locks import require_unlocked

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/flows", tags=["flows"])


class FlowPatchRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {"enabled": False},
                {"state": "Draft"},
                {
                    "schema_artifact_id": "bronze.dummyjson.users__history-value",
                    "schema_version": 1,
                },
            ]
        },
    )

    enabled: Optional[bool] = None
    schema_artifact_id: Optional[str] = None
    schema_version: Optional[int] = Field(default=None, ge=1)
    schema_inference_job_id: Optional[str] = None
    state: Optional[str] = None


class ControllerServiceUpdateRequest(BaseModel):
    properties: Dict[str, Any]


def _topic_token(value: Optional[str], fallback: str) -> str:
    raw = (value or "").strip().lower()
    if not raw:
        return fallback
    out_chars = []
    prev_underscore = False
    for ch in raw:
        if ch.isalnum():
            out_chars.append(ch)
            prev_underscore = False
        else:
            if not prev_underscore:
                out_chars.append("_")
                prev_underscore = True
    token = "".join(out_chars).strip("_")
    return token or fallback


def _derive_kafka_topic(source_name: Optional[str], stream_name: Optional[str]) -> str:
    source_token = _topic_token(source_name, "source")
    stream_token = _topic_token(stream_name, "stream")
    return f"bronze.{source_token}.{stream_token}__history"


def _legacy_flow_topic(source: dict, primary_stream: dict) -> str:
    for stream in source.get("streams") or []:
        if not isinstance(stream, dict):
            continue
        entity = stream.get("entity")
        if not isinstance(entity, dict):
            continue
        kafka = entity.get("kafka")
        if isinstance(kafka, dict) and str(kafka.get("topic") or "").strip():
            return str(kafka.get("topic")).strip()
        return _derive_kafka_topic(source.get("name"), stream.get("name") or stream.get("id"))
    return _derive_kafka_topic(source.get("name"), primary_stream.get("name") or primary_stream.get("id"))


_PATH_VAR_RE = re.compile(r"\$\{([^}]+)\}|\{([^{}]+)\}")


def _attr_token(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")


def _path_variables(path: Any) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for match in _PATH_VAR_RE.finditer(str(path or "")):
        name = (match.group(1) or match.group(2) or "").strip()
        if name and name not in seen:
            seen.add(name)
            out.append(name)
    return out


def _stream_extracted_attrs(stream: dict) -> Dict[str, str]:
    attrs: Dict[str, str] = {}
    for rule in stream.get("extraction_rules") or []:
        if not isinstance(rule, dict):
            continue
        raw = str(rule.get("attribute_name") or "").strip()
        token = _attr_token(raw)
        if raw and token and token not in attrs:
            attrs[token] = raw
    for raw in stream.get("primary_key_fields") or []:
        token = _attr_token(raw)
        if raw and token and token not in attrs:
            attrs[token] = str(raw)
    return attrs


def _repair_rest_stream_graph_for_deploy(source: dict) -> tuple[dict, bool, List[str]]:
    """Infer missing REST fan-out edges from URL placeholders before NiFi generation.

    This is intentionally conservative: only streams with path variables can be inferred,
    and the parent must be a previous stream that extracts a matching attribute.
    """
    if "rest" not in str(source.get("type") or "").lower():
        return source, False, []
    streams = source.get("streams") or []
    if not isinstance(streams, list) or len(streams) < 2:
        return source, False, []

    repaired = deepcopy(source)
    repaired_streams = repaired.get("streams") or []
    changed = False
    notes: List[str] = []

    attrs_by_id = {
        str(stream.get("id") or stream.get("name") or ""): _stream_extracted_attrs(stream)
        for stream in repaired_streams
        if isinstance(stream, dict)
    }

    for idx, stream in enumerate(repaired_streams):
        if not isinstance(stream, dict):
            continue
        sid = str(stream.get("id") or stream.get("name") or "").strip()
        route_source = stream.get("route_source") if isinstance(stream.get("route_source"), dict) else {}
        if route_source.get("parent_stream_id"):
            continue
        variables = _path_variables(stream.get("endpoint_path"))
        if not sid or not variables:
            continue

        fan = stream.get("fan_out") if isinstance(stream.get("fan_out"), dict) else {}
        parent_id = str(fan.get("parent_stream_id") or "").strip() if fan.get("enabled") else ""

        if not parent_id:
            best_parent_id = ""
            best_field = ""
            best_inject = ""
            for variable in variables:
                token = _attr_token(variable)
                for candidate in reversed(repaired_streams[:idx]):
                    if not isinstance(candidate, dict):
                        continue
                    candidate_id = str(candidate.get("id") or candidate.get("name") or "").strip()
                    if not candidate_id or candidate_id == sid:
                        continue
                    attrs = attrs_by_id.get(candidate_id) or {}
                    if token in attrs:
                        best_parent_id = candidate_id
                        best_field = attrs[token]
                        best_inject = variable
                        break
                if best_parent_id:
                    break

            if best_parent_id:
                stream["fan_out"] = {
                    **fan,
                    "enabled": True,
                    "parent_stream_id": best_parent_id,
                    "parent_field": best_field,
                    "inject_as": fan.get("inject_as") or "path_param",
                    "inject_field_name": fan.get("inject_field_name") or best_inject,
                    "inject_template": fan.get("inject_template") or f"${{{best_inject}}}",
                }
                changed = True
                notes.append(f"{stream.get('name') or sid} parent inferred as {best_parent_id}")
            continue

        parent_attrs = attrs_by_id.get(parent_id) or {}
        for variable in variables:
            token = _attr_token(variable)
            if token not in parent_attrs:
                continue
            current_field = str(fan.get("parent_field") or "").strip()
            current_token = _attr_token(current_field)
            current_inject = str(fan.get("inject_field_name") or "").strip()
            current_inject_token = _attr_token(current_inject)
            if current_token not in parent_attrs or current_inject_token not in parent_attrs:
                stream["fan_out"] = {
                    **fan,
                    "enabled": True,
                    "parent_stream_id": parent_id,
                    "parent_field": parent_attrs[token],
                    "inject_as": fan.get("inject_as") or "path_param",
                    "inject_field_name": current_inject if current_inject_token in parent_attrs else variable,
                    "inject_template": fan.get("inject_template") or f"${{{variable}}}",
                }
                changed = True
                notes.append(f"{stream.get('name') or sid} parent field repaired to {parent_attrs[token]}")
            break

    return repaired, changed, notes


def _repair_rest_pagination_for_deploy(source: dict) -> tuple[dict, bool, List[str]]:
    """Fill safe defaults for known REST pagination shapes before NiFi generation."""
    if "rest" not in str(source.get("type") or "").lower():
        return source, False, []
    streams = source.get("streams") or []
    if not isinstance(streams, list):
        return source, False, []

    source_hint = f"{source.get('name') or ''} {source.get('base_url') or ''}".lower()
    is_rapid7_like = "rapid7" in source_hint or "/api/3" in source_hint
    if not is_rapid7_like:
        return source, False, []

    repaired = deepcopy(source)
    changed = False
    notes: List[str] = []

    for stream in repaired.get("streams") or []:
        if not isinstance(stream, dict):
            continue
        pag = stream.get("pagination") if isinstance(stream.get("pagination"), dict) else {}
        if not pag.get("enabled") or str(pag.get("type") or "").strip().lower() != "page":
            continue

        stream_changed = False
        defaults = {
            "page_param": "page",
            "page_size_param": "size",
            "page_size": 100,
            "total_count_path": "$.page.totalPages",
            "stop_condition": "TOTAL_COUNT",
        }
        for key, value in defaults.items():
            if pag.get(key) in (None, ""):
                pag[key] = value
                stream_changed = True

        if stream_changed:
            stream["pagination"] = pag
            changed = True
            notes.append(f"{stream.get('name') or stream.get('id') or 'stream'} Rapid7 pagination defaults applied")

    return repaired, changed, notes


async def _require_nifi_reachable(db: AsyncIOMotorDatabase) -> None:
    conn = await resolve_connection(db, "nifi")
    if conn and conn.get("reachability") == "Unreachable":
        raise HTTPException(status_code=409, detail="NiFi is unreachable — try again when it is back. Deployment state was not changed.")


async def _log_audit(db, action, object_type, target, status, details=None):
    from models.audit import AuditEvent
    event = AuditEvent(action=action, object_type=object_type, target=target or "", status=status, details=details)
    await db.audit_events.insert_one(event.dict())


async def _nifi_pg_exists(flow: dict, db: AsyncIOMotorDatabase) -> bool:
    status = await _nifi_pg_status(flow, db)
    return bool(status.get("exists"))


def _nifi_result_is_missing(result: Dict[str, Any]) -> bool:
    if result.get("status_code") == 404:
        return True
    error = str(result.get("error") or "").lower()
    return "not found" in error or "does not exist" in error


async def _nifi_pg_status(flow: dict, db: AsyncIOMotorDatabase) -> Dict[str, Any]:
    pg_id = flow.get("nifi_process_group_id")
    if not pg_id:
        return {"exists": False, "missing": False, "reason": "exists", "pg_id": None, "error": None}
    nifi_conn = await resolve_connection(db, "nifi")
    if not nifi_conn:
        return {"exists": None, "missing": False, "reason": "unreachable", "pg_id": pg_id, "error": "No NiFi connection configured."}
    try:
        from services.nifi_client import nifi_api_request
        r = await nifi_api_request(
            nifi_conn["endpoint"],
            "GET",
            f"/nifi-api/process-groups/{pg_id}",
            auth_type=nifi_conn.get("auth_type", "NONE"),
            username=nifi_conn.get("username"),
            password=nifi_conn.get("password"),
        )
        if r.get("ok"):
            return {"exists": True, "missing": False, "reason": "exists", "pg_id": pg_id, "error": None}
        if _nifi_result_is_missing(r):
            # Disambiguate a missing PG: probe the current instance fingerprint
            fp = await probe_nifi_fingerprint(nifi_conn)
            if not fp["reachable"]:
                return {"exists": None, "missing": False, "reason": "unreachable", "pg_id": pg_id, "error": fp.get("error")}
            stored = flow.get("nifi_instance_fingerprint")
            current = fp.get("fingerprint")
            if stored is None or current is None:
                return {"exists": False, "missing": False, "reason": "unknown", "pg_id": pg_id, "error": None}
            if stored == current:
                return {"exists": False, "missing": True, "reason": "missing_same_instance", "pg_id": pg_id, "error": None}
            # stored != current
            return {"exists": False, "missing": False, "reason": "different_instance", "pg_id": pg_id, "error": None}
        # Check for connection failure (no status_code indicates network/connection error)
        if r.get("status_code") is None:
            return {"exists": None, "missing": False, "reason": "unreachable", "pg_id": pg_id, "error": r.get("error") or "NiFi connection failed."}
        return {"exists": None, "missing": False, "reason": "unknown", "pg_id": pg_id, "error": r.get("error") or "Unable to verify NiFi process group."}
    except Exception as exc:
        return {"exists": None, "missing": False, "reason": "unreachable", "pg_id": pg_id, "error": str(exc)[:180]}


async def _clear_stale_nifi_pg_reference(
    flow: dict,
    db: AsyncIOMotorDatabase,
    *,
    reason: str,
) -> dict:
    pg_id = flow.get("nifi_process_group_id")
    now = datetime.utcnow()
    await db.flows.update_one(
        {"id": flow["id"]},
        {"$set": {"nifi_process_group_id": None, "state": "Stopped", "updated_at": now}},
    )
    if flow.get("state") == "Running":
        await _close_latest_running_run(db, flow["id"], status="Success", error=reason)
    await content_store.sync_flow(db, flow["id"])
    await _log_audit(
        db,
        "Cleared stale NiFi deployment",
        "Flow",
        flow.get("name", ""),
        "Success",
        f"NiFi PG {pg_id} was missing. {reason}",
    )
    updated = dict(flow)
    updated["nifi_process_group_id"] = None
    updated["state"] = "Stopped"
    updated["updated_at"] = now
    return updated


async def _clear_nifi_pg_if_missing(
    flow: dict,
    db: AsyncIOMotorDatabase,
    *,
    action: str,
) -> tuple[dict, bool, Optional[str]]:
    status = await _nifi_pg_status(flow, db)
    reason = status.get("reason")

    if reason == "missing_same_instance":
        pg_id = status.get("pg_id")
        message = (
            f"NiFi process group {pg_id} no longer exists. "
            "Local deployment reference was cleared. Deploy the flow again to recreate it."
        )
        updated = await _clear_stale_nifi_pg_reference(flow, db, reason=f"{action}: {message}")
        return updated, True, message
    elif reason == "different_instance":
        return (flow, False, "This flow was deployed in a different NiFi instance. Repoint or migrate the NiFi connection to reconcile.")
    elif reason == "unreachable":
        return (flow, False, "NiFi is unreachable — deployment state preserved.")
    elif reason in ("unknown", "exists"):
        return (flow, False, status.get("error"))
    else:
        return (flow, False, status.get("error"))


def _stale_nifi_deployment_error() -> HTTPException:
    return HTTPException(
        status_code=409,
        detail=(
            "This flow is no longer deployed in NiFi. "
            "The local deployment reference was cleared. Deploy it again before using live NiFi actions."
        ),
    )


def _is_trino_source_type(source_type: Optional[str]) -> bool:
    return str(source_type or "").strip().lower() == "trino"


async def _validate_schema_link(
    db: AsyncIOMotorDatabase,
    artifact_id: Optional[str],
    version: Optional[int],
) -> None:
    if not artifact_id and version is None:
        return
    if not artifact_id or version is None:
        raise HTTPException(status_code=422, detail="schema_artifact_id and schema_version must be provided together.")
    art = await db.schema_artifacts.find_one({"artifact_id": artifact_id}, {"_id": 0})
    if not art:
        raise HTTPException(status_code=400, detail="Linked schema artifact not found.")
    if not any(v.get("version") == version for v in art.get("versions", [])):
        raise HTTPException(status_code=400, detail=f"Linked schema version {version} not found.")


async def _is_webhook_flow(flow: dict, db: AsyncIOMotorDatabase) -> bool:
    source = await db.sources.find_one({"id": flow.get("source_id")}, {"_id": 0})
    return (source or {}).get("type") == "Webhook"


def _collect_entity_destinations(flow: dict, source: Optional[dict]) -> List[Dict[str, Any]]:
    destinations: List[Dict[str, Any]] = []
    seen_topics: set[str] = set()
    for stream in (source or {}).get("streams") or []:
        if not isinstance(stream, dict):
            continue
        entity = stream.get("entity") or {}
        if not isinstance(entity, dict) or not entity:
            continue
        kafka_cfg = entity.get("kafka") or {}
        if not isinstance(kafka_cfg, dict):
            kafka_cfg = {}
        topic = str(kafka_cfg.get("topic") or "").strip()
        schema_artifact_id = str(
            entity.get("schema_artifact_id")
            or kafka_cfg.get("schema_artifact_id")
            or ""
        ).strip()
        schema_version = entity.get("schema_version") or kafka_cfg.get("schema_version")
        try:
            schema_version = int(schema_version) if schema_version not in (None, "") else None
        except Exception:
            schema_version = None
        if not topic and not schema_artifact_id:
            continue
        if topic:
            seen_topics.add(topic)
        destinations.append({
            "stream_id": str(stream.get("id") or stream.get("name") or "").strip(),
            "stream_name": str(stream.get("name") or stream.get("id") or "").strip(),
            "topic": topic or None,
            "schema_artifact_id": schema_artifact_id or None,
            "schema_version": schema_version,
        })

    fallback_topic = str(flow.get("kafka_topic") or "").strip()
    fallback_schema = str(flow.get("schema_artifact_id") or "").strip()
    if not destinations and (fallback_topic or fallback_schema):
        stream_id = str(flow.get("primary_stream_id") or "").strip()
        stream = _stream_by_id(source or {}, stream_id)
        destinations.append({
            "stream_id": stream_id,
            "stream_name": str((stream or {}).get("name") or stream_id or "primary").strip(),
            "topic": fallback_topic or None,
            "schema_artifact_id": fallback_schema or None,
            "schema_version": flow.get("schema_version"),
        })
    return destinations


async def _entity_destinations_for_flow(flow: dict, db: AsyncIOMotorDatabase) -> List[Dict[str, Any]]:
    source_id = flow.get("source_id")
    source = None
    if source_id:
        source = await db.sources.find_one({"id": source_id}, {"_id": 0, "streams": 1})
    return _collect_entity_destinations(flow, source)


async def _to_response_with_entities(f: dict, db: AsyncIOMotorDatabase) -> dict:
    out = _to_response(f)
    out["entity_destinations"] = await _entity_destinations_for_flow(f, db)
    primary_stream_id = str(out.get("primary_stream_id") or "").strip()
    if primary_stream_id:
        primary_destination = next(
            (
                destination
                for destination in out["entity_destinations"]
                if str(destination.get("stream_id") or "").strip() == primary_stream_id
            ),
            None,
        )
        if primary_destination:
            out["primary_stream_name"] = primary_destination.get("stream_name")
    return out


def _to_response(f: dict) -> dict:
    f = dict(f)
    f.pop("_id", None)
    f.pop("schema_inference_active", None)
    for field in ("created_at", "updated_at", "last_run"):
        if isinstance(f.get(field), datetime):
            f[field] = f[field].isoformat()
    return f


async def _flow_name_exists(
    db: AsyncIOMotorDatabase,
    name: str,
    exclude_id: Optional[str] = None,
) -> bool:
    normalized = (name or "").strip()
    if not normalized:
        return False
    query: Dict[str, Any] = {
        "name": {
            "$regex": f"^{re.escape(normalized)}$",
            "$options": "i",
        }
    }
    if exclude_id:
        query["id"] = {"$ne": exclude_id}
    return await db.flows.find_one(query, {"_id": 1}) is not None


def _build_default_metrics(
    *,
    flow: dict,
    kafka_metrics: Dict[str, Any],
    records_processed: Optional[int] = None,
    processors: Optional[List[Dict[str, Any]]] = None,
    error: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "throughput": 0,
        "queued": 0,
        "records_processed": int(records_processed if records_processed is not None else flow.get("total_records", 0)),
        "kafka_messages": kafka_metrics["kafka_messages"],
        "kafka_topic_total_messages": kafka_metrics["kafka_topic_total_messages"],
        "kafka_topic": kafka_metrics["kafka_topic"],
        "kafka_topics": kafka_metrics.get("kafka_topics") or [],
        "kafka_topic_counts": kafka_metrics.get("kafka_topic_counts") or [],
        "kafka_message_error": kafka_metrics.get("kafka_message_error"),
        "bytes_in": 0,
        "bytes_out": 0,
        "active_threads": 0,
        "processors": processors or [],
        "error": error,
    }


def _stream_by_id(source: dict, stream_id: Optional[str]) -> Optional[dict]:
    token = (stream_id or "").strip()
    if not token:
        return None
    for stream in source.get("streams") or []:
        if not isinstance(stream, dict):
            continue
        if str(stream.get("id") or "").strip() == token:
            return stream
    return None


def _route_graph_edges(streams: List[dict]) -> tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    try:
        result = validate_rest_branch_graph({"streams": streams})
    except RestBranchValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return result.children_by_parent, result.incoming_by_child


def _validate_rest_pagination_for_deploy(source: dict) -> None:
    try:
        validate_rest_stream_pagination(source)
    except RestPaginationValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


def _validate_rest_polling_for_deploy(source: dict) -> None:
    try:
        validate_rest_stream_polling(source)
    except RestPollingValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


async def _validate_entity_stream_schemas(source: dict, db: AsyncIOMotorDatabase) -> bool:
    streams = source.get("streams") or []
    if not streams:
        raise HTTPException(status_code=400, detail="Source has no streams to deploy.")

    children, _incoming = _route_graph_edges(streams)
    by_id = {str(s.get("id") or s.get("name") or "").strip(): s for s in streams}
    for sid, child_ids in children.items():
        stream = by_id.get(sid) or {}
        stream_name = stream.get("name") or sid
        if child_ids and str(stream.get("default_route_action") or "").strip().lower() == "keep":
            raise HTTPException(
                status_code=400,
                detail=f"Stream {stream_name} keeps unmatched records while routing to child streams. Route unmatched records to a default child stream or drop them.",
            )
    if source.get("type") == "REST API":
        for sid, parents in _incoming.items():
            if not parents:
                continue
            stream = by_id.get(sid) or {}
            stream_name = stream.get("name") or sid
            if not stream.get("request_enabled"):
                continue
            endpoint_path = str(stream.get("endpoint_path") or "").strip()
            if not endpoint_path:
                raise HTTPException(status_code=400, detail=f"Routed request stream {stream_name} must define an endpoint path.")
            if not endpoint_path.startswith("/"):
                raise HTTPException(status_code=400, detail=f"Routed request stream {stream_name} endpoint path must start with '/'.")
    entity_streams = [s for s in streams if bool(s.get("entity"))]
    if not entity_streams:
        raise HTTPException(status_code=400, detail="No entity streams selected. At least one output entity is required.")

    schema_required = not _is_trino_source_type(source.get("type"))
    for stream in entity_streams:
        sid = str(stream.get("id") or stream.get("name") or "").strip()
        name = stream.get("name") or sid
        entity = stream.get("entity") or {}
        kafka_cfg = entity.get("kafka") or {}
        topic = str(kafka_cfg.get("topic") or "").strip()
        if not topic:
            raise HTTPException(status_code=400, detail=f"Entity stream {name} has no generated Kafka topic.")
        if not schema_required:
            continue
        artifact_id = (
            entity.get("schema_artifact_id")
            or kafka_cfg.get("schema_artifact_id")
            or f"{topic}-value"
        )
        version = entity.get("schema_version") or kafka_cfg.get("schema_version")
        if not artifact_id or not version:
            raise HTTPException(status_code=400, detail=f"Entity stream {name} has no linked schema version.")
        artifact = await db.schema_artifacts.find_one({"artifact_id": artifact_id}, {"_id": 0, "versions": 1})
        if not artifact:
            raise HTTPException(status_code=400, detail=f"Entity stream {name} schema artifact {artifact_id} was not found.")
        matched = next((v for v in artifact.get("versions", []) if v.get("version") == version), None)
        if not matched:
            raise HTTPException(status_code=400, detail=f"Entity stream {name} schema version {version} was not found.")
        if matched.get("status") != "Verified":
            raise HTTPException(status_code=400, detail=f"Entity stream {name} schema version {version} is not verified.")
    return True


async def _validate_verified_schema_for_flow(flow: dict, db: AsyncIOMotorDatabase) -> None:
    # New entity-based routing model: entity streams carry their own
    # schema config (schema_artifact_id, schema_version on entity.kafka).
    source_id = flow.get("source_id")
    if source_id:
        source = await db.sources.find_one({"id": source_id}, {"_id": 0, "type": 1, "streams": 1, "kafka_output": 1})
        if source:
            has_valid_entity_model = await _validate_entity_stream_schemas(source, db)
            if has_valid_entity_model:
                return

    if not flow.get("schema_artifact_id") or not flow.get("schema_version"):
        raise HTTPException(status_code=400, detail="No schema linked. Link a verified schema before continuing.")
    art = await db.schema_artifacts.find_one({"artifact_id": flow["schema_artifact_id"]})
    if not art:
        raise HTTPException(status_code=400, detail="Linked schema artifact not found. Re-link a valid schema.")
    versions = art.get("versions", [])
    version = next((v for v in versions if v.get("version") == flow["schema_version"]), None)
    if not version:
        raise HTTPException(status_code=400, detail=f"Linked schema version {flow['schema_version']} not found.")
    if version.get("status") != "Verified":
        raise HTTPException(status_code=400, detail=f"Schema version {flow['schema_version']} is not verified.")


async def _ensure_deploy_schemas_registered(
    flow: dict,
    source: dict,
    db: AsyncIOMotorDatabase,
) -> None:
    """Ensure schemas referenced by generated NiFi services exist in Apicurio.

    A schema can be marked Verified in our DB while missing from Apicurio if it
    was verified before native/ccompat dual-registration was added, or if the
    registry was recreated. Deploy must repair that before NiFi references it.
    """
    apicurio_conn = await resolve_connection(db, "apicurio")
    if not apicurio_conn:
        return

    from services.apicurio_client import register_schema, schema_available

    seen: set[tuple[str, int]] = set()
    destinations = _collect_entity_destinations(flow, source)
    if not destinations and flow.get("schema_artifact_id") and flow.get("schema_version"):
        destinations = [{
            "schema_artifact_id": flow.get("schema_artifact_id"),
            "schema_version": flow.get("schema_version"),
            "stream_name": flow.get("name") or "primary",
        }]

    for dest in destinations:
        artifact_id = str(dest.get("schema_artifact_id") or "").strip()
        try:
            version = int(dest.get("schema_version")) if dest.get("schema_version") not in (None, "") else None
        except Exception:
            version = None
        if not artifact_id or version is None:
            continue
        key = (artifact_id, version)
        if key in seen:
            continue
        seen.add(key)

        artifact = await db.schema_artifacts.find_one({"artifact_id": artifact_id}, {"_id": 0})
        versions = artifact.get("versions", []) if artifact else []
        target_version = next((v for v in versions if v.get("version") == version), None)
        if not target_version:
            raise HTTPException(status_code=400, detail=f"Linked schema {artifact_id} v{version} was not found.")
        if target_version.get("status") != "Verified":
            raise HTTPException(status_code=400, detail=f"Linked schema {artifact_id} v{version} is not verified.")
        avro_schema = target_version.get("avro_schema")
        if not avro_schema:
            raise HTTPException(status_code=400, detail=f"Linked schema {artifact_id} v{version} has no Avro schema.")

        available = await schema_available(
            url=apicurio_conn.get("endpoint", ""),
            group_id=apicurio_conn.get("group_id", "nif-platform"),
            artifact_id=artifact_id,
            auth_type=apicurio_conn.get("auth_type", "NONE"),
            username=apicurio_conn.get("username"),
            password=apicurio_conn.get("password"),
            token=apicurio_conn.get("token"),
        )
        if available.get("ok"):
            continue

        result = await register_schema(
            url=apicurio_conn.get("endpoint", ""),
            group_id=apicurio_conn.get("group_id", "nif-platform"),
            artifact_id=artifact_id,
            avro_schema=avro_schema,
            auth_type=apicurio_conn.get("auth_type", "NONE"),
            username=apicurio_conn.get("username"),
            password=apicurio_conn.get("password"),
            token=apicurio_conn.get("token"),
        )
        if not result.get("ok"):
            raise HTTPException(
                status_code=400,
                detail=f"Schema {artifact_id} could not be registered in Apicurio before deploy: {result.get('error') or 'registry rejected schema'}",
            )

        if result.get("global_id") is not None or result.get("ccompat_id") is not None:
            for item in versions:
                if item.get("version") == version:
                    if result.get("global_id") is not None:
                        item["apicurio_global_id"] = result.get("global_id")
                    if result.get("ccompat_id") is not None:
                        item["apicurio_ccompat_id"] = result.get("ccompat_id")
                    item["updated_at"] = datetime.utcnow()
                    break
            await db.schema_artifacts.update_one(
                {"artifact_id": artifact_id},
                {"$set": {"versions": versions, "updated_at": datetime.utcnow()}},
            )
            await content_store.sync_schema_artifact(db, artifact_id)


async def _append_flow_run_start(
    db: AsyncIOMotorDatabase,
    flow_id: str,
    *,
    kafka_start_count: Optional[int] = None,
) -> None:
    flow = await db.flows.find_one({"id": flow_id}, {"_id": 0})
    if not flow:
        return
    if kafka_start_count is None:
        kafka_metrics = await _build_kafka_topic_metrics(flow, db)
        kafka_start_count = int(kafka_metrics.get("kafka_topic_total_messages") or 0)
    run_id = str(uuid.uuid4())
    now = datetime.utcnow()
    run_doc = {
        "id": run_id,
        "started_at": now,
        "ended_at": None,
        "duration_seconds": None,
        "records_processed": 0,
        "kafka_start_count": int(kafka_start_count),
        "status": "Running",
        "error": None,
    }
    await db.flows.update_one({"id": flow_id}, {"$push": {"runs": run_doc}})
    await content_store.sync_flow(db, flow_id)


def _records_processed_for_run(run: Dict[str, Any], *, kafka_total: int) -> int:
    start_count = run.get("kafka_start_count")
    if start_count is None:
        return int(run.get("records_processed") or 0)
    return max(0, int(kafka_total) - int(start_count))


async def _close_latest_running_run(
    db: AsyncIOMotorDatabase,
    flow_id: str,
    *,
    status: str = "Success",
    error: Optional[str] = None,
) -> None:
    flow = await db.flows.find_one({"id": flow_id}, {"_id": 0})
    if not flow:
        return
    runs = flow.get("runs") or []
    idx = None
    for i in range(len(runs) - 1, -1, -1):
        if runs[i].get("status") == "Running":
            idx = i
            break
    if idx is None:
        return
    started_at = runs[idx].get("started_at")
    now = datetime.utcnow()
    duration = None
    if isinstance(started_at, datetime):
        duration = max(0, int((now - started_at).total_seconds()))
    kafka_metrics = await _build_kafka_topic_metrics(flow, db)
    records_processed = _records_processed_for_run(
        runs[idx],
        kafka_total=int(kafka_metrics.get("kafka_topic_total_messages") or 0),
    )
    patch = {
        f"runs.{idx}.status": status,
        f"runs.{idx}.ended_at": now,
        f"runs.{idx}.duration_seconds": duration,
        f"runs.{idx}.records_processed": records_processed,
        f"runs.{idx}.error": error,
        "total_records": int(flow.get("total_records") or 0) + records_processed,
    }
    await db.flows.update_one({"id": flow_id}, {"$set": patch})
    await content_store.sync_flow(db, flow_id)


def _kafbat_runtime_config(kafka_conn: dict) -> Dict[str, Optional[str]]:
    """Resolve Kafbat settings only from an explicitly saved Kafbat Kafka connection."""
    mode = (kafka_conn.get("kafka_connection_mode") or "native").strip().lower()
    if mode != "kafbat":
        return {"kafbat_url": None, "kafbat_username": None, "kafbat_password": None}
    return {
        "kafbat_url": kafka_conn.get("kafbat_url") or kafka_conn.get("endpoint"),
        "kafbat_username": kafka_conn.get("kafbat_username"),
        "kafbat_password": kafka_conn.get("kafbat_password"),
    }


async def _build_kafka_topic_metrics(flow: dict, db: AsyncIOMotorDatabase) -> Dict[str, Any]:
    destinations = await _entity_destinations_for_flow(flow, db)
    topics = [str(item.get("topic") or "").strip() for item in destinations if str(item.get("topic") or "").strip()]
    if not topics and (flow.get("kafka_topic") or "").strip():
        topics = [(flow.get("kafka_topic") or "").strip()]
    topic = topics[0] if topics else ""
    payload: Dict[str, Any] = {
        "kafka_topic": topic or None,
        "kafka_topics": topics,
        "kafka_messages": 0,
        "kafka_topic_total_messages": 0,
        "kafka_topic_counts": [],
    }
    if not topics:
        payload["kafka_message_error"] = "Flow has no Kafka topics configured."
        return payload

    kafka_conn = await resolve_connection(db, "kafka")
    if not kafka_conn:
        payload["kafka_message_error"] = "No Kafka connection configured."
        return payload

    try:
        from services.kafka_client import get_topic_message_count

        total = 0
        errors: List[str] = []
        counts: List[Dict[str, Any]] = []
        kafbat_config = _kafbat_runtime_config(kafka_conn)
        for topic_name in topics:
            result = await get_topic_message_count(
                bootstrap_servers=(kafka_conn.get("endpoint") or "").strip(),
                topic=topic_name,
                security_protocol=kafka_conn.get("security_protocol") or "PLAINTEXT",
                sasl_mechanism=kafka_conn.get("sasl_mechanism"),
                sasl_username=kafka_conn.get("sasl_username"),
                sasl_password=kafka_conn.get("sasl_password"),
                **kafbat_config,
            )
            if result.get("ok"):
                topic_total = int(result.get("total_messages") or 0)
                total += topic_total
                counts.append({"topic": topic_name, "total_messages": topic_total})
            else:
                error = result.get("error") or "Kafka topic count lookup failed."
                errors.append(f"{topic_name}: {error}")
                counts.append({"topic": topic_name, "total_messages": 0, "error": error})
    except Exception as e:
        payload["kafka_message_error"] = f"Kafka topic count lookup failed: {str(e)[:180]}"
        return payload

    payload["kafka_messages"] = total
    payload["kafka_topic_total_messages"] = total
    payload["kafka_topic_counts"] = counts
    if errors:
        payload["kafka_message_error"] = "; ".join(errors)[:500]
    return payload


@router.get("/")
async def list_flows(db: AsyncIOMotorDatabase = Depends(get_db)):
    flows = await db.flows.find({}, {"_id": 0}).sort("created_at", -1).to_list(200)
    return [await _to_response_with_entities(f, db) for f in flows]


@router.post(
    "/",
    status_code=201,
    summary="Create a flow",
    description="Create a flow record linked to an existing source. Kafka topic is derived from source name and primary stream.",
)
async def create_flow(data: FlowCreate, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Create a flow record linked to an existing source configuration."""
    # Validate source exists
    source = await db.sources.find_one({"id": data.source_id})
    if not source:
        raise HTTPException(status_code=400, detail="Source not found for given source_id")
    if source.get("type") != data.source_type:
        raise HTTPException(status_code=400, detail=f"source_type does not match source record type ({source.get('type')}).")
    await _validate_schema_link(db, data.schema_artifact_id, data.schema_version)
    primary_stream = _stream_by_id(source, data.primary_stream_id)
    if not primary_stream:
        raise HTTPException(status_code=422, detail="primary_stream_id does not exist on the selected source.")

    # Guard: flow names must be unique.
    if await _flow_name_exists(db, data.name):
        raise HTTPException(status_code=409, detail="Flow name already exists. Please choose a different name.")

    now = datetime.utcnow()
    flow = Flow(**data.dict())
    doc = flow.dict()
    doc.pop("primary_stream", None)
    doc["kafka_topic"] = _legacy_flow_topic(source, primary_stream)
    doc["created_at"] = now
    doc["updated_at"] = now
    await db.flows.insert_one(doc)
    await _log_audit(db, "Created flow", "Flow", data.name, "Success")
    await content_store.sync_flow(db, doc["id"])
    return await _to_response_with_entities(doc, db)


@router.put("/{flow_id}", summary="Replace a flow")
async def update_flow(flow_id: str, data: FlowCreate, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Full update of a flow. Used by Flow Designer when editing an existing flow."""
    existing = await db.flows.find_one({"id": flow_id})
    if not existing:
        raise HTTPException(status_code=404, detail="Flow not found")
    # Validate source exists
    source = await db.sources.find_one({"id": data.source_id})
    if not source:
        raise HTTPException(status_code=400, detail="Source not found for given source_id")
    if source.get("type") != data.source_type:
        raise HTTPException(status_code=400, detail=f"source_type does not match source record type ({source.get('type')}).")
    await _validate_schema_link(db, data.schema_artifact_id, data.schema_version)
    primary_stream = _stream_by_id(source, data.primary_stream_id)
    if not primary_stream:
        raise HTTPException(status_code=422, detail="primary_stream_id does not exist on the selected source.")
    if await _flow_name_exists(db, data.name, exclude_id=flow_id):
        raise HTTPException(status_code=409, detail="Flow name already exists. Please choose a different name.")

    updates = data.dict()
    updates.pop("primary_stream", None)
    updates["kafka_topic"] = _legacy_flow_topic(source, primary_stream)
    updates["updated_at"] = datetime.utcnow()
    await db.flows.update_one({"id": flow_id}, {"$set": updates})
    updated = await db.flows.find_one({"id": flow_id}, {"_id": 0})
    await _log_audit(db, "Updated flow", "Flow", data.name, "Success")
    await content_store.sync_flow(db, flow_id)

    try:
        await iceberg_sinks.sync_sinks_for_flow(db, updated, source)
    except Exception as exc:
        logger.warning("Iceberg sink sync skipped for flow %s: %s", flow_id, exc)

    return await _to_response_with_entities(updated, db)


@router.get("/{flow_id}")
async def get_flow(flow_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    flow = await db.flows.find_one({"id": flow_id}, {"_id": 0})
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    return await _to_response_with_entities(flow, db)


@router.get("/{flow_id}/export")
async def export_flow(flow_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    try:
        package = await flow_export.build_flow_export_package(db, flow_id)
    except ValueError as exc:
        message = str(exc) or "Flow export failed"
        status_code = 404 if "not found" in message.lower() else 400
        raise HTTPException(status_code=status_code, detail=message)

    filename = flow_export.export_filename(package)
    return Response(
        content=flow_export.dumps_export_package(package),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.patch(
    "/{flow_id}",
    summary="Patch flow operational metadata",
    description="Enable/disable a flow, update schema links, store schema inference job ID, or normalize Draft/Stopped state.",
)
async def patch_flow(flow_id: str, updates: FlowPatchRequest = Body(...), db: AsyncIOMotorDatabase = Depends(get_db)):
    """Partial update of a flow (e.g., enable/disable).
    
    When disabling (enabled=False), deactivates the NiFi process group for this flow.
    """
    flow = await db.flows.find_one({"id": flow_id})
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    allowed = {"enabled", "schema_artifact_id", "schema_version", "schema_inference_job_id", "state"}
    raw_updates = updates.dict(exclude_unset=True)
    unknown = sorted(set(raw_updates.keys()) - allowed)
    if unknown:
        raise HTTPException(status_code=422, detail=f"Unknown flow patch field(s): {', '.join(unknown)}")
    if not raw_updates:
        raise HTTPException(status_code=422, detail="Patch payload must include at least one supported field.")
    safe_updates = {k: v for k, v in raw_updates.items() if k in allowed}
    if "schema_artifact_id" in safe_updates or "schema_version" in safe_updates:
        candidate_artifact = safe_updates.get("schema_artifact_id", flow.get("schema_artifact_id"))
        candidate_version = safe_updates.get("schema_version", flow.get("schema_version"))
        await _validate_schema_link(db, candidate_artifact, candidate_version)
    safe_updates["updated_at"] = datetime.utcnow()
    if "state" in safe_updates:
        allowed_states = {"Draft", "Stopped"}
        if safe_updates["state"] not in allowed_states:
            raise HTTPException(
                status_code=400,
                detail="Only state='Draft' or state='Stopped' is allowed via patch.",
            )

    # Draft guard: only edit/delete should be allowed for Draft flows.
    # Block enable/disable toggles and operational patches.
    if flow.get("state") == "Draft" and "enabled" in raw_updates:
        raise HTTPException(status_code=400, detail="Draft flow cannot be enabled/disabled. Save/finalize first.")

    # When disabling, deactivate the NiFi process group for THIS flow:
    # stop processors + disable processors + disable controller services.
    if "enabled" in raw_updates and not raw_updates["enabled"]:
        pg_id = flow.get("nifi_process_group_id")
        if pg_id:
            flow, stale_pg, _ = await _clear_nifi_pg_if_missing(flow, db, action="disable flow")
            if not stale_pg:
                nifi_conn = await resolve_connection(db, "nifi")
                if not nifi_conn:
                    raise HTTPException(status_code=400, detail="Cannot disable deployed flow: No NiFi connection configured.")
                try:
                    from services.nifi_flow_manager import deactivate_process_group
                    result = await deactivate_process_group(
                        url=nifi_conn["endpoint"],
                        process_group_id=pg_id,
                        auth_type=nifi_conn.get("auth_type", "NONE"),
                        username=nifi_conn.get("username"),
                        password=nifi_conn.get("password"),
                    )
                    if not result.get("ok"):
                        raise HTTPException(status_code=502, detail=result.get("error") or "Failed to deactivate NiFi process group.")
                    logger.info(f"Deactivated NiFi PG {pg_id} for disabled flow {flow_id}: {result}")
                except HTTPException:
                    raise
                except Exception as e:
                    raise HTTPException(status_code=502, detail=f"Failed to deactivate NiFi process group: {str(e)[:180]}")
        # Mark state as Stopped when disabling
        if flow.get("state") == "Running":
            safe_updates["state"] = "Stopped"

    await db.flows.update_one({"id": flow_id}, {"$set": safe_updates})
    if "enabled" in raw_updates and not raw_updates["enabled"] and flow.get("state") == "Running":
        await _close_latest_running_run(db, flow_id, status="Success")
    updated = await db.flows.find_one({"id": flow_id}, {"_id": 0})
    await content_store.sync_flow(db, flow_id)

    try:
        patch_source = await db.sources.find_one({"id": updated.get("source_id")}, {"_id": 0}) if updated else None
        await iceberg_sinks.sync_sinks_for_flow(db, updated, patch_source)
    except Exception as exc:
        logger.warning("Iceberg sink sync skipped for flow %s: %s", flow_id, exc)

    return await _to_response_with_entities(updated, db)


@router.delete("/{flow_id}", status_code=204)
async def delete_flow(flow_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Delete a flow record and remove its associated NiFi process group."""
    flow = await db.flows.find_one({"id": flow_id})
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")

    # Delete the NiFi process group for this specific flow. Stale NiFi metadata
    # should not block deleting the application flow record.
    pg_id = flow.get("nifi_process_group_id")
    nifi_cleanup_error = None
    if pg_id:
        nifi_conn = await resolve_connection(db, "nifi")
        if not nifi_conn:
            nifi_cleanup_error = "No NiFi connection configured."
            logger.warning(f"Deleting flow {flow_id} without NiFi cleanup: {nifi_cleanup_error}")
        else:
            try:
                from services.nifi_flow_generator import _delete_old_pg
                await _delete_old_pg(
                    nifi_url=nifi_conn["endpoint"],
                    pg_id=pg_id,
                    auth_type=nifi_conn.get("auth_type", "NONE"),
                    username=nifi_conn.get("username"),
                    password=nifi_conn.get("password"),
                    strict=False,
                )
                logger.info(f"Deleted NiFi process group {pg_id} for flow {flow_id}")
            except Exception as e:
                nifi_cleanup_error = str(e)[:180]
                logger.warning(f"Deleting flow {flow_id} despite NiFi cleanup failure for PG {pg_id}: {nifi_cleanup_error}")

    source_id = str(flow.get("source_id") or "").strip()

    try:
        await iceberg_sinks.delete_sinks_for_flow(db, flow_id)
    except Exception as exc:
        logger.warning("Iceberg sink cleanup skipped for flow %s: %s", flow_id, exc)

    await db.flows.delete_one({"id": flow_id})
    await _log_audit(db, "Deleted flow", "Flow", flow.get("name", ""), "Success",
                     f"NiFi cleanup skipped/failed for PG {pg_id}: {nifi_cleanup_error}" if nifi_cleanup_error else (f"NiFi PG {pg_id} removed" if pg_id else "No NiFi PG to remove"))
    if source_id and await db.flows.find_one({"source_id": source_id}, {"_id": 1}):
        await content_store.sync_source(db, source_id)
    else:
        content_store.delete_flow(flow_id, source_id=source_id or None)
        # A source is owned by its flow. Once no remaining flow references it,
        # remove the orphaned source record so it stops inflating dashboard counts.
        if source_id:
            await db.sources.delete_one({"id": source_id})


@router.post("/{flow_id}/start")
async def start_flow(flow_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Start a flow — checks eligibility, then starts via NiFi API."""
    await _require_nifi_reachable(db)
    await require_unlocked(db, "flows", flow_id)
    flow = await db.flows.find_one({"id": flow_id}, {"_id": 0})
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    is_webhook = await _is_webhook_flow(flow, db)
    if flow.get("state") == "Draft":
        raise HTTPException(status_code=409, detail="Draft flow cannot be started. Save/finalize first.")

    # Eligibility checks
    if not flow.get("enabled"):
        raise HTTPException(status_code=409, detail="Flow is disabled. Enable it before starting.")
    if flow.get("state") == "Running":
        raise HTTPException(status_code=409, detail="Flow is already running.")
    await require_runtime_connections(db, action="start flow")
    await _validate_verified_schema_for_flow(flow, db)
    start_metrics = await _build_kafka_topic_metrics(flow, db)
    kafka_start_count = int(start_metrics.get("kafka_topic_total_messages") or 0)
    claimed = await claim_flow_state(
        db,
        flow_id,
        from_states={"Stopped", "Degraded", "Error"},
        claimed_state="Starting",
    )
    if not claimed:
        raise HTTPException(
            status_code=409,
            detail="Flow is already starting, running, stopping, or deploying.",
        )
    flow = claimed
    await _append_flow_run_start(
        db,
        flow_id,
        kafka_start_count=kafka_start_count,
    )

    try:
        if is_webhook:
            await db.flows.update_one({"id": flow_id}, {"$set": {"state": "Running", "last_run": datetime.utcnow(), "updated_at": datetime.utcnow()}})
            await _log_audit(db, "Started flow", "Flow", flow.get("name", ""), "Success")
            await content_store.sync_flow(db, flow_id)
            return {"ok": True, "state": "Running"}

        if not flow.get("nifi_process_group_id"):
            raise HTTPException(status_code=400, detail="Flow has not been deployed yet. Deploy the flow first.")

        flow, stale_pg, _ = await _clear_nifi_pg_if_missing(flow, db, action="start flow")
        if stale_pg:
            raise _stale_nifi_deployment_error()

        # Call NiFi to start
        nifi_conn = await resolve_connection(db, "nifi")
        if not nifi_conn:
            raise HTTPException(status_code=400, detail="No NiFi connection configured.")

        from services.nifi_flow_manager import prepare_process_group_for_start, start_process_group
        await prepare_process_group_for_start(
            url=nifi_conn["endpoint"],
            process_group_id=flow["nifi_process_group_id"],
            auth_type=nifi_conn.get("auth_type", "NONE"),
            username=nifi_conn.get("username"),
            password=nifi_conn.get("password"),
        )
        result = await start_process_group(
            url=nifi_conn["endpoint"],
            process_group_id=flow["nifi_process_group_id"],
            auth_type=nifi_conn.get("auth_type", "NONE"),
            username=nifi_conn.get("username"),
            password=nifi_conn.get("password"),
        )
        if result.get("ok"):
            await db.flows.update_one({"id": flow_id}, {"$set": {"state": "Running", "last_run": datetime.utcnow(), "updated_at": datetime.utcnow()}})
            await _log_audit(db, "Started flow", "Flow", flow.get("name", ""), "Success")
            await content_store.sync_flow(db, flow_id)
            return {"ok": True, "state": "Running"}
        else:
            raise HTTPException(status_code=502, detail=result.get("error", "NiFi start failed"))
    except ImportError:
        # nifi_flow_manager not yet implemented
        await db.flows.update_one({"id": flow_id}, {"$set": {"state": "Running", "last_run": datetime.utcnow(), "updated_at": datetime.utcnow()}})
        await _log_audit(db, "Started flow", "Flow", flow.get("name", ""), "Success")
        await content_store.sync_flow(db, flow_id)
        return {"ok": True, "state": "Running"}
    except HTTPException as exc:
        await release_flow_state(
            db,
            flow_id,
            claimed_state="Starting",
            final_state="Stopped",
        )
        await _close_latest_running_run(
            db,
            flow_id,
            status="Failed",
            error=str(exc.detail),
        )
        raise
    except Exception as exc:
        await release_flow_state(
            db,
            flow_id,
            claimed_state="Starting",
            final_state="Stopped",
        )
        await _close_latest_running_run(
            db,
            flow_id,
            status="Failed",
            error=str(exc)[:180],
        )
        raise HTTPException(status_code=502, detail=str(exc)[:180]) from exc


@router.post("/{flow_id}/stop")
async def stop_flow(flow_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    await _require_nifi_reachable(db)
    await require_unlocked(db, "flows", flow_id)
    flow = await db.flows.find_one({"id": flow_id}, {"_id": 0})
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    is_webhook = await _is_webhook_flow(flow, db)
    if flow.get("state") == "Draft":
        raise HTTPException(status_code=409, detail="Draft flow cannot be stopped. Save/finalize first.")
    claimed = await claim_flow_state(
        db,
        flow_id,
        from_states={"Running"},
        claimed_state="Stopping",
    )
    if not claimed:
        raise HTTPException(
            status_code=409,
            detail="Flow is not running or another runtime operation is active.",
        )
    flow = claimed

    try:
        if flow.get("nifi_process_group_id") and not is_webhook:
            flow, stale_pg, stale_message = await _clear_nifi_pg_if_missing(flow, db, action="stop flow")
            if stale_pg:
                await _close_latest_running_run(db, flow_id, status="Success")
                await _log_audit(db, "Stopped flow", "Flow", flow.get("name", ""), "Success", stale_message)
                return {"ok": True, "state": "Stopped", "stale_deployment": True, "message": stale_message}
            nifi_conn = await resolve_connection(db, "nifi")
            if not nifi_conn:
                raise HTTPException(status_code=400, detail="Cannot stop deployed flow: No NiFi connection configured.")
            from services.nifi_flow_manager import stop_and_clear_process_group
            stop_result = await stop_and_clear_process_group(
                url=nifi_conn["endpoint"],
                process_group_id=flow["nifi_process_group_id"],
                auth_type=nifi_conn.get("auth_type", "NONE"),
                username=nifi_conn.get("username"),
                password=nifi_conn.get("password"),
            )
            if not stop_result.get("ok"):
                raise HTTPException(status_code=502, detail=stop_result.get("error") or "NiFi stop/queue clear failed")
    except ImportError:
        pass
    except Exception:
        await release_flow_state(
            db,
            flow_id,
            claimed_state="Stopping",
            final_state="Running",
        )
        raise

    await db.flows.update_one({"id": flow_id}, {"$set": {"state": "Stopped", "updated_at": datetime.utcnow()}})
    await _close_latest_running_run(db, flow_id, status="Success")
    await _log_audit(db, "Stopped flow", "Flow", flow.get("name", ""), "Success")
    await content_store.sync_flow(db, flow_id)
    return {"ok": True, "state": "Stopped"}


@router.post("/{flow_id}/deploy")
async def deploy_flow(flow_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Deploy a configured source as a NiFi flow.

    Idempotency: if the flow is already in 'Deploying' state, returns 409 to prevent duplicates.
    """
    await _require_nifi_reachable(db)
    await require_unlocked(db, "flows", flow_id)
    flow = await db.flows.find_one({"id": flow_id}, {"_id": 0})
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    is_webhook = await _is_webhook_flow(flow, db)

    # Idempotency guard: if PG already exists in NiFi, do not create another.
    if flow.get("nifi_process_group_id") and not is_webhook:
        nifi_conn_for_check = await resolve_connection(db, "nifi")
        if not nifi_conn_for_check:
            raise HTTPException(
                status_code=400,
                detail="Cannot deploy flow because a NiFi process group is already linked but no NiFi connection is configured.",
            )
        pg_status = await _nifi_pg_status(flow, db)
        if pg_status.get("exists"):
            raise HTTPException(
                status_code=409,
                detail="Flow is already deployed to NiFi. Use 'Delete from NiFi' before deploying again.",
            )
        if pg_status.get("missing"):
            # PG id is stale (manually deleted from NiFi). Clear it and continue deploy.
            flow, _, _ = await _clear_nifi_pg_if_missing(flow, db, action="deploy flow")
        else:
            raise HTTPException(
                status_code=502,
                detail=f"Cannot verify existing NiFi process group before deploy: {pg_status.get('error') or 'unknown NiFi error'}",
            )

    # Guard: Flow must be explicitly saved/finalized (not Draft) before deploy
    if flow.get("state") == "Draft":
        raise HTTPException(status_code=400, detail="Flow is in Draft state. Save/finalize the flow before deploying.")

    source = await db.sources.find_one({"id": flow.get("source_id")}, {"_id": 0})
    if not source:
        raise HTTPException(status_code=404, detail="Source configuration not found")
    source = await resolve_source_application_service(db, source)

    source, graph_repaired, graph_notes = _repair_rest_stream_graph_for_deploy(source)
    source, pagination_repaired, pagination_notes = _repair_rest_pagination_for_deploy(source)
    if graph_repaired or pagination_repaired:
        await db.sources.update_one(
            {"id": flow.get("source_id")},
            {
                "$set": {
                    "streams": source.get("streams") or [],
                    "updated_at": datetime.utcnow(),
                }
            },
        )
        repair_notes = [*graph_notes, *pagination_notes]
        logger.info("Repaired REST source before deploy for flow %s: %s", flow_id, "; ".join(repair_notes))
        if flow.get("source_id"):
            await content_store.sync_source(db, flow.get("source_id"))

    _validate_rest_pagination_for_deploy(source)
    _validate_rest_polling_for_deploy(source)

    await require_runtime_connections(db, action="deploy flow")
    await _validate_verified_schema_for_flow(flow, db)
    await _ensure_deploy_schemas_registered(flow, source, db)

    if is_webhook:
        # Webhook flow runtime is backend-native. Deploy is a no-op after schema validation.
        await db.flows.update_one(
            {"id": flow_id},
            {"$set": {"state": "Stopped", "updated_at": datetime.utcnow()}},
        )
        await _log_audit(db, "Deployed flow", "Flow", flow.get("name", ""), "Success", "Webhook flow deploy is a backend runtime no-op")
        await content_store.sync_flow(db, flow_id)
        return {"ok": True, "process_group_id": None, "message": "Webhook flow deploy is a no-op."}

    # Idempotency guard: prevent duplicate concurrent deployments
    if flow.get("state") == "Deploying":
        raise HTTPException(status_code=409, detail="Flow is already being deployed. Please wait.")

    nifi_conn = await resolve_connection(db, "nifi")
    if not nifi_conn:
        raise HTTPException(status_code=400, detail="No NiFi connection configured")

    kafka_conn = await resolve_connection(db, "kafka")
    apicurio_conn = await resolve_connection(db, "apicurio")
    try:
        from services.nifi_global_services import resolve_flow_global_service_ids
        global_service_ids = await resolve_flow_global_service_ids(db, source)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to resolve NiFi global services: {str(exc)[:180]}")

    # Atomically claim deployment to prevent duplicate concurrent deploys.
    claimed = await db.flows.find_one_and_update(
        {"id": flow_id, "state": {"$ne": "Deploying"}},
        {"$set": {"state": "Deploying", "updated_at": datetime.utcnow()}},
        return_document=ReturnDocument.AFTER,
        projection={"_id": 0},
    )
    if not claimed:
        raise HTTPException(status_code=409, detail="Flow is already being deployed. Please wait.")
    flow = claimed
    await content_store.sync_flow(db, flow_id)

    deployed_pg_id: Optional[str] = None
    try:
        from services.nifi_flow_generator import generate_and_deploy_flow
        result = await generate_and_deploy_flow(
            source=source,
            flow_id=flow_id,
            nifi_conn=nifi_conn,
            kafka_conn=kafka_conn,
            apicurio_conn=apicurio_conn,
            flow=flow,
            global_service_ids=global_service_ids,
        )
        if result.get("ok"):
            pg_id = result.get("process_group_id")
            deployed_pg_id = pg_id

            # Stamp connection provenance on successful deploy
            nifi_instance_fingerprint = None
            try:
                nifi_fp = await probe_nifi_fingerprint(nifi_conn)
                nifi_instance_fingerprint = nifi_fp.get("fingerprint")
            except Exception:
                pass

            kafka_cluster_id = None
            if kafka_conn:
                try:
                    kafka_fp = await probe_kafka_fingerprint(kafka_conn)
                    kafka_cluster_id = kafka_fp.get("fingerprint")
                except Exception:
                    pass

            update_dict = {
                "nifi_process_group_id": pg_id,
                "state": "Stopped",
                "updated_at": datetime.utcnow(),
                "nifi_connection_id": nifi_conn.get("id"),
                "nifi_instance_fingerprint": nifi_instance_fingerprint,
                "kafka_connection_id": kafka_conn.get("id") if kafka_conn else None,
                "kafka_cluster_id": kafka_cluster_id,
            }
            await db.flows.update_one(
                {"id": flow_id},
                {"$set": update_dict}
            )
            await _log_audit(db, "Deployed flow", "Flow", flow.get("name", ""), "Success", f"NiFi PG: {pg_id}")
            await content_store.sync_flow(db, flow_id)

            sink_results: List[Dict[str, Any]] = []
            try:
                sink_results = await iceberg_sinks.deploy_sinks_for_flow(db, flow, source)
            except Exception as exc:
                logger.warning("Iceberg sink deployment skipped for flow %s: %s", flow_id, exc)

            return {"ok": True, "process_group_id": pg_id, "iceberg_sinks": sink_results}
        else:
            await db.flows.update_one(
                {"id": flow_id},
                {"$set": {"state": "Stopped", "updated_at": datetime.utcnow()}}
            )
            await _log_audit(db, "Deployed flow", "Flow", flow.get("name", ""), "Failed", result.get("error"))
            await content_store.sync_flow(db, flow_id)
            raise HTTPException(status_code=502, detail=result.get("error", "Deployment failed"))
    except ImportError:
        await db.flows.update_one(
            {"id": flow_id},
            {"$set": {"state": "Stopped", "updated_at": datetime.utcnow()}}
        )
        await content_store.sync_flow(db, flow_id)
        raise HTTPException(status_code=501, detail="NiFi flow generator not yet implemented (Phase 4)")
    except HTTPException:
        raise
    except Exception as e:
        if deployed_pg_id:
            await _delete_failed_deployment_pg(nifi_conn, deployed_pg_id)
        await db.flows.update_one(
            {"id": flow_id},
            {
                "$set": {
                    "nifi_process_group_id": None,
                    "state": "Stopped",
                    "updated_at": datetime.utcnow(),
                }
            },
        )
        logger.error(f"Deploy error for flow {flow_id}: {e}")
        await content_store.sync_flow(db, flow_id)
        raise HTTPException(status_code=502, detail=str(e)[:240])


async def _delete_failed_deployment_pg(nifi_conn: dict, process_group_id: str) -> None:
    try:
        from services.nifi_flow_generator import _delete_old_pg

        await _delete_old_pg(
            nifi_conn["endpoint"],
            process_group_id,
            auth_type=nifi_conn.get("auth_type", "NONE"),
            username=nifi_conn.get("username"),
            password=nifi_conn.get("password"),
        )
    except Exception as exc:
        logger.warning(
            "Could not clean up failed deployment PG %s: %s",
            process_group_id,
            str(exc)[:180],
        )


@router.post("/{flow_id}/undeploy")
async def undeploy_flow(flow_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Delete the linked NiFi process group for a flow without deleting the flow itself."""
    await _require_nifi_reachable(db)
    await require_unlocked(db, "flows", flow_id)
    flow = await db.flows.find_one({"id": flow_id}, {"_id": 0})
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    is_webhook = await _is_webhook_flow(flow, db)
    if is_webhook:
        await db.flows.update_one(
            {"id": flow_id},
            {"$set": {"state": "Stopped", "updated_at": datetime.utcnow()}},
        )
        await _log_audit(db, "Undeployed flow", "Flow", flow.get("name", ""), "Success", "Webhook flow undeploy is a no-op")
        await content_store.sync_flow(db, flow_id)
        return {"ok": True, "message": "Webhook flow undeploy is a no-op."}

    pg_id = flow.get("nifi_process_group_id")
    if not pg_id:
        return {"ok": True, "message": "Flow is not deployed in NiFi."}

    nifi_conn = await resolve_connection(db, "nifi")
    if not nifi_conn:
        raise HTTPException(status_code=400, detail="No NiFi connection configured")

    flow, stale_pg, stale_message = await _clear_nifi_pg_if_missing(flow, db, action="undeploy flow")
    if stale_pg:
        return {
            "ok": True,
            "removed_process_group_id": pg_id,
            "stale_deployment": True,
            "message": stale_message,
        }

    try:
        from services.nifi_flow_generator import _delete_old_pg
        await _delete_old_pg(
            nifi_url=nifi_conn["endpoint"],
            pg_id=pg_id,
            auth_type=nifi_conn.get("auth_type", "NONE"),
            username=nifi_conn.get("username"),
            password=nifi_conn.get("password"),
            strict=True,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to delete NiFi PG {pg_id}: {str(e)[:180]}")

    await db.flows.update_one(
        {"id": flow_id},
        {"$set": {"nifi_process_group_id": None, "state": "Stopped", "updated_at": datetime.utcnow()}},
    )
    await _log_audit(db, "Undeployed flow", "Flow", flow.get("name", ""), "Success", f"NiFi PG: {pg_id}")
    await content_store.sync_flow(db, flow_id)

    response: Dict[str, Any] = {"ok": True, "removed_process_group_id": pg_id}
    try:
        sinks = await iceberg_sinks.list_sinks_for_flow(db, flow_id)
        enabled_count = sum(1 for sink in sinks if sink.get("enabled"))
        if enabled_count:
            response["iceberg_sinks_note"] = (
                f"{enabled_count} Iceberg sink(s) left running to drain remaining Kafka messages."
            )
    except Exception:
        pass
    return response


@router.get("/{flow_id}/iceberg-sinks")
async def get_flow_iceberg_sinks(
    flow_id: str,
    live: bool = Query(default=True),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    flow = await db.flows.find_one({"id": flow_id}, {"_id": 0})
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    return await iceberg_sinks.get_flow_sink_statuses(db, flow_id, live=live)


@router.get("/{flow_id}/metrics")
async def get_flow_metrics(flow_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    flow = await db.flows.find_one({"id": flow_id}, {"_id": 0})
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")

    kafka_metrics = await _build_kafka_topic_metrics(flow, db)

    pg_id = flow.get("nifi_process_group_id")
    if not pg_id:
        return _build_default_metrics(flow=flow, kafka_metrics=kafka_metrics)

    nifi_conn = await resolve_connection(db, "nifi")
    if not nifi_conn:
        return _build_default_metrics(flow=flow, kafka_metrics=kafka_metrics, error="No NiFi connection")

    flow, stale_pg, stale_message = await _clear_nifi_pg_if_missing(flow, db, action="load flow metrics")
    if stale_pg:
        return _build_default_metrics(flow=flow, kafka_metrics=kafka_metrics, error=stale_message)

    try:
        from services.nifi_flow_manager import get_process_group_metrics
        metrics = await get_process_group_metrics(
            url=nifi_conn["endpoint"],
            process_group_id=pg_id,
            auth_type=nifi_conn.get("auth_type", "NONE"),
            username=nifi_conn.get("username"),
            password=nifi_conn.get("password"),
        )
        metrics.update({
            "kafka_messages": kafka_metrics["kafka_messages"],
            "kafka_topic_total_messages": kafka_metrics["kafka_topic_total_messages"],
            "kafka_topic": kafka_metrics["kafka_topic"],
            "kafka_topics": kafka_metrics.get("kafka_topics") or [],
            "kafka_topic_counts": kafka_metrics.get("kafka_topic_counts") or [],
            "kafka_message_error": kafka_metrics.get("kafka_message_error"),
        })
        runs = flow.get("runs") or []
        latest_run = runs[-1] if runs else None
        if isinstance(latest_run, dict):
            metrics["records_processed"] = _records_processed_for_run(
                latest_run,
                kafka_total=int(kafka_metrics.get("kafka_topic_total_messages") or 0),
            )
        metrics.setdefault("throughput", 0)
        metrics.setdefault("queued", 0)
        metrics.setdefault("records_processed", int(flow.get("total_records", 0)))
        metrics.setdefault("bytes_in", 0)
        metrics.setdefault("bytes_out", 0)
        metrics.setdefault("active_threads", 0)
        metrics.setdefault("processors", [])
        return metrics
    except ImportError:
        return _build_default_metrics(flow=flow, kafka_metrics=kafka_metrics)


@router.get("/{flow_id}/kafka-messages")
async def get_flow_kafka_messages(
    flow_id: str,
    limit: int = Query(default=50, ge=1, le=50),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    flow = await db.flows.find_one({"id": flow_id}, {"_id": 0})
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")

    destinations = await _entity_destinations_for_flow(flow, db)
    topics = [str(item.get("topic") or "").strip() for item in destinations if str(item.get("topic") or "").strip()]
    if not topics and (flow.get("kafka_topic") or "").strip():
        topics = [(flow.get("kafka_topic") or "").strip()]
    if not topics:
        raise HTTPException(status_code=400, detail="Flow has no Kafka topics configured.")

    kafka_conn = await resolve_connection(db, "kafka")
    if not kafka_conn:
        raise HTTPException(status_code=400, detail="No Kafka connection configured.")

    try:
        from services.kafka_client import get_recent_topic_messages

        topic_results: List[Dict[str, Any]] = []
        combined_messages: List[Dict[str, Any]] = []
        errors: List[str] = []
        source_name = None
        direct_errors: List[str] = []
        kafbat_config = _kafbat_runtime_config(kafka_conn)
        for topic in topics:
            result = await get_recent_topic_messages(
                bootstrap_servers=(kafka_conn.get("endpoint") or "").strip(),
                topic=topic,
                limit=limit,
                security_protocol=kafka_conn.get("security_protocol") or "PLAINTEXT",
                sasl_mechanism=kafka_conn.get("sasl_mechanism"),
                sasl_username=kafka_conn.get("sasl_username"),
                sasl_password=kafka_conn.get("sasl_password"),
                **kafbat_config,
            )
            if result.get("ok"):
                messages = result.get("messages") or []
                for msg in messages:
                    if isinstance(msg, dict):
                        combined = dict(msg)
                        combined["topic"] = topic
                        combined_messages.append(combined)
                topic_results.append({
                    "topic": topic,
                    "messages": messages,
                    "source": result.get("source") or "kafka",
                    "direct_error": result.get("direct_error"),
                })
                source_name = source_name or result.get("source") or "kafka"
                if result.get("direct_error"):
                    direct_errors.append(f"{topic}: {result.get('direct_error')}")
                continue
            error = result.get("error") or "Failed to fetch Kafka messages."
            errors.append(f"{topic}: {error}")
            topic_results.append({"topic": topic, "messages": [], "error": error})

        combined_messages.sort(key=lambda m: int(m.get("timestamp_ms") or 0), reverse=True)
        combined_messages = combined_messages[:limit]
        return {
            "topic": topics[0] if len(topics) == 1 else None,
            "topics": topic_results,
            "messages": combined_messages,
            "source": source_name or "kafka",
            "direct_error": "; ".join(direct_errors)[:500] if direct_errors else None,
            "fetched_at": datetime.utcnow().isoformat(),
            "error": "; ".join(errors)[:500] if errors else None,
        }
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=502, detail=f"Failed to fetch Kafka messages: {str(e)[:180]}")


@router.get("/{flow_id}/runtime-errors")
async def get_flow_runtime_errors(
    flow_id: str,
    limit: int = Query(default=50, ge=1, le=100),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    flow = await db.flows.find_one({"id": flow_id}, {"_id": 0})
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")

    source = None
    source_id = flow.get("source_id")
    if source_id:
        source = await db.sources.find_one({"id": source_id}, {"_id": 0})

    from services.flow_runtime_errors import runtime_error_topic, runtime_errors_from_kafka_messages

    topic = runtime_error_topic(flow=flow, source=source)
    kafka_conn = await resolve_connection(db, "kafka")
    if not kafka_conn:
        return {
            "ok": True,
            "source": "unverified",
            "complete_history": False,
            "topic": topic,
            "items": [],
            "message": (
                "Kafka is not configured. Phase 12 runtime error publishing/history "
                "cannot be end-to-end verified."
            ),
            "fetched_at": datetime.utcnow().isoformat(),
        }

    try:
        from services.kafka_client import get_recent_topic_messages

        kafbat_config = _kafbat_runtime_config(kafka_conn)
        result = await get_recent_topic_messages(
            bootstrap_servers=(kafka_conn.get("endpoint") or "").strip(),
            topic=topic,
            limit=limit,
            security_protocol=kafka_conn.get("security_protocol") or "PLAINTEXT",
            sasl_mechanism=kafka_conn.get("sasl_mechanism"),
            sasl_username=kafka_conn.get("sasl_username"),
            sasl_password=kafka_conn.get("sasl_password"),
            **kafbat_config,
        )
        if not result.get("ok"):
            return {
                "ok": True,
                "source": "unverified",
                "complete_history": False,
                "topic": topic,
                "items": [],
                "message": (
                    f"Kafka runtime error topic could not be read: "
                    f"{result.get('error') or 'unknown Kafka error'}"
                )[:500],
                "fetched_at": datetime.utcnow().isoformat(),
            }

        messages = result.get("messages") or []
        items = runtime_errors_from_kafka_messages(messages, flow_id=flow_id)
        return {
            "ok": True,
            "source": result.get("source") or "kafka",
            "complete_history": True,
            "topic": topic,
            "items": items,
            "direct_error": result.get("direct_error"),
            "fetched_at": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=502, detail=f"Failed to fetch runtime errors: {str(e)[:180]}")


@router.post("/{flow_id}/kafka-messages/clear")
async def clear_flow_kafka_messages(
    flow_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    flow = await db.flows.find_one({"id": flow_id}, {"_id": 0})
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")

    destinations = await _entity_destinations_for_flow(flow, db)
    topics = [str(item.get("topic") or "").strip() for item in destinations if str(item.get("topic") or "").strip()]
    if not topics and (flow.get("kafka_topic") or "").strip():
        topics = [(flow.get("kafka_topic") or "").strip()]
    topics = list(dict.fromkeys(topics))
    if not topics:
        raise HTTPException(status_code=400, detail="Flow has no Kafka topics configured.")

    kafka_conn = await resolve_connection(db, "kafka")
    if not kafka_conn:
        raise HTTPException(status_code=400, detail="No Kafka connection configured.")

    bootstrap = (kafka_conn.get("endpoint") or "").strip()
    security_protocol = kafka_conn.get("security_protocol") or "PLAINTEXT"
    sasl_mechanism = kafka_conn.get("sasl_mechanism")
    sasl_username = kafka_conn.get("sasl_username")
    sasl_password = kafka_conn.get("sasl_password")
    kafbat_config = _kafbat_runtime_config(kafka_conn)

    try:
        from services.kafka_client import clear_topic_messages, get_topic_message_count

        before_total = 0
        after_total = 0
        cleared_messages = 0
        topic_results: List[Dict[str, Any]] = []
        for topic in topics:
            before_count_result = await get_topic_message_count(
                bootstrap_servers=bootstrap,
                topic=topic,
                security_protocol=security_protocol,
                sasl_mechanism=sasl_mechanism,
                sasl_username=sasl_username,
                sasl_password=sasl_password,
                **kafbat_config,
            )
            topic_before = int(before_count_result.get("total_messages") or 0) if before_count_result.get("ok") else None

            clear_result = await clear_topic_messages(
                bootstrap_servers=bootstrap,
                topic=topic,
                security_protocol=security_protocol,
                sasl_mechanism=sasl_mechanism,
                sasl_username=sasl_username,
                sasl_password=sasl_password,
                **kafbat_config,
            )
            if not clear_result.get("ok"):
                error = clear_result.get("error") or f"Failed to clear Kafka topic {topic}."
                status_code = 404 if "not found" in error.lower() else 502
                raise HTTPException(status_code=status_code, detail=error)

            after_count_result = await get_topic_message_count(
                bootstrap_servers=bootstrap,
                topic=topic,
                security_protocol=security_protocol,
                sasl_mechanism=sasl_mechanism,
                sasl_username=sasl_username,
                sasl_password=sasl_password,
                **kafbat_config,
            )
            topic_after = int(after_count_result.get("total_messages") or 0) if after_count_result.get("ok") else None
            topic_before = topic_before if topic_before is not None else clear_result.get("before_total_messages")
            topic_after = topic_after if topic_after is not None else clear_result.get("after_total_messages")
            topic_cleared = clear_result.get("cleared_messages")
            if isinstance(topic_before, int):
                before_total += topic_before
            if isinstance(topic_after, int):
                after_total += topic_after
            if isinstance(topic_cleared, int):
                cleared_messages += topic_cleared
            topic_results.append({
                "topic": topic,
                "before_total_messages": topic_before,
                "after_total_messages": topic_after,
                "cleared_messages": topic_cleared,
                "source": clear_result.get("source") or "kafka",
                "direct_error": clear_result.get("direct_error"),
            })
        await _log_audit(
            db,
            "Cleared Kafka topic messages",
            "Flow",
            flow.get("name", ""),
            "Success",
            f"Topics cleared: {', '.join(topics)}. before={before_total}, after={after_total}, cleared={cleared_messages}",
        )

        return {
            "ok": True,
            "topic": topics[0] if len(topics) == 1 else None,
            "topics": topic_results,
            "before_total_messages": before_total,
            "after_total_messages": after_total,
            "cleared_messages": cleared_messages,
            "source": topic_results[0].get("source") if topic_results else "kafka",
            "direct_error": "; ".join([f"{r['topic']}: {r.get('direct_error')}" for r in topic_results if r.get("direct_error")])[:500] or None,
            "cleared_at": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        if isinstance(e, HTTPException):
            raise
        raise HTTPException(status_code=502, detail=f"Failed to clear Kafka topic: {str(e)[:180]}")


@router.get("/{flow_id}/runs")
async def get_flow_runs(
    flow_id: str,
    limit: int = Query(default=20, ge=1, le=100),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    flow = await db.flows.find_one({"id": flow_id}, {"_id": 0})
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    runs = flow.get("runs", [])[-limit:]
    return list(reversed(runs))


@router.get("/{flow_id}/controller-services")
async def list_flow_controller_services(flow_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """List all NiFi controller services for a deployed flow, with full component details."""
    flow = await db.flows.find_one({"id": flow_id}, {"_id": 0})
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    pg_id = flow.get("nifi_process_group_id")
    if not pg_id:
        raise HTTPException(status_code=400, detail="Flow is not deployed to NiFi.")
    nifi_conn = await resolve_connection(db, "nifi")
    if not nifi_conn:
        raise HTTPException(status_code=400, detail="No NiFi connection configured.")
    flow, stale_pg, stale_message = await _clear_nifi_pg_if_missing(flow, db, action="list controller services")
    if stale_pg:
        return {"ok": True, "services": [], "stale_deployment": True, "message": stale_message}
    try:
        from services.nifi_flow_manager import list_pg_controller_services_detail
        result = await list_pg_controller_services_detail(
            url=nifi_conn["endpoint"],
            process_group_id=pg_id,
            auth_type=nifi_conn.get("auth_type", "NONE"),
            username=nifi_conn.get("username"),
            password=nifi_conn.get("password"),
        )
        if not result.get("ok"):
            raise HTTPException(status_code=502, detail=result.get("error", "Failed to fetch controller services"))
        try:
            from services.nifi_global_services import list_global_services
            global_services = await list_global_services(db)
            existing_ids = {service.get("id") for service in result.get("services", [])}
            inherited = []
            for service in global_services:
                nifi_id = service.get("nifi_controller_service_id")
                if not nifi_id or nifi_id in existing_ids:
                    continue
                live = service.get("live") or {}
                inherited.append({
                    "id": nifi_id,
                    "name": service.get("name"),
                    "type": service.get("nifi_type"),
                    "state": live.get("state", ""),
                    "validation_errors": live.get("validation_errors") or [],
                    "bulletin_level": "",
                    "revision": live.get("revision", 0),
                    "scope": "global",
                    "service_kind": service.get("service_kind"),
                    "global_service_id": service.get("id"),
                })
            result["services"] = list(result.get("services", [])) + inherited
        except Exception as exc:
            logger.warning(f"Failed to append inherited global services for flow {flow_id}: {exc}")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch controller services: {str(e)[:180]}")


@router.get("/{flow_id}/processors")
async def list_flow_processors(flow_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """List all live NiFi processors for a deployed flow."""
    flow = await db.flows.find_one({"id": flow_id}, {"_id": 0})
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    pg_id = flow.get("nifi_process_group_id")
    if not pg_id:
        raise HTTPException(status_code=400, detail="Flow is not deployed to NiFi.")
    nifi_conn = await resolve_connection(db, "nifi")
    if not nifi_conn:
        raise HTTPException(status_code=400, detail="No NiFi connection configured.")
    flow, stale_pg, stale_message = await _clear_nifi_pg_if_missing(flow, db, action="list processors")
    if stale_pg:
        return {"ok": True, "processors": [], "stale_deployment": True, "message": stale_message}
    try:
        from services.nifi_flow_manager import list_pg_processors_detail
        result = await list_pg_processors_detail(
            url=nifi_conn["endpoint"],
            process_group_id=pg_id,
            auth_type=nifi_conn.get("auth_type", "NONE"),
            username=nifi_conn.get("username"),
            password=nifi_conn.get("password"),
        )
        if not result.get("ok"):
            raise HTTPException(status_code=502, detail=result.get("error", "Failed to fetch processors"))
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch processors: {str(e)[:180]}")


@router.get("/{flow_id}/processors/{processor_id}")
async def get_flow_processor(flow_id: str, processor_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Fetch full live NiFi configuration for a specific processor."""
    flow = await db.flows.find_one({"id": flow_id}, {"_id": 0})
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    if not flow.get("nifi_process_group_id"):
        raise HTTPException(status_code=400, detail="Flow is not deployed to NiFi.")
    nifi_conn = await resolve_connection(db, "nifi")
    if not nifi_conn:
        raise HTTPException(status_code=400, detail="No NiFi connection configured.")
    flow, stale_pg, _ = await _clear_nifi_pg_if_missing(flow, db, action="fetch processor")
    if stale_pg:
        raise _stale_nifi_deployment_error()
    try:
        from services.nifi_flow_manager import get_processor_config
        result = await get_processor_config(
            url=nifi_conn["endpoint"],
            processor_id=processor_id,
            auth_type=nifi_conn.get("auth_type", "NONE"),
            username=nifi_conn.get("username"),
            password=nifi_conn.get("password"),
        )
        if not result.get("ok"):
            raise HTTPException(status_code=502, detail=result.get("error", "Failed to fetch processor"))
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch processor: {str(e)[:180]}")


@router.put("/{flow_id}/processors/{processor_id}")
async def update_flow_processor(
    flow_id: str,
    processor_id: str,
    data: ControllerServiceUpdateRequest,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Update a live NiFi processor's dynamic properties."""
    flow = await db.flows.find_one({"id": flow_id}, {"_id": 0})
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    if not flow.get("nifi_process_group_id"):
        raise HTTPException(status_code=400, detail="Flow is not deployed to NiFi.")
    nifi_conn = await resolve_connection(db, "nifi")
    if not nifi_conn:
        raise HTTPException(status_code=400, detail="No NiFi connection configured.")
    flow, stale_pg, _ = await _clear_nifi_pg_if_missing(flow, db, action="update processor")
    if stale_pg:
        raise _stale_nifi_deployment_error()
    try:
        from services.nifi_flow_manager import update_processor_config
        result = await update_processor_config(
            url=nifi_conn["endpoint"],
            processor_id=processor_id,
            properties=data.properties,
            auth_type=nifi_conn.get("auth_type", "NONE"),
            username=nifi_conn.get("username"),
            password=nifi_conn.get("password"),
        )
        if not result.get("ok"):
            raise HTTPException(status_code=502, detail=result.get("error", "Failed to update processor"))
        await _log_audit(
            db, "Updated processor", "Processor", processor_id, "Success",
            f"flow_id={flow_id}",
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to update processor: {str(e)[:180]}")


@router.get("/{flow_id}/controller-services/{service_id}")
async def get_flow_controller_service(flow_id: str, service_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Fetch full configuration and property descriptors for a specific NiFi controller service."""
    flow = await db.flows.find_one({"id": flow_id}, {"_id": 0})
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    if not flow.get("nifi_process_group_id"):
        raise HTTPException(status_code=400, detail="Flow is not deployed to NiFi.")
    nifi_conn = await resolve_connection(db, "nifi")
    if not nifi_conn:
        raise HTTPException(status_code=400, detail="No NiFi connection configured.")
    flow, stale_pg, _ = await _clear_nifi_pg_if_missing(flow, db, action="fetch controller service")
    if stale_pg:
        raise _stale_nifi_deployment_error()
    try:
        from services.nifi_flow_manager import get_controller_service_config
        result = await get_controller_service_config(
            url=nifi_conn["endpoint"],
            service_id=service_id,
            auth_type=nifi_conn.get("auth_type", "NONE"),
            username=nifi_conn.get("username"),
            password=nifi_conn.get("password"),
        )
        if not result.get("ok"):
            raise HTTPException(status_code=502, detail=result.get("error", "Failed to fetch controller service"))
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch controller service: {str(e)[:180]}")


@router.put("/{flow_id}/controller-services/{service_id}")
async def update_flow_controller_service(
    flow_id: str,
    service_id: str,
    data: ControllerServiceUpdateRequest,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Update a NiFi controller service's properties.

    Handles the disable → update → re-enable cycle required by NiFi.
    """
    flow = await db.flows.find_one({"id": flow_id}, {"_id": 0})
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    if not flow.get("nifi_process_group_id"):
        raise HTTPException(status_code=400, detail="Flow is not deployed to NiFi.")
    nifi_conn = await resolve_connection(db, "nifi")
    if not nifi_conn:
        raise HTTPException(status_code=400, detail="No NiFi connection configured.")
    flow, stale_pg, _ = await _clear_nifi_pg_if_missing(flow, db, action="update controller service")
    if stale_pg:
        raise _stale_nifi_deployment_error()
    try:
        from services.nifi_flow_manager import update_controller_service_config
        result = await update_controller_service_config(
            url=nifi_conn["endpoint"],
            service_id=service_id,
            properties=data.properties,
            auth_type=nifi_conn.get("auth_type", "NONE"),
            username=nifi_conn.get("username"),
            password=nifi_conn.get("password"),
        )
        if not result.get("ok"):
            raise HTTPException(status_code=502, detail=result.get("error", "Failed to update controller service"))
        await _log_audit(
            db, "Updated controller service", "ControllerService", service_id, "Success",
            f"flow_id={flow_id}",
        )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to update controller service: {str(e)[:180]}")
