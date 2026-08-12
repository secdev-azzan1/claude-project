"""
Schema Inference API Router.

Endpoints:
  POST /api/schema-inference/start          — start inference for a flow
  GET  /api/schema-inference/flow/{flow_id} — get active job for a flow
  GET  /api/schema-inference/{job_id}       — get job status
  POST /api/schema-inference/{job_id}/stop  — stop inference
  POST /api/schema-inference/{job_id}/accept — accept schema, link to flow
"""
import asyncio
import logging
import uuid
import re
from copy import deepcopy
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

from fastapi import APIRouter, HTTPException, Depends, Body, Query
from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import BaseModel, ConfigDict, Field
from pymongo import ReturnDocument

from db import get_db
from models.schema_inference import SchemaInferenceJob
from services.application_services import resolve_source_application_service
from services.runtime_connections import require_runtime_connections
from services.runtime_recovery import APP_INSTANCE_ID
from services.connection_resolver import resolve_connection

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/schema-inference", tags=["schema-inference"])
TEMPLATE_PLACEHOLDER_PATTERN = re.compile(r"\$\{([^}]+)\}")
PATH_PLACEHOLDER_PATTERN = re.compile(r"\$\{([^}]+)\}|\{([^{}]+)\}")
AUTO_INFERENCE_BLOCKED_METHODS = {"PUT", "PATCH", "DELETE"}
TERMINAL_INFERENCE_STATES = {"complete", "failed", "stopped", "cancelled"}


async def _cleanup_terminal_inference_pg(db: AsyncIOMotorDatabase, job: Dict[str, Any]) -> Dict[str, Any]:
    if str(job.get("status") or "").lower() not in TERMINAL_INFERENCE_STATES:
        return job
    pg_id = str(job.get("nifi_pg_id") or "").strip()
    if not pg_id:
        return job
    nifi_conn = await resolve_connection(db, "nifi")
    if not nifi_conn:
        logger.warning("Inference job %s has stale NiFi PG %s but no NiFi connection is configured.", job.get("id"), pg_id)
        return job
    try:
        from services.schema_inference_runner import _delete_inference_nifi_pg

        await _delete_inference_nifi_pg(nifi_conn, pg_id)
        await db.schema_inference_jobs.update_one(
            {"id": job.get("id")},
            {"$set": {"nifi_pg_id": None, "updated_at": datetime.utcnow()}},
        )
        job = dict(job)
        job["nifi_pg_id"] = None
    except Exception as exc:
        logger.warning("Could not clean up terminal inference PG %s for job %s: %s", pg_id, job.get("id"), exc)
    return job


class StartInferenceRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "flow_id": "paste-flow-id-here",
                    "target_messages": 10,
                }
            ]
        },
    )

    flow_id: str
    target_messages: int = Field(default=10, ge=1, le=1000)
    runtime_values: Dict[str, Any] = Field(default_factory=dict)
    entity_stream_id: Optional[str] = None  # Infer schema for a specific entity stream


def _collect_required_manual_runtime_params(source: dict) -> List[Tuple[str, str, str]]:
    """Return (stream_name, param_name, runtime_key) tuples for required manual runtime params."""
    required: List[Tuple[str, str, str]] = []
    streams = source.get("streams") or []
    for stream in streams:
        stream_name = (stream.get("name") or stream.get("id") or "stream").strip()
        bindings = stream.get("parameter_bindings") or []
        for binding in bindings:
            if not isinstance(binding, dict):
                continue
            if not bool(binding.get("enabled", True)):
                continue
            if not bool(binding.get("required", False)):
                continue
            if str(binding.get("value_source") or "").strip().lower() != "manual_runtime":
                continue
            param_name = str(binding.get("param_name") or "").strip()
            runtime_key = str(binding.get("runtime_var_name") or param_name).strip()
            if not runtime_key:
                continue
            required.append((stream_name, param_name or runtime_key, runtime_key))
    return required


def _missing_runtime_params(source: dict, runtime_values: Dict[str, Any]) -> List[Tuple[str, str, str]]:
    missing: List[Tuple[str, str, str]] = []
    provided = dict(runtime_values or {})
    default_by_key: Dict[str, Any] = {}
    for stream in source.get("streams") or []:
        defaults = stream.get("path_param_defaults") or {}
        if not isinstance(defaults, dict):
            continue
        for k, v in defaults.items():
            key = str(k or "").strip()
            text = str(v).strip() if v is not None else ""
            if not key or not text:
                continue
            default_by_key.setdefault(key, text)

    for stream_name, param_name, runtime_key in _collect_required_manual_runtime_params(source):
        if runtime_key not in provided and runtime_key in default_by_key:
            provided[runtime_key] = default_by_key[runtime_key]
        if param_name and runtime_key not in provided and param_name in default_by_key:
            provided[runtime_key] = default_by_key[param_name]
        raw = provided.get(runtime_key, "")
        if raw is None or not str(raw).strip():
            missing.append((stream_name, param_name, runtime_key))
    return missing


def _merge_runtime_values_with_stream_defaults(source: dict, runtime_values: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(runtime_values or {})
    for stream in source.get("streams") or []:
        defaults = stream.get("path_param_defaults") or {}
        if not isinstance(defaults, dict):
            continue
        for k, v in defaults.items():
            key = str(k or "").strip()
            text = str(v).strip() if v is not None else ""
            if not key or not text:
                continue
            if key not in merged or merged.get(key) is None or not str(merged.get(key)).strip():
                merged[key] = text
    return merged


def _normalize_attr_token(value: str) -> str:
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


def _source_for_inference_validation(source: dict, target_stream_id: Optional[str]) -> dict:
    """Return source scoped to the selected stream and its ancestors for inference validation."""
    if not target_stream_id:
        return source

    streams = source.get("streams") or []
    by_token: Dict[str, dict] = {}
    for stream in streams:
        if not isinstance(stream, dict):
            continue
        sid = str(stream.get("id") or "").strip()
        name = str(stream.get("name") or "").strip()
        if sid:
            by_token[sid] = stream
        if name:
            by_token[name] = stream

    target = by_token.get(str(target_stream_id).strip())
    if not target:
        return source

    needed: set[str] = set()

    def visit(stream: dict) -> None:
        sid = _stream_identity(stream)
        if not sid or sid in needed:
            return
        needed.add(sid)
        parent_id = _parent_stream_id(stream)
        parent = by_token.get(parent_id)
        if parent:
            visit(parent)

    visit(target)

    scoped = deepcopy(source)
    scoped["streams"] = [
        deepcopy(stream)
        for stream in streams
        if isinstance(stream, dict) and _stream_identity(stream) in needed
    ]
    return scoped


def _template_values_for_stream(stream: dict) -> List[str]:
    values = [str(stream.get("endpoint_path") or "")]
    body_template = stream.get("body_template")
    if isinstance(body_template, str):
        values.append(body_template)
    for mapping_key in ("headers", "query_params"):
        mapping = stream.get(mapping_key)
        if isinstance(mapping, dict):
            values.extend(str(value) for value in mapping.values() if isinstance(value, str))
    return values


def _blocked_auto_inference_target(source: dict, target_stream_id: Optional[str]) -> Optional[Tuple[str, str]]:
    streams = source.get("streams") or []
    target: Optional[dict] = None
    if target_stream_id:
        wanted = str(target_stream_id).strip()
        target = next(
            (
                stream
                for stream in streams
                if isinstance(stream, dict)
                and str(stream.get("id") or stream.get("name") or "").strip() == wanted
            ),
            None,
        )
    else:
        primary_id = str(source.get("primary_stream_id") or "").strip()
        target = next(
            (
                stream
                for stream in streams
                if isinstance(stream, dict)
                and (
                    (primary_id and str(stream.get("id") or "").strip() == primary_id)
                    or bool(stream.get("is_primary"))
                    or bool(stream.get("entity"))
                )
            ),
            None,
        )
    if not target:
        return None
    method = str(target.get("method") or "GET").strip().upper()
    if method in AUTO_INFERENCE_BLOCKED_METHODS:
        return (str(target.get("name") or target.get("id") or "stream").strip(), method)
    return None


def _rest_inference_request_issues(source: dict) -> List[str]:
    issues: List[str] = []
    for stream in source.get("streams") or []:
        if not isinstance(stream, dict):
            continue
        stream_name = str(stream.get("name") or stream.get("id") or "stream").strip()
        body_format = str(stream.get("body_format") or "empty").strip().lower()
        body_template = str(stream.get("body_template") or "")
        if body_format != "empty" and not body_template.strip():
            issues.append(f"{stream_name}: body_format={body_format} requires a request body template")
    return issues


def _missing_template_param_resolutions(source: dict) -> List[Tuple[str, List[str]]]:
    """
    Return [(stream_name, missing_vars)] for REST streams that still have
    unresolved endpoint/header/query/body placeholders.

    Resolution rules:
      - Root stream: requires stream.path_param_defaults[var]
      - Child stream: resolved by same-named extracted parent attribute OR
        stream.path_param_defaults[var]
    """
    streams = source.get("streams") or []
    by_id: Dict[str, dict] = {}
    for stream in streams:
        sid = str(stream.get("id") or "").strip()
        if sid:
            by_id[sid] = stream

    def _stream_attr_set(stream: Optional[dict]) -> set:
        attrs: set = set()
        if not stream:
            return attrs
        for rule in stream.get("extraction_rules") or []:
            if not isinstance(rule, dict):
                continue
            name = str(rule.get("attribute_name") or "").strip()
            if not name:
                continue
            attrs.add(_normalize_attr_token(name))
        return attrs

    def _upstream_attr_set(stream: dict) -> set:
        attrs: set = set()
        seen: set = set()
        parent_id = _parent_stream_id(stream)
        while parent_id and parent_id not in seen:
            seen.add(parent_id)
            parent_stream = by_id.get(parent_id)
            if not parent_stream:
                break
            attrs.update(_stream_attr_set(parent_stream))
            parent_id = _parent_stream_id(parent_stream)
        return attrs

    missing: List[Tuple[str, List[str]]] = []
    for stream in streams:
        parent_id = _parent_stream_id(stream)
        is_child = bool(parent_id)
        parent_attrs = _upstream_attr_set(stream)
        placeholders: List[str] = []
        for template_value in _template_values_for_stream(stream):
            if not template_value:
                continue
            for match in TEMPLATE_PLACEHOLDER_PATTERN.finditer(template_value):
                key = str(match.group(1) or "").strip()
                if key:
                    placeholders.append(key)
        if not placeholders:
            continue
        defaults = stream.get("path_param_defaults") or {}
        defaults_map = defaults if isinstance(defaults, dict) else {}
        unresolved = sorted(
            {
                key
                for key in placeholders
                if (
                    not str(defaults_map.get(key) or "").strip()
                    and (not is_child or _normalize_attr_token(key) not in parent_attrs)
                )
            }
        )
        if unresolved:
            stream_name = str(stream.get("name") or stream.get("id") or "stream").strip()
            missing.append((stream_name, unresolved))
    return missing


class AcceptSchemaRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "namespace": "com.nif",
                }
            ]
        },
    )

    namespace: Optional[str] = "com.nif"


def _to_response(job: dict) -> dict:
    job = dict(job)
    job.pop("_id", None)
    for field in ("created_at", "updated_at"):
        if isinstance(job.get(field), datetime):
            job[field] = job[field].isoformat()
    return job


@router.post(
    "/start",
    status_code=202,
    summary="Start schema inference",
    description="Deploy a temporary inference flow, collect Kafka samples, infer an Avro schema, and save it as Needs Verification.",
)
async def start_inference(req: StartInferenceRequest, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Start schema inference for a flow."""
    # Validate flow exists
    flow = await db.flows.find_one({"id": req.flow_id}, {"_id": 0})
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")

    # Validate source exists
    source = await db.sources.find_one({"id": flow.get("source_id")}, {"_id": 0})
    if not source:
        raise HTTPException(status_code=400, detail="Flow source configuration not found")
    source = await resolve_source_application_service(db, source)

    source_type = source.get("type")
    validation_source = _source_for_inference_validation(source, req.entity_stream_id)
    if source_type == "REST API":
        blocked_target = _blocked_auto_inference_target(source, req.entity_stream_id)
        if blocked_target:
            stream_name, method = blocked_target
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Automatic schema inference is not available for {method} stream {stream_name}. "
                    "Use an existing schema."
                ),
            )
        request_issues = _rest_inference_request_issues(validation_source)
        if request_issues:
            raise HTTPException(
                status_code=422,
                detail="Invalid REST request configuration for schema inference. " + "; ".join(request_issues),
            )
        missing_defaults = _missing_template_param_resolutions(validation_source)
        if missing_defaults:
            msg_parts = [f"{stream}: {', '.join(keys)}" for stream, keys in missing_defaults]
            raise HTTPException(
                status_code=422,
                detail=(
                    "Missing template value resolution for REST streams. "
                    "Go back to Stream setup and add defaults or extract same-named parent attributes: "
                    + "; ".join(msg_parts)
                ),
            )

    runtime_values = _merge_runtime_values_with_stream_defaults(validation_source, req.runtime_values or {})
    if source_type == "REST API":
        missing = _missing_runtime_params(validation_source, runtime_values)
        if missing:
            by_stream: Dict[str, List[str]] = {}
            for stream_name, _param_name, runtime_key in missing:
                by_stream.setdefault(stream_name, []).append(runtime_key)
            msg_parts = [f"{stream}: {', '.join(sorted(set(keys)))}" for stream, keys in by_stream.items()]
            raise HTTPException(
                status_code=422,
                detail="Missing required runtime params for inference. " + "; ".join(msg_parts),
            )
    await require_runtime_connections(db, action="run schema inference")
    nifi_conn = await resolve_connection(db, "nifi")
    kafka_conn = await resolve_connection(db, "kafka")

    # Determine Kafka topic: entity stream overrides flow-level kafka_topic
    kafka_topic = flow.get("kafka_topic") or source.get("kafka_output", {}).get("topic") or flow.get("name", "topic")
    if req.entity_stream_id:
        # Find entity stream by ID
        streams = source.get("streams") or []
        entity_stream = next((s for s in streams if s.get("id") == req.entity_stream_id), None)
        if not entity_stream:
            raise HTTPException(status_code=422, detail="Target entity stream was not found on this flow source.")
        entity_cfg = entity_stream.get("entity") or {}
        entity_kafka = entity_cfg.get("kafka") or {}
        entity_topic = entity_kafka.get("topic") or ""
        if not entity_topic:
            raise HTTPException(status_code=422, detail="Target entity stream has no generated Kafka topic.")
        kafka_topic = entity_topic
        logger.info(f"Using entity stream '{entity_stream.get('name')}' topic: {kafka_topic}")
    inference_topic = f"{kafka_topic}-schema-inference"

    # Atomically acquire an inference lock on the flow to prevent concurrent jobs.
    lock_claim = await db.flows.find_one_and_update(
        {"id": req.flow_id, "schema_inference_active": {"$ne": True}},
        {"$set": {"schema_inference_active": True, "updated_at": datetime.utcnow()}},
        return_document=ReturnDocument.AFTER,
        projection={"_id": 0, "id": 1},
    )
    if not lock_claim:
        existing_job = await db.schema_inference_jobs.find_one(
            {
                "flow_id": req.flow_id,
                "status": {"$in": ["deploying_nifi", "nifi_running", "collecting", "inferring"]},
            },
            {"_id": 0, "id": 1},
            sort=[("created_at", -1)],
        )
        suffix = f" (job {existing_job['id']})" if existing_job and existing_job.get("id") else ""
        raise HTTPException(status_code=409, detail=f"Schema inference is already running for this flow{suffix}. Stop it first.")

    # Create inference job
    now = datetime.utcnow()
    job = SchemaInferenceJob(
        flow_id=req.flow_id,
        flow_name=flow.get("name", req.flow_id),
        kafka_topic=kafka_topic,
        inference_topic=inference_topic,
        entity_stream_id=req.entity_stream_id or None,
        target_messages=req.target_messages,
    )
    job_doc = job.dict()
    job_doc["created_at"] = now
    job_doc["updated_at"] = now
    job_doc["worker_instance_id"] = APP_INSTANCE_ID
    job_doc["heartbeat_at"] = now
    try:
        await db.schema_inference_jobs.insert_one(job_doc)
    except Exception:
        await db.flows.update_one(
            {"id": req.flow_id},
            {"$set": {"schema_inference_active": False, "updated_at": datetime.utcnow()}},
        )
        raise

    # Link job to flow
    await db.flows.update_one(
        {"id": req.flow_id},
        {"$set": {"schema_inference_job_id": job.id, "updated_at": now}},
    )

    # Start background task
    try:
        from services.schema_inference_runner import run_inference_background
        asyncio.create_task(
            run_inference_background(
                job_id=job.id,
                flow_id=req.flow_id,
                source=source,
                flow=flow,
                nifi_conn=nifi_conn,
                kafka_conn=kafka_conn,
                inference_topic=inference_topic,
                target_messages=req.target_messages,
                runtime_values=runtime_values,
                db=db,
                entity_stream_id=req.entity_stream_id,
                entity_kafka_topic=kafka_topic if req.entity_stream_id else None,
            )
        )
    except Exception as exc:
        await db.schema_inference_jobs.update_one(
            {"id": job.id},
            {"$set": {"status": "failed", "error": f"Failed to start inference worker: {str(exc)[:180]}", "updated_at": datetime.utcnow()}},
        )
        await db.flows.update_one(
            {"id": req.flow_id},
            {"$set": {"schema_inference_active": False, "updated_at": datetime.utcnow()}},
        )
        raise HTTPException(status_code=500, detail="Failed to start inference worker")

    logger.info(f"Started schema inference job {job.id} for flow {req.flow_id}")
    return _to_response(job_doc)


@router.get("/flow/{flow_id}")
async def get_inference_job_for_flow(
    flow_id: str,
    entity_stream_id: Optional[str] = Query(default=None),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Get the most recent schema inference job for a flow."""
    flow = await db.flows.find_one({"id": flow_id}, {"_id": 0})
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found")
    query = {"flow_id": flow_id}
    if entity_stream_id:
        query["entity_stream_id"] = entity_stream_id
    job = await db.schema_inference_jobs.find_one(
        query,
        {"_id": 0},
        sort=[("created_at", -1)],
    )
    if not job:
        return None
    job = await _cleanup_terminal_inference_pg(db, job)
    return _to_response(job)


@router.get("/{job_id}")
async def get_inference_job(job_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Get schema inference job status."""
    job = await db.schema_inference_jobs.find_one({"id": job_id}, {"_id": 0})
    if not job:
        raise HTTPException(status_code=404, detail="Inference job not found")
    job = await _cleanup_terminal_inference_pg(db, job)
    return _to_response(job)


@router.post("/{job_id}/stop")
async def stop_inference(job_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Stop an active inference job."""
    job = await db.schema_inference_jobs.find_one({"id": job_id}, {"_id": 0})
    if not job:
        raise HTTPException(status_code=404, detail="Inference job not found")

    active_states = ["deploying_nifi", "nifi_running", "collecting", "inferring"]
    if job.get("status") not in active_states:
        raise HTTPException(status_code=400, detail=f"Job is not active (status: {job.get('status')}). Cannot stop.")

    now = datetime.utcnow()
    await db.schema_inference_jobs.update_one(
        {"id": job_id},
        {"$set": {"status": "stopped", "updated_at": now}},
    )
    await db.flows.update_one(
        {"id": job.get("flow_id")},
        {"$set": {"schema_inference_active": False, "updated_at": now}},
    )

    # Stop and delete NiFi PG if any
    pg_id = job.get("nifi_pg_id")
    if pg_id:
        nifi_conn = await resolve_connection(db, "nifi")
        if nifi_conn:
            try:
                from services.schema_inference_runner import _delete_inference_nifi_pg
                asyncio.create_task(_delete_inference_nifi_pg(nifi_conn, pg_id))
            except Exception as e:
                logger.warning(f"Could not schedule NiFi PG deletion: {e}")

    return {"ok": True, "status": "stopped"}


@router.post("/{job_id}/accept")
async def accept_inference_schema(
    job_id: str,
    req: AcceptSchemaRequest = Body(...),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """
    Accept the generated schema from an inference job.
    - Schema is already saved as 'Needs Verification' (done in the background runner)
    - Links the schema to the flow if not already linked
    - Deletes the inference NiFi process group
    - Returns the schema_artifact_id and schema_version for navigation
    """
    job = await db.schema_inference_jobs.find_one({"id": job_id}, {"_id": 0})
    if not job:
        raise HTTPException(status_code=404, detail="Inference job not found")
    if job.get("status") != "complete":
        raise HTTPException(status_code=400, detail=f"Job is not complete (status: {job.get('status')}). Cannot accept.")
    if not job.get("schema_artifact_id"):
        raise HTTPException(status_code=400, detail="No schema was generated by this job.")

    schema_artifact_id = job["schema_artifact_id"]
    schema_version = job["schema_version"]
    artifact = await db.schema_artifacts.find_one({"artifact_id": schema_artifact_id}, {"_id": 0, "versions": 1})
    if not artifact:
        raise HTTPException(status_code=409, detail="Generated schema artifact no longer exists. Retry inference.")
    versions = artifact.get("versions", [])
    if not any(v.get("version") == schema_version for v in versions):
        raise HTTPException(status_code=409, detail="Generated schema version no longer exists. Retry inference.")

    # Ensure flow is linked to the schema (should already be done in runner, but be idempotent)
    flow_id = job["flow_id"]
    await db.flows.update_one(
        {"id": flow_id},
        {"$set": {
            "schema_artifact_id": schema_artifact_id,
            "schema_version": schema_version,
            "updated_at": datetime.utcnow(),
        }},
    )

    entity_stream_id = job.get("entity_stream_id")
    if entity_stream_id:
        flow = await db.flows.find_one({"id": flow_id}, {"_id": 0, "source_id": 1})
        if flow and flow.get("source_id"):
            source = await db.sources.find_one({"id": flow.get("source_id")}, {"_id": 0, "streams": 1})
            if source:
                streams = source.get("streams") or []
                changed = False
                for stream in streams:
                    if stream.get("id") != entity_stream_id:
                        continue
                    entity = stream.get("entity") or {}
                    kafka_cfg = entity.get("kafka") or {}
                    kafka_cfg["schema_artifact_id"] = schema_artifact_id
                    kafka_cfg["schema_version"] = schema_version
                    kafka_cfg["schema_mode"] = "existing"
                    entity["kafka"] = kafka_cfg
                    entity["schema_artifact_id"] = schema_artifact_id
                    entity["schema_version"] = schema_version
                    stream["entity"] = entity
                    changed = True
                    break
                if changed:
                    await db.sources.update_one(
                        {"id": flow.get("source_id")},
                        {"$set": {"streams": streams, "updated_at": datetime.utcnow()}},
                    )

    # Delete the inference NiFi PG
    pg_id = job.get("nifi_pg_id")
    if pg_id:
        nifi_conn = await resolve_connection(db, "nifi")
        if nifi_conn:
            try:
                from services.schema_inference_runner import _delete_inference_nifi_pg
                asyncio.create_task(_delete_inference_nifi_pg(nifi_conn, pg_id))
            except Exception as e:
                logger.warning(f"Could not schedule NiFi PG deletion: {e}")

    logger.info(f"Accepted schema {schema_artifact_id} v{schema_version} from inference job {job_id}")

    # Audit
    try:
        from models.audit import AuditEvent
        event = AuditEvent(
            action="Accepted inferred schema",
            object_type="Schema",
            target=schema_artifact_id,
            status="Success",
            details=f"From inference job {job_id}, {job.get('messages_collected', 0)} messages sampled",
        )
        await db.audit_events.insert_one(event.dict())
    except Exception:
        pass

    return {
        "ok": True,
        "schema_artifact_id": schema_artifact_id,
        "schema_version": schema_version,
        "messages_sampled": job.get("messages_collected", 0),
    }
