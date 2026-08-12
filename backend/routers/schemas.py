import asyncio
import logging
import uuid
import json
import re
from datetime import datetime
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Depends, Body
from motor.motor_asyncio import AsyncIOMotorDatabase
from fastavro import parse_schema

from db import get_db
from models.schema_artifact import SchemaArtifact, SchemaArtifactCreate, SchemaVersion
from services import content_store
from services.schema_inferencer import infer_avro_schema
from services.connection_resolver import resolve_connection

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/schemas", tags=["schemas"])
_schema_operation_locks: Dict[str, asyncio.Lock] = {}


def _schema_operation_lock(artifact_id: str) -> asyncio.Lock:
    lock = _schema_operation_locks.get(artifact_id)
    if lock is None:
        lock = asyncio.Lock()
        _schema_operation_locks[artifact_id] = lock
    return lock


async def _log_audit(db, action, object_type, target, status, details=None):
    from models.audit import AuditEvent
    if details is not None and not isinstance(details, str):
        details = json.dumps(details, default=str)
    event = AuditEvent(action=action, object_type=object_type, target=target or "", status=status, details=details)
    await db.audit_events.insert_one(event.dict())


async def _unlink_flows_for_schema(
    db: AsyncIOMotorDatabase, artifact_id: str, version: Optional[int] = None
) -> int:
    query: Dict[str, Any] = {"schema_artifact_id": artifact_id}
    if version is not None:
        query["schema_version"] = version
    affected_flows = await db.flows.find(query, {"_id": 0, "id": 1}).to_list(1000)
    result = await db.flows.update_many(
        query,
        {"$set": {"schema_artifact_id": None, "schema_version": None, "updated_at": datetime.utcnow()}},
    )
    for flow in affected_flows:
        if flow.get("id"):
            await content_store.sync_flow(db, flow["id"])
    return int(result.modified_count)


def _schema_ref_matches(entity: Dict[str, Any], artifact_id: str, version: Optional[int]) -> bool:
    kafka_cfg = entity.get("kafka") or {}
    if not isinstance(kafka_cfg, dict):
        kafka_cfg = {}

    refs = [
        (entity.get("schema_artifact_id"), entity.get("schema_version")),
        (kafka_cfg.get("schema_artifact_id"), kafka_cfg.get("schema_version")),
    ]
    for ref_artifact_id, ref_version in refs:
        if ref_artifact_id != artifact_id:
            continue
        if version is None:
            return True
        try:
            if int(ref_version) == version:
                return True
        except Exception:
            continue
    return False


async def _unlink_source_streams_for_schema(
    db: AsyncIOMotorDatabase, artifact_id: str, version: Optional[int] = None
) -> int:
    modified = 0
    sources = await db.sources.find({}, {"_id": 0, "id": 1, "streams": 1}).to_list(1000)
    for source in sources:
        streams = source.get("streams") or []
        changed = False
        for stream in streams:
            if not isinstance(stream, dict):
                continue
            entity = stream.get("entity") or {}
            if not isinstance(entity, dict) or not _schema_ref_matches(entity, artifact_id, version):
                continue

            kafka_cfg = entity.get("kafka") or {}
            if not isinstance(kafka_cfg, dict):
                kafka_cfg = {}

            if entity.get("schema_artifact_id") == artifact_id:
                entity["schema_artifact_id"] = None
                entity["schema_version"] = None
            if kafka_cfg.get("schema_artifact_id") == artifact_id:
                kafka_cfg["schema_artifact_id"] = None
                kafka_cfg["schema_version"] = None
                kafka_cfg["schema_mode"] = "auto_generate"

            entity["kafka"] = kafka_cfg
            stream["entity"] = entity
            changed = True

        if changed and source.get("id"):
            await db.sources.update_one(
                {"id": source["id"]},
                {"$set": {"streams": streams, "updated_at": datetime.utcnow()}},
            )
            await content_store.sync_source(db, source["id"])
            modified += 1
    return modified


async def _blocking_schema_flows(
    db: AsyncIOMotorDatabase,
    artifact_id: str,
    version: Optional[int] = None,
) -> List[Dict[str, Any]]:
    query: Dict[str, Any] = {"schema_artifact_id": artifact_id}
    if version is not None:
        query["schema_version"] = version
    linked = await db.flows.find(query, {"_id": 0, "id": 1, "name": 1, "state": 1, "nifi_process_group_id": 1}).to_list(200)
    blocking = [
        flow
        for flow in linked
        if flow.get("state") in {"Running", "Deploying"} or bool(flow.get("nifi_process_group_id"))
    ]
    return blocking


def _to_response(artifact: dict) -> dict:
    artifact = dict(artifact)
    artifact.pop("_id", None)
    if artifact.get("avro_schema") is None:
        artifact.pop("avro_schema", None)
    for field in ("created_at", "updated_at"):
        if isinstance(artifact.get(field), datetime):
            artifact[field] = artifact[field].isoformat()
    for version in artifact.get("versions", []):
        for f in ("created_at", "updated_at", "verified_at"):
            if isinstance(version.get(f), datetime):
                version[f] = version[f].isoformat()
    return artifact


def _avro_safe_name(value: str, fallback: str = "SchemaRecord") -> str:
    token = re.sub(r"[^A-Za-z0-9_]", "_", value or "").strip("_")
    if not token:
        token = fallback
    if not re.match(r"^[A-Za-z_]", token):
        token = f"_{token}"
    return token


def _validate_avro_schema(schema: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(schema, dict):
        raise HTTPException(status_code=422, detail="Avro schema must be a JSON object.")
    try:
        parse_schema(schema)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Invalid Avro schema: {str(exc)[:240]}") from exc
    return schema


@router.get("/")
async def list_artifacts(db: AsyncIOMotorDatabase = Depends(get_db)):
    artifacts = await db.schema_artifacts.find({}, {"_id": 0}).sort("updated_at", -1).to_list(200)
    return [_to_response(a) for a in artifacts]


@router.get("/{artifact_id}")
async def get_artifact(artifact_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    art = await db.schema_artifacts.find_one({"artifact_id": artifact_id}, {"_id": 0})
    if not art:
        raise HTTPException(status_code=404, detail="Schema artifact not found")
    return _to_response(art)


@router.post(
    "/",
    status_code=201,
    summary="Create schema artifact",
    description="Create an Avro schema artifact with initial draft version 1.",
)
async def create_artifact(data: SchemaArtifactCreate, db: AsyncIOMotorDatabase = Depends(get_db)):
    existing = await db.schema_artifacts.find_one({"artifact_id": data.artifact_id})
    if existing:
        raise HTTPException(status_code=409, detail="Artifact ID already exists")
    now = datetime.utcnow()
    artifact = SchemaArtifact(**data.dict(exclude={"avro_schema"}))
    # Create initial v1 Draft version
    initial_schema = data.avro_schema or {
        "type": "record",
        "name": _avro_safe_name(data.artifact_id),
        "namespace": data.namespace or "com.nif",
        "fields": [],
    }
    _validate_avro_schema(initial_schema)
    initial_version = SchemaVersion(
        version=1,
        status="Draft",
        avro_schema=initial_schema,
        raw_avro=json.dumps(initial_schema, indent=2),
    )
    artifact.versions = [initial_version]
    doc = artifact.dict()
    doc.pop("avro_schema", None)
    doc["created_at"] = now
    doc["updated_at"] = now
    await db.schema_artifacts.insert_one(doc)
    await _log_audit(db, "Created schema artifact", "Schema", data.artifact_id, "Success")
    await content_store.sync_schema_artifact(db, data.artifact_id)
    return _to_response(doc)


@router.delete("/{artifact_id}", status_code=204)
async def delete_artifact(artifact_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    lock = _schema_operation_lock(artifact_id)
    if lock.locked():
        raise HTTPException(status_code=409, detail="Schema operation is already in progress.")
    async with lock:
        return await _delete_artifact_locked(artifact_id, db)


async def _delete_artifact_locked(artifact_id: str, db: AsyncIOMotorDatabase):
    existing = await db.schema_artifacts.find_one({"artifact_id": artifact_id})
    if not existing:
        raise HTTPException(status_code=404, detail="Schema artifact not found")
    blocking = await _blocking_schema_flows(db, artifact_id)
    if blocking:
        names = [f.get("name") or f.get("id") for f in blocking]
        raise HTTPException(
            status_code=409,
            detail=(
                "Schema is linked to active or deployed flows. Stop and undeploy these flows first: "
                + ", ".join(names[:10])
            ),
        )
    affected = await _unlink_flows_for_schema(db, artifact_id)
    affected_sources = await _unlink_source_streams_for_schema(db, artifact_id)
    await db.schema_artifacts.delete_one({"artifact_id": artifact_id})
    await _log_audit(
        db,
        "Deleted schema artifact",
        "Schema",
        artifact_id,
        "Success",
        details={"affected_flows": affected, "affected_sources": affected_sources},
    )
    content_store.delete_schema_artifact(artifact_id)


@router.delete("/{artifact_id}/versions/{version}", status_code=204)
async def delete_artifact_version(artifact_id: str, version: int, db: AsyncIOMotorDatabase = Depends(get_db)):
    lock = _schema_operation_lock(artifact_id)
    if lock.locked():
        raise HTTPException(status_code=409, detail="Schema operation is already in progress.")
    async with lock:
        return await _delete_artifact_version_locked(artifact_id, version, db)


async def _delete_artifact_version_locked(
    artifact_id: str,
    version: int,
    db: AsyncIOMotorDatabase,
):
    art = await db.schema_artifacts.find_one({"artifact_id": artifact_id})
    if not art:
        raise HTTPException(status_code=404, detail="Schema artifact not found")

    versions = art.get("versions", [])
    target = next((v for v in versions if v.get("version") == version), None)
    if not target:
        raise HTTPException(status_code=404, detail=f"Version {version} not found")
    if len(versions) <= 1:
        raise HTTPException(
            status_code=400,
            detail="Cannot delete the only version. Delete the whole schema artifact instead.",
        )
    blocking = await _blocking_schema_flows(db, artifact_id, version)
    if blocking:
        names = [f.get("name") or f.get("id") for f in blocking]
        raise HTTPException(
            status_code=409,
            detail=(
                "Schema version is linked to active or deployed flows. Stop and undeploy these flows first: "
                + ", ".join(names[:10])
            ),
        )

    remaining = [v for v in versions if v.get("version") != version]
    await db.schema_artifacts.update_one(
        {"artifact_id": artifact_id},
        {"$set": {"versions": remaining, "updated_at": datetime.utcnow()}},
    )
    affected = await _unlink_flows_for_schema(db, artifact_id, version)
    affected_sources = await _unlink_source_streams_for_schema(db, artifact_id, version)
    await _log_audit(
        db,
        "Deleted schema version",
        "Schema",
        f"{artifact_id} v{version}",
        "Success",
        details={"affected_flows": affected, "affected_sources": affected_sources},
    )
    await content_store.sync_schema_artifact(db, artifact_id)


@router.put("/{artifact_id}/versions/{version}")
async def update_version_schema(
    artifact_id: str,
    version: int,
    avro_schema: Dict[str, Any] = Body(
        ...,
        examples=[
            {
                "type": "record",
                "name": "DummyJsonUser",
                "namespace": "com.nif",
                "fields": [
                    {"name": "id", "type": ["null", "int"], "default": None},
                    {"name": "firstName", "type": ["null", "string"], "default": None},
                ],
            }
        ],
    ),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Update the Avro schema content of a specific version."""
    avro_schema = _validate_avro_schema(avro_schema)
    art = await db.schema_artifacts.find_one({"artifact_id": artifact_id})
    if not art:
        raise HTTPException(status_code=404, detail="Artifact not found")

    versions = art.get("versions", [])
    target = next((v for v in versions if v["version"] == version), None)
    if not target:
        raise HTTPException(status_code=404, detail=f"Version {version} not found")

    now = datetime.utcnow()
    for v in versions:
        if v["version"] == version:
            if v["status"] == "Verified":
                # Fork a new version
                new_ver_num = max(v2["version"] for v2 in versions) + 1
                new_ver = SchemaVersion(
                    version=new_ver_num,
                    status="Needs Verification",
                    avro_schema=avro_schema,
                    raw_avro=json.dumps(avro_schema, indent=2),
                )
                versions.append(new_ver.dict())
                await db.schema_artifacts.update_one(
                    {"artifact_id": artifact_id},
                    {"$set": {"versions": versions, "updated_at": now}}
                )
                await content_store.sync_schema_artifact(db, artifact_id)
                return {"forked": True, "new_version": new_ver_num}
            else:
                v["avro_schema"] = avro_schema
                v["raw_avro"] = json.dumps(avro_schema, indent=2)
                v["updated_at"] = now
                break

    await db.schema_artifacts.update_one(
        {"artifact_id": artifact_id},
        {"$set": {"versions": versions, "updated_at": now}}
    )
    await content_store.sync_schema_artifact(db, artifact_id)
    return {"forked": False, "version": version}


@router.post("/{artifact_id}/versions/{version}/generate")
async def generate_version(
    artifact_id: str,
    version: int,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Mark a version as Needs Verification (after generation)."""
    art = await db.schema_artifacts.find_one({"artifact_id": artifact_id})
    if not art:
        raise HTTPException(status_code=404, detail="Artifact not found")

    versions = art.get("versions", [])
    now = datetime.utcnow()
    for v in versions:
        if v["version"] == version:
            if v["status"] == "Verified":
                raise HTTPException(status_code=400, detail="Cannot re-generate a Verified version. Edit it to fork a new version.")
            v["status"] = "Needs Verification"
            v["updated_at"] = now
            break
    else:
        raise HTTPException(status_code=404, detail=f"Version {version} not found")

    await db.schema_artifacts.update_one(
        {"artifact_id": artifact_id},
        {"$set": {"versions": versions, "updated_at": now}}
    )
    await _log_audit(db, "Generated schema", "Schema", f"{artifact_id} v{version}", "Success")
    await content_store.sync_schema_artifact(db, artifact_id)
    return {"status": "Needs Verification", "version": version}


@router.post(
    "/{artifact_id}/versions/{version}/verify",
    summary="Verify schema version",
    description="Validate the Avro schema and register it with Apicurio when a registry connection exists.",
)
async def verify_version(
    artifact_id: str,
    version: int,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Verify a schema version and optionally register it in Apicurio."""
    async with _schema_operation_lock(artifact_id):
        return await _verify_version_locked(artifact_id, version, db)


async def _verify_version_locked(
    artifact_id: str,
    version: int,
    db: AsyncIOMotorDatabase,
):
    art = await db.schema_artifacts.find_one({"artifact_id": artifact_id})
    if not art:
        raise HTTPException(status_code=404, detail="Artifact not found")

    versions = art.get("versions", [])
    now = datetime.utcnow()
    target_version = None
    for v in versions:
        if v["version"] == version:
            target_version = v
            break
    else:
        raise HTTPException(status_code=404, detail=f"Version {version} not found")

    assert target_version is not None
    if target_version.get("status") == "Verified":
        verified_at = target_version.get("verified_at")
        return {
            "status": "Verified",
            "version": version,
            "verified_at": verified_at.isoformat() if isinstance(verified_at, datetime) else verified_at,
            "apicurio": None,
        }
    verified_schema = target_version.get("avro_schema")
    if not verified_schema:
        raise HTTPException(status_code=400, detail="Cannot verify an empty schema version.")
    _validate_avro_schema(verified_schema)

    # Try to register in Apicurio if configured
    apicurio_result = None
    apicurio_conn = await resolve_connection(db, "apicurio")
    if apicurio_conn:
        from services.apicurio_client import register_schema
        apicurio_result = await register_schema(
            url=apicurio_conn.get("endpoint", ""),
            group_id=apicurio_conn.get("group_id", "nif-platform"),
            artifact_id=artifact_id,
            avro_schema=verified_schema,
            auth_type=apicurio_conn.get("auth_type", "NONE"),
            username=apicurio_conn.get("username"),
            password=apicurio_conn.get("password"),
            token=apicurio_conn.get("token"),
        )
        if not apicurio_result.get("ok"):
            raise HTTPException(
                status_code=400,
                detail="Schema verification failed: schema registry rejected the schema or is unavailable.",
            )

    # Mark as verified only after registry registration succeeds (or when no registry is configured).
    target_version["status"] = "Verified"
    target_version["verified_at"] = now
    target_version["verified_by"] = "admin"
    target_version["updated_at"] = now
    if apicurio_result and apicurio_result.get("global_id") is not None:
        target_version["apicurio_global_id"] = apicurio_result["global_id"]
        if apicurio_conn:
            target_version["apicurio_connection_id"] = apicurio_conn.get("id")

    await db.schema_artifacts.update_one(
        {"artifact_id": artifact_id},
        {"$set": {"versions": versions, "updated_at": now}}
    )

    await _log_audit(db, "Verified schema", "Schema", f"{artifact_id} v{version}", "Success")
    await content_store.sync_schema_artifact(db, artifact_id)
    return {
        "status": "Verified",
        "version": version,
        "verified_at": now.isoformat(),
        "apicurio": apicurio_result,
    }

# Note: /infer endpoint removed - schema inference from sample data has been discontinued.
