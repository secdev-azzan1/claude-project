import logging
import re
from datetime import datetime
from typing import List
from fastapi import APIRouter, HTTPException, Depends
from motor.motor_asyncio import AsyncIOMotorDatabase
import uuid

from db import get_db
from models.connection import Connection, ConnectionCreate, ConnectionUpdate, ConnectionRepoint
from services.nifi_client import test_nifi_connection
from services.kafka_client import test_kafka_connection, test_kafbat_connection
from services.apicurio_client import test_apicurio_connection
from services.connection_fingerprint import (
    probe_nifi_fingerprint,
    probe_kafka_fingerprint,
    probe_apicurio_fingerprint,
    probe_kafka_connect_fingerprint,
    probe_iceberg_fingerprint,
)
from services import kafka_connect_client
from services import iceberg_catalog_client
from services import connection_lifecycle_runner

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/connections", tags=["connections"])


def _to_response(c: dict) -> dict:
    """Convert stored connection document to safe API response (no secrets)."""
    return {
        "id": c.get("id"),
        "name": c.get("name"),
        "type": c.get("type"),
        "description": c.get("description"),
        "endpoint": c.get("endpoint"),
        "auth_type": c.get("auth_type", "NONE"),
        "health": c.get("health", "Not Tested"),
        "last_tested": c.get("last_tested").isoformat() if c.get("last_tested") else None,
        "created_at": c.get("created_at").isoformat() if c.get("created_at") else None,
        "updated_at": c.get("updated_at").isoformat() if c.get("updated_at") else None,
        "security_protocol": c.get("security_protocol"),
        "sasl_mechanism": c.get("sasl_mechanism"),
        "sasl_username": c.get("sasl_username"),
        "default_topic_prefix": c.get("default_topic_prefix"),
        "kafka_connection_mode": c.get("kafka_connection_mode") or "native",
        "kafbat_url": c.get("kafbat_url"),
        "kafbat_username": c.get("kafbat_username"),
        "group_id": c.get("group_id"),
        "username": c.get("username"),
        "has_password": bool(c.get("password")),
        "has_token": bool(c.get("token")),
        "has_sasl_password": bool(c.get("sasl_password")),
        "has_kafbat_password": bool(c.get("kafbat_password")),
        "iceberg_warehouse": c.get("iceberg_warehouse"),
        "iceberg_oauth2_server_uri": c.get("iceberg_oauth2_server_uri"),
        "iceberg_scope": c.get("iceberg_scope"),
        "s3_endpoint": c.get("s3_endpoint"),
        "s3_access_key_id": c.get("s3_access_key_id"),
        "s3_region": c.get("s3_region"),
        "s3_path_style_access": c.get("s3_path_style_access"),
        "has_iceberg_credential": bool(c.get("iceberg_credential")),
        "has_s3_secret_access_key": bool(c.get("s3_secret_access_key")),
        "is_active": c.get("is_active", False),
        "reachability": c.get("reachability", "Unknown"),
    }


async def _log_audit(db: AsyncIOMotorDatabase, action: str, object_type: str, target: str, status: str, details: str = None):
    from models.audit import AuditEvent
    event = AuditEvent(action=action, object_type=object_type, target=target or "", status=status, details=details)
    await db.audit_events.insert_one(event.dict())


async def _connection_name_exists(
    db: AsyncIOMotorDatabase,
    name: str,
    exclude_id: str | None = None,
) -> bool:
    normalized = (name or "").strip()
    if not normalized:
        return False
    query = {
        "name": {
            "$regex": f"^{re.escape(normalized)}$",
            "$options": "i",
        }
    }
    if exclude_id:
        query["id"] = {"$ne": exclude_id}
    return await db.connections.find_one(query, {"_id": 1}) is not None


async def refresh_reachability(db: AsyncIOMotorDatabase, conn_type: str) -> str:
    """Probe active connection of conn_type and update reachability. Best-effort, never raises."""
    try:
        active_conn = await db.connections.find_one({"type": conn_type, "is_active": True}, {"_id": 0})
        if not active_conn:
            return "Unknown"

        # Get the appropriate probe function
        if conn_type == "nifi":
            result = await probe_nifi_fingerprint(active_conn)
        elif conn_type == "kafka":
            result = await probe_kafka_fingerprint(active_conn)
        elif conn_type == "apicurio":
            result = await probe_apicurio_fingerprint(active_conn)
        elif conn_type == "kafka_connect":
            result = await probe_kafka_connect_fingerprint(active_conn)
        elif conn_type == "iceberg":
            result = await probe_iceberg_fingerprint(active_conn)
        else:
            return "Unknown"

        reachability = "Reachable" if result.get("reachable") else "Unreachable"
        await db.connections.update_one(
            {"type": conn_type, "is_active": True},
            {"$set": {"reachability": reachability, "updated_at": datetime.utcnow()}}
        )
        return reachability
    except Exception:
        return "Unknown"


@router.get("/", summary="List service connections")
async def list_connections(db: AsyncIOMotorDatabase = Depends(get_db)):
    conns = await db.connections.find({}, {"_id": 0}).to_list(100)
    return [_to_response(c) for c in conns]


@router.get("/{conn_id}", summary="Get service connection")
async def get_connection(conn_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    conn = await db.connections.find_one({"id": conn_id}, {"_id": 0})
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")
    return _to_response(conn)


@router.post(
    "/",
    status_code=201,
    summary="Create service connection",
    description="Create a NiFi, Kafka, or Apicurio connection. Secrets are accepted but never returned directly.",
)
async def create_connection(data: ConnectionCreate, db: AsyncIOMotorDatabase = Depends(get_db)):
    if await _connection_name_exists(db, data.name):
        raise HTTPException(status_code=409, detail="Connection name already exists. Please choose a different name.")
    # Determine is_active: first of a type is auto-active, others are inactive
    existing_of_type = await db.connections.find_one({"type": data.type}, {"_id": 0, "id": 1})
    is_active = existing_of_type is None
    now = datetime.utcnow()
    doc = {
        "id": str(uuid.uuid4()),
        **data.dict(),
        "health": "Not Tested",
        "reachability": "Unknown",
        "last_tested": None,
        "created_at": now,
        "updated_at": now,
        "is_active": is_active,
    }
    await db.connections.insert_one(doc)
    await _log_audit(db, "Created connection", "Connection", data.name, "Success")
    return _to_response(doc)


@router.put("/{conn_id}", summary="Update service connection")
async def update_connection(conn_id: str, data: ConnectionUpdate, db: AsyncIOMotorDatabase = Depends(get_db)):
    existing = await db.connections.find_one({"id": conn_id}, {"_id": 0})
    if not existing:
        raise HTTPException(status_code=404, detail="Connection not found")
    if data.name and await _connection_name_exists(db, data.name, exclude_id=conn_id):
        raise HTTPException(status_code=409, detail="Connection name already exists. Please choose a different name.")

    # Check if update includes endpoint or auth field changes
    is_endpoint_change = data.endpoint is not None
    is_auth_change = any([
        data.auth_type is not None,
        data.username is not None,
        data.password is not None,
        data.token is not None,
    ])

    # If endpoint or auth would be changed, check for dependents
    if is_endpoint_change or is_auth_change:
        # Compute dependents: documents that reference THIS connection by id
        nifi_flow_count = await db.flows.count_documents({"nifi_connection_id": conn_id})
        kafka_flow_count = await db.flows.count_documents({"kafka_connection_id": conn_id})
        nifi_service_count = await db.nifi_global_services.count_documents({"nifi_connection_id": conn_id})
        schema_artifact_count = await db.schema_artifacts.count_documents({"versions.apicurio_connection_id": conn_id})

        total_dependents = nifi_flow_count + kafka_flow_count + nifi_service_count + schema_artifact_count

        # If there are dependents, reject the update
        if total_dependents > 0:
            raise HTTPException(
                status_code=409,
                detail="Changing the endpoint/auth of a connection with dependents must go through POST /{id}/repoint so the switch is tracked and dependents are handled."
            )

    updates = {k: v for k, v in data.dict(exclude_none=True).items()}
    # Don't clear secrets if empty string provided — keep existing
    for field in ("password", "token", "sasl_password", "kafbat_password", "iceberg_credential", "s3_secret_access_key"):
        if field in updates and not updates[field]:
            del updates[field]

    updates["updated_at"] = datetime.utcnow()
    await db.connections.update_one({"id": conn_id}, {"$set": updates})
    updated = await db.connections.find_one({"id": conn_id}, {"_id": 0})
    await _log_audit(db, "Updated connection", "Connection", existing.get("name"), "Success")
    return _to_response(updated)


@router.delete("/{conn_id}", status_code=204, summary="Delete service connection")
async def delete_connection(conn_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    existing = await db.connections.find_one({"id": conn_id})
    if not existing:
        raise HTTPException(status_code=404, detail="Connection not found")

    # Compute dependents: documents that reference THIS connection by id
    nifi_flow_count = await db.flows.count_documents({"nifi_connection_id": conn_id})
    kafka_flow_count = await db.flows.count_documents({"kafka_connection_id": conn_id})
    nifi_service_count = await db.nifi_global_services.count_documents({"nifi_connection_id": conn_id})
    schema_artifact_count = await db.schema_artifacts.count_documents({"versions.apicurio_connection_id": conn_id})

    dependents = {
        "flows_with_nifi": nifi_flow_count,
        "flows_with_kafka": kafka_flow_count,
        "global_services": nifi_service_count,
        "schema_artifacts": schema_artifact_count,
    }
    total_dependents = nifi_flow_count + kafka_flow_count + nifi_service_count + schema_artifact_count

    # Check if connection is inactive
    is_active = existing.get("is_active", False)

    # Rule: If the connection is inactive AND has zero dependents → delete it (204).
    # If it has dependents → 409 with structured detail.
    # If it is active → allow delete (the resolver's repair path will promote another).
    if not is_active and total_dependents == 0:
        # OK to delete
        pass
    elif total_dependents > 0:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Connection has dependents; use the repoint/reset lifecycle operation to move or release them (coming in a later phase).",
                "dependents": dependents,
            }
        )
    # If is_active and/or has dependents but is_active=True, allow delete

    await db.connections.delete_one({"id": conn_id})
    await _log_audit(db, "Deleted connection", "Connection", existing.get("name", ""), "Success")


@router.post("/{conn_id}/test", summary="Test service connection")
async def test_connection(conn_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    conn = await db.connections.find_one({"id": conn_id}, {"_id": 0})
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")

    conn_type = conn.get("type")
    result: dict = {}

    if conn_type == "nifi":
        result = await test_nifi_connection(
            url=conn.get("endpoint", ""),
            auth_type=conn.get("auth_type", "NONE"),
            username=conn.get("username"),
            password=conn.get("password"),
            token=conn.get("token"),
        )
    elif conn_type == "kafka":
        kafka_mode = (conn.get("kafka_connection_mode") or "native").strip().lower()
        if kafka_mode == "kafbat":
            result = await test_kafbat_connection(
                kafbat_url=conn.get("kafbat_url") or conn.get("endpoint", ""),
                kafbat_username=conn.get("kafbat_username"),
                kafbat_password=conn.get("kafbat_password"),
            )
        else:
            result = await test_kafka_connection(
                bootstrap_servers=conn.get("endpoint", ""),
                security_protocol=conn.get("security_protocol", "PLAINTEXT"),
                sasl_mechanism=conn.get("sasl_mechanism"),
                sasl_username=conn.get("sasl_username"),
                sasl_password=conn.get("sasl_password"),
                kafbat_url=None,
                kafbat_username=None,
                kafbat_password=None,
            )
    elif conn_type == "apicurio":
        result = await test_apicurio_connection(
            url=conn.get("endpoint", ""),
            auth_type=conn.get("auth_type", "NONE"),
            username=conn.get("username"),
            password=conn.get("password"),
            token=conn.get("token"),
        )
    elif conn_type == "kafka_connect":
        result = await kafka_connect_client.test_kafka_connect_connection(
            conn.get("endpoint", ""),
            conn.get("auth_type", "NONE"),
            conn.get("username"),
            conn.get("password"),
            conn.get("token"),
        )
    elif conn_type == "iceberg":
        result = await iceberg_catalog_client.test_iceberg_connection(conn)
    else:
        raise HTTPException(status_code=400, detail=f"Unknown connection type: {conn_type}")

    health = "Healthy" if result.get("ok") else "Failed"
    reachability = "Reachable" if result.get("ok") else "Unreachable"
    now = datetime.utcnow()
    await db.connections.update_one(
        {"id": conn_id},
        {"$set": {"health": health, "reachability": reachability, "last_tested": now, "updated_at": now}}
    )

    status = "Success" if result.get("ok") else "Failed"
    await _log_audit(db, f"Tested {conn_type} connection", "Connection", conn.get("name", ""), status)

    return {
        "ok": result.get("ok"),
        "health": health,
        "message": result.get("message", result.get("error", "")),
        "error_code": result.get("error_code"),
        "last_tested": now.isoformat(),
    }


async def _has_dependents_on_active(db: AsyncIOMotorDatabase, conn_type: str) -> bool:
    """Check if any flows or global services depend on the currently active connection of a type."""
    active_conn = await db.connections.find_one({"type": conn_type, "is_active": True}, {"_id": 0, "id": 1})
    if not active_conn:
        return False

    active_conn_id = active_conn.get("id")

    # Check flows
    if conn_type == "nifi":
        flow_count = await db.flows.count_documents({"nifi_connection_id": active_conn_id})
        if flow_count > 0:
            return True
    elif conn_type == "kafka":
        flow_count = await db.flows.count_documents({"kafka_connection_id": active_conn_id})
        if flow_count > 0:
            return True

    # Check global services
    service_count = await db.nifi_global_services.count_documents({"nifi_connection_id": active_conn_id})
    if service_count > 0:
        return True

    return False


@router.post("/{conn_id}/activate", summary="Activate service connection")
async def activate_connection(conn_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    conn = await db.connections.find_one({"id": conn_id}, {"_id": 0})
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")

    conn_type = conn.get("type")

    # Check if there are dependents on the currently active connection of this type
    if await _has_dependents_on_active(db, conn_type):
        raise HTTPException(
            status_code=409,
            detail="Use repoint to switch a connection that has dependents (coming in a later phase).",
        )

    # Probe the connection being activated
    try:
        if conn_type == "nifi":
            probe_result = await probe_nifi_fingerprint(conn)
        elif conn_type == "kafka":
            probe_result = await probe_kafka_fingerprint(conn)
        elif conn_type == "apicurio":
            probe_result = await probe_apicurio_fingerprint(conn)
        else:
            probe_result = {"reachable": False, "error": f"Unknown connection type: {conn_type}"}
    except Exception as e:
        # Treat unexpected exceptions as unreachable
        probe_result = {"reachable": False, "error": f"Probe failed: {str(e)[:100]}"}

    # Persist reachability
    reachability = "Reachable" if probe_result.get("reachable") else "Unreachable"
    now = datetime.utcnow()
    await db.connections.update_one(
        {"id": conn_id},
        {"$set": {"reachability": reachability, "updated_at": now}}
    )

    # Refuse if unreachable
    if not probe_result.get("reachable"):
        raise HTTPException(
            status_code=409,
            detail="Cannot activate: the connection is unreachable. Fix connectivity and try again."
        )

    # Activate: clear others, then set this one active
    # ORDER MATTERS: clear others first, then set this one
    await db.connections.update_many(
        {"type": conn_type, "id": {"$ne": conn_id}},
        {"$set": {"is_active": False, "updated_at": now}}
    )
    await db.connections.update_one(
        {"id": conn_id},
        {"$set": {"is_active": True, "updated_at": now}}
    )

    updated = await db.connections.find_one({"id": conn_id}, {"_id": 0})
    await _log_audit(db, "Activated connection", "Connection", conn.get("name"), "Success")
    return _to_response(updated)


@router.post("/{conn_id}/repoint", summary="Repoint connection to new endpoint")
async def repoint_connection(conn_id: str, data: ConnectionRepoint, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Repoint a connection to a new endpoint/auth with strategy (adopt|migrate|reset)."""
    conn = await db.connections.find_one({"id": conn_id}, {"_id": 0})
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")

    # Build new_auth dict from optional fields
    new_auth = {}
    if data.auth_type is not None:
        new_auth["auth_type"] = data.auth_type
    if data.username is not None:
        new_auth["username"] = data.username
    if data.password is not None:
        new_auth["password"] = data.password
    if data.token is not None:
        new_auth["token"] = data.token

    result = await connection_lifecycle_runner.run_repoint(
        db,
        conn_id=conn_id,
        new_endpoint=data.endpoint,
        new_auth=new_auth,
        strategy=data.strategy,
        pace=data.pace,
        abandon_old=bool(data.abandon_old)
    )

    if not result.get("ok"):
        if result.get("blocked") == "unknown_identity":
            raise HTTPException(
                status_code=409,
                detail="Cannot prove the target instance identity (e.g. Kafka reachable only via Kafbat). Pass abandon_old=true to override."
            )
        # Other errors
        raise HTTPException(status_code=500, detail=result.get("error", "Repoint failed"))

    # Return the job doc
    job_doc = await db.connection_lifecycle_jobs.find_one(
        {"id": result.get("job_id")},
        {"_id": 0}
    )
    return job_doc if job_doc else result


@router.get("/{conn_id}/impact", summary="Preview connection operation impact (read-only)")
async def get_connection_impact(
    conn_id: str,
    operation: str = "delete",
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """
    Get the impact preview of an operation on a connection.

    Returns affected flows, global services, schema versions, and kafka identity.
    This endpoint is read-only and makes no changes to the database.
    """
    from services.connection_impact import compute_impact

    conn = await db.connections.find_one({"id": conn_id}, {"_id": 0})
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")

    impact = await compute_impact(db, conn_id, operation)
    return impact
