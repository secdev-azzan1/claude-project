import hashlib
import json
import re
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import BaseModel, Field

from db import get_db
from models.adapter import AppService, Flow, FlowBlock
from services.connection_resolver import resolve_connection
from services.iceberg_sink_config import ICEBERG_CONNECTOR_CLASS
from services.kafka_connect_client import get_cluster_info, list_connector_plugins, list_connectors_with_status
from services.adapter.common import COLLECTIONS, audit, new_id, now_iso
from services.kafka_connect_link_validation import validate_sync_link
from services.kafka_connect_client import (
    delete_connector,
    get_connector_config,
    get_connector_status,
    pause_connector,
    restart_connector,
    resume_connector,
    start_connector,
    stop_connector,
    upsert_connector,
    validate_connector_config,
)


router = APIRouter(prefix="/api/kafka-connect", tags=["kafka-connect"])


class SyncUpsert(BaseModel):
    id: Optional[str] = None
    name: str
    description: str = ""
    direction: str = "sink"
    connector_class: str = ""
    connector_name: Optional[str] = None
    config: Dict[str, Any] = Field(default_factory=dict)
    linked_flow_id: Optional[str] = None
    linked_block_id: Optional[str] = None


class SyncLink(BaseModel):
    flow_id: str
    block_id: str


_SECRET_KEY = re.compile(r"(pass(word)?|secret|token|credential|api.?key|access.?key|private.?key)", re.I)


def _public_sync(doc: Dict[str, Any]) -> Dict[str, Any]:
    result = dict(doc)
    result.pop("_id", None)
    configuration_state = _configuration_state(result)
    config = dict(result.get("config") or {})
    result["config"] = {
        key: "[secret]" if _SECRET_KEY.search(str(key)) and value not in (None, "") else value
        for key, value in config.items()
    }
    result["has_secrets"] = any(
        _SECRET_KEY.search(str(key)) and value not in (None, "", "[secret]")
        for key, value in config.items()
    )
    result["remote_present"] = bool(result.get("remote_present"))
    result["configuration_state"] = configuration_state
    result["pending_changes"] = configuration_state == "changes_pending"
    return result


def _config_fingerprint(config: Dict[str, Any]) -> str:
    """Return a stable fingerprint for the configuration applied remotely."""
    payload = json.dumps(config or {}, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _configuration_state(doc: Dict[str, Any]) -> str:
    """Describe whether the saved app config matches Kafka Connect."""
    if not doc.get("remote_present"):
        return "draft"
    remote_hash = str(doc.get("remote_config_hash") or "").strip()
    if not remote_hash:
        return "needs_review"
    return "synced" if remote_hash == _config_fingerprint(dict(doc.get("config") or {})) else "changes_pending"


async def _adopt_remote_snapshot(
    db: AsyncIOMotorDatabase, doc: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """Read the remote connector config without changing its runtime state."""
    connector_name = str(doc.get("connector_name") or "").strip()
    if not connector_name:
        return None
    result = await _remote_result(db, get_connector_config, connector_name)
    remote_config = dict(result.get("data") or {})
    return {
        "config": remote_config,
        "remote_present": True,
        "remote_config_hash": _config_fingerprint(remote_config),
    }


async def _get_sync_or_404(db: AsyncIOMotorDatabase, sync_id: str) -> Dict[str, Any]:
    doc = await db[COLLECTIONS.kafka_connect_syncs].find_one({"id": sync_id}, {"_id": 0})
    if not doc:
        raise HTTPException(status_code=404, detail="Kafka Connect sync not found.")
    # Flow Builder stores the relationship on the block, so recover it even
    # when the sync was linked there before the sync document was updated.
    flows = await db[COLLECTIONS.flows].find({}, {"_id": 0, "id": 1, "blocks": 1}).to_list(10000)
    for flow in flows:
        for block in flow.get("blocks") or []:
            if (block.get("config") or {}).get("syncId") == sync_id:
                doc["linked_flow_id"] = flow.get("id")
                doc["linked_block_id"] = block.get("id")
                return doc
    return doc


async def _linked_sync_flow_docs(db: AsyncIOMotorDatabase, sync_id: str) -> List[Dict[str, Any]]:
    """Return every flow that still references a sync from its block config.

    The block reference is the dependency source of truth.  A sync can be
    linked from more than one flow even though the legacy sync document keeps
    only one convenience link for display.
    """
    flows = await db[COLLECTIONS.flows].find(
        {}, {"_id": 0, "id": 1, "name": 1, "blocks": 1, "deployedAt": 1}
    ).to_list(10000)
    return [
        flow
        for flow in flows
        if any((block.get("config") or {}).get("syncId") == sync_id for block in flow.get("blocks") or [])
    ]


async def _require_sync_flow_queue_unlocked(db: AsyncIOMotorDatabase, sync_id: str) -> None:
    """Prevent lifecycle/deletion races with a queued flow operation."""
    from routers.v2.flows import _get_flow_queue_lock

    for flow in await _linked_sync_flow_docs(db, sync_id):
        locked = await _get_flow_queue_lock(db, flow.get("id"))
        if locked:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Flow '{flow.get('name') or flow.get('id')}' is locked by queued operation "
                    f"'{locked.get('verb')}'. Wait for it to finish or cancel the queued item."
                ),
            )


async def _record_sync_error(db: AsyncIOMotorDatabase, sync_id: str, error: str) -> None:
    await db[COLLECTIONS.kafka_connect_syncs].update_one(
        {"id": sync_id}, {"$set": {"last_error": error[:500], "updated_at": now_iso()}}
    )


async def _sync_remote_result(db: AsyncIOMotorDatabase, sync_id: str, fn, *args, **kwargs) -> Dict[str, Any]:
    """Run a remote operation and persist failures for the status card."""
    try:
        return await _remote_result(db, fn, *args, **kwargs)
    except HTTPException as exc:
        await _record_sync_error(db, sync_id, str(exc.detail))
        raise


def _sync_is_retired(doc: Dict[str, Any]) -> bool:
    return bool(doc.get("retired"))


def _require_active_sync(doc: Dict[str, Any]) -> None:
    if _sync_is_retired(doc):
        raise HTTPException(
            status_code=409,
            detail=(
                f"Kafka Connect sync '{doc.get('name') or doc.get('id')}' is retired. "
                "Reinstate it before applying or changing its runtime state."
            ),
        )


async def _validate_sync_link_target(
    db: AsyncIOMotorDatabase,
    sync: Dict[str, Any],
    flow_id: str,
    block_id: str,
) -> tuple[Dict[str, Any], Flow, FlowBlock]:
    """Resolve and validate a sync's flow target before mutating either record."""
    flow_doc = await db[COLLECTIONS.flows].find_one({"id": flow_id}, {"_id": 0})
    if not flow_doc:
        raise HTTPException(status_code=404, detail="Flow not found.")
    from routers.v2.flows import _get_flow_queue_lock

    locked = await _get_flow_queue_lock(db, flow_id)
    if locked:
        raise HTTPException(status_code=409, detail="This flow is locked by a queued operation.")
    flow = Flow(**flow_doc)
    block = next((item for item in flow.blocks if item.id == block_id), None)
    if not block:
        raise HTTPException(status_code=404, detail="Flow block not found.")

    service_docs = await db[COLLECTIONS.services].find({}, {"_id": 0}).to_list(None)
    services = [AppService(**item) for item in service_docs]
    issues = validate_sync_link(sync, flow, block, services)

    current_sync_id = str((block.config or {}).get("syncId") or "").strip()
    sync_id = str(sync.get("id") or "").strip()
    if current_sync_id and current_sync_id != sync_id:
        issues.append(
            f"This flow block is already linked to sync '{current_sync_id}'. Unlink it before choosing another sync."
        )

    if flow_doc.get("deployedAt") and flow_doc.get("state") != "Stopped":
        raise HTTPException(
            status_code=409,
            detail="Editing a deployed flow is refused until it is stopped before changing its Kafka Connect link.",
        )

    if issues:
        raise HTTPException(
            status_code=422,
            detail={
                "issues": [
                    {"blockId": block.id, "where": block.name or "Kafka Connect sink", "message": message}
                    for message in issues
                ]
            },
        )
    return flow_doc, flow, block


async def _set_flow_sync_link(db: AsyncIOMotorDatabase, flow_id: str, block_id: str, sync_id: Optional[str]) -> None:
    from routers.v2.flows import _get_flow_queue_lock

    locked = await _get_flow_queue_lock(db, flow_id)
    if locked:
        raise HTTPException(status_code=409, detail="This flow is locked by a queued operation.")
    flow = await db[COLLECTIONS.flows].find_one({"id": flow_id}, {"_id": 0})
    if not flow:
        raise HTTPException(status_code=404, detail="Flow not found.")
    if flow.get("deployedAt") and flow.get("state") != "Stopped":
        raise HTTPException(
            status_code=409,
            detail="Editing a deployed flow is refused until it is stopped before changing its Kafka Connect link.",
        )
    block = next((b for b in flow.get("blocks") or [] if b.get("id") == block_id), None)
    if not block:
        raise HTTPException(status_code=404, detail="Flow block not found.")
    if block.get("adapter") not in ("kc", "kafka_kc"):
        raise HTTPException(status_code=422, detail="Kafka Connect syncs can only be linked to Kafka Connect blocks.")
    config = dict(block.get("config") or {})
    if sync_id:
        config["syncId"] = sync_id
    else:
        config.pop("syncId", None)
    block["config"] = config
    await db[COLLECTIONS.flows].update_one(
        {"id": flow_id}, {"$set": {"blocks": flow.get("blocks") or [], "updatedAt": now_iso()}}
    )


async def _remote_result(db: AsyncIOMotorDatabase, fn, *args, **kwargs) -> Dict[str, Any]:
    conn = await resolve_connection(db, "kafka_connect", required=False)
    if not conn:
        raise HTTPException(status_code=503, detail="No active Kafka Connect connection.")
    result = await fn(conn, *args, **kwargs)
    if not result.get("ok"):
        raise HTTPException(status_code=502, detail=result.get("error") or "Kafka Connect request failed.")
    return result


@router.get("/cluster", summary="Get Kafka Connect cluster info + Iceberg plugin/cluster-match status")
async def get_cluster(db: AsyncIOMotorDatabase = Depends(get_db)):
    connect_conn = await resolve_connection(db, "kafka_connect", required=False)
    if not connect_conn:
        return {"ok": False, "reachable": False, "error": "No active Kafka Connect connection."}

    cluster_result = await get_cluster_info(connect_conn)
    if not cluster_result.get("ok"):
        return {
            "ok": False,
            "reachable": bool(cluster_result.get("reachable")),
            "error": cluster_result.get("error") or "Could not reach Kafka Connect.",
        }

    data = cluster_result.get("data") or {}
    version = data.get("version") if isinstance(data, dict) else None
    commit = data.get("commit") if isinstance(data, dict) else None
    connect_cluster_id = data.get("kafka_cluster_id") if isinstance(data, dict) else None

    plugins_result = await list_connector_plugins(connect_conn)
    plugin_classes = set()
    if plugins_result.get("ok") and isinstance(plugins_result.get("data"), list):
        plugin_classes = {
            plugin.get("class") for plugin in plugins_result["data"] if isinstance(plugin, dict)
        }
    iceberg_plugin_installed = ICEBERG_CONNECTOR_CLASS in plugin_classes

    # Mirror services.iceberg_sinks.preflight_sink's cluster_match resolution: never guess a
    # cluster match, only report it when both sides have a proven cluster id.
    kafka_conn = await resolve_connection(db, "kafka", required=False)
    proven_kafka_cluster_id = None
    if kafka_conn:
        kafka_flow = await db.flows.find_one(
            {
                "kafka_connection_id": kafka_conn.get("id"),
                "kafka_cluster_id": {"$exists": True, "$ne": None},
            },
            {"_id": 0, "kafka_cluster_id": 1},
        )
        if kafka_flow:
            proven_kafka_cluster_id = kafka_flow.get("kafka_cluster_id")

    if not connect_cluster_id or not proven_kafka_cluster_id:
        cluster_match = None
        cluster_match_detail = (
            "Could not verify Kafka Connect and Kafka point to the same cluster "
            "(no proven cluster id available yet)."
        )
    elif connect_cluster_id == proven_kafka_cluster_id:
        cluster_match = True
        cluster_match_detail = "Kafka Connect and Kafka cluster IDs match."
    else:
        cluster_match = False
        cluster_match_detail = (
            f"Kafka Connect cluster id '{connect_cluster_id}' does not match the Kafka "
            f"connection's proven cluster id '{proven_kafka_cluster_id}'."
        )

    return {
        "ok": True,
        "reachable": True,
        "version": version,
        "commit": commit,
        "kafka_cluster_id": connect_cluster_id,
        "iceberg_plugin_installed": iceberg_plugin_installed,
        "cluster_match": cluster_match,
        "cluster_match_detail": cluster_match_detail,
    }


@router.get("/orphans", summary="Report unmanaged/missing Iceberg Sink connectors (report-only)")
async def get_orphans(db: AsyncIOMotorDatabase = Depends(get_db)):
    connect_conn = await resolve_connection(db, "kafka_connect", required=False)
    if not connect_conn:
        return {"ok": False, "error": "No active Kafka Connect connection."}

    result = await list_connectors_with_status(connect_conn)
    if not result.get("ok"):
        return {"ok": False, "error": result.get("error") or "Could not list Kafka Connect connectors."}

    connectors_by_name = result.get("data") or {}
    if not isinstance(connectors_by_name, dict):
        connectors_by_name = {}
    iceberg_connector_names = {
        name for name in connectors_by_name if isinstance(name, str) and name.endswith("__iceberg")
    }

    sink_connector_names = set()
    cursor = db.iceberg_sinks.find({"enabled": True}, {"_id": 0, "connector_name": 1})
    async for doc in cursor:
        connector_name = doc.get("connector_name")
        if connector_name:
            sink_connector_names.add(connector_name)

    unmanaged_connectors = sorted(iceberg_connector_names - sink_connector_names)
    missing_connectors = sorted(sink_connector_names - iceberg_connector_names)

    return {
        "ok": True,
        "unmanaged_connectors": unmanaged_connectors,
        "missing_connectors": missing_connectors,
    }


# ------------------------------------------------------------------ user syncs

@router.get("/syncs", summary="List user-managed Kafka Connect syncs")
async def list_syncs(db: AsyncIOMotorDatabase = Depends(get_db)):
    docs = await db[COLLECTIONS.kafka_connect_syncs].find({}, {"_id": 0}).to_list(1000)
    flows = await db[COLLECTIONS.flows].find({}, {"_id": 0, "id": 1, "name": 1, "blocks": 1}).to_list(10000)
    links: Dict[str, Dict[str, str]] = {}
    for flow in flows:
        for block in flow.get("blocks") or []:
            sync_id = (block.get("config") or {}).get("syncId")
            if sync_id:
                links[str(sync_id)] = {"flow_id": flow.get("id"), "block_id": block.get("id")}
    for doc in docs:
        link = links.get(doc.get("id"))
        # The flow block is the source of truth because Flow Builder can link
        # a sync without updating the sync document first. Clear stale stored
        # metadata when the block was moved or unlinked.
        doc["linked_flow_id"] = link["flow_id"] if link else None
        doc["linked_block_id"] = link["block_id"] if link else None
    return [_public_sync(doc) for doc in docs]


@router.get("/syncs/statuses", summary="Refresh all active Kafka Connect sync statuses")
async def sync_statuses(db: AsyncIOMotorDatabase = Depends(get_db)):
    """Refresh active syncs in one request for a Kafbat-style status view.

    One failed connector must not hide the status of its neighbors, so remote
    errors are retained on that sync and the successful records are returned.
    """
    conn = await resolve_connection(db, "kafka_connect", required=False)
    if not conn:
        raise HTTPException(status_code=503, detail="No active Kafka Connect connection.")

    docs = await db[COLLECTIONS.kafka_connect_syncs].find(
        {"enabled": True, "retired": {"$ne": True}}, {"_id": 0}
    ).to_list(1000)
    refreshed: List[Dict[str, Any]] = []
    for doc in docs:
        connector_name = doc.get("connector_name")
        if not connector_name:
            refreshed.append(_public_sync(doc))
            continue
        result = await get_connector_status(conn, connector_name)
        if result.get("ok"):
            doc["last_status"] = result.get("data")
            doc["last_error"] = None
        else:
            doc["last_error"] = result.get("error") or "Could not refresh connector status."
        doc["updated_at"] = now_iso()
        await db[COLLECTIONS.kafka_connect_syncs].update_one(
            {"id": doc["id"]},
            {
                "$set": {
                    "last_status": doc.get("last_status"),
                    "last_error": doc.get("last_error"),
                    "updated_at": doc["updated_at"],
                }
            },
        )
        refreshed.append(_public_sync(doc))
    return refreshed


@router.get("/syncs/{sync_id}", summary="Get one Kafka Connect sync")
async def get_sync(sync_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    return _public_sync(await _get_sync_or_404(db, sync_id))


@router.post("/syncs", summary="Create or update a Kafka Connect sync")
async def save_sync(payload: SyncUpsert, db: AsyncIOMotorDatabase = Depends(get_db)):
    name = payload.name.strip()
    connector_class = payload.connector_class.strip() or str(payload.config.get("connector.class") or "").strip()
    if not name:
        raise HTTPException(status_code=422, detail="Sync name is required.")
    if payload.direction not in ("sink", "source"):
        raise HTTPException(status_code=422, detail="Direction must be sink or source.")
    if not connector_class:
        raise HTTPException(status_code=422, detail="Connector class is required.")
    if (payload.linked_flow_id is None) != (payload.linked_block_id is None):
        raise HTTPException(status_code=422, detail="A flow link must include both flow_id and block_id.")

    sync_id = payload.id or new_id("sync")
    existing = await db[COLLECTIONS.kafka_connect_syncs].find_one({"id": sync_id}, {"_id": 0})
    config = dict(payload.config or {})
    if existing:
        # The UI sends [secret] placeholders for values it cannot read back.
        # Preserve the real stored value rather than replacing it with the
        # placeholder during an ordinary edit. Also retain secret keys omitted
        # by a partial editor payload; deleting a credential must be explicit.
        existing_config = existing.get("config") or {}
        for key, value in list(config.items()):
            if value == "[secret]" and key in existing_config:
                config[key] = existing_config[key]
        for key, value in existing_config.items():
            if key not in config and _SECRET_KEY.search(str(key)) and value not in (None, ""):
                config[key] = value
    config["connector.class"] = connector_class
    connector_name = (payload.connector_name or "").strip()
    if not connector_name:
        connector_name = re.sub(r"[^a-zA-Z0-9._-]+", "-", name).strip("-._")[:180] or sync_id

    duplicate = await db[COLLECTIONS.kafka_connect_syncs].find_one(
        {"connector_name": connector_name, "id": {"$ne": sync_id}}, {"_id": 0, "name": 1}
    )
    if duplicate:
        raise HTTPException(
            status_code=409,
            detail=f"Connector name '{connector_name}' is already used by sync '{duplicate.get('name') or 'another sync'}'.",
        )

    now = now_iso()
    linked_flow_id = payload.linked_flow_id if payload.linked_flow_id is not None else (existing.get("linked_flow_id") if existing else None)
    linked_block_id = payload.linked_block_id if payload.linked_block_id is not None else (existing.get("linked_block_id") if existing else None)
    doc = {
        "id": sync_id,
        "name": name,
        "description": payload.description.strip(),
        "direction": payload.direction,
        "connector_class": connector_class,
        "connector_name": connector_name,
        "config": config,
        "enabled": bool(existing.get("enabled")) if existing else False,
        "retired": bool(existing.get("retired")) if existing else False,
        # A saved definition is not necessarily present in Kafka Connect yet.
        # Keep that fact separate from enabled/runtime state so the UI can
        # distinguish a real draft from an existing connector.
        "remote_present": bool(existing.get("remote_present")) if existing else False,
        "remote_config_hash": existing.get("remote_config_hash") if existing else None,
        "linked_flow_id": linked_flow_id,
        "linked_block_id": linked_block_id,
        "last_status": existing.get("last_status") if existing else None,
        "last_error": existing.get("last_error") if existing else None,
        "created_at": existing.get("created_at") if existing else now,
        "updated_at": now,
    }

    if linked_flow_id and linked_block_id:
        await _validate_sync_link_target(db, doc, linked_flow_id, linked_block_id)

    if existing:
        await db[COLLECTIONS.kafka_connect_syncs].update_one({"id": sync_id}, {"$set": doc})
    else:
        await db[COLLECTIONS.kafka_connect_syncs].insert_one(doc)

    if linked_flow_id and linked_block_id:
        await _set_flow_sync_link(db, linked_flow_id, linked_block_id, sync_id)
    await audit(db, "Kafka Connect sync saved", name, object="Kafka Connect Sync")
    return _public_sync(doc)


@router.post("/syncs/{sync_id}/retire", summary="Retire a Kafka Connect sync")
async def retire_sync(sync_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    doc = await _get_sync_or_404(db, sync_id)
    await _require_sync_flow_queue_unlocked(db, sync_id)
    linked = await _linked_sync_flow_docs(db, sync_id)
    deployed = [flow for flow in linked if flow.get("deployedAt")]
    if deployed:
        names = ", ".join(str(flow.get("name") or flow.get("id")) for flow in deployed[:8])
        if len(deployed) > 8:
            names += f", +{len(deployed) - 8} more"
        raise HTTPException(
            status_code=409,
            detail=(
                f"Cannot retire sync '{doc.get('name') or sync_id}' while it is linked to deployed "
                f"flow(s): {names}. Undeploy the flow(s) first so the live sink is not interrupted."
            ),
        )

    remote_error: Optional[str] = None
    conn = await resolve_connection(db, "kafka_connect", required=False)
    if conn and doc.get("enabled") and doc.get("connector_name"):
        result = await pause_connector(conn, doc["connector_name"])
        if not result.get("ok") and result.get("status_code") != 404:
            remote_error = result.get("error") or "Could not pause the remote connector during retirement."

    now = now_iso()
    update: Dict[str, Any] = {"retired": True, "updated_at": now}
    if remote_error:
        update["last_error"] = remote_error[:500]
    else:
        update["last_error"] = None
    await db[COLLECTIONS.kafka_connect_syncs].update_one({"id": sync_id}, {"$set": update})

    dependents = await _linked_sync_flow_docs(db, sync_id)
    details = f"{len(dependents)} dependent flow(s) flagged: action required" if dependents else None
    if remote_error:
        details = f"{details}; {remote_error}" if details else remote_error
    await audit(
        db,
        "Kafka Connect sync retired",
        doc.get("name") or sync_id,
        status="Warning",
        details=details,
        object="Kafka Connect Sync",
    )
    return _public_sync(await _get_sync_or_404(db, sync_id))


@router.post("/syncs/{sync_id}/reinstate", summary="Reinstate a retired Kafka Connect sync")
async def reinstate_sync(sync_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    doc = await _get_sync_or_404(db, sync_id)
    await _require_sync_flow_queue_unlocked(db, sync_id)
    await db[COLLECTIONS.kafka_connect_syncs].update_one(
        {"id": sync_id}, {"$set": {"retired": False, "updated_at": now_iso(), "last_error": None}}
    )
    await audit(db, "Kafka Connect sync reinstated", doc.get("name") or sync_id, object="Kafka Connect Sync")
    return _public_sync(await _get_sync_or_404(db, sync_id))


@router.delete("/syncs/{sync_id}", summary="Delete a Kafka Connect sync")
async def delete_sync(sync_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    doc = await _get_sync_or_404(db, sync_id)
    if not _sync_is_retired(doc):
        raise HTTPException(
            status_code=409,
            detail=(
                f"Cannot delete '{doc.get('name') or sync_id}' while it is active. "
                "Retire the sync first -- retirement is reversible, deletion is not."
            ),
        )
    await _require_sync_flow_queue_unlocked(db, sync_id)
    linked = await _linked_sync_flow_docs(db, sync_id)
    deployed = [flow for flow in linked if flow.get("deployedAt")]
    if deployed:
        names = ", ".join(str(flow.get("name") or flow.get("id")) for flow in deployed[:8])
        if len(deployed) > 8:
            names += f", +{len(deployed) - 8} more"
        raise HTTPException(
            status_code=409,
            detail=(
                f"Cannot delete sync '{doc.get('name') or sync_id}' while it is linked to deployed "
                f"flow(s): {names}. Undeploy the flow(s) first so the live sink is not interrupted."
            ),
        )

    remote_expected = bool(doc.get("remote_present") or doc.get("enabled"))
    conn = await resolve_connection(db, "kafka_connect", required=False)
    if remote_expected and not conn:
        message = "Kafka Connect is unavailable; the remote connector could not be confirmed for deletion."
        await _record_sync_error(db, sync_id, message)
        raise HTTPException(status_code=503, detail=message)
    if conn and doc.get("connector_name"):
        result = await delete_connector(conn, doc["connector_name"])
        if not result.get("ok"):
            await _record_sync_error(db, sync_id, result.get("error") or "Could not delete connector.")
            raise HTTPException(status_code=502, detail=result.get("error") or "Could not delete connector.")

    # Keep the block reference, just like Application Service deletion keeps
    # service references.  Undeployed flows will fail validation with the
    # missing sync id; deployed flows get an explicit drift warning below.
    await db[COLLECTIONS.kafka_connect_syncs].delete_one({"id": sync_id})

    name = doc.get("name") or sync_id
    for flow in deployed:
        await db[COLLECTIONS.flows].update_one(
            {"id": flow.get("id")},
            {
                "$set": {
                    "drift": (
                        f"Kafka Connect sync \"{name}\" was deleted. The running deployment "
                        "may still have an older connector state, but the saved definition no "
                        "longer resolves -- select or create a replacement sync before the next deploy."
                    ),
                    "updatedAt": now_iso(),
                }
            },
        )

    details = "Permanent -- the sync record and its remote connector are gone"
    if linked:
        details += f"; {len(linked)} linked flow(s), {len(deployed)} deployed and flagged"
    await audit(
        db,
        "Kafka Connect sync deleted",
        name,
        status="Warning",
        details=details,
        object="Kafka Connect Sync",
    )
    return {"ok": True}


@router.post("/syncs/{sync_id}/link", summary="Link a sync to a flow Kafka Connect block")
async def link_sync(sync_id: str, payload: SyncLink, db: AsyncIOMotorDatabase = Depends(get_db)):
    doc = await _get_sync_or_404(db, sync_id)
    _require_active_sync(doc)
    await _validate_sync_link_target(db, doc, payload.flow_id, payload.block_id)
    await _set_flow_sync_link(db, payload.flow_id, payload.block_id, sync_id)
    if doc.get("linked_flow_id") and doc.get("linked_block_id") and (
        doc["linked_flow_id"] != payload.flow_id or doc["linked_block_id"] != payload.block_id
    ):
        await _set_flow_sync_link(db, doc["linked_flow_id"], doc["linked_block_id"], None)
    await db[COLLECTIONS.kafka_connect_syncs].update_one(
        {"id": sync_id}, {"$set": {"linked_flow_id": payload.flow_id, "linked_block_id": payload.block_id, "updated_at": now_iso()}}
    )
    return _public_sync(await _get_sync_or_404(db, sync_id))


@router.post("/syncs/{sync_id}/unlink", summary="Unlink a sync from its flow")
async def unlink_sync(sync_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    doc = await _get_sync_or_404(db, sync_id)
    if doc.get("linked_flow_id") and doc.get("linked_block_id"):
        await _set_flow_sync_link(db, doc["linked_flow_id"], doc["linked_block_id"], None)
    await db[COLLECTIONS.kafka_connect_syncs].update_one(
        {"id": sync_id}, {"$set": {"linked_flow_id": None, "linked_block_id": None, "updated_at": now_iso()}}
    )
    return _public_sync(await _get_sync_or_404(db, sync_id))


@router.post("/syncs/{sync_id}/validate", summary="Validate a sync against its installed connector plugin")
async def validate_sync(sync_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    doc = await _get_sync_or_404(db, sync_id)
    result = await _remote_result(db, validate_connector_config, doc["connector_class"], doc.get("config") or {})
    return {"ok": True, "message": result.get("message"), "data": result.get("data")}


@router.post("/syncs/{sync_id}/apply", summary="Create or update the remote Kafka Connect connector")
async def apply_sync(sync_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    doc = await _get_sync_or_404(db, sync_id)
    _require_active_sync(doc)
    result = await _sync_remote_result(db, sync_id, upsert_connector, doc["connector_name"], doc.get("config") or {})

    # PUT /config does not return the runtime state.  Fetch it immediately so
    # the page can leave Draft with a real Kafka Connect state when possible.
    last_status = result.get("data")
    last_error = None
    try:
        status_result = await _sync_remote_result(db, sync_id, get_connector_status, doc["connector_name"])
        last_status = status_result.get("data")
    except HTTPException as exc:
        last_error = str(exc.detail)
    await db[COLLECTIONS.kafka_connect_syncs].update_one(
        {"id": sync_id},
        {
            "$set": {
                "enabled": True,
                "remote_present": True,
                "remote_config_hash": _config_fingerprint(dict(doc.get("config") or {})),
                "last_status": last_status,
                "last_error": last_error,
                "updated_at": now_iso(),
            }
        },
    )
    return _public_sync(await _get_sync_or_404(db, sync_id))


@router.post("/syncs/{sync_id}/adopt", summary="Adopt an existing Kafka Connect connector")
async def adopt_sync(sync_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Backfill an already-existing connector into the application catalog.

    Adoption only reads the connector configuration and records its fingerprint;
    it never creates, updates, starts, stops, pauses, or resumes the connector.
    """
    doc = await _get_sync_or_404(db, sync_id)
    _require_active_sync(doc)
    snapshot = await _adopt_remote_snapshot(db, doc)
    if snapshot is None:
        raise HTTPException(status_code=422, detail="A connector name is required before adoption.")

    remote_config = dict(snapshot["config"])
    connector_class = str(remote_config.get("connector.class") or doc.get("connector_class") or "").strip()
    update = {
        **snapshot,
        "enabled": True,
        "connector_class": connector_class,
        "last_error": None,
        "updated_at": now_iso(),
    }
    await db[COLLECTIONS.kafka_connect_syncs].update_one({"id": sync_id}, {"$set": update})
    await audit(db, "Kafka Connect sync adopted", doc.get("name") or sync_id, object="Kafka Connect Sync")
    return _public_sync(await _get_sync_or_404(db, sync_id))


async def _sync_action(sync_id: str, db: AsyncIOMotorDatabase, fn) -> Dict[str, Any]:
    doc = await _get_sync_or_404(db, sync_id)
    _require_active_sync(doc)
    result = await _sync_remote_result(db, sync_id, fn, doc["connector_name"])
    last_status = result.get("data")
    last_error = None
    try:
        status_result = await _sync_remote_result(db, sync_id, get_connector_status, doc["connector_name"])
        last_status = status_result.get("data")
    except HTTPException as exc:
        # The lifecycle call succeeded; keep that result and expose the status
        # refresh issue without turning a successful action into a failure.
        last_error = str(exc.detail)
    await db[COLLECTIONS.kafka_connect_syncs].update_one(
        {"id": sync_id},
        {"$set": {"last_status": last_status, "last_error": last_error, "updated_at": now_iso()}},
    )
    return _public_sync(await _get_sync_or_404(db, sync_id))


@router.post("/syncs/{sync_id}/pause", summary="Pause a remote sync")
async def pause_sync(sync_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    return await _sync_action(sync_id, db, pause_connector)


@router.post("/syncs/{sync_id}/stop", summary="Fully stop a remote sync")
async def stop_sync(sync_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    return await _sync_action(sync_id, db, stop_connector)


@router.post("/syncs/{sync_id}/resume", summary="Resume a remote sync")
async def resume_sync(sync_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    return await _sync_action(sync_id, db, resume_connector)


@router.post("/syncs/{sync_id}/start", summary="Start a fully stopped remote sync")
async def start_sync(sync_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    return await _sync_action(sync_id, db, start_connector)


@router.post("/syncs/{sync_id}/restart", summary="Restart a remote sync")
async def restart_sync(sync_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    return await _sync_action(sync_id, db, restart_connector)


@router.get("/syncs/{sync_id}/status", summary="Refresh remote sync status")
async def sync_status(sync_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    doc = await _get_sync_or_404(db, sync_id)
    result = await _sync_remote_result(db, sync_id, get_connector_status, doc["connector_name"])
    await db[COLLECTIONS.kafka_connect_syncs].update_one(
        {"id": sync_id}, {"$set": {"last_status": result.get("data"), "last_error": None, "updated_at": now_iso()}}
    )
    return _public_sync(await _get_sync_or_404(db, sync_id))
