from __future__ import annotations

from datetime import datetime
import logging
from typing import Any, Dict, List, Optional
import uuid

from fastapi import HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase

from services import content_store
from services.nifi_client import get_nifi_root_process_group_id, nifi_api_request
from services.nifi_flow_manager import get_controller_service_config, update_controller_service_config
from services.connection_resolver import resolve_connection
from services.connection_fingerprint import probe_nifi_fingerprint


logger = logging.getLogger(__name__)

KIND_TYPE_HINTS: Dict[str, List[str]] = {
    "kafka": ["Kafka3ConnectionService"],
    "schema_registry": ["ConfluentSchemaRegistry"],
    "mongodb": ["MongoDBControllerService"],
}

PLATFORM_DEFAULT_KINDS = {"kafka", "schema_registry"}


def infer_service_kind(nifi_type: str) -> str:
    text = str(nifi_type or "").lower()
    if "kafka" in text and "connectionservice" in text:
        return "kafka"
    if "schemaregistry" in text:
        return "schema_registry"
    if "mongodbcontrollerservice" in text:
        return "mongodb"
    return "custom"


def _safe_response(doc: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(doc or {})
    out.pop("_id", None)
    for field in ("created_at", "updated_at"):
        if isinstance(out.get(field), datetime):
            out[field] = out[field].isoformat()
    # Mask secret properties
    if "properties" in out and isinstance(out["properties"], dict):
        secret_keys = []
        masked_props = {}
        for k, v in out["properties"].items():
            k_lower = str(k or "").lower()
            if any(keyword in k_lower for keyword in ["password", "secret", "token", "key"]):
                masked_props[k] = None
                secret_keys.append(k)
            else:
                masked_props[k] = v
        out["properties"] = masked_props
        if secret_keys:
            out["has_secret_properties"] = secret_keys
    return out


async def get_nifi_connection(db: AsyncIOMotorDatabase) -> Dict[str, Any]:
    conn = await resolve_connection(db, "nifi")
    if not conn:
        raise HTTPException(status_code=400, detail="No application NiFi connection is configured.")
    return conn


async def get_global_services_pg_id(nifi_conn: Dict[str, Any]) -> str:
    pg_id = await get_nifi_root_process_group_id(
        nifi_conn["endpoint"],
        auth_type=nifi_conn.get("auth_type", "NONE"),
        username=nifi_conn.get("username"),
        password=nifi_conn.get("password"),
        token=nifi_conn.get("token"),
    )
    if not pg_id:
        raise HTTPException(status_code=502, detail="Could not resolve NiFi root process group for global services.")
    return pg_id


async def list_controller_service_types(db: AsyncIOMotorDatabase, service_kind: Optional[str] = None) -> List[Dict[str, Any]]:
    conn = await get_nifi_connection(db)
    result = await nifi_api_request(
        conn["endpoint"],
        "GET",
        "/nifi-api/flow/controller-service-types",
        auth_type=conn.get("auth_type", "NONE"),
        username=conn.get("username"),
        password=conn.get("password"),
        token=conn.get("token"),
    )
    if not result.get("ok"):
        raise HTTPException(status_code=502, detail=result.get("error") or "Failed to load NiFi controller service types.")

    items = (result.get("data") or {}).get("controllerServiceTypes") or []
    hints = KIND_TYPE_HINTS.get(service_kind or "", [])
    if hints:
        items = [item for item in items if any(hint in str(item.get("type") or "") for hint in hints)]
    return items


async def _fetch_live_service(db: AsyncIOMotorDatabase, service_id: str) -> Dict[str, Any]:
    conn = await get_nifi_connection(db)
    return await get_controller_service_config(
        conn["endpoint"],
        service_id,
        auth_type=conn.get("auth_type", "NONE"),
        username=conn.get("username"),
        password=conn.get("password"),
        token=conn.get("token"),
    )


async def _set_default(db: AsyncIOMotorDatabase, service_id: str, service_kind: str) -> None:
    affected = await db.nifi_global_services.find(
        {"service_kind": service_kind},
        {"_id": 0, "id": 1},
    ).to_list(1000)
    await db.nifi_global_services.update_many(
        {"service_kind": service_kind, "id": {"$ne": service_id}},
        {"$set": {"is_default": False, "updated_at": datetime.utcnow()}},
    )
    await db.nifi_global_services.update_one(
        {"id": service_id},
        {"$set": {"is_default": True, "updated_at": datetime.utcnow()}},
    )
    affected_ids = {str(item.get("id") or "") for item in affected}
    affected_ids.add(str(service_id))
    for affected_id in sorted(affected_ids):
        if affected_id:
            await content_store.sync_nifi_global_service(db, affected_id)


async def create_global_service(
    db: AsyncIOMotorDatabase,
    *,
    name: str,
    service_kind: Optional[str],
    nifi_type: str,
    properties: Dict[str, Any],
    is_default: bool,
) -> Dict[str, Any]:
    conn = await get_nifi_connection(db)
    pg_id = await get_global_services_pg_id(conn)
    resolved_kind = service_kind or infer_service_kind(nifi_type)

    body = {
        "revision": {"version": 0},
        "component": {
            "type": nifi_type,
            "name": name,
            "properties": {k: v for k, v in (properties or {}).items() if v is not None},
        },
    }
    result = await nifi_api_request(
        conn["endpoint"],
        "POST",
        f"/nifi-api/process-groups/{pg_id}/controller-services",
        auth_type=conn.get("auth_type", "NONE"),
        username=conn.get("username"),
        password=conn.get("password"),
        token=conn.get("token"),
        json_body=body,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=502, detail=result.get("error") or "Failed to create NiFi controller service.")

    data = result.get("data") or {}
    component = data.get("component") or {}

    # Stamp NiFi connection provenance
    nifi_instance_fingerprint = None
    try:
        nifi_fp = await probe_nifi_fingerprint(conn)
        nifi_instance_fingerprint = nifi_fp.get("fingerprint")
    except Exception:
        pass

    service_doc = {
        "id": str(uuid.uuid4()),
        "name": component.get("name") or name,
        "service_kind": resolved_kind,
        "nifi_controller_service_id": component.get("id") or data.get("id"),
        "nifi_process_group_id": pg_id,
        "nifi_type": component.get("type") or nifi_type,
        "bundle": component.get("bundle"),
        "properties": {k: v for k, v in (properties or {}).items() if v is not None},
        "is_default": bool(is_default),
        "nifi_connection_id": conn.get("id"),
        "nifi_instance_fingerprint": nifi_instance_fingerprint,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }
    await db.nifi_global_services.insert_one(service_doc)
    if is_default or resolved_kind in PLATFORM_DEFAULT_KINDS:
        existing_default = await db.nifi_global_services.find_one(
            {"service_kind": resolved_kind, "is_default": True, "id": {"$ne": service_doc["id"]}},
            {"_id": 1},
        )
        if is_default or not existing_default:
            await _set_default(db, service_doc["id"], resolved_kind)
            service_doc["is_default"] = True
    await content_store.sync_nifi_global_service(db, service_doc["id"])
    return await enrich_global_service(db, service_doc)


async def enrich_global_service(db: AsyncIOMotorDatabase, doc: Dict[str, Any]) -> Dict[str, Any]:
    out = _safe_response(doc)
    live = await _fetch_live_service(db, str(doc.get("nifi_controller_service_id") or ""))
    out["live"] = {
        "ok": bool(live.get("ok")),
        "state": live.get("state"),
        "validation_errors": live.get("validation_errors") or [],
        "revision": live.get("revision"),
        "name": live.get("name") or out.get("name"),
        "type": live.get("type") or out.get("nifi_type"),
        "error": live.get("error"),
    }
    return out


async def list_global_services(db: AsyncIOMotorDatabase) -> List[Dict[str, Any]]:
    docs = await db.nifi_global_services.find({}, {"_id": 0}).sort("created_at", -1).to_list(200)
    return [await enrich_global_service(db, doc) for doc in docs]


async def get_global_service(db: AsyncIOMotorDatabase, service_id: str) -> Dict[str, Any]:
    doc = await db.nifi_global_services.find_one({"id": service_id}, {"_id": 0})
    if not doc:
        raise HTTPException(status_code=404, detail="NiFi global service not found.")
    return doc


async def get_global_service_config(db: AsyncIOMotorDatabase, service_id: str) -> Dict[str, Any]:
    doc = await get_global_service(db, service_id)
    live = await _fetch_live_service(db, doc["nifi_controller_service_id"])
    if not live.get("ok"):
        raise HTTPException(status_code=502, detail=live.get("error") or "Failed to fetch NiFi controller service.")
    live["global_service"] = _safe_response(doc)
    return live


async def update_global_service(
    db: AsyncIOMotorDatabase,
    service_id: str,
    *,
    name: Optional[str] = None,
    properties: Optional[Dict[str, Any]] = None,
    is_default: Optional[bool] = None,
) -> Dict[str, Any]:
    doc = await get_global_service(db, service_id)
    conn = await get_nifi_connection(db)
    if properties is not None:
        result = await update_controller_service_config(
            conn["endpoint"],
            doc["nifi_controller_service_id"],
            properties,
            auth_type=conn.get("auth_type", "NONE"),
            username=conn.get("username"),
            password=conn.get("password"),
            token=conn.get("token"),
        )
        if not result.get("ok"):
            raise HTTPException(status_code=502, detail=result.get("error") or "Failed to update NiFi controller service.")

    updates: Dict[str, Any] = {"updated_at": datetime.utcnow()}
    if name is not None:
        updates["name"] = name
    if properties is not None:
        merged_props = dict(doc.get("properties") or {})
        merged_props.update({k: v for k, v in properties.items() if v is not None})
        updates["properties"] = merged_props
    if is_default is not None:
        updates["is_default"] = bool(is_default)
    if len(updates) > 1:
        await db.nifi_global_services.update_one({"id": service_id}, {"$set": updates})
    if is_default:
        await _set_default(db, service_id, doc["service_kind"])
    updated = await get_global_service(db, service_id)
    await content_store.sync_nifi_global_service(db, service_id)
    return await enrich_global_service(db, updated)


async def set_global_service_run_status(db: AsyncIOMotorDatabase, service_id: str, state: str) -> Dict[str, Any]:
    doc = await get_global_service(db, service_id)
    conn = await get_nifi_connection(db)
    current = await _fetch_live_service(db, doc["nifi_controller_service_id"])
    if not current.get("ok"):
        raise HTTPException(status_code=502, detail=current.get("error") or "Failed to fetch NiFi controller service.")
    result = await nifi_api_request(
        conn["endpoint"],
        "PUT",
        f"/nifi-api/controller-services/{doc['nifi_controller_service_id']}/run-status",
        auth_type=conn.get("auth_type", "NONE"),
        username=conn.get("username"),
        password=conn.get("password"),
        token=conn.get("token"),
        json_body={
            "revision": {"version": current.get("revision", 0)},
            "state": state,
            "disconnectedNodeAcknowledged": False,
        },
    )
    if not result.get("ok"):
        raise HTTPException(status_code=502, detail=result.get("error") or f"Failed to set service {state}.")
    await content_store.sync_nifi_global_service(db, service_id)
    return await get_global_service_config(db, service_id)


def _service_reference_values(doc: Dict[str, Any]) -> set[str]:
    return {
        str(doc.get("id") or "").strip(),
        str(doc.get("nifi_controller_service_id") or "").strip(),
    } - {""}


def _source_references_service(source: Dict[str, Any], ref_values: set[str]) -> bool:
    refs = source.get("nifi_service_refs")
    if not isinstance(refs, dict):
        return False
    return any(str(value or "").strip() in ref_values for value in refs.values())


async def _collect_app_service_references(
    db: AsyncIOMotorDatabase,
    doc: Dict[str, Any],
) -> List[Dict[str, Any]]:
    ref_values = _service_reference_values(doc)
    sources = await db.sources.find(
        {},
        {"_id": 0, "id": 1, "name": 1, "type": 1, "state": 1, "nifi_service_refs": 1},
    ).to_list(1000)
    referencing_sources = [source for source in sources if _source_references_service(source, ref_values)]
    if not referencing_sources:
        return []

    source_ids = [source.get("id") for source in referencing_sources if source.get("id")]
    flows = await db.flows.find(
        {"source_id": {"$in": source_ids}},
        {"_id": 0, "id": 1, "name": 1, "state": 1, "source_id": 1, "nifi_process_group_id": 1},
    ).to_list(1000)
    flows_by_source: Dict[str, List[Dict[str, Any]]] = {}
    for flow in flows:
        flows_by_source.setdefault(str(flow.get("source_id") or ""), []).append(flow)

    references: List[Dict[str, Any]] = []
    for source in referencing_sources:
        sid = str(source.get("id") or "")
        linked_flows = flows_by_source.get(sid) or []
        if linked_flows:
            for flow in linked_flows:
                references.append({
                    "kind": "flow",
                    "id": flow.get("id"),
                    "name": flow.get("name") or source.get("name") or flow.get("id"),
                    "state": flow.get("state") or "Unknown",
                    "source_id": sid,
                    "source_name": source.get("name"),
                    "deployed": bool(flow.get("nifi_process_group_id")),
                })
        else:
            references.append({
                "kind": "source",
                "id": sid,
                "name": source.get("name") or sid,
                "state": source.get("state") or "Draft",
                "source_type": source.get("type"),
                "deployed": False,
            })
    return references


async def _collect_nifi_service_references(
    db: AsyncIOMotorDatabase,
    doc: Dict[str, Any],
) -> List[Dict[str, Any]]:
    conn = await get_nifi_connection(db)
    result = await nifi_api_request(
        conn["endpoint"],
        "GET",
        f"/nifi-api/controller-services/{doc['nifi_controller_service_id']}/references",
        auth_type=conn.get("auth_type", "NONE"),
        username=conn.get("username"),
        password=conn.get("password"),
        token=conn.get("token"),
    )
    if not result.get("ok"):
        raise HTTPException(
            status_code=502,
            detail=result.get("error") or "Failed to check NiFi controller service references.",
        )

    items = (result.get("data") or {}).get("controllerServiceReferencingComponents") or []
    references: List[Dict[str, Any]] = []
    for item in items:
        component = item.get("component") or {}
        references.append({
            "kind": "nifi_component",
            "id": component.get("id") or item.get("id"),
            "name": component.get("name") or item.get("id"),
            "type": component.get("type"),
            "state": component.get("state"),
            "group_id": component.get("groupId") or component.get("parentGroupId"),
        })
    return references


def _is_missing_nifi_controller_service(result: Dict[str, Any]) -> bool:
    error = str((result or {}).get("error") or "").lower()
    return "404" in error and "unable to locate controller service" in error


def _raise_service_in_use(doc: Dict[str, Any], references: List[Dict[str, Any]]) -> None:
    names = []
    for ref in references[:8]:
        label = str(ref.get("name") or ref.get("id") or "unknown")
        state = str(ref.get("state") or "").strip()
        names.append(f"{label} ({state})" if state else label)
    suffix = "; ".join(names)
    if len(references) > 8:
        suffix += f"; +{len(references) - 8} more"
    message = (
        f"Cannot delete NiFi global service '{doc.get('name')}'. "
        f"It is still referenced by: {suffix}. "
        "Remove or replace those references before deleting the service."
    )
    raise HTTPException(
        status_code=409,
        detail={
            "message": message,
            "references": references,
        },
    )


async def _promote_replacement_default_if_needed(db: AsyncIOMotorDatabase, doc: Dict[str, Any]) -> Optional[str]:
    if doc.get("service_kind") not in PLATFORM_DEFAULT_KINDS or not doc.get("is_default"):
        return None
    replacement = await db.nifi_global_services.find_one(
        {"service_kind": doc["service_kind"], "id": {"$ne": doc["id"]}},
        {"_id": 0},
    )
    if replacement:
        await _set_default(db, replacement["id"], doc["service_kind"])
        return str(replacement["id"])
    return None


async def delete_global_service(db: AsyncIOMotorDatabase, service_id: str) -> None:
    doc = await get_global_service(db, service_id)
    conn = await get_nifi_connection(db)
    app_references = await _collect_app_service_references(db, doc)
    if app_references:
        _raise_service_in_use(doc, app_references)

    live = await _fetch_live_service(db, doc["nifi_controller_service_id"])
    if live.get("ok"):
        nifi_references = await _collect_nifi_service_references(db, doc)
        if nifi_references:
            _raise_service_in_use(doc, nifi_references)
    elif not _is_missing_nifi_controller_service(live):
        raise HTTPException(status_code=502, detail=live.get("error") or "Failed to fetch NiFi controller service.")

    # Only protect a service that is ITSELF live. A dangling service (its controller
    # service no longer exists in the current NiFi) is protecting nothing, so it must
    # always be deletable — otherwise a dangling service blocks deleting itself.
    if doc.get("service_kind") in PLATFORM_DEFAULT_KINDS and live.get("ok"):
        # Count only live siblings: those whose controller service is reachable in NiFi
        siblings = await db.nifi_global_services.find({
            "service_kind": doc["service_kind"],
            "id": {"$ne": doc["id"]},
        }, {"_id": 0, "nifi_controller_service_id": 1}).to_list(25)

        live_sibling_count = 0
        nifi_unreachable = False

        for sibling in siblings:
            sibling_live = await _fetch_live_service(db, sibling.get("nifi_controller_service_id") or "")
            if sibling_live.get("ok"):
                live_sibling_count += 1
            elif not _is_missing_nifi_controller_service(sibling_live):
                # This is a connectivity error (not just a missing service)
                nifi_unreachable = True

        # Only block if there are zero live siblings
        if live_sibling_count == 0:
            if nifi_unreachable:
                # NiFi is unreachable, so we can't verify liveness
                raise HTTPException(
                    status_code=409,
                    detail={
                        "message": (
                            f"Cannot determine if other global NiFi {doc['service_kind']} services are available. "
                            "NiFi is currently unreachable; try again when reachable."
                        ),
                        "references": [],
                    },
                )
            else:
                # No live siblings and NiFi was reachable - block the delete
                raise HTTPException(
                    status_code=409,
                    detail={
                        "message": (
                            f"Cannot delete the last global NiFi {doc['service_kind']} service. "
                            "Generated flows require a platform default service for this role."
                        ),
                        "references": [],
                    },
                )

    if live.get("ok"):
        if str(live.get("state") or "").upper() != "DISABLED":
            await set_global_service_run_status(db, service_id, "DISABLED")
            live = await _fetch_live_service(db, doc["nifi_controller_service_id"])
        delete_result = await nifi_api_request(
            conn["endpoint"],
            "DELETE",
            f"/nifi-api/controller-services/{doc['nifi_controller_service_id']}",
            auth_type=conn.get("auth_type", "NONE"),
            username=conn.get("username"),
            password=conn.get("password"),
            token=conn.get("token"),
            params={"version": str(live.get("revision", 0)), "disconnectedNodeAcknowledged": "false"},
        )
        if not delete_result.get("ok"):
            raise HTTPException(
                status_code=502,
                detail=delete_result.get("error") or "Failed to delete NiFi controller service.",
            )
    await db.nifi_global_services.delete_one({"id": service_id})
    await content_store.sync_nifi_global_service(db, service_id)
    promoted_id = await _promote_replacement_default_if_needed(db, doc)
    if promoted_id:
        await content_store.sync_nifi_global_service(db, promoted_id)


async def resolve_default_service_id(db: AsyncIOMotorDatabase, service_kind: str) -> str:
    query = {"service_kind": service_kind, "is_default": True}
    doc = await db.nifi_global_services.find_one(query, {"_id": 0})
    if not doc:
        count = await db.nifi_global_services.count_documents({"service_kind": service_kind})
        if count == 1:
            doc = await db.nifi_global_services.find_one({"service_kind": service_kind}, {"_id": 0})
    if not doc:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot deploy flow: default global NiFi {service_kind} service is missing.",
        )
    live = await _fetch_live_service(db, doc["nifi_controller_service_id"])
    state = str(live.get("state") or "").upper()
    if not live.get("ok") or state != "ENABLED":
        raise HTTPException(
            status_code=400,
            detail=f"Cannot deploy flow: global NiFi service '{doc.get('name')}' must be ENABLED. Current state: {state or 'unknown'}.",
        )
    return doc["nifi_controller_service_id"]


async def resolve_default_service_doc(db: AsyncIOMotorDatabase, service_kind: str) -> Dict[str, Any]:
    doc = await db.nifi_global_services.find_one({"service_kind": service_kind, "is_default": True}, {"_id": 0})
    if not doc:
        count = await db.nifi_global_services.count_documents({"service_kind": service_kind})
        if count == 1:
            doc = await db.nifi_global_services.find_one({"service_kind": service_kind}, {"_id": 0})
        elif count > 1:
            # Repair: multiple services but none marked default. Promote most-recently-updated.
            doc = await db.nifi_global_services.find_one({"service_kind": service_kind}, {"_id": 0}, sort=[("updated_at", -1)])
            if doc:
                await db.nifi_global_services.update_one({"id": doc["id"]}, {"$set": {"is_default": True, "updated_at": datetime.utcnow()}})
    if not doc:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot deploy flow: default global NiFi {service_kind} service is missing.",
        )
    live = await _fetch_live_service(db, doc["nifi_controller_service_id"])

    if not live.get("ok"):
        if _is_missing_nifi_controller_service(live):
            raise HTTPException(
                status_code=400,
                detail=f"Cannot deploy: global NiFi {service_kind} service '{doc.get('name')}' does not exist in the current NiFi instance (it may have been created in a different instance). Re-provision or repoint.",
            )
        else:
            raise HTTPException(
                status_code=502,
                detail=f"Cannot deploy: NiFi is unreachable; cannot verify global {service_kind} service.",
            )

    state = str(live.get("state") or "").upper()
    if state != "ENABLED":
        raise HTTPException(
            status_code=400,
            detail=f"Cannot deploy flow: global NiFi service '{doc.get('name')}' must be ENABLED. Current state: {state or 'unknown'}.",
        )
    doc = dict(doc)
    doc["live_type"] = live.get("type") or doc.get("nifi_type")
    return doc


async def resolve_flow_global_service_ids(db: AsyncIOMotorDatabase, source: Dict[str, Any]) -> Dict[str, str]:
    kafka_doc = await resolve_default_service_doc(db, "kafka")
    schema_doc = await resolve_default_service_doc(db, "schema_registry")
    resolved = {
        "kafka": kafka_doc["nifi_controller_service_id"],
        "kafka_type": kafka_doc.get("live_type") or "",
        "schema_registry": schema_doc["nifi_controller_service_id"],
        "schema_registry_type": schema_doc.get("live_type") or "",
    }
    if (source.get("type") or "") == "MongoDB" and (source.get("mongo_connection_mode") or "").lower() == "nifi_global_service":
        refs = source.get("nifi_service_refs") if isinstance(source.get("nifi_service_refs"), dict) else {}
        ref = str((refs or {}).get("mongodb") or "").strip()
        if not ref:
            raise HTTPException(status_code=400, detail="Cannot deploy MongoDB flow: no global MongoDB service is selected.")
        doc = await db.nifi_global_services.find_one({"id": ref, "service_kind": "mongodb"}, {"_id": 0})
        if not doc:
            doc = await db.nifi_global_services.find_one({"nifi_controller_service_id": ref, "service_kind": "mongodb"}, {"_id": 0})
        if not doc:
            raise HTTPException(status_code=400, detail="Cannot deploy MongoDB flow: selected global MongoDB service was not found.")
        live = await _fetch_live_service(db, doc["nifi_controller_service_id"])
        state = str(live.get("state") or "").upper()
        if not live.get("ok") or state != "ENABLED":
            raise HTTPException(
                status_code=400,
                detail=f"Cannot deploy MongoDB flow: global MongoDB service '{doc.get('name')}' must be ENABLED. Current state: {state or 'unknown'}.",
            )
        resolved["mongodb"] = doc["nifi_controller_service_id"]
    return resolved
