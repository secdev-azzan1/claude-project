from copy import deepcopy
from datetime import datetime
from typing import Any, Dict, List, Optional
import asyncio
import re
import uuid

import httpx
from fastapi import HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase
from smb.SMBConnection import SMBConnection

from models.application_service import (
    ApplicationService,
    ApplicationServiceCreate,
    ApplicationServiceUpdate,
    SECRET_CONFIG_FIELDS,
    normalize_application_service_config,
)
from services import content_store


SOURCE_TYPE_TO_SERVICE_TYPE = {
    "REST API": "REST API",
    "SMB": "SMB",
    "Webhook": "Webhook",
}


def to_response(doc: Dict[str, Any]) -> Dict[str, Any]:
    out = deepcopy(doc or {})
    out.pop("_id", None)
    config = dict(out.get("config") or {})
    has_secrets = {}
    for key in SECRET_CONFIG_FIELDS:
        has_secrets[key] = bool(config.get(key))
        if key in config:
            config[key] = None
    out["config"] = config
    out["has_secrets"] = {key: value for key, value in has_secrets.items() if value}
    for field in ("created_at", "updated_at", "last_tested"):
        if isinstance(out.get(field), datetime):
            out[field] = out[field].isoformat()
    return out


async def list_application_services(db: AsyncIOMotorDatabase) -> List[Dict[str, Any]]:
    docs = await db.application_services.find({}, {"_id": 0}).sort("created_at", -1).to_list(500)
    return [to_response(doc) for doc in docs]


async def get_application_service_doc(db: AsyncIOMotorDatabase, service_id: str) -> Dict[str, Any]:
    doc = await db.application_services.find_one({"id": service_id}, {"_id": 0})
    if not doc:
        raise HTTPException(status_code=404, detail="Application Service not found")
    return doc


async def get_application_service(db: AsyncIOMotorDatabase, service_id: str) -> Dict[str, Any]:
    return to_response(await get_application_service_doc(db, service_id))


async def create_application_service(db: AsyncIOMotorDatabase, data: ApplicationServiceCreate) -> Dict[str, Any]:
    if await _application_service_name_exists(db, data.name):
        raise HTTPException(status_code=409, detail="Application Service name already exists. Please choose a different name.")
    now = datetime.utcnow()
    service = ApplicationService(**data.dict())
    doc = service.dict()
    doc["id"] = str(uuid.uuid4())
    doc["created_at"] = now
    doc["updated_at"] = now
    await db.application_services.insert_one(doc)
    await _log_audit(db, "Created application service", "ApplicationService", data.name, "Success")
    await content_store.sync_application_service(db, doc["id"])
    return to_response(doc)


async def update_application_service(db: AsyncIOMotorDatabase, service_id: str, data: ApplicationServiceUpdate) -> Dict[str, Any]:
    existing = await get_application_service_doc(db, service_id)
    if data.name and await _application_service_name_exists(db, data.name, exclude_id=service_id):
        raise HTTPException(status_code=409, detail="Application Service name already exists. Please choose a different name.")
    updates = data.dict(exclude_unset=True)
    if "config" in updates:
        incoming = _preserve_existing_secrets(updates.get("config") or {}, existing.get("config") or {})
        try:
            updates["config"] = normalize_application_service_config(existing["service_type"], incoming)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc))
    updates["updated_at"] = datetime.utcnow()
    await db.application_services.update_one({"id": service_id}, {"$set": updates})
    updated = await get_application_service_doc(db, service_id)
    await _log_audit(db, "Updated application service", "ApplicationService", updated.get("name", ""), "Success")
    await content_store.sync_application_service(db, service_id)
    return to_response(updated)


async def delete_application_service(db: AsyncIOMotorDatabase, service_id: str) -> None:
    existing = await get_application_service_doc(db, service_id)
    references = await collect_application_service_references(db, service_id)
    if references:
        _raise_service_in_use(existing, references)
    await db.application_services.delete_one({"id": service_id})
    await _log_audit(db, "Deleted application service", "ApplicationService", existing.get("name", ""), "Success")
    await content_store.sync_application_service(db, service_id)


async def test_application_service(db: AsyncIOMotorDatabase, service_id: str) -> Dict[str, Any]:
    doc = await get_application_service_doc(db, service_id)
    if doc.get("service_type") == "REST API":
        result = await _test_rest_application_service(doc)
    elif doc.get("service_type") == "SMB":
        result = await _test_smb_application_service(doc)
    elif doc.get("service_type") == "Webhook":
        result = {"ok": True, "message": "Webhook security configuration is valid."}
    else:
        result = {"ok": False, "message": "Unsupported Application Service type."}
    now = datetime.utcnow()
    health = "Healthy" if result["ok"] else "Failed"
    await db.application_services.update_one(
        {"id": service_id},
        {"$set": {"health": health, "last_tested": now, "updated_at": now}},
    )
    await content_store.sync_application_service(db, service_id)
    return {**result, "health": health, "last_tested": now.isoformat()}


async def _test_rest_application_service(doc: Dict[str, Any]) -> Dict[str, Any]:
    cfg = doc.get("config") or {}
    base_url = str(cfg.get("base_url") or "").rstrip("/")
    headers = _rest_auth_headers(cfg)
    timeout = int(cfg.get("connection_timeout") or 30)
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            response = await client.get(base_url, headers=headers)
    except httpx.TimeoutException:
        return {"ok": False, "message": f"Request timed out after {timeout}s"}
    except Exception as exc:
        return {"ok": False, "message": str(exc)[:240]}
    if response.status_code < 400:
        return {"ok": True, "message": f"REST endpoint reachable (HTTP {response.status_code})."}
    return {"ok": False, "message": f"REST endpoint returned HTTP {response.status_code}: {(response.text or '')[:160]}"}


def _rest_auth_headers(cfg: Dict[str, Any]) -> Dict[str, str]:
    auth_type = str(cfg.get("auth_type") or "NONE").upper()
    headers: Dict[str, str] = {}
    if auth_type == "BEARER" and cfg.get("bearer_token"):
        headers["Authorization"] = f"Bearer {cfg['bearer_token']}"
    elif auth_type == "API_KEY" and str(cfg.get("api_key_location") or "").upper() == "HEADER":
        key_name = str(cfg.get("api_key_name") or "X-Api-Key")
        headers[key_name] = str(cfg.get("api_key_value") or "")
    elif auth_type == "BASIC" and cfg.get("rest_username"):
        import base64
        raw = f"{cfg.get('rest_username') or ''}:{cfg.get('rest_password') or ''}"
        headers["Authorization"] = f"Basic {base64.b64encode(raw.encode()).decode()}"
    return headers


async def _test_smb_application_service(doc: Dict[str, Any]) -> Dict[str, Any]:
    cfg = doc.get("config") or {}
    host = str(cfg.get("smb_hostname") or "").strip()
    share = str(cfg.get("smb_share") or "").strip()
    username = str(cfg.get("smb_username") or "").strip()
    password = str(cfg.get("smb_password") or "")
    domain = str(cfg.get("smb_domain") or "").strip()
    timeout = int(cfg.get("connection_timeout") or 30)

    def _probe() -> Dict[str, Any]:
        conn = SMBConnection(
            username,
            password,
            "nif-abstractor",
            host,
            domain=domain,
            use_ntlm_v2=True,
            is_direct_tcp=True,
        )
        try:
            if not conn.connect(host, 445, timeout=timeout):
                return {"ok": False, "message": "SMB connection failed."}
            conn.listPath(share, "/")
            return {"ok": True, "message": "SMB share is reachable."}
        finally:
            try:
                conn.close()
            except Exception:
                pass

    try:
        return await asyncio.to_thread(_probe)
    except Exception as exc:
        return {"ok": False, "message": str(exc)[:240]}


async def collect_application_service_references(db: AsyncIOMotorDatabase, service_id: str) -> List[Dict[str, Any]]:
    sources = await db.sources.find(
        {"application_service_id": service_id},
        {"_id": 0, "id": 1, "name": 1, "type": 1, "state": 1},
    ).to_list(1000)
    if not sources:
        return []
    source_ids = [source.get("id") for source in sources if source.get("id")]
    flows = await db.flows.find(
        {"source_id": {"$in": source_ids}},
        {"_id": 0, "id": 1, "name": 1, "source_id": 1, "state": 1, "nifi_process_group_id": 1},
    ).to_list(1000)
    flows_by_source: Dict[str, List[Dict[str, Any]]] = {}
    for flow in flows:
        flows_by_source.setdefault(str(flow.get("source_id") or ""), []).append(flow)

    references: List[Dict[str, Any]] = []
    for source in sources:
        sid = str(source.get("id") or "")
        linked = flows_by_source.get(sid) or []
        if linked:
            for flow in linked:
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


async def resolve_source_application_service(db: AsyncIOMotorDatabase, source: Dict[str, Any]) -> Dict[str, Any]:
    resolved = deepcopy(source or {})
    if (resolved.get("connection_setup_mode") or "manual") != "application_service":
        return resolved
    service_id = str(resolved.get("application_service_id") or "").strip()
    if not service_id:
        raise HTTPException(status_code=422, detail="application_service_id is required when connection_setup_mode is application_service")
    service = await get_application_service_doc(db, service_id)
    expected_type = SOURCE_TYPE_TO_SERVICE_TYPE.get(resolved.get("type"))
    if not expected_type:
        raise HTTPException(status_code=422, detail=f"Application Services are not supported for source type {resolved.get('type')}")
    if service.get("service_type") != expected_type:
        raise HTTPException(
            status_code=422,
            detail=f"Application Service type {service.get('service_type')} cannot be used with {resolved.get('type')} sources",
        )
    config = deepcopy(service.get("config") or {})
    if expected_type == "REST API":
        _merge_config(resolved, config, (
            "base_url",
            "auth_type",
            "api_key_name",
            "api_key_value",
            "api_key_location",
            "bearer_token",
            "rest_username",
            "rest_password",
            "connection_timeout",
            "read_timeout",
            "write_timeout",
            "idle_timeout",
            "rate_limit",
            "openapi_spec_id",
            "openapi_selected_server_url",
        ))
    elif expected_type == "SMB":
        _merge_config(resolved, config, (
            "smb_hostname",
            "smb_share",
            "smb_domain",
            "smb_username",
            "smb_password",
            "connection_timeout",
        ))
    elif expected_type == "Webhook":
        _merge_config(resolved, config, (
            "webhook_secret",
            "webhook_signature_header",
            "webhook_signature_algo",
        ))
    resolved["application_service"] = {
        "id": service.get("id"),
        "name": service.get("name"),
        "service_type": service.get("service_type"),
        "updated_at": service.get("updated_at"),
    }
    return resolved


def _merge_config(target: Dict[str, Any], config: Dict[str, Any], keys: tuple[str, ...]) -> None:
    for key in keys:
        if key in config:
            target[key] = config.get(key)


def _preserve_existing_secrets(payload: Dict[str, Any], existing: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(payload or {})
    for key in SECRET_CONFIG_FIELDS:
        incoming = out.get(key)
        if (incoming is None or incoming == "") and existing.get(key):
            out[key] = existing.get(key)
    return out


async def _application_service_name_exists(
    db: AsyncIOMotorDatabase,
    name: str,
    exclude_id: Optional[str] = None,
) -> bool:
    normalized = (name or "").strip()
    if not normalized:
        return False
    query: Dict[str, Any] = {"name": {"$regex": f"^{re.escape(normalized)}$", "$options": "i"}}
    if exclude_id:
        query["id"] = {"$ne": exclude_id}
    return await db.application_services.find_one(query, {"_id": 1}) is not None


def _raise_service_in_use(doc: Dict[str, Any], references: List[Dict[str, Any]]) -> None:
    names = []
    for ref in references[:8]:
        kind_label = "flow" if ref.get("kind") == "flow" else "source"
        label = str(ref.get("name") or ref.get("id") or "unknown")
        state = str(ref.get("state") or "").strip()
        item = f"{kind_label}: {label}"
        names.append(f"{item} ({state})" if state else item)
    suffix = "; ".join(names)
    if len(references) > 8:
        suffix += f"; +{len(references) - 8} more"
    raise HTTPException(
        status_code=409,
        detail={
            "message": (
                f"Cannot delete Application Service '{doc.get('name')}'. "
                f"It is still referenced by: {suffix}. "
                "Remove or replace those references before deleting the service."
            ),
            "references": references,
        },
    )


async def _log_audit(db: AsyncIOMotorDatabase, action: str, object_type: str, target: str, status: str, details: str = None):
    from models.audit import AuditEvent
    event = AuditEvent(action=action, object_type=object_type, target=target or "", status=status, details=details)
    await db.audit_events.insert_one(event.dict())
