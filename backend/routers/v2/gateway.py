"""APISIX Gateway v2 router — proxy catalog, cert profiles, host allowlist,
and REAL reconciliation onto a live APISIX Admin API.

Contract source: frontend/src/prototype/api.ts (listGatewayProxies,
saveGatewayProxy, deleteGatewayProxy, testGatewayProxy, reconcileGatewayProxy,
getGatewayResources/updateGatewayResources) + frontend/src/prototype/types.ts
(GatewayProxy, GatewayResources) + frontend/src/pages/Apisix.tsx (allowlist
admin-confirm, cert refCount, delete guards). Validation wording is ported
from Apisix.tsx's `proxySaveBlockReason` / `hostFieldError` so the server
refuses exactly what the UI would have refused, with the same messages.

Storage: a single document `{_id: "gateway", proxies, certProfiles,
allowlist}` in the `gateway_v2` collection (see services/adapter/common.py's
`COLLECTIONS`). Unlike the frontend mock (which never persists secret
values), cert profile private key material IS accepted and stored here
server-side — the real backend needs it to present a client certificate on
egress — but it is never returned; only `hasCert`/`hasKey` presence flags are.

Reconciliation is real: on `POST /proxies/{id}/reconcile` this pushes an
upstream and two routes (a bare-prefix route and a wildcard route rewritten
via `proxy-rewrite`) onto the active APISIX Admin API via
`services/apisix_client.py`. `POST /proxies/{id}/test` probes the *runtime*
gateway URL directly with httpx.
"""

from __future__ import annotations

import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, Body, Depends, HTTPException, Response
from motor.motor_asyncio import AsyncIOMotorDatabase

from db import get_db
from services import apisix_client
from services.adapter.common import COLLECTIONS, audit, new_id, now_iso
from services.adapter.naming import tokenize
from services.http_tls import tls_verify_enabled

router = APIRouter(prefix="/api/v2/gateway", tags=["gateway-v2"])

GATEWAY_DOC_ID = "gateway"

# Defaults mirror frontend/src/pages/Apisix.tsx's `emptyProxyForm()`.
DEFAULT_CONNECT_TIMEOUT_MS = 5000
DEFAULT_READ_TIMEOUT_MS = 30000

# ---------------------------------------------------------------- hostnames

# Bare hostname — no scheme, no port, no path. Ported from Apisix.tsx's HOST_RE.
_HOST_RE = re.compile(r"^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?(\.[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?)*$")
# Lowercase-only variant — the allowlist stores hosts verbatim. Ported from HOST_LOWER_RE.
_HOST_LOWER_RE = re.compile(r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*$")
_SCHEME_RE = re.compile(r"^[a-z]+://", re.IGNORECASE)


# ------------------------------------------------------------------ storage


async def _load_gateway(db: AsyncIOMotorDatabase) -> Dict[str, Any]:
    """Load (creating if absent) the single gateway_v2 document."""
    doc = await db[COLLECTIONS.gateway].find_one({"_id": GATEWAY_DOC_ID})
    if doc is None:
        doc = {"_id": GATEWAY_DOC_ID, "proxies": [], "certProfiles": [], "allowlist": []}
        await db[COLLECTIONS.gateway].insert_one(dict(doc))
    doc.setdefault("proxies", [])
    doc.setdefault("certProfiles", [])
    doc.setdefault("allowlist", [])
    return doc


async def _save_gateway(db: AsyncIOMotorDatabase, doc: Dict[str, Any]) -> None:
    doc["_id"] = GATEWAY_DOC_ID
    await db[COLLECTIONS.gateway].replace_one({"_id": GATEWAY_DOC_ID}, doc, upsert=True)


async def _apisix_conn(db: AsyncIOMotorDatabase) -> Dict[str, Any]:
    """The active APISIX platform connection's admin/runtime facts.

    404s with "No active APISIX connection" when there is none — a real
    reconcile/test call has nothing to talk to without it. Config keys
    (adminUrl/adminKey/runtimeUrl) come from the `connections_v2` document
    `{"type": "apisix", "active": True}`; `adminKey` is a secret stored
    server-side in `config`.
    """
    conn = await _try_apisix_conn(db)
    if not conn:
        raise HTTPException(status_code=404, detail="No active APISIX connection")
    return conn


async def _try_apisix_conn(db: AsyncIOMotorDatabase) -> Optional[Dict[str, Any]]:
    """Same lookup as `_apisix_conn`, but returns `None` instead of raising —
    for best-effort cleanup paths (E4's proxy-delete teardown) that must
    never hard-fail just because the APISIX connection was retired/never
    configured."""
    conn = await db[COLLECTIONS.connections].find_one({"type": "apisix", "active": True})
    if not conn:
        return None
    config = conn.get("config") or {}
    return {
        "adminUrl": config.get("adminUrl") or "",
        "adminKey": config.get("adminKey") or "",
        "runtimeUrl": config.get("runtimeUrl") or "",
    }


# --------------------------------------------------------------- public shapes


def _public_cert_profile(cert: Dict[str, Any], proxies: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Browser-safe cert profile: presence flags instead of key material.
    refCount is recomputed live from the proxy catalog rather than trusted
    from storage, mirroring Apisix.tsx's `certRefs` comment: "the stored
    `refCount` goes stale the moment a proxy changes"."""
    cert_id = cert.get("id")
    ref_count = sum(1 for p in proxies if p.get("certProfileId") == cert_id)
    return {
        "id": cert_id,
        "name": cert.get("name", ""),
        "subject": cert.get("subject", ""),
        "expiresAt": cert.get("expiresAt", ""),
        "refCount": ref_count,
        "hasCert": bool(cert.get("certPem")),
        "hasKey": bool(cert.get("keyPem")),
    }


# ------------------------------------------------------------------ helpers


def _optional_int(raw: Any) -> Optional[int]:
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _proxy_save_block_reason(payload: Dict[str, Any]) -> Optional[str]:
    """Port of Apisix.tsx's `proxySaveBlockReason`. Returns the first reason
    Save would be refused, or None."""
    name = str(payload.get("name") or "").strip()
    if not name:
        return "Name the proxy — flows reference it by name."
    host = str(payload.get("targetHost") or "").strip()
    if not host:
        return "Set the target host."
    if _SCHEME_RE.match(host):
        return "Target host is a bare hostname — drop the scheme."
    if "/" in host:
        return "Target host is a bare hostname — the path belongs in the path field."
    if ":" in host:
        return "Target host is a bare hostname — the port belongs in the port field."
    if not _HOST_RE.match(host):
        return "Target host is not a valid hostname."
    port = _optional_int(payload.get("port"))
    if port is None or port < 1 or port > 65535:
        return "Port must be between 1 and 65535."
    path = str(payload.get("path") or "").strip()
    if not path:
        return "Set the route path prefix."
    if not path.startswith("/"):
        return "The route path must start with “/”."
    methods = payload.get("methods") or []
    if len(methods) == 0:
        return "Allow at least one HTTP method."
    for label, raw in (("Connect timeout", payload.get("connectTimeoutMs")), ("Read timeout", payload.get("readTimeoutMs"))):
        if raw is None or raw == "":
            continue
        n = _optional_int(raw)
        if n is None or n <= 0:
            return f"{label} must be a positive number of milliseconds."
    return None


def _host_field_error(raw: str) -> Optional[str]:
    """Port of Apisix.tsx's `hostFieldError`. Empty input is not an error
    here (callers reject blank separately) — mirrors the frontend's "empty
    input is not an error yet" comment."""
    host = (raw or "").strip()
    if not host:
        return None
    if _SCHEME_RE.match(host):
        return "Enter a bare hostname — drop the scheme (e.g. “https://”)."
    if "/" in host:
        return "Enter a bare hostname — no path."
    if ":" in host:
        return "Enter a bare hostname — no port."
    if re.search(r"[A-Z]", host):
        return "Hostnames are lowercase."
    if not _HOST_LOWER_RE.match(host):
        return "Not a valid hostname."
    return None


def _reconciled_fields(p: Dict[str, Any]) -> Any:
    """Config that, once changed, has to be pushed to the gateway again.
    Port of Apisix's `reconciledFields`."""
    return (
        p.get("targetHost"),
        p.get("port"),
        p.get("sni") or None,
        p.get("path"),
        tuple(sorted(p.get("methods") or [])),
        p.get("certProfileId") or None,
    )


def _block_proxy_id(block: Dict[str, Any], services_by_id: Dict[str, Dict[str, Any]]) -> Optional[str]:
    """Port of validation.ts's `blockProxyId`: the proxy an http block routes
    through lives on its bound service's config, with a legacy block-level
    fallback for datasets saved before the move."""
    if block.get("adapter") != "http":
        return None
    service = services_by_id.get(block.get("serviceId"))
    from_service = (service or {}).get("config", {}).get("proxyId") if service else None
    if isinstance(from_service, str) and from_service.strip():
        return from_service
    legacy = (block.get("config") or {}).get("proxyId")
    if isinstance(legacy, str) and legacy.strip():
        return legacy
    return None


async def _proxy_dependents(db: AsyncIOMotorDatabase, proxy_id: str) -> List[str]:
    """Names of flows with a block routing through this proxy — every flow,
    not only deployed ones (a Draft referencing a deleted proxy is broken
    too). Port of api.ts's `proxyDependents`."""
    services_by_id: Dict[str, Dict[str, Any]] = {}
    async for svc in db[COLLECTIONS.services].find({}, {"_id": 0}):
        if svc.get("id"):
            services_by_id[svc["id"]] = svc

    names: List[str] = []
    async for flow in db[COLLECTIONS.flows].find({}, {"_id": 0}):
        for block in flow.get("blocks") or []:
            if _block_proxy_id(block, services_by_id) == proxy_id:
                names.append(flow.get("name") or flow.get("id") or "")
                break
    return names


def _default_cert_expiry() -> str:
    dt = datetime.now(timezone.utc) + timedelta(days=365)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


# ------------------------------------------------------------------- GET /


@router.get("")
@router.get("/")
async def get_gateway_state(db: AsyncIOMotorDatabase = Depends(get_db)) -> Dict[str, Any]:
    gateway = await _load_gateway(db)
    proxies = gateway.get("proxies", [])
    return {
        "proxies": proxies,
        "certProfiles": [_public_cert_profile(c, proxies) for c in gateway.get("certProfiles", [])],
        "allowlist": gateway.get("allowlist", []),
    }


# --------------------------------------------------------------- proxy CRUD


@router.post("/proxies")
async def save_proxy(
    response: Response,
    payload: Dict[str, Any] = Body(...),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> Dict[str, Any]:
    reason = _proxy_save_block_reason(payload)
    if reason:
        raise HTTPException(status_code=422, detail=reason)

    gateway = await _load_gateway(db)
    proxies = gateway.get("proxies", [])

    proxy_id = str(payload.get("id") or "").strip()
    name = str(payload.get("name") or "").strip()

    clash = next((p for p in proxies if p.get("id") != proxy_id and str(p.get("name") or "").strip() == name), None)
    if clash:
        raise HTTPException(status_code=409, detail=f'Another proxy is already called "{name}".')

    now = now_iso()
    idx = next((i for i, p in enumerate(proxies) if proxy_id and p.get("id") == proxy_id), -1)

    next_proxy: Dict[str, Any] = {
        "name": name,
        "description": (str(payload.get("description") or "").strip() or None),
        "targetHost": str(payload.get("targetHost") or "").strip(),
        "port": _optional_int(payload.get("port")),
        "sni": (str(payload.get("sni") or "").strip() or None),
        "connectTimeoutMs": _optional_int(payload.get("connectTimeoutMs")),
        "readTimeoutMs": _optional_int(payload.get("readTimeoutMs")),
        "path": str(payload.get("path") or "").strip(),
        "methods": list(dict.fromkeys(payload.get("methods") or [])),
        "certProfileId": payload.get("certProfileId") or None,
        "updatedAt": now,
    }

    if idx == -1:
        next_proxy["id"] = proxy_id or new_id("gw-proxy")
        next_proxy["createdAt"] = now
        next_proxy["status"] = "Pending"
        next_proxy["statusDetail"] = "Created — not yet reconciled onto the gateway."
        next_proxy["everReconciled"] = False
        proxies.append(next_proxy)
        response.status_code = 201
        await audit(
            db,
            "Gateway proxy created",
            next_proxy["name"],
            "Success",
            details=f'{next_proxy["targetHost"]}:{next_proxy["port"]}{next_proxy["path"]}',
            object="Gateway",
        )
    else:
        before = proxies[idx]
        next_proxy["id"] = before.get("id")
        next_proxy["createdAt"] = before.get("createdAt", now)
        if _reconciled_fields(before) != _reconciled_fields(next_proxy):
            next_proxy["status"] = "Pending"
            next_proxy["statusDetail"] = "Configuration changed — reconcile to push it to the gateway."
        else:
            next_proxy["status"] = before.get("status", "Pending")
            next_proxy["statusDetail"] = before.get("statusDetail")
        # A proxy that already has a lastTest keeps it across an edit.
        if "lastTest" in before:
            next_proxy["lastTest"] = before["lastTest"]
        # E4: "was this proxy ever successfully reconciled onto the live
        # gateway" survives every subsequent edit (which resets `status`
        # back to "Pending" until reconciled again) — delete needs this to
        # know whether there is anything live on APISIX to tear down.
        next_proxy["everReconciled"] = bool(before.get("everReconciled"))
        proxies[idx] = next_proxy
        response.status_code = 200
        await audit(
            db,
            "Gateway proxy updated",
            next_proxy["name"],
            "Success",
            details="Reconciliation required" if next_proxy["status"] == "Pending" else None,
            object="Gateway",
        )

    gateway["proxies"] = proxies
    await _save_gateway(db, gateway)
    return next_proxy


async def _teardown_live_apisix_objects(db: AsyncIOMotorDatabase, proxy_id: str) -> Dict[str, Any]:
    """E4: best-effort delete of the live APISIX routes + upstream a prior
    `reconcile_proxy()` call pushed for this proxy — `dmp_<id>_root` /
    `dmp_<id>_wild` (routes) then `dmp_<id>` (upstream), mirroring
    `reconcile_proxy`'s own naming and the "routes before upstream" order
    (an upstream with live routes still pointing at it is the wrong order
    to delete in). Never raises: every failure is collected into `errors`
    for the caller to fold into the audit detail rather than swallow."""
    conn = await _try_apisix_conn(db)
    if conn is None:
        return {"cleaned": False, "errors": ["No active APISIX connection — live gateway objects were not cleaned up."]}

    admin_url, admin_key = conn["adminUrl"], conn["adminKey"]
    errors: List[str] = []
    for route_id in (f"dmp_{proxy_id}_root", f"dmp_{proxy_id}_wild"):
        result = await apisix_client.delete_route(admin_url, admin_key, route_id)
        if not result.get("ok"):
            errors.append(f'route "{route_id}": {result.get("error") or "delete failed"}')

    upstream_result = await apisix_client.delete_upstream(admin_url, admin_key, f"dmp_{proxy_id}")
    if not upstream_result.get("ok"):
        errors.append(f'upstream "dmp_{proxy_id}": {upstream_result.get("error") or "delete failed"}')

    return {"cleaned": not errors, "errors": errors}


@router.delete("/proxies/{proxy_id}")
async def delete_proxy(proxy_id: str, db: AsyncIOMotorDatabase = Depends(get_db)) -> Dict[str, Any]:
    gateway = await _load_gateway(db)
    proxies = gateway.get("proxies", [])
    proxy = next((p for p in proxies if p.get("id") == proxy_id), None)
    if not proxy:
        raise HTTPException(status_code=404, detail="Proxy not found.")

    deps = await _proxy_dependents(db, proxy_id)
    if deps:
        shown = ", ".join(deps[:3]) + ("…" if len(deps) > 3 else "")
        raise HTTPException(
            status_code=409,
            detail=(
                f'Cannot delete: {len(deps)} flow(s) route through "{proxy.get("name")}" ({shown}). '
                "Remove the dependency from those flows first."
            ),
        )

    # E4: a proxy that was ever reconciled pushed real objects onto the live
    # gateway (`reconcile_proxy`) — deleting only the platform's own Mongo
    # record left them running (and routing traffic) forever. A proxy that
    # never made it past "Pending" has nothing live to clean up.
    apisix_cleaned = False
    cleanup_detail: Optional[str] = None
    if proxy.get("everReconciled"):
        teardown = await _teardown_live_apisix_objects(db, proxy_id)
        apisix_cleaned = bool(teardown["cleaned"])
        if teardown["errors"]:
            cleanup_detail = "APISIX cleanup incomplete — " + "; ".join(teardown["errors"])
        else:
            cleanup_detail = "Live APISIX routes and upstream removed."

    gateway["proxies"] = [p for p in proxies if p.get("id") != proxy_id]
    await _save_gateway(db, gateway)
    await audit(db, "Gateway proxy deleted", proxy.get("name", ""), "Warning", details=cleanup_detail, object="Gateway")
    return {"ok": True, "id": proxy_id, "apisixCleaned": apisix_cleaned}


# ------------------------------------------------------------- reconcile


async def _fail_reconcile(
    db: AsyncIOMotorDatabase,
    gateway: Dict[str, Any],
    proxies: List[Dict[str, Any]],
    idx: int,
    proxy: Dict[str, Any],
    message: str,
) -> Dict[str, Any]:
    proxy["status"] = "Failed"
    proxy["statusDetail"] = message
    proxy["updatedAt"] = now_iso()
    proxies[idx] = proxy
    gateway["proxies"] = proxies
    await _save_gateway(db, gateway)
    await audit(db, "Gateway proxy reconciliation failed", proxy.get("name", ""), "Failed", details=message, object="Gateway")
    return proxy


@router.post("/proxies/{proxy_id}/reconcile")
async def reconcile_proxy(proxy_id: str, db: AsyncIOMotorDatabase = Depends(get_db)) -> Dict[str, Any]:
    gateway = await _load_gateway(db)
    proxies = gateway.get("proxies", [])
    idx = next((i for i, p in enumerate(proxies) if p.get("id") == proxy_id), -1)
    if idx == -1:
        raise HTTPException(status_code=404, detail="Proxy not found.")
    proxy = proxies[idx]

    conn = await _apisix_conn(db)

    allowlist = gateway.get("allowlist", [])
    if proxy.get("targetHost") not in allowlist:
        message = f'Host "{proxy.get("targetHost")}" is not on the gateway allowlist — an administrator has to add it first.'
        return await _fail_reconcile(db, gateway, proxies, idx, proxy, message)

    proxy_token = tokenize(proxy.get("name") or "")
    upstream_id = f"dmp_{proxy_id}"
    port = proxy.get("port")
    sni = proxy.get("sni")
    scheme = "https" if (port == 443 or bool(sni)) else "http"
    connect_ms = proxy.get("connectTimeoutMs") or DEFAULT_CONNECT_TIMEOUT_MS
    read_ms = proxy.get("readTimeoutMs") or DEFAULT_READ_TIMEOUT_MS

    upstream_body: Dict[str, Any] = {
        "type": "roundrobin",
        "nodes": {f'{proxy.get("targetHost")}:{port}': 1},
        "scheme": scheme,
        "pass_host": "node",
        "timeout": {
            "connect": connect_ms / 1000,
            "send": read_ms / 1000,
            "read": read_ms / 1000,
        },
    }

    cert_profile_id = proxy.get("certProfileId")
    if cert_profile_id:
        cert = next((c for c in gateway.get("certProfiles", []) if c.get("id") == cert_profile_id), None)
        if cert and cert.get("certPem") and cert.get("keyPem"):
            upstream_body["tls"] = {"client_cert": cert["certPem"], "client_key": cert["keyPem"]}

    methods = proxy.get("methods") or []
    path = proxy.get("path") or "/"
    path_normalized = path.rstrip("/")

    route_root_id = f"dmp_{proxy_id}_root"
    route_wild_id = f"dmp_{proxy_id}_wild"
    route_root_body = {
        "uri": f"/{proxy_token}",
        "methods": methods,
        "upstream_id": upstream_id,
        "plugins": {"proxy-rewrite": {"uri": path}},
    }
    route_wild_body = {
        "uri": f"/{proxy_token}/*",
        "methods": methods,
        "upstream_id": upstream_id,
        "plugins": {
            "proxy-rewrite": {
                "regex_uri": [f"^/{proxy_token}/(.*)", f"{path_normalized}/$1"],
            }
        },
    }

    admin_url = conn["adminUrl"]
    admin_key = conn["adminKey"]

    up_result = await apisix_client.put_upstream(admin_url, admin_key, upstream_id, upstream_body)
    if not up_result.get("ok"):
        return await _fail_reconcile(db, gateway, proxies, idx, proxy, up_result.get("error") or "Failed to push the upstream to APISIX.")

    root_result = await apisix_client.put_route(admin_url, admin_key, route_root_id, route_root_body)
    if not root_result.get("ok"):
        return await _fail_reconcile(db, gateway, proxies, idx, proxy, root_result.get("error") or "Failed to push the route to APISIX.")

    wild_result = await apisix_client.put_route(admin_url, admin_key, route_wild_id, route_wild_body)
    if not wild_result.get("ok"):
        return await _fail_reconcile(db, gateway, proxies, idx, proxy, wild_result.get("error") or "Failed to push the route to APISIX.")

    proxy["status"] = "Reconciled"
    proxy["statusDetail"] = None
    proxy["everReconciled"] = True
    proxy["updatedAt"] = now_iso()
    proxies[idx] = proxy
    gateway["proxies"] = proxies
    await _save_gateway(db, gateway)
    await audit(
        db,
        "Gateway proxy reconciled",
        proxy.get("name", ""),
        "Success",
        details=f'{proxy.get("targetHost")}:{port}{path} is live on the gateway',
        object="Gateway",
    )
    return proxy


# ----------------------------------------------------------------- test


@router.post("/proxies/{proxy_id}/test")
async def test_proxy(proxy_id: str, db: AsyncIOMotorDatabase = Depends(get_db)) -> Dict[str, Any]:
    gateway = await _load_gateway(db)
    proxies = gateway.get("proxies", [])
    idx = next((i for i, p in enumerate(proxies) if p.get("id") == proxy_id), -1)
    if idx == -1:
        raise HTTPException(status_code=404, detail="Proxy not found.")
    proxy = proxies[idx]

    conn = await _apisix_conn(db)
    runtime_url = (conn.get("runtimeUrl") or "").rstrip("/")
    proxy_token = tokenize(proxy.get("name") or "")
    path = proxy.get("path") or "/"
    url = f"{runtime_url}/{proxy_token}{path}"

    start = time.monotonic()
    try:
        async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=10.0, follow_redirects=False) as client:
            resp = await client.get(url)
        ms = int((time.monotonic() - start) * 1000)
        status_code = resp.status_code
        if status_code in (502, 504):
            result = {
                "ok": False,
                "status": status_code,
                "ms": ms,
                "message": f"Gateway reached {proxy_token}{path} but the upstream target failed (HTTP {status_code}).",
            }
        elif 200 <= status_code < 300:
            result = {
                "ok": True,
                "status": status_code,
                "ms": ms,
                "message": f"Reached {proxy_token}{path} — HTTP {status_code}.",
            }
        else:
            result = {
                "ok": False,
                "status": status_code,
                "ms": ms,
                "message": f"Reached the gateway at {proxy_token}{path} — HTTP {status_code}.",
            }
    except httpx.TimeoutException:
        ms = int((time.monotonic() - start) * 1000)
        result = {
            "ok": False,
            "status": None,
            "ms": ms,
            "message": f"Gateway unreachable — request to {url} timed out.",
        }
    except httpx.RequestError as exc:
        ms = int((time.monotonic() - start) * 1000)
        result = {
            "ok": False,
            "status": None,
            "ms": ms,
            "message": f"Gateway unreachable — {exc}",
        }

    result["testedAt"] = now_iso()
    proxy["lastTest"] = result
    proxies[idx] = proxy
    gateway["proxies"] = proxies
    await _save_gateway(db, gateway)
    await audit(
        db,
        "Gateway proxy tested" if result["ok"] else "Gateway proxy test failed",
        proxy.get("name", ""),
        "Success" if result["ok"] else "Failed",
        details=result["message"],
        object="Gateway",
    )
    return result


# ------------------------------------------------------------ cert profiles


@router.post("/cert-profiles", status_code=201)
async def create_cert_profile(payload: Dict[str, Any] = Body(...), db: AsyncIOMotorDatabase = Depends(get_db)) -> Dict[str, Any]:
    name = str(payload.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=422, detail="Name the certificate profile.")
    subject = str(payload.get("subject") or "").strip()
    if not subject:
        raise HTTPException(status_code=422, detail="Set the subject — e.g. CN=api-client.example.corp.")

    expires_at = str(payload.get("expiresAt") or "").strip() or _default_cert_expiry()
    now = now_iso()
    cert: Dict[str, Any] = {
        "id": new_id("gw-cert"),
        "name": name,
        "subject": subject,
        "expiresAt": expires_at,
        "certPem": payload.get("certPem") or None,
        "keyPem": payload.get("keyPem") or None,
        "createdAt": now,
        "updatedAt": now,
    }

    gateway = await _load_gateway(db)
    gateway.setdefault("certProfiles", []).append(cert)
    await _save_gateway(db, gateway)
    await audit(db, "Gateway cert profile created", name, "Success", details=subject, object="Gateway")
    return _public_cert_profile(cert, gateway.get("proxies", []))


@router.delete("/cert-profiles/{cert_id}")
async def delete_cert_profile(cert_id: str, db: AsyncIOMotorDatabase = Depends(get_db)) -> Dict[str, Any]:
    gateway = await _load_gateway(db)
    certs = gateway.get("certProfiles", [])
    cert = next((c for c in certs if c.get("id") == cert_id), None)
    if not cert:
        raise HTTPException(status_code=404, detail="Certificate profile not found.")

    proxies = gateway.get("proxies", [])
    refs = sum(1 for p in proxies if p.get("certProfileId") == cert_id)
    if refs > 0:
        raise HTTPException(
            status_code=409,
            detail=f'Certificate profile "{cert.get("name")}" is referenced by {refs} prox{"y" if refs == 1 else "ies"} — re-point them first.',
        )

    gateway["certProfiles"] = [c for c in certs if c.get("id") != cert_id]
    await _save_gateway(db, gateway)
    await audit(db, "Gateway cert profile deleted", cert.get("name", ""), "Warning", object="Gateway")
    return {"ok": True, "id": cert_id}


# ---------------------------------------------------------------- allowlist


@router.post("/allowlist")
async def update_allowlist(payload: Dict[str, Any] = Body(...), db: AsyncIOMotorDatabase = Depends(get_db)) -> Dict[str, Any]:
    if payload.get("adminConfirmed") is not True:
        raise HTTPException(status_code=422, detail="Administrator confirmation is required for allowlist changes.")

    action = payload.get("action")
    if action not in ("add", "remove"):
        raise HTTPException(status_code=422, detail='action must be "add" or "remove".')

    host = str(payload.get("host") or "").strip()
    if not host:
        raise HTTPException(status_code=422, detail="Set a host.")
    err = _host_field_error(host)
    if err:
        raise HTTPException(status_code=422, detail=err)

    gateway = await _load_gateway(db)
    allowlist = gateway.get("allowlist", [])

    if action == "add":
        if host not in allowlist:
            allowlist = allowlist + [host]
            gateway["allowlist"] = allowlist
            await _save_gateway(db, gateway)
            await audit(
                db,
                "Gateway resources updated",
                "APISIX Gateway",
                "Success",
                details=f'Admin action recorded — "{host}" added to the gateway allowlist',
                object="Gateway",
            )
    else:
        stranded = [p for p in gateway.get("proxies", []) if p.get("targetHost") == host]
        allowlist = [h for h in allowlist if h != host]
        gateway["allowlist"] = allowlist
        await _save_gateway(db, gateway)
        details = f'Admin action recorded — "{host}" removed from the gateway allowlist'
        if stranded:
            details += f"; {len(stranded)} proxy/proxies can no longer deploy"
        await audit(db, "Gateway resources updated", "APISIX Gateway", "Warning", details=details, object="Gateway")

    return {"allowlist": gateway["allowlist"]}
