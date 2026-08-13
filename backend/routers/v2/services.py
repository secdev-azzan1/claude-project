"""Application Services v2 router.

Mirrors frontend/src/prototype/api.ts's `listServices` / `saveService`
(incl. its revision-bump semantics) / `retireService` / `reinstateService` /
`testService`, plus a REAL (non-simulated) live connectivity test per
service type -- the frontend mock fakes `testService` with a
name-contains-"legacy" check; this router actually calls out.

Contract sources (read in full before touching this file):
  - frontend/src/prototype/api.ts              -- listServices/saveService/...
  - frontend/src/prototype/types.ts             -- AppService shape
  - frontend/src/components/service-form/ServiceFormFields.tsx
                                                 -- exact config field names
                                                    (buildConfig / saveBlockReason)
  - backend/models/adapter/service.py           -- AppService + redact()
  - backend/models/adapter/_secrets.py          -- SECRET_CONFIG_KEYS + redact_config
  - backend/services/adapter/common.py          -- now_iso/new_id/audit/COLLECTIONS
  - backend/services/http_tls.py                -- tls_verify_enabled()
  - backend/services/iceberg_catalog_client.py  -- reused for the iceberg probe

Field-name notes (the frontend files above are the source of truth over any
paraphrase of them):
  - session_token's JSONPath field is literally named `tokenPath` in
    ServiceFormFields.tsx's `buildConfig()` (labelled "Token JSONPath" in
    the UI) -- NOT `tokenJsonPath`.
  - external_kafka's bootstrap field is literally named `bootstrapServers`
    in `buildConfig()`; `bootstrap` is accepted too as a defensive alias.
  - database dialect values are "postgresql" | "trino" | "mysql" (the UI's
    Select literally submits "mysql", not "mysql_mariadb").
  - session_token's login POST: ServiceFormFields.tsx does not collect a
    separate username/password for this auth mode (only loginPath /
    tokenPath / tokenHeader). The login request below is issued with
    whatever `username` / `password` happen to already be present in
    `config` (e.g. a private/manual service that carries them), which is a
    documented gap in the prototype UI rather than a contract deviation.

Secrets: server-side `config` may legitimately hold real secret values
(see `_secrets.py`'s docstring) because a real backend has to use them to
call out. `saveService` here merges blank/absent secret fields forward from
the existing record (frontend SecretField: "Leave blank to keep the
existing secret"). Past-revision snapshots appended to `revisions` store
their `config` pre-redacted (via `redact_config`), so secret values are
never duplicated across history and `AppService.redact()` -- which only
redacts the *current* `config` -- never has to reach into history to stay
safe.
"""

from __future__ import annotations

import asyncio
import re
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, Body, Depends, HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase

from db import get_db
from models.adapter import AppService
from models.adapter._secrets import SECRET_CONFIG_KEYS, redact_config
from services.adapter.common import COLLECTIONS, audit, new_id, now_iso
from services.http_tls import tls_verify_enabled
from services.iceberg_catalog_client import test_iceberg_connection

router = APIRouter(prefix="/api/v2/services", tags=["services-v2"])

HTTP_TIMEOUT_SECONDS = 10.0
TCP_PROBE_TIMEOUT_SECONDS = 5.0

_DB_DEFAULT_PORTS = {
    "postgresql": 5432,
    "mysql": 3306,
    "mysql_mariadb": 3306,
    "trino": 8080,
}


# --------------------------------------------------------------- persistence


def _has_any_secret(config: Dict[str, Any]) -> bool:
    return any(bool(config.get(key)) for key in SECRET_CONFIG_KEYS)


def _redact_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """Redact a stored revision-history snapshot's config (safe copy)."""
    snap = dict(snapshot)
    safe_config, flags = redact_config(dict(snap.get("config") or {}))
    snap["config"] = safe_config
    snap.update(flags)
    return snap


async def _service_dependents(db: AsyncIOMotorDatabase, service_id: str) -> List[str]:
    """Flow names referencing this service, mirroring api.ts's
    `serviceDependents`: `f.blocks.some(b => b.serviceId === id ||
    b.config.sinkServiceId === id)`."""
    flows = await db[COLLECTIONS.flows].find({}, {"_id": 0, "name": 1, "blocks": 1}).to_list(10000)
    names: List[str] = []
    for flow in flows:
        blocks = flow.get("blocks") or []
        hit = any(
            (block.get("serviceId") == service_id)
            or ((block.get("config") or {}).get("sinkServiceId") == service_id)
            for block in blocks
        )
        if hit:
            names.append(flow.get("name") or "")
    return names


# ------------------------------------------------------------------ validate


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise HTTPException(status_code=422, detail=message)


def _validate_service(service_type: Optional[str], name: str, config: Dict[str, Any]) -> None:
    """Mirrors ServiceFormFields.tsx's `saveBlockReason`, plus the
    http-authMode-specific required fields the task brief additionally
    asked for (saveBlockReason itself only checks `baseUrl`)."""
    if not service_type:
        raise HTTPException(status_code=422, detail="Pick a service type first.")
    if service_type not in ("http", "database", "external_kafka", "sink_destination"):
        raise HTTPException(status_code=422, detail=f"Unknown service type: {service_type!r}.")
    _require(bool(name.strip()), "Name the service.")

    if service_type == "http":
        _require(bool(str(config.get("baseUrl") or "").strip()), "Base URL is required.")
        auth_mode = config.get("authMode") or "none"
        if auth_mode == "basic":
            _require(bool(str(config.get("username") or "").strip()), "Username is required for basic auth.")
            _require(bool(config.get("password")), "Password is required for basic auth.")
        elif auth_mode == "bearer":
            _require(bool(config.get("token")), "Token is required for bearer auth.")
        elif auth_mode == "api_key":
            _require(bool(str(config.get("keyName") or "").strip()), "Key name is required for API key auth.")
            _require(bool(config.get("keyValue")), "Key value is required for API key auth.")
        elif auth_mode == "oauth2":
            _require(bool(str(config.get("tokenUrl") or "").strip()), "Token URL is required for OAuth2 auth.")
            _require(bool(str(config.get("clientId") or "").strip()), "Client id is required for OAuth2 auth.")
            _require(bool(config.get("clientSecret")), "Client secret is required for OAuth2 auth.")
        elif auth_mode == "session_token":
            _require(bool(str(config.get("loginPath") or "").strip()), "Login path is required for session token auth.")
            _require(bool(str(config.get("tokenPath") or "").strip()), "Token JSONPath is required for session token auth.")
            _require(bool(str(config.get("tokenHeader") or "").strip()), "Token header is required for session token auth.")
    elif service_type == "database":
        host_ok = bool(str(config.get("host") or "").strip())
        database_ok = bool(str(config.get("database") or "").strip())
        _require(host_ok and database_ok, "Host and database are required.")
        caps = config.get("capabilities") or []
        _require(bool(caps), "Select at least one capability.")
    elif service_type == "external_kafka":
        bootstrap = config.get("bootstrapServers") or config.get("bootstrap")
        _require(bool(str(bootstrap or "").strip()), "Bootstrap servers are required.")
    elif service_type == "sink_destination":
        kind = config.get("kind")
        if kind == "opensearch":
            _require(bool(str(config.get("url") or "").strip()), "OpenSearch URL is required.")
        elif kind == "iceberg_catalog":
            _require(bool(str(config.get("catalogUrl") or "").strip()), "Catalog URL is required.")
        else:
            raise HTTPException(
                status_code=422,
                detail="sink_destination requires config.kind of 'opensearch' or 'iceberg_catalog'.",
            )


# ----------------------------------------------------------------- endpoints


@router.get("/", summary="List Application Services (v2, redacted)")
async def list_services(db: AsyncIOMotorDatabase = Depends(get_db)):
    docs = await db[COLLECTIONS.services].find({}, {"_id": 0}).to_list(10000)
    return [AppService(**doc).redact() for doc in docs]


@router.post("/", summary="Create or update an Application Service (v2, revision bump)")
async def save_service(payload: Dict[str, Any] = Body(...), db: AsyncIOMotorDatabase = Depends(get_db)):
    svc_id = str(payload.get("id") or "").strip()
    existing = await db[COLLECTIONS.services].find_one({"id": svc_id}, {"_id": 0}) if svc_id else None

    service_type = payload.get("type") or (existing or {}).get("type")
    name = str(payload.get("name") or "").strip()

    incoming_config = dict(payload.get("config") or {})
    existing_config = dict((existing or {}).get("config") or {})
    # Blank/absent secrets keep the existing value (SecretField: "Leave
    # blank to keep the existing secret").
    merged_config = dict(incoming_config)
    for key in SECRET_CONFIG_KEYS:
        if not incoming_config.get(key) and existing_config.get(key):
            merged_config[key] = existing_config[key]

    _validate_service(service_type, name, merged_config)

    now = now_iso()
    if existing is None:
        doc_id = svc_id or new_id("svc")
        doc: Dict[str, Any] = {
            "id": doc_id,
            "type": service_type,
            "name": name,
            "revision": 1,
            "retired": False,
            "private": payload.get("private"),
            "health": "Not Tested",
            "lastTestedAt": None,
            "config": merged_config,
            "hasSecret": _has_any_secret(merged_config),
            "createdAt": now,
            "updatedAt": now,
            "revisions": [],
        }
        await db[COLLECTIONS.services].insert_one(dict(doc))
        await audit(db, "Service created", name, object="Application Service")
    else:
        prior_snapshot = _redact_snapshot(
            {
                "revision": existing.get("revision", 1),
                "config": existing.get("config") or {},
                "name": existing.get("name"),
                "type": existing.get("type"),
                "updatedAt": existing.get("updatedAt"),
            }
        )
        revisions = list(existing.get("revisions") or [])
        revisions.append(prior_snapshot)
        next_revision = int(existing.get("revision") or 1) + 1
        doc = {
            **existing,
            "type": service_type,
            "name": name,
            "revision": next_revision,
            "retired": bool(payload.get("retired", existing.get("retired", False))),
            "private": payload.get("private", existing.get("private")),
            "config": merged_config,
            "hasSecret": _has_any_secret(merged_config),
            "updatedAt": now,
            "revisions": revisions,
        }
        await db[COLLECTIONS.services].update_one({"id": svc_id}, {"$set": doc})
        await audit(
            db,
            "Service revision created",
            f"{name} (rev {next_revision})",
            status="Success",
            details="Linked flows adopt at next deploy",
            object="Application Service",
        )

    return AppService(**doc).redact()


@router.post("/{service_id}/retire", summary="Retire an Application Service (v2)")
async def retire_service(service_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    doc = await db[COLLECTIONS.services].find_one({"id": service_id}, {"_id": 0})
    if not doc:
        raise HTTPException(status_code=404, detail="Service not found")

    now = now_iso()
    await db[COLLECTIONS.services].update_one({"id": service_id}, {"$set": {"retired": True, "updatedAt": now}})
    dependents = await _service_dependents(db, service_id)
    details = f"{len(dependents)} dependent flow(s) flagged: action required" if dependents else None
    await audit(db, "Service retired", doc.get("name", ""), status="Warning", details=details, object="Application Service")

    updated = {**doc, "retired": True, "updatedAt": now}
    return AppService(**updated).redact()


@router.post("/{service_id}/reinstate", summary="Reinstate a retired Application Service (v2)")
async def reinstate_service(service_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    doc = await db[COLLECTIONS.services].find_one({"id": service_id}, {"_id": 0})
    if not doc:
        raise HTTPException(status_code=404, detail="Service not found")

    now = now_iso()
    await db[COLLECTIONS.services].update_one({"id": service_id}, {"$set": {"retired": False, "updatedAt": now}})
    await audit(db, "Service reinstated", doc.get("name", ""), object="Application Service")

    updated = {**doc, "retired": False, "updatedAt": now}
    return AppService(**updated).redact()


@router.post("/{service_id}/test", summary="Live-test an Application Service (v2)")
async def test_service_endpoint(service_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    doc = await db[COLLECTIONS.services].find_one({"id": service_id}, {"_id": 0})
    if not doc:
        raise HTTPException(status_code=404, detail="Service not found")

    result = await _run_service_test(doc)
    now = now_iso()
    await db[COLLECTIONS.services].update_one(
        {"id": service_id},
        {"$set": {"health": result["health"], "lastTestedAt": now, "updatedAt": now}},
    )
    await audit(
        db,
        "Service tested" if result["ok"] else "Service test failed",
        doc.get("name", ""),
        status="Success" if result["ok"] else "Failed",
        details=result.get("message"),
        object="Application Service",
    )

    updated = {**doc, "health": result["health"], "lastTestedAt": now, "updatedAt": now}
    return AppService(**updated).redact()


# ------------------------------------------------------------------ testing


async def _run_service_test(doc: Dict[str, Any]) -> Dict[str, Any]:
    service_type = doc.get("type")
    config = dict(doc.get("config") or {})
    try:
        if service_type == "http":
            return await _test_http(config)
        if service_type == "database":
            return await _test_database(config)
        if service_type == "external_kafka":
            return await _test_kafka(config)
        if service_type == "sink_destination":
            return await _test_sink(config)
    except Exception as exc:  # pragma: no cover - defensive catch-all
        return {"ok": False, "health": "Failed", "message": f"Unexpected test error: {exc}"[:300]}
    return {"ok": False, "health": "Failed", "message": f"Unsupported service type {service_type!r}."}


# ---- tiny JSONPath resolver ($.a.b / $.a[0].b) -----------------------------


def _resolve_jsonpath(data: Any, path: str) -> Any:
    """Minimal JSONPath resolver: supports "$.a.b", "a.b", and "$.a[0].b"."""
    if not path:
        return None
    trimmed = path.strip()
    if trimmed.startswith("$"):
        trimmed = trimmed[1:]
    trimmed = trimmed.lstrip(".")
    if not trimmed:
        return data
    tokens = re.findall(r"[^.\[\]]+|\[\d+\]", trimmed)
    current = data
    for token in tokens:
        if token.startswith("[") and token.endswith("]"):
            if not isinstance(current, list):
                return None
            idx = int(token[1:-1])
            if idx >= len(current) or idx < -len(current):
                return None
            current = current[idx]
        else:
            if not isinstance(current, dict) or token not in current:
                return None
            current = current[token]
    return current


# ---- shared TCP probe -------------------------------------------------------


async def _tcp_probe(host: str, port: int, timeout: float) -> Tuple[bool, str]:
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return True, f"TCP connection to {host}:{port} succeeded."
    except asyncio.TimeoutError:
        return False, f"TCP connection to {host}:{port} timed out after {timeout}s."
    except OSError as exc:
        return False, f"TCP connection to {host}:{port} failed: {exc}"


def _parse_host_port(hostport: str, default_port: int) -> Tuple[str, int]:
    hostport = hostport.strip()
    if ":" in hostport:
        host, _, port_s = hostport.rpartition(":")
        try:
            return host, int(port_s)
        except ValueError:
            return hostport, default_port
    return hostport, default_port


# ---- http --------------------------------------------------------------


def _classify_http_response(resp: httpx.Response, auth_mode: str) -> Dict[str, Any]:
    code = resp.status_code
    if 200 <= code < 400:
        return {"ok": True, "health": "Healthy", "message": f"Reachable — HTTP {code}."}
    if code in (401, 403):
        if auth_mode == "none":
            return {"ok": True, "health": "Healthy", "message": f"Reachable — HTTP {code} (auth required)."}
        return {"ok": False, "health": "Failed", "message": f"Authentication failed — HTTP {code}."}
    return {"ok": False, "health": "Failed", "message": f"Unexpected response — HTTP {code}."}


async def _test_http(config: Dict[str, Any]) -> Dict[str, Any]:
    base_url = str(config.get("baseUrl") or "").strip()
    if not base_url:
        return {"ok": False, "health": "Failed", "message": "No base URL configured."}
    auth_mode = config.get("authMode") or "none"

    if auth_mode == "session_token":
        return await _test_http_session_token(base_url, config)
    if auth_mode == "oauth2":
        return await _test_http_oauth2(base_url, config)

    headers: Dict[str, str] = {}
    params: Dict[str, str] = {}
    auth = None
    if auth_mode == "basic":
        auth = (str(config.get("username") or ""), str(config.get("password") or ""))
    elif auth_mode == "bearer":
        headers["Authorization"] = f"Bearer {config.get('token') or ''}"
    elif auth_mode == "api_key":
        key_name = str(config.get("keyName") or "")
        key_value = str(config.get("keyValue") or "")
        if str(config.get("keyLocation") or "header") == "query":
            params[key_name] = key_value
        else:
            headers[key_name] = key_value

    try:
        async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=HTTP_TIMEOUT_SECONDS) as client:
            resp = await client.head(base_url, headers=headers, params=params, auth=auth)
            if resp.status_code >= 400:
                resp = await client.get(base_url, headers=headers, params=params, auth=auth)
            return _classify_http_response(resp, auth_mode)
    except httpx.TimeoutException:
        return {"ok": False, "health": "Failed", "message": f"Connection timed out after {HTTP_TIMEOUT_SECONDS:.0f}s."}
    except httpx.ConnectError as exc:
        return {"ok": False, "health": "Failed", "message": f"Cannot connect to {base_url}: {exc}"[:300]}
    except httpx.HTTPError as exc:
        return {"ok": False, "health": "Failed", "message": str(exc)[:300]}


async def _test_http_session_token(base_url: str, config: Dict[str, Any]) -> Dict[str, Any]:
    login_path = str(config.get("loginPath") or "")
    token_path = str(config.get("tokenPath") or "")
    token_header = str(config.get("tokenHeader") or "Authorization")
    login_url = f"{base_url.rstrip('/')}/{login_path.lstrip('/')}" if login_path else base_url

    try:
        async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=HTTP_TIMEOUT_SECONDS) as client:
            login_resp = await client.post(
                login_url,
                json={"username": config.get("username"), "password": config.get("password")},
            )
            if login_resp.status_code >= 400:
                return {"ok": False, "health": "Failed", "message": f"Login failed — HTTP {login_resp.status_code}."}
            try:
                body = login_resp.json()
            except Exception:
                return {"ok": False, "health": "Failed", "message": "Login response was not valid JSON."}
            token = _resolve_jsonpath(body, token_path)
            if not token:
                return {
                    "ok": False,
                    "health": "Failed",
                    "message": f"Token JSONPath {token_path!r} did not resolve to a value in the login response.",
                }
            follow_resp = await client.get(base_url, headers={token_header: str(token)})
            return _classify_http_response(follow_resp, "session_token")
    except httpx.TimeoutException:
        return {"ok": False, "health": "Failed", "message": f"Connection timed out after {HTTP_TIMEOUT_SECONDS:.0f}s."}
    except httpx.ConnectError as exc:
        return {"ok": False, "health": "Failed", "message": f"Cannot connect to {login_url}: {exc}"[:300]}
    except httpx.HTTPError as exc:
        return {"ok": False, "health": "Failed", "message": str(exc)[:300]}


async def _test_http_oauth2(base_url: str, config: Dict[str, Any]) -> Dict[str, Any]:
    token_url = str(config.get("tokenUrl") or "").strip()
    if not token_url:
        return {"ok": False, "health": "Failed", "message": "No token URL configured for OAuth2."}
    client_id = str(config.get("clientId") or "")
    client_secret = str(config.get("clientSecret") or "")

    try:
        async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=HTTP_TIMEOUT_SECONDS) as client:
            token_resp = await client.post(
                token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                },
            )
            if token_resp.status_code >= 400:
                return {
                    "ok": False,
                    "health": "Failed",
                    "message": f"OAuth2 token request failed — HTTP {token_resp.status_code}.",
                }
            try:
                body = token_resp.json()
            except Exception:
                return {"ok": False, "health": "Failed", "message": "OAuth2 token response was not valid JSON."}
            token = body.get("access_token")
            if not token:
                return {"ok": False, "health": "Failed", "message": "OAuth2 token response did not contain access_token."}
            follow_resp = await client.get(base_url, headers={"Authorization": f"Bearer {token}"})
            return _classify_http_response(follow_resp, "oauth2")
    except httpx.TimeoutException:
        return {"ok": False, "health": "Failed", "message": f"Connection timed out after {HTTP_TIMEOUT_SECONDS:.0f}s."}
    except httpx.ConnectError as exc:
        return {"ok": False, "health": "Failed", "message": f"Cannot connect to {token_url}: {exc}"[:300]}
    except httpx.HTTPError as exc:
        return {"ok": False, "health": "Failed", "message": str(exc)[:300]}


# ---- database ------------------------------------------------------------


async def _test_database(config: Dict[str, Any]) -> Dict[str, Any]:
    dialect = str(config.get("dialect") or "").strip().lower()
    host = str(config.get("host") or "").strip()
    if not host:
        return {"ok": False, "health": "Failed", "message": "No host configured."}

    raw_port = config.get("port")
    try:
        port = int(raw_port) if raw_port else _DB_DEFAULT_PORTS.get(dialect, 5432)
    except (TypeError, ValueError):
        port = _DB_DEFAULT_PORTS.get(dialect, 5432)

    if dialect == "trino":
        url = f"http://{host}:{port}/v1/info"
        try:
            async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=HTTP_TIMEOUT_SECONDS) as client:
                resp = await client.get(url)
            if resp.status_code < 400:
                return {"ok": True, "health": "Healthy", "message": f"Trino coordinator reachable — HTTP {resp.status_code}."}
            return {"ok": False, "health": "Failed", "message": f"Trino coordinator returned HTTP {resp.status_code}."}
        except httpx.TimeoutException:
            return {"ok": False, "health": "Failed", "message": f"Connection to Trino coordinator timed out after {HTTP_TIMEOUT_SECONDS:.0f}s."}
        except httpx.HTTPError as exc:
            return {"ok": False, "health": "Failed", "message": f"Cannot reach Trino coordinator at {url}: {exc}"[:300]}

    # postgresql / mysql (mariadb) -- TCP reachability probe only. A full
    # driver login (asyncpg / aiomysql) is out of scope for this probe.
    ok, message = await _tcp_probe(host, port, timeout=TCP_PROBE_TIMEOUT_SECONDS)
    note = " Full driver login was not attempted — this is a TCP reachability probe only."
    if ok:
        return {"ok": True, "health": "Healthy", "message": f"{message}{note}"}
    return {"ok": False, "health": "Failed", "message": f"{message}{note}"}


# ---- external_kafka --------------------------------------------------------


async def _test_kafka(config: Dict[str, Any]) -> Dict[str, Any]:
    bootstrap = str(config.get("bootstrapServers") or config.get("bootstrap") or "").strip()
    if not bootstrap:
        return {"ok": False, "health": "Failed", "message": "No bootstrap servers configured."}
    first = bootstrap.split(",")[0].strip()
    host, port = _parse_host_port(first, 9092)
    ok, message = await _tcp_probe(host, port, timeout=TCP_PROBE_TIMEOUT_SECONDS)
    note = " Brokers may be cluster-internal and unreachable from this host."
    if ok:
        return {"ok": True, "health": "Healthy", "message": f"{message}"}
    return {"ok": False, "health": "Failed", "message": f"{message}{note}"}


# ---- sink_destination -------------------------------------------------------


async def _test_sink(config: Dict[str, Any]) -> Dict[str, Any]:
    kind = config.get("kind")
    if kind == "opensearch":
        return await _test_opensearch(config)
    if kind == "iceberg_catalog":
        return await _test_iceberg(config)
    return {"ok": False, "health": "Failed", "message": f"Unknown sink kind {kind!r}."}


async def _test_opensearch(config: Dict[str, Any]) -> Dict[str, Any]:
    url = str(config.get("url") or "").strip()
    if not url:
        return {"ok": False, "health": "Failed", "message": "No URL configured."}
    auth = None
    username = config.get("username")
    if username:
        auth = (str(username), str(config.get("password") or ""))
    try:
        async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=HTTP_TIMEOUT_SECONDS) as client:
            resp = await client.get(url, auth=auth)
        if (200 <= resp.status_code < 300) or resp.status_code == 401:
            return {"ok": True, "health": "Healthy", "message": f"OpenSearch reachable — HTTP {resp.status_code}."}
        return {"ok": False, "health": "Failed", "message": f"OpenSearch returned HTTP {resp.status_code}."}
    except httpx.TimeoutException:
        return {"ok": False, "health": "Failed", "message": f"Connection timed out after {HTTP_TIMEOUT_SECONDS:.0f}s."}
    except httpx.HTTPError as exc:
        return {"ok": False, "health": "Failed", "message": f"Cannot connect to {url}: {exc}"[:300]}


async def _test_iceberg(config: Dict[str, Any]) -> Dict[str, Any]:
    catalog_url = str(config.get("catalogUrl") or "").strip()
    if not catalog_url:
        return {"ok": False, "health": "Failed", "message": "No catalog URL configured."}
    warehouse = str(config.get("warehouse") or "bronze")
    client_id = config.get("oauthClientId")
    client_secret = config.get("oauthClientSecret")
    credential = f"{client_id}:{client_secret}" if client_id and client_secret else None

    conn = {
        "endpoint": catalog_url,
        "iceberg_warehouse": warehouse,
        "iceberg_credential": credential,
    }
    result = await test_iceberg_connection(conn)
    if result.get("ok"):
        return {"ok": True, "health": "Healthy", "message": result.get("message") or "Iceberg catalog reachable."}
    return {"ok": False, "health": "Failed", "message": result.get("error") or "Iceberg catalog unreachable."}
