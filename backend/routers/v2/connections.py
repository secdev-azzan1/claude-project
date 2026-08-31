"""Platform Connections v2 (T2.1 + T2.2).

CONTRACT SOURCE: frontend/src/prototype/api.ts (`listConnections`,
`saveConnection`, `testConnection`, `activateConnection`, `repointConnection`,
`deleteConnection`, `connectionDependents`) and frontend/src/prototype/types.ts
(`PlatformConnection`), with per-type config fields read off
frontend/src/pages/Connections.tsx's `defaultDraft` / `connectionToDraft` /
`draftToConnection` / `ConnectionFormFields`. This router is the server-side
counterpart of that mock: same guard reasons, same upsert semantics, same
one-active-per-type + dependents rules -- but backed by Mongo
(`connections_v2`) and dispatching REAL connectivity probes instead of the
mock's deterministic "legacy" substring check.

Config field names, per type (server-side; secrets ARE stored in `config`
here, unlike the frontend mock -- see models/adapter/_secrets.py's docstring
for why):
  nifi          -- url, authMode ("bearer"|"basic"), username?, + secret
                   token (bearer) or password (basic)
  kafka         -- bootstrapServers, mode ("native"|"kafbat"), proxyUrl?
                   (kafbat URL), securityProtocol, saslUsername?, + secret
                   saslPassword
  apicurio      -- url, authMode ("none"|"basic"|"bearer"), username?, +
                   secret token (bearer) or password (basic)
  kafka_connect -- url
  redis         -- host, port, dedupDb, bookmarksDb, mode:"standalone", +
                   secret password
  apisix        -- adminUrl, runtimeUrl, + secret adminKey

These are the LITERAL field names frontend/src/pages/Connections.tsx's
`draftToConnection` writes into `PlatformConnection.config` (e.g. kafka's
kafbat URL is `proxyUrl`, not `kafbatUrl`) -- read from that file directly
rather than guessed, since the task brief's own paraphrase drifts slightly
from the real field names.
"""
from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Response
from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import BaseModel

from db import get_db
from models.adapter import AppService, Flow, PlatformConnection
from models.adapter._secrets import SECRET_CONFIG_KEYS
from services import apicurio_client, apisix_client, kafka_client, kafka_connect_client, nifi_client
from services.adapter.common import COLLECTIONS, audit, new_id, now_iso
from services.adapter import nifi_repoint
from services.adapter.validation import block_proxy_id

router = APIRouter(prefix="/api/v2/connections", tags=["connections-v2"])

CONNECTION_TYPES = ("nifi", "kafka", "apicurio", "kafka_connect", "redis", "apisix")


# --------------------------------------------------------------------- utils


def _to_response(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Browser-safe, camelCase, redacted representation of a stored
    connection document -- see PlatformConnection.redact()."""
    conn = PlatformConnection.model_validate(doc)
    return conn.redact()


async def _get_conn_or_404(db: AsyncIOMotorDatabase, conn_id: str) -> Dict[str, Any]:
    doc = await db[COLLECTIONS.connections].find_one({"id": conn_id}, {"_id": 0})
    if not doc:
        raise HTTPException(status_code=404, detail="Connection not found")
    return doc


async def _count(db: AsyncIOMotorDatabase, collection: str, query: Optional[Dict[str, Any]] = None) -> int:
    """`len(find(...).to_list())` instead of `count_documents` -- the latter
    is not implemented by the FaultInjectingCollection test double this
    package's tests are built on (see tests/resilience/conftest.py), and a
    plain find+len is just as correct against real Mongo for the small
    (v2-adapter-scale) collections this router touches."""
    docs = await db[collection].find(query or {}, {"_id": 0}).to_list(None)
    return len(docs)


def _resolve_secret_key(ctype: str, config: Dict[str, Any]) -> Optional[str]:
    """Which SECRET_CONFIG_KEYS entry this type/config combination's secret
    (if any) is stored under."""
    if ctype in ("nifi", "apicurio"):
        auth_mode = config.get("authMode")
        if auth_mode == "bearer":
            return "token"
        if auth_mode == "basic":
            return "password"
        return None
    if ctype == "kafka":
        # kafbat mode authenticates to the Kafbat UI, native mode to the broker
        if config.get("mode") == "kafbat":
            return "kafbatPassword"
        return "saslPassword"
    if ctype == "redis":
        return "password"
    if ctype == "apisix":
        return "adminKey"
    return None  # kafka_connect never carries a secret


def _no_secret_possible(ctype: str, config: Dict[str, Any]) -> bool:
    """Mirrors draftToConnection's `noSecretPossible`."""
    if ctype == "kafka_connect":
        return True
    if ctype == "apicurio" and str(config.get("authMode") or "none") == "none":
        return True
    return False


def _validate_payload(ctype: str, config: Dict[str, Any], name: str, is_update: bool) -> Optional[str]:
    """Port of Connections.tsx's `validateDraft`. Returns the first error
    message, or None when the payload is acceptable."""
    if not (name or "").strip():
        return "Name is required."

    if ctype == "nifi":
        if not str(config.get("url") or "").strip():
            return "URL is required."
        auth_mode = config.get("authMode")
        if auth_mode == "basic" and not str(config.get("username") or "").strip():
            return "Username is required for basic auth."
        if not is_update and auth_mode == "bearer" and not str(config.get("token") or "").strip():
            return "Bearer token is required when creating this connection."
        if not is_update and auth_mode == "basic" and not str(config.get("password") or "").strip():
            return "Password is required when creating this connection."
    elif ctype == "kafka":
        if not str(config.get("bootstrapServers") or "").strip():
            return "Bootstrap servers are required."
        if config.get("mode") == "kafbat" and not str(config.get("proxyUrl") or "").strip():
            return "Proxy URL is required in kafbat mode."
        if str(config.get("securityProtocol") or "").startswith("SASL") and not str(config.get("saslUsername") or "").strip():
            return "SASL username is required for SASL protocols."
    elif ctype == "apicurio":
        if not str(config.get("url") or "").strip():
            return "URL is required."
        auth_mode = config.get("authMode")
        if auth_mode == "basic" and not str(config.get("username") or "").strip():
            return "Username is required for basic auth."
        if not is_update and auth_mode == "basic" and not str(config.get("password") or "").strip():
            return "Password is required when creating this connection."
        if not is_update and auth_mode == "bearer" and not str(config.get("token") or "").strip():
            return "Bearer token is required when creating this connection."
    elif ctype == "kafka_connect":
        if not str(config.get("url") or "").strip():
            return "URL is required."
    elif ctype == "redis":
        if not str(config.get("host") or "").strip():
            return "Host is required."
        try:
            port = int(config.get("port"))
        except (TypeError, ValueError):
            port = None
        if port is None or not (1 <= port <= 65535):
            return "Port must be 1-65535."
        try:
            dedup = int(config.get("dedupDb"))
        except (TypeError, ValueError):
            dedup = None
        try:
            bookmarks = int(config.get("bookmarksDb"))
        except (TypeError, ValueError):
            bookmarks = None
        if dedup is None or dedup < 0:
            return "Dedup logical DB must be a non-negative number."
        if bookmarks is None or bookmarks < 0:
            return "Bookmarks logical DB must be a non-negative number."
        if dedup == bookmarks:
            return "Dedup and bookmarks must use different logical databases."
    elif ctype == "apisix":
        if not str(config.get("adminUrl") or "").strip():
            return "Admin URL is required."
        if not str(config.get("runtimeUrl") or "").strip():
            return "Runtime URL is required."
    return None


# --------------------------------------------------------------- dependents


async def connection_dependents(db: AsyncIOMotorDatabase, conn: Dict[str, Any]) -> List[str]:
    """Deployed flow names that depend on `conn`.

    Mirrors frontend/src/prototype/api.ts's `connectionDependents`: only the
    ACTIVE connection of a type ever has dependents (flows follow "the
    active NiFi/Kafka/.../APISIX connection" of their type, never a specific
    inactive one -- an inactive connection is always a free delete). What
    "depends on it" means is type-specific:

      nifi / kafka / apicurio -- every deployed flow (deployedAt set).
      kafka_connect           -- deployed flows with a kafka_kc or kc block.
      redis                   -- deployed flows with a dedup transform on any
                                  block, or a jdbc block with incremental
                                  reads (config.incremental truthy).
      apisix                  -- deployed flows with an http block whose
                                  bound service (or, legacy, the block
                                  itself) references a gateway proxy --
                                  `block_proxy_id` (services/adapter/
                                  validation.py) is reused verbatim rather
                                  than re-implemented here.

    `deployedAt` set is the whole "is this flow deployed" test for now, per
    the task brief -- a flow's per-connection-id provenance (which specific
    NiFi/Kafka/Registry connection it was actually compiled against) is a
    finer-grained check the deployment engine will supply later.
    """
    if not conn.get("active"):
        return []
    ctype = conn.get("type")

    flow_docs = await db[COLLECTIONS.flows].find({}, {"_id": 0}).to_list(None)
    deployed = [Flow.model_validate(f) for f in flow_docs if f.get("deployedAt")]
    if not deployed:
        return []

    if ctype in ("nifi", "kafka", "apicurio"):
        return [f.name for f in deployed]

    if ctype == "kafka_connect":
        return [f.name for f in deployed if any(b.adapter in ("kafka_kc", "kc") for b in f.blocks)]

    if ctype == "redis":
        def _uses_redis(f: Flow) -> bool:
            for b in f.blocks:
                if any(t.kind == "dedup" for t in b.transforms):
                    return True
                if b.adapter == "jdbc" and bool((b.config or {}).get("incremental")):
                    return True
            return False

        return [f.name for f in deployed if _uses_redis(f)]

    if ctype == "apisix":
        service_docs = await db[COLLECTIONS.services].find({}, {"_id": 0}).to_list(None)
        services = [AppService.model_validate(s) for s in service_docs]

        def _uses_gateway(f: Flow) -> bool:
            return any(b.adapter == "http" and block_proxy_id(b, services) for b in f.blocks)

        return [f.name for f in deployed if _uses_gateway(f)]

    return []


# ------------------------------------------------------------- test dispatch


def _from_client_result(result: Dict[str, Any]) -> Dict[str, Any]:
    """Fold a services.*_client "uniform contract" result
    ({"ok", "reachable"?, "message"|"error", ...}) into
    {"health", "reachability", "message"}."""
    ok = bool(result.get("ok"))
    health = "Healthy" if ok else "Failed"
    reachable = result.get("reachable")
    if reachable is True:
        reachability = "Reachable"
    elif reachable is False:
        reachability = "Unreachable"
    else:
        reachability = "Reachable" if ok else "Unreachable"
    message = result.get("message") or result.get("error") or ("Connected." if ok else "Connection failed.")
    return {"health": health, "reachability": reachability, "message": str(message)}


async def run_connection_test(db: AsyncIOMotorDatabase, conn: Dict[str, Any]) -> Dict[str, Any]:
    """Dispatch a live connectivity probe for `conn` by type. Returns
    {"health", "reachability", "message"} -- never raises; the per-type
    client functions already catch their own exceptions and fold failures
    into an `ok: False` result.

    Type mapping (CONTRACT SOURCE, per the task brief):
      nifi          -> nifi_client.test_nifi_connection (token/basic + system-diagnostics)
      kafka         -> mode kafbat: kafka_client.test_kafbat_connection (Kafbat API probe)
                       mode native: kafka_client.test_kafka_connection (direct TCP;
                       legitimately Failed from a dev machine without broker access)
      apicurio      -> apicurio_client.test_apicurio_connection
      kafka_connect -> kafka_connect_client.test_kafka_connect_connection (GET / -> version)
      redis         -> cannot be dialed from the app host: "verified indirectly" --
                       Healthy when an active NiFi connection is itself Healthy,
                       else Not Tested with an explanatory message.
      apisix        -> apisix_client.test_admin(adminUrl, adminKey)
    """
    ctype = conn.get("type")
    config = conn.get("config") or {}

    if ctype == "nifi":
        auth_mode = str(config.get("authMode") or "bearer")
        auth_type = "BASIC" if auth_mode == "basic" else "BEARER"
        result = await nifi_client.test_nifi_connection(
            url=str(config.get("url") or ""),
            auth_type=auth_type,
            username=config.get("username"),
            password=config.get("password"),
            token=config.get("token"),
        )
        return _from_client_result(result)

    if ctype == "kafka":
        if str(config.get("mode") or "native") == "kafbat":
            result = await kafka_client.test_kafbat_connection(
                kafbat_url=str(config.get("proxyUrl") or ""),
                kafbat_username=str(config.get("kafbatUsername") or "") or None,
                kafbat_password=str(config.get("kafbatPassword") or "") or None,
            )
        else:
            result = await kafka_client.test_kafka_connection(
                bootstrap_servers=str(config.get("bootstrapServers") or ""),
                security_protocol=str(config.get("securityProtocol") or "PLAINTEXT"),
                sasl_username=config.get("saslUsername"),
                sasl_password=config.get("saslPassword"),
            )
        return _from_client_result(result)

    if ctype == "apicurio":
        auth_mode = str(config.get("authMode") or "none")
        auth_type = "BEARER" if auth_mode == "bearer" else ("BASIC" if auth_mode == "basic" else "NONE")
        result = await apicurio_client.test_apicurio_connection(
            url=str(config.get("url") or ""),
            auth_type=auth_type,
            username=config.get("username"),
            password=config.get("password"),
            token=config.get("token"),
        )
        return _from_client_result(result)

    if ctype == "kafka_connect":
        result = await kafka_connect_client.test_kafka_connect_connection(url=str(config.get("url") or ""))
        return _from_client_result(result)

    if ctype == "redis":
        active_nifi = await db[COLLECTIONS.connections].find_one({"type": "nifi", "active": True})
        if active_nifi and active_nifi.get("health") == "Healthy":
            return {
                "health": "Healthy",
                "reachability": "Reachable",
                "message": (
                    "Assumed reachable from NiFi (redis is cluster-internal); "
                    "verified at deploy via controller service validation"
                ),
            }
        return {
            "health": "Not Tested",
            "reachability": "Unknown",
            "message": (
                "Redis is cluster-internal and cannot be dialed directly from the app host; "
                "no active, healthy NiFi connection is available to verify reachability through either."
            ),
        }

    if ctype == "apisix":
        result = await apisix_client.test_admin(
            admin_url=str(config.get("adminUrl") or ""),
            admin_key=str(config.get("adminKey") or ""),
        )
        return _from_client_result(result)

    return {"health": "Not Tested", "reachability": "Unknown", "message": f"Unknown connection type '{ctype}'."}


# ------------------------------------------------------------------ routes


@router.get("/", summary="List platform connections (v2)")
async def list_connections_v2(db: AsyncIOMotorDatabase = Depends(get_db)):
    docs = await db[COLLECTIONS.connections].find({}, {"_id": 0}).to_list(None)
    return [_to_response(d) for d in docs]


@router.post("/", summary="Create or update a platform connection (v2)")
async def upsert_connection_v2(
    response: Response,
    payload: Dict[str, Any] = Body(...),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Mirrors api.ts `saveConnection`: a body carrying an `id` that matches
    an existing document updates it in place; anything else (no id, blank
    id, or an id nothing has) creates a new connection, auto-activating it
    when it is the first of its type."""
    ctype = payload.get("type")
    if ctype not in CONNECTION_TYPES:
        raise HTTPException(status_code=400, detail=f"Unknown connection type: {ctype!r}")

    name = str(payload.get("name") or "")
    incoming_config = dict(payload.get("config") or {})
    payload_id = str(payload.get("id") or "").strip()

    existing = await db[COLLECTIONS.connections].find_one({"id": payload_id}, {"_id": 0}) if payload_id else None
    is_update = existing is not None

    if is_update and existing.get("repointInProgress"):
        raise HTTPException(status_code=409, detail="This connection is locked by an active NiFi migration.")

    if is_update and existing.get("type") != ctype:
        raise HTTPException(status_code=400, detail="Connection type cannot be changed once created.")

    # api.ts saveConnection: an ACTIVE apicurio connection cannot be edited
    # while approved schemas are registered through it.
    if is_update and ctype == "apicurio" and existing.get("active"):
        schema_count = await _count(db, COLLECTIONS.schemas)
        if schema_count > 0:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"The registry connection cannot be edited while {schema_count} "
                    "approved schema(s) are registered through it."
                ),
            )

    error = _validate_payload(ctype, incoming_config, name, is_update)
    if error:
        raise HTTPException(status_code=400, detail=error)

    secret_key = _resolve_secret_key(ctype, incoming_config)
    no_secret_possible = _no_secret_possible(ctype, incoming_config)

    # Never persist a secret-shaped key that isn't this config's own secret slot.
    final_config = {k: v for k, v in incoming_config.items() if k not in SECRET_CONFIG_KEYS}
    has_secret = False
    if not no_secret_possible and secret_key:
        new_val = incoming_config.get(secret_key)
        if isinstance(new_val, str) and new_val.strip():
            final_config[secret_key] = new_val
            has_secret = True
        elif is_update:
            # Blank/absent secret on update keeps the existing value (write-only semantics).
            prior_val = (existing.get("config") or {}).get(secret_key)
            if isinstance(prior_val, str) and prior_val.strip():
                final_config[secret_key] = prior_val
                has_secret = True

    if is_update:
        conn_id = existing["id"]
        await db[COLLECTIONS.connections].update_one(
            {"id": conn_id},
            {"$set": {"name": name, "config": final_config, "hasSecret": has_secret}},
        )
        await audit(db, action="Connection updated", target=name, object="Platform Connection", status="Success")
        response.status_code = 200
        updated = await db[COLLECTIONS.connections].find_one({"id": conn_id}, {"_id": 0})
        return _to_response(updated)

    conn_id = new_id("conn")
    existing_of_type = await db[COLLECTIONS.connections].find_one({"type": ctype})
    doc = {
        "id": conn_id,
        "type": ctype,
        "name": name,
        "active": existing_of_type is None,
        "health": "Not Tested",
        "reachability": "Unknown",
        "lastTestedAt": None,
        "config": final_config,
        "hasSecret": has_secret,
    }
    # PyMongo mutates the inserted mapping by attaching an ObjectId under
    # `_id`. Insert a copy so the response remains the browser-safe v2 shape;
    # otherwise creation succeeds in Mongo but FastAPI fails while serializing
    # the ObjectId, making the UI report a save error for a card that exists.
    await db[COLLECTIONS.connections].insert_one(dict(doc))
    await audit(db, action="Connection created", target=name, object="Platform Connection", status="Success")
    response.status_code = 201
    return _to_response(doc)


@router.delete("/{conn_id}", status_code=204, summary="Delete a platform connection (v2)")
async def delete_connection_v2(conn_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    conn = await _get_conn_or_404(db, conn_id)
    if conn.get("repointInProgress"):
        raise HTTPException(status_code=409, detail="This connection is locked by an active NiFi migration.")

    deps = await connection_dependents(db, conn)
    if deps:
        preview = ", ".join(deps[:3]) + ("…" if len(deps) > 3 else "")
        raise HTTPException(
            status_code=409,
            detail=f"Cannot delete: {len(deps)} deployed flow(s) depend on it ({preview}).",
        )

    if conn.get("type") == "apicurio" and conn.get("active"):
        schema_count = await _count(db, COLLECTIONS.schemas)
        if schema_count > 0:
            raise HTTPException(
                status_code=409,
                detail=f"Cannot delete: {schema_count} approved schema(s) are registered through this registry connection.",
            )

    await db[COLLECTIONS.connections].delete_one({"id": conn_id})
    await audit(db, action="Connection deleted", target=conn.get("name", ""), object="Platform Connection", status="Warning")


@router.post("/{conn_id}/test", summary="Live-test a platform connection (v2)")
async def test_connection_v2(conn_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    conn = await _get_conn_or_404(db, conn_id)
    probe = await run_connection_test(db, conn)
    now = now_iso()
    await db[COLLECTIONS.connections].update_one(
        {"id": conn_id},
        {"$set": {"health": probe["health"], "reachability": probe["reachability"], "lastTestedAt": now}},
    )
    await audit(
        db,
        action="Connection tested" if probe["health"] == "Healthy" else "Connection test failed",
        target=conn.get("name", ""),
        object="Platform Connection",
        status="Success" if probe["health"] == "Healthy" else "Failed",
        details=probe["message"],
    )
    updated = await db[COLLECTIONS.connections].find_one({"id": conn_id}, {"_id": 0})
    out = _to_response(updated)
    out["message"] = probe["message"]
    return out


@router.post("/{conn_id}/activate", summary="Activate a platform connection (v2)")
async def activate_connection_v2(conn_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    conn = await _get_conn_or_404(db, conn_id)
    ctype = conn.get("type")
    locked = await db[COLLECTIONS.connections].find_one({"type": ctype, "repointInProgress": {"$ne": None}})
    if locked and locked.get("repointInProgress"):
        raise HTTPException(status_code=409, detail="A connection migration is already in progress.")

    peer = await db[COLLECTIONS.connections].find_one({"type": ctype, "active": True, "id": {"$ne": conn_id}})
    if peer:
        peer_deps = await connection_dependents(db, peer)
        # Redis is the sole exception (mirrors api.ts activateConnection):
        # switching it is allowed even with dependents -- the old instance's
        # dedup windows/bookmarks are simply lost, which the audit records.
        if peer_deps and ctype != "redis":
            raise HTTPException(
                status_code=409,
                detail=(
                    f'"{peer.get("name")}" has {len(peer_deps)} dependent flow(s) — '
                    "use Repoint (adopt / migrate / reset) instead of a bare activation."
                ),
            )

    probe = await run_connection_test(db, conn)
    now = now_iso()
    if probe["health"] == "Failed":
        await db[COLLECTIONS.connections].update_one(
            {"id": conn_id},
            {"$set": {"health": probe["health"], "reachability": probe["reachability"], "lastTestedAt": now}},
        )
        await audit(
            db,
            action="Connection activation failed",
            target=conn.get("name", ""),
            object="Platform Connection",
            status="Failed",
            details=probe["message"],
        )
        raise HTTPException(status_code=409, detail=f"Cannot activate: the connection is unreachable — {probe['message']}")

    if peer:
        await db[COLLECTIONS.connections].update_one({"id": peer["id"]}, {"$set": {"active": False}})
    await db[COLLECTIONS.connections].update_one(
        {"id": conn_id},
        {"$set": {"active": True, "health": probe["health"], "reachability": probe["reachability"], "lastTestedAt": now}},
    )
    await audit(
        db,
        action="Connection activated",
        target=conn.get("name", ""),
        object="Platform Connection",
        status="Success",
        details="Redis switch: dedup windows and bookmarks on the old instance are lost" if ctype == "redis" else None,
    )
    updated = await db[COLLECTIONS.connections].find_one({"id": conn_id}, {"_id": 0})
    return _to_response(updated)


class RepointRequest(BaseModel):
    mode: Literal["adopt", "migrate"]


@router.post("/{conn_id}/repoint", summary="Repoint a platform connection (v2)")
async def repoint_connection_v2(conn_id: str, body: RepointRequest, db: AsyncIOMotorDatabase = Depends(get_db)):
    """Identity-safe adopt or rollback-safe migration for Apache NiFi."""
    conn = await _get_conn_or_404(db, conn_id)
    if conn.get("type") != "nifi":
        raise HTTPException(status_code=501, detail="Safe repoint is currently implemented for Apache NiFi only.")
    if conn.get("active"):
        raise HTTPException(status_code=409, detail="This NiFi connection is already active.")
    try:
        result = await (nifi_repoint.adopt(db, conn) if body.mode == "adopt" else nifi_repoint.migrate(db, conn))
    except nifi_repoint.NifiRepointError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    updated = await db[COLLECTIONS.connections].find_one({"id": conn_id}, {"_id": 0})
    return {"connection": _to_response(updated), "result": result}


@router.get("/{conn_id}/impact", summary="Preview a platform connection's dependents (v2)")
async def get_connection_impact_v2(conn_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    conn = await _get_conn_or_404(db, conn_id)
    deps = await connection_dependents(db, conn)
    return {
        "connectionId": conn_id,
        "connectionType": conn.get("type"),
        "connectionName": conn.get("name", ""),
        "active": bool(conn.get("active")),
        "dependentFlowCount": len(deps),
        "dependentFlows": deps,
    }
