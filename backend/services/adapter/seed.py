"""Environment-driven seed for `connections_v2` (T2.2).

On an empty `connections_v2` collection, creates one connection per platform
type the environment has enough variables for (see backend/.env /
docker-compose.yml for the real local-dev values), activates each one
(first-of-type is always active -- matches routers/v2/connections.py's
create path), then runs a best-effort live test against each so a freshly
seeded connection's `health` starts out accurate instead of the default
"Not Tested".

A type with missing required env vars is skipped outright -- there is no
hardcoded fallback endpoint anywhere in this module (unlike the legacy
`server.py: seed_default_connections()`, which does default to hardcoded
devtunnels URLs; this v2 seed is deliberately honest instead: no env, no
connection).

Live-testing runs in a fixed order (nifi, kafka, apicurio, kafka_connect,
redis, apisix) -- the same TYPE_ORDER frontend/src/pages/Connections.tsx
renders in -- so that redis's "verified indirectly through NiFi" probe (see
routers/v2/connections.py's `run_connection_test`) sees a freshly tested
NiFi health value whenever NiFi was also seeded, rather than racing it.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase

from routers.v2.connections import run_connection_test
from services.adapter.common import COLLECTIONS, audit, new_id, now_iso

_TEST_ORDER = ["nifi", "kafka", "apicurio", "kafka_connect", "redis", "apisix"]


def _env(name: str) -> Optional[str]:
    value = os.environ.get(name)
    if isinstance(value, str):
        value = value.strip()
    return value or None


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _new_connection(ctype: str, name: str, config: Dict[str, Any], has_secret: bool) -> Dict[str, Any]:
    return {
        "id": new_id("conn"),
        "type": ctype,
        "name": name,
        "active": True,
        "health": "Not Tested",
        "reachability": "Unknown",
        "lastTestedAt": None,
        "config": config,
        "hasSecret": has_secret,
    }


def _build_from_env() -> List[Dict[str, Any]]:
    connections: List[Dict[str, Any]] = []

    # nifi: NIFI_URL / NIFI_USERNAME / NIFI_PASSWORD -> basic auth.
    nifi_url, nifi_user, nifi_pass = _env("NIFI_URL"), _env("NIFI_USERNAME"), _env("NIFI_PASSWORD")
    if nifi_url and nifi_user and nifi_pass:
        connections.append(
            _new_connection(
                "nifi",
                "Apache NiFi",
                {"url": nifi_url, "authMode": "basic", "username": nifi_user, "password": nifi_pass},
                True,
            )
        )

    # kafka: KAFKA_IN_CLUSTER_BOOTSTRAP (preferred) or KAFKA_BOOTSTRAP_SERVERS
    # for bootstrap, KAFBAT_URL for the Kafbat proxy -> mode "kafbat".
    kafka_bootstrap = _env("KAFKA_IN_CLUSTER_BOOTSTRAP") or _env("KAFKA_BOOTSTRAP_SERVERS")
    kafbat_url = _env("KAFBAT_URL")
    if kafka_bootstrap and kafbat_url:
        connections.append(
            _new_connection(
                "kafka",
                "Apache Kafka",
                {
                    "bootstrapServers": kafka_bootstrap,
                    "mode": "kafbat",
                    "proxyUrl": kafbat_url,
                    "kafbatUsername": _env("KAFBAT_USERNAME") or "",
                    "kafbatPassword": _env("KAFBAT_PASSWORD") or "",
                    "securityProtocol": "PLAINTEXT",
                },
                False,
            )
        )

    # apicurio: APICURIO_URL -> authMode "none".
    apicurio_url = _env("APICURIO_URL")
    if apicurio_url:
        connections.append(
            _new_connection("apicurio", "Apicurio Schema Registry", {"url": apicurio_url, "authMode": "none"}, False)
        )

    # kafka_connect: KAFKA_CONNECT_URL.
    kafka_connect_url = _env("KAFKA_CONNECT_URL")
    if kafka_connect_url:
        connections.append(_new_connection("kafka_connect", "Kafka Connect", {"url": kafka_connect_url}, False))

    # redis: REDIS_CONNECTION_STRING ("host:port"), REDIS_PASSWORD,
    # REDIS_DEDUP_DB, REDIS_BOOKMARKS_DB (default 0 / 1, matching the
    # frontend's defaultDraft).
    redis_conn_str = _env("REDIS_CONNECTION_STRING")
    if redis_conn_str:
        host, _, port_str = redis_conn_str.partition(":")
        host = host.strip()
        try:
            port = int(port_str) if port_str else 6379
        except ValueError:
            port = 6379
        if host:
            redis_password = _env("REDIS_PASSWORD")
            redis_config: Dict[str, Any] = {
                "host": host,
                "port": port,
                "dedupDb": _env_int("REDIS_DEDUP_DB", 0),
                "bookmarksDb": _env_int("REDIS_BOOKMARKS_DB", 1),
                "mode": "standalone",
            }
            if redis_password:
                redis_config["password"] = redis_password
            connections.append(_new_connection("redis", "Redis", redis_config, bool(redis_password)))

    # apisix: APISIX_ADMIN_URL + APISIX_RUNTIME_URL required; APISIX_ADMIN_KEY optional.
    apisix_admin_url = _env("APISIX_ADMIN_URL")
    apisix_runtime_url = _env("APISIX_RUNTIME_URL")
    if apisix_admin_url and apisix_runtime_url:
        apisix_admin_key = _env("APISIX_ADMIN_KEY")
        apisix_config: Dict[str, Any] = {"adminUrl": apisix_admin_url, "runtimeUrl": apisix_runtime_url}
        if apisix_admin_key:
            apisix_config["adminKey"] = apisix_admin_key
        connections.append(_new_connection("apisix", "API Gateway (APISIX)", apisix_config, bool(apisix_admin_key)))

    return connections


async def seed_v2_connections(db: AsyncIOMotorDatabase) -> List[Dict[str, Any]]:
    """If `connections_v2` is empty, create one connection per type the
    environment has enough variables for, each active, then best-effort
    live-test each one.

    Returns the list of created (and, where a probe ran, live-tested)
    connection documents -- empty when the collection was already seeded, or
    when no type had enough environment variables set to create one. Never
    wired into server.py by this module -- the startup orchestrator calls it.
    """
    existing = await db[COLLECTIONS.connections].find({}).to_list(1)
    if existing:
        return []

    connections = _build_from_env()
    if not connections:
        return []

    for conn in connections:
        await db[COLLECTIONS.connections].insert_one(dict(conn))
        await audit(
            db,
            action="Connection created",
            target=conn["name"],
            object="Platform Connection",
            status="Success",
            details="Seeded from environment",
        )

    by_type = {c["type"]: c for c in connections}
    for ctype in _TEST_ORDER:
        conn = by_type.get(ctype)
        if not conn:
            continue
        try:
            probe = await run_connection_test(db, conn)
        except Exception:
            # Best-effort: a live probe blowing up must never fail the seed.
            continue
        now = now_iso()
        await db[COLLECTIONS.connections].update_one(
            {"id": conn["id"]},
            {"$set": {"health": probe["health"], "reachability": probe["reachability"], "lastTestedAt": now}},
        )
        conn["health"] = probe["health"]
        conn["reachability"] = probe["reachability"]
        conn["lastTestedAt"] = now

    return connections
