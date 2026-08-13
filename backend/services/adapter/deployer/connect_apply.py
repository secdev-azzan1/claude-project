"""DeploymentPlan.connectors -> live Kafka Connect connectors (T7.2).

Thin orchestration over `services/kafka_connect_client.py` (connector CRUD
against the active `kafka_connect` connection) — every function here takes
the same plain `kc_conn` dict shape that module already uses:
`{"endpoint": <base url>, "auth_type": "NONE"|"BASIC"|"BEARER", "username"?,
"password"?, "token"?}`.

`stop_connectors` / `start_connectors` map onto Kafka Connect's PAUSE/RESUME
states, not a hard stop: `kafka_connect_client.py`'s current surface has no
`PUT /connectors/{name}/stop` (Kafka Connect's newer fully-stopped state,
KIP-875, offsets + config both retained but the connector fully
deprovisioned) — the task scope here is reusing that client's existing CRUD,
not extending it. PAUSED is the closest faithful equivalent to the NiFi-side
"stop (queues retained)" semantics: consumption halts, the connector's
config and committed offsets are both retained, and RESUME picks back up
where it left off — matching compiler-spec.md §7's stop/start verb pairing
one-for-one in spirit even though the underlying Connect run-state name
differs from NiFi's.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from services import kafka_connect_client
from services.adapter.compiler.ir import ConnectorSpec

logger = logging.getLogger(__name__)


class ConnectApplyError(RuntimeError):
    """Raised when a Connect operation that `lifecycle.py` treats as
    fatal (creating a connector at deploy time) fails. Verb-level
    start/stop/delete calls deliberately do NOT raise this — they return
    a per-connector result list instead, since one connector's hiccup
    should not block starting/stopping the rest of the flow (mirrors how
    `nifi_flow_manager`'s verb helpers report `{"ok": False, ...}` rather
    than raising)."""


async def create_connectors(kc_conn: Dict[str, Any], connectors: List[ConnectorSpec]) -> List[Dict[str, Any]]:
    """Create (or upsert) every connector in `connectors`, then immediately
    pause each one — compiler-spec.md §5: "Created stopped at deploy"."""
    created: List[Dict[str, Any]] = []
    for spec in connectors:
        upsert = await kafka_connect_client.upsert_connector(kc_conn, spec.name, spec.config)
        if not upsert.get("ok"):
            raise ConnectApplyError(f"Failed to create connector {spec.name!r}: {upsert.get('error')}")
        pause = await kafka_connect_client.pause_connector(kc_conn, spec.name)
        if not pause.get("ok"):
            # Non-fatal: the connector exists and is configured correctly,
            # it just started RUNNING instead of PAUSED. Surfaced in the
            # per-connector result rather than aborting the whole deploy.
            logger.warning("Connector %r created but could not be paused: %s", spec.name, pause.get("error"))
        created.append({"name": spec.name, "ownerBlockId": spec.ownerBlockId, "ok": True, "paused": bool(pause.get("ok"))})
    return created


async def start_connectors(kc_conn: Dict[str, Any], names: List[str]) -> List[Dict[str, Any]]:
    results = []
    for name in names:
        r = await kafka_connect_client.resume_connector(kc_conn, name)
        results.append({"name": name, "ok": bool(r.get("ok")), "error": r.get("error")})
    return results


async def stop_connectors(kc_conn: Dict[str, Any], names: List[str]) -> List[Dict[str, Any]]:
    results = []
    for name in names:
        r = await kafka_connect_client.pause_connector(kc_conn, name)
        results.append({"name": name, "ok": bool(r.get("ok")), "error": r.get("error")})
    return results


async def delete_connectors(kc_conn: Dict[str, Any], names: List[str]) -> List[Dict[str, Any]]:
    """Delete every named connector. A connector that is already gone
    (kafka_connect_client.delete_connector treats 404 as ok) counts as a
    success — undeploy/delete must be idempotent."""
    results = []
    for name in names:
        r = await kafka_connect_client.delete_connector(kc_conn, name)
        results.append({"name": name, "ok": bool(r.get("ok")), "error": r.get("error")})
    return results
