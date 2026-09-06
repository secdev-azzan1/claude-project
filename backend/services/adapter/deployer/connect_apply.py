"""DeploymentPlan.connectors -> live Kafka Connect connectors (T7.2).

Thin orchestration over `services/kafka_connect_client.py` (connector CRUD
against the active `kafka_connect` connection) — every function here takes
the same plain `kc_conn` dict shape that module already uses:
`{"endpoint": <base url>, "auth_type": "NONE"|"BASIC"|"BEARER", "username"?,
"password"?, "token"?}`.

`stop_connectors` / `start_connectors` map onto Kafka Connect's fully-stopped
/ RUNNING states via `PUT /connectors/{name}/stop` and `/resume`
(`kafka_connect_client.py` lines ~350/374) — not the legacy PAUSE/RESUME
pair. STOP (KIP-875) fully deprovisions the connector's tasks while
retaining both config and committed offsets, which is a strictly cleaner
match for the NiFi-side "stop (queues retained)" semantics than PAUSE ever
was: a paused connector still holds worker resources and can drift into
FAILED on its own; a stopped one just sits there until resumed. Matches
compiler-spec.md §7's stop/start verb pairing one-for-one.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

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
    than raising).

    `surviving_names`: when a create fails partway through a multi-connector
    deploy, `create_connectors` best-effort deletes the connectors it had
    already created before raising. If that cleanup itself fails for some of
    them, their names are carried here rather than dropped — the caller
    (`lifecycle._deploy_impl`) stamps them onto the flow's `drift` field so
    they are not silently forgotten just because the deploy as a whole
    failed."""

    def __init__(self, message: str, surviving_names: Optional[List[str]] = None) -> None:
        super().__init__(message)
        self.surviving_names: List[str] = list(surviving_names or [])


async def _cleanup_partial_create(kc_conn: Dict[str, Any], names: List[str]) -> List[str]:
    """Best-effort delete of connectors created earlier in a `create_connectors`
    run that then failed partway through. Returns the names that could NOT be
    deleted (i.e. would otherwise be orphaned) so the caller can carry them
    forward instead of losing track of them."""
    surviving: List[str] = []
    for name in names:
        r = await kafka_connect_client.delete_connector(kc_conn, name)
        if not r.get("ok"):
            surviving.append(name)
    return surviving


async def create_connectors(kc_conn: Dict[str, Any], connectors: List[ConnectorSpec]) -> List[Dict[str, Any]]:
    """Create (or upsert) every connector in `connectors`, then immediately
    stop each one — compiler-spec.md §5: "Created stopped at deploy", now via
    the fully-stopped state (see module docstring) rather than PAUSE, so a
    freshly deployed flow's connectors land in the same resting state that
    `stop`/`undeploy` leave them in — one resting state, not two.

    If a connector fails to create, the ones already created in this call
    are rolled back (deleted) before raising, so a partial deploy doesn't
    leave live connectors behind that nothing else knows about."""
    created: List[Dict[str, Any]] = []
    for spec in connectors:
        upsert = await kafka_connect_client.upsert_connector(kc_conn, spec.name, spec.config)
        if not upsert.get("ok"):
            surviving = await _cleanup_partial_create(kc_conn, [c["name"] for c in created])
            raise ConnectApplyError(
                f"Failed to create connector {spec.name!r}: {upsert.get('error')}",
                surviving_names=surviving,
            )
        stop = await kafka_connect_client.stop_connector(kc_conn, spec.name)
        if not stop.get("ok"):
            # Non-fatal: the connector exists and is configured correctly,
            # it just started RUNNING instead of STOPPED. Surfaced in the
            # per-connector result rather than aborting the whole deploy.
            logger.warning("Connector %r created but could not be stopped: %s", spec.name, stop.get("error"))
        created.append({"name": spec.name, "ownerBlockId": spec.ownerBlockId, "ok": True, "stopped": bool(stop.get("ok"))})
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
        r = await kafka_connect_client.stop_connector(kc_conn, name)
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
