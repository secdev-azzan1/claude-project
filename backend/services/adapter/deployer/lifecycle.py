"""Flow lifecycle verbs (T7.2+T7.3+T7.4) — `deploy`/`redeploy`/`start`/
`pause`/`resume`/`stop`/`stop_clear`/`undeploy`/`delete`/`clear_dedup_cache`,
operating on a `flows_v2` document + `db`. `routers/v2/flows.py` dispatches
here after its own transition-guard checks (`_get_verb_block_reason`) pass —
those guards are unchanged; this module assumes the transition is already
legal and just does the work.

This is the only module in `services/adapter/deployer` that touches Mongo —
`nifi_apply` / `connect_apply` / `topics` are pure "given a plain connection
dict and a plan/id, talk to the live system" modules. Every `_*_conn_dict`
helper below is the translation from a `connections_v2` `PlatformConnection`
document's config shape (`url`/`authMode`/... for nifi,
`bootstrapServers`/`mode`/`proxyUrl`/... for kafka) into the plain dict
shape those modules — and `services/nifi_client.py` /
`services/kafka_client.py` themselves — already use
(`endpoint`/`auth_type`/... ).

Dedup cache clearing (`clear_dedup_cache`): Redis is cluster-internal and
not reachable by TCP from this app host (see `services/kafka_client.py`'s
and `routers/v2/connections.py`'s `run_connection_test`'s own notes on the
same constraint for Kafka) — there is no way to issue a real `FLUSHDB`/key-
pattern delete against the dedup cache's Redis instance from here, only from
inside the cluster (e.g. a NiFi controller-service-scoped action, which
NiFi's REST API does not expose either — `POST
/processors/{id}/state/clear-requests` clears NiFi's own processor
*component* state, not an external distributed cache service's backing
store). The honest substitute implemented here: every `DetectDuplicate`
cache key this flow's dedup Groovy script produces is prefixed
`<flowToken>__<blockId>[__e<epoch>]` (`services/adapter/compiler/
transforms.py`'s `_compile_dedup`); bumping `dedupEpoch` on the block's
dedup transform rule changes that prefix, so every key already sitting in
Redis for this block is silently orphaned (never matched again) the next
time the flow is redeployed with the new epoch compiled in. It is a real
flush of this block's keys, just deferred to the next redeploy rather than
instantaneous — callers are told this via the `redeployRequired` flag this
function returns.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from models.adapter import AppService, ApprovedSchema, Flow, GatewayProxy, PlatformConnection
from services import kafka_connect_client
from services.adapter.common import COLLECTIONS, audit, now_iso
from services.adapter.compiler import CompileContext, CompileError, DeploymentPlan, compile_flow
from services.adapter.legality import root_block
from services.adapter.naming import dlq_name
from services.adapter.validation import GatewaySnapshot, deploy_preflight
from services.connection_fingerprint import probe_kafka_connect_fingerprint, probe_nifi_fingerprint

from . import connect_apply, nifi_apply, topics

logger = logging.getLogger(__name__)


class DeployPreflightFailed(Exception):
    """Raised by `deploy()` when one or more preflight rows fail. `rows` is
    the full `[{"label", "ok", "detail"}, ...]` list (passing AND failing
    rows both included, mirroring the frontend `deployPreflight()` panel) —
    `routers/v2/flows.py` turns this into a 422 carrying every row."""

    def __init__(self, rows: List[Dict[str, Any]]):
        super().__init__("Deploy preflight failed")
        self.rows = rows


class LifecycleError(RuntimeError):
    """Raised when a verb's underlying NiFi/Kafka Connect/Kafka operation
    fails outright (no active connection configured, `NifiApplyError`,
    `ConnectApplyError`, a failed stop/start...). `routers/v2/flows.py`
    turns this into a 502."""


# --------------------------------------------------------------------- loaders


async def _load_services(db) -> List[AppService]:
    docs = await db[COLLECTIONS.services].find({}, {"_id": 0}).to_list(None)
    return [AppService(**d) for d in docs]


async def _load_schemas(db) -> List[ApprovedSchema]:
    docs = await db[COLLECTIONS.schemas].find({}, {"_id": 0}).to_list(None)
    return [ApprovedSchema(**d) for d in docs]


async def _load_connections(db) -> List[PlatformConnection]:
    docs = await db[COLLECTIONS.connections].find({}, {"_id": 0}).to_list(None)
    return [PlatformConnection(**d) for d in docs]


async def _load_gateway(db) -> GatewaySnapshot:
    doc = await db[COLLECTIONS.gateway].find_one({}, {"_id": 0}) or {}
    proxies = [GatewayProxy(**p) for p in (doc.get("proxies") or [])]
    allowlist = list(doc.get("allowlist") or [])
    return GatewaySnapshot(proxies=proxies, allowlist=allowlist)


def _active_connection(connections: List[PlatformConnection], type_: str) -> Optional[PlatformConnection]:
    return next((c for c in connections if c.type == type_ and c.active), None)


# ------------------------------------------------------- connection-dict adapters


def _nifi_conn_dict(conn: PlatformConnection) -> Dict[str, Any]:
    cfg = conn.config or {}
    auth_mode = str(cfg.get("authMode") or "bearer")
    return {
        "endpoint": cfg.get("url") or "",
        "auth_type": "BASIC" if auth_mode == "basic" else "BEARER",
        "username": cfg.get("username"),
        "password": cfg.get("password"),
        "token": cfg.get("token"),
    }


def _kafka_connect_conn_dict(conn: PlatformConnection) -> Dict[str, Any]:
    cfg = conn.config or {}
    return {"endpoint": cfg.get("url") or "", "auth_type": "NONE", "username": None, "password": None, "token": None}


def _kafka_conn_dict(conn: PlatformConnection) -> Dict[str, Any]:
    cfg = conn.config or {}
    mode = str(cfg.get("mode") or "native")
    return {
        "endpoint": cfg.get("bootstrapServers") or "",
        "security_protocol": cfg.get("securityProtocol") or "PLAINTEXT",
        "sasl_mechanism": cfg.get("saslMechanism"),
        "sasl_username": cfg.get("saslUsername"),
        "sasl_password": cfg.get("saslPassword"),
        "kafka_connection_mode": mode,
        "kafbat_url": cfg.get("proxyUrl") if mode == "kafbat" else None,
        "kafbat_username": cfg.get("kafbatUsername"),
        "kafbat_password": cfg.get("kafbatPassword"),
    }


def _build_compile_context(
    services: List[AppService], connections: List[PlatformConnection], gateway: GatewaySnapshot, schemas_for_flow: Dict[str, ApprovedSchema],
) -> CompileContext:
    return CompileContext(
        services={s.id: s for s in services},
        connections={c.type: c for c in connections if c.active},
        gateway_proxies={p.id: p for p in gateway.proxies},
        approved_schemas=schemas_for_flow,
    )


# --------------------------------------------------------------------- preflight

_CONNECT_PLUGIN_CLASSES = {
    "iceberg_catalog": "org.apache.iceberg.connect.IcebergSinkConnector",
    "opensearch": "io.aiven.kafka.connect.opensearch.OpensearchSinkConnector",
}


def _needed_connector_classes(flow: Flow, services: List[AppService]) -> List[str]:
    """Connector classes this flow's kafka_kc/kc blocks need installed on
    the live Kafka Connect cluster — mirrors
    `services/adapter/compiler/connectors.py`'s own `kind -> connector.class`
    mapping (duplicated here rather than imported: these are stable
    third-party plugin class names, not compiler internals, and deriving
    them without a full `CompileContext`/compile pass keeps this check
    usable even when other preflight rows are still failing)."""
    services_by_id = {s.id: s for s in services}
    classes: List[str] = []
    for b in flow.blocks:
        if b.adapter not in ("kafka_kc", "kc"):
            continue
        sink_service_id = (b.config or {}).get("sinkServiceId") or b.serviceId
        svc = services_by_id.get(sink_service_id) if sink_service_id else None
        if not svc:
            continue
        cls = _CONNECT_PLUGIN_CLASSES.get(str((svc.config or {}).get("kind") or ""))
        if cls and cls not in classes:
            classes.append(cls)
    return classes


async def _preflight_rows(
    flow: Flow, services: List[AppService], schemas_all: List[ApprovedSchema], connections: List[PlatformConnection], gateway: GatewaySnapshot,
) -> "tuple[List[Dict[str, Any]], Optional[DeploymentPlan]]":
    """compiler-spec.md §8: mirror the frontend `deployPreflight()` rows,
    plus two rows the ported `validation.deploy_preflight()` (a line-for-
    line port of the same TS function) does not carry: whether the flow
    actually compiles to a plan at all, and — only once it does, since it
    needs the plan's connector configs — whether Kafka Connect has every
    plugin class the flow's connectors need installed."""
    schemas_for_flow = [s for s in schemas_all if s.flowId == flow.id]
    active_connections = [{"type": c.type, "name": c.name, "health": c.health} for c in connections if c.active]
    checks = deploy_preflight(flow, services, schemas_for_flow, active_connections, gateway)
    rows: List[Dict[str, Any]] = [{"label": c.label, "ok": c.ok, "detail": c.detail} for c in checks]

    plan: Optional[DeploymentPlan] = None
    if all(r["ok"] for r in rows):
        ctx = _build_compile_context(services, connections, gateway, {s.blockId: s for s in schemas_for_flow})
        try:
            plan = compile_flow(flow, ctx)
        except CompileError as exc:
            rows.append({"label": "Compiles to a deployment plan", "ok": False, "detail": str(exc)})

    if plan is not None and any(b.adapter in ("kafka_kc", "kc") for b in flow.blocks):
        kc_conn_doc = _active_connection(connections, "kafka_connect")
        needed = _needed_connector_classes(flow, services)
        if not kc_conn_doc:
            rows.append({"label": "Kafka Connect plugins installed", "ok": False, "detail": "No active Kafka Connect connection is configured."})
        elif needed:
            plugins = await kafka_connect_client.list_connector_plugins(_kafka_connect_conn_dict(kc_conn_doc))
            if plugins.get("ok"):
                installed = {p.get("class") for p in (plugins.get("data") or []) if isinstance(p, dict)}
                missing = [c for c in needed if c not in installed]
                rows.append(
                    {
                        "label": "Kafka Connect plugins installed",
                        "ok": not missing,
                        "detail": "All required connector plugins are installed." if not missing else f"Missing plugin(s): {', '.join(missing)}.",
                    }
                )
            else:
                rows.append({"label": "Kafka Connect plugins installed", "ok": False, "detail": plugins.get("error") or "Could not query installed Kafka Connect plugins."})

    return rows, plan


# ----------------------------------------------------------------------- deploy


def _needed_service_pins(flow: Flow, services: List[AppService]) -> Dict[str, int]:
    used_ids = set()
    for b in flow.blocks:
        sid = (b.config or {}).get("sinkServiceId") or b.serviceId
        if sid:
            used_ids.add(sid)
    return {s.id: s.revision for s in services if s.id in used_ids}


def _build_runtime_scope_map(plan: DeploymentPlan, applied: "nifi_apply.AppliedResult") -> Dict[str, Any]:
    connectors_by_block: Dict[str, List[str]] = {}
    for c in plan.connectors:
        connectors_by_block.setdefault(c.ownerBlockId, []).append(c.name)
    topics_by_block: Dict[str, List[str]] = {}
    for t in plan.topics:
        if t.ownerBlockId:
            topics_by_block.setdefault(t.ownerBlockId, []).append(t.name)

    scope_map: Dict[str, Any] = {}
    for block_id, entry in plan.scopeMap.items():
        scope_map[block_id] = {
            "adapter": entry.adapter,
            "engine": entry.engine,
            "groupName": entry.groupName,
            "processGroupId": applied.groups.get(block_id),
            "components": applied.components.get(block_id, {}),
            "connectorNames": connectors_by_block.get(block_id, []),
            "topics": topics_by_block.get(block_id, []),
        }
    return scope_map


async def _stamp_provenance(
    nifi_conn_doc: PlatformConnection, kafka_conn_doc: PlatformConnection, connections: List[PlatformConnection],
) -> Dict[str, Any]:
    """Best-effort connection provenance (compiler-spec.md §7). A probe
    failure never blocks deploy — deploy has already succeeded by the time
    this runs; provenance is a diagnostic record, not a gate."""
    provenance: Dict[str, Any] = {"kafka": {"connectionId": kafka_conn_doc.id}}
    try:
        fp = await probe_nifi_fingerprint(_nifi_conn_dict(nifi_conn_doc))
        provenance["nifi"] = {"connectionId": nifi_conn_doc.id, "fingerprint": fp.get("fingerprint")}
    except Exception:
        logger.exception("Best-effort NiFi fingerprint probe failed; continuing without it.")
    kc_conn_doc = _active_connection(connections, "kafka_connect")
    if kc_conn_doc:
        try:
            fp = await probe_kafka_connect_fingerprint(_kafka_connect_conn_dict(kc_conn_doc))
            provenance["kafka_connect"] = {"connectionId": kc_conn_doc.id, "fingerprint": fp.get("fingerprint")}
        except Exception:
            logger.exception("Best-effort Kafka Connect fingerprint probe failed; continuing without it.")
    return provenance


async def deploy(db, flow_doc: Dict[str, Any]) -> Dict[str, Any]:
    """compiler-spec.md §7/§8. Redeploy reuses this same entry point:
    when `flow_doc["nifiProcessGroupId"]` is already set, the previous PG
    is torn down first (`nifi_apply.delete_flow_pg`) before a fresh one is
    built — `topics.ensure_topics()` is create-or-verify, so the flow's DLQ
    and data topics are naturally kept, never recreated."""
    flow = Flow(**flow_doc)
    services = await _load_services(db)
    schemas_all = await _load_schemas(db)
    connections = await _load_connections(db)
    gateway = await _load_gateway(db)

    rows, plan = await _preflight_rows(flow, services, schemas_all, connections, gateway)
    if plan is None or not all(r["ok"] for r in rows):
        await audit(
            db, action="Flow deploy refused (preflight)", target=flow.name, status="Failed",
            details=f"{sum(1 for r in rows if not r['ok'])} failing check(s)", object="Flow",
        )
        raise DeployPreflightFailed(rows)

    nifi_conn_doc = _active_connection(connections, "nifi")
    kafka_conn_doc = _active_connection(connections, "kafka")
    if not nifi_conn_doc or not kafka_conn_doc:
        raise LifecycleError("No active NiFi/Kafka connection is configured.")
    nifi_conn = _nifi_conn_dict(nifi_conn_doc)
    kafka_conn = _kafka_conn_dict(kafka_conn_doc)

    if flow_doc.get("nifiProcessGroupId"):
        await nifi_apply.delete_flow_pg(nifi_conn, flow_doc["nifiProcessGroupId"])

    topic_results = await topics.ensure_topics(kafka_conn, plan.topics)
    failed_topics = [t for t in topic_results if not t["ok"]]
    if failed_topics:
        await audit(db, action="Flow deploy failed", target=flow.name, status="Failed",
                    details=f"Topic creation failed: {', '.join(t['name'] for t in failed_topics)}", object="Flow")
        raise LifecycleError(f"Failed to create topic(s): {', '.join(t['name'] for t in failed_topics)}")

    try:
        applied = await nifi_apply.apply_plan(nifi_conn, plan)
    except nifi_apply.NifiApplyError as exc:
        await audit(db, action="Flow deploy failed", target=flow.name, status="Failed", details=str(exc), object="Flow")
        raise LifecycleError(str(exc)) from exc

    if plan.connectors:
        kc_conn_doc = _active_connection(connections, "kafka_connect")
        if not kc_conn_doc:
            await nifi_apply.delete_flow_pg(nifi_conn, applied.process_group_id)
            await audit(db, action="Flow deploy failed", target=flow.name, status="Failed",
                        details="No active kafka_connect connection is configured.", object="Flow")
            raise LifecycleError("Flow needs Kafka Connect but no active kafka_connect connection is configured.")
        try:
            await connect_apply.create_connectors(_kafka_connect_conn_dict(kc_conn_doc), plan.connectors)
        except connect_apply.ConnectApplyError as exc:
            await nifi_apply.delete_flow_pg(nifi_conn, applied.process_group_id)
            await audit(db, action="Flow deploy failed", target=flow.name, status="Failed", details=str(exc), object="Flow")
            raise LifecycleError(str(exc)) from exc

    runtime_scope_map = _build_runtime_scope_map(plan, applied)
    provenance = await _stamp_provenance(nifi_conn_doc, kafka_conn_doc, connections)
    service_pins = _needed_service_pins(flow, services)

    now = now_iso()
    update = {
        "runtimeScopeMap": runtime_scope_map,
        "nifiProcessGroupId": applied.process_group_id,
        "provenance": provenance,
        "state": "Stopped",
        "deployedAt": now,
        "servicePins": service_pins,
        "updatedAt": now,
    }
    await db[COLLECTIONS.flows].update_one({"id": flow.id}, {"$set": update})
    await audit(
        db, action="Flow deployed", target=flow.name, status="Success",
        details=f"{len(plan.rootGroup.childGroups)} block group(s), {len(plan.connectors)} connector(s).", object="Flow",
    )
    return await db[COLLECTIONS.flows].find_one({"id": flow.id}, {"_id": 0})


async def redeploy(db, flow_doc: Dict[str, Any]) -> Dict[str, Any]:
    return await deploy(db, flow_doc)


# ------------------------------------------------------------------- run verbs


def _all_connector_names(flow_doc: Dict[str, Any]) -> List[str]:
    scope_map = flow_doc.get("runtimeScopeMap") or {}
    names: List[str] = []
    for entry in scope_map.values():
        names.extend((entry or {}).get("connectorNames") or [])
    return names


# The plan `key`s that identify a root block's trigger/ingest processor(s),
# by adapter convention. Only "trigger" (http-root, compiler-spec.md §3.1)
# exists today — jdbc's root trigger lives ON the read processor itself
# (§3.2), and will add its own key here once that adapter compiles for real
# (it is a NotImplementedError stub as of T7.1 — see
# services/adapter/compiler/blocks_jdbc.py).
_TRIGGER_KEYS = ("trigger",)


def _trigger_component_ids(flow: Flow, flow_doc: Dict[str, Any]) -> List[str]:
    root = root_block(flow)
    if root is None:
        return []
    entry = (flow_doc.get("runtimeScopeMap") or {}).get(root.id) or {}
    components = entry.get("components") or {}
    return [components[k] for k in _TRIGGER_KEYS if k in components]


async def start(db, flow_doc: Dict[str, Any]) -> Dict[str, Any]:
    flow = Flow(**flow_doc)
    pg_id = flow_doc.get("nifiProcessGroupId")
    if not pg_id:
        raise LifecycleError("Flow is not deployed.")
    connections = await _load_connections(db)
    nifi_conn_doc = _active_connection(connections, "nifi")
    if not nifi_conn_doc:
        raise LifecycleError("No active NiFi connection is configured.")

    result = await nifi_apply.start_pg(_nifi_conn_dict(nifi_conn_doc), pg_id)
    if not result.get("ok"):
        raise LifecycleError(result.get("error") or "Failed to start the NiFi process group.")

    connector_names = _all_connector_names(flow_doc)
    if connector_names:
        kc_conn_doc = _active_connection(connections, "kafka_connect")
        if kc_conn_doc:
            await connect_apply.start_connectors(_kafka_connect_conn_dict(kc_conn_doc), connector_names)

    now = now_iso()
    await db[COLLECTIONS.flows].update_one({"id": flow.id}, {"$set": {"state": "Running", "lastRunAt": now, "updatedAt": now}})
    await audit(db, action="Flow started", target=flow.name, status="Success", object="Flow")
    return await db[COLLECTIONS.flows].find_one({"id": flow.id}, {"_id": 0})


async def pause(db, flow_doc: Dict[str, Any]) -> Dict[str, Any]:
    flow = Flow(**flow_doc)
    connections = await _load_connections(db)
    nifi_conn_doc = _active_connection(connections, "nifi")
    if not nifi_conn_doc:
        raise LifecycleError("No active NiFi connection is configured.")
    trigger_ids = _trigger_component_ids(flow, flow_doc)
    if not trigger_ids:
        raise LifecycleError("Could not resolve the flow's trigger processor(s) to pause.")
    result = await nifi_apply.pause_trigger(_nifi_conn_dict(nifi_conn_doc), trigger_ids)
    if not result.get("ok"):
        raise LifecycleError(f"Failed to pause trigger processor(s): {result.get('failed')}")
    now = now_iso()
    await db[COLLECTIONS.flows].update_one({"id": flow.id}, {"$set": {"state": "Paused", "updatedAt": now}})
    await audit(db, action="Flow paused", target=flow.name, status="Success", object="Flow")
    return await db[COLLECTIONS.flows].find_one({"id": flow.id}, {"_id": 0})


async def resume(db, flow_doc: Dict[str, Any]) -> Dict[str, Any]:
    flow = Flow(**flow_doc)
    connections = await _load_connections(db)
    nifi_conn_doc = _active_connection(connections, "nifi")
    if not nifi_conn_doc:
        raise LifecycleError("No active NiFi connection is configured.")
    trigger_ids = _trigger_component_ids(flow, flow_doc)
    if not trigger_ids:
        raise LifecycleError("Could not resolve the flow's trigger processor(s) to resume.")
    result = await nifi_apply.resume_trigger(_nifi_conn_dict(nifi_conn_doc), trigger_ids)
    if not result.get("ok"):
        raise LifecycleError(f"Failed to resume trigger processor(s): {result.get('failed')}")
    now = now_iso()
    await db[COLLECTIONS.flows].update_one({"id": flow.id}, {"$set": {"state": "Running", "updatedAt": now}})
    await audit(db, action="Flow resumed", target=flow.name, status="Success", object="Flow")
    return await db[COLLECTIONS.flows].find_one({"id": flow.id}, {"_id": 0})


async def stop(db, flow_doc: Dict[str, Any]) -> Dict[str, Any]:
    flow = Flow(**flow_doc)
    pg_id = flow_doc.get("nifiProcessGroupId")
    if not pg_id:
        raise LifecycleError("Flow is not deployed.")
    connections = await _load_connections(db)
    nifi_conn_doc = _active_connection(connections, "nifi")
    if not nifi_conn_doc:
        raise LifecycleError("No active NiFi connection is configured.")

    result = await nifi_apply.stop_pg(_nifi_conn_dict(nifi_conn_doc), pg_id)
    if not result.get("ok"):
        raise LifecycleError(result.get("error") or "Failed to stop the NiFi process group.")

    connector_names = _all_connector_names(flow_doc)
    if connector_names:
        kc_conn_doc = _active_connection(connections, "kafka_connect")
        if kc_conn_doc:
            await connect_apply.stop_connectors(_kafka_connect_conn_dict(kc_conn_doc), connector_names)

    now = now_iso()
    await db[COLLECTIONS.flows].update_one({"id": flow.id}, {"$set": {"state": "Stopped", "updatedAt": now}})
    await audit(db, action="Flow stopped", target=flow.name, status="Success", object="Flow")
    return await db[COLLECTIONS.flows].find_one({"id": flow.id}, {"_id": 0})


async def stop_clear(db, flow_doc: Dict[str, Any]) -> Dict[str, Any]:
    flow = Flow(**flow_doc)
    pg_id = flow_doc.get("nifiProcessGroupId")
    if not pg_id:
        raise LifecycleError("Flow is not deployed.")
    connections = await _load_connections(db)
    nifi_conn_doc = _active_connection(connections, "nifi")
    if not nifi_conn_doc:
        raise LifecycleError("No active NiFi connection is configured.")
    nifi_conn = _nifi_conn_dict(nifi_conn_doc)

    result = await nifi_apply.stop_pg(nifi_conn, pg_id)
    if not result.get("ok"):
        raise LifecycleError(result.get("error") or "Failed to stop the NiFi process group.")

    connector_names = _all_connector_names(flow_doc)
    if connector_names:
        kc_conn_doc = _active_connection(connections, "kafka_connect")
        if kc_conn_doc:
            await connect_apply.stop_connectors(_kafka_connect_conn_dict(kc_conn_doc), connector_names)

    drop_result = await nifi_apply.drop_all_queues(nifi_conn, pg_id)

    now = now_iso()
    await db[COLLECTIONS.flows].update_one({"id": flow.id}, {"$set": {"state": "Stopped", "updatedAt": now}})
    await audit(
        db, action="Flow stopped and cleared", target=flow.name, status="Success",
        details=f"Dropped {drop_result.get('dropped', 0)} queued record(s) across {drop_result.get('connections', 0)} connection(s).",
        object="Flow",
    )
    return await db[COLLECTIONS.flows].find_one({"id": flow.id}, {"_id": 0})


def _dlq_topic_name(flow_doc: Dict[str, Any]) -> str:
    return dlq_name(flow_doc.get("name") or "")


def _owned_data_topic_names(flow_doc: Dict[str, Any]) -> List[str]:
    """Every topic this flow's `runtimeScopeMap` says it owns, excluding
    the flow's own DLQ topic (kept across undeploy — compiler-spec.md §7:
    "undeploy = ... empty owned data topics + keep DLQ")."""
    dlq = _dlq_topic_name(flow_doc)
    names: List[str] = []
    for entry in (flow_doc.get("runtimeScopeMap") or {}).values():
        for name in (entry or {}).get("topics") or []:
            if name != dlq and name not in names:
                names.append(name)
    return names


async def undeploy(db, flow_doc: Dict[str, Any]) -> Dict[str, Any]:
    flow = Flow(**flow_doc)
    connections = await _load_connections(db)

    pg_id = flow_doc.get("nifiProcessGroupId")
    nifi_conn_doc = _active_connection(connections, "nifi")
    if pg_id and nifi_conn_doc:
        await nifi_apply.delete_flow_pg(_nifi_conn_dict(nifi_conn_doc), pg_id)

    connector_names = _all_connector_names(flow_doc)
    kc_conn_doc = _active_connection(connections, "kafka_connect")
    if connector_names and kc_conn_doc:
        await connect_apply.delete_connectors(_kafka_connect_conn_dict(kc_conn_doc), connector_names)

    owned_topics = _owned_data_topic_names(flow_doc)
    kafka_conn_doc = _active_connection(connections, "kafka")
    if owned_topics and kafka_conn_doc:
        kafka_conn = _kafka_conn_dict(kafka_conn_doc)
        for name in owned_topics:
            await topics.empty_topic(kafka_conn, name)

    now = now_iso()
    await db[COLLECTIONS.flows].update_one(
        {"id": flow.id},
        {"$set": {"state": "Draft", "deployedAt": None, "runtimeScopeMap": None, "nifiProcessGroupId": None, "updatedAt": now}},
    )
    await audit(db, action="Flow undeployed", target=flow.name, status="Success", object="Flow")
    return await db[COLLECTIONS.flows].find_one({"id": flow.id}, {"_id": 0})


async def delete(db, flow_doc: Dict[str, Any]) -> Dict[str, Any]:
    """Used by `DELETE /api/v2/flows/{id}`: full teardown when deployed
    (undeploy, then delete the DLQ + every topic undeploy only emptied),
    then the flow/runtime docs themselves."""
    flow = Flow(**flow_doc)
    owned_topics = _owned_data_topic_names(flow_doc)
    dlq_topic = _dlq_topic_name(flow_doc)

    if flow_doc.get("deployedAt") or flow_doc.get("nifiProcessGroupId"):
        await undeploy(db, flow_doc)

    connections = await _load_connections(db)
    kafka_conn_doc = _active_connection(connections, "kafka")
    if kafka_conn_doc:
        kafka_conn = _kafka_conn_dict(kafka_conn_doc)
        await topics.delete_topic(kafka_conn, dlq_topic)
        for name in owned_topics:
            await topics.delete_topic(kafka_conn, name)

    await db[COLLECTIONS.flows].delete_one({"id": flow.id})
    await db[COLLECTIONS.runtimes].delete_one({"flowId": flow.id})
    await audit(db, action="Flow deleted", target=flow.name, status="Warning", object="Flow")
    return {"ok": True, "id": flow.id}


# ------------------------------------------------------------- dedup cache clear


async def clear_dedup_cache(db, flow_doc: Dict[str, Any], block_id: str) -> Dict[str, Any]:
    """See the module docstring's "Dedup cache clearing" section for why
    this bumps an epoch rather than issuing a real Redis flush."""
    flow = Flow(**flow_doc)
    block_idx: Optional[int] = None
    block = None
    for i, b in enumerate(flow.blocks):
        if b.id == block_id:
            block_idx, block = i, b
            break
    if block is None or block_idx is None:
        raise LifecycleError(f"Block {block_id!r} not found on flow {flow.id!r}.")

    dedup_idx = next((i for i, t in enumerate(block.transforms) if t.kind == "dedup"), None)
    if dedup_idx is None:
        raise LifecycleError(f'Block "{block.name}" has no dedup transform to clear.')

    current_epoch_raw = (block.transforms[dedup_idx].config or {}).get("dedupEpoch")
    try:
        current_epoch = int(current_epoch_raw) if current_epoch_raw is not None else 0
    except (TypeError, ValueError):
        current_epoch = 0
    new_epoch = current_epoch + 1

    await db[COLLECTIONS.flows].update_one(
        {"id": flow.id},
        {"$set": {f"blocks.{block_idx}.transforms.{dedup_idx}.config.dedupEpoch": new_epoch, "updatedAt": now_iso()}},
    )

    message = (
        f'Dedup cache epoch bumped for "{block.name}" to {new_epoch} — redeploy the flow for the running '
        "NiFi processor to pick up the new cache namespace; until then it keeps matching against the epoch "
        "it was deployed with."
    )
    await audit(db, action="Dedup cache cleared", target=f"{flow.name} / {block.name}", status="Success", details=message, object="Flow")
    return {"blockId": block_id, "dedupEpoch": new_epoch, "redeployRequired": True, "message": message}
