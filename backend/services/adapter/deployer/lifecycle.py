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

import hashlib
import json
import logging
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from models.adapter import AppService, ApprovedSchema, Flow, FlowBlock, GatewayProxy, PlatformConnection
from services import kafka_connect_client
from services.adapter.common import COLLECTIONS, audit, now_iso
from services.adapter.compiler import CompileContext, CompileError, DeploymentPlan, compile_flow
from services.adapter.legality import root_block
from services.adapter.naming import dlq_name
from services.adapter.validation import GatewaySnapshot, deploy_preflight
from services.connection_fingerprint import probe_kafka_connect_fingerprint, probe_nifi_fingerprint

from . import bookmark_store, connect_apply, nifi_apply, topics

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


async def _topic_reservation_rows(
    db, flow: Flow, flow_doc: Dict[str, Any], plan: DeploymentPlan, connections: List[PlatformConnection],
) -> List[Dict[str, Any]]:
    """M13: compiler-spec.md §8/§7.12 row 3 — the flow's derived topics (the
    plan's own `topics`, data + DLQ) must not collide with (a) another
    flow's owned topics or (b) a live Kafka topic that exists but is not
    owned by THIS flow (best-effort via the Kafbat listing — skipped
    silently, never a deploy blocker, when the listing is unavailable). One
    row per derived name; a failing row names the exact colliding topic,
    per the review's failure scenario (two flows named identically both
    deriving `raw.asset_sync.asset`).

    Ownership (both "this flow" and "every other flow") is the UNION of a
    real deployed ownership record (`runtimeScopeMap.*.topics`, kept for
    robustness against post-deploy block edits / custom-named legacy
    topics a rename could no longer re-derive) and the DETERMINISTIC NAMING
    WALK (`runtime._owned_topic_names`, the same derivation
    `deployer._delete_candidate_data_topics` already reuses this same way)
    over the flow's OWN CURRENT block definitions — never the scope map
    alone. Redeploy-after-undeploy bug: `undeploy()` intentionally KEEPS a
    flow's owned data topics (emptied, not deleted, per MVP §7.9) while
    NULLING `runtimeScopeMap` (compiler-spec.md §7) -- a scope-map-only
    ownership check therefore reads a just-undeployed flow's own leftover
    topics as foreign on the very next redeploy and blocks it forever. A
    topic this flow itself derives (data topics + its DLQ) must never
    collide with itself, deployed or not -- and the same reasoning applies
    symmetrically to every OTHER flow's topics, so an other flow's own
    ownership is likewise re-derived here rather than trusted from its
    scope map alone (an undeployed other flow would otherwise vanish from
    this check too)."""
    derived_names = sorted({t.name for t in plan.topics})
    if not derived_names:
        return []

    # Function-level import: runtime.py imports this module's connection-dict
    # helpers at module scope, so a module-level import here would be circular
    # (same reason `_delete_candidate_data_topics` below imports it locally).
    from services.adapter.runtime import _owned_topic_names

    other_owned: Dict[str, str] = {}
    other_flow_docs = await db[COLLECTIONS.flows].find(
        {"id": {"$ne": flow.id}}, {"_id": 0, "id": 1, "name": 1, "runtimeScopeMap": 1, "blocks": 1, "topics": 1}
    ).to_list(None)
    for doc in other_flow_docs:
        owner_name = doc.get("name") or doc.get("id") or "another flow"
        owned_names: set = set()
        for entry in (doc.get("runtimeScopeMap") or {}).values():
            owned_names.update((entry or {}).get("topics") or [])
        owned_names.update(_owned_topic_names(doc))
        for name in owned_names:
            other_owned.setdefault(name, owner_name)

    live_topics: Optional[set] = None
    kafka_conn_doc = _active_connection(connections, "kafka")
    if kafka_conn_doc:
        listing = await topics.list_topics(_kafka_conn_dict(kafka_conn_doc))
        if listing.get("ok"):
            live_topics = set(listing.get("topics") or [])

    # This flow's own topics -- the UNION of its LAST deploy's recorded
    # ownership (name-frozen by M12, so a redeploy always re-derives the
    # identical names) and the deterministic naming walk over its current
    # blocks, so a topic this flow derives is excluded here whether or not
    # `runtimeScopeMap` currently reflects it (undeployed or never
    # deployed) -- a live topic that is already this flow's own is never a
    # collision.
    own_prior_topics = set(_owned_data_topic_names(flow_doc)) | {_dlq_topic_name(flow_doc)} | _owned_topic_names(flow_doc)

    rows: List[Dict[str, Any]] = []
    for name in derived_names:
        label = f'Topic reservation — "{name}"'
        if name in other_owned:
            rows.append({"label": label, "ok": False, "detail": f'"{name}" is already owned by flow "{other_owned[name]}".'})
            continue
        if live_topics is not None and name in live_topics and name not in own_prior_topics:
            rows.append({"label": label, "ok": False, "detail": f'"{name}" already exists on the Kafka cluster and is not owned by this flow.'})
            continue
        rows.append({"label": label, "ok": True, "detail": "No collision."})
    return rows


async def _preflight_rows(
    db, flow: Flow, flow_doc: Dict[str, Any], services: List[AppService], schemas_all: List[ApprovedSchema],
    connections: List[PlatformConnection], gateway: GatewaySnapshot,
) -> "tuple[List[Dict[str, Any]], Optional[DeploymentPlan]]":
    """compiler-spec.md §8: mirror the frontend `deployPreflight()` rows,
    plus rows the ported `validation.deploy_preflight()` (a line-for-line
    port of the same TS function) does not carry: whether the flow actually
    compiles to a plan at all; once it does, whether Kafka Connect has
    every plugin class the flow's connectors need installed (needs the
    plan's connector configs); and — needs the plan's derived topic names —
    topic-reservation collisions against other flows / the live cluster
    (M13)."""
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

    if plan is not None:
        rows.extend(await _topic_reservation_rows(db, flow, flow_doc, plan, connections))

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


# ------------------------------------------------------- M11: dedup config-change detection


def _dedup_rule_signature(block: FlowBlock) -> Optional[Dict[str, Any]]:
    """The part of a block's dedup transform that matters for cache
    validity — `identityFields`/`excludedFields`/`windowHours` (and the
    presence of the rule at all, i.e. dedup "on/off"). `dedupEpoch` itself
    is deliberately excluded: it is the OUTPUT of this comparison (bumped
    when the signature changes), not an input to it, or every deploy would
    see its own previous bump as "yet another change"."""
    rule = next((t for t in block.transforms if t.kind == "dedup"), None)
    if rule is None:
        return None
    identity_fields = sorted(f for f in (rule.config.get("identityFields") or []) if isinstance(f, str))
    excluded_fields = sorted(f for f in (rule.config.get("excludedFields") or []) if isinstance(f, str))
    window_hours = rule.config.get("windowHours", 24)
    return {"identityFields": identity_fields, "excludedFields": excluded_fields, "windowHours": window_hours}


def _dedup_config_hash(block: FlowBlock) -> Optional[str]:
    sig = _dedup_rule_signature(block)
    if sig is None:
        return None
    return hashlib.sha256(json.dumps(sig, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _current_dedup_hashes(flow: Flow) -> Dict[str, str]:
    """`{blockId: sha256(dedup config)}` for every block that currently
    carries a dedup transform — persisted into `flow.provenance.
    dedupConfigHashes` at the end of a successful `deploy()`, compared
    against on the NEXT deploy to detect a config change (M11)."""
    out: Dict[str, str] = {}
    for b in flow.blocks:
        h = _dedup_config_hash(b)
        if h is not None:
            out[b.id] = h
    return out


def _bump_changed_dedup_epochs(flow_doc: Dict[str, Any], prev_hashes: Dict[str, str], current_hashes: Dict[str, str]) -> List[str]:
    """M11: for every block whose dedup config hash differs from what was
    stored at the LAST deploy (a block newly gaining or losing its dedup
    rule is not a "change" here — there is no previous cache namespace to
    invalidate either way), bump that rule's `dedupEpoch` directly on
    `flow_doc` IN PLACE — same epoch mechanism `clear_dedup_cache()` /
    `undeploy()` (M10) use — so the bump is baked into the plan this same
    deploy() call compiles and applies, not deferred to a following one.
    Returns the changed block ids for the preflight warning row."""
    changed_block_ids = [bid for bid, h in current_hashes.items() if bid in prev_hashes and prev_hashes[bid] != h]
    if not changed_block_ids:
        return []
    changed = set(changed_block_ids)
    for b in flow_doc.get("blocks") or []:
        if b.get("id") not in changed:
            continue
        for t in b.get("transforms") or []:
            if t.get("kind") != "dedup":
                continue
            cfg = t.setdefault("config", {})
            try:
                current_epoch = int(cfg.get("dedupEpoch") or 0)
            except (TypeError, ValueError):
                current_epoch = 0
            cfg["dedupEpoch"] = current_epoch + 1
    return changed_block_ids


_DEDUP_CHANGE_WARNING = "Dedup settings changed — the cache resets and previously suppressed records may reappear."


@dataclass
class StagedNifiDeployment:
    """A target-NiFi deployment that has not changed MongoDB yet."""

    flow_id: str
    flow_name: str
    old_process_group_id: Optional[str]
    parameter_context_id: Optional[str]
    parameter_context_created: bool
    staged_group_name: str
    update: Dict[str, Any]


async def deploy(db, flow_doc: Dict[str, Any]) -> Dict[str, Any]:
    """compiler-spec.md §7/§8. Redeploy reuses this same entry point:
    when `flow_doc["nifiProcessGroupId"]` is already set, the previous PG
    is torn down first (`nifi_apply.delete_flow_pg`) before a fresh one is
    built — `topics.ensure_topics()` is create-or-verify, so the flow's DLQ
    and data topics are naturally kept, never recreated.

    M11: before compiling, this flow's per-block dedup config is compared
    against the hashes stamped into `provenance.dedupConfigHashes` at the
    LAST successful deploy. A changed block gets its `dedupEpoch` bumped
    in `flow_doc` right here — in place, before `_preflight_rows` compiles
    — so the bump is baked into the very plan this call applies (not
    deferred to a following deploy, unlike a manual `clear_dedup_cache()`
    call, which has no plan to bake it into). A non-blocking warning row is
    appended to the preflight response either way."""
    return await _deploy_impl(db, flow_doc)


async def stage_nifi_migration(
    db,
    flow_doc: Dict[str, Any],
    target_connection: Dict[str, Any],
    *,
    staged_group_name: str,
) -> StagedNifiDeployment:
    """Build one flow on a different NiFi without cutting over MongoDB.

    Kafka topics and Kafka Connect connectors already belong to the flow and
    are deliberately left untouched. The old NiFi process group is retained
    as a rollback copy until the coordinator verifies the complete target.
    """
    staged = await _deploy_impl(
        db,
        deepcopy(flow_doc),
        target_nifi=PlatformConnection.model_validate(target_connection),
        preserve_existing_nifi=True,
        provision_external_resources=False,
        persist=False,
        nifi_group_name_override=staged_group_name,
    )
    parameter_context_id = staged.pop("_migrationParameterContextId", None)
    parameter_context_created = bool(staged.pop("_migrationParameterContextCreated", False))
    actual_group_name = str(staged.pop("_migrationRootGroupName", staged_group_name))
    return StagedNifiDeployment(
        flow_id=str(flow_doc.get("id") or ""),
        flow_name=str(flow_doc.get("name") or ""),
        old_process_group_id=flow_doc.get("nifiProcessGroupId"),
        parameter_context_id=parameter_context_id,
        parameter_context_created=parameter_context_created,
        staged_group_name=actual_group_name,
        update=staged,
    )


async def _deploy_impl(
    db,
    flow_doc: Dict[str, Any],
    *,
    target_nifi: Optional[PlatformConnection] = None,
    preserve_existing_nifi: bool = False,
    provision_external_resources: bool = True,
    persist: bool = True,
    nifi_group_name_override: Optional[str] = None,
) -> Dict[str, Any]:
    flow = Flow(**flow_doc)
    prev_dedup_hashes = ((flow_doc.get("provenance") or {}).get("dedupConfigHashes")) or {}
    current_dedup_hashes = _current_dedup_hashes(flow)
    changed_dedup_blocks = _bump_changed_dedup_epochs(flow_doc, prev_dedup_hashes, current_dedup_hashes)
    if changed_dedup_blocks:
        flow = Flow(**flow_doc)  # re-read so the compiled plan sees the bumped epoch(s)

    services = await _load_services(db)
    schemas_all = await _load_schemas(db)
    connections = await _load_connections(db)
    if target_nifi is not None:
        # Preflight/compiler resolve the active connection by type. Replace
        # only the in-memory NiFi selection; Mongo stays on the source until
        # every target process group has been staged and validated.
        for connection in connections:
            if connection.type == "nifi":
                connection.active = connection.id == target_nifi.id
        if not any(c.type == "nifi" and c.id == target_nifi.id for c in connections):
            target_nifi.active = True
            connections.append(target_nifi)
    gateway = await _load_gateway(db)

    rows, plan = await _preflight_rows(db, flow, flow_doc, services, schemas_all, connections, gateway)
    if changed_dedup_blocks:
        changed_names = sorted({b.name or b.id for b in flow.blocks if b.id in changed_dedup_blocks})
        rows.append({"label": "Dedup cache impact", "ok": True, "detail": f"{_DEDUP_CHANGE_WARNING} (block(s): {', '.join(changed_names)}.)"})
    if plan is None or not all(r["ok"] for r in rows):
        await audit(
            db, action="Flow deploy refused (preflight)", target=flow.name, status="Failed",
            details=f"{sum(1 for r in rows if not r['ok'])} failing check(s)", object="Flow",
        )
        raise DeployPreflightFailed(rows)

    if nifi_group_name_override:
        # Migration must never delete a retained rollback copy merely
        # because it has the canonical flow name. Stage under a unique name;
        # the coordinator finalizes the name only after full validation.
        plan.rootGroup.name = nifi_group_name_override

    nifi_conn_doc = _active_connection(connections, "nifi")
    kafka_conn_doc = _active_connection(connections, "kafka")
    if not nifi_conn_doc or not kafka_conn_doc:
        raise LifecycleError("No active NiFi/Kafka connection is configured.")
    nifi_conn = _nifi_conn_dict(nifi_conn_doc)
    kafka_conn = _kafka_conn_dict(kafka_conn_doc)

    if flow_doc.get("nifiProcessGroupId") and not preserve_existing_nifi:
        await nifi_apply.delete_flow_pg(nifi_conn, flow_doc["nifiProcessGroupId"])

    if provision_external_resources:
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

    if plan.connectors and provision_external_resources:
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
    provenance["dedupConfigHashes"] = current_dedup_hashes
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
    if changed_dedup_blocks:
        # Persist the bumped dedupEpoch(s) baked into the just-applied plan
        # (flow_doc["blocks"] was mutated in place by
        # `_bump_changed_dedup_epochs` above) — otherwise the next deploy's
        # comparison would see a stored epoch that never matches what NiFi
        # actually has compiled in.
        update["blocks"] = flow_doc.get("blocks")
    if not persist:
        update["_migrationParameterContextId"] = applied.parameter_context_id
        update["_migrationParameterContextCreated"] = applied.parameter_context_created
        update["_migrationRootGroupName"] = plan.rootGroup.name
        return update
    await db[COLLECTIONS.flows].update_one({"id": flow.id}, {"$set": update})
    await audit(
        db, action="Flow deployed", target=flow.name, status="Success",
        details=f"{len(plan.rootGroup.childGroups)} block group(s), {len(plan.connectors)} connector(s).", object="Flow",
    )
    result_doc = await db[COLLECTIONS.flows].find_one({"id": flow.id}, {"_id": 0})
    if changed_dedup_blocks:
        # M11: the non-blocking dedup-change warning row surfaces on the
        # successful deploy response too (not persisted -- ephemeral,
        # exactly like the preflight rows returned on a FAILED deploy via
        # `DeployPreflightFailed.rows`) since deploy() itself, not a
        # separate always-called preflight endpoint, is what actually ran
        # the comparison and the epoch bump.
        result_doc["preflightWarnings"] = [r for r in rows if r["label"] == "Dedup cache impact"]
    return result_doc


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
# by adapter convention: "trigger" (http root — GenerateFlowFile,
# compiler-spec.md §3.1), "query" (jdbc root — QueryDatabaseTableRecord
# carries its own cron/timer scheduling directly, compiler-spec.md §3.2 /
# blocks_jdbc.py's `_compile_read`'s `key="query"`), "consume" (kafka-read
# root — ConsumeKafka, blocks_kafka.py's `key="consume"`). M9 fix: pause/
# resume used to only ever match "trigger", so `_trigger_component_ids`
# came back empty for every jdbc- or kafka-read-rooted flow and pause/resume
# 502'd outright ("Could not resolve the flow's trigger processor(s)") for
# two of the platform's three root adapters.
_TRIGGER_KEYS = ("trigger", "query", "consume")


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


def _delete_candidate_data_topics(flow_doc: Dict[str, Any]) -> List[str]:
    """Every data topic `delete()` must remove, DLQ excluded (the caller
    lists the DLQ separately, always first).

    Journey-R teardown gap (journey-r-reverify.md, cleanup item 2 /
    DEFECT-6 family): `undeploy()` nulls `runtimeScopeMap`, so a flow
    deleted from post-undeploy Draft state used to find NO owned data
    topics via `_owned_data_topic_names` and leave `raw.<flow>.<entity>`
    alive on the cluster (proven live: `raw.e2er_fresh.e2er_product`
    survived while the DLQ — derived independently — was deleted). The
    candidates are therefore the UNION of the scope map's recorded
    ownership (real deploy-time record, kept for robustness against
    post-deploy block edits) and the deterministic naming walk over the
    flow's own blocks (`runtime._owned_topic_names`, the same derivation
    the compiler/messages endpoints use) — the walk needs no deploy state
    at all, closing the gap."""
    # Function-level import: runtime.py imports this module's connection-dict
    # helpers at module scope, so a module-level import here would be circular.
    from services.adapter.runtime import _owned_topic_names

    dlq = _dlq_topic_name(flow_doc)
    names = set(_owned_data_topic_names(flow_doc)) | set(_owned_topic_names(flow_doc))
    return sorted(n for n in names if n != dlq)


def _dedup_epoch_bump_updates(flow: Flow) -> Dict[str, Any]:
    """M10: undeploy must clear dedup caches per MVP §7.9 — the same epoch-
    bump mechanism `clear_dedup_cache()` uses (Redis is not reachable from
    this app host to issue a real flush; see the module docstring's "Dedup
    cache clearing" section), applied to EVERY dedup-bearing block at once
    rather than one block at a time. Returns a `$set`-ready
    `{"blocks.<i>.transforms.<j>.config.dedupEpoch": <new epoch>}` dict."""
    updates: Dict[str, Any] = {}
    for b_idx, block in enumerate(flow.blocks):
        for t_idx, rule in enumerate(block.transforms):
            if rule.kind != "dedup":
                continue
            current_epoch_raw = (rule.config or {}).get("dedupEpoch")
            try:
                current_epoch = int(current_epoch_raw) if current_epoch_raw is not None else 0
            except (TypeError, ValueError):
                current_epoch = 0
            updates[f"blocks.{b_idx}.transforms.{t_idx}.config.dedupEpoch"] = current_epoch + 1
    return updates


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

    # An undeploy is a destructive reset of the flow's runtime identity.  A
    # JDBC bookmark belongs to that runtime identity, so it must not survive
    # an undeploy.  Redeploy never calls this function and therefore keeps its
    # bookmark.  Keep the result on the response (rather than persisting it)
    # so the UI/API caller can see when Redis cleanup could not be completed.
    incremental_block_ids = [
        block.id
        for block in flow.blocks
        if block.adapter == "jdbc" and block.mode == "read" and (block.config or {}).get("incremental") is True
    ]
    bookmark_cleanup: Optional[Dict[str, Any]] = None
    if incremental_block_ids:
        redis_conn_doc = _active_connection(connections, "redis")
        if not redis_conn_doc:
            bookmark_cleanup = {
                "ok": False,
                "deleted": 0,
                "error": "No active Redis connection is configured; incremental bookmark keys were not removed.",
            }
        else:
            bookmark_cleanup = await bookmark_store.delete_flow_bookmarks(
                redis_conn_doc.config or {}, flow.id, incremental_block_ids
            )

    now = now_iso()
    update = {"state": "Draft", "deployedAt": None, "runtimeScopeMap": None, "nifiProcessGroupId": None, "updatedAt": now}
    update.update(_dedup_epoch_bump_updates(flow))
    await db[COLLECTIONS.flows].update_one({"id": flow.id}, {"$set": update})
    result_doc = await db[COLLECTIONS.flows].find_one({"id": flow.id}, {"_id": 0})
    if bookmark_cleanup is not None:
        result_doc["bookmarkCleanup"] = bookmark_cleanup
    await audit(
        db,
        action="Flow undeployed",
        target=flow.name,
        status="Warning" if bookmark_cleanup and not bookmark_cleanup.get("ok") else "Success",
        details=(
            f"Incremental bookmark cleanup failed: {bookmark_cleanup.get('error')}"
            if bookmark_cleanup and not bookmark_cleanup.get("ok")
            else None
        ),
        object="Flow",
    )
    return result_doc


async def delete(
    db,
    flow_doc: Dict[str, Any],
    *,
    delete_options: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Used by `DELETE /api/v2/flows/{id}`: full teardown when deployed
    (undeploy, then delete the DLQ + every topic undeploy only emptied),
    then the flow/runtime docs themselves. `delete_options` can retain the
    generated Kafka topics (`deleteTopics=False`) and request invalidation of
    all dedup namespaces (`clearCache=True`); omitted options preserve the
    historical delete behaviour.

    E7 fix: `undeploy()` above is the ONLY place that deletes this flow's
    Kafka Connect connectors, and it only runs when `deployedAt` /
    `nifiProcessGroupId` are still set. After `runtime.repair_runtime`
    force-clears both (returning the flow to Draft) while deliberately
    KEEPING `runtimeScopeMap` — including its `connectorNames` — a delete
    right after a repair used to skip `undeploy()` entirely and leave the
    flow's connectors permanently orphaned (proven live,
    docs/orchestration/e2e/journey-a-e.md DEFECT 6). Connectors (and, for
    symmetry/robustness, the topic deletes below) are now always
    best-effort attempted regardless of whether `undeploy()` ran, and any
    individual failure is recorded as an orphan entry rather than silently
    dropped (`lifecycle.py` MINOR 13 in the review: `delete_topic` failures
    used to be swallowed outright).

    Journey-R teardown gap: owned data topics are derived via
    `_delete_candidate_data_topics` (scope map UNION naming walk) rather
    than the scope map alone, so a delete AFTER undeploy (scope map already
    nulled) still removes the flow's `raw.*` topics."""
    flow = Flow(**flow_doc)
    options = delete_options or {}
    delete_topics = bool(options.get("deleteTopics", options.get("delete_topics", True)))
    clear_cache = bool(options.get("clearCache", options.get("clear_cache", False)))
    owned_topics = _delete_candidate_data_topics(flow_doc)
    dlq_topic = _dlq_topic_name(flow_doc)
    connector_names = _all_connector_names(flow_doc)
    orphans: List[Dict[str, Any]] = []
    retained_topics: List[str] = []
    cache_block_count = sum(
        1
        for block in flow.blocks
        for transform in block.transforms
        if transform.kind == "dedup"
    )

    was_deployed = bool(flow_doc.get("deployedAt") or flow_doc.get("nifiProcessGroupId"))
    undeploy_result: Optional[Dict[str, Any]] = None
    if was_deployed:
        undeploy_result = await undeploy(db, flow_doc)
        bookmark_cleanup = (undeploy_result or {}).get("bookmarkCleanup")
        if bookmark_cleanup and not bookmark_cleanup.get("ok"):
            orphans.append({
                "kind": "bookmark",
                "ref": flow.id,
                "reason": bookmark_cleanup.get("error") or "Incremental bookmark cleanup failed.",
            })

    # A deployed flow has already bumped every dedup epoch as part of
    # undeploy.  For a draft/stopped flow, honour the explicit cache choice
    # with the same namespace invalidation used by the flow's Clear cache
    # action before removing the document.  Redis is cluster-internal here,
    # so this is an invalidation marker rather than a direct FLUSHDB call.
    if clear_cache and cache_block_count and not was_deployed:
        cache_updates = _dedup_epoch_bump_updates(flow)
        if cache_updates:
            await db[COLLECTIONS.flows].update_one(
                {"id": flow.id},
                {"$set": {**cache_updates, "updatedAt": now_iso()}},
            )
    if clear_cache:
        await audit(
            db,
            action="Dedup caches cleared",
            target=flow.name,
            status="Success",
            details=(
                f"Invalidated {cache_block_count} dedup cache namespace(s) using the flow epoch reset."
                if cache_block_count
                else "No dedup cache namespaces were configured for this flow."
            ),
            object="Flow",
        )

    connections = await _load_connections(db)

    incremental_block_ids = [
        block.id
        for block in flow.blocks
        if block.adapter == "jdbc" and block.mode == "read" and (block.config or {}).get("incremental") is True
    ]
    # A deployed flow's undeploy path already removes its bookmarks.  Keep
    # this fallback for a flow that is already Draft (for example after a
    # repair or a previous undeploy) so permanent deletion remains complete.
    if incremental_block_ids and not was_deployed:
        redis_conn_doc = _active_connection(connections, "redis")
        if not redis_conn_doc:
            orphans.append({
                "kind": "bookmark",
                "ref": flow.id,
                "reason": "No active Redis connection is configured; incremental bookmark keys were not removed.",
            })
        else:
            bookmark_result = await bookmark_store.delete_flow_bookmarks(
                redis_conn_doc.config or {}, flow.id, incremental_block_ids
            )
            if not bookmark_result.get("ok"):
                orphans.append({
                    "kind": "bookmark",
                    "ref": flow.id,
                    "reason": bookmark_result.get("error") or "Incremental bookmark cleanup failed.",
                })

    if connector_names and not was_deployed:
        # `undeploy()` above already best-effort deletes connectors when it
        # runs — only reached when it did NOT (the post-repair gap E7 fixes).
        kc_conn_doc = _active_connection(connections, "kafka_connect")
        if kc_conn_doc:
            results = await connect_apply.delete_connectors(_kafka_connect_conn_dict(kc_conn_doc), connector_names)
            for r in results:
                if not r.get("ok"):
                    orphans.append({"kind": "connector", "ref": r.get("name"), "reason": r.get("error") or "Connector delete failed."})
        else:
            for name in connector_names:
                orphans.append({"kind": "connector", "ref": name, "reason": "No active kafka_connect connection is configured."})

    all_topics = [dlq_topic] + owned_topics
    if delete_topics:
        kafka_conn_doc = _active_connection(connections, "kafka")
        if kafka_conn_doc:
            kafka_conn = _kafka_conn_dict(kafka_conn_doc)
            for name in all_topics:
                result = await topics.delete_topic(kafka_conn, name)
                if not result.get("ok"):
                    orphans.append({"kind": "topic", "ref": name, "reason": result.get("error") or "Topic delete failed."})
        else:
            for name in all_topics:
                orphans.append({"kind": "topic", "ref": name, "reason": "No active kafka connection is configured."})
    else:
        retained_topics = all_topics

    await db[COLLECTIONS.flows].delete_one({"id": flow.id})
    await db[COLLECTIONS.runtimes].delete_one({"flowId": flow.id})
    details_parts: List[str] = []
    if orphans:
        details_parts.append(
            f"{len(orphans)} object(s) could not be cleaned up: "
            + "; ".join(f"{o['kind']} {o['ref']}" for o in orphans)
        )
    if retained_topics:
        details_parts.append("Retained topics: " + ", ".join(retained_topics))
    await audit(
        db,
        action="Flow deleted",
        target=flow.name,
        status="Warning" if orphans else "Success",
        details=" ".join(details_parts) or None,
        object="Flow",
    )
    return {
        "ok": True,
        "id": flow.id,
        "orphans": orphans,
        "retainedTopics": retained_topics,
        "cacheCleared": bool(clear_cache and cache_block_count),
        "cacheBlockCount": cache_block_count if clear_cache else 0,
    }


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
