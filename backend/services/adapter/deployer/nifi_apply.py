"""DeploymentPlan.rootGroup -> live NiFi process groups/components (T7.2).

`apply_plan(nifi_conn, plan)` is the only entry point that builds anything;
everything else here (`delete_flow_pg`, `start_pg`, `stop_pg`,
`pause_trigger`/`resume_trigger`, `drop_all_queues`) operates on ids the
caller already has (from a prior `apply_plan()` call, persisted into
`flow.runtimeScopeMap` / `flow.nifiProcessGroupId` by
`services/adapter/deployer/lifecycle.py`).

`nifi_conn` everywhere in this module is the same plain dict shape
`services/nifi_client.py` and `services/nifi_flow_manager.py` already take:
`{"endpoint": <base url>, "auth_type": "NONE"|"BASIC"|"BEARER", "username"?,
"password"?, "token"?}` — never a `connections_v2` document directly (that
translation is `lifecycle.py`'s job, since only it talks to Mongo).

REST call shapes below are lifted from `services/nifi_flow_generator.py`
(the alpha's from-scratch flow builder — `_create_process_group`,
`_create_controller_service`, `_create_processor`, `_create_connection`,
`_create_input_port`/`_create_output_port`, `_wait_cs_enabled`) rather than
importing that 6.5k-line module: this file re-derives the same NiFi 2.9 REST
shapes cleanly, and reuses `services/nifi_flow_manager.py`'s already-tested
verb helpers (`start_process_group`, `stop_process_group`,
`clear_process_group_queues`, `prepare_process_group_for_start`,
`get_controller_service_config`) wherever a direct match exists, instead of
re-implementing them a third time.

Apply order (compiler-spec.md §7): parameter context -> flow PG (direct
child of the true NiFi root) -> one child PG per BlockGroup (flat — every
BlockGroup is a sibling under the flow PG, never nested under another
BlockGroup; a block's logical parent/child relationship is expressed purely
through `rootGroup.connections` PortLinks, not PG nesting) -> controller
services (create with empty properties -> resolve any property whose value
is exactly another component's plan `key` to that component's real NiFi id,
now that every CS in the group has one -> PUT the resolved properties ->
enable, multi-pass, poll-verified) -> processors (same key -> id
resolution, `sensitiveDynamicPropertyNames` computed for any property whose
value references a `sensitive: true` plan parameter) -> ports -> intra-group
connections -> cross-group PortLink connections (created in the FLOW pg,
since NiFi requires a connection between two ports in different PGs to live
in their nearest common parent). Everything is left in NiFi's default
post-creation state — STOPPED processors, ENABLED controller services —
matching compiler-spec.md §7's "leave everything STOPPED".

On any failure, `apply_plan` best-effort deletes the (partially built) flow
PG via `delete_flow_pg` before re-raising `NifiApplyError`.
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from services import nifi_flow_manager
from services.nifi_client import get_nifi_root_process_group_id, nifi_api_request
from services.adapter.compiler.ir import BlockGroup, DeploymentPlan, ParameterContextSpec

logger = logging.getLogger(__name__)


class NifiApplyError(RuntimeError):
    """Raised for any reason `apply_plan` (or a verb helper) could not carry
    out its NiFi-side work. Always carries a human-readable message; the
    flow PG id (if one had been created) is attached so callers can log it
    even though `apply_plan` itself already best-effort deletes it."""

    def __init__(self, message: str, pg_id: Optional[str] = None):
        super().__init__(message)
        self.pg_id = pg_id


@dataclass
class AppliedResult:
    """Everything `lifecycle.deploy()` needs to persist into
    `flow.nifiProcessGroupId` / `flow.runtimeScopeMap`."""

    process_group_id: str
    parameter_context_id: str
    parameter_context_name: str
    groups: Dict[str, str] = field(default_factory=dict)  # blockId -> child PG id
    components: Dict[str, Dict[str, str]] = field(default_factory=dict)  # blockId -> {plan key: nifi id}


# --------------------------------------------------------------------- auth


def _auth(nifi_conn: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "auth_type": nifi_conn.get("auth_type", "NONE"),
        "username": nifi_conn.get("username"),
        "password": nifi_conn.get("password"),
        "token": nifi_conn.get("token"),
    }


def _endpoint(nifi_conn: Dict[str, Any]) -> str:
    return (nifi_conn.get("endpoint") or "").rstrip("/")


# ------------------------------------------------------------------ apply_plan


async def apply_plan(nifi_conn: Dict[str, Any], plan: DeploymentPlan) -> AppliedResult:
    url = _endpoint(nifi_conn)
    auth = _auth(nifi_conn)

    root_pg_id = await get_nifi_root_process_group_id(
        url, auth_type=auth["auth_type"], username=auth["username"], password=auth["password"], token=auth["token"]
    )
    if not root_pg_id:
        raise NifiApplyError("Could not resolve the NiFi root process group.")

    pc_id, pc_name = await _ensure_parameter_context(url, auth, plan.parameterContext)

    # Best-effort crash-recovery: a same-named PG left over from a previous
    # failed/aborted deploy would otherwise make the create below a harmless
    # duplicate-named sibling — clean it out first. `lifecycle.deploy()`
    # normally already deletes the prior PG itself (via `delete_flow_pg`)
    # before calling us again on redeploy, so this is a safety net, not the
    # primary teardown path.
    await _delete_pg_by_name(url, auth, root_pg_id, plan.rootGroup.name)

    flow_pg_id = await _create_pg(url, auth, root_pg_id, plan.rootGroup.name, x=0.0, y=0.0)
    if not flow_pg_id:
        raise NifiApplyError(f"Failed to create flow process group {plan.rootGroup.name!r}.")

    try:
        await _bind_parameter_context(url, auth, flow_pg_id, pc_id)

        groups: Dict[str, str] = {}
        components: Dict[str, Dict[str, str]] = {}

        for idx, group in enumerate(plan.rootGroup.childGroups):
            gid = await _create_pg(url, auth, flow_pg_id, group.name, x=idx * 420.0, y=200.0)
            if not gid:
                raise NifiApplyError(f"Failed to create block process group {group.name!r}.")
            await _bind_parameter_context(url, auth, gid, pc_id)
            groups[group.blockId] = gid
            components[group.blockId] = {}

        for group in plan.rootGroup.childGroups:
            await _apply_controller_services(url, auth, groups[group.blockId], group, components[group.blockId])

        for group in plan.rootGroup.childGroups:
            await _apply_processors(url, auth, groups[group.blockId], group, components[group.blockId], plan)

        for group in plan.rootGroup.childGroups:
            await _apply_ports(url, auth, groups[group.blockId], group, plan.rootGroup.connections, components[group.blockId])

        for group in plan.rootGroup.childGroups:
            await _apply_intra_connections(url, auth, groups[group.blockId], group, components[group.blockId])

        await _apply_port_links(url, auth, flow_pg_id, plan.rootGroup.connections, groups, components)
    except Exception as exc:
        logger.error("Applying plan for flow %s failed — deleting partial process group %s: %s", plan.flowId, flow_pg_id, exc)
        try:
            await delete_flow_pg(nifi_conn, flow_pg_id)
        except Exception:
            logger.exception("Best-effort cleanup of failed-deploy process group %s also failed.", flow_pg_id)
        if isinstance(exc, NifiApplyError):
            raise
        raise NifiApplyError(f"Failed to apply deployment plan: {exc}", pg_id=flow_pg_id) from exc

    return AppliedResult(
        process_group_id=flow_pg_id,
        parameter_context_id=pc_id,
        parameter_context_name=pc_name,
        groups=groups,
        components=components,
    )


# ------------------------------------------------------------ parameter context


def _param_value_for_nifi(value: Optional[str]) -> str:
    """M8 fix: a `None`-valued parameter serializes as JSON `null` in NiFi's
    parameter-context payload — on the UPDATE path (`_update_parameter_
    context`, taken on every redeploy of an already-deployed flow) a `null`
    value is NiFi's DELETE-this-parameter instruction, not "this parameter
    has no value". `add_param("redis_password", None, True)` (an in-cluster
    Redis with no password configured — the documented deployment) and
    every optional secret/field compiles a `None`-valued
    `ParameterSpec.value` today; on first deploy that is harmless (nothing
    to delete yet), but redeploy silently removes the parameter while
    `#{redis_password}` (or the equivalent property) still references it,
    which fails that controller service's validation and the redeploy dies
    ~45s later. Coercing `None` -> `""` here — for both sensitive and
    non-sensitive parameters — keeps the parameter (and therefore every
    `#{...}` reference to it) alive across redeploys; the referencing
    property just resolves to an empty string, exactly like an
    unconfigured optional secret should."""
    return "" if value is None else value


async def _ensure_parameter_context(url: str, auth: Dict[str, Any], spec: ParameterContextSpec) -> Tuple[str, str]:
    listing = await nifi_api_request(url, "GET", "/nifi-api/flow/parameter-contexts", **auth)
    existing_id: Optional[str] = None
    if listing.get("ok"):
        for pc in (listing.get("data") or {}).get("parameterContexts") or []:
            comp = pc.get("component") or {}
            if comp.get("name") == spec.name:
                existing_id = comp.get("id")
                break

    params_body = [
        {"parameter": {"name": p.name, "value": _param_value_for_nifi(p.value), "sensitive": p.sensitive}}
        for p in spec.parameters
    ]

    if existing_id is None:
        body = {"revision": {"version": 0}, "component": {"name": spec.name, "parameters": params_body}}
        r = await nifi_api_request(url, "POST", "/nifi-api/parameter-contexts", json_body=body, **auth)
        if not r.get("ok"):
            raise NifiApplyError(f"Failed to create parameter context {spec.name!r}: {r.get('error')}")
        data = r.get("data") or {}
        pc_id = data.get("id") or (data.get("component") or {}).get("id")
        if not pc_id:
            raise NifiApplyError(f"NiFi did not return an id for parameter context {spec.name!r}.")
        return pc_id, spec.name

    await _update_parameter_context(url, auth, existing_id, params_body)
    return existing_id, spec.name


async def _update_parameter_context(url: str, auth: Dict[str, Any], pc_id: str, params_body: List[Dict[str, Any]]) -> None:
    current = await nifi_api_request(url, "GET", f"/nifi-api/parameter-contexts/{pc_id}", **auth)
    if not current.get("ok"):
        raise NifiApplyError(f"Failed to read parameter context {pc_id} before updating it: {current.get('error')}")
    rev = (current.get("data") or {}).get("revision") or {"version": 0}
    body = {"revision": rev, "component": {"id": pc_id, "parameters": params_body}}
    submitted = await nifi_api_request(url, "POST", f"/nifi-api/parameter-contexts/{pc_id}/update-requests", json_body=body, **auth)
    if not submitted.get("ok"):
        raise NifiApplyError(f"Failed to submit a parameter context update for {pc_id}: {submitted.get('error')}")

    req = (submitted.get("data") or {}).get("request") or submitted.get("data") or {}
    request_id = req.get("requestId") or req.get("id")
    if not request_id:
        # Some NiFi builds apply a parameter-context update synchronously
        # and return the finished result inline — nothing left to poll.
        return

    deadline = asyncio.get_event_loop().time() + 30
    while asyncio.get_event_loop().time() < deadline:
        poll = await nifi_api_request(url, "GET", f"/nifi-api/parameter-contexts/{pc_id}/update-requests/{request_id}", **auth)
        if poll.get("ok"):
            preq = (poll.get("data") or {}).get("request") or poll.get("data") or {}
            if preq.get("complete"):
                break
        await asyncio.sleep(1)
    await nifi_api_request(url, "DELETE", f"/nifi-api/parameter-contexts/{pc_id}/update-requests/{request_id}", **auth)


async def _bind_parameter_context(url: str, auth: Dict[str, Any], pg_id: str, pc_id: str) -> None:
    current = await nifi_api_request(url, "GET", f"/nifi-api/process-groups/{pg_id}", **auth)
    if not current.get("ok"):
        raise NifiApplyError(f"Failed to read process group {pg_id} before binding its parameter context: {current.get('error')}")
    rev = (current.get("data") or {}).get("revision") or {"version": 0}
    body = {"revision": rev, "component": {"id": pg_id, "parameterContext": {"id": pc_id}}}
    r = await nifi_api_request(url, "PUT", f"/nifi-api/process-groups/{pg_id}", json_body=body, **auth)
    if not r.get("ok"):
        raise NifiApplyError(f"Failed to bind parameter context {pc_id} to process group {pg_id}: {r.get('error')}")


# --------------------------------------------------------------- process groups


async def _create_pg(url: str, auth: Dict[str, Any], parent_pg_id: str, name: str, *, x: float, y: float) -> Optional[str]:
    body = {"revision": {"version": 0}, "component": {"name": name, "position": {"x": x, "y": y}}}
    r = await nifi_api_request(url, "POST", f"/nifi-api/process-groups/{parent_pg_id}/process-groups", json_body=body, **auth)
    if r.get("ok"):
        data = r.get("data") or {}
        return data.get("id") or (data.get("component") or {}).get("id")
    logger.error("Failed to create process group %r under %s: %s", name, parent_pg_id, r.get("error"))
    return None


async def _delete_pg_by_name(url: str, auth: Dict[str, Any], parent_pg_id: str, name: str) -> None:
    listing = await nifi_api_request(url, "GET", f"/nifi-api/process-groups/{parent_pg_id}/process-groups", **auth)
    if not listing.get("ok"):
        return
    for pg in (listing.get("data") or {}).get("processGroups") or []:
        comp = pg.get("component") or {}
        if comp.get("name") == name:
            old_id = comp.get("id") or pg.get("id")
            if old_id:
                logger.warning("Leftover process group named %r (%s) found under %s — deleting before redeploy.", name, old_id, parent_pg_id)
                try:
                    await delete_flow_pg({"endpoint": url, **auth}, old_id)
                except Exception:
                    logger.exception("Best-effort cleanup of leftover process group %s failed; continuing.", old_id)


# ---------------------------------------------------------- controller services

_PARAM_REF_RE = re.compile(r"#\{([A-Za-z0-9_]+)\}")
# Static, predefined-sensitive properties this compiler ever emits a
# `#{sensitive_param}` reference into. NiFi rejects listing a STATIC
# property in `sensitiveDynamicPropertyNames` (it isn't dynamic), so these
# must never end up in the dynamic-sensitive list computed below even
# though their value does reference a sensitive parameter.
_STATIC_SENSITIVE_PROPERTY_NAMES = {"Request Password", "Client Secret", "Password"}


def _resolve_component_refs(properties: Dict[str, Any], key_to_id: Dict[str, str]) -> Dict[str, Any]:
    """Drop `None`-valued properties (matches the alpha's `_create_processor`
    — NiFi rejects null property values for some descriptors) and replace
    any property value that is EXACTLY a known local plan `key` (a
    controller service the same BlockGroup declared) with that service's
    real NiFi id."""
    out: Dict[str, Any] = {}
    for k, v in properties.items():
        if v is None:
            continue
        if isinstance(v, str) and v in key_to_id:
            out[k] = key_to_id[v]
        else:
            out[k] = v
    return out


def _sensitive_dynamic_props(properties: Dict[str, Any], sensitive_param_names: set) -> List[str]:
    if not sensitive_param_names:
        return []
    found: List[str] = []
    for name, value in properties.items():
        if name in _STATIC_SENSITIVE_PROPERTY_NAMES or not isinstance(value, str):
            continue
        refs = _PARAM_REF_RE.findall(value)
        if any(r in sensitive_param_names for r in refs):
            found.append(name)
    return found


async def _create_cs(url: str, auth: Dict[str, Any], pg_id: str, cs_type: str, name: str) -> Optional[str]:
    body = {"revision": {"version": 0}, "component": {"type": cs_type, "name": name, "properties": {}}}
    r = await nifi_api_request(url, "POST", f"/nifi-api/process-groups/{pg_id}/controller-services", json_body=body, **auth)
    if r.get("ok"):
        data = r.get("data") or {}
        return data.get("id") or (data.get("component") or {}).get("id")
    logger.error("Failed to create controller service %r (%s): %s", name, cs_type, r.get("error"))
    return None


async def _update_cs_properties(url: str, auth: Dict[str, Any], cs_id: str, properties: Dict[str, Any]) -> bool:
    current = await nifi_api_request(url, "GET", f"/nifi-api/controller-services/{cs_id}", **auth)
    if not current.get("ok"):
        return False
    rev = (current.get("data") or {}).get("revision") or {"version": 0}
    body = {"revision": rev, "component": {"id": cs_id, "properties": properties}}
    r = await nifi_api_request(url, "PUT", f"/nifi-api/controller-services/{cs_id}", json_body=body, **auth)
    return bool(r.get("ok"))


async def _wait_all_cs_enabled(url: str, auth: Dict[str, Any], cs_ids: List[str], *, timeout: int = 45) -> None:
    """Poll-verify every id in `cs_ids` reaches ENABLED, re-nudging the
    group's enable pass in between polls (dependency chains among
    same-group services — e.g. the dedup Redis cache CS referencing the
    Redis pool CS — can need more than one enable pass; mirrors the
    alpha's `_wait_cs_enabled`, generalized to a whole set of ids)."""
    deadline = asyncio.get_event_loop().time() + timeout
    last: Dict[str, Dict[str, Any]] = {}
    while asyncio.get_event_loop().time() < deadline:
        last = {}
        pending: List[str] = []
        for cid in cs_ids:
            info = await nifi_flow_manager.get_controller_service_config(
                url, cid, auth_type=auth["auth_type"], username=auth["username"], password=auth["password"], token=auth["token"]
            )
            last[cid] = info
            if info.get("state") != "ENABLED":
                pending.append(cid)
        if not pending:
            return
        for cid in pending:
            await _set_cs_enabled(url, auth, cid)
        await asyncio.sleep(2)

    details = "; ".join(
        f"{cid} state={info.get('state')} errors={info.get('validation_errors')}"
        for cid, info in last.items()
        if info.get("state") != "ENABLED"
    )
    raise NifiApplyError(f"Controller service(s) did not reach ENABLED within {timeout}s: {details}")


async def _set_cs_enabled(url: str, auth: Dict[str, Any], cs_id: str) -> None:
    current = await nifi_api_request(url, "GET", f"/nifi-api/controller-services/{cs_id}", **auth)
    if not current.get("ok"):
        return
    rev = ((current.get("data") or {}).get("revision") or {}).get("version", 0)
    await nifi_api_request(
        url, "PUT", f"/nifi-api/controller-services/{cs_id}/run-status",
        json_body={"revision": {"version": rev}, "state": "ENABLED", "disconnectedNodeAcknowledged": False}, **auth,
    )


async def _apply_controller_services(url: str, auth: Dict[str, Any], gid: str, group: BlockGroup, out_components: Dict[str, str]) -> None:
    cs_ids: Dict[str, str] = {}
    for cs in group.controllerServices:
        cid = await _create_cs(url, auth, gid, cs.type, cs.name)
        if not cid:
            raise NifiApplyError(f"Failed to create controller service {cs.name!r} ({cs.type}) in group {group.name!r}.")
        cs_ids[cs.key] = cid
        out_components[cs.key] = cid

    for cs in group.controllerServices:
        resolved = _resolve_component_refs(cs.properties, cs_ids)
        if not await _update_cs_properties(url, auth, cs_ids[cs.key], resolved):
            raise NifiApplyError(f"Failed to configure controller service {cs.name!r} ({cs_ids[cs.key]}) in group {group.name!r}.")

    if cs_ids:
        await _wait_all_cs_enabled(url, auth, list(cs_ids.values()))


# ---------------------------------------------------------------------- processors


async def _create_processor(
    url: str, auth: Dict[str, Any], pg_id: str, proc_type: str, name: str, properties: Dict[str, Any], *,
    auto_terminate: List[str], scheduling_strategy: str, scheduling_period: str,
    execution_node: Optional[str], penalty: Optional[str], sensitive_dynamic_property_names: List[str],
    x: float, y: float,
) -> Optional[str]:
    config: Dict[str, Any] = {
        "properties": properties,
        "schedulingStrategy": scheduling_strategy,
        "schedulingPeriod": scheduling_period,
        "autoTerminatedRelationships": list(auto_terminate),
    }
    if execution_node:
        config["executionNode"] = execution_node
    if penalty:
        config["penaltyDuration"] = penalty
    if sensitive_dynamic_property_names:
        config["sensitiveDynamicPropertyNames"] = sensitive_dynamic_property_names
    body = {
        "revision": {"version": 0},
        "component": {"type": proc_type, "name": name, "position": {"x": x, "y": y}, "config": config},
    }
    r = await nifi_api_request(url, "POST", f"/nifi-api/process-groups/{pg_id}/processors", json_body=body, **auth)
    if r.get("ok"):
        data = r.get("data") or {}
        return data.get("id") or (data.get("component") or {}).get("id")
    logger.error("Failed to create processor %r (%s): %s", name, proc_type, r.get("error"))
    return None


async def _apply_processors(url: str, auth: Dict[str, Any], gid: str, group: BlockGroup, out_components: Dict[str, str], plan: DeploymentPlan) -> None:
    cs_key_to_id = dict(out_components)  # CS entries only at this point in apply_plan's ordering.
    sensitive_param_names = {p.name for p in plan.parameterContext.parameters if p.sensitive}

    for i, proc in enumerate(group.processors):
        resolved = _resolve_component_refs(proc.properties, cs_key_to_id)
        dyn_sensitive = _sensitive_dynamic_props(resolved, sensitive_param_names)
        pid = await _create_processor(
            url, auth, gid, proc.type, proc.name, resolved,
            auto_terminate=proc.autoTerminate,
            scheduling_strategy=proc.schedulingStrategy or "TIMER_DRIVEN",
            scheduling_period=proc.schedulingPeriod or "0 sec",
            execution_node=("PRIMARY" if proc.runOnPrimary else None),
            penalty=proc.penalty,
            sensitive_dynamic_property_names=dyn_sensitive,
            x=200.0, y=float(i * 150),
        )
        if not pid:
            raise NifiApplyError(f"Failed to create processor {proc.name!r} ({proc.type}) in group {group.name!r}.")
        out_components[proc.key] = pid


# --------------------------------------------------------------------------- ports


async def _create_port(url: str, auth: Dict[str, Any], pg_id: str, kind: str, name: str, *, x: float, y: float) -> Optional[str]:
    body = {"revision": {"version": 0}, "component": {"name": name, "position": {"x": x, "y": y}}}
    r = await nifi_api_request(url, "POST", f"/nifi-api/process-groups/{pg_id}/{kind}", json_body=body, **auth)
    if r.get("ok"):
        data = r.get("data") or {}
        return data.get("id") or (data.get("component") or {}).get("id")
    logger.error("Failed to create %s %r: %s", kind, name, r.get("error"))
    return None


async def _apply_ports(url: str, auth: Dict[str, Any], gid: str, group: BlockGroup, port_links, out_components: Dict[str, str]) -> None:
    if group.inputPort:
        pid = await _create_port(url, auth, gid, "input-ports", f"{group.name}__in", x=200.0, y=0.0)
        if not pid:
            raise NifiApplyError(f"Failed to create the input port for group {group.name!r}.")
        out_components["inputPort"] = pid

    if group.outputPort:
        children = [pl for pl in port_links if pl.fromBlockId == group.blockId]
        for i, pl in enumerate(children):
            pid = await _create_port(url, auth, gid, "output-ports", f"{group.name}__out__{pl.toBlockId}", x=200.0 + i * 220.0, y=900.0)
            if not pid:
                raise NifiApplyError(f"Failed to create the output port for group {group.name!r} -> block {pl.toBlockId!r}.")
            out_components[pl.viaPort] = pid


# ---------------------------------------------------------------------- connections


async def _create_connection(
    url: str, auth: Dict[str, Any], pg_id: str, src_id: str, dst_id: str, relationships: List[str], *,
    src_type: str = "PROCESSOR", dst_type: str = "PROCESSOR",
    src_group_id: Optional[str] = None, dst_group_id: Optional[str] = None,
) -> Optional[str]:
    # Ports carry no outgoing relationship labels; NiFi requires an empty list.
    effective_rels = list(relationships) if src_type == "PROCESSOR" else []
    body = {
        "revision": {"version": 0},
        "component": {
            "source": {"id": src_id, "groupId": src_group_id or pg_id, "type": src_type},
            "destination": {"id": dst_id, "groupId": dst_group_id or pg_id, "type": dst_type},
            "selectedRelationships": effective_rels,
        },
    }
    r = await nifi_api_request(url, "POST", f"/nifi-api/process-groups/{pg_id}/connections", json_body=body, **auth)
    if r.get("ok"):
        data = r.get("data") or {}
        return data.get("id") or (data.get("component") or {}).get("id")
    logger.error("Failed to create connection %s -> %s: %s", src_id, dst_id, r.get("error"))
    return None


def _resolve_endpoint(key: str, components: Dict[str, str]) -> Tuple[Optional[str], str]:
    if key == "inputPort":
        return components.get(key), "INPUT_PORT"
    if key.startswith("outputPort:"):
        return components.get(key), "OUTPUT_PORT"
    return components.get(key), "PROCESSOR"


async def _apply_intra_connections(url: str, auth: Dict[str, Any], gid: str, group: BlockGroup, components: Dict[str, str]) -> None:
    for conn in group.connections:
        # The "dlq" token always resolves locally to this group's own
        # dlq__meta processor — DLQ never crosses a process-group boundary
        # (see ir.py's ConnectionSpec docstring).
        dst_key = "dlq__meta" if conn.to == "dlq" else conn.to
        src_id, src_type = _resolve_endpoint(conn.from_, components)
        dst_id, dst_type = _resolve_endpoint(dst_key, components)
        if not src_id or not dst_id:
            raise NifiApplyError(f"Cannot wire group {group.name!r}: unresolved endpoint {conn.from_!r} -> {dst_key!r}.")
        cid = await _create_connection(url, auth, gid, src_id, dst_id, conn.relationships, src_type=src_type, dst_type=dst_type)
        if not cid:
            raise NifiApplyError(f"Failed to create connection {conn.from_!r} -> {dst_key!r} in group {group.name!r}.")


async def _apply_port_links(
    url: str, auth: Dict[str, Any], flow_pg_id: str, port_links, groups: Dict[str, str], components: Dict[str, Dict[str, str]],
) -> None:
    for pl in port_links:
        src_group_id = groups.get(pl.fromBlockId)
        dst_group_id = groups.get(pl.toBlockId)
        src_port_id = (components.get(pl.fromBlockId) or {}).get(pl.viaPort)
        dst_port_id = (components.get(pl.toBlockId) or {}).get("inputPort")
        if not (src_group_id and dst_group_id and src_port_id and dst_port_id):
            raise NifiApplyError(f"Cannot wire block {pl.fromBlockId!r} -> {pl.toBlockId!r}: a port or group id is missing.")
        cid = await _create_connection(
            url, auth, flow_pg_id, src_port_id, dst_port_id, [],
            src_type="OUTPUT_PORT", dst_type="INPUT_PORT", src_group_id=src_group_id, dst_group_id=dst_group_id,
        )
        if not cid:
            raise NifiApplyError(f"Failed to create the cross-group connection {pl.fromBlockId!r} -> {pl.toBlockId!r}.")


# ------------------------------------------------------------------------- verbs


async def delete_flow_pg(nifi_conn: Dict[str, Any], process_group_id: str) -> Dict[str, Any]:
    """Best-effort teardown of a flow's root PG: stop -> disable controller
    services -> purge queues -> delete (409-retried). Mirrors
    `services/nifi_flow_generator.py`'s `_delete_old_pg`. Never raises — a
    process group that is already gone is treated as success, and any
    other failure is returned as `{"ok": False, "error": ...}` rather than
    an exception, since every caller of this (redeploy/undeploy/delete)
    wants "best-effort" semantics, not a hard failure that leaves the flow
    doc stuck."""
    if not process_group_id:
        return {"ok": True, "message": "No process group id given."}
    url = _endpoint(nifi_conn)
    auth = _auth(nifi_conn)
    try:
        await nifi_api_request(
            url, "PUT", f"/nifi-api/flow/process-groups/{process_group_id}",
            json_body={"id": process_group_id, "state": "STOPPED", "disconnectedNodeAcknowledged": False}, **auth,
        )
        await asyncio.sleep(2)

        await _disable_all_controller_services_recursive(url, auth, process_group_id)

        await nifi_api_request(url, "POST", f"/nifi-api/process-groups/{process_group_id}/empty-all-connections-requests", json_body={}, **auth)
        await asyncio.sleep(1)

        r = await nifi_api_request(url, "GET", f"/nifi-api/process-groups/{process_group_id}", **auth)
        if not r.get("ok"):
            return {"ok": True, "message": "Process group already gone."}
        version = (r.get("data") or {}).get("revision", {}).get("version", 0)

        for attempt in range(6):
            del_r = await nifi_api_request(
                url, "DELETE", f"/nifi-api/process-groups/{process_group_id}",
                params={"version": str(version), "disconnectedNodeAcknowledged": "false"}, **auth,
            )
            if del_r.get("ok"):
                return {"ok": True}
            if del_r.get("status_code") == 409:
                logger.warning("409 deleting process group %s (attempt %d): %s", process_group_id, attempt + 1, del_r.get("error"))
                await asyncio.sleep(3)
                r2 = await nifi_api_request(url, "GET", f"/nifi-api/process-groups/{process_group_id}", **auth)
                if r2.get("ok"):
                    version = (r2.get("data") or {}).get("revision", {}).get("version", 0)
                continue
            return {"ok": False, "error": del_r.get("error") or "Failed to delete process group."}
        return {"ok": False, "error": f"Could not delete process group {process_group_id} after retries."}
    except Exception as exc:  # pragma: no cover - defensive, matches this fn's documented never-raise contract
        logger.exception("delete_flow_pg(%s) failed", process_group_id)
        return {"ok": False, "error": str(exc)[:300]}


async def _collect_pg_ids_recursive(url: str, auth: Dict[str, Any], pg_id: str, seen: Optional[set] = None) -> List[str]:
    seen = seen if seen is not None else set()
    if pg_id in seen:
        return []
    seen.add(pg_id)
    r = await nifi_api_request(url, "GET", f"/nifi-api/flow/process-groups/{pg_id}", **auth)
    if not r.get("ok"):
        return [pg_id]
    flow = ((r.get("data") or {}).get("processGroupFlow") or {}).get("flow") or {}
    ids = [pg_id]
    for child in flow.get("processGroups") or []:
        cid = (child.get("component") or {}).get("id") or child.get("id")
        if cid:
            ids.extend(await _collect_pg_ids_recursive(url, auth, cid, seen))
    return ids


async def _disable_all_controller_services_recursive(url: str, auth: Dict[str, Any], root_pg_id: str) -> None:
    """Disable every controller service OWNED anywhere in `root_pg_id`'s PG
    tree — every BlockGroup child PG has its own (a controller service
    created inside a nested PG is scoped to that PG's own subtree; it is
    NOT returned by `GET /flow/process-groups/{id}/controller-services`
    called on an ANCESTOR of that PG, only on that PG or one of ITS OWN
    descendants — the opposite direction from what a first read of that
    endpoint suggests. Calling it once on `root_pg_id` alone only surfaces
    services INHERITED FROM root_pg_id's ancestors (e.g. shared
    platform-level services), which this function must never touch — so it
    walks every PG in the tree and disables only what each one actually
    owns (`component.parentGroupId == that PG's own id`)."""
    pg_ids = await _collect_pg_ids_recursive(url, auth, root_pg_id)
    to_disable: List[Dict[str, Any]] = []
    for pg_id in pg_ids:
        cs_resp = await nifi_api_request(url, "GET", f"/nifi-api/flow/process-groups/{pg_id}/controller-services", **auth)
        if not cs_resp.get("ok"):
            continue
        for cs in (cs_resp.get("data") or {}).get("controllerServices") or []:
            component = cs.get("component") or {}
            owner = component.get("parentGroupId") or component.get("processGroupId") or component.get("groupId")
            if owner != pg_id:
                continue  # inherited from an ancestor outside this tree — never ours to touch
            cs_id = component.get("id") or cs.get("id")
            if cs_id:
                to_disable.append({"id": cs_id, "revision": (cs.get("revision") or {}).get("version", 0)})

    for cs in to_disable:
        await nifi_api_request(
            url, "PUT", f"/nifi-api/controller-services/{cs['id']}/run-status",
            json_body={"revision": {"version": cs["revision"]}, "state": "DISABLED", "disconnectedNodeAcknowledged": False}, **auth,
        )

    if not to_disable:
        return

    # Poll-verify DISABLED (mirrors _wait_all_cs_enabled's pattern, inverted) —
    # NiFi's disable is usually fast but is still an async state transition.
    deadline = asyncio.get_event_loop().time() + 20
    while asyncio.get_event_loop().time() < deadline:
        pending = []
        for cs in to_disable:
            info = await nifi_api_request(url, "GET", f"/nifi-api/controller-services/{cs['id']}", **auth)
            state = ((info.get("data") or {}).get("component") or {}).get("state") if info.get("ok") else None
            if state != "DISABLED":
                pending.append(cs["id"])
        if not pending:
            return
        await asyncio.sleep(1)
    logger.warning("Controller service(s) did not confirm DISABLED before deleting process group %s: %s", root_pg_id, [c["id"] for c in to_disable])


async def start_pg(nifi_conn: Dict[str, Any], process_group_id: str) -> Dict[str, Any]:
    auth = _auth(nifi_conn)
    return await nifi_flow_manager.start_process_group(
        _endpoint(nifi_conn), process_group_id,
        auth_type=auth["auth_type"], username=auth["username"], password=auth["password"], token=auth["token"],
    )


async def stop_pg(nifi_conn: Dict[str, Any], process_group_id: str) -> Dict[str, Any]:
    auth = _auth(nifi_conn)
    return await nifi_flow_manager.stop_process_group(
        _endpoint(nifi_conn), process_group_id,
        auth_type=auth["auth_type"], username=auth["username"], password=auth["password"], token=auth["token"],
    )


async def drop_all_queues(nifi_conn: Dict[str, Any], process_group_id: str) -> Dict[str, Any]:
    auth = _auth(nifi_conn)
    return await nifi_flow_manager.clear_process_group_queues(
        _endpoint(nifi_conn), process_group_id,
        auth_type=auth["auth_type"], username=auth["username"], password=auth["password"], token=auth["token"],
    )


async def _set_processors_state(nifi_conn: Dict[str, Any], processor_ids: List[str], target_state: str) -> Dict[str, Any]:
    url = _endpoint(nifi_conn)
    auth = _auth(nifi_conn)
    failed: List[str] = []
    for pid in processor_ids:
        current = await nifi_api_request(url, "GET", f"/nifi-api/processors/{pid}", **auth)
        if not current.get("ok"):
            failed.append(pid)
            continue
        rev = ((current.get("data") or {}).get("revision") or {}).get("version", 0)
        r = await nifi_api_request(
            url, "PUT", f"/nifi-api/processors/{pid}/run-status",
            json_body={"revision": {"version": rev}, "state": target_state, "disconnectedNodeAcknowledged": False}, **auth,
        )
        if not r.get("ok"):
            failed.append(pid)
    return {"ok": not failed, "failed": failed}


async def pause_trigger(nifi_conn: Dict[str, Any], processor_ids: List[str]) -> Dict[str, Any]:
    """Stop ONLY the given processor ids (the root block's trigger/ingest
    processors — resolved by `lifecycle.py` from `flow.runtimeScopeMap`,
    since only it knows which plan `key`s are the trigger for a given
    adapter). Everything else in the flow PG keeps whatever state it is in,
    which is the whole point of pause vs. stop."""
    return await _set_processors_state(nifi_conn, processor_ids, "STOPPED")


async def resume_trigger(nifi_conn: Dict[str, Any], processor_ids: List[str]) -> Dict[str, Any]:
    return await _set_processors_state(nifi_conn, processor_ids, "RUNNING")
