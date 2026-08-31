"""Port of frontend/src/prototype/validation.ts: block-level and flow-level
issues that drive the frontend's error badges / Validate panel / deploy
preflight, now the server-side source of truth for the same checks.

`validate_block` / `validate_flow` return the same `{blockId, where,
message}` issue shape as the TS `ValidationIssue` interface (camelCase).

Two small helpers this file needs (`gateway`/proxy facts and "is this
branch's rule set incomplete") are pure ports of code that lives in
sibling frontend files rather than validation.ts itself:
  - `GatewaySnapshot` / `blockProxyId` / `gatewayRefusals` -- from
    validation.ts (unchanged location).
  - branch-incompleteness -- ported from frontend/src/prototype/branches.ts
    (`rulesOf` / `opNeedsValue` / `ruleIncomplete` / `branchIncomplete`),
    inlined here as `_branch_incomplete` since validation.ts only imports
    the one function it needs from that file.

Beyond the literal TS port, `validate_block` adds three dedup checks the
task brief asked for that do not exist in validation.ts today: "only one
dedup transform per block", "dedup window (`windowHours`) must be within
1/60-8760 hours (1 minute - 365 days), default 24", and "dedup needs at
least one identity field". validation.ts today only checks "dedup must be
the last transformation" (findIndex on the *first* dedup, which is why the
port below computes the same first-index check for that one message, then
layers the three new checks on top, scanning every dedup transform found).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from models.adapter import AppService, ApprovedSchema, BranchInfo, Flow, FlowBlock, GatewayProxy

from .legality import flow_has_trigger, hosts_transforms, root_block
from .naming import derive_topic_name, is_valid_cron, override_matches_derived, topic_name_collision


@dataclass
class ValidationIssue:
    blockId: Optional[str]  # None = flow-level
    where: str  # "Flow settings" or block name
    message: str


@dataclass
class GatewaySnapshot:
    """The gateway facts the http checks need. Passed in so validation stays
    pure -- the router layer is responsible for reading the real
    proxies/allowlist (from COLLECTIONS.gateway) and constructing this."""

    proxies: List[GatewayProxy] = field(default_factory=list)
    allowlist: List[str] = field(default_factory=list)


def _is_kafka_family_write(block: FlowBlock) -> bool:
    return (block.adapter == "kafka" and block.mode == "write") or block.adapter == "kafka_kc"


def block_proxy_id(block: FlowBlock, services: Optional[List[AppService]] = None) -> Optional[str]:
    """The proxy an http block routes through, or None.

    A proxy is how a HOST is reached, and the host belongs to the service --
    every block using that service inherits the same egress. A block-level
    `config.proxyId` is still honoured as a fallback for datasets saved
    before the reference moved to the service; nothing writes one any more.
    """
    if block.adapter != "http":
        return None
    services = services or []
    service = next((s for s in services if s.id == block.serviceId), None)
    from_service = (service.config or {}).get("proxyId") if service else None
    if isinstance(from_service, str) and from_service.strip():
        return from_service
    legacy = (block.config or {}).get("proxyId")
    return legacy if isinstance(legacy, str) and legacy.strip() else None


def gateway_refusals(block: FlowBlock, gateway: GatewaySnapshot, services: Optional[List[AppService]] = None) -> List[str]:
    """Every reason the referenced proxy is not deployable, in user-facing words."""
    proxy_id = block_proxy_id(block, services)
    if not proxy_id:
        return []
    proxy = next((p for p in gateway.proxies if p.id == proxy_id), None)
    if not proxy:
        return [f"The APISIX proxy this block routes through ({proxy_id}) no longer exists — pick one on the APISIX Gateway page."]
    refusals: List[str] = []
    if proxy.status != "Reconciled":
        detail = f" {proxy.statusDetail}" if proxy.statusDetail else ""
        refusals.append(
            f'APISIX proxy "{proxy.name}" is {proxy.status} — it must reconcile onto the gateway before this flow can deploy.{detail}'
        )
    if proxy.targetHost not in gateway.allowlist:
        refusals.append(
            f'Host "{proxy.targetHost}" (proxy "{proxy.name}") is not on the gateway allowlist — egress hosts are admin-allowlisted, so an administrator must add it first.'
        )
    return refusals


def _is_write(block: FlowBlock) -> bool:
    return block.mode == "write" or block.adapter == "kafka_kc"


_PLACEHOLDER_RE = re.compile(r"\$\{([a-zA-Z0-9_.-]+)\}")


def _unresolved_placeholders(flow: Flow, block: FlowBlock) -> List[str]:
    text = json.dumps(block.config or {}, default=str)
    found = _PLACEHOLDER_RE.findall(text)
    if not found:
        return []
    # Resolved by: this flow's variables, or extraction attributes anywhere
    # upstream. Global variables are gone -- per-flow is the only scope.
    flow_vars = {v.name for v in flow.variables}
    by_id = {b.id: b for b in flow.blocks}
    upstream_attrs: set = set()
    cur: Optional[FlowBlock] = block
    while cur is not None:
        for t in cur.transforms:
            attribute = (t.config or {}).get("attribute")
            if t.kind == "extract" and isinstance(attribute, str):
                upstream_attrs.add(attribute)
        cur = by_id.get(cur.parentId) if cur.parentId else None
    ordered_unique: List[str] = []
    seen = set()
    for name in found:
        if name not in seen:
            seen.add(name)
            ordered_unique.append(name)
    return [name for name in ordered_unique if name not in flow_vars and name not in upstream_attrs]


_CONNECTOR_CLASS_RE = re.compile(r"^[\w$]+(\.[\w$]+)+$", re.ASCII)


def _sink_config_refusals(block: FlowBlock) -> List[str]:
    """Sink-configuration sanity for kc / kafka_kc. Deliberately narrow: an
    empty sink config is a legitimate "not configured yet" state, so only a
    config that *says* something wrong is an issue."""
    if block.adapter != "kc" and block.adapter != "kafka_kc":
        return []
    sink = (block.config or {}).get("sinkConfig")
    if not isinstance(sink, dict) or len(sink) == 0:
        return []
    refusals: List[str] = []
    connector_class = sink.get("connector.class")
    if not isinstance(connector_class, str) or not connector_class.strip():
        refusals.append("Set connector.class — the platform has to know which Connect plugin runs this sink.")
    elif not _CONNECTOR_CLASS_RE.match(connector_class.strip()):
        # A class outside the shipped catalog is a CUSTOM sink, not an error.
        refusals.append(
            f'connector.class "{connector_class}" is not a class name — a custom sink still needs a fully-qualified class, e.g. com.example.kafka.connect.MySinkConnector.'
        )
    # Platform-owned keys are rendered as disabled rows and computed at render;
    # a persisted copy goes stale the moment a name changes.
    owned = [k for k in ("topics", "key.converter", "value.converter") if k in sink]
    if owned:
        refusals.append(f"The platform owns {', '.join(owned)} — remove {'them' if len(owned) > 1 else 'it'}; the value is derived at deploy.")
    return refusals


# ------------------------------------------------------ branch incompleteness
# Ported from frontend/src/prototype/branches.ts.

_BRANCH_OPS_NEED_VALUE = {
    "equals": True,
    "not_equals": True,
    "contains": True,
    "starts_with": True,
    "regex": True,
    "is_empty": False,
}


def _op_needs_value(op: str) -> bool:
    return _BRANCH_OPS_NEED_VALUE.get(op, True)


def _rule_incomplete(rule: Any) -> bool:
    if not (getattr(rule, "field", "") or "").strip():
        return True
    return _op_needs_value(getattr(rule, "op", "equals")) and not (getattr(rule, "value", "") or "").strip()


def _branch_incomplete(branch: Optional[BranchInfo]) -> bool:
    rules = (branch.rules if branch and branch.rules else None) or []
    return any(_rule_incomplete(r) for r in rules)


# --------------------------------------------------------------------- dedup

DEDUP_MIN_WINDOW_HOURS = 1 / 60  # 1 minute
DEDUP_MAX_WINDOW_HOURS = 8760  # 365 days
DEDUP_DEFAULT_WINDOW_HOURS = 24

_RETENTION_KINDS = {"extract", "add_field", "set_from_attribute", "rename"}
_PAGINATION_TYPES = {"none", "page", "cursor", "offset", "next_url"}
_PAGINATION_STOPS = {"empty_response", "total_count", "has_more"}


def _whole_number(value: Any, *, minimum: int = 1) -> bool:
    try:
        text = str(value).strip()
        return bool(text) and int(text) >= minimum and str(int(text)) == text
    except (TypeError, ValueError):
        return False


def _pagination_refusals(block: FlowBlock) -> List[str]:
    """Validate the v2 UI/compiler pagination contract before deployment."""
    pagination = (block.config or {}).get("pagination") or {}
    ptype = str(pagination.get("type") or "none").strip().lower()
    fields = pagination.get("fields") or {}
    if ptype not in _PAGINATION_TYPES:
        return [f"Unsupported pagination type: {ptype or '(blank)' }."]
    if ptype == "none":
        return []

    issues: List[str] = []
    if block.mode == "write" and ptype in {"cursor", "next_url"}:
        issues.append("HTTP write pagination supports page or offset counters, not cursor or next URL.")
    if block.mode == "write" and str((block.config or {}).get("writeForwards") or "original") != "response":
        issues.append('HTTP write pagination requires "Continue with" to be the response.')

    max_pages = fields.get("maxPages")
    has_max_pages = max_pages is not None and bool(str(max_pages).strip())
    if has_max_pages and not _whole_number(max_pages):
        issues.append("Pagination maximum pages must be a positive whole number.")

    if ptype == "page":
        if fields.get("sizeValue") is not None and str(fields.get("sizeValue")).strip() and not _whole_number(fields.get("sizeValue")):
            issues.append("Pagination page size must be a positive whole number.")
        if fields.get("firstPage") is not None and str(fields.get("firstPage")).strip() and not _whole_number(fields.get("firstPage"), minimum=0):
            issues.append("Pagination first page must be a non-negative whole number.")
        stop = str(fields.get("stop") or "empty_response")
    elif ptype == "offset":
        if fields.get("limitValue") is not None and str(fields.get("limitValue")).strip() and not _whole_number(fields.get("limitValue")):
            issues.append("Pagination limit must be a positive whole number.")
        stop = str(fields.get("offsetStop") or "empty_response")
    elif ptype == "cursor":
        cursor_size = fields.get("cursorSizeValue", fields.get("sizeValue"))
        if cursor_size is not None and str(cursor_size).strip() and not _whole_number(cursor_size):
            issues.append("Cursor page size must be a positive whole number.")
        source = str(fields.get("cursorSource") or "body")
        if source not in {"body", "header"}:
            issues.append("Cursor source must be the response body or a response header.")
        return issues
    else:  # next_url
        source = str(fields.get("nextUrlSource") or ("body" if fields.get("urlPath") else "link_header"))
        if source not in {"body", "header", "link_header"}:
            issues.append("Next URL source must be the body, a response header, or the Link header.")
        return issues

    if stop not in _PAGINATION_STOPS:
        issues.append(f"Unsupported pagination stop condition: {stop}.")
    elif stop in {"total_count", "has_more"} and not _whole_number(max_pages):
        issues.append("Total-count and has-more stopping require a positive maximum-pages safety limit.")
    return issues


def _retention_target(rule: Any, index: int) -> Optional[tuple[str, str]]:
    """Return (plane, name) for a field-producing transform."""
    cfg = rule.config or {}
    if rule.kind == "extract":
        return "attribute", str(cfg.get("attribute") or f"extract_{index}").strip()
    if rule.kind in {"add_field", "set_from_attribute"}:
        value = str(cfg.get("field") or "").strip()
        return ("record", value) if value else None
    if rule.kind == "rename":
        value = str(cfg.get("to") or "").strip()
        return ("record", value) if value else None
    return None


def _contains_placeholder(value: Any, name: str) -> bool:
    if isinstance(value, str):
        return f"${{{name}}}" in value
    if isinstance(value, dict):
        return any(_contains_placeholder(k, name) or _contains_placeholder(v, name) for k, v in value.items())
    if isinstance(value, list):
        return any(_contains_placeholder(v, name) for v in value)
    return False


def _descendant_blocks(flow: Flow, block: FlowBlock) -> List[FlowBlock]:
    """Return descendants in graph order for temporary-key safety checks."""
    by_parent: Dict[Optional[str], List[FlowBlock]] = {}
    for candidate in flow.blocks:
        by_parent.setdefault(candidate.parentId, []).append(candidate)
    result: List[FlowBlock] = []
    queue = list(by_parent.get(block.id, []))
    while queue:
        current = queue.pop(0)
        result.append(current)
        queue.extend(by_parent.get(current.id, []))
    return result


def _block_references_key(block: FlowBlock, name: str, plane: str) -> bool:
    """Best-effort reference check for values crossing a block boundary."""
    if _contains_placeholder(block.config or {}, name):
        return True
    for condition in (block.branch.rules if block.branch and block.branch.rules else []):
        if plane == "attribute" and condition.field == name:
            return True
        if plane == "record" and condition.field == name:
            return True
    for rule in block.transforms:
        cfg = rule.config or {}
        if rule.kind in {"add_field", "set_from_attribute"} and _contains_placeholder(cfg.get("value"), name):
            return True
        if rule.kind == "set_from_attribute" and plane == "attribute" and cfg.get("attribute") == name:
            return True
        if rule.kind in {"coerce", "remove_field"} and plane == "record" and cfg.get("field") == name:
            return True
        if rule.kind == "rename" and plane == "record" and cfg.get("from") == name:
            return True
        if rule.kind == "dedup" and plane == "record":
            if name in (cfg.get("identityFields") or []) or name in (cfg.get("excludedFields") or []):
                return True
    return False


def dedup_stream_not_per_record_reason(flow: Flow, block: FlowBlock) -> Optional[str]:
    """Dedup (DetectDuplicate + the hash script) is per-FlowFile, so it is
    only sound when the stream feeding the block is one-record-per-FlowFile.
    Returns a human-readable reason when that guarantee is broken, else None.

    The guarantee is established at the stream's SOURCE and preserved by
    everything in between (transforms/routing/write-passthrough never merge
    FlowFiles), so this walks the parent chain up to the nearest
    record-producing source:
      - http read (or http write forwarding its response): per-record only
        when `split` is true (the default) — `split: false` carries the whole
        response page in one FlowFile.
      - jdbc read: always per-record (`SplitRecord`, 1 record per split).
      - kafka read: always per-record for json/csv/xml (`SplitRecord`); raw
        branches cannot host transforms at all (R8), so dedup is already
        excluded there by the raw-branch check.
    """
    by_id = {b.id: b for b in flow.blocks}
    cur: Optional[FlowBlock] = block
    seen: set = set()
    while cur is not None and cur.id not in seen:
        seen.add(cur.id)
        cfg = cur.config or {}
        if cur.adapter == "http":
            if cur.mode == "read":
                if cfg.get("split") is False and str(cfg.get("recordPath") or "$").strip() != "$":
                    return f'"{cur.name}" reads whole responses (split is off), so one FlowFile carries many records'
                return None
            if cur.mode == "write" and str(cfg.get("writeForwards") or "original") == "response":
                if cfg.get("split") is False and str(cfg.get("recordPath") or "$").strip() != "$":
                    return f'"{cur.name}" forwards whole responses (split is off), so one FlowFile carries many records'
                return None
            # http write forwarding the original / http lookup: granularity
            # comes from the parent stream — keep walking up.
        elif cur.adapter == "jdbc" and cur.mode == "read":
            return None
        elif cur.adapter == "kafka" and cur.mode == "read":
            return None
        cur = by_id.get(cur.parentId) if cur.parentId else None
    return None


def validate_block(
    flow: Flow,
    block: FlowBlock,
    services: List[AppService],
    schemas: List[ApprovedSchema],
    gateway: Optional[GatewaySnapshot] = None,
) -> List[ValidationIssue]:
    if gateway is None:
        gateway = GatewaySnapshot()
    issues: List[ValidationIssue] = []

    def at(message: str) -> None:
        issues.append(ValidationIssue(blockId=block.id, where=block.name, message=message))

    if not block.name.strip():
        at("Block needs a name.")

    needs_service = block.adapter in ("http", "jdbc", "kafka_kc", "kc")
    if needs_service and not block.serviceId:
        at("Select a service — hosts and credentials always come from a saved service.")
    if block.serviceId:
        svc = next((s for s in services if s.id == block.serviceId), None)
        if not svc:
            at("The selected service no longer exists.")
        elif svc.retired:
            at(f'Service "{svc.name}" is retired — action required: select a replacement.')

    if _is_write(block) and not (block.entity or "").strip():
        at("No write without an entity, ever — set the entity label.")
    # kc is a write in spec terms but not in `_is_write()` terms: widening that
    # predicate would leak kc into service-type mapping, transform hosting and
    # legality. The entity requirement is therefore its own targeted check.
    if block.adapter == "kc" and not (block.entity or "").strip():
        at("No write without an entity, ever — this subscription delivers records, so it needs an entity label.")

    if block.adapter == "http":
        path = (block.config or {}).get("path") or ""
        if not path:
            at("Set the request path.")
        elif str(path).strip().lower().startswith(("http://", "https://")):
            # Mirrors frontend httpPathIssue() — the bound service supplies the
            # base URL; a full URL here compiles to base+url concatenation and
            # an invalid InvokeHTTP target (user-reported live failure).
            at("HTTP path must be a path (the service provides the base URL) — got a full URL.")
        missing = _unresolved_placeholders(flow, block)
        if missing:
            at(f"Unresolved ${{...}} values: {', '.join(missing)} — extract them upstream or define a flow variable.")
        for refusal in gateway_refusals(block, gateway, services):
            at(refusal)
        for refusal in _pagination_refusals(block):
            at(refusal)
    if block.adapter == "jdbc" and not (block.config or {}).get("table"):
        at("Pick a table.")
    if block.adapter == "kafka" and block.mode == "read" and not block.parentId and not (block.config or {}).get("topicName"):
        at("Pick a topic to consume.")
    # The override is legal on the whole kafka family (R7), so the collision
    # check has to cover kafka_kc too -- its derived name is overridable now.
    if _is_kafka_family_write(block) and block.topicOverride and not override_matches_derived(flow, block):
        collision = topic_name_collision(derive_topic_name(flow, block).value)
        if collision:
            at(collision)
    if block.adapter == "kafka_kc":
        if not any(s.flowId == flow.id and s.blockId == block.id for s in schemas):
            at("Schema ceremony required — the flow cannot deploy until this write's schema is approved.")
        if not (block.config or {}).get("sinkServiceId") and not block.serviceId:
            at("Select the sink destination service.")
    if block.adapter == "kc" and not (block.config or {}).get("attachTopicId"):
        at("Attach the subscription to a topic.")
    for refusal in _sink_config_refusals(block):
        at(refusal)

    # A half-written rule matches nothing, so the branch silently receives no
    # records. NO rules is legal and means "everything" -- only an unfinished
    # rule is an issue.
    if _branch_incomplete(block.branch):
        branch_name = block.branch.name if block.branch and block.branch.name else block.name
        at(
            f'Branch "{branch_name}" has an unfinished rule — it matches no records until every rule has a field, an operator and a value.'
        )

    # Transforms sanity.
    dedup_positions = [i for i, t in enumerate(block.transforms) if t.kind == "dedup"]
    if dedup_positions and dedup_positions[0] != len(block.transforms) - 1:
        at("Dedup must be the last transformation.")
    if len(dedup_positions) > 1:
        at("Only one dedup transformation is allowed per block.")
    if not hosts_transforms(flow, block) and len(block.transforms) > 0 and block.adapter != "kc":
        at("R8 — this branch carries raw bytes; transformations are not available here.")

    for i in dedup_positions:
        cfg = block.transforms[i].config or {}
        identity_fields = cfg.get("identityFields")
        if not isinstance(identity_fields, list) or not any(isinstance(f, str) and f.strip() for f in identity_fields):
            at("Dedup needs at least one identity field.")
        window = cfg.get("windowHours", DEDUP_DEFAULT_WINDOW_HOURS)
        if window is None:
            window = DEDUP_DEFAULT_WINDOW_HOURS
        valid_number = isinstance(window, (int, float)) and not isinstance(window, bool)
        if not valid_number or not (DEDUP_MIN_WINDOW_HOURS <= window <= DEDUP_MAX_WINDOW_HOURS):
            at("Dedup window must be between 1 minute and 365 days (1/60-8760 hours).")

    # A temporary key is still available to every operation in this block,
    # then is removed from each outbound copy.  Validate the small persisted
    # contract here so malformed values never reach the compiler silently.
    temporary_targets: List[tuple[str, str]] = []
    for index, rule in enumerate(block.transforms):
        cfg = rule.config or {}
        retention = str(cfg.get("retention", "flow")).strip().lower()
        if retention not in {"flow", "block"}:
            at(f"Transform {index + 1} has an invalid retention value; use 'flow' or 'block'.")
            continue
        if retention != "block" or rule.kind not in _RETENTION_KINDS:
            continue
        target = _retention_target(rule, index)
        if target is None or not target[1]:
            at(f"Transform {index + 1} cannot be temporary until its output name is set.")
            continue
        if (
            target == ("attribute", "kafka.key")
            and block.adapter == "kafka"
            and block.mode == "write"
        ):
            at("The temporary attribute 'kafka.key' is consumed by this Kafka destination; keep it available through publish.")
            continue
        if target in temporary_targets:
            at(f"The temporary {target[0]} '{target[1]}' is produced by more than one transform; use one owner per key.")
        temporary_targets.append(target)

        downstream_users = [child.name or child.id for child in _descendant_blocks(flow, block)
                            if _block_references_key(child, target[1], target[0])]
        if downstream_users:
            at(
                f"Temporary {target[0]} '{target[1]}' is referenced downstream by "
                f"{', '.join(downstream_users)}; keep it downstream or recreate it before use."
            )

    if dedup_positions:
        per_record_reason = dedup_stream_not_per_record_reason(flow, block)
        if per_record_reason:
            at(
                f"Dedup requires one record per FlowFile — {per_record_reason}. "
                f"Enable per-record splitting or remove the dedup."
            )

    return issues


def validate_flow(
    flow: Flow,
    services: List[AppService],
    schemas: List[ApprovedSchema],
    gateway: Optional[GatewaySnapshot] = None,
) -> List[ValidationIssue]:
    if gateway is None:
        gateway = GatewaySnapshot()
    issues: List[ValidationIssue] = []

    def flow_level(message: str) -> None:
        issues.append(ValidationIssue(blockId=None, where="Flow settings", message=message))

    if not flow.name.strip():
        flow_level("Name the flow — the name is the first half of every derived name.")
    if len(flow.blocks) == 0 and len(flow.topics) == 0:
        flow_level("The flow is empty — add a root block.")
    if flow_has_trigger(flow):
        if not flow.cron:
            flow_level("Set the cron schedule on the root block.")
        elif not is_valid_cron(flow.cron):
            flow_level("Cron must be a 5-field expression (UTC).")
    root = root_block(flow)
    if not root and any(b.adapter != "kc" for b in flow.blocks):
        flow_level("The flow has no legal root (R2).")

    writes = [b for b in flow.blocks if _is_write(b) or b.adapter == "kc"]
    if len(flow.blocks) > 0 and len(writes) == 0:
        flow_level("Data goes nowhere — add at least one write or sink.")

    for block in flow.blocks:
        issues.extend(validate_block(flow, block, services, schemas, gateway))
    return issues


# ------------------------------------------------------------- deploy preflight


@dataclass
class PreflightCheck:
    label: str
    ok: bool
    detail: str


def deploy_preflight(
    flow: Flow,
    services: List[AppService],
    schemas: List[ApprovedSchema],
    active_connections: List[Dict[str, Any]],
    gateway: Optional[GatewaySnapshot] = None,
) -> List[PreflightCheck]:
    """Issues that specifically block Deploy (beyond plain validation).

    `active_connections` mirrors the TS `{type, name, health}[]` shape as
    plain dicts -- the router layer supplies these (e.g. from
    COLLECTIONS.connections), same convention as `gateway`.
    """
    if gateway is None:
        gateway = GatewaySnapshot()
    checks: List[PreflightCheck] = []
    validation = validate_flow(flow, services, schemas, gateway)
    checks.append(
        PreflightCheck(
            label="Configuration valid",
            ok=len(validation) == 0,
            detail="All blocks pass validation." if len(validation) == 0 else f"{len(validation)} issue(s) — run Validate for details.",
        )
    )

    needed: List[tuple] = [("nifi", "NiFi"), ("kafka", "Kafka"), ("apicurio", "Schema registry")]
    uses_connect = any(b.adapter == "kafka_kc" or b.adapter == "kc" for b in flow.blocks)
    if uses_connect:
        needed.append(("kafka_connect", "Kafka Connect"))
    uses_dedup = any(any(t.kind == "dedup" for t in b.transforms) for b in flow.blocks)
    uses_bookmarks = any(b.adapter == "jdbc" and (b.config or {}).get("incremental") is True for b in flow.blocks)
    if uses_dedup or uses_bookmarks:
        needed.append(("redis", "Redis"))
    proxied_blocks = [b for b in flow.blocks if block_proxy_id(b, services)]
    if proxied_blocks:
        needed.append(("apisix", "API gateway"))

    for type_, label in needed:
        conn = next((c for c in active_connections if c.get("type") == type_), None)
        checks.append(
            PreflightCheck(
                label=f"{label} connection active",
                ok=bool(conn) and conn.get("health") == "Healthy",
                detail=f"{conn['name']} — {conn['health']}" if conn else f"No active {label} connection.",
            )
        )

    # One pair of rows per referenced proxy: reconciliation and allowlisting
    # are separate refusals with separate owners (self-serve vs admin).
    referenced_proxy_ids: List[str] = []
    seen_pids: set = set()
    for b in proxied_blocks:
        pid = block_proxy_id(b, services)
        if pid and pid not in seen_pids:
            seen_pids.add(pid)
            referenced_proxy_ids.append(pid)

    for proxy_id in referenced_proxy_ids:
        proxy = next((p for p in gateway.proxies if p.id == proxy_id), None)
        users = [b.name for b in proxied_blocks if block_proxy_id(b, services) == proxy_id]
        if not proxy:
            checks.append(
                PreflightCheck(
                    label=f"Gateway proxy resolves — {proxy_id}",
                    ok=False,
                    detail=f"{', '.join(users)} route through a proxy that no longer exists. Pick one on the APISIX Gateway page.",
                )
            )
            continue
        checks.append(
            PreflightCheck(
                label=f"Gateway proxy reconciled — {proxy.name}",
                ok=proxy.status == "Reconciled",
                detail=(
                    f"{proxy.targetHost}:{proxy.port}{proxy.path} — reconciled onto the gateway."
                    if proxy.status == "Reconciled"
                    else f"{proxy.status}{f' — {proxy.statusDetail}' if proxy.statusDetail else ''}"
                ),
            )
        )
        allowlisted = proxy.targetHost in gateway.allowlist
        checks.append(
            PreflightCheck(
                label=f"Gateway host allowlisted — {proxy.targetHost}",
                ok=allowlisted,
                detail="Host is on the admin allowlist." if allowlisted else "Egress hosts are admin-allowlisted; this one is not on the list yet.",
            )
        )

    kafka_kc_blocks = [b for b in flow.blocks if b.adapter == "kafka_kc"]
    for b in kafka_kc_blocks:
        approved = any(s.flowId == flow.id and s.blockId == b.id for s in schemas)
        checks.append(
            PreflightCheck(
                label=f"Schema approved — {b.name}",
                ok=approved,
                detail="Approved and registered." if approved else "The schema ceremony has not been completed.",
            )
        )

    used_service_ids: List[str] = []
    seen_sids: set = set()
    for b in flow.blocks:
        if b.serviceId and b.serviceId not in seen_sids:
            seen_sids.add(b.serviceId)
            used_service_ids.append(b.serviceId)
    used_services = [s for sid in used_service_ids for s in services if s.id == sid]
    failing = [s for s in used_services if s.health == "Failed"]
    checks.append(
        PreflightCheck(
            label="Bound services reachable",
            ok=len(failing) == 0,
            detail=(
                "No services bound."
                if len(used_services) == 0
                else (f"{len(used_services)} service(s) — none failing." if len(failing) == 0 else f"Failing: {', '.join(s.name for s in failing)}.")
            ),
        )
    )

    pinned_retired = [s for sid in flow.servicePins.keys() for s in services if s.id == sid and s.retired]
    checks.append(
        PreflightCheck(
            label="No retired services",
            ok=len(pinned_retired) == 0,
            detail=(
                "All bound services are live."
                if len(pinned_retired) == 0
                else f"Action required: {', '.join(s.name for s in pinned_retired)} retired."
            ),
        )
    )

    return checks
