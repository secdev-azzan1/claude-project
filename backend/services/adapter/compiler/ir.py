"""DeploymentPlan IR — see docs/orchestration/compiler-spec.md §1.

Pure data shapes plus the small bits of shared compiler machinery
(`CompileContext`, `CompileError`, `BlockBuilder`, EL/formatting helpers) that
every `blocks_*` / `transforms` / `routing` / `connectors` / `dlq` submodule
needs. Kept in one module (rather than a separate "shared" file) so those
submodules can import from `ir` without risking a circular import with
`compile_flow` (the orchestrator imports all of them).

Nothing in this module does any I/O. `DeploymentPlan.to_dict()` /
`.to_json()` are the only serialization entry points, and they are
deterministic: dict key order follows dataclass field declaration order,
which is fixed for a given Python version, so the SAME plan always dumps to
the SAME JSON text (see `test_compiler.py`'s determinism test).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only
    from models.adapter import AppService, ApprovedSchema, Flow, FlowBlock, GatewayProxy, PlatformConnection


# --------------------------------------------------------------------- errors


class CompileError(Exception):
    """Raised for any reason `compile_flow` cannot produce a DeploymentPlan:
    failed placement validation, a missing approved schema, a missing
    required connection/service field, etc. Always carries a human-readable
    message — this is a compile-time refusal, not a crash."""


# ------------------------------------------------------------------- IR nodes


@dataclass
class ParameterSpec:
    name: str
    value: Optional[str]
    sensitive: bool = False


@dataclass
class ParameterContextSpec:
    name: str
    parameters: List[ParameterSpec] = field(default_factory=list)


@dataclass
class ProcessorSpec:
    key: str
    name: str
    type: str
    properties: Dict[str, Any] = field(default_factory=dict)
    schedulingPeriod: Optional[str] = None
    schedulingStrategy: Optional[str] = None
    executionNode: Optional[str] = None
    autoTerminate: List[str] = field(default_factory=list)
    penalty: Optional[str] = None
    runOnPrimary: Optional[bool] = None


@dataclass
class ControllerServiceSpec:
    key: str
    name: str
    type: str
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ConnectionSpec:
    """One wire inside a BlockGroup.

    `from_`/`to` reference a processor `key`, or one of the symbolic tokens:
      - "inputPort"        — this group's own input port (cross-PG source).
      - "outputPort:<id>"  — this group's output port dedicated to the child
                              block `<id>` (see the module docstring in
                              `routing.py` for why the port is per-child
                              rather than a single shared "outputPort" token).
      - "dlq"               — this group's own DLQ entry processor
                              (`dlq__meta`), resolved locally, never cross-PG.
    """

    from_: str
    to: str
    relationships: List[str] = field(default_factory=list)


@dataclass
class BlockGroup:
    blockId: str
    name: str
    processors: List[ProcessorSpec] = field(default_factory=list)
    controllerServices: List[ControllerServiceSpec] = field(default_factory=list)
    connections: List[ConnectionSpec] = field(default_factory=list)
    inputPort: bool = False
    outputPort: bool = False
    dlqPort: bool = False


@dataclass
class PortLink:
    """Parent output port -> child input port, at the flow's root group.

    `viaPort` names which of `fromBlockId`'s (possibly several) output ports
    this link attaches to — see `ConnectionSpec.to`'s "outputPort:<id>" token,
    which `viaPort` always echoes (`outputPort:<toBlockId>`).
    """

    fromBlockId: str
    toBlockId: str
    viaPort: str = ""


@dataclass
class RootGroup:
    name: str
    childGroups: List[BlockGroup] = field(default_factory=list)
    connections: List[PortLink] = field(default_factory=list)


@dataclass
class TopicSpec:
    name: str
    kind: str  # "data" | "dlq"
    ownerBlockId: Optional[str] = None


@dataclass
class ConnectorSpec:
    name: str
    config: Dict[str, Any] = field(default_factory=dict)
    ownerBlockId: str = ""


@dataclass
class ScopeMapEntry:
    adapter: str
    engine: str  # "nifi" | "connect"
    groupName: str
    components: List[str] = field(default_factory=list)


@dataclass
class DeploymentPlan:
    flowId: str
    flowToken: str
    parameterContext: ParameterContextSpec
    rootGroup: RootGroup
    topics: List[TopicSpec] = field(default_factory=list)
    connectors: List[ConnectorSpec] = field(default_factory=list)
    scopeMap: Dict[str, ScopeMapEntry] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "flowId": self.flowId,
            "flowToken": self.flowToken,
            "parameterContext": {
                "name": self.parameterContext.name,
                "parameters": [
                    {"name": p.name, "value": p.value, "sensitive": p.sensitive}
                    for p in self.parameterContext.parameters
                ],
            },
            "rootGroup": {
                "name": self.rootGroup.name,
                "childGroups": [_block_group_to_dict(g) for g in self.rootGroup.childGroups],
                "connections": [
                    {"fromBlockId": pl.fromBlockId, "toBlockId": pl.toBlockId, "viaPort": pl.viaPort}
                    for pl in self.rootGroup.connections
                ],
            },
            "topics": [{"name": t.name, "kind": t.kind, "ownerBlockId": t.ownerBlockId} for t in self.topics],
            "connectors": [
                {"name": c.name, "config": c.config, "ownerBlockId": c.ownerBlockId} for c in self.connectors
            ],
            "scopeMap": {
                block_id: {
                    "adapter": entry.adapter,
                    "engine": entry.engine,
                    "groupName": entry.groupName,
                    "components": list(entry.components),
                }
                for block_id, entry in self.scopeMap.items()
            },
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=False, ensure_ascii=False) + "\n"


def _block_group_to_dict(g: BlockGroup) -> Dict[str, Any]:
    return {
        "blockId": g.blockId,
        "name": g.name,
        "processors": [_processor_to_dict(p) for p in g.processors],
        "controllerServices": [
            {"key": cs.key, "name": cs.name, "type": cs.type, "properties": cs.properties}
            for cs in g.controllerServices
        ],
        "connections": [{"from": c.from_, "to": c.to, "relationships": list(c.relationships)} for c in g.connections],
        "inputPort": g.inputPort,
        "outputPort": g.outputPort,
        "dlqPort": g.dlqPort,
    }


def _processor_to_dict(p: ProcessorSpec) -> Dict[str, Any]:
    d: Dict[str, Any] = {
        "key": p.key,
        "name": p.name,
        "type": p.type,
        "properties": p.properties,
    }
    if p.schedulingPeriod is not None:
        d["schedulingPeriod"] = p.schedulingPeriod
    if p.schedulingStrategy is not None:
        d["schedulingStrategy"] = p.schedulingStrategy
    if p.executionNode is not None:
        d["executionNode"] = p.executionNode
    d["autoTerminate"] = list(p.autoTerminate)
    if p.penalty is not None:
        d["penalty"] = p.penalty
    if p.runOnPrimary is not None:
        d["runOnPrimary"] = p.runOnPrimary
    return d


# ------------------------------------------------------------- CompileContext


@dataclass
class CompileContext:
    """Everything `compile_flow` needs beyond the `Flow` document itself,
    assembled by the router layer WITHOUT the compiler ever touching a
    database. Real (unredacted) secret values are expected in `services[...]
    .config` / `connections[...].config` under the `_secrets.SECRET_CONFIG_KEYS`
    keys — see backend/models/adapter/_secrets.py.
    """

    services: Dict[str, "AppService"] = field(default_factory=dict)
    connections: Dict[str, "PlatformConnection"] = field(default_factory=dict)  # keyed by ConnectionType
    gateway_proxies: Dict[str, "GatewayProxy"] = field(default_factory=dict)
    approved_schemas: Dict[str, "ApprovedSchema"] = field(default_factory=dict)  # keyed by blockId

    def connection_config(self, type_: str) -> Dict[str, Any]:
        conn = self.connections.get(type_)
        return dict(conn.config or {}) if conn else {}


# ------------------------------------------------------------- BlockBuilder


class BlockBuilder:
    """Accumulates the processors/controller-services/connections for ONE
    BlockGroup while the `blocks_*`/`transforms`/`routing`/`dlq` helpers run
    against it. A thin, mutable scratchpad — nothing here is clever, it just
    saves every helper from re-threading three lists through every call."""

    def __init__(self) -> None:
        self.processors: List[ProcessorSpec] = []
        self.controller_services: List[ControllerServiceSpec] = []
        self.connections: List[ConnectionSpec] = []
        self._cs_keys: set = set()
        self._processor_keys: set = set()

    def add_processor(self, spec: ProcessorSpec) -> ProcessorSpec:
        if spec.key in self._processor_keys:
            raise CompileError(
                f"Duplicate processor key {spec.key!r} within one BlockGroup — two rules/branches "
                f"produced the same NiFi component name (e.g. two sibling branches sharing a branch "
                f"name). Give them distinct names."
            )
        self._processor_keys.add(spec.key)
        self.processors.append(spec)
        return spec

    def add_cs(self, spec: ControllerServiceSpec) -> ControllerServiceSpec:
        if spec.key in self._cs_keys:
            return spec
        self._cs_keys.add(spec.key)
        self.controller_services.append(spec)
        return spec

    def has_cs(self, key: str) -> bool:
        return key in self._cs_keys

    def link(self, from_: str, to: str, relationships: List[str]) -> None:
        self.connections.append(ConnectionSpec(from_=from_, to=to, relationships=list(relationships)))

    def auto_terminate_tail(self, key: str, relationship: str) -> None:
        """Mark `relationship` auto-terminated on the processor keyed `key` —
        used when a non-terminal block's finished output has no child to
        receive it (an incomplete-but-placement-valid flow)."""
        for p in self.processors:
            if p.key == key:
                if relationship not in p.autoTerminate:
                    p.autoTerminate.append(relationship)
                return
        raise CompileError(f"auto_terminate_tail: no processor keyed {key!r} in this group")

    def to_dlq(self, from_: str, relationship: str) -> None:
        self.link(from_, "dlq", [relationship])

    def _check_relationship_dispositions(self) -> None:
        """Compile-time invariant (review M6/M7): a relationship must have
        exactly ONE disposition on its processor — connected somewhere, or
        auto-terminated, never both. NiFi treats the two as mutually
        exclusive; a graph that declares both is at best ambiguous and at
        worst refused at apply time. Raising here keeps the whole class of
        bug out of every emitted plan."""
        connected: Dict[str, set] = {}
        for c in self.connections:
            connected.setdefault(c.from_, set()).update(c.relationships)
        for p in self.processors:
            both = set(p.autoTerminate) & connected.get(p.key, set())
            if both:
                raise CompileError(
                    f"Processor {p.key!r}: relationship(s) {sorted(both)} are both connected and "
                    f"auto-terminated — a relationship must have exactly one disposition."
                )

    def build_group(self, block_id: str, name: str, *, input_port: bool, output_port: bool) -> BlockGroup:
        self._check_relationship_dispositions()
        return BlockGroup(
            blockId=block_id,
            name=name,
            processors=self.processors,
            controllerServices=self.controller_services,
            connections=self.connections,
            inputPort=input_port,
            outputPort=output_port,
            dlqPort=True,
        )


# --------------------------------------------------------------- EL helpers


_EL_SINGLE_QUOTE_RE = re.compile(r"'")


def escape_el_literal(value: str) -> str:
    """Escape a literal for use inside a NiFi EL `'...'` string argument."""
    return _EL_SINGLE_QUOTE_RE.sub("\\'", value)


def branch_rule_el(attr: str, op: str, value: str) -> str:
    """EL mapping per compiler-spec §4."""
    v = escape_el_literal(value)
    if op == "equals":
        return f"${{{attr}:equals('{v}')}}"
    if op == "not_equals":
        return f"${{{attr}:equals('{v}'):not()}}"
    if op == "contains":
        return f"${{{attr}:contains('{v}')}}"
    if op == "starts_with":
        return f"${{{attr}:startsWith('{v}')}}"
    if op == "regex":
        return f"${{{attr}:matches('{v}')}}"
    if op == "is_empty":
        return f"${{{attr}:isEmpty()}}"
    raise CompileError(f"Unknown branch operator {op!r}")


def format_duration_hours(hours: float) -> str:
    """Render a fractional-hours window as a NiFi duration string. Whole
    hours render as "<n> hours"; sub-hour windows render as "<n> mins" so a
    1-minute TTL doesn't round away to "0 hours"."""
    minutes = hours * 60.0
    if abs(minutes - round(minutes)) < 1e-6 and round(minutes) % 60 != 0:
        return f"{round(minutes)} mins"
    return f"{round(minutes / 60.0)} hours"


def ensure_json_record_services(builder: "BlockBuilder") -> "tuple[str, str]":
    """Add (once per group) a generic JsonTreeReader/JsonRecordSetWriter pair.

    Every record-oriented processor (`UpdateRecord`, `RemoveRecordField`, and
    a plain-JSON `PublishKafka`) requires a Record Reader AND a Record Writer
    controller service — confirmed both by the reference flow export
    (`user__enrich__set_metadata`'s "Record Reader"/"Record Writer"
    properties) and by the existing `backend/services/nifi_flow_generator.py`
    (alpha) implementation's `RemoveRecordField`/`UpdateRecord` calls. One
    shared pair per block group is enough — every step in a block's chain
    reads/writes the same loosely-typed JSON record shape.

    `Output Grouping: output-oneline` (not `output-array`): every flowfile
    that ever reaches this writer already carries exactly one record — every
    read path (`http`/`kafka`/`jdbc`) splits to one-record-per-flowfile
    before any transform runs, and nothing downstream of this writer
    (`UpdateRecord`, `RemoveRecordField`, `PublishKafka`) batches records back
    together. `output-array` wrapped that single record in `[...]` anyway,
    which silently broke any bare `$.field` EvaluateJsonPath run after an
    `add_field`/`remove_field` step (both user-authored `extract` transforms
    and `routing.py`'s own branch-field-promotion step) — the field would
    resolve to `""` (Path Not Found Behavior: ignore) instead of erroring,
    which is exactly wrong for a `not_equals` exclusion rule. `output-oneline`
    writes a bare object for the single-record case, so `$.field` keeps
    working wherever it appears in the chain.
    """
    reader_key, writer_key = "cs_json_reader", "cs_json_writer"
    if not builder.has_cs(reader_key):
        builder.add_cs(
            ControllerServiceSpec(
                key=reader_key, name="json_reader", type="org.apache.nifi.json.JsonTreeReader",
                properties={"Schema Access Strategy": "infer-schema"},
            )
        )
    if not builder.has_cs(writer_key):
        builder.add_cs(
            ControllerServiceSpec(
                key=writer_key, name="json_writer", type="org.apache.nifi.json.JsonRecordSetWriter",
                properties={"Schema Access Strategy": "inherit-record-schema", "Schema Write Strategy": "no-schema",
                            "Output Grouping": "output-oneline"},
            )
        )
    return reader_key, writer_key


def apicurio_ccompat_url(raw_url: str) -> str:
    """Registry root (or an already-ccompat) URL -> ccompat base
    (`/apis/ccompat/v7`). Mirrors `services/apicurio_client.py`'s
    `_normalize_apicurio_endpoints` (not imported directly — that module
    pulls in `httpx` and does live connection testing, which this pure
    compile stage must not depend on)."""
    root = (raw_url or "").rstrip("/")
    if not root:
        return ""
    if "/apis/ccompat/" in root:
        return root
    for marker in ("/apis/registry/v3", "/apis/registry/v2"):
        idx = root.find(marker)
        if idx != -1:
            root = root[:idx]
            break
    return f"{root.rstrip('/')}/apis/ccompat/v7"


def apicurio_registry_v3_url(raw_url: str) -> str:
    """Registry root (or an already-API-suffixed) URL -> NATIVE registry API
    base (`/apis/registry/v3`). Counterpart of `apicurio_ccompat_url` for the
    Kafka Connect Apicurio `AvroConverter`, whose registry 3.x resolver
    fetches by content id against the NATIVE API only — proven live (E2b):
    `{ccompat}/ids/contentIds/<id>` 404s while `{v3}/ids/contentIds/<id>`
    200s. NiFi's ConfluentSchemaRegistry controller service keeps the ccompat
    URL (`apicurio_ccompat_url`); only Connect converters use this one."""
    root = (raw_url or "").rstrip("/")
    if not root:
        return ""
    for marker in ("/apis/ccompat/", "/apis/registry/"):
        idx = root.find(marker)
        if idx != -1:
            root = root[:idx]
            break
    return f"{root.rstrip('/')}/apis/registry/v3"


def dedupe_preserve_order(values: List[str]) -> List[str]:
    seen: set = set()
    out: List[str] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out
