"""Adapter-model mirror of frontend/src/prototype/types.ts (Flow section).

Field names are the literal camelCase names from types.ts — no aliasing, no
snake_case. We store camelCase in Mongo for these models, so the wire shape,
the Python attribute name and the stored document key are all identical.

Datetimes are ISO-8601 strings (`str`), never `datetime` objects, to match
the TS `string` type exactly (TS has no separate date type).

`Flow` and `FlowBlock` use `extra="allow"` so unknown keys (frontend-added
fields we haven't mirrored yet, or fields other backend code stashes on the
document) survive a parse/dump round trip instead of being silently dropped.
"""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

AdapterId = Literal["http", "jdbc", "kafka", "kafka_kc", "kc"]

BlockMode = Literal["read", "write", "lookup"]

FlowState = Literal[
    "Draft",
    "Deploying",
    "Running",
    "Paused",
    "Stopped",
    "Degraded",
    "Error",
]

TransformKind = Literal[
    "extract",
    "add_field",
    "remove_field",
    "set_from_attribute",
    "rename",
    "coerce",
    "dedup",
]

BranchOp = Literal["equals", "not_equals", "contains", "starts_with", "regex", "is_empty"]

BranchMatch = Literal["all", "any"]


class BranchCondition(BaseModel):
    model_config = ConfigDict(extra="allow")

    field: str = ""
    op: BranchOp = "equals"
    value: str = ""


class TransformRule(BaseModel):
    """`config` is a loosely typed per-kind payload — see types.ts TransformRule
    doc comment for the shape each `kind` expects (extract/add_field/... )."""

    model_config = ConfigDict(extra="allow")

    id: str = ""
    kind: TransformKind
    config: Dict[str, Any] = Field(default_factory=dict)


class BlockTestResult(BaseModel):
    model_config = ConfigDict(extra="allow")

    ok: bool = False
    reason: Optional[str] = None
    records: Optional[List[Any]] = None
    detectedFields: Optional[List[str]] = None
    testedAt: str = ""


class BranchInfo(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str = ""
    rules: Optional[List[BranchCondition]] = None
    match: Optional[BranchMatch] = None


class FlowBlock(BaseModel):
    """See types.ts FlowBlock doc comment for the per-adapter `config` payload
    conventions (http/jdbc/kafka/kafka_kc/kc) — `config` is intentionally
    `Dict[str, Any]`, none of those per-adapter keys are type-checked here,
    exactly as on the frontend."""

    model_config = ConfigDict(extra="allow")

    id: str
    adapter: AdapterId
    mode: Optional[BlockMode] = None
    name: str = ""
    parentId: Optional[str] = None
    branch: Optional[BranchInfo] = None
    serviceId: Optional[str] = None
    entity: Optional[str] = None
    config: Dict[str, Any] = Field(default_factory=dict)
    transforms: List[TransformRule] = Field(default_factory=list)
    topicOverride: Optional[str] = None
    testResult: Optional[BlockTestResult] = None


class FlowTopic(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    kind: Literal["adopted", "materialized"] = "materialized"
    name: str = ""
    sealed: bool = False
    writerBlockId: Optional[str] = None
    backlogEstimate: Optional[int] = None


class FlowVariable(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str = ""
    value: str = ""
    secret: bool = False


class Flow(BaseModel):
    """Mirrors types.ts `Flow` exactly for every field the frontend reads or
    writes, plus additive server-only fields the runtime needs. The extra
    fields are additive-only (optional, default None) so they never change
    the shape the frontend already understands — it simply ignores unknown
    fields, per the project convention.

    Server-only additions:
      - runtimeScopeMap: the compiler's block-id -> generated-component-id(s)
        map (the "runtime-scope map" referenced in types.ts's NifiComponent
        doc comment) — needed to resolve drift/orphans back to a block.
      - nifiProcessGroupId: convenience denormalization of the current
        deployment's NiFi process group id (also tracked per-deploy on
        FlowRuntime.processGroupId; kept here too for fast lookups without a
        runtime-collection join).
      - provenance: connection fingerprints and other deploy-time provenance
        facts (e.g. which NiFi/Kafka/Registry connection ids and instance
        fingerprints this flow was last compiled against).
    """

    model_config = ConfigDict(extra="allow")

    id: str
    name: str = ""
    description: Optional[str] = None
    state: FlowState = "Draft"
    enabled: bool = False
    cron: Optional[str] = None
    blocks: List[FlowBlock] = Field(default_factory=list)
    topics: List[FlowTopic] = Field(default_factory=list)
    variables: List[FlowVariable] = Field(default_factory=list)
    servicePins: Dict[str, int] = Field(default_factory=dict)
    drift: Optional[str] = None
    deployedAt: Optional[str] = None
    lastRunAt: Optional[str] = None
    createdAt: str = ""
    updatedAt: str = ""

    # ---- server-only, additive (frontend ignores unknown fields) ----
    runtimeScopeMap: Optional[Dict[str, Any]] = None
    nifiProcessGroupId: Optional[str] = None
    provenance: Optional[Dict[str, Any]] = None
