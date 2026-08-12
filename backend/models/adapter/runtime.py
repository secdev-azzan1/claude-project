"""Adapter-model mirror of frontend/src/prototype/types.ts (runtime section):
FlowRuntime, NifiComponent, ConnectConnectorRuntime, DriftFinding,
OrphanRecord (TS: RuntimeOrphan), DlqRecord, FlowMetrics, TopicMessage — plus
the supporting types those interfaces reference (RuntimeProperty,
ControllerServiceRuntime, ConnectTaskRuntime, BlockMetric).

This is the read-only "what a deployed flow actually looks like on the
runtime" model — see types.ts's own section banner: a refresh only ever
touches states/live property values/timestamps, it never rewrites a flow
definition and never clears a drift finding (that's a separate, explicit,
audited force action).

See flow.py for the general camelCase / ISO-string-datetime / extra="allow"
conventions this package follows.
"""

from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

# ------------------------------------------------------------- NiFi runtime

NifiComponentState = Literal["RUNNING", "STOPPED", "DISABLED", "INVALID"]

ControllerServiceState = Literal["ENABLED", "DISABLED", "INVALID"]


class RuntimeProperty(BaseModel):
    """A property as it comes back from a live NiFi descriptor. `value` is
    `None` when the descriptor is sensitive — sensitive values are never
    shipped to the browser at all, so there is no redact() here (unlike
    AppService/PlatformConnection, the value simply never gets this far)."""

    model_config = ConfigDict(extra="allow")

    name: str = ""
    value: Optional[str] = None
    sensitive: Optional[bool] = None
    divergedFrom: Optional[str] = None


class NifiComponent(BaseModel):
    """One generated NiFi component, attributed to the block that produced it
    via the compiler's runtime-scope map (see Flow.runtimeScopeMap)."""

    model_config = ConfigDict(extra="allow")

    id: str
    name: str = ""
    type: str = ""
    blockId: str = ""
    state: NifiComponentState = "STOPPED"
    invalidReasons: Optional[List[str]] = None
    properties: List[RuntimeProperty] = Field(default_factory=list)


class ControllerServiceRuntime(BaseModel):
    """A compiled NiFi controller service, generated per Application Service
    revision — never authored by the user."""

    model_config = ConfigDict(extra="allow")

    id: str
    name: str = ""
    type: str = ""
    state: ControllerServiceState = "DISABLED"
    appServiceId: Optional[str] = None
    pinnedRevision: Optional[int] = None
    scope: Literal["flow", "shared"] = "flow"
    sharedWith: Optional[List[str]] = None
    properties: List[RuntimeProperty] = Field(default_factory=list)


# ---------------------------------------------------------- Connect runtime

ConnectRunState = Literal["RUNNING", "PAUSED", "FAILED", "UNASSIGNED", "RESTARTING"]


class ConnectTaskRuntime(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: int = 0
    state: ConnectRunState = "UNASSIGNED"
    workerId: str = ""
    lastErrorTrace: Optional[str] = None


class ConnectConnectorRuntime(BaseModel):
    """Owning kc / kafka_kc block's live Connect connector state. Counters here
    are deliberately separate from NiFi FlowFile counts."""

    model_config = ConfigDict(extra="allow")

    name: str = ""
    blockId: str = ""
    connectorClass: str = ""
    state: ConnectRunState = "UNASSIGNED"
    workerId: str = ""
    tasks: List[ConnectTaskRuntime] = Field(default_factory=list)
    recordsSent: int = 0
    recordsFailed: int = 0
    lastErrorTrace: Optional[str] = None


# --------------------------------------------------------------------- drift

DriftKind = Literal[
    "process_group_missing",
    "property_edited",
    "component_state_changed",
    "connector_missing",
    "consumer_lag",
]

DriftVerdict = Literal["really_deleted", "deployed_elsewhere", "unknown", "out_of_band_edit"]


class DriftFinding(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    kind: DriftKind
    summary: str = ""
    where: str = ""
    expected: Optional[str] = None
    observed: Optional[str] = None
    verdict: DriftVerdict = "unknown"
    verdictDetail: str = ""
    observedAt: str = ""
    repairable: bool = False


class OrphanRecord(BaseModel):
    """types.ts `RuntimeOrphan` — left behind on a runtime the platform no
    longer manages. Never deleted for you."""

    model_config = ConfigDict(extra="allow")

    id: str
    kind: Literal["process_group", "connector", "controller_service"] = "process_group"
    ref: str = ""
    instance: str = ""
    recordedAt: str = ""


# Alias matching the literal types.ts interface name.
RuntimeOrphan = OrphanRecord


class FlowRuntime(BaseModel):
    """types.ts `FlowRuntime` — per-deployed-flow runtime record. Absent for
    flows that were never deployed."""

    model_config = ConfigDict(extra="allow")

    flowId: str
    nifiConnectionId: str = ""
    processGroupId: Optional[str] = None
    deployedFingerprint: str = ""
    observedFingerprint: Optional[str] = None
    reachable: bool = False
    unreachableReason: Optional[str] = None
    lastReadAt: Optional[str] = None
    components: List[NifiComponent] = Field(default_factory=list)
    controllerServices: List[ControllerServiceRuntime] = Field(default_factory=list)
    connectors: List[ConnectConnectorRuntime] = Field(default_factory=list)
    drift: List[DriftFinding] = Field(default_factory=list)
    orphans: List[OrphanRecord] = Field(default_factory=list)


# -------------------------------------------------------- DLQ / metrics / topics


class DlqRecord(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    flowId: str = ""
    ts: str = ""
    blockName: str = ""
    errorClass: str = ""
    payloadPreview: str = ""


class BlockMetric(BaseModel):
    model_config = ConfigDict(extra="allow")

    blockId: str = ""
    label: str = ""
    recordsIn: int = 0
    recordsOut: int = 0
    queued: int = 0


class TopicCount(BaseModel):
    model_config = ConfigDict(extra="allow")

    topic: str = ""
    messages: int = 0


class FlowMetrics(BaseModel):
    model_config = ConfigDict(extra="allow")

    flowId: str
    records24h: int = 0
    errors24h: int = 0
    queued: int = 0
    perBlock: List[BlockMetric] = Field(default_factory=list)
    topicCounts: List[TopicCount] = Field(default_factory=list)
    lastRunOutcome: Optional[Literal["Success", "Failed", "Skipped (overlap)"]] = None


class TopicMessage(BaseModel):
    model_config = ConfigDict(extra="allow")

    offset: int = 0
    ts: str = ""
    key: Optional[str] = None
    value: Optional[str] = None
    bytes: int = 0
