"""Adapter-model package: Pydantic domain models that mirror
frontend/src/prototype/types.ts 1:1 (camelCase field names, ISO-string
datetimes, `extra="allow"` for round-trip safety), plus a small set of
additive server-only fields the runtime needs.

This package is intentionally import-clean: nothing here imports a NiFi or
Kafka client, so importing `models.adapter` never pulls in heavy runtime
dependencies.
"""

from .audit import AuditEvent, AuditStatus
from .connection import (
    ConnectionHealth,
    ConnectionType,
    PlatformConnection,
    Reachability,
)
from .dashboard import DashboardSummary
from .flow import (
    AdapterId,
    BlockMode,
    BranchCondition,
    BranchInfo,
    BranchMatch,
    BranchOp,
    BlockTestResult,
    Flow,
    FlowBlock,
    FlowState,
    FlowTopic,
    FlowVariable,
    TransformKind,
    TransformRule,
)
from .gateway import (
    CertProfile,
    GatewayCertProfile,
    GatewayProxy,
    GatewayReconcileStatus,
    GatewayResources,
    GatewayState,
)
from .runtime import (
    BlockMetric,
    ConnectConnectorRuntime,
    ConnectRunState,
    ConnectTaskRuntime,
    ControllerServiceRuntime,
    ControllerServiceState,
    DlqRecord,
    DriftFinding,
    DriftKind,
    DriftVerdict,
    FlowMetrics,
    FlowRuntime,
    NifiComponent,
    NifiComponentState,
    OrphanRecord,
    RuntimeOrphan,
    RuntimeProperty,
    TopicCount,
    TopicMessage,
)
from .schema import (
    ApprovedSchema,
    AvroField,
    SchemaApproval,
    SchemaProvenance,
    SchemaTemplate,
)
from .service import (
    AppService,
    HttpAuthMode,
    JdbcDialect,
    ServiceType,
    SinkKind,
)

__all__ = [
    # audit
    "AuditEvent",
    "AuditStatus",
    # connection
    "ConnectionHealth",
    "ConnectionType",
    "PlatformConnection",
    "Reachability",
    # dashboard
    "DashboardSummary",
    # flow
    "AdapterId",
    "BlockMode",
    "BranchCondition",
    "BranchInfo",
    "BranchMatch",
    "BranchOp",
    "BlockTestResult",
    "Flow",
    "FlowBlock",
    "FlowState",
    "FlowTopic",
    "FlowVariable",
    "TransformKind",
    "TransformRule",
    # gateway
    "CertProfile",
    "GatewayCertProfile",
    "GatewayProxy",
    "GatewayReconcileStatus",
    "GatewayResources",
    "GatewayState",
    # runtime
    "BlockMetric",
    "ConnectConnectorRuntime",
    "ConnectRunState",
    "ConnectTaskRuntime",
    "ControllerServiceRuntime",
    "ControllerServiceState",
    "DlqRecord",
    "DriftFinding",
    "DriftKind",
    "DriftVerdict",
    "FlowMetrics",
    "FlowRuntime",
    "NifiComponent",
    "NifiComponentState",
    "OrphanRecord",
    "RuntimeOrphan",
    "RuntimeProperty",
    "TopicCount",
    "TopicMessage",
    # schema
    "ApprovedSchema",
    "AvroField",
    "SchemaApproval",
    "SchemaProvenance",
    "SchemaTemplate",
    # service
    "AppService",
    "HttpAuthMode",
    "JdbcDialect",
    "ServiceType",
    "SinkKind",
]
