"""Adapter-model mirror of the `DashboardSummary` interface returned by
frontend/src/prototype/api.ts's `getDashboardSummary()`.
"""

from pydantic import BaseModel, ConfigDict


class DashboardSummary(BaseModel):
    model_config = ConfigDict(extra="allow")

    totalFlows: int = 0
    runningFlows: int = 0
    approvedSchemas: int = 0
    connectionsHealthy: int = 0
    connectionsTotal: int = 0
    # Sink connectors on the Connect cluster whose state is RUNNING.
    sinkConnectorsRunning: int = 0
    # Every sink connector a deployed flow owns — the denominator.
    sinkConnectorsTotal: int = 0
    # kc / kafka_kc blocks in flows that were never deployed: real sinks the
    # user configured, but no connector exists for them yet, so they are
    # counted separately instead of quietly widening the denominator.
    sinkConnectorsUndeployed: int = 0
