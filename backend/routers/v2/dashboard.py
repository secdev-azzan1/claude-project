"""Dashboard v2 router (`/api/v2/dashboard`) — T1.2b.

`GET /summary` computes the same `DashboardSummary` shape as
frontend/src/prototype/api.ts's `getDashboardSummary()`, off `COLLECTIONS`
docs instead of the frontend's in-memory `PrototypeState`. Sink-connector
counts are derived from `runtimes_v2` connector records exactly as the
frontend derives them from `state.runtimes` — with no runtime records yet
(the deployment engine hasn't landed), `sinkConnectorsRunning` and
`sinkConnectorsTotal` are honestly 0 and every kc/kafka_kc block on every
flow counts as `sinkConnectorsUndeployed`.
"""

from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter, Depends
from motor.motor_asyncio import AsyncIOMotorDatabase

from db import get_db
from services.adapter.common import COLLECTIONS

router = APIRouter(prefix="/api/v2/dashboard", tags=["dashboard-v2"])

# kc / kafka_kc are the two adapters that ever own a Kafka Connect sink
# connector (see models.adapter.flow.AdapterId).
_SINK_ADAPTERS = ("kc", "kafka_kc")


@router.get("/summary")
async def get_dashboard_summary_v2(db: AsyncIOMotorDatabase = Depends(get_db)) -> Dict[str, Any]:
    flows: List[Dict[str, Any]] = await db[COLLECTIONS.flows].find({}, {"_id": 0}).to_list(None)
    connections: List[Dict[str, Any]] = await db[COLLECTIONS.connections].find({}, {"_id": 0}).to_list(None)
    runtimes: List[Dict[str, Any]] = await db[COLLECTIONS.runtimes].find({}, {"_id": 0}).to_list(None)
    schemas: List[Dict[str, Any]] = await db[COLLECTIONS.schemas].find({}, {"_id": 0}).to_list(None)

    active_connections = [c for c in connections if c.get("active")]

    # api.ts: `const connectors = s.runtimes.flatMap((r) => r.connectors);`
    connectors: List[Dict[str, Any]] = [c for rt in runtimes for c in (rt.get("connectors") or [])]

    # api.ts: `const deployedFlowIds = new Set(s.runtimes.map((r) => r.flowId));`
    # -- driven by runtimes existing, not by flow.deployedAt.
    deployed_flow_ids = {rt.get("flowId") for rt in runtimes}

    undeployed_sinks = 0
    for f in flows:
        if f.get("id") in deployed_flow_ids:
            continue
        for b in f.get("blocks") or []:
            if b.get("adapter") in _SINK_ADAPTERS:
                undeployed_sinks += 1

    return {
        "totalFlows": len(flows),
        "runningFlows": sum(1 for f in flows if f.get("state") in ("Running", "Degraded")),
        "approvedSchemas": len(schemas),
        "connectionsHealthy": sum(1 for c in active_connections if c.get("health") == "Healthy"),
        "connectionsTotal": len(active_connections),
        "sinkConnectorsRunning": sum(1 for c in connectors if c.get("state") == "RUNNING"),
        "sinkConnectorsTotal": len(connectors),
        "sinkConnectorsUndeployed": undeployed_sinks,
    }
