import logging
from fastapi import APIRouter, Depends
from motor.motor_asyncio import AsyncIOMotorDatabase
from datetime import datetime, timedelta

from db import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


@router.get("/summary")
async def get_summary(db: AsyncIOMotorDatabase = Depends(get_db)):
    """Return platform-wide KPI stats."""
    total_sources = await db.sources.count_documents({})
    total_flows = await db.flows.count_documents({})
    running_flows = await db.flows.count_documents({"state": "Running"})
    failed_connections = await db.connections.count_documents({"health": "Failed"})
    verified_schemas = await db.schema_artifacts.count_documents({"versions.status": "Verified"})

    # Count flow runs in last 24h
    last_24h = datetime.utcnow() - timedelta(hours=24)
    pipeline2 = [
        {"$unwind": "$runs"},
        {"$match": {"runs.started_at": {"$gte": last_24h}}},
        {"$count": "total"}
    ]
    runs_result = await db.flows.aggregate(pipeline2).to_list(1)
    recent_runs = runs_result[0]["total"] if runs_result else 0

    return {
        "totalSources": total_sources,
        "totalFlows": total_flows,
        "runningFlows": running_flows,
        "verifiedSchemas": verified_schemas,
        "failedConnections": failed_connections,
        "recentRuns": recent_runs,
    }


@router.get("/flow-summary")
async def get_flow_summary(db: AsyncIOMotorDatabase = Depends(get_db)):
    """Return a list of flows with name, state, and topic for the dashboard panel."""
    flows = await db.flows.find({}, {"_id": 0}).sort("updated_at", -1).limit(10).to_list(10)
    return [
        {
            "id": f.get("id"),
            "name": f.get("name"),
            "state": f.get("state", "Stopped"),
            "topic": f.get("kafka_topic", ""),
        }
        for f in flows
    ]
