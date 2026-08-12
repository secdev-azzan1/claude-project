import logging
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, Query
from motor.motor_asyncio import AsyncIOMotorDatabase
from typing import Optional
import re

from db import get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/audit", tags=["audit"])


@router.get("/")
async def get_audit_log(
    limit: int = Query(default=50, le=500),
    offset: int = Query(default=0, ge=0),
    user: Optional[str] = Query(default=None),
    search: Optional[str] = Query(default=None),
    from_date: Optional[str] = Query(default=None),
    to_date: Optional[str] = Query(default=None),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    query: dict = {}

    if user:
        query["user"] = user
    if search:
        pattern = re.escape(search)
        query["$or"] = [
            {"action": {"$regex": pattern, "$options": "i"}},
            {"target": {"$regex": pattern, "$options": "i"}},
            {"object_type": {"$regex": pattern, "$options": "i"}},
        ]
    if from_date or to_date:
        ts_filter = {}
        try:
            if from_date:
                ts_filter["$gte"] = datetime.fromisoformat(from_date)
            if to_date:
                ts_filter["$lte"] = datetime.fromisoformat(to_date)
        except Exception:
            from fastapi import HTTPException
            raise HTTPException(status_code=422, detail="from_date and to_date must be valid ISO-8601 datetime strings.")
        query["timestamp"] = ts_filter

    total = await db.audit_events.count_documents(query)
    events = await db.audit_events.find(query, {"_id": 0}).sort("timestamp", -1).skip(offset).limit(limit).to_list(limit)

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "items": [
            {
                **e,
                "timestamp": e["timestamp"].isoformat() if isinstance(e.get("timestamp"), datetime) else e.get("timestamp"),
            }
            for e in events
        ],
    }
