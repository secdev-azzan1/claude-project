"""Audit v2 router (`/api/v2/audit`) — T1.2b.

`GET /` mirrors frontend/src/prototype/api.ts's `listAudit(search?)`: reads
`audit_v2` (written by services/adapter/common.py's `audit()`), newest
first, with an optional case-insensitive substring search across the same
fields the frontend checks (`action`, `object`, `target`, `user`,
`details`) -- `object` is included in `_load_and_filter` for parity even
though the task brief's field list only names action/target/user/details.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from db import get_db
from services.adapter.common import COLLECTIONS

router = APIRouter(prefix="/api/v2/audit", tags=["audit-v2"])

_SEARCH_FIELDS = ("action", "object", "target", "user", "details")


@router.get("/")
async def list_audit_v2(
    limit: int = Query(default=100, ge=1, le=500),
    search: Optional[str] = Query(default=None),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> List[Dict[str, Any]]:
    docs: List[Dict[str, Any]] = await db[COLLECTIONS.audit].find({}, {"_id": 0}).to_list(None)
    docs.sort(key=lambda d: d.get("ts") or "", reverse=True)

    q = (search or "").strip().lower()
    if q:
        def _matches(d: Dict[str, Any]) -> bool:
            return any(q in str(d.get(field) or "").lower() for field in _SEARCH_FIELDS)

        docs = [d for d in docs if _matches(d)]

    return docs[:limit]
