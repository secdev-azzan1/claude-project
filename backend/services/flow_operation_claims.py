from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable

from pymongo import ReturnDocument


async def claim_flow_state(
    db: Any,
    flow_id: str,
    *,
    from_states: Iterable[str],
    claimed_state: str,
) -> dict | None:
    return await db.flows.find_one_and_update(
        {"id": flow_id, "state": {"$in": list(from_states)}},
        {"$set": {"state": claimed_state, "updated_at": datetime.utcnow()}},
        return_document=ReturnDocument.AFTER,
        projection={"_id": 0},
    )


async def release_flow_state(
    db: Any,
    flow_id: str,
    *,
    claimed_state: str,
    final_state: str,
) -> bool:
    result = await db.flows.update_one(
        {"id": flow_id, "state": claimed_state},
        {"$set": {"state": final_state, "updated_at": datetime.utcnow()}},
    )
    return bool(result.modified_count)
