from typing import Optional


async def _normalize_active_flags(db, candidate: dict) -> None:
    """Keep both the legacy and v2 active markers aligned."""
    candidate_id = candidate.get("id")
    if not candidate_id:
        return
    await db.connections.update_one({"id": candidate_id}, {"$set": {"active": True, "is_active": True}})
    candidate["active"] = True
    candidate["is_active"] = True


async def resolve_connection(db, conn_type: str, *, required: bool = False) -> Optional[dict]:
    """Resolve the connection document for a service type.

    Prefers the active connection, accepting either the legacy `is_active`
    marker or the v2 `active` marker. If none found, repairs by promoting the
    most-recently-updated connection of that type.
    """
    doc = await db.connections.find_one(
        {"type": conn_type, "$or": [{"is_active": True}, {"active": True}]},
        {"_id": 0},
    )
    if doc is None:
        # Repair path: no active connection of this type. Self-heal by promoting a
        # deterministic candidate (most-recently-updated, else any), and fall back
        # to any connection of the type for backward compatibility with pre-is_active data.
        candidate = await db.connections.find_one({"type": conn_type}, {"_id": 0}, sort=[("updated_at", -1)])
        if candidate is not None:
            await _normalize_active_flags(db, candidate)
            doc = candidate
    elif doc.get("active") and not doc.get("is_active"):
        await _normalize_active_flags(db, doc)
    elif doc.get("is_active") and not doc.get("active"):
        await _normalize_active_flags(db, doc)
    if doc is None and required:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail=f"No {conn_type} connection configured.")
    return doc
