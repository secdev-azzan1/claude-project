from typing import Optional


async def resolve_connection(db, conn_type: str, *, required: bool = False) -> Optional[dict]:
    """Resolve the connection document for a service type.

    Prefers the is_active=True connection. If none found, repairs by promoting
    the most-recently-updated connection of that type.
    """
    doc = await db.connections.find_one({"type": conn_type, "is_active": True}, {"_id": 0})
    if doc is None:
        # Repair path: no active connection of this type. Self-heal by promoting a
        # deterministic candidate (most-recently-updated, else any), and fall back
        # to any connection of the type for backward compatibility with pre-is_active data.
        candidate = await db.connections.find_one({"type": conn_type}, {"_id": 0}, sort=[("updated_at", -1)])
        if candidate is not None:
            candidate_id = candidate.get("id")
            if candidate_id:
                await db.connections.update_one({"id": candidate_id}, {"$set": {"is_active": True}})
                candidate["is_active"] = True
            doc = candidate
    if doc is None and required:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail=f"No {conn_type} connection configured.")
    return doc
