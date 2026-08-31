from typing import Optional


def _v2_client_connection(doc: dict) -> dict:
    """Translate a v2 platform connection into the legacy client shape.

    The older runtime clients expect ``endpoint``/``auth_type`` at the top
    level, while the v2 connection router stores those values under
    ``config``.  Keep the stored v2 document untouched and only adapt the
    in-memory value returned to callers.
    """
    config = dict(doc.get("config") or {})
    connection = dict(doc)
    connection["endpoint"] = config.get("url") or ""
    connection["auth_type"] = "NONE"
    connection["username"] = config.get("username")
    connection["password"] = config.get("password")
    connection["token"] = config.get("token")
    connection["active"] = bool(doc.get("active"))
    connection["is_active"] = bool(doc.get("active"))
    return connection


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

    # The adapter/v2 application stores platform connections in
    # `connections_v2`, while several legacy runtime clients (including the
    # Kafka Connect sync page) still resolve through this helper.  Fall back
    # to the active v2 record when the legacy collection has no connection of
    # this type.  Kafka Connect currently uses no application-level auth, so
    # only its URL needs translating for the client.
    if doc is None and conn_type == "kafka_connect":
        v2_doc = await db["connections_v2"].find_one(
            {"type": conn_type, "active": True}, {"_id": 0}
        )
        if v2_doc is not None:
            doc = _v2_client_connection(v2_doc)
    if doc is None and required:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail=f"No {conn_type} connection configured.")
    return doc
