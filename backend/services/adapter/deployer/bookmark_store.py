"""Small Redis adapter used only for lifecycle bookmark cleanup.

Runtime bookmark reads and writes happen inside NiFi through the Redis
controller service.  The backend still needs one narrowly-scoped operation:
when a flow is deleted, remove that flow's cursor keys.  This module keeps
that concern optional and isolated from the compiler and makes a connection
failure visible to the delete response instead of claiming the key was gone.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, Iterable

try:  # The backend can still start for installations that have not installed Redis support yet.
    import redis as redis_lib
except ImportError:  # pragma: no cover - exercised only by an incomplete deployment image
    redis_lib = None


def _delete_sync(config: Dict[str, Any], flow_id: str, block_ids: Iterable[str]) -> Dict[str, Any]:
    if redis_lib is None:
        return {"ok": False, "error": "The backend Redis client is not installed."}

    host = str(config.get("host") or "redis")
    port = int(config.get("port") or 6379)
    database = int(config.get("bookmarksDb") if config.get("bookmarksDb") is not None else 1)
    password = config.get("password")
    prefix = f"dmp:jdbc:bookmark:{flow_id}:"
    keys = [f"{prefix}{block_id}" for block_id in block_ids]
    if not keys:
        return {"ok": True, "deleted": 0}

    client = redis_lib.Redis(
        host=host,
        port=port,
        db=database,
        password=password,
        socket_connect_timeout=5,
        socket_timeout=5,
        decode_responses=False,
    )
    try:
        client.ping()
        deleted = int(client.delete(*keys))
        return {"ok": True, "deleted": deleted}
    finally:
        try:
            client.close()
        except Exception:
            pass


async def delete_flow_bookmarks(config: Dict[str, Any], flow_id: str, block_ids: Iterable[str]) -> Dict[str, Any]:
    """Delete the exact incremental bookmark keys owned by one flow."""
    try:
        return await asyncio.to_thread(_delete_sync, config, flow_id, list(block_ids))
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

