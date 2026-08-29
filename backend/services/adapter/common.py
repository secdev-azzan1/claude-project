"""Shared v2 helpers: timestamps, id generation, audit logging, and the
Mongo collection-name constants for the adapter (flow-model) domain.

now_iso() / new_id() are deliberately format-compatible with the frontend
mock so ids and timestamps written by the server look identical to ones the
frontend prototype would have produced:

  - now_iso()   mirrors `const nowIso = () => new Date().toISOString();`
                (frontend/src/prototype/api.ts) -- millisecond precision,
                trailing "Z", e.g. "2026-08-13T12:34:56.789Z".
  - new_id(pfx) mirrors `uid()` (frontend/src/prototype/store.ts):
                `${prefix}-${Math.random().toString(36).slice(2, 8)}` -- a
                6-character lowercase base36 (a-z0-9) suffix. The frontend
                itself is not collision-proof (Math.random, 36^6 space); this
                port keeps the same shape/entropy rather than upgrading to a
                UUID, since ids are compared as opaque strings everywhere and
                a different format is not part of this port's contract.
"""

from __future__ import annotations

import random
import string
from datetime import datetime, timezone
from typing import Any, Dict, Optional

try:  # pragma: no cover - typing only; avoids a hard runtime dependency here.
    from motor.motor_asyncio import AsyncIOMotorDatabase
except ImportError:  # pragma: no cover
    AsyncIOMotorDatabase = Any  # type: ignore[assignment,misc]


def now_iso() -> str:
    """ISO-8601 UTC timestamp with millisecond precision and a trailing "Z",
    matching JavaScript's `Date.prototype.toISOString()` exactly (Python's
    `datetime.isoformat()` uses microseconds and no "Z", so it is not used
    directly)."""
    dt = datetime.now(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


_ID_ALPHABET = string.ascii_lowercase + string.digits


def new_id(prefix: str) -> str:
    """`${prefix}-${6-char lowercase base36 suffix}` -- see module docstring."""
    suffix = "".join(random.choices(_ID_ALPHABET, k=6))
    return f"{prefix}-{suffix}"


class COLLECTIONS:
    """Mongo collection names for the v2 (adapter) domain. Class attributes
    rather than a dict so callers get `COLLECTIONS.flows` autocomplete /
    attribute-typo errors instead of a silently-wrong string key.

    `openapi_specs` already matches the collection name in production use
    (see backend/routers/v2/openapi.py's `db.openapi_specs_v2`); the rest are
    named by the same `<name>_v2` convention.
    """

    flows = "flows_v2"
    services = "services_v2"
    connections = "connections_v2"
    gateway = "gateway_v2"  # single document: {proxies, certProfiles, allowlist}
    schemas = "schemas_v2"  # approved schemas
    schema_templates = "schema_templates_v2"
    runtimes = "runtimes_v2"
    audit = "audit_v2"
    openapi_specs = "openapi_specs_v2"
    bulk_jobs = "bulk_jobs_v2"  # background bulk flow-verb runs


async def audit(
    db: "AsyncIOMotorDatabase",
    action: str,
    target: str,
    status: str = "Success",
    user: str = "admin",
    details: Optional[str] = None,
    object: Optional[str] = None,  # noqa: A002 - matches the AuditEvent field name.
) -> Dict[str, Any]:
    """Write one document to COLLECTIONS.audit shaped exactly like
    backend/models/adapter/audit.py's `AuditEvent` (id, ts, user, action,
    object, target, status, details), mirroring
    frontend/src/prototype/api.ts's `audit()`:

        function audit(state, action, object, target, status = "Success", details?) {
          state.audit.unshift({ id: uid("a"), ts: nowIso(), user: "admin", action, object, target, status, details });
        }

    `object` is the TS `audit()`'s third positional arg (the object *type*,
    e.g. "Flow" / "Schema" / "Stream") -- kept as a keyword arg here, per the
    task's requested signature, defaulting to "" like every other string
    field on AuditEvent when not supplied.

    Returns the inserted document (including its generated `id`) so callers
    that want to reference the just-written event don't have to re-query.
    """
    event: Dict[str, Any] = {
        "id": new_id("a"),
        "ts": now_iso(),
        "user": user,
        "action": action,
        "object": object or "",
        "target": target,
        "status": status,
        "details": details,
    }
    await db[COLLECTIONS.audit].insert_one(dict(event))
    return event
