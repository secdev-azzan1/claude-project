"""Adapter-model mirror of frontend/src/prototype/types.ts `AuditEvent`.

Not a re-export of backend/models/audit.py's `AuditEvent`: that model uses
`timestamp: datetime` and `object_type: str` (snake_case, real datetime
object), which is incompatible with both the camelCase-JSON decision for
this package and the actual field names the frontend reads. types.ts and
frontend/src/prototype/api.ts (see `audit()` and `listAudit()`) agree on:
`id, ts, user, action, object, target, status, details?` — that is what is
implemented here.
"""

from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict

AuditStatus = Literal["Success", "Failed", "Warning"]


class AuditEvent(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    ts: str = ""
    user: str = ""
    action: str = ""
    object: str = ""
    target: str = ""
    status: AuditStatus = "Success"
    details: Optional[str] = None
