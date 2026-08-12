from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
import uuid


class AuditEvent(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    user: str = "admin"
    action: str
    object_type: str  # Connection | Source | Schema | Flow | Settings
    target: str
    status: str  # Success | Failed | Info
    details: Optional[str] = None


class AuditEventCreate(BaseModel):
    action: str
    object_type: str
    target: str
    status: str
    details: Optional[str] = None
    user: str = "admin"
