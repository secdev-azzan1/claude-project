"""Connection Lifecycle Job model."""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
import uuid


class ConnectionLifecycleJob(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    connection_id: str
    operation: str                      # delete | repoint | migrate | reset | activate
    strategy: Optional[str] = None      # adopt | migrate | reset (for repoint)
    status: str = "pending"             # pending | running | interrupted | failed | completed
    pace: Optional[str] = None          # all | one_by_one
    owner_instance_id: Optional[str] = None
    heartbeat_at: Optional[datetime] = None
    per_object: List[Dict[str, Any]] = Field(default_factory=list)   # [{id,type,step,status,error}]
    error: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
