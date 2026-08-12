"""Orphaned artifact tracking model."""
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
import uuid


class OrphanedArtifact(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    system: str            # nifi | kafka | apicurio
    external_id: str       # the abandoned handle (PG id, controller service id, topic, global id)
    kind: str              # process_group | controller_service | topic | schema
    connection_id: Optional[str] = None
    reason: str
    job_id: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
