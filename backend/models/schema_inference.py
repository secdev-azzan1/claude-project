"""Schema Inference Job model."""
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime
import uuid


class SchemaInferenceJob(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    flow_id: str
    flow_name: str
    kafka_topic: str
    inference_topic: str
    entity_stream_id: Optional[str] = None  # Which entity stream this inference targets
    nifi_pg_id: Optional[str] = None
    nifi_connection_id: Optional[str] = None
    status: str = "idle"  # idle|deploying_nifi|nifi_running|collecting|inferring|complete|failed|stopped
    messages_collected: int = 0
    target_messages: int = 50
    error: Optional[str] = None
    generated_schema: Optional[Dict[str, Any]] = None
    schema_artifact_id: Optional[str] = None
    schema_version: Optional[int] = None
    worker_instance_id: Optional[str] = None
    heartbeat_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
