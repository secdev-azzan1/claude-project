from pydantic import BaseModel, ConfigDict, Field
from typing import Optional, Dict, Any
from datetime import datetime
import uuid


ICEBERG_SINK_DISPLAY_STATES = ("Running", "Paused", "Failed", "Degraded", "Disabled", "Not Deployed", "Unknown")


class IcebergSink(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    flow_id: str
    stream_id: str
    topic: str
    connector_name: str
    table_name: str
    # Runtime desired state; seeded from designer intent then owned by enable/disable.
    enabled: bool = False
    # Denormalized copy of designer intent AT LAST SYNC (change detector).
    # Only `enabled` is authoritative; table/config values are platform-derived.
    overrides: Dict[str, Any] = Field(default_factory=dict)
    connect_connection_id: Optional[str] = None
    connect_cluster_fingerprint: Optional[str] = None
    iceberg_connection_id: Optional[str] = None
    kafka_connection_id: Optional[str] = None
    last_status: Optional[Dict[str, Any]] = None
    last_error: Optional[str] = None
    lifecycle_lock_job_id: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
