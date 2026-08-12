from pydantic import BaseModel, ConfigDict, Field, field_validator
from typing import Optional, List, Dict, Any
from datetime import datetime
import uuid


class FlowCreate(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "name": "DummyJson Users Flow",
                    "source_id": "paste-source-id-here",
                    "source_type": "REST API",
                    "primary_stream_id": "paste-stream-id-here",
                    "enabled": True,
                    "schema_artifact_id": None,
                    "schema_version": None,
                }
            ]
        },
    )

    name: str
    source_id: str
    source_type: str
    primary_stream_id: str
    kafka_topic: str = ""
    enabled: bool = True
    schema_artifact_id: Optional[str] = None
    schema_version: Optional[int] = None

    @field_validator("name", "source_id", "source_type", "primary_stream_id")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        text = (value or "").strip()
        if not text:
            raise ValueError("must not be empty")
        return text

    @field_validator("source_type")
    @classmethod
    def _normalize_source_type(cls, value: str) -> str:
        text = (value or "").strip().lower().replace("_", " ")
        aliases = {
            "rest api": "REST API",
            "rest": "REST API",
            "mongodb": "MongoDB",
            "mongo": "MongoDB",
            "smb": "SMB",
            "webhook": "Webhook",
            "postgresql": "PostgreSQL",
            "postgres": "PostgreSQL",
            "trino": "Trino",
        }
        if text not in aliases:
            raise ValueError("source_type must be one of: REST API, MongoDB, SMB, Webhook, PostgreSQL, Trino")
        return aliases[text]

    @field_validator("schema_version")
    @classmethod
    def _positive_schema_version(cls, value: Optional[int]) -> Optional[int]:
        if value is not None and value < 1:
            raise ValueError("schema_version must be greater than or equal to 1")
        return value


class FlowRun(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    started_at: datetime = Field(default_factory=datetime.utcnow)
    ended_at: Optional[datetime] = None
    duration_seconds: Optional[int] = None
    records_processed: int = 0
    kafka_start_count: Optional[int] = None
    status: str = "Running"  # Running | Success | Failed
    error: Optional[str] = None


class Flow(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    source_id: str
    source_type: str
    primary_stream_id: str
    kafka_topic: str = ""
    state: str = "Stopped"  # Starting | Running | Stopping | Stopped | Degraded | Error
    enabled: bool = True
    schema_artifact_id: Optional[str] = None
    schema_version: Optional[int] = None
    nifi_process_group_id: Optional[str] = None
    nifi_connection_id: Optional[str] = None
    nifi_instance_fingerprint: Optional[str] = None
    kafka_connection_id: Optional[str] = None
    kafka_cluster_id: Optional[str] = None
    schema_inference_job_id: Optional[str] = None
    last_run: Optional[datetime] = None
    total_records: int = 0
    total_errors: int = 0
    runs: List[FlowRun] = []
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
