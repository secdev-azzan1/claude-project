from pydantic import BaseModel, ConfigDict, Field, field_validator
from typing import Optional, List, Dict, Any
from datetime import datetime
import uuid
import re


class SchemaField(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    type: Any  # Can be string or complex avro type
    nullable: bool = True
    doc: Optional[str] = None
    default: Optional[Any] = None


class SchemaVersion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int
    status: str = "Draft"  # Draft | Needs Verification | Verified
    avro_schema: Optional[Dict[str, Any]] = None
    raw_avro: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    verified_at: Optional[datetime] = None
    verified_by: Optional[str] = None
    apicurio_global_id: Optional[int] = None
    apicurio_connection_id: Optional[str] = None


class SchemaArtifactCreate(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "artifact_id": "bronze.dummyjson.users__history-value",
                    "namespace": "com.nif",
                    "avro_schema": {
                        "type": "record",
                        "name": "DummyJsonUser",
                        "namespace": "com.nif",
                        "fields": [
                            {"name": "id", "type": ["null", "int"], "default": None},
                            {"name": "firstName", "type": ["null", "string"], "default": None},
                        ],
                    },
                }
            ]
        },
    )

    artifact_id: str
    stream: Optional[str] = None
    namespace: Optional[str] = "com.nif"
    avro_schema: Optional[Dict[str, Any]] = None

    @field_validator("artifact_id")
    @classmethod
    def _validate_artifact_id(cls, value: str) -> str:
        text = (value or "").strip()
        if not text:
            raise ValueError("artifact_id must not be empty")
        if not re.fullmatch(r"[A-Za-z0-9._-]+", text):
            raise ValueError("artifact_id may contain only letters, numbers, dots, underscores, and hyphens")
        return text

    @field_validator("namespace")
    @classmethod
    def _validate_namespace(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        text = value.strip()
        if not text:
            return "com.nif"
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*", text):
            raise ValueError("namespace must be a valid Avro namespace")
        return text


class SchemaArtifact(SchemaArtifactCreate):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    versions: List[SchemaVersion] = []
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class FlowSchemaLink(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    flow_id: str
    schema_artifact_id: str
    schema_version: int
    created_at: datetime = Field(default_factory=datetime.utcnow)
