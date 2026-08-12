from datetime import datetime
from typing import Optional, Dict, Any
import uuid

from pydantic import BaseModel, ConfigDict, Field, field_validator


VALID_SERVICE_KINDS = {"kafka", "schema_registry", "mongodb", "custom"}


class NifiGlobalServiceCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    service_kind: Optional[str] = None
    nifi_type: str
    properties: Dict[str, Any] = Field(default_factory=dict)
    is_default: bool = False

    @field_validator("name", "nifi_type")
    @classmethod
    def _non_empty(cls, value: str) -> str:
        text = (value or "").strip()
        if not text:
            raise ValueError("must not be empty")
        return text

    @field_validator("service_kind")
    @classmethod
    def _normalize_kind(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        text = (value or "").strip().lower()
        aliases = {
            "kafka": "kafka",
            "schema": "schema_registry",
            "schema_registry": "schema_registry",
            "schema registry": "schema_registry",
            "apicurio": "schema_registry",
            "mongodb": "mongodb",
            "mongo": "mongodb",
            "custom": "custom",
        }
        if text not in aliases:
            raise ValueError("service_kind must be one of: kafka, schema_registry, mongodb, custom")
        return aliases[text]


class NifiGlobalServiceUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: Optional[str] = None
    properties: Optional[Dict[str, Any]] = None
    is_default: Optional[bool] = None

    @field_validator("name")
    @classmethod
    def _non_empty_optional(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        text = value.strip()
        if not text:
            raise ValueError("must not be empty")
        return text


class NifiGlobalService(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    service_kind: str
    nifi_controller_service_id: str
    nifi_process_group_id: str
    nifi_type: str
    bundle: Optional[Dict[str, Any]] = None
    properties: Dict[str, Any] = Field(default_factory=dict)
    nifi_connection_id: Optional[str] = None
    nifi_instance_fingerprint: Optional[str] = None
    is_default: bool = False
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
