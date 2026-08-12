from pydantic import BaseModel, ConfigDict, Field, field_validator
from typing import Optional
from datetime import datetime
import uuid


class PlatformSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    default_topic_prefix: str = "nif."
    default_schema_group: str = "nif-platform"
    default_partitions: int = 6
    default_replication: int = 3
    require_schema_verification: bool = True
    auto_pause_on_drift: bool = True
    send_alerts_on_connection_failure: bool = False
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class PlatformSettingsUpdate(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "default_topic_prefix": "nif.",
                    "default_schema_group": "nif-platform",
                    "default_partitions": 6,
                    "default_replication": 3,
                    "require_schema_verification": True,
                    "auto_pause_on_drift": True,
                    "send_alerts_on_connection_failure": False,
                }
            ]
        },
    )

    default_topic_prefix: Optional[str] = None
    default_schema_group: Optional[str] = None
    default_partitions: Optional[int] = None
    default_replication: Optional[int] = None
    require_schema_verification: Optional[bool] = None
    auto_pause_on_drift: Optional[bool] = None
    send_alerts_on_connection_failure: Optional[bool] = None

    @field_validator("default_topic_prefix", "default_schema_group")
    @classmethod
    def _non_empty_optional(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value
        text = value.strip()
        if not text:
            raise ValueError("must not be empty")
        return text

    @field_validator("default_partitions", "default_replication")
    @classmethod
    def _positive_optional_int(cls, value: Optional[int]) -> Optional[int]:
        if value is not None and value < 1:
            raise ValueError("must be greater than or equal to 1")
        return value
