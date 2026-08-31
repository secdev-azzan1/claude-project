"""Persisted, user-managed Kafka Connect sync definitions.

Unlike the generated connector records owned by an Iceberg sink, these are
explicit reusable syncs a user can configure and attach to a flow terminal.
The connector configuration is kept as a flat Kafka Connect property map so
the UI can support arbitrary installed plugins without hard-coding every
plugin's schema.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from services.adapter.common import new_id


class KafkaConnectSync(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str = Field(default_factory=lambda: new_id("sync"))
    name: str
    description: str = ""
    direction: Literal["sink", "source"] = "sink"
    connector_class: str = ""
    connector_name: str = ""
    config: Dict[str, Any] = Field(default_factory=dict)
    enabled: bool = False
    retired: bool = False
    remote_present: bool = False
    remote_config_hash: Optional[str] = None
    linked_flow_id: Optional[str] = None
    linked_block_id: Optional[str] = None
    last_status: Optional[Dict[str, Any]] = None
    last_error: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
