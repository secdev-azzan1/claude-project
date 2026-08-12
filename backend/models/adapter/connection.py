"""Adapter-model mirror of frontend/src/prototype/types.ts (connections section).

See flow.py for the general camelCase / ISO-string-datetime / extra="allow"
conventions this package follows.
"""

from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from ._secrets import redact_config

ConnectionType = Literal["nifi", "kafka", "apicurio", "kafka_connect", "redis", "apisix"]

ConnectionHealth = Literal["Healthy", "Failed", "Not Tested"]

Reachability = Literal["Reachable", "Unreachable", "Unknown"]


class PlatformConnection(BaseModel):
    """Mirrors types.ts `PlatformConnection`. `config` holds non-secret fields
    on the frontend; server-side it may additionally hold a real secret value
    under one of the keys in `_secrets.SECRET_CONFIG_KEYS` (see that module's
    docstring for exactly which keys and where they were sourced from) — the
    top-level `hasSecret` flag stays the TS-mirrored source of truth for "is a
    secret set", while `redact()` below is what strips the actual value(s) out
    of `config` before a response ever reaches the browser.
    """

    model_config = ConfigDict(extra="allow")

    id: str
    type: ConnectionType
    name: str = ""
    active: bool = False
    health: ConnectionHealth = "Not Tested"
    reachability: Reachability = "Unknown"
    lastTestedAt: Optional[str] = None
    config: Dict[str, Any] = Field(default_factory=dict)
    hasSecret: bool = False

    def redact(self) -> Dict[str, Any]:
        """Browser-safe dict: known secret keys in `config` become `None`, and
        a `has<Key>` boolean is added at the top level for each known secret
        key (True if that key held a value). The model's own `hasSecret`
        field is left untouched — it remains the TS-shape summary flag."""
        data = self.model_dump()
        safe_config, flags = redact_config(self.config)
        data["config"] = safe_config
        data.update(flags)
        return data
