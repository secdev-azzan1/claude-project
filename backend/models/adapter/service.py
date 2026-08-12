"""Adapter-model mirror of frontend/src/prototype/types.ts (AppService section).

See flow.py for the general camelCase / ISO-string-datetime / extra="allow"
conventions this package follows.

Field-name note: types.ts's `AppService` interface (read in full — line 279
onward) names the revision counter `revision: number`, not `rev`. The task
brief that requested this module described the field as `rev: number`, but
types.ts is the stated source of truth and the two disagree, so `revision` is
what's implemented here (matching the frontend exactly, since responses must
serialize identically to the TS shape). The server-side revision *history*
the brief also asked for is modeled separately as the additive `revisions`
field (a list, distinct from the singular `revision` counter) so neither name
collides with the TS shape.
"""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from .connection import ConnectionHealth
from ._secrets import redact_config

ServiceType = Literal["http", "database", "external_kafka", "sink_destination"]

HttpAuthMode = Literal["none", "basic", "bearer", "api_key", "oauth2", "session_token"]

JdbcDialect = Literal["postgresql", "trino", "mysql"]

SinkKind = Literal["opensearch", "iceberg_catalog"]


class AppService(BaseModel):
    """Mirrors types.ts `AppService`. `config` is non-secret on the frontend;
    server-side it may additionally carry a real secret value under one of
    the keys in `_secrets.SECRET_CONFIG_KEYS` — see that module's docstring
    for the exact key set and where each was sourced from in
    ServiceFormFields.tsx. `redact()` produces the browser-safe payload.
    """

    model_config = ConfigDict(extra="allow")

    id: str
    type: ServiceType
    name: str = ""
    revision: int = 1
    retired: bool = False
    private: Optional[bool] = None
    health: ConnectionHealth = "Not Tested"
    lastTestedAt: Optional[str] = None
    config: Dict[str, Any] = Field(default_factory=dict)
    hasSecret: bool = False
    createdAt: str = ""
    updatedAt: str = ""

    # ---- server-only, additive (frontend ignores unknown fields) ----
    # Past revision snapshots, newest last. Distinct from the TS-mirrored
    # `revision` counter above — this is the history, not the current value.
    revisions: Optional[List[Dict[str, Any]]] = None

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
