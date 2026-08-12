"""Adapter-model mirror of frontend/src/prototype/types.ts (gateway section:
GatewayCertProfile, GatewayProxy, GatewayResources).

See flow.py for the general camelCase / ISO-string-datetime / extra="allow"
conventions this package follows.

Field-name note: the task brief described GatewayProxy as having a `host`
field; types.ts (the stated source of truth, read in full) actually names it
`targetHost` (see the doc comment on GatewayProxy: "must be admin-allowlisted
to deploy"), and every consumer in the frontend (api.ts's
`reconciledFields`/`saveGatewayProxy`/`testGatewayProxy`, pages/Apisix.tsx,
ServiceFormFields.tsx's ProxyField) reads/writes `targetHost`. Implemented as
`targetHost` here to match the real wire shape.

Class-name note: the task brief asked for a class named `CertProfile`;
types.ts names the interface `GatewayCertProfile`. Since only the JSON field
names (not the Python class name) need to match the TS shape 1:1, this module
defines the class as `CertProfile` per the brief, with a `GatewayCertProfile`
alias for anyone importing by the types.ts name. Likewise the brief calls the
allowlist holder "GatewayState"; types.ts calls it `GatewayResources` — both
names are exported, `GatewayState` as an alias of `GatewayResources`.
"""

from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

GatewayReconcileStatus = Literal["Reconciled", "Pending", "Failed"]


class CertProfile(BaseModel):
    """types.ts `GatewayCertProfile`: id, name, subject, expiresAt, refCount."""

    model_config = ConfigDict(extra="allow")

    id: str
    name: str = ""
    subject: str = ""
    expiresAt: str = ""
    refCount: int = 0


# Alias matching the literal types.ts interface name.
GatewayCertProfile = CertProfile


class GatewayProxy(BaseModel):
    """types.ts `GatewayProxy` — one named APISIX proxy in the catalog. An http
    block references one by id via `config.proxyId`."""

    model_config = ConfigDict(extra="allow")

    id: str
    name: str = ""
    description: Optional[str] = None
    targetHost: str = ""
    port: int = 0
    sni: Optional[str] = None
    connectTimeoutMs: Optional[int] = None
    readTimeoutMs: Optional[int] = None
    path: str = ""
    methods: List[str] = Field(default_factory=list)
    certProfileId: Optional[str] = None
    status: GatewayReconcileStatus = "Pending"
    statusDetail: Optional[str] = None
    createdAt: str = ""
    updatedAt: str = ""


class GatewayResources(BaseModel):
    """types.ts `GatewayResources`: cert profiles + host allowlist. Proxies are
    a sibling top-level collection (`GatewayProxy`), not nested here."""

    model_config = ConfigDict(extra="allow")

    certProfiles: List[CertProfile] = Field(default_factory=list)
    allowlist: List[str] = Field(default_factory=list)


# Alias matching the task brief's "GatewayState" naming.
GatewayState = GatewayResources
