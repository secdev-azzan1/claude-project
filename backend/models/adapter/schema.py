"""Adapter-model mirror of frontend/src/prototype/types.ts (schema section:
AvroField, SchemaApproval, ApprovedSchema, SchemaTemplate).

See flow.py for the general camelCase / ISO-string-datetime / extra="allow"
conventions this package follows.
"""

from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

SchemaProvenance = Literal["sample_run", "uploaded", "manual"]


class AvroField(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str = ""
    type: str = "string"
    doc: Optional[str] = None
    nullable: Optional[bool] = None
    children: Optional[List["AvroField"]] = None


AvroField.model_rebuild()


class SchemaApproval(BaseModel):
    """One past (or current) approval of an approved schema. Approve = register,
    so every entry carries the registry global id it was registered under."""

    model_config = ConfigDict(extra="allow")

    version: int = 1
    approvedAt: str = ""
    provenance: SchemaProvenance = "manual"
    registryGlobalId: int = 0
    rawAvro: str = ""
    supersededAt: Optional[str] = None
    prefilledFromLabel: Optional[str] = None


class ApprovedSchema(BaseModel):
    """types.ts `ApprovedSchema`, plus additive server-only `draftAvro` /
    `draftUpdatedAt` for an in-progress, unregistered edit staged against this
    record (mirrors the frontend's separate `CeremonyDraft` concept, kept
    server-side instead of in a single global slot)."""

    model_config = ConfigDict(extra="allow")

    id: str
    subject: str = ""
    entity: str = ""
    flowId: str = ""
    blockId: str = ""
    provenance: SchemaProvenance = "manual"
    fields: List[AvroField] = Field(default_factory=list)
    rawAvro: str = ""
    approvedAt: str = ""
    registryGlobalId: int = 0
    approvals: List[SchemaApproval] = Field(default_factory=list)

    # ---- server-only, additive (frontend ignores unknown fields) ----
    draftAvro: Optional[str] = None
    draftUpdatedAt: Optional[str] = None


class SchemaTemplate(BaseModel):
    """types.ts `SchemaTemplate` — hand-authored library schema, not registered,
    not bound to a flow/block."""

    model_config = ConfigDict(extra="allow")

    id: str
    name: str = ""
    description: Optional[str] = None
    rawAvro: str = ""
    createdAt: str = ""
    updatedAt: str = ""
