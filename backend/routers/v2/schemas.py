"""Schemas v2 router (T5.1-T5.4): approved schemas, library templates, the
ceremony's Approve & register, independent verify/register, granular delete
and multipart upload inference.

Contract sources read for this router:
  - frontend/src/prototype/api.ts (listSchemas/listSchemaTemplates/
    saveSchemaTemplate/createSchemaTemplate/deleteSchemaTemplate/
    approveSchema/stageCeremonyDraft, plus saveApprovedSchemaDraft/
    deleteApprovedSchemaVersion/deleteApprovedSchema).
  - frontend/src/prototype/types.ts (ApprovedSchema, SchemaApproval,
    SchemaTemplate, SchemaProvenance).
  - services/apicurio_client.py (register_schema, _normalize_apicurio_endpoints)
  - services/schema_inferencer.py (infer_avro_schema)

Design notes / deliberate deviations from the literal frontend shape:
  - `GET /` returns the COMBINED `{approved, templates}` shape (one of the two
    options the task offered) rather than two separate list endpoints. The
    frontend calls `listSchemas()` and `listSchemaTemplates()` back-to-back
    from the same Schemas page mount; a single response saves a round trip
    and there is exactly one reasonable "list everything for this page"
    endpoint to mirror. Template CRUD still gets its own sub-resource
    (`/templates`) since it has its own create/save/delete verbs.
  - The frontend's `ApprovedSchema.rawAvro` (a pretty-printed Avro JSON
    *string*) and `fields` (a simplified UI tree) are replaced here by a
    single `avro` field holding the actual parsed Avro JSON Schema (a dict).
    This router's ceremony endpoints (`/approve`, `/verify`, `/register`,
    `/infer`) all operate on real Avro schemas (fastavro-validated, and in
    `/infer`'s case produced by `schema_inferencer.infer_avro_schema`), so
    carrying the canonical dict around (instead of round-tripping through a
    pretty-printed string) avoids a lossy re-serialization step on every
    read/write. `models/adapter/schema.py`'s `ApprovedSchema` pydantic model
    (read-only for this task) is therefore treated as documentation of the
    *frontend-facing* shape, not as the literal Mongo document shape written
    by this router.
  - Approve = register, exactly like the frontend: a failed registration
    call means the approval never happened -- no document is written, only a
    "Schema approval failed" / Failed audit event, and the endpoint returns
    502.
"""

from __future__ import annotations

import csv
import io
import json
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

import httpx
import openpyxl
from fastapi import APIRouter, Body, Depends, File, Form, HTTPException, UploadFile
from motor.motor_asyncio import AsyncIOMotorDatabase

from db import get_db
from services.adapter.common import COLLECTIONS, audit, new_id, now_iso
from services.apicurio_client import _normalize_apicurio_endpoints, register_schema
from services.http_tls import tls_verify_enabled
from services.schema_inferencer import infer_avro_schema

try:
    from fastavro import parse_schema as _fastavro_parse_schema
except ImportError:  # pragma: no cover - fastavro is a hard dependency in practice
    _fastavro_parse_schema = None

router = APIRouter(prefix="/api/v2/schemas", tags=["schemas-v2"])

MAX_INFER_UPLOAD_BYTES = 5 * 1024 * 1024
_PROVENANCES = {"sample_run", "uploaded", "manual"}
_EVIDENCE_PHRASES = {
    "sample_run": "live sample run",
    "uploaded": "uploaded samples",
    "manual": "manually authored — not sample-validated",
}


# --------------------------------------------------------------------- avro


def _normalize_avro_type(t: Any) -> Any:
    """Recursively normalize one Avro type position: unions that contain
    "null" become null-first (dedup order preserved for everything else);
    record/array/map containers are walked so nested unions are normalized
    too. Leaves (primitive name strings, enum/fixed dicts) pass through."""
    if isinstance(t, list):
        has_null = False
        flat: List[Any] = []
        seen: set = set()
        for member in t:
            if member == "null":
                has_null = True
                continue
            key = member if isinstance(member, str) else json.dumps(member, sort_keys=True, default=str)
            if key in seen:
                continue
            seen.add(key)
            flat.append(_normalize_avro_type(member))
        return (["null"] + flat) if has_null else flat
    if isinstance(t, dict):
        t = dict(t)
        kind = t.get("type")
        if kind == "record":
            fields = []
            for raw_field in t.get("fields") or []:
                f = dict(raw_field) if isinstance(raw_field, dict) else {}
                f["type"] = _normalize_avro_type(f.get("type"))
                if isinstance(f["type"], list) and f["type"][:1] == ["null"]:
                    f["default"] = None
                fields.append(f)
            t["fields"] = fields
        elif kind == "array" and "items" in t:
            t["items"] = _normalize_avro_type(t["items"])
        elif kind == "map" and "values" in t:
            t["values"] = _normalize_avro_type(t["values"])
        return t
    return t


def normalize_avro_schema(schema: Any) -> Dict[str, Any]:
    """Normalize a candidate Avro schema: nullable unions become null-first
    with `default: null`, recursively. Raises ValueError when the root is
    not an Avro record -- every approved/registered/verified schema here is
    a Kafka message value, and a bare union/array/scalar root is not one."""
    if not isinstance(schema, dict):
        raise ValueError("Avro schema must be a JSON object.")
    if schema.get("type") != "record":
        raise ValueError('Root Avro schema must be a record (type: "record").')
    return _normalize_avro_type(schema)


def _validate_avro(avro: Any) -> tuple[Optional[Dict[str, Any]], List[str]]:
    """Normalize + fastavro-parse. Returns (normalized_schema_or_None, issues)."""
    try:
        normalized = normalize_avro_schema(avro)
    except ValueError as exc:
        return None, [str(exc)]
    if _fastavro_parse_schema is None:  # pragma: no cover
        return normalized, ["fastavro is not installed on the server."]
    try:
        _fastavro_parse_schema(json.loads(json.dumps(normalized)))
    except Exception as exc:  # fastavro raises a handful of different types
        return None, [f"Invalid Avro schema: {exc}"]
    return normalized, []


# ------------------------------------------------------------- serialization


def _strip_id(doc: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(doc)
    out.pop("_id", None)
    return out


_serialize_schema = _strip_id
_serialize_template = _strip_id


def _coerce_int(value: Any) -> Any:
    """Best-effort int coercion for registry version numbers. ccompat's
    `/subjects/.../versions/latest` response normally hands back an int, but
    some registries/clients round-trip it as a numeric string (e.g. "4") --
    store it as int either way so callers can compare/sort on it. Anything
    that isn't a clean integer (None, non-numeric strings) passes through
    unchanged rather than raising."""
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped and stripped.lstrip("-").isdigit():
            return int(stripped)
    return value


# ------------------------------------------------------------- apicurio glue


async def _active_apicurio_connection(db: AsyncIOMotorDatabase) -> Optional[Dict[str, Any]]:
    return await db[COLLECTIONS.connections].find_one({"type": "apicurio", "active": True})


def _apicurio_auth(conn: Dict[str, Any]) -> Dict[str, Optional[str]]:
    config = conn.get("config") or {}
    mode = str(config.get("authMode") or "none").upper()
    if mode not in ("NONE", "BASIC", "BEARER"):
        mode = "NONE"
    return {
        "auth_type": mode,
        "username": config.get("username"),
        "password": config.get("password"),
        "token": config.get("token"),
    }


def _httpx_auth_kwargs(auth: Dict[str, Optional[str]]) -> tuple[Dict[str, str], Optional[tuple]]:
    headers: Dict[str, str] = {}
    basic_auth = None
    if auth["auth_type"] == "BEARER" and auth.get("token"):
        headers["Authorization"] = f"Bearer {auth['token']}"
    elif auth["auth_type"] == "BASIC" and auth.get("username") and auth.get("password"):
        basic_auth = (auth["username"], auth["password"])
    return headers, basic_auth


def _ccompat_base(conn: Dict[str, Any]) -> str:
    url = (conn.get("config") or {}).get("url") or ""
    return _normalize_apicurio_endpoints(url)["ccompat"]


async def _check_compatibility(conn: Dict[str, Any], subject: str, avro: Dict[str, Any]) -> Dict[str, Any]:
    ccompat = _ccompat_base(conn)
    if not ccompat:
        return {"checked": False, "compatible": None, "message": "Apicurio connection has no URL configured."}
    headers, basic_auth = _httpx_auth_kwargs(_apicurio_auth(conn))
    headers["Content-Type"] = "application/vnd.schemaregistry.v1+json"
    payload = {"schema": json.dumps(avro), "schemaType": "AVRO"}
    url = f"{ccompat}/compatibility/subjects/{subject}/versions/latest"
    try:
        async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=10.0) as client:
            resp = await client.post(url, json=payload, headers=headers, auth=basic_auth)
    except Exception as exc:
        return {"checked": False, "compatible": None, "message": f"Compatibility check failed: {exc}"}
    if resp.status_code == 404:
        return {"checked": True, "compatible": True, "message": "New subject — nothing to be compatible with."}
    if resp.status_code == 200:
        try:
            data = resp.json()
        except Exception:
            data = {}
        compatible = bool(data.get("is_compatible"))
        return {
            "checked": True,
            "compatible": compatible,
            "message": "Compatible with the latest registered version." if compatible else "Not compatible with the latest registered version.",
        }
    return {
        "checked": True,
        "compatible": False,
        "message": f"Compatibility check returned {resp.status_code}: {resp.text[:200]}",
    }


async def _ccompat_delete(url: str, conn: Dict[str, Any]) -> bool:
    """Best-effort DELETE against a ccompat URL. Non-fatal: any failure just
    reports `False` (registry not confirmed deleted); the caller proceeds
    with the local delete regardless."""
    headers, basic_auth = _httpx_auth_kwargs(_apicurio_auth(conn))
    try:
        async with httpx.AsyncClient(verify=tls_verify_enabled(), timeout=10.0) as client:
            resp = await client.delete(url, headers=headers, auth=basic_auth)
        return resp.status_code == 200
    except Exception:
        return False


# ---------------------------------------------------------------- list / GET


@router.get("/")
async def list_schemas_and_templates(db: AsyncIOMotorDatabase = Depends(get_db)):
    approved = await db[COLLECTIONS.schemas].find({}, {"_id": 0}).to_list(10000)
    templates = await db[COLLECTIONS.schema_templates].find({}, {"_id": 0}).to_list(10000)
    return {
        "approved": [_serialize_schema(d) for d in approved],
        "templates": [_serialize_template(d) for d in templates],
    }


# ----------------------------------------------------------------- templates


@router.post("/templates", status_code=201)
async def create_template(body: Dict[str, Any] = Body(...), db: AsyncIOMotorDatabase = Depends(get_db)):
    name = str(body.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=422, detail="name is required.")
    avro = body.get("avro")
    if avro is not None:
        _, issues = _validate_avro(avro)
        if issues:
            raise HTTPException(status_code=422, detail="; ".join(issues))
    now = now_iso()
    tpl = {
        "id": new_id("tpl"),
        "name": name,
        "description": body.get("description"),
        "avro": avro,
        "createdAt": now,
        "updatedAt": now,
    }
    await db[COLLECTIONS.schema_templates].insert_one(tpl)
    await audit(
        db, "Schema template created", tpl["name"], status="Success",
        details="Library template — not registered, bound to no flow", object="Schema",
    )
    return _serialize_template(tpl)


@router.put("/templates/{template_id}")
async def save_template(template_id: str, body: Dict[str, Any] = Body(...), db: AsyncIOMotorDatabase = Depends(get_db)):
    existing = await db[COLLECTIONS.schema_templates].find_one({"id": template_id}, {"_id": 0})
    if not existing:
        raise HTTPException(status_code=404, detail="Template not found.")
    name = str(body.get("name", existing.get("name")) or "").strip()
    if not name:
        raise HTTPException(status_code=422, detail="name is required.")
    avro = body.get("avro", existing.get("avro"))
    if avro is not None:
        _, issues = _validate_avro(avro)
        if issues:
            raise HTTPException(status_code=422, detail="; ".join(issues))
    updated = dict(existing)
    updated["name"] = name
    updated["description"] = body.get("description", existing.get("description"))
    updated["avro"] = avro
    updated["updatedAt"] = now_iso()
    await db[COLLECTIONS.schema_templates].update_one({"id": template_id}, {"$set": updated})
    await audit(db, "Schema template saved", updated["name"], object="Schema")
    return _serialize_template(updated)


@router.delete("/templates/{template_id}")
async def delete_template(template_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    tpl = await db[COLLECTIONS.schema_templates].find_one({"id": template_id}, {"_id": 0})
    await db[COLLECTIONS.schema_templates].delete_one({"id": template_id})
    if tpl:
        await audit(
            db, "Schema template deleted", tpl.get("name", ""), status="Warning",
            details="Approvals pre-filled from it keep its name as a frozen history line", object="Schema",
        )
    return {"ok": True}


# ------------------------------------------------------------- ceremony: approve


@router.post("/approve")
async def approve_schema(body: Dict[str, Any] = Body(...), db: AsyncIOMotorDatabase = Depends(get_db)):
    flow_id = body.get("flowId")
    block_id = body.get("blockId")
    entity = str(body.get("entity") or "")
    topic = str(body.get("topic") or "")
    subject = str(body.get("subject") or (f"{topic}-value" if topic else "")).strip()
    provenance = body.get("provenance") or "manual"
    evidence = body.get("evidence")
    avro = body.get("avro")

    if not flow_id or not block_id:
        raise HTTPException(status_code=422, detail="flowId and blockId are required.")
    if not subject:
        raise HTTPException(status_code=422, detail="subject (or topic) is required.")
    if provenance not in _PROVENANCES:
        raise HTTPException(status_code=422, detail=f"provenance must be one of {sorted(_PROVENANCES)}.")

    normalized, issues = _validate_avro(avro)
    if issues:
        raise HTTPException(status_code=422, detail="; ".join(issues))

    existing = await db[COLLECTIONS.schemas].find_one({"flowId": flow_id, "blockId": block_id})

    # Re-approving an identical schema is a no-op: nothing new to register.
    if existing is not None and existing.get("avro") == normalized:
        return _serialize_schema(existing)

    conn = await _active_apicurio_connection(db)
    if not conn:
        await audit(
            db, "Schema approval failed", entity or subject, status="Failed",
            details="Registry registration failed — approval fails with it.", object="Schema",
        )
        raise HTTPException(
            status_code=502,
            detail="Registration failed: no active Apicurio connection. Approve = register — the approval fails with it.",
        )

    auth = _apicurio_auth(conn)
    result = await register_schema(
        url=(conn.get("config") or {}).get("url") or "",
        group_id="default",
        artifact_id=subject,
        avro_schema=normalized,
        auth_type=auth["auth_type"],
        username=auth["username"],
        password=auth["password"],
        token=auth["token"],
        # E6: register exactly once via ccompat — see
        # services/apicurio_client.py::_register_schema_ccompat_only's
        # docstring. Every v2 call site uses this so the app's own
        # per-approval version counter stays 1:1 with the registry's
        # ccompat version numbers (the delete-by-version path depends on it).
        ccompat_only=True,
    )
    if not result.get("ok"):
        await audit(
            db, "Schema approval failed", entity or subject, status="Failed",
            details="Registry registration failed — approval fails with it.", object="Schema",
        )
        raise HTTPException(status_code=502, detail=f"Registration failed: {result.get('error') or 'unknown error'}")

    global_id = result.get("global_id")
    if global_id is None:
        global_id = result.get("ccompat_id") or 0

    approved_at = now_iso()
    history: List[Dict[str, Any]] = [dict(a) for a in ((existing or {}).get("approvals") or [])]
    if history and not history[-1].get("supersededAt"):
        history[-1]["supersededAt"] = approved_at
    new_version = len(history) + 1
    entry: Dict[str, Any] = {
        "version": new_version,
        "approvedAt": approved_at,
        "provenance": provenance,
        "registryGlobalId": global_id,
        "avro": normalized,
        "supersededAt": None,
    }
    if evidence is not None:
        entry["evidence"] = evidence
    history.append(entry)

    schema_doc = {
        "id": existing["id"] if existing else new_id("schema"),
        "subject": subject,
        "entity": entity,
        "topic": topic,
        "flowId": flow_id,
        "blockId": block_id,
        "provenance": provenance,
        "avro": normalized,
        "registryGlobalId": global_id,
        "approvedAt": approved_at,
        "approvals": history,
        "draftAvro": None,
        "draftUpdatedAt": None,
        "createdAt": existing.get("createdAt") if existing else approved_at,
        "updatedAt": approved_at,
    }

    if existing:
        await db[COLLECTIONS.schemas].update_one({"id": schema_doc["id"]}, {"$set": schema_doc})
    else:
        await db[COLLECTIONS.schemas].insert_one(schema_doc)

    supersedes = f" (supersedes global id {history[-2]['registryGlobalId']})" if len(history) > 1 else ""
    evidence_phrase = _EVIDENCE_PHRASES.get(provenance, provenance)
    details = f"Registered as global id {global_id} · approval {new_version}{supersedes} · evidence: {evidence_phrase}"
    await audit(db, "Approved & registered", subject, status="Success", details=details, object="Schema")

    return _serialize_schema(schema_doc)


# --------------------------------------------------------------- verify / register


@router.post("/verify")
async def verify_schema(body: Dict[str, Any] = Body(...), db: AsyncIOMotorDatabase = Depends(get_db)):
    avro = body.get("avro")
    subject = body.get("subject")

    normalized, issues = _validate_avro(avro)
    ok = normalized is not None and not issues

    compatibility: Dict[str, Any] = {"checked": False, "compatible": None, "message": "No subject given — compatibility not checked."}
    if subject:
        if normalized is None:
            compatibility = {"checked": False, "compatible": None, "message": "Schema is structurally invalid — compatibility not checked."}
        else:
            conn = await _active_apicurio_connection(db)
            if not conn:
                compatibility = {"checked": False, "compatible": None, "message": "No active Apicurio connection — compatibility not checked."}
            else:
                compatibility = await _check_compatibility(conn, subject, normalized)

    await audit(
        db, "Schema verified", str(subject or "(unbound)"),
        status="Success" if ok else "Failed",
        details="; ".join(issues) if issues else "Structurally valid.",
        object="Schema",
    )
    return {"ok": ok, "issues": issues, "compatibility": compatibility}


@router.post("/register")
async def register_schema_standalone(body: Dict[str, Any] = Body(...), db: AsyncIOMotorDatabase = Depends(get_db)):
    subject = str(body.get("subject") or "").strip()
    if not subject:
        raise HTTPException(status_code=422, detail="subject is required.")
    avro = body.get("avro")
    template_id = body.get("templateId")

    normalized, issues = _validate_avro(avro)
    if issues:
        raise HTTPException(status_code=422, detail="; ".join(issues))

    conn = await _active_apicurio_connection(db)
    if not conn:
        raise HTTPException(status_code=502, detail="No active Apicurio connection — registration requires a healthy registry.")

    auth = _apicurio_auth(conn)
    result = await register_schema(
        url=(conn.get("config") or {}).get("url") or "",
        group_id="default",
        artifact_id=subject,
        avro_schema=normalized,
        auth_type=auth["auth_type"],
        username=auth["username"],
        password=auth["password"],
        token=auth["token"],
        ccompat_only=True,  # E6 — see approve_schema's call site for the rationale.
    )
    if not result.get("ok"):
        raise HTTPException(status_code=502, detail=f"Registration failed: {result.get('error') or 'unknown error'}")

    global_id = result.get("global_id")
    if global_id is None:
        global_id = result.get("ccompat_id") or 0
    version = _coerce_int(result.get("version"))
    registered_at = now_iso()

    tpl_name = None
    if template_id:
        tpl = await db[COLLECTIONS.schema_templates].find_one({"id": template_id}, {"_id": 0})
        if tpl:
            tpl_name = tpl.get("name")
            # Unconditional $set on every registration -- whether this is the
            # template's first register or a RE-register of changed/unchanged
            # avro, all four fields are refreshed from what the registry just
            # returned (ccompat hands back a new version when the schema
            # changed, the same version when it didn't -- either way we just
            # persist it, no error).
            await db[COLLECTIONS.schema_templates].update_one(
                {"id": template_id},
                {"$set": {
                    "registeredSubject": subject,
                    "registryGlobalId": global_id,
                    "registeredVersion": version,
                    "registeredAt": registered_at,
                    "updatedAt": registered_at,
                }},
            )

    details = f"Registered as global id {global_id}" + (f' · linked to template "{tpl_name}"' if tpl_name else "")
    await audit(db, "Schema registered", subject, status="Success", details=details, object="Schema")

    return {"globalId": global_id, "subject": subject, "version": version, "registeredAt": registered_at}


# -------------------------------------------------------------------- draft


@router.post("/{schema_id}/draft")
async def save_approved_schema_draft(schema_id: str, body: Dict[str, Any] = Body(...), db: AsyncIOMotorDatabase = Depends(get_db)):
    schema = await db[COLLECTIONS.schemas].find_one({"id": schema_id}, {"_id": 0})
    if not schema:
        raise HTTPException(status_code=404, detail="Approved schema not found.")
    avro = body.get("avro")
    updated_at = now_iso()
    await db[COLLECTIONS.schemas].update_one({"id": schema_id}, {"$set": {"draftAvro": avro, "draftUpdatedAt": updated_at}})
    schema["draftAvro"] = avro
    schema["draftUpdatedAt"] = updated_at
    await audit(
        db, "Schema draft saved", schema.get("subject", ""), status="Success",
        details="Edited buffer saved — not registered", object="Schema",
    )
    return _serialize_schema(schema)


# ------------------------------------------------------------------- deletes


@router.delete("/{schema_id}/versions/{version}")
async def delete_approved_schema_version(schema_id: str, version: int, db: AsyncIOMotorDatabase = Depends(get_db)):
    schema = await db[COLLECTIONS.schemas].find_one({"id": schema_id}, {"_id": 0})
    if not schema:
        raise HTTPException(status_code=404, detail="Approved schema not found.")

    approvals: List[Dict[str, Any]] = list(schema.get("approvals") or [])
    if len(approvals) <= 1:
        raise HTTPException(status_code=400, detail="This is the only remaining version — delete the entire schema instead.")

    idx = next((i for i, a in enumerate(approvals) if a.get("version") == version), None)
    if idx is None:
        raise HTTPException(status_code=404, detail="Approval not found.")
    removed = approvals.pop(idx)

    registry_deleted = False
    conn = await _active_apicurio_connection(db)
    if conn:
        ccompat = _ccompat_base(conn)
        if ccompat:
            registry_deleted = await _ccompat_delete(f"{ccompat}/subjects/{schema.get('subject', '')}/versions/{version}", conn)

    if not removed.get("supersededAt"):
        # The current approval was removed — promote the new latest.
        new_latest = approvals[-1] if approvals else None
        if new_latest is not None:
            new_latest["supersededAt"] = None
            schema["avro"] = new_latest.get("avro")
            schema["provenance"] = new_latest.get("provenance")
            schema["registryGlobalId"] = new_latest.get("registryGlobalId")
            schema["approvedAt"] = new_latest.get("approvedAt")

    schema["approvals"] = approvals
    schema["updatedAt"] = now_iso()
    await db[COLLECTIONS.schemas].update_one(
        {"id": schema_id},
        {"$set": {
            "approvals": approvals,
            "avro": schema["avro"],
            "provenance": schema["provenance"],
            "registryGlobalId": schema["registryGlobalId"],
            "approvedAt": schema["approvedAt"],
            "updatedAt": schema["updatedAt"],
        }},
    )

    await audit(
        db, "Schema version deleted", schema.get("subject", ""), status="Warning",
        details=f"Approval v{removed.get('version')} (global id {removed.get('registryGlobalId')}) removed from history",
        object="Schema",
    )

    response = _serialize_schema(schema)
    response["registryDeleted"] = registry_deleted
    return response


@router.delete("/{schema_id}")
async def delete_approved_schema(schema_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    schema = await db[COLLECTIONS.schemas].find_one({"id": schema_id}, {"_id": 0})
    if not schema:
        raise HTTPException(status_code=404, detail="Approved schema not found.")

    flow = await db[COLLECTIONS.flows].find_one({"id": schema.get("flowId")}, {"_id": 0})
    if flow and flow.get("deployedAt"):
        blocks = flow.get("blocks") or []
        referencing = any(
            isinstance(b, dict) and b.get("id") == schema.get("blockId") and b.get("adapter") == "kafka_kc"
            for b in blocks
        )
        if referencing:
            raise HTTPException(
                status_code=409,
                detail=(
                    f'Cannot delete — flow "{flow.get("name") or flow.get("id")}" is deployed and its kafka_kc '
                    "block still references this schema. Undeploy or repoint the block first."
                ),
            )

    registry_deleted = False
    conn = await _active_apicurio_connection(db)
    if conn:
        ccompat = _ccompat_base(conn)
        if ccompat:
            registry_deleted = await _ccompat_delete(f"{ccompat}/subjects/{schema.get('subject', '')}", conn)

    await db[COLLECTIONS.schemas].delete_one({"id": schema_id})
    await audit(
        db, "Schema deleted", schema.get("subject", ""), status="Warning",
        details=f"{len(schema.get('approvals') or [])} approval(s) removed, including current global id {schema.get('registryGlobalId')}",
        object="Schema",
    )
    return {"ok": True, "registryDeleted": registry_deleted}


# ------------------------------------------------------------- upload inference


def _infer_file_format(filename: str, content_type: str) -> str:
    lower = (filename or "").lower()
    if lower.endswith(".ndjson") or lower.endswith(".jsonl"):
        return "ndjson"
    if lower.endswith(".json"):
        return "json"
    if lower.endswith(".csv"):
        return "csv"
    if lower.endswith(".xlsx"):
        return "xlsx"
    if lower.endswith(".xml"):
        return "xml"
    ct = (content_type or "").lower()
    if "ndjson" in ct:
        return "ndjson"
    if "json" in ct:
        return "json"
    if "csv" in ct:
        return "csv"
    if "sheet" in ct or "excel" in ct:
        return "xlsx"
    if "xml" in ct:
        return "xml"
    raise HTTPException(status_code=422, detail=f"Unsupported file type: {filename or '(unnamed)'}")


def _xml_element_to_record(elem: ET.Element) -> Any:
    children = list(elem)
    if not children:
        text = (elem.text or "").strip()
        return text if text else None
    out: Dict[str, Any] = {}
    for child in children:
        value = _xml_element_to_record(child)
        if child.tag in out:
            existing = out[child.tag]
            if isinstance(existing, list):
                existing.append(value)
            else:
                out[child.tag] = [existing, value]
        else:
            out[child.tag] = value
    return out


def _parse_upload(raw: bytes, fmt: str) -> Any:
    if fmt == "json":
        return json.loads(raw.decode("utf-8"))
    if fmt == "ndjson":
        text = raw.decode("utf-8")
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    if fmt == "csv":
        text = raw.decode("utf-8-sig")
        return [dict(row) for row in csv.DictReader(io.StringIO(text))]
    if fmt == "xlsx":
        wb = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
        try:
            ws = wb.worksheets[0]
            rows_iter = ws.iter_rows(values_only=True)
            header = next(rows_iter, None)
            if not header:
                return []
            header_names = [str(h) if h is not None else f"col{i}" for i, h in enumerate(header)]
            records = []
            for row in rows_iter:
                records.append({header_names[i]: (row[i] if i < len(row) else None) for i in range(len(header_names))})
            return records
        finally:
            wb.close()
    if fmt == "xml":
        root = ET.fromstring(raw.decode("utf-8"))
        return [_xml_element_to_record(child) for child in list(root)]
    raise ValueError(f"Unsupported format: {fmt}")


def _as_records(value: Any) -> List[Any]:
    if isinstance(value, list):
        return list(value)
    if value is None:
        return []
    return [value]


def _resolve_record_path(value: Any, path: str) -> List[Any]:
    """Resolve a small subset of JSONPath: `$.a.b[*]` — dot-separated keys,
    each optionally suffixed with `[*]` meaning "iterate this list"."""
    cleaned = path.strip()
    if cleaned.startswith("$."):
        cleaned = cleaned[2:]
    elif cleaned.startswith("$"):
        cleaned = cleaned[1:].lstrip(".")
    segments = [s for s in cleaned.split(".") if s]

    def walk(cur: Any, segs: List[str]) -> List[Any]:
        if not segs:
            return _as_records(cur)
        seg = segs[0]
        rest = segs[1:]
        iterate = seg.endswith("[*]")
        key = seg[:-3] if iterate else seg
        if key:
            cur = cur.get(key) if isinstance(cur, dict) else None
        if iterate:
            if not isinstance(cur, list):
                return []
            out: List[Any] = []
            for item in cur:
                out.extend(walk(item, rest))
            return out
        return walk(cur, rest) if rest else _as_records(cur)

    return walk(value, segments)


def _find_array_paths(value: Any, prefix: str = "$", depth: int = 0, max_depth: int = 3, out: Optional[List[str]] = None) -> List[str]:
    """Scan for arrays-of-objects up to `max_depth`, returning candidate
    `$.a.b[*]`-style recordPath suggestions."""
    if out is None:
        out = []
    if depth > max_depth:
        return out
    if isinstance(value, dict):
        for key, val in value.items():
            _find_array_paths(val, f"{prefix}.{key}", depth + 1, max_depth, out)
    elif isinstance(value, list):
        sample = value[:5]
        if sample and all(isinstance(v, dict) for v in sample):
            path = f"{prefix}[*]"
            if path not in out:
                out.append(path)
            _find_array_paths(value[0], f"{prefix}[*]", depth + 1, max_depth, out)
    return out


@router.post("/infer")
async def infer_schema_from_upload(
    files: List[UploadFile] = File(...),
    recordPath: Optional[str] = Form(None),
    name: Optional[str] = Form(None),
    namespace: Optional[str] = Form(None),
):
    if not files:
        raise HTTPException(status_code=422, detail="At least one file is required.")

    all_samples: List[Any] = []
    per_file_structures: List[Any] = []
    notes: List[str] = []

    for upload in files:
        raw = await upload.read()
        if len(raw) > MAX_INFER_UPLOAD_BYTES:
            raise HTTPException(
                status_code=413,
                detail=f"{upload.filename or 'file'} is too large ({len(raw)} bytes). Maximum is {MAX_INFER_UPLOAD_BYTES} bytes (5 MB) per file.",
            )
        fmt = _infer_file_format(upload.filename or "", upload.content_type or "")
        try:
            parsed = _parse_upload(raw, fmt)
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=422, detail=f"Could not parse {upload.filename or 'file'} as {fmt}: {exc}")

        per_file_structures.append(parsed)
        records = _resolve_record_path(parsed, recordPath) if recordPath else _as_records(parsed)
        all_samples.extend(records)
        notes.append(f"{upload.filename or fmt}: {len(records)} record(s) parsed as {fmt}.")

    if not all_samples:
        raise HTTPException(status_code=422, detail="No records could be extracted from the uploaded file(s).")

    root_name = (name or "InferredRecord").strip() or "InferredRecord"
    root_namespace = (namespace or "com.nif").strip() or "com.nif"
    avro = infer_avro_schema(all_samples, root_name, root_namespace)

    suggested_paths: List[str] = []
    for structure in per_file_structures:
        for path in _find_array_paths(structure):
            if path not in suggested_paths:
                suggested_paths.append(path)

    field_count = len(avro.get("fields") or []) if isinstance(avro, dict) else 0
    report = {
        "recordCount": len(all_samples),
        "fieldCount": field_count,
        "notes": notes,
    }
    return {"avro": avro, "report": report, "suggestedPaths": suggested_paths}
