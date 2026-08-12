import logging
import uuid
import re
import json
import asyncio
import csv
import io
import math
from datetime import datetime
from typing import List, Optional, Dict, Any
from xml.etree import ElementTree as ET
from fastapi import APIRouter, HTTPException, Depends, Query, Body
from fastapi.encoders import jsonable_encoder
from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
import httpx
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId
from smb.SMBConnection import SMBConnection

from db import get_db
from models.source import Source, SourceCreate, Stream
from services import content_store
from services.application_services import resolve_source_application_service
from services.http_tls import tls_verify_enabled
from services.rest_stream_request_resolver import RestStreamRequestError, resolve_rest_stream_request

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/sources", tags=["sources"])
TEST_STREAM_PREVIEW_MAX_CHARS = 1_000_000
MONGO_TEMPLATE_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")
MONGO_TEMPLATE_KEY_PATTERN = re.compile(r'(?:^|[,{]\s*)(?:"\s*\$\{[^}]+\}\s*"|\$\{[^}]+\})\s*:', re.MULTILINE)
MONGO_QUOTED_TEMPLATE_PATTERN = re.compile(r'"[^"\n\r]*\$\{[^}]+\}[^"\n\r]*"')
PLACEHOLDER_TOKEN_PATTERN = re.compile(r"\$\{([A-Za-z0-9_.-]+)\}|\{([A-Za-z0-9_.-]+)\}")
SOURCE_SECRET_FIELDS = {
    "api_key_value",
    "bearer_token",
    "rest_password",
    "db_password",
    "db_connection_uri",
    "smb_password",
    "webhook_secret",
}


class TestStreamRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "stream_id": "users",
                    "parent_values": {},
                },
                {
                    "stream_name": "posts",
                    "parent_values": {"user_id": 1},
                },
            ]
        },
    )

    stream_id: Optional[str] = None
    stream_name: Optional[str] = None
    parent_values: Dict[str, Any] = Field(default_factory=dict)
    confirm_mutation: bool = False

    @model_validator(mode="after")
    def _require_stream_reference(self):
        if not (self.stream_id or "").strip() and not (self.stream_name or "").strip():
            raise ValueError("Either stream_id or stream_name is required")
        return self


async def _log_audit(db, action, object_type, target, status, details=None):
    from models.audit import AuditEvent
    event = AuditEvent(action=action, object_type=object_type, target=target or "", status=status, details=details)
    await db.audit_events.insert_one(event.dict())


def _to_response(s: dict) -> dict:
    s = dict(s)
    s.pop("_id", None)
    for field in SOURCE_SECRET_FIELDS:
        s[f"has_{field}"] = bool(s.get(field))
        s[field] = None
    if isinstance(s.get("designer_payload"), dict):
        s["designer_payload"] = _redact_designer_payload_secrets(s["designer_payload"])
    for field in ("created_at", "updated_at"):
        if isinstance(s.get(field), datetime):
            s[field] = s[field].isoformat()
    return s


def _redact_designer_payload_secrets(value: Any) -> Any:
    if isinstance(value, dict):
        output: Dict[str, Any] = {}
        for key, item in value.items():
            if key in SOURCE_SECRET_FIELDS:
                output[key] = None
            else:
                output[key] = _redact_designer_payload_secrets(item)
        return output
    if isinstance(value, list):
        return [_redact_designer_payload_secrets(item) for item in value]
    return value


def _preserve_existing_secrets(payload: Dict[str, Any], existing: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(payload or {})
    if out.get("connection_setup_mode") == "application_service":
        return out
    for key in SOURCE_SECRET_FIELDS:
        incoming = out.get(key)
        if (incoming is None or incoming == "") and existing.get(key):
            out[key] = existing.get(key)
    return out


async def _source_name_exists(
    db: AsyncIOMotorDatabase,
    name: str,
    exclude_id: Optional[str] = None,
) -> bool:
    normalized = (name or "").strip()
    if not normalized:
        return False
    query: Dict[str, Any] = {
        "name": {
            "$regex": f"^{re.escape(normalized)}$",
            "$options": "i",
        }
    }
    if exclude_id:
        query["id"] = {"$ne": exclude_id}
    return await db.sources.find_one(query, {"_id": 1}) is not None


def _redact_mongo_uri(uri: str) -> str:
    text = (uri or "").strip()
    if not text:
        return text
    # mongodb://user:pass@host:27017/db -> mongodb://***:***@host:27017/db
    return re.sub(r"^(mongodb(?:\+srv)?://)([^/@]+)@", r"\1***:***@", text, flags=re.IGNORECASE)


@router.get("/")
async def list_sources(db: AsyncIOMotorDatabase = Depends(get_db)):
    sources = await db.sources.find({}, {"_id": 0}).sort("created_at", -1).to_list(200)
    try:
        from services.stream_migration import migrate_source
        sources = [migrate_source(s) for s in sources]
    except Exception as exc:
        logger.warning(f"stream_migration failed in list_sources: {exc}")
    return [_to_response(s) for s in sources]


@router.get("/{source_id}")
async def get_source(source_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    source = await db.sources.find_one({"id": source_id}, {"_id": 0})
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    # Apply migration on the read path to upgrade legacy routing_rules / is_primary fields
    try:
        from services.stream_migration import migrate_source
        source = migrate_source(source)
    except Exception as exc:
        logger.warning(f"stream_migration failed for source {source_id}: {exc}")
    return _to_response(source)


SOURCE_REQUEST_BODY_OPENAPI = {
    "requestBody": {
        "content": {
            "application/json": {
                "schema": SourceCreate.model_json_schema(ref_template="#/components/schemas/{model}"),
                "examples": {
                    "REST API": {
                        "summary": "REST API source",
                        "value": SourceCreate.model_config["json_schema_extra"]["examples"][0],
                    },
                    "MongoDB": {
                        "summary": "MongoDB source",
                        "value": SourceCreate.model_config["json_schema_extra"]["examples"][1],
                    },
                    "SMB": {
                        "summary": "SMB source",
                        "value": SourceCreate.model_config["json_schema_extra"]["examples"][2],
                    },
                    "Webhook": {
                        "summary": "Webhook source",
                        "value": SourceCreate.model_config["json_schema_extra"]["examples"][3],
                    },
                },
            }
        },
        "required": True,
    }
}


@router.post(
    "/",
    status_code=201,
    summary="Create a source",
    description="Create a REST API, MongoDB, SMB, or PostgreSQL source definition with one or more streams.",
    openapi_extra=SOURCE_REQUEST_BODY_OPENAPI,
)
async def create_source(payload: Dict[str, Any] = Body(...), db: AsyncIOMotorDatabase = Depends(get_db)):
    payload = _prepare_source_payload(payload or {})
    if await _source_name_exists(db, payload.get("name", "")):
        raise HTTPException(status_code=409, detail="Source name already exists. Please choose a different name.")
    try:
        data = SourceCreate(**payload)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=jsonable_encoder(exc.errors()))
    if data.connection_setup_mode == "application_service":
        await resolve_source_application_service(db, data.dict())
    now = datetime.utcnow()
    source = Source(**data.dict())
    doc = source.dict()
    doc["created_at"] = now
    doc["updated_at"] = now
    # Ensure stream ids are set
    for stream in doc.get("streams", []):
        if not stream.get("id"):
            stream["id"] = str(uuid.uuid4())
    await db.sources.insert_one(doc)
    await _log_audit(db, "Created source", "Source", data.name, "Success")
    await content_store.sync_source(db, doc["id"])
    return _to_response(doc)


@router.put(
    "/{source_id}",
    summary="Replace a source",
    description="Fully replace a source definition. Use the same payload shape as create source.",
    openapi_extra=SOURCE_REQUEST_BODY_OPENAPI,
)
async def update_source(source_id: str, payload: Dict[str, Any] = Body(...), db: AsyncIOMotorDatabase = Depends(get_db)):
    existing = await db.sources.find_one({"id": source_id})
    if not existing:
        raise HTTPException(status_code=404, detail="Source not found")
    payload = _prepare_source_payload(payload or {})
    payload = _preserve_existing_secrets(payload, existing)
    if await _source_name_exists(db, payload.get("name", ""), exclude_id=source_id):
        raise HTTPException(status_code=409, detail="Source name already exists. Please choose a different name.")
    try:
        data = SourceCreate(**payload)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=jsonable_encoder(exc.errors()))
    if data.connection_setup_mode == "application_service":
        await resolve_source_application_service(db, data.dict())
    now = datetime.utcnow()
    updates = data.dict()
    updates["updated_at"] = now
    # Preserve streams with existing ids
    for stream in updates.get("streams", []):
        if not stream.get("id"):
            stream["id"] = str(uuid.uuid4())
    await db.sources.update_one({"id": source_id}, {"$set": updates})
    updated = await db.sources.find_one({"id": source_id}, {"_id": 0})
    await _log_audit(db, "Updated source", "Source", data.name, "Success")
    await content_store.sync_source(db, source_id)
    return _to_response(updated)


def _prepare_source_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Backward-compat cleanup for pre-change extraction-rule payloads.

    Removes deprecated extraction action fields from stream payloads and
    designer payload so strict extraction model validation can pass.
    """
    payload = dict(payload or {})
    # CamelCase compatibility for webhook payloads from UI drafts.
    if "webhookEndpointId" in payload and "webhook_endpoint_id" not in payload:
        payload["webhook_endpoint_id"] = payload.pop("webhookEndpointId")
    if "webhookSecret" in payload and "webhook_secret" not in payload:
        payload["webhook_secret"] = payload.pop("webhookSecret")
    if "webhookSignatureHeader" in payload and "webhook_signature_header" not in payload:
        payload["webhook_signature_header"] = payload.pop("webhookSignatureHeader")
    if "webhookSignatureAlgo" in payload and "webhook_signature_algo" not in payload:
        payload["webhook_signature_algo"] = payload.pop("webhookSignatureAlgo")
    if "webhookEnabled" in payload and "webhook_enabled" not in payload:
        payload["webhook_enabled"] = payload.pop("webhookEnabled")
    if "webhookContentType" in payload and "webhook_content_type" not in payload:
        payload["webhook_content_type"] = payload.pop("webhookContentType")
    streams = payload.get("streams")
    if isinstance(streams, list):
        for stream in streams:
            if not isinstance(stream, dict):
                continue
            if "webhookAttributes" in stream and isinstance(stream.get("webhookAttributes"), dict):
                stream["webhook_attributes"] = stream.pop("webhookAttributes")
            if "endpoint" in stream and "endpoint_path" not in stream:
                stream["endpoint_path"] = stream.pop("endpoint")
            if "record_path" in stream and "response_data_path" not in stream:
                stream["response_data_path"] = stream.pop("record_path")
            if "primary_stream" in stream and "is_primary" not in stream:
                stream["is_primary"] = stream.pop("primary_stream")
            rules = stream.get("extraction_rules")
            if isinstance(rules, list):
                for rule in rules:
                    if isinstance(rule, dict):
                        rule.pop("action", None)
            routing_rules = stream.get("routing_rules")
            if isinstance(routing_rules, list):
                for rule in routing_rules:
                    if not isinstance(rule, dict):
                        continue
                    # Route label is UI-only and no longer part of runtime contract.
                    rule.pop("rule_name", None)
                    # Backward-compat: legacy topic_suffix -> topic_name.
                    if (not rule.get("topic_name")) and rule.get("topic_suffix"):
                        suffix = str(rule.get("topic_suffix") or "").strip()
                        if suffix:
                            rule["topic_name"] = suffix
                    rule.pop("topic_suffix", None)

    designer = payload.get("designer_payload")
    if isinstance(designer, dict):
        d_streams = designer.get("streams")
        if isinstance(d_streams, list):
            for d_stream in d_streams:
                if not isinstance(d_stream, dict):
                    continue
                d_rules = d_stream.get("attributeExtractions")
                if isinstance(d_rules, list):
                    for d_rule in d_rules:
                        if isinstance(d_rule, dict):
                            d_rule.pop("action", None)
                            d_rule.pop("targetStreamId", None)
                            d_rule.pop("injectAs", None)
                            d_rule.pop("injectFieldName", None)
                d_routing_rules = d_stream.get("routingRules")
                if isinstance(d_routing_rules, list):
                    for d_rule in d_routing_rules:
                        if not isinstance(d_rule, dict):
                            continue
                        d_rule.pop("ruleName", None)
                        if (not d_rule.get("topicName")) and d_rule.get("topicSuffix"):
                            topic = str(d_rule.get("topicSuffix") or "").strip()
                            if topic:
                                d_rule["topicName"] = topic
                        d_rule.pop("topicSuffix", None)
    return payload


@router.delete("/{source_id}", status_code=204)
async def delete_source(source_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    existing = await db.sources.find_one({"id": source_id})
    if not existing:
        raise HTTPException(status_code=404, detail="Source not found")
    dependent_flows = await db.flows.find({"source_id": source_id}, {"_id": 0, "id": 1, "name": 1}).to_list(50)
    if dependent_flows:
        flow_names = [f.get("name") or f.get("id") for f in dependent_flows]
        raise HTTPException(
            status_code=409,
            detail=(
                "Source is used by existing flows. Delete or re-link those flows first: "
                + ", ".join(flow_names[:10])
            ),
        )
    await db.sources.delete_one({"id": source_id})
    await _log_audit(db, "Deleted source", "Source", existing.get("name", ""), "Success")
    content_store.delete_source(source_id)


@router.put("/{source_id}/streams")
async def update_streams(source_id: str, streams: List[Dict[str, Any]], db: AsyncIOMotorDatabase = Depends(get_db)):
    existing = await db.sources.find_one({"id": source_id})
    if not existing:
        raise HTTPException(status_code=404, detail="Source not found")
    # Assign ids to new streams
    normalized_streams: List[Dict[str, Any]] = []
    if existing.get("type") == "Webhook" and len(streams or []) != 1:
        raise HTTPException(status_code=422, detail="Webhook source must contain exactly one stream")
    for s in streams:
        if not s.get("id"):
            s["id"] = str(uuid.uuid4())
        if "endpoint" in s and "endpoint_path" not in s:
            s["endpoint_path"] = s.pop("endpoint")
        if "record_path" in s and "response_data_path" not in s:
            s["response_data_path"] = s.pop("record_path")
        if "primary_stream" in s and "is_primary" not in s:
            s["is_primary"] = s.pop("primary_stream")
        rules = s.get("extraction_rules")
        if isinstance(rules, list):
            for rule in rules:
                if isinstance(rule, dict):
                    rule.pop("action", None)
        routing_rules = s.get("routing_rules")
        if isinstance(routing_rules, list):
            for rule in routing_rules:
                if not isinstance(rule, dict):
                    continue
                rule.pop("rule_name", None)
                if (not rule.get("topic_name")) and rule.get("topic_suffix"):
                    suffix = str(rule.get("topic_suffix") or "").strip()
                    if suffix:
                        rule["topic_name"] = suffix
                rule.pop("topic_suffix", None)
        try:
            normalized_streams.append(Stream(**s).dict())
        except ValidationError as exc:
            raise HTTPException(status_code=422, detail=jsonable_encoder(exc.errors()))
    if existing.get("type") == "Webhook":
        stream0 = normalized_streams[0]
        fan = stream0.get("fan_out") or {}
        pag = stream0.get("pagination") or {}
        if fan.get("enabled") or fan.get("parent_stream_id") or fan.get("inject_as") or fan.get("inject_field_name"):
            raise HTTPException(status_code=422, detail="Webhook stream does not support parent-child request mapping")
        if pag.get("enabled"):
            raise HTTPException(status_code=422, detail="Webhook stream does not support pagination")
    await db.sources.update_one({"id": source_id}, {"$set": {"streams": normalized_streams, "updated_at": datetime.utcnow()}})
    updated = await db.sources.find_one({"id": source_id}, {"_id": 0})
    await content_store.sync_source(db, source_id)
    return _to_response(updated)


def _resolve_path(obj: Any, path: str) -> Any:
    """Resolve a dot-notation or bracket-notation path in a dict/list."""
    if not path:
        return obj
    parts = re.split(r'[.\[\]]+', path.strip('.').strip(']'))
    current = obj
    for part in parts:
        if not part:
            continue
        try:
            if isinstance(current, list):
                current = current[int(part)]
            elif isinstance(current, dict):
                current = current[part]
            else:
                return None
        except (KeyError, IndexError, ValueError, TypeError):
            return None
    return current


def _substitute_params(template: str, values: Dict[str, Any]) -> str:
    """Replace ${key} or {key} placeholders in a string with values."""
    result = template
    # Defensive normalization for malformed tokens like `$${param}` produced by
    # older client-side path binding replacements.
    result = re.sub(r"\$\$\{([^}]+)\}", r"${\1}", result)
    for key, val in values.items():
        result = result.replace(f"${{{key}}}", str(val))
        result = result.replace(f"{{{key}}}", str(val))
    return result


def _find_unresolved_placeholders(value: str) -> List[str]:
    if not isinstance(value, str) or not value:
        return []
    found: List[str] = []
    seen: set = set()
    for match in PLACEHOLDER_TOKEN_PATTERN.finditer(value):
        token = (match.group(1) or match.group(2) or "").strip()
        if not token or token in seen:
            continue
        seen.add(token)
        found.append(token)
    return found


def _substitute_in_structure(value: Any, values: Dict[str, Any]) -> Any:
    """Recursively substitute placeholders inside dict/list/string payloads."""
    if isinstance(value, str):
        return _substitute_params(value, values)
    if isinstance(value, list):
        return [_substitute_in_structure(item, values) for item in value]
    if isinstance(value, dict):
        return {k: _substitute_in_structure(v, values) for k, v in value.items()}
    return value


def _extract_mongo_template_variables(value: str) -> List[str]:
    found: List[str] = []
    seen: set = set()
    for match in MONGO_TEMPLATE_VAR_PATTERN.finditer(value or ""):
        variable = (match.group(1) or "").strip()
        if variable and variable not in seen:
            seen.add(variable)
            found.append(variable)
    return found


def _render_mongo_json_template(
    value: Any,
    values: Dict[str, Any],
    field_name: str,
    fallback: Dict[str, Any],
) -> Dict[str, Any]:
    """Parse Mongo JSON template with strict placeholder semantics.

    Rules:
      - placeholders can be used only as raw values (e.g. {"k":${var}})
      - placeholder keys are invalid
      - quoted placeholders are invalid
    """
    if value is None:
        return fallback
    if isinstance(value, dict):
        # Existing payloads that already provide a dict remain supported.
        return _substitute_in_structure(value, values)
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a JSON object")

    text = value.strip()
    if not text:
        return fallback

    if MONGO_TEMPLATE_KEY_PATTERN.search(text):
        raise ValueError(
            f"{field_name} contains an invalid placeholder key. "
            "Use placeholders only as values (example: {\"asset_id\":${asset_id}})."
        )
    if MONGO_QUOTED_TEMPLATE_PATTERN.search(text):
        raise ValueError(
            f"{field_name} contains a quoted placeholder. "
            "Use raw placeholders (example: {\"asset_id\":${asset_id}})."
        )

    required_vars = _extract_mongo_template_variables(text)
    missing = [var for var in required_vars if str(values.get(var, "")).strip() == ""]
    if missing:
        raise ValueError(
            f"{field_name} is missing values for placeholders: {', '.join(missing)}"
        )

    def _replace(match: re.Match) -> str:
        variable = (match.group(1) or "").strip()
        raw = values.get(variable)
        if isinstance(raw, (dict, list, bool, int, float)) or raw is None:
            return json.dumps(raw)
        text_value = str(raw).strip()
        if text_value == "":
            return "null"
        # If the provided value is already a JSON literal, keep it typed.
        try:
            parsed = json.loads(text_value)
            return json.dumps(parsed)
        except Exception:
            return json.dumps(text_value)

    rendered = MONGO_TEMPLATE_VAR_PATTERN.sub(_replace, text)
    try:
        parsed = json.loads(rendered)
    except Exception as exc:
        raise ValueError(f"Invalid JSON in {field_name}: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{field_name} must be a JSON object")
    return parsed


def _normalize_attr_key(value: str) -> str:
    key = (value or "").strip()
    if not key:
        return ""
    # Allow legacy JSONPath-like inputs (e.g. $.id or results[*].id) by
    # mapping to the trailing token as an attribute fallback.
    key = key.replace("[*]", "")
    if "." in key:
        key = key.split(".")[-1]
    return key.lstrip("$")


def _json_safe(value: Any) -> Any:
    """Convert BSON/non-JSON values into JSON-safe primitives."""
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    # Handle numpy/pandas scalar wrappers if present.
    if hasattr(value, "item") and callable(getattr(value, "item")):
        try:
            return _json_safe(value.item())
        except Exception:
            pass
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


def _truncate_text(value: str, max_chars: int = TEST_STREAM_PREVIEW_MAX_CHARS) -> tuple[str, bool]:
    if len(value) <= max_chars:
        return value, False
    return value[:max_chars], True


def _prepare_preview_json(value: Any, max_chars: int = TEST_STREAM_PREVIEW_MAX_CHARS) -> tuple[Any, bool]:
    """Create a bounded JSON preview for UI rendering."""
    safe = _json_safe(value)
    try:
        serialized = json.dumps(safe, ensure_ascii=False)
    except Exception:
        txt, truncated = _truncate_text(str(safe), max_chars=max_chars)
        return txt, truncated
    if len(serialized) <= max_chars:
        return safe, False

    # Try smaller structured previews before falling back to truncated string.
    if isinstance(safe, list):
        subset = safe[:1]
        try:
            s2 = json.dumps(subset, ensure_ascii=False)
            if len(s2) <= max_chars:
                return subset, True
        except Exception:
            pass
    if isinstance(safe, dict):
        subset = {k: safe[k] for k in list(safe.keys())[:20]}
        try:
            s2 = json.dumps(subset, ensure_ascii=False)
            if len(s2) <= max_chars:
                return subset, True
        except Exception:
            pass

    txt, truncated = _truncate_text(serialized, max_chars=max_chars)
    return txt, truncated


def _parse_json_object(value: Any, fallback: Dict[str, Any], field_name: str) -> Dict[str, Any]:
    if value is None:
        return fallback
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return fallback
        try:
            parsed = json.loads(text)
        except Exception as exc:
            raise ValueError(f"Invalid JSON in {field_name}: {exc}") from exc
        if not isinstance(parsed, dict):
            raise ValueError(f"{field_name} must be a JSON object")
        return parsed
    raise ValueError(f"{field_name} must be a JSON object")


def _decode_preview_bytes(content: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "utf-16", "latin-1"):
        try:
            return content.decode(encoding)
        except Exception:
            continue
    return content.decode("utf-8", errors="replace")


def _decode_bytes_with_encoding(content: bytes, preferred_encoding: Optional[str]) -> str:
    encodings: List[str] = []
    if preferred_encoding and str(preferred_encoding).strip():
        encodings.append(str(preferred_encoding).strip())
    encodings.extend(["utf-8-sig", "utf-8", "utf-16", "latin-1"])
    seen: set[str] = set()
    for encoding in encodings:
        key = encoding.lower()
        if key in seen:
            continue
        seen.add(key)
        try:
            return content.decode(encoding)
        except Exception:
            continue
    return content.decode("utf-8", errors="replace")


def _normalize_csv_char(value: Any, fallback: str) -> str:
    text = str(value if value is not None else "").strip()
    if not text:
        return fallback
    aliases = {
        r"\t": "\t",
        "tab": "\t",
        r"\n": "\n",
        r"\r": "\r",
    }
    if text in aliases:
        return aliases[text]
    return text[0]


def _parse_rest_csv_preview(content: bytes, stream: Dict[str, Any]) -> tuple[List[Dict[str, Any]], List[str], bool]:
    delimiter = _normalize_csv_char(stream.get("csv_delimiter"), ",")
    quote_char_raw = stream.get("csv_quote_char")
    quote_char = _normalize_csv_char(quote_char_raw, '"') if quote_char_raw not in (None, "") else '"'
    escape_char_raw = stream.get("csv_escape_char")
    escape_char = _normalize_csv_char(escape_char_raw, "\\") if escape_char_raw not in (None, "") else "\\"
    has_header = stream.get("csv_has_header_row")
    has_header = True if has_header is None else bool(has_header)
    encoding = "utf-8"

    preview_bytes = content[: TEST_STREAM_PREVIEW_MAX_CHARS * 2]
    text = _decode_bytes_with_encoding(preview_bytes, encoding)
    sio = io.StringIO(text)
    csv_kwargs: Dict[str, Any] = {
        "delimiter": delimiter,
        "quotechar": quote_char,
        "escapechar": escape_char,
        "skipinitialspace": True,
    }

    rows: List[Dict[str, Any]] = []
    columns: List[str] = []
    truncated_rows = len(content) > len(preview_bytes)

    if has_header:
        reader = csv.DictReader(sio, **csv_kwargs)
        columns = [str(c).strip() for c in (reader.fieldnames or []) if c is not None]
        for row in reader:
            if row is None:
                continue
            normalized_row: Dict[str, Any] = {}
            for key, value in row.items():
                if key is None:
                    continue
                normalized_key = str(key).strip()
                if not normalized_key:
                    continue
                normalized_row[normalized_key] = value.strip() if isinstance(value, str) else value
            rows.append(normalized_row)
    else:
        reader = csv.reader(sio, **csv_kwargs)
        raw_rows: List[List[str]] = []
        width = 0
        for row in reader:
            if not row:
                continue
            raw_rows.append(row)
            width = max(width, len(row))
        columns = [f"column_{col_idx + 1}" for col_idx in range(width)]
        for row in raw_rows:
            rows.append({key: (row[col_idx] if col_idx < len(row) else "") for col_idx, key in enumerate(columns)})

    return rows, columns, truncated_rows


def _strip_xml_namespace(tag: str) -> str:
    if not tag:
        return ""
    out = tag
    if out.startswith("{") and "}" in out:
        out = out.split("}", 1)[1]
    if ":" in out:
        out = out.split(":", 1)[1]
    return out


def _xml_element_to_json(element: ET.Element) -> Any:
    attributes = {
        f"@{_strip_xml_namespace(key)}": value
        for key, value in (element.attrib or {}).items()
    }

    children = list(element)
    text_value = (element.text or "").strip()

    if not children:
        if attributes:
            if text_value:
                attributes["#text"] = text_value
            return attributes
        return text_value

    grouped: Dict[str, List[Any]] = {}
    for child in children:
        key = _strip_xml_namespace(child.tag)
        grouped.setdefault(key, []).append(_xml_element_to_json(child))

    payload: Dict[str, Any] = {}
    payload.update(attributes)
    for key, values in grouped.items():
        payload[key] = values[0] if len(values) == 1 else values
    if text_value:
        payload["#text"] = text_value
    return payload


def _infer_smb_preview_format(configured_format: str, file_name: str) -> str:
    fmt = (configured_format or "").strip().upper()
    if fmt in {"CSV", "JSON", "JSONL", "XML", "XLSX", "XLS", "EXCEL", "BINARY"}:
        if fmt in {"XLS", "XLSX", "EXCEL"}:
            return "XLSX"
        return fmt

    lower = (file_name or "").strip().lower()
    if lower.endswith(".jsonl") or lower.endswith(".ndjson"):
        return "JSONL"
    if lower.endswith(".json"):
        return "JSON"
    if lower.endswith(".csv"):
        return "CSV"
    if lower.endswith(".xml"):
        return "XML"
    if lower.endswith(".xlsx") or lower.endswith(".xls"):
        return "XLSX"
    return "BINARY"


def _parse_smb_preview_content(content: bytes, *, source: Dict[str, Any], file_name: str) -> tuple[Any, str]:
    configured_format = source.get("smb_file_format") or ""
    fmt = _infer_smb_preview_format(configured_format, file_name)

    if fmt == "CSV":
        delimiter = source.get("smb_csv_delimiter") or ","
        has_header = source.get("smb_has_header_row")
        text = _decode_preview_bytes(content)
        text_preview, _ = _truncate_text(text)
        sio = io.StringIO(text_preview)

        if has_header is False:
            reader = csv.reader(sio, delimiter=delimiter)
            raw_rows: List[List[str]] = []
            width = 0
            for row in reader:
                if not row:
                    continue
                raw_rows.append(row)
                width = max(width, len(row))
                if len(raw_rows) >= 50:
                    break
            if not raw_rows:
                return [], fmt
            keys = [f"column_{idx + 1}" for idx in range(width)]
            rows = []
            for row in raw_rows:
                rows.append({
                    key: (row[idx] if idx < len(row) else "")
                    for idx, key in enumerate(keys)
                })
            return rows, fmt

        reader = csv.DictReader(sio, delimiter=delimiter)
        rows: List[Dict[str, Any]] = []
        for row in reader:
            if row is None:
                continue
            rows.append(dict(row))
            if len(rows) >= 50:
                break
        return rows, fmt

    if fmt == "JSON":
        text = _decode_preview_bytes(content)
        parsed = json.loads(text)
        return parsed, fmt

    if fmt == "JSONL":
        text = _decode_preview_bytes(content)
        rows = []
        for line in text.splitlines():
            payload = line.strip()
            if not payload:
                continue
            rows.append(json.loads(payload))
            if len(rows) >= 50:
                break
        return rows, fmt

    if fmt == "XML":
        text = _decode_preview_bytes(content)
        root = ET.fromstring(text)
        root_name = _strip_xml_namespace(root.tag) or "root"
        return {root_name: _xml_element_to_json(root)}, fmt

    if fmt == "XLSX":
        try:
            import pandas as pd
        except Exception as exc:
            raise ValueError(
                "XLSX preview requires pandas and openpyxl. Install openpyxl in backend environment."
            ) from exc

        try:
            df = pd.read_excel(io.BytesIO(content), nrows=50)
        except Exception as exc:
            raise ValueError(f"Unable to parse XLSX preview: {exc}") from exc

        df = df.astype(object).where(pd.notnull(df), None)
        return df.to_dict(orient="records"), fmt

    raise ValueError(
        "Preview for binary/unrecognized files is not supported. Use CSV, JSON, JSONL, XML, or XLSX format for structure preview."
    )


def _derive_smb_target(source: Dict[str, Any]) -> tuple[str, str, str]:
    host = (source.get("smb_hostname") or "").strip()
    share = (source.get("smb_share") or "").strip()
    base_dir = (source.get("smb_base_directory") or "").strip() or "/"

    if host.lower().startswith("smb://"):
        trimmed = host[6:]
        host_part, _, path_part = trimmed.partition("/")
        host = host_part.strip()
        if path_part and not share:
            share = path_part.split("/", 1)[0].strip()
            remainder = path_part.split("/", 1)[1] if "/" in path_part else ""
            if remainder:
                base_dir = f"/{remainder.lstrip('/')}"

    if not share and base_dir.startswith("/"):
        parts = [p for p in base_dir.split("/") if p]
        if parts:
            share = parts[0]
            rest = "/".join(parts[1:])
            base_dir = f"/{rest}" if rest else "/"

    return host, share, base_dir


_SENSITIVE_RESPONSE_HEADERS = {
    "set-cookie",
    "authorization",
    "proxy-authenticate",
    "proxy-authorization",
    "www-authenticate",
}


def _safe_response_headers(headers: Any) -> Dict[str, str]:
    """Response headers for the UI, preserving the server's original casing.

    Casing is preserved deliberately: NiFi attribute lookup is case-sensitive, so pagination
    auto-detection must capture the header name exactly as the server sent it.
    Credential-bearing headers are redacted — this payload goes to the browser.
    """
    out: Dict[str, str] = {}
    try:
        for key, value in headers.items():
            if str(key).strip().lower() in _SENSITIVE_RESPONSE_HEADERS:
                out[str(key)] = "[redacted]"
            else:
                out[str(key)] = str(value)
    except Exception:
        return {}
    return out


@router.post(
    "/{source_id}/test-stream",
    summary="Test a source stream",
    description=(
        "Run a read-only stream test and return response/file/document preview data. "
        "Use parent_values when the stream depends on a parent stream value."
    ),
)
async def test_stream(
    source_id: str,
    data: TestStreamRequest = Body(...),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    """Test an individual stream by making the actual HTTP request.
    
    Request body:
      - stream_id: (optional) the stream's UUID
      - stream_name: (optional) the stream's name
      - parent_values: (optional) dict of values from parent stream run, e.g. {"site_id": "abc"}
    
    Returns:
      - ok: bool
      - status_code: int
      - url: str (the actual URL called)
      - error: str (if failed)
      - response_data: preview payload (json/text)
      - response_data_type: "json" | "text"
      - response_data_truncated: bool
    """
    source = await db.sources.find_one({"id": source_id}, {"_id": 0})
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    source = await resolve_source_application_service(db, source)

    stream_id = data.stream_id
    stream_name = data.stream_name
    parent_values: Dict[str, Any] = data.parent_values or {}

    # Find the stream
    streams = source.get("streams", [])
    stream = None
    if stream_id:
        stream = next((s for s in streams if s.get("id") == stream_id), None)
    elif stream_name:
        stream = next((s for s in streams if s.get("name") == stream_name), None)
    
    if not stream:
        raise HTTPException(status_code=404, detail="Stream not found")

    # REST API, MongoDB, SMB, and Webhook sources can be tested this way
    source_type = source.get("type", "REST API")
    if source_type not in ("REST API", "MongoDB", "SMB", "Webhook"):
        raise HTTPException(
            status_code=400,
            detail=f"Stream testing is only supported for REST API, MongoDB, SMB, and Webhook sources. Source type: {source_type}",
        )

    if source_type == "Webhook":
        endpoint_id = (source.get("webhook_endpoint_id") or "").strip()
        if not endpoint_id:
            raise HTTPException(status_code=400, detail="Webhook source is missing webhook_endpoint_id")

        flow = await db.flows.find_one({"source_id": source_id}, {"_id": 0, "id": 1})
        sample_query: Dict[str, Any] = {"source_id": source_id}
        if flow and flow.get("id"):
            sample_query["flow_id"] = flow["id"]

        sample = await db.webhook_samples.find_one(sample_query, sort=[("created_at", -1)], projection={"_id": 0})
        if not sample:
            return {
                "ok": False,
                "status_code": 404,
                "url": f"/api/webhooks/{endpoint_id}",
                "error": "No webhook samples received yet. Send an event to this endpoint, then test again.",
                "response_data": None,
                "response_data_type": None,
                "response_data_truncated": False,
            }

        response_format = str(stream.get("response_format") or "json").strip().lower()
        sample_payload = sample.get("payload")
        sample_raw = sample.get("payload_raw")

        if response_format == "xml":
            xml_body = sample_raw if isinstance(sample_raw, str) else (
                sample_payload if isinstance(sample_payload, str) else None
            )
            if xml_body is None:
                xml_body = json.dumps(sample_payload, ensure_ascii=False) if sample_payload is not None else ""
            xml_preview, xml_truncated = _truncate_text(xml_body)
            return {
                "ok": True,
                "status_code": 200,
                "url": f"/api/webhooks/{endpoint_id}",
                "error": None,
                "response_data": xml_preview,
                "response_data_type": "text",
                "response_data_truncated": xml_truncated,
            }

        preview_data, preview_truncated = _prepare_preview_json(sample_payload)
        preview_type = "json" if isinstance(preview_data, (dict, list)) else "text"
        return {
            "ok": True,
            "status_code": 200,
            "url": f"/api/webhooks/{endpoint_id}",
            "error": None,
            "response_data": preview_data,
            "response_data_type": preview_type,
            "response_data_truncated": preview_truncated,
        }

    # MongoDB stream test path (read-only probe of first row)
    if source_type == "MongoDB":
        if (source.get("mongo_connection_mode") or "").strip().lower() == "nifi_global_service":
            return {
                "ok": False,
                "error": "This MongoDB source uses a NiFi global service. App-side stream preview is unavailable because the connection is configured inside NiFi.",
                "status_code": None,
                "url": None,
                "response_data": None,
                "response_data_type": None,
                "response_data_truncated": False,
            }
        db_name = (source.get("db_database") or "").strip()
        if not db_name:
            raise HTTPException(status_code=400, detail="MongoDB source is missing db_database")

        # Mongo collection name comes from stream.endpoint_path ("Collection Name" in UI),
        # with legacy fallback to stream.name.
        raw_collection_name = (stream.get("endpoint_path") or stream.get("name") or stream.get("id") or "").strip()
        collection_name = raw_collection_name.strip("/")
        if not collection_name:
            raise HTTPException(status_code=422, detail="Stream is missing collection name")

        stream_query = stream.get("mongo_query")
        stream_projection = stream.get("mongo_projection")
        stream_sort = stream.get("mongo_sort")
        stream_limit = stream.get("mongo_limit")
        if stream_limit is None:
            stream_limit = source.get("db_limit")

        try:
            query = _render_mongo_json_template(
                stream_query if stream_query is not None else source.get("db_query"),
                parent_values,
                "db_query",
                {},
            )
            projection = _render_mongo_json_template(
                stream_projection if stream_projection is not None else source.get("db_projection"),
                parent_values,
                "db_projection",
                {},
            )
            sort_obj = _render_mongo_json_template(
                stream_sort if stream_sort is not None else source.get("db_sort"),
                parent_values,
                "db_sort",
                {},
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc))

        # If this stream depends on parent values, inject a query filter in Mongo tests.
        fan_out = stream.get("fan_out") or {}
        if fan_out.get("enabled"):
            parent_field = fan_out.get("parent_field") or ""
            inject_field = fan_out.get("inject_field_name") or ""
            parent_attr = parent_field or inject_field
            parent_val = parent_values.get(parent_attr, "")
            if parent_val == "":
                parent_val = parent_values.get(_normalize_attr_key(parent_attr), "")
            if parent_val == "":
                parent_val = parent_values.get(inject_field, "")
            if parent_val != "":
                mongo_filter_key = _normalize_attr_key(inject_field or parent_attr)
                if mongo_filter_key:
                    query[mongo_filter_key] = parent_val

        timeout_ms = max(1000, int((source.get("connection_timeout") or 30) * 1000))
        mongo_uri = (source.get("db_connection_uri") or "").strip()
        if not mongo_uri:
            host = (source.get("db_host") or "localhost").strip()
            port = int(source.get("db_port") or 27017)
            mongo_uri = f"mongodb://{host}:{port}"

        pseudo_url = f"{_redact_mongo_uri(mongo_uri).rstrip('/')}/{db_name}.{collection_name}"

        try:
            client = AsyncIOMotorClient(
                mongo_uri,
                serverSelectionTimeoutMS=timeout_ms,
                connectTimeoutMS=timeout_ms,
                socketTimeoutMS=timeout_ms,
            )
            collection_names = await client[db_name].list_collection_names()
            if collection_name not in collection_names:
                client.close()
                raise HTTPException(status_code=404, detail=f"MongoDB collection not found: {db_name}.{collection_name}")
            collection = client[db_name][collection_name]
            # Small bounded preview sample for structure/key suggestions.
            mongo_limit = 10
            try:
                parsed_limit = int(stream_limit) if stream_limit is not None else None
            except Exception:
                parsed_limit = None
            if isinstance(parsed_limit, int) and parsed_limit > 0:
                mongo_limit = min(parsed_limit, 50)
            cursor = collection.find(query, projection or None).limit(mongo_limit)
            if sort_obj:
                cursor = cursor.sort(list(sort_obj.items()))
            docs = await cursor.to_list(length=1)
            client.close()
        except Exception as e:
            if isinstance(e, HTTPException):
                raise
            return {
                "ok": False,
                "error": str(e),
                "status_code": None,
                "url": pseudo_url,
                "response_data": None,
                "response_data_type": None,
                "response_data_truncated": False,
            }
        preview_data, preview_truncated = _prepare_preview_json(docs)

        return {
            "ok": True,
            "status_code": 200,
            "url": pseudo_url,
            "error": None,
            "response_data": preview_data,
            "response_data_type": "json",
            "response_data_truncated": preview_truncated,
        }

    if source_type == "SMB":
        smb_host, smb_share, smb_base_dir = _derive_smb_target(source)
        smb_user = (source.get("smb_username") or "").strip()
        smb_password = source.get("smb_password") or ""
        smb_domain = (source.get("smb_domain") or "").strip()
        file_filter = (source.get("smb_file_filter") or "").strip()
        timeout = int(source.get("connection_timeout") or 30)
        pseudo_url = f"smb://{smb_host}/{smb_share}{smb_base_dir}"

        missing = []
        if not smb_host:
            missing.append("smb_hostname")
        if not smb_share:
            missing.append("smb_share")
        if not smb_user:
            missing.append("smb_username")
        if not smb_password:
            missing.append("smb_password")
        if missing:
            raise HTTPException(status_code=400, detail=f"SMB source is missing required fields: {', '.join(missing)}")

        def _sample_smb_preview() -> tuple[List[Dict[str, Any]], Optional[Dict[str, Any]], Any, Optional[str]]:
            conn = SMBConnection(
                smb_user,
                smb_password,
                "nif-abstractor",
                smb_host,
                domain=smb_domain or "",
                use_ntlm_v2=True,
                is_direct_tcp=True,
            )
            connected = conn.connect(smb_host, 445, timeout=timeout)
            if not connected:
                raise RuntimeError("SMB connection failed")
            try:
                entries = conn.listPath(smb_share, smb_base_dir)
                regex = re.compile(file_filter) if file_filter else None
                files: List[Dict[str, Any]] = []
                for entry in entries:
                    if entry.filename in (".", "..") or entry.isDirectory:
                        continue
                    if regex and not regex.search(entry.filename):
                        continue
                    rel_dir = smb_base_dir.rstrip("/")
                    remote_path = f"{rel_dir}/{entry.filename}" if rel_dir else f"/{entry.filename}"
                    files.append({
                        "file_name": entry.filename,
                        "path": remote_path,
                        "size": int(entry.file_size),
                        "modified_at": datetime.fromtimestamp(entry.last_write_time).isoformat() if entry.last_write_time else None,
                        "_last_write_time": int(entry.last_write_time or 0),
                    })

                files.sort(key=lambda item: item.get("_last_write_time", 0), reverse=True)
                if not files:
                    return files, None, None, None

                sample_file = files[0]
                sample_buffer = io.BytesIO()
                conn.retrieveFile(smb_share, sample_file["path"], sample_buffer, timeout=timeout)
                sample_content = sample_buffer.getvalue()
                parsed_preview, detected_format = _parse_smb_preview_content(
                    sample_content,
                    source=source,
                    file_name=sample_file.get("file_name") or "",
                )
                return files, sample_file, parsed_preview, detected_format
            finally:
                conn.close()

        try:
            files, sample_file, parsed_preview, detected_format = await asyncio.to_thread(_sample_smb_preview)
        except re.error as exc:
            return {
                "ok": False,
                "error": f"Invalid smb_file_filter regex: {exc}",
                "status_code": None,
                "url": pseudo_url,
                "response_data": None,
                "response_data_type": None,
                "response_data_truncated": False,
            }
        except ValueError as exc:
            return {
                "ok": False,
                "error": str(exc),
                "status_code": 422,
                "url": pseudo_url,
                "response_data": {
                    "preview_file": {
                        "file_name": (sample_file or {}).get("file_name") if "sample_file" in locals() else None,
                        "path": (sample_file or {}).get("path") if "sample_file" in locals() else None,
                    },
                    "detected_format": detected_format if "detected_format" in locals() else None,
                },
                "response_data_type": "json",
                "response_data_truncated": False,
            }
        except Exception as exc:
            return {
                "ok": False,
                "error": str(exc),
                "status_code": None,
                "url": pseudo_url,
                "response_data": None,
                "response_data_type": None,
                "response_data_truncated": False,
            }

        visible_files = [
            {k: v for k, v in file_entry.items() if not k.startswith("_")}
            for file_entry in files
        ]
        if not visible_files:
            return {
                "ok": False,
                "error": "No files matched the SMB path/filter. Check base directory and file filter.",
                "status_code": 404,
                "url": pseudo_url,
                "response_data": {
                    "matched_files": 0,
                },
                "response_data_type": "json",
                "response_data_truncated": False,
            }

        preview_data, preview_truncated = _prepare_preview_json(parsed_preview)
        sample_visible = {k: v for k, v in (sample_file or {}).items() if not k.startswith("_")}

        return {
            "ok": True,
            "status_code": 200,
            "url": pseudo_url,
            "error": None,
            "response_data": preview_data,
            "response_data_type": "json",
            "response_data_truncated": preview_truncated,
            "preview_file": sample_visible,
            "matched_files": len(visible_files),
            "detected_format": detected_format,
        }

    try:
        resolved_request = resolve_rest_stream_request(
            source,
            stream,
            parent_values,
            confirmed_mutation=data.confirm_mutation,
        )
    except RestStreamRequestError as exc:
        return {
            "ok": False,
            "error": str(exc),
            "status_code": 422,
            "url": None,
            "response_data": None,
            "response_data_type": None,
            "response_data_truncated": False,
        }

    # Make the HTTP request
    timeout = source.get("connection_timeout", 30)
    try:
        async with httpx.AsyncClient(timeout=timeout, verify=tls_verify_enabled(), follow_redirects=True) as client:
            resp = await client.request(
                resolved_request.method,
                resolved_request.url,
                headers=resolved_request.headers,
                params=resolved_request.params,
                content=resolved_request.body,
            )
    except httpx.TimeoutException:
        return {
            "ok": False, "error": f"Request timed out after {timeout}s",
            "status_code": None, "url": resolved_request.masked_url,
            "response_data": None,
            "response_data_type": None,
            "response_data_truncated": False,
        }
    except Exception as e:
        err_text = str(e)
        if "Temporary failure in name resolution" in err_text or "Name or service not known" in err_text:
            err_text = (
                "DNS resolution failed for the target host from backend runtime. "
                "Verify base URL hostname is reachable from backend (or use an IP/host resolvable in this environment)."
            )
        return {
            "ok": False, "error": err_text,
            "status_code": None, "url": resolved_request.masked_url,
            "response_data": None,
            "response_data_type": None,
            "response_data_truncated": False,
        }
    response_format = str(stream.get("response_format") or "json").strip().lower()
    raw_text = resp.text or ""
    content_type = (resp.headers.get("content-type") or "").lower()
    preview_data: Any = None
    preview_type = "text"
    preview_truncated = False
    parsed_csv_columns: Optional[List[str]] = None
    parsed_csv_rows_truncated = False
    should_parse_csv = response_format == "csv" or "text/csv" in content_type or "application/csv" in content_type

    if should_parse_csv:
        try:
            parsed_rows, parsed_columns, csv_rows_truncated = _parse_rest_csv_preview(resp.content or b"", stream)
            preview_data, preview_truncated = _prepare_preview_json(parsed_rows)
            preview_type = "json"
            parsed_csv_columns = parsed_columns
            parsed_csv_rows_truncated = csv_rows_truncated
            preview_truncated = bool(preview_truncated or csv_rows_truncated)
        except Exception as exc:
            preview_data, preview_truncated = _truncate_text(raw_text)
            preview_type = "text"
            if resp.status_code < 400:
                return {
                    "ok": False,
                    "status_code": resp.status_code,
                    "url": resolved_request.masked_url,
                    "error": f"CSV parse failed: {str(exc)[:240]}",
                    "response_data": preview_data,
                    "response_data_type": preview_type,
                    "response_data_truncated": preview_truncated,
                }
    elif "json" in content_type:
        try:
            parsed = resp.json()
            preview_data, preview_truncated = _prepare_preview_json(parsed)
            preview_type = "json"
        except Exception:
            preview_data, preview_truncated = _truncate_text(raw_text)
    else:
        preview_data, preview_truncated = _truncate_text(raw_text)

    result = {
        "ok": resp.status_code < 400,
        "status_code": resp.status_code,
        "url": resolved_request.masked_url,
        "error": None if resp.status_code < 400 else f"HTTP {resp.status_code}: {resp.text[:200]}",
        "response_data": preview_data,
        "response_data_type": preview_type,
        "response_data_truncated": preview_truncated,
        "response_headers": _safe_response_headers(resp.headers),
    }
    if parsed_csv_columns is not None:
        result["csv_columns"] = parsed_csv_columns
        result["csv_rows_truncated"] = parsed_csv_rows_truncated
    return result
