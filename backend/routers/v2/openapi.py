import base64
import gzip
import uuid
from datetime import datetime
from typing import Any, Dict, List

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from motor.motor_asyncio import AsyncIOMotorDatabase

from db import get_db
from services.openapi_parser import OpenApiParseError, parse_openapi_document

router = APIRouter(prefix="/api/v2/openapi", tags=["openapi-v2"])

MAX_OPENAPI_UPLOAD_BYTES = 5 * 1024 * 1024
YAML_CONTENT_TYPES = {"application/yaml", "application/x-yaml", "text/yaml", "text/x-yaml"}


def _compress_b64(raw: bytes) -> str:
    return base64.b64encode(gzip.compress(raw)).decode("ascii")


def _to_spec_summary(doc: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "specId": doc.get("id"),
        "title": doc.get("title") or "",
        "version": doc.get("version") or "",
        "format": doc.get("format") or "",
        "openapiVersion": doc.get("openapi_version") or "",
        "operationsCount": len(doc.get("operations") or []),
        "servers": doc.get("servers") or [],
        "warnings": doc.get("warnings") or [],
    }


def _to_parameter(p: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "name": p.get("name"),
        "location": p.get("in"),
        "required": bool(p.get("required")),
        "schemaType": p.get("schema_type"),
        "default": p.get("default"),
    }


def _to_operation_summary(op: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "operationId": op.get("operation_id"),
        "method": op.get("method"),
        "path": op.get("path_original"),
        "summary": op.get("summary") or "",
        "tags": op.get("tags") or [],
        "parameters": [_to_parameter(p) for p in (op.get("parameters") or [])],
    }


def _to_operation_detail(op: Dict[str, Any]) -> Dict[str, Any]:
    detail = _to_operation_summary(op)
    detail.update(
        {
            "description": op.get("description") or "",
            "deprecated": bool(op.get("deprecated")),
            "pathRuntime": op.get("path_runtime"),
            "security": op.get("security") or [],
            "requestContentTypes": op.get("request_content_types") or [],
        }
    )
    return detail


@router.post("/parse", status_code=201, summary="Upload and parse an OpenAPI document (v2)")
async def parse_openapi_v2(
    file: UploadFile = File(...),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    filename = (file.filename or "").strip()
    lower_name = filename.lower()
    content_type = (file.content_type or "").strip().lower()

    if lower_name.endswith(".yaml") or lower_name.endswith(".yml") or content_type in YAML_CONTENT_TYPES:
        raise HTTPException(
            status_code=422,
            detail="YAML OpenAPI documents are not supported. Only JSON OpenAPI/Swagger documents can be parsed; please convert the file to JSON and re-upload.",
        )
    if lower_name and not lower_name.endswith(".json"):
        raise HTTPException(status_code=422, detail="Only OpenAPI JSON files are supported. Please upload a .json file.")

    raw = await file.read()
    if len(raw) > MAX_OPENAPI_UPLOAD_BYTES:
        raise HTTPException(
            status_code=413,
            detail=(
                f"OpenAPI file is too large ({len(raw)} bytes). "
                f"Maximum supported size is {MAX_OPENAPI_UPLOAD_BYTES} bytes (5 MB)."
            ),
        )

    try:
        parsed = parse_openapi_document(raw, filename=filename)
    except OpenApiParseError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    existing = await db.openapi_specs_v2.find_one({"checksum_sha256": parsed["checksum_sha256"]}, {"_id": 0})
    if existing:
        return _to_spec_summary(existing)

    now = datetime.utcnow()
    spec_id = str(uuid.uuid4())
    doc = {
        "id": spec_id,
        "filename": filename or "openapi.json",
        "raw_content_gzip_b64": _compress_b64(raw),
        "checksum_sha256": parsed["checksum_sha256"],
        "raw_size_bytes": parsed["raw_size_bytes"],
        "format": parsed["format"],
        "openapi_version": parsed["openapi_version"],
        "title": parsed["title"],
        "version": parsed["version"],
        "servers": parsed["servers"],
        "operations": parsed["operations"],
        "warnings": parsed["warnings"],
        "errors": parsed["errors"],
        "created_at": now,
        "updated_at": now,
    }
    await db.openapi_specs_v2.insert_one(doc)

    return _to_spec_summary(doc)


@router.get("/{spec_id}", summary="Get parsed OpenAPI spec summary (v2)")
async def get_openapi_spec_v2(spec_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    doc = await db.openapi_specs_v2.find_one(
        {"id": spec_id}, {"_id": 0, "operations": 0, "raw_content_gzip_b64": 0}
    )
    if not doc:
        raise HTTPException(status_code=404, detail="OpenAPI spec not found")
    return _to_spec_summary(doc)


@router.get("/{spec_id}/operations", summary="Search parsed operations (v2)")
async def list_operations_v2(
    spec_id: str,
    search: str = Query("", max_length=200),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=200, alias="pageSize"),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    doc = await db.openapi_specs_v2.find_one({"id": spec_id}, {"_id": 0, "operations": 1})
    if not doc:
        raise HTTPException(status_code=404, detail="OpenAPI spec not found")

    ops: List[Dict[str, Any]] = doc.get("operations") or []
    q = (search or "").strip().lower()

    def _match(item: Dict[str, Any]) -> bool:
        if not q:
            return True
        target = " ".join(
            [
                str(item.get("operation_id") or ""),
                str(item.get("path_original") or ""),
                str(item.get("summary") or ""),
                " ".join(item.get("tags") or []),
            ]
        ).lower()
        return q in target

    filtered = [item for item in ops if _match(item)]
    total = len(filtered)
    start = (page - 1) * page_size
    end = start + page_size
    page_items = filtered[start:end]

    return {
        "items": [_to_operation_summary(item) for item in page_items],
        "total": total,
        "page": page,
        "pageSize": page_size,
    }


@router.get("/{spec_id}/operations/{operation_id}", summary="Get operation detail (v2)")
async def get_operation_detail_v2(spec_id: str, operation_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    doc = await db.openapi_specs_v2.find_one({"id": spec_id}, {"_id": 0, "operations": 1})
    if not doc:
        raise HTTPException(status_code=404, detail="OpenAPI spec not found")
    ops: List[Dict[str, Any]] = doc.get("operations") or []
    match = next((item for item in ops if item.get("operation_id") == operation_id), None)
    if not match:
        raise HTTPException(status_code=404, detail="Operation not found")
    return _to_operation_detail(match)
