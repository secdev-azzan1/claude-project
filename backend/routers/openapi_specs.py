import gzip
import base64
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, Depends, File, HTTPException, Query, UploadFile
from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import BaseModel, ConfigDict, Field

from db import get_db
from services import content_store
from services.openapi_parser import OpenApiParseError, parse_openapi_document


router = APIRouter(prefix="/api/openapi", tags=["openapi"])
MAX_OPENAPI_UPLOAD_BYTES = 10 * 1024 * 1024


def _compress_b64(raw: bytes) -> str:
    return base64.b64encode(gzip.compress(raw)).decode("ascii")


def _to_spec_response(doc: Dict[str, Any], include_ops_count: bool = True) -> Dict[str, Any]:
    item = {
        "spec_id": doc.get("id"),
        "title": doc.get("title") or "",
        "version": doc.get("version") or "",
        "format": doc.get("format") or "",
        "openapi_version": doc.get("openapi_version") or "",
        "servers": doc.get("servers") or [],
        "warnings": doc.get("warnings") or [],
        "created_at": doc.get("created_at").isoformat() if isinstance(doc.get("created_at"), datetime) else doc.get("created_at"),
        "updated_at": doc.get("updated_at").isoformat() if isinstance(doc.get("updated_at"), datetime) else doc.get("updated_at"),
    }
    if include_ops_count:
        item["operations_count"] = len(doc.get("operations") or [])
    return item


@router.post("/parse", status_code=201, summary="Upload and parse an OpenAPI document")
async def parse_openapi(
    file: UploadFile = File(...),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    filename = (file.filename or "").strip()
    content_type = (file.content_type or "").strip().lower()
    if filename and not filename.lower().endswith(".json"):
        raise HTTPException(status_code=422, detail="Only OpenAPI JSON files are supported. Please upload a .json file.")
    if content_type and content_type not in {"application/json", "text/json", "application/octet-stream"}:
        raise HTTPException(status_code=422, detail=f"Unsupported file content type '{content_type}'. Only JSON is supported.")

    raw = await file.read()
    if len(raw) > MAX_OPENAPI_UPLOAD_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"OpenAPI file is too large ({len(raw)} bytes). Max supported size is {MAX_OPENAPI_UPLOAD_BYTES} bytes.",
        )
    try:
        parsed = parse_openapi_document(raw, filename=file.filename or "")
    except OpenApiParseError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    existing = await db.openapi_specs.find_one({"checksum_sha256": parsed["checksum_sha256"]}, {"_id": 0})
    if existing:
        await content_store.sync_openapi_spec(db, existing["id"])
        return _to_spec_response(existing)

    now = datetime.utcnow()
    spec_id = str(uuid.uuid4())
    doc = {
        "id": spec_id,
        "filename": file.filename or "openapi.json",
        "source_type": "REST API",
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
    await db.openapi_specs.insert_one(doc)
    await content_store.sync_openapi_spec(db, spec_id)

    res = _to_spec_response(doc)
    return res


@router.get("/{spec_id}", summary="Get parsed OpenAPI spec summary")
async def get_openapi_spec(spec_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    doc = await db.openapi_specs.find_one({"id": spec_id}, {"_id": 0, "operations": 0, "raw_content_gzip_b64": 0})
    if not doc:
        raise HTTPException(status_code=404, detail="OpenAPI spec not found")
    return _to_spec_response(doc)


@router.get("/{spec_id}/operations", summary="List parsed operations")
async def list_operations(
    spec_id: str,
    search: str = Query("", max_length=200),
    method: str = Query("", max_length=10),
    tag: str = Query("", max_length=120),
    page: int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=200),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    doc = await db.openapi_specs.find_one({"id": spec_id}, {"_id": 0, "operations": 1})
    if not doc:
        raise HTTPException(status_code=404, detail="OpenAPI spec not found")

    ops = doc.get("operations") or []
    q = (search or "").strip().lower()
    method_u = (method or "").strip().upper()
    tag_q = (tag or "").strip().lower()

    def _match(item: Dict[str, Any]) -> bool:
        if method_u and item.get("method") != method_u:
            return False
        if tag_q:
            tags = [str(t).lower() for t in (item.get("tags") or [])]
            if tag_q not in tags:
                return False
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

    out_items = []
    for item in page_items:
        params = item.get("parameters") or []
        out_items.append(
            {
                "operation_id": item.get("operation_id"),
                "method": item.get("method"),
                "path_original": item.get("path_original"),
                "path_runtime": item.get("path_runtime"),
                "summary": item.get("summary"),
                "tags": item.get("tags") or [],
                "deprecated": bool(item.get("deprecated")),
                "required_path_params": [p.get("name") for p in params if p.get("in") == "path" and p.get("required")],
                "required_query_params": [p.get("name") for p in params if p.get("in") == "query" and p.get("required")],
            }
        )

    return {
        "items": out_items,
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/{spec_id}/operations/{operation_id}", summary="Get operation details")
async def get_operation_detail(spec_id: str, operation_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    doc = await db.openapi_specs.find_one({"id": spec_id}, {"_id": 0, "operations": 1})
    if not doc:
        raise HTTPException(status_code=404, detail="OpenAPI spec not found")
    ops = doc.get("operations") or []
    match = next((item for item in ops if item.get("operation_id") == operation_id), None)
    if not match:
        raise HTTPException(status_code=404, detail="Operation not found")
    return match


class AttachSpecRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    spec_id: str = Field(..., min_length=1)
    selected_server_url: Optional[str] = None


@router.post("/sources/{source_id}/attach", summary="Attach OpenAPI spec to a source")
async def attach_openapi_to_source(
    source_id: str,
    payload: AttachSpecRequest = Body(...),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    source = await db.sources.find_one({"id": source_id}, {"_id": 0, "id": 1, "type": 1})
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    if source.get("type") != "REST API":
        raise HTTPException(status_code=422, detail="OpenAPI attachment is only supported for REST API sources.")

    spec = await db.openapi_specs.find_one({"id": payload.spec_id}, {"_id": 0, "id": 1, "servers": 1})
    if not spec:
        raise HTTPException(status_code=404, detail="OpenAPI spec not found")

    selected = (payload.selected_server_url or "").strip() or None
    if selected:
        known = [str(item.get("url") or "").strip() for item in (spec.get("servers") or [])]
        if known and selected not in known:
            raise HTTPException(status_code=422, detail="selected_server_url is not listed in spec servers")

    await db.sources.update_one(
        {"id": source_id},
        {"$set": {"openapi_spec_id": payload.spec_id, "openapi_selected_server_url": selected, "updated_at": datetime.utcnow()}},
    )
    await content_store.sync_source(db, source_id)
    return {"ok": True, "source_id": source_id, "spec_id": payload.spec_id, "selected_server_url": selected}


@router.delete("/sources/{source_id}/attach", summary="Detach OpenAPI spec from a source")
async def detach_openapi_from_source(source_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    source = await db.sources.find_one({"id": source_id}, {"_id": 0, "id": 1})
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    await db.sources.update_one(
        {"id": source_id},
        {"$set": {"openapi_spec_id": None, "openapi_selected_server_url": None, "updated_at": datetime.utcnow()}},
    )
    await content_store.sync_source(db, source_id)
    return {"ok": True, "source_id": source_id}
