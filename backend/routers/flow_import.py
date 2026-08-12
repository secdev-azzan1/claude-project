import json

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from motor.motor_asyncio import AsyncIOMotorDatabase

from db import get_db
from services.flow_import_credentials import FlowImportCredentialError, validate_import_credentials
from services.flow_import_finalize import FlowImportFinalizeError, finalize_flow_import
from services.flow_import_preview import FlowImportPreviewError, preview_flow_import


router = APIRouter(prefix="/api/flows/import", tags=["flows"])


def _validate_flowpack_upload(file: UploadFile) -> None:
    filename = (file.filename or "").strip()
    if filename and not filename.lower().endswith(".flowpack.json"):
        raise HTTPException(status_code=422, detail="Only .flowpack.json files are supported.")
    content_type = (file.content_type or "").strip().lower()
    if content_type and content_type not in {"application/json", "text/json", "application/octet-stream"}:
        raise HTTPException(status_code=422, detail=f"Unsupported file content type '{content_type}'.")


@router.post("/preview", summary="Preview a flowpack import without writing records")
async def preview_flowpack_import(
    file: UploadFile = File(...),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    _validate_flowpack_upload(file)

    raw = await file.read()
    try:
        return await preview_flow_import(raw, db=db)
    except FlowImportPreviewError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@router.post("/credentials/validate", summary="Validate re-entered flowpack credentials without importing")
async def validate_flowpack_import_credentials(
    file: UploadFile = File(...),
    credentials_json: str = Form(...),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    _validate_flowpack_upload(file)
    try:
        credentials = json.loads(credentials_json)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=422, detail="credentials_json must be valid JSON.") from exc

    raw = await file.read()
    try:
        return await validate_import_credentials(raw, credentials, db=db)
    except FlowImportCredentialError as exc:
        raise HTTPException(status_code=422, detail=exc.result)
    except FlowImportPreviewError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@router.post("/finalize", summary="Import a validated flowpack as a new draft flow")
async def finalize_flowpack_import(
    file: UploadFile = File(...),
    credentials_json: str = Form(...),
    options_json: str = Form(...),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    _validate_flowpack_upload(file)
    try:
        credentials = json.loads(credentials_json)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=422, detail="credentials_json must be valid JSON.") from exc
    try:
        options = json.loads(options_json)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=422, detail="options_json must be valid JSON.") from exc

    raw = await file.read()
    try:
        return await finalize_flow_import(raw, credentials, options, db=db)
    except FlowImportFinalizeError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
