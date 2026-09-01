"""V2 schema ceremony endpoints.

Unlike the legacy `/api/schema-inference` route, this router operates on the
V2 block graph and delegates execution to the V2 compiler/deployer.  The
temporary runtime is asynchronous so the browser can show real progress and
offer a stop action while NiFi is collecting records.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, Set

from fastapi import APIRouter, Body, Depends, HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import BaseModel, Field
from pymongo import ReturnDocument

from db import get_db
from models.adapter import Flow
from services.adapter.common import COLLECTIONS, new_id, now_iso
from services.adapter.compiler import CompileError
from services.adapter.schema_inference import (
    ACTIVE_STATUSES,
    DEFAULT_TARGET_MESSAGES,
    get_job,
    public_job,
    run_inference_background,
    target_block_or_error,
    _connections_and_context,
    _temporary_topic,
    _schema_identity,
)
from services.adapter.compiler.inference import build_inference_plan

router = APIRouter(prefix="/api/v2/schema-inference", tags=["schema-inference-v2"])


class StartSchemaInferenceRequest(BaseModel):
    flowId: str
    targetBlockId: str
    targetMessages: int = Field(default=DEFAULT_TARGET_MESSAGES, ge=1, le=100)


_TASKS: Set[asyncio.Task] = set()


def _retain_task(task: asyncio.Task) -> None:
    _TASKS.add(task)
    task.add_done_callback(_TASKS.discard)


@router.post("/start", status_code=202)
async def start_schema_inference(
    body: StartSchemaInferenceRequest = Body(...),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    flow_doc = await db[COLLECTIONS.flows].find_one({"id": body.flowId}, {"_id": 0})
    if not flow_doc:
        raise HTTPException(status_code=404, detail="Flow not found.")
    flow = Flow(**flow_doc)

    try:
        target = target_block_or_error(flow, body.targetBlockId)
        target_topic, _schema_name, _namespace = _schema_identity(flow, target)
        _nifi_conn, _kafka_conn, context, _nifi_doc, _kafka_doc = await _connections_and_context(db, flow)
    except (CompileError, RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    running = await db[COLLECTIONS.schema_inference_jobs].find_one(
        {
            "flowId": body.flowId,
            "targetBlockId": body.targetBlockId,
            "status": {"$in": sorted(ACTIVE_STATUSES)},
        },
        {"_id": 0},
    )
    if running:
        raise HTTPException(status_code=409, detail="Schema inference is already running for this target block.")

    job_id = new_id("schema-inference")
    inference_topic = _temporary_topic(flow, target, job_id)
    try:
        # Compile once before creating the job so an invalid V2 graph reports
        # immediately in the dialog instead of becoming an opaque background
        # failure.  The runner repeats this after reload as a safety boundary.
        build_inference_plan(
            flow,
            context,
            target_block_id=target.id,
            inference_topic=inference_topic,
            job_id=job_id,
        )
    except CompileError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    now = now_iso()
    document: Dict[str, Any] = {
        "id": job_id,
        "flowId": flow.id,
        "targetBlockId": target.id,
        "flowName": flow.name,
        "targetTopic": target_topic,
        "inferenceTopic": inference_topic,
        "status": "queued",
        "messagesCollected": 0,
        "targetMessages": body.targetMessages,
        "nifiProcessGroupId": None,
        "generatedSchema": None,
        "schemaStatus": "Needs Verification",
        "error": None,
        "cleanupError": None,
        "cancelRequested": False,
        "createdAt": now,
        "updatedAt": now,
    }
    await db[COLLECTIONS.schema_inference_jobs].insert_one(document)
    task = asyncio.create_task(run_inference_background(db, job_id, flow_doc))
    _retain_task(task)
    return public_job(document)


@router.get("/{job_id}")
async def get_schema_inference(job_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    job = await get_job(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Schema inference job not found.")
    return public_job(job)


@router.post("/{job_id}/stop")
async def stop_schema_inference(job_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    job = await get_job(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Schema inference job not found.")
    if job.get("status") in {"complete", "failed", "stopped"}:
        return public_job(job)
    updated = await db[COLLECTIONS.schema_inference_jobs].find_one_and_update(
        {"id": job_id, "status": {"$in": sorted(ACTIVE_STATUSES)}},
        {"$set": {"cancelRequested": True, "status": "stopping", "updatedAt": now_iso()}},
        return_document=ReturnDocument.AFTER,
        projection={"_id": 0},
    )
    # If a race moved the job to terminal, return its current state.
    if not updated:
        updated = await get_job(db, job_id)
    return public_job(updated or job)
