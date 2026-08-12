from fastapi import APIRouter, Depends, HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase

from db import get_db
from services import iceberg_sinks as svc
from services import lifecycle_locks
from services.iceberg_sink_config import redact_connector_config


router = APIRouter(prefix="/api/iceberg-sinks", tags=["iceberg-sinks"])


async def _get_sink_or_404(db: AsyncIOMotorDatabase, sink_id: str) -> dict:
    sink = await db.iceberg_sinks.find_one({"id": sink_id}, {"_id": 0})
    if sink is None:
        raise HTTPException(status_code=404, detail=f"Iceberg sink '{sink_id}' not found.")
    return sink


@router.get("/", summary="List Iceberg sinks (cached status)")
async def list_sinks(db: AsyncIOMotorDatabase = Depends(get_db)):
    cursor = db.iceberg_sinks.find({}, {"_id": 0})
    return {"items": [svc.sink_response(doc) async for doc in cursor]}


@router.post("/{sink_id}/preflight", summary="Run preflight checks for an Iceberg sink")
async def preflight_sink(sink_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    sink = await _get_sink_or_404(db, sink_id)
    return await svc.preflight_sink(db, sink)


@router.post("/{sink_id}/enable", summary="Enable an Iceberg sink (preflight + upsert connector)")
async def enable_sink(sink_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    await _get_sink_or_404(db, sink_id)
    await lifecycle_locks.require_unlocked(db, "iceberg_sinks", sink_id)
    return await svc.enable_sink(db, sink_id)


@router.post("/{sink_id}/disable", summary="Disable an Iceberg sink (delete connector, keep doc)")
async def disable_sink(sink_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    await _get_sink_or_404(db, sink_id)
    await lifecycle_locks.require_unlocked(db, "iceberg_sinks", sink_id)
    return await svc.disable_sink(db, sink_id)


@router.post("/{sink_id}/pause", summary="Pause an Iceberg sink connector")
async def pause_sink(sink_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    await _get_sink_or_404(db, sink_id)
    await lifecycle_locks.require_unlocked(db, "iceberg_sinks", sink_id)
    return await svc.pause_sink(db, sink_id)


@router.post("/{sink_id}/resume", summary="Resume an Iceberg sink connector")
async def resume_sink(sink_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    await _get_sink_or_404(db, sink_id)
    await lifecycle_locks.require_unlocked(db, "iceberg_sinks", sink_id)
    return await svc.resume_sink(db, sink_id)


@router.post("/{sink_id}/restart", summary="Restart an Iceberg sink connector")
async def restart_sink(sink_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    await _get_sink_or_404(db, sink_id)
    await lifecycle_locks.require_unlocked(db, "iceberg_sinks", sink_id)
    return await svc.restart_sink(db, sink_id)


@router.get("/{sink_id}/config", summary="Get the (redacted) connector config for an Iceberg sink")
async def get_sink_config(sink_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    sink = await _get_sink_or_404(db, sink_id)
    config = await svc.build_config_for_sink(db, sink)
    return {
        "ok": True,
        "connector_name": sink.get("connector_name"),
        "config": redact_connector_config(config),
    }
