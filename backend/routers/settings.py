import logging
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase

from db import get_db
from models.settings import PlatformSettings, PlatformSettingsUpdate

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/settings", tags=["settings"])
SETTINGS_SINGLETON_ID = "platform"


async def _get_or_create_settings(db: AsyncIOMotorDatabase) -> dict:
    """Get settings, creating defaults if they don't exist."""
    settings = await db.settings.find_one({"id": SETTINGS_SINGLETON_ID}, {"_id": 0})
    if not settings:
        legacy = await db.settings.find_one({}, {"_id": 0})
        if legacy:
            legacy["id"] = SETTINGS_SINGLETON_ID
            await db.settings.delete_many({})
            await db.settings.insert_one(legacy)
            return legacy
    if not settings:
        defaults = PlatformSettings()
        doc = defaults.dict()
        doc["id"] = SETTINGS_SINGLETON_ID
        await db.settings.insert_one(doc)
        return doc
    return settings


@router.get("/")
async def get_settings(db: AsyncIOMotorDatabase = Depends(get_db)):
    settings = await _get_or_create_settings(db)
    settings.pop("_id", None)
    if isinstance(settings.get("updated_at"), datetime):
        settings["updated_at"] = settings["updated_at"].isoformat()
    return settings


@router.put("/")
async def update_settings(data: PlatformSettingsUpdate, db: AsyncIOMotorDatabase = Depends(get_db)):
    await _get_or_create_settings(db)  # ensure settings doc exists
    updates = {k: v for k, v in data.dict(exclude_none=True).items()}
    if "require_schema_verification" in updates and updates["require_schema_verification"] is False:
        raise HTTPException(status_code=422, detail="require_schema_verification cannot be disabled in this environment.")
    updates["updated_at"] = datetime.utcnow()
    await db.settings.update_one({"id": SETTINGS_SINGLETON_ID}, {"$set": updates}, upsert=True)
    updated = await db.settings.find_one({"id": SETTINGS_SINGLETON_ID}, {"_id": 0})
    if isinstance(updated.get("updated_at"), datetime):
        updated["updated_at"] = updated["updated_at"].isoformat()
    # Log the settings update
    from models.audit import AuditEvent
    event = AuditEvent(action="Updated settings", object_type="Settings", target="platform", status="Success")
    await db.audit_events.insert_one(event.dict())
    return updated
