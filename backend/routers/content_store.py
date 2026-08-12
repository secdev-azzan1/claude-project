from fastapi import APIRouter, Depends
from motor.motor_asyncio import AsyncIOMotorDatabase

from db import get_db
from services import content_store


router = APIRouter(prefix="/api/content-store", tags=["content-store"])


@router.get("/validate", summary="Validate content-store mirror")
async def validate_content_store_status(db: AsyncIOMotorDatabase = Depends(get_db)):
    report = await content_store.validate_content_store(db)
    return report.to_dict()


@router.post("/rematerialize", summary="Rebuild content-store mirror from MongoDB")
async def rematerialize_content_store_endpoint(db: AsyncIOMotorDatabase = Depends(get_db)):
    report = await content_store.rematerialize_content_store(db)
    return report.to_dict()
