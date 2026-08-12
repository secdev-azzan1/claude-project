from fastapi import APIRouter, Depends, Query, status
from motor.motor_asyncio import AsyncIOMotorDatabase

from db import get_db
from models.nifi_global_service import NifiGlobalServiceCreate, NifiGlobalServiceUpdate
from services import nifi_global_services as svc


router = APIRouter(prefix="/api/nifi-services", tags=["nifi-services"])


@router.get("/types", summary="List live NiFi controller service types")
async def list_types(
    service_kind: str | None = Query(default=None),
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    return {
        "items": await svc.list_controller_service_types(db, service_kind=service_kind),
    }


@router.get("/", summary="List NiFi global services")
async def list_services(db: AsyncIOMotorDatabase = Depends(get_db)):
    return {"items": await svc.list_global_services(db)}


@router.post("/", status_code=201, summary="Create a NiFi global controller service")
async def create_service(
    data: NifiGlobalServiceCreate,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    return await svc.create_global_service(
        db,
        name=data.name,
        service_kind=data.service_kind,
        nifi_type=data.nifi_type,
        properties=data.properties,
        is_default=data.is_default,
    )


@router.get("/{service_id}", summary="Get live NiFi global service config")
async def get_service(service_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    return await svc.get_global_service_config(db, service_id)


@router.put("/{service_id}", summary="Update NiFi global service metadata/properties")
async def update_service(
    service_id: str,
    data: NifiGlobalServiceUpdate,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    return await svc.update_global_service(
        db,
        service_id,
        name=data.name,
        properties=data.properties,
        is_default=data.is_default,
    )


@router.post("/{service_id}/enable", summary="Enable a NiFi global service")
async def enable_service(service_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    return await svc.set_global_service_run_status(db, service_id, "ENABLED")


@router.post("/{service_id}/disable", summary="Disable a NiFi global service")
async def disable_service(service_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    return await svc.set_global_service_run_status(db, service_id, "DISABLED")


@router.post("/{service_id}/set-default", summary="Mark a NiFi global service as default")
async def set_default_service(service_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    return await svc.update_global_service(db, service_id, is_default=True)


@router.delete("/{service_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete a NiFi global service")
async def delete_service(service_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    await svc.delete_global_service(db, service_id)
