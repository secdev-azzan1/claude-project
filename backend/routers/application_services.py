from fastapi import APIRouter, Depends, status
from motor.motor_asyncio import AsyncIOMotorDatabase

from db import get_db
from models.application_service import ApplicationServiceCreate, ApplicationServiceUpdate
from services import application_services as svc


router = APIRouter(prefix="/api/application-services", tags=["application-services"])


@router.get("/", summary="List Application Services")
async def list_services(db: AsyncIOMotorDatabase = Depends(get_db)):
    return await svc.list_application_services(db)


@router.post("/", status_code=201, summary="Create Application Service")
async def create_service(data: ApplicationServiceCreate, db: AsyncIOMotorDatabase = Depends(get_db)):
    return await svc.create_application_service(db, data)


@router.get("/{service_id}", summary="Get Application Service")
async def get_service(service_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    return await svc.get_application_service(db, service_id)


@router.put("/{service_id}", summary="Update Application Service")
async def update_service(
    service_id: str,
    data: ApplicationServiceUpdate,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    return await svc.update_application_service(db, service_id, data)


@router.post("/{service_id}/test", summary="Test Application Service")
async def test_service(service_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    return await svc.test_application_service(db, service_id)


@router.delete("/{service_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete Application Service")
async def delete_service(service_id: str, db: AsyncIOMotorDatabase = Depends(get_db)):
    await svc.delete_application_service(db, service_id)
