"""Operation locks for flow and service lifecycle management."""
from typing import Optional, Any
from fastapi import HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase


async def lock_records(
    db: AsyncIOMotorDatabase,
    job_id: str,
    *,
    flow_ids: Optional[list[str]] = None,
    service_ids: Optional[list[str]] = None,
    sink_ids: Optional[list[str]] = None,
) -> None:
    """Set lifecycle_lock_job_id on the given records.

    Args:
        db: Motor async database
        job_id: The job ID to set as the lock
        flow_ids: Optional list of flow IDs to lock
        service_ids: Optional list of nifi_global_service IDs to lock
        sink_ids: Optional list of iceberg_sinks IDs to lock
    """
    if flow_ids:
        await db.flows.update_many(
            {"id": {"$in": flow_ids}},
            {"$set": {"lifecycle_lock_job_id": job_id}},
        )

    if service_ids:
        await db.nifi_global_services.update_many(
            {"id": {"$in": service_ids}},
            {"$set": {"lifecycle_lock_job_id": job_id}},
        )

    if sink_ids:
        await db.iceberg_sinks.update_many(
            {"id": {"$in": sink_ids}},
            {"$set": {"lifecycle_lock_job_id": job_id}},
        )


async def unlock_job(db: AsyncIOMotorDatabase, job_id: str) -> int:
    """Clear lifecycle_lock_job_id where == job_id, across flows + nifi_global_services + iceberg_sinks.

    Args:
        db: Motor async database
        job_id: The job ID to unlock

    Returns:
        The total number of records unlocked (flows + nifi_global_services + iceberg_sinks)
    """
    flows_result = await db.flows.update_many(
        {"lifecycle_lock_job_id": job_id},
        {"$unset": {"lifecycle_lock_job_id": ""}},
    )

    services_result = await db.nifi_global_services.update_many(
        {"lifecycle_lock_job_id": job_id},
        {"$unset": {"lifecycle_lock_job_id": ""}},
    )

    sinks_result = await db.iceberg_sinks.update_many(
        {"lifecycle_lock_job_id": job_id},
        {"$unset": {"lifecycle_lock_job_id": ""}},
    )

    return flows_result.modified_count + services_result.modified_count + sinks_result.modified_count


async def is_locked(
    db: AsyncIOMotorDatabase,
    collection_name: str,
    obj_id: str,
) -> Optional[str]:
    """Return the locking job_id or None if not locked.

    Args:
        db: Motor async database
        collection_name: Name of the collection (e.g., "flows", "nifi_global_services")
        obj_id: The object ID to check

    Returns:
        The locking job_id if locked, None otherwise
    """
    collection = getattr(db, collection_name)
    doc = await collection.find_one({"id": obj_id}, {"lifecycle_lock_job_id": 1, "_id": 0})
    if not doc:
        return None
    return doc.get("lifecycle_lock_job_id")


async def require_unlocked(
    db: AsyncIOMotorDatabase,
    collection_name: str,
    obj_id: str,
) -> None:
    """Raise HTTPException(423, ...) if locked.

    Args:
        db: Motor async database
        collection_name: Name of the collection (e.g., "flows", "nifi_global_services")
        obj_id: The object ID to check

    Raises:
        HTTPException: With status 423 if the record is locked
    """
    lock_job_id = await is_locked(db, collection_name, obj_id)
    if lock_job_id:
        raise HTTPException(
            status_code=423,
            detail={
                "message": "This record is locked by an active connection lifecycle job.",
                "job_id": lock_job_id,
            },
        )
