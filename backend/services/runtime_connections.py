from typing import Dict, List, Tuple

from fastapi import HTTPException
from motor.motor_asyncio import AsyncIOMotorDatabase

from services.connection_resolver import resolve_connection

ESSENTIAL_CONNECTION_TYPES: Tuple[str, ...] = ("nifi", "kafka", "apicurio")
SERVICE_LABELS: Dict[str, str] = {
    "nifi": "NiFi",
    "kafka": "Kafka",
    "apicurio": "Apicurio",
}


async def get_missing_connection_types(
    db: AsyncIOMotorDatabase,
    required: Tuple[str, ...] = ESSENTIAL_CONNECTION_TYPES,
) -> List[str]:
    missing: List[str] = []
    for connection_type in required:
        if not await resolve_connection(db, connection_type):
            missing.append(connection_type)
    return missing


def format_connection_labels(connection_types: List[str]) -> str:
    return ", ".join(SERVICE_LABELS.get(value, value) for value in connection_types)


async def require_runtime_connections(
    db: AsyncIOMotorDatabase,
    *,
    action: str,
) -> None:
    missing = await get_missing_connection_types(db)
    if missing:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Cannot {action} because required service connection(s) are missing: "
                f"{format_connection_labels(missing)}."
            ),
        )
