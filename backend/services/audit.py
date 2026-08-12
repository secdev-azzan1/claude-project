import json
from models.audit import AuditEvent
from motor.motor_asyncio import AsyncIOMotorDatabase


async def log_audit(db: AsyncIOMotorDatabase, action: str, object_type: str, target: str, status: str,
                    details=None, *, before=None, after=None) -> None:
    """
    Log an audit event to the database.

    Args:
        db: AsyncIOMotorDatabase instance
        action: The audit action (e.g., 'create', 'update', 'delete')
        object_type: The type of object being audited (e.g., 'Connection', 'Source', 'Schema', 'Flow', 'Settings')
        target: The target object identifier
        status: The status of the action (e.g., 'Success', 'Failed', 'Info')
        details: Optional plain string (legacy) or dict with additional details
        before: Optional dict with state before the change
        after: Optional dict with state after the change
    """
    # If details is a dict, or before/after are provided, serialize to JSON
    if isinstance(details, dict) or before is not None or after is not None:
        combined = {}
        if isinstance(details, dict):
            combined.update(details)
        if before is not None:
            combined['before'] = before
        if after is not None:
            combined['after'] = after
        details_str = json.dumps(combined, default=str)
    else:
        # details is None or already a string
        details_str = details

    event = AuditEvent(action=action, object_type=object_type, target=target or "", status=status, details=details_str)
    await db.audit_events.insert_one(event.dict())
