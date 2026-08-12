"""Orphaned artifact registry service."""
from models.orphaned_artifact import OrphanedArtifact
from datetime import datetime


async def record_orphan(db, *, system, external_id, kind, connection_id=None, reason, job_id=None) -> None:
    """
    Record an orphaned artifact. Idempotent on (system, external_id, kind).
    Never raises (best-effort).
    """
    try:
        orphan = OrphanedArtifact(
            system=system,
            external_id=external_id,
            kind=kind,
            connection_id=connection_id,
            reason=reason,
            job_id=job_id,
        )

        # Upsert on system, external_id, kind
        await db.orphaned_artifacts.update_one(
            {
                "system": system,
                "external_id": external_id,
                "kind": kind,
            },
            {
                "$set": orphan.dict()
            },
            upsert=True
        )
    except Exception:
        # Best-effort: silently ignore any errors
        pass
