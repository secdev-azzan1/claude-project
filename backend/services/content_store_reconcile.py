"""Content store reconciliation service."""
from services import content_store


async def reconcile_content_store(db) -> dict:
    """
    Validate content store and rematerialize if needed.
    Returns a summary dict {validated: bool, rematerialized: bool, report: ...}.
    Never raises (best-effort).
    """
    try:
        # Validate the content store
        validation_report = await content_store.validate_content_store(db)

        # Check if there are any issues
        has_issues = validation_report.missing or validation_report.stale or validation_report.extra

        rematerialized = False
        if has_issues:
            # Rematerialize if there are issues
            rematerialization_report = await content_store.rematerialize_content_store(db)
            rematerialized = True
            final_report = rematerialization_report.to_dict()
        else:
            final_report = validation_report.to_dict()

        return {
            "validated": validation_report.ok,
            "rematerialized": rematerialized,
            "report": final_report
        }
    except Exception as exc:
        # Best-effort: return error summary instead of raising
        return {
            "validated": False,
            "rematerialized": False,
            "report": {
                "ok": False,
                "root": "",
                "written": [],
                "deleted": [],
                "missing": [],
                "stale": [],
                "extra": [],
                "errors": [str(exc)],
                "counts": {},
            }
        }
