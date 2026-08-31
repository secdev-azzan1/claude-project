"""Sequential background runner for bulk flow-verb jobs.

Deliberately sequential: one flow finishes before the next starts, so NiFi
sees exactly the load it would from running the verb by hand N times.
Parameter-context updates and controller-service enables in `nifi_apply` are
global-ish operations that this codebase has never exercised concurrently,
and a bulk action is not the place to find out.

Nothing here reimplements lifecycle logic. Each item dispatches through the
same handlers `routers/v2/flows.py` uses for the single-flow path, so the two
routes can never drift apart.
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from models.adapter.bulk_job import BulkJob, BulkJobItem
from services.adapter.common import COLLECTIONS, audit, now_iso
from services.adapter.deployer import lifecycle
from services.adapter.deployer.connect_apply import ConnectApplyError
from services.adapter.deployer.nifi_apply import NifiApplyError

logger = logging.getLogger(__name__)

_QUEUE_OWNER = f"worker-{uuid.uuid4()}"
_QUEUE_LEASE_SECONDS = 120

# Verbs that go through lifecycle's dispatch table. Kept in sync with
# `_VERB_HANDLERS` in routers/v2/flows.py by importing it there rather than
# re-listing the mapping here.
_LIFECYCLE_VERBS = (
    "deploy",
    "redeploy",
    "start",
    "pause",
    "resume",
    "stop",
    "stop_clear",
    "undeploy",
)


async def create_bulk_job(
    db: Any,
    *,
    verb: str,
    flow_docs: List[Dict[str, Any]],
    owner_instance_id: Optional[str] = None,
    label: str = "",
    delete_options: Optional[Dict[str, bool]] = None,
) -> Dict[str, Any]:
    """Insert the job doc in `queued` state. Does NOT run it -- the single
    worker picks it up in submission order."""
    job = BulkJob(
        verb=verb,
        status="queued",
        label=label or f"{verb} {len(flow_docs)} flow(s)",
        total=len(flow_docs),
        items=[
            BulkJobItem(flow_id=str(doc.get("id") or ""), flow_name=str(doc.get("name") or ""))
            for doc in flow_docs
        ],
        owner_instance_id=owner_instance_id,
        heartbeat_at=datetime.now(timezone.utc),
        delete_options=dict(delete_options or {}) if verb == "delete" else {},
    )
    doc = job.model_dump()
    await db[COLLECTIONS.bulk_jobs].insert_one(dict(doc))
    return doc


async def _patch(db: Any, job_id: str, updates: Dict[str, Any]) -> None:
    updates = {**updates, "updated_at": now_iso(), "heartbeat_at": datetime.now(timezone.utc)}
    await db[COLLECTIONS.bulk_jobs].update_one({"id": job_id}, {"$set": updates})


async def _patch_item(db: Any, job_id: str, index: int, updates: Dict[str, Any]) -> None:
    """Positional update of one item. Uses an explicit index rather than an
    array filter so two flows with the same name cannot collide."""
    prefixed = {f"items.{index}.{key}": value for key, value in updates.items()}
    prefixed["updated_at"] = now_iso()
    prefixed["heartbeat_at"] = datetime.now(timezone.utc)
    await db[COLLECTIONS.bulk_jobs].update_one({"id": job_id}, {"$set": prefixed})


async def _run_one(
    db: Any,
    verb: str,
    flow_doc: Dict[str, Any],
    delete_options: Optional[Dict[str, bool]] = None,
) -> None:
    """Execute one verb against one flow, reusing the single-flow code paths.

    Imported lazily: routers/v2/flows.py imports this module, so a top-level
    import here would be circular.
    """
    from models.adapter import Flow
    from routers.v2.flows import (
        _VERB_HANDLERS,
        _get_enable_block_reason,
        _get_verb_block_reason,
        _load_connections,
        _load_gateway,
        _load_schemas,
        _load_services,
    )

    if verb == "delete":
        await lifecycle.delete(db, flow_doc, delete_options=delete_options)
        return

    flow = Flow(**flow_doc)
    if verb in ("enable", "disable"):
        reason = _get_enable_block_reason(flow, verb == "enable")
    else:
        reason = _get_verb_block_reason(
            flow,
            verb,
            await _load_services(db),
            await _load_schemas(db),
            await _load_gateway(db),
            await _load_connections(db),
        )
    if reason:
        raise lifecycle.LifecycleError(reason)

    if verb in ("enable", "disable"):
        await db[COLLECTIONS.flows].update_one(
            {"id": flow_doc.get("id")},
            {"$set": {"enabled": verb == "enable", "updatedAt": now_iso()}},
        )
        return

    handler = _VERB_HANDLERS[verb]
    await handler(db, flow_doc)


async def run_bulk_job(db: Any, job_id: str) -> None:
    """The background task. Never raises -- any escape is recorded on the job
    instead, because there is no caller left to catch it."""
    try:
        await _run_bulk_job_inner(db, job_id)
    except Exception as exc:  # noqa: BLE001 - last line of defence for a detached task
        logger.exception("bulk job %s crashed", job_id)
        try:
            await _patch(
                db,
                job_id,
                {"status": "failed", "error": str(exc)[:500], "finished_at": now_iso()},
            )
        except Exception:  # noqa: BLE001 - nothing useful left to do
            logger.exception("could not record failure for bulk job %s", job_id)


async def _run_bulk_job_inner(db: Any, job_id: str) -> None:
    job = await db[COLLECTIONS.bulk_jobs].find_one({"id": job_id}, {"_id": 0})
    if not job:
        logger.warning("bulk job %s vanished before it started", job_id)
        return

    verb = str(job.get("verb") or "")
    delete_options = dict(job.get("delete_options") or {})
    items = list(job.get("items") or [])
    # The worker claims the job before entering here. Keep this write for
    # direct/test callers, but never turn a cancelled job back into running.
    if job.get("status") == "cancelled":
        return
    await _patch(db, job_id, {"status": "running"})

    succeeded = 0
    failed = 0
    cancelled = 0

    # Cancellation is only allowed while the job remains queued. Once the
    # worker has claimed it, the current flow operation must finish.
    for index, item in enumerate(items):
        flow_id = str(item.get("flow_id") or "")
        flow_name = str(item.get("flow_name") or flow_id)

        # A bulk job can stay running while later items are still pending.
        # Re-read this item immediately before claiming it so cancelling one
        # queued flow does not accidentally turn it into a running operation.
        current_job = await db[COLLECTIONS.bulk_jobs].find_one({"id": job_id}, {"items": 1, "status": 1})
        current_item = ((current_job or {}).get("items") or [])[index] if index < len(((current_job or {}).get("items") or [])) else item
        if current_item.get("status") == "cancelled":
            cancelled += 1
            await _patch(
                db,
                job_id,
                {"succeeded": succeeded, "failed": failed, "completed": succeeded + failed + cancelled},
            )
            logger.info("bulk %s: cancelled queued item %s (%s)", verb, flow_id, flow_name)
            continue
        await _patch_item(db, job_id, index, {"status": "running", "started_at": now_iso()})

        # Re-read the flow each time: an earlier item in this same run may have
        # changed it, and the doc captured at submit time can be minutes stale.
        flow_doc = await db[COLLECTIONS.flows].find_one({"id": flow_id}, {"_id": 0})
        if not flow_doc:
            failed += 1
            await _patch_item(
                db,
                job_id,
                index,
                {"status": "failed", "error": "Flow no longer exists.", "finished_at": now_iso()},
            )
        else:
            try:
                if verb == "delete":
                    await _run_one(db, verb, flow_doc, delete_options)
                else:
                    # Keep the established three-argument seam for the
                    # non-delete verb handlers and their test doubles.
                    await _run_one(db, verb, flow_doc)
                succeeded += 1
                await _patch_item(db, job_id, index, {"status": "succeeded", "finished_at": now_iso()})
            except lifecycle.DeployPreflightFailed as exc:
                failed += 1
                failed_rows = [
                    f"{row.get('label')}: {row.get('detail')}"
                    for row in (exc.rows or [])
                    if not row.get("ok")
                ]
                detail = "; ".join(failed_rows) or str(exc)
                await _patch_item(
                    db,
                    job_id,
                    index,
                    {"status": "failed", "error": detail[:500], "finished_at": now_iso()},
                )
            except (NifiApplyError, ConnectApplyError, lifecycle.LifecycleError) as exc:
                failed += 1
                await _patch_item(
                    db, job_id, index, {"status": "failed", "error": str(exc)[:500], "finished_at": now_iso()}
                )
            except Exception as exc:  # noqa: BLE001 - one bad flow must not end the run
                failed += 1
                logger.exception("bulk %s failed for flow %s", verb, flow_id)
                await _patch_item(
                    db, job_id, index, {"status": "failed", "error": str(exc)[:500], "finished_at": now_iso()}
                )

        # Counters are written after every item so the progress bar advances
        # live rather than jumping at the end.
        await _patch(
            db,
            job_id,
            {"succeeded": succeeded, "failed": failed, "completed": succeeded + failed + cancelled},
        )
        logger.info("bulk %s: %s/%s (%s)", verb, succeeded + failed, len(items), flow_name)

    await _patch(
        db,
        job_id,
        {
            # "completed" means the run finished, not that every item passed --
            # per-item failures are in `items` and counted in `failed`.
            "status": "completed",
            "succeeded": succeeded,
            "failed": failed,
            "completed": succeeded + failed + cancelled,
            "finished_at": now_iso(),
        },
    )
    await audit(
        db,
        f"Bulk {verb} finished",
        f"{succeeded} succeeded, {failed} failed",
        status="Warning" if failed else "Success",
        object="Flow",
    )


# ------------------------------------------------------------------ worker
#
# Exactly one worker drains the queue, so jobs run strictly one at a time in
# submission order. That is the same sequential guarantee a single bulk run
# already had, now extended across separately-submitted jobs: clicking
# undeploy on three flows in a row enqueues three jobs that run back to back
# rather than racing each other on NiFi.

_worker_task: Optional["asyncio.Task[None]"] = None


async def _next_queued(db: Any) -> Optional[Dict[str, Any]]:
    return await db[COLLECTIONS.bulk_jobs].find_one(
        {"status": "queued"}, {"_id": 0}, sort=[("created_at", 1)]
    )


async def _acquire_queue_lease(db: Any) -> bool:
    """Acquire the single queue lease across backend processes.

    The in-memory test DBs do not provide this optional collection; those
    callers already have one event loop and safely use the local task guard.
    """
    try:
        collection = db[COLLECTIONS.bulk_queue_leases]
    except AttributeError:
        return True
    from pymongo import ReturnDocument

    now = datetime.now(timezone.utc)
    doc = await collection.find_one_and_update(
        {
            "id": "flow-operations",
            "$or": [
                {"owner_id": _QUEUE_OWNER},
                {"lease_until": {"$lt": now}},
                {"lease_until": {"$exists": False}},
            ],
        },
        {"$set": {"owner_id": _QUEUE_OWNER, "lease_until": now + timedelta(seconds=_QUEUE_LEASE_SECONDS)}},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    return bool(doc and doc.get("owner_id") == _QUEUE_OWNER)


async def _release_queue_lease(db: Any) -> None:
    try:
        collection = db[COLLECTIONS.bulk_queue_leases]
    except AttributeError:
        return
    await collection.update_one(
        {"id": "flow-operations", "owner_id": _QUEUE_OWNER},
        {"$set": {"owner_id": None, "lease_until": datetime.now(timezone.utc)}},
    )


async def _drain_queue(db: Any) -> bool:
    """Run queued jobs oldest-first until the queue is empty."""
    if not await _acquire_queue_lease(db):
        return False
    try:
        while True:
            job = await _next_queued(db)
            if not job:
                return True
            job_id = str(job.get("id") or "")
            # Claim it before running so a second worker (or a restart racing
            # this one) cannot pick up the same job.
            claimed = await db[COLLECTIONS.bulk_jobs].update_one(
                {"id": job_id, "status": "queued"},
                {"$set": {"status": "running", "updated_at": now_iso()}},
            )
            if getattr(claimed, "modified_count", 1) == 0:
                continue
            await run_bulk_job(db, job_id)
            await _acquire_queue_lease(db)
    finally:
        await _release_queue_lease(db)


async def _worker_loop(db: Any) -> None:
    global _worker_task
    drained = False
    try:
        drained = await _drain_queue(db)
    except Exception:  # noqa: BLE001 - a detached worker has no caller
        logger.exception("bulk queue worker crashed")
    finally:
        _worker_task = None
        # A job enqueued while we were finishing would otherwise sit forever.
        try:
            if drained and await _next_queued(db):
                ensure_worker(db)
        except Exception:  # noqa: BLE001
            logger.exception("could not re-arm bulk queue worker")


def ensure_worker(db: Any) -> None:
    """Start the drain loop if it is not already running. Safe to call on
    every enqueue and at app startup."""
    global _worker_task
    if _worker_task is not None and not _worker_task.done():
        return
    _worker_task = asyncio.create_task(_worker_loop(db))


def launch_bulk_job(db: Any, job_id: str) -> None:
    """Compatibility entry point used by the router and older tests.

    The queue worker drains all queued jobs, so the id is intentionally only
    used to ensure a worker exists; ordering is determined by created_at.
    """
    ensure_worker(db)


def worker_is_running() -> bool:
    return _worker_task is not None and not _worker_task.done()
