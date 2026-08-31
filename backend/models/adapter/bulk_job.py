"""Bulk flow-verb job model.

A bulk job is one background run of a single verb over many flows, executed
strictly sequentially so the NiFi load is identical to running the verb by
hand N times. It exists because deploy is slow -- `nifi_apply` polls up to
30s for a parameter context and up to 45s for controller services per flow --
so a 20-flow bulk deploy is minutes of work that must not die with the tab
that started it.

Shape borrows deliberately from the two job systems already in this codebase:
  - `models/schema_inference.py`      -- numeric N-of-M progress fields
  - `models/connection_lifecycle_job.py` -- per-item records, owner_instance_id
                                            + heartbeat_at for restart recovery

State lives only in Mongo (never in process memory), so a backend restart
leaves a readable record rather than a job that silently vanished;
`services/runtime_recovery.py` sweeps orphans to `interrupted` on startup.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from services.adapter.common import new_id, now_iso

# Terminal states -- a job in one of these will never change again, so the
# frontend stops polling when it sees one.
TERMINAL_BULK_STATES = ("completed", "failed", "cancelled", "interrupted")

# States a job passes through before running. A job can only be cancelled
# while it is still QUEUED: once the worker picks it up it must run to
# completion, because a half-applied NiFi teardown is worse than a finished
# one.
CANCELLABLE_BULK_STATES = ("queued",)

# Every verb a bulk run may carry. "deploy"/"start"/... dispatch through the
# same `_VERB_HANDLERS` table `run_flow_verb_v2` uses; "enable"/"disable" and
# "delete" are handled separately, mirroring how the frontend's `runBulk`
# splits `setFlowEnabled` from `runFlowVerb`.
BULK_VERBS = (
    "deploy",
    "redeploy",
    "start",
    "pause",
    "resume",
    "stop",
    "stop_clear",
    "undeploy",
    "enable",
    "disable",
    "delete",
)


class BulkJobItem(BaseModel):
    """One flow's slot in the run. `status` starts "pending" and only ever
    moves forward, so the UI can render a stable per-row indicator."""

    id: str = Field(default_factory=lambda: new_id("bulk-item"))
    flow_id: str
    flow_name: str
    status: str = "pending"  # pending | running | succeeded | failed | skipped | cancelled
    error: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None


class BulkJob(BaseModel):
    id: str = Field(default_factory=lambda: new_id("bulk"))
    verb: str
    # queued -> running -> completed | failed
    # queued -> cancelled          (user removed it from the queue)
    # running -> interrupted       (backend restarted mid-run)
    status: str = "queued"
    # Position is derived from created_at ordering, not stored, so cancelling
    # one job cannot leave stale indices on the others.
    label: str = ""  # human summary, e.g. "Undeploy 3 flows"

    # Progress counters. `completed` is what drives the progress bar; it is
    # succeeded + failed, i.e. items that will not be touched again.
    total: int = 0
    completed: int = 0
    succeeded: int = 0
    failed: int = 0

    items: List[BulkJobItem] = Field(default_factory=list)

    # Cooperative cancellation: the runner checks this before each item. It
    # cannot abort a NiFi call already in flight.
    cancel_requested: bool = False

    # Restart recovery, same mechanism as ConnectionLifecycleJob.
    owner_instance_id: Optional[str] = None
    heartbeat_at: Optional[datetime] = None

    error: Optional[str] = None
    created_at: str = Field(default_factory=now_iso)
    updated_at: str = Field(default_factory=now_iso)
    finished_at: Optional[str] = None
    # Optional controls for a flow deletion.  Kept on the durable job so a
    # queued delete has exactly the same intent after a refresh or restart.
    delete_options: Dict[str, bool] = Field(default_factory=dict)

    def to_response(self) -> Dict[str, Any]:
        """camelCase view for the frontend, matching the rest of the v2 API."""
        return {
            "id": self.id,
            "verb": self.verb,
            "label": self.label,
            "status": self.status,
            "cancellable": self.status in CANCELLABLE_BULK_STATES,
            "total": self.total,
            "completed": self.completed,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "cancelRequested": self.cancel_requested,
            "items": [
                {
                    "id": item.id,
                    "flowId": item.flow_id,
                    "flowName": item.flow_name,
                    "status": item.status,
                    "error": item.error,
                    "startedAt": item.started_at,
                    "finishedAt": item.finished_at,
                    "cancellable": (
                        item.status == "pending"
                        and self.status in ("queued", "running")
                    ),
                }
                for item in self.items
            ],
            "error": self.error,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "finishedAt": self.finished_at,
        }


def bulk_job_to_response(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Response view straight from a raw Mongo doc, without a full model
    round-trip. Used on the read paths, which are polled once a second and
    should stay cheap. Older recovery writes used datetime values for some
    timestamps, so normalize those before validating the current model."""
    normalized = dict(doc)

    def timestamp(value: Any) -> Any:
        if not isinstance(value, datetime):
            return value
        value = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    for key in ("created_at", "updated_at", "finished_at"):
        normalized[key] = timestamp(normalized.get(key))
    normalized["items"] = [
        {
            **item,
            "started_at": timestamp(item.get("started_at")),
            "finished_at": timestamp(item.get("finished_at")),
        }
        for item in (normalized.get("items") or [])
    ]
    return BulkJob(**normalized).to_response()
