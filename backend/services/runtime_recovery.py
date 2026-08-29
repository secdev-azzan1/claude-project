from __future__ import annotations

import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any

from services.connection_resolver import resolve_connection

logger = logging.getLogger(__name__)

APP_INSTANCE_ID = str(uuid.uuid4())
ACTIVE_INFERENCE_STATES = {
    "deploying_nifi",
    "nifi_running",
    "collecting",
    "inferring",
}
TERMINAL_INFERENCE_STATES = {
    "complete",
    "failed",
    "stopped",
    "cancelled",
}


@dataclass
class RuntimeRecoveryReport:
    inference_jobs_recovered: int = 0
    bulk_jobs_recovered: int = 0
    flow_locks_released: int = 0
    cleaned_nifi_process_groups: list[str] = field(default_factory=list)
    cleaned_kafka_topics: list[str] = field(default_factory=list)
    cleanup_errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _is_orphaned(
    job: dict[str, Any],
    *,
    instance_id: str,
    now: datetime,
    stale_after_seconds: int,
) -> bool:
    if job.get("status") not in ACTIVE_INFERENCE_STATES:
        return False
    worker_instance_id = str(job.get("worker_instance_id") or "")
    heartbeat = job.get("heartbeat_at") or job.get("updated_at")
    stale_before = now - timedelta(seconds=stale_after_seconds)
    heartbeat_is_stale = not isinstance(heartbeat, datetime) or heartbeat < stale_before
    return worker_instance_id != instance_id or heartbeat_is_stale


async def reconcile_runtime_state(
    db: Any,
    *,
    instance_id: str,
    now: datetime,
    stale_after_seconds: int = 30,
) -> RuntimeRecoveryReport:
    report = RuntimeRecoveryReport()

    # Stale-lock recovery: clear lifecycle locks on orphaned connection lifecycle jobs
    lifecycle_jobs = await db.connection_lifecycle_jobs.find(
        {"status": "running"},
        {"_id": 0},
    ).to_list(1000)

    for job in lifecycle_jobs:
        job_id = str(job.get("id") or "")
        owner_instance_id = str(job.get("owner_instance_id") or "")
        heartbeat = job.get("heartbeat_at") or job.get("updated_at")
        stale_before = now - timedelta(seconds=stale_after_seconds)
        heartbeat_is_stale = not isinstance(heartbeat, datetime) or heartbeat < stale_before

        # Mark as interrupted if owner mismatch or stale heartbeat
        if owner_instance_id != instance_id or heartbeat_is_stale:
            await db.connection_lifecycle_jobs.update_one(
                {"id": job_id},
                {
                    "$set": {
                        "status": "interrupted",
                        "updated_at": now,
                    }
                },
            )
            # Release locks held by this job
            if job_id:
                from services.lifecycle_locks import unlock_job
                report.flow_locks_released += await unlock_job(db, job_id)

    # Bulk flow-verb jobs run as detached in-process tasks, so a restart kills
    # the runner while the doc still says "running". Mark those interrupted --
    # there is no safe automatic resume, since we cannot know whether the
    # in-flight flow's NiFi call landed.
    # getattr rather than db[...]: the resilience test fakes expose collections
    # as plain attributes and have no __getitem__, and older databases predate
    # this collection entirely.
    bulk_collection = getattr(db, "bulk_jobs_v2", None)
    if bulk_collection is not None:
        bulk_jobs = await bulk_collection.find(
            {"status": {"$in": ["pending", "running"]}},
            {"_id": 0},
        ).to_list(1000)

        for job in bulk_jobs:
            job_id = str(job.get("id") or "")
            owner_instance_id = str(job.get("owner_instance_id") or "")
            heartbeat = job.get("heartbeat_at") or job.get("updated_at")
            stale_before = now - timedelta(seconds=stale_after_seconds)
            heartbeat_is_stale = not isinstance(heartbeat, datetime) or heartbeat < stale_before

            if owner_instance_id != instance_id or heartbeat_is_stale:
                await bulk_collection.update_one(
                    {"id": job_id},
                    {
                        "$set": {
                            "status": "interrupted",
                            "error": "The backend restarted while this run was in progress.",
                            "updated_at": now,
                        }
                    },
                )
                report.bulk_jobs_recovered += 1

    terminal_jobs = await db.schema_inference_jobs.find(
        {
            "status": {"$in": sorted(TERMINAL_INFERENCE_STATES)},
            "nifi_pg_id": {"$nin": [None, ""]},
        },
        {"_id": 0},
    ).to_list(1000)

    for job in terminal_jobs:
        pg_id = str(job.get("nifi_pg_id") or "")
        try:
            nifi_conn = await resolve_connection(db, "nifi")
            if nifi_conn:
                from services.schema_inference_runner import (
                    _delete_inference_nifi_pg,
                )

                await _delete_inference_nifi_pg(nifi_conn, pg_id)
                report.cleaned_nifi_process_groups.append(pg_id)
            await db.schema_inference_jobs.update_one(
                {"id": job.get("id")},
                {"$set": {"nifi_pg_id": None, "updated_at": now}},
            )
        except Exception as exc:
            report.cleanup_errors.append(f"Terminal NiFi PG {pg_id}: {str(exc)[:180]}")

    jobs = await db.schema_inference_jobs.find(
        {"status": {"$in": sorted(ACTIVE_INFERENCE_STATES)}},
        {"_id": 0},
    ).to_list(1000)

    for job in jobs:
        if not _is_orphaned(
            job,
            instance_id=instance_id,
            now=now,
            stale_after_seconds=stale_after_seconds,
        ):
            continue

        error = "Schema inference was interrupted because the backend restarted."
        await db.schema_inference_jobs.update_one(
            {"id": job.get("id"), "status": {"$in": sorted(ACTIVE_INFERENCE_STATES)}},
            {
                "$set": {
                    "status": "failed",
                    "error": error,
                    "updated_at": now,
                    "heartbeat_at": now,
                }
            },
        )
        report.inference_jobs_recovered += 1

        flow_id = job.get("flow_id")
        if flow_id:
            await db.flows.update_one(
                {"id": flow_id},
                {
                    "$set": {
                        "schema_inference_active": False,
                        "updated_at": now,
                    }
                },
            )
            report.flow_locks_released += 1

        pg_id = str(job.get("nifi_pg_id") or "")
        if pg_id:
            try:
                nifi_conn = await resolve_connection(db, "nifi")
                if nifi_conn:
                    from services.schema_inference_runner import (
                        _delete_inference_nifi_pg,
                    )

                    await _delete_inference_nifi_pg(nifi_conn, pg_id)
                    report.cleaned_nifi_process_groups.append(pg_id)
            except Exception as exc:
                report.cleanup_errors.append(f"NiFi PG {pg_id}: {str(exc)[:180]}")

        inference_topic = str(job.get("inference_topic") or "")
        if inference_topic:
            try:
                kafka_conn = await resolve_connection(db, "kafka")
                if kafka_conn:
                    from services.schema_inference_runner import (
                        _delete_kafka_inference_topic,
                    )

                    cleanup_error = await _delete_kafka_inference_topic(
                        inference_topic,
                        kafka_conn,
                    )
                    if cleanup_error:
                        report.cleanup_errors.append(
                            f"Kafka topic {inference_topic}: {cleanup_error}",
                        )
                    else:
                        report.cleaned_kafka_topics.append(inference_topic)
            except Exception as exc:
                report.cleanup_errors.append(
                    f"Kafka topic {inference_topic}: {str(exc)[:180]}",
                )

    return report
