import asyncio
from datetime import datetime, timedelta

from services.runtime_recovery import reconcile_runtime_state
from tests.resilience.conftest import ResilienceFakeDB


def test_reconcile_marks_orphaned_active_job_failed_and_unlocks_flow():
    now = datetime.utcnow()
    db = ResilienceFakeDB()
    db.flows.docs.append(
        {"id": "flow-1", "schema_inference_active": True},
    )
    db.schema_inference_jobs.docs.append(
        {
            "id": "job-1",
            "flow_id": "flow-1",
            "status": "collecting",
            "worker_instance_id": "old-process",
            "heartbeat_at": now - timedelta(minutes=2),
            "inference_topic": "",
        },
    )

    report = asyncio.run(
        reconcile_runtime_state(
            db,
            instance_id="new-process",
            now=now,
        ),
    )

    assert db.schema_inference_jobs.docs[0]["status"] == "failed"
    assert "backend restarted" in db.schema_inference_jobs.docs[0]["error"].lower()
    assert db.flows.docs[0]["schema_inference_active"] is False
    assert report.inference_jobs_recovered == 1
    assert report.flow_locks_released == 1


def test_reconcile_does_not_touch_recent_job_owned_by_current_process():
    now = datetime.utcnow()
    db = ResilienceFakeDB()
    db.flows.docs.append(
        {"id": "flow-1", "schema_inference_active": True},
    )
    db.schema_inference_jobs.docs.append(
        {
            "id": "job-1",
            "flow_id": "flow-1",
            "status": "collecting",
            "worker_instance_id": "current-process",
            "heartbeat_at": now,
        },
    )

    report = asyncio.run(
        reconcile_runtime_state(
            db,
            instance_id="current-process",
            now=now,
        ),
    )

    assert db.schema_inference_jobs.docs[0]["status"] == "collecting"
    assert db.flows.docs[0]["schema_inference_active"] is True
    assert report.inference_jobs_recovered == 0


def test_reconcile_clears_terminal_job_nifi_pg_reference():
    now = datetime.utcnow()
    db = ResilienceFakeDB()
    db.schema_inference_jobs.docs.append(
        {
            "id": "job-1",
            "flow_id": "flow-1",
            "status": "complete",
            "worker_instance_id": "old-process",
            "heartbeat_at": now - timedelta(days=1),
            "nifi_pg_id": "pg-1",
        },
    )

    report = asyncio.run(
        reconcile_runtime_state(
            db,
            instance_id="new-process",
            now=now,
        ),
    )

    assert db.schema_inference_jobs.docs[0]["status"] == "complete"
    assert db.schema_inference_jobs.docs[0]["nifi_pg_id"] is None
    assert report.inference_jobs_recovered == 0
