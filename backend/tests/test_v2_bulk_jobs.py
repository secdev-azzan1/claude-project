"""Tests for background bulk flow-verb jobs (backend/routers/v2/flows.py's
/bulk endpoints + backend/services/adapter/bulk_runner.py).

Uses the same TestClient + in-memory FakeDB approach as tests/test_v2_services.py.
The lifecycle handlers are monkeypatched throughout -- this suite is about the
JOB machinery (ordering, counters, per-item status, cancellation, recovery),
not about NiFi, and no real deployment code should run here.
"""
import asyncio
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from db import get_db
from routers.v2 import flows as flows_router
from services.adapter import bulk_runner
from tests.resilience.conftest import FaultInjectingCollection


class FakeDB:
    def __init__(self):
        self.flows_v2 = FaultInjectingCollection(unique_fields=("id",))
        self.audit_v2 = FaultInjectingCollection()
        self.bulk_jobs_v2 = FaultInjectingCollection(unique_fields=("id",))

    def __getitem__(self, name):
        return getattr(self, name)


def _make_client(fake_db: FakeDB) -> TestClient:
    app = FastAPI()
    app.include_router(flows_router.router)
    app.dependency_overrides[get_db] = lambda: fake_db
    return TestClient(app)


async def _seed_flows(fake_db: FakeDB, names: List[str]) -> None:
    for i, name in enumerate(names):
        await fake_db.flows_v2.insert_one(
            {"id": f"flow-{i}", "name": name, "state": "Stopped", "enabled": False, "blocks": [], "topics": []}
        )


def _seed(fake_db: FakeDB, names: List[str]) -> None:
    asyncio.get_event_loop().run_until_complete(_seed_flows(fake_db, names))


@pytest.fixture()
def fake_db():
    db = FakeDB()
    asyncio.run(_seed_flows(db, ["alpha", "bravo", "charlie"]))
    return db


def _job(fake_db: FakeDB) -> Dict[str, Any]:
    return fake_db.bulk_jobs_v2.docs[0]


# ------------------------------------------------------------- start / shape


def test_start_bulk_job_returns_202_and_creates_job(fake_db, monkeypatch):
    calls: List[str] = []

    async def fake_run_one(db, verb, flow_doc):
        calls.append(flow_doc["id"])

    monkeypatch.setattr(bulk_runner, "_run_one", fake_run_one)
    client = _make_client(fake_db)

    resp = client.post(
        "/api/v2/flows/bulk", json={"verb": "start", "flowIds": ["flow-0", "flow-1", "flow-2"]}
    )
    assert resp.status_code == 202, resp.text
    job_id = resp.json()["jobId"]
    assert job_id.startswith("bulk-")

    job = _job(fake_db)
    assert job["verb"] == "start"
    assert job["total"] == 3
    assert [item["flow_name"] for item in job["items"]] == ["alpha", "bravo", "charlie"]


def test_items_preserve_caller_ordering(fake_db, monkeypatch):
    monkeypatch.setattr(bulk_runner, "_run_one", lambda db, verb, doc: asyncio.sleep(0))
    client = _make_client(fake_db)

    client.post("/api/v2/flows/bulk", json={"verb": "stop", "flowIds": ["flow-2", "flow-0", "flow-1"]})
    assert [i["flow_id"] for i in _job(fake_db)["items"]] == ["flow-2", "flow-0", "flow-1"]


def test_unknown_verb_is_422(fake_db):
    client = _make_client(fake_db)
    resp = client.post("/api/v2/flows/bulk", json={"verb": "explode", "flowIds": ["flow-0"]})
    assert resp.status_code == 422
    assert "Unknown bulk verb" in resp.json()["detail"]


def test_empty_selection_is_422(fake_db):
    client = _make_client(fake_db)
    resp = client.post("/api/v2/flows/bulk", json={"verb": "start", "flowIds": []})
    assert resp.status_code == 422


def test_all_unknown_flow_ids_is_404(fake_db):
    client = _make_client(fake_db)
    resp = client.post("/api/v2/flows/bulk", json={"verb": "start", "flowIds": ["nope-1"]})
    assert resp.status_code == 404


def test_second_submission_is_queued(fake_db, monkeypatch):
    monkeypatch.setattr(bulk_runner, "launch_bulk_job", lambda db, job_id: None)  # leave both queued
    client = _make_client(fake_db)

    first = client.post("/api/v2/flows/bulk", json={"verb": "start", "flowIds": ["flow-0"]})
    assert first.status_code == 202
    second = client.post("/api/v2/flows/bulk", json={"verb": "stop", "flowIds": ["flow-1"]})
    assert second.status_code == 202
    jobs = fake_db.bulk_jobs_v2.docs
    assert [job["verb"] for job in jobs] == ["start", "stop"]
    assert all(job["status"] == "queued" for job in jobs)


# ------------------------------------------------------------------- runner


def _run_job_to_completion(fake_db: FakeDB, verb: str, flow_ids: List[str]) -> Dict[str, Any]:
    """Create + run a job synchronously, bypassing the detached task so the
    assertions see a finished job rather than racing it."""

    async def go():
        docs = [d for fid in flow_ids for d in fake_db.flows_v2.docs if d["id"] == fid]
        job = await bulk_runner.create_bulk_job(fake_db, verb=verb, flow_docs=docs, owner_instance_id="test")
        await bulk_runner.run_bulk_job(fake_db, job["id"])
        return await fake_db.bulk_jobs_v2.find_one({"id": job["id"]}, {"_id": 0})

    return asyncio.run(go())


def test_runner_marks_every_item_succeeded_and_counts(fake_db, monkeypatch):
    seen: List[str] = []

    async def fake_run_one(db, verb, flow_doc):
        seen.append(flow_doc["id"])

    monkeypatch.setattr(bulk_runner, "_run_one", fake_run_one)
    job = _run_job_to_completion(fake_db, "start", ["flow-0", "flow-1", "flow-2"])

    assert seen == ["flow-0", "flow-1", "flow-2"]  # strictly sequential, in order
    assert job["status"] == "completed"
    assert (job["succeeded"], job["failed"], job["completed"]) == (3, 0, 3)
    assert all(item["status"] == "succeeded" for item in job["items"])


def test_one_failing_flow_does_not_stop_the_run(fake_db, monkeypatch):
    async def fake_run_one(db, verb, flow_doc):
        if flow_doc["id"] == "flow-1":
            raise RuntimeError("NiFi said no")

    monkeypatch.setattr(bulk_runner, "_run_one", fake_run_one)
    job = _run_job_to_completion(fake_db, "deploy", ["flow-0", "flow-1", "flow-2"])

    # This is the behaviour the old in-browser loop had, and it must survive.
    assert job["status"] == "completed"
    assert (job["succeeded"], job["failed"]) == (2, 1)
    statuses = [i["status"] for i in job["items"]]
    assert statuses == ["succeeded", "failed", "succeeded"]
    assert "NiFi said no" in job["items"][1]["error"]


def test_missing_flow_is_recorded_not_crashed(fake_db, monkeypatch):
    async def fake_run_one(db, verb, flow_doc):
        return None

    monkeypatch.setattr(bulk_runner, "_run_one", fake_run_one)

    async def go():
        docs = [d for d in fake_db.flows_v2.docs if d["id"] == "flow-0"]
        job = await bulk_runner.create_bulk_job(fake_db, verb="start", flow_docs=docs, owner_instance_id="t")
        # Delete the flow after the job was created but before it runs.
        await fake_db.flows_v2.delete_one({"id": "flow-0"})
        await bulk_runner.run_bulk_job(fake_db, job["id"])
        return await fake_db.bulk_jobs_v2.find_one({"id": job["id"]}, {"_id": 0})

    job = asyncio.run(go())
    assert job["failed"] == 1
    assert job["items"][0]["error"] == "Flow no longer exists."


def test_counters_total_matches_succeeded_plus_failed(fake_db, monkeypatch):
    async def fake_run_one(db, verb, flow_doc):
        if flow_doc["id"] != "flow-0":
            raise RuntimeError("boom")

    monkeypatch.setattr(bulk_runner, "_run_one", fake_run_one)
    job = _run_job_to_completion(fake_db, "stop", ["flow-0", "flow-1", "flow-2"])
    assert job["succeeded"] + job["failed"] == job["total"] == 3


# -------------------------------------------------------------- cancellation


def test_running_job_cannot_be_cancelled_mid_operation(fake_db, monkeypatch):
    processed: List[str] = []

    async def fake_run_one(db, verb, flow_doc):
        processed.append(flow_doc["id"])
        # A stale flag from another caller must not interrupt a running job.
        await fake_db.bulk_jobs_v2.update_one(
            {"id": fake_db.bulk_jobs_v2.docs[0]["id"]}, {"$set": {"cancel_requested": True}}
        )

    monkeypatch.setattr(bulk_runner, "_run_one", fake_run_one)
    job = _run_job_to_completion(fake_db, "deploy", ["flow-0", "flow-1", "flow-2"])

    assert processed == ["flow-0", "flow-1", "flow-2"]
    assert job["status"] == "completed"
    assert [i["status"] for i in job["items"]] == ["succeeded", "succeeded", "succeeded"]


def test_cancel_endpoint_sets_the_flag(fake_db, monkeypatch):
    monkeypatch.setattr(bulk_runner, "launch_bulk_job", lambda db, job_id: None)
    client = _make_client(fake_db)
    job_id = client.post("/api/v2/flows/bulk", json={"verb": "start", "flowIds": ["flow-0"]}).json()["jobId"]

    resp = client.post(f"/api/v2/flows/bulk/{job_id}/cancel")
    assert resp.status_code == 200, resp.text
    assert resp.json()["cancelRequested"] is True
    assert resp.json()["status"] == "cancelled"


def test_cancel_unknown_job_404(fake_db):
    client = _make_client(fake_db)
    assert client.post("/api/v2/flows/bulk/bulk-nope/cancel").status_code == 404


# --------------------------------------------------------------- read paths


def test_active_endpoint_reattaches_to_a_running_job(fake_db, monkeypatch):
    monkeypatch.setattr(bulk_runner, "launch_bulk_job", lambda db, job_id: None)
    client = _make_client(fake_db)
    job_id = client.post("/api/v2/flows/bulk", json={"verb": "deploy", "flowIds": ["flow-0"]}).json()["jobId"]

    active = client.get("/api/v2/flows/bulk/active")
    assert active.status_code == 200
    assert active.json()["id"] == job_id  # this is what survives a page refresh


def test_active_endpoint_returns_null_when_idle(fake_db):
    client = _make_client(fake_db)
    assert client.get("/api/v2/flows/bulk/active").json() is None


def test_get_job_returns_camelcase_shape(fake_db, monkeypatch):
    monkeypatch.setattr(bulk_runner, "launch_bulk_job", lambda db, job_id: None)
    client = _make_client(fake_db)
    job_id = client.post("/api/v2/flows/bulk", json={"verb": "start", "flowIds": ["flow-0"]}).json()["jobId"]

    body = client.get(f"/api/v2/flows/bulk/{job_id}").json()
    assert body["verb"] == "start"
    assert body["total"] == 1
    assert body["items"][0]["flowName"] == "alpha"
    assert "cancelRequested" in body


def test_get_unknown_job_404(fake_db):
    client = _make_client(fake_db)
    assert client.get("/api/v2/flows/bulk/bulk-nope").status_code == 404


# ------------------------------------------------------------------ recovery


def test_restart_marks_orphaned_running_job_interrupted():
    """A detached runner dies with its process. The doc must not be left
    saying "running" forever."""
    from services.runtime_recovery import reconcile_runtime_state

    class RecoveryDB:
        def __init__(self):
            self.bulk_jobs_v2 = FaultInjectingCollection(unique_fields=("id",))
            self.connection_lifecycle_jobs = FaultInjectingCollection()
            self.schema_inference_jobs = FaultInjectingCollection()
            self.flows = FaultInjectingCollection()

    db = RecoveryDB()
    now = datetime.now(timezone.utc)

    async def go():
        await db.bulk_jobs_v2.insert_one(
            {
                "id": "bulk-old",
                "status": "running",
                "owner_instance_id": "a-dead-process",
                "heartbeat_at": now - timedelta(minutes=5),
            }
        )
        report = await reconcile_runtime_state(db, instance_id="this-process", now=now)
        job = await db.bulk_jobs_v2.find_one({"id": "bulk-old"}, {"_id": 0})
        return report, job

    report, job = asyncio.run(go())
    assert report.bulk_jobs_recovered == 1
    assert job["status"] == "interrupted"
    assert "restarted" in job["error"]
