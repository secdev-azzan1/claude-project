import asyncio
from datetime import datetime

from fastapi import HTTPException

from routers import flows
from tests.resilience.conftest import (
    ResilienceFakeDB,
    make_rest_source,
    make_verified_rest_flow,
)


def _runtime_db():
    db = ResilienceFakeDB()
    flow = make_verified_rest_flow()
    flow["nifi_process_group_id"] = "pg-1"
    db.flows.docs.append(flow)
    db.sources.docs.append(make_rest_source())
    db.connections.docs.extend(
        [
            {"type": "nifi", "endpoint": "https://nifi.example", "auth_type": "NONE"},
            {"type": "kafka", "endpoint": "kafka:9092"},
        ],
    )
    return db


def _patch_start_prerequisites(monkeypatch):
    async def no_op(*_args, **_kwargs):
        return None

    async def not_webhook(*_args, **_kwargs):
        return False

    async def present(flow, db, action):
        return flow, False, None

    async def metrics(*_args, **_kwargs):
        return {"kafka_topic_total_messages": 0}

    monkeypatch.setattr(flows, "require_runtime_connections", no_op)
    monkeypatch.setattr(flows, "_validate_verified_schema_for_flow", no_op)
    monkeypatch.setattr(flows, "_is_webhook_flow", not_webhook)
    monkeypatch.setattr(flows, "_clear_nifi_pg_if_missing", present)
    monkeypatch.setattr(flows, "_build_kafka_topic_metrics", metrics)
    monkeypatch.setattr(flows, "_log_audit", no_op)
    monkeypatch.setattr(flows.content_store, "sync_flow", no_op)


def test_concurrent_starts_create_one_run_and_one_nifi_start(monkeypatch):
    db = _runtime_db()
    _patch_start_prerequisites(monkeypatch)
    start_calls = 0
    entered = asyncio.Event()
    release = asyncio.Event()

    async def prepare(**_kwargs):
        return None

    async def start(**_kwargs):
        nonlocal start_calls
        start_calls += 1
        entered.set()
        await release.wait()
        return {"ok": True}

    from services import nifi_flow_manager

    monkeypatch.setattr(nifi_flow_manager, "prepare_process_group_for_start", prepare)
    monkeypatch.setattr(nifi_flow_manager, "start_process_group", start)

    async def run():
        first = asyncio.create_task(flows.start_flow("flow-1", db=db))
        await entered.wait()
        second = asyncio.create_task(flows.start_flow("flow-1", db=db))
        await asyncio.sleep(0)
        release.set()
        return await asyncio.gather(first, second, return_exceptions=True)

    results = asyncio.run(run())

    assert start_calls == 1
    assert sum(isinstance(result, HTTPException) and result.status_code == 409 for result in results) == 1
    assert sum(isinstance(result, dict) and result.get("ok") for result in results) == 1
    assert db.flows.docs[0]["state"] == "Running"
    assert len(db.flows.docs[0]["runs"]) == 1


def test_start_failure_marks_run_failed_and_releases_state(monkeypatch):
    db = _runtime_db()
    _patch_start_prerequisites(monkeypatch)

    async def prepare(**_kwargs):
        return None

    async def start(**_kwargs):
        return {"ok": False, "error": "NiFi unavailable"}

    from services import nifi_flow_manager

    monkeypatch.setattr(nifi_flow_manager, "prepare_process_group_for_start", prepare)
    monkeypatch.setattr(nifi_flow_manager, "start_process_group", start)

    try:
        asyncio.run(flows.start_flow("flow-1", db=db))
    except HTTPException as exc:
        assert exc.status_code == 502
    else:
        raise AssertionError("Start failure should raise HTTP 502")

    assert db.flows.docs[0]["state"] == "Stopped"
    assert db.flows.docs[0]["runs"][0]["status"] == "Failed"


def test_concurrent_stops_send_one_nifi_command(monkeypatch):
    db = _runtime_db()
    db.flows.docs[0]["state"] = "Running"
    db.flows.docs[0]["runs"] = [
        {
            "id": "run-1",
            "started_at": datetime.utcnow(),
            "status": "Running",
            "records_processed": 0,
            "kafka_start_count": 0,
        }
    ]
    _patch_start_prerequisites(monkeypatch)
    stop_calls = 0
    entered = asyncio.Event()
    release = asyncio.Event()

    async def stop(**_kwargs):
        nonlocal stop_calls
        stop_calls += 1
        entered.set()
        await release.wait()
        return {"ok": True}

    from services import nifi_flow_manager

    monkeypatch.setattr(nifi_flow_manager, "stop_and_clear_process_group", stop)

    async def run():
        first = asyncio.create_task(flows.stop_flow("flow-1", db=db))
        await entered.wait()
        second = asyncio.create_task(flows.stop_flow("flow-1", db=db))
        await asyncio.sleep(0)
        release.set()
        return await asyncio.gather(first, second, return_exceptions=True)

    results = asyncio.run(run())

    assert stop_calls == 1
    assert sum(isinstance(result, HTTPException) and result.status_code == 409 for result in results) == 1
    assert sum(isinstance(result, dict) and result.get("ok") for result in results) == 1
    assert db.flows.docs[0]["state"] == "Stopped"
    assert db.flows.docs[0]["runs"][0]["status"] == "Success"


def test_independent_flows_can_start_concurrently_without_cross_updates(monkeypatch):
    db = _runtime_db()
    second_flow = make_verified_rest_flow(
        flow_id="flow-2",
        source_id="source-2",
    )
    second_flow["name"] = "Resilience Flow Two"
    second_flow["nifi_process_group_id"] = "pg-2"
    db.flows.docs.append(second_flow)
    db.sources.docs.append(make_rest_source(source_id="source-2", stream_id="stream-2"))
    _patch_start_prerequisites(monkeypatch)
    entered = set()
    both_entered = asyncio.Event()
    release = asyncio.Event()

    async def prepare(**_kwargs):
        return None

    async def start(*, process_group_id, **_kwargs):
        entered.add(process_group_id)
        if entered == {"pg-1", "pg-2"}:
            both_entered.set()
        await release.wait()
        return {"ok": True}

    from services import nifi_flow_manager

    monkeypatch.setattr(nifi_flow_manager, "prepare_process_group_for_start", prepare)
    monkeypatch.setattr(nifi_flow_manager, "start_process_group", start)

    async def run():
        starts = [
            asyncio.create_task(flows.start_flow("flow-1", db=db)),
            asyncio.create_task(flows.start_flow("flow-2", db=db)),
        ]
        await asyncio.wait_for(both_entered.wait(), timeout=1)
        release.set()
        return await asyncio.gather(*starts)

    results = asyncio.run(run())

    assert [result["ok"] for result in results] == [True, True]
    assert entered == {"pg-1", "pg-2"}
    assert [flow["state"] for flow in db.flows.docs] == ["Running", "Running"]
    assert [len(flow["runs"]) for flow in db.flows.docs] == [1, 1]
