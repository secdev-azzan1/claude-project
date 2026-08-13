"""Tests for T7.5: real runtime observability + block test
(`backend/services/adapter/runtime.py`, wired into `routers/v2/flows.py`).

Follows the `FakeDB`-on-`FaultInjectingCollection` + `async_test`
(`asyncio.run`) pattern from `tests/test_deployer.py`, and the
scripted-`httpx.AsyncClient` pattern from `tests/test_v2_services.py`, for
the same reason both of those files use them: no pytest-asyncio dependency,
and no real network access from this suite.

Covers:
  - metrics: per-block attribution via `runtimeScopeMap`, DLQ inflow ->
    errors24h, topic counts.
  - dlq: header-derived block/errorClass mapping.
  - messages: topic-ownership 404.
  - runtime: drift verdicts (pg missing/same instance, pg missing/different
    instance, NiFi unreachable carries prior state forward).
  - repair: clears the dead reference, records an orphan, audits.
  - block test: http-read happy path (record path + <=10 slice + detected
    fields), unresolved-placeholder 422, write-block 422 refusal.
"""
from __future__ import annotations

import asyncio
import sys
from functools import wraps
from pathlib import Path
from typing import Any, Dict, List, Optional

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from fastapi import FastAPI
from fastapi.testclient import TestClient

from db import get_db
from routers.v2 import flows as v2_flows
from services.adapter import runtime as runtime_svc
from services.adapter.common import COLLECTIONS
from tests.resilience.conftest import FaultInjectingCollection


def async_test(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


# --------------------------------------------------------------------- FakeDB


class FakeDB:
    def __init__(self):
        self.flows_v2 = FaultInjectingCollection()
        self.services_v2 = FaultInjectingCollection()
        self.connections_v2 = FaultInjectingCollection()
        self.gateway_v2 = FaultInjectingCollection()
        self.schemas_v2 = FaultInjectingCollection()
        self.runtimes_v2 = FaultInjectingCollection()
        self.audit_v2 = FaultInjectingCollection()

    def __getitem__(self, name):
        return getattr(self, name)


app = FastAPI()
app.include_router(v2_flows.router)


def _make_client(fake_db: FakeDB) -> TestClient:
    app.dependency_overrides[get_db] = lambda: fake_db
    return TestClient(app)


def _clear_overrides():
    app.dependency_overrides.pop(get_db, None)


# ------------------------------------------------------------------ fixtures


def _http_kafka_flow(flow_id: str = "flow-1", **overrides) -> Dict[str, Any]:
    flow = {
        "id": flow_id,
        "name": "Test Flow",
        "description": None,
        "state": "Stopped",
        "enabled": True,
        "cron": "*/5 * * * *",
        "blocks": [
            {
                "id": "b1", "adapter": "http", "mode": "read", "name": "Fetch", "parentId": None,
                "serviceId": "svc-1", "entity": None,
                "config": {"path": "/x", "method": "GET", "responseFormat": "json", "recordPath": "$.items[*]",
                           "split": True, "pagination": {"type": "none", "fields": {}}},
                "transforms": [],
            },
            {
                "id": "b2", "adapter": "kafka", "mode": "write", "name": "Write Topic", "parentId": "b1",
                "serviceId": None, "entity": "thing", "config": {}, "transforms": [],
            },
        ],
        "topics": [{"id": "t1", "kind": "materialized", "name": "raw.test_flow.thing", "sealed": False, "writerBlockId": "b2"}],
        "variables": [], "servicePins": {},
        "deployedAt": "2026-01-01T00:00:00.000Z",
        "nifiProcessGroupId": "pg-flow-1",
        "runtimeScopeMap": {
            "b1": {"adapter": "http", "engine": "nifi", "groupName": "fetch", "processGroupId": "pg-block-b1",
                   "components": {"trigger": "nid-trigger", "init": "nid-init", "fetch": "nid-fetch"},
                   "connectorNames": [], "topics": []},
            "b2": {"adapter": "kafka", "engine": "nifi", "groupName": "write", "processGroupId": "pg-block-b2",
                   "components": {"publish": "nid-publish"},
                   "connectorNames": [], "topics": ["raw.test_flow.thing"]},
        },
        "provenance": {"nifi": {"connectionId": "conn-nifi", "fingerprint": "root-A"}},
        "lastRunAt": None, "createdAt": "2026-01-01T00:00:00.000Z", "updatedAt": "2026-01-01T00:00:00.000Z",
    }
    flow.update(overrides)
    return flow


def _seed_http_service(fake_db: FakeDB, service_id: str = "svc-1", **cfg_overrides):
    config = {"baseUrl": "https://example.internal", "authMode": "none"}
    config.update(cfg_overrides)
    fake_db.services_v2.docs.append({
        "id": service_id, "type": "http", "name": "Test Service", "revision": 1, "retired": False,
        "health": "Healthy", "config": config, "hasSecret": False,
        "createdAt": "2026-01-01T00:00:00.000Z", "updatedAt": "2026-01-01T00:00:00.000Z",
    })


def _seed_nifi_connection(fake_db: FakeDB, conn_id: str = "conn-nifi"):
    fake_db.connections_v2.docs.append({
        "id": conn_id, "type": "nifi", "name": "NiFi", "active": True, "health": "Healthy",
        "reachability": "Reachable", "lastTestedAt": None,
        "config": {"url": "https://nifi.test", "authMode": "basic", "username": "admin", "password": "pw"},
        "hasSecret": True,
    })


def _seed_kafka_connection(fake_db: FakeDB):
    fake_db.connections_v2.docs.append({
        "id": "conn-kafka", "type": "kafka", "name": "Kafka", "active": True, "health": "Healthy",
        "reachability": "Reachable", "lastTestedAt": None,
        "config": {"bootstrapServers": "kafka:9092", "mode": "native", "securityProtocol": "PLAINTEXT"},
        "hasSecret": False,
    })


# ============================================================== 1. metrics


@async_test
async def test_metrics_attribution_via_scope_map(monkeypatch):
    fake_db = FakeDB()
    flow_doc = _http_kafka_flow()
    _seed_nifi_connection(fake_db)
    _seed_kafka_connection(fake_db)

    async def fake_nifi_api_request(url, method, path, **kwargs):
        assert path == "/nifi-api/flow/process-groups/pg-flow-1/status"
        assert kwargs.get("params") == {"recursive": "true"}
        return {
            "ok": True,
            "data": {
                "processGroupStatus": {
                    "aggregateSnapshot": {
                        "flowFilesOut": 120,
                        "flowFilesQueued": 7,
                        "connectionStatusSnapshots": [
                            {"connectionStatusSnapshot": {"destinationName": "dlq__meta", "flowFilesIn": 4}},
                        ],
                        "processGroupStatusSnapshots": [
                            {"processGroupStatusSnapshot": {
                                "id": "pg-block-b1", "flowFilesIn": 60, "flowFilesOut": 58, "flowFilesQueued": 2,
                                "connectionStatusSnapshots": [], "processGroupStatusSnapshots": [],
                            }},
                            {"processGroupStatusSnapshot": {
                                "id": "pg-block-b2", "flowFilesIn": 58, "flowFilesOut": 58, "flowFilesQueued": 0,
                                "connectionStatusSnapshots": [
                                    {"connectionStatusSnapshot": {"destinationName": "dlq__meta", "flowFilesIn": 1}},
                                ],
                                "processGroupStatusSnapshots": [],
                            }},
                        ],
                    }
                }
            },
        }

    async def fake_count_topic(kafka_conn, name):
        return {"ok": True, "total_messages": 42 if name == "raw.test_flow.thing" else 5}

    monkeypatch.setattr(runtime_svc, "nifi_api_request", fake_nifi_api_request)
    monkeypatch.setattr(runtime_svc.topics_mod, "count_topic", fake_count_topic)

    result = await runtime_svc.get_flow_metrics(fake_db, flow_doc)

    assert result["available"] is True
    assert result["records24h"] == 120
    assert result["queued"] == 7
    assert result["errors24h"] == 5  # 4 (flow-level) + 1 (nested under b2)

    per_block = {b["blockId"]: b for b in result["perBlock"]}
    assert per_block["b1"]["label"] == "http · Fetch"
    assert per_block["b1"]["recordsIn"] == 60
    assert per_block["b1"]["recordsOut"] == 58
    assert per_block["b1"]["queued"] == 2
    assert per_block["b2"]["recordsOut"] == 58

    topic_names = {t["topic"]: t["messages"] for t in result["topicCounts"]}
    assert topic_names["raw.test_flow.thing"] == 42
    assert topic_names["dlq.test_flow"] == 5


@async_test
async def test_metrics_not_deployed_is_honest_unavailable():
    fake_db = FakeDB()
    flow_doc = _http_kafka_flow(nifiProcessGroupId=None)
    result = await runtime_svc.get_flow_metrics(fake_db, flow_doc)
    assert result == {"available": False, "reason": "not deployed"}


@async_test
async def test_metrics_nifi_unreachable_never_fakes_zeros(monkeypatch):
    fake_db = FakeDB()
    flow_doc = _http_kafka_flow()
    _seed_nifi_connection(fake_db)

    async def fake_unreachable(url, method, path, **kwargs):
        return {"ok": False, "error": "Cannot connect to NiFi.", "reachable": False}

    monkeypatch.setattr(runtime_svc, "nifi_api_request", fake_unreachable)

    result = await runtime_svc.get_flow_metrics(fake_db, flow_doc)
    assert result["available"] is False
    assert "Cannot connect" in result["reason"]


# ==================================================================== 2. dlq


@async_test
async def test_dlq_maps_headers_to_block_and_error_class(monkeypatch):
    fake_db = FakeDB()
    flow_doc = _http_kafka_flow()
    _seed_kafka_connection(fake_db)

    async def fake_fetch(kafka_conn, topic, limit):
        assert topic == "dlq.test_flow"
        assert limit == 50
        return {
            "ok": True,
            "messages": [
                {
                    "partition": 0, "offset": 7, "timestamp": "2026-08-01T00:00:00.000Z",
                    "key": "k1", "value": "x" * 600,
                    "headers": {"dlq.block": "b1", "dlq.reason": "invalid_json"},
                },
                {
                    "partition": 0, "offset": 6, "timestamp": "2026-07-31T00:00:00.000Z",
                    "key": None, "value": "short payload", "headers": {},
                },
            ],
        }

    monkeypatch.setattr(runtime_svc, "_fetch_recent_records", fake_fetch)

    result = await runtime_svc.get_flow_dlq(fake_db, flow_doc)
    records = result["records"]
    assert len(records) == 2
    first = next(r for r in records if r["id"] == "dlq.test_flow-0-7")
    assert first["blockName"] == "Fetch"  # resolved from dlq.block=b1 -> block b1's name
    assert first["errorClass"] == "invalid_json"
    assert len(first["payloadPreview"]) == 500  # truncated

    second = next(r for r in records if r["id"] == "dlq.test_flow-0-6")
    assert second["blockName"] == ""  # no headers present
    assert second["errorClass"] == ""
    assert second["payloadPreview"] == "short payload"


@async_test
async def test_dlq_no_kafka_connection_is_honest_empty():
    fake_db = FakeDB()
    flow_doc = _http_kafka_flow()
    result = await runtime_svc.get_flow_dlq(fake_db, flow_doc)
    assert result == {"records": []}


# =============================================================== 3. messages


def test_messages_topic_not_owned_by_flow_404s():
    fake_db = FakeDB()
    flow_doc = _http_kafka_flow()
    fake_db.flows_v2.docs.append(flow_doc)
    client = _make_client(fake_db)
    try:
        resp = client.get(f"/api/v2/flows/{flow_doc['id']}/messages", params={"topic": "not.owned"})
        assert resp.status_code == 404, resp.text
    finally:
        _clear_overrides()


def test_messages_owned_topic_no_kafka_connection_returns_empty_list():
    fake_db = FakeDB()
    flow_doc = _http_kafka_flow()
    fake_db.flows_v2.docs.append(flow_doc)
    client = _make_client(fake_db)
    try:
        resp = client.get(f"/api/v2/flows/{flow_doc['id']}/messages", params={"topic": "raw.test_flow.thing"})
        assert resp.status_code == 200, resp.text
        assert resp.json() == {"messages": []}
    finally:
        _clear_overrides()


# ================================================================ 4. runtime


@async_test
async def test_runtime_drift_pg_missing_same_instance_really_deleted(monkeypatch):
    fake_db = FakeDB()
    flow_doc = _http_kafka_flow()  # provenance fingerprint == "root-A"
    _seed_nifi_connection(fake_db)

    async def fake_probe(conn):
        return {"ok": True, "fingerprint": "root-A", "reachable": True, "error": None}

    async def fake_pg_check(url, method, path, **kwargs):
        assert path == "/nifi-api/process-groups/pg-flow-1"
        return {"ok": False, "error": "HTTP 404", "status_code": 404, "reachable": True}

    monkeypatch.setattr(runtime_svc, "probe_nifi_fingerprint", fake_probe)
    monkeypatch.setattr(runtime_svc, "nifi_api_request", fake_pg_check)

    result = await runtime_svc.read_runtime(fake_db, flow_doc)
    assert result["reachable"] is True
    assert result["observedFingerprint"] == "root-A"
    assert len(result["drift"]) == 1
    finding = result["drift"][0]
    assert finding["kind"] == "process_group_missing"
    assert finding["verdict"] == "really_deleted"
    assert finding["repairable"] is True


@async_test
async def test_runtime_drift_pg_missing_different_instance_deployed_elsewhere(monkeypatch):
    fake_db = FakeDB()
    flow_doc = _http_kafka_flow()  # deployed fingerprint == "root-A"
    _seed_nifi_connection(fake_db)

    async def fake_probe(conn):
        return {"ok": True, "fingerprint": "root-B", "reachable": True, "error": None}

    async def fake_pg_check(url, method, path, **kwargs):
        return {"ok": False, "error": "HTTP 404", "status_code": 404, "reachable": True}

    monkeypatch.setattr(runtime_svc, "probe_nifi_fingerprint", fake_probe)
    monkeypatch.setattr(runtime_svc, "nifi_api_request", fake_pg_check)

    result = await runtime_svc.read_runtime(fake_db, flow_doc)
    finding = result["drift"][0]
    assert finding["verdict"] == "deployed_elsewhere"
    assert finding["expected"] == "root-A"
    assert finding["observed"] == "root-B"
    assert finding["repairable"] is True


@async_test
async def test_runtime_unreachable_carries_prior_state_forward_and_never_concludes(monkeypatch):
    fake_db = FakeDB()
    flow_doc = _http_kafka_flow()
    _seed_nifi_connection(fake_db)
    prior_drift = [{"id": "drift-old", "kind": "process_group_missing", "summary": "old", "where": "x",
                    "expected": "root-A", "observed": "root-A", "verdict": "really_deleted",
                    "verdictDetail": "d", "observedAt": "2026-01-01T00:00:00.000Z", "repairable": True}]
    fake_db.runtimes_v2.docs.append({
        "flowId": "flow-1", "nifiConnectionId": "conn-nifi", "processGroupId": "pg-flow-1",
        "deployedFingerprint": "root-A", "observedFingerprint": "root-A", "reachable": True,
        "unreachableReason": None, "lastReadAt": "2026-01-01T00:00:00.000Z",
        "components": [], "controllerServices": [], "connectors": [],
        "drift": prior_drift, "orphans": [],
    })

    async def fake_probe(conn):
        return {"ok": False, "fingerprint": None, "reachable": False, "error": "Cannot connect to NiFi."}

    monkeypatch.setattr(runtime_svc, "probe_nifi_fingerprint", fake_probe)

    result = await runtime_svc.read_runtime(fake_db, flow_doc)
    assert result["reachable"] is False
    assert result["unreachableReason"] == "Cannot connect to NiFi."
    assert result["observedFingerprint"] is None
    # Nothing was concluded from the failed read -- prior drift carried forward untouched.
    assert result["drift"] == prior_drift


@async_test
async def test_runtime_healthy_pg_reads_components_and_controller_services(monkeypatch):
    fake_db = FakeDB()
    flow_doc = _http_kafka_flow()
    _seed_nifi_connection(fake_db)

    async def fake_probe(conn):
        return {"ok": True, "fingerprint": "root-A", "reachable": True, "error": None}

    async def fake_pg_check(url, method, path, **kwargs):
        if path == "/nifi-api/process-groups/pg-flow-1":
            return {"ok": True, "data": {"id": "pg-flow-1"}}
        raise AssertionError(f"unexpected path {path}")

    async def fake_get_processor_config(url, processor_id, **kwargs):
        return {"ok": True, "id": processor_id, "name": "fetch", "type": "org.apache.nifi.processors.standard.InvokeHTTP",
                "state": "STOPPED", "properties": {"HTTP Method": "GET"}, "descriptors": {}, "validation_errors": [], "revision": 0}

    monkeypatch.setattr(runtime_svc, "probe_nifi_fingerprint", fake_probe)
    monkeypatch.setattr(runtime_svc, "nifi_api_request", fake_pg_check)
    monkeypatch.setattr(runtime_svc.nifi_flow_manager, "get_processor_config", fake_get_processor_config)

    result = await runtime_svc.read_runtime(fake_db, flow_doc)
    assert result["reachable"] is True
    assert result["drift"] == []
    ids = {c["id"] for c in result["components"]}
    assert {"nid-trigger", "nid-init", "nid-fetch", "nid-publish"} <= ids
    for c in result["components"]:
        assert c["state"] == "STOPPED"


def test_runtime_no_process_group_returns_404_when_no_prior_doc():
    fake_db = FakeDB()
    flow_doc = _http_kafka_flow(nifiProcessGroupId=None, deployedAt=None)
    fake_db.flows_v2.docs.append(flow_doc)
    client = _make_client(fake_db)
    try:
        resp = client.get(f"/api/v2/flows/{flow_doc['id']}/runtime")
        assert resp.status_code == 404, resp.text
    finally:
        _clear_overrides()


# -------------------------------------------------------------- 4b. repair


@async_test
async def test_repair_clears_reference_records_orphan_and_audits(monkeypatch):
    fake_db = FakeDB()
    flow_doc = _http_kafka_flow()
    fake_db.flows_v2.docs.append(flow_doc)
    _seed_nifi_connection(fake_db)

    async def fake_probe(conn):
        return {"ok": True, "fingerprint": "root-B", "reachable": True, "error": None}  # different instance

    async def fake_pg_check(url, method, path, **kwargs):
        return {"ok": False, "error": "HTTP 404", "status_code": 404, "reachable": True}

    monkeypatch.setattr(runtime_svc, "probe_nifi_fingerprint", fake_probe)
    monkeypatch.setattr(runtime_svc, "nifi_api_request", fake_pg_check)

    stored_flow_doc = fake_db.flows_v2.docs[0]
    result = await runtime_svc.repair_runtime(fake_db, stored_flow_doc)

    assert result["clearedFindings"] == 1
    assert len(result["orphans"]) == 1
    assert result["orphans"][0]["kind"] == "process_group"
    assert result["orphans"][0]["ref"] == "pg-flow-1"
    assert result["orphans"][0]["instance"] == "root-A"  # the ORIGINAL (deployed) instance
    assert result["runtime"]["processGroupId"] is None
    assert result["runtime"]["drift"] == []  # the resolved finding no longer shows up

    updated_flow = fake_db.flows_v2.docs[0]
    assert updated_flow["nifiProcessGroupId"] is None
    assert updated_flow["state"] == "Draft"
    assert updated_flow["deployedAt"] is None

    updated_runtime = fake_db.runtimes_v2.docs[0]
    assert updated_runtime["processGroupId"] is None
    assert len(updated_runtime["orphans"]) == 1

    audited = [e for e in fake_db.audit_v2.docs if e["action"] == "Runtime force repaired"]
    assert len(audited) == 1
    assert audited[0]["status"] == "Warning"


def test_repair_refused_when_nothing_repairable_returns_409(monkeypatch):
    fake_db = FakeDB()
    flow_doc = _http_kafka_flow()
    fake_db.flows_v2.docs.append(flow_doc)
    _seed_nifi_connection(fake_db)

    async def fake_probe(conn):
        return {"ok": True, "fingerprint": "root-A", "reachable": True, "error": None}

    async def fake_pg_check(url, method, path, **kwargs):
        return {"ok": True, "data": {"id": "pg-flow-1"}}

    async def fake_get_processor_config(url, processor_id, **kwargs):
        return {"ok": True, "id": processor_id, "name": "x", "type": "t", "state": "STOPPED",
                "properties": {}, "descriptors": {}, "validation_errors": [], "revision": 0}

    monkeypatch.setattr(runtime_svc, "probe_nifi_fingerprint", fake_probe)
    monkeypatch.setattr(runtime_svc, "nifi_api_request", fake_pg_check)
    monkeypatch.setattr(runtime_svc.nifi_flow_manager, "get_processor_config", fake_get_processor_config)

    client = _make_client(fake_db)
    try:
        resp = client.post(f"/api/v2/flows/{flow_doc['id']}/runtime/repair")
        assert resp.status_code == 409, resp.text
    finally:
        _clear_overrides()


# ============================================================== 5. block test

# --------------------------------------------------------------- fake httpx


class FakeJsonResponse:
    def __init__(self, status_code: int, json_data: Any):
        self.status_code = status_code
        self._json_data = json_data

    def json(self):
        return self._json_data


def patch_probe_httpx(monkeypatch, response: FakeJsonResponse):
    calls: List[tuple] = []

    class _Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, url, **kw):
            calls.append((url, kw))
            return response

    monkeypatch.setattr(runtime_svc.httpx, "AsyncClient", _Client)
    return calls


def test_block_test_http_read_happy_path(monkeypatch):
    fake_db = FakeDB()
    flow_doc = _http_kafka_flow()
    fake_db.flows_v2.docs.append(flow_doc)
    _seed_http_service(fake_db)

    items = [{"id": i, "name": f"item-{i}"} for i in range(15)]
    calls = patch_probe_httpx(monkeypatch, FakeJsonResponse(200, {"items": items}))

    client = _make_client(fake_db)
    try:
        resp = client.post(f"/api/v2/flows/{flow_doc['id']}/blocks/b1/test")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["ok"] is True
        assert len(body["records"]) == 10  # bounded to <=10
        assert body["records"][0] == {"id": 0, "name": "item-0"}
        assert set(body["detectedFields"]) == {"id", "name"}
        assert body["testedAt"]
        assert len(calls) == 1
    finally:
        _clear_overrides()

    stored = fake_db.flows_v2.docs[0]
    persisted = stored["blocks"][0]["testResult"]
    assert persisted["ok"] is True
    tested_actions = [e for e in fake_db.audit_v2.docs if e["action"] == "Block tested"]
    assert len(tested_actions) == 1
    assert tested_actions[0]["status"] == "Success"


def test_block_test_unresolved_placeholder_refused_422():
    fake_db = FakeDB()
    flow_doc = _http_kafka_flow()
    flow_doc["blocks"][0]["config"]["path"] = "/x/${accountId}"
    fake_db.flows_v2.docs.append(flow_doc)
    _seed_http_service(fake_db)

    client = _make_client(fake_db)
    try:
        resp = client.post(f"/api/v2/flows/{flow_doc['id']}/blocks/b1/test")
        assert resp.status_code == 422, resp.text
        assert "accountId" in resp.json()["detail"]
    finally:
        _clear_overrides()


def test_block_test_write_block_refused_422():
    fake_db = FakeDB()
    flow_doc = _http_kafka_flow()  # b2 is a kafka write block
    fake_db.flows_v2.docs.append(flow_doc)

    client = _make_client(fake_db)
    try:
        resp = client.post(f"/api/v2/flows/{flow_doc['id']}/blocks/b2/test")
        assert resp.status_code == 422, resp.text
        assert resp.json()["detail"] == "write blocks are not test-runnable"
    finally:
        _clear_overrides()


def test_block_test_kafka_read_returns_501():
    fake_db = FakeDB()
    flow_doc = _http_kafka_flow(
        blocks=[
            {"id": "kr1", "adapter": "kafka", "mode": "read", "name": "Consume", "parentId": None,
             "serviceId": None, "entity": None, "config": {"topicName": "raw.x"}, "transforms": []},
        ],
    )
    fake_db.flows_v2.docs.append(flow_doc)

    client = _make_client(fake_db)
    try:
        resp = client.post(f"/api/v2/flows/{flow_doc['id']}/blocks/kr1/test")
        assert resp.status_code == 501, resp.text
        assert "compiler support" in resp.json()["detail"]
    finally:
        _clear_overrides()


def test_block_test_unknown_block_404s():
    fake_db = FakeDB()
    flow_doc = _http_kafka_flow()
    fake_db.flows_v2.docs.append(flow_doc)

    client = _make_client(fake_db)
    try:
        resp = client.post(f"/api/v2/flows/{flow_doc['id']}/blocks/nope/test")
        assert resp.status_code == 404, resp.text
    finally:
        _clear_overrides()
