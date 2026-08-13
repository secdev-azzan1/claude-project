"""Tests for T1.2b: routers/v2/flows.py, routers/v2/dashboard.py,
routers/v2/audit.py.

Follows the TestClient + FakeDB pattern from tests/test_v2_openapi.py, but
builds its own minimal FastAPI app (rather than importing `server.app`)
since backend/server.py does not wire these v2 routers up yet -- that is
out of scope for this task (server.py is read-only here). The FakeDB is
built on the same FaultInjectingCollection helper the resilience test suite
uses, keyed by the exact v2 collection names in
services/adapter/common.py's `COLLECTIONS`.
"""
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from fastapi import FastAPI
from fastapi.testclient import TestClient

from db import get_db
from routers.v2 import audit as v2_audit
from routers.v2 import dashboard as v2_dashboard
from routers.v2 import flows as v2_flows
from services.adapter.common import COLLECTIONS
from tests.resilience.conftest import FaultInjectingCollection

app = FastAPI()
app.include_router(v2_flows.router)
app.include_router(v2_dashboard.router)
app.include_router(v2_audit.router)


class FakeDB:
    def __init__(self):
        self.flows_v2 = FaultInjectingCollection()
        self.services_v2 = FaultInjectingCollection()
        self.connections_v2 = FaultInjectingCollection()
        self.gateway_v2 = FaultInjectingCollection()
        self.schemas_v2 = FaultInjectingCollection()
        self.schema_templates_v2 = FaultInjectingCollection()
        self.runtimes_v2 = FaultInjectingCollection()
        self.audit_v2 = FaultInjectingCollection()
        self.openapi_specs_v2 = FaultInjectingCollection()

    def __getitem__(self, name):
        return getattr(self, name)


def _make_client(fake_db: FakeDB) -> TestClient:
    app.dependency_overrides[get_db] = lambda: fake_db
    return TestClient(app)


def _clear_overrides():
    app.dependency_overrides.pop(get_db, None)


# ------------------------------------------------------------- flow fixtures

def _valid_flow(flow_id: str = "flow-valid-1", **overrides):
    """A minimal flow that passes both `validate_placement` and the
    flow-level (and, incidentally, block-level too) checks in `validate_flow`:
    an http-read root (bound to a real, non-retired service) feeding a
    kafka-write sink, on a valid 5-field cron."""
    flow = {
        "id": flow_id,
        "name": "Valid Flow",
        "description": None,
        "state": "Draft",
        "enabled": False,
        "cron": "*/5 * * * *",
        "blocks": [
            {
                "id": "b1",
                "adapter": "http",
                "mode": "read",
                "name": "Fetch",
                "parentId": None,
                "serviceId": "svc-1",
                "entity": None,
                "config": {"path": "/x", "method": "GET"},
                "transforms": [],
            },
            {
                "id": "b2",
                "adapter": "kafka",
                "mode": "write",
                "name": "Write Topic",
                "parentId": "b1",
                "serviceId": None,
                "entity": "thing",
                "config": {},
                "transforms": [],
            },
        ],
        "topics": [],
        "variables": [],
        "servicePins": {},
        "deployedAt": None,
        "lastRunAt": None,
        "createdAt": "",
        "updatedAt": "",
    }
    flow.update(overrides)
    return flow


def _seed_valid_service(fake_db: FakeDB, service_id: str = "svc-1"):
    fake_db.services_v2.docs.append(
        {
            "id": service_id,
            "type": "http",
            "name": "Test Service",
            "revision": 1,
            "retired": False,
            "health": "Healthy",
            "config": {"baseUrl": "https://example.internal"},
            "hasSecret": False,
            "createdAt": "2026-01-01T00:00:00.000Z",
            "updatedAt": "2026-01-01T00:00:00.000Z",
        }
    )


# ------------------------------------------------------------------- save

def test_save_valid_flow_ok_and_audit_written():
    fake_db = FakeDB()
    _seed_valid_service(fake_db)
    client = _make_client(fake_db)
    try:
        resp = client.post("/api/v2/flows/", json=_valid_flow())
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["id"] == "flow-valid-1"
        assert body["name"] == "Valid Flow"
        assert body["updatedAt"]
        assert body["createdAt"]

        assert len(fake_db.flows_v2.docs) == 1

        audit_actions = [e["action"] for e in fake_db.audit_v2.docs]
        assert "Flow created" in audit_actions
    finally:
        _clear_overrides()


def test_save_flow_with_terminal_parent_violation_returns_4xx_with_issue_message():
    fake_db = FakeDB()
    _seed_valid_service(fake_db)
    client = _make_client(fake_db)
    try:
        flow = _valid_flow(flow_id="flow-bad-1")
        # kafka_kc is terminal (R3/R5) -- nothing may follow it. Attach a
        # third block under it anyway.
        flow["blocks"].append(
            {
                "id": "b3",
                "adapter": "kafka_kc",
                "name": "KC Write",
                "parentId": "b1",
                "serviceId": None,
                "entity": "thing2",
                "config": {},
                "transforms": [],
            }
        )
        flow["blocks"].append(
            {
                "id": "b4",
                "adapter": "jdbc",
                "mode": "write",
                "name": "Bad Child",
                "parentId": "b3",
                "serviceId": "svc-1",
                "entity": "thing3",
                "config": {"table": "t"},
                "transforms": [],
            }
        )
        resp = client.post("/api/v2/flows/", json=flow)
        assert 400 <= resp.status_code < 500, resp.text
        detail = resp.json()["detail"]
        messages = [i["message"] for i in detail["issues"]]
        assert any("terminal" in m for m in messages), messages
        assert fake_db.flows_v2.docs == []
    finally:
        _clear_overrides()


def test_save_while_running_is_refused_with_mirrored_guard_reason():
    fake_db = FakeDB()
    _seed_valid_service(fake_db)
    existing = _valid_flow(
        flow_id="flow-running-1",
        state="Running",
        deployedAt="2026-08-01T00:00:00.000Z",
        createdAt="2026-08-01T00:00:00.000Z",
        updatedAt="2026-08-01T00:00:00.000Z",
    )
    fake_db.flows_v2.docs.append(existing)
    client = _make_client(fake_db)
    try:
        updated = dict(existing)
        updated["name"] = "Renamed While Running"
        resp = client.post("/api/v2/flows/", json=updated)
        assert resp.status_code == 409, resp.text
        assert resp.json()["detail"] == (
            "Editing a deployed flow is refused until it is stopped. (kc sink subscriptions save live.)"
        )
        # Untouched.
        assert fake_db.flows_v2.docs[0]["name"] == "Valid Flow"
    finally:
        _clear_overrides()


# ---------------------------------------------------------------- delete

def test_delete_draft_flow_ok():
    fake_db = FakeDB()
    fake_db.flows_v2.docs.append(_valid_flow(flow_id="flow-del-1"))
    client = _make_client(fake_db)
    try:
        resp = client.delete("/api/v2/flows/flow-del-1")
        assert resp.status_code == 200, resp.text
        assert fake_db.flows_v2.docs == []
        assert any(e["action"] == "Flow deleted" for e in fake_db.audit_v2.docs)
    finally:
        _clear_overrides()


def test_delete_deployed_flow_tears_down_and_removes():
    """T7.2+T7.3+T7.4: DELETE on a deployed flow now performs a full
    teardown (lifecycle.delete -> undeploy, best-effort against whatever
    connections happen to be configured, then removes the docs) instead of
    refusing with 409 — with no connections_v2 docs seeded here, every
    NiFi/Connect/Kafka step is a no-op skip (no active connection to act
    against), so this also exercises that skip-if-missing path end to end."""
    fake_db = FakeDB()
    fake_db.flows_v2.docs.append(
        _valid_flow(flow_id="flow-del-2", state="Stopped", deployedAt="2026-08-01T00:00:00.000Z")
    )
    client = _make_client(fake_db)
    try:
        resp = client.delete("/api/v2/flows/flow-del-2")
        assert resp.status_code == 200, resp.text
        assert resp.json() == {"ok": True, "id": "flow-del-2"}
        assert fake_db.flows_v2.docs == []
        actions = [e["action"] for e in fake_db.audit_v2.docs]
        assert "Flow undeployed" in actions
        assert "Flow deleted" in actions
    finally:
        _clear_overrides()


# ------------------------------------------------------------------ verbs

def test_start_from_draft_refused_409_with_reason():
    fake_db = FakeDB()
    fake_db.flows_v2.docs.append(_valid_flow(flow_id="flow-verb-1"))
    client = _make_client(fake_db)
    try:
        resp = client.post("/api/v2/flows/flow-verb-1/verbs/start")
        assert resp.status_code == 409, resp.text
        assert resp.json()["detail"] == "Deploy the flow first."
        refusals = [e for e in fake_db.audit_v2.docs if e["action"] == "Flow start refused"]
        assert len(refusals) == 1
        assert refusals[0]["status"] == "Failed"
    finally:
        _clear_overrides()


def test_deploy_from_draft_without_connections_returns_422_preflight_rows():
    """T7.2+T7.3+T7.4: the verb guard (`_get_verb_block_reason`) passes for
    a valid Draft flow, so this now reaches the real deployment engine
    (`services/adapter/deployer/lifecycle.deploy`) — which refuses at
    preflight (compiler-spec.md §8) because no connections_v2 docs are
    seeded here, reporting every failing row rather than a bare 501."""
    fake_db = FakeDB()
    _seed_valid_service(fake_db)
    fake_db.flows_v2.docs.append(_valid_flow(flow_id="flow-verb-2"))
    client = _make_client(fake_db)
    try:
        resp = client.post("/api/v2/flows/flow-verb-2/verbs/deploy")
        assert resp.status_code == 422, resp.text
        rows = resp.json()["detail"]["rows"]
        assert any(not r["ok"] and "NiFi" in r["label"] for r in rows)
        # State untouched — preflight failure happens before any apply step.
        assert fake_db.flows_v2.docs[0]["state"] == "Draft"
        refused = [e for e in fake_db.audit_v2.docs if e["action"] == "Flow deploy refused (preflight)"]
        assert len(refused) == 1
        assert refused[0]["status"] == "Failed"
    finally:
        _clear_overrides()


def test_unknown_verb_is_404():
    fake_db = FakeDB()
    fake_db.flows_v2.docs.append(_valid_flow(flow_id="flow-verb-3"))
    client = _make_client(fake_db)
    try:
        resp = client.post("/api/v2/flows/flow-verb-3/verbs/frobnicate")
        assert resp.status_code == 404, resp.text
    finally:
        _clear_overrides()


def test_pause_illegal_transition_from_draft_409():
    fake_db = FakeDB()
    fake_db.flows_v2.docs.append(_valid_flow(flow_id="flow-verb-4"))
    client = _make_client(fake_db)
    try:
        resp = client.post("/api/v2/flows/flow-verb-4/verbs/pause")
        assert resp.status_code == 409, resp.text
        assert resp.json()["detail"] == "Only a running flow can be paused."
    finally:
        _clear_overrides()


# --------------------------------------------------------------- enabled

def test_enabled_guard_disable_refused_while_running():
    fake_db = FakeDB()
    fake_db.flows_v2.docs.append(
        _valid_flow(flow_id="flow-en-1", state="Running", enabled=True, deployedAt="2026-08-01T00:00:00.000Z")
    )
    client = _make_client(fake_db)
    try:
        resp = client.post("/api/v2/flows/flow-en-1/enabled", json={"enabled": False})
        assert resp.status_code == 409, resp.text
        assert resp.json()["detail"] == "Stop the flow first."
    finally:
        _clear_overrides()


def test_enabled_guard_disable_ok_while_stopped():
    fake_db = FakeDB()
    fake_db.flows_v2.docs.append(
        _valid_flow(flow_id="flow-en-2", state="Stopped", enabled=True, deployedAt="2026-08-01T00:00:00.000Z")
    )
    client = _make_client(fake_db)
    try:
        resp = client.post("/api/v2/flows/flow-en-2/enabled", json={"enabled": False})
        assert resp.status_code == 200, resp.text
        assert fake_db.flows_v2.docs[0]["enabled"] is False
        assert any(e["action"] == "Flow disabled" for e in fake_db.audit_v2.docs)
    finally:
        _clear_overrides()


def test_enabled_guard_already_enabled_refused():
    fake_db = FakeDB()
    fake_db.flows_v2.docs.append(_valid_flow(flow_id="flow-en-3", enabled=True))
    client = _make_client(fake_db)
    try:
        resp = client.post("/api/v2/flows/flow-en-3/enabled", json={"enabled": True})
        assert resp.status_code == 409, resp.text
        assert resp.json()["detail"] == "Already enabled."
    finally:
        _clear_overrides()


# -------------------------------------------------------------- validate

def test_validate_endpoint_returns_issues_list():
    fake_db = FakeDB()
    fake_db.flows_v2.docs.append(
        {
            "id": "flow-empty",
            "name": "",
            "state": "Draft",
            "enabled": False,
            "cron": None,
            "blocks": [],
            "topics": [],
            "variables": [],
            "servicePins": {},
            "deployedAt": None,
            "lastRunAt": None,
            "createdAt": "",
            "updatedAt": "",
        }
    )
    client = _make_client(fake_db)
    try:
        resp = client.post("/api/v2/flows/flow-empty/validate")
        assert resp.status_code == 200, resp.text
        issues = resp.json()
        assert isinstance(issues, list)
        messages = [i["message"] for i in issues]
        assert any("Name the flow" in m for m in messages)
        assert any("empty" in m for m in messages)
        for issue in issues:
            assert set(issue.keys()) == {"blockId", "where", "message"}
    finally:
        _clear_overrides()


def test_validate_endpoint_clean_flow_returns_no_issues():
    fake_db = FakeDB()
    _seed_valid_service(fake_db)
    fake_db.flows_v2.docs.append(_valid_flow(flow_id="flow-clean-1"))
    client = _make_client(fake_db)
    try:
        resp = client.post("/api/v2/flows/flow-clean-1/validate")
        assert resp.status_code == 200, resp.text
        assert resp.json() == []
    finally:
        _clear_overrides()


# --------------------------------------------------------- read-only stubs

def test_dlq_metrics_messages_runtime_honest_placeholders():
    fake_db = FakeDB()
    fake_db.flows_v2.docs.append(_valid_flow(flow_id="flow-ro-1"))
    client = _make_client(fake_db)
    try:
        assert client.get("/api/v2/flows/flow-ro-1/dlq").json() == {"records": []}
        assert client.get("/api/v2/flows/flow-ro-1/metrics").json() == {
            "available": False,
            "reason": "not deployed",
        }
        assert client.get("/api/v2/flows/flow-ro-1/messages", params={"topic": "x"}).json() == {
            "messages": [],
            "source": "unavailable",
        }
        assert client.get("/api/v2/flows/flow-ro-1/runtime").status_code == 404
    finally:
        _clear_overrides()


# ------------------------------------------------------------- dashboard

def test_dashboard_summary_math_over_seeded_set():
    fake_db = FakeDB()

    flow_a = _valid_flow(flow_id="fa", state="Running", deployedAt="2026-08-01T00:00:00.000Z")
    flow_a["blocks"] = [
        {
            "id": "a-kc",
            "adapter": "kafka_kc",
            "name": "A KC",
            "parentId": None,
            "serviceId": None,
            "entity": "x",
            "config": {},
            "transforms": [],
        }
    ]
    flow_b = _valid_flow(flow_id="fb", state="Draft", deployedAt=None)
    flow_b["blocks"] = [
        {
            "id": "b-kc",
            "adapter": "kc",
            "name": "B KC",
            "parentId": None,
            "serviceId": None,
            "entity": "x",
            "config": {},
            "transforms": [],
        },
        {
            "id": "b-kc2",
            "adapter": "kafka_kc",
            "name": "B KC2",
            "parentId": None,
            "serviceId": None,
            "entity": "y",
            "config": {},
            "transforms": [],
        },
    ]
    flow_c = _valid_flow(flow_id="fc", state="Stopped", deployedAt="2026-08-01T00:00:00.000Z")
    flow_c["blocks"] = []

    fake_db.flows_v2.docs.extend([flow_a, flow_b, flow_c])

    fake_db.connections_v2.docs.extend(
        [
            {"id": "c1", "type": "nifi", "name": "NiFi", "active": True, "health": "Healthy"},
            {"id": "c2", "type": "kafka", "name": "Kafka", "active": True, "health": "Failed"},
            {"id": "c3", "type": "apicurio", "name": "Registry", "active": False, "health": "Healthy"},
        ]
    )

    fake_db.schemas_v2.docs.extend([{"id": "s1"}, {"id": "s2"}])

    fake_db.runtimes_v2.docs.append(
        {
            "flowId": "fa",
            "connectors": [
                {"name": "conn1", "state": "RUNNING"},
                {"name": "conn2", "state": "PAUSED"},
            ],
        }
    )

    client = _make_client(fake_db)
    try:
        resp = client.get("/api/v2/dashboard/summary")
        assert resp.status_code == 200, resp.text
        assert resp.json() == {
            "totalFlows": 3,
            "runningFlows": 1,
            "approvedSchemas": 2,
            "connectionsHealthy": 1,
            "connectionsTotal": 2,
            "sinkConnectorsRunning": 1,
            "sinkConnectorsTotal": 2,
            "sinkConnectorsUndeployed": 2,
        }
    finally:
        _clear_overrides()


# ------------------------------------------------------------------ audit

def test_audit_search_and_limit():
    fake_db = FakeDB()
    fake_db.audit_v2.docs.extend(
        [
            {
                "id": "a1",
                "ts": "2026-08-01T00:00:00.000Z",
                "user": "admin",
                "action": "Flow created",
                "object": "Flow",
                "target": "Alpha",
                "status": "Success",
                "details": None,
            },
            {
                "id": "a2",
                "ts": "2026-08-02T00:00:00.000Z",
                "user": "admin",
                "action": "Flow deleted",
                "object": "Flow",
                "target": "Beta",
                "status": "Warning",
                "details": "gone",
            },
            {
                "id": "a3",
                "ts": "2026-08-03T00:00:00.000Z",
                "user": "admin",
                "action": "Schema approved",
                "object": "Schema",
                "target": "beta-value",
                "status": "Success",
                "details": None,
            },
        ]
    )
    client = _make_client(fake_db)
    try:
        resp = client.get("/api/v2/audit/")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert [e["id"] for e in body] == ["a3", "a2", "a1"]  # ts desc

        resp_search = client.get("/api/v2/audit/", params={"search": "beta"})
        ids = {e["id"] for e in resp_search.json()}
        assert ids == {"a2", "a3"}  # target "Beta" / "beta-value", case-insensitive

        resp_limit = client.get("/api/v2/audit/", params={"limit": 1})
        assert len(resp_limit.json()) == 1
        assert resp_limit.json()[0]["id"] == "a3"

        resp_over_cap = client.get("/api/v2/audit/", params={"limit": 501})
        assert resp_over_cap.status_code == 422
    finally:
        _clear_overrides()
