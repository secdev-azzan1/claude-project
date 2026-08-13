"""Unit tests for T7.2+T7.3+T7.4's flow lifecycle
(`services/adapter/deployer/lifecycle.py`), with `nifi_apply` /
`connect_apply` / `topics` monkeypatched so nothing here talks to a real
NiFi/Kafka Connect/Kafka — that live-path coverage is
`tests/live/test_nifi_apply_live.py` (marked `@pytest.mark.live`, excluded
by default).

Follows the `FakeDB`-on-`FaultInjectingCollection` pattern from
`tests/test_v2_flows.py` / `tests/resilience/conftest.py`, and the
`async_test` (`asyncio.run`) pattern from `tests/test_iceberg_sinks_lifecycle.py`
(this repo has no pytest-asyncio dependency).
"""
from __future__ import annotations

import asyncio
from functools import wraps
from pathlib import Path
import sys

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services.adapter.common import COLLECTIONS
from services.adapter.deployer import connect_apply, lifecycle, nifi_apply, topics
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


# ------------------------------------------------------------------ fixtures


def _http_kafka_flow(flow_id: str = "flow-1", **overrides):
    """http read (no auth) -> kafka write. Compiles cleanly with just
    nifi/kafka/apicurio connections active — no kafka_connect/redis/apisix
    needed, keeping the preflight-happy-path fixtures minimal."""
    flow = {
        "id": flow_id,
        "name": "Test Flow",
        "description": None,
        "state": "Draft",
        "enabled": True,
        "cron": "*/5 * * * *",
        "blocks": [
            {
                "id": "b1", "adapter": "http", "mode": "read", "name": "Fetch", "parentId": None,
                "serviceId": "svc-1", "entity": None,
                "config": {
                    "path": "/x", "method": "GET", "responseFormat": "json", "recordPath": "$.items[*]",
                    "split": True, "pagination": {"type": "none", "fields": {}},
                },
                "transforms": [],
            },
            {
                "id": "b2", "adapter": "kafka", "mode": "write", "name": "Write Topic", "parentId": "b1",
                "serviceId": None, "entity": "thing", "config": {}, "transforms": [],
            },
        ],
        "topics": [], "variables": [], "servicePins": {},
        "deployedAt": None, "nifiProcessGroupId": None, "runtimeScopeMap": None,
        "lastRunAt": None, "createdAt": "2026-01-01T00:00:00.000Z", "updatedAt": "2026-01-01T00:00:00.000Z",
    }
    flow.update(overrides)
    return flow


def _kc_flow(flow_id: str = "flow-kc-1", **overrides):
    """http read -> kafka_kc (governed Iceberg sink, dedup last)."""
    flow = {
        "id": flow_id,
        "name": "KC Flow",
        "description": None,
        "state": "Draft",
        "enabled": True,
        "cron": "*/5 * * * *",
        "blocks": [
            {
                "id": "b1", "adapter": "http", "mode": "read", "name": "Fetch", "parentId": None,
                "serviceId": "svc-1", "entity": None,
                "config": {
                    "path": "/x", "method": "GET", "responseFormat": "json", "recordPath": "$.items[*]",
                    "split": True, "pagination": {"type": "none", "fields": {}},
                },
                "transforms": [],
            },
            {
                "id": "b2", "adapter": "kafka_kc", "name": "To Iceberg", "parentId": "b1",
                "serviceId": "svc-iceberg", "entity": "thing",
                "config": {"sinkServiceId": "svc-iceberg"},
                "transforms": [
                    {"id": "t1", "kind": "dedup", "config": {"identityFields": ["id"], "excludedFields": [], "windowHours": 24}},
                ],
            },
        ],
        "topics": [], "variables": [], "servicePins": {},
        "deployedAt": None, "nifiProcessGroupId": None, "runtimeScopeMap": None,
        "lastRunAt": None, "createdAt": "2026-01-01T00:00:00.000Z", "updatedAt": "2026-01-01T00:00:00.000Z",
    }
    flow.update(overrides)
    return flow


def _seed_http_service(fake_db: FakeDB, service_id: str = "svc-1"):
    fake_db.services_v2.docs.append({
        "id": service_id, "type": "http", "name": "Test Service", "revision": 1, "retired": False,
        "health": "Healthy", "config": {"baseUrl": "https://example.internal", "authMode": "none"},
        "hasSecret": False, "createdAt": "2026-01-01T00:00:00.000Z", "updatedAt": "2026-01-01T00:00:00.000Z",
    })


def _seed_iceberg_service(fake_db: FakeDB, service_id: str = "svc-iceberg"):
    fake_db.services_v2.docs.append({
        "id": service_id, "type": "sink_destination", "name": "Iceberg", "revision": 1, "retired": False,
        "health": "Healthy",
        "config": {"kind": "iceberg_catalog", "catalogUrl": "http://polaris.internal:8181/api/catalog", "warehouse": "bronze"},
        "hasSecret": False, "createdAt": "2026-01-01T00:00:00.000Z", "updatedAt": "2026-01-01T00:00:00.000Z",
    })


def _seed_core_connections(fake_db: FakeDB, *, kafka_connect: bool = False, redis: bool = False):
    fake_db.connections_v2.docs.extend([
        {"id": "conn-nifi", "type": "nifi", "name": "NiFi", "active": True, "health": "Healthy",
         "reachability": "Reachable", "lastTestedAt": None,
         "config": {"url": "https://nifi.test", "authMode": "basic", "username": "admin", "password": "pw"}, "hasSecret": True},
        {"id": "conn-kafka", "type": "kafka", "name": "Kafka", "active": True, "health": "Healthy",
         "reachability": "Reachable", "lastTestedAt": None,
         "config": {"bootstrapServers": "kafka:9092", "mode": "native", "securityProtocol": "PLAINTEXT"}, "hasSecret": False},
        {"id": "conn-apicurio", "type": "apicurio", "name": "Apicurio", "active": True, "health": "Healthy",
         "reachability": "Reachable", "lastTestedAt": None,
         "config": {"url": "https://apicurio.test", "authMode": "none"}, "hasSecret": False},
    ])
    if kafka_connect:
        fake_db.connections_v2.docs.append(
            {"id": "conn-kc", "type": "kafka_connect", "name": "Connect", "active": True, "health": "Healthy",
             "reachability": "Reachable", "lastTestedAt": None, "config": {"url": "https://connect.test"}, "hasSecret": False}
        )
    if redis:
        fake_db.connections_v2.docs.append(
            {"id": "conn-redis", "type": "redis", "name": "Redis", "active": True, "health": "Healthy",
             "reachability": "Reachable", "lastTestedAt": None,
             "config": {"host": "redis.internal", "port": 6379, "dedupDb": 0, "password": "pw"}, "hasSecret": True}
        )


def _seed_approved_schema(fake_db: FakeDB, *, flow_id: str, block_id: str):
    fake_db.schemas_v2.docs.append({
        "id": "schema-1", "subject": "raw.kc_flow.thing-value", "entity": "thing", "flowId": flow_id, "blockId": block_id,
        "provenance": "manual", "fields": [], "rawAvro": "", "approvedAt": "2026-01-01T00:00:00.000Z",
        "registryGlobalId": 1, "approvals": [],
    })


def _patch_provenance_probes(monkeypatch):
    """The real probes make live network calls — stub them so
    deploy()'s best-effort provenance stamp stays instant and offline."""
    async def fake_nifi_probe(conn):
        return {"ok": True, "fingerprint": "root-pg-fingerprint", "reachable": True, "error": None}

    async def fake_connect_probe(conn):
        return {"ok": True, "fingerprint": "kafka-cluster-id", "reachable": True, "error": None}

    monkeypatch.setattr(lifecycle, "probe_nifi_fingerprint", fake_nifi_probe)
    monkeypatch.setattr(lifecycle, "probe_kafka_connect_fingerprint", fake_connect_probe)


def _patch_ensure_topics_ok(monkeypatch, calls: list):
    async def fake_ensure_topics(kafka_conn, topic_specs):
        calls.append(list(topic_specs))
        return [{"name": t.name, "kind": t.kind, "ok": True, "error": None} for t in topic_specs]

    monkeypatch.setattr(topics, "ensure_topics", fake_ensure_topics)


# --------------------------------------------------------------- 1. preflight


@async_test
async def test_deploy_preflight_failure_applies_nothing(monkeypatch):
    """No connections_v2 docs seeded at all -> every "<X> connection active"
    preflight row fails -> deploy() must raise DeployPreflightFailed WITHOUT
    ever calling into nifi_apply/topics/connect_apply."""
    fake_db = FakeDB()
    _seed_http_service(fake_db)
    flow_doc = _http_kafka_flow()
    fake_db.flows_v2.docs.append(flow_doc)

    apply_calls = []
    ensure_topics_calls = []

    async def fail_if_called(*args, **kwargs):
        apply_calls.append((args, kwargs))
        raise AssertionError("nifi_apply.apply_plan must not be called when preflight fails")

    monkeypatch.setattr(nifi_apply, "apply_plan", fail_if_called)
    _patch_ensure_topics_ok(monkeypatch, ensure_topics_calls)  # would still record a call if reached

    try:
        await lifecycle.deploy(fake_db, flow_doc)
        assert False, "expected DeployPreflightFailed"
    except lifecycle.DeployPreflightFailed as exc:
        failing = [r for r in exc.rows if not r["ok"]]
        assert failing, "expected at least one failing preflight row"
        assert any("NiFi" in r["label"] for r in failing)

    assert apply_calls == []
    assert ensure_topics_calls == []
    # State never touched.
    assert fake_db.flows_v2.docs[0]["state"] == "Draft"
    assert fake_db.flows_v2.docs[0]["nifiProcessGroupId"] is None
    refused = [e for e in fake_db.audit_v2.docs if e["action"] == "Flow deploy refused (preflight)"]
    assert len(refused) == 1
    assert refused[0]["status"] == "Failed"


# ----------------------------------------------------------------- 2. deploy


@async_test
async def test_deploy_happy_path_persists_scope_map_and_state(monkeypatch):
    fake_db = FakeDB()
    _seed_http_service(fake_db)
    _seed_core_connections(fake_db)
    flow_doc = _http_kafka_flow()
    fake_db.flows_v2.docs.append(flow_doc)
    _patch_provenance_probes(monkeypatch)

    ensure_topics_calls = []
    _patch_ensure_topics_ok(monkeypatch, ensure_topics_calls)

    applied_call_plans = []

    async def fake_apply_plan(nifi_conn, plan):
        applied_call_plans.append(plan)
        assert nifi_conn["endpoint"] == "https://nifi.test"
        groups = {g.blockId: f"pg-{g.blockId}" for g in plan.rootGroup.childGroups}
        components = {
            g.blockId: {p.key: f"nifi-{g.blockId}-{p.key}" for p in g.processors}
            for g in plan.rootGroup.childGroups
        }
        return nifi_apply.AppliedResult(
            process_group_id="pg-root", parameter_context_id="pc-1", parameter_context_name=plan.parameterContext.name,
            groups=groups, components=components,
        )

    monkeypatch.setattr(nifi_apply, "apply_plan", fake_apply_plan)

    create_connectors_calls = []

    async def fake_create_connectors(kc_conn, connectors):
        create_connectors_calls.append(connectors)
        return []

    monkeypatch.setattr(connect_apply, "create_connectors", fake_create_connectors)

    result = await lifecycle.deploy(fake_db, flow_doc)

    # Applied exactly once, with a plan whose topics were also ensured.
    assert len(applied_call_plans) == 1
    assert len(ensure_topics_calls) == 1
    assert create_connectors_calls == []  # no kafka_kc/kc block on this flow -> no connectors

    assert result["state"] == "Stopped"
    assert result["deployedAt"]
    assert result["nifiProcessGroupId"] == "pg-root"
    scope_map = result["runtimeScopeMap"]
    assert set(scope_map.keys()) == {"b1", "b2"}
    assert scope_map["b1"]["processGroupId"] == "pg-b1"
    assert scope_map["b1"]["components"]["trigger"] == "nifi-b1-trigger"
    assert scope_map["b2"]["adapter"] == "kafka"
    assert scope_map["b2"]["topics"] == ["raw.test_flow.thing"]

    deployed_events = [e for e in fake_db.audit_v2.docs if e["action"] == "Flow deployed"]
    assert len(deployed_events) == 1
    assert deployed_events[0]["status"] == "Success"


@async_test
async def test_deploy_with_kafka_kc_creates_connectors_and_records_names(monkeypatch):
    fake_db = FakeDB()
    _seed_http_service(fake_db)
    _seed_iceberg_service(fake_db)
    _seed_core_connections(fake_db, kafka_connect=True, redis=True)
    flow_doc = _kc_flow()
    _seed_approved_schema(fake_db, flow_id=flow_doc["id"], block_id="b2")
    fake_db.flows_v2.docs.append(flow_doc)
    _patch_provenance_probes(monkeypatch)

    async def fake_list_plugins(conn):
        return {"ok": True, "data": [{"class": "org.apache.iceberg.connect.IcebergSinkConnector"}]}

    monkeypatch.setattr(lifecycle.kafka_connect_client, "list_connector_plugins", fake_list_plugins)

    ensure_topics_calls = []
    _patch_ensure_topics_ok(monkeypatch, ensure_topics_calls)

    async def fake_apply_plan(nifi_conn, plan):
        groups = {g.blockId: f"pg-{g.blockId}" for g in plan.rootGroup.childGroups}
        components = {g.blockId: {p.key: f"nifi-{p.key}" for p in g.processors} for g in plan.rootGroup.childGroups}
        return nifi_apply.AppliedResult(
            process_group_id="pg-root", parameter_context_id="pc-1", parameter_context_name=plan.parameterContext.name,
            groups=groups, components=components,
        )

    monkeypatch.setattr(nifi_apply, "apply_plan", fake_apply_plan)

    create_connectors_calls = []

    async def fake_create_connectors(kc_conn, connectors):
        create_connectors_calls.append(connectors)
        assert kc_conn["endpoint"] == "https://connect.test"
        return [{"name": c.name, "ownerBlockId": c.ownerBlockId, "ok": True, "paused": True} for c in connectors]

    monkeypatch.setattr(connect_apply, "create_connectors", fake_create_connectors)

    result = await lifecycle.deploy(fake_db, flow_doc)

    assert len(create_connectors_calls) == 1
    created = create_connectors_calls[0]
    assert len(created) == 1
    assert created[0].name == "kc_flow.b2.kafka_kc"

    scope_map = result["runtimeScopeMap"]
    assert scope_map["b2"]["connectorNames"] == ["kc_flow.b2.kafka_kc"]
    assert scope_map["b2"]["engine"] == "nifi"  # kafka_kc still compiles NiFi components + a connector


# --------------------------------------------------------------- 3. undeploy


@async_test
async def test_undeploy_keeps_dlq_and_empties_only_owned_topics(monkeypatch):
    fake_db = FakeDB()
    _seed_core_connections(fake_db)
    flow_doc = _http_kafka_flow(
        flow_id="flow-u1", state="Stopped", deployedAt="2026-08-01T00:00:00.000Z", nifiProcessGroupId="pg-root",
        runtimeScopeMap={
            "b1": {"adapter": "http", "engine": "nifi", "groupName": "fetch__http", "processGroupId": "pg-b1",
                   "components": {}, "connectorNames": [], "topics": []},
            "b2": {"adapter": "kafka", "engine": "nifi", "groupName": "write_topic__kafka", "processGroupId": "pg-b2",
                   "components": {}, "connectorNames": [], "topics": ["raw.test_flow.thing"]},
        },
    )
    fake_db.flows_v2.docs.append(flow_doc)

    delete_pg_calls = []

    async def fake_delete_flow_pg(nifi_conn, pg_id):
        delete_pg_calls.append(pg_id)
        return {"ok": True}

    monkeypatch.setattr(nifi_apply, "delete_flow_pg", fake_delete_flow_pg)

    empty_topic_calls = []

    async def fake_empty_topic(kafka_conn, topic):
        empty_topic_calls.append(topic)
        return {"ok": True, "cleared_messages": 3}

    monkeypatch.setattr(topics, "empty_topic", fake_empty_topic)

    delete_connectors_calls = []

    async def fake_delete_connectors(kc_conn, names):
        delete_connectors_calls.append(names)
        return []

    monkeypatch.setattr(connect_apply, "delete_connectors", fake_delete_connectors)

    result = await lifecycle.undeploy(fake_db, flow_doc)

    assert delete_pg_calls == ["pg-root"]
    assert delete_connectors_calls == []  # no connector names in scope map on this flow
    # Exactly the owned data topic is emptied -- the DLQ ("dlq.test_flow")
    # never appears in any block's `topics` list (compile_flow gives it
    # ownerBlockId=None), so it is never a candidate here either.
    assert empty_topic_calls == ["raw.test_flow.thing"]
    assert "dlq.test_flow" not in empty_topic_calls

    assert result["state"] == "Draft"
    assert result["deployedAt"] is None
    assert result["nifiProcessGroupId"] is None
    assert result["runtimeScopeMap"] is None
    undeployed = [e for e in fake_db.audit_v2.docs if e["action"] == "Flow undeployed"]
    assert len(undeployed) == 1


# -------------------------------------------------------------- 4. stop_clear


@async_test
async def test_stop_clear_audits_dropped_queue_counts(monkeypatch):
    fake_db = FakeDB()
    _seed_core_connections(fake_db)
    flow_doc = _http_kafka_flow(
        flow_id="flow-sc1", state="Running", deployedAt="2026-08-01T00:00:00.000Z", nifiProcessGroupId="pg-root",
        runtimeScopeMap={
            "b1": {"adapter": "http", "engine": "nifi", "groupName": "fetch__http", "processGroupId": "pg-b1", "components": {}, "connectorNames": [], "topics": []},
            "b2": {"adapter": "kafka", "engine": "nifi", "groupName": "write_topic__kafka", "processGroupId": "pg-b2", "components": {}, "connectorNames": [], "topics": ["raw.test_flow.thing"]},
        },
    )
    fake_db.flows_v2.docs.append(flow_doc)

    async def fake_stop_pg(nifi_conn, pg_id):
        return {"ok": True, "state": "Stopped"}

    monkeypatch.setattr(nifi_apply, "stop_pg", fake_stop_pg)

    drop_calls = []

    async def fake_drop_all_queues(nifi_conn, pg_id):
        drop_calls.append(pg_id)
        return {"ok": True, "connections": 3, "dropped": 42, "failed": []}

    monkeypatch.setattr(nifi_apply, "drop_all_queues", fake_drop_all_queues)

    result = await lifecycle.stop_clear(fake_db, flow_doc)

    assert drop_calls == ["pg-root"]
    assert result["state"] == "Stopped"
    cleared = [e for e in fake_db.audit_v2.docs if e["action"] == "Flow stopped and cleared"]
    assert len(cleared) == 1
    assert "42" in cleared[0]["details"]
    assert "3" in cleared[0]["details"]


# ------------------------------------------------------------ 5. dedup epoch


@async_test
async def test_clear_dedup_cache_bumps_epoch_and_flags_redeploy():
    fake_db = FakeDB()
    flow_doc = _kc_flow(flow_id="flow-dedup-1")
    fake_db.flows_v2.docs.append(flow_doc)

    result = await lifecycle.clear_dedup_cache(fake_db, flow_doc, "b2")
    assert result["dedupEpoch"] == 1
    assert result["redeployRequired"] is True
    assert result["blockId"] == "b2"

    stored = fake_db.flows_v2.docs[0]
    assert stored["blocks"][1]["transforms"][0]["config"]["dedupEpoch"] == 1

    # Clearing again bumps again -- reads back the persisted epoch, not the
    # original in-memory `flow_doc` argument (mirrors the real
    # find-doc-then-update flow through the router).
    stored_doc = await fake_db.flows_v2.find_one({"id": "flow-dedup-1"})
    result2 = await lifecycle.clear_dedup_cache(fake_db, stored_doc, "b2")
    assert result2["dedupEpoch"] == 2

    cleared_events = [e for e in fake_db.audit_v2.docs if e["action"] == "Dedup cache cleared"]
    assert len(cleared_events) == 2


@async_test
async def test_clear_dedup_cache_no_dedup_transform_raises():
    fake_db = FakeDB()
    flow_doc = _http_kafka_flow(flow_id="flow-nodedup-1")  # b2 has no transforms at all
    fake_db.flows_v2.docs.append(flow_doc)

    try:
        await lifecycle.clear_dedup_cache(fake_db, flow_doc, "b2")
        assert False, "expected LifecycleError"
    except lifecycle.LifecycleError as exc:
        assert "no dedup transform" in str(exc)
