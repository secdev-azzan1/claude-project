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


# ------------------------------------------------------- 6. M9: pause/resume trigger keys


def _jdbc_read_flow(flow_id: str = "flow-jdbc-1", **overrides):
    """A jdbc-rooted flow already Running -- carries its schedule on the
    "query" processor (QueryDatabaseTableRecord), per blocks_jdbc.py's
    `_compile_read`."""
    flow = {
        "id": flow_id, "name": "JDBC Flow", "description": None, "state": "Running", "enabled": True,
        "cron": "*/5 * * * *",
        "blocks": [
            {
                "id": "b1", "adapter": "jdbc", "mode": "read", "name": "Query", "parentId": None,
                "serviceId": "svc-db", "entity": "assets", "config": {"table": "assets"}, "transforms": [],
            },
        ],
        "topics": [], "variables": [], "servicePins": {},
        "deployedAt": "2026-08-01T00:00:00.000Z", "nifiProcessGroupId": "pg-root",
        "runtimeScopeMap": {
            "b1": {"adapter": "jdbc", "engine": "nifi", "groupName": "query__jdbc", "processGroupId": "pg-b1",
                   "components": {"query": "nid-query", "split": "nid-split"}, "connectorNames": [], "topics": []},
        },
        "lastRunAt": None, "createdAt": "2026-01-01T00:00:00.000Z", "updatedAt": "2026-01-01T00:00:00.000Z",
    }
    flow.update(overrides)
    return flow


def _kafka_read_flow(flow_id: str = "flow-kread-1", **overrides):
    """A kafka-read-rooted flow -- ingests through the "consume" processor
    (ConsumeKafka), per blocks_kafka.py."""
    flow = {
        "id": flow_id, "name": "Kafka Read Flow", "description": None, "state": "Running", "enabled": True,
        "cron": None,
        "blocks": [
            {
                "id": "b1", "adapter": "kafka", "mode": "read", "name": "Consume", "parentId": None,
                "serviceId": None, "entity": None, "config": {"topicName": "raw.x.y"}, "transforms": [],
            },
        ],
        "topics": [], "variables": [], "servicePins": {},
        "deployedAt": "2026-08-01T00:00:00.000Z", "nifiProcessGroupId": "pg-root",
        "runtimeScopeMap": {
            "b1": {"adapter": "kafka", "engine": "nifi", "groupName": "consume__kafka", "processGroupId": "pg-b1",
                   "components": {"consume": "nid-consume"}, "connectorNames": [], "topics": []},
        },
        "lastRunAt": None, "createdAt": "2026-01-01T00:00:00.000Z", "updatedAt": "2026-01-01T00:00:00.000Z",
    }
    flow.update(overrides)
    return flow


@async_test
async def test_pause_resolves_jdbc_root_query_processor(monkeypatch):
    """M9: before the fix, `_TRIGGER_KEYS = ("trigger",)` meant a jdbc-rooted
    flow's `_trigger_component_ids` came back empty and `pause()` raised
    "Could not resolve the flow's trigger processor(s)"."""
    fake_db = FakeDB()
    _seed_core_connections(fake_db)
    flow_doc = _jdbc_read_flow()
    fake_db.flows_v2.docs.append(flow_doc)

    pause_calls = []

    async def fake_pause_trigger(nifi_conn, ids):
        pause_calls.append(list(ids))
        return {"ok": True, "failed": []}

    monkeypatch.setattr(nifi_apply, "pause_trigger", fake_pause_trigger)

    result = await lifecycle.pause(fake_db, flow_doc)
    assert pause_calls == [["nid-query"]]
    assert result["state"] == "Paused"


@async_test
async def test_resume_resolves_kafka_read_root_consume_processor(monkeypatch):
    fake_db = FakeDB()
    _seed_core_connections(fake_db)
    flow_doc = _kafka_read_flow(state="Paused")
    fake_db.flows_v2.docs.append(flow_doc)

    resume_calls = []

    async def fake_resume_trigger(nifi_conn, ids):
        resume_calls.append(list(ids))
        return {"ok": True, "failed": []}

    monkeypatch.setattr(nifi_apply, "resume_trigger", fake_resume_trigger)

    result = await lifecycle.resume(fake_db, flow_doc)
    assert resume_calls == [["nid-consume"]]
    assert result["state"] == "Running"


@async_test
async def test_pause_http_root_still_resolves_trigger_key_no_regression(monkeypatch):
    fake_db = FakeDB()
    _seed_core_connections(fake_db)
    flow_doc = _http_kafka_flow(
        flow_id="flow-http-pause-1", state="Running", deployedAt="2026-08-01T00:00:00.000Z", nifiProcessGroupId="pg-root",
        runtimeScopeMap={
            "b1": {"adapter": "http", "engine": "nifi", "groupName": "fetch__http", "processGroupId": "pg-b1",
                   "components": {"trigger": "nid-trigger", "init": "nid-init", "fetch": "nid-fetch"},
                   "connectorNames": [], "topics": []},
            "b2": {"adapter": "kafka", "engine": "nifi", "groupName": "write_topic__kafka", "processGroupId": "pg-b2",
                   "components": {"publish": "nid-publish"}, "connectorNames": [], "topics": ["raw.test_flow.thing"]},
        },
    )
    fake_db.flows_v2.docs.append(flow_doc)

    pause_calls = []

    async def fake_pause_trigger(nifi_conn, ids):
        pause_calls.append(list(ids))
        return {"ok": True, "failed": []}

    monkeypatch.setattr(nifi_apply, "pause_trigger", fake_pause_trigger)
    result = await lifecycle.pause(fake_db, flow_doc)

    assert pause_calls == [["nid-trigger"]]
    assert result["state"] == "Paused"


# --------------------------------------------------------- 7. E7: delete after repair


@async_test
async def test_delete_after_repair_still_deletes_connectors_and_topics(monkeypatch):
    """E7: `runtime.repair_runtime` clears `deployedAt`/`nifiProcessGroupId`
    but deliberately KEEPS `runtimeScopeMap` -- `delete()` must still
    best-effort delete the flow's connectors + owned topics + DLQ even
    though neither `deployedAt` nor `nifiProcessGroupId` is set anymore
    (proven live -- docs/orchestration/e2e/journey-a-e.md DEFECT 6)."""
    fake_db = FakeDB()
    _seed_core_connections(fake_db, kafka_connect=True)
    flow_doc = _kc_flow(
        flow_id="flow-repair-1", state="Draft", deployedAt=None, nifiProcessGroupId=None,
        runtimeScopeMap={
            "b1": {"adapter": "http", "engine": "nifi", "groupName": "fetch__http", "processGroupId": "pg-b1",
                   "components": {}, "connectorNames": [], "topics": []},
            "b2": {"adapter": "kafka_kc", "engine": "nifi", "groupName": "to_iceberg__kafka_kc", "processGroupId": "pg-b2",
                   "components": {}, "connectorNames": ["kc_flow.b2.kafka_kc"], "topics": ["raw.kc_flow.thing"]},
        },
    )
    fake_db.flows_v2.docs.append(flow_doc)

    async def fail_if_undeploy_called(*args, **kwargs):
        raise AssertionError("undeploy() must not run when neither deployedAt nor nifiProcessGroupId is set")

    monkeypatch.setattr(lifecycle, "undeploy", fail_if_undeploy_called)

    delete_connectors_calls = []

    async def fake_delete_connectors(kc_conn, names):
        delete_connectors_calls.append(list(names))
        return [{"name": n, "ok": True, "error": None} for n in names]

    monkeypatch.setattr(connect_apply, "delete_connectors", fake_delete_connectors)

    delete_topic_calls = []

    async def fake_delete_topic(kafka_conn, name):
        delete_topic_calls.append(name)
        return {"ok": True}

    monkeypatch.setattr(topics, "delete_topic", fake_delete_topic)

    result = await lifecycle.delete(fake_db, flow_doc)

    assert delete_connectors_calls == [["kc_flow.b2.kafka_kc"]]
    assert "dlq.kc_flow" in delete_topic_calls
    assert "raw.kc_flow.thing" in delete_topic_calls
    assert result["ok"] is True
    assert result["orphans"] == []
    assert fake_db.flows_v2.docs == []

    deleted_events = [e for e in fake_db.audit_v2.docs if e["action"] == "Flow deleted"]
    assert len(deleted_events) == 1


@async_test
async def test_delete_records_orphan_when_connector_delete_fails(monkeypatch):
    fake_db = FakeDB()
    _seed_core_connections(fake_db, kafka_connect=True)
    flow_doc = _kc_flow(
        flow_id="flow-repair-2", state="Draft", deployedAt=None, nifiProcessGroupId=None,
        runtimeScopeMap={
            "b2": {"adapter": "kafka_kc", "engine": "nifi", "groupName": "to_iceberg__kafka_kc", "processGroupId": "pg-b2",
                   "components": {}, "connectorNames": ["kc_flow.b2.kafka_kc"], "topics": ["raw.kc_flow.thing"]},
        },
    )
    fake_db.flows_v2.docs.append(flow_doc)

    async def fake_delete_connectors(kc_conn, names):
        return [{"name": n, "ok": False, "error": "Connect unreachable"} for n in names]

    monkeypatch.setattr(connect_apply, "delete_connectors", fake_delete_connectors)

    async def fake_delete_topic(kafka_conn, name):
        return {"ok": True}

    monkeypatch.setattr(topics, "delete_topic", fake_delete_topic)

    result = await lifecycle.delete(fake_db, flow_doc)

    assert result["orphans"] == [{"kind": "connector", "ref": "kc_flow.b2.kafka_kc", "reason": "Connect unreachable"}]
    deleted_events = [e for e in fake_db.audit_v2.docs if e["action"] == "Flow deleted"]
    assert "connector" in deleted_events[0]["details"]


@async_test
async def test_delete_still_undeploys_when_deployed_and_records_no_double_connector_delete(monkeypatch):
    """When the flow IS still deployed, `undeploy()` runs (and handles
    connectors itself) -- the post-repair best-effort connector path must
    not run a SECOND time on top of it."""
    fake_db = FakeDB()
    _seed_core_connections(fake_db)
    flow_doc = _http_kafka_flow(
        flow_id="flow-del-normal-1", state="Stopped", deployedAt="2026-08-01T00:00:00.000Z", nifiProcessGroupId="pg-root",
        runtimeScopeMap={
            "b1": {"adapter": "http", "engine": "nifi", "groupName": "fetch__http", "processGroupId": "pg-b1",
                   "components": {}, "connectorNames": [], "topics": []},
            "b2": {"adapter": "kafka", "engine": "nifi", "groupName": "write_topic__kafka", "processGroupId": "pg-b2",
                   "components": {}, "connectorNames": [], "topics": ["raw.test_flow.thing"]},
        },
    )
    fake_db.flows_v2.docs.append(flow_doc)

    undeploy_calls = []
    orig_undeploy = lifecycle.undeploy

    async def spy_undeploy(db, doc):
        undeploy_calls.append(doc["id"])
        return await orig_undeploy(db, doc)

    monkeypatch.setattr(lifecycle, "undeploy", spy_undeploy)

    async def fake_delete_flow_pg(nifi_conn, pg_id):
        return {"ok": True}

    monkeypatch.setattr(nifi_apply, "delete_flow_pg", fake_delete_flow_pg)

    delete_topic_calls = []

    async def fake_delete_topic(kafka_conn, name):
        delete_topic_calls.append(name)
        return {"ok": True}

    monkeypatch.setattr(topics, "delete_topic", fake_delete_topic)

    async def fake_empty_topic(kafka_conn, name):
        return {"ok": True}

    monkeypatch.setattr(topics, "empty_topic", fake_empty_topic)

    result = await lifecycle.delete(fake_db, flow_doc)
    assert undeploy_calls == ["flow-del-normal-1"]
    assert result["ok"] is True
    assert set(delete_topic_calls) == {"dlq.test_flow", "raw.test_flow.thing"}
    assert result["orphans"] == []


# ------------------------------------------------- 7b. Journey-R teardown gap: delete after undeploy


@async_test
async def test_delete_after_undeploy_still_deletes_derived_data_topics(monkeypatch):
    """Journey-R teardown gap (journey-r-reverify.md cleanup item 2):
    `undeploy()` nulls `runtimeScopeMap`, so deleting the flow from its
    post-undeploy Draft state used to find NO owned data topics and leave
    `raw.<flow>.<entity>` alive on the cluster (proven live:
    `raw.e2er_fresh.e2er_product` survived; the DLQ, derived independently,
    was deleted). `delete()` now derives the data topics via the naming
    walk regardless of deploy state."""
    fake_db = FakeDB()
    _seed_core_connections(fake_db)
    flow_doc = _http_kafka_flow(
        flow_id="flow-post-undeploy-1", state="Draft",
        deployedAt=None, nifiProcessGroupId=None, runtimeScopeMap=None,  # undeploy() already ran
    )
    fake_db.flows_v2.docs.append(flow_doc)

    async def fail_if_undeploy_called(*args, **kwargs):
        raise AssertionError("undeploy() must not run for an already-undeployed flow")

    monkeypatch.setattr(lifecycle, "undeploy", fail_if_undeploy_called)

    delete_topic_calls = []

    async def fake_delete_topic(kafka_conn, name):
        delete_topic_calls.append(name)
        return {"ok": True}

    monkeypatch.setattr(topics, "delete_topic", fake_delete_topic)

    result = await lifecycle.delete(fake_db, flow_doc)

    # The data topic comes from the naming walk (blocks), NOT the (nulled) scope map.
    assert "raw.test_flow.thing" in delete_topic_calls
    assert "dlq.test_flow" in delete_topic_calls
    assert result["ok"] is True
    assert result["orphans"] == []
    assert fake_db.flows_v2.docs == []


# ------------------------------------------------------------------ 8. M10: undeploy clears dedup


@async_test
async def test_undeploy_bumps_dedup_epoch_for_every_dedup_bearing_block(monkeypatch):
    """M10: undeploy must clear dedup caches per MVP §7.9 -- the same
    dedupEpoch-bump mechanism `clear_dedup_cache()` uses, applied to every
    dedup-bearing block automatically."""
    fake_db = FakeDB()
    _seed_core_connections(fake_db)
    flow_doc = _kc_flow(
        flow_id="flow-u10-1", state="Stopped", deployedAt="2026-08-01T00:00:00.000Z", nifiProcessGroupId="pg-root",
        runtimeScopeMap={
            "b1": {"adapter": "http", "engine": "nifi", "groupName": "fetch__http", "processGroupId": "pg-b1",
                   "components": {}, "connectorNames": [], "topics": []},
            "b2": {"adapter": "kafka_kc", "engine": "nifi", "groupName": "to_iceberg__kafka_kc", "processGroupId": "pg-b2",
                   "components": {}, "connectorNames": [], "topics": []},
        },
    )
    fake_db.flows_v2.docs.append(flow_doc)

    async def fake_delete_flow_pg(nifi_conn, pg_id):
        return {"ok": True}

    monkeypatch.setattr(nifi_apply, "delete_flow_pg", fake_delete_flow_pg)

    result = await lifecycle.undeploy(fake_db, flow_doc)
    assert result["state"] == "Draft"

    stored = fake_db.flows_v2.docs[0]
    assert stored["blocks"][1]["transforms"][0]["config"]["dedupEpoch"] == 1
    # b1 has no dedup transform -- untouched.
    assert stored["blocks"][0]["transforms"] == []


@async_test
async def test_undeploy_bumps_epoch_again_on_second_undeploy(monkeypatch):
    fake_db = FakeDB()
    _seed_core_connections(fake_db)
    flow_doc = _kc_flow(
        flow_id="flow-u10-2", state="Stopped", deployedAt="2026-08-01T00:00:00.000Z", nifiProcessGroupId="pg-root",
        runtimeScopeMap={"b2": {"adapter": "kafka_kc", "engine": "nifi", "groupName": "g", "processGroupId": "pg-b2",
                                 "components": {}, "connectorNames": [], "topics": []}},
    )
    flow_doc["blocks"][1]["transforms"][0]["config"]["dedupEpoch"] = 3  # already bumped once before
    fake_db.flows_v2.docs.append(flow_doc)

    async def fake_delete_flow_pg(nifi_conn, pg_id):
        return {"ok": True}

    monkeypatch.setattr(nifi_apply, "delete_flow_pg", fake_delete_flow_pg)

    await lifecycle.undeploy(fake_db, flow_doc)
    stored = fake_db.flows_v2.docs[0]
    assert stored["blocks"][1]["transforms"][0]["config"]["dedupEpoch"] == 4


# --------------------------------------------------------- 9. M11: dedup config-change warning


@async_test
async def test_deploy_dedup_config_change_bumps_epoch_and_warns(monkeypatch):
    fake_db = FakeDB()
    _seed_http_service(fake_db)
    _seed_iceberg_service(fake_db)
    _seed_core_connections(fake_db, kafka_connect=True, redis=True)
    flow_doc = _kc_flow(flow_id="flow-dedup-warn-1")
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

    async def fake_create_connectors(kc_conn, connectors):
        return [{"name": c.name, "ownerBlockId": c.ownerBlockId, "ok": True, "paused": True} for c in connectors]

    monkeypatch.setattr(connect_apply, "create_connectors", fake_create_connectors)

    async def fake_delete_flow_pg(nifi_conn, pg_id):
        return {"ok": True}

    monkeypatch.setattr(nifi_apply, "delete_flow_pg", fake_delete_flow_pg)

    first = await lifecycle.deploy(fake_db, flow_doc)
    assert not first.get("preflightWarnings")
    assert (first["blocks"][1]["transforms"][0]["config"].get("dedupEpoch") or 0) == 0
    assert first["provenance"]["dedupConfigHashes"]["b2"]

    # Change identityFields on the already-deployed flow -- config changed
    # vs. what was hashed at the last deploy.
    stored = fake_db.flows_v2.docs[0]
    stored["blocks"][1]["transforms"][0]["config"]["identityFields"] = ["id", "sku"]
    stored["state"] = "Stopped"

    second = await lifecycle.deploy(fake_db, stored)
    assert second["blocks"][1]["transforms"][0]["config"]["dedupEpoch"] == 1
    assert second.get("preflightWarnings")
    warning = second["preflightWarnings"][0]
    assert warning["ok"] is True  # non-blocking
    assert "Dedup settings changed" in warning["detail"]
    assert "cache resets" in warning["detail"]


@async_test
async def test_deploy_dedup_config_unchanged_no_warning_no_bump(monkeypatch):
    fake_db = FakeDB()
    _seed_http_service(fake_db)
    _seed_iceberg_service(fake_db)
    _seed_core_connections(fake_db, kafka_connect=True, redis=True)
    flow_doc = _kc_flow(flow_id="flow-dedup-nowarn-1")
    _seed_approved_schema(fake_db, flow_id=flow_doc["id"], block_id="b2")
    fake_db.flows_v2.docs.append(flow_doc)
    _patch_provenance_probes(monkeypatch)

    async def fake_list_plugins(conn):
        return {"ok": True, "data": [{"class": "org.apache.iceberg.connect.IcebergSinkConnector"}]}

    monkeypatch.setattr(lifecycle.kafka_connect_client, "list_connector_plugins", fake_list_plugins)
    _patch_ensure_topics_ok(monkeypatch, [])

    async def fake_apply_plan(nifi_conn, plan):
        groups = {g.blockId: f"pg-{g.blockId}" for g in plan.rootGroup.childGroups}
        components = {g.blockId: {p.key: f"nifi-{p.key}" for p in g.processors} for g in plan.rootGroup.childGroups}
        return nifi_apply.AppliedResult(
            process_group_id="pg-root", parameter_context_id="pc-1", parameter_context_name=plan.parameterContext.name,
            groups=groups, components=components,
        )

    monkeypatch.setattr(nifi_apply, "apply_plan", fake_apply_plan)

    async def fake_create_connectors(kc_conn, connectors):
        return [{"name": c.name, "ownerBlockId": c.ownerBlockId, "ok": True, "paused": True} for c in connectors]

    monkeypatch.setattr(connect_apply, "create_connectors", fake_create_connectors)

    async def fake_delete_flow_pg(nifi_conn, pg_id):
        return {"ok": True}

    monkeypatch.setattr(nifi_apply, "delete_flow_pg", fake_delete_flow_pg)

    await lifecycle.deploy(fake_db, flow_doc)
    stored = fake_db.flows_v2.docs[0]
    second = await lifecycle.deploy(fake_db, stored)

    assert not second.get("preflightWarnings")
    assert (second["blocks"][1]["transforms"][0]["config"].get("dedupEpoch") or 0) == 0


# ------------------------------------------------------- 9b. R3-F1: post-apply validation gate


@async_test
async def test_deploy_fails_when_processor_invalid_after_apply(monkeypatch):
    """R3-F1 (journey-r-reverify.md): deploy() must NOT report success while
    a compiled processor is INVALID in NiFi (proven live — the session-token
    flow's login step could never validate, yet deploy returned 200/Stopped
    and the failure only surfaced at start). The gate runs inside the REAL
    `apply_plan` here (only `nifi_api_request` is faked): INVALID ->
    best-effort PG teardown -> NifiApplyError -> LifecycleError (502 at the
    router), and NO deployed state is persisted."""
    from tests.test_nifi_apply import FakeNifi

    fake_db = FakeDB()
    _seed_http_service(fake_db)
    _seed_core_connections(fake_db)
    flow_doc = _http_kafka_flow(flow_id="flow-invalid-proc-1")
    fake_db.flows_v2.docs.append(flow_doc)
    _patch_provenance_probes(monkeypatch)
    _patch_ensure_topics_ok(monkeypatch, [])

    fake = FakeNifi(invalid_processor_names={"fetch"},
                    validation_errors=["'HTTP URL' is invalid because the property cannot be resolved."])
    monkeypatch.setattr(nifi_apply, "nifi_api_request", fake.request)

    async def fake_root_pg(*args, **kwargs):
        return "root-pg"

    monkeypatch.setattr(nifi_apply, "get_nifi_root_process_group_id", fake_root_pg)
    monkeypatch.setattr(nifi_apply, "_VALIDATION_GATE_TIMEOUT_SECS", 0.0)
    monkeypatch.setattr(nifi_apply, "_VALIDATION_GATE_POLL_SECS", 0.0)

    async def fake_cs_config(url, cs_id, **kwargs):
        return {"state": "ENABLED", "validation_errors": []}

    monkeypatch.setattr(nifi_apply.nifi_flow_manager, "get_controller_service_config", fake_cs_config)

    teardown_calls = []

    async def fake_delete_flow_pg(conn, pg_id):
        teardown_calls.append(pg_id)
        return {"ok": True}

    monkeypatch.setattr(nifi_apply, "delete_flow_pg", fake_delete_flow_pg)

    try:
        await lifecycle.deploy(fake_db, flow_doc)
        assert False, "expected LifecycleError"
    except lifecycle.LifecycleError as exc:
        msg = str(exc)
        assert "fetch" in msg  # component named in the surfaced 502 reason
        assert "'HTTP URL' is invalid" in msg

    assert len(teardown_calls) == 1  # the created PG was torn down

    stored = fake_db.flows_v2.docs[0]
    assert stored["state"] == "Draft"  # never flipped to Stopped/deployed
    assert stored.get("nifiProcessGroupId") is None
    assert stored.get("runtimeScopeMap") is None
    assert stored.get("deployedAt") is None
    failed_events = [e for e in fake_db.audit_v2.docs if e["action"] == "Flow deploy failed"]
    assert len(failed_events) == 1


# ------------------------------------------------------- 10. M13: topic reservation


@async_test
async def test_deploy_refuses_when_topic_collides_with_another_flows_owned_topic(monkeypatch):
    fake_db = FakeDB()
    _seed_http_service(fake_db)
    _seed_core_connections(fake_db)
    flow_doc = _http_kafka_flow(flow_id="flow-collide-1")
    fake_db.flows_v2.docs.append(flow_doc)

    # Another flow already owns the exact topic this one would derive
    # ("raw.test_flow.thing" -- both flows named "Test Flow").
    fake_db.flows_v2.docs.append({
        "id": "flow-other-owner", "name": "Some Other Flow",
        "runtimeScopeMap": {"b2": {"topics": ["raw.test_flow.thing"], "connectorNames": []}},
    })

    _patch_provenance_probes(monkeypatch)
    _patch_ensure_topics_ok(monkeypatch, [])

    apply_calls = []

    async def fail_if_called(*a, **k):
        apply_calls.append((a, k))
        raise AssertionError("apply_plan must not be called when a topic-reservation row fails")

    monkeypatch.setattr(nifi_apply, "apply_plan", fail_if_called)

    try:
        await lifecycle.deploy(fake_db, flow_doc)
        assert False, "expected DeployPreflightFailed"
    except lifecycle.DeployPreflightFailed as exc:
        failing = [r for r in exc.rows if not r["ok"]]
        assert any("raw.test_flow.thing" in r["detail"] and "Some Other Flow" in r["detail"] for r in failing)

    assert apply_calls == []


@async_test
async def test_deploy_topic_reservation_passes_when_no_collision(monkeypatch):
    fake_db = FakeDB()
    _seed_http_service(fake_db)
    _seed_core_connections(fake_db)
    flow_doc = _http_kafka_flow(flow_id="flow-nocollide-1")
    fake_db.flows_v2.docs.append(flow_doc)
    _patch_provenance_probes(monkeypatch)
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

    result = await lifecycle.deploy(fake_db, flow_doc)
    assert result["state"] == "Stopped"
    assert len(ensure_topics_calls) == 1


@async_test
async def test_deploy_topic_reservation_skips_silently_when_kafka_listing_unavailable(monkeypatch):
    """The live-cluster half of the check is best-effort -- an unreachable
    Kafbat listing must never itself block a deploy."""
    fake_db = FakeDB()
    _seed_http_service(fake_db)
    _seed_core_connections(fake_db)
    flow_doc = _http_kafka_flow(flow_id="flow-listing-unavailable-1")
    fake_db.flows_v2.docs.append(flow_doc)
    _patch_provenance_probes(monkeypatch)
    _patch_ensure_topics_ok(monkeypatch, [])

    async def fake_list_topics(kafka_conn):
        return {"ok": False, "error": "Kafbat unreachable"}

    monkeypatch.setattr(topics, "list_topics", fake_list_topics)

    async def fake_apply_plan(nifi_conn, plan):
        groups = {g.blockId: f"pg-{g.blockId}" for g in plan.rootGroup.childGroups}
        components = {g.blockId: {p.key: f"nifi-{p.key}" for p in g.processors} for g in plan.rootGroup.childGroups}
        return nifi_apply.AppliedResult(
            process_group_id="pg-root", parameter_context_id="pc-1", parameter_context_name=plan.parameterContext.name,
            groups=groups, components=components,
        )

    monkeypatch.setattr(nifi_apply, "apply_plan", fake_apply_plan)

    result = await lifecycle.deploy(fake_db, flow_doc)
    assert result["state"] == "Stopped"


@async_test
async def test_deploy_treats_live_topic_matching_its_own_derivation_as_owned(monkeypatch):
    """M13 redeploy-after-undeploy fix (this replaces the old
    `test_deploy_refuses_when_topic_exists_live_and_unowned`, whose premise
    the fix intentionally changes -- see below): a live topic whose name is
    exactly what THIS flow's own deterministic naming walk derives is now
    always excluded from the "live and unowned" check, deployed before or
    not. `runtimeScopeMap` cannot be trusted to tell "never deployed" apart
    from "deployed, then undeployed (topic emptied but left live, MVP
    §7.9)" -- `undeploy()` nulls it in both the leftover-topic AND the
    never-existed cases alike, so a scope-map-only exclusion set reads a
    just-undeployed flow's own topic as foreign and blocks its own
    redeploy forever (the reported bug -- see
    `test_deploy_redeploy_after_undeploy_excludes_own_leftover_topic`
    below). Cross-flow uniqueness is guaranteed by the OTHER-flows side of
    this same check instead (naming walk + scope map over every OTHER
    flow -- see `test_deploy_refuses_when_other_flows_naming_walk_derives_same_topic`
    and `test_deploy_refuses_when_topic_collides_with_another_flows_owned_topic`
    below): once no other tracked flow claims a name, only this flow could
    ever have derived it, so a coincidental live match is assumed to be
    this flow reclaiming its own topic rather than a foreign collision."""
    fake_db = FakeDB()
    _seed_http_service(fake_db)
    _seed_core_connections(fake_db)
    flow_doc = _http_kafka_flow(flow_id="flow-live-collide-1")
    fake_db.flows_v2.docs.append(flow_doc)
    _patch_provenance_probes(monkeypatch)
    _patch_ensure_topics_ok(monkeypatch, [])

    async def fake_list_topics(kafka_conn):
        return {"ok": True, "topics": ["raw.test_flow.thing", "some.unrelated.topic"]}

    monkeypatch.setattr(topics, "list_topics", fake_list_topics)

    async def fake_apply_plan(nifi_conn, plan):
        groups = {g.blockId: f"pg-{g.blockId}" for g in plan.rootGroup.childGroups}
        components = {g.blockId: {p.key: f"nifi-{p.key}" for p in g.processors} for g in plan.rootGroup.childGroups}
        return nifi_apply.AppliedResult(
            process_group_id="pg-root", parameter_context_id="pc-1", parameter_context_name=plan.parameterContext.name,
            groups=groups, components=components,
        )

    monkeypatch.setattr(nifi_apply, "apply_plan", fake_apply_plan)

    result = await lifecycle.deploy(fake_db, flow_doc)
    assert result["state"] == "Stopped"


@async_test
async def test_deploy_redeploy_after_undeploy_excludes_own_leftover_topic(monkeypatch):
    """The reported bug, reproduced and fixed: deploy -> undeploy (per
    MVP §7.9, `undeploy()` EMPTIES owned data topics but never deletes
    them, and NULLS `runtimeScopeMap`) -> redeploy must not read its own
    leftover topic as foreign. `flow_doc` here mirrors exactly what
    `undeploy()` leaves behind (`state: "Draft"`, `deployedAt`/
    `nifiProcessGroupId`/`runtimeScopeMap` all `None`) while the fake Kafka
    listing mirrors the leftover, emptied-but-not-deleted topic still
    being live -- before the fix this blocked the redeploy forever with
    "already exists on the Kafka cluster and is not owned by this flow"."""
    fake_db = FakeDB()
    _seed_http_service(fake_db)
    _seed_core_connections(fake_db)
    flow_doc = _http_kafka_flow(
        flow_id="flow-undeploy-redeploy-1",
        state="Draft", deployedAt=None, nifiProcessGroupId=None, runtimeScopeMap=None,
    )
    fake_db.flows_v2.docs.append(flow_doc)
    _patch_provenance_probes(monkeypatch)
    _patch_ensure_topics_ok(monkeypatch, [])

    async def fake_list_topics(kafka_conn):
        # undeploy() only empties owned data topics -- never deletes them.
        return {"ok": True, "topics": ["raw.test_flow.thing"]}

    monkeypatch.setattr(topics, "list_topics", fake_list_topics)

    async def fake_apply_plan(nifi_conn, plan):
        groups = {g.blockId: f"pg-{g.blockId}" for g in plan.rootGroup.childGroups}
        components = {g.blockId: {p.key: f"nifi-{p.key}" for p in g.processors} for g in plan.rootGroup.childGroups}
        return nifi_apply.AppliedResult(
            process_group_id="pg-root", parameter_context_id="pc-1", parameter_context_name=plan.parameterContext.name,
            groups=groups, components=components,
        )

    monkeypatch.setattr(nifi_apply, "apply_plan", fake_apply_plan)

    result = await lifecycle.deploy(fake_db, flow_doc)
    assert result["state"] == "Stopped"
    assert result["runtimeScopeMap"] is not None  # redeploy re-populated it


@async_test
async def test_deploy_refuses_when_other_flows_naming_walk_derives_same_topic(monkeypatch):
    """M13 other-flows-side fix: another flow's ownership must be
    re-derived via the same deterministic naming walk, not trusted from
    its `runtimeScopeMap` alone -- an undeployed (or never-deployed) OTHER
    flow's topics would otherwise vanish from this check too, letting a
    second flow silently start publishing into (and reading DLQ off) the
    first flow's topic (the exact M13 review failure scenario: two flows
    named identically both deriving `raw.asset_sync.asset`)."""
    fake_db = FakeDB()
    _seed_http_service(fake_db)
    _seed_core_connections(fake_db)
    flow_doc = _http_kafka_flow(flow_id="flow-naming-collide-1")
    fake_db.flows_v2.docs.append(flow_doc)

    # A DIFFERENT flow -- never deployed, no runtimeScopeMap record at all
    # -- whose own blocks independently derive the identical topic name.
    # Only catchable via the deterministic naming walk, not the scope map.
    fake_db.flows_v2.docs.append({
        "id": "flow-other-naming-1", "name": "Test Flow",
        "blocks": [
            {"id": "ob2", "adapter": "kafka", "mode": "write", "name": "Write Topic", "parentId": None,
             "entity": "thing", "config": {}, "transforms": []},
        ],
        "topics": [], "runtimeScopeMap": None,
    })

    _patch_provenance_probes(monkeypatch)
    _patch_ensure_topics_ok(monkeypatch, [])

    apply_calls = []

    async def fail_if_called(*a, **k):
        apply_calls.append((a, k))
        raise AssertionError("apply_plan must not be called when a topic-reservation row fails")

    monkeypatch.setattr(nifi_apply, "apply_plan", fail_if_called)

    try:
        await lifecycle.deploy(fake_db, flow_doc)
        assert False, "expected DeployPreflightFailed"
    except lifecycle.DeployPreflightFailed as exc:
        failing = [r for r in exc.rows if not r["ok"]]
        assert any("raw.test_flow.thing" in r["detail"] and "Test Flow" in r["detail"] for r in failing)

    assert apply_calls == []


@async_test
async def test_deploy_refuses_when_topic_owned_by_different_deployed_flow_also_appears_live(monkeypatch):
    """An unrelated, currently-deployed OTHER flow's own topic (recorded
    in its `runtimeScopeMap`, not merely derivable by naming) must still
    block -- with the existing "already owned by flow ..." message, not
    the generic live-cluster message -- even when the live Kafka listing
    independently confirms the same topic exists. The M13 fix's naming-
    walk exclusion is scoped to the DEPLOYING flow's own derivation only;
    it must never widen to swallow a genuine cross-flow collision."""
    fake_db = FakeDB()
    _seed_http_service(fake_db)
    _seed_core_connections(fake_db)
    flow_doc = _http_kafka_flow(flow_id="flow-collide-live-2")
    fake_db.flows_v2.docs.append(flow_doc)

    # Another flow, already deployed, that genuinely owns the exact topic
    # this one would derive ("raw.test_flow.thing" -- both named "Test Flow").
    fake_db.flows_v2.docs.append({
        "id": "flow-other-owner-2", "name": "Some Other Flow",
        "runtimeScopeMap": {"b2": {"topics": ["raw.test_flow.thing"], "connectorNames": []}},
    })

    _patch_provenance_probes(monkeypatch)
    _patch_ensure_topics_ok(monkeypatch, [])

    async def fake_list_topics(kafka_conn):
        return {"ok": True, "topics": ["raw.test_flow.thing", "some.unrelated.topic"]}

    monkeypatch.setattr(topics, "list_topics", fake_list_topics)

    apply_calls = []

    async def fail_if_called(*a, **k):
        apply_calls.append((a, k))
        raise AssertionError("apply_plan must not be called when a topic-reservation row fails")

    monkeypatch.setattr(nifi_apply, "apply_plan", fail_if_called)

    try:
        await lifecycle.deploy(fake_db, flow_doc)
        assert False, "expected DeployPreflightFailed"
    except lifecycle.DeployPreflightFailed as exc:
        failing = [r for r in exc.rows if not r["ok"]]
        assert any(
            "raw.test_flow.thing" in r["detail"] and "Some Other Flow" in r["detail"] and "already owned by flow" in r["detail"]
            for r in failing
        )

    assert apply_calls == []


@async_test
async def test_deploy_redeploy_does_not_flag_its_own_prior_live_topic(monkeypatch):
    """A REDEPLOY of the SAME flow re-derives the identical topic names
    (name-frozen, M12) -- those already existing live (from the flow's own
    prior deploy) must never be flagged as a collision against itself."""
    fake_db = FakeDB()
    _seed_http_service(fake_db)
    _seed_core_connections(fake_db)
    flow_doc = _http_kafka_flow(
        flow_id="flow-redeploy-1", state="Stopped", deployedAt="2026-08-01T00:00:00.000Z", nifiProcessGroupId="pg-old",
        runtimeScopeMap={
            "b1": {"adapter": "http", "engine": "nifi", "groupName": "fetch__http", "processGroupId": "pg-b1",
                   "components": {}, "connectorNames": [], "topics": []},
            "b2": {"adapter": "kafka", "engine": "nifi", "groupName": "write_topic__kafka", "processGroupId": "pg-b2",
                   "components": {}, "connectorNames": [], "topics": ["raw.test_flow.thing"]},
        },
    )
    fake_db.flows_v2.docs.append(flow_doc)
    _patch_provenance_probes(monkeypatch)
    _patch_ensure_topics_ok(monkeypatch, [])

    async def fake_list_topics(kafka_conn):
        return {"ok": True, "topics": ["raw.test_flow.thing"]}  # this flow's own, already live

    monkeypatch.setattr(topics, "list_topics", fake_list_topics)

    async def fake_delete_flow_pg(nifi_conn, pg_id):
        return {"ok": True}

    monkeypatch.setattr(nifi_apply, "delete_flow_pg", fake_delete_flow_pg)

    async def fake_apply_plan(nifi_conn, plan):
        groups = {g.blockId: f"pg-{g.blockId}" for g in plan.rootGroup.childGroups}
        components = {g.blockId: {p.key: f"nifi-{p.key}" for p in g.processors} for g in plan.rootGroup.childGroups}
        return nifi_apply.AppliedResult(
            process_group_id="pg-root", parameter_context_id="pc-1", parameter_context_name=plan.parameterContext.name,
            groups=groups, components=components,
        )

    monkeypatch.setattr(nifi_apply, "apply_plan", fake_apply_plan)

    result = await lifecycle.deploy(fake_db, flow_doc)
    assert result["state"] == "Stopped"
