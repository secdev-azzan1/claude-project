"""Tests for the v2 Platform Connections subsystem
(backend/routers/v2/connections.py + backend/services/adapter/seed.py).

The task brief forbids wiring routers/v2/connections.py into server.py (a
future orchestrator does that), so -- unlike tests/test_v2_openapi.py, which
exercises the real `server.app` because openapi.py IS already mounted there
-- these tests build a minimal standalone FastAPI app that mounts only the
connections-v2 router, with the Mongo dependency (`db.get_db`) overridden by
an in-memory FakeDB built on the same FaultInjectingCollection helper the
resilience test suite and tests/test_v2_openapi.py use. Nothing here touches
`server.app` or a real Mongo instance.

Every live-infra client call (nifi_client, kafka_client, apicurio_client,
kafka_connect_client, apisix_client) is monkeypatched per-test -- nothing
here ever touches the network either.
"""
import asyncio
import sys
from functools import wraps
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from fastapi import FastAPI
from fastapi.testclient import TestClient
from bson import ObjectId

from db import get_db
from routers.v2 import connections as connections_v2
from services.adapter import seed as seed_module
from services import nifi_service_readiness
from tests.resilience.conftest import FaultInjectingCollection


def async_test(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


class FakeDB:
    """`db[COLLECTIONS.xxx]` (bracket access) is the convention used
    throughout routers/v2/connections.py and services/adapter/common.py's
    `audit()` -- so `__getitem__` is what needs to resolve here, mirroring
    each v2 collection name to its own FaultInjectingCollection."""

    def __init__(self):
        self.connections_v2 = FaultInjectingCollection()
        self.flows_v2 = FaultInjectingCollection()
        self.services_v2 = FaultInjectingCollection()
        self.schemas_v2 = FaultInjectingCollection()
        self.audit_v2 = FaultInjectingCollection()
        self.nifi_global_services = FaultInjectingCollection()

    def __getitem__(self, name):
        return getattr(self, name)


class InsertMutatingCollection(FaultInjectingCollection):
    """Model PyMongo's insert_one behavior of attaching an ObjectId to the
    mapping passed by the caller."""

    async def insert_one(self, document):
        result = await super().insert_one(document)
        document["_id"] = ObjectId()
        return result


class MongoLikeDB(FakeDB):
    def __init__(self):
        super().__init__()
        self.connections_v2 = InsertMutatingCollection()


def _make_client(fake_db: FakeDB) -> TestClient:
    test_app = FastAPI()
    test_app.include_router(connections_v2.router)
    test_app.dependency_overrides[get_db] = lambda: fake_db
    return TestClient(test_app)


def _clear_overrides():
    """Each test builds its own throwaway FastAPI app instance (see
    `_make_client`), so there is no shared global app state to reset --
    kept as a no-op so every test's existing `finally: _clear_overrides()`
    scaffolding stays valid."""
    return None


def _deployed_flow(flow_id="flow-1", name="Critical Flow", blocks=None):
    return {
        "id": flow_id,
        "name": name,
        "state": "Running",
        "enabled": True,
        "cron": None,
        "blocks": blocks or [],
        "topics": [],
        "variables": [],
        "servicePins": {},
        "deployedAt": "2026-08-01T00:00:00.000Z",
        "lastRunAt": None,
        "createdAt": "2026-08-01T00:00:00.000Z",
        "updatedAt": "2026-08-01T00:00:00.000Z",
    }


# ------------------------------------------------------------------ CRUD / redaction


def test_create_first_of_type_auto_activates_and_redacts_secret():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        resp = client.post(
            "/api/v2/connections/",
            json={
                "type": "nifi",
                "name": "Primary NiFi",
                "config": {"url": "https://nifi.example.com", "authMode": "bearer", "token": "super-secret-token"},
            },
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["active"] is True
        assert body["health"] == "Not Tested"
        assert body["hasSecret"] is True
        assert body["hasToken"] is True
        assert body["config"]["token"] is None
        assert "super-secret-token" not in resp.text
        conn_id = body["id"]

        # Internal storage really has the secret (needed to actually call NiFi).
        stored = next(d for d in fake_db.connections_v2.docs if d["id"] == conn_id)
        assert stored["config"]["token"] == "super-secret-token"

        # A second nifi connection is NOT auto-active.
        resp2 = client.post(
            "/api/v2/connections/",
            json={
                "type": "nifi",
                "name": "Backup NiFi",
                "config": {"url": "https://nifi2.example.com", "authMode": "bearer", "token": "tok2"},
            },
        )
        assert resp2.status_code == 201
        assert resp2.json()["active"] is False
    finally:
        _clear_overrides()


def test_create_response_survives_mongo_insert_id_mutation():
    fake_db = MongoLikeDB()
    client = _make_client(fake_db)
    try:
        resp = client.post(
            "/api/v2/connections/",
            json={
                "type": "nifi",
                "name": "Mongo-like NiFi",
                "config": {"url": "https://nifi.example.com", "authMode": "bearer", "token": "token"},
            },
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["name"] == "Mongo-like NiFi"
        assert "_id" not in body
    finally:
        _clear_overrides()


@pytest.mark.parametrize(
    ("connection_type", "config"),
    [
        ("nifi", {"url": "https://nifi.example.com", "authMode": "bearer", "token": "token"}),
        ("kafka", {"bootstrapServers": "kafka.example.com:9092", "mode": "native", "securityProtocol": "PLAINTEXT"}),
        ("apicurio", {"url": "https://registry.example.com", "authMode": "none"}),
        ("kafka_connect", {"url": "https://connect.example.com"}),
        ("redis", {"host": "redis.example.com", "port": 6379, "dedupDb": 0, "bookmarksDb": 1}),
        ("apisix", {"adminUrl": "https://apisix-admin.example.com", "runtimeUrl": "https://apisix.example.com"}),
    ],
)
def test_all_platform_connection_types_return_clean_create_responses(connection_type, config):
    fake_db = MongoLikeDB()
    client = _make_client(fake_db)
    try:
        resp = client.post(
            "/api/v2/connections/",
            json={"type": connection_type, "name": f"Mongo-like {connection_type}", "config": config},
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()
        assert body["type"] == connection_type
        assert "_id" not in body
    finally:
        _clear_overrides()


def test_update_with_blank_secret_keeps_existing_value():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        created = client.post(
            "/api/v2/connections/",
            json={
                "type": "apisix",
                "name": "Gateway",
                "config": {"adminUrl": "https://admin.example.com", "runtimeUrl": "https://runtime.example.com", "adminKey": "abc123"},
            },
        ).json()
        conn_id = created["id"]
        assert created["hasSecret"] is True

        # Update name only; blank adminKey keeps the prior secret (write-only semantics).
        updated_resp = client.post(
            "/api/v2/connections/",
            json={
                "id": conn_id,
                "type": "apisix",
                "name": "Gateway Renamed",
                "config": {"adminUrl": "https://admin.example.com", "runtimeUrl": "https://runtime.example.com", "adminKey": ""},
            },
        )
        assert updated_resp.status_code == 200, updated_resp.text
        updated = updated_resp.json()
        assert updated["name"] == "Gateway Renamed"
        assert updated["hasSecret"] is True
        assert updated["config"]["adminKey"] is None

        stored = next(d for d in fake_db.connections_v2.docs if d["id"] == conn_id)
        assert stored["config"]["adminKey"] == "abc123"

        # Supplying a new secret replaces it.
        replace_resp = client.post(
            "/api/v2/connections/",
            json={
                "id": conn_id,
                "type": "apisix",
                "name": "Gateway Renamed",
                "config": {"adminUrl": "https://admin.example.com", "runtimeUrl": "https://runtime.example.com", "adminKey": "new-key"},
            },
        )
        assert replace_resp.status_code == 200
        stored2 = next(d for d in fake_db.connections_v2.docs if d["id"] == conn_id)
        assert stored2["config"]["adminKey"] == "new-key"
    finally:
        _clear_overrides()


def test_create_rejects_missing_required_fields():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        resp = client.post("/api/v2/connections/", json={"type": "kafka_connect", "name": "KC", "config": {}})
        assert resp.status_code == 400
        assert "URL is required" in resp.json()["detail"]

        resp2 = client.post(
            "/api/v2/connections/",
            json={"type": "redis", "name": "R", "config": {"host": "r", "port": 6379, "dedupDb": 0, "bookmarksDb": 0}},
        )
        assert resp2.status_code == 400
        assert "different logical databases" in resp2.json()["detail"]
    finally:
        _clear_overrides()


def test_list_returns_redacted_connections():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        client.post(
            "/api/v2/connections/",
            json={
                "type": "redis",
                "name": "Redis",
                "config": {"host": "redis.internal", "port": 6379, "dedupDb": 0, "bookmarksDb": 1, "password": "hunter2"},
            },
        )
        resp = client.get("/api/v2/connections/")
        assert resp.status_code == 200
        items = resp.json()
        assert len(items) == 1
        assert items[0]["hasPassword"] is True
        assert items[0]["config"]["password"] is None
        assert "hunter2" not in resp.text
    finally:
        _clear_overrides()


# ------------------------------------------------------------------- dependents


def test_activate_blocked_when_active_peer_has_dependents():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        a = client.post(
            "/api/v2/connections/", json={"type": "nifi", "name": "A", "config": {"url": "https://a", "authMode": "bearer", "token": "t"}}
        ).json()
        b = client.post(
            "/api/v2/connections/", json={"type": "nifi", "name": "B", "config": {"url": "https://b", "authMode": "bearer", "token": "t2"}}
        ).json()
        assert a["active"] is True
        assert b["active"] is False

        fake_db.flows_v2.docs.append(_deployed_flow())

        resp = client.post(f"/api/v2/connections/{b['id']}/activate")
        assert resp.status_code == 409
        assert "dependent flow" in resp.json()["detail"]

        stored_a = next(d for d in fake_db.connections_v2.docs if d["id"] == a["id"])
        assert stored_a["active"] is True
    finally:
        _clear_overrides()


def test_delete_blocked_when_active_connection_has_dependents():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        a = client.post(
            "/api/v2/connections/", json={"type": "nifi", "name": "A", "config": {"url": "https://a", "authMode": "bearer", "token": "t"}}
        ).json()
        fake_db.flows_v2.docs.append(_deployed_flow())

        resp = client.delete(f"/api/v2/connections/{a['id']}")
        assert resp.status_code == 409
        assert "deployed flow" in resp.json()["detail"]
        assert any(d["id"] == a["id"] for d in fake_db.connections_v2.docs)
    finally:
        _clear_overrides()


def test_delete_allowed_when_inactive_even_with_deployed_flows():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        client.post(
            "/api/v2/connections/", json={"type": "nifi", "name": "A", "config": {"url": "https://a", "authMode": "bearer", "token": "t"}}
        ).json()
        b = client.post(
            "/api/v2/connections/", json={"type": "nifi", "name": "B", "config": {"url": "https://b", "authMode": "bearer", "token": "t2"}}
        ).json()
        # Depends on active A; B is inactive and thus freely deletable.
        fake_db.flows_v2.docs.append(_deployed_flow())

        resp = client.delete(f"/api/v2/connections/{b['id']}")
        assert resp.status_code == 204
        assert not any(d["id"] == b["id"] for d in fake_db.connections_v2.docs)
    finally:
        _clear_overrides()


def test_kafka_connect_dependents_requires_kc_block():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        conn = client.post("/api/v2/connections/", json={"type": "kafka_connect", "name": "KC", "config": {"url": "https://kc"}}).json()

        fake_db.flows_v2.docs.append(_deployed_flow(flow_id="f1", name="No KC", blocks=[{"id": "b1", "adapter": "http", "config": {}}]))
        resp = client.get(f"/api/v2/connections/{conn['id']}/impact")
        assert resp.json()["dependentFlowCount"] == 0

        fake_db.flows_v2.docs.append(
            _deployed_flow(flow_id="f2", name="Has KC", blocks=[{"id": "b2", "adapter": "kafka_kc", "config": {}}])
        )
        resp2 = client.get(f"/api/v2/connections/{conn['id']}/impact")
        body = resp2.json()
        assert body["dependentFlowCount"] == 1
        assert body["dependentFlows"] == ["Has KC"]
    finally:
        _clear_overrides()


def test_redis_dependents_from_dedup_or_incremental_jdbc():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        conn = client.post(
            "/api/v2/connections/", json={"type": "redis", "name": "Redis", "config": {"host": "r", "port": 6379, "dedupDb": 0, "bookmarksDb": 1}}
        ).json()

        fake_db.flows_v2.docs.append(
            _deployed_flow(
                flow_id="f1",
                name="Dedup Flow",
                blocks=[{"id": "b1", "adapter": "kafka", "mode": "write", "config": {}, "transforms": [{"id": "t1", "kind": "dedup", "config": {}}]}],
            )
        )
        fake_db.flows_v2.docs.append(
            _deployed_flow(
                flow_id="f2",
                name="Incremental JDBC Flow",
                blocks=[{"id": "b2", "adapter": "jdbc", "mode": "read", "config": {"incremental": True}}],
            )
        )
        fake_db.flows_v2.docs.append(
            _deployed_flow(flow_id="f3", name="Unrelated Flow", blocks=[{"id": "b3", "adapter": "http", "config": {}}])
        )

        resp = client.get(f"/api/v2/connections/{conn['id']}/impact")
        body = resp.json()
        assert body["dependentFlowCount"] == 2
        assert set(body["dependentFlows"]) == {"Dedup Flow", "Incremental JDBC Flow"}
    finally:
        _clear_overrides()


def test_apisix_dependents_from_http_block_proxy_reference():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        conn = client.post(
            "/api/v2/connections/", json={"type": "apisix", "name": "GW", "config": {"adminUrl": "https://a", "runtimeUrl": "https://r"}}
        ).json()

        fake_db.flows_v2.docs.append(
            _deployed_flow(flow_id="f1", name="Proxied Flow", blocks=[{"id": "b1", "adapter": "http", "config": {"proxyId": "proxy-1"}}])
        )
        fake_db.flows_v2.docs.append(
            _deployed_flow(flow_id="f2", name="No Proxy Flow", blocks=[{"id": "b2", "adapter": "http", "config": {}}])
        )

        resp = client.get(f"/api/v2/connections/{conn['id']}/impact")
        body = resp.json()
        assert body["dependentFlowCount"] == 1
        assert body["dependentFlows"] == ["Proxied Flow"]
    finally:
        _clear_overrides()


# --------------------------------------------------------------- test dispatch


def test_dispatch_nifi(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        async def fake_test(*, url, auth_type, username, password, token):
            assert url == "https://nifi.example.com"
            assert auth_type == "BEARER"
            assert token == "tok"
            return {"ok": True, "message": "NiFi connected successfully. Version: 2.x", "reachable": True}

        monkeypatch.setattr(connections_v2.nifi_client, "test_nifi_connection", fake_test)

        conn = client.post(
            "/api/v2/connections/", json={"type": "nifi", "name": "N", "config": {"url": "https://nifi.example.com", "authMode": "bearer", "token": "tok"}}
        ).json()
        resp = client.post(f"/api/v2/connections/{conn['id']}/test")
        assert resp.status_code == 200
        body = resp.json()
        assert body["health"] == "Healthy"
        assert body["reachability"] == "Reachable"
        assert "NiFi connected" in body["message"]

        stored = next(d for d in fake_db.connections_v2.docs if d["id"] == conn["id"])
        assert stored["health"] == "Healthy"
        assert stored["lastTestedAt"]
    finally:
        _clear_overrides()


# ------------------------------------------------------- NiFi service readiness


def _platform_connection_docs():
    return [
        {"id": "nifi-1", "type": "nifi", "name": "Primary NiFi", "active": True,
         "config": {"url": "https://nifi.example.com", "authMode": "bearer", "token": "nifi-token"}},
        {"id": "kafka-1", "type": "kafka", "name": "Kafka", "active": True,
         "config": {"bootstrapServers": "kafka.internal:9092", "securityProtocol": "PLAINTEXT"}},
        {"id": "registry-1", "type": "apicurio", "name": "Apicurio", "active": True,
         "config": {"url": "https://registry.example.com", "authMode": "none"}},
        {"id": "redis-1", "type": "redis", "name": "Redis", "active": True,
         "config": {"host": "redis.internal", "port": 6379, "dedupDb": 0}},
    ]


def test_nifi_readiness_creates_only_platform_services_and_is_idempotent(monkeypatch):
    fake_db = FakeDB()
    fake_db.connections_v2.docs.extend(_platform_connection_docs())
    client = _make_client(fake_db)
    created_ids = {}
    update_calls = []

    async def fake_root(url, **auth):
        return "root-1"

    async def fake_list(endpoint, root_id, auth):
        return [
            {"component": {"id": service_id, "name": name, "type": spec["type"], "parentGroupId": root_id}}
            for spec in nifi_service_readiness.PLATFORM_SERVICE_SPECS
            for name, service_id in [(spec["name"], created_ids.get(spec["kind"], f"missing-{spec['kind']}"))]
            if spec["kind"] in created_ids
        ]

    async def fake_create(endpoint, root_id, auth, spec):
        service_id = f"cs-{spec['kind']}"
        created_ids[spec["kind"]] = service_id
        return service_id

    async def fake_config(endpoint, service_id, **auth):
        kind = service_id.removeprefix("cs-")
        spec = next(s for s in nifi_service_readiness.PLATFORM_SERVICE_SPECS if s["kind"] == kind)
        dep = next(d for d in _platform_connection_docs() if d["type"] == spec["dependency"])
        return {"ok": True, "id": service_id, "name": spec["name"], "type": spec["type"], "state": "ENABLED",
                "properties": nifi_service_readiness._desired_properties(kind, dep), "validation_errors": [], "revision": 1}

    async def fake_update(endpoint, service_id, properties, **auth):
        update_calls.append(service_id)
        return {"ok": True, "state": "ENABLED"}

    monkeypatch.setattr(nifi_service_readiness, "get_nifi_root_process_group_id", fake_root)
    monkeypatch.setattr(nifi_service_readiness, "_list_root_services", fake_list)
    monkeypatch.setattr(nifi_service_readiness, "_create_service", fake_create)
    monkeypatch.setattr(nifi_service_readiness, "get_controller_service_config", fake_config)
    monkeypatch.setattr(nifi_service_readiness, "update_controller_service_config", fake_update)

    try:
        first = client.post("/api/v2/connections/nifi-1/nifi-services/readiness")
        assert first.status_code == 200, first.text
        first_body = first.json()
        assert first_body["ok"] is True
        assert first_body["summary"]["created"] == 3
        assert first_body["summary"]["failed"] == 0
        assert first_body["flowScopedServicesUntouched"] is True
        assert len(fake_db.nifi_global_services.docs) == 3

        update_calls.clear()
        second = client.post("/api/v2/connections/nifi-1/nifi-services/readiness")
        assert second.status_code == 200, second.text
        second_body = second.json()
        assert second_body["summary"]["healthy"] == 3
        assert second_body["summary"]["created"] == 0
        assert update_calls == []
    finally:
        _clear_overrides()


def test_nifi_readiness_repairs_drift_and_reports_missing_dependency(monkeypatch):
    fake_db = FakeDB()
    docs = _platform_connection_docs()
    fake_db.connections_v2.docs.extend(docs[:3])  # Redis is intentionally not configured.
    client = _make_client(fake_db)
    update_calls = []

    async def fake_root(url, **auth):
        return "root-1"

    async def fake_list(endpoint, root_id, auth):
        kafka = next(s for s in nifi_service_readiness.PLATFORM_SERVICE_SPECS if s["kind"] == "kafka")
        registry = next(s for s in nifi_service_readiness.PLATFORM_SERVICE_SPECS if s["kind"] == "schema_registry")
        return [
            {"component": {"id": "cs-kafka", "name": kafka["name"], "type": kafka["type"], "parentGroupId": root_id}},
            {"component": {"id": "cs-registry", "name": registry["name"], "type": registry["type"], "parentGroupId": root_id}},
        ]

    async def fake_config(endpoint, service_id, **auth):
        spec = next(s for s in nifi_service_readiness.PLATFORM_SERVICE_SPECS if service_id.endswith(s["kind"]) or (service_id == "cs-kafka" and s["kind"] == "kafka") or (service_id == "cs-registry" and s["kind"] == "schema_registry"))
        return {"ok": True, "id": service_id, "state": "DISABLED", "properties": {"bootstrap.servers": "old:9092"}, "validation_errors": ["old target"], "revision": 3, "type": spec["type"]}

    async def fake_update(endpoint, service_id, properties, **auth):
        update_calls.append((service_id, properties))
        return {"ok": True, "state": "ENABLED"}

    async def fake_create(endpoint, root_id, auth, spec):
        return "cs-redis"

    async def fake_wait(endpoint, service_id, auth, timeout=12.0):
        spec_kind = "redis" if service_id == "cs-redis" else ("kafka" if service_id == "cs-kafka" else "schema_registry")
        spec = next(s for s in nifi_service_readiness.PLATFORM_SERVICE_SPECS if s["kind"] == spec_kind)
        return {"ok": True, "state": "ENABLED", "validation_errors": [], "properties": {}, "type": spec["type"]}

    async def fake_set_state(endpoint, service_id, state, auth):
        assert state == "ENABLED"
        return {"ok": True}

    monkeypatch.setattr(nifi_service_readiness, "get_nifi_root_process_group_id", fake_root)
    monkeypatch.setattr(nifi_service_readiness, "_list_root_services", fake_list)
    monkeypatch.setattr(nifi_service_readiness, "_create_service", fake_create)
    monkeypatch.setattr(nifi_service_readiness, "get_controller_service_config", fake_config)
    monkeypatch.setattr(nifi_service_readiness, "update_controller_service_config", fake_update)
    monkeypatch.setattr(nifi_service_readiness, "_wait_enabled", fake_wait)
    monkeypatch.setattr(nifi_service_readiness, "_set_state", fake_set_state)

    try:
        resp = client.post("/api/v2/connections/nifi-1/nifi-services/readiness")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        by_kind = {item["kind"]: item for item in body["services"]}
        assert by_kind["kafka"]["status"] == "repaired"
        assert by_kind["schema_registry"]["status"] == "repaired"
        assert by_kind["redis"]["status"] == "blocked"
        assert body["ok"] is False
        assert len(update_calls) == 2
    finally:
        _clear_overrides()


def test_nifi_readiness_rejects_non_nifi_connection():
    fake_db = FakeDB()
    fake_db.connections_v2.docs.append({"id": "kafka-1", "type": "kafka", "name": "Kafka", "active": True, "config": {}})
    client = _make_client(fake_db)
    try:
        resp = client.post("/api/v2/connections/kafka-1/nifi-services/readiness")
        assert resp.status_code == 400
        assert "Apache NiFi" in resp.json()["detail"]
    finally:
        _clear_overrides()


def test_dispatch_kafka_kafbat_mode(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        async def fake_kafbat(*, kafbat_url, **kwargs):
            assert kafbat_url == "https://kafbat.example.com"
            return {"ok": True, "message": "Kafka cluster online. 3 broker(s)."}

        monkeypatch.setattr(connections_v2.kafka_client, "test_kafbat_connection", fake_kafbat)

        conn = client.post(
            "/api/v2/connections/",
            json={
                "type": "kafka",
                "name": "K",
                "config": {"bootstrapServers": "kafka:9092", "mode": "kafbat", "proxyUrl": "https://kafbat.example.com", "securityProtocol": "PLAINTEXT"},
            },
        ).json()
        resp = client.post(f"/api/v2/connections/{conn['id']}/test")
        assert resp.json()["health"] == "Healthy"
    finally:
        _clear_overrides()


def test_dispatch_kafka_native_mode_can_legitimately_fail(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        async def fake_native(*, bootstrap_servers, security_protocol, sasl_username, sasl_password):
            return {"ok": False, "error": "Cannot reach Kafka broker from the application backend network."}

        monkeypatch.setattr(connections_v2.kafka_client, "test_kafka_connection", fake_native)

        conn = client.post(
            "/api/v2/connections/",
            json={"type": "kafka", "name": "K2", "config": {"bootstrapServers": "kafka.internal:9092", "mode": "native", "securityProtocol": "PLAINTEXT"}},
        ).json()
        resp = client.post(f"/api/v2/connections/{conn['id']}/test")
        body = resp.json()
        assert body["health"] == "Failed"
        assert body["reachability"] == "Unreachable"
    finally:
        _clear_overrides()


def test_dispatch_apicurio(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        async def fake_apicurio(*, url, auth_type, username, password, token):
            assert auth_type == "NONE"
            return {"ok": True, "message": "Apicurio Registry reachable."}

        monkeypatch.setattr(connections_v2.apicurio_client, "test_apicurio_connection", fake_apicurio)

        conn = client.post("/api/v2/connections/", json={"type": "apicurio", "name": "AP", "config": {"url": "https://apicurio", "authMode": "none"}}).json()
        resp = client.post(f"/api/v2/connections/{conn['id']}/test")
        assert resp.json()["health"] == "Healthy"
    finally:
        _clear_overrides()


def test_dispatch_kafka_connect(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        async def fake_kc(*, url):
            assert url == "https://kc"
            return {"ok": True, "message": "Kafka Connect cluster reachable."}

        monkeypatch.setattr(connections_v2.kafka_connect_client, "test_kafka_connect_connection", fake_kc)

        conn = client.post("/api/v2/connections/", json={"type": "kafka_connect", "name": "KC", "config": {"url": "https://kc"}}).json()
        resp = client.post(f"/api/v2/connections/{conn['id']}/test")
        assert resp.json()["health"] == "Healthy"
    finally:
        _clear_overrides()


def test_dispatch_apisix(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        async def fake_apisix(*, admin_url, admin_key):
            assert admin_key == "key123"
            return {"ok": True, "message": "APISIX Admin API reachable."}

        monkeypatch.setattr(connections_v2.apisix_client, "test_admin", fake_apisix)

        conn = client.post(
            "/api/v2/connections/", json={"type": "apisix", "name": "GW", "config": {"adminUrl": "https://a", "runtimeUrl": "https://r", "adminKey": "key123"}}
        ).json()
        resp = client.post(f"/api/v2/connections/{conn['id']}/test")
        assert resp.json()["health"] == "Healthy"
    finally:
        _clear_overrides()


def test_dispatch_redis_indirect_verification():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        redis_conn = client.post(
            "/api/v2/connections/", json={"type": "redis", "name": "R", "config": {"host": "r", "port": 6379, "dedupDb": 0, "bookmarksDb": 1}}
        ).json()

        # No active, healthy NiFi yet -> Not Tested.
        resp1 = client.post(f"/api/v2/connections/{redis_conn['id']}/test")
        body1 = resp1.json()
        assert body1["health"] == "Not Tested"
        assert "cluster-internal" in body1["message"]

        # Seed an active, healthy NiFi directly -> Healthy (assumed reachable).
        fake_db.connections_v2.docs.append(
            {
                "id": "conn-nifi-1",
                "type": "nifi",
                "name": "N",
                "active": True,
                "health": "Healthy",
                "reachability": "Reachable",
                "lastTestedAt": "x",
                "config": {"url": "https://n"},
                "hasSecret": True,
            }
        )
        resp2 = client.post(f"/api/v2/connections/{redis_conn['id']}/test")
        body2 = resp2.json()
        assert body2["health"] == "Healthy"
        assert "Assumed reachable from NiFi" in body2["message"]
    finally:
        _clear_overrides()


# ------------------------------------------------------------------------ repoint


def test_repoint_adopt_uses_identity_safe_service(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        a = client.post(
            "/api/v2/connections/", json={"type": "nifi", "name": "A", "config": {"url": "https://a", "authMode": "bearer", "token": "t"}}
        ).json()
        b = client.post(
            "/api/v2/connections/", json={"type": "nifi", "name": "B", "config": {"url": "https://b", "authMode": "bearer", "token": "t2"}}
        ).json()

        async def fake_adopt(db, target):
            await db.connections_v2.update_one({"id": a["id"]}, {"$set": {"active": False}})
            await db.connections_v2.update_one({"id": target["id"]}, {"$set": {"active": True}})
            return {"mode": "adopt", "flowCount": 0}

        monkeypatch.setattr(connections_v2.nifi_repoint, "adopt", fake_adopt)
        resp = client.post(f"/api/v2/connections/{b['id']}/repoint", json={"mode": "adopt"})
        assert resp.status_code == 200
        assert resp.json()["connection"]["active"] is True
        assert resp.json()["result"]["mode"] == "adopt"

        stored_a = next(d for d in fake_db.connections_v2.docs if d["id"] == a["id"])
        assert stored_a["active"] is False
    finally:
        _clear_overrides()


def test_repoint_migrate_dispatches_and_reset_is_not_an_api_mode(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        client.post(
            "/api/v2/connections/", json={"type": "nifi", "name": "A", "config": {"url": "https://a", "authMode": "bearer", "token": "t"}}
        ).json()
        b = client.post(
            "/api/v2/connections/", json={"type": "nifi", "name": "B", "config": {"url": "https://b", "authMode": "bearer", "token": "t2"}}
        ).json()

        async def fake_migrate(_db, target):
            return {"mode": "migrate", "targetConnectionId": target["id"], "flowCount": 2}

        monkeypatch.setattr(connections_v2.nifi_repoint, "migrate", fake_migrate)
        migrated = client.post(f"/api/v2/connections/{b['id']}/repoint", json={"mode": "migrate"})
        assert migrated.status_code == 200
        assert migrated.json()["result"]["flowCount"] == 2

        reset = client.post(f"/api/v2/connections/{b['id']}/repoint", json={"mode": "reset"})
        assert reset.status_code == 422
    finally:
        _clear_overrides()


# --------------------------------------------------------------------------- seed


def test_seed_v2_connections_creates_from_env(monkeypatch):
    fake_db = FakeDB()

    monkeypatch.setenv("NIFI_URL", "https://nifi.example.com")
    monkeypatch.setenv("NIFI_USERNAME", "admin")
    monkeypatch.setenv("NIFI_PASSWORD", "secret")
    monkeypatch.setenv("KAFKA_IN_CLUSTER_BOOTSTRAP", "kafka:9092")
    monkeypatch.setenv("KAFBAT_URL", "https://kafbat.example.com")
    monkeypatch.setenv("APICURIO_URL", "https://apicurio.example.com")
    monkeypatch.setenv("KAFKA_CONNECT_URL", "https://connect.example.com")
    monkeypatch.setenv("REDIS_CONNECTION_STRING", "redis.internal:6379")
    monkeypatch.setenv("REDIS_PASSWORD", "redispass")
    monkeypatch.setenv("REDIS_DEDUP_DB", "0")
    monkeypatch.setenv("REDIS_BOOKMARKS_DB", "1")
    monkeypatch.setenv("APISIX_ADMIN_URL", "https://apisix-admin.example.com")
    monkeypatch.setenv("APISIX_RUNTIME_URL", "https://apisix.example.com")
    monkeypatch.setenv("APISIX_ADMIN_KEY", "adminkey")

    async def ok(*args, **kwargs):
        return {"ok": True, "message": "ok", "reachable": True}

    monkeypatch.setattr(connections_v2.nifi_client, "test_nifi_connection", ok)
    monkeypatch.setattr(connections_v2.kafka_client, "test_kafbat_connection", ok)
    monkeypatch.setattr(connections_v2.apicurio_client, "test_apicurio_connection", ok)
    monkeypatch.setattr(connections_v2.kafka_connect_client, "test_kafka_connect_connection", ok)
    monkeypatch.setattr(connections_v2.apisix_client, "test_admin", ok)

    @async_test
    async def run():
        created = await seed_module.seed_v2_connections(fake_db)
        assert len(created) == 6
        assert {c["type"] for c in created} == {"nifi", "kafka", "apicurio", "kafka_connect", "redis", "apisix"}
        assert all(c["active"] for c in created)

        stored = {d["type"]: d for d in fake_db.connections_v2.docs}
        assert stored["nifi"]["health"] == "Healthy"
        assert stored["nifi"]["config"]["password"] == "secret"
        assert stored["kafka"]["config"]["mode"] == "kafbat"
        assert stored["kafka"]["config"]["proxyUrl"] == "https://kafbat.example.com"
        assert stored["kafka"]["health"] == "Healthy"
        # Redis's indirect check runs after nifi in the fixed test order, so it
        # sees the freshly-tested healthy nifi and comes back Healthy too.
        assert stored["redis"]["health"] == "Healthy"
        assert stored["redis"]["config"]["host"] == "redis.internal"
        assert stored["redis"]["config"]["port"] == 6379
        assert stored["redis"]["config"]["password"] == "redispass"
        assert stored["apisix"]["config"]["adminKey"] == "adminkey"

        # Seeding again on a non-empty collection is a no-op.
        created_again = await seed_module.seed_v2_connections(fake_db)
        assert created_again == []

    run()


def test_seed_v2_connections_skips_types_without_env(monkeypatch):
    fake_db = FakeDB()

    for var in (
        "NIFI_URL", "NIFI_USERNAME", "NIFI_PASSWORD",
        "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_IN_CLUSTER_BOOTSTRAP", "KAFBAT_URL",
        "KAFKA_CONNECT_URL", "REDIS_CONNECTION_STRING",
        "APISIX_ADMIN_URL", "APISIX_RUNTIME_URL",
    ):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("APICURIO_URL", "https://apicurio.example.com")

    async def ok(*args, **kwargs):
        return {"ok": True, "message": "ok"}

    monkeypatch.setattr(connections_v2.apicurio_client, "test_apicurio_connection", ok)

    @async_test
    async def run():
        created = await seed_module.seed_v2_connections(fake_db)
        assert len(created) == 1
        assert created[0]["type"] == "apicurio"
        assert created[0]["active"] is True

    run()
