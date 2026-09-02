"""Offline API coverage for user-managed Kafka Connect sync definitions."""
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from fastapi import FastAPI
from fastapi.testclient import TestClient

from db import get_db
from routers import kafka_connect
from tests.resilience.conftest import FaultInjectingCollection


class FakeDB:
    def __init__(self):
        self.kafka_connect_syncs_v2 = FaultInjectingCollection(unique_fields=("id", "connector_name"))
        self.flows_v2 = FaultInjectingCollection()
        self.services_v2 = FaultInjectingCollection()
        self.bulk_jobs_v2 = FaultInjectingCollection()
        self.audit_v2 = FaultInjectingCollection()
        self.connections_v2 = FaultInjectingCollection()
        self.connections = FaultInjectingCollection()

    def __getitem__(self, name):
        return getattr(self, name)


def client_for(db):
    app = FastAPI()
    app.include_router(kafka_connect.router)
    app.dependency_overrides[get_db] = lambda: db
    return TestClient(app)


def test_sync_crud_redacts_secrets_and_links_to_flow_block():
    db = FakeDB()
    db.flows_v2.docs.append(
        {
            "id": "flow-1",
            "name": "Orders",
            "blocks": [{
                "id": "block-1",
                "adapter": "kc",
                "entity": "orders",
                "serviceId": "sink-1",
                "config": {"attachTopicId": "topic-1"},
            }],
            "topics": [{"id": "topic-1", "name": "orders-topic", "sealed": False}],
        }
    )
    db.services_v2.docs.append({"id": "sink-1", "type": "sink_destination", "name": "Orders sink", "config": {}, "retired": False})
    client = client_for(db)
    created = client.post(
        "/api/kafka-connect/syncs",
        json={
            "name": "orders-sync",
                "connector_class": "com.example.Sink",
                "config": {
                    "connector.class": "com.example.Sink",
                    "topics": "orders-topic",
                    "password": "real-secret",
                "s3AccessKey": "real-access-key",
            },
        },
    )
    assert created.status_code == 200
    body = created.json()
    assert body["config"]["password"] == "[secret]"
    assert body["config"]["s3AccessKey"] == "[secret]"
    assert body["has_secrets"] is True
    assert body["remote_present"] is False
    assert body["configuration_state"] == "draft"
    assert body["pending_changes"] is False

    sync_id = body["id"]
    linked = client.post(f"/api/kafka-connect/syncs/{sync_id}/link", json={"flow_id": "flow-1", "block_id": "block-1"})
    assert linked.status_code == 200
    assert linked.json()["linked_flow_id"] == "flow-1"
    assert db.flows_v2.docs[0]["blocks"][0]["config"]["syncId"] == sync_id

    listed = client.get("/api/kafka-connect/syncs")
    assert listed.status_code == 200
    assert listed.json()[0]["linked_block_id"] == "block-1"

    edited = client.post(
        "/api/kafka-connect/syncs",
        json={
            "id": sync_id,
            "name": "orders-sync-renamed",
            "connector_class": "com.example.Sink",
            "config": {"connector.class": "com.example.Sink", "topics": "orders-topic", "password": "[secret]"},
        },
    )
    assert edited.status_code == 200
    assert db.kafka_connect_syncs_v2.docs[0]["config"]["password"] == "real-secret"
    assert db.kafka_connect_syncs_v2.docs[0]["config"]["s3AccessKey"] == "real-access-key"

    db.bulk_jobs_v2.docs.append(
        {
            "id": "bulk-1",
            "status": "queued",
            "verb": "deploy",
            "created_at": "2026-08-30T00:00:00.000Z",
            "items": [{"flow_id": "flow-1", "status": "pending"}],
        }
    )
    locked_unlink = client.post(f"/api/kafka-connect/syncs/{sync_id}/unlink")
    assert locked_unlink.status_code == 409
    db.bulk_jobs_v2.docs.clear()

    unlinked = client.post(f"/api/kafka-connect/syncs/{sync_id}/unlink")
    assert unlinked.status_code == 200
    assert "syncId" not in db.flows_v2.docs[0]["blocks"][0]["config"]


def test_adopt_existing_connector_sets_baseline_without_lifecycle_call(monkeypatch):
    db = FakeDB()
    client = client_for(db)
    sync = client.post(
        "/api/kafka-connect/syncs",
        json={
            "name": "existing-sync",
            "connector_name": "existing-connector",
            "connector_class": "com.example.Sink",
            "config": {"connector.class": "com.example.Sink", "topics": "orders-topic", "tasks.max": "1"},
        },
    ).json()
    calls = []

    async def fake_resolve(_db, _kind, required=False):
        return {"endpoint": "http://connect:8083", "auth_type": "NONE"}

    async def fake_config(_conn, name):
        calls.append(("config", name))
        return {
            "ok": True,
            "data": {
                "connector.class": "com.example.Sink",
                "topics": "orders-topic",
                "tasks.max": "1",
            },
        }

    monkeypatch.setattr(kafka_connect, "resolve_connection", fake_resolve)
    monkeypatch.setattr(kafka_connect, "get_connector_config", fake_config)
    monkeypatch.setattr(kafka_connect, "upsert_connector", lambda *_args, **_kwargs: calls.append(("upsert",)) or {"ok": True})

    adopted = client.post(f"/api/kafka-connect/syncs/{sync['id']}/adopt")
    assert adopted.status_code == 200, adopted.text
    assert adopted.json()["enabled"] is True
    assert adopted.json()["remote_present"] is True
    assert adopted.json()["configuration_state"] == "synced"
    assert adopted.json()["pending_changes"] is False
    assert calls == [("config", "existing-connector")]

    edited = client.post(
        "/api/kafka-connect/syncs",
        json={
            "id": sync["id"],
            "name": "existing-sync",
            "connector_name": "existing-connector",
            "connector_class": "com.example.Sink",
            "config": {"connector.class": "com.example.Sink", "topics": "orders-topic", "tasks.max": "2"},
        },
    )
    assert edited.status_code == 200, edited.text
    assert edited.json()["configuration_state"] == "changes_pending"
    assert edited.json()["pending_changes"] is True

    assert not any(call[0] == "upsert" for call in calls)


def test_sync_rejects_non_kafka_connect_block():
    db = FakeDB()
    db.flows_v2.docs.append({"id": "flow-1", "blocks": [{"id": "block-1", "adapter": "http", "config": {}}]})
    client = client_for(db)
    sync = client.post("/api/kafka-connect/syncs", json={"name": "sync", "connector_class": "com.example.Sink", "config": {}}).json()
    response = client.post(f"/api/kafka-connect/syncs/{sync['id']}/link", json={"flow_id": "flow-1", "block_id": "block-1"})
    assert response.status_code == 422


def test_sync_link_rejects_source_direction_and_topic_mismatch():
    db = FakeDB()
    db.services_v2.docs.append({"id": "sink-1", "type": "sink_destination", "name": "Orders sink", "config": {}, "retired": False})
    db.flows_v2.docs.append(
        {
            "id": "flow-1",
            "name": "Orders",
            "topics": [{"id": "topic-1", "name": "orders-topic", "sealed": False}],
            "blocks": [{
                "id": "block-1",
                "adapter": "kc",
                "name": "Orders sink",
                "entity": "orders",
                "serviceId": "sink-1",
                "config": {"attachTopicId": "topic-1"},
            }],
        }
    )
    client = client_for(db)
    sync = client.post(
        "/api/kafka-connect/syncs",
        json={
            "name": "wrong-direction",
            "direction": "source",
            "connector_class": "com.example.Source",
            "config": {"connector.class": "com.example.Source", "topics": "different-topic"},
        },
    ).json()

    response = client.post(
        f"/api/kafka-connect/syncs/{sync['id']}/link",
        json={"flow_id": "flow-1", "block_id": "block-1"},
    )
    assert response.status_code == 422
    messages = [issue["message"] for issue in response.json()["detail"]["issues"]]
    assert any("sink-direction" in message for message in messages)
    assert any("Topic mismatch" in message for message in messages)
    assert "syncId" not in db.flows_v2.docs[0]["blocks"][0]["config"]


def test_sync_link_rejects_unattached_topic_and_missing_sync_topic():
    # Post-migration, a kc/kafka_kc block's sink config is a verbatim
    # passthrough (compiler no longer derives it from a bound Application
    # Service), so "Select the sink destination service." was removed as a
    # refusal here -- linking no longer cares whether a destination service
    # is bound, only whether the flow side has a resolvable topic and the
    # sync side has exactly one. This covers both of those still-real checks:
    # the block's flow-topic attachment is missing, and the sync's own
    # config has no `topics`/`topic` at all.
    db = FakeDB()
    db.flows_v2.docs.append(
        {
            "id": "flow-1",
            "name": "Incomplete",
            "blocks": [{"id": "block-1", "adapter": "kc", "entity": "orders", "config": {}}],
        }
    )
    client = client_for(db)
    sync = client.post(
        "/api/kafka-connect/syncs",
        json={"name": "incomplete", "connector_class": "com.example.Sink", "config": {}},
    ).json()
    response = client.post(
        f"/api/kafka-connect/syncs/{sync['id']}/link",
        json={"flow_id": "flow-1", "block_id": "block-1"},
    )
    assert response.status_code == 422
    messages = [issue["message"] for issue in response.json()["detail"]["issues"]]
    assert any("Attach the Kafka Connect subscription" in message for message in messages)
    assert any("Set exactly one topic" in message for message in messages)


def test_sync_link_rejects_connector_class_mismatch():
    # The flow block now carries its own complete sinkConfig (including
    # connector.class) rather than pointing at a bound sink service; linking
    # a sync whose connector_class disagrees with the block's sinkConfig
    # connector.class must still be refused.
    db = FakeDB()
    db.flows_v2.docs.append(
        {
            "id": "flow-1",
            "name": "Orders",
            "topics": [{"id": "topic-1", "name": "orders-topic", "sealed": False}],
            "blocks": [{
                "id": "block-1",
                "adapter": "kc",
                "name": "Orders sink",
                "entity": "orders",
                "config": {
                    "attachTopicId": "topic-1",
                    "sinkConfig": {"connector.class": "org.apache.iceberg.connect.IcebergSinkConnector"},
                },
            }],
        }
    )
    client = client_for(db)
    sync = client.post(
        "/api/kafka-connect/syncs",
        json={
            "name": "mismatched-class",
            "connector_class": "com.example.OtherSinkConnector",
            "config": {"connector.class": "com.example.OtherSinkConnector", "topics": "orders-topic"},
        },
    ).json()
    response = client.post(
        f"/api/kafka-connect/syncs/{sync['id']}/link",
        json={"flow_id": "flow-1", "block_id": "block-1"},
    )
    assert response.status_code == 422
    messages = [issue["message"] for issue in response.json()["detail"]["issues"]]
    assert any("Connector class mismatch" in message for message in messages)
    assert any(
        "org.apache.iceberg.connect.IcebergSinkConnector" in message and "com.example.OtherSinkConnector" in message
        for message in messages
    )


def test_flow_builder_link_is_resolved_and_delete_requires_retirement():
    db = FakeDB()
    client = client_for(db)
    sync = client.post(
        "/api/kafka-connect/syncs",
        json={"name": "builder-sync", "connector_class": "com.example.Sink", "config": {}},
    ).json()
    db.flows_v2.docs.append(
        {"id": "flow-2", "blocks": [{"id": "block-2", "adapter": "kafka_kc", "config": {"syncId": sync["id"]}}]}
    )

    resolved = client.get(f"/api/kafka-connect/syncs/{sync['id']}")
    assert resolved.status_code == 200
    assert resolved.json()["linked_flow_id"] == "flow-2"

    active_delete = client.delete(f"/api/kafka-connect/syncs/{sync['id']}")
    assert active_delete.status_code == 409
    assert "Retire the sync first" in active_delete.json()["detail"]

    retired = client.post(f"/api/kafka-connect/syncs/{sync['id']}/retire")
    assert retired.status_code == 200
    assert retired.json()["retired"] is True

    deleted = client.delete(f"/api/kafka-connect/syncs/{sync['id']}")
    assert deleted.status_code == 200
    # Like Application Service deletion, the dependent flow retains its
    # reference so validation can present an explicit replacement warning.
    assert db.flows_v2.docs[0]["blocks"][0]["config"]["syncId"] == sync["id"]


def test_sync_lifecycle_actions_persist_live_status_and_retirement(monkeypatch):
    db = FakeDB()
    client = client_for(db)
    sync = client.post(
        "/api/kafka-connect/syncs",
        json={"name": "runtime-sync", "connector_class": "com.example.Sink", "config": {}},
    ).json()
    db.kafka_connect_syncs_v2.docs[0]["enabled"] = True
    calls = []

    async def fake_resolve(_db, _kind, required=False):
        return {"endpoint": "http://connect:8083", "auth_type": "NONE"}

    async def fake_lifecycle(_conn, name):
        calls.append(name)
        return {"ok": True, "data": None}

    async def fake_status(_conn, name):
        return {
            "ok": True,
            "data": {
                "name": name,
                "connector": {"state": "RUNNING", "worker_id": "worker-1"},
                "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "worker-1"}],
            },
        }

    monkeypatch.setattr(kafka_connect, "resolve_connection", fake_resolve)
    monkeypatch.setattr(kafka_connect, "pause_connector", fake_lifecycle)
    monkeypatch.setattr(kafka_connect, "resume_connector", fake_lifecycle)
    monkeypatch.setattr(kafka_connect, "start_connector", fake_lifecycle)
    monkeypatch.setattr(kafka_connect, "stop_connector", fake_lifecycle)
    monkeypatch.setattr(kafka_connect, "restart_connector", fake_lifecycle)
    monkeypatch.setattr(kafka_connect, "get_connector_status", fake_status)

    for verb in ("start", "stop", "pause", "resume", "restart"):
        response = client.post(f"/api/kafka-connect/syncs/{sync['id']}/{verb}")
        assert response.status_code == 200, response.text
        assert response.json()["last_status"]["connector"]["state"] == "RUNNING"
        assert response.json()["last_status"]["tasks"][0]["state"] == "RUNNING"

    refreshed = client.get("/api/kafka-connect/syncs/statuses")
    assert refreshed.status_code == 200
    assert refreshed.json()[0]["last_status"]["connector"]["state"] == "RUNNING"
    assert len(calls) == 5

    retired = client.post(f"/api/kafka-connect/syncs/{sync['id']}/retire")
    assert retired.status_code == 200
    assert retired.json()["retired"] is True
    assert client.post(f"/api/kafka-connect/syncs/{sync['id']}/start").status_code == 409

    reinstated = client.post(f"/api/kafka-connect/syncs/{sync['id']}/reinstate")
    assert reinstated.status_code == 200
    assert reinstated.json()["retired"] is False


def test_sync_retire_and_delete_refuse_deployed_dependents(monkeypatch):
    db = FakeDB()
    client = client_for(db)
    sync = client.post(
        "/api/kafka-connect/syncs",
        json={"name": "deployed-sync", "connector_class": "com.example.Sink", "config": {}},
    ).json()
    db.kafka_connect_syncs_v2.docs[0]["enabled"] = True
    db.flows_v2.docs.append(
        {
            "id": "flow-1",
            "name": "Deployed flow",
            "deployedAt": "2026-08-30T00:00:00.000Z",
            "blocks": [{"id": "block-1", "adapter": "kafka_kc", "config": {"syncId": sync["id"]}}],
        }
    )
    lifecycle_calls = []
    deleted_names = []

    async def fake_resolve(_db, _kind, required=False):
        return {"endpoint": "http://connect:8083", "auth_type": "NONE"}

    async def fake_pause(_conn, name):
        lifecycle_calls.append(("pause", name))
        return {"ok": True, "data": None}

    async def fake_delete(_conn, name):
        deleted_names.append(name)
        return {"ok": True, "data": None}

    monkeypatch.setattr(kafka_connect, "resolve_connection", fake_resolve)
    monkeypatch.setattr(kafka_connect, "pause_connector", fake_pause)
    monkeypatch.setattr(kafka_connect, "delete_connector", fake_delete)

    retired = client.post(f"/api/kafka-connect/syncs/{sync['id']}/retire")
    assert retired.status_code == 409
    assert "deployed flow(s)" in retired.json()["detail"]
    assert lifecycle_calls == []

    # A legacy retired record must also be protected by the hard-delete gate.
    db.kafka_connect_syncs_v2.docs[0]["retired"] = True
    response = client.delete(f"/api/kafka-connect/syncs/{sync['id']}")
    assert response.status_code == 409, response.text
    assert "deployed flow(s)" in response.json()["detail"]
    assert deleted_names == []
    assert db.kafka_connect_syncs_v2.docs != []
    assert db.flows_v2.docs[0]["blocks"][0]["config"]["syncId"] == sync["id"]


def test_delete_retired_remote_sync_requires_kafka_connect(monkeypatch):
    db = FakeDB()
    client = client_for(db)
    sync = client.post(
        "/api/kafka-connect/syncs",
        json={
            "name": "remote-sync",
            "connector_name": "remote-connector",
            "connector_class": "com.example.Sink",
            "config": {"connector.class": "com.example.Sink"},
        },
    ).json()
    db.kafka_connect_syncs_v2.docs[0].update({"retired": True, "enabled": True, "remote_present": True})

    async def no_connection(_db, _kind, required=False):
        return None

    monkeypatch.setattr(kafka_connect, "resolve_connection", no_connection)

    response = client.delete(f"/api/kafka-connect/syncs/{sync['id']}")
    assert response.status_code == 503
    assert "could not be confirmed" in response.json()["detail"]
    assert db.kafka_connect_syncs_v2.docs[0]["retired"] is True


def test_enabled_toggle_is_not_redacted_but_credentials_still_are():
    from services.adapter.sink_secrets import merge_preserving_secrets, redact_config

    config = {
        "iceberg.catalog.token-refresh-enabled": "true",
        "iceberg.catalog.credential": "real-credential",
        "iceberg.catalog.s3.access-key-id": "real-access-key-id",
        "iceberg.catalog.s3.secret-access-key": "real-secret-access-key",
    }

    redacted = redact_config(config)
    assert redacted["iceberg.catalog.token-refresh-enabled"] == "true"
    assert redacted["iceberg.catalog.credential"] == "[secret]"
    assert redacted["iceberg.catalog.s3.access-key-id"] == "[secret]"
    assert redacted["iceberg.catalog.s3.secret-access-key"] == "[secret]"

    # Simulate a client round trip: the boolean comes back with its true
    # value (never redacted so never a placeholder), the credentials come
    # back as the placeholder because the client couldn't see their values.
    incoming = {
        "iceberg.catalog.token-refresh-enabled": "true",
        "iceberg.catalog.credential": "[secret]",
        "iceberg.catalog.s3.access-key-id": "[secret]",
        "iceberg.catalog.s3.secret-access-key": "[secret]",
    }
    merged = merge_preserving_secrets(incoming, config)
    assert merged["iceberg.catalog.token-refresh-enabled"] == "true"
    assert merged["iceberg.catalog.credential"] == "real-credential"
    assert merged["iceberg.catalog.s3.access-key-id"] == "real-access-key-id"
    assert merged["iceberg.catalog.s3.secret-access-key"] == "real-secret-access-key"
