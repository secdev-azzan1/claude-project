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


def test_adopt_writes_config_into_linked_block_sink_config(monkeypatch):
    # Adoption reads the real config off the cluster; that config must also
    # land in the linked block's sinkConfig (with `name` stripped) so
    # `authoritative_sink_config`'s block-wins rule doesn't push the block's
    # old, poorer config back over the connector that was just adopted --
    # see `migrations/repair_sync_configs.py` for how that drift happened.
    db = FakeDB()
    db.flows_v2.docs.append(_flow_with_kc_block())
    db.kafka_connect_syncs_v2.docs.append(
        {
            "id": "sync-1",
            "name": "Orders sink",
            "description": "",
            "direction": "sink",
            "connector_class": "com.example.Sink",
            "connector_name": "orders-connector",
            "config": {"connector.class": "com.example.Sink", "topics": "orders-topic"},
            "enabled": False,
            "retired": False,
            "remote_present": False,
            "remote_config_hash": None,
            "linked_flow_id": "flow-1",
            "linked_block_id": "block-1",
            "last_status": None,
            "last_error": None,
            "created_at": "2026-08-30T00:00:00.000Z",
            "updated_at": "2026-08-30T00:00:00.000Z",
        }
    )
    db.flows_v2.docs[0]["blocks"][0]["config"]["syncId"] = "sync-1"
    client = client_for(db)

    async def fake_resolve(_db, _kind, required=False):
        return {"endpoint": "http://connect:8083", "auth_type": "NONE"}

    async def fake_config(_conn, name):
        return {
            "ok": True,
            "data": {
                "name": name,
                "connector.class": "com.example.Sink",
                "topics": "orders-topic",
                "tasks.max": "2",
                "iceberg.catalog": "rest",
            },
        }

    monkeypatch.setattr(kafka_connect, "resolve_connection", fake_resolve)
    monkeypatch.setattr(kafka_connect, "get_connector_config", fake_config)

    adopted = client.post("/api/kafka-connect/syncs/sync-1/adopt")
    assert adopted.status_code == 200, adopted.text

    block_sink_config = db.flows_v2.docs[0]["blocks"][0]["config"]["sinkConfig"]
    assert block_sink_config == {
        "connector.class": "com.example.Sink",
        "topics": "orders-topic",
        "tasks.max": "2",
        "iceberg.catalog": "rest",
    }
    assert "name" not in block_sink_config

    # `name` is only stripped from what's written into the block -- the
    # sync record's own stored config still keeps it, matching the existing,
    # unchanged behaviour of adopt updating the sync record from the remote
    # snapshot verbatim.
    stored = db.kafka_connect_syncs_v2.docs[0]
    assert stored["config"]["name"] == "orders-connector"
    assert stored["config"]["tasks.max"] == "2"
    assert stored["remote_present"] is True


def test_adopt_with_no_linked_block_still_works_and_touches_no_flow(monkeypatch):
    db = FakeDB()
    client = client_for(db)
    sync = client.post(
        "/api/kafka-connect/syncs",
        json={
            "name": "unlinked-sync",
            "connector_name": "unlinked-connector",
            "connector_class": "com.example.Sink",
            "config": {"connector.class": "com.example.Sink", "topics": "orders-topic"},
        },
    ).json()

    async def fake_resolve(_db, _kind, required=False):
        return {"endpoint": "http://connect:8083", "auth_type": "NONE"}

    async def fake_config(_conn, name):
        return {
            "ok": True,
            "data": {"name": name, "connector.class": "com.example.Sink", "topics": "orders-topic"},
        }

    monkeypatch.setattr(kafka_connect, "resolve_connection", fake_resolve)
    monkeypatch.setattr(kafka_connect, "get_connector_config", fake_config)

    adopted = client.post(f"/api/kafka-connect/syncs/{sync['id']}/adopt")
    assert adopted.status_code == 200, adopted.text
    assert adopted.json()["enabled"] is True
    assert adopted.json()["remote_present"] is True
    # No linked block -- nothing in flows_v2 to touch, and nothing was.
    assert db.flows_v2.docs == []


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


def test_flow_builder_link_is_resolved_and_delete_retires_atomically():
    # `delete_sync` now retires-then-deletes in one call instead of refusing
    # a non-retired sync: the old two-step UI flow (retire, then delete)
    # could strand a sync permanently RETIRED if the second call failed.
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

    deleted = client.delete(f"/api/kafka-connect/syncs/{sync['id']}")
    assert deleted.status_code == 200, deleted.text
    assert not any(doc["id"] == sync["id"] for doc in db.kafka_connect_syncs_v2.docs)
    # Like Application Service deletion, the dependent flow retains its
    # reference so validation can present an explicit replacement warning.
    assert db.flows_v2.docs[0]["blocks"][0]["config"]["syncId"] == sync["id"]


def test_delete_sync_un_retires_on_remote_delete_failure(monkeypatch):
    db = FakeDB()
    client = client_for(db)
    sync = client.post(
        "/api/kafka-connect/syncs",
        json={
            "name": "flaky-sync",
            "connector_name": "flaky-connector",
            "connector_class": "com.example.Sink",
            "config": {"connector.class": "com.example.Sink"},
        },
    ).json()
    db.kafka_connect_syncs_v2.docs[0].update({"enabled": True, "remote_present": True})

    async def fake_resolve(_db, _kind, required=False):
        return {"endpoint": "http://connect:8083", "auth_type": "NONE"}

    async def fake_pause(_conn, name):
        return {"ok": True, "data": None}

    async def failing_delete(_conn, name):
        return {"ok": False, "error": "cluster rejected the delete", "error_code": None}

    monkeypatch.setattr(kafka_connect, "resolve_connection", fake_resolve)
    monkeypatch.setattr(kafka_connect, "pause_connector", fake_pause)
    monkeypatch.setattr(kafka_connect, "delete_connector", failing_delete)

    response = client.delete(f"/api/kafka-connect/syncs/{sync['id']}")
    assert response.status_code == 502
    # The sync was retired as part of this same delete call and must be
    # restored to not-retired since the delete itself failed -- otherwise
    # it would be stuck RETIRED with no controls and no way back.
    assert db.kafka_connect_syncs_v2.docs[0]["retired"] is False


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


# ------------------------------------------------------ flow+block sink API

def _flow_with_kc_block(flow_id="flow-1", block_id="block-1", flow_name="Orders"):
    return {
        "id": flow_id,
        "name": flow_name,
        "blocks": [
            {
                "id": block_id,
                "adapter": "kafka_kc",
                "name": "Orders sink",
                "config": {"sinkConfig": {"connector.class": "com.example.Sink", "topics": "orders-topic"}},
            }
        ],
    }


def test_flow_sink_status_reports_undeployed_for_missing_connector(monkeypatch):
    db = FakeDB()
    db.flows_v2.docs.append(_flow_with_kc_block())
    client = client_for(db)

    async def fake_resolve(_db, _kind, required=False):
        return {"endpoint": "http://connect:8083", "auth_type": "NONE"}

    async def fake_listing(_conn):
        return {"ok": True, "reachable": True, "data": {}}

    monkeypatch.setattr(kafka_connect, "resolve_connection", fake_resolve)
    monkeypatch.setattr(kafka_connect, "list_connectors_with_status", fake_listing)

    response = client.get("/api/kafka-connect/flows/flow-1/sink-status")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["ok"] is True
    assert body["reachable"] is True
    assert len(body["sinks"]) == 1
    sink = body["sinks"][0]
    assert sink["blockId"] == "block-1"
    assert sink["state"] == "UNDEPLOYED"
    assert sink["syncId"] is None


def test_flow_sink_status_unreachable_reports_null_state_not_undeployed(monkeypatch):
    db = FakeDB()
    db.flows_v2.docs.append(_flow_with_kc_block())
    client = client_for(db)

    async def fake_resolve(_db, _kind, required=False):
        return {"endpoint": "http://connect:8083", "auth_type": "NONE"}

    async def fake_listing(_conn):
        return {"ok": False, "reachable": False, "error": "Cannot connect to Kafka Connect."}

    monkeypatch.setattr(kafka_connect, "resolve_connection", fake_resolve)
    monkeypatch.setattr(kafka_connect, "list_connectors_with_status", fake_listing)

    response = client.get("/api/kafka-connect/flows/flow-1/sink-status")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["reachable"] is False
    assert len(body["sinks"]) == 1
    assert body["sinks"][0]["state"] is None


def test_flow_sink_start_pushes_block_config_not_stale_sync_copy(monkeypatch):
    # Regression for the bug where Start pushed `sync["config"]` -- a legacy
    # 2-key copy made when the sync record was created -- and silently
    # overwrote a working connector built from the block's full sinkConfig.
    db = FakeDB()
    db.flows_v2.docs.append(_flow_with_kc_block())
    flow_sink_config = db.flows_v2.docs[0]["blocks"][0]["config"]["sinkConfig"]
    db.kafka_connect_syncs_v2.docs.append(
        {
            "id": "sync-1",
            "name": "Orders sink",
            "description": "",
            "direction": "sink",
            "connector_class": "com.example.Sink",
            "connector_name": "orders-connector",
            # Deliberately stale/smaller than the block's sinkConfig.
            "config": {"connector.class": "com.example.Sink", "topics": "orders-topic"},
            "enabled": False,
            "retired": False,
            "remote_present": False,
            "remote_config_hash": None,
            "linked_flow_id": "flow-1",
            "linked_block_id": "block-1",
            "last_status": None,
            "last_error": None,
            "created_at": "2026-08-30T00:00:00.000Z",
            "updated_at": "2026-08-30T00:00:00.000Z",
        }
    )
    db.flows_v2.docs[0]["blocks"][0]["config"]["syncId"] = "sync-1"
    client = client_for(db)
    pushed = []

    async def fake_resolve(_db, _kind, required=False):
        return {"endpoint": "http://connect:8083", "auth_type": "NONE"}

    async def fake_upsert(_conn, name, config):
        pushed.append(config)
        return {"ok": True, "data": None}

    async def fake_start(_conn, name):
        return {"ok": True, "data": None}

    async def fake_status(_conn, name):
        return {"ok": True, "data": {"name": name, "connector": {"state": "RUNNING"}, "tasks": []}}

    monkeypatch.setattr(kafka_connect, "resolve_connection", fake_resolve)
    monkeypatch.setattr(kafka_connect, "upsert_connector", fake_upsert)
    monkeypatch.setattr(kafka_connect, "start_connector", fake_start)
    monkeypatch.setattr(kafka_connect, "get_connector_status", fake_status)

    response = client.post("/api/kafka-connect/flows/flow-1/sinks/block-1/start")
    assert response.status_code == 200, response.text

    # The connector was pushed the block's config, not the sync's stale copy.
    assert pushed == [flow_sink_config]

    stored = db.kafka_connect_syncs_v2.docs[0]
    assert stored["config"] == flow_sink_config
    assert stored["remote_config_hash"] == kafka_connect._config_fingerprint(flow_sink_config)


def test_apply_sync_pushes_block_config_not_stale_sync_copy(monkeypatch):
    # Same regression as above, for the `apply_sync` push path.
    db = FakeDB()
    db.flows_v2.docs.append(_flow_with_kc_block())
    flow_sink_config = db.flows_v2.docs[0]["blocks"][0]["config"]["sinkConfig"]
    db.kafka_connect_syncs_v2.docs.append(
        {
            "id": "sync-1",
            "name": "Orders sink",
            "description": "",
            "direction": "sink",
            "connector_class": "com.example.Sink",
            "connector_name": "orders-connector",
            "config": {"connector.class": "com.example.Sink", "topics": "orders-topic"},
            "enabled": False,
            "retired": False,
            "remote_present": False,
            "remote_config_hash": None,
            "linked_flow_id": "flow-1",
            "linked_block_id": "block-1",
            "last_status": None,
            "last_error": None,
            "created_at": "2026-08-30T00:00:00.000Z",
            "updated_at": "2026-08-30T00:00:00.000Z",
        }
    )
    db.flows_v2.docs[0]["blocks"][0]["config"]["syncId"] = "sync-1"
    client = client_for(db)
    pushed = []

    async def fake_resolve(_db, _kind, required=False):
        return {"endpoint": "http://connect:8083", "auth_type": "NONE"}

    async def fake_upsert(_conn, name, config):
        pushed.append(config)
        return {"ok": True, "data": None}

    async def fake_status(_conn, name):
        return {"ok": True, "data": {"name": name, "connector": {"state": "RUNNING"}, "tasks": []}}

    monkeypatch.setattr(kafka_connect, "resolve_connection", fake_resolve)
    monkeypatch.setattr(kafka_connect, "upsert_connector", fake_upsert)
    monkeypatch.setattr(kafka_connect, "get_connector_status", fake_status)

    response = client.post("/api/kafka-connect/syncs/sync-1/apply")
    assert response.status_code == 200, response.text

    assert pushed == [flow_sink_config]
    stored = db.kafka_connect_syncs_v2.docs[0]
    assert stored["config"] == flow_sink_config
    assert stored["remote_config_hash"] == kafka_connect._config_fingerprint(flow_sink_config)


def test_flow_sink_start_falls_back_to_sync_config_when_no_block_sink_config(monkeypatch):
    # A sync with no linked block (adopted/unlinked) -- or a linked block
    # whose sinkConfig is empty -- must keep pushing its own stored config.
    db = FakeDB()
    own_config = {"connector.class": "com.example.Sink", "topics": "orders-topic", "tasks.max": "3"}
    sync = client_for(db).post(
        "/api/kafka-connect/syncs",
        json={
            "name": "unlinked-sync",
            "connector_name": "unlinked-connector",
            "connector_class": "com.example.Sink",
            "config": own_config,
        },
    ).json()
    client = client_for(db)
    pushed = []

    async def fake_resolve(_db, _kind, required=False):
        return {"endpoint": "http://connect:8083", "auth_type": "NONE"}

    async def fake_upsert(_conn, name, config):
        pushed.append(config)
        return {"ok": True, "data": None}

    async def fake_status(_conn, name):
        return {"ok": True, "data": {"name": name, "connector": {"state": "RUNNING"}, "tasks": []}}

    monkeypatch.setattr(kafka_connect, "resolve_connection", fake_resolve)
    monkeypatch.setattr(kafka_connect, "upsert_connector", fake_upsert)
    monkeypatch.setattr(kafka_connect, "get_connector_status", fake_status)

    response = client.post(f"/api/kafka-connect/syncs/{sync['id']}/apply")
    assert response.status_code == 200, response.text
    assert pushed == [own_config]


def test_flow_sink_start_creates_connector_and_sync_record(monkeypatch):
    db = FakeDB()
    db.flows_v2.docs.append(_flow_with_kc_block())
    client = client_for(db)
    calls = []

    async def fake_resolve(_db, _kind, required=False):
        return {"endpoint": "http://connect:8083", "auth_type": "NONE"}

    async def fake_upsert(_conn, name, config):
        calls.append(("upsert", name))
        return {"ok": True, "data": None}

    async def fake_start(_conn, name):
        calls.append(("start", name))
        return {"ok": True, "data": None}

    async def fake_status(_conn, name):
        return {"ok": True, "data": {"name": name, "connector": {"state": "RUNNING"}, "tasks": []}}

    monkeypatch.setattr(kafka_connect, "resolve_connection", fake_resolve)
    monkeypatch.setattr(kafka_connect, "upsert_connector", fake_upsert)
    monkeypatch.setattr(kafka_connect, "start_connector", fake_start)
    monkeypatch.setattr(kafka_connect, "get_connector_status", fake_status)

    assert db.kafka_connect_syncs_v2.docs == []
    response = client.post("/api/kafka-connect/flows/flow-1/sinks/block-1/start")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["enabled"] is True
    assert body["remote_present"] is True
    assert body["linked_flow_id"] == "flow-1"
    assert body["linked_block_id"] == "block-1"
    assert body["connector_class"] == "com.example.Sink"

    assert len(db.kafka_connect_syncs_v2.docs) == 1
    stored = db.kafka_connect_syncs_v2.docs[0]
    assert stored["linked_flow_id"] == "flow-1"
    assert stored["linked_block_id"] == "block-1"
    assert stored["connector_name"] == body["connector_name"]
    assert [c[0] for c in calls] == ["upsert", "start"]


def test_flow_sink_verb_refused_while_flow_has_queued_operation():
    db = FakeDB()
    db.flows_v2.docs.append(_flow_with_kc_block())
    db.bulk_jobs_v2.docs.append(
        {
            "id": "bulk-1",
            "status": "queued",
            "verb": "deploy",
            "created_at": "2026-08-30T00:00:00.000Z",
            "items": [{"flow_id": "flow-1", "status": "pending"}],
        }
    )
    client = client_for(db)

    response = client.post("/api/kafka-connect/flows/flow-1/sinks/block-1/start")
    assert response.status_code == 409
    assert "locked by queued operation" in response.json()["detail"]
    # No sync record must be created while the flow is locked.
    assert db.kafka_connect_syncs_v2.docs == []


def _sink_entry_for(connector_state, task_states):
    """Build the `cluster_data` shape `_build_sink_entry` expects and run it,
    with a fixed block/connector name -- only `state` on the connector and
    the per-task states vary between cases."""
    block = _flow_with_kc_block()["blocks"][0]
    tasks = [{"id": i, "state": s} for i, s in enumerate(task_states)]
    cluster_data = {
        "conn-1": {
            "status": {"connector": {"state": connector_state}, "tasks": tasks},
            "info": {"config": {}},
        }
    }
    return kafka_connect._build_sink_entry(block, None, "conn-1", True, cluster_data)


def test_build_sink_entry_reports_failed_when_running_but_all_tasks_failed():
    entry = _sink_entry_for("RUNNING", ["FAILED", "FAILED"])
    assert entry["state"] == "FAILED"
    # The per-task detail and trace still reflect Connect's own view --
    # only the top-line state is overridden.
    assert [t["state"] for t in entry["tasks"]] == ["FAILED", "FAILED"]


def test_build_sink_entry_stays_running_when_some_tasks_failed():
    entry = _sink_entry_for("RUNNING", ["RUNNING", "FAILED", "RUNNING"])
    assert entry["state"] == "RUNNING"
    assert [t["state"] for t in entry["tasks"]] == ["RUNNING", "FAILED", "RUNNING"]


def test_build_sink_entry_stays_running_when_no_tasks_at_all():
    entry = _sink_entry_for("RUNNING", [])
    assert entry["state"] == "RUNNING"
    assert entry["tasks"] == []


def test_build_sink_entry_leaves_paused_state_alone_even_if_all_tasks_failed():
    entry = _sink_entry_for("PAUSED", ["FAILED", "FAILED"])
    assert entry["state"] == "PAUSED"
    assert [t["state"] for t in entry["tasks"]] == ["FAILED", "FAILED"]
