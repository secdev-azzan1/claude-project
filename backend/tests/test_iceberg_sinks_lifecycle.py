"""Regression tests for Kafka Connect failure handling in the Iceberg sink lifecycle.

These cover the class of bug where a non-2xx Kafka Connect response was reported as
success, which let enable_sink stamp a sink as enabled/provenanced even though Connect
had rejected the connector config. Offline only — httpx is stubbed, no live cluster.
"""
import asyncio
from functools import wraps
from pathlib import Path
import sys

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from models.connection import ConnectionUpdate
from services import iceberg_sinks, kafka_connect_client


def async_test(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


# --------------------------------------------------------------------------- stubs


class FakeResponse:
    def __init__(self, status_code, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text
        self.content = b"x" if (payload is not None or text) else b""

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


class FakeAsyncClient:
    """Stands in for httpx.AsyncClient, returning a scripted response."""

    scripted = FakeResponse(200, {})

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def request(self, method, url, **kwargs):
        return FakeAsyncClient.scripted


def _use_response(monkeypatch, response):
    FakeAsyncClient.scripted = response
    monkeypatch.setattr(kafka_connect_client.httpx, "AsyncClient", FakeAsyncClient)


CONN = {"endpoint": "http://connect:8083", "auth_type": "NONE"}


# ------------------------------------------------------- _request status semantics


def test_request_marks_400_as_failure(monkeypatch):
    """A 400 must NOT be reported as success (the original bug)."""
    _use_response(monkeypatch, FakeResponse(400, {"message": "Invalid config"}))

    @async_test
    async def run():
        result = await kafka_connect_client._request(CONN, "PUT", "/connectors/x/config", json_body={})
        assert result["ok"] is False
        assert result["reachable"] is True
        assert result["status_code"] == 400
        assert result["error_code"] == kafka_connect_client.CONNECT_CONFIG_INVALID
        assert "Invalid config" in result["error"]

    run()


def test_request_marks_500_as_failure(monkeypatch):
    _use_response(monkeypatch, FakeResponse(500, {"message": "boom"}))

    @async_test
    async def run():
        result = await kafka_connect_client._request(CONN, "GET", "/connectors")
        assert result["ok"] is False
        assert result["status_code"] == 500

    run()


def test_request_marks_404_as_failure_with_not_found_code(monkeypatch):
    _use_response(monkeypatch, FakeResponse(404, {"message": "Unknown connector"}))

    @async_test
    async def run():
        result = await kafka_connect_client._request(CONN, "GET", "/connectors/x/status")
        assert result["ok"] is False
        assert result["error_code"] == kafka_connect_client.CONNECT_NOT_FOUND

    run()


def test_request_accepts_2xx(monkeypatch):
    _use_response(monkeypatch, FakeResponse(201, {"name": "c"}))

    @async_test
    async def run():
        result = await kafka_connect_client._request(CONN, "PUT", "/connectors/c/config", json_body={})
        assert result["ok"] is True
        assert result["data"] == {"name": "c"}

    run()


def test_request_401_is_auth_failed(monkeypatch):
    _use_response(monkeypatch, FakeResponse(401, {"message": "nope"}))

    @async_test
    async def run():
        result = await kafka_connect_client._request(CONN, "GET", "/")
        assert result["ok"] is False
        assert result["error_code"] == kafka_connect_client.CONNECT_AUTH_FAILED

    run()


def test_request_409_is_rebalancing(monkeypatch):
    _use_response(monkeypatch, FakeResponse(409, {"message": "rebalancing"}))

    @async_test
    async def run():
        result = await kafka_connect_client._request(CONN, "PUT", "/connectors/x/config", json_body={})
        assert result["ok"] is False
        assert result["error_code"] == kafka_connect_client.CONNECT_REBALANCING

    run()


# ------------------------------------------------------------------ caller wrappers


def test_upsert_connector_reports_invalid_config(monkeypatch):
    _use_response(monkeypatch, FakeResponse(400, {"message": "Missing required field"}))

    @async_test
    async def run():
        result = await kafka_connect_client.upsert_connector(CONN, "t__iceberg", {"a": "b"})
        assert result["ok"] is False
        assert result["error_code"] == kafka_connect_client.CONNECT_CONFIG_INVALID

    run()


def test_delete_connector_treats_404_as_success(monkeypatch):
    """Deleting an already-gone connector is still a success."""
    _use_response(monkeypatch, FakeResponse(404, {"message": "Unknown connector"}))

    @async_test
    async def run():
        result = await kafka_connect_client.delete_connector(CONN, "t__iceberg")
        assert result["ok"] is True
        assert result["status_code"] == 404

    run()


def test_restart_connector_404_is_failure(monkeypatch):
    _use_response(monkeypatch, FakeResponse(404, {"message": "Unknown connector"}))

    @async_test
    async def run():
        result = await kafka_connect_client.restart_connector(CONN, "t__iceberg")
        assert result["ok"] is False
        assert result["error_code"] == kafka_connect_client.CONNECT_NOT_FOUND

    run()


def test_start_and_stop_connector_use_supported_lifecycle_routes(monkeypatch):
    seen = []

    class RecordingAsyncClient(FakeAsyncClient):
        async def request(self, method, url, **kwargs):
            seen.append((method, url))
            return FakeResponse(202)

    monkeypatch.setattr(kafka_connect_client.httpx, "AsyncClient", RecordingAsyncClient)

    @async_test
    async def run():
        started = await kafka_connect_client.start_connector(CONN, "sync name")
        stopped = await kafka_connect_client.stop_connector(CONN, "sync name")
        assert started["ok"] is True
        assert stopped["ok"] is True

    run()
    assert seen == [
        ("PUT", "http://connect:8083/connectors/sync%20name/resume"),
        ("PUT", "http://connect:8083/connectors/sync%20name/stop"),
    ]


# ------------------------------------------------ enable_sink must not stamp on failure


class FakeSinkCollection:
    def __init__(self, docs):
        self.docs = {d["id"]: dict(d) for d in docs}

    async def find_one(self, query, projection=None):
        doc = self.docs.get(query.get("id"))
        return dict(doc) if doc else None

    async def update_one(self, query, update):
        doc = self.docs.get(query.get("id"))
        if doc is not None:
            doc.update(update.get("$set") or {})


class FakeAuditCollection:
    def __init__(self):
        self.events = []

    async def insert_one(self, doc):
        self.events.append(doc)


class FakeDB:
    def __init__(self, sinks):
        self.iceberg_sinks = FakeSinkCollection(sinks)
        self.audit_events = FakeAuditCollection()


SINK = {
    "id": "sink-1",
    "flow_id": "flow-1",
    "stream_id": "stream-1",
    "topic": "bronze.src.entity__history",
    "connector_name": "bronze.src.entity__history__iceberg",
    "table_name": "src.entity__history",
    "enabled": False,
    "overrides": {"enabled": True},
}


def test_enable_sink_does_not_stamp_enabled_when_connect_rejects(monkeypatch):
    """The core regression: a rejected connector must leave the sink disabled."""
    db = FakeDB([SINK])

    async def fake_preflight(_db, _sink):
        return {"ok": True, "checks": []}

    async def fake_resolve(_db):
        return {
            "ok": True,
            "missing": [],
            "connect": {"id": "conn-connect"},
            "iceberg": {"id": "conn-iceberg"},
            "apicurio": {"id": "conn-apicurio"},
            "kafka": {"id": "conn-kafka"},
        }

    async def fake_build(_db, _sink):
        return {"connector.class": "x"}

    async def fake_upsert(_conn, _name, _config):
        return {
            "ok": False,
            "reachable": True,
            "status_code": 400,
            "error": "Invalid config: missing iceberg.catalog.uri",
            "error_code": kafka_connect_client.CONNECT_CONFIG_INVALID,
        }

    monkeypatch.setattr(iceberg_sinks, "preflight_sink", fake_preflight)
    monkeypatch.setattr(iceberg_sinks, "_resolve_sink_connections", fake_resolve)
    monkeypatch.setattr(iceberg_sinks, "build_config_for_sink", fake_build)
    monkeypatch.setattr(iceberg_sinks, "upsert_connector", fake_upsert)

    @async_test
    async def run():
        result = await iceberg_sinks.enable_sink(db, "sink-1")

        assert result["ok"] is False
        assert "Invalid config" in result["error"]

        stored = db.iceberg_sinks.docs["sink-1"]
        assert stored["enabled"] is False, "sink must NOT be marked enabled after a rejected config"
        assert stored.get("connect_connection_id") is None, "provenance must not be stamped on failure"
        assert "Invalid config" in (stored.get("last_error") or "")

    run()


def test_connection_update_accepts_iceberg_fields():
    """ConnectionUpdate is extra="forbid"; without the iceberg fields, editing an
    iceberg connection 422s. Regression guard for that mismatch."""
    update = ConnectionUpdate(
        endpoint="http://polaris:8181/api/catalog",
        iceberg_warehouse="bronze",
        iceberg_credential="root:s3cr3t",
        iceberg_oauth2_server_uri="http://polaris:8181/api/catalog/v1/oauth/tokens",
        iceberg_scope="PRINCIPAL_ROLE:ALL",
        s3_endpoint="http://s3g:9878",
        s3_access_key_id="admin",
        s3_secret_access_key="password",
        s3_region="us-east-1",
        s3_path_style_access=True,
    )
    assert update.iceberg_warehouse == "bronze"
    assert update.s3_path_style_access is True


def test_enable_sink_returns_checks_when_preflight_fails(monkeypatch):
    """A failed preflight must surface the checklist and never call Connect."""
    db = FakeDB([SINK])
    called = {"upsert": False}

    async def fake_preflight(_db, _sink):
        return {"ok": False, "checks": [{"name": "plugin_installed", "ok": False, "detail": "missing"}]}

    async def fake_upsert(_conn, _name, _config):
        called["upsert"] = True
        return {"ok": True}

    monkeypatch.setattr(iceberg_sinks, "preflight_sink", fake_preflight)
    monkeypatch.setattr(iceberg_sinks, "upsert_connector", fake_upsert)

    @async_test
    async def run():
        result = await iceberg_sinks.enable_sink(db, "sink-1")
        assert result["ok"] is False
        assert result["checks"][0]["name"] == "plugin_installed"
        assert called["upsert"] is False, "Connect must not be contacted when preflight fails"
        assert db.iceberg_sinks.docs["sink-1"]["enabled"] is False

    run()
