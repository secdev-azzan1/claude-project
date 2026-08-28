"""Tests for the v2 Application Services subsystem
(backend/routers/v2/services.py).

Uses FastAPI's TestClient against a small, test-local FastAPI app that
mounts ONLY `routers.v2.services.router` (this task is not permitted to
touch backend/server.py to wire the router into the real app there), with
the Mongo dependency (`db.get_db`) overridden by an in-memory FakeDB built
on the same FaultInjectingCollection helper the resilience test suite and
tests/test_v2_openapi.py use.

Outbound network calls (`httpx.AsyncClient`, `asyncio.open_connection`, and
`services.iceberg_catalog_client.test_iceberg_connection`) are monkeypatched
per-test -- no real network access happens here.
"""
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from db import get_db
from routers.v2 import services as services_router
from tests.resilience.conftest import FaultInjectingCollection


class FakeDB:
    def __init__(self):
        self.services_v2 = FaultInjectingCollection()
        self.flows_v2 = FaultInjectingCollection()
        self.audit_v2 = FaultInjectingCollection()

    def __getitem__(self, name):
        return getattr(self, name)


def _make_client(fake_db: FakeDB) -> TestClient:
    app = FastAPI()
    app.include_router(services_router.router)
    app.dependency_overrides[get_db] = lambda: fake_db
    return TestClient(app)


# --------------------------------------------------------------- fake httpx


class FakeResponse:
    def __init__(self, status_code: int, json_data: Optional[Dict[str, Any]] = None):
        self.status_code = status_code
        self._json_data = json_data

    def json(self):
        if self._json_data is None:
            raise ValueError("response has no JSON body")
        return self._json_data


def make_scripted_client(responses: Dict[str, Any]):
    """Build an httpx.AsyncClient stand-in. `responses` maps HTTP method
    ("HEAD"/"GET"/"POST") to a FakeResponse or an Exception instance to
    raise. Returns (client_class, calls) where `calls` records every
    (method, url, kwargs) made through the client."""
    calls: List[tuple] = []

    class _Client:
        def __init__(self, *args, **kwargs):
            self.init_kwargs = kwargs

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def head(self, url, **kw):
            return self._respond("HEAD", url, kw)

        async def get(self, url, **kw):
            return self._respond("GET", url, kw)

        async def post(self, url, **kw):
            return self._respond("POST", url, kw)

        def _respond(self, method, url, kw):
            calls.append((method, url, kw))
            resp = responses.get(method)
            if isinstance(resp, Exception):
                raise resp
            return resp

    return _Client, calls


def patch_httpx(monkeypatch, responses: Dict[str, Any]):
    client_cls, calls = make_scripted_client(responses)
    monkeypatch.setattr(services_router.httpx, "AsyncClient", client_cls)
    return calls


def patch_tcp(monkeypatch, *, succeed: bool):
    async def fake_open_connection(host, port):
        if not succeed:
            raise OSError("Connection refused")

        class _Writer:
            def close(self):
                pass

            async def wait_closed(self):
                pass

        return None, _Writer()

    monkeypatch.setattr(services_router.asyncio, "open_connection", fake_open_connection)


# ------------------------------------------------------------------- CRUD


def test_create_service_assigns_id_and_revision_and_audits():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    resp = client.post(
        "/api/v2/services/",
        json={"type": "external_kafka", "name": "Partner Kafka", "config": {"bootstrapServers": "kafka.partner:9093"}},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["id"].startswith("svc-")
    assert body["revision"] == 1
    assert body["type"] == "external_kafka"
    assert body["name"] == "Partner Kafka"
    assert body["createdAt"] == body["updatedAt"]

    assert len(fake_db.services_v2.docs) == 1
    assert len(fake_db.audit_v2.docs) == 1
    event = fake_db.audit_v2.docs[0]
    assert event["action"] == "Service created"
    assert event["object"] == "Application Service"
    assert event["target"] == "Partner Kafka"
    assert event["status"] == "Success"


def test_update_service_bumps_revision_and_appends_history():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    created = client.post(
        "/api/v2/services/",
        json={"type": "external_kafka", "name": "Partner Kafka", "config": {"bootstrapServers": "kafka.partner:9093"}},
    ).json()

    updated_resp = client.post(
        "/api/v2/services/",
        json={
            "id": created["id"],
            "type": "external_kafka",
            "name": "Partner Kafka",
            "config": {"bootstrapServers": "kafka.partner:9094"},
        },
    )
    assert updated_resp.status_code == 200, updated_resp.text
    updated = updated_resp.json()
    assert updated["revision"] == 2
    assert updated["config"]["bootstrapServers"] == "kafka.partner:9094"

    stored = fake_db.services_v2.docs[0]
    assert stored["revision"] == 2
    assert len(stored["revisions"]) == 1
    assert stored["revisions"][0]["revision"] == 1
    assert stored["revisions"][0]["config"]["bootstrapServers"] == "kafka.partner:9093"

    events = [e["action"] for e in fake_db.audit_v2.docs]
    assert events == ["Service created", "Service revision created"]
    revise_event = fake_db.audit_v2.docs[1]
    assert revise_event["target"] == "Partner Kafka (rev 2)"
    assert revise_event["details"] == "Linked flows adopt at next deploy"


def test_blank_secret_keeps_existing_value_and_is_redacted():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    created = client.post(
        "/api/v2/services/",
        json={
            "type": "http",
            "name": "Rapid7",
            "config": {"baseUrl": "https://rapid7.example", "authMode": "basic", "username": "svc", "password": "s3cret1"},
        },
    ).json()
    assert created["config"]["password"] is None
    assert created["hasPassword"] is True

    # Update without touching the password -- it must be preserved server-side.
    updated = client.post(
        "/api/v2/services/",
        json={
            "id": created["id"],
            "type": "http",
            "name": "Rapid7",
            "config": {"baseUrl": "https://rapid7.example/v2", "authMode": "basic", "username": "svc", "password": ""},
        },
    ).json()
    assert updated["config"]["password"] is None
    assert updated["hasPassword"] is True
    assert updated["config"]["baseUrl"] == "https://rapid7.example/v2"

    stored = fake_db.services_v2.docs[0]
    assert stored["config"]["password"] == "s3cret1"

    # Revision history snapshot must also be redacted -- no plaintext secret leaks.
    assert stored["revisions"][0]["config"]["password"] is None
    assert stored["revisions"][0]["hasPassword"] is True


def test_list_services_returns_redacted_config():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    client.post(
        "/api/v2/services/",
        json={
            "type": "database",
            "name": "Asset DB",
            "config": {
                "dialect": "postgresql",
                "host": "db.internal",
                "database": "assets",
                "username": "svc",
                "password": "hunter2",
                "capabilities": ["read"],
            },
        },
    )
    resp = client.get("/api/v2/services/")
    assert resp.status_code == 200
    items = resp.json()
    assert len(items) == 1
    assert items[0]["config"]["password"] is None
    assert items[0]["hasPassword"] is True
    assert items[0]["config"]["host"] == "db.internal"


def test_private_flag_passes_through():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    resp = client.post(
        "/api/v2/services/",
        json={
            "type": "database",
            "name": "Inline DB",
            "private": True,
            "config": {"dialect": "postgresql", "host": "h", "database": "d", "capabilities": ["read"]},
        },
    )
    assert resp.status_code == 200
    assert resp.json()["private"] is True


# ------------------------------------------------------------- validation


@pytest.mark.parametrize(
    "payload,expected_fragment",
    [
        ({"type": "http", "name": "", "config": {"baseUrl": "https://x"}}, "Name the service"),
        ({"type": "http", "name": "X", "config": {}}, "Base URL is required"),
        (
            {"type": "http", "name": "X", "config": {"baseUrl": "https://x", "authMode": "basic", "username": ""}},
            "Username is required for basic auth",
        ),
        (
            {"type": "http", "name": "X", "config": {"baseUrl": "https://x", "authMode": "bearer"}},
            "Token is required for bearer auth",
        ),
        (
            {"type": "database", "name": "X", "config": {"host": "h", "database": "", "capabilities": ["read"]}},
            "Host and database are required",
        ),
        (
            {"type": "database", "name": "X", "config": {"host": "h", "database": "d", "capabilities": []}},
            "Select at least one capability",
        ),
        ({"type": "external_kafka", "name": "X", "config": {}}, "Bootstrap servers are required"),
        (
            {"type": "sink_destination", "name": "X", "config": {"kind": "opensearch"}},
            "OpenSearch URL is required",
        ),
        (
            {"type": "sink_destination", "name": "X", "config": {"kind": "iceberg_catalog"}},
            "Catalog URL is required",
        ),
        ({"type": None, "name": "X", "config": {}}, "Pick a service type first"),
    ],
)
def test_save_service_validation_errors(payload, expected_fragment):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    resp = client.post("/api/v2/services/", json=payload)
    assert resp.status_code == 422, resp.text
    assert expected_fragment in resp.json()["detail"]
    assert fake_db.services_v2.docs == []


# ------------------------------------------------------- retire / reinstate


def test_retire_and_reinstate_lifecycle():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    created = client.post(
        "/api/v2/services/",
        json={"type": "external_kafka", "name": "Partner Kafka", "config": {"bootstrapServers": "k:9093"}},
    ).json()

    retired = client.post(f"/api/v2/services/{created['id']}/retire")
    assert retired.status_code == 200, retired.text
    assert retired.json()["retired"] is True
    retire_event = fake_db.audit_v2.docs[-1]
    assert retire_event["action"] == "Service retired"
    assert retire_event["status"] == "Warning"
    assert retire_event["details"] is None  # no dependent flows

    reinstated = client.post(f"/api/v2/services/{created['id']}/reinstate")
    assert reinstated.status_code == 200, reinstated.text
    assert reinstated.json()["retired"] is False
    reinstate_event = fake_db.audit_v2.docs[-1]
    assert reinstate_event["action"] == "Service reinstated"
    assert reinstate_event["status"] == "Success"


def test_retire_flags_dependent_flows_in_audit_details():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    created = client.post(
        "/api/v2/services/",
        json={"type": "http", "name": "Rapid7", "config": {"baseUrl": "https://x", "authMode": "none"}},
    ).json()
    fake_db.flows_v2.docs.append(
        {"id": "flow-1", "name": "Vuln Ingest", "blocks": [{"id": "b1", "serviceId": created["id"], "config": {}}]}
    )

    retired = client.post(f"/api/v2/services/{created['id']}/retire")
    assert retired.status_code == 200
    retire_event = fake_db.audit_v2.docs[-1]
    assert retire_event["details"] == "1 dependent flow(s) flagged: action required"


def test_retire_unknown_service_404():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    resp = client.post("/api/v2/services/does-not-exist/retire")
    assert resp.status_code == 404


# ------------------------------------------------------------------ test: http


def test_http_test_none_auth_healthy(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    created = client.post(
        "/api/v2/services/",
        json={"type": "http", "name": "X", "config": {"baseUrl": "https://x.example", "authMode": "none"}},
    ).json()
    patch_httpx(monkeypatch, {"HEAD": FakeResponse(200)})

    resp = client.post(f"/api/v2/services/{created['id']}/test")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["health"] == "Healthy"

    event = fake_db.audit_v2.docs[-1]
    assert event["action"] == "Service tested"
    assert event["status"] == "Success"


def test_http_test_401_none_auth_is_healthy_reachable(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    created = client.post(
        "/api/v2/services/",
        json={"type": "http", "name": "X", "config": {"baseUrl": "https://x.example", "authMode": "none"}},
    ).json()
    calls = patch_httpx(monkeypatch, {"HEAD": FakeResponse(405), "GET": FakeResponse(401)})

    resp = client.post(f"/api/v2/services/{created['id']}/test")
    body = resp.json()
    assert body["health"] == "Healthy"
    assert [c[0] for c in calls] == ["HEAD", "GET"]  # HEAD fell back to GET


def test_http_test_basic_auth_401_is_failed(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    created = client.post(
        "/api/v2/services/",
        json={
            "type": "http",
            "name": "X",
            "config": {"baseUrl": "https://x.example", "authMode": "basic", "username": "u", "password": "p"},
        },
    ).json()
    patch_httpx(monkeypatch, {"HEAD": FakeResponse(401)})

    resp = client.post(f"/api/v2/services/{created['id']}/test")
    body = resp.json()
    assert body["health"] == "Failed"
    event = fake_db.audit_v2.docs[-1]
    assert event["action"] == "Service test failed"
    assert event["status"] == "Failed"


def test_http_test_bearer_auth_sends_header(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    created = client.post(
        "/api/v2/services/",
        json={"type": "http", "name": "X", "config": {"baseUrl": "https://x.example", "authMode": "bearer", "token": "tok123"}},
    ).json()
    calls = patch_httpx(monkeypatch, {"HEAD": FakeResponse(200)})

    resp = client.post(f"/api/v2/services/{created['id']}/test")
    assert resp.json()["health"] == "Healthy"
    method, url, kw = calls[0]
    assert kw["headers"]["Authorization"] == "Bearer tok123"


def test_http_test_api_key_query_location(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    created = client.post(
        "/api/v2/services/",
        json={
            "type": "http",
            "name": "X",
            "config": {
                "baseUrl": "https://x.example",
                "authMode": "api_key",
                "keyName": "api_key",
                "keyLocation": "query",
                "keyValue": "abc123",
            },
        },
    ).json()
    calls = patch_httpx(monkeypatch, {"HEAD": FakeResponse(200)})

    resp = client.post(f"/api/v2/services/{created['id']}/test")
    assert resp.json()["health"] == "Healthy"
    method, url, kw = calls[0]
    assert kw["params"] == {"api_key": "abc123"}
    assert kw["headers"] == {}


def test_http_test_session_token_login_flow(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    created = client.post(
        "/api/v2/services/",
        json={
            "type": "http",
            "name": "SessionSvc",
            "config": {
                "baseUrl": "https://x.example",
                "authMode": "session_token",
                "loginPath": "/rest/login",
                "tokenPath": "$.sessionToken",
                "tokenHeader": "X-Auth-Token",
            },
        },
    ).json()
    calls = patch_httpx(
        monkeypatch,
        {
            "POST": FakeResponse(200, {"sessionToken": "tok-abc"}),
            "GET": FakeResponse(200),
        },
    )

    resp = client.post(f"/api/v2/services/{created['id']}/test")
    body = resp.json()
    assert body["health"] == "Healthy", body

    assert len(calls) == 2
    login_method, login_url, login_kw = calls[0]
    assert login_method == "POST"
    assert login_url == "https://x.example/rest/login"

    follow_method, follow_url, follow_kw = calls[1]
    assert follow_method == "GET"
    assert follow_kw["headers"]["X-Auth-Token"] == "tok-abc"


def test_http_test_session_token_bad_jsonpath_fails(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    created = client.post(
        "/api/v2/services/",
        json={
            "type": "http",
            "name": "SessionSvc",
            "config": {
                "baseUrl": "https://x.example",
                "authMode": "session_token",
                "loginPath": "/rest/login",
                "tokenPath": "$.missing.path",
                "tokenHeader": "X-Auth-Token",
            },
        },
    ).json()
    patch_httpx(monkeypatch, {"POST": FakeResponse(200, {"sessionToken": "tok-abc"})})

    resp = client.post(f"/api/v2/services/{created['id']}/test")
    body = resp.json()
    assert body["health"] == "Failed"
    assert fake_db.audit_v2.docs[-1]["status"] == "Failed"
    assert "JSONPath" in fake_db.audit_v2.docs[-1]["details"]


def test_http_test_oauth2_flow(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    created = client.post(
        "/api/v2/services/",
        json={
            "type": "http",
            "name": "OAuthSvc",
            "config": {
                "baseUrl": "https://x.example",
                "authMode": "oauth2",
                "tokenUrl": "https://auth.example/token",
                "clientId": "cid",
                "clientSecret": "csecret",
            },
        },
    ).json()
    calls = patch_httpx(
        monkeypatch,
        {
            "POST": FakeResponse(200, {"access_token": "atok"}),
            "GET": FakeResponse(200),
        },
    )

    resp = client.post(f"/api/v2/services/{created['id']}/test")
    assert resp.json()["health"] == "Healthy"

    token_method, token_url, token_kw = calls[0]
    assert token_url == "https://auth.example/token"
    assert token_kw["data"]["client_id"] == "cid"
    assert token_kw["data"]["client_secret"] == "csecret"

    follow_method, follow_url, follow_kw = calls[1]
    assert follow_kw["headers"]["Authorization"] == "Bearer atok"


def test_http_test_connection_error_is_failed(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    created = client.post(
        "/api/v2/services/",
        json={"type": "http", "name": "X", "config": {"baseUrl": "https://unreachable.example", "authMode": "none"}},
    ).json()
    patch_httpx(monkeypatch, {"HEAD": httpx.ConnectError("boom")})

    resp = client.post(f"/api/v2/services/{created['id']}/test")
    assert resp.json()["health"] == "Failed"


# -------------------------------------------------------------- test: database


def test_database_test_trino_uses_http_info_endpoint(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    created = client.post(
        "/api/v2/services/",
        json={
            "type": "database",
            "name": "TrinoDB",
            "config": {"dialect": "trino", "host": "trino.internal", "port": 8080, "database": "hive", "capabilities": ["read"]},
        },
    ).json()
    calls = patch_httpx(monkeypatch, {"GET": FakeResponse(200)})

    resp = client.post(f"/api/v2/services/{created['id']}/test")
    assert resp.json()["health"] == "Healthy"
    method, url, kw = calls[0]
    assert url == "http://trino.internal:8080/v1/info"


def test_database_test_postgresql_tcp_probe_success(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    created = client.post(
        "/api/v2/services/",
        json={
            "type": "database",
            "name": "PgDB",
            "config": {"dialect": "postgresql", "host": "db.internal", "port": 5432, "database": "assets", "capabilities": ["read"]},
        },
    ).json()
    patch_tcp(monkeypatch, succeed=True)

    resp = client.post(f"/api/v2/services/{created['id']}/test")
    body = resp.json()
    assert body["health"] == "Healthy"


def test_database_test_mysql_tcp_probe_failure(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    created = client.post(
        "/api/v2/services/",
        json={
            "type": "database",
            "name": "MyDB",
            "config": {"dialect": "mysql", "host": "db.internal", "port": 3306, "database": "assets", "capabilities": ["read"]},
        },
    ).json()
    patch_tcp(monkeypatch, succeed=False)

    resp = client.post(f"/api/v2/services/{created['id']}/test")
    body = resp.json()
    assert body["health"] == "Failed"


# ---------------------------------------------------------- test: external_kafka


def test_external_kafka_test_tcp_probe(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    created = client.post(
        "/api/v2/services/",
        json={"type": "external_kafka", "name": "PartnerKafka", "config": {"bootstrapServers": "kafka.partner:9093,kafka2.partner:9093"}},
    ).json()
    patch_tcp(monkeypatch, succeed=True)

    resp = client.post(f"/api/v2/services/{created['id']}/test")
    assert resp.json()["health"] == "Healthy"


def test_external_kafka_test_tcp_probe_failure_message(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    created = client.post(
        "/api/v2/services/",
        json={"type": "external_kafka", "name": "PartnerKafka", "config": {"bootstrapServers": "kafka.partner:9093"}},
    ).json()
    patch_tcp(monkeypatch, succeed=False)

    resp = client.post(f"/api/v2/services/{created['id']}/test")
    assert resp.json()["health"] == "Failed"
    assert "cluster-internal" in fake_db.audit_v2.docs[-1]["details"]


# ------------------------------------------------------------ test: sink_destination


def test_sink_opensearch_test_accepts_401(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    created = client.post(
        "/api/v2/services/",
        json={
            "type": "sink_destination",
            "name": "OS Sink",
            "config": {"kind": "opensearch", "url": "https://os.internal:9200", "indexPrefix": "dmp-", "writeMode": "upsert"},
        },
    ).json()
    patch_httpx(monkeypatch, {"GET": FakeResponse(401)})

    resp = client.post(f"/api/v2/services/{created['id']}/test")
    assert resp.json()["health"] == "Healthy"


def test_sink_iceberg_test_monkeypatched(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    created = client.post(
        "/api/v2/services/",
        json={
            "type": "sink_destination",
            "name": "Iceberg Sink",
            "config": {
                "kind": "iceberg_catalog",
                "catalogUrl": "http://polaris.corp:8181/api/catalog",
                "warehouse": "bronze",
                "oauthClientId": "cid",
                "oauthClientSecret": "csecret",
                "s3Endpoint": "http://minio.corp:9000",
                "s3AccessKey": "AKIA...",
                "s3SecretKey": "s3secret",
                "s3Region": "us-east-1",
                "s3PathStyle": True,
            },
        },
    ).json()

    # T3.3 extended iceberg secrets must be redacted on save.
    assert created["config"]["oauthClientSecret"] is None
    assert created["hasOauthClientSecret"] is True
    assert created["config"]["s3AccessKey"] is None
    assert created["hasS3AccessKey"] is True
    assert created["config"]["s3SecretKey"] is None
    assert created["hasS3SecretKey"] is True
    assert created["config"]["s3Endpoint"] == "http://minio.corp:9000"

    async def fake_test_iceberg_connection(conn):
        assert conn["endpoint"] == "http://polaris.corp:8181/api/catalog"
        assert conn["iceberg_credential"] == "cid:csecret"
        return {"ok": True, "message": "Iceberg catalog reachable."}

    monkeypatch.setattr(services_router, "test_iceberg_connection", fake_test_iceberg_connection)

    resp = client.post(f"/api/v2/services/{created['id']}/test")
    assert resp.status_code == 200, resp.text
    assert resp.json()["health"] == "Healthy"


def test_sink_iceberg_s3_access_key_keeps_existing_value_on_blank_update():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    created = client.post(
        "/api/v2/services/",
        json={
            "type": "sink_destination",
            "name": "Iceberg Sink",
            "config": {
                "kind": "iceberg_catalog",
                "catalogUrl": "http://polaris.corp:8181/api/catalog",
                "warehouse": "bronze",
                "oauthClientId": "cid",
                "s3Endpoint": "http://minio.corp:9000",
                "s3AccessKey": "AKIA-ORIGINAL",
                "s3Region": "us-east-1",
                "s3PathStyle": True,
            },
        },
    ).json()
    assert created["config"]["s3AccessKey"] is None
    assert created["hasS3AccessKey"] is True

    updated = client.post(
        "/api/v2/services/",
        json={
            "id": created["id"],
            "type": "sink_destination",
            "name": "Iceberg Sink",
            "config": {
                "kind": "iceberg_catalog",
                "catalogUrl": "http://polaris.corp:8181/api/catalog/v2",
                "warehouse": "bronze",
                "oauthClientId": "cid",
                "s3Endpoint": "http://minio.corp:9000",
                "s3AccessKey": "",
                "s3Region": "us-east-1",
                "s3PathStyle": True,
            },
        },
    ).json()
    assert updated["config"]["s3AccessKey"] is None
    assert updated["hasS3AccessKey"] is True
    assert fake_db.services_v2.docs[0]["config"]["s3AccessKey"] == "AKIA-ORIGINAL"

    replaced = client.post(
        "/api/v2/services/",
        json={
            "id": created["id"],
            "type": "sink_destination",
            "name": "Iceberg Sink",
            "config": {
                "kind": "iceberg_catalog",
                "catalogUrl": "http://polaris.corp:8181/api/catalog/v3",
                "warehouse": "bronze",
                "oauthClientId": "cid",
                "s3Endpoint": "http://minio.corp:9000",
                "s3AccessKey": "AKIA-REPLACED",
                "s3Region": "us-east-1",
                "s3PathStyle": True,
            },
        },
    ).json()
    assert replaced["config"]["s3AccessKey"] is None
    assert replaced["hasS3AccessKey"] is True
    assert fake_db.services_v2.docs[0]["config"]["s3AccessKey"] == "AKIA-REPLACED"


def test_sink_iceberg_test_failure_monkeypatched(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    created = client.post(
        "/api/v2/services/",
        json={
            "type": "sink_destination",
            "name": "Iceberg Sink",
            "config": {"kind": "iceberg_catalog", "catalogUrl": "http://polaris.corp:8181/api/catalog", "warehouse": "bronze"},
        },
    ).json()

    async def fake_test_iceberg_connection(conn):
        return {"ok": False, "error": "Cannot connect to Iceberg catalog."}

    monkeypatch.setattr(services_router, "test_iceberg_connection", fake_test_iceberg_connection)

    resp = client.post(f"/api/v2/services/{created['id']}/test")
    assert resp.json()["health"] == "Failed"
    assert fake_db.audit_v2.docs[-1]["status"] == "Failed"


def test_test_unknown_service_404():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    resp = client.post("/api/v2/services/does-not-exist/test")
    assert resp.status_code == 404
