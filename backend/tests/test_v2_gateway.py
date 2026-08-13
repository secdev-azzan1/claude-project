"""Tests for the v2 APISIX Gateway router (backend/routers/v2/gateway.py).

Uses FastAPI's TestClient against a small standalone app that mounts only
`gateway.router` (this task is not permitted to edit server.py, so the
suite does not depend on it being wired in there), with the Mongo
dependency (`db.get_db`) overridden by a hand-rolled in-memory FakeDB. The
real `services.apisix_client` put_upstream/put_route functions and
`httpx.AsyncClient` (used by the runtime `/test` probe) are monkeypatched
per test so nothing here ever touches a network.
"""
from __future__ import annotations

import copy
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from db import get_db
from routers.v2 import gateway as gw


# --------------------------------------------------------------- fake mongo


def _get_nested(document: Dict[str, Any], dotted_key: str) -> Any:
    value: Any = document
    for part in dotted_key.split("."):
        if isinstance(value, dict):
            value = value.get(part)
        else:
            return None
    return value


def _matches(document: Dict[str, Any], query: Dict[str, Any]) -> bool:
    for key, expected in (query or {}).items():
        if _get_nested(document, key) != expected:
            return False
    return True


class _Result:
    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)


class FakeCursor:
    def __init__(self, docs: List[Dict[str, Any]]):
        self._docs = docs
        self._iter = None

    def __aiter__(self):
        self._iter = iter(self._docs)
        return self

    async def __anext__(self):
        try:
            doc = next(self._iter)
        except StopIteration:
            raise StopAsyncIteration
        return copy.deepcopy(doc)

    async def to_list(self, length: Optional[int] = None):
        docs = self._docs if length is None else self._docs[:length]
        return [copy.deepcopy(d) for d in docs]


class FakeCollection:
    def __init__(self, docs: Optional[List[Dict[str, Any]]] = None):
        self.docs: List[Dict[str, Any]] = [copy.deepcopy(d) for d in (docs or [])]

    async def find_one(self, query: Optional[Dict[str, Any]] = None, projection=None):
        for d in self.docs:
            if _matches(d, query or {}):
                return copy.deepcopy(d)
        return None

    def find(self, query: Optional[Dict[str, Any]] = None, projection: Optional[Dict[str, Any]] = None):
        return FakeCursor([d for d in self.docs if _matches(d, query or {})])

    async def insert_one(self, document: Dict[str, Any]):
        self.docs.append(copy.deepcopy(document))
        return _Result(inserted_id=document.get("_id") or document.get("id"))

    async def replace_one(self, query: Dict[str, Any], replacement: Dict[str, Any], upsert: bool = False):
        for i, d in enumerate(self.docs):
            if _matches(d, query):
                self.docs[i] = copy.deepcopy(replacement)
                return _Result(matched_count=1, modified_count=1)
        if upsert:
            self.docs.append(copy.deepcopy(replacement))
            return _Result(matched_count=0, modified_count=0, upserted_id=replacement.get("_id"))
        return _Result(matched_count=0, modified_count=0)

    async def delete_one(self, query: Dict[str, Any]):
        before = len(self.docs)
        self.docs = [d for d in self.docs if not _matches(d, query)]
        return _Result(deleted_count=before - len(self.docs))


class FakeDB:
    def __init__(self):
        self.gateway_v2 = FakeCollection()
        self.connections_v2 = FakeCollection()
        self.services_v2 = FakeCollection()
        self.flows_v2 = FakeCollection()
        self.audit_v2 = FakeCollection()

    def __getitem__(self, name: str) -> FakeCollection:
        return getattr(self, name)


def _make_client(fake_db: FakeDB) -> TestClient:
    app = FastAPI()
    app.include_router(gw.router)
    app.dependency_overrides[get_db] = lambda: fake_db
    return TestClient(app)


# ------------------------------------------------------------------ helpers


def _proxy_payload(**overrides: Any) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "name": "Rapid7 Egress",
        "description": "Asset API",
        "targetHost": "api.rapid7.example.corp",
        "port": 443,
        "sni": None,
        "connectTimeoutMs": 5000,
        "readTimeoutMs": 30000,
        "path": "/rest/v1/assets",
        "methods": ["GET", "POST"],
        "certProfileId": None,
    }
    payload.update(overrides)
    return payload


def _install_apisix_puts(monkeypatch, *, ok: bool = True, error: str = "boom"):
    """Monkeypatch apisix_client.put_upstream/put_route to succeed (or fail)
    and record every call as (kind, obj_id, body)."""
    calls: List[Dict[str, Any]] = []

    async def fake_put_upstream(admin_url, admin_key, upstream_id, body):
        calls.append({"kind": "upstream", "id": upstream_id, "body": body, "admin_url": admin_url, "admin_key": admin_key})
        if ok:
            return {"ok": True, "status_code": 200}
        return {"ok": False, "status_code": 400, "error": error}

    async def fake_put_route(admin_url, admin_key, route_id, body):
        calls.append({"kind": "route", "id": route_id, "body": body, "admin_url": admin_url, "admin_key": admin_key})
        if ok:
            return {"ok": True, "status_code": 200}
        return {"ok": False, "status_code": 400, "error": error}

    monkeypatch.setattr(gw.apisix_client, "put_upstream", fake_put_upstream)
    monkeypatch.setattr(gw.apisix_client, "put_route", fake_put_route)
    return calls


def _apisix_connection_doc(**overrides) -> Dict[str, Any]:
    doc = {
        "id": "conn-apisix-1",
        "type": "apisix",
        "active": True,
        "config": {
            "adminUrl": "https://apisix-admin.internal:9180",
            "adminKey": "s3cr3t-admin-key",
            "runtimeUrl": "https://gateway.internal",
        },
    }
    doc.update(overrides)
    return doc


class FakeHttpClient:
    """Stands in for httpx.AsyncClient for the runtime `/test` probe. Only
    implements the subset gateway.py's test_proxy handler actually calls:
    async context manager + `.get(url)`."""

    response_factory = None
    captured_urls: List[str] = []
    captured_kwargs: List[Dict[str, Any]] = []

    def __init__(self, *args, **kwargs):
        self.init_kwargs = kwargs

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def get(self, url):
        FakeHttpClient.captured_urls.append(url)
        result = FakeHttpClient.response_factory(url)
        if isinstance(result, Exception):
            raise result
        return result


def _install_http_get(monkeypatch, factory):
    FakeHttpClient.response_factory = factory
    FakeHttpClient.captured_urls = []
    monkeypatch.setattr(gw.httpx, "AsyncClient", FakeHttpClient)


# =========================================================================
# GET / — full gateway state
# =========================================================================


def test_get_empty_state_creates_default_doc():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    resp = client.get("/api/v2/gateway/")
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"proxies": [], "certProfiles": [], "allowlist": []}
    # The default document is persisted so a second read is stable.
    assert len(fake_db.gateway_v2.docs) == 1
    assert fake_db.gateway_v2.docs[0]["_id"] == "gateway"


# =========================================================================
# Proxy CRUD + validation
# =========================================================================


def test_create_proxy_starts_pending_and_is_audited():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    resp = client.post("/api/v2/gateway/proxies", json=_proxy_payload())
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["status"] == "Pending"
    assert "not yet reconciled" in body["statusDetail"]
    assert body["id"]
    assert body["createdAt"] and body["updatedAt"]

    state = client.get("/api/v2/gateway/").json()
    assert len(state["proxies"]) == 1
    assert state["proxies"][0]["name"] == "Rapid7 Egress"

    assert len(fake_db.audit_v2.docs) == 1
    assert fake_db.audit_v2.docs[0]["action"] == "Gateway proxy created"
    assert fake_db.audit_v2.docs[0]["object"] == "Gateway"


@pytest.mark.parametrize(
    "overrides, expected_snippet",
    [
        ({"name": ""}, "Name the proxy"),
        ({"targetHost": ""}, "Set the target host"),
        ({"targetHost": "https://api.example.corp"}, "drop the scheme"),
        ({"targetHost": "api.example.corp/path"}, "path field"),
        ({"targetHost": "api.example.corp:8080"}, "port field"),
        ({"targetHost": "not a host!"}, "not a valid hostname"),
        ({"port": 0}, "Port must be between"),
        ({"port": 70000}, "Port must be between"),
        ({"path": ""}, "Set the route path prefix"),
        ({"path": "no-leading-slash"}, "must start with"),
        ({"methods": []}, "Allow at least one HTTP method"),
        ({"connectTimeoutMs": -5}, "positive number of milliseconds"),
        ({"readTimeoutMs": 0}, "positive number of milliseconds"),
    ],
)
def test_save_proxy_validation_failures(overrides, expected_snippet):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    resp = client.post("/api/v2/gateway/proxies", json=_proxy_payload(**overrides))
    assert resp.status_code == 422, resp.text
    assert expected_snippet in resp.json()["detail"]
    assert fake_db.gateway_v2.docs == [] or fake_db.gateway_v2.docs[0]["proxies"] == []


def test_save_proxy_duplicate_name_conflict():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    client.post("/api/v2/gateway/proxies", json=_proxy_payload(name="Dup"))
    resp = client.post("/api/v2/gateway/proxies", json=_proxy_payload(name="Dup"))
    assert resp.status_code == 409, resp.text
    assert "already called" in resp.json()["detail"]


def test_update_proxy_config_change_resets_to_pending():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    created = client.post("/api/v2/gateway/proxies", json=_proxy_payload()).json()

    # Simulate a prior successful reconcile so we can observe the reset.
    doc = fake_db.gateway_v2.docs[0]
    doc["proxies"][0]["status"] = "Reconciled"
    doc["proxies"][0]["statusDetail"] = None
    doc["allowlist"] = [created["targetHost"]]

    updated_payload = _proxy_payload(id=created["id"], path="/rest/v2/assets")
    resp = client.post("/api/v2/gateway/proxies", json=updated_payload)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "Pending"
    assert "Configuration changed" in body["statusDetail"]
    assert body["id"] == created["id"]
    assert body["createdAt"] == created["createdAt"]


def test_update_proxy_without_config_change_keeps_status():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    created = client.post("/api/v2/gateway/proxies", json=_proxy_payload()).json()
    doc = fake_db.gateway_v2.docs[0]
    doc["proxies"][0]["status"] = "Reconciled"
    doc["proxies"][0]["statusDetail"] = None

    same_payload = _proxy_payload(id=created["id"], description="Updated description only")
    resp = client.post("/api/v2/gateway/proxies", json=same_payload)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "Reconciled"
    assert body["statusDetail"] is None


def test_delete_proxy_blocked_when_flow_references_it_via_service():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    proxy = client.post("/api/v2/gateway/proxies", json=_proxy_payload()).json()

    fake_db.services_v2.docs.append(
        {"id": "svc-1", "type": "http", "name": "Rapid7 Service", "config": {"proxyId": proxy["id"]}}
    )
    fake_db.flows_v2.docs.append(
        {
            "id": "flow-1",
            "name": "Rapid7 Ingest",
            "blocks": [{"id": "b1", "adapter": "http", "serviceId": "svc-1", "config": {}}],
        }
    )

    resp = client.delete(f"/api/v2/gateway/proxies/{proxy['id']}")
    assert resp.status_code == 409, resp.text
    assert "Rapid7 Ingest" in resp.json()["detail"]

    # Nothing removed.
    assert len(fake_db.gateway_v2.docs[0]["proxies"]) == 1


def test_delete_proxy_succeeds_with_no_dependents():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    proxy = client.post("/api/v2/gateway/proxies", json=_proxy_payload()).json()

    resp = client.delete(f"/api/v2/gateway/proxies/{proxy['id']}")
    assert resp.status_code == 200, resp.text
    assert fake_db.gateway_v2.docs[0]["proxies"] == []
    assert any(a["action"] == "Gateway proxy deleted" for a in fake_db.audit_v2.docs)


def test_delete_proxy_not_found():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    resp = client.delete("/api/v2/gateway/proxies/does-not-exist")
    assert resp.status_code == 404


# =========================================================================
# E4 — delete tears down live APISIX objects when the proxy was reconciled
# =========================================================================


def _install_apisix_deletes(monkeypatch, *, ok: bool = True, error: str = "boom"):
    """Monkeypatch apisix_client.delete_route/delete_upstream, recording
    every call as {"kind", "id", ...} in call order."""
    calls: List[Dict[str, Any]] = []

    async def fake_delete_route(admin_url, admin_key, route_id):
        calls.append({"kind": "route", "id": route_id, "admin_url": admin_url, "admin_key": admin_key})
        if ok:
            return {"ok": True, "status_code": 200}
        return {"ok": False, "status_code": 400, "error": error}

    async def fake_delete_upstream(admin_url, admin_key, upstream_id):
        calls.append({"kind": "upstream", "id": upstream_id, "admin_url": admin_url, "admin_key": admin_key})
        if ok:
            return {"ok": True, "status_code": 200}
        return {"ok": False, "status_code": 400, "error": error}

    monkeypatch.setattr(gw.apisix_client, "delete_route", fake_delete_route)
    monkeypatch.setattr(gw.apisix_client, "delete_upstream", fake_delete_upstream)
    return calls


def test_delete_proxy_after_reconcile_cleans_up_live_apisix_objects(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    proxy = client.post("/api/v2/gateway/proxies", json=_proxy_payload()).json()
    fake_db.connections_v2.docs.append(_apisix_connection_doc())
    fake_db.gateway_v2.docs[0]["allowlist"] = [proxy["targetHost"]]

    _install_apisix_puts(monkeypatch, ok=True)
    reconcile_resp = client.post(f"/api/v2/gateway/proxies/{proxy['id']}/reconcile")
    assert reconcile_resp.status_code == 200, reconcile_resp.text
    assert reconcile_resp.json()["status"] == "Reconciled"

    delete_calls = _install_apisix_deletes(monkeypatch, ok=True)
    resp = client.delete(f"/api/v2/gateway/proxies/{proxy['id']}")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["apisixCleaned"] is True
    assert fake_db.gateway_v2.docs[0]["proxies"] == []

    # Routes before the upstream — an upstream still referenced by a live
    # route is the wrong order to delete in.
    ids_in_order = [c["id"] for c in delete_calls]
    assert ids_in_order == [f"dmp_{proxy['id']}_root", f"dmp_{proxy['id']}_wild", f"dmp_{proxy['id']}"]
    for c in delete_calls:
        assert c["admin_url"] == "https://apisix-admin.internal:9180"
        assert c["admin_key"] == "s3cr3t-admin-key"

    deleted_event = next(a for a in fake_db.audit_v2.docs if a["action"] == "Gateway proxy deleted")
    assert "removed" in (deleted_event.get("details") or "").lower()


def test_delete_proxy_never_reconciled_skips_apisix_cleanup(monkeypatch):
    """A proxy that never made it past "Pending" pushed nothing live —
    delete must not call APISIX at all for it."""
    fake_db = FakeDB()
    client = _make_client(fake_db)
    proxy = client.post("/api/v2/gateway/proxies", json=_proxy_payload()).json()

    delete_calls = _install_apisix_deletes(monkeypatch, ok=True)
    resp = client.delete(f"/api/v2/gateway/proxies/{proxy['id']}")
    assert resp.status_code == 200, resp.text
    assert resp.json()["apisixCleaned"] is False
    assert delete_calls == []


def test_delete_proxy_apisix_cleanup_failure_is_logged_not_swallowed(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    proxy = client.post("/api/v2/gateway/proxies", json=_proxy_payload()).json()
    fake_db.connections_v2.docs.append(_apisix_connection_doc())
    fake_db.gateway_v2.docs[0]["allowlist"] = [proxy["targetHost"]]
    _install_apisix_puts(monkeypatch, ok=True)
    client.post(f"/api/v2/gateway/proxies/{proxy['id']}/reconcile")

    _install_apisix_deletes(monkeypatch, ok=False, error="route delete boom")
    resp = client.delete(f"/api/v2/gateway/proxies/{proxy['id']}")
    # The platform-side delete still succeeds (best-effort cleanup) --
    # but the failure must be visible, not swallowed.
    assert resp.status_code == 200, resp.text
    assert resp.json()["apisixCleaned"] is False

    deleted_event = next(a for a in fake_db.audit_v2.docs if a["action"] == "Gateway proxy deleted")
    details = deleted_event.get("details") or ""
    assert "incomplete" in details.lower()
    assert "route delete boom" in details


def test_delete_proxy_reconciled_but_no_active_apisix_connection_best_effort(monkeypatch):
    """The APISIX connection was retired after reconcile — delete must not
    hard-fail; it records that cleanup could not be attempted."""
    fake_db = FakeDB()
    client = _make_client(fake_db)
    proxy = client.post("/api/v2/gateway/proxies", json=_proxy_payload()).json()
    fake_db.connections_v2.docs.append(_apisix_connection_doc())
    fake_db.gateway_v2.docs[0]["allowlist"] = [proxy["targetHost"]]
    _install_apisix_puts(monkeypatch, ok=True)
    client.post(f"/api/v2/gateway/proxies/{proxy['id']}/reconcile")

    # Connection retired before delete.
    fake_db.connections_v2.docs[0]["active"] = False

    delete_calls = _install_apisix_deletes(monkeypatch, ok=True)
    resp = client.delete(f"/api/v2/gateway/proxies/{proxy['id']}")
    assert resp.status_code == 200, resp.text
    assert resp.json()["apisixCleaned"] is False
    assert delete_calls == []

    deleted_event = next(a for a in fake_db.audit_v2.docs if a["action"] == "Gateway proxy deleted")
    assert "no active apisix connection" in (deleted_event.get("details") or "").lower()


# =========================================================================
# Reconcile
# =========================================================================


def test_reconcile_happy_path_builds_expected_apisix_bodies(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)

    cert = client.post(
        "/api/v2/gateway/cert-profiles",
        json={"name": "prod-client-cert", "subject": "CN=api-client.example.corp", "certPem": "-- CERT --", "keyPem": "-- KEY --"},
    ).json()

    proxy = client.post(
        "/api/v2/gateway/proxies",
        json=_proxy_payload(certProfileId=cert["id"]),
    ).json()

    fake_db.connections_v2.docs.append(_apisix_connection_doc())
    fake_db.gateway_v2.docs[0]["allowlist"] = [proxy["targetHost"]]

    calls = _install_apisix_puts(monkeypatch, ok=True)

    resp = client.post(f"/api/v2/gateway/proxies/{proxy['id']}/reconcile")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "Reconciled"
    assert body["statusDetail"] is None

    assert len(calls) == 3
    upstream_call = next(c for c in calls if c["kind"] == "upstream")
    root_call = next(c for c in calls if c["id"] == f"dmp_{proxy['id']}_root")
    wild_call = next(c for c in calls if c["id"] == f"dmp_{proxy['id']}_wild")

    assert upstream_call["id"] == f"dmp_{proxy['id']}"
    assert upstream_call["admin_url"] == "https://apisix-admin.internal:9180"
    assert upstream_call["admin_key"] == "s3cr3t-admin-key"
    assert upstream_call["body"] == {
        "type": "roundrobin",
        "nodes": {"api.rapid7.example.corp:443": 1},
        "scheme": "https",
        "pass_host": "node",
        "timeout": {"connect": 5.0, "send": 30.0, "read": 30.0},
        "tls": {"client_cert": "-- CERT --", "client_key": "-- KEY --"},
    }

    token = "rapid7_egress"
    assert root_call["body"] == {
        "uri": f"/{token}",
        "methods": ["GET", "POST"],
        "upstream_id": f"dmp_{proxy['id']}",
        "plugins": {"proxy-rewrite": {"uri": "/rest/v1/assets"}},
    }
    assert wild_call["body"] == {
        "uri": f"/{token}/*",
        "methods": ["GET", "POST"],
        "upstream_id": f"dmp_{proxy['id']}",
        "plugins": {
            "proxy-rewrite": {"regex_uri": [f"^/{token}/(.*)", "/rest/v1/assets/$1"]}
        },
    }

    assert any(a["action"] == "Gateway proxy reconciled" for a in fake_db.audit_v2.docs)


def test_reconcile_http_scheme_and_no_tls_when_cert_has_no_keys(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    proxy = client.post(
        "/api/v2/gateway/proxies",
        json=_proxy_payload(port=8080, sni=None),
    ).json()
    fake_db.connections_v2.docs.append(_apisix_connection_doc())
    fake_db.gateway_v2.docs[0]["allowlist"] = [proxy["targetHost"]]

    calls = _install_apisix_puts(monkeypatch, ok=True)
    resp = client.post(f"/api/v2/gateway/proxies/{proxy['id']}/reconcile")
    assert resp.status_code == 200, resp.text

    upstream_call = next(c for c in calls if c["kind"] == "upstream")
    assert upstream_call["body"]["scheme"] == "http"
    assert upstream_call["body"]["nodes"] == {"api.rapid7.example.corp:8080": 1}
    assert "tls" not in upstream_call["body"]


def test_reconcile_refuses_when_host_not_allowlisted(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    proxy = client.post("/api/v2/gateway/proxies", json=_proxy_payload()).json()
    fake_db.connections_v2.docs.append(_apisix_connection_doc())
    # allowlist deliberately left empty.

    async def _boom(*args, **kwargs):
        raise AssertionError("apisix_client must not be called when host is not allowlisted")

    monkeypatch.setattr(gw.apisix_client, "put_upstream", _boom)
    monkeypatch.setattr(gw.apisix_client, "put_route", _boom)

    resp = client.post(f"/api/v2/gateway/proxies/{proxy['id']}/reconcile")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "Failed"
    assert "not on the gateway allowlist" in body["statusDetail"]
    assert any(a["action"] == "Gateway proxy reconciliation failed" for a in fake_db.audit_v2.docs)


def test_reconcile_without_active_connection_404s():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    proxy = client.post("/api/v2/gateway/proxies", json=_proxy_payload()).json()
    resp = client.post(f"/api/v2/gateway/proxies/{proxy['id']}/reconcile")
    assert resp.status_code == 404
    assert "No active APISIX connection" in resp.json()["detail"]


def test_reconcile_apisix_error_marks_failed(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    proxy = client.post("/api/v2/gateway/proxies", json=_proxy_payload()).json()
    fake_db.connections_v2.docs.append(_apisix_connection_doc())
    fake_db.gateway_v2.docs[0]["allowlist"] = [proxy["targetHost"]]

    _install_apisix_puts(monkeypatch, ok=False, error="APISIX Admin API returned HTTP 400.")

    resp = client.post(f"/api/v2/gateway/proxies/{proxy['id']}/reconcile")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "Failed"
    assert body["statusDetail"] == "APISIX Admin API returned HTTP 400."
    assert any(a["action"] == "Gateway proxy reconciliation failed" for a in fake_db.audit_v2.docs)


def test_reconcile_proxy_not_found():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    resp = client.post("/api/v2/gateway/proxies/nope/reconcile")
    assert resp.status_code == 404


# =========================================================================
# Test (runtime probe)
# =========================================================================


def test_test_proxy_ok(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    proxy = client.post("/api/v2/gateway/proxies", json=_proxy_payload()).json()
    fake_db.connections_v2.docs.append(_apisix_connection_doc())

    _install_http_get(monkeypatch, lambda url: httpx.Response(200, content=b"ok"))

    resp = client.post(f"/api/v2/gateway/proxies/{proxy['id']}/test")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["ok"] is True
    assert body["status"] == 200
    assert isinstance(body["ms"], int)
    assert FakeHttpClient.captured_urls == ["https://gateway.internal/rapid7_egress/rest/v1/assets"]

    # lastTest is persisted on the proxy doc.
    stored_proxy = fake_db.gateway_v2.docs[0]["proxies"][0]
    assert stored_proxy["lastTest"]["ok"] is True


def test_test_proxy_classifies_upstream_error(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    proxy = client.post("/api/v2/gateway/proxies", json=_proxy_payload()).json()
    fake_db.connections_v2.docs.append(_apisix_connection_doc())

    _install_http_get(monkeypatch, lambda url: httpx.Response(502, content=b"bad gateway"))

    resp = client.post(f"/api/v2/gateway/proxies/{proxy['id']}/test")
    body = resp.json()
    assert body["ok"] is False
    assert body["status"] == 502
    assert "upstream target failed" in body["message"]


def test_test_proxy_classifies_gateway_unreachable(monkeypatch):
    fake_db = FakeDB()
    client = _make_client(fake_db)
    proxy = client.post("/api/v2/gateway/proxies", json=_proxy_payload()).json()
    fake_db.connections_v2.docs.append(_apisix_connection_doc())

    _install_http_get(monkeypatch, lambda url: httpx.ConnectError("connection refused"))

    resp = client.post(f"/api/v2/gateway/proxies/{proxy['id']}/test")
    body = resp.json()
    assert body["ok"] is False
    assert body["status"] is None
    assert "unreachable" in body["message"]


def test_test_proxy_without_active_connection_404s():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    proxy = client.post("/api/v2/gateway/proxies", json=_proxy_payload()).json()
    resp = client.post(f"/api/v2/gateway/proxies/{proxy['id']}/test")
    assert resp.status_code == 404


# =========================================================================
# Cert profiles
# =========================================================================


def test_create_cert_profile_never_returns_key_material():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    resp = client.post(
        "/api/v2/gateway/cert-profiles",
        json={"name": "prod-client-cert", "subject": "CN=api-client.example.corp", "certPem": "-- CERT --", "keyPem": "TOP SECRET"},
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert "keyPem" not in body
    assert "certPem" not in body
    assert body["hasKey"] is True
    assert body["hasCert"] is True
    assert body["refCount"] == 0

    # But the secret really is stored server-side.
    stored = fake_db.gateway_v2.docs[0]["certProfiles"][0]
    assert stored["keyPem"] == "TOP SECRET"

    state = client.get("/api/v2/gateway/").json()
    assert "keyPem" not in state["certProfiles"][0]


def test_create_cert_profile_validation():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    resp = client.post("/api/v2/gateway/cert-profiles", json={"name": "", "subject": "CN=x"})
    assert resp.status_code == 422
    assert "Name the certificate" in resp.json()["detail"]

    resp2 = client.post("/api/v2/gateway/cert-profiles", json={"name": "x", "subject": ""})
    assert resp2.status_code == 422
    assert "subject" in resp2.json()["detail"]


def test_delete_cert_profile_blocked_by_refcount_then_succeeds():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    cert = client.post(
        "/api/v2/gateway/cert-profiles", json={"name": "cert-a", "subject": "CN=a.example.corp"}
    ).json()
    proxy = client.post(
        "/api/v2/gateway/proxies", json=_proxy_payload(certProfileId=cert["id"])
    ).json()

    resp = client.delete(f"/api/v2/gateway/cert-profiles/{cert['id']}")
    assert resp.status_code == 409, resp.text
    assert "referenced by 1" in resp.json()["detail"]

    # Re-point the proxy away from the cert, then delete succeeds.
    client.post("/api/v2/gateway/proxies", json=_proxy_payload(id=proxy["id"], certProfileId=None))
    resp2 = client.delete(f"/api/v2/gateway/cert-profiles/{cert['id']}")
    assert resp2.status_code == 200, resp2.text
    assert fake_db.gateway_v2.docs[0]["certProfiles"] == []


def test_delete_cert_profile_not_found():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    resp = client.delete("/api/v2/gateway/cert-profiles/nope")
    assert resp.status_code == 404


# =========================================================================
# Allowlist (admin-gated)
# =========================================================================


def test_allowlist_add_requires_admin_confirmed():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    resp = client.post("/api/v2/gateway/allowlist", json={"host": "api.example.corp", "action": "add"})
    assert resp.status_code == 422
    assert "confirmation" in resp.json()["detail"]
    assert fake_db.gateway_v2.docs == []


def test_allowlist_add_success_is_audited_with_admin_wording():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    resp = client.post(
        "/api/v2/gateway/allowlist",
        json={"host": "api.example.corp", "action": "add", "adminConfirmed": True},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["allowlist"] == ["api.example.corp"]

    state = client.get("/api/v2/gateway/").json()
    assert state["allowlist"] == ["api.example.corp"]

    audit_entry = fake_db.audit_v2.docs[0]
    assert "Admin action recorded" in audit_entry["details"]
    assert "api.example.corp" in audit_entry["details"]
    assert "added" in audit_entry["details"]


def test_allowlist_remove_success_flags_stranded_proxies():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    client.post(
        "/api/v2/gateway/allowlist",
        json={"host": "api.example.corp", "action": "add", "adminConfirmed": True},
    )
    client.post("/api/v2/gateway/proxies", json=_proxy_payload(targetHost="api.example.corp"))

    resp = client.post(
        "/api/v2/gateway/allowlist",
        json={"host": "api.example.corp", "action": "remove", "adminConfirmed": True},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["allowlist"] == []

    audit_entry = fake_db.audit_v2.docs[-1]
    assert "removed" in audit_entry["details"]
    assert "can no longer deploy" in audit_entry["details"]
    assert audit_entry["status"] == "Warning"


def test_allowlist_rejects_invalid_hostname():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    resp = client.post(
        "/api/v2/gateway/allowlist",
        json={"host": "https://api.example.corp", "action": "add", "adminConfirmed": True},
    )
    assert resp.status_code == 422
    assert "scheme" in resp.json()["detail"]


# =========================================================================
# Import smoke test
# =========================================================================


def test_module_imports_cleanly():
    import importlib

    importlib.reload(gw)
