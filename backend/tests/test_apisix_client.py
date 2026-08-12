"""Unit tests for apisix_client.py — offline only, using a fake httpx.AsyncClient."""
import asyncio
import json
from functools import wraps
from pathlib import Path
import sys

import httpx

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import apisix_client


def async_test(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


class FakeAsyncClient:
    """Hand-written fake httpx.AsyncClient that returns a scripted response."""

    # Set by each test before use.
    response_factory = None
    captured_requests = []

    def __init__(self, *args, **kwargs):
        self.init_kwargs = kwargs

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def request(self, method, url, **kwargs):
        FakeAsyncClient.captured_requests.append({"method": method, "url": url, **kwargs})
        result = FakeAsyncClient.response_factory(method, url, **kwargs)
        if isinstance(result, Exception):
            raise result
        return result


def _json_response(status_code, body):
    return httpx.Response(
        status_code,
        content=json.dumps(body).encode("utf-8"),
        headers={"content-type": "application/json"},
    )


def _install(monkeypatch, factory):
    FakeAsyncClient.response_factory = factory
    FakeAsyncClient.captured_requests = []
    monkeypatch.setattr(apisix_client.httpx, "AsyncClient", FakeAsyncClient)


# --- success list parse -------------------------------------------------


@async_test
async def test_list_routes_success_parses_body(monkeypatch):
    body = {
        "total": 2,
        "list": [
            {"key": "/apisix/routes/1", "value": {"id": "1", "uri": "/foo/*"}},
            {"key": "/apisix/routes/2", "value": {"id": "2", "uri": "/bar/*"}},
        ],
    }

    def factory(method, url, **kwargs):
        assert method == "GET"
        assert url == "https://admin.example.com/apisix/admin/routes"
        assert kwargs["headers"]["X-API-KEY"] == "secret-key"
        return _json_response(200, body)

    _install(monkeypatch, factory)

    result = await apisix_client.list_routes("https://admin.example.com", "secret-key")

    assert result["ok"] is True
    assert result["status_code"] == 200
    assert result["error_code"] is None
    assert result["data"]["total"] == 2
    assert len(result["data"]["list"]) == 2


# --- 401 -> AUTH_FAILED ---------------------------------------------------


@async_test
async def test_list_routes_401_maps_to_auth_failed(monkeypatch):
    def factory(method, url, **kwargs):
        return _json_response(401, {"error_msg": "invalid api key"})

    _install(monkeypatch, factory)

    result = await apisix_client.list_routes("https://admin.example.com", "bad-key")

    assert result["ok"] is False
    assert result["reachable"] is True
    assert result["status_code"] == 401
    assert result["error_code"] == apisix_client.APISIX_AUTH_FAILED


@async_test
async def test_test_admin_403_maps_to_auth_failed(monkeypatch):
    def factory(method, url, **kwargs):
        return _json_response(403, {"error_msg": "forbidden"})

    _install(monkeypatch, factory)

    result = await apisix_client.test_admin("https://admin.example.com", "bad-key")

    assert result["ok"] is False
    assert result["error_code"] == apisix_client.APISIX_AUTH_FAILED


# --- timeout -> TIMEOUT ----------------------------------------------------


@async_test
async def test_list_upstreams_timeout_maps_to_timeout(monkeypatch):
    def factory(method, url, **kwargs):
        return httpx.TimeoutException("timed out")

    _install(monkeypatch, factory)

    result = await apisix_client.list_upstreams("https://admin.example.com", "secret-key")

    assert result["ok"] is False
    assert result["reachable"] is False
    assert result["error_code"] == apisix_client.APISIX_TIMEOUT


# --- connect error -> UNREACHABLE ------------------------------------------


@async_test
async def test_test_admin_connect_error_maps_to_unreachable(monkeypatch):
    def factory(method, url, **kwargs):
        return httpx.ConnectError("connection refused")

    _install(monkeypatch, factory)

    result = await apisix_client.test_admin("https://admin.example.com", "secret-key")

    assert result["ok"] is False
    assert result["reachable"] is False
    assert result["error_code"] == apisix_client.APISIX_UNREACHABLE


# --- 400 body message propagation ------------------------------------------


@async_test
async def test_put_route_400_propagates_message(monkeypatch):
    def factory(method, url, **kwargs):
        assert method == "PUT"
        assert url == "https://admin.example.com/apisix/admin/routes/route-1"
        return _json_response(400, {"error_msg": "invalid uri format"})

    _install(monkeypatch, factory)

    result = await apisix_client.put_route(
        "https://admin.example.com", "secret-key", "route-1", {"uri": "???"}
    )

    assert result["ok"] is False
    assert result["status_code"] == 400
    assert result["error_code"] == apisix_client.APISIX_INVALID
    assert result["error"] == "invalid uri format"


@async_test
async def test_put_upstream_400_propagates_message(monkeypatch):
    def factory(method, url, **kwargs):
        return _json_response(400, {"message": "nodes must not be empty"})

    _install(monkeypatch, factory)

    result = await apisix_client.put_upstream(
        "https://admin.example.com", "secret-key", "up-1", {"type": "roundrobin", "nodes": {}}
    )

    assert result["ok"] is False
    assert result["error_code"] == apisix_client.APISIX_INVALID
    assert result["error"] == "nodes must not be empty"


# --- delete 404 -> NOT_FOUND (but treated as ok) ----------------------------


@async_test
async def test_delete_route_404_is_ok_true(monkeypatch):
    def factory(method, url, **kwargs):
        assert method == "DELETE"
        return _json_response(404, {"error_msg": "route not found"})

    _install(monkeypatch, factory)

    result = await apisix_client.delete_route("https://admin.example.com", "secret-key", "missing-route")

    assert result["ok"] is True
    assert result["status_code"] == 404
    assert result["error_code"] is None


@async_test
async def test_get_route_404_maps_to_not_found(monkeypatch):
    def factory(method, url, **kwargs):
        assert method == "GET"
        return _json_response(404, {"error_msg": "route not found"})

    _install(monkeypatch, factory)

    result = await apisix_client.get_route("https://admin.example.com", "secret-key", "missing-route")

    assert result["ok"] is False
    assert result["status_code"] == 404
    assert result["error_code"] == apisix_client.APISIX_NOT_FOUND


@async_test
async def test_delete_upstream_404_is_ok_true(monkeypatch):
    def factory(method, url, **kwargs):
        return _json_response(404, {"error_msg": "upstream not found"})

    _install(monkeypatch, factory)

    result = await apisix_client.delete_upstream("https://admin.example.com", "secret-key", "missing-upstream")

    assert result["ok"] is True
    assert result["status_code"] == 404


@async_test
async def test_delete_ssl_404_is_ok_true(monkeypatch):
    def factory(method, url, **kwargs):
        return _json_response(404, {"error_msg": "ssl not found"})

    _install(monkeypatch, factory)

    result = await apisix_client.delete_ssl("https://admin.example.com", "secret-key", "missing-ssl")

    assert result["ok"] is True
    assert result["status_code"] == 404


# --- put_ssl / list_ssls sanity ---------------------------------------------


@async_test
async def test_put_ssl_success(monkeypatch):
    def factory(method, url, **kwargs):
        assert method == "PUT"
        assert url == "https://admin.example.com/apisix/admin/ssls/ssl-1"
        return _json_response(200, {"key": "/apisix/ssls/ssl-1", "value": {"id": "ssl-1"}})

    _install(monkeypatch, factory)

    result = await apisix_client.put_ssl(
        "https://admin.example.com",
        "secret-key",
        "ssl-1",
        {"cert": "CERT", "key": "KEY", "snis": ["example.com"]},
    )

    assert result["ok"] is True
    assert result["status_code"] == 200


@async_test
async def test_list_ssls_success(monkeypatch):
    def factory(method, url, **kwargs):
        assert method == "GET"
        assert url == "https://admin.example.com/apisix/admin/ssls"
        return _json_response(200, {"total": 0, "list": []})

    _install(monkeypatch, factory)

    result = await apisix_client.list_ssls("https://admin.example.com", "secret-key")

    assert result["ok"] is True
    assert result["data"]["total"] == 0
