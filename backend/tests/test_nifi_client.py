"""Unit tests for nifi_client.py — offline only."""
import asyncio
from functools import wraps

import httpx

from services import nifi_client
from services.nifi_client import _normalize_nifi_base_url


def async_test(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


def test_normalize_nifi_base_url_strips_single_trailing_slash():
    """Trailing slash should be stripped."""
    assert _normalize_nifi_base_url("https://localhost:8443/") == "https://localhost:8443"


def test_normalize_nifi_base_url_strips_multiple_trailing_slashes():
    """Multiple trailing slashes should all be stripped."""
    assert _normalize_nifi_base_url("https://localhost:8443///") == "https://localhost:8443"


def test_normalize_nifi_base_url_preserves_nifi_suffix():
    """Trailing /nifi should NOT be stripped — reverse proxy deployments need it."""
    assert _normalize_nifi_base_url("https://localhost:8443/nifi") == "https://localhost:8443/nifi"


def test_normalize_nifi_base_url_preserves_nifi_suffix_with_slash():
    """Trailing /nifi/ should strip trailing slash but preserve /nifi."""
    assert _normalize_nifi_base_url("https://localhost:8443/nifi/") == "https://localhost:8443/nifi"


def test_normalize_nifi_base_url_no_trailing_slash():
    """URL without trailing slash should remain unchanged."""
    assert _normalize_nifi_base_url("https://localhost:8443") == "https://localhost:8443"


def test_normalize_nifi_base_url_remote_url():
    """Remote URL should remain unchanged."""
    assert _normalize_nifi_base_url("https://nifi.datapasc.com") == "https://nifi.datapasc.com"


def test_normalize_nifi_base_url_remote_url_with_trailing_slash():
    """Remote URL with trailing slash should be stripped."""
    assert _normalize_nifi_base_url("https://nifi.datapasc.com/") == "https://nifi.datapasc.com"


def test_normalize_nifi_base_url_empty_string():
    """Empty string should return empty string."""
    assert _normalize_nifi_base_url("") == ""


def test_normalize_nifi_base_url_whitespace():
    """Whitespace should be stripped."""
    assert _normalize_nifi_base_url("  https://localhost:8443  ") == "https://localhost:8443"


def test_normalize_nifi_base_url_none():
    """None converted to string should return empty string."""
    result = _normalize_nifi_base_url(None)  # type: ignore
    assert result == ""


def test_test_nifi_connection_reports_ui_path_during_token_exchange(monkeypatch):
    class MockAsyncClient:
        def __init__(self, verify=True, timeout=None):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def post(self, *args, **kwargs):
            return httpx.Response(302, headers={"location": "/nifi/#/404"})

    monkeypatch.setattr("services.nifi_client.httpx.AsyncClient", MockAsyncClient)

    @async_test
    async def run_test():
        result = await nifi_client.test_nifi_connection(
            "https://localhost:8443/nifi",
            auth_type="BASIC",
            username="admin",
            password="password",
        )

        assert result["ok"] is False
        assert result["reachable"] is True
        assert "ui path" in result["error"].lower()

    run_test()


def test_test_nifi_connection_reports_invalid_sni_during_token_exchange(monkeypatch):
    class MockAsyncClient:
        def __init__(self, verify=True, timeout=None):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def post(self, *args, **kwargs):
            return httpx.Response(400, text="<html><title>Error 400 Invalid SNI</title></html>")

    monkeypatch.setattr("services.nifi_client.httpx.AsyncClient", MockAsyncClient)

    @async_test
    async def run_test():
        result = await nifi_client.test_nifi_connection(
            "https://127.0.0.1:8443",
            auth_type="BASIC",
            username="admin",
            password="password",
        )

        assert result["ok"] is False
        assert result["reachable"] is False
        assert "invalid sni" in result["error"].lower()
        assert "hostname" in result["error"].lower()

    run_test()


def test_test_nifi_connection_reports_connect_error_during_token_exchange(monkeypatch):
    class MockAsyncClient:
        def __init__(self, verify=True, timeout=None):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def post(self, *args, **kwargs):
            raise httpx.ConnectError("All connection attempts failed")

    monkeypatch.setattr("services.nifi_client.httpx.AsyncClient", MockAsyncClient)

    @async_test
    async def run_test():
        result = await nifi_client.test_nifi_connection(
            "https://localhost:65500",
            auth_type="BASIC",
            username="admin",
            password="password",
        )

        assert result["ok"] is False
        assert result["reachable"] is False
        assert "cannot connect" in result["error"].lower()

    run_test()
