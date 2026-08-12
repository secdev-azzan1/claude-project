import asyncio
from functools import wraps
from pathlib import Path
import sys

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import connection_fingerprint


def async_test(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


def test_kafka_down_no_brokers_available(monkeypatch):
    """Kafka down (NoBrokersAvailable) → reachable False, ok False."""

    class MockAdminClientDown:
        def __init__(self, bootstrap_servers, request_timeout_ms):
            # Simulate NoBrokersAvailable on init
            from kafka.errors import NoBrokersAvailable
            raise NoBrokersAvailable()

        def describe_cluster(self):
            pass

        def close(self):
            pass

    monkeypatch.setattr("services.connection_fingerprint.KafkaAdminClient", MockAdminClientDown)

    @async_test
    async def run_test():
        conn = {
            "endpoint": "localhost:9094",
            "kafka_connection_mode": "native",
        }
        result = await connection_fingerprint.probe_kafka_fingerprint(conn)

        assert result["ok"] is False
        assert result["reachable"] is False
        assert result["fingerprint"] is None
        assert result["error"] is not None
        assert "unreachable" in result["error"].lower()

    run_test()


def test_nifi_unreachable_result_stays_unreachable(monkeypatch):
    """NiFi API helper failure with reachable=False must not become Unknown/reachable."""

    async def mock_root_result(**kwargs):
        return {
            "ok": False,
            "reachable": False,
            "error": "Cannot connect to NiFi at https://localhost:65500.",
        }

    monkeypatch.setattr(
        "services.connection_fingerprint.get_nifi_root_process_group_result",
        mock_root_result,
    )

    @async_test
    async def run_test():
        result = await connection_fingerprint.probe_nifi_fingerprint(
            {
                "endpoint": "https://localhost:65500",
                "auth_type": "BASIC",
                "username": "admin",
                "password": "bad",
            }
        )

        assert result["ok"] is False
        assert result["reachable"] is False
        assert result["fingerprint"] is None
        assert "connect" in result["error"].lower()

    run_test()


def test_nifi_auth_failure_is_reachable_unknown(monkeypatch):
    """Auth/permission failure proves NiFi responded, but identity remains unknown."""

    async def mock_root_result(**kwargs):
        return {
            "ok": False,
            "reachable": True,
            "status_code": 401,
            "error": "Authentication failed. Check credentials.",
        }

    monkeypatch.setattr(
        "services.connection_fingerprint.get_nifi_root_process_group_result",
        mock_root_result,
    )

    @async_test
    async def run_test():
        result = await connection_fingerprint.probe_nifi_fingerprint(
            {
                "endpoint": "https://localhost:8443",
                "auth_type": "BASIC",
                "username": "admin",
                "password": "wrong",
            }
        )

        assert result["ok"] is False
        assert result["reachable"] is True
        assert result["fingerprint"] is None
        assert "authentication" in result["error"].lower()

    run_test()


def test_kafka_up_with_cluster_id(monkeypatch):
    """Kafka up + cluster_id → ok True, fingerprint == cluster_id, reachable True."""

    class MockAdminClient:
        def __init__(self, bootstrap_servers, request_timeout_ms):
            self.bootstrap_servers = bootstrap_servers

        def describe_cluster(self):
            return {"cluster_id": "test-cluster-xyz"}

        def close(self):
            pass

    monkeypatch.setattr("services.connection_fingerprint.KafkaAdminClient", MockAdminClient)

    @async_test
    async def run_test():
        conn = {
            "endpoint": "localhost:9094",
            "kafka_connection_mode": "native",
        }
        result = await connection_fingerprint.probe_kafka_fingerprint(conn)

        assert result["ok"] is True
        assert result["reachable"] is True
        assert result["fingerprint"] == "test-cluster-xyz"
        assert result["error"] is None

    run_test()


def test_kafka_no_cluster_id(monkeypatch):
    """describe_cluster returns no cluster_id → reachable True, ok False, fingerprint None."""

    class MockAdminClient:
        def __init__(self, bootstrap_servers, request_timeout_ms):
            pass

        def describe_cluster(self):
            return {"cluster_id": None}  # or missing

        def close(self):
            pass

    monkeypatch.setattr("services.connection_fingerprint.KafkaAdminClient", MockAdminClient)

    @async_test
    async def run_test():
        conn = {
            "endpoint": "localhost:9094",
            "kafka_connection_mode": "native",
        }
        result = await connection_fingerprint.probe_kafka_fingerprint(conn)

        assert result["ok"] is False
        assert result["reachable"] is True
        assert result["fingerprint"] is None
        assert result["error"] is not None

    run_test()


def test_kafka_authorization_error_after_broker_responded(monkeypatch):
    """Authorization-style error after broker responded → reachable True, ok False (NOT unreachable)."""

    class MockAdminClient:
        def __init__(self, bootstrap_servers, request_timeout_ms):
            pass

        def describe_cluster(self):
            # Simulate auth error (not a connection error)
            raise PermissionError("Not authorized")

        def close(self):
            pass

    monkeypatch.setattr("services.connection_fingerprint.KafkaAdminClient", MockAdminClient)

    @async_test
    async def run_test():
        conn = {
            "endpoint": "localhost:9094",
            "kafka_connection_mode": "native",
        }
        result = await connection_fingerprint.probe_kafka_fingerprint(conn)

        assert result["ok"] is False
        assert result["reachable"] is True  # Broker DID respond
        assert result["fingerprint"] is None
        assert result["error"] is not None

    run_test()


def test_kafbat_mode_no_bootstrap_kafbat_url_responds(monkeypatch):
    """Kafbat mode, no bootstrap, Kafbat URL responds → reachable True, fingerprint None."""

    # Mock httpx.AsyncClient
    class MockAsyncClient:
        def __init__(self, verify=False, timeout=None):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get(self, url):
            # Simulate successful response
            pass

    # Patch httpx.AsyncClient in the connection_fingerprint module
    monkeypatch.setattr("services.connection_fingerprint.httpx.AsyncClient", MockAsyncClient)

    @async_test
    async def run_test():
        conn = {
            "endpoint": "",  # No bootstrap
            "kafbat_url": "localhost:8080",
            "kafka_connection_mode": "kafbat",
        }
        result = await connection_fingerprint.probe_kafka_fingerprint(conn)

        assert result["ok"] is False
        assert result["reachable"] is True
        assert result["fingerprint"] is None
        assert result["error"] is not None

    run_test()


def test_kafbat_mode_with_usable_bootstrap(monkeypatch):
    """Kafbat mode WITH usable bootstrap → cluster_id returned (bootstrap wins)."""

    class MockAdminClient:
        def __init__(self, bootstrap_servers, request_timeout_ms):
            pass

        def describe_cluster(self):
            return {"cluster_id": "bootstrap-cluster-123"}

        def close(self):
            pass

    monkeypatch.setattr("services.connection_fingerprint.KafkaAdminClient", MockAdminClient)

    @async_test
    async def run_test():
        conn = {
            "endpoint": "localhost:9094",  # Usable bootstrap
            "kafbat_url": "localhost:8080",
            "kafka_connection_mode": "kafbat",
        }
        result = await connection_fingerprint.probe_kafka_fingerprint(conn)

        # Bootstrap wins: we get cluster_id
        assert result["ok"] is True
        assert result["reachable"] is True
        assert result["fingerprint"] == "bootstrap-cluster-123"
        assert result["error"] is None

    run_test()
