import asyncio
import copy
from datetime import datetime
from functools import wraps
from pathlib import Path
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from routers import connections as connections_router
from tests.resilience.conftest import FaultInjectingCollection, _matches, _set_nested


def async_test(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


class ExtendedFaultInjectingCollection(FaultInjectingCollection):
    """FaultInjectingCollection with update_many support."""

    async def update_many(self, query, update):
        """Update all matching documents."""
        count = 0
        for document in self.docs:
            if not _matches(document, query):
                continue
            for key, value in update.get("$set", {}).items():
                _set_nested(document, key, copy.deepcopy(value))
            for key in update.get("$unset", {}):
                target = document
                parts = key.split(".")
                for part in parts[:-1]:
                    target = target.get(part, {})
                target.pop(parts[-1], None)
            count += 1
        return type('Result', (), {'modified_count': count})()

    async def count_documents(self, query):
        """Count documents matching query."""
        return sum(1 for doc in self.docs if _matches(doc, query))


class FakeDB:
    """Minimal fake DB for connection tests."""

    def __init__(self):
        self.connections = ExtendedFaultInjectingCollection()
        self.flows = ExtendedFaultInjectingCollection()
        self.nifi_global_services = ExtendedFaultInjectingCollection()
        self.schema_artifacts = ExtendedFaultInjectingCollection()
        self.audit_events = ExtendedFaultInjectingCollection()


def test_activate_reachable_connection(monkeypatch):
    """Reachable connection → activation succeeds, reachability == 'Reachable'."""

    async def mock_probe(conn):
        return {
            "ok": True,
            "fingerprint": "test-fingerprint",
            "reachable": True,
            "error": None,
        }

    monkeypatch.setattr(
        "routers.connections.probe_nifi_fingerprint", mock_probe
    )

    @async_test
    async def run_test():
        db = FakeDB()
        conn_id = "test-conn-1"
        conn_doc = {
            "id": conn_id,
            "name": "Test NiFi",
            "type": "nifi",
            "endpoint": "http://localhost:8080",
            "auth_type": "NONE",
            "is_active": False,
            "reachability": "Unknown",
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        }
        await db.connections.insert_one(conn_doc)

        # Mock the db dependency
        async def get_db():
            return db

        monkeypatch.setattr("routers.connections.get_db", lambda: get_db)

        # Call activate_connection
        result = await connections_router.activate_connection(conn_id, db=db)

        # Verify activation succeeded
        assert result["is_active"] is True
        assert result["reachability"] == "Reachable"

        # Verify DB state
        activated = await db.connections.find_one({"id": conn_id})
        assert activated["is_active"] is True
        assert activated["reachability"] == "Reachable"

    run_test()


def test_activate_unreachable_connection(monkeypatch):
    """Unreachable connection → 409 error, connection NOT activated."""

    async def mock_probe(conn):
        return {
            "ok": False,
            "fingerprint": None,
            "reachable": False,
            "error": "Connection refused",
        }

    monkeypatch.setattr(
        "routers.connections.probe_nifi_fingerprint", mock_probe
    )

    @async_test
    async def run_test():
        from fastapi import HTTPException

        db = FakeDB()
        conn_id = "test-conn-2"
        conn_doc = {
            "id": conn_id,
            "name": "Test NiFi Down",
            "type": "nifi",
            "endpoint": "http://localhost:8080",
            "auth_type": "NONE",
            "is_active": False,
            "reachability": "Unknown",
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        }
        await db.connections.insert_one(conn_doc)

        # Mock the db dependency
        async def get_db():
            return db

        monkeypatch.setattr("routers.connections.get_db", lambda: get_db)

        # Call activate_connection - should raise HTTPException
        with pytest.raises(HTTPException) as exc_info:
            await connections_router.activate_connection(conn_id, db=db)

        # Verify it's a 409
        assert exc_info.value.status_code == 409
        assert "unreachable" in exc_info.value.detail.lower()

        # Verify connection is still NOT active
        conn = await db.connections.find_one({"id": conn_id})
        assert conn["is_active"] is False
        # But reachability should have been updated to "Unreachable"
        assert conn["reachability"] == "Unreachable"

    run_test()


def test_activate_reachable_unknown_identity_no_dependents(monkeypatch):
    """Reachable but Unknown fingerprint (None) with no dependents → activation succeeds."""

    async def mock_probe(conn):
        return {
            "ok": False,
            "fingerprint": None,
            "reachable": True,
            "error": "unsupported: apicurio has no stable instance id",
        }

    monkeypatch.setattr(
        "routers.connections.probe_apicurio_fingerprint", mock_probe
    )

    @async_test
    async def run_test():
        db = FakeDB()
        conn_id = "test-apicurio-1"
        conn_doc = {
            "id": conn_id,
            "name": "Test Apicurio",
            "type": "apicurio",
            "endpoint": "http://localhost:8081",
            "auth_type": "NONE",
            "is_active": False,
            "reachability": "Unknown",
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        }
        await db.connections.insert_one(conn_doc)

        # Mock the db dependency
        async def get_db():
            return db

        monkeypatch.setattr("routers.connections.get_db", lambda: get_db)

        # Call activate_connection
        result = await connections_router.activate_connection(conn_id, db=db)

        # Verify activation succeeded
        assert result["is_active"] is True
        assert result["reachability"] == "Reachable"

    run_test()


def test_activate_with_dependents_blocks_activation(monkeypatch):
    """Dependents present on active connection → existing dependent 409 still wins (precedence)."""

    @async_test
    async def run_test():
        from fastapi import HTTPException

        db = FakeDB()

        # Create active NiFi connection
        active_conn_id = "active-nifi-1"
        active_conn_doc = {
            "id": active_conn_id,
            "name": "Active NiFi",
            "type": "nifi",
            "endpoint": "http://localhost:8080",
            "auth_type": "NONE",
            "is_active": True,
            "reachability": "Reachable",
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        }
        await db.connections.insert_one(active_conn_doc)

        # Create inactive NiFi connection to activate
        inactive_conn_id = "inactive-nifi-1"
        inactive_conn_doc = {
            "id": inactive_conn_id,
            "name": "Inactive NiFi",
            "type": "nifi",
            "endpoint": "http://localhost:8081",
            "auth_type": "NONE",
            "is_active": False,
            "reachability": "Unknown",
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        }
        await db.connections.insert_one(inactive_conn_doc)

        # Create a flow that depends on the active connection
        flow_doc = {
            "id": "flow-1",
            "name": "Test Flow",
            "nifi_connection_id": active_conn_id,
            "created_at": datetime.utcnow(),
        }
        await db.flows.insert_one(flow_doc)

        # Mock the db dependency
        async def get_db():
            return db

        monkeypatch.setattr("routers.connections.get_db", lambda: get_db)

        # Try to activate the inactive connection
        # Should raise 409 due to dependents on active connection (before probe is called)
        with pytest.raises(HTTPException) as exc_info:
            await connections_router.activate_connection(inactive_conn_id, db=db)

        # Verify it's a 409
        assert exc_info.value.status_code == 409
        assert "repoint" in exc_info.value.detail.lower()

    run_test()


def test_activate_probe_exception_treated_as_unreachable(monkeypatch):
    """Probe function raises unexpectedly → treated as unreachable, 409 returned."""

    async def mock_probe_error(conn):
        raise RuntimeError("Unexpected probe error")

    monkeypatch.setattr(
        "routers.connections.probe_nifi_fingerprint", mock_probe_error
    )

    @async_test
    async def run_test():
        from fastapi import HTTPException

        db = FakeDB()
        conn_id = "test-conn-3"
        conn_doc = {
            "id": conn_id,
            "name": "Test NiFi",
            "type": "nifi",
            "endpoint": "http://localhost:8080",
            "auth_type": "NONE",
            "is_active": False,
            "reachability": "Unknown",
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        }
        await db.connections.insert_one(conn_doc)

        # Mock the db dependency
        async def get_db():
            return db

        monkeypatch.setattr("routers.connections.get_db", lambda: get_db)

        # Call activate_connection - should raise HTTPException
        with pytest.raises(HTTPException) as exc_info:
            await connections_router.activate_connection(conn_id, db=db)

        # Verify it's a 409
        assert exc_info.value.status_code == 409
        assert "unreachable" in exc_info.value.detail.lower()

        # Verify reachability was set to Unreachable
        conn = await db.connections.find_one({"id": conn_id})
        assert conn["reachability"] == "Unreachable"

    run_test()
