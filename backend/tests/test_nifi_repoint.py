from __future__ import annotations

import asyncio
import sys
from functools import wraps
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services.adapter import nifi_repoint
from services.adapter.deployer.lifecycle import StagedNifiDeployment


def async_test(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


def _ok(data):
    return {"ok": True, "data": data}


@async_test
async def test_verify_process_group_walks_nested_controller_services(monkeypatch):
    async def fake_request(_conn, method, path, **_kwargs):
        if path == "/nifi-api/process-groups/flow-1":
            return _ok({"invalidCount": 0})
        if path == "/nifi-api/flow/process-groups/flow-1/status":
            return _ok({"processGroupStatus": {"aggregateSnapshot": {"invalidCount": 0}}})
        if path == "/nifi-api/flow/process-groups/flow-1":
            return _ok({"processGroupFlow": {"flow": {
                "processGroups": [{"component": {"id": "child-1", "name": "child"}}],
                "processors": [],
            }}})
        if path == "/nifi-api/flow/process-groups/child-1":
            return _ok({"processGroupFlow": {"flow": {
                "processGroups": [],
                "processors": [{"component": {"id": "p-1", "name": "publish", "validationStatus": "VALID"}}],
            }}})
        if path == "/nifi-api/flow/process-groups/flow-1/controller-services":
            return _ok({"controllerServices": []})
        if path == "/nifi-api/flow/process-groups/child-1/controller-services":
            return _ok({"controllerServices": [{"component": {"id": "cs-1", "parentGroupId": "child-1"}}]})
        if path == "/nifi-api/controller-services/cs-1":
            return _ok({"component": {
                "id": "cs-1",
                "name": "kafka_connection",
                "type": "org.apache.nifi.kafka.service.Kafka3ConnectionService",
                "state": "ENABLED",
                "validationStatus": "VALID",
            }})
        raise AssertionError(f"unexpected request {method} {path}")

    monkeypatch.setattr(nifi_repoint, "_request", fake_request)
    result = await nifi_repoint._verify_process_group({"endpoint": "https://nifi"}, "flow-1")

    assert result["processGroups"] == 2
    assert result["processors"] == 1
    assert result["controllerServiceCount"] == 1
    assert result["controllerServices"][0]["type"] == "Kafka3ConnectionService"


@async_test
async def test_verify_process_group_rejects_invalid_nested_service(monkeypatch):
    async def fake_request(_conn, _method, path, **_kwargs):
        if path == "/nifi-api/process-groups/flow-1":
            return _ok({"invalidCount": 0})
        if path == "/nifi-api/flow/process-groups/flow-1/status":
            return _ok({"processGroupStatus": {"aggregateSnapshot": {}}})
        if path == "/nifi-api/flow/process-groups/flow-1":
            return _ok({"processGroupFlow": {"flow": {"processGroups": [], "processors": []}}})
        if path == "/nifi-api/flow/process-groups/flow-1/controller-services":
            return _ok({"controllerServices": [{"component": {"id": "cs-1", "parentGroupId": "flow-1"}}]})
        if path == "/nifi-api/controller-services/cs-1":
            return _ok({"component": {
                "id": "cs-1",
                "name": "schema_registry",
                "state": "DISABLED",
                "validationStatus": "INVALID",
                "validationErrors": ["Registry URL is required"],
            }})
        raise AssertionError(path)

    monkeypatch.setattr(nifi_repoint, "_request", fake_request)
    with pytest.raises(nifi_repoint.NifiRepointError, match="schema_registry"):
        await nifi_repoint._verify_process_group({"endpoint": "https://nifi"}, "flow-1")


@async_test
async def test_cleanup_does_not_delete_reused_parameter_context(monkeypatch):
    deleted_groups = []
    requested_paths = []

    async def fake_delete_flow_pg(_conn, pg_id):
        deleted_groups.append(pg_id)

    async def fake_request(_conn, _method, path, **_kwargs):
        requested_paths.append(path)
        return _ok({})

    monkeypatch.setattr(nifi_repoint.nifi_apply, "delete_flow_pg", fake_delete_flow_pg)
    monkeypatch.setattr(nifi_repoint, "_request", fake_request)
    item = StagedNifiDeployment(
        flow_id="flow-1",
        flow_name="Flow",
        old_process_group_id="old-pg",
        parameter_context_id="existing-pc",
        parameter_context_created=False,
        staged_group_name="flow__migration_1",
        update={"nifiProcessGroupId": "new-pg"},
    )

    await nifi_repoint._cleanup_staged({"endpoint": "https://nifi"}, [item], [])

    assert deleted_groups == ["new-pg"]
    assert all("parameter-contexts/existing-pc" not in path for path in requested_paths)
