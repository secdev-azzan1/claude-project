import asyncio
import copy

import pytest

from services import content_store, flow_export
from test_content_store import populate_flow_bundle_db


def _database_snapshot(db):
    return {
        "flows": copy.deepcopy(db.flows.docs),
        "sources": copy.deepcopy(db.sources.docs),
        "schemas": copy.deepcopy(db.schema_artifacts.docs),
        "application_services": copy.deepcopy(db.application_services.docs),
        "nifi_services": copy.deepcopy(db.nifi_global_services.docs),
        "openapi_specs": copy.deepcopy(db.openapi_specs.docs),
    }


def _tree_snapshot(root):
    if not root.exists():
        return {}
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }


def test_export_does_not_mutate_database_or_content_store(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()
    asyncio.run(content_store.sync_flow(db, "flow-1"))
    before_db = _database_snapshot(db)
    before_files = _tree_snapshot(tmp_path)

    package = asyncio.run(flow_export.build_flow_export_package(db, "flow-1"))

    assert package["kind"] == flow_export.EXPORT_KIND
    assert _database_snapshot(db) == before_db
    assert _tree_snapshot(tmp_path) == before_files


def test_runtime_state_change_during_export_does_not_leak_into_package(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()
    original_payloads = content_store.flow_bundle_payloads
    flow_read = asyncio.Event()
    continue_export = asyncio.Event()

    async def paused_payloads(db_arg, flow):
        flow_read.set()
        await continue_export.wait()
        return await original_payloads(db_arg, flow)

    monkeypatch.setattr(flow_export.content_store, "flow_bundle_payloads", paused_payloads)

    async def run_export_with_state_change():
        export_task = asyncio.create_task(
            flow_export.build_flow_export_package(db, "flow-1"),
        )
        await asyncio.wait_for(flow_read.wait(), timeout=1)
        await db.flows.update_one(
            {"id": "flow-1"},
            {"$set": {"state": "Starting", "nifi_process_group_id": "transient-pg"}},
        )
        continue_export.set()
        return await export_task

    package = asyncio.run(run_export_with_state_change())
    exported_flow = package["files"]["flow.json"]

    assert "state" not in exported_flow
    assert "nifi_process_group_id" not in exported_flow
    assert package["bundle"]["flow_id"] == exported_flow["id"]
    assert package["bundle"]["source_id"] == exported_flow["source_id"]


def test_export_read_failure_is_non_mutating_and_retryable(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()
    before_db = _database_snapshot(db)
    original_payloads = content_store.flow_bundle_payloads
    calls = 0

    async def fail_once(db_arg, flow):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("injected bundle read failure")
        return await original_payloads(db_arg, flow)

    monkeypatch.setattr(flow_export.content_store, "flow_bundle_payloads", fail_once)

    with pytest.raises(OSError, match="injected bundle read failure"):
        asyncio.run(flow_export.build_flow_export_package(db, "flow-1"))

    package = asyncio.run(flow_export.build_flow_export_package(db, "flow-1"))

    assert package["kind"] == flow_export.EXPORT_KIND
    assert _database_snapshot(db) == before_db
