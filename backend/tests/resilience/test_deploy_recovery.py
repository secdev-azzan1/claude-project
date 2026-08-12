import asyncio

import pytest
from fastapi import HTTPException

from routers import flows
from tests.resilience.conftest import (
    ResilienceFakeDB,
    make_rest_source,
    make_verified_rest_flow,
)


def _deployable_db():
    db = ResilienceFakeDB()
    db.flows.docs.append(make_verified_rest_flow())
    db.sources.docs.append(make_rest_source())
    db.connections.docs.extend(
        [
            {"type": "nifi", "endpoint": "https://nifi.example", "auth_type": "NONE"},
            {"type": "kafka", "endpoint": "kafka:9092"},
            {"type": "apicurio", "endpoint": "https://registry.example"},
        ],
    )
    return db


def _patch_deploy_prerequisites(monkeypatch):
    async def no_op(*_args, **_kwargs):
        return None

    async def service_ids(*_args, **_kwargs):
        return {"kafka": "kafka-service", "schema_registry": "schema-service"}

    monkeypatch.setattr(flows, "require_runtime_connections", no_op)
    monkeypatch.setattr(flows, "_validate_verified_schema_for_flow", no_op)
    monkeypatch.setattr(flows, "_ensure_deploy_schemas_registered", no_op)
    monkeypatch.setattr(flows, "_log_audit", no_op)

    from services import nifi_global_services

    monkeypatch.setattr(
        nifi_global_services,
        "resolve_flow_global_service_ids",
        service_ids,
    )


def test_persistence_failure_deletes_new_process_group_and_releases_state(monkeypatch):
    db = _deployable_db()
    _patch_deploy_prerequisites(monkeypatch)
    deleted = []
    sync_calls = 0

    async def generate(**_kwargs):
        return {"ok": True, "process_group_id": "pg-new"}

    async def cleanup(_conn, pg_id):
        deleted.append(pg_id)

    async def sync_flow(_db, _flow_id):
        nonlocal sync_calls
        sync_calls += 1
        if sync_calls == 2:
            raise RuntimeError("content store unavailable")

    from services import nifi_flow_generator

    monkeypatch.setattr(nifi_flow_generator, "generate_and_deploy_flow", generate)
    monkeypatch.setattr(flows, "_delete_failed_deployment_pg", cleanup)
    monkeypatch.setattr(flows.content_store, "sync_flow", sync_flow)

    with pytest.raises(HTTPException, match="content store unavailable"):
        asyncio.run(flows.deploy_flow("flow-1", db=db))

    flow = db.flows.docs[0]
    assert flow["state"] == "Stopped"
    assert flow.get("nifi_process_group_id") is None
    assert deleted == ["pg-new"]


def test_concurrent_deployments_create_only_one_process_group(monkeypatch):
    db = _deployable_db()
    _patch_deploy_prerequisites(monkeypatch)
    generator_calls = 0
    entered = asyncio.Event()
    release = asyncio.Event()

    async def generate(**_kwargs):
        nonlocal generator_calls
        generator_calls += 1
        entered.set()
        await release.wait()
        return {"ok": True, "process_group_id": "pg-one"}

    async def no_op(*_args, **_kwargs):
        return None

    from services import nifi_flow_generator

    monkeypatch.setattr(nifi_flow_generator, "generate_and_deploy_flow", generate)
    monkeypatch.setattr(flows.content_store, "sync_flow", no_op)

    async def run():
        first = asyncio.create_task(flows.deploy_flow("flow-1", db=db))
        await entered.wait()
        second = asyncio.create_task(flows.deploy_flow("flow-1", db=db))
        await asyncio.sleep(0)
        release.set()
        return await asyncio.gather(first, second, return_exceptions=True)

    results = asyncio.run(run())

    assert generator_calls == 1
    assert sum(isinstance(result, HTTPException) and result.status_code == 409 for result in results) == 1
    assert sum(isinstance(result, dict) and result.get("ok") for result in results) == 1
    assert db.flows.docs[0]["nifi_process_group_id"] == "pg-one"
