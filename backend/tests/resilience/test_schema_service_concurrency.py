import asyncio

from fastapi import HTTPException

from routers import schemas
from services import apicurio_client, application_services
from tests.resilience.conftest import ResilienceFakeDB


def _schema_artifact():
    return {
        "artifact_id": "resilience.concurrent-value",
        "versions": [
            {
                "version": 1,
                "status": "Needs Verification",
                "avro_schema": {
                    "type": "record",
                    "name": "ConcurrentRecord",
                    "fields": [],
                },
            }
        ],
    }


def test_concurrent_schema_verification_registers_once(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = ResilienceFakeDB()
    db.schema_artifacts.docs.append(_schema_artifact())
    db.connections.docs.append(
        {
            "id": "apicurio",
            "type": "apicurio",
            "endpoint": "http://registry.invalid",
            "group_id": "resilience",
            "auth_type": "NONE",
        }
    )
    calls = 0
    first_registration_started = asyncio.Event()
    release_registration = asyncio.Event()

    async def delayed_register_schema(**_kwargs):
        nonlocal calls
        calls += 1
        first_registration_started.set()
        await release_registration.wait()
        return {"ok": True, "global_id": 101}

    monkeypatch.setattr(apicurio_client, "register_schema", delayed_register_schema)

    async def run_verifications():
        first = asyncio.create_task(
            schemas.verify_version("resilience.concurrent-value", 1, db),
        )
        await asyncio.wait_for(first_registration_started.wait(), timeout=1)
        second = asyncio.create_task(
            schemas.verify_version("resilience.concurrent-value", 1, db),
        )
        await asyncio.sleep(0)
        release_registration.set()
        return await asyncio.gather(first, second)

    results = asyncio.run(run_verifications())

    assert calls == 1
    assert [result["status"] for result in results] == ["Verified", "Verified"]
    assert db.schema_artifacts.docs[0]["versions"][0]["status"] == "Verified"


def test_delete_is_rejected_while_schema_verification_is_running(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = ResilienceFakeDB()
    db.schema_artifacts.docs.append(_schema_artifact())
    db.connections.docs.append(
        {
            "id": "apicurio",
            "type": "apicurio",
            "endpoint": "http://registry.invalid",
            "group_id": "resilience",
            "auth_type": "NONE",
        }
    )
    registration_started = asyncio.Event()
    release_registration = asyncio.Event()

    async def delayed_register_schema(**_kwargs):
        registration_started.set()
        await release_registration.wait()
        return {"ok": True, "global_id": 102}

    async def no_blocking_flows(*_args, **_kwargs):
        return []

    async def no_unlinks(*_args, **_kwargs):
        return 0

    monkeypatch.setattr(apicurio_client, "register_schema", delayed_register_schema)
    monkeypatch.setattr(schemas, "_blocking_schema_flows", no_blocking_flows)
    monkeypatch.setattr(schemas, "_unlink_flows_for_schema", no_unlinks)
    monkeypatch.setattr(schemas, "_unlink_source_streams_for_schema", no_unlinks)
    monkeypatch.setattr(schemas.content_store, "delete_schema_artifact", lambda _artifact_id: None)

    async def run_race():
        verification = asyncio.create_task(
            schemas.verify_version("resilience.concurrent-value", 1, db),
        )
        await asyncio.wait_for(registration_started.wait(), timeout=1)
        deletion = asyncio.create_task(
            schemas.delete_artifact("resilience.concurrent-value", db),
        )
        await asyncio.sleep(0)
        release_registration.set()
        return await asyncio.gather(verification, deletion, return_exceptions=True)

    verification_result, deletion_result = asyncio.run(run_race())

    assert verification_result["status"] == "Verified"
    assert isinstance(deletion_result, HTTPException)
    assert deletion_result.status_code == 409
    assert len(db.schema_artifacts.docs) == 1


def test_referenced_application_service_cannot_be_deleted(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = ResilienceFakeDB()
    db.application_services.docs.append(
        {
            "id": "service-1",
            "name": "Referenced REST Service",
            "service_type": "REST API",
            "config": {"base_url": "https://example.invalid"},
        }
    )
    db.sources.docs.append(
        {
            "id": "source-1",
            "name": "Referenced Source",
            "type": "REST API",
            "application_service_id": "service-1",
        }
    )
    db.flows.docs.append(
        {
            "id": "flow-1",
            "name": "Referenced Flow",
            "source_id": "source-1",
            "state": "Stopped",
        }
    )

    async def delete_service():
        await application_services.delete_application_service(db, "service-1")

    try:
        asyncio.run(delete_service())
    except HTTPException as exc:
        assert exc.status_code == 409
    else:
        raise AssertionError("Expected referenced service deletion to be rejected")

    assert len(db.application_services.docs) == 1
    assert len(db.sources.docs) == 1
    assert len(db.flows.docs) == 1
