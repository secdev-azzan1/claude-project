import asyncio
import json
from functools import wraps
from pathlib import Path
import sys

import pytest
from fastapi import HTTPException

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services.flow_import_finalize import FlowImportFinalizeError, finalize_flow_import
from test_content_store import FakeDB
from test_flow_import_preview import Upload, _checksum, add_route_child_stream, make_flowpack, make_flowpack_with_secret_refs


def async_test(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


def raw_package(package=None):
    return json.dumps(package or make_flowpack()).encode("utf-8")


def credential_values(package=None):
    package = package or make_flowpack()
    result = {}
    for item in package["security"]["credentials_required"]:
        result.setdefault(item["path"], {})[item["field"]] = "secret-value"
    return result


def package_with_designer_payload():
    package = make_flowpack()
    package["files"]["source/source.json"]["designer_payload"] = {
        "sourceType": "rest",
        "restSource": {
            "sourceName": "Imported Rapid7",
            "baseUrl": "https://rapid7.example",
        },
        "streams": [
            {
                "id": "sites",
                "name": "sites",
                "entity": {
                    "schemaArtifactId": "bronze.imported_rapid7.sites-value",
                    "schemaVersion": "1",
                    "kafka": {
                        "schemaArtifactId": "bronze.imported_rapid7.sites-value",
                        "schemaVersion": "1",
                    },
                },
            }
        ],
        "entityDestinations": {
            "sites": {
                "schemaMode": "existing",
                "existingSchemaArtifactId": "bronze.imported_rapid7.sites-value",
                "existingSchemaVersion": "1",
            }
        },
    }
    package["checksums"] = {path: _checksum(payload) for path, payload in package["files"].items()}
    return package


def package_with_schema_version_4():
    package = package_with_designer_payload()
    files = package["files"]
    files["flow.json"]["schema_version"] = 4
    stream = files["source/streams/sites.json"]
    stream["entity"]["schema_version"] = 4
    stream["entity"]["kafka"]["schema_version"] = 4
    payload_stream = files["source/source.json"]["designer_payload"]["streams"][0]
    payload_stream["entity"]["schemaVersion"] = "4"
    payload_stream["entity"]["kafka"]["schemaVersion"] = "4"
    files["source/source.json"]["designer_payload"]["entityDestinations"]["sites"]["existingSchemaVersion"] = "4"
    version_doc = files.pop("schemas/bronze.imported_rapid7.sites-value/versions/1.json")
    version_doc["version"] = 4
    version_doc["status"] = "Verified"
    version_doc["verified_at"] = "2026-06-01T00:00:00"
    version_doc["verified_by"] = "admin"
    version_doc["apicurio_global_id"] = 123
    files["schemas/bronze.imported_rapid7.sites-value/versions/4.json"] = version_doc
    package["checksums"] = {path: _checksum(payload) for path, payload in files.items()}
    return package


def package_with_helper_primary_and_entity_child():
    package = make_flowpack()
    files = package["files"]
    files["flow.json"]["primary_stream_id"] = "login"
    files["flow.json"]["kafka_topic"] = "bronze.imported_rapid7.login__history"
    site_stream = files.pop("source/streams/sites.json")
    site_stream["id"] = "orders"
    site_stream["name"] = "orders"
    site_stream["fan_out"] = {"enabled": True, "parent_stream_id": "login"}
    files["source/streams/login.json"] = {
        "id": "login",
        "name": "login",
        "method": "POST",
        "endpoint_path": "/login",
        "request_enabled": True,
        "response_format": "json",
        "is_primary": True,
        "extraction_rules": [{"attribute_name": "access_token", "path": "$.access_token"}],
    }
    files["source/streams/orders.json"] = site_stream
    package["checksums"] = {path: _checksum(payload) for path, payload in package["files"].items()}
    return package


def package_with_runtime_state():
    package = make_flowpack()
    flow = package["files"]["flow.json"]
    flow.update(
        {
            "state": "Running",
            "enabled": True,
            "nifi_process_group_id": "old-pg",
            "schema_inference_job_id": "old-job",
            "schema_inference_active": True,
            "schema_inference_started_at": "2026-07-01T00:00:00",
            "schema_inference_completed_at": "2026-07-01T00:01:00",
            "schema_inference_error": "old error",
            "last_run": "2026-07-01T00:00:00",
            "last_error": "old failure",
            "total_records": 99,
            "total_errors": 3,
            "runs": [{"id": "old-run", "status": "Running"}],
            "deployed_at": "2026-07-01T00:00:00",
            "started_at": "2026-07-01T00:00:01",
            "stopped_at": "2026-07-01T00:00:02",
        }
    )
    package["files"]["source/source.json"]["nifi_process_group_id"] = "old-source-pg"
    package["checksums"] = {path: _checksum(payload) for path, payload in package["files"].items()}
    return package


@async_test
async def test_finalize_import_creates_draft_flow_and_syncs_content_store(monkeypatch, tmp_path):
    from services import flow_import_finalize

    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = FakeDB()

    result = await finalize_flow_import(
        raw_package(),
        credential_values(),
        {"flow_name": "Imported Rapid7 Clone", "schema_resolutions": {}},
        db=db,
    )

    assert result["ok"] is True
    assert result["flow"]["name"] == "Imported Rapid7 Clone"
    assert result["flow"]["state"] == "Draft"
    assert result["flow"]["enabled"] is False
    assert result["flow"]["id"] != "flow-old"
    assert result["flow"]["source_id"] != "source-old"
    assert db.sources.docs[0]["name"] == "Imported Rapid7 Clone"
    assert db.application_services.docs[0]["id"] != "app-old"
    assert db.openapi_specs.docs[0]["id"] != "openapi-old"
    assert db.sources.docs[0]["application_service_id"] == db.application_services.docs[0]["id"]
    assert db.sources.docs[0]["openapi_spec_id"] == db.openapi_specs.docs[0]["id"]
    assert db.application_services.docs[0]["config"]["rest_password"] == "secret-value"
    assert "secret-value" not in json.dumps(result)
    assert (tmp_path / "flows" / "Imported Rapid7 Clone" / "flow.json").exists()
    assert result["flow"]["primary_stream_id"]
    assert "primary_stream" not in result["flow"]


@async_test
async def test_finalize_import_rehydrates_values_from_secret_refs(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = FakeDB()
    package = make_flowpack_with_secret_refs()

    result = await finalize_flow_import(
        raw_package(package),
        credential_values(package),
        {"flow_name": "Imported Rapid7 Secret Refs", "schema_resolutions": {}},
        db=db,
    )

    assert result["ok"] is True
    assert db.application_services.docs[0]["config"]["rest_password"] == "secret-value"
    secrets_file = tmp_path / "flows" / "Imported Rapid7 Secret Refs" / "secrets.json"
    assert secrets_file.exists()
    assert "secret-value" in secrets_file.read_text(encoding="utf-8")


@async_test
async def test_finalize_import_hydrates_compact_source_and_stream_defaults():
    db = FakeDB()

    await finalize_flow_import(
        raw_package(),
        credential_values(),
        {"flow_name": "Imported Rapid7 Hydrated", "schema_resolutions": {}},
        db=db,
    )

    source = db.sources.docs[0]
    stream = source["streams"][0]
    assert source["connection_setup_mode"] == "application_service"
    assert source["auth_type"] == "NONE"
    assert source["connection_timeout"] == 30
    assert source["read_timeout"] == 30
    assert source["write_timeout"] == 30
    assert source["idle_timeout"] == 30
    assert stream["method"] == "GET"
    assert stream["request_enabled"] is True
    assert stream["response_format"] == "json"
    assert stream["body_format"] == "empty"
    assert stream["split_response_records"] is False
    assert stream["pagination"] == {"enabled": False, "type": "none"}


@async_test
async def test_finalize_import_clears_runtime_and_schema_inference_state():
    package = package_with_runtime_state()
    db = FakeDB()

    result = await finalize_flow_import(
        raw_package(package),
        credential_values(package),
        {"flow_name": "Imported Rapid7 Runtime Reset", "schema_resolutions": {}},
        db=db,
    )

    flow = db.flows.docs[0]
    source = db.sources.docs[0]
    assert result["flow"]["state"] == "Draft"
    assert flow["state"] == "Draft"
    assert flow["enabled"] is False
    assert flow["nifi_process_group_id"] is None
    assert flow["schema_inference_job_id"] is None
    assert flow["schema_inference_active"] is False
    assert flow["last_run"] is None
    assert flow["total_records"] == 0
    assert flow["total_errors"] == 0
    assert flow["runs"] == []
    assert "schema_inference_started_at" not in flow
    assert "schema_inference_completed_at" not in flow
    assert "schema_inference_error" not in flow
    assert "last_error" not in flow
    assert "deployed_at" not in flow
    assert "started_at" not in flow
    assert "stopped_at" not in flow
    assert source["state"] == "Draft"
    assert source["nifi_process_group_id"] is None


@async_test
async def test_finalize_import_rewrites_designer_payload_for_renamed_copy():
    package = package_with_designer_payload()
    db = FakeDB()
    db.schema_artifacts.docs.append({
        "artifact_id": "bronze.imported_rapid7.sites-value",
        "versions": [{"version": 1, "schema": {"type": "record", "name": "Different", "fields": []}}],
    })

    await finalize_flow_import(
        raw_package(package),
        credential_values(package),
        {
            "flow_name": "Imported Rapid7 Designer Copy",
            "schema_resolutions": {
                "bronze.imported_rapid7.sites-value": {"action": "rename"}
            },
        },
        db=db,
    )

    source = db.sources.docs[0]
    expected_artifact_id = "bronze.imported_rapid7_designer_copy.sites__history-value"
    stream_id = source["streams"][0]["id"]
    assert source["designer_payload"]["restSource"]["sourceName"] == "Imported Rapid7 Designer Copy"
    assert source["designer_payload"]["entityDestinations"][stream_id]["existingSchemaArtifactId"] == expected_artifact_id
    assert source["streams"][0]["entity"]["schema_artifact_id"] == expected_artifact_id


@async_test
async def test_finalize_import_renamed_schema_resets_exported_version_to_unverified_v1():
    package = package_with_schema_version_4()
    db = FakeDB()
    db.schema_artifacts.docs.append({
        "artifact_id": "bronze.imported_rapid7.sites-value",
        "versions": [{"version": 1, "schema": {"type": "record", "name": "Different", "fields": []}}],
    })

    await finalize_flow_import(
        raw_package(package),
        credential_values(package),
        {
            "flow_name": "Imported Rapid7 Renamed V4",
            "schema_resolutions": {
                "bronze.imported_rapid7.sites-value": {"action": "rename"}
            },
        },
        db=db,
    )

    expected_artifact_id = "bronze.imported_rapid7_renamed_v4.sites__history-value"
    imported_schema = db.schema_artifacts.docs[1]
    assert imported_schema["artifact_id"] == expected_artifact_id
    assert len(imported_schema["versions"]) == 1
    imported_version = imported_schema["versions"][0]
    assert imported_version["version"] == 1
    assert imported_version["status"] == "Needs Verification"
    assert "verified_at" not in imported_version
    assert "verified_by" not in imported_version
    assert "apicurio_global_id" not in imported_version

    flow = db.flows.docs[0]
    source = db.sources.docs[0]
    stream = source["streams"][0]
    stream_id = stream["id"]
    assert flow["schema_artifact_id"] == expected_artifact_id
    assert flow["schema_version"] == 1
    assert stream["entity"]["schema_artifact_id"] == expected_artifact_id
    assert stream["entity"]["schema_version"] == 1
    assert stream["entity"]["kafka"]["schema_artifact_id"] == expected_artifact_id
    assert stream["entity"]["kafka"]["schema_version"] == 1
    assert source["designer_payload"]["streams"][0]["entity"]["schemaVersion"] == "1"
    assert source["designer_payload"]["streams"][0]["entity"]["kafka"]["schemaVersion"] == "1"
    assert source["designer_payload"]["entityDestinations"][stream_id]["existingSchemaArtifactId"] == expected_artifact_id
    assert source["designer_payload"]["entityDestinations"][stream_id]["existingSchemaVersion"] == "1"


@async_test
async def test_finalize_import_requires_new_flow_name_when_name_exists():
    db = FakeDB()
    db.flows.docs.append({"id": "existing", "name": "Imported Rapid7"})

    with pytest.raises(FlowImportFinalizeError, match="Flow name already exists"):
        await finalize_flow_import(raw_package(), credential_values(), {"flow_name": "Imported Rapid7"}, db=db)


@async_test
async def test_finalize_import_links_compatible_existing_schema():
    db = FakeDB()
    db.schema_artifacts.docs.append({
        "artifact_id": "bronze.imported_rapid7.sites-value",
        "versions": [
            {
                "version": 5,
                "schema": {"type": "record", "name": "Site", "fields": []},
            }
        ],
    })

    result = await finalize_flow_import(
        raw_package(),
        credential_values(),
        {
            "flow_name": "Imported Rapid7 Linked",
            "schema_resolutions": {
                "bronze.imported_rapid7.sites-value": {"action": "link_existing"}
            },
        },
        db=db,
    )

    assert result["created"]["schemas"] == 0
    assert result["created"]["linked_schemas"] == 1
    assert len(db.schema_artifacts.docs) == 1
    assert db.flows.docs[0]["schema_artifact_id"] == "bronze.imported_rapid7.sites-value"
    assert db.sources.docs[0]["streams"][0]["entity"]["schema_artifact_id"] == "bronze.imported_rapid7.sites-value"


@async_test
async def test_finalize_import_rejects_linking_incompatible_schema():
    db = FakeDB()
    db.schema_artifacts.docs.append({
        "artifact_id": "bronze.imported_rapid7.sites-value",
        "versions": [
            {
                "version": 1,
                "schema": {"type": "record", "name": "Different", "fields": []},
            }
        ],
    })

    with pytest.raises(FlowImportFinalizeError, match="not compatible"):
        await finalize_flow_import(
            raw_package(),
            credential_values(),
            {
                "flow_name": "Imported Rapid7",
                "schema_resolutions": {
                    "bronze.imported_rapid7.sites-value": {"action": "link_existing"}
                },
            },
            db=db,
        )


@async_test
async def test_finalize_import_renames_incompatible_schema_and_rewrites_refs():
    db = FakeDB()
    db.schema_artifacts.docs.append({
        "artifact_id": "bronze.imported_rapid7.sites-value",
        "versions": [{"version": 1, "schema": {"type": "record", "name": "Different", "fields": []}}],
    })

    result = await finalize_flow_import(
        raw_package(),
        credential_values(),
        {
            "flow_name": "Imported Rapid7 Renamed",
            "schema_resolutions": {
                "bronze.imported_rapid7.sites-value": {
                    "action": "rename",
                }
            },
        },
        db=db,
    )

    assert result["created"]["schemas"] == 1
    expected_artifact_id = "bronze.imported_rapid7_renamed.sites__history-value"
    assert db.flows.docs[0]["schema_artifact_id"] == expected_artifact_id
    assert db.sources.docs[0]["streams"][0]["entity"]["kafka"]["schema_artifact_id"] == expected_artifact_id
    assert db.schema_artifacts.docs[1]["artifact_id"] == expected_artifact_id


@async_test
async def test_finalize_import_derives_schema_rename_from_stream_name_not_user_input():
    db = FakeDB()
    db.schema_artifacts.docs.append({
        "artifact_id": "bronze.imported_rapid7.sites-value",
        "versions": [{"version": 1, "schema": {"type": "record", "name": "Different", "fields": []}}],
    })

    await finalize_flow_import(
        raw_package(),
        credential_values(),
        {
            "flow_name": "Imported Rapid7 Manual Name Ignored",
            "schema_resolutions": {
                "bronze.imported_rapid7.sites-value": {
                    "action": "rename",
                    "artifact_id": "bronze.user_supplied.wrong-value",
                }
            },
        },
        db=db,
    )

    expected_artifact_id = "bronze.imported_rapid7_manual_name_ignored.sites__history-value"
    assert db.flows.docs[0]["schema_artifact_id"] == expected_artifact_id
    assert db.schema_artifacts.docs[1]["artifact_id"] == expected_artifact_id


@async_test
async def test_finalize_import_rewrites_parallel_route_stream_ids():
    package = add_route_child_stream(make_flowpack())
    db = FakeDB()

    await finalize_flow_import(
        raw_package(package),
        credential_values(package),
        {"flow_name": "Imported Rapid7 Routes", "schema_resolutions": {}},
        db=db,
    )

    streams = db.sources.docs[0]["streams"]
    old_ids = {"sites", "site_assets"}
    new_ids = {stream["id"] for stream in streams}
    assert not old_ids.intersection(new_ids)
    parent = next(stream for stream in streams if stream["name"] == "sites")
    child = next(stream for stream in streams if stream["name"] == "site_assets")
    assert parent["routes"][0]["child_stream_id"] == child["id"]
    assert child["route_source"]["parent_stream_id"] == parent["id"]


@async_test
async def test_finalize_import_remaps_primary_stream_id_to_new_stream_id():
    package = make_flowpack()
    package["files"]["flow.json"]["primary_stream_id"] = "sites"
    package["checksums"] = {path: _checksum(payload) for path, payload in package["files"].items()}
    db = FakeDB()

    result = await finalize_flow_import(
        raw_package(package),
        credential_values(package),
        {"flow_name": "Imported Rapid7 Canonical", "schema_resolutions": {}},
        db=db,
    )

    stream = db.sources.docs[0]["streams"][0]
    assert result["flow"]["primary_stream_id"] == stream["id"]
    assert db.flows.docs[0]["primary_stream_id"] == stream["id"]
    assert "primary_stream" not in db.flows.docs[0]


@async_test
async def test_finalize_import_rejects_package_without_primary_stream_id():
    package = make_flowpack()
    package["files"]["flow.json"].pop("primary_stream_id", None)
    package["checksums"] = {path: _checksum(payload) for path, payload in package["files"].items()}
    db = FakeDB()

    with pytest.raises(FlowImportFinalizeError, match="primary_stream_id is required"):
        await finalize_flow_import(
            raw_package(package),
            credential_values(package),
            {"flow_name": "Imported Rapid7 Missing Primary", "schema_resolutions": {}},
            db=db,
        )


@async_test
async def test_finalize_import_rejects_old_primary_stream_field():
    package = make_flowpack()
    package["files"]["flow.json"]["primary_stream"] = "sites"
    package["checksums"] = {path: _checksum(payload) for path, payload in package["files"].items()}
    db = FakeDB()

    with pytest.raises(FlowImportFinalizeError, match="primary_stream is not supported"):
        await finalize_flow_import(
            raw_package(package),
            credential_values(package),
            {"flow_name": "Imported Rapid7 Old Primary", "schema_resolutions": {}},
            db=db,
        )


@async_test
async def test_finalize_import_regenerates_kafka_topics_from_imported_flow_name():
    db = FakeDB()

    result = await finalize_flow_import(
        raw_package(),
        credential_values(),
        {"flow_name": "Imported Rapid7 Topic Copy", "schema_resolutions": {}},
        db=db,
    )

    expected_topic = "bronze.imported_rapid7_topic_copy.sites__history"
    assert result["flow"]["kafka_topic"] == expected_topic
    stream = db.sources.docs[0]["streams"][0]
    assert stream["entity"]["kafka"]["topic"] == expected_topic


@async_test
async def test_finalize_import_legacy_topic_uses_first_entity_not_helper_primary():
    package = package_with_helper_primary_and_entity_child()
    db = FakeDB()

    result = await finalize_flow_import(
        raw_package(package),
        credential_values(package),
        {"flow_name": "Imported Helper Primary Copy", "schema_resolutions": {}},
        db=db,
    )

    assert result["flow"]["kafka_topic"] == "bronze.imported_helper_primary_copy.orders__history"
    streams = {stream["name"]: stream for stream in db.sources.docs[0]["streams"]}
    assert result["flow"]["primary_stream_id"] == streams["login"]["id"]
    assert result["flow"]["primary_stream_id"] != streams["orders"]["id"]
    assert "entity" not in streams["login"]
    assert streams["orders"]["entity"]["kafka"]["topic"] == "bronze.imported_helper_primary_copy.orders__history"


@async_test
async def test_finalize_import_router_returns_import_result():
    from routers import flow_import

    result = await flow_import.finalize_flowpack_import(
        file=Upload(raw_package()),
        credentials_json=json.dumps(credential_values()),
        options_json=json.dumps({"flow_name": "Imported Via Router", "schema_resolutions": {}}),
        db=FakeDB(),
    )

    assert result["ok"] is True
    assert result["flow"]["name"] == "Imported Via Router"


@async_test
async def test_finalize_import_router_rejects_invalid_options_json():
    from routers import flow_import

    with pytest.raises(HTTPException) as exc:
        await flow_import.finalize_flowpack_import(
            file=Upload(raw_package()),
            credentials_json=json.dumps(credential_values()),
            options_json="{not-json",
            db=FakeDB(),
        )

    assert exc.value.status_code == 422
    assert "options_json" in exc.value.detail
