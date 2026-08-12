import asyncio
from datetime import datetime
from functools import wraps
from pathlib import Path
import sys

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import content_store
from services.flow_export import build_flow_export_package
from test_content_store import FakeDB, populate_flow_bundle_db


def async_test(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


@async_test
async def test_export_package_preserves_content_store_paths(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()

    package = await build_flow_export_package(db, "flow-1")

    assert package["kind"] == "lovable.flow.export"
    assert package["format_version"] == 2
    assert package["bundle"]["flow_id"] == "flow-1"
    assert package["bundle"]["source_id"] == "source-1"
    assert package["bundle"]["bundle_id"] == "Flow"
    assert "flow.json" in package["files"]
    assert "source/source.json" in package["files"]
    assert "source/streams/users.json" in package["files"]
    assert "source/streams/orders.json" in package["files"]
    assert "services/application.json" in package["files"]
    assert "services/nifi.json" in package["files"]
    assert "services/application/app-1.json" not in package["files"]
    assert "services/nifi/nifi-1.json" not in package["files"]
    assert "openapi/spec-1/spec.json" in package["files"]
    assert "schemas/artifact-users/versions/2.json" in package["files"]
    assert all(not path.startswith("flows/") for path in package["files"])
    assert all("\\" not in path for path in package["files"])


@async_test
async def test_export_package_preserves_full_logical_schema_path_when_physical_path_is_shortened(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()
    long_flow_name = "imported-flow-" + ("copy-" * 40)
    long_artifact_id = "bronze." + ("imported_flow_copy_" * 20) + "plants__history-value"
    db.flows.docs[0]["name"] = long_flow_name
    db.flows.docs[0]["schema_artifact_id"] = long_artifact_id
    db.sources.docs[0]["streams"][0]["entity"]["schema_artifact_id"] = long_artifact_id
    db.schema_artifacts.docs.append({
        "artifact_id": long_artifact_id,
        "namespace": "com.nif",
        "versions": [{"version": 1, "status": "Verified"}],
    })

    package = await build_flow_export_package(db, "flow-1")

    assert f"schemas/{long_artifact_id}/artifact.json" in package["files"]
    assert f"schemas/{long_artifact_id}/versions/1.json" in package["files"]
    assert package["bundle"]["name"] == long_flow_name


@async_test
async def test_export_package_is_compact_and_does_not_include_designer_payload(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()
    db.sources.docs[0]["designer_payload"] = {"step": 2, "streams": [{"id": "users"}]}
    db.sources.docs[0]["connection_timeout"] = 30
    db.sources.docs[0]["rate_limit"] = 120
    db.sources.docs[0]["streams"][0]["headers"] = {}
    db.sources.docs[0]["streams"][0]["filters"] = []

    package = await build_flow_export_package(db, "flow-1")

    source_doc = package["files"]["source/source.json"]
    stream_doc = package["files"]["source/streams/users.json"]
    assert "designer_payload" not in source_doc
    assert "connection_timeout" not in source_doc
    assert "rate_limit" not in source_doc
    assert "headers" not in stream_doc
    assert "filters" not in stream_doc


@async_test
async def test_export_package_serializes_datetime_values_from_db(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()
    db.application_services.docs[0]["config"]["checked_at"] = datetime(2026, 6, 22, 9, 30, 0)

    package = await build_flow_export_package(db, "flow-1")

    from services.flow_export import dumps_export_package

    dumped = dumps_export_package(package)
    assert '"checked_at": "2026-06-22T09:30:00"' in dumped


@async_test
async def test_export_package_does_not_rewrite_existing_content_store_files(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    noisy_source = tmp_path / "flows" / "Flow" / "source" / "source.json"
    noisy_source.parent.mkdir(parents=True)
    noisy_source.write_text('{"designer_payload":{"step":2},"id":"source-1"}\n', encoding="utf-8")
    db = populate_flow_bundle_db()

    await build_flow_export_package(db, "flow-1")

    assert noisy_source.read_text(encoding="utf-8") == '{"designer_payload":{"step":2},"id":"source-1"}\n'


@async_test
async def test_export_package_sanitizes_secrets_and_marks_required_credentials(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()

    package = await build_flow_export_package(db, "flow-1")

    source_doc = package["files"]["source/source.json"]
    service_doc = package["files"]["services/application.json"]
    secrets_doc = package["files"]["secrets.json"]
    assert source_doc["api_key_value"] == {"$secretRef": "source/source.json#api_key_value"}
    assert service_doc["services"][0]["config"]["api_key_value"] == {
        "$secretRef": "services/application.json#services.0.config.api_key_value",
    }
    assert secrets_doc["source/source.json#api_key_value"] is None
    assert secrets_doc["services/application.json#services.0.config.api_key_value"] is None
    required = package["security"]["credentials_required"]
    assert {"path": "source/source.json", "field": "api_key_value"} in required
    assert {"path": "services/application.json", "field": "services.0.config.api_key_value"} in required
    serialized = content_store.to_canonical_json(package)
    assert '"api_key_value": "secret"' not in serialized
    assert "service-secret" not in serialized


@async_test
async def test_export_package_includes_sanitized_secrets_file_for_secret_refs(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()
    db.sources.docs[0]["base_url"] = "https://api.example"

    package = await build_flow_export_package(db, "flow-1")

    source_doc = package["files"]["source/source.json"]
    service_doc = package["files"]["services/application.json"]
    secrets_doc = package["files"]["secrets.json"]
    assert source_doc["api_key_value"] == {"$secretRef": "source/source.json#api_key_value"}
    assert source_doc["base_url"] == {"$secretRef": "source/source.json#base_url"}
    assert service_doc["services"][0]["config"]["api_key_value"] == {
        "$secretRef": "services/application.json#services.0.config.api_key_value",
    }
    assert secrets_doc == {
        "source/source.json#api_key_value": None,
        "source/source.json#base_url": None,
        "services/application.json#services.0.config.api_key_value": None,
        "services/nifi.json#services.1.properties.Schema Registry URLs": None,
        "services/nifi.json#services.2.properties.bootstrap.servers": None,
    }
    assert {"path": "source/source.json", "field": "api_key_value"} in package["security"]["credentials_required"]
    assert {"path": "source/source.json", "field": "base_url"} in package["security"]["credentials_required"]
    assert {"path": "services/application.json", "field": "services.0.config.api_key_value"} in package["security"]["credentials_required"]
    serialized = content_store.to_canonical_json(package)
    assert "https://api.example" not in serialized
    assert '"api_key_value": "secret"' not in serialized
    assert "service-secret" not in serialized


@async_test
async def test_export_openapi_urls_do_not_require_import_reentry(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()
    db.sources.docs[0].update({
        "base_url": "https://runtime.example",
        "openapi_selected_server_url": "https://documented.example",
        "rest_username": "apiuser",
        "rest_password": "secret",
    })
    db.openapi_specs.docs[0]["servers"] = [{"url": "https://documented.example"}]

    package = await build_flow_export_package(db, "flow-1")

    required = package["security"]["credentials_required"]
    assert {"path": "source/source.json", "field": "base_url"} in required
    assert {"path": "source/source.json", "field": "rest_username"} in required
    assert {"path": "source/source.json", "field": "rest_password"} in required
    assert {"path": "source/source.json", "field": "openapi_selected_server_url"} not in required
    assert {"path": "openapi/spec-1/spec.json", "field": "servers.0.url"} not in required
    assert package["files"]["source/source.json"]["openapi_selected_server_url"] == "https://documented.example"
    assert package["files"]["openapi/spec-1/spec.json"]["servers"][0]["url"] == "https://documented.example"


@async_test
async def test_export_service_backed_rest_source_does_not_require_duplicate_designer_credentials(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()
    db.sources.docs[0].update({
        "base_url": None,
        "rest_username": None,
        "rest_password": None,
        "designer_payload": {
            "sourceType": "rest",
            "restConnectionMode": "application_service",
            "restApplicationServiceId": "app-1",
            "restSource": {
                "baseUrl": "https://rapid7.example",
                "username": "apiuser",
                "password": "source-designer-secret",
            },
            "streams": [{"id": "users", "name": "users"}],
            "kafkaOutput": {"topic": "users-topic"},
            "schedule": {"type": "interval"},
        },
    })
    db.application_services.docs[0]["config"] = {
        "base_url": "https://rapid7.example",
        "rest_username": "apiuser",
        "rest_password": "service-secret",
    }

    package = await build_flow_export_package(db, "flow-1")

    required = package["security"]["credentials_required"]
    assert {"path": "services/application.json", "field": "services.0.config.base_url"} in required
    assert {"path": "services/application.json", "field": "services.0.config.rest_username"} in required
    assert {"path": "services/application.json", "field": "services.0.config.rest_password"} in required
    assert {"path": "source/source.json", "field": "designer_payload.restSource.baseUrl"} not in required
    assert {"path": "source/source.json", "field": "designer_payload.restSource.username"} not in required
    assert {"path": "source/source.json", "field": "designer_payload.restSource.password"} not in required
    source_doc = package["files"]["source/source.json"]
    assert "designer_payload" not in source_doc


@async_test
async def test_export_package_removes_runtime_deployment_state(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()
    db.flows.docs[0].update({
        "state": "Running",
        "enabled": True,
        "last_run": "2026-06-14T10:00:00",
        "total_records": 42,
        "total_errors": 3,
        "runs": [{"status": "Success"}],
    })

    package = await build_flow_export_package(db, "flow-1")

    flow_doc = package["files"]["flow.json"]
    assert "state" not in flow_doc
    assert "enabled" not in flow_doc
    assert "nifi_process_group_id" not in flow_doc
    assert "last_run" not in flow_doc
    assert "total_records" not in flow_doc
    assert "total_errors" not in flow_doc
    assert "runs" not in flow_doc


@async_test
async def test_export_package_requires_existing_flow(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = FakeDB()

    with pytest.raises(ValueError, match="Flow not found"):
        await build_flow_export_package(db, "missing-flow")


@async_test
async def test_export_package_requires_primary_stream_id(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()
    db.flows.docs[0].pop("primary_stream", None)
    db.flows.docs[0].pop("primary_stream_id", None)

    with pytest.raises(ValueError, match="primary_stream_id is required"):
        await build_flow_export_package(db, "flow-1")


@async_test
async def test_export_filename_uses_flow_name_without_timestamp():
    from services.flow_export import export_filename

    assert export_filename({"bundle": {"name": "Flow One"}}) == "Flow One.flowpack.json"


@async_test
async def test_flow_export_router_returns_download_response(monkeypatch):
    from routers import flows

    async def fake_build_package(db, flow_id):
        assert flow_id == "flow-1"
        return {
            "kind": "lovable.flow.export",
            "format_version": 2,
            "bundle": {"name": "Flow One"},
            "files": {"flow.json": {"id": "flow-1"}},
            "security": {"secrets_sanitized": True, "credentials_required": []},
            "checksums": {},
        }

    monkeypatch.setattr(flows.flow_export, "build_flow_export_package", fake_build_package)
    monkeypatch.setattr(flows.flow_export, "export_filename", lambda package: "Flow One.flowpack.json")

    response = await flows.export_flow("flow-1", db=FakeDB())

    assert response.media_type == "application/json"
    assert response.headers["content-disposition"] == 'attachment; filename="Flow One.flowpack.json"'
    assert b'"flow.json"' in response.body


def test_server_exposes_content_disposition_for_browser_downloads():
    from server import app

    cors = next((middleware for middleware in app.user_middleware if middleware.cls.__name__ == "CORSMiddleware"), None)

    assert cors is not None
    assert "Content-Disposition" in cors.kwargs.get("expose_headers", [])
