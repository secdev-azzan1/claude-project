import asyncio
import copy
import hashlib
import json
from functools import wraps
from pathlib import Path
import sys

import pytest
from fastapi import HTTPException

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import content_store
from services.flow_import_preview import FlowImportPreviewError, preview_flow_import
from test_content_store import FakeDB


def async_test(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


def _checksum(value):
    return hashlib.sha256(content_store.to_canonical_json(value).encode("utf-8")).hexdigest()


def make_flowpack():
    files = {
        "flow.json": {
            "id": "flow-old",
            "name": "Imported Rapid7",
            "source_id": "source-old",
            "source_type": "REST API",
            "primary_stream_id": "sites",
            "schema_artifact_id": "bronze.imported_rapid7.sites-value",
            "schema_version": 1,
        },
        "source/source.json": {
            "id": "source-old",
            "name": "Imported Rapid7",
            "type": "REST API",
            "connection_setup_mode": "application_service",
            "application_service_id": "app-old",
            "openapi_spec_id": "openapi-old",
        },
        "source/streams/sites.json": {
            "id": "sites",
            "name": "sites",
            "entity": {
                "schema_artifact_id": "bronze.imported_rapid7.sites-value",
                "schema_version": 1,
                "kafka": {
                    "topic": "bronze.imported_rapid7.sites__history",
                    "schema_artifact_id": "bronze.imported_rapid7.sites-value",
                    "schema_version": 1,
                },
            },
        },
        "schemas/bronze.imported_rapid7.sites-value/artifact.json": {
            "artifact_id": "bronze.imported_rapid7.sites-value",
            "name": "sites",
        },
        "schemas/bronze.imported_rapid7.sites-value/versions/1.json": {
            "version": 1,
            "status": "Verified",
            "schema": {"type": "record", "name": "Site", "fields": []},
        },
        "services/application.json": {
            "services": [
                {
                    "id": "app-old",
                    "name": "Rapid7 HTTP",
                    "service_type": "REST API",
                    "config": {"base_url": "https://rapid7.example", "rest_password": None},
                }
            ],
        },
        "services/nifi.json": {
            "services": [
                {
                    "id": "kafka-old",
                    "name": "Default Kafka",
                    "service_kind": "kafka",
                    "is_default": True,
                    "resolution": "default",
                },
                {
                    "id": "schema-old",
                    "name": "Default Schema Registry",
                    "service_kind": "schema_registry",
                    "is_default": True,
                    "resolution": "default",
                },
            ],
        },
        "openapi/openapi-old/spec.json": {
            "id": "openapi-old",
            "title": "Rapid7 VM API",
            "operations": [],
        },
    }
    return {
        "kind": "lovable.flow.export",
        "format_version": 2,
        "exported_at": "2026-06-15T00:00:00Z",
        "bundle": {
            "flow_id": "flow-old",
            "source_id": "source-old",
            "bundle_id": "Imported Rapid7",
            "name": "Imported Rapid7",
        },
        "security": {
            "secrets_sanitized": True,
            "credentials_required": [
                {"path": "services/application.json", "field": "services.0.config.rest_password"},
            ],
        },
        "files": files,
        "checksums": {path: _checksum(payload) for path, payload in files.items()},
    }


def make_flowpack_with_secret_refs():
    package = make_flowpack()
    files = package["files"]
    files["services/application.json"]["services"][0]["config"]["rest_password"] = {
        "$secretRef": "services/application.json#services.0.config.rest_password"
    }
    files["secrets.json"] = {
        "services/application.json#services.0.config.rest_password": None,
    }
    package["checksums"] = {path: _checksum(payload) for path, payload in files.items()}
    return package


def add_route_child_stream(package):
    files = package["files"]
    files["source/streams/sites.json"]["routes"] = [
        {"id": "route-old", "child_stream_id": "site_assets", "ordinal": 1}
    ]
    files["source/streams/site_assets.json"] = {
        "id": "site_assets",
        "name": "site_assets",
        "route_source": {"parent_stream_id": "sites", "route_id": "route-old"},
        "entity": {
            "schema_artifact_id": "bronze.imported_rapid7.site_assets-value",
            "schema_version": 1,
            "kafka": {
                "topic": "bronze.imported_rapid7.site_assets__history",
                "schema_artifact_id": "bronze.imported_rapid7.site_assets-value",
                "schema_version": 1,
            },
        },
    }
    files["schemas/bronze.imported_rapid7.site_assets-value/artifact.json"] = {
        "artifact_id": "bronze.imported_rapid7.site_assets-value",
        "name": "site_assets",
    }
    files["schemas/bronze.imported_rapid7.site_assets-value/versions/1.json"] = {
        "version": 1,
        "status": "Verified",
        "schema": {"type": "record", "name": "SiteAsset", "fields": []},
    }
    package["checksums"] = {path: _checksum(payload) for path, payload in files.items()}
    return package


@async_test
async def test_preview_valid_flowpack_returns_summary_and_credentials():
    db = FakeDB()

    preview = await preview_flow_import(json.dumps(make_flowpack()).encode("utf-8"), db=db)

    assert preview["ok"] is True
    assert preview["summary"] == {
        "flow_name": "Imported Rapid7",
        "source_name": "Imported Rapid7",
        "source_type": "REST API",
        "streams": 1,
        "schemas": 1,
        "schema_versions": 1,
        "application_services": 1,
        "nifi_services": 2,
        "openapi_specs": 1,
        "credentials_required": 1,
    }
    assert preview["credentials_required"] == [
        {"path": "services/application.json", "field": "services.0.config.rest_password"}
    ]
    assert preview["nifi_service_resolutions"] == [
        {
            "service_kind": "kafka",
            "name": "Default Kafka",
            "resolution": "default",
            "import_enabled": False,
            "import_reason": "Imported global NiFi services are visible for review but cannot be activated while this environment enforces one default service per kind.",
        },
        {
            "service_kind": "schema_registry",
            "name": "Default Schema Registry",
            "resolution": "default",
            "import_enabled": False,
            "import_reason": "Imported global NiFi services are visible for review but cannot be activated while this environment enforces one default service per kind.",
        },
    ]
    assert preview["conflicts"] == {"flow_name_exists": False, "schema_artifacts": []}
    assert preview["errors"] == []


@async_test
async def test_preview_accepts_sanitized_secrets_file_for_secret_refs():
    db = FakeDB()

    preview = await preview_flow_import(json.dumps(make_flowpack_with_secret_refs()).encode("utf-8"), db=db)

    assert preview["ok"] is True
    assert preview["credentials_required"] == [
        {"path": "services/application.json", "field": "services.0.config.rest_password"}
    ]


@async_test
async def test_preview_accepts_compact_flowpack_without_ui_or_default_fields():
    package = make_flowpack()

    preview = await preview_flow_import(json.dumps(package).encode("utf-8"), db=FakeDB())

    assert preview["ok"] is True
    assert "designer_payload" not in package["files"]["source/source.json"]
    assert "state" not in package["files"]["flow.json"]
    assert "method" not in package["files"]["source/streams/sites.json"]


@async_test
async def test_preview_rejects_old_format_version():
    package = make_flowpack()
    package["format_version"] = 1

    with pytest.raises(FlowImportPreviewError, match="Unsupported flowpack format_version: 1"):
        await preview_flow_import(json.dumps(package).encode("utf-8"), db=FakeDB())


@async_test
async def test_preview_detects_flow_name_and_schema_conflicts():
    db = FakeDB()
    db.flows.docs.append({"id": "existing-flow", "name": "Imported Rapid7"})
    db.schema_artifacts.docs.append({"artifact_id": "bronze.imported_rapid7.sites-value"})

    preview = await preview_flow_import(json.dumps(make_flowpack()).encode("utf-8"), db=db)

    assert preview["conflicts"]["flow_name_exists"] is True
    assert preview["conflicts"]["schema_artifacts"] == [
        {
            "artifact_id": "bronze.imported_rapid7.sites-value",
            "exists": True,
            "compatible": False,
            "reason": "Existing schema artifact has no matching exported schema version",
        }
    ]


@async_test
async def test_preview_marks_existing_schema_conflict_compatible_when_schema_matches():
    db = FakeDB()
    db.schema_artifacts.docs.append({
        "artifact_id": "bronze.imported_rapid7.sites-value",
        "versions": [
            {
                "version": 4,
                "schema": {"type": "record", "name": "Site", "fields": []},
            }
        ],
    })

    preview = await preview_flow_import(json.dumps(make_flowpack()).encode("utf-8"), db=db)

    assert preview["conflicts"]["schema_artifacts"] == [
        {
            "artifact_id": "bronze.imported_rapid7.sites-value",
            "exists": True,
            "compatible": True,
            "reason": "Schema body matches existing version 4",
        }
    ]


@async_test
async def test_preview_marks_existing_schema_conflict_compatible_when_avro_schema_matches():
    package = make_flowpack()
    version_path = "schemas/bronze.imported_rapid7.sites-value/versions/1.json"
    package["files"][version_path].pop("schema")
    package["files"][version_path]["avro_schema"] = {"type": "record", "name": "Site", "fields": []}
    package["checksums"] = {path: _checksum(payload) for path, payload in package["files"].items()}
    db = FakeDB()
    db.schema_artifacts.docs.append({
        "artifact_id": "bronze.imported_rapid7.sites-value",
        "versions": [
            {
                "version": 7,
                "avro_schema": {"type": "record", "name": "Site", "fields": []},
            }
        ],
    })

    preview = await preview_flow_import(json.dumps(package).encode("utf-8"), db=db)

    assert preview["conflicts"]["schema_artifacts"] == [
        {
            "artifact_id": "bronze.imported_rapid7.sites-value",
            "exists": True,
            "compatible": True,
            "reason": "Schema body matches existing version 7",
        }
    ]


@async_test
async def test_preview_rejects_invalid_json():
    with pytest.raises(FlowImportPreviewError, match="valid JSON"):
        await preview_flow_import(b"{not-json", db=FakeDB())


@async_test
async def test_preview_rejects_wrong_kind():
    package = make_flowpack()
    package["kind"] = "other"

    with pytest.raises(FlowImportPreviewError, match="Unsupported flowpack kind"):
        await preview_flow_import(json.dumps(package).encode("utf-8"), db=FakeDB())


@async_test
async def test_preview_rejects_missing_required_files():
    package = make_flowpack()
    del package["files"]["flow.json"]
    del package["checksums"]["flow.json"]

    with pytest.raises(FlowImportPreviewError, match="Missing required file: flow.json"):
        await preview_flow_import(json.dumps(package).encode("utf-8"), db=FakeDB())


@async_test
async def test_preview_rejects_missing_primary_stream_id():
    package = make_flowpack()
    package["files"]["flow.json"].pop("primary_stream_id", None)
    package["checksums"]["flow.json"] = _checksum(package["files"]["flow.json"])

    with pytest.raises(FlowImportPreviewError, match="primary_stream_id is required"):
        await preview_flow_import(json.dumps(package).encode("utf-8"), db=FakeDB())


@async_test
async def test_preview_rejects_old_primary_stream_field():
    package = make_flowpack()
    package["files"]["flow.json"]["primary_stream"] = "sites"
    package["checksums"]["flow.json"] = _checksum(package["files"]["flow.json"])

    with pytest.raises(FlowImportPreviewError, match="primary_stream is not supported"):
        await preview_flow_import(json.dumps(package).encode("utf-8"), db=FakeDB())


@async_test
async def test_preview_rejects_unknown_primary_stream_id():
    package = make_flowpack()
    package["files"]["flow.json"]["primary_stream_id"] = "missing-stream"
    package["checksums"]["flow.json"] = _checksum(package["files"]["flow.json"])

    with pytest.raises(FlowImportPreviewError, match="must reference an exported stream id"):
        await preview_flow_import(json.dumps(package).encode("utf-8"), db=FakeDB())


@async_test
async def test_preview_rejects_route_to_missing_child_stream():
    package = make_flowpack()
    package["files"]["source/streams/sites.json"]["routes"] = [
        {"id": "route-missing", "child_stream_id": "missing-child", "ordinal": 1}
    ]
    package["checksums"]["source/streams/sites.json"] = _checksum(package["files"]["source/streams/sites.json"])

    with pytest.raises(FlowImportPreviewError, match="routes.0.child_stream_id must reference an exported stream id"):
        await preview_flow_import(json.dumps(package).encode("utf-8"), db=FakeDB())


@async_test
async def test_preview_rejects_fan_out_missing_parent_stream():
    package = make_flowpack()
    package["files"]["source/streams/sites.json"]["fan_out"] = {
        "enabled": True,
        "parent_stream_id": "missing-parent",
    }
    package["checksums"]["source/streams/sites.json"] = _checksum(package["files"]["source/streams/sites.json"])

    with pytest.raises(FlowImportPreviewError, match="fan_out.parent_stream_id must reference an exported stream id"):
        await preview_flow_import(json.dumps(package).encode("utf-8"), db=FakeDB())


@async_test
async def test_preview_rejects_parameter_binding_missing_parent_stream():
    package = make_flowpack()
    package["files"]["source/streams/sites.json"]["parameter_bindings"] = [
        {
            "param_name": "site_id",
            "param_in": "path",
            "value_source": "parent_field",
            "parent_stream_id": "missing-parent",
            "parent_field": "site_id",
        }
    ]
    package["checksums"]["source/streams/sites.json"] = _checksum(package["files"]["source/streams/sites.json"])

    with pytest.raises(FlowImportPreviewError, match="parameter_bindings.0.parent_stream_id must reference an exported stream id"):
        await preview_flow_import(json.dumps(package).encode("utf-8"), db=FakeDB())


@async_test
async def test_preview_rejects_unsafe_paths():
    package = make_flowpack()
    package["files"]["../flow.json"] = package["files"]["flow.json"]
    package["checksums"]["../flow.json"] = _checksum(package["files"]["flow.json"])

    with pytest.raises(FlowImportPreviewError, match="Unsafe file path"):
        await preview_flow_import(json.dumps(package).encode("utf-8"), db=FakeDB())


@async_test
async def test_preview_rejects_checksum_mismatch():
    package = make_flowpack()
    package["checksums"]["flow.json"] = "bad-checksum"

    with pytest.raises(FlowImportPreviewError, match="Checksum mismatch: flow.json"):
        await preview_flow_import(json.dumps(package).encode("utf-8"), db=FakeDB())


@async_test
async def test_preview_rejects_unsanitized_required_credential():
    package = make_flowpack()
    package["files"]["services/application.json"]["services"][0]["config"]["rest_password"] = "secret"
    package["checksums"]["services/application.json"] = _checksum(
        package["files"]["services/application.json"]
    )

    with pytest.raises(FlowImportPreviewError, match="Credential field must be null"):
        await preview_flow_import(json.dumps(package).encode("utf-8"), db=FakeDB())


@async_test
async def test_preview_rejects_unsanitized_secret_file_value():
    package = make_flowpack_with_secret_refs()
    package["files"]["secrets.json"]["services/application.json#services.0.config.rest_password"] = "secret"
    package["checksums"]["secrets.json"] = _checksum(package["files"]["secrets.json"])

    with pytest.raises(FlowImportPreviewError, match="Credential secret value must be null"):
        await preview_flow_import(json.dumps(package).encode("utf-8"), db=FakeDB())


@async_test
async def test_preview_rejects_imported_global_nifi_service_resolution():
    package = make_flowpack()
    package["files"]["services/nifi.json"]["services"][0]["resolution"] = "imported"
    package["checksums"]["services/nifi.json"] = _checksum(package["files"]["services/nifi.json"])

    with pytest.raises(FlowImportPreviewError, match="resolution must be default"):
        await preview_flow_import(json.dumps(package).encode("utf-8"), db=FakeDB())


class Upload:
    filename = "import.flowpack.json"
    content_type = "application/json"

    def __init__(self, payload):
        self.payload = payload

    async def read(self):
        return self.payload


@async_test
async def test_import_preview_router_returns_preview(monkeypatch):
    from routers import flow_import

    db = FakeDB()
    response = await flow_import.preview_flowpack_import(
        file=Upload(json.dumps(make_flowpack()).encode("utf-8")),
        db=db,
    )

    assert response["ok"] is True
    assert response["summary"]["flow_name"] == "Imported Rapid7"


@async_test
async def test_import_preview_router_rejects_wrong_extension():
    from routers import flow_import

    upload = Upload(json.dumps(make_flowpack()).encode("utf-8"))
    upload.filename = "import.json"

    with pytest.raises(HTTPException) as exc:
        await flow_import.preview_flowpack_import(file=upload, db=FakeDB())

    assert exc.value.status_code == 422
    assert ".flowpack.json" in exc.value.detail
