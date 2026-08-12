import asyncio
import json
import os
from datetime import datetime
from functools import wraps
from pathlib import Path
import sys

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import content_store


def async_test(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


def test_content_store_root_prefers_env(monkeypatch, tmp_path):
    configured = tmp_path / "store"
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(configured))

    assert content_store.get_content_store_root() == configured.resolve()


def test_content_store_root_defaults_under_backend(monkeypatch):
    monkeypatch.delenv("CONTENT_STORE_ROOT", raising=False)

    root = content_store.get_content_store_root()

    assert root == content_store.DEFAULT_CONTENT_STORE_ROOT.resolve()


def test_content_store_root_relative_path_anchors_to_repo_root_from_backend_cwd(monkeypatch):
    """Relative paths should resolve against repo root, not CWD."""
    repo_root = content_store.BACKEND_DIR.parent
    relative_path = "backend/.content-store"
    monkeypatch.setenv("CONTENT_STORE_ROOT", relative_path)
    monkeypatch.chdir(str(content_store.BACKEND_DIR))

    root = content_store.get_content_store_root()

    assert root == (repo_root / relative_path).resolve()


def test_content_store_root_relative_path_anchors_to_repo_root_from_repo_cwd(monkeypatch):
    """Relative paths should resolve against repo root even when CWD is repo root."""
    repo_root = content_store.BACKEND_DIR.parent
    relative_path = "backend/.content-store"
    monkeypatch.setenv("CONTENT_STORE_ROOT", relative_path)
    monkeypatch.chdir(str(repo_root))

    root = content_store.get_content_store_root()

    assert root == (repo_root / relative_path).resolve()


def test_content_store_root_relative_path_same_from_any_cwd(monkeypatch):
    """Relative paths must resolve to the same result regardless of CWD."""
    repo_root = content_store.BACKEND_DIR.parent
    relative_path = "backend/.content-store"
    monkeypatch.setenv("CONTENT_STORE_ROOT", relative_path)

    monkeypatch.chdir(str(content_store.BACKEND_DIR))
    root_from_backend = content_store.get_content_store_root()

    monkeypatch.chdir(str(repo_root))
    root_from_repo = content_store.get_content_store_root()

    assert root_from_backend == root_from_repo


def test_content_store_root_absolute_path_returned_unchanged(monkeypatch, tmp_path):
    """Absolute paths should be returned unchanged (proof existing tests unaffected)."""
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))

    root = content_store.get_content_store_root()

    assert root == tmp_path.resolve()


def test_to_jsonable_removes_mongo_id_and_converts_datetimes():
    value = {
        "_id": "mongo-object-id",
        "id": "source-1",
        "created_at": datetime(2026, 6, 14, 10, 30, 0),
        "nested": {"_id": "nested-mongo-id", "updated_at": datetime(2026, 6, 14, 11, 0, 0)},
        "items": [{"_id": "item-mongo-id", "name": "stream"}],
    }

    out = content_store.to_jsonable(value)

    assert "_id" not in out
    assert out["created_at"] == "2026-06-14T10:30:00"
    assert "_id" not in out["nested"]
    assert out["nested"]["updated_at"] == "2026-06-14T11:00:00"
    assert "_id" not in out["items"][0]


def test_canonical_json_is_stable():
    payload = {"b": 2, "a": 1}

    assert content_store.to_canonical_json(payload) == '{\n  "a": 1,\n  "b": 2\n}\n'


def test_atomic_write_json_writes_canonical_file(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    target = tmp_path / "flows" / "flow-1" / "flow.json"

    content_store.atomic_write_json(target, {"id": "flow-1", "z": 1, "a": 2})

    assert target.read_text(encoding="utf-8") == '{\n  "a": 2,\n  "id": "flow-1",\n  "z": 1\n}\n'
    assert not list(target.parent.glob("*.tmp"))


def test_safe_delete_tree_removes_child_path(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    child = tmp_path / "flows" / "flow-1"
    child.mkdir(parents=True)
    (child / "flow.json").write_text("{}", encoding="utf-8")

    content_store.safe_delete_tree(child)

    assert not child.exists()


def test_safe_delete_tree_rejects_outside_root(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path / "store"))
    outside = tmp_path / "outside"
    outside.mkdir()

    with pytest.raises(ValueError, match="outside content store root"):
        content_store.safe_delete_tree(outside)


def test_layout_paths_are_flow_scoped(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))

    assert content_store.flow_path("Flow Name") == tmp_path.resolve() / "flows" / "Flow Name" / "flow.json"
    assert content_store.secrets_path("Flow Name") == tmp_path.resolve() / "flows" / "Flow Name" / "secrets.json"
    assert content_store.source_path("Flow Name") == tmp_path.resolve() / "flows" / "Flow Name" / "source" / "source.json"
    assert content_store.stream_path("Flow Name", "stream-1") == tmp_path.resolve() / "flows" / "Flow Name" / "source" / "streams" / "stream-1.json"
    assert content_store.application_services_path("Flow Name") == tmp_path.resolve() / "flows" / "Flow Name" / "services" / "application.json"
    assert content_store.nifi_services_path("Flow Name") == tmp_path.resolve() / "flows" / "Flow Name" / "services" / "nifi.json"
    assert content_store.schema_artifact_path("Flow Name", "artifact-1") == tmp_path.resolve() / "flows" / "Flow Name" / "schemas" / "artifact-1" / "artifact.json"
    assert content_store.schema_version_path("Flow Name", "artifact-1", 2) == tmp_path.resolve() / "flows" / "Flow Name" / "schemas" / "artifact-1" / "versions" / "2.json"
    assert content_store.openapi_spec_path("Flow Name", "spec-1") == tmp_path.resolve() / "flows" / "Flow Name" / "openapi" / "spec-1" / "spec.json"


def test_overlong_flow_and_schema_names_use_bounded_physical_segments(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    flow_name = "imported-flow-" + ("copy-" * 40)
    artifact_id = "bronze." + ("imported_flow_copy_" * 20) + "plants__history-value"

    flow_segment = content_store.flow_dir(flow_name).name
    schema_segment = content_store.schema_dir(flow_name, artifact_id).name

    assert len(flow_segment) <= content_store.MAX_PATH_SEGMENT_LENGTH
    assert len(schema_segment) <= content_store.MAX_PATH_SEGMENT_LENGTH
    assert flow_segment != flow_name
    assert schema_segment != artifact_id


class FakeUpdateResult:
    def __init__(self, modified_count=1):
        self.modified_count = modified_count


class FakeInsertResult:
    inserted_id = "inserted"


class FakeCursor:
    def __init__(self, docs):
        self.docs = list(docs)

    def sort(self, *args, **kwargs):
        return self

    async def to_list(self, length):
        if length is None:
            return list(self.docs)
        return list(self.docs)[:length]


def _field_value(doc, dotted_key):
    value = doc
    for part in dotted_key.split("."):
        if isinstance(value, list):
            values = []
            for item in value:
                nested = _field_value(item, ".".join(dotted_key.split(".")[dotted_key.split(".").index(part):]))
                if isinstance(nested, list):
                    values.extend(nested)
                elif nested is not None:
                    values.append(nested)
            return values
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def _matches(doc, query):
    for key, expected in (query or {}).items():
        actual = _field_value(doc, key)
        actual_values = actual if isinstance(actual, list) else [actual]
        if isinstance(expected, dict):
            if "$ne" in expected and any(value == expected["$ne"] for value in actual_values):
                return False
            if "$in" in expected and not any(value in expected["$in"] for value in actual_values):
                return False
        elif expected not in actual_values:
            return False
    return True


class FakeCollection:
    def __init__(self, docs=None):
        self.docs = list(docs or [])

    async def find_one(self, query, projection=None):
        for doc in self.docs:
            if _matches(doc, query):
                return dict(doc)
        return None

    def find(self, query=None, projection=None):
        return FakeCursor([dict(doc) for doc in self.docs if _matches(doc, query or {})])

    async def insert_one(self, doc):
        self.docs.append(dict(doc))
        return FakeInsertResult()

    async def delete_one(self, query):
        before = len(self.docs)
        self.docs = [doc for doc in self.docs if not _matches(doc, query)]
        return FakeUpdateResult(before - len(self.docs))

    async def update_one(self, query, update):
        for doc in self.docs:
            if not _matches(doc, query):
                continue
            if "$set" in update:
                doc.update(update["$set"])
            if "$unset" in update:
                for key in update["$unset"]:
                    doc.pop(key, None)
            if "$push" in update:
                for key, value in update["$push"].items():
                    doc.setdefault(key, []).append(value)
            return FakeUpdateResult(1)
        return FakeUpdateResult(0)

    async def update_many(self, query, update):
        modified = 0
        for doc in self.docs:
            if _matches(doc, query):
                if "$set" in update:
                    doc.update(update["$set"])
                modified += 1
        return FakeUpdateResult(modified)


class FakeDB:
    def __init__(self):
        self.flows = FakeCollection()
        self.sources = FakeCollection()
        self.application_services = FakeCollection()
        self.nifi_global_services = FakeCollection()
        self.schema_artifacts = FakeCollection()
        self.openapi_specs = FakeCollection()
        self.audit_events = FakeCollection()


def populate_flow_bundle_db():
    db = FakeDB()
    db.flows.docs.append({
        "id": "flow-1",
        "name": "Flow",
        "source_id": "source-1",
        "primary_stream_id": "users",
        "schema_artifact_id": "artifact-flow",
        "schema_version": 1,
        "nifi_process_group_id": "pg-1",
    })
    db.sources.docs.append({
        "_id": "mongo-id",
        "id": "source-1",
        "name": "REST Source",
        "type": "REST API",
        "connection_setup_mode": "application_service",
        "application_service_id": "app-1",
        "openapi_spec_id": "spec-1",
        "api_key_value": "secret",
        "nifi_service_refs": {"ssl": "nifi-1"},
        "streams": [
            {
                "id": "users",
                "name": "users",
                "entity": {
                    "schema_artifact_id": "artifact-users",
                    "schema_version": 2,
                    "kafka": {"topic": "users-topic"},
                },
            },
            {
                "id": "orders",
                "name": "orders",
                "entity": {
                    "schema_artifact_id": "artifact-orders",
                    "schema_version": 1,
                    "kafka": {"topic": "orders-topic"},
                },
            },
        ],
    })
    db.application_services.docs.append({"id": "app-1", "config": {"api_key_value": "service-secret"}})
    db.nifi_global_services.docs.extend([
        {"id": "nifi-1", "service_kind": "ssl-context"},
        {
            "id": "kafka-1",
            "name": "Default Kafka",
            "service_kind": "kafka",
            "nifi_type": "org.apache.nifi.kafka.service.Kafka3ConnectionService",
            "is_default": True,
            "properties": {"bootstrap.servers": "kafka:9092"},
        },
        {
            "id": "schema-1",
            "name": "Default Schema Registry",
            "service_kind": "schema_registry",
            "nifi_type": "org.apache.nifi.confluent.schemaregistry.ConfluentSchemaRegistry",
            "is_default": True,
            "properties": {"Schema Registry URLs": "http://apicurio:8080/apis/ccompat/v7"},
        },
    ])
    db.openapi_specs.docs.append({"id": "spec-1", "title": "API", "raw_content_gzip_b64": "abc"})
    db.schema_artifacts.docs.extend([
        {"artifact_id": "artifact-flow", "namespace": "com.nif", "versions": [{"version": 1, "status": "Verified"}]},
        {"artifact_id": "artifact-users", "namespace": "com.nif", "versions": [{"version": 2, "status": "Verified"}]},
        {"artifact_id": "artifact-orders", "namespace": "com.nif", "versions": [{"version": 1, "status": "Verified"}]},
    ])
    return db


@async_test
async def test_sync_flow_writes_self_contained_flow_bundle(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()

    await content_store.sync_flow(db, "flow-1")

    assert (tmp_path / "flows" / "Flow" / "flow.json").exists()
    assert not (tmp_path / "flows" / "flow-1").exists()
    assert not (tmp_path / "flows" / "source-1").exists()
    assert (tmp_path / "flows" / "Flow" / "source" / "source.json").exists()
    assert (tmp_path / "flows" / "Flow" / "source" / "streams" / "users.json").exists()
    assert (tmp_path / "flows" / "Flow" / "source" / "streams" / "orders.json").exists()
    assert (tmp_path / "flows" / "Flow" / "services" / "application.json").exists()
    assert (tmp_path / "flows" / "Flow" / "services" / "nifi.json").exists()
    assert not (tmp_path / "flows" / "Flow" / "services" / "application" / "app-1.json").exists()
    assert not (tmp_path / "flows" / "Flow" / "services" / "nifi" / "nifi-1.json").exists()
    assert (tmp_path / "flows" / "Flow" / "openapi" / "spec-1" / "spec.json").exists()
    assert (tmp_path / "flows" / "Flow" / "schemas" / "artifact-flow" / "versions" / "1.json").exists()
    assert (tmp_path / "flows" / "Flow" / "schemas" / "artifact-users" / "versions" / "2.json").exists()
    assert (tmp_path / "flows" / "Flow" / "schemas" / "artifact-orders" / "versions" / "1.json").exists()
    assert not (tmp_path / "sources").exists()
    assert not (tmp_path / "schemas").exists()
    assert not (tmp_path / "services").exists()
    assert not (tmp_path / "openapi").exists()


@async_test
async def test_sync_flow_writes_concise_human_editable_bundle_files(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()
    db.sources.docs[0]["designer_payload"] = {"step": 5, "streams": [{"id": "users"}]}
    db.sources.docs[0]["created_at"] = datetime(2026, 6, 1, 0, 0, 0)
    db.sources.docs[0]["connection_timeout"] = 30
    db.sources.docs[0]["rate_limit"] = 120
    db.sources.docs[0]["streams"][0].update({
        "headers": {},
        "filters": [],
        "pagination": {"enabled": False, "type": "none", "page_param": None},
        "created_at": datetime(2026, 6, 1, 0, 0, 0),
    })

    await content_store.sync_flow(db, "flow-1")

    source_payload = json.loads((tmp_path / "flows" / "Flow" / "source" / "source.json").read_text(encoding="utf-8"))
    stream_payload = json.loads((tmp_path / "flows" / "Flow" / "source" / "streams" / "users.json").read_text(encoding="utf-8"))
    assert "designer_payload" not in source_payload
    assert "created_at" not in source_payload
    assert "connection_timeout" not in source_payload
    assert "rate_limit" not in source_payload
    assert source_payload["application_service_id"] == "app-1"
    assert (tmp_path / "flows" / "Flow" / "source" / "streams" / "users.json").exists()
    assert "headers" not in stream_payload
    assert "filters" not in stream_payload
    assert "pagination" not in stream_payload
    assert "created_at" not in stream_payload


@async_test
async def test_sync_flow_extracts_sensitive_values_to_bundle_secrets_file(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()
    db.sources.docs[0]["base_url"] = "https://api.example"
    db.sources.docs[0]["rest_username"] = "api-user"

    await content_store.sync_flow(db, "flow-1")

    bundle = tmp_path / "flows" / "Flow"
    source_payload = json.loads((bundle / "source" / "source.json").read_text(encoding="utf-8"))
    service_payload = json.loads((bundle / "services" / "application.json").read_text(encoding="utf-8"))
    secrets_payload = json.loads((bundle / "secrets.json").read_text(encoding="utf-8"))

    assert source_payload["api_key_value"] == {
        "$secretRef": "source/source.json#api_key_value",
    }
    assert source_payload["base_url"] == {
        "$secretRef": "source/source.json#base_url",
    }
    assert source_payload["rest_username"] == {
        "$secretRef": "source/source.json#rest_username",
    }
    assert service_payload["services"][0]["config"]["api_key_value"] == {
        "$secretRef": "services/application.json#services.0.config.api_key_value",
    }
    assert secrets_payload == {
        "source/source.json#api_key_value": "secret",
        "source/source.json#base_url": "https://api.example",
        "source/source.json#rest_username": "api-user",
        "services/application.json#services.0.config.api_key_value": "service-secret",
        "services/nifi.json#services.1.properties.Schema Registry URLs": "http://apicurio:8080/apis/ccompat/v7",
        "services/nifi.json#services.2.properties.bootstrap.servers": "kafka:9092",
    }


@async_test
async def test_sync_flow_groups_application_and_nifi_services(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()

    await content_store.sync_flow(db, "flow-1")

    bundle = tmp_path / "flows" / "Flow"
    app_services = json.loads((bundle / "services" / "application.json").read_text(encoding="utf-8"))
    nifi_services = json.loads((bundle / "services" / "nifi.json").read_text(encoding="utf-8"))

    assert [service["id"] for service in app_services["services"]] == ["app-1"]
    assert [service["service_kind"] for service in nifi_services["services"]] == [
        "ssl-context",
        "schema_registry",
        "kafka",
    ]
    assert nifi_services["services"][1]["resolution"] == "default"
    assert nifi_services["services"][2]["resolution"] == "default"


@pytest.mark.skipif(os.name != "nt", reason="Windows path-length regression")
def test_atomic_write_json_uses_short_temp_name_for_deep_content_store_paths(monkeypatch, tmp_path):
    root = tmp_path / "content-store"
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(root))
    parent = root / "flows"
    segment_index = 0
    while len(str(parent)) < 238:
        parent = parent / f"segment-{segment_index:02d}"
        segment_index += 1
    target = parent / "1.json"

    content_store.atomic_write_json(target, {"ok": True})

    assert json.loads(target.read_text(encoding="utf-8")) == {"ok": True}


@async_test
async def test_sync_flow_removes_old_id_named_bundle_for_same_flow(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    old_bundle = tmp_path / "flows" / "source-1"
    old_bundle.mkdir(parents=True)
    (old_bundle / "flow.json").write_text('{"id":"flow-1"}', encoding="utf-8")
    db = populate_flow_bundle_db()

    await content_store.sync_flow(db, "flow-1")

    assert not old_bundle.exists()
    assert (tmp_path / "flows" / "Flow" / "flow.json").exists()


@async_test
async def test_shared_source_flow_records_create_one_bundle_per_flow_name(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()
    db.flows.docs.append({
        "id": "flow-2",
        "name": "Orders Flow",
        "source_id": "source-1",
        "primary_stream_id": "orders",
    })

    await content_store.sync_flow(db, "flow-1")
    await content_store.sync_flow(db, "flow-2")

    flow_folders = sorted(path.name for path in (tmp_path / "flows").iterdir() if path.is_dir())
    assert flow_folders == ["Flow", "Orders Flow"]
    assert (tmp_path / "flows" / "Flow" / "source" / "streams" / "users.json").exists()
    assert (tmp_path / "flows" / "Orders Flow" / "source" / "streams" / "orders.json").exists()
    assert (tmp_path / "flows" / "Flow" / "schemas" / "artifact-users").exists()
    assert (tmp_path / "flows" / "Orders Flow" / "schemas" / "artifact-orders").exists()


@async_test
async def test_flow_bundle_includes_route_child_streams(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()
    db.sources.docs[0]["streams"][0]["routes"] = [
        {"id": "route-1", "child_stream_id": "orders", "ordinal": 1},
    ]

    await content_store.sync_flow(db, "flow-1")

    assert (tmp_path / "flows" / "Flow" / "source" / "streams" / "users.json").exists()
    assert (tmp_path / "flows" / "Flow" / "source" / "streams" / "orders.json").exists()
    assert (tmp_path / "flows" / "Flow" / "schemas" / "artifact-users").exists()
    assert (tmp_path / "flows" / "Flow" / "schemas" / "artifact-orders").exists()


@async_test
async def test_sync_source_resyncs_only_flows_that_reference_source(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()
    db.flows.docs.append({"id": "flow-2", "name": "Other Flow", "source_id": "source-2"})
    db.sources.docs.append({"id": "source-2", "name": "Other", "streams": [{"id": "other"}]})

    await content_store.sync_source(db, "source-1")

    assert (tmp_path / "flows" / "Flow" / "source" / "source.json").exists()
    assert not (tmp_path / "flows" / "flow-2").exists()
    assert not (tmp_path / "sources").exists()


@async_test
async def test_sync_source_deletes_stale_stream_inside_affected_flow(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    stale = tmp_path / "flows" / "Flow" / "source" / "streams" / "old.json"
    stale.parent.mkdir(parents=True)
    stale.write_text("{}", encoding="utf-8")
    db = populate_flow_bundle_db()

    await content_store.sync_source(db, "source-1")

    assert not stale.exists()
    assert (tmp_path / "flows" / "Flow" / "source" / "streams" / "users.json").exists()


@async_test
async def test_sync_referenced_schema_resyncs_affected_flow(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()

    await content_store.sync_schema_artifact(db, "artifact-users")

    assert (tmp_path / "flows" / "Flow" / "schemas" / "artifact-users" / "artifact.json").exists()
    assert not (tmp_path / "schemas").exists()


@async_test
async def test_sync_referenced_services_and_openapi_resync_affected_flow(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()

    await content_store.sync_application_service(db, "app-1")
    await content_store.sync_nifi_global_service(db, "nifi-1")
    await content_store.sync_openapi_spec(db, "spec-1")

    assert (tmp_path / "flows" / "Flow" / "services" / "application.json").exists()
    assert (tmp_path / "flows" / "Flow" / "services" / "nifi.json").exists()
    assert (tmp_path / "flows" / "Flow" / "openapi" / "spec-1" / "spec.json").exists()
    assert not (tmp_path / "services").exists()
    assert not (tmp_path / "openapi").exists()


@async_test
async def test_rematerialize_writes_only_flow_bundles_and_manifest(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()

    report = await content_store.rematerialize_content_store(db)

    assert report.ok is True
    assert (tmp_path / "manifest.json").exists()
    assert (tmp_path / "flows" / "Flow" / "flow.json").exists()
    assert not (tmp_path / "sources").exists()
    assert not (tmp_path / "schemas").exists()
    assert not (tmp_path / "services").exists()
    assert not (tmp_path / "openapi").exists()


@async_test
async def test_rematerialize_preserves_content_store_root(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()
    stale_dir = tmp_path / "stale"
    stale_dir.mkdir()
    (stale_dir / "old.json").write_text("{}", encoding="utf-8")
    (tmp_path / "old.json").write_text("{}", encoding="utf-8")

    deleted_trees = []
    original_safe_delete_tree = content_store.safe_delete_tree

    def track_deleted_tree(path):
        deleted_trees.append(path.resolve())
        original_safe_delete_tree(path)

    monkeypatch.setattr(content_store, "safe_delete_tree", track_deleted_tree)

    report = await content_store.rematerialize_content_store(db)

    assert report.ok is True
    assert tmp_path.resolve() not in deleted_trees
    assert not stale_dir.exists()
    assert not (tmp_path / "old.json").exists()
    assert (tmp_path / "manifest.json").exists()


@async_test
async def test_rematerialize_handles_same_schema_with_specific_and_unspecified_versions(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()
    db.flows.docs[0]["schema_artifact_id"] = "artifact-users"
    db.flows.docs[0]["schema_version"] = None

    report = await content_store.rematerialize_content_store(db)

    assert report.ok is True
    assert (tmp_path / "flows" / "Flow" / "schemas" / "artifact-users" / "versions" / "2.json").exists()


@async_test
async def test_validate_content_store_detects_missing_flow_bundle_file(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()

    report = await content_store.validate_content_store(db)

    assert report.ok is False
    assert str(tmp_path / "flows" / "Flow" / "flow.json") in report.missing


@async_test
async def test_validate_content_store_marks_extra_files_unhealthy(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    db = populate_flow_bundle_db()
    await content_store.rematerialize_content_store(db)

    extra = tmp_path / "flows" / "Flow" / "unexpected.json"
    extra.write_text("{}", encoding="utf-8")

    report = await content_store.validate_content_store(db)
    data = report.to_dict()

    assert report.ok is False
    assert data["ok"] is False
    assert str(extra) in report.extra


@async_test
async def test_delete_schema_artifact_removes_copies_from_flow_bundles(monkeypatch, tmp_path):
    monkeypatch.setenv("CONTENT_STORE_ROOT", str(tmp_path))
    copied = tmp_path / "flows" / "Flow" / "schemas" / "artifact-users"
    copied.mkdir(parents=True)
    (copied / "artifact.json").write_text("{}", encoding="utf-8")

    content_store.delete_schema_artifact("artifact-users")

    assert not copied.exists()


@async_test
async def test_content_store_router_validate(monkeypatch):
    from routers import content_store as content_store_router

    class Report:
        def to_dict(self):
            return {"ok": True}

    async def fake_validate(db):
        return Report()

    monkeypatch.setattr(content_store_router.content_store, "validate_content_store", fake_validate)

    assert await content_store_router.validate_content_store_status(db=FakeDB()) == {"ok": True}


@async_test
async def test_openapi_parse_syncs_created_spec(monkeypatch):
    from routers import openapi_specs

    class Upload:
        filename = "openapi.json"
        content_type = "application/json"

        async def read(self):
            return b'{"openapi":"3.0.0","info":{"title":"API","version":"1"},"paths":{"/users":{"get":{"responses":{"200":{"description":"ok"}}}}}}'

    db = FakeDB()
    synced = []

    async def fake_sync(db_arg, spec_id):
        synced.append(spec_id)

    monkeypatch.setattr(openapi_specs.content_store, "sync_openapi_spec", fake_sync)

    result = await openapi_specs.parse_openapi(file=Upload(), db=db)

    assert result["spec_id"] in synced


@async_test
async def test_openapi_attach_syncs_source(monkeypatch):
    from routers import openapi_specs

    db = FakeDB()
    db.sources.docs.append({"id": "source-1", "type": "REST API"})
    db.openapi_specs.docs.append({"id": "spec-1", "servers": []})
    synced = []

    async def fake_sync(db_arg, source_id):
        synced.append(source_id)

    monkeypatch.setattr(openapi_specs.content_store, "sync_source", fake_sync)

    payload = openapi_specs.AttachSpecRequest(spec_id="spec-1")
    await openapi_specs.attach_openapi_to_source("source-1", payload, db=db)

    assert synced == ["source-1"]


@async_test
async def test_clear_stale_pg_reference_syncs_flow(monkeypatch):
    from routers import flows

    synced = []

    async def fake_sync(db_arg, flow_id):
        synced.append(flow_id)

    async def fake_log(*args, **kwargs):
        return None

    db = FakeDB()
    db.flows.docs.append({"id": "flow-1", "state": "Running", "nifi_process_group_id": "pg-1"})
    monkeypatch.setattr(flows.content_store, "sync_flow", fake_sync)
    monkeypatch.setattr(flows, "_log_audit", fake_log)

    await flows._clear_stale_nifi_pg_reference(db.flows.docs[0], db, reason="test")

    assert synced == ["flow-1"]


@async_test
async def test_application_service_create_syncs(monkeypatch):
    from models.application_service import ApplicationServiceCreate
    from services import application_services

    db = FakeDB()
    synced = []

    async def fake_sync(db_arg, service_id):
        synced.append(service_id)

    monkeypatch.setattr(application_services.content_store, "sync_application_service", fake_sync)

    payload = ApplicationServiceCreate(
        name="HTTP",
        service_type="REST API",
        config={"base_url": "https://example.com", "auth_type": "NONE"},
    )
    result = await application_services.create_application_service(db, payload)

    assert result["id"] in synced


@async_test
async def test_application_service_delete_blocks_orphan_draft_source(monkeypatch):
    from fastapi import HTTPException
    from services import application_services

    db = FakeDB()
    db.application_services.docs.append({"id": "app-1", "name": "HTTP", "service_type": "REST API"})
    db.sources.docs.append({
        "id": "source-1",
        "name": "orphan_import_draft",
        "type": "REST API",
        "application_service_id": "app-1",
        "state": "Draft",
    })

    with pytest.raises(HTTPException) as exc:
        await application_services.delete_application_service(db, "app-1")

    assert exc.value.status_code == 409
    assert db.application_services.docs
    assert db.sources.docs


@async_test
async def test_application_service_delete_still_blocks_real_flow_reference(monkeypatch):
    from fastapi import HTTPException
    from services import application_services

    db = FakeDB()
    db.application_services.docs.append({"id": "app-1", "name": "HTTP", "service_type": "REST API"})
    db.sources.docs.append({
        "id": "source-1",
        "name": "linked_source",
        "type": "REST API",
        "application_service_id": "app-1",
        "state": "Draft",
    })
    db.flows.docs.append({
        "id": "flow-1",
        "name": "linked_flow",
        "source_id": "source-1",
        "state": "Stopped",
    })

    with pytest.raises(HTTPException) as exc:
        await application_services.delete_application_service(db, "app-1")

    assert exc.value.status_code == 409
    assert db.application_services.docs
    assert db.sources.docs


@async_test
async def test_unlink_source_streams_syncs_changed_source(monkeypatch):
    from routers import schemas

    db = FakeDB()
    db.sources.docs.append({
        "id": "source-1",
        "streams": [
            {"id": "s1", "entity": {"schema_artifact_id": "artifact-1", "schema_version": 1, "kafka": {}}},
        ],
    })
    synced = []

    async def fake_sync(db_arg, source_id):
        synced.append(source_id)

    monkeypatch.setattr(schemas.content_store, "sync_source", fake_sync)

    count = await schemas._unlink_source_streams_for_schema(db, "artifact-1", 1)

    assert count == 1
    assert synced == ["source-1"]
