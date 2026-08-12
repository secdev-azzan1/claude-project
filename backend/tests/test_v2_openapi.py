"""Tests for the v2 OpenAPI subsystem (backend/routers/v2/openapi.py).

Uses FastAPI's TestClient against the real `server.app`, with the Mongo
dependency (`db.get_db`) overridden by an in-memory FakeDB built on the same
FaultInjectingCollection helper used by the resilience test suite.

NOTE: `server.app`'s startup event connects to a real Mongo instance. We
never use `TestClient(app)` as a context manager here, since Starlette only
runs lifespan/startup handlers when the client is used as a context manager
-- a plain `TestClient(app)` never triggers `init_db()`, so no real Mongo
connection is attempted.
"""
import json
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from fastapi.testclient import TestClient

from db import get_db
from server import app
from tests.resilience.conftest import FaultInjectingCollection


class FakeDB:
    def __init__(self):
        self.openapi_specs_v2 = FaultInjectingCollection()


SMALL_OAS3_DOC = {
    "openapi": "3.0.0",
    "info": {"title": "Pet Store", "version": "1.2.3"},
    "servers": [{"url": "https://api.example.com", "description": "prod"}],
    "paths": {
        "/pets": {
            "get": {
                "operationId": "listPets",
                "summary": "List pets",
                "tags": ["pets"],
                "parameters": [
                    {
                        "name": "limit",
                        "in": "query",
                        "required": False,
                        "schema": {"type": "integer", "default": 10},
                    }
                ],
                "responses": {"200": {"description": "ok"}},
            },
            "post": {
                "operationId": "createPet",
                "summary": "Create a pet",
                "tags": ["pets"],
                "responses": {"201": {"description": "created"}},
            },
        },
        "/owners/{ownerId}": {
            "get": {
                "operationId": "getOwner",
                "summary": "Get owner",
                "tags": ["owners"],
                "parameters": [
                    {
                        "name": "ownerId",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "string"},
                    }
                ],
                "responses": {"200": {"description": "ok"}},
            }
        },
    },
}


def _small_doc_bytes() -> bytes:
    return json.dumps(SMALL_OAS3_DOC).encode("utf-8")


def _make_client(fake_db: FakeDB) -> TestClient:
    app.dependency_overrides[get_db] = lambda: fake_db
    return TestClient(app)


def _clear_overrides():
    app.dependency_overrides.pop(get_db, None)


def test_parse_happy_path_returns_camelcase_summary():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        resp = client.post(
            "/api/v2/openapi/parse",
            files={"file": ("petstore.json", _small_doc_bytes(), "application/json")},
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()

        assert set(body.keys()) == {
            "specId",
            "title",
            "version",
            "format",
            "openapiVersion",
            "operationsCount",
            "servers",
            "warnings",
        }
        assert body["title"] == "Pet Store"
        assert body["version"] == "1.2.3"
        assert body["format"] == "openapi3"
        assert body["openapiVersion"] == "3.0.0"
        assert body["operationsCount"] == 3
        assert body["servers"] == [{"url": "https://api.example.com", "description": "prod"}]
        assert body["warnings"] == []
        assert isinstance(body["specId"], str) and body["specId"]

        # Stored in the v2-specific collection.
        assert len(fake_db.openapi_specs_v2.docs) == 1
        stored = fake_db.openapi_specs_v2.docs[0]
        assert stored["id"] == body["specId"]
        assert "raw_content_gzip_b64" in stored
    finally:
        _clear_overrides()


def test_parse_rejects_yaml_with_clear_reason():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        resp = client.post(
            "/api/v2/openapi/parse",
            files={"file": ("petstore.yaml", b"openapi: 3.0.0\ninfo:\n  title: X\n", "application/x-yaml")},
        )
        assert resp.status_code == 422, resp.text
        detail = resp.json()["detail"]
        assert "YAML" in detail
        assert fake_db.openapi_specs_v2.docs == []
    finally:
        _clear_overrides()


def test_parse_rejects_over_5mb_before_parsing():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        oversized = b"x" * (5 * 1024 * 1024 + 1)
        resp = client.post(
            "/api/v2/openapi/parse",
            files={"file": ("huge.json", oversized, "application/json")},
        )
        assert resp.status_code == 413, resp.text
        detail = resp.json()["detail"]
        assert "too large" in detail.lower()
        assert "5" in detail
        # Nothing should have been stored, and the (invalid-JSON) body must
        # never have reached the parser.
        assert fake_db.openapi_specs_v2.docs == []
    finally:
        _clear_overrides()


def test_parse_checksum_dedupe_returns_same_spec_id():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        raw = _small_doc_bytes()
        resp1 = client.post(
            "/api/v2/openapi/parse",
            files={"file": ("petstore.json", raw, "application/json")},
        )
        resp2 = client.post(
            "/api/v2/openapi/parse",
            files={"file": ("petstore-again.json", raw, "application/json")},
        )
        assert resp1.status_code == 201
        assert resp2.status_code == 201
        assert resp1.json()["specId"] == resp2.json()["specId"]
        # Only one document should ever be persisted.
        assert len(fake_db.openapi_specs_v2.docs) == 1
    finally:
        _clear_overrides()


def test_get_spec_summary_matches_parse_response():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        parsed = client.post(
            "/api/v2/openapi/parse",
            files={"file": ("petstore.json", _small_doc_bytes(), "application/json")},
        ).json()

        resp = client.get(f"/api/v2/openapi/{parsed['specId']}")
        assert resp.status_code == 200, resp.text
        assert resp.json() == parsed
    finally:
        _clear_overrides()


def test_operations_search_filters_by_path_substring():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        spec_id = client.post(
            "/api/v2/openapi/parse",
            files={"file": ("petstore.json", _small_doc_bytes(), "application/json")},
        ).json()["specId"]

        resp = client.get(f"/api/v2/openapi/{spec_id}/operations", params={"search": "pets"})
        assert resp.status_code == 200, resp.text
        body = resp.json()

        assert body["total"] == 2
        assert body["page"] == 1
        assert body["pageSize"] == 25
        paths = {item["path"] for item in body["items"]}
        assert paths == {"/pets"}

        for item in body["items"]:
            assert set(item.keys()) == {"operationId", "method", "path", "summary", "tags", "parameters"}
            for param in item["parameters"]:
                assert set(param.keys()) == {"name", "location", "required", "schemaType", "default"}

        # Unmatched search returns no items.
        resp_none = client.get(f"/api/v2/openapi/{spec_id}/operations", params={"search": "nonexistent"})
        assert resp_none.json()["total"] == 0
        assert resp_none.json()["items"] == []
    finally:
        _clear_overrides()


def test_operations_pagination_page_size():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        spec_id = client.post(
            "/api/v2/openapi/parse",
            files={"file": ("petstore.json", _small_doc_bytes(), "application/json")},
        ).json()["specId"]

        resp = client.get(
            f"/api/v2/openapi/{spec_id}/operations",
            params={"page": 1, "pageSize": 2},
        )
        body = resp.json()
        assert body["total"] == 3
        assert len(body["items"]) == 2
        assert body["pageSize"] == 2

        resp_page2 = client.get(
            f"/api/v2/openapi/{spec_id}/operations",
            params={"page": 2, "pageSize": 2},
        )
        assert len(resp_page2.json()["items"]) == 1
    finally:
        _clear_overrides()


def test_operation_detail_returns_full_operation():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        spec_id = client.post(
            "/api/v2/openapi/parse",
            files={"file": ("petstore.json", _small_doc_bytes(), "application/json")},
        ).json()["specId"]

        resp = client.get(f"/api/v2/openapi/{spec_id}/operations/getOwner")
        assert resp.status_code == 200, resp.text
        body = resp.json()

        assert body["operationId"] == "getOwner"
        assert body["method"] == "GET"
        assert body["path"] == "/owners/{ownerId}"
        assert body["parameters"][0]["name"] == "ownerId"
        assert body["parameters"][0]["location"] == "path"
        assert body["parameters"][0]["required"] is True

        resp_missing = client.get(f"/api/v2/openapi/{spec_id}/operations/doesNotExist")
        assert resp_missing.status_code == 404
    finally:
        _clear_overrides()


def test_get_spec_and_operations_404_for_unknown_spec():
    fake_db = FakeDB()
    client = _make_client(fake_db)
    try:
        assert client.get("/api/v2/openapi/does-not-exist").status_code == 404
        assert client.get("/api/v2/openapi/does-not-exist/operations").status_code == 404
        assert client.get("/api/v2/openapi/does-not-exist/operations/op").status_code == 404
    finally:
        _clear_overrides()
