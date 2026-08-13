"""Tests for the v2 Schemas subsystem (backend/routers/v2/schemas.py).

Uses FastAPI's TestClient against the real `server.app`, with the Mongo
dependency (`db.get_db`) overridden by an in-memory FakeDB built on the same
FaultInjectingCollection helper used by the resilience test suite (see
tests/test_v2_openapi.py for the pattern this mirrors).

`server.py` does not (and, per this task, must not) `include_router` the v2
schemas router yet, so this test module registers it onto the shared
`server.app` once at import time -- the same TestClient instance every other
v2 test file uses, just with one more router mounted on it.

`services.apicurio_client.register_schema` and the module's own `httpx`
import are monkeypatched per-test so no real network calls are made: the
"NEVER touch a real registry in tests" contract from the openapi v2 suite
applies here too.
"""
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from fastapi.testclient import TestClient

from db import get_db
from server import app
from routers.v2 import schemas as v2_schemas
from tests.resilience.conftest import FaultInjectingCollection

if not any(getattr(r, "path", "").startswith(v2_schemas.router.prefix) for r in app.routes):
    app.include_router(v2_schemas.router)


class FakeDB:
    def __init__(self):
        self.schemas_v2 = FaultInjectingCollection()
        self.schema_templates_v2 = FaultInjectingCollection()
        self.connections_v2 = FaultInjectingCollection()
        self.flows_v2 = FaultInjectingCollection()
        self.audit_v2 = FaultInjectingCollection()

    def __getitem__(self, name):
        return getattr(self, name)


def _make_client(fake_db: FakeDB) -> TestClient:
    app.dependency_overrides[get_db] = lambda: fake_db
    return TestClient(app)


def _clear_overrides():
    app.dependency_overrides.pop(get_db, None)


def make_apicurio_connection(**over) -> Dict[str, Any]:
    doc = {
        "id": "conn-apicurio",
        "type": "apicurio",
        "active": True,
        "name": "Apicurio Schema Registry",
        "config": {"url": "http://apicurio.test:8080", "authMode": "none"},
    }
    doc.update(over)
    return doc


def make_avro(field_type: Optional[Any] = None) -> Dict[str, Any]:
    return {
        "type": "record",
        "name": "Order",
        "namespace": "com.nif",
        "fields": [
            {"name": "id", "type": "string"},
            # Deliberately null-NOT-first, no default -- exercises the normalizer.
            {"name": "amount", "type": field_type or ["string", "null"]},
        ],
    }


def _install_register_schema(monkeypatch, results: Optional[List[Dict[str, Any]]] = None):
    """Monkeypatch services.apicurio_client.register_schema (imported into
    the router module) with a fake that records every call and, absent an
    explicit `results` queue, hands out incrementing global ids."""
    calls: List[Dict[str, Any]] = []
    queue = list(results) if results is not None else None
    counter = {"n": 100}

    async def fake_register(**kwargs):
        calls.append(kwargs)
        if queue:
            return queue.pop(0)
        counter["n"] += 1
        return {"ok": True, "api_version": "v3", "global_id": counter["n"], "version": 1, "ccompat_id": counter["n"]}

    monkeypatch.setattr(v2_schemas, "register_schema", fake_register)
    return calls


class FakeResponse:
    def __init__(self, status_code: int, json_data: Any = None, text: str = ""):
        self.status_code = status_code
        self._json = json_data if json_data is not None else {}
        self.text = text

    def json(self):
        return self._json


def install_fake_httpx(monkeypatch, calls: List[Dict[str, Any]], responses: Optional[List[FakeResponse]] = None, default: Optional[FakeResponse] = None):
    """Monkeypatch v2_schemas.httpx.AsyncClient. Each call records into
    `calls` and pops the next canned response off `responses` (falling back
    to `default`, or a bare 200, once the queue is drained)."""
    queue = list(responses or [])
    fallback = default if default is not None else FakeResponse(200, {})

    class _FakeClient:
        def __init__(self, *a, **kw):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def _handle(self, method, url, **kw):
            calls.append({"method": method, "url": url, **kw})
            return queue.pop(0) if queue else fallback

        async def post(self, url, **kw):
            return await self._handle("POST", url, **kw)

        async def delete(self, url, **kw):
            return await self._handle("DELETE", url, **kw)

        async def get(self, url, **kw):
            return await self._handle("GET", url, **kw)

    monkeypatch.setattr(v2_schemas.httpx, "AsyncClient", _FakeClient)


def _approve(client: TestClient, **overrides) -> Any:
    body = {
        "flowId": "flow-1",
        "blockId": "block-1",
        "entity": "Orders",
        "topic": "raw.orders",
        "subject": "raw.orders-value",
        "avro": make_avro(),
        "provenance": "sample_run",
    }
    body.update(overrides)
    return client.post("/api/v2/schemas/approve", json=body)


# ------------------------------------------------------------------- approve


def test_approve_happy_path_registers_and_appends_history(monkeypatch):
    fake_db = FakeDB()
    fake_db.connections_v2.docs.append(make_apicurio_connection())
    calls = _install_register_schema(monkeypatch)
    client = _make_client(fake_db)
    try:
        resp = _approve(client)
        assert resp.status_code == 200, resp.text
        body = resp.json()

        # Normalizer: null-first union, default null.
        amount_field = next(f for f in body["avro"]["fields"] if f["name"] == "amount")
        assert amount_field["type"] == ["null", "string"]
        assert amount_field["default"] is None

        assert len(body["approvals"]) == 1
        assert body["approvals"][0]["version"] == 1
        assert body["registryGlobalId"] == body["approvals"][0]["registryGlobalId"]
        assert body["subject"] == "raw.orders-value"
        assert body["draftAvro"] is None

        assert len(calls) == 1
        assert calls[0]["artifact_id"] == "raw.orders-value"
        # E6: every v2 call site registers ccompat-only (exactly one ccompat
        # version consumed per approval) -- see services/apicurio_client.py's
        # register_schema(ccompat_only=...) and tests/test_apicurio_client.py.
        assert calls[0]["ccompat_only"] is True

        assert len(fake_db.schemas_v2.docs) == 1
        assert any(e["action"] == "Approved & registered" for e in fake_db.audit_v2.docs)

        # Re-approving the identical avro is an idempotent no-op: no new
        # registration call, no new approval, same record back.
        resp2 = _approve(client)
        assert resp2.status_code == 200, resp2.text
        assert len(calls) == 1
        body2 = resp2.json()
        assert body2 == body
        assert len(fake_db.schemas_v2.docs) == 1
    finally:
        _clear_overrides()


def test_approve_fails_when_registration_fails_no_doc_written(monkeypatch):
    fake_db = FakeDB()
    fake_db.connections_v2.docs.append(make_apicurio_connection())

    async def fake_register_fail(**kwargs):
        return {"ok": False, "error": "registry unreachable"}

    monkeypatch.setattr(v2_schemas, "register_schema", fake_register_fail)
    client = _make_client(fake_db)
    try:
        resp = _approve(client, flowId="flow-x", blockId="block-x", topic="raw.x", subject="raw.x-value")
        assert resp.status_code == 502, resp.text
        assert "registry unreachable" in resp.json()["detail"]
        assert fake_db.schemas_v2.docs == []
        assert any(e["action"] == "Schema approval failed" and e["status"] == "Failed" for e in fake_db.audit_v2.docs)
    finally:
        _clear_overrides()


def test_approve_fails_without_active_apicurio_connection(monkeypatch):
    fake_db = FakeDB()  # no connections_v2 docs at all
    calls = _install_register_schema(monkeypatch)
    client = _make_client(fake_db)
    try:
        resp = _approve(client)
        assert resp.status_code == 502
        assert calls == []
        assert fake_db.schemas_v2.docs == []
    finally:
        _clear_overrides()


def test_approve_rejects_non_record_root(monkeypatch):
    fake_db = FakeDB()
    fake_db.connections_v2.docs.append(make_apicurio_connection())
    _install_register_schema(monkeypatch)
    client = _make_client(fake_db)
    try:
        resp = _approve(client, avro={"type": "string"})
        assert resp.status_code == 422
        assert "record" in resp.json()["detail"].lower()
        assert fake_db.schemas_v2.docs == []
    finally:
        _clear_overrides()


# -------------------------------------------------------------------- verify


def test_verify_structural_failure_reports_issues():
    client = _make_client(FakeDB())
    try:
        resp = client.post("/api/v2/schemas/verify", json={"avro": {"type": "string"}})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["ok"] is False
        assert body["issues"]
        assert body["compatibility"]["checked"] is False
    finally:
        _clear_overrides()


def test_verify_compat_check_called_and_404_subject_is_compatible(monkeypatch):
    fake_db = FakeDB()
    fake_db.connections_v2.docs.append(make_apicurio_connection())
    calls: List[Dict[str, Any]] = []
    install_fake_httpx(monkeypatch, calls, responses=[FakeResponse(200, {"is_compatible": True}), FakeResponse(404)])
    client = _make_client(fake_db)
    try:
        resp1 = client.post(
            "/api/v2/schemas/verify",
            json={"avro": make_avro(["null", "string"]), "subject": "raw.orders-value"},
        )
        assert resp1.status_code == 200, resp1.text
        body1 = resp1.json()
        assert body1["ok"] is True
        assert body1["compatibility"] == {
            "checked": True,
            "compatible": True,
            "message": "Compatible with the latest registered version.",
        }
        assert len(calls) == 1
        assert "compatibility/subjects/raw.orders-value/versions/latest" in calls[0]["url"]

        resp2 = client.post(
            "/api/v2/schemas/verify",
            json={"avro": make_avro(["null", "string"]), "subject": "brand.new-value"},
        )
        assert resp2.status_code == 200, resp2.text
        body2 = resp2.json()
        assert body2["compatibility"] == {
            "checked": True,
            "compatible": True,
            "message": "New subject — nothing to be compatible with.",
        }
        assert len(calls) == 2
    finally:
        _clear_overrides()


def test_verify_no_active_connection_skips_compat_check():
    client = _make_client(FakeDB())
    try:
        resp = client.post(
            "/api/v2/schemas/verify",
            json={"avro": make_avro(["null", "string"]), "subject": "raw.orders-value"},
        )
        body = resp.json()
        assert body["ok"] is True
        assert body["compatibility"]["checked"] is False
    finally:
        _clear_overrides()


# ------------------------------------------------------------------ register


def test_register_standalone_and_template_linking(monkeypatch):
    fake_db = FakeDB()
    fake_db.connections_v2.docs.append(make_apicurio_connection())
    # ccompat's "version" sometimes round-trips as a numeric string -- (a)
    # exercises int coercion end to end, both on the response and the doc.
    calls = _install_register_schema(monkeypatch, results=[{"ok": True, "global_id": 77, "version": "3"}])
    client = _make_client(fake_db)
    try:
        tpl_resp = client.post("/api/v2/schemas/templates", json={"name": "Order Template", "avro": make_avro()})
        assert tpl_resp.status_code == 201, tpl_resp.text
        tpl_id = tpl_resp.json()["id"]

        resp = client.post(
            "/api/v2/schemas/register",
            json={"subject": "raw.orders-value", "avro": make_avro(), "templateId": tpl_id},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["globalId"] == 77
        assert body["subject"] == "raw.orders-value"
        assert body["version"] == 3
        assert isinstance(body["version"], int)
        assert body.get("registeredAt")
        assert calls[0]["artifact_id"] == "raw.orders-value"
        assert calls[0]["ccompat_only"] is True  # E6 — see approve's equivalent assertion.

        tpl_doc = next(d for d in fake_db.schema_templates_v2.docs if d["id"] == tpl_id)
        assert tpl_doc["registeredSubject"] == "raw.orders-value"
        assert tpl_doc["registryGlobalId"] == 77
        assert tpl_doc["registeredVersion"] == 3
        assert isinstance(tpl_doc["registeredVersion"], int)
        assert tpl_doc.get("registeredAt")
        assert any(e["action"] == "Schema registered" for e in fake_db.audit_v2.docs)
    finally:
        _clear_overrides()


def test_register_reregister_refreshes_all_four_fields(monkeypatch):
    """(b) Re-registering under the same subject: CHANGED avro must land a
    NEW global id + version on the template doc (ccompat's job -- we just
    persist whatever it hands back); a further re-register with the SAME
    (unchanged) avro is idempotent at the registry -- same version comes
    back -- and must persist cleanly with no error."""
    fake_db = FakeDB()
    fake_db.connections_v2.docs.append(make_apicurio_connection())
    calls = _install_register_schema(
        monkeypatch,
        results=[
            {"ok": True, "global_id": 77, "version": 1},
            {"ok": True, "global_id": 78, "version": 2},  # changed avro -> new version
            {"ok": True, "global_id": 78, "version": 2},  # unchanged avro -> same version, idempotent
        ],
    )
    client = _make_client(fake_db)
    try:
        tpl_resp = client.post("/api/v2/schemas/templates", json={"name": "Order Template", "avro": make_avro()})
        tpl_id = tpl_resp.json()["id"]

        first = client.post(
            "/api/v2/schemas/register",
            json={"subject": "raw.orders-value", "avro": make_avro(), "templateId": tpl_id},
        )
        assert first.status_code == 200, first.text
        assert first.json()["version"] == 1

        tpl_doc_1 = next(d for d in fake_db.schema_templates_v2.docs if d["id"] == tpl_id)
        assert tpl_doc_1["registryGlobalId"] == 77
        assert tpl_doc_1["registeredVersion"] == 1
        first_registered_at = tpl_doc_1["registeredAt"]

        changed_avro = make_avro(["null", "long"])
        second = client.post(
            "/api/v2/schemas/register",
            json={"subject": "raw.orders-value", "avro": changed_avro, "templateId": tpl_id},
        )
        assert second.status_code == 200, second.text
        body2 = second.json()
        assert body2["globalId"] == 78
        assert body2["version"] == 2
        assert body2.get("registeredAt")

        tpl_doc_2 = next(d for d in fake_db.schema_templates_v2.docs if d["id"] == tpl_id)
        assert tpl_doc_2["registryGlobalId"] == 78
        assert tpl_doc_2["registeredVersion"] == 2
        assert tpl_doc_2["registeredSubject"] == "raw.orders-value"
        assert tpl_doc_2["registeredAt"] >= first_registered_at

        # Re-register the SAME (unchanged) avro again -- registry idempotent,
        # same version/global id come back, no error, doc still refreshed.
        third = client.post(
            "/api/v2/schemas/register",
            json={"subject": "raw.orders-value", "avro": changed_avro, "templateId": tpl_id},
        )
        assert third.status_code == 200, third.text
        assert third.json()["globalId"] == 78
        assert third.json()["version"] == 2

        tpl_doc_3 = next(d for d in fake_db.schema_templates_v2.docs if d["id"] == tpl_id)
        assert tpl_doc_3["registryGlobalId"] == 78
        assert tpl_doc_3["registeredVersion"] == 2
        assert len(calls) == 3
    finally:
        _clear_overrides()


def test_list_templates_carries_registration_fields(monkeypatch):
    """(c) GET /api/v2/schemas/ round-trips all four registration fields for
    a registered template (passthrough serialization -- no projection should
    drop any of them)."""
    fake_db = FakeDB()
    fake_db.connections_v2.docs.append(make_apicurio_connection())
    _install_register_schema(monkeypatch, results=[{"ok": True, "global_id": 55, "version": "7"}])
    client = _make_client(fake_db)
    try:
        tpl_resp = client.post("/api/v2/schemas/templates", json={"name": "Ledger Template", "avro": make_avro()})
        tpl_id = tpl_resp.json()["id"]

        reg = client.post(
            "/api/v2/schemas/register",
            json={"subject": "raw.ledger-value", "avro": make_avro(), "templateId": tpl_id},
        )
        assert reg.status_code == 200, reg.text

        listing = client.get("/api/v2/schemas/")
        assert listing.status_code == 200, listing.text
        tpl = next(t for t in listing.json()["templates"] if t["id"] == tpl_id)
        assert tpl["registeredSubject"] == "raw.ledger-value"
        assert tpl["registryGlobalId"] == 55
        assert tpl["registeredVersion"] == 7
        assert isinstance(tpl["registeredVersion"], int)
        assert tpl.get("registeredAt")
    finally:
        _clear_overrides()


# ---------------------------------------------------------------- templates


def test_template_crud():
    client = _make_client(FakeDB())
    try:
        create = client.post("/api/v2/schemas/templates", json={"name": "T1", "description": "d"})
        assert create.status_code == 201, create.text
        tpl_id = create.json()["id"]

        save = client.put(f"/api/v2/schemas/templates/{tpl_id}", json={"name": "T1 renamed"})
        assert save.status_code == 200, save.text
        assert save.json()["name"] == "T1 renamed"

        listing = client.get("/api/v2/schemas/")
        assert listing.status_code == 200
        assert any(t["id"] == tpl_id for t in listing.json()["templates"])

        delete = client.delete(f"/api/v2/schemas/templates/{tpl_id}")
        assert delete.status_code == 200

        listing2 = client.get("/api/v2/schemas/")
        assert listing2.json()["templates"] == []
    finally:
        _clear_overrides()


# ------------------------------------------------------------------- draft


def test_save_approved_schema_draft(monkeypatch):
    fake_db = FakeDB()
    fake_db.connections_v2.docs.append(make_apicurio_connection())
    _install_register_schema(monkeypatch)
    client = _make_client(fake_db)
    try:
        approved = _approve(client).json()
        schema_id = approved["id"]

        draft_avro = {"type": "record", "name": "Draft", "fields": []}
        resp = client.post(f"/api/v2/schemas/{schema_id}/draft", json={"avro": draft_avro})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["draftAvro"] == draft_avro
        assert body["draftUpdatedAt"]
        # Never touches approvals / the current avro / registry global id.
        assert body["avro"] == approved["avro"]
        assert body["approvals"] == approved["approvals"]
        assert body["registryGlobalId"] == approved["registryGlobalId"]
        assert any(e["action"] == "Schema draft saved" for e in fake_db.audit_v2.docs)

        missing = client.post("/api/v2/schemas/does-not-exist/draft", json={"avro": {}})
        assert missing.status_code == 404
    finally:
        _clear_overrides()


# --------------------------------------------------------------- delete v.


def test_delete_version_refuses_last_and_promotes_new_latest(monkeypatch):
    fake_db = FakeDB()
    fake_db.connections_v2.docs.append(make_apicurio_connection())
    _install_register_schema(monkeypatch)
    calls: List[Dict[str, Any]] = []
    install_fake_httpx(monkeypatch, calls, default=FakeResponse(200))
    client = _make_client(fake_db)
    try:
        a1 = _approve(client, avro=make_avro(["null", "string"])).json()
        schema_id = a1["id"]

        # Only one version so far -- refuse.
        refusal = client.delete(f"/api/v2/schemas/{schema_id}/versions/1")
        assert refusal.status_code == 400
        assert "only remaining version" in refusal.json()["detail"]

        a2 = _approve(client, avro=make_avro(["null", "long"])).json()
        assert len(a2["approvals"]) == 2

        deleted = client.delete(f"/api/v2/schemas/{schema_id}/versions/1")
        assert deleted.status_code == 200, deleted.text
        body = deleted.json()
        assert body["registryDeleted"] is True
        assert len(calls) == 1
        assert calls[0]["url"].endswith("/subjects/raw.orders-value/versions/1")
        assert [a["version"] for a in body["approvals"]] == [2]
        # v2 was already current -- deleting v1 (non-current) leaves it untouched.
        assert body["registryGlobalId"] == a2["registryGlobalId"]

        # Back down to one version -- refuse again.
        refusal2 = client.delete(f"/api/v2/schemas/{schema_id}/versions/2")
        assert refusal2.status_code == 400
        assert any(e["action"] == "Schema version deleted" for e in fake_db.audit_v2.docs)
    finally:
        _clear_overrides()


def test_delete_version_promotes_new_latest_when_current_removed(monkeypatch):
    fake_db = FakeDB()
    fake_db.connections_v2.docs.append(make_apicurio_connection())
    _install_register_schema(monkeypatch)
    calls: List[Dict[str, Any]] = []
    install_fake_httpx(monkeypatch, calls, default=FakeResponse(200))
    client = _make_client(fake_db)
    try:
        _approve(client, avro=make_avro(["null", "string"]))
        a2 = _approve(client, avro=make_avro(["null", "long"])).json()
        a3 = _approve(client, avro=make_avro(["null", "boolean"])).json()
        schema_id = a3["id"]
        assert len(a3["approvals"]) == 3

        resp = client.delete(f"/api/v2/schemas/{schema_id}/versions/3")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert [a["version"] for a in body["approvals"]] == [1, 2]
        assert body["approvals"][-1]["supersededAt"] is None
        assert body["registryGlobalId"] == a2["registryGlobalId"]
        assert body["avro"] == a2["avro"]
    finally:
        _clear_overrides()


# --------------------------------------------------------------- delete all


def test_delete_whole_schema_and_deployed_flow_guard(monkeypatch):
    fake_db = FakeDB()
    fake_db.connections_v2.docs.append(make_apicurio_connection())
    _install_register_schema(monkeypatch)
    calls: List[Dict[str, Any]] = []
    install_fake_httpx(monkeypatch, calls, default=FakeResponse(200))
    client = _make_client(fake_db)
    try:
        approved = _approve(client).json()
        schema_id = approved["id"]

        resp = client.delete(f"/api/v2/schemas/{schema_id}")
        assert resp.status_code == 200, resp.text
        assert resp.json() == {"ok": True, "registryDeleted": True}
        assert fake_db.schemas_v2.docs == []
        assert any(e["action"] == "Schema deleted" for e in fake_db.audit_v2.docs)

        # Deployed flow whose kafka_kc block still references the schema blocks deletion.
        approved2 = _approve(
            client, flowId="flow-2", blockId="block-2", topic="raw.other", subject="raw.other-value"
        ).json()
        schema_id2 = approved2["id"]
        fake_db.flows_v2.docs.append({
            "id": "flow-2",
            "name": "Other Flow",
            "deployedAt": "2026-01-01T00:00:00.000Z",
            "blocks": [{"id": "block-2", "adapter": "kafka_kc"}],
        })
        blocked = client.delete(f"/api/v2/schemas/{schema_id2}")
        assert blocked.status_code == 409
        assert any(d["id"] == schema_id2 for d in fake_db.schemas_v2.docs)
    finally:
        _clear_overrides()


# ------------------------------------------------------------------- infer


def test_infer_from_csv_json_ndjson():
    client = _make_client(FakeDB())
    try:
        csv_bytes = b"id,name\n1,Alice\n2,Bob\n"
        json_bytes = json.dumps({"id": 3, "name": "Carol"}).encode()
        ndjson_bytes = (json.dumps({"id": 4, "name": "Dave"}) + "\n" + json.dumps({"id": 5, "name": "Eve"}) + "\n").encode()

        resp = client.post(
            "/api/v2/schemas/infer",
            files=[
                ("files", ("people.csv", csv_bytes, "text/csv")),
                ("files", ("person.json", json_bytes, "application/json")),
                ("files", ("people.ndjson", ndjson_bytes, "application/x-ndjson")),
            ],
            data={"name": "Person", "namespace": "com.test"},
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()

        assert body["report"]["recordCount"] == 5
        assert body["report"]["fieldCount"] == 2
        assert len(body["report"]["notes"]) == 3

        avro = body["avro"]
        assert avro["type"] == "record"
        field_names = {f["name"] for f in avro["fields"]}
        assert field_names == {"id", "name"}
        for f in avro["fields"]:
            assert isinstance(f["type"], list)
            assert f["type"][0] == "null"
            assert f["default"] is None
    finally:
        _clear_overrides()


def test_infer_record_path_resolution_and_suggestion():
    client = _make_client(FakeDB())
    try:
        nested = {"data": {"items": [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]}}

        resolved = client.post(
            "/api/v2/schemas/infer",
            files=[("files", ("nested.json", json.dumps(nested).encode(), "application/json"))],
            data={"recordPath": "$.data.items[*]"},
        )
        assert resolved.status_code == 200, resolved.text
        body = resolved.json()
        assert body["report"]["recordCount"] == 2
        assert {f["name"] for f in body["avro"]["fields"]} == {"id", "name"}

        unresolved = client.post(
            "/api/v2/schemas/infer",
            files=[("files", ("nested.json", json.dumps(nested).encode(), "application/json"))],
        )
        assert unresolved.status_code == 200, unresolved.text
        assert "$.data.items[*]" in unresolved.json()["suggestedPaths"]
    finally:
        _clear_overrides()


def test_infer_requires_at_least_one_file():
    client = _make_client(FakeDB())
    try:
        resp = client.post("/api/v2/schemas/infer", files=[], data={})
        assert resp.status_code in (400, 422)
    finally:
        _clear_overrides()


# ---------------------------------------------------- registry version browsing


def test_list_registry_subject_versions_ascending(monkeypatch):
    fake_db = FakeDB()
    fake_db.connections_v2.docs.append(make_apicurio_connection())
    calls: List[Dict[str, Any]] = []
    # Deliberately out of order + one numeric-string entry, to exercise both
    # the sort and _coerce_int.
    install_fake_httpx(monkeypatch, calls, default=FakeResponse(200, [3, "1", 2]))
    client = _make_client(fake_db)
    try:
        resp = client.get("/api/v2/schemas/registry-subject/raw.orders-value/versions")
        assert resp.status_code == 200, resp.text
        assert resp.json() == [{"version": 1}, {"version": 2}, {"version": 3}]
        assert len(calls) == 1
        assert calls[0]["method"] == "GET"
        assert calls[0]["url"] == "http://apicurio.test:8080/apis/ccompat/v7/subjects/raw.orders-value/versions"
    finally:
        _clear_overrides()


def test_get_registry_subject_version_parses_schema_string(monkeypatch):
    fake_db = FakeDB()
    fake_db.connections_v2.docs.append(make_apicurio_connection())
    calls: List[Dict[str, Any]] = []
    avro = make_avro()
    install_fake_httpx(
        monkeypatch,
        calls,
        default=FakeResponse(200, {"id": 42, "version": "2", "schema": json.dumps(avro)}),
    )
    client = _make_client(fake_db)
    try:
        resp = client.get("/api/v2/schemas/registry-subject/raw.orders-value/versions/2")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["version"] == 2
        assert isinstance(body["version"], int)
        assert body["globalId"] == 42
        assert body["avro"] == avro
        assert calls[0]["url"] == "http://apicurio.test:8080/apis/ccompat/v7/subjects/raw.orders-value/versions/2"
    finally:
        _clear_overrides()


def test_registry_subject_versions_404_subject_not_found(monkeypatch):
    fake_db = FakeDB()
    fake_db.connections_v2.docs.append(make_apicurio_connection())
    calls: List[Dict[str, Any]] = []
    install_fake_httpx(monkeypatch, calls, default=FakeResponse(404, {"error_code": 40401, "message": "Subject not found."}))
    client = _make_client(fake_db)
    try:
        resp1 = client.get("/api/v2/schemas/registry-subject/does.not-exist-value/versions")
        assert resp1.status_code == 404, resp1.text
        assert "does.not-exist-value" in resp1.json()["detail"]

        resp2 = client.get("/api/v2/schemas/registry-subject/does.not-exist-value/versions/1")
        assert resp2.status_code == 404, resp2.text
        assert "does.not-exist-value" in resp2.json()["detail"]
    finally:
        _clear_overrides()


def test_registry_subject_with_dots_is_url_encoded(monkeypatch):
    """The subject naming convention (`<topic>-value`) routinely contains dots
    (e.g. `raw.orders-value`) -- confirm the path segment sent to ccompat is
    the properly quoted subject, dots preserved (they're legal in a URL path
    segment and must NOT be double-encoded or stripped)."""
    fake_db = FakeDB()
    fake_db.connections_v2.docs.append(make_apicurio_connection())
    calls: List[Dict[str, Any]] = []
    install_fake_httpx(monkeypatch, calls, default=FakeResponse(200, [1]))
    client = _make_client(fake_db)
    try:
        resp = client.get("/api/v2/schemas/registry-subject/raw.orders.v2-value/versions")
        assert resp.status_code == 200, resp.text
        assert calls[0]["url"].endswith("/subjects/raw.orders.v2-value/versions")
    finally:
        _clear_overrides()


def test_registry_subject_versions_no_active_connection():
    client = _make_client(FakeDB())
    try:
        resp = client.get("/api/v2/schemas/registry-subject/raw.orders-value/versions")
        assert resp.status_code == 502
        resp2 = client.get("/api/v2/schemas/registry-subject/raw.orders-value/versions/1")
        assert resp2.status_code == 502
    finally:
        _clear_overrides()
