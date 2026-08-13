"""Regression test for E6: `services/apicurio_client.py::register_schema`'s
dual-write (a Confluent-compatible POST *and* a native v3/v2 POST for every
logical registration) made every `/schemas/approve` or `/schemas/register`
call consume 2 ccompat version slots instead of 1 -- proven live against a
real Apicurio instance (docs/orchestration/e2e/journey-c-d.md DEFECT-2):
approving a schema twice left ccompat versions `[1,2,3,4]` (v1/v2 both the
first approval's content, v3/v4 both the second's) instead of `[1,2]`, which
made `routers/v2/schemas.py::delete_approved_schema_version` -- which forwards
the app's own 1-per-approval version counter straight through as the literal
ccompat version to delete -- target the WRONG registry version from the
second approval onward.

Fix: `register_schema(..., ccompat_only=True)` registers via ccompat ONLY --
no native v3/v2 follow-up write -- so one logical registration call consumes
exactly one ccompat version. Every v2 router call site
(`routers/v2/schemas.py`'s `approve_schema`/`register_schema_standalone`)
passes it; the legacy alpha router does not, so its dual-write behaviour is
unchanged (regression-tested here too).

Uses the same scripted-httpx.AsyncClient pattern as
tests/test_v2_services.py's `make_scripted_client` (no pytest-asyncio
dependency in this repo, no real network access).
"""
from __future__ import annotations

import asyncio
import sys
from functools import wraps
from pathlib import Path
from typing import Any, Dict, List, Optional

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import apicurio_client


def async_test(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


AVRO = {"type": "record", "name": "Thing", "fields": [{"name": "id", "type": "long"}]}


class FakeResponse:
    def __init__(self, status_code: int, json_data: Optional[Dict[str, Any]] = None, text: str = ""):
        self.status_code = status_code
        self._json_data = json_data if json_data is not None else {}
        self.text = text or ""

    def json(self):
        return self._json_data


def _install_scripted_client(monkeypatch, *, post_responses: List[Any], get_responses: List[Any]):
    """`post_responses`/`get_responses` are consumed in call order (a single
    entry is reused for every remaining call once the list is exhausted).
    Records every (method, url, kwargs) call for assertions."""
    calls: List[tuple] = []

    class _Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, **kw):
            calls.append(("POST", url, kw))
            resp = post_responses.pop(0) if post_responses else FakeResponse(500, {})
            if isinstance(resp, Exception):
                raise resp
            return resp

        async def get(self, url, **kw):
            calls.append(("GET", url, kw))
            resp = get_responses.pop(0) if get_responses else FakeResponse(500, {})
            if isinstance(resp, Exception):
                raise resp
            return resp

    monkeypatch.setattr(apicurio_client.httpx, "AsyncClient", _Client)
    return calls


# --------------------------------------------------------- ccompat_only=True


@async_test
async def test_ccompat_only_registers_exactly_once(monkeypatch):
    calls = _install_scripted_client(
        monkeypatch,
        post_responses=[FakeResponse(200, {"id": 16})],
        get_responses=[FakeResponse(200, {"subject": "e2ec.thing-value", "id": 16, "version": 1})],
    )

    result = await apicurio_client.register_schema(
        url="https://apicurio.test/apis/registry/v3",
        group_id="default",
        artifact_id="e2ec.thing-value",
        avro_schema=AVRO,
        ccompat_only=True,
    )

    assert result["ok"] is True
    assert result["global_id"] == 16
    assert result["ccompat_id"] == 16
    assert result["version"] == 1
    assert result["api_version"] == "ccompat"

    post_calls = [c for c in calls if c[0] == "POST"]
    # Exactly ONE registration write -- no native v3/v2 follow-up POST, unlike
    # the dual-write default path.
    assert len(post_calls) == 1
    assert post_calls[0][1] == "https://apicurio.test/apis/ccompat/v7/subjects/e2ec.thing-value/versions"


@async_test
async def test_ccompat_only_second_registration_advances_exactly_one_version(monkeypatch):
    """Two logical registrations -> ccompat versions [1, 2], not [1,2,3,4]
    (the dual-write bug's live-observed symptom)."""
    calls = _install_scripted_client(
        monkeypatch,
        post_responses=[FakeResponse(200, {"id": 16}), FakeResponse(200, {"id": 17})],
        get_responses=[
            FakeResponse(200, {"subject": "s", "id": 16, "version": 1}),
            FakeResponse(200, {"subject": "s", "id": 17, "version": 2}),
        ],
    )

    first = await apicurio_client.register_schema(
        url="https://apicurio.test", group_id="default", artifact_id="s", avro_schema=AVRO, ccompat_only=True,
    )
    evolved = dict(AVRO, fields=AVRO["fields"] + [{"name": "note", "type": ["null", "string"], "default": None}])
    second = await apicurio_client.register_schema(
        url="https://apicurio.test", group_id="default", artifact_id="s", avro_schema=evolved, ccompat_only=True,
    )

    assert first["version"] == 1
    assert second["version"] == 2  # NOT 3 or 4 -- one version consumed per call
    post_calls = [c for c in calls if c[0] == "POST"]
    assert len(post_calls) == 2


@async_test
async def test_ccompat_only_failure_surfaces_as_not_ok(monkeypatch):
    _install_scripted_client(monkeypatch, post_responses=[FakeResponse(422, text="incompatible schema")], get_responses=[])

    result = await apicurio_client.register_schema(
        url="https://apicurio.test", group_id="default", artifact_id="s", avro_schema=AVRO, ccompat_only=True,
    )
    assert result["ok"] is False
    assert "422" in result["error"]


# ------------------------------------------------- default (legacy) behaviour


@async_test
async def test_default_dual_write_unchanged_for_legacy_callers(monkeypatch):
    """`ccompat_only` defaults to False -- the legacy alpha router
    (backend/routers/flows.py, backend/routers/schemas.py) calls
    `register_schema` without it and must keep getting the original
    dual-write (ccompat POST + native v3 POST) behaviour."""
    calls = _install_scripted_client(
        monkeypatch,
        post_responses=[
            FakeResponse(200, {"id": 16}),  # ccompat sync
            FakeResponse(200, {"globalId": 99, "version": 1}),  # native v3
        ],
        get_responses=[],
    )

    result = await apicurio_client.register_schema(
        url="https://apicurio.test/apis/registry/v3",
        group_id="default",
        artifact_id="legacy.subject",
        avro_schema=AVRO,
    )

    assert result["ok"] is True
    assert result["api_version"] == "v3"
    assert result["global_id"] == 99
    assert result["ccompat_id"] == 16

    post_calls = [c for c in calls if c[0] == "POST"]
    assert len(post_calls) == 2  # ccompat AND native -- unchanged dual-write
    assert post_calls[0][1] == "https://apicurio.test/apis/ccompat/v7/subjects/legacy.subject/versions"
    assert post_calls[1][1] == "https://apicurio.test/apis/registry/v3/groups/default/artifacts/legacy.subject/versions"
