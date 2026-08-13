"""Regression test for M8: `services/adapter/deployer/nifi_apply.py`
parameter-context handling used to serialize a `None`-valued
`ParameterSpec` (e.g. `add_param("redis_password", None, True)` for an
in-cluster Redis with no password — the documented deployment) as JSON
`null`. On the UPDATE path (`_update_parameter_context`, taken on every
REDEPLOY of an already-deployed flow) a `null` value is NiFi's DELETE-this-
parameter instruction, not "no value" — so a redeploy silently dropped the
parameter while every `#{...}` reference to it (e.g. the Redis pool
controller service's `Password` property) still pointed at it, failing that
controller service's validation ~45s later.

Fix: `_param_value_for_nifi` coerces `None` -> `""` for both sensitive and
non-sensitive parameters, on both the CREATE and UPDATE paths.
"""
from __future__ import annotations

import asyncio
import sys
from functools import wraps
from pathlib import Path
from typing import Any, Dict, List

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services.adapter.compiler.ir import ParameterContextSpec, ParameterSpec
from services.adapter.deployer import nifi_apply

_AUTH = {"auth_type": "NONE", "username": None, "password": None, "token": None}


def async_test(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


def _params_by_name(params_body: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {p["parameter"]["name"]: p["parameter"]["value"] for p in params_body}


@async_test
async def test_create_parameter_context_coerces_none_to_empty_string(monkeypatch):
    calls: List[tuple] = []

    async def fake_request(url, method, path, json_body=None, **kwargs):
        calls.append((method, path, json_body))
        if method == "GET" and path == "/nifi-api/flow/parameter-contexts":
            return {"ok": True, "data": {"parameterContexts": []}}
        if method == "POST" and path == "/nifi-api/parameter-contexts":
            return {"ok": True, "data": {"id": "pc-1"}}
        raise AssertionError(f"unexpected call {method} {path}")

    monkeypatch.setattr(nifi_apply, "nifi_api_request", fake_request)

    spec = ParameterContextSpec(
        name="pc",
        parameters=[
            ParameterSpec(name="redis_password", value=None, sensitive=True),
            ParameterSpec(name="basic_auth_password", value=None, sensitive=False),
            ParameterSpec(name="topic_b1", value="raw.x.y", sensitive=False),
        ],
    )
    pc_id, pc_name = await nifi_apply._ensure_parameter_context("https://nifi.test", _AUTH, spec)

    assert pc_id == "pc-1"
    assert pc_name == "pc"

    create_call = next(c for c in calls if c[0] == "POST" and c[1] == "/nifi-api/parameter-contexts")
    values = _params_by_name(create_call[2]["component"]["parameters"])
    assert values["redis_password"] == ""
    assert values["basic_auth_password"] == ""
    assert values["topic_b1"] == "raw.x.y"
    assert all(v is not None for v in values.values())  # never a JSON null


@async_test
async def test_update_parameter_context_on_redeploy_coerces_none_to_empty_string(monkeypatch):
    """The M8 failure scenario itself: an already-deployed flow (parameter
    context already exists) redeploys with a None-valued sensitive param —
    the UPDATE payload must never carry a literal `null`."""
    calls: List[tuple] = []

    async def fake_request(url, method, path, json_body=None, **kwargs):
        calls.append((method, path, json_body))
        if method == "GET" and path == "/nifi-api/flow/parameter-contexts":
            return {"ok": True, "data": {"parameterContexts": [{"component": {"id": "pc-1", "name": "pc"}}]}}
        if method == "GET" and path == "/nifi-api/parameter-contexts/pc-1":
            return {"ok": True, "data": {"revision": {"version": 2}}}
        if method == "POST" and path == "/nifi-api/parameter-contexts/pc-1/update-requests":
            return {"ok": True, "data": {"request": {"complete": True}}}
        raise AssertionError(f"unexpected call {method} {path}")

    monkeypatch.setattr(nifi_apply, "nifi_api_request", fake_request)

    spec = ParameterContextSpec(name="pc", parameters=[ParameterSpec(name="redis_password", value=None, sensitive=True)])
    pc_id, _ = await nifi_apply._ensure_parameter_context("https://nifi.test", _AUTH, spec)

    assert pc_id == "pc-1"
    update_call = next(c for c in calls if c[0] == "POST" and c[1] == "/nifi-api/parameter-contexts/pc-1/update-requests")
    values = _params_by_name(update_call[2]["component"]["parameters"])
    assert values["redis_password"] == ""
    assert values["redis_password"] is not None


def test_param_value_for_nifi_helper():
    assert nifi_apply._param_value_for_nifi(None) == ""
    assert nifi_apply._param_value_for_nifi("") == ""
    assert nifi_apply._param_value_for_nifi("kafka:9092") == "kafka:9092"
