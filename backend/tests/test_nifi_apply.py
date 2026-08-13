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

from services.adapter.compiler.ir import (
    BlockGroup,
    ConnectionSpec,
    DeploymentPlan,
    ParameterContextSpec,
    ParameterSpec,
    ProcessorSpec,
    RootGroup,
)
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


def test_sensitive_dynamic_props_matches_hyphenated_param_names():
    """Service ids contain hyphens (`svc-pw4309`), so compiled parameter
    names do too — the original `[A-Za-z0-9_]+` reference regex silently
    missed them and the sensitive dynamic property was never listed."""
    props = {
        "PASSWORD": "#{svc_svc-pw4309_password}",
        "USERNAME": "#{svc_svc-pw4309_username}",
        "LOGIN_URL": "#{svc_svc-pw4309_base_url}/auth/login",
        "Request Password": "#{svc_svc-pw4309_password}",  # static — never listed
    }
    sensitive = {"svc_svc-pw4309_password"}
    assert nifi_apply._sensitive_dynamic_props(props, sensitive) == ["PASSWORD"]


# ---------------------------------------------------------------------------
# R3-F1 — post-apply processor validation gate
# ---------------------------------------------------------------------------


class FakeNifi:
    """Stateful fake for `nifi_apply.nifi_api_request` covering every REST
    call `apply_plan` makes: generates ids, remembers each created
    processor's name so the validation-gate GETs can answer per-processor.
    Processors whose NAME is in `invalid_processor_names` report
    `validationStatus: INVALID` (with `validation_errors`); everything else
    reports VALID. Also reused by `tests/test_deployer.py`'s lifecycle-level
    R3-F1 test."""

    def __init__(self, invalid_processor_names=(), validation_errors=("'X' is invalid because of Y.",)):
        self.invalid_processor_names = set(invalid_processor_names)
        self.validation_errors = list(validation_errors)
        self.processor_names: Dict[str, str] = {}
        self.calls: List[tuple] = []
        self._counter = 0

    def _next_id(self, prefix: str) -> str:
        self._counter += 1
        return f"{prefix}-{self._counter}"

    async def request(self, url, method, path, json_body=None, params=None, **kwargs):
        self.calls.append((method, path))
        if method == "GET":
            if path == "/nifi-api/flow/parameter-contexts":
                return {"ok": True, "data": {"parameterContexts": []}}
            if path.startswith("/nifi-api/processors/"):
                pid = path.rsplit("/", 1)[1]
                name = self.processor_names.get(pid, "")
                invalid = name in self.invalid_processor_names
                comp = {"id": pid, "name": name, "state": "STOPPED",
                        "validationStatus": "INVALID" if invalid else "VALID"}
                if invalid:
                    comp["validationErrors"] = list(self.validation_errors)
                return {"ok": True, "data": {"revision": {"version": 0}, "component": comp}}
            if path.startswith("/nifi-api/controller-services/"):
                return {"ok": True, "data": {"revision": {"version": 0}, "component": {"state": "ENABLED"}}}
            if path.startswith("/nifi-api/flow/process-groups/") and path.endswith("/controller-services"):
                return {"ok": True, "data": {"controllerServices": []}}
            if path.startswith("/nifi-api/flow/process-groups/"):
                return {"ok": True, "data": {"processGroupFlow": {"flow": {"processGroups": []}}}}
            if path.endswith("/process-groups"):
                return {"ok": True, "data": {"processGroups": []}}
            return {"ok": True, "data": {"revision": {"version": 0}}}
        if method == "POST":
            if path.endswith("/processors"):
                pid = self._next_id("proc")
                self.processor_names[pid] = str(((json_body or {}).get("component") or {}).get("name") or "")
                return {"ok": True, "data": {"id": pid}}
            if path == "/nifi-api/parameter-contexts":
                return {"ok": True, "data": {"id": self._next_id("pc")}}
            if path.endswith("/update-requests"):
                return {"ok": True, "data": {"request": {"complete": True}}}
            return {"ok": True, "data": {"id": self._next_id("obj")}}
        return {"ok": True, "data": {}}


def _minimal_plan() -> DeploymentPlan:
    group = BlockGroup(
        blockId="b1", name="read__http",
        processors=[
            ProcessorSpec(key="fetch", name="fetch", type="org.apache.nifi.processors.standard.InvokeHTTP",
                          autoTerminate=["No Retry", "Retry", "Original", "Failure"]),
            ProcessorSpec(key="login", name="login", type="org.apache.nifi.processors.groovyx.ExecuteGroovyScript",
                          autoTerminate=["failure"]),
        ],
        connections=[ConnectionSpec(from_="login", to="fetch", relationships=["success"])],
        inputPort=False, outputPort=False, dlqPort=False,
    )
    return DeploymentPlan(
        flowId="flow-x", flowToken="flow_x",
        parameterContext=ParameterContextSpec(name="pc-flow-x", parameters=[]),
        rootGroup=RootGroup(name="flow_x", childGroups=[group], connections=[]),
        topics=[], connectors=[], scopeMap={},
    )


def _patch_validation_gate_fast(monkeypatch):
    monkeypatch.setattr(nifi_apply, "_VALIDATION_GATE_TIMEOUT_SECS", 0.0)
    monkeypatch.setattr(nifi_apply, "_VALIDATION_GATE_POLL_SECS", 0.0)


def _patch_root_pg(monkeypatch):
    async def fake_root_pg(*args, **kwargs):
        return "root-pg"

    monkeypatch.setattr(nifi_apply, "get_nifi_root_process_group_id", fake_root_pg)


@async_test
async def test_apply_plan_validation_gate_fails_on_invalid_processor(monkeypatch):
    """R3-F1: one INVALID processor after apply -> the whole deploy fails
    (NifiApplyError naming the component + its validation errors) and the
    partially built PG is best-effort torn down."""
    fake = FakeNifi(
        invalid_processor_names={"login"},
        validation_errors=[
            "'Replacement Value' is invalid because the Sensitivity of the parameter "
            "does not match the Sensitivity of the property."
        ],
    )
    monkeypatch.setattr(nifi_apply, "nifi_api_request", fake.request)
    _patch_root_pg(monkeypatch)
    _patch_validation_gate_fast(monkeypatch)

    teardown_calls = []

    async def fake_delete_flow_pg(conn, pg_id):
        teardown_calls.append(pg_id)
        return {"ok": True}

    monkeypatch.setattr(nifi_apply, "delete_flow_pg", fake_delete_flow_pg)

    try:
        await nifi_apply.apply_plan({"endpoint": "https://nifi.test", **_AUTH}, _minimal_plan())
        assert False, "expected NifiApplyError"
    except nifi_apply.NifiApplyError as exc:
        msg = str(exc)
        assert "read__http/login" in msg  # component named
        assert "Sensitivity of the parameter" in msg  # validation error surfaced
        assert "all-or-nothing" in msg

    assert len(teardown_calls) == 1  # created PG best-effort deleted


@async_test
async def test_apply_plan_validation_gate_passes_when_all_valid(monkeypatch):
    fake = FakeNifi()
    monkeypatch.setattr(nifi_apply, "nifi_api_request", fake.request)
    _patch_root_pg(monkeypatch)
    _patch_validation_gate_fast(monkeypatch)

    async def fail_if_teardown(*args, **kwargs):
        raise AssertionError("delete_flow_pg must not run on a valid deploy")

    monkeypatch.setattr(nifi_apply, "delete_flow_pg", fail_if_teardown)

    applied = await nifi_apply.apply_plan({"endpoint": "https://nifi.test", **_AUTH}, _minimal_plan())
    assert applied.process_group_id
    assert set(applied.components["b1"].keys()) == {"fetch", "login"}
    # The gate actually read every created processor's validation state.
    validation_gets = [c for c in fake.calls if c[0] == "GET" and c[1].startswith("/nifi-api/processors/")]
    assert len(validation_gets) == 2
