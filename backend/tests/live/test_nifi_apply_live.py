"""LIVE integration test (T7.2): applies a real, minimal http-read -> kafka
write `DeploymentPlan` (compiled by the actual `compile_flow`, no mocking)
to the REAL NiFi instance configured in `backend/.env`, verifies the result
via `GET`, then always deletes what it created.

Marked `@pytest.mark.live` (registered + excluded by default in
`backend/pytest.ini`). Run explicitly:

    .venv\\Scripts\\python.exe -m pytest tests/live/test_nifi_apply_live.py -m live -q

Needs a reachable NiFi at `NIFI_URL` (`backend/.env`: admin/basic auth).
Does not need Mongo, Kafka, or any other live service — the plan's `kafka`
block only needs a syntactically valid bootstrap-servers string (never
actually connected to; everything is created STOPPED).
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(BACKEND_DIR / ".env")

from models.adapter import AppService, Flow, FlowBlock, PlatformConnection  # noqa: E402
from services.adapter.compiler import CompileContext, compile_flow  # noqa: E402
from services.adapter.deployer import nifi_apply  # noqa: E402
from services.nifi_client import nifi_api_request  # noqa: E402

pytestmark = pytest.mark.live

FLOW_NAME = "dmp_ci_smoke"


def _nifi_conn() -> dict:
    url = os.environ.get("NIFI_URL")
    username = os.environ.get("NIFI_USERNAME")
    password = os.environ.get("NIFI_PASSWORD")
    if not (url and username and password):
        pytest.skip("NIFI_URL/NIFI_USERNAME/NIFI_PASSWORD not set (backend/.env) — skipping live NiFi test.")
    return {"endpoint": url, "auth_type": "BASIC", "username": username, "password": password, "token": None}


def _smoke_flow() -> Flow:
    """http read (no auth, no split, no pagination) -> kafka write. The
    smallest tree that still exercises apply_plan's full path: parameter
    context, two BlockGroup PGs, a cross-group PortLink, controller
    services (shared Kafka3ConnectionService for each group's own DLQ
    publish + the write's PublishKafka), and the DLQ chain."""
    return Flow(
        id="flow-dmp-ci-smoke",
        name=FLOW_NAME,
        cron="0 2 * * *",
        state="Draft",
        enabled=True,
        createdAt="2026-01-01T00:00:00.000Z",
        updatedAt="2026-01-01T00:00:00.000Z",
        blocks=[
            FlowBlock(
                id="b-read", adapter="http", mode="read", name="Fetch", parentId=None, serviceId="svc-http",
                config={
                    "method": "GET", "path": "/get", "responseFormat": "json",
                    "recordPath": "$.args", "split": False,
                    "pagination": {"type": "none", "fields": {}},
                },
            ),
            FlowBlock(
                id="b-write", adapter="kafka", mode="write", name="Write", parentId="b-read", entity="smoke", config={},
            ),
        ],
        topics=[], variables=[], servicePins={},
    )


def _smoke_ctx() -> CompileContext:
    services = {
        "svc-http": AppService(
            id="svc-http", type="http", name="Smoke Service", config={"baseUrl": "https://httpbin.org", "authMode": "none"},
        ),
    }
    connections = {
        "kafka": PlatformConnection(
            id="conn-kafka", type="kafka", name="Kafka",
            config={"bootstrapServers": os.environ.get("KAFKA_IN_CLUSTER_BOOTSTRAP", "kafka:9092")},
        ),
    }
    return CompileContext(services=services, connections=connections, gateway_proxies={}, approved_schemas={})


async def _get(nifi_conn: dict, path: str) -> dict:
    r = await nifi_api_request(
        nifi_conn["endpoint"], "GET", path,
        auth_type=nifi_conn["auth_type"], username=nifi_conn["username"], password=nifi_conn["password"], token=nifi_conn["token"],
    )
    assert r.get("ok"), f"GET {path} failed: {r.get('error')}"
    return r["data"]


async def _delete_parameter_context(nifi_conn: dict, pc_id: str) -> None:
    current = await nifi_api_request(
        nifi_conn["endpoint"], "GET", f"/nifi-api/parameter-contexts/{pc_id}",
        auth_type=nifi_conn["auth_type"], username=nifi_conn["username"], password=nifi_conn["password"], token=nifi_conn["token"],
    )
    if not current.get("ok"):
        return
    version = (current["data"].get("revision") or {}).get("version", 0)
    await nifi_api_request(
        nifi_conn["endpoint"], "DELETE", f"/nifi-api/parameter-contexts/{pc_id}",
        auth_type=nifi_conn["auth_type"], username=nifi_conn["username"], password=nifi_conn["password"], token=nifi_conn["token"],
        params={"version": str(version)},
    )


def test_apply_minimal_http_kafka_plan_against_live_nifi():
    nifi_conn = _nifi_conn()
    flow = _smoke_flow()
    plan = compile_flow(flow, _smoke_ctx())
    assert plan.flowToken == FLOW_NAME

    applied = None
    try:
        applied = asyncio.run(nifi_apply.apply_plan(nifi_conn, plan))

        # --- evidence: PG id + component counts -----------------------------
        assert applied.process_group_id
        assert set(applied.groups.keys()) == {"b-read", "b-write"}
        print(f"\n[live] flow process group id: {applied.process_group_id}")
        print(f"[live] parameter context: {applied.parameter_context_name} ({applied.parameter_context_id})")
        for block_id, components in applied.components.items():
            print(f"[live] {block_id} -> {len(components)} component(s): {sorted(components.keys())}")

        # --- verify the flow PG itself ---------------------------------------
        pg_info = asyncio.run(_get(nifi_conn, f"/nifi-api/process-groups/{applied.process_group_id}"))
        assert pg_info["component"]["name"] == FLOW_NAME
        assert pg_info["component"]["parameterContext"]["id"] == applied.parameter_context_id

        # --- verify child block PGs + processor counts ------------------------
        flow_status = asyncio.run(_get(nifi_conn, f"/nifi-api/flow/process-groups/{applied.process_group_id}"))
        child_pgs = flow_status["processGroupFlow"]["flow"]["processGroups"]
        assert len(child_pgs) == 2, f"expected 2 child block process groups, got {len(child_pgs)}"

        total_processors = 0
        for block_id, pg_id in applied.groups.items():
            child_flow = asyncio.run(_get(nifi_conn, f"/nifi-api/flow/process-groups/{pg_id}"))["processGroupFlow"]["flow"]
            processors = child_flow["processors"]
            assert processors, f"block {block_id!r} (pg {pg_id}) has no processors"
            total_processors += len(processors)
            for proc in processors:
                state = proc["component"]["state"]
                assert state == "STOPPED", f"processor {proc['component']['name']!r} in {block_id!r} is {state}, expected STOPPED (compiler-spec.md §7)"

            cs_list = asyncio.run(_get(nifi_conn, f"/nifi-api/flow/process-groups/{pg_id}/controller-services"))["controllerServices"]
            for cs in cs_list:
                if (cs["component"].get("parentGroupId") or cs["component"].get("groupId")) != pg_id:
                    continue  # a controller service reference from elsewhere, not one WE created here
                state = cs["component"]["state"]
                assert state == "ENABLED", f"controller service {cs['component']['name']!r} in {block_id!r} is {state}, expected ENABLED"

        print(f"[live] total processors across both block groups: {total_processors}")
        assert total_processors >= 6  # trigger+init+fetch+dlq__meta+dlq__publish (b-read) + publish+dlq__meta+dlq__publish (b-write), minus overlap tolerance

        # --- verify the parameter context ---------------------------------
        pc_info = asyncio.run(_get(nifi_conn, f"/nifi-api/parameter-contexts/{applied.parameter_context_id}"))
        assert pc_info["component"]["name"] == plan.parameterContext.name
        live_param_names = {p["parameter"]["name"] for p in pc_info["component"]["parameters"]}
        expected_param_names = {p.name for p in plan.parameterContext.parameters}
        assert expected_param_names <= live_param_names

    finally:
        if applied is not None:
            cleanup = asyncio.run(nifi_apply.delete_flow_pg(nifi_conn, applied.process_group_id))
            print(f"[live] cleanup delete_flow_pg({applied.process_group_id}): {cleanup}")
            asyncio.run(_delete_parameter_context(nifi_conn, applied.parameter_context_id))
            print(f"[live] cleanup parameter context {applied.parameter_context_id} deleted (best-effort)")
