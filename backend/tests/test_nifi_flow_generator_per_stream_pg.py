"""Tests for per-stream NiFi process group topology in REST API flows."""
import asyncio
import sys
from pathlib import Path
from typing import Any, Dict, List

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import nifi_flow_generator as gen


def _make_source(streams: List[dict]) -> dict:
    return {
        "name": "test_source",
        "type": "REST API",
        "base_url": "https://api.example.com",
        "streams": streams,
        "schedule": {"interval_value": 5, "interval_unit": "minutes"},
    }


def _make_nifi_conn() -> dict:
    return {"endpoint": "http://nifi:8080", "auth_type": "NONE"}


def _make_kafka_conn() -> dict:
    return {"endpoint": "kafka:9092"}


def _fake_nifi_env(monkeypatch):
    """Patch NiFi API calls and return a call log for inspection."""
    call_log: List[Dict[str, Any]] = []
    id_counter = {"n": 0}

    def _next_id() -> str:
        id_counter["n"] += 1
        return f"fake-{id_counter['n']}"

    async def fake_get_root(*a, **kw):
        return "root-pg"

    async def fake_create_pg(nifi_url, parent_pg_id, name, **kw):
        pg_id = _next_id()
        call_log.append({"action": "create_pg", "parent": parent_pg_id, "name": name, "id": pg_id})
        return pg_id

    async def fake_create_input_port(nifi_url, pg_id, name, **kw):
        port_id = _next_id()
        call_log.append({"action": "create_input_port", "pg": pg_id, "name": name, "id": port_id})
        return port_id

    async def fake_create_output_port(nifi_url, pg_id, name, **kw):
        port_id = _next_id()
        call_log.append({"action": "create_output_port", "pg": pg_id, "name": name, "id": port_id})
        return port_id

    async def fake_create_processor(nifi_url, pg_id, proc_type, name, **kw):
        proc_id = _next_id()
        call_log.append({
            "action": "create_processor",
            "pg": pg_id,
            "type": proc_type,
            "name": name,
            "id": proc_id,
            "properties": kw.get("properties", {}),
            "auto_terminate": kw.get("auto_terminate", []),
        })
        return proc_id

    async def fake_create_connection(nifi_url, pg_id, src_id, dst_id, rels, **kw):
        conn_id = _next_id()
        call_log.append({
            "action": "create_connection",
            "pg": pg_id,
            "src": src_id,
            "dst": dst_id,
            "rels": rels,
            "src_type": kw.get("src_type", "PROCESSOR"),
            "dst_type": kw.get("dst_type", "PROCESSOR"),
            "src_group_id": kw.get("src_group_id"),
            "dst_group_id": kw.get("dst_group_id"),
        })
        return conn_id

    async def fake_create_cs(nifi_url, pg_id, svc_type, name, properties, **kw):
        cs_id = _next_id()
        return cs_id, 0

    async def fake_enable_cs(*a, **kw):
        return True

    async def fake_wait_cs(*a, **kw):
        return True

    async def fake_nifi_api_request(nifi_url, method, path, **kw):
        return {"ok": True, "data": {"revision": {"version": 0}}}

    monkeypatch.setattr(gen, "get_nifi_root_process_group_id", fake_get_root)
    monkeypatch.setattr(gen, "_create_process_group", fake_create_pg)
    monkeypatch.setattr(gen, "_create_input_port", fake_create_input_port)
    monkeypatch.setattr(gen, "_create_output_port", fake_create_output_port)
    monkeypatch.setattr(gen, "_create_processor", fake_create_processor)
    monkeypatch.setattr(gen, "_create_connection", fake_create_connection)
    monkeypatch.setattr(gen, "_create_controller_service", fake_create_cs)
    monkeypatch.setattr(gen, "_enable_controller_service", fake_enable_cs)
    monkeypatch.setattr(gen, "_wait_cs_enabled", fake_wait_cs)
    monkeypatch.setattr(gen, "nifi_api_request", fake_nifi_api_request)
    return call_log


def test_single_stream_creates_child_pg(monkeypatch):
    """A flow with one stream should produce exactly one child PG inside the flow PG."""
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "events", "name": "events",
            "endpoint_path": "/events",
            "entity": {"kafka": {"topic": "bronze.events", "value_format": ""}},
        }
    ])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))

    assert result["ok"] is True

    create_pg_calls = [c for c in call_log if c["action"] == "create_pg"]
    # First PG is the top-level flow PG (parent = root-pg).
    # Second PG is the stream child PG (parent = flow_pg_id).
    flow_pg_id = create_pg_calls[0]["id"]
    assert len(create_pg_calls) == 2, f"Expected 2 PGs, got {len(create_pg_calls)}: {create_pg_calls}"
    child_pg = create_pg_calls[1]
    assert child_pg["parent"] == flow_pg_id
    assert "events" in child_pg["name"]


def test_stream_processors_live_in_child_pg(monkeypatch):
    """All stream processors (init, fetch, extract, publish) must be created inside the child PG, not the flow PG."""
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "orders", "name": "orders",
            "endpoint_path": "/orders",
            "entity": {"kafka": {"topic": "bronze.orders", "value_format": ""}},
        }
    ])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))

    assert result["ok"] is True

    create_pg_calls = [c for c in call_log if c["action"] == "create_pg"]
    flow_pg_id = create_pg_calls[0]["id"]
    child_pg_id = create_pg_calls[1]["id"]

    proc_calls = [c for c in call_log if c["action"] == "create_processor"]
    # Trigger and set_auth live in the flow PG
    flow_pg_procs = [p for p in proc_calls if p["pg"] == flow_pg_id]
    assert any("trigger" in p["name"] for p in flow_pg_procs)
    assert any(
        "set_request_meta" in p["name"] or "set_auth" in p["name"] or "request_meta" in p["name"]
        for p in flow_pg_procs
    )

    # All other processors should be in the child PG
    child_pg_procs = [p for p in proc_calls if p["pg"] == child_pg_id]
    assert len(child_pg_procs) > 0, "No processors in child PG"
    assert any("fetch" in p["name"] for p in child_pg_procs)


def test_post_json_stream_builds_body_and_request_headers_before_invokehttp(monkeypatch):
    """A body-enabled REST stream must create request content before InvokeHTTP."""
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "search",
            "name": "search",
            "method": "POST",
            "endpoint_path": "/search",
            "body_format": "json",
            "body_template": '{"status":"active"}',
            "response_format": "json",
            "entity": {"kafka": {"topic": "bronze.search", "value_format": ""}},
        }
    ])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))

    assert result["ok"] is True

    processors = [c for c in call_log if c["action"] == "create_processor"]
    body_builder = next((p for p in processors if p["name"] == "search__build_request_body"), None)
    assert body_builder is not None
    assert body_builder["type"] == "org.apache.nifi.processors.standard.ReplaceText"
    assert body_builder["properties"]["Replacement Value"] == '{"status":"active"}'

    fetch = next(p for p in processors if p["name"] == "search__fetch")
    assert fetch["properties"]["HTTP Method"] == "POST"
    assert fetch["properties"]["Request Header Attributes Pattern"] == "Content\\-Type|Accept"

    connections = [c for c in call_log if c["action"] == "create_connection"]
    assert any(c["src"] == body_builder["id"] and c["dst"] == fetch["id"] for c in connections)


def test_xml_and_csv_body_templates_generate_request_content(monkeypatch):
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "xml_update",
            "name": "xml_update",
            "method": "PATCH",
            "endpoint_path": "/xml",
            "body_format": "xml",
            "body_template": "<record><status>active</status></record>",
            "response_format": "xml",
            "split_response_records": False,
            "entity": {"kafka": {"topic": "bronze.xml_update", "value_format": ""}},
        },
        {
            "id": "csv_upload",
            "name": "csv_upload",
            "method": "PUT",
            "endpoint_path": "/csv",
            "body_format": "csv",
            "body_template": "id,name\n1,Ada",
            "response_format": "csv",
            "split_response_records": False,
            "entity": {"kafka": {"topic": "bronze.csv_upload", "value_format": ""}},
        },
    ])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))

    assert result["ok"] is True
    processors = [c for c in call_log if c["action"] == "create_processor"]

    xml_body = next(p for p in processors if p["name"] == "xml_update__build_request_body")
    xml_fetch = next(p for p in processors if p["name"] == "xml_update__fetch")
    assert xml_body["properties"]["Replacement Value"] == "<record><status>active</status></record>"
    assert xml_fetch["properties"]["HTTP Method"] == "PATCH"
    assert xml_fetch["properties"]["Request Content-Type"] == "${Content-Type}"
    assert xml_fetch["properties"]["Request Header Attributes Pattern"] == "Content\\-Type|Accept"

    csv_body = next(p for p in processors if p["name"] == "csv_upload__build_request_body")
    csv_fetch = next(p for p in processors if p["name"] == "csv_upload__fetch")
    assert csv_body["properties"]["Replacement Value"] == "id,name\n1,Ada"
    assert csv_fetch["properties"]["HTTP Method"] == "PUT"
    assert csv_fetch["properties"]["Request Content-Type"] == "${Content-Type}"
    assert csv_fetch["properties"]["Request Header Attributes Pattern"] == "Content\\-Type|Accept"


def test_non_publishing_leaf_stream_terminates_response_relationship(monkeypatch):
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "update_record",
            "name": "update_record",
            "method": "PATCH",
            "endpoint_path": "/records/${record_id}",
            "body_format": "json",
            "body_template": '{"recordId":"${record_id}"}',
            "response_format": "json",
        }
    ])

    ok = asyncio.run(gen._build_rest_api_flow(
        "http://nifi:8080",
        "flow-pg",
        source,
        kafka_svc_id=None,
        json_reader_id=None,
        schema_reg_id=None,
        avro_writer_id=None,
        kafka_schema_reg_id=None,
        schema_ref_writer_id=None,
        kafka_topic="unused",
        schedule_str="5 min",
        auth_type_nifi="NONE",
        username_nifi=None,
        password_nifi=None,
    ))

    assert ok is True
    processors = [c for c in call_log if c["action"] == "create_processor"]
    fetch = next(p for p in processors if p["name"] == "update_record__fetch")
    complete = next(p for p in processors if p["name"] == "update_record__complete")
    assert complete["type"] == "org.apache.nifi.processors.attributes.UpdateAttribute"
    assert complete["auto_terminate"] == ["success"]

    connections = [c for c in call_log if c["action"] == "create_connection"]
    assert any(
        c["src"] == fetch["id"] and c["dst"] == complete["id"] and c["rels"] == ["Response"]
        for c in connections
    )


def test_delete_body_is_rejected_for_nifi_until_supported(monkeypatch):
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "delete_record",
            "name": "delete_record",
            "method": "DELETE",
            "endpoint_path": "/records/1",
            "body_format": "json",
            "body_template": '{"reason":"expired"}',
            "response_format": "json",
            "entity": {"kafka": {"topic": "bronze.delete_record", "value_format": ""}},
        }
    ])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))

    assert result["ok"] is False
    assert "DELETE request bodies are not supported" in result["error"]


def test_delete_empty_body_method_is_supported(monkeypatch):
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "delete_record",
            "name": "delete_record",
            "method": "DELETE",
            "endpoint_path": "/records/1",
            "body_format": "empty",
            "response_format": "json",
            "entity": {"kafka": {"topic": "bronze.delete_record", "value_format": ""}},
        }
    ])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))

    assert result["ok"] is True
    processors = [c for c in call_log if c["action"] == "create_processor"]
    fetch = next(p for p in processors if p["name"] == "delete_record__fetch")
    assert not any(p["name"] == "delete_record__build_request_body" for p in processors)
    assert fetch["properties"]["HTTP Method"] == "DELETE"
    assert "Request Content-Type" not in fetch["properties"]


def test_empty_get_stream_does_not_build_request_body(monkeypatch):
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "records",
            "name": "records",
            "method": "GET",
            "endpoint_path": "/records",
            "body_format": "empty",
            "response_format": "json",
            "entity": {"kafka": {"topic": "bronze.records", "value_format": ""}},
        }
    ])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))

    assert result["ok"] is True
    processors = [c for c in call_log if c["action"] == "create_processor"]
    assert not any(p["name"] == "records__build_request_body" for p in processors)
    fetch = next(p for p in processors if p["name"] == "records__fetch")
    assert "Request Content-Type" not in fetch["properties"]
    assert fetch["properties"]["Request Header Attributes Pattern"] == "Accept"


def test_user_content_type_and_accept_headers_override_defaults(monkeypatch):
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "vendor_search",
            "name": "vendor_search",
            "method": "POST",
            "endpoint_path": "/search",
            "headers": {
                "Content-Type": "application/vnd.example+json",
                "Accept": "application/vnd.example.result+json",
            },
            "body_format": "json",
            "body_template": '{"status":"active"}',
            "response_format": "json",
            "entity": {"kafka": {"topic": "bronze.vendor_search", "value_format": ""}},
        }
    ])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))

    assert result["ok"] is True
    processors = [c for c in call_log if c["action"] == "create_processor"]
    init = next(p for p in processors if p["name"] == "vendor_search__init_page")
    fetch = next(p for p in processors if p["name"] == "vendor_search__fetch")
    assert init["properties"]["Content-Type"] == "application/vnd.example+json"
    assert init["properties"]["Accept"] == "application/vnd.example.result+json"
    assert fetch["properties"]["Request Header Attributes Pattern"] == "Content\\-Type|Accept"


def test_auth_headers_and_api_key_query_are_generated(monkeypatch):
    bearer_log = _fake_nifi_env(monkeypatch)
    bearer_source = _make_source([
        {
            "id": "records",
            "name": "records",
            "endpoint_path": "/records",
            "entity": {"kafka": {"topic": "bronze.records", "value_format": ""}},
        }
    ])
    bearer_source.update({"auth_type": "BEARER", "bearer_token": "secret-token"})

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=bearer_source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))
    assert result["ok"] is True
    bearer_processors = [c for c in bearer_log if c["action"] == "create_processor"]
    set_meta = next(p for p in bearer_processors if p["name"] == "set_request_meta")
    fetch = next(p for p in bearer_processors if p["name"] == "records__fetch")
    assert set_meta["properties"]["Authorization"] == "Bearer secret-token"
    assert fetch["properties"]["Request Header Attributes Pattern"] == "Authorization|Accept"

    query_log = _fake_nifi_env(monkeypatch)
    query_source = _make_source([
        {
            "id": "items",
            "name": "items",
            "endpoint_path": "/items",
            "entity": {"kafka": {"topic": "bronze.items", "value_format": ""}},
        }
    ])
    query_source.update({
        "auth_type": "API_KEY",
        "api_key_location": "QUERY",
        "api_key_name": "api_key",
        "api_key_value": "secret-key",
    })

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=query_source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))
    assert result["ok"] is True
    query_processors = [c for c in query_log if c["action"] == "create_processor"]
    query_fetch = next(p for p in query_processors if p["name"] == "items__fetch")
    assert query_fetch["properties"]["HTTP URL"] == "https://api.example.com/items?api_key=secret-key"


def test_basic_auth_and_api_key_header_are_generated(monkeypatch):
    basic_log = _fake_nifi_env(monkeypatch)
    basic_source = _make_source([
        {
            "id": "secure",
            "name": "secure",
            "endpoint_path": "/secure",
            "entity": {"kafka": {"topic": "bronze.secure", "value_format": ""}},
        }
    ])
    basic_source.update({"auth_type": "BASIC", "rest_username": "user", "rest_password": "pass"})

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=basic_source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))

    assert result["ok"] is True
    basic_processors = [c for c in basic_log if c["action"] == "create_processor"]
    basic_fetch = next(p for p in basic_processors if p["name"] == "secure__fetch")
    assert basic_fetch["properties"]["Request Username"] == "user"
    assert basic_fetch["properties"]["Request Password"] == "pass"

    api_key_log = _fake_nifi_env(monkeypatch)
    api_key_source = _make_source([
        {
            "id": "header_key",
            "name": "header_key",
            "endpoint_path": "/records",
            "entity": {"kafka": {"topic": "bronze.header_key", "value_format": ""}},
        }
    ])
    api_key_source.update({
        "auth_type": "API_KEY",
        "api_key_location": "HEADER",
        "api_key_name": "X-Api-Key",
        "api_key_value": "secret-key",
    })

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=api_key_source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))

    assert result["ok"] is True
    api_key_processors = [c for c in api_key_log if c["action"] == "create_processor"]
    set_meta = next(p for p in api_key_processors if p["name"] == "set_request_meta")
    fetch = next(p for p in api_key_processors if p["name"] == "header_key__fetch")
    assert set_meta["properties"]["X-Api-Key"] == "secret-key"
    assert fetch["properties"]["Request Header Attributes Pattern"] == "X\\-Api\\-Key|Accept"


def test_root_stream_entry_wired_cross_pg(monkeypatch):
    """set_auth must connect to each root stream's input port via a cross-PG connection at the flow PG level."""
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "items", "name": "items",
            "endpoint_path": "/items",
            "entity": {"kafka": {"topic": "bronze.items", "value_format": ""}},
        }
    ])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))
    assert result["ok"] is True

    create_pg_calls = [c for c in call_log if c["action"] == "create_pg"]
    flow_pg_id = create_pg_calls[0]["id"]
    child_pg_id = create_pg_calls[1]["id"]

    in_port_calls = [c for c in call_log if c["action"] == "create_input_port" and c["pg"] == child_pg_id]
    assert len(in_port_calls) >= 1, "Expected at least one input port in child PG"
    in_port_id = in_port_calls[0]["id"]

    # Look for the cross-PG connection: posted to flow_pg, dst=input_port, dst_type=INPUT_PORT
    cross_pg_conns = [
        c for c in call_log
        if c["action"] == "create_connection"
        and c["pg"] == flow_pg_id
        and c["dst"] == in_port_id
        and c["dst_type"] == "INPUT_PORT"
    ]
    assert len(cross_pg_conns) >= 1, "Expected cross-PG connection from flow PG to child PG input port"


def test_two_streams_each_get_child_pg(monkeypatch):
    """A flow with two independent root streams produces two child PGs."""
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "posts", "name": "posts", "endpoint_path": "/posts",
            "entity": {"kafka": {"topic": "bronze.posts", "value_format": ""}},
        },
        {
            "id": "comments", "name": "comments", "endpoint_path": "/comments",
            "entity": {"kafka": {"topic": "bronze.comments", "value_format": ""}},
        },
    ])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))
    assert result["ok"] is True

    create_pg_calls = [c for c in call_log if c["action"] == "create_pg"]
    flow_pg_id = create_pg_calls[0]["id"]
    child_pgs = [c for c in create_pg_calls if c["parent"] == flow_pg_id]
    assert len(child_pgs) == 2, f"Expected 2 child PGs, got {len(child_pgs)}"


def test_route_child_wired_via_ports(monkeypatch):
    """Route child streams must be connected through output→input port pairs across PG boundaries."""
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "all_events", "name": "all_events", "endpoint_path": "/events",
            "extraction_rules": [{"attribute_name": "userId", "path": "$.userId"}],
            "routes": [
                {
                    "ordinal": 0,
                    "child_stream_id": "user1_events",
                    "condition": {"attribute": "userId", "condition": "equals", "value": "1"},
                }
            ],
            "default_route_action": "drop",
        },
        {
            "id": "user1_events", "name": "user1_events",
            "route_source": {"parent_stream_id": "all_events"},
            "entity": {"kafka": {"topic": "bronze.user1_events", "value_format": ""}},
        },
    ])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))
    assert result["ok"] is True

    # There must be an output port created in all_events' child PG
    out_port_calls = [c for c in call_log if c["action"] == "create_output_port"]
    assert len(out_port_calls) >= 1, "Expected at least one output port for route child wiring"

    # There must be a cross-PG connection at the flow PG level linking that output port to an input port
    create_pg_calls = [c for c in call_log if c["action"] == "create_pg"]
    flow_pg_id = create_pg_calls[0]["id"]
    cross_pg_route_conns = [
        c for c in call_log
        if c["action"] == "create_connection"
        and c["pg"] == flow_pg_id
        and c["src_type"] == "OUTPUT_PORT"
        and c["dst_type"] == "INPUT_PORT"
    ]
    assert len(cross_pg_route_conns) >= 1, "Expected cross-PG connection from parent route output port to child input port"


def test_json_conditional_route_extracts_scalar_attribute(monkeypatch):
    """RouteOnAttribute comparisons should see JSON string values without JSON quotes."""
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "items",
            "name": "items",
            "endpoint_path": "/items",
            "extraction_rules": [
                {"attribute_name": "item_id", "path": "$.item_id"},
                {"attribute_name": "status", "path": "$.status"},
            ],
            "routes": [
                {
                    "id": "ready_route",
                    "ordinal": 0,
                    "child_stream_id": "ready_detail",
                    "condition": {"attribute": "status", "condition": "equals", "value": "READY"},
                }
            ],
            "default_route_action": "route_to_stream",
            "default_route_child_stream_id": "default_audit",
        },
        {
            "id": "ready_detail",
            "name": "ready_detail",
            "route_source": {"parent_stream_id": "items", "route_id": "ready_route"},
            "request_enabled": False,
            "entity": {"kafka": {"topic": "bronze.ready", "value_format": ""}},
        },
        {
            "id": "default_audit",
            "name": "default_audit",
            "route_source": {"parent_stream_id": "items", "route_id": "default"},
            "request_enabled": False,
            "entity": {"kafka": {"topic": "bronze.default", "value_format": ""}},
        },
    ])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))

    assert result["ok"] is True
    processors = [c for c in call_log if c["action"] == "create_processor"]
    extract = next(p for p in processors if p["name"] == "items__extract")
    route = next(p for p in processors if p["name"] == "items__route_0")

    assert extract["type"] == "org.apache.nifi.processors.standard.EvaluateJsonPath"
    assert extract["properties"]["Destination"] == "flowfile-attribute"
    assert extract["properties"]["Return Type"] == "scalar"
    assert extract["properties"]["status"] == "$.status"
    assert route["properties"]["Routing Strategy"] == "Route to Property name"
    assert route["properties"]["route_0"] == "${status:equals('READY')}"


def test_single_stream_without_entity_output_does_not_publish(monkeypatch):
    """Kafka output must come from selected entity output, not fallback root/detail inference."""
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {"id": "login", "name": "login", "method": "POST", "endpoint_path": "/login"},
    ])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))

    assert result["ok"] is True
    processors = [c for c in call_log if c["action"] == "create_processor"]
    assert not any(p["name"] == "login__publish" for p in processors)


def test_parent_stream_with_entity_output_publishes_and_feeds_parallel_child(monkeypatch):
    """A parent stream can publish while still feeding a parallel child stream."""
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "records",
            "name": "records",
            "endpoint_path": "/records",
            "entity": {"kafka": {"topic": "bronze.records", "value_format": ""}},
        },
        {
            "id": "details",
            "name": "details",
            "endpoint_path": "/records/${id}",
            "fan_out": {"enabled": True, "parent_stream_id": "records"},
        },
    ])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))

    assert result["ok"] is True
    processors = [c for c in call_log if c["action"] == "create_processor"]
    assert any(p["name"] == "records__publish" for p in processors)
    assert not any(p["name"] == "details__publish" for p in processors)

    create_pg_calls = [c for c in call_log if c["action"] == "create_pg"]
    flow_pg_id = create_pg_calls[0]["id"]
    cross_pg_parallel_conns = [
        c for c in call_log
        if c["action"] == "create_connection"
        and c["pg"] == flow_pg_id
        and c["src_type"] == "OUTPUT_PORT"
        and c["dst_type"] == "INPUT_PORT"
    ]
    assert cross_pg_parallel_conns, "Expected parent stream to feed child PG through ports"


def test_after_all_parallel_child_uses_mergecontent_barrier(monkeypatch):
    """After-all fan-out must wait for all successful split fragments before child entry."""
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "records",
            "name": "records",
            "endpoint_path": "/records",
            "response_data_path": "$.items",
            "split_response_records": True,
            "extraction_rules": [{"attribute_name": "id", "path": "$.id"}],
        },
        {
            "id": "update_record",
            "name": "update_record",
            "method": "PUT",
            "endpoint_path": "/records/${id}",
            "body_format": "json",
            "body_template": '{"name":"updated"}',
            "fan_out": {"enabled": True, "parent_stream_id": "records"},
            "wait_for_all_records_before_downstream": True,
            "extraction_rules": [{"attribute_name": "id", "path": "$.id"}],
        },
        {
            "id": "fetch_all",
            "name": "fetch_all",
            "endpoint_path": "/records",
            "response_data_path": "$.items",
            "split_response_records": True,
            "fan_out": {"enabled": True, "parent_stream_id": "update_record"},
            "entity": {"kafka": {"topic": "bronze.fetch_all", "value_format": ""}},
        },
    ])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))

    assert result["ok"] is True
    processors = [c for c in call_log if c["action"] == "create_processor"]
    barrier = next((p for p in processors if p["name"] == "update_record__after_all__fetch_all"), None)
    assert barrier is not None
    assert barrier["type"] == "org.apache.nifi.processors.standard.MergeContent"
    assert barrier["properties"]["Merge Strategy"] == "Defragment"
    assert barrier["auto_terminate"] == ["failure", "original"]

    connections = [c for c in call_log if c["action"] == "create_connection"]
    out_port = next(c for c in call_log if c["action"] == "create_output_port" and c["name"] == "update_record__fanout__fetch_all")
    assert any(
        c["src"] == barrier["id"] and c["dst"] == out_port["id"] and c["rels"] == ["merged"]
        for c in connections
    )


def test_child_stream_maps_parent_values_into_request_path_query_header_and_body(monkeypatch):
    """Child request mappings should remain runtime attributes with defaults as fallback only."""
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "devices",
            "name": "devices",
            "endpoint_path": "/devices",
            "extraction_rules": [
                {"attribute_name": "device_id", "path": "$.id"},
                {"attribute_name": "account_id", "path": "$.account"},
                {"attribute_name": "token", "path": "$.token"},
            ],
        },
        {
            "id": "update_device",
            "name": "update_device",
            "method": "PATCH",
            "endpoint_path": "/devices/${device_id}",
            "query_params": {"account": "${account_id}"},
            "headers": {"Authorization": "Bearer ${token}"},
            "body_format": "json",
            "body_template": '{"device":"${device_id}","account":"${account_id}"}',
            "path_param_defaults": {
                "device_id": "demo-device",
                "account_id": "demo-account",
                "token": "demo-token",
            },
            "fan_out": {"enabled": True, "parent_stream_id": "devices"},
        },
    ])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))

    assert result["ok"] is True
    processors = [c for c in call_log if c["action"] == "create_processor"]
    init = next(p for p in processors if p["name"] == "update_device__init_page")
    body = next(p for p in processors if p["name"] == "update_device__build_request_body")
    fetch = next(p for p in processors if p["name"] == "update_device__fetch")

    assert init["properties"]["Authorization"] == "Bearer ${token:replaceEmpty('demo-token')}"
    assert body["properties"]["Replacement Value"] == (
        '{"device":"${device_id:replaceEmpty(\'demo-device\')}",'
        '"account":"${account_id:replaceEmpty(\'demo-account\')}"}'
    )
    assert fetch["properties"]["HTTP URL"] == (
        "https://api.example.com/devices/${device_id:replaceEmpty('demo-device')}"
        "?account=${account_id:replaceEmpty('demo-account')}"
    )
    assert fetch["properties"]["Request Header Attributes Pattern"] == "Authorization|Content\\-Type|Accept"


def test_parallel_and_route_branch_outputs_are_independent(monkeypatch):
    """Each branch stream publishes only when that stream is selected as entity output."""
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "events",
            "name": "events",
            "endpoint_path": "/events",
            "extraction_rules": [{"attribute_name": "eventType", "path": "$.type"}],
            "routes": [
                {
                    "id": "paid_route",
                    "ordinal": 0,
                    "child_stream_id": "paid_events",
                    "condition": {"attribute": "eventType", "condition": "equals", "value": "paid"},
                }
            ],
            "default_route_action": "route_to_stream",
            "default_route_child_stream_id": "other_events",
        },
        {
            "id": "audit_events",
            "name": "audit_events",
            "endpoint_path": "/events/${id}/audit",
            "fan_out": {"enabled": True, "parent_stream_id": "events"},
            "entity": {"kafka": {"topic": "bronze.audit_events", "value_format": ""}},
        },
        {
            "id": "paid_events",
            "name": "paid_events",
            "route_source": {"parent_stream_id": "events", "route_id": "paid_route"},
            "request_enabled": False,
            "entity": {"kafka": {"topic": "bronze.paid_events", "value_format": ""}},
        },
        {
            "id": "other_events",
            "name": "other_events",
            "route_source": {"parent_stream_id": "events", "route_id": "default"},
            "request_enabled": False,
        },
    ])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))

    assert result["ok"] is True
    processors = [c for c in call_log if c["action"] == "create_processor"]
    processor_names = {p["name"] for p in processors}

    assert "events__publish" not in processor_names
    assert "audit_events__publish" in processor_names
    assert "paid_events__publish" in processor_names
    assert "other_events__publish" not in processor_names
    assert "paid_events__fetch" not in processor_names
    assert "other_events__fetch" not in processor_names


def test_nested_parallel_and_conditional_branches_create_process_groups(monkeypatch):
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "events",
            "name": "events",
            "endpoint_path": "/events",
            "extraction_rules": [{"attribute_name": "eventType", "path": "$.type"}],
            "routes": [
                {
                    "id": "paid_route",
                    "ordinal": 0,
                    "child_stream_id": "paid_events",
                    "condition": {"attribute": "eventType", "condition": "equals", "value": "paid"},
                }
            ],
            "default_route_action": "drop",
        },
        {
            "id": "audit_events",
            "name": "audit_events",
            "endpoint_path": "/events/${id}/audit",
            "fan_out": {"enabled": True, "parent_stream_id": "events"},
        },
        {
            "id": "paid_events",
            "name": "paid_events",
            "route_source": {"parent_stream_id": "events", "route_id": "paid_route"},
            "request_enabled": True,
            "method": "PATCH",
            "endpoint_path": "/paid/${id}",
        },
        {
            "id": "paid_details",
            "name": "paid_details",
            "endpoint_path": "/paid/${id}/details",
            "fan_out": {"enabled": True, "parent_stream_id": "paid_events"},
        },
    ])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))

    assert result["ok"] is True
    create_pg_calls = [c for c in call_log if c["action"] == "create_pg"]
    stream_pg_names = {c["name"] for c in create_pg_calls[1:]}
    assert {"events", "audit_events", "paid_events", "paid_details"}.issubset(stream_pg_names)

    processors = [c for c in call_log if c["action"] == "create_processor"]
    processor_names = {p["name"] for p in processors}
    assert "events__route_0" in processor_names
    assert "paid_events__fetch" in processor_names
    assert "paid_details__fetch" in processor_names

    flow_pg_id = create_pg_calls[0]["id"]
    cross_pg_conns = [
        c for c in call_log
        if c["action"] == "create_connection"
        and c["pg"] == flow_pg_id
        and c["src_type"] == "OUTPUT_PORT"
        and c["dst_type"] == "INPUT_PORT"
    ]
    assert len(cross_pg_conns) >= 3


def test_csv_page_pagination_is_not_ignored(monkeypatch):
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "csv_export",
            "name": "csv_export",
            "endpoint_path": "/export.csv",
            "response_format": "csv",
            "split_response_records": False,
            "pagination": {
                "enabled": True,
                "type": "page",
                "page_param": "page",
                "page_size_param": "limit",
                "page_size": 100,
                "max_pages": 2,
            },
        }
    ])

    ok = asyncio.run(gen._build_rest_api_flow(
        "http://nifi:8080",
        "flow-pg",
        source,
        kafka_svc_id=None,
        json_reader_id=None,
        schema_reg_id=None,
        avro_writer_id=None,
        kafka_schema_reg_id=None,
        schema_ref_writer_id=None,
        kafka_topic="unused",
        schedule_str="5 min",
        auth_type_nifi="NONE",
        username_nifi=None,
        password_nifi=None,
        json_writer_id="json-writer",
    ))

    assert ok is True
    processors = [c for c in call_log if c["action"] == "create_processor"]
    fetch = next(p for p in processors if p["name"] == "csv_export__fetch")
    assert fetch["properties"]["HTTP URL"] == "https://api.example.com/export.csv?page=${page}&limit=100"


def test_next_url_pagination_uses_next_url_expression(monkeypatch):
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "records",
            "name": "records",
            "endpoint_path": "/records",
            "response_format": "json",
            "response_data_path": "$.items",
            "split_response_records": True,
            "pagination": {
                "enabled": True,
                "type": "next_url",
                "next_url_path": "$.next",
                "max_pages": 2,
            },
        }
    ])

    ok = asyncio.run(gen._build_rest_api_flow(
        "http://nifi:8080",
        "flow-pg",
        source,
        kafka_svc_id=None,
        json_reader_id=None,
        schema_reg_id=None,
        avro_writer_id=None,
        kafka_schema_reg_id=None,
        schema_ref_writer_id=None,
        kafka_topic="unused",
        schedule_str="5 min",
        auth_type_nifi="NONE",
        username_nifi=None,
        password_nifi=None,
    ))

    assert ok is True
    processors = [c for c in call_log if c["action"] == "create_processor"]
    fetch = next(p for p in processors if p["name"] == "records__fetch")
    init = next(p for p in processors if p["name"] == "records__init_page")
    page_meta = next(p for p in processors if p["name"] == "records__page_meta")
    has_more = next(p for p in processors if p["name"] == "records__has_more")
    next_page = next(p for p in processors if p["name"] == "records__next_page")
    assert init["properties"]["page_count"] == "1"
    assert fetch["properties"]["HTTP URL"] == "${next_url:replaceEmpty('https://api.example.com/records')}"
    assert page_meta["properties"]["next_url_candidate"] == "$.next"
    assert has_more["properties"]["has_more"] == (
        "${next_url_candidate:trim():isEmpty():not():and(${page_count:toNumber():lt(2)})}"
    )
    assert next_page["properties"]["next_url"] == "${next_url_candidate}"
    assert next_page["properties"]["page_count"] == "${page_count:toNumber():plus(1)}"


def test_legacy_polling_config_does_not_generate_polling_processors(monkeypatch):
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "poll_job",
            "name": "poll_job",
            "endpoint_path": "/jobs/${job_id}/status",
            "path_param_defaults": {"job_id": "demo-job"},
            "response_format": "json",
            "response_data_path": "$.items",
            "split_response_records": True,
            "polling": {
                "enabled": True,
                "status_path": "$.status",
                "pending_values": ["queued", "running"],
                "success_values": ["complete"],
                "failure_values": ["failed"],
                "interval_seconds": 10,
                "max_attempts": 3,
            },
        }
    ])

    ok = asyncio.run(gen._build_rest_api_flow(
        "http://nifi:8080",
        "flow-pg",
        source,
        kafka_svc_id=None,
        json_reader_id=None,
        schema_reg_id=None,
        avro_writer_id=None,
        kafka_schema_reg_id=None,
        schema_ref_writer_id=None,
        kafka_topic="unused",
        schedule_str="5 min",
        auth_type_nifi="NONE",
        username_nifi=None,
        password_nifi=None,
    ))

    assert ok is True
    processors = [c for c in call_log if c["action"] == "create_processor"]
    names = {p["name"] for p in processors}
    assert "poll_job__fetch" in names
    assert "poll_job__split" in names
    assert not any("__poll_" in name or "polling" in name for name in names)
    init = next(p for p in processors if p["name"] == "poll_job__init_page")
    assert "poll_attempt" not in init["properties"]


def test_rest_stream_http_failures_publish_runtime_error_event(monkeypatch):
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "customers",
            "name": "customers",
            "endpoint_path": "/customers",
            "response_format": "json",
            "entity": {"kafka": {"topic": "bronze.customers", "value_format": "json"}},
        }
    ])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source,
        flow_id="flow-phase12",
        nifi_conn=_make_nifi_conn(),
        kafka_conn=_make_kafka_conn(),
    ))

    assert result["ok"] is True
    processors = [c for c in call_log if c["action"] == "create_processor"]
    fetch = next(p for p in processors if p["name"] == "customers__fetch")
    marker = next(p for p in processors if p["name"] == "customers__mark_invoke_http_failure")
    builder = next(p for p in processors if p["name"] == "customers__build_failure_event")
    publisher = next(p for p in processors if p["name"] == "customers__publish_failure_event")

    assert "Failure" not in fetch["auto_terminate"]
    assert "No Retry" not in fetch["auto_terminate"]
    assert "Retry" not in fetch["auto_terminate"]
    assert marker["properties"]["rest.error.flow_id"] == "flow-phase12"
    assert marker["properties"]["rest.error.stream_id"] == "customers"
    assert marker["properties"]["rest.error.stage"] == "invoke_http"
    assert marker["properties"]["rest.error.processor"] == "customers__fetch"
    assert marker["properties"]["rest.error.category"] == "http"
    assert '"event_type":"rest_flow_failure"' in builder["properties"]["Replacement Value"]
    assert '"stage":"${rest.error.stage}"' in builder["properties"]["Replacement Value"]
    assert publisher["properties"]["Topic Name"] == "rest-flow-failures"
    assert publisher["properties"]["Kafka Connection Service"]

    connections = [c for c in call_log if c["action"] == "create_connection"]
    assert any(c["src"] == fetch["id"] and c["dst"] == marker["id"] and c["rels"] == ["Failure", "No Retry", "Retry"] for c in connections)
    assert any(c["src"] == marker["id"] and c["dst"] == builder["id"] and c["rels"] == ["success"] for c in connections)
    assert any(c["src"] == builder["id"] and c["dst"] == publisher["id"] and c["rels"] == ["success"] for c in connections)


# ---------------------------------------------------------------------------
# Failure-path tests: verify deploy returns ok=False when NiFi rejects a
# specific boundary connection type (the new cross-PG wiring calls).
# ---------------------------------------------------------------------------

def _make_failing_conn_mock(fail_when):
    """Return an async mock for _create_connection that returns None when
    fail_when(kw) is True, otherwise returns a fake connection ID.

    fail_when receives the keyword-argument dict of the call.
    """
    counter = {"n": 0}

    async def _mock(nifi_url, pg_id, src_id, dst_id, rels, **kw):
        if fail_when(kw):
            return None
        counter["n"] += 1
        return f"conn-{counter['n']}"

    return _mock


def _route_child_source():
    """Two-stream source: all_events (root with one route) + user1_events (route child)."""
    return _make_source([
        {
            "id": "all_events", "name": "all_events", "endpoint_path": "/events",
            "extraction_rules": [{"attribute_name": "userId", "path": "$.userId"}],
            "routes": [
                {
                    "ordinal": 0,
                    "child_stream_id": "user1_events",
                    "condition": {"attribute": "userId", "condition": "equals", "value": "1"},
                }
            ],
            "default_route_action": "drop",
        },
        {
            "id": "user1_events", "name": "user1_events",
            "route_source": {"parent_stream_id": "all_events"},
            "entity": {"kafka": {"topic": "bronze.user1_events", "value_format": ""}},
        },
    ])


def test_deploy_fails_if_port_to_proxy_conn_rejected(monkeypatch):
    """NiFi rejecting the input_port→proxy connection inside a child PG must fail the deploy.

    src_type='INPUT_PORT' uniquely identifies these connections (line 1045 in the pre-creation loop).
    """
    _fake_nifi_env(monkeypatch)
    monkeypatch.setattr(
        gen, "_create_connection",
        _make_failing_conn_mock(lambda kw: kw.get("src_type") == "INPUT_PORT"),
    )

    source = _make_source([
        {"id": "s1", "name": "s1", "endpoint_path": "/s1",
         "entity": {"kafka": {"topic": "bronze.s1", "value_format": ""}}},
    ])
    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))
    assert result["ok"] is False, (
        "Deploy should fail when input_port→proxy connection is rejected by NiFi"
    )


def test_deploy_fails_if_root_entry_cross_pg_conn_rejected(monkeypatch):
    """NiFi rejecting the set_auth→root-stream-input-port connection must fail the deploy.

    dst_type='INPUT_PORT' uniquely identifies the cross-PG entry connections
    posted to the flow PG (set_auth is the source, so src_type is PROCESSOR).
    """
    _fake_nifi_env(monkeypatch)
    monkeypatch.setattr(
        gen, "_create_connection",
        _make_failing_conn_mock(
            lambda kw: kw.get("dst_type") == "INPUT_PORT" and kw.get("src_type", "PROCESSOR") == "PROCESSOR"
        ),
    )

    source = _make_source([
        {"id": "s1", "name": "s1", "endpoint_path": "/s1",
         "entity": {"kafka": {"topic": "bronze.s1", "value_format": ""}}},
    ])
    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))
    assert result["ok"] is False, (
        "Deploy should fail when set_auth→root-stream input-port connection is rejected by NiFi"
    )


def test_deploy_fails_if_roa_to_output_port_conn_rejected(monkeypatch):
    """NiFi rejecting the RouteOnAttribute→output-port connection must fail the deploy.

    dst_type='OUTPUT_PORT' identifies the roa→output_port wiring inside the parent
    stream's child PG (route children and fan-out children share this pattern).
    """
    _fake_nifi_env(monkeypatch)
    monkeypatch.setattr(
        gen, "_create_connection",
        _make_failing_conn_mock(lambda kw: kw.get("dst_type") == "OUTPUT_PORT"),
    )

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=_route_child_source(), flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))
    assert result["ok"] is False, (
        "Deploy should fail when roa→output-port connection inside parent PG is rejected by NiFi"
    )


def test_deploy_fails_if_cross_pg_boundary_conn_rejected(monkeypatch):
    """NiFi rejecting the output-port→input-port cross-PG connection must fail the deploy.

    src_type='OUTPUT_PORT' identifies the cross-PG boundary connections posted
    to the flow PG (both route-child and fan-out wiring share this pattern).
    """
    _fake_nifi_env(monkeypatch)
    monkeypatch.setattr(
        gen, "_create_connection",
        _make_failing_conn_mock(lambda kw: kw.get("src_type") == "OUTPUT_PORT"),
    )

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=_route_child_source(), flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))
    assert result["ok"] is False, (
        "Deploy should fail when output-port→input-port cross-PG connection is rejected by NiFi"
    )
