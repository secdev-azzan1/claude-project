"""Tests for pagination loop-back wiring: POST body streams must loop to the
body builder, GET streams must loop to fetch, and neither must ever loop
back to init_page (which would reset the page counter)."""
import asyncio
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from tests.test_nifi_flow_generator_per_stream_pg import (
    _fake_nifi_env, _make_source, _make_nifi_conn, _make_kafka_conn,
)
from services import nifi_flow_generator as gen


def test_post_body_pagination_loops_back_to_body_builder(monkeypatch):
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "items", "name": "items", "method": "POST", "endpoint_path": "/search",
            "body_format": "json", "body_template": '{"page": ${page}}',
            "response_format": "json", "split_response_records": True,
            "response_data_path": "$.items",
            "pagination": {"enabled": True, "type": "page", "page_param": "page",
                           "page_size_param": "page_size", "page_size": 10,
                           "first_page_number": 1, "max_pages": 10,
                           "stop_condition": "EMPTY_ARRAY"},
            "entity": {"kafka": {"topic": "bronze.items", "value_format": ""}},
        }
    ])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))
    assert result["ok"] is True

    processors = [c for c in call_log if c["action"] == "create_processor"]
    next_page = next(p for p in processors if p["name"] == "items__next_page")
    build_request_body = next(p for p in processors if p["name"] == "items__build_request_body")
    fetch = next(p for p in processors if p["name"] == "items__fetch")

    connections = [c for c in call_log if c["action"] == "create_connection"]
    assert any(
        c["src"] == next_page["id"] and c["dst"] == build_request_body["id"] for c in connections
    )
    assert not any(
        c["src"] == next_page["id"] and c["dst"] == fetch["id"] for c in connections
    )


def test_get_pagination_loops_back_to_fetch(monkeypatch):
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "items", "name": "items", "method": "GET", "endpoint_path": "/items",
            "body_format": "empty",
            "response_format": "json", "split_response_records": True,
            "response_data_path": "$.items",
            "pagination": {"enabled": True, "type": "page", "page_param": "page",
                           "page_size_param": "page_size", "page_size": 10,
                           "first_page_number": 1, "max_pages": 10,
                           "stop_condition": "EMPTY_ARRAY"},
            "entity": {"kafka": {"topic": "bronze.items", "value_format": ""}},
        }
    ])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))
    assert result["ok"] is True

    processors = [c for c in call_log if c["action"] == "create_processor"]
    next_page = next(p for p in processors if p["name"] == "items__next_page")
    fetch = next(p for p in processors if p["name"] == "items__fetch")

    assert not any(p["name"] == "items__build_request_body" for p in processors)

    connections = [c for c in call_log if c["action"] == "create_connection"]
    assert any(
        c["src"] == next_page["id"] and c["dst"] == fetch["id"] for c in connections
    )

    init_page_procs = [p for p in processors if p["name"].endswith("__init_page")]
    init_page_ids = {p["id"] for p in init_page_procs}
    assert not any(
        c["src"] == next_page["id"] and c["dst"] in init_page_ids for c in connections
    )
