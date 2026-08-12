"""Tests for pagination header sources and body-location injection."""
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


def test_cursor_header_uses_update_attribute_not_evaluate_jsonpath(monkeypatch):
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "items",
            "name": "items",
            "method": "GET",
            "endpoint_path": "/items",
            "response_format": "json",
            "response_data_path": "$.items",
            "split_response_records": True,
            "entity": {"kafka": {"topic": "bronze.items", "value_format": ""}},
            "pagination": {
                "enabled": True, "type": "cursor", "cursor_param": "cursor",
                "cursor_header": "X-Next-Cursor", "page_size_param": "page_size", "page_size": 10,
                "max_pages": 10, "stop_condition": "EMPTY_ARRAY",
            },
        }
    ])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))
    assert result["ok"] is True

    processors = [c for c in call_log if c["action"] == "create_processor"]
    page_meta_headers = next(p for p in processors if p["name"] == "items__page_meta_headers")
    assert page_meta_headers["type"] == "org.apache.nifi.processors.attributes.UpdateAttribute"
    assert page_meta_headers["properties"]["next_cursor"] == (
        "${'X-Next-Cursor':replaceEmpty(${'x-next-cursor'})}"
    )

    assert not any(p["name"] == "items__page_meta" for p in processors)

    connections = [c for c in call_log if c["action"] == "create_connection"]
    has_more = next(p for p in processors if p["name"] == "items__has_more")
    split = next(p for p in processors if p["name"] == "items__split")

    assert any(
        c["src"] == page_meta_headers["id"] and c["dst"] == has_more["id"] and c["rels"] == ["success"]
        for c in connections
    )
    assert any(
        c["src"] == split["id"] and c["dst"] == page_meta_headers["id"] and c["rels"] == ["original"]
        for c in connections
    )


def test_next_url_link_header_uses_guarded_regex(monkeypatch):
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "items",
            "name": "items",
            "method": "GET",
            "endpoint_path": "/items",
            "response_format": "json",
            "response_data_path": "$.items",
            "split_response_records": True,
            "entity": {"kafka": {"topic": "bronze.items", "value_format": ""}},
            "pagination": {
                "enabled": True, "type": "next_url", "next_url_header": "Link",
                "next_url_header_format": "link", "max_pages": 10, "stop_condition": "EMPTY_ARRAY",
            },
        }
    ])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))
    assert result["ok"] is True

    processors = [c for c in call_log if c["action"] == "create_processor"]
    page_meta_headers = next(p for p in processors if p["name"] == "items__page_meta_headers")
    # The guard is mandatory because a bare replaceAll returns the input unchanged
    # when there is no rel="next", which loops forever on the last page.
    assert page_meta_headers["properties"]["next_url_candidate"] == (
        "${'Link':find('rel=\"next\"'):ifElse(${'Link':replaceAll('(?s).*<([^>]*)>;[^,]*rel=\"next\".*','$1')}, '')}"
    )


def test_next_url_raw_header_format(monkeypatch):
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "items",
            "name": "items",
            "method": "GET",
            "endpoint_path": "/items",
            "response_format": "json",
            "response_data_path": "$.items",
            "split_response_records": True,
            "entity": {"kafka": {"topic": "bronze.items", "value_format": ""}},
            "pagination": {
                "enabled": True, "type": "next_url", "next_url_header": "X-Next-Url",
                "next_url_header_format": "raw", "max_pages": 10, "stop_condition": "EMPTY_ARRAY",
            },
        }
    ])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))
    assert result["ok"] is True

    processors = [c for c in call_log if c["action"] == "create_processor"]
    page_meta_headers = next(p for p in processors if p["name"] == "items__page_meta_headers")
    assert page_meta_headers["properties"]["next_url_candidate"] == (
        "${'X-Next-Url':replaceEmpty(${'x-next-url'})}"
    )


def test_body_location_injects_into_body_and_keeps_url_clean(monkeypatch):
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "items",
            "name": "items",
            "method": "POST",
            "endpoint_path": "/search",
            "body_format": "json",
            "body_template": '{"filter": {"status": "active"}}',
            "response_format": "json",
            "split_response_records": True,
            "entity": {"kafka": {"topic": "bronze.items", "value_format": ""}},
            "pagination": {
                "enabled": True, "type": "page", "page_param": "page", "page_size_param": "page_size",
                "page_size": 10, "first_page_number": 1, "max_pages": 10, "location": "body",
                "stop_condition": "HAS_MORE", "has_more_path": "$.has_more",
            },
        }
    ])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))
    assert result["ok"] is True

    processors = [c for c in call_log if c["action"] == "create_processor"]
    build_body = next(p for p in processors if p["name"] == "items__build_request_body")
    assert build_body["properties"]["Replacement Value"] == (
        '{"filter": {"status": "active"}, "page": ${page}, "page_size": 10}'
    )

    fetch = next(p for p in processors if p["name"] == "items__fetch")
    assert "page=" not in fetch["properties"]["HTTP URL"]


def test_query_location_still_puts_values_in_url(monkeypatch):
    call_log = _fake_nifi_env(monkeypatch)
    source = _make_source([
        {
            "id": "items",
            "name": "items",
            "method": "GET",
            "endpoint_path": "/items",
            "body_format": "empty",
            "response_format": "json",
            "split_response_records": True,
            "entity": {"kafka": {"topic": "bronze.items", "value_format": ""}},
            "pagination": {
                "enabled": True, "type": "page", "page_param": "page", "page_size_param": "page_size",
                "page_size": 10, "first_page_number": 1, "max_pages": 10, "location": "query",
                "stop_condition": "HAS_MORE", "has_more_path": "$.has_more",
            },
        }
    ])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))
    assert result["ok"] is True

    processors = [c for c in call_log if c["action"] == "create_processor"]
    fetch = next(p for p in processors if p["name"] == "items__fetch")
    assert "page=${page}" in fetch["properties"]["HTTP URL"]
