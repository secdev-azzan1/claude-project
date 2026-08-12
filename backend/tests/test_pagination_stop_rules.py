"""Tests for pagination stop-rule expressions in NiFi flow generation."""
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


def _stream(pagination):
    return {
        "id": "items", "name": "items", "method": "GET", "endpoint_path": "/items",
        "response_format": "json", "split_response_records": True,
        "response_data_path": "$.items", "pagination": pagination,
        "entity": {"kafka": {"topic": "bronze.items", "value_format": ""}},
    }


def test_page_total_count_uses_page_count_times_page_size(monkeypatch):
    call_log = _fake_nifi_env(monkeypatch)
    pagination = {
        "enabled": True, "type": "page", "page_param": "page", "page_size_param": "page_size",
        "page_size": 10, "first_page_number": 1, "max_pages": 10,
        "stop_condition": "TOTAL_COUNT", "total_count_path": "$.total_count",
    }
    source = _make_source([_stream(pagination)])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))
    assert result["ok"] is True

    processors = [c for c in call_log if c["action"] == "create_processor"]
    has_more = next(p for p in processors if p["name"] == "items__has_more")
    assert has_more["properties"]["has_more"] == (
        "${total_count:isEmpty():or(${page_count:toNumber():multiply(10):lt(${total_count:toNumber()})}):and(${page_count:toNumber():lt(10)})}"
    )

    page_meta = next(p for p in processors if p["name"] == "items__page_meta")
    assert page_meta["properties"]["total_count"] == "$.total_count"
    assert "total_pages" not in page_meta["properties"]


def test_page_empty_array_stop_uses_fragment_count(monkeypatch):
    call_log = _fake_nifi_env(monkeypatch)
    pagination = {
        "enabled": True, "type": "page", "page_param": "page", "page_size_param": "page_size",
        "page_size": 10, "first_page_number": 1, "max_pages": 10,
        "stop_condition": "EMPTY_ARRAY",
    }
    source = _make_source([_stream(pagination)])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))
    assert result["ok"] is True

    processors = [c for c in call_log if c["action"] == "create_processor"]
    has_more = next(p for p in processors if p["name"] == "items__has_more")
    assert has_more["properties"]["has_more"] == (
        "${fragment.count:replaceEmpty('0'):gt(0):and(${page_count:toNumber():lt(10)})}"
    )


def test_page_has_more_flag_stop(monkeypatch):
    call_log = _fake_nifi_env(monkeypatch)
    pagination = {
        "enabled": True, "type": "page", "page_param": "page", "page_size_param": "page_size",
        "page_size": 10, "first_page_number": 1, "max_pages": 10,
        "stop_condition": "HAS_MORE", "has_more_path": "$.has_more",
    }
    source = _make_source([_stream(pagination)])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))
    assert result["ok"] is True

    processors = [c for c in call_log if c["action"] == "create_processor"]
    has_more = next(p for p in processors if p["name"] == "items__has_more")
    assert has_more["properties"]["has_more"] == (
        "${has_more_flag:equals('false'):not():and(${page_count:toNumber():lt(10)})}"
    )

    page_meta = next(p for p in processors if p["name"] == "items__page_meta")
    assert page_meta["properties"]["has_more_flag"] == "$.has_more"


def test_offset_total_count_expression_unchanged(monkeypatch):
    call_log = _fake_nifi_env(monkeypatch)
    pagination = {
        "enabled": True, "type": "offset", "offset_param": "offset", "limit_param": "limit",
        "page_size": 10, "max_pages": 10,
        "stop_condition": "TOTAL_COUNT", "total_count_path": "$.total_count",
    }
    source = _make_source([_stream(pagination)])

    result = asyncio.run(gen.generate_and_deploy_flow(
        source=source, flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))
    assert result["ok"] is True

    processors = [c for c in call_log if c["action"] == "create_processor"]
    has_more = next(p for p in processors if p["name"] == "items__has_more")
    assert has_more["properties"]["has_more"] == (
        "${total_count:isEmpty():or(${offset:toNumber():plus(10):lt(${total_count:toNumber()})}):and(${page_count:toNumber():lt(10)})}"
    )
