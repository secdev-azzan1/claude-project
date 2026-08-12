"""Optional max_pages: generator emission + validation rules.

Encodes the external-review test matrix for making max_pages optional:
 - generator OMITS the max-pages bound when max_pages is absent (was: silently defaulted to 1)
 - generator KEEPS the bound when max_pages is present (regression)
 - validation REQUIRES max_pages only for stop conditions with no intrinsic terminator
   (total_count / has_more) and ALLOWS it blank for cursor / next_url / empty-page.
"""
import asyncio
import sys
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from tests.test_nifi_flow_generator_per_stream_pg import (
    _fake_nifi_env, _make_source, _make_nifi_conn, _make_kafka_conn,
)
from services import nifi_flow_generator as gen
from services.rest_stream_pagination_validation import (
    RestPaginationValidationError,
    validate_rest_stream_pagination,
)


def _gen_stream(pagination):
    return {
        "id": "items", "name": "items", "method": "GET", "endpoint_path": "/items",
        "response_format": "json", "split_response_records": True,
        "response_data_path": "$.items", "pagination": pagination,
        "entity": {"kafka": {"topic": "bronze.items", "value_format": ""}},
    }


def _has_more_prop(monkeypatch, pagination):
    call_log = _fake_nifi_env(monkeypatch)
    result = asyncio.run(gen.generate_and_deploy_flow(
        source=_make_source([_gen_stream(pagination)]), flow_id="flow-1",
        nifi_conn=_make_nifi_conn(), kafka_conn=_make_kafka_conn(),
    ))
    assert result["ok"] is True
    procs = [c for c in call_log if c["action"] == "create_processor"]
    hm = next(p for p in procs if p["name"] == "items__has_more")
    return hm["properties"]["has_more"]


# ---- Generator: max_pages OMITTED -> no bound term ----

def test_cursor_without_max_pages_omits_bound(monkeypatch):
    has_more = _has_more_prop(monkeypatch, {
        "enabled": True, "type": "cursor", "cursor_param": "cursor", "cursor_path": "$.next_cursor",
        "page_size_param": "page_size", "page_size": 10, "stop_condition": "EMPTY_ARRAY",
    })
    assert has_more == "${next_cursor:trim():isEmpty():not()}"


def test_next_url_without_max_pages_omits_bound(monkeypatch):
    has_more = _has_more_prop(monkeypatch, {
        "enabled": True, "type": "next_url", "next_url_path": "$.next_url", "stop_condition": "EMPTY_ARRAY",
    })
    assert has_more == "${next_url_candidate:trim():isEmpty():not()}"


def test_page_empty_stop_without_max_pages_omits_bound(monkeypatch):
    has_more = _has_more_prop(monkeypatch, {
        "enabled": True, "type": "page", "page_param": "page", "page_size_param": "page_size",
        "page_size": 10, "first_page_number": 1, "stop_condition": "EMPTY_ARRAY",
    })
    assert has_more == "${fragment.count:replaceEmpty('0'):gt(0)}"


def test_cursor_with_max_pages_keeps_bound(monkeypatch):
    has_more = _has_more_prop(monkeypatch, {
        "enabled": True, "type": "cursor", "cursor_param": "cursor", "cursor_path": "$.next_cursor",
        "page_size_param": "page_size", "page_size": 10, "max_pages": 10, "stop_condition": "EMPTY_ARRAY",
    })
    assert has_more == "${next_cursor:trim():isEmpty():not():and(${page_count:toNumber():lt(10)})}"


# ---- Validation: required only without an intrinsic terminator ----

def test_total_count_without_max_pages_is_rejected():
    with pytest.raises(RestPaginationValidationError, match="safety limit"):
        validate_rest_stream_pagination({"streams": [{
            "id": "s", "response_format": "json", "split_response_records": True,
            "pagination": {"enabled": True, "type": "page", "page_param": "page",
                           "page_size_param": "page_size", "page_size": 10,
                           "stop_condition": "TOTAL_COUNT", "total_count_path": "$.total_count"},
        }]})


def test_has_more_without_max_pages_is_rejected():
    with pytest.raises(RestPaginationValidationError, match="safety limit"):
        validate_rest_stream_pagination({"streams": [{
            "id": "s", "response_format": "json", "split_response_records": True,
            "pagination": {"enabled": True, "type": "page", "page_param": "page",
                           "page_size_param": "page_size", "page_size": 10,
                           "stop_condition": "HAS_MORE", "has_more_path": "$.has_more"},
        }]})


def test_cursor_without_max_pages_is_allowed():
    validate_rest_stream_pagination({"streams": [{
        "id": "s", "response_format": "json", "split_response_records": True,
        "pagination": {"enabled": True, "type": "cursor", "cursor_param": "cursor",
                       "cursor_path": "$.next_cursor"},
    }]})


def test_next_url_without_max_pages_is_allowed():
    validate_rest_stream_pagination({"streams": [{
        "id": "s", "response_format": "json", "split_response_records": True,
        "pagination": {"enabled": True, "type": "next_url", "next_url_path": "$.next_url"},
    }]})


def test_page_empty_stop_without_max_pages_is_allowed():
    validate_rest_stream_pagination({"streams": [{
        "id": "s", "response_format": "json", "split_response_records": True,
        "pagination": {"enabled": True, "type": "page", "page_param": "page",
                       "page_size_param": "page_size", "page_size": 10,
                       "stop_condition": "EMPTY_ARRAY"},
    }]})
