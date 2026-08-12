from pathlib import Path
import sys

import pytest
from pydantic import ValidationError

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from models.source import Stream


def test_stream_body_format_defaults_to_empty():
    stream = Stream(name="Records")

    assert stream.body_format == "empty"


def test_stream_body_format_normalizes_supported_values():
    stream = Stream(name="Create Record", method="post", body_format="JSON")

    assert stream.method == "POST"
    assert stream.body_format == "json"


def test_stream_body_format_rejects_unsupported_values():
    with pytest.raises(ValidationError, match="body_format must be one of"):
        Stream(name="Create Record", method="POST", body_format="yaml")


def test_stream_pagination_preserves_ui_contract_fields():
    stream = Stream(
        name="Records",
        pagination={
            "enabled": True,
            "type": "page",
            "page_param": "page",
            "page_size_param": "limit",
            "page_size": 100,
            "first_page_number": 0,
            "max_pages": 5,
            "total_count_stop_rule": "FETCHED_RECORDS_GTE_TOTAL",
            "stop_condition": "MAX_PAGES",
        },
    )

    assert stream.pagination.first_page_number == 0
    assert stream.pagination.max_pages == 5
    assert stream.pagination.total_count_stop_rule == "FETCHED_RECORDS_GTE_TOTAL"
