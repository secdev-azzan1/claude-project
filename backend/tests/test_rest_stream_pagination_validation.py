import sys
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services.rest_stream_pagination_validation import (
    RestPaginationValidationError,
    validate_rest_stream_pagination,
)


def test_validates_json_page_offset_cursor_and_next_url_pagination():
    streams = [
        {
            "id": "json_page",
            "response_format": "json",
            "split_response_records": True,
            "pagination": {
                "enabled": True,
                "type": "page",
                "page_param": "page",
                "page_size_param": "limit",
                "page_size": 100,
                "max_pages": 3,
            },
        },
        {
            "id": "json_offset",
            "response_format": "json",
            "split_response_records": True,
            "pagination": {
                "enabled": True,
                "type": "offset",
                "offset_param": "offset",
                "limit_param": "limit",
                "page_size": 100,
                "max_pages": 3,
            },
        },
        {
            "id": "json_cursor",
            "response_format": "json",
            "split_response_records": True,
            "pagination": {
                "enabled": True,
                "type": "cursor",
                "cursor_param": "cursor",
                "cursor_path": "$.next_cursor",
                "max_pages": 3,
            },
        },
        {
            "id": "json_next",
            "response_format": "json",
            "split_response_records": True,
            "pagination": {
                "enabled": True,
                "type": "next_url",
                "next_url_path": "$.next",
                "max_pages": 3,
            },
        },
    ]

    validate_rest_stream_pagination({"streams": streams})


def test_csv_supports_page_and_offset_but_rejects_cursor_without_header_source():
    validate_rest_stream_pagination({
        "streams": [
            {
                "id": "csv_page",
                "response_format": "csv",
                "split_response_records": True,
                "pagination": {
                    "enabled": True,
                    "type": "page",
                    "page_param": "page",
                    "page_size_param": "limit",
                    "page_size": 100,
                    "max_pages": 3,
                },
            },
            {
                "id": "csv_offset",
                "response_format": "csv",
                "split_response_records": True,
                "pagination": {
                    "enabled": True,
                    "type": "offset",
                    "offset_param": "offset",
                    "limit_param": "limit",
                    "page_size": 100,
                    "max_pages": 3,
                },
            },
        ]
    })

    with pytest.raises(RestPaginationValidationError, match="CSV cursor pagination requires header"):
        validate_rest_stream_pagination({
            "streams": [
                {
                    "id": "csv_cursor",
                    "response_format": "csv",
                    "split_response_records": True,
                    "pagination": {
                        "enabled": True,
                        "type": "cursor",
                        "cursor_param": "cursor",
                        "cursor_path": "$.next",
                        "max_pages": 3,
                    },
                }
            ]
        })


def test_rejects_pagination_without_safe_stop_condition():
    with pytest.raises(RestPaginationValidationError, match="max_pages"):
        validate_rest_stream_pagination({
            "streams": [
                {
                    "id": "unsafe_page",
                    "response_format": "json",
                    "pagination": {
                        "enabled": True,
                        "type": "page",
                        "page_param": "page",
                        "page_size_param": "limit",
                        "page_size": 100,
                    },
                }
            ]
        })
