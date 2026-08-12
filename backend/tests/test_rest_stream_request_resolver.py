from pathlib import Path
import sys

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services.rest_stream_request_resolver import (
    RestStreamRequestError,
    resolve_rest_stream_request,
)


def test_resolves_post_json_request_with_defaults_auth_and_headers():
    source = {
        "base_url": "https://api.example.com",
        "auth_type": "BEARER",
        "bearer_token": "secret-token",
    }
    stream = {
        "name": "Update Device",
        "method": "POST",
        "endpoint_path": "/devices/${device_id}",
        "path_param_defaults": {"device_id": "dev-1"},
        "headers": {"X-Tenant": "${tenant}", "Accept": "application/vnd.example+json"},
        "query_params": {"mode": "full"},
        "body_format": "json",
        "body_template": '{"status":"${status}"}',
        "response_format": "json",
    }

    request = resolve_rest_stream_request(
        source,
        stream,
        {"tenant": "acme", "status": "active"},
        confirmed_mutation=True,
    )

    assert request.method == "POST"
    assert request.url == "https://api.example.com/devices/dev-1"
    assert request.params == {"mode": "full"}
    assert request.headers["Authorization"] == "Bearer secret-token"
    assert request.headers["Content-Type"] == "application/json"
    assert request.headers["Accept"] == "application/vnd.example+json"
    assert request.headers["X-Tenant"] == "acme"
    assert request.body == b'{"status":"active"}'
    assert request.body_json == {"status": "active"}
    assert request.masked_headers["Authorization"] == "Bearer ***"


def test_requires_confirmation_for_mutating_methods():
    with pytest.raises(RestStreamRequestError, match="requires explicit confirmation"):
        resolve_rest_stream_request(
            {"base_url": "https://api.example.com", "auth_type": "NONE"},
            {
                "name": "Delete Device",
                "method": "DELETE",
                "endpoint_path": "/devices/1",
                "body_format": "empty",
                "response_format": "json",
            },
            {},
        )


def test_rejects_unresolved_placeholders_before_request_is_sent():
    with pytest.raises(RestStreamRequestError, match="unresolved placeholder"):
        resolve_rest_stream_request(
            {"base_url": "https://api.example.com", "auth_type": "NONE"},
            {
                "name": "Device",
                "method": "GET",
                "endpoint_path": "/devices/${device_id}",
                "response_format": "json",
            },
            {},
        )


def test_rejects_legacy_brace_placeholder_syntax():
    with pytest.raises(RestStreamRequestError, match=r"Use \$\{device_id\}"):
        resolve_rest_stream_request(
            {"base_url": "https://api.example.com", "auth_type": "NONE"},
            {
                "name": "Device",
                "method": "GET",
                "endpoint_path": "/devices/{device_id}",
                "response_format": "json",
            },
            {"device_id": "dev-1"},
        )


def test_resolves_parent_values_in_path_query_header_and_json_body():
    request = resolve_rest_stream_request(
        {"base_url": "https://api.example.com", "auth_type": "NONE"},
        {
            "name": "Update Device",
            "method": "PATCH",
            "endpoint_path": "/devices/${device_id}",
            "query_params": {"account": "${account_id}"},
            "headers": {"Authorization": "Bearer ${token}"},
            "body_format": "json",
            "body_template": '{"device":"${device_id}","status":"${status}"}',
            "response_format": "json",
        },
        {
            "device_id": "dev-1",
            "account_id": "acct-7",
            "token": "secret-token",
            "status": "active",
        },
        confirmed_mutation=True,
    )

    assert request.url == "https://api.example.com/devices/dev-1"
    assert request.params == {"account": "acct-7"}
    assert request.headers["Authorization"] == "Bearer secret-token"
    assert request.masked_headers["Authorization"] == "Bearer ***"
    assert request.body_text == '{"device":"dev-1","status":"active"}'


def test_resolves_parent_values_in_xml_and_csv_bodies():
    xml_request = resolve_rest_stream_request(
        {"base_url": "https://api.example.com", "auth_type": "NONE"},
        {
            "name": "Xml Update",
            "method": "PATCH",
            "endpoint_path": "/records/${record_id}",
            "body_format": "xml",
            "body_template": "<record><id>${record_id}</id></record>",
            "response_format": "xml",
        },
        {"record_id": "rec-1"},
        confirmed_mutation=True,
    )
    assert xml_request.body_text == "<record><id>rec-1</id></record>"

    csv_request = resolve_rest_stream_request(
        {"base_url": "https://api.example.com", "auth_type": "NONE"},
        {
            "name": "Csv Upload",
            "method": "PUT",
            "endpoint_path": "/records/${record_id}",
            "body_format": "csv",
            "body_template": "id,status\n${record_id},processed",
            "response_format": "csv",
        },
        {"record_id": "rec-1"},
        confirmed_mutation=True,
    )
    assert csv_request.body_text == "id,status\nrec-1,processed"


def test_rejects_invalid_json_body_after_substitution():
    with pytest.raises(RestStreamRequestError, match="valid JSON"):
        resolve_rest_stream_request(
            {"base_url": "https://api.example.com", "auth_type": "NONE"},
            {
                "name": "Search",
                "method": "POST",
                "endpoint_path": "/search",
                "body_format": "json",
                "body_template": '{"status": ${status}}',
                "response_format": "json",
            },
            {"status": "active"},
            confirmed_mutation=True,
        )


def test_resolves_api_key_query_auth_and_csv_body():
    request = resolve_rest_stream_request(
        {
            "base_url": "https://api.example.com",
            "auth_type": "API_KEY",
            "api_key_name": "api_key",
            "api_key_value": "secret-key",
            "api_key_location": "QUERY",
        },
        {
            "name": "Upload Rows",
            "method": "PUT",
            "endpoint_path": "/rows",
            "body_format": "csv",
            "body_template": "id,name\n1,Ada",
            "response_format": "csv",
        },
        {},
        confirmed_mutation=True,
    )

    assert request.params == {"api_key": "secret-key"}
    assert request.masked_params == {"api_key": "***"}
    assert request.headers["Content-Type"] == "text/csv"
    assert request.headers["Accept"] == "text/csv"
    assert request.body == b"id,name\n1,Ada"


def test_resolves_basic_auth_and_xml_body():
    request = resolve_rest_stream_request(
        {
            "base_url": "https://api.example.com",
            "auth_type": "BASIC",
            "rest_username": "user",
            "rest_password": "pass",
        },
        {
            "name": "Update",
            "method": "PATCH",
            "endpoint_path": "/records/1",
            "body_format": "xml",
            "body_template": "<record><status>active</status></record>",
            "response_format": "xml",
        },
        {},
        confirmed_mutation=True,
    )

    assert request.headers["Authorization"].startswith("Basic ")
    assert request.masked_headers["Authorization"] == "Basic ***"
    assert request.headers["Content-Type"] == "application/xml"
    assert request.headers["Accept"] == "application/xml"
    assert request.body == b"<record><status>active</status></record>"


def test_rejects_malformed_xml_body_after_substitution():
    with pytest.raises(RestStreamRequestError, match="well-formed XML"):
        resolve_rest_stream_request(
            {"base_url": "https://api.example.com", "auth_type": "NONE"},
            {
                "name": "Update",
                "method": "PATCH",
                "endpoint_path": "/records/1",
                "body_format": "xml",
                "body_template": "<record>",
                "response_format": "xml",
            },
            {},
            confirmed_mutation=True,
        )
