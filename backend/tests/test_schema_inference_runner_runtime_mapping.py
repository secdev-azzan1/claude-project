import sys
from pathlib import Path

import pytest


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services.schema_inference_runner import _compile_rest_source_for_inference


def test_inference_runtime_values_resolve_path_query_header_and_body():
    source = {
        "type": "REST API",
        "streams": [
            {
                "id": "records",
                "name": "records",
                "endpoint_path": "/accounts/${account_id}/records",
                "query_params": {"status": "${status}"},
                "headers": {"Authorization": "Bearer ${token}"},
                "body_template": '{"account":"${account_id}"}',
                "parameter_bindings": [
                    {
                        "enabled": True,
                        "required": True,
                        "value_source": "manual_runtime",
                        "param_name": "account_id",
                    },
                    {
                        "enabled": True,
                        "required": True,
                        "value_source": "manual_runtime",
                        "param_name": "status",
                    },
                    {
                        "enabled": True,
                        "required": True,
                        "value_source": "manual_runtime",
                        "param_name": "token",
                    },
                ],
            }
        ],
    }

    compiled = _compile_rest_source_for_inference(
        source,
        {"account_id": "acct-1", "status": "active", "token": "secret-token"},
    )

    stream = compiled["streams"][0]
    assert stream["endpoint_path"] == "/accounts/acct-1/records"
    assert stream["query_params"] == {"status": "active"}
    assert stream["headers"] == {"Authorization": "Bearer secret-token"}
    assert stream["body_template"] == '{"account":"acct-1"}'


def test_inference_rejects_legacy_brace_placeholder_syntax():
    source = {
        "type": "REST API",
        "streams": [
            {
                "id": "records",
                "name": "records",
                "endpoint_path": "/accounts/{account_id}/records",
            }
        ],
    }

    with pytest.raises(ValueError, match=r"Use \$\{account_id\}"):
        _compile_rest_source_for_inference(source, {"account_id": "acct-1"})


def test_child_inference_preserves_parent_extracted_placeholders_over_defaults():
    source = {
        "type": "REST API",
        "streams": [
            {
                "id": "customers",
                "name": "customers",
                "endpoint_path": "/customers",
                "extraction_rules": [
                    {"attribute_name": "customer_id", "path": "$.items[0].id"},
                    {"attribute_name": "auth_token", "path": "$.token"},
                ],
            },
            {
                "id": "orders",
                "name": "orders",
                "endpoint_path": "/customers/${customer_id}/orders",
                "query_params": {"token": "${auth_token}", "status": "${status}"},
                "headers": {"X-Customer": "${customer_id}"},
                "body_template": '{"customer":"${customer_id}","status":"${status}"}',
                "path_param_defaults": {
                    "customer_id": "demo-customer",
                    "auth_token": "demo-token",
                    "status": "open",
                },
                "fan_out": {
                    "enabled": True,
                    "parent_stream_id": "customers",
                },
            },
        ],
    }

    compiled = _compile_rest_source_for_inference(source, {})
    child = compiled["streams"][1]

    assert child["endpoint_path"] == "/customers/${customer_id}/orders"
    assert child["query_params"]["token"] == "${auth_token}"
    assert child["query_params"]["status"] == "open"
    assert child["headers"]["X-Customer"] == "${customer_id}"
    assert child["body_template"] == '{"customer":"${customer_id}","status":"open"}'


def test_child_inference_parent_extraction_takes_priority_over_runtime_value():
    source = {
        "type": "REST API",
        "streams": [
            {
                "id": "customers",
                "name": "customers",
                "endpoint_path": "/customers",
                "extraction_rules": [
                    {"attribute_name": "customer_id", "path": "$.items[0].id"},
                ],
            },
            {
                "id": "orders",
                "name": "orders",
                "endpoint_path": "/customers/${customer_id}/orders",
                "path_param_defaults": {"customer_id": "demo-customer"},
                "fan_out": {
                    "enabled": True,
                    "parent_stream_id": "customers",
                },
            },
        ],
    }

    compiled = _compile_rest_source_for_inference(source, {"customer_id": "runtime-customer"})

    assert compiled["streams"][1]["endpoint_path"] == "/customers/${customer_id}/orders"
