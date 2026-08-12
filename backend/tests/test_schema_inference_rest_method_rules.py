from pathlib import Path
import sys

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from routers.schema_inference import (  # noqa: E402
    _blocked_auto_inference_target,
    _missing_template_param_resolutions,
    _rest_inference_request_issues,
)


def _source(method: str = "GET", **stream_overrides):
    stream = {
        "id": "target",
        "name": "target_stream",
        "method": method,
        "endpoint_path": "/items",
        "entity": {"kafka": {"topic": "bronze.target"}},
        **stream_overrides,
    }
    return {"type": "REST API", "streams": [stream]}


def test_get_and_post_are_allowed_auto_inference_targets():
    assert _blocked_auto_inference_target(_source("GET"), "target") is None
    assert _blocked_auto_inference_target(_source("POST"), "target") is None


def test_put_patch_delete_are_blocked_auto_inference_targets():
    assert _blocked_auto_inference_target(_source("PUT"), "target") == ("target_stream", "PUT")
    assert _blocked_auto_inference_target(_source("PATCH"), "target") == ("target_stream", "PATCH")
    assert _blocked_auto_inference_target(_source("DELETE"), "target") == ("target_stream", "DELETE")


def test_post_body_format_requires_template_for_inference():
    issues = _rest_inference_request_issues(_source("POST", body_format="json", body_template=""))
    assert issues == ["target_stream: body_format=json requires a request body template"]


def test_post_parent_placeholders_are_resolved_from_parent_extractions():
    source = {
        "type": "REST API",
        "streams": [
            {
                "id": "items",
                "name": "items",
                "endpoint_path": "/items",
                "extraction_rules": [
                    {"attribute_name": "item_id", "path": "$.item_id"},
                    {"attribute_name": "item_name", "path": "$.name"},
                ],
            },
            {
                "id": "audit",
                "name": "audit",
                "method": "POST",
                "endpoint_path": "/audit",
                "body_format": "json",
                "body_template": '{"item_id":"${item_id}","item_name":"${item_name}"}',
                "fan_out": {"enabled": True, "parent_stream_id": "items"},
                "entity": {"kafka": {"topic": "bronze.audit"}},
            },
        ],
    }
    assert _missing_template_param_resolutions(source) == []


def test_post_unresolved_body_placeholder_is_reported_for_inference():
    source = _source(
        "POST",
        body_format="json",
        body_template='{"item_id":"${missing_id}"}',
    )
    assert _missing_template_param_resolutions(source) == [("target_stream", ["missing_id"])]
