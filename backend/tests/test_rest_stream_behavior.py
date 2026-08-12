import sys
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services.rest_stream_behavior import infer_rest_stream_behaviors, stream_publishes_to_kafka


def test_output_selection_is_only_entity_configuration():
    login = {
        "id": "login",
        "extraction_rules": [{"attribute_name": "jwt", "path": "$.token"}],
    }
    records = {
        "id": "records",
        "entity": {"kafka": {"topic": "bronze.records", "value_format": ""}},
    }

    assert stream_publishes_to_kafka(login) is False
    assert stream_publishes_to_kafka(records) is True


def test_target_stream_id_only_selects_inference_publish_target():
    stream = {"id": "login", "extraction_rules": [{"attribute_name": "jwt", "path": "$.token"}]}

    assert stream_publishes_to_kafka(stream) is False
    assert stream_publishes_to_kafka(stream, target_stream_id="login") is True
    assert stream_publishes_to_kafka(stream, target_stream_id="records") is False


def test_parent_parallel_and_route_behavior_are_independent_from_output():
    source = {
        "streams": [
            {
                "id": "records",
                "entity": {"kafka": {"topic": "bronze.records", "value_format": ""}},
                "routes": [
                    {
                        "id": "paid_route",
                        "child_stream_id": "paid_records",
                        "condition": {"attribute": "status", "condition": "equals", "value": "paid"},
                    }
                ],
                "default_route_action": "route_to_stream",
                "default_route_child_stream_id": "other_records",
            },
            {
                "id": "details",
                "fan_out": {"enabled": True, "parent_stream_id": "records"},
            },
            {
                "id": "paid_records",
                "route_source": {"parent_stream_id": "records", "route_id": "paid_route"},
                "request_enabled": False,
                "entity": {"kafka": {"topic": "bronze.paid_records", "value_format": ""}},
            },
            {
                "id": "other_records",
                "route_source": {"parent_stream_id": "records", "route_id": "default"},
                "request_enabled": True,
            },
        ]
    }

    behaviors = infer_rest_stream_behaviors(source)

    assert behaviors["records"].publishes_to_kafka is True
    assert behaviors["records"].is_parent is True
    assert behaviors["records"].has_parallel_children is True

    assert behaviors["details"].publishes_to_kafka is False
    assert behaviors["details"].is_child_request is True

    assert behaviors["paid_records"].publishes_to_kafka is True
    assert behaviors["paid_records"].is_conditional_route_child is True
    assert behaviors["paid_records"].route_child_makes_request is False

    assert behaviors["other_records"].publishes_to_kafka is False
    assert behaviors["other_records"].is_default_route_child is True
    assert behaviors["other_records"].route_child_makes_request is True
