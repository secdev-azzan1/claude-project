import sys
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services.rest_stream_branch_validation import RestBranchValidationError, validate_rest_branch_graph
from models.source import SourceCreate


def test_nested_parallel_and_conditional_branch_tree_is_valid():
    source = {
        "streams": [
            {
                "id": "orders",
            },
            {
                "id": "audit_branch",
                "fan_out": {"enabled": True, "parent_stream_id": "orders"},
            },
            {
                "id": "route_branch",
                "fan_out": {"enabled": True, "parent_stream_id": "orders"},
                "extraction_rules": [{"attribute_name": "order_status", "path": "$.status"}],
                "routes": [
                    {
                        "id": "ready_route",
                        "child_stream_id": "ready_orders",
                        "condition": {"attribute": "order_status", "condition": "equals", "value": "READY"},
                    }
                ],
                "default_route_action": "drop",
            },
            {
                "id": "ready_orders",
                "route_source": {"parent_stream_id": "route_branch", "route_id": "ready_route"},
                "request_enabled": True,
            },
            {
                "id": "ready_details",
                "fan_out": {"enabled": True, "parent_stream_id": "ready_orders"},
            },
        ]
    }

    result = validate_rest_branch_graph(source)

    assert result.children_by_parent["orders"] == ["audit_branch", "route_branch"]
    assert result.children_by_parent["route_branch"] == ["ready_orders"]
    assert result.children_by_parent["ready_orders"] == ["ready_details"]
    assert result.incoming_by_child["ready_orders"] == ["route_branch"]
    assert result.incoming_by_child["ready_details"] == ["ready_orders"]


def test_same_stream_can_have_parallel_and_conditional_children():
    source = {
        "streams": [
            {
                "id": "events",
                "extraction_rules": [{"attribute_name": "event_type", "path": "$.type"}],
                "routes": [
                    {
                        "id": "paid_route",
                        "child_stream_id": "paid_events",
                        "condition": {"attribute": "event_type", "condition": "equals", "value": "paid"},
                    }
                ],
            },
            {"id": "audit_events", "fan_out": {"enabled": True, "parent_stream_id": "events"}},
            {"id": "paid_events", "route_source": {"parent_stream_id": "events", "route_id": "paid_route"}},
        ]
    }

    result = validate_rest_branch_graph(source)

    assert result.children_by_parent["events"] == ["paid_events", "audit_events"]
    assert result.incoming_by_child["paid_events"] == ["events"]
    assert result.incoming_by_child["audit_events"] == ["events"]


def test_conditional_route_requires_extracted_attribute():
    source = {
        "streams": [
            {
                "id": "orders",
                "extraction_rules": [{"attribute_name": "id", "path": "$.id"}],
                "routes": [
                    {
                        "id": "ready_route",
                        "child_stream_id": "ready_orders",
                        "condition": {"attribute": "status", "condition": "equals", "value": "READY"},
                    }
                ],
            },
            {"id": "ready_orders", "route_source": {"parent_stream_id": "orders", "route_id": "ready_route"}},
        ]
    }

    with pytest.raises(RestBranchValidationError, match="route attribute status is not extracted"):
        validate_rest_branch_graph(source)


def test_branch_graph_rejects_cycles():
    source = {
        "streams": [
            {"id": "a", "fan_out": {"enabled": True, "parent_stream_id": "c"}},
            {"id": "b", "fan_out": {"enabled": True, "parent_stream_id": "a"}},
            {"id": "c", "fan_out": {"enabled": True, "parent_stream_id": "b"}},
        ]
    }

    with pytest.raises(RestBranchValidationError, match="contains a cycle"):
        validate_rest_branch_graph(source)


def test_branch_graph_rejects_converging_parents():
    source = {
        "streams": [
            {"id": "a"},
            {"id": "b"},
            {"id": "shared", "fan_out": {"enabled": True, "parent_stream_id": "a"}},
        ]
    }
    source["streams"][1]["routes"] = [
        {
            "id": "to_shared",
            "child_stream_id": "shared",
            "condition": {"attribute": "kind", "condition": "is_empty", "value": ""},
        }
    ]
    source["streams"][1]["extraction_rules"] = [{"attribute_name": "kind", "path": "$.kind"}]

    with pytest.raises(RestBranchValidationError, match="multiple parents"):
        validate_rest_branch_graph(source)


def test_after_all_fanout_rejects_per_record_placeholders():
    with pytest.raises(ValueError, match="after_all fan-out cannot use per-record placeholders"):
        SourceCreate(
            name="after all invalid",
            type="REST API",
            base_url="https://api.example.com",
            streams=[
                {
                    "id": "items",
                    "name": "items",
                    "endpoint_path": "/items",
                    "response_data_path": "$.items",
                    "split_response_records": True,
                },
                {
                    "id": "final_get",
                    "name": "final_get",
                    "endpoint_path": "/items/${id}",
                    "fan_out": {
                        "enabled": True,
                        "parent_stream_id": "items",
                        "completion_mode": "after_all",
                    },
                },
            ],
        )


def test_stream_rejects_split_and_wait_together():
    with pytest.raises(ValueError, match="split response and wait for all records cannot both be enabled"):
        SourceCreate(
            name="split wait invalid",
            type="REST API",
            base_url="https://api.example.com",
            streams=[
                {
                    "id": "items",
                    "name": "items",
                    "endpoint_path": "/items",
                    "response_data_path": "$.items",
                    "split_response_records": True,
                    "wait_for_all_records_before_downstream": True,
                },
            ],
        )
