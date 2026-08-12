import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services.flow_runtime_errors import runtime_error_from_kafka_message
from services.flow_runtime_errors import runtime_errors_from_kafka_messages


def test_runtime_error_from_kafka_message_maps_failure_event():
    msg = {
        "topic": "rest-flow-failures",
        "partition": 0,
        "offset": 12,
        "timestamp": "2026-07-08T08:41:38Z",
        "value": (
            '{"event_type":"rest_flow_failure","flow_id":"flow-1","stream_id":"customers",'
            '"stream_name":"customers","stage":"invoke_http","processor_name":"customers__fetch",'
            '"http_status":"401","message":"API request failed","error_category":"http",'
            '"attributes":{"customer_id":"123","Authorization":"Bearer secret-token"}}'
        ),
    }

    item = runtime_error_from_kafka_message(msg)

    assert item is not None
    assert item["flow_id"] == "flow-1"
    assert item["stream_id"] == "customers"
    assert item["stream_name"] == "customers"
    assert item["stage"] == "invoke_http"
    assert item["processor_name"] == "customers__fetch"
    assert item["http_status"] == "401"
    assert item["message"] == "API request failed"
    assert item["error_category"] == "http"
    assert item["topic"] == "rest-flow-failures"
    assert item["offset"] == 12
    assert item["attributes"] == {"customer_id": "123"}


def test_runtime_error_from_kafka_message_ignores_non_failure_events():
    assert runtime_error_from_kafka_message({"value": '{"event_type":"normal"}'}) is None
    assert runtime_error_from_kafka_message({"value": "not-json"}) is None


def test_runtime_errors_from_kafka_messages_filters_by_flow_id():
    messages = [
        {
            "topic": "rest-flow-failures",
            "offset": 1,
            "value": '{"event_type":"rest_flow_failure","flow_id":"flow-1","stream_id":"a","message":"failed"}',
        },
        {
            "topic": "rest-flow-failures",
            "offset": 2,
            "value": '{"event_type":"rest_flow_failure","flow_id":"flow-2","stream_id":"b","message":"failed"}',
        },
    ]

    items = runtime_errors_from_kafka_messages(messages, flow_id="flow-1")

    assert len(items) == 1
    assert items[0]["flow_id"] == "flow-1"
    assert items[0]["stream_id"] == "a"
