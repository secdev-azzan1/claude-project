import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from routers.flows import _collect_entity_destinations


def test_entity_destinations_do_not_include_derived_fallback_topic_when_stream_outputs_exist():
    flow = {
        "primary_stream_id": "orders",
        "kafka_topic": "bronze.derived.orders__history",
        "schema_artifact_id": "orders-value",
        "schema_version": 1,
    }
    source = {
        "streams": [
            {
                "id": "orders",
                "name": "orders",
                "entity": {
                    "kafka": {
                        "topic": "nif.orders",
                        "schema_artifact_id": "orders-value",
                        "schema_version": 1,
                    }
                },
            }
        ]
    }

    assert _collect_entity_destinations(flow, source) == [
        {
            "stream_id": "orders",
            "stream_name": "orders",
            "topic": "nif.orders",
            "schema_artifact_id": "orders-value",
            "schema_version": 1,
        }
    ]
