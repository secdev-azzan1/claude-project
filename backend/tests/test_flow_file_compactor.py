from pathlib import Path
import sys

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services.flow_file_compactor import compact_flow_file


def test_compact_source_removes_designer_payload_runtime_empty_and_defaults():
    source = {
        "id": "source-1",
        "name": "FakeBook",
        "type": "REST API",
        "connection_setup_mode": "manual",
        "base_url": "https://api.example",
        "auth_type": "NONE",
        "connection_timeout": 30,
        "read_timeout": 30,
        "write_timeout": 30,
        "idle_timeout": 30,
        "rate_limit": 120,
        "openapi_spec_id": "spec-1",
        "designer_payload": {"step": 3, "streams": [{"id": "stream-1"}]},
        "streams": [{"id": "stream-1"}],
        "created_at": "2026-06-01T00:00:00",
        "updated_at": "2026-06-01T00:00:00",
        "state": "Stopped",
        "nifi_process_group_id": "pg-1",
        "api_key_value": None,
        "headers": {},
        "filters": [],
        "nifi_service_refs": {},
    }

    compact = compact_flow_file("source/source.json", source)

    assert compact == {
        "id": "source-1",
        "name": "FakeBook",
        "type": "REST API",
        "base_url": "https://api.example",
        "openapi_spec_id": "spec-1",
    }


def test_compact_stream_removes_empty_defaults_but_keeps_routes_entity_and_bindings():
    stream = {
        "id": "stream-authors",
        "name": "Authors",
        "method": "GET",
        "endpoint_path": "/Authors/${id}",
        "request_enabled": True,
        "response_format": "json",
        "body_format": "empty",
        "split_response_records": False,
        "headers": {},
        "query_params": {},
        "filters": [],
        "routes": [],
        "routing_rules": [],
        "transformation_rules": [],
        "pagination": {
            "enabled": False,
            "type": "none",
            "page_param": None,
            "cursor_param": None,
        },
        "parameter_bindings": [
            {
                "param_name": "id",
                "param_in": "path",
                "source_stream_id": "stream-all",
                "source_attribute": "id",
                "required": True,
                "enabled": True,
                "static_value": "",
            }
        ],
        "entity": {
            "schema_artifact_id": "bronze.fakebook.authors-value",
            "schema_version": 1,
            "kafka": {
                "topic": "bronze.fakebook.authors",
                "partitions": 6,
                "replication_factor": 3,
                "key_strategy": "none",
                "value_format": "avro",
                "schema_artifact_id": "bronze.fakebook.authors-value",
                "schema_version": 1,
            },
        },
        "created_at": "2026-06-01T00:00:00",
        "updated_at": "2026-06-01T00:00:00",
    }

    compact = compact_flow_file("source/streams/stream-authors.json", stream)

    assert compact["id"] == "stream-authors"
    assert compact["endpoint_path"] == "/Authors/${id}"
    assert "headers" not in compact
    assert "filters" not in compact
    assert "pagination" not in compact
    assert "created_at" not in compact
    assert compact["parameter_bindings"] == [
        {
            "param_name": "id",
            "param_in": "path",
            "source_stream_id": "stream-all",
            "source_attribute": "id",
            "required": True,
            "enabled": True,
        }
    ]
    assert compact["entity"]["kafka"]["topic"] == "bronze.fakebook.authors"


def test_compact_stream_keeps_non_default_body_format_and_template():
    stream = {
        "id": "stream-create",
        "name": "Create",
        "method": "POST",
        "endpoint_path": "/records",
        "request_enabled": True,
        "response_format": "json",
        "body_format": "json",
        "body_template": '{"name":"${name}"}',
        "pagination": {"enabled": False, "type": "none"},
    }

    compact = compact_flow_file("source/streams/stream-create.json", stream)

    assert compact["body_format"] == "json"
    assert compact["body_template"] == '{"name":"${name}"}'


def test_compact_stream_drops_removed_polling_config():
    stream = {
        "id": "stream-poll",
        "name": "Poll",
        "method": "GET",
        "endpoint_path": "/jobs/${job_id}/status",
        "request_enabled": True,
        "response_format": "json",
        "body_format": "empty",
        "polling": {
            "enabled": True,
            "status_path": "$.status",
            "pending_values": ["queued", "running"],
            "success_values": ["complete"],
            "failure_values": ["failed"],
            "interval_seconds": 10,
            "max_attempts": 5,
            "missing_status_behavior": "fail",
        },
    }

    compact = compact_flow_file("source/streams/stream-poll.json", stream)

    assert "polling" not in compact


def test_compact_flow_removes_runtime_state_but_keeps_cross_file_references():
    flow = {
        "id": "flow-1",
        "name": "Flow",
        "source_id": "source-1",
        "source_type": "REST API",
        "primary_stream_id": "stream-1",
        "kafka_topic": "bronze.flow.stream",
        "state": "Running",
        "enabled": True,
        "last_run": "2026-06-01T00:00:00",
        "runs": [{"ok": True}],
        "total_records": 12,
        "created_at": "2026-06-01T00:00:00",
        "updated_at": "2026-06-01T00:00:00",
        "nifi_process_group_id": "pg-1",
    }

    compact = compact_flow_file("flow.json", flow)

    assert compact == {
        "id": "flow-1",
        "name": "Flow",
        "source_id": "source-1",
        "source_type": "REST API",
        "primary_stream_id": "stream-1",
        "kafka_topic": "bronze.flow.stream",
    }


def test_compact_schema_version_preserves_avro_null_defaults():
    version = {
        "_id": "mongo-id",
        "version": 1,
        "status": "Verified",
        "avro_schema": {
            "type": "record",
            "name": "Order",
            "fields": [
                {"name": "order_id", "type": ["null", "string"], "default": None},
                {"name": "total", "type": ["null", "double"], "default": None},
            ],
        },
        "created_at": "2026-06-01T00:00:00",
        "updated_at": "2026-06-01T00:00:00",
    }

    compact = compact_flow_file("schemas/bronze.orders-value/versions/1.json", version)

    assert "_id" not in compact
    assert "created_at" not in compact
    assert compact["avro_schema"]["fields"][0]["default"] is None
    assert compact["avro_schema"]["fields"][1]["default"] is None
