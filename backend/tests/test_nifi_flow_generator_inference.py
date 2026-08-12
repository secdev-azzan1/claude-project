import asyncio
import sys
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import nifi_flow_generator as gen
from routers.flows import _repair_rest_stream_graph_for_deploy


def test_rest_xml_inference_passes_xml_reader_to_rest_builder(monkeypatch):
    captured = {}

    async def fake_get_root(*args, **kwargs):
        return "root-pg"

    async def fake_create_pg(*args, **kwargs):
        return "inference-pg"

    async def fake_nifi_api_request(*args, **kwargs):
        path = args[2] if len(args) > 2 else ""
        if path == "/nifi-api/flow/parameter-contexts":
            return {"ok": True, "data": {"parameterContexts": []}}
        if path.startswith("/nifi-api/process-groups/"):
            return {"ok": True, "data": {"processGroups": []}}
        return {"ok": True, "data": {}}

    async def fake_create_services(*args, **kwargs):
        return {
            "kafka_svc": "kafka-id",
            "json_reader": "json-reader-id",
            "json_writer": "json-writer-id",
            "avro_writer": "json-writer-id",
            "xml_reader": "xml-reader-id",
        }

    async def fake_build_rest_api_flow(*args, **kwargs):
        captured["xml_reader_id"] = kwargs.get("xml_reader_id")
        captured["json_reader_id"] = kwargs.get("json_reader_id")
        captured["avro_writer_id"] = kwargs.get("avro_writer_id")
        return True

    monkeypatch.setattr(gen, "get_nifi_root_process_group_id", fake_get_root)
    monkeypatch.setattr(gen, "_create_process_group", fake_create_pg)
    monkeypatch.setattr(gen, "nifi_api_request", fake_nifi_api_request)
    monkeypatch.setattr(gen, "_create_inference_controller_services", fake_create_services)
    monkeypatch.setattr(gen, "_build_rest_api_flow", fake_build_rest_api_flow)

    source = {
        "name": "fortisiem_device",
        "type": "REST API",
        "streams": [
            {
                "id": "device_detail",
                "name": "device_detail",
                "response_format": "xml",
                "transformation_rules": [
                    {"type": "set_from_attribute", "field_name": "object_id", "source_attribute": "naturalId"}
                ],
            }
        ],
    }

    result = asyncio.run(
        gen.generate_and_deploy_inference_flow(
            source=source,
            flow_id="flow-1",
            inference_topic="topic",
            nifi_conn={"endpoint": "http://nifi.local", "auth_type": "NONE"},
        )
    )

    assert result["ok"] is True
    assert captured["json_reader_id"] == "json-reader-id"
    assert captured["avro_writer_id"] == "json-writer-id"
    assert captured["xml_reader_id"] == "xml-reader-id"


def test_rest_deploy_repair_does_not_add_fanout_to_route_child():
    source = {
        "type": "REST API",
        "streams": [
            {
                "id": "parent",
                "name": "parent",
                "endpoint_path": "/parents",
                "extraction_rules": [{"attribute_name": "org_name", "path": "$.org"}],
                "routes": [
                    {
                        "id": "route_has_ip",
                        "child_stream_id": "child",
                        "condition": {"attribute": "access_ip", "condition": "regex", "value": ".+"},
                    }
                ],
            },
            {
                "id": "child",
                "name": "child",
                "endpoint_path": "/devices?organization=${org_name}&ip=${access_ip}",
                "extraction_rules": [],
                "fan_out": {"enabled": False},
                "route_source": {"parent_stream_id": "parent", "route_id": "route_has_ip"},
            },
        ],
    }

    repaired, changed, notes = _repair_rest_stream_graph_for_deploy(source)

    child = next(stream for stream in repaired["streams"] if stream["id"] == "child")
    assert child["fan_out"].get("enabled") is False
    assert child["route_source"]["parent_stream_id"] == "parent"
    assert changed is False
    assert notes == []


def test_xml_avro_publish_uses_stream_schema_xml_reader(monkeypatch):
    created_xml_readers = []
    convert_record_readers = {}

    async def fake_create_processor(*args, **kwargs):
        proc_type = args[2]
        name = args[3]
        properties = kwargs.get("properties") or {}
        if proc_type == "org.apache.nifi.processors.standard.ConvertRecord":
            convert_record_readers[name] = properties.get("Record Reader")
        return f"{name}-id"

    async def fake_create_connection(*args, **kwargs):
        return True

    async def fake_create_schema_avro_writer(**kwargs):
        return f"writer-for-{kwargs['schema_name']}"

    async def fake_create_xml_reader_service(**kwargs):
        schema_name = kwargs["schema_name"]
        reader_id = f"xml-reader-for-{schema_name}"
        created_xml_readers.append(schema_name)
        return reader_id, 1

    async def fake_enable_controller_service(*args, **kwargs):
        return True

    async def fake_wait_cs_enabled(*args, **kwargs):
        return True

    monkeypatch.setattr(gen, "_create_processor", fake_create_processor)
    monkeypatch.setattr(gen, "_create_connection", fake_create_connection)
    monkeypatch.setattr(gen, "_create_schema_avro_writer", fake_create_schema_avro_writer)
    monkeypatch.setattr(gen, "_create_xml_reader_service", fake_create_xml_reader_service)
    monkeypatch.setattr(gen, "_enable_controller_service", fake_enable_controller_service)
    monkeypatch.setattr(gen, "_wait_cs_enabled", fake_wait_cs_enabled)

    source = {
        "streams": [
            {
                "id": "forti_organizations",
                "name": "organizations",
                "response_format": "xml",
                "split_response_records": True,
                "xml_split_depth": 3,
                "endpoint_path": "/config/Domain",
                "entity": {
                    "schema_artifact_id": "organizations-value",
                    "kafka": {
                        "topic": "organizations-topic",
                        "value_format": "avro",
                        "schema_artifact_id": "organizations-value",
                    },
                },
            }
        ],
        "kafka_output": {"schema_artifact_id": "flow-level-value"},
    }

    result = asyncio.run(
        gen._build_stream_chain(
            "http://nifi.local",
            "pg-id",
            source,
            source["streams"][0],
            entry_processor_id="entry-id",
            entry_relationship="success",
            stream_results={},
            kafka_svc_id="kafka-id",
            json_reader_id="shared-json-reader",
            schema_reg_id="schema-registry-id",
            xml_reader_id="shared-xml-reader",
            avro_writer_id="shared-avro-writer",
            kafka_schema_reg_id="kafka-schema-registry-id",
            schema_ref_writer_id="schema-ref-writer-id",
            avro_writer_cache={},
            json_reader_cache={},
            json_writer_id="json-writer-id",
            avro_transform_writer_id="avro-transform-writer-id",
            target_stream_id=None,
            kafka_topic="fallback-topic",
            base_url="http://example.test",
            src_auth="NONE",
            auth_header_pattern_keys=[],
            source_username="",
            source_password="",
            connection_timeout=30,
            read_timeout=30,
            x=0,
            y=0,
            auth_type="NONE",
            username=None,
            password=None,
        )
    )

    assert result is not None
    assert created_xml_readers == ["organizations-value"]
    assert convert_record_readers["organizations__convert_avro"] == "xml-reader-for-organizations-value"
