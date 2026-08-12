import sys
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from routers.schema_inference import _missing_template_param_resolutions, _source_for_inference_validation
from services.schema_inference_runner import INFERENCE_SAMPLE_LIMIT, _inference_collection_complete
from services.schema_inferencer import infer_avro_schema


def _fortisiem_like_source():
    return {
        "type": "REST API",
        "streams": [
            {
                "id": "organizations",
                "name": "organizations",
                "endpoint_path": "/config/Domain",
                "extraction_rules": [{"attribute_name": "org_name", "path": "/domain/name/text()"}],
                "fan_out": {"enabled": False},
            },
            {
                "id": "device_list",
                "name": "device_list",
                "endpoint_path": "/cmdbDeviceInfo/devices?organization=${org_name}",
                "extraction_rules": [
                    {"attribute_name": "access_ip", "path": "/device/accessIp/text()"},
                    {"attribute_name": "org_name", "path": "/device/org_name/text()"},
                ],
                "fan_out": {
                    "enabled": True,
                    "parent_stream_id": "organizations",
                    "parent_field": "org_name",
                },
                "routes": [
                    {
                        "id": "route_has_ip",
                        "child_stream_id": "device_detail",
                        "condition": {"attribute": "access_ip", "condition": "regex", "value": ".+"},
                    }
                ],
            },
            {
                "id": "device_detail",
                "name": "device_detail",
                "endpoint_path": "/cmdbDeviceInfo/device?organization=${org_name}&ip=${access_ip}",
                "extraction_rules": [],
                "fan_out": {"enabled": False},
                "route_source": {"parent_stream_id": "device_list", "route_id": "route_has_ip"},
            },
        ],
    }


def test_route_child_path_params_resolve_from_route_parent_extractions():
    source = _fortisiem_like_source()

    assert _missing_template_param_resolutions(source) == []


def test_selected_root_entity_validation_ignores_unrelated_child_streams():
    source = _fortisiem_like_source()
    source["streams"][2]["endpoint_path"] = "/cmdbDeviceInfo/device?organization=${missing_org}&ip=${missing_ip}"

    validation_source = _source_for_inference_validation(source, "organizations")

    assert [stream["id"] for stream in validation_source["streams"]] == ["organizations"]
    assert _missing_template_param_resolutions(validation_source) == []


def test_inference_collection_completes_after_requested_minimum():
    assert _inference_collection_complete(collected=10, requested_minimum=10, stable_polls=0)
    assert _inference_collection_complete(collected=36, requested_minimum=10, stable_polls=2)
    assert _inference_collection_complete(
        collected=INFERENCE_SAMPLE_LIMIT,
        requested_minimum=10,
        stable_polls=0,
    )


def test_mixed_numeric_and_text_values_in_later_samples_infer_string():
    samples = [{"ZONE": index} for index in range(10)]
    samples.extend([{"ZONE": "Annual"}, {"ZONE": "3 - 5"}])

    schema = infer_avro_schema(samples, name="plants")

    zone = next(field for field in schema["fields"] if field["name"] == "ZONE")
    assert zone["type"] == ["null", "string"]
