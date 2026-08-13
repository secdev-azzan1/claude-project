import sys
from pathlib import Path

import fastavro
import pytest

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


# ---------------------------------------------- nested record name collisions
#
# Regression coverage for the ceremony's B4 defect: dummyjson `/users` carries
# two same-named nested objects -- root `address` and `company.address` --
# each with its own nested `coordinates`. Naming a nested record after the
# leaf field name alone would generate the SAME Avro name twice, which
# fastavro/the registry reject as "redefined named type" on approve. The
# AvroBuilder derives nested record names from the full field path instead
# (`address` -> "address", `company.address` -> "company_address"), so the
# two never collide; these tests lock that behavior in against real sample
# shapes and prove fastavro accepts the output.

DUMMYJSON_USERS = [
    {
        "id": 1,
        "firstName": "Emily",
        "hair": {"color": "Brown", "type": "Curly"},
        "address": {
            "address": "626 Main Street",
            "city": "Phoenix",
            "coordinates": {"lat": -77.16213, "lng": -92.084824},
        },
        "bank": {"cardType": "Diners Club International", "iban": "GB74MH2UZLR9TRPHYNU8F8"},
        "company": {
            "name": "Dooley, Kozey and Cronin",
            "address": {
                "address": "263 Tenth Street",
                "city": "San Francisco",
                "coordinates": {"lat": 71.814525, "lng": -161.150263},
            },
        },
        "crypto": {"coin": "Bitcoin", "wallet": "0xb9fc2fe63b2a6c003f1c324c3bfa53259162181a"},
    },
    {
        "id": 2,
        "firstName": "Michael",
        "hair": {"color": "Green", "type": "Straight"},
        "address": {
            "address": "385 Fifth Street",
            "city": "Houston",
            "coordinates": {"lat": 22.815468, "lng": 115.608581},
        },
        "bank": {"cardType": "JCB", "iban": "DE26362283149158045865"},
        "company": {
            "name": "Spinka - Dickinson",
            "address": {
                "address": "395 Main Street",
                "city": "Los Angeles",
                "coordinates": {"lat": 79.098326, "lng": -119.624845},
            },
        },
        "crypto": {"coin": "Bitcoin", "wallet": "0xb9fc2fe63b2a6c003f1c324c3bfa53259162181a"},
    },
]


def _named_type_names(node):
    """Every Avro named-type ("record") name reachable from a schema node, recursively."""
    names = []

    def walk(t):
        if isinstance(t, list):
            for branch in t:
                walk(branch)
            return
        if isinstance(t, dict):
            if t.get("type") == "record":
                names.append(t["name"])
                for f in t.get("fields", []):
                    walk(f["type"])
            elif t.get("type") == "array":
                walk(t.get("items"))

    walk(node)
    return names


def _field(schema, name):
    return next(f for f in schema["fields"] if f["name"] == name)


def _unwrap(avro_type):
    if isinstance(avro_type, list):
        return next(t for t in avro_type if t != "null")
    return avro_type


def test_dummyjson_users_shape_produces_unique_named_types_and_parses():
    schema = infer_avro_schema(DUMMYJSON_USERS, name="ice_user", namespace="raw.ice_users")

    names = _named_type_names(schema)
    assert names, "expected at least one named record type"
    assert len(names) == len(set(names)), f"duplicate named types: {names}"

    # fastavro (and, in production, the registry) is the ground truth: a schema
    # with a "redefined named type" fails to parse exactly like it 422'd live.
    fastavro.parse_schema(schema)

    address = _unwrap(_field(schema, "address")["type"])
    address_coordinates = _unwrap(_field(address, "coordinates")["type"])
    company = _unwrap(_field(schema, "company")["type"])
    company_address = _unwrap(_field(company, "address")["type"])
    company_address_coordinates = _unwrap(_field(company_address, "coordinates")["type"])

    # the two "address" record types (and their nested "coordinates") are
    # named from their full path, so they are distinct, not the same name twice.
    assert address["name"] != company_address["name"]
    assert address_coordinates["name"] != company_address_coordinates["name"]


def test_live_dummyjson_users_fetch_produces_unique_named_types():
    """Live proof: 2 real users fetched fresh from dummyjson.com, not a fixture."""
    import urllib.request

    try:
        req = urllib.request.Request("https://dummyjson.com/users?limit=2", headers={"User-Agent": "curl/8"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            import json

            data = json.load(resp)
    except Exception as exc:  # pragma: no cover - network unavailable in some environments
        pytest.skip(f"dummyjson.com unreachable: {exc}")

    users = data["users"]
    assert len(users) == 2

    schema = infer_avro_schema(users, name="ice_user", namespace="raw.ice_users")
    names = _named_type_names(schema)
    assert len(names) == len(set(names)), f"duplicate named types from live data: {names}"
    fastavro.parse_schema(schema)


def test_unique_record_name_dedupes_a_residual_collision_as_a_safety_net():
    # "a.b_c" and "a_b.c" both sanitize to "a_b_c" -- an adversarial input the
    # full-path scheme alone cannot tell apart. The uniqueness pass catches it.
    samples = [
        {"a": {"b_c": {"x": 1}}, "a_b": {"c": {"y": 2}}},
        {"a": {"b_c": {"x": 3}}, "a_b": {"c": {"y": 4}}},
    ]

    schema = infer_avro_schema(samples, name="r", namespace="ns")

    names = _named_type_names(schema)
    assert len(names) == len(set(names)), f"duplicate named types: {names}"
    assert names.count("a_b_c") <= 1
    fastavro.parse_schema(schema)
