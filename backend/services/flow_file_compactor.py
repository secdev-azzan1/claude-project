from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict


DROP_KEYS = {
    "_id",
    "created_at",
    "updated_at",
    "last_run",
    "last_error",
    "runs",
    "total_records",
    "total_errors",
    "deployed_at",
    "started_at",
    "stopped_at",
    "nifi_process_group_id",
    "nifi_processor_id",
    "nifi_processor_ids",
    "nifi_controller_service_id",
    "processor_id",
    "processor_ids",
    "controller_service_id",
    "controller_service_ids",
    "designer_payload",
    "schema_inference_job_id",
    "schema_inference_active",
}

FLOW_RUNTIME_KEYS = {
    "state",
    "enabled",
}

SOURCE_DEFAULTS = {
    "connection_setup_mode": "manual",
    "auth_type": "NONE",
    "api_key_location": "HEADER",
    "connection_timeout": 30,
    "read_timeout": 30,
    "write_timeout": 30,
    "idle_timeout": 30,
    "rate_limit": 120,
    "webhook_enabled": True,
}

STREAM_DEFAULTS = {
    "method": "GET",
    "request_enabled": True,
    "response_format": "json",
    "body_format": "empty",
    "split_response_records": False,
    "wait_for_all_records_before_downstream": False,
    "is_primary": False,
}

PAGINATION_DEFAULTS = {
    "enabled": False,
    "type": "none",
}

def _is_empty(value: Any) -> bool:
    return value is None or value == "" or value == [] or value == {}


def _compact_value(value: Any) -> Any:
    if isinstance(value, list):
        items = [_compact_value(item) for item in value]
        return [item for item in items if not _is_empty(item)]
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for key, item in value.items():
            if key in DROP_KEYS:
                continue
            compacted = _compact_value(item)
            if _is_empty(compacted):
                continue
            out[str(key)] = compacted
        return out
    return value


def _compact_schema_value(value: Any) -> Any:
    if isinstance(value, list):
        return [_compact_schema_value(item) for item in value]
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for key, item in value.items():
            if key in DROP_KEYS:
                continue
            out[str(key)] = _compact_schema_value(item)
        return out
    return value


def _drop_defaults(payload: Dict[str, Any], defaults: Dict[str, Any]) -> Dict[str, Any]:
    return {key: value for key, value in payload.items() if defaults.get(key, object()) != value}


def _compact_parameter_bindings(bindings: Any) -> Any:
    if not isinstance(bindings, list):
        return bindings
    compacted = []
    for binding in bindings:
        if not isinstance(binding, dict):
            continue
        item = _compact_value(binding)
        if isinstance(item, dict):
            item = _drop_defaults(item, {"static_value": ""})
        if item:
            compacted.append(item)
    return compacted


def _compact_stream(payload: Dict[str, Any]) -> Dict[str, Any]:
    compact = _drop_defaults(_compact_value(payload), STREAM_DEFAULTS)
    pagination = compact.get("pagination")
    if isinstance(pagination, dict):
        pagination = _drop_defaults(pagination, PAGINATION_DEFAULTS)
        if pagination:
            compact["pagination"] = pagination
        else:
            compact.pop("pagination", None)
    compact.pop("polling", None)
    if "parameter_bindings" in compact:
        compact["parameter_bindings"] = _compact_parameter_bindings(compact["parameter_bindings"])
        if not compact["parameter_bindings"]:
            compact.pop("parameter_bindings", None)
    return compact


def compact_flow_file(rel_path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    compact = deepcopy(payload or {})
    if rel_path == "flow.json":
        compact = _compact_value(compact)
        for key in FLOW_RUNTIME_KEYS:
            compact.pop(key, None)
        return compact
    if rel_path == "source/source.json":
        compact = _compact_value(compact)
        compact.pop("streams", None)
        compact.pop("state", None)
        return _drop_defaults(compact, SOURCE_DEFAULTS)
    if rel_path.startswith("source/streams/") and rel_path.endswith(".json"):
        return _compact_stream(compact)
    if rel_path.startswith("schemas/") and rel_path.endswith(".json"):
        return _compact_schema_value(compact)
    return _compact_value(compact)
