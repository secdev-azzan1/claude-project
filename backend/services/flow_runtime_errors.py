import json
import re
from typing import Any, Dict, Iterable, List, Optional


SENSITIVE_ATTRIBUTE_RE = re.compile(
    r"(token|secret|password|authorization|api[_-]?key|x-api-key|cookie|session)",
    re.IGNORECASE,
)


def _safe_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    return str(value)


def _safe_attributes(value: Any) -> Dict[str, str]:
    if not isinstance(value, dict):
        return {}
    safe: Dict[str, str] = {}
    for key, raw in value.items():
        name = str(key)
        if SENSITIVE_ATTRIBUTE_RE.search(name):
            continue
        if raw is None:
            continue
        safe[name] = str(raw)
    return safe


def runtime_error_from_kafka_message(message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    raw_value = message.get("value")
    if not isinstance(raw_value, str) or not raw_value.strip():
        return None
    try:
        event = json.loads(raw_value)
    except Exception:
        return None
    if not isinstance(event, dict) or event.get("event_type") != "rest_flow_failure":
        return None

    return {
        "event_type": "rest_flow_failure",
        "flow_id": _safe_str(event.get("flow_id")) or "",
        "source_id": _safe_str(event.get("source_id")) or "",
        "stream_id": _safe_str(event.get("stream_id")) or "",
        "stream_name": _safe_str(event.get("stream_name")) or "",
        "stage": _safe_str(event.get("stage")) or "unknown",
        "processor_name": _safe_str(event.get("processor_name")) or "",
        "http_status": _safe_str(event.get("http_status")),
        "message": _safe_str(event.get("message")) or "Runtime failure",
        "error_category": _safe_str(event.get("error_category")) or "unknown",
        "pagination_page_count": _safe_str(event.get("pagination_page_count")),
        "request_method": _safe_str(event.get("request_method")),
        "request_url": _safe_str(event.get("request_url")),
        "event_time": _safe_str(event.get("timestamp")) or _safe_str(message.get("timestamp")),
        "topic": _safe_str(message.get("topic")),
        "partition": message.get("partition"),
        "offset": message.get("offset"),
        "attributes": _safe_attributes(event.get("attributes")),
        "raw_event": event,
    }


def runtime_errors_from_kafka_messages(
    messages: Iterable[Dict[str, Any]],
    flow_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for message in messages:
        item = runtime_error_from_kafka_message(message)
        if not item:
            continue
        if flow_id and item.get("flow_id") != flow_id:
            continue
        items.append(item)
    return items


def runtime_error_topic(flow: Optional[Dict[str, Any]] = None, source: Optional[Dict[str, Any]] = None) -> str:
    for owner in (source, flow):
        cfg = (owner or {}).get("error_handling") if isinstance(owner, dict) else None
        if isinstance(cfg, dict):
            topic = str(cfg.get("failure_topic") or "").strip()
            if topic:
                return topic
    return "rest-flow-failures"
