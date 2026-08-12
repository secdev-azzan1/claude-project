"""REST stream behavior inference.

This module keeps user-selected Kafka output separate from graph behavior.
It is intentionally pure: no NiFi, Kafka, database, or HTTP calls.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class RestStreamBehavior:
    stream_id: str
    publishes_to_kafka: bool
    is_parent: bool
    has_parallel_children: bool
    is_route_child: bool
    is_conditional_route_child: bool
    is_default_route_child: bool
    route_child_makes_request: bool
    is_child_request: bool
    produces_attributes: bool


def _stream_id(stream: dict, fallback_idx: int = 0) -> str:
    return str(stream.get("id") or stream.get("name") or f"stream_{fallback_idx}")


def _has_entity_output(stream: dict) -> bool:
    entity = stream.get("entity")
    if not isinstance(entity, dict) or not entity:
        return False
    kafka = entity.get("kafka")
    if isinstance(kafka, dict) and kafka:
        return True
    return any(value not in (None, "", {}, []) for value in entity.values())


def stream_publishes_to_kafka(stream: dict, target_stream_id: Optional[str] = None) -> bool:
    """Return whether this stream should publish in the current generation context."""
    sid = _stream_id(stream)
    if target_stream_id:
        return sid == str(target_stream_id)
    return _has_entity_output(stream)


def infer_rest_stream_behaviors(
    source: dict,
    target_stream_id: Optional[str] = None,
) -> Dict[str, RestStreamBehavior]:
    streams = [s for s in (source.get("streams") or []) if isinstance(s, dict)]
    ids = [_stream_id(stream, idx) for idx, stream in enumerate(streams)]
    child_ids_by_parent: Dict[str, set[str]] = {sid: set() for sid in ids}
    route_child_ids_by_parent: Dict[str, set[str]] = {sid: set() for sid in ids}

    routes_by_child: Dict[str, dict] = {}
    default_route_children: set[str] = set()
    for parent in streams:
        parent_id = _stream_id(parent)
        for route in parent.get("routes") or []:
            if not isinstance(route, dict):
                continue
            child_id = str(route.get("child_stream_id") or "").strip()
            if child_id:
                routes_by_child[child_id] = route
                route_child_ids_by_parent.setdefault(parent_id, set()).add(child_id)
                child_ids_by_parent.setdefault(parent_id, set()).add(child_id)
        if str(parent.get("default_route_action") or "").strip().lower() == "route_to_stream":
            default_child = str(parent.get("default_route_child_stream_id") or "").strip()
            if default_child:
                default_route_children.add(default_child)
                route_child_ids_by_parent.setdefault(parent_id, set()).add(default_child)
                child_ids_by_parent.setdefault(parent_id, set()).add(default_child)

    fanout_child_ids: set[str] = set()
    for stream in streams:
        sid = _stream_id(stream)
        fan = stream.get("fan_out") or {}
        if isinstance(fan, dict) and fan.get("enabled") and fan.get("parent_stream_id"):
            parent_id = str(fan.get("parent_stream_id"))
            fanout_child_ids.add(sid)
            child_ids_by_parent.setdefault(parent_id, set()).add(sid)

        route_source = stream.get("route_source") or {}
        if isinstance(route_source, dict) and route_source.get("parent_stream_id"):
            parent_id = str(route_source.get("parent_stream_id"))
            route_child_ids_by_parent.setdefault(parent_id, set()).add(sid)
            child_ids_by_parent.setdefault(parent_id, set()).add(sid)

    behaviors: Dict[str, RestStreamBehavior] = {}
    for idx, stream in enumerate(streams):
        sid = ids[idx]
        route_source = stream.get("route_source") or {}
        is_route_child = isinstance(route_source, dict) and bool(route_source.get("parent_stream_id"))
        route_id = str(route_source.get("route_id") or "").strip().lower() if isinstance(route_source, dict) else ""
        is_default_route_child = is_route_child and (route_id == "default" or sid in default_route_children)
        is_conditional_route_child = is_route_child and not is_default_route_child
        route_child_makes_request = is_route_child and bool(stream.get("request_enabled"))
        extraction_rules = stream.get("extraction_rules") or []

        behaviors[sid] = RestStreamBehavior(
            stream_id=sid,
            publishes_to_kafka=stream_publishes_to_kafka(stream, target_stream_id),
            is_parent=bool(child_ids_by_parent.get(sid)),
            has_parallel_children=bool(
                [
                    child_id
                    for child_id in child_ids_by_parent.get(sid, set())
                    if child_id in fanout_child_ids
                ]
            ),
            is_route_child=is_route_child,
            is_conditional_route_child=is_conditional_route_child,
            is_default_route_child=is_default_route_child,
            route_child_makes_request=route_child_makes_request,
            is_child_request=sid in fanout_child_ids,
            produces_attributes=bool(extraction_rules),
        )
    return behaviors
