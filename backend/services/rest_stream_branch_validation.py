"""Validation helpers for REST stream branch graphs."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List


class RestBranchValidationError(ValueError):
    """Raised when a REST stream branch graph is invalid."""


@dataclass(frozen=True)
class RestBranchValidationResult:
    children_by_parent: Dict[str, List[str]]
    incoming_by_child: Dict[str, List[str]]


def _stream_id(stream: dict, fallback_idx: int = 0) -> str:
    return str(stream.get("id") or stream.get("name") or f"stream_{fallback_idx}").strip()


def _extracted_attribute_names(stream: dict) -> set[str]:
    names: set[str] = set()
    for rule in stream.get("extraction_rules") or []:
        if not isinstance(rule, dict):
            continue
        name = str(rule.get("attribute_name") or "").strip()
        if name:
            names.add(name)
    return names


def _add_edge(
    children: Dict[str, List[str]],
    incoming: Dict[str, List[str]],
    parent_id: str,
    child_id: str,
) -> None:
    if child_id not in children.setdefault(parent_id, []):
        children[parent_id].append(child_id)
    if parent_id not in incoming.setdefault(child_id, []):
        incoming[child_id].append(parent_id)


def validate_rest_branch_graph(source: dict) -> RestBranchValidationResult:
    """Validate nested REST branch graph structure.

    Branches may be nested to arbitrary depth, but they must remain acyclic and
    cannot converge into the same child from multiple parents.
    """
    streams = [stream for stream in (source.get("streams") or []) if isinstance(stream, dict)]
    ids: List[str] = [_stream_id(stream, idx) for idx, stream in enumerate(streams)]
    stream_ids = {sid for sid in ids if sid}
    by_id = {sid: stream for sid, stream in zip(ids, streams) if sid}
    children: Dict[str, List[str]] = {sid: [] for sid in stream_ids}
    incoming: Dict[str, List[str]] = {sid: [] for sid in stream_ids}
    outgoing_kinds: Dict[str, set[str]] = {sid: set() for sid in stream_ids}
    route_targets_by_parent: Dict[str, set[str]] = {sid: set() for sid in stream_ids}
    default_targets_by_parent: Dict[str, str] = {}

    for sid, stream in by_id.items():
        fan = stream.get("fan_out") or {}
        if isinstance(fan, dict) and fan.get("enabled") and fan.get("parent_stream_id"):
            parent_id = str(fan.get("parent_stream_id") or "").strip()
            if parent_id not in stream_ids:
                raise RestBranchValidationError(f"Stream {sid} references missing parent stream {parent_id}.")
            if parent_id == sid:
                raise RestBranchValidationError(f"Stream {sid} cannot branch to itself.")
            _add_edge(children, incoming, parent_id, sid)
            outgoing_kinds.setdefault(parent_id, set()).add("fanout")

        extracted = _extracted_attribute_names(stream)
        for route in stream.get("routes") or []:
            if not isinstance(route, dict):
                continue
            cond = route.get("condition") or {}
            if not isinstance(cond, dict):
                raise RestBranchValidationError(f"Stream {sid} has an invalid route condition.")
            attr = str(cond.get("attribute") or "").strip()
            op = str(cond.get("condition") or "equals").strip().lower()
            value = str(cond.get("value") or "").strip()
            if not attr:
                raise RestBranchValidationError(f"Stream {sid} has a route without a condition field.")
            if extracted and attr not in extracted:
                raise RestBranchValidationError(f"Stream {sid} route attribute {attr} is not extracted.")
            if op not in {"equals", "contains", "starts_with", "ends_with", "regex", "is_empty"}:
                raise RestBranchValidationError(f"Stream {sid} has an unsupported route operator {op}.")
            if op != "is_empty" and not value:
                raise RestBranchValidationError(f"Stream {sid} has a route without a comparison value.")
            child_id = str(route.get("child_stream_id") or "").strip()
            if not child_id:
                raise RestBranchValidationError(f"Stream {sid} has a route without a target child stream.")
            if child_id not in stream_ids:
                raise RestBranchValidationError(f"Stream {sid} routes to missing stream {child_id}.")
            if child_id == sid:
                raise RestBranchValidationError(f"Stream {sid} cannot route to itself.")
            _add_edge(children, incoming, sid, child_id)
            outgoing_kinds.setdefault(sid, set()).add("conditional")
            route_targets_by_parent.setdefault(sid, set()).add(child_id)

        default_child = str(stream.get("default_route_child_stream_id") or "").strip()
        default_action = str(stream.get("default_route_action") or "").strip().lower()
        if default_action and default_action not in {"drop", "keep", "route_to_stream"}:
            raise RestBranchValidationError(f"Stream {sid} has invalid default route action {default_action}.")
        if default_action == "route_to_stream" and not default_child:
            raise RestBranchValidationError(f"Stream {sid} default route must target a child stream.")
        if default_action == "route_to_stream" and default_child:
            if default_child not in stream_ids:
                raise RestBranchValidationError(f"Stream {sid} default route targets missing stream {default_child}.")
            if default_child == sid:
                raise RestBranchValidationError(f"Stream {sid} default route cannot target itself.")
            _add_edge(children, incoming, sid, default_child)
            outgoing_kinds.setdefault(sid, set()).add("conditional")
            default_targets_by_parent[sid] = default_child

    for sid, stream in by_id.items():
        route_source = stream.get("route_source") or {}
        if not isinstance(route_source, dict) or not route_source.get("parent_stream_id"):
            continue
        parent_id = str(route_source.get("parent_stream_id") or "").strip()
        if parent_id not in stream_ids:
            raise RestBranchValidationError(f"Stream {sid} references missing route parent stream {parent_id}.")
        route_id = str(route_source.get("route_id") or "").strip().lower()
        parent_targets = route_targets_by_parent.get(parent_id, set())
        default_target = default_targets_by_parent.get(parent_id)
        if route_id == "default":
            if default_target and default_target != sid:
                raise RestBranchValidationError(f"Stream {sid} route_source does not match parent default route.")
        elif parent_targets and sid not in parent_targets:
            raise RestBranchValidationError(f"Stream {sid} route_source does not match any parent route.")
        _add_edge(children, incoming, parent_id, sid)
        outgoing_kinds.setdefault(parent_id, set()).add("conditional")

    for child_id, parents in incoming.items():
        if len(parents) > 1:
            raise RestBranchValidationError(f"Stream {child_id} has multiple parents. Routed graphs cannot converge.")

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(sid: str) -> None:
        if sid in visiting:
            raise RestBranchValidationError("Stream routing graph contains a cycle.")
        if sid in visited:
            return
        visiting.add(sid)
        for child in children.get(sid) or []:
            visit(child)
        visiting.remove(sid)
        visited.add(sid)

    for sid in list(children.keys()):
        visit(sid)

    return RestBranchValidationResult(children_by_parent=children, incoming_by_child=incoming)
