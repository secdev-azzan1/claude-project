"""Schema inference service - wraps the core logic from infer_kafka_schema.py."""
from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set


BOOL_TRUE = {"true", "t", "yes", "y"}
BOOL_FALSE = {"false", "f", "no", "n"}
MAX_COMPLEX_DEPTH = 5


def sanitize_name(name: str) -> str:
    out = re.sub(r"[^A-Za-z0-9_]", "_", name)
    if not out:
        out = "Record"
    if re.match(r"^[0-9]", out):
        out = "_" + out
    return out


@dataclass
class Node:
    type_counts: Counter = field(default_factory=Counter)
    object_count: int = 0
    fields: Dict[str, "Node"] = field(default_factory=dict)
    field_presence: Counter = field(default_factory=Counter)
    array_count: int = 0
    item: Optional["Node"] = None
    scalar_examples: List[Any] = field(default_factory=list)


def new_node() -> Node:
    return Node()


def add_scalar_example(node: Node, value: Any, max_examples: int = 100) -> None:
    if len(node.scalar_examples) < max_examples:
        node.scalar_examples.append(value)


def observe(node: Node, value: Any) -> None:
    if value is None:
        node.type_counts["null"] += 1
        return
    if isinstance(value, bool):
        node.type_counts["boolean"] += 1
        add_scalar_example(node, value)
        return
    if isinstance(value, int):
        node.type_counts["integer"] += 1
        add_scalar_example(node, value)
        return
    if isinstance(value, float):
        node.type_counts["number"] += 1
        add_scalar_example(node, value)
        return
    if isinstance(value, str):
        node.type_counts["string"] += 1
        add_scalar_example(node, value)
        return
    if isinstance(value, dict):
        node.type_counts["object"] += 1
        node.object_count += 1
        for k, v in value.items():
            if k not in node.fields:
                node.fields[k] = new_node()
            node.field_presence[k] += 1
            observe(node.fields[k], v)
        return
    if isinstance(value, list):
        node.type_counts["array"] += 1
        node.array_count += 1
        if node.item is None:
            node.item = new_node()
        for item in value:
            observe(node.item, item)
        return
    node.type_counts["string"] += 1
    add_scalar_example(node, str(value))


def merge_into(dst: Node, src: Node, max_examples: int = 100) -> None:
    dst.type_counts.update(src.type_counts)
    dst.object_count += src.object_count
    dst.field_presence.update(src.field_presence)
    dst.array_count += src.array_count
    for ex in src.scalar_examples:
        if len(dst.scalar_examples) >= max_examples:
            break
        dst.scalar_examples.append(ex)
    for k, src_field in src.fields.items():
        if k not in dst.fields:
            dst.fields[k] = new_node()
        merge_into(dst.fields[k], src_field, max_examples=max_examples)
    if src.item is not None:
        if dst.item is None:
            dst.item = new_node()
        merge_into(dst.item, src.item, max_examples=max_examples)


def clone_node(src: Node) -> Node:
    out = new_node()
    merge_into(out, src)
    return out


class AvroBuilder:
    def __init__(self, namespace: str, root_name: str, always_nullable: bool = True, max_complex_depth: int = MAX_COMPLEX_DEPTH):
        self.namespace = namespace
        self.root_name = sanitize_name(root_name)
        self.used_record_names: set = set()
        self.always_nullable = always_nullable
        self.max_complex_depth = max(1, int(max_complex_depth))

    def unique_record_name(self, base: str) -> str:
        base = sanitize_name(base)
        if base not in self.used_record_names:
            self.used_record_names.add(base)
            return base
        i = 2
        while f"{base}_{i}" in self.used_record_names:
            i += 1
        name = f"{base}_{i}"
        self.used_record_names.add(name)
        return name

    def _flatten_union_types(self, avro_type: Any) -> List[Any]:
        """Flatten nested unions and preserve stable order."""
        if not isinstance(avro_type, list):
            return [avro_type]

        flat: List[Any] = []
        for t in avro_type:
            if isinstance(t, list):
                flat.extend(self._flatten_union_types(t))
            else:
                flat.append(t)

        deduped: List[Any] = []
        seen = set()
        for t in flat:
            key = repr(t)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(t)
        return deduped

    def ensure_nullable(self, avro_type: Any) -> List[Any]:
        union_members = self._flatten_union_types(avro_type)
        non_null = [x for x in union_members if x != "null"]
        return ["null"] + non_null

    def _observed_non_null_types(self, node: Node) -> Set[str]:
        return {t for t, c in node.type_counts.items() if c > 0 and t != "null"}

    def _scalar_counts(self, node: Node) -> Dict[str, int]:
        return {
            "boolean": int(node.type_counts.get("boolean", 0)),
            "integer": int(node.type_counts.get("integer", 0)),
            "number": int(node.type_counts.get("number", 0)),
            "string": int(node.type_counts.get("string", 0)),
        }

    def _looks_like_temporal(self, path: str) -> bool:
        token = path.split(".")[-1].replace("[]", "").replace("{}", "").lower()
        temporal_keywords = {"time", "timestamp", "datetime", "date", "ingest_ts"}
        if token in temporal_keywords:
            return True
        for suffix in ("_ts", "_timestamp", "timestamp", "_time", "time", "_date", "date"):
            if token.endswith(suffix):
                return True
        return False

    def _timestamp_logical_type_from_ints(self, ints: List[int]) -> Optional[str]:
        if not ints:
            return None
        sec_min, sec_max = 946684800, 4102444800
        ms_min, ms_max = sec_min * 1000, sec_max * 1000
        us_min, us_max = sec_min * 1_000_000, sec_max * 1_000_000
        is_ms = all(ms_min <= v <= ms_max for v in ints) and all(len(str(abs(v))) >= 13 for v in ints)
        is_us = all(us_min <= v <= us_max for v in ints) and all(len(str(abs(v))) >= 16 for v in ints)
        if is_ms:
            return "timestamp-millis"
        if is_us:
            return "timestamp-micros"
        return None

    def _infer_timestamp_type(self, node: Node, path: str) -> Optional[Dict[str, str]]:
        if not self._looks_like_temporal(path):
            return None
        examples = [x for x in node.scalar_examples if x is not None]
        if len(examples) < 3:
            return None
        ints: List[int] = []
        for ex in examples:
            if isinstance(ex, bool) or isinstance(ex, float):
                return None
            if isinstance(ex, int):
                ints.append(ex)
                continue
            return None
        logical = self._timestamp_logical_type_from_ints(ints)
        if logical is None:
            return None
        return {"type": "long", "logicalType": logical}

    def _scalar_type_from_observation(self, node: Node, path: str) -> Any:
        counts = self._scalar_counts(node)
        has_bool = counts["boolean"] > 0
        has_int = counts["integer"] > 0
        has_num = counts["number"] > 0
        has_str = counts["string"] > 0
        if not (has_bool or has_int or has_num or has_str):
            return "string"
        if has_str and (has_bool or has_int or has_num):
            return "string"
        if has_bool and (has_int or has_num):
            return "string"
        if has_num:
            return "double"
        if has_int:
            ts_type = self._infer_timestamp_type(node, path)
            if ts_type is not None:
                return ts_type
            return "long"
        if has_bool:
            return "boolean"
        return "string"

    def _build_record(self, node: Node, path: str, depth: int) -> Dict[str, Any]:
        rec_name = self.unique_record_name(path or self.root_name)
        fields = []
        used_field_names: Set[str] = set()
        # Preserve first-seen field order from samples instead of sorting.
        # For tabular sources (CSV/XLSX), this keeps schema column order
        # aligned with source file column order.
        for fname in node.fields.keys():
            child_path = f"{path}.{fname}" if path else fname
            child_node = node.fields.get(fname, new_node())
            child_type = self.type_for_node(child_node, child_path, depth + 1)
            if self.always_nullable:
                child_type = self.ensure_nullable(child_type)
            safe_name = sanitize_name(fname)
            if safe_name in used_field_names:
                i = 2
                while f"{safe_name}_{i}" in used_field_names:
                    i += 1
                safe_name = f"{safe_name}_{i}"
            used_field_names.add(safe_name)

            field_obj: Dict[str, Any] = {"name": safe_name, "type": child_type}
            if self.always_nullable:
                field_obj["default"] = None
            fields.append(field_obj)
        return {"type": "record", "name": rec_name, "fields": fields}

    def _build_array(self, node: Node, path: str, depth: int) -> Dict[str, Any]:
        item_node = clone_node(node.item) if node.item is not None else new_node()
        if node.object_count > 0:
            object_as_item = new_node()
            object_as_item.type_counts["object"] = node.object_count
            object_as_item.object_count = node.object_count
            object_as_item.fields = node.fields
            object_as_item.field_presence = node.field_presence
            merge_into(item_node, object_as_item)
        item_type = self.type_for_node(item_node, f"{path}[]", depth + 1)
        if item_type == "null":
            item_type = "string"
        if self.always_nullable:
            item_type = self.ensure_nullable(item_type)
        return {"type": "array", "items": item_type}

    def type_for_node(self, node: Node, path: str, depth: int) -> Any:
        observed = self._observed_non_null_types(node)
        has_object = "object" in observed
        has_array = "array" in observed
        has_scalar = any(t in observed for t in ("boolean", "integer", "number", "string"))
        if depth > self.max_complex_depth and (has_object or has_array):
            return "string"
        if has_array and has_object and not has_scalar:
            return self._build_array(node, path, depth)
        if has_array and not has_scalar:
            return self._build_array(node, path, depth)
        if has_object and not has_scalar:
            return self._build_record(node, path, depth)
        return self._scalar_type_from_observation(node, path)

    def build_root_schema(self, root_node: Node) -> Any:
        root_type = self.type_for_node(root_node, "", 1)
        if isinstance(root_type, dict) and root_type.get("type") == "record":
            root_type["namespace"] = self.namespace
        return root_type


def infer_avro_schema(samples: List[Any], name: str, namespace: str = "com.nif") -> Dict[str, Any]:
    """Infer an Avro schema from a list of sample objects.
    
    Args:
        samples: List of Python dicts/lists/primitives representing sample data.
        name: Root record name for the schema.
        namespace: Avro namespace for the schema.
    
    Returns:
        Avro schema as a dict.
    """
    root = new_node()
    for sample in samples:
        observe(root, sample)
    builder = AvroBuilder(namespace=namespace, root_name=name, always_nullable=True)
    return builder.build_root_schema(root)
