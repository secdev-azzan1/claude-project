"""Migrate legacy source/stream documents to the new routing model.

Idempotent: safe to call on already-migrated documents.
"""
import uuid
from typing import Any, Dict, List, Optional


def migrate_source(source_doc: Dict[str, Any]) -> Dict[str, Any]:
    """Convert old is_primary / routing_rules shape to new entity/routes/filters shape.

    Changes:
    - routing_rules with destination=include/exclude -> stream.filters
    - routing_rules with destination=route_to_topic -> stream.routes + synthetic child streams
    - is_primary=True -> stream.entity with kafka config from source kafka_output
    """
    streams = source_doc.get("streams") or []
    kafka_output = source_doc.get("kafka_output") or {}
    has_explicit_entity_stream = any(isinstance(stream, dict) and bool(stream.get("entity")) for stream in streams)

    migrated_streams = []
    synthetic_children: List[Dict[str, Any]] = []

    for stream in streams:
        stream = dict(stream)  # shallow copy

        # Skip already-migrated streams (have entity or route_source already)
        already_has_entity = bool(stream.get("entity"))
        already_has_route_source = bool(stream.get("route_source"))

        # Migrate routing_rules -> filters + entity + synthetic route-children
        routing_rules = stream.pop("routing_rules", []) or []

        # Separate include/exclude from route_to_topic
        filter_rules = []
        topic_routes = []
        for rr in routing_rules:
            dest = (rr.get("destination") or "include").lower()
            if dest in ("include", "exclude"):
                filter_rules.append({
                    "id": str(uuid.uuid4()),
                    "attribute": rr.get("attribute", ""),
                    "condition": rr.get("condition", "equals"),
                    "value": rr.get("value", ""),
                    "action": dest,
                })
            elif dest == "route_to_topic":
                topic_routes.append(rr)

        # Set filters if not already present
        if not stream.get("filters") and filter_rules:
            stream["filters"] = filter_rules
        elif not stream.get("filters"):
            stream["filters"] = []

        # Ensure routes list exists
        if not stream.get("routes"):
            stream["routes"] = []

        # Migrate is_primary -> entity config
        is_primary = stream.get("is_primary", False)
        if is_primary and not already_has_entity and not has_explicit_entity_stream:
            stream["entity"] = {
                "kafka": dict(kafka_output),
                "schema_artifact_id": None,
                "schema_version": None,
            }

        # Migrate route_to_topic rules -> synthetic child streams + Route entries
        routes = list(stream.get("routes") or [])
        for rr in topic_routes:
            child_id = str(uuid.uuid4())
            child_topic = rr.get("topic_name") or ""
            route_id = str(uuid.uuid4())

            # Create Route entry on parent
            routes.append({
                "id": route_id,
                "condition": {
                    "attribute": rr.get("attribute", ""),
                    "condition": rr.get("condition", "equals"),
                    "value": rr.get("value", ""),
                },
                "child_stream_id": child_id,
                "ordinal": len(routes),
            })

            # Create synthetic child stream
            synthetic_children.append({
                "id": child_id,
                "name": f"{stream.get('name', 'stream')}_routed_{child_topic.replace('.', '_')}",
                "endpoint_path": stream.get("endpoint_path", "/"),
                "method": stream.get("method", "GET"),
                "response_format": stream.get("response_format", "json"),
                "route_source": {
                    "parent_stream_id": stream.get("id") or stream.get("name", ""),
                    "route_id": route_id,
                },
                "entity": {
                    "kafka": {
                        "topic": child_topic,
                        "partitions": 6,
                        "replication_factor": 3,
                        "key_strategy": "none",
                        "value_format": "avro",
                        "schema_mode": "existing",
                    },
                    "schema_artifact_id": None,
                    "schema_version": None,
                },
                "extraction_rules": [],
                "transformation_rules": [],
                "filters": [],
                "routes": [],
                "is_primary": False,
            })

        if routes:
            stream["routes"] = routes

        migrated_streams.append(stream)

    migrated_streams.extend(synthetic_children)
    result = dict(source_doc)
    result["streams"] = migrated_streams
    return result
