"""Validation shared by Kafka Connect sync linking and flow validation.

The Kafka Connect page stores a flat connector property map, while Flow
Builder owns the topic selected/derived for a sink block.  A link is only
safe when those two views describe one concrete sink topic.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from models.adapter import AppService, Flow, FlowBlock
from services.adapter.naming import derive_topic_name


_EXPECTED_CONNECTOR_CLASSES = {
    "iceberg_catalog": "org.apache.iceberg.connect.IcebergSinkConnector",
    "opensearch": "io.aiven.kafka.connect.opensearch.OpensearchSinkConnector",
}


def _topic_values(value: Any) -> List[str]:
    if isinstance(value, list):
        values: List[str] = []
        for item in value:
            values.extend(_topic_values(item))
        return values
    if value is None:
        return []
    return [part.strip() for part in str(value).split(",") if part.strip()]


def sync_topic(config: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """Return the one explicit topic, or a user-facing refusal.

    ``topics`` is Kafka Connect's standard sink property.  ``topic`` is
    accepted for simple/custom connector configurations.  Regex and
    multi-topic subscriptions cannot be safely attached to one flow sink.
    """
    if config.get("topics.regex") not in (None, ""):
        return None, "A flow-linked sync must use one exact topic; topics.regex is not supported."

    values: List[str] = []
    for key in ("topics", "topic"):
        if key in config:
            values.extend(_topic_values(config.get(key)))
    values = list(dict.fromkeys(values))
    if not values:
        return None, "Set exactly one topic (topics or topic) in the sync before linking it to a flow."
    if len(values) != 1:
        return None, "A flow-linked sync must target exactly one topic."
    return values[0], None


def flow_sink_topic(flow: Flow, block: FlowBlock) -> Tuple[Optional[str], List[str]]:
    """Resolve the topic owned/consumed by a Kafka Connect block."""
    issues: List[str] = []
    config = block.config or {}

    if not (block.entity or "").strip():
        issues.append("Set the sink entity before linking a Kafka Connect sync.")

    if block.adapter == "kc":
        attach_id = str(config.get("attachTopicId") or "").strip()
        if not attach_id:
            issues.append("Attach the Kafka Connect subscription to a flow topic before linking a sync.")
            return None, issues
        topic = next((item for item in flow.topics if item.id == attach_id), None)
        if topic is None:
            issues.append("The selected flow topic no longer exists.")
            return None, issues
        if topic.sealed:
            issues.append("Sealed flow topics are managed with their governed sink and cannot accept this subscription.")
        name = (topic.name or "").strip()
        if not name:
            issues.append("The selected flow topic has no name.")
        return name or None, issues

    topic = next((item for item in flow.topics if item.writerBlockId == block.id), None)
    name = (topic.name or "").strip() if topic else ""
    if not name:
        derived = derive_topic_name(flow, block)
        name = derived.value.strip()
        if not name or name == "raw.<entity missing>":
            issues.append("The flow sink does not have a resolvable topic name yet.")
            return None, issues
    return name, issues


def validate_sync_link(
    sync: Dict[str, Any],
    flow: Flow,
    block: FlowBlock,
    services: List[AppService],
) -> List[str]:
    """Return all deterministic refusals for one sync-to-block link."""
    issues: List[str] = []
    if block.adapter not in ("kc", "kafka_kc"):
        issues.append("Kafka Connect syncs can only be linked to Kafka Connect sink blocks.")
        return issues

    if sync.get("direction", "sink") != "sink":
        issues.append("Only sink-direction Kafka Connect syncs can be linked to flow sink blocks.")

    config = dict(sync.get("config") or {})
    sync_class = str(sync.get("connector_class") or config.get("connector.class") or "").strip()
    block_sink = (block.config or {}).get("sinkConfig")
    if isinstance(block_sink, dict):
        block_class = str(block_sink.get("connector.class") or "").strip()
        if block_class and sync_class and block_class != sync_class:
            issues.append(
                f"Connector class mismatch: the flow block uses '{block_class}' but the sync uses '{sync_class}'."
            )

    service_id = str((block.config or {}).get("sinkServiceId") or block.serviceId or "").strip()
    if not service_id:
        issues.append("Select a sink destination service on the flow block before linking a sync.")
        service = None
    else:
        service = next((item for item in services if item.id == service_id), None)
        if service is None:
            issues.append("The selected sink destination service no longer exists.")
        elif service.retired:
            issues.append(f'Sink destination service "{service.name}" is retired.')
        elif service.type != "sink_destination":
            issues.append("The selected service is not a sink destination service.")

    if service is not None:
        expected_class = _EXPECTED_CONNECTOR_CLASSES.get(str((service.config or {}).get("kind") or ""))
        if expected_class and sync_class and sync_class != expected_class:
            issues.append(
                f"Connector class '{sync_class}' does not match the destination service; expected '{expected_class}'."
            )

    flow_topic, topic_issues = flow_sink_topic(flow, block)
    issues.extend(topic_issues)
    sync_topic_name, sync_topic_issue = sync_topic(config)
    if sync_topic_issue:
        issues.append(sync_topic_issue)
    elif flow_topic and sync_topic_name != flow_topic:
        issues.append(
            f"Topic mismatch: the flow resolves to '{flow_topic}', but the sync targets '{sync_topic_name}'."
        )
    return issues
