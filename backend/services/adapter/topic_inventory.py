"""Canonical topic metadata for adapter flows.

The flow model stores materialized topics as ``flow.topics`` nodes, while the
compiler can also determine a Kafka topic directly from a Kafka-family writer
block (including its ``topicOverride``).  Older/imported flows may contain the
writer configuration but no materialized topic node.  Keep those two
representations consistent at the API boundary so every consumer sees the
same topic inventory.
"""

from __future__ import annotations

from typing import Any, Dict

from models.adapter import Flow
from services.adapter.naming import derive_topic_name


def _is_kafka_family_write(block) -> bool:
    return (block.adapter == "kafka" and block.mode == "write") or block.adapter == "kafka_kc"


def materialize_flow_topics(flow_doc: Dict[str, Any]) -> Dict[str, Any]:
    """Return a flow document with materialized writer topics present.

    This is deliberately additive for reads: adopted topics and any existing
    topic records are preserved.  A missing topic is created with a stable id
    derived from its writer block, so repeated list/detail requests do not
    generate different topic ids.  Existing writer topics are refreshed from
    the same naming function used by compilation, including overrides.

    The helper does not call Kafka, NiFi, or Mongo and does not claim that a
    topic exists on the broker; it only reconciles flow metadata.
    """
    try:
        flow = Flow(**flow_doc)
    except Exception:
        # The list/detail endpoints historically returned raw documents. Keep
        # that compatibility for a malformed legacy document rather than
        # making an unrelated flow disappear from the UI.
        return dict(flow_doc)

    normalized = dict(flow_doc)
    topic_docs = [dict(topic) for topic in (flow_doc.get("topics") or [])]
    by_writer = {
        topic.get("writerBlockId"): topic
        for topic in topic_docs
        if topic.get("writerBlockId")
    }
    used_ids = {str(topic.get("id")) for topic in topic_docs if topic.get("id")}

    for block in flow.blocks:
        if not _is_kafka_family_write(block):
            continue

        topic_name = derive_topic_name(flow, block).value
        if not topic_name:
            continue

        topic = by_writer.get(block.id)
        if topic is None:
            topic_id = f"topic-{block.id}"
            suffix = 2
            while topic_id in used_ids:
                topic_id = f"topic-{block.id}-{suffix}"
                suffix += 1
            topic = {
                "id": topic_id,
                "kind": "materialized",
                "name": topic_name,
                "sealed": block.adapter == "kafka_kc",
                "writerBlockId": block.id,
            }
            topic_docs.append(topic)
            by_writer[block.id] = topic
            used_ids.add(topic_id)
        else:
            # Match the compiler's current name and governance state. This
            # repairs stale metadata without touching adopted topics.
            topic["name"] = topic_name
            topic["sealed"] = block.adapter == "kafka_kc"

    normalized["topics"] = topic_docs
    return normalized
