"""`kafka` adapter compilation — compiler-spec.md §3.3.

FULL scope for T7.1: `write` mode (JSON passthrough). `read` mode raises
`NotImplementedError` — it lands in a follow-up task (R8 raw-bytes
passthrough needs its own careful handling, out of scope here).

A `kafka` write block is never a flow root (`compute_root_menu()` on the
frontend has no "kafka · write" root entry), so it always arrives via its
BlockGroup's input port — `compile_flow` always builds this adapter's group
with `inputPort=True`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from models.adapter import FlowBlock
from services.adapter.naming import derive_topic_name

from .dlq import ensure_kafka_connection_cs
from .ir import CompileError, ProcessorSpec, TopicSpec, ensure_json_record_services
from .transforms import Tail

if TYPE_CHECKING:  # pragma: no cover
    from models.adapter import Flow
    from .ir import BlockBuilder, CompileContext


def compile_entry(block: FlowBlock, *, is_root: bool) -> Tail:
    if block.mode != "write":
        raise NotImplementedError(
            f"kafka {block.mode} is not implemented yet (T7.1 scope: kafka write only) — block {block.id}"
        )
    if is_root:
        raise CompileError(f"kafka write block {block.id!r} cannot be a flow root")
    return "inputPort", ""


def _kafka_key_expr(block: FlowBlock) -> Optional[str]:
    """`kafka key from an extract-transform-designated attribute when
    present` (spec §3.3): the convention, matching the rapid7-securado /
    sentinelone reference flows, is an `extract` rule that targets the
    well-known attribute name `kafka.key`."""
    for rule in block.transforms:
        if rule.kind == "extract" and rule.config.get("attribute") == "kafka.key":
            return "${kafka.key}"
    return None


def compile_publish(
    builder: "BlockBuilder",
    *,
    flow: "Flow",
    block: FlowBlock,
    ctx: "CompileContext",
    flow_token: str,
    add_param,
    topics_out,
    tail: Tail,
) -> None:
    topic = derive_topic_name(flow, block).value
    add_param(f"topic_{block.id}", topic, False)
    topics_out.append(TopicSpec(name=topic, kind="data", ownerBlockId=block.id))

    cs_key = ensure_kafka_connection_cs(builder, ctx, add_param)
    reader_key, writer_key = ensure_json_record_services(builder)
    kafka_key = _kafka_key_expr(block)

    props = {
        "Kafka Connection Service": cs_key,
        "Topic Name": f"#{{topic_{block.id}}}",
        "Record Reader": reader_key,
        "Record Writer": writer_key,
        "Publish Strategy": "USE_VALUE",
        "Record Metadata Strategy": "FROM_PROPERTIES",
        "Failure Strategy": "Route to Failure",
        "acks": "all",
        "compression.type": "none",
        "partitioner.class": "org.apache.kafka.clients.producer.internals.DefaultPartitioner",
        "Transactions Enabled": "false",
        "Kafka Key Attribute Encoding": "utf-8",
        "Header Encoding": "UTF-8",
        "max.request.size": "1 MB",
    }
    if kafka_key:
        props["Kafka Key"] = kafka_key

    tail_key, tail_rel = tail
    builder.add_processor(
        ProcessorSpec(key="publish", name="publish", type="org.apache.nifi.kafka.processors.PublishKafka",
                      properties=props, autoTerminate=["success"])
    )
    builder.link(tail_key, "publish", [tail_rel] if tail_rel else [])
    builder.to_dlq("publish", "failure")
