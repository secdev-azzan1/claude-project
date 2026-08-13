"""`kafka` adapter compilation — compiler-spec.md §3.3.

FULL scope: both `write` mode (JSON passthrough) and `read` mode (`ConsumeKafka`
+ per-`parseFormat` splitting, R8 raw-bytes passthrough).

A `kafka` write block is never a flow root (`compute_root_menu()` on the
frontend has no "kafka · write" root entry), so it always arrives via its
BlockGroup's input port — `compile_flow` always builds this adapter's group
with `inputPort=True`.

A `kafka` READ block is a continuous consumer (R1: "kafka reads are never
scheduled"), so `ConsumeKafka` needs no trigger/input-port wiring at all,
root or not.

ARCHITECTURE NOTE: `compile_flow.py`'s kafka dispatch calls
`compile_entry(block, is_root=is_root)` (no `builder`) which, for read mode,
returns a "promised" Tail — the key/relationship of the parse/split processor
that `compile_publish()` (which does receive `builder`) adds afterwards.
Safe because BlockBuilder's graph is flat lists: `transforms.build_chain()`
only records connections referencing the key, which exists by
`build_group()` time.

kafka blocks are NOT terminal (R3: the chain continues after a kafka write;
a read is a source — only kafka_kc/kc are terminal). `compile_flow.py`
always calls `compile_publish()` for kafka blocks (read → consume+split
chain; write → PublishKafka consuming the tail) and then wires children off
the tail via `routing.wire_children()`, or auto-terminates the tail itself
in the childless-read case. This module must NOT auto-terminate the tail —
tail disposition belongs to `compile_flow.py` (regression-tested by
`test_kafka_read_with_child_gets_port_link`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from models.adapter import FlowBlock
from services.adapter.naming import derive_topic_name

from .dlq import ensure_kafka_connection_cs
from .ir import CompileError, ControllerServiceSpec, ProcessorSpec, TopicSpec, ensure_json_record_services
from .transforms import Tail

if TYPE_CHECKING:  # pragma: no cover
    from models.adapter import Flow
    from .ir import BlockBuilder, CompileContext

_CONSUME_KAFKA_TYPE = "org.apache.nifi.kafka.processors.ConsumeKafka"


def compile_entry(block: FlowBlock, *, is_root: bool) -> Tail:
    if block.mode == "read":
        return _read_entry_tail(block)
    if block.mode != "write":
        raise NotImplementedError(f"kafka {block.mode} is not implemented yet — block {block.id}")
    if is_root:
        raise CompileError(f"kafka write block {block.id!r} cannot be a flow root")
    return "inputPort", ""


def _read_entry_tail(block: FlowBlock) -> Tail:
    """The "promised" tail `compile_publish()`'s read branch will fulfill —
    see the module docstring's ARCHITECTURE NOTE. `raw` skips parsing
    entirely (R8 quarantine: byte passthrough, no record processing) so the
    promise is `ConsumeKafka`'s own `success` relationship directly; every
    other format (json included — M17, see `_compile_read_terminal`) promises
    the eventual per-record `SplitRecord` step's `splits` relationship."""
    parse_format = str(block.config.get("parseFormat", "json"))
    if parse_format == "raw":
        return "consume", "success"
    if parse_format not in ("json", "csv", "xml"):
        raise NotImplementedError(f"kafka read parseFormat {parse_format!r} is not implemented — block {block.id}")
    return "split", "splits"


def _resolve_topic(flow: "Flow", block: FlowBlock) -> str:
    """The topic a kafka-read block consumes: the ADOPTED (or, mid-flow, a
    materialized) topic node's own name when this block's `parentId` IS a
    topic node (compute_topic_menu's "attach a kafka·read to a topic" path —
    mirrors how `compile_flow._compile_kc` resolves `attachTopicId`, just
    keyed by `parentId` instead since that's how a plain kafka-read attaches,
    per types.ts's FlowBlock.parentId doc comment); `config.topicName`
    otherwise (a bare, unattached read — defensive fallback, not the primary
    path the frontend produces)."""
    topic = next((t for t in flow.topics if t.id == block.parentId), None)
    if topic is not None:
        return topic.name
    name = str(block.config.get("topicName", "")).strip()
    if not name:
        raise CompileError(f"kafka read block {block.id!r} has no topic configured")
    return name


def _ensure_csv_reader(builder: "BlockBuilder") -> str:
    key = "cs_csv_reader"
    if not builder.has_cs(key):
        builder.add_cs(
            ControllerServiceSpec(
                key=key, name="csv_reader", type="org.apache.nifi.csv.CSVReader",
                properties={"Schema Access Strategy": "infer-schema", "Treat First Line as Header": "true"},
            )
        )
    return key


def _ensure_xml_reader(builder: "BlockBuilder") -> str:
    key = "cs_xml_reader"
    if not builder.has_cs(key):
        builder.add_cs(
            ControllerServiceSpec(
                key=key, name="xml_reader", type="org.apache.nifi.xml.XMLReader",
                properties={"Schema Access Strategy": "infer-schema", "Expect Records as Array": "false"},
            )
        )
    return key


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
    if block.mode == "read":
        _compile_read_terminal(builder, flow=flow, block=block, ctx=ctx, flow_token=flow_token,
                               add_param=add_param, tail=tail)
        return

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


def _compile_read_terminal(
    builder: "BlockBuilder", *, flow: "Flow", block: FlowBlock, ctx: "CompileContext", flow_token: str,
    add_param, tail: Tail,
) -> None:
    """Fulfills `_read_entry_tail()`'s promise — see the module docstring's
    ARCHITECTURE NOTE for why this lives here (in `compile_publish`, called
    AFTER `transforms.build_chain()` already ran) rather than in
    `compile_entry()`.

    `ConsumeKafka` property names/allowed-values below are NOT confirmed
    against any reference flow — none of the 5 reference exports
    (`docs/orchestration/analysis/nifi-reference-flows.md`) use `ConsumeKafka`
    at all (only `PublishKafka`). Flagged explicitly for live E2E
    verification: "Kafka Connection Service", "Group ID", "Topics",
    "Topic Format", "Auto Offset Reset" are this module's best-known NiFi
    2.9 Kafka3-family property names, by analogy with `PublishKafka`'s own
    confirmed "Kafka Connection Service" property and long-standing NiFi
    Kafka-consumer convention, but are unverified here.
    """
    parse_format = str(block.config.get("parseFormat", "json"))
    topic = _resolve_topic(flow, block)
    add_param(f"topic_{block.id}", topic, False)

    cs_key = ensure_kafka_connection_cs(builder, ctx, add_param)
    initial_position = str(block.config.get("initialPosition", "beginning"))
    offset_reset = "latest" if initial_position == "new" else "earliest"

    builder.add_processor(
        ProcessorSpec(
            key="consume", name="consume", type=_CONSUME_KAFKA_TYPE,
            properties={
                "Kafka Connection Service": cs_key,
                "Group ID": f"{flow_token}__{block.id}",
                "Topic Format": "names",
                "Topics": f"#{{topic_{block.id}}}",
                "Auto Offset Reset": offset_reset,
            },
        )
    )

    if parse_format != "raw":
        # M17: json splits via record-based SplitRecord (JsonTreeReader ->
        # JsonRecordSetWriter, 1 record per split), NOT SplitJson `$[*]` —
        # a Kafka message is normally one JSON OBJECT, and Jayway's `$[*]`
        # over an object shreds it into its VALUES (scalars). SplitRecord
        # handles both shapes: an object message is one record, an
        # array-batched message is N records. csv/xml already took this
        # path; json now shares it with its own reader.
        if parse_format == "json":
            reader_key, writer_key = ensure_json_record_services(builder)
        else:
            reader_key = _ensure_csv_reader(builder) if parse_format == "csv" else _ensure_xml_reader(builder)
            _, writer_key = ensure_json_record_services(builder)
        builder.add_processor(
            ProcessorSpec(key="split", name="split", type="org.apache.nifi.processors.standard.SplitRecord",
                          properties={"Record Reader": reader_key, "Record Writer": writer_key,
                                      "Records Per Split": "1"},
                          autoTerminate=["original"])
        )
        builder.link("consume", "split", ["success"])
        builder.to_dlq("split", "failure")

    # Tail termination is compile_flow.py's responsibility: children (legal
    # per R3) are wired off the tail there, and the childless case is
    # auto-terminated by its "non-terminal, childless" branch.
