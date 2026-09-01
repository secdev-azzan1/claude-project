"""`kafka_kc` adapter compilation — compiler-spec.md §3.4 (FULL scope, T7.1).

`kafka_kc` is always terminal (R3) and never a flow root (R2), so it always
arrives via its BlockGroup's input port. Two entry points, called by
`compile_flow.py` around the generic `transforms.build_chain()` call so the
compiler-spec ordering holds exactly: envelope -> [user transforms incl.
dedup LAST] -> Avro publish.

  - `compile_envelope()` — the `UpdateRecord` `envelope` step
    (`/ingest_id=${uuid}`, `/ingest_ts=${now():toNumber()}`), producing the
    tail `transforms.build_chain()` continues from.
  - `compile_publish()` — `PublishKafka` with JsonTreeReader (reader) +
    AvroRecordSetWriter (writer, schema-name + ConfluentSchemaRegistry +
    schema-reference-writer), Failure Strategy = Route to Failure -> DLQ,
    PLUS the Kafka Connect sink connector (`connectors.py`). Raises
    `CompileError` if no schema has been approved for this block — "Deploy
    gate: approved schema required" (spec §3.4).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from models.adapter import FlowBlock
from services.adapter.naming import derive_topic_name, tokenize

from . import connectors
from .dlq import ensure_kafka_connection_cs
from .ir import CompileError, ControllerServiceSpec, ProcessorSpec, TopicSpec, apicurio_ccompat_url, ensure_json_record_services
from .jdbc_bookmarks import BookmarkSource, attach_bookmark_commit
from .transforms import Tail

if TYPE_CHECKING:  # pragma: no cover
    from models.adapter import Flow
    from .ir import BlockBuilder, CompileContext, ConnectorSpec


def compile_envelope(builder: "BlockBuilder", *, is_root: bool) -> Tail:
    if is_root:
        raise CompileError("kafka_kc block cannot be a flow root (R2)")
    reader_key, writer_key = ensure_json_record_services(builder)
    builder.add_processor(
        ProcessorSpec(
            key="envelope", name="envelope", type="org.apache.nifi.processors.standard.UpdateRecord",
            properties={
                "Record Reader": reader_key, "Record Writer": writer_key,
                "Replacement Value Strategy": "literal-value",
                "/ingest_id": "${uuid}",
                "/ingest_ts": "${now():toNumber()}",
            },
        )
    )
    builder.link("inputPort", "envelope", [])
    builder.to_dlq("envelope", "failure")
    return "envelope", "success"


def compile_publish(
    builder: "BlockBuilder",
    *,
    flow: "Flow",
    block: FlowBlock,
    ctx: "CompileContext",
    flow_token: str,
    add_param,
    topics_out,
    connectors_out: "list[ConnectorSpec]",
    tail: Tail,
    bookmark_source: BookmarkSource | None = None,
) -> None:
    # Schema inference deliberately uses the same compiled upstream chain as
    # deployment, but writes JSON to a disposable Kafka topic.  This is the
    # V1 ceremony contract: infer from records after NiFi has added envelope
    # fields and applied every configured transform, rather than inferring
    # from a browser probe or a pre-transform response.
    if ctx.inference_target_block_id == block.id:
        topic = (ctx.inference_topic or "").strip()
        if not topic:
            raise CompileError("Schema inference target has no temporary Kafka topic.")
        add_param(f"topic_{block.id}", topic, False)
        topics_out.append(TopicSpec(name=topic, kind="schema_inference", ownerBlockId=block.id))

        cs_kafka = ensure_kafka_connection_cs(builder, ctx, add_param)
        reader_key, writer_key = ensure_json_record_services(builder)
        tail_key, tail_rel = tail
        builder.add_processor(
            ProcessorSpec(
                key="publish", name="publish", type="org.apache.nifi.kafka.processors.PublishKafka",
                properties={
                    "Kafka Connection Service": cs_kafka,
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
                },
                autoTerminate=["success"],
            )
        )
        builder.link(tail_key, "publish", [tail_rel] if tail_rel else [])
        builder.to_dlq("publish", "failure")
        return

    if ctx.approved_schemas.get(block.id) is None:
        raise CompileError(
            f'kafka_kc block {block.id!r} ("{block.name}") has no approved schema — the schema '
            f"ceremony must be completed before this flow can compile for deploy."
        )

    topic = derive_topic_name(flow, block).value
    schema_name = f"{topic}-value"
    add_param(f"topic_{block.id}", topic, False)
    add_param(f"schema_{block.id}", schema_name, False)
    add_param("apicurio_ccompat_url", apicurio_ccompat_url(ctx.connection_config("apicurio").get("url", "")), False)
    topics_out.append(TopicSpec(name=topic, kind="data", ownerBlockId=block.id))

    cs_kafka = ensure_kafka_connection_cs(builder, ctx, add_param)
    reader_key, _json_writer_key = ensure_json_record_services(builder)

    builder.add_cs(
        ControllerServiceSpec(
            key="cs_schema_registry", name="schema_registry",
            type="org.apache.nifi.confluent.schemaregistry.ConfluentSchemaRegistry",
            properties={"Schema Registry URLs": "#{apicurio_ccompat_url}", "Cache Size": "1000",
                        "Cache Expiration": "1 hour", "Communications Timeout": "30 secs"},
        )
    )
    builder.add_cs(
        ControllerServiceSpec(
            key="cs_schema_ref_writer", name="schema_ref_writer",
            type="org.apache.nifi.confluent.schemaregistry.ConfluentEncodedSchemaReferenceWriter",
            properties={},
        )
    )
    builder.add_cs(
        ControllerServiceSpec(
            key="cs_avro_writer", name="avro_writer", type="org.apache.nifi.avro.AvroRecordSetWriter",
            properties={
                "Schema Access Strategy": "schema-name",
                "Schema Name": f"#{{schema_{block.id}}}",
                "Schema Registry": "cs_schema_registry",
                "Schema Write Strategy": "schema-reference-writer",
                "Schema Reference Writer": "cs_schema_ref_writer",
                "Encoder Pool Size": "32",
                "Cache Size": "1000",
                "Compression Format": "NONE",
            },
        )
    )

    tail_key, tail_rel = tail
    builder.add_processor(
        ProcessorSpec(
            key="publish", name="publish", type="org.apache.nifi.kafka.processors.PublishKafka",
            properties={
                "Kafka Connection Service": cs_kafka,
                "Topic Name": f"#{{topic_{block.id}}}",
                "Record Reader": reader_key,
                "Record Writer": "cs_avro_writer",
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
            },
            autoTerminate=[] if bookmark_source else ["success"],
        )
    )
    builder.link(tail_key, "publish", [tail_rel])
    builder.to_dlq("publish", "failure")

    if bookmark_source:
        attach_bookmark_commit(
            builder,
            ctx=ctx,
            flow_id=flow.id,
            source=bookmark_source,
            add_param=add_param,
            tail=("publish", "success"),
            key_prefix="egress",
        )

    entity_token = tokenize(block.entity or block.name)
    connector = connectors.build_kafka_kc_connector(
        flow=flow, block=block, ctx=ctx, flow_token=flow_token, topic=topic, entity_token=entity_token,
    )
    connectors_out.append(connector)
