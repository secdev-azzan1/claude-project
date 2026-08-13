"""Per-block DLQ wiring — compiler-spec.md §6.

Every block group gets its own `dlq__meta` (UpdateAttribute) -> `dlq__publish`
(PublishKafka) pair. Every record-failure relationship anywhere else in the
group is wired to the symbolic `"dlq"` connection target (see
`ir.ConnectionSpec`), which `build()` resolves locally to `dlq__meta`'s key —
DLQ never crosses a process-group boundary, so this needs no PortLink.

`ensure_kafka_connection_cs()` is shared with the block's own PublishKafka
step (kafka write / kafka_kc) so a group that both publishes AND has a DLQ
path gets exactly one `Kafka3ConnectionService` instance, not two.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .ir import ControllerServiceSpec, ProcessorSpec

if TYPE_CHECKING:  # pragma: no cover
    from models.adapter import Flow
    from .ir import BlockBuilder, CompileContext

KAFKA_CONNECTION_CS_KEY = "cs_kafka_conn"


def ensure_kafka_connection_cs(builder: "BlockBuilder", ctx: "CompileContext", add_param) -> str:
    """Add (once per group) the shared Kafka3ConnectionService CS, return its key."""
    if not builder.has_cs(KAFKA_CONNECTION_CS_KEY):
        add_param("kafka_bootstrap", ctx.connection_config("kafka").get("bootstrapServers") or "kafka:9092", False)
        builder.add_cs(
            ControllerServiceSpec(
                key=KAFKA_CONNECTION_CS_KEY,
                name="kafka_connection",
                type="org.apache.nifi.kafka.service.Kafka3ConnectionService",
                properties={
                    "bootstrap.servers": "#{kafka_bootstrap}",
                    "security.protocol": "PLAINTEXT",
                    "ack.wait.time": "5 sec",
                    "max.block.ms": "5 sec",
                    "default.api.timeout.ms": "60 sec",
                    "max.poll.records": "10000",
                    "isolation.level": "read_committed",
                },
            )
        )
    return KAFKA_CONNECTION_CS_KEY


def build(builder: "BlockBuilder", *, flow: "Flow", block, ctx: "CompileContext", flow_token: str, add_param) -> None:
    add_param("dlq_topic", f"dlq.{flow_token}", False)
    cs_key = ensure_kafka_connection_cs(builder, ctx, add_param)

    builder.add_processor(
        ProcessorSpec(
            key="dlq__meta",
            name="dlq__meta",
            type="org.apache.nifi.processors.attributes.UpdateAttribute",
            properties={
                "dlq.block": block.id,
                "dlq.reason": "${dlq.reason:isEmpty():ifElse('unspecified_failure', ${dlq.reason})}",
            },
        )
    )
    builder.link("dlq__meta", "dlq__publish", ["success"])
    builder.add_processor(
        ProcessorSpec(
            key="dlq__publish",
            name="dlq__publish",
            type="org.apache.nifi.kafka.processors.PublishKafka",
            properties={
                "Kafka Connection Service": cs_key,
                "Topic Name": "#{dlq_topic}",
                "Record Reader": None,
                "Record Writer": None,
                "Publish Strategy": "USE_VALUE",
                "Record Metadata Strategy": "FROM_PROPERTIES",
                "Failure Strategy": "Route to Failure",
                "acks": "all",
                "compression.type": "none",
                "FlowFile Attribute Header Pattern": "dlq\\..*",
            },
            # `success` is fully delivered, so it auto-terminates. `failure`
            # is deliberately NOT auto-terminated and NOT sent anywhere new —
            # per compiler-spec §6 ("the FlowFile stays queued (connection
            # backpressure) — matches 'park in own queue'") it self-loops
            # back onto dlq__publish's own input below, so a broker outage
            # backs the self-loop connection up (and, via backpressure, the
            # whole block) instead of silently dropping DLQ records.
            autoTerminate=["success"],
        )
    )
    builder.link("dlq__publish", "dlq__publish", ["failure"])
