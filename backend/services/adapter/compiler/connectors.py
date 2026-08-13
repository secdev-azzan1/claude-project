"""Kafka Connect connector configs — compiler-spec.md §5.

Two callers:
  - `build_kafka_kc_connector()` — the sink connector for a `kafka_kc` block's
    OWN governed topic (Iceberg or OpenSearch, chosen by the sink service's
    `kind`), value converter always Avro + Apicurio ccompat (the topic is
    always Avro-encoded — kafka_kc publishes through AvroRecordSetWriter).
  - `build_kc_connector()` — the sink connector for a `kc` block, consuming
    whatever topic it is attached to. Value converter is Avro+registry when
    the attached topic was written by a kafka_kc block (governed, schema'd),
    JSON otherwise (plain kafka-write topics are schemaless).

Connector naming: `<flowToken>.<blockId>.<kafka_kc|kc>` (live-evidence
convention, compiler-spec §2).

Locked keys (never taken from the user's `sinkConfig`, always platform-set):
`topics`, `key.converter`, `value.converter*`, and — for Iceberg —
every `iceberg.tables*` key (compiler-spec §5 "iceberg.tables* for
lakehouse"). Everything else in `sinkConfig` passes through, letting a user
tune `tasks.max`, batch sizes, etc. (see frontend seed `b-r7-sink`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict

from models.adapter import FlowBlock

from .ir import CompileError, ConnectorSpec, apicurio_ccompat_url

if TYPE_CHECKING:  # pragma: no cover
    from models.adapter import Flow
    from .ir import CompileContext

_LOCKED_PREFIXES = ("value.converter", "key.converter", "iceberg.tables")
_LOCKED_EXACT = {"topics"}


def _merge_locked(base: Dict[str, str], user: Dict[str, Any]) -> Dict[str, str]:
    merged: Dict[str, str] = {str(k): str(v) for k, v in user.items()}
    for k, v in base.items():
        merged[k] = v
    for k in list(merged.keys()):
        if k in _LOCKED_EXACT or any(k.startswith(p) for p in _LOCKED_PREFIXES):
            if k not in base:
                del merged[k]  # a locked-family key the platform didn't set for this connector: drop the stale user value
    return merged


def _sink_service(ctx: "CompileContext", block: FlowBlock):
    sink_service_id = block.config.get("sinkServiceId") or block.serviceId
    if not sink_service_id:
        raise CompileError(f"kafka_kc block {block.id!r} has no sink destination service configured")
    svc = ctx.services.get(sink_service_id)
    if svc is None:
        raise CompileError(f"kafka_kc block {block.id!r} references unknown service {sink_service_id!r}")
    return svc


def build_kafka_kc_connector(
    *, flow: "Flow", block: FlowBlock, ctx: "CompileContext", flow_token: str, topic: str, entity_token: str
) -> ConnectorSpec:
    svc = _sink_service(ctx, block)
    kind = svc.config.get("kind", "iceberg_catalog")
    ccompat = apicurio_ccompat_url(ctx.connection_config("apicurio").get("url", ""))
    user_sink = block.config.get("sinkConfig") or {}

    if kind == "iceberg_catalog":
        base: Dict[str, str] = {
            "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
            "topics": topic,
            "tasks.max": "1",
            "iceberg.tables": f"bronze.{entity_token}",
            "iceberg.tables.auto-create-enabled": "true",
            "iceberg.catalog.uri": str(svc.config.get("catalogUrl", "")),
            "iceberg.catalog.warehouse": str(svc.config.get("warehouse", "bronze")),
            "iceberg.control.commit.interval-ms": "60000",
            "value.converter": "io.apicurio.registry.utils.converter.AvroConverter",
            "value.converter.apicurio.registry.url": ccompat,
            "value.converter.apicurio.registry.as-confluent": "true",
            "value.converter.apicurio.registry.find-latest": "true",
            "value.converter.schemas.enable": "true",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            # DLQ config OFF — Connect keeps its own error path (spec §5).
        }
    elif kind == "opensearch":
        base = {
            "connector.class": "io.aiven.kafka.connect.opensearch.OpensearchSinkConnector",
            "topics": topic,
            "tasks.max": "1",
            "connection.url": str(svc.config.get("url", "")),
            "type.name": "_doc",
            "key.ignore": "false" if svc.config.get("writeMode") == "upsert" else "true",
            "schema.ignore": "true",
            "value.converter": "io.apicurio.registry.utils.converter.AvroConverter",
            "value.converter.apicurio.registry.url": ccompat,
            "value.converter.apicurio.registry.as-confluent": "true",
            "value.converter.apicurio.registry.find-latest": "true",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        }
        prefix = svc.config.get("indexPrefix")
        if prefix:
            base["topic.index.map"] = f"{topic}:{prefix}{entity_token}"
    else:
        raise CompileError(f"Unknown sink kind {kind!r} for service {svc.id!r}")

    config = _merge_locked(base, user_sink)
    name = f"{flow_token}.{block.id}.kafka_kc"
    return ConnectorSpec(name=name, config=config, ownerBlockId=block.id)


def build_kc_connector(
    *, flow: "Flow", block: FlowBlock, ctx: "CompileContext", flow_token: str, topic: str, entity_token: str,
    topic_is_governed: bool,
) -> ConnectorSpec:
    if not block.serviceId:
        raise CompileError(f"kc block {block.id!r} has no sink destination service configured")
    svc = ctx.services.get(block.serviceId)
    if svc is None:
        raise CompileError(f"kc block {block.id!r} references unknown service {block.serviceId!r}")
    kind = svc.config.get("kind", "opensearch")
    ccompat = apicurio_ccompat_url(ctx.connection_config("apicurio").get("url", ""))
    user_sink = block.config.get("sinkConfig") or {}

    if topic_is_governed:
        value_converter: Dict[str, str] = {
            "value.converter": "io.apicurio.registry.utils.converter.AvroConverter",
            "value.converter.apicurio.registry.url": ccompat,
            "value.converter.apicurio.registry.as-confluent": "true",
            "value.converter.apicurio.registry.find-latest": "true",
        }
    else:
        value_converter = {"value.converter": "org.apache.kafka.connect.json.JsonConverter",
                            "value.converter.schemas.enable": "false"}

    if kind == "iceberg_catalog":
        base = {
            "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
            "topics": topic,
            "tasks.max": "1",
            "iceberg.tables": f"bronze.{entity_token}",
            "iceberg.tables.auto-create-enabled": "true",
            "iceberg.catalog.uri": str(svc.config.get("catalogUrl", "")),
            "iceberg.catalog.warehouse": str(svc.config.get("warehouse", "bronze")),
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            **value_converter,
        }
    elif kind == "opensearch":
        base = {
            "connector.class": "io.aiven.kafka.connect.opensearch.OpensearchSinkConnector",
            "topics": topic,
            "tasks.max": "1",
            "connection.url": str(svc.config.get("url", "")),
            "key.ignore": "false" if svc.config.get("writeMode") == "upsert" else "true",
            "schema.ignore": "true" if not topic_is_governed else "false",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            **value_converter,
        }
    else:
        raise CompileError(f"Unknown sink kind {kind!r} for service {svc.id!r}")

    config = _merge_locked(base, user_sink)
    name = f"{flow_token}.{block.id}.kc"
    return ConnectorSpec(name=name, config=config, ownerBlockId=block.id)
