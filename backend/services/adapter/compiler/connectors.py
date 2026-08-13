"""Kafka Connect connector configs — compiler-spec.md §5.

Two callers:
  - `build_kafka_kc_connector()` — the sink connector for a `kafka_kc` block's
    OWN governed topic (Iceberg or OpenSearch, chosen by the sink service's
    `kind`), value converter always Avro + Apicurio (the topic is always
    Avro-encoded — kafka_kc publishes through AvroRecordSetWriter).
  - `build_kc_connector()` — the sink connector for a `kc` block, consuming
    whatever topic it is attached to. Value converter is Avro+registry when
    the attached topic was written by a kafka_kc block (governed, schema'd),
    JSON otherwise (plain kafka-write topics are schemaless).

Connector naming: `<flowToken>.<blockId>.<kafka_kc|kc>` (live-evidence
convention, compiler-spec §2).

Iceberg sinks (E2, proven live in Journey A + mirrored from the alpha
`services/iceberg_sink_config.py` production generator): the REST-catalog
config must carry `iceberg.catalog.type=rest` (otherwise Iceberg defaults to
HiveCatalog and the task dies with `ClassNotFoundException:
...hive.HiveCatalog`), the OAuth2 client credentials
(`iceberg.catalog.credential=<oauthClientId>:<oauthClientSecret>` plus
`oauth2-server-uri`/`scope`/`rest.auth.type`), and the S3FileIO settings
(`io-impl`, endpoint, access/secret keys, path-style, region) — all read
from the bound sink service's config. `initialPosition` on the block maps to
`consumer.override.auto.offset.reset`.

Value converter (E2b, proven live): the Apicurio `AvroConverter` (registry
3.x resolver) fetches by content id against the NATIVE registry API —
`value.converter.apicurio.registry.url` MUST be `<root>/apis/registry/v3`
(`{ccompat}/ids/contentIds/<id>` 404s, `{v3}/...` 200s). The NiFi-side
ConfluentSchemaRegistry controller service keeps the ccompat URL
(blocks_kafka_kc.py) — only Connect converters use the native URL.

Locked keys (never taken from the user's `sinkConfig`, always platform-set):
`topics`, `key.converter`, `value.converter*`, and — for Iceberg —
every `iceberg.tables*` key (compiler-spec §5 "iceberg.tables* for
lakehouse"). Everything else in `sinkConfig` passes through, letting a user
tune `tasks.max`, batch sizes, etc. (see frontend seed `b-r7-sink`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict

from models.adapter import FlowBlock

from .ir import CompileError, ConnectorSpec, apicurio_registry_v3_url

if TYPE_CHECKING:  # pragma: no cover
    from models.adapter import AppService, Flow
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
    # Accept the sink service reference from EITHER field (journey finding):
    # `block.serviceId` is what the frontend/validation require on kafka_kc,
    # `config.sinkServiceId` is the legacy/config-level spelling. Prefer
    # serviceId; fall back to sinkServiceId.
    sink_service_id = block.serviceId or block.config.get("sinkServiceId")
    if not sink_service_id:
        raise CompileError(f"kafka_kc block {block.id!r} has no sink destination service configured")
    svc = ctx.services.get(sink_service_id)
    if svc is None:
        raise CompileError(f"kafka_kc block {block.id!r} references unknown service {sink_service_id!r}")
    return svc


def _offset_reset(block: FlowBlock) -> str:
    """`initialPosition` -> Kafka consumer `auto.offset.reset` (same mapping
    blocks_kafka.py uses for ConsumeKafka: "new" -> latest, else earliest)."""
    return "latest" if str(block.config.get("initialPosition", "beginning")) == "new" else "earliest"


def _avro_converter_props(ctx: "CompileContext") -> Dict[str, str]:
    """Apicurio AvroConverter config for governed (Avro) topics — E2b: the
    registry URL is the NATIVE v3 API, never ccompat (see module docstring).
    `use-id=contentId`/`auto-register=false`/`as-confluent=true` mirror the
    alpha production generator's proven set."""
    registry_v3 = apicurio_registry_v3_url(ctx.connection_config("apicurio").get("url", ""))
    return {
        "value.converter": "io.apicurio.registry.utils.converter.AvroConverter",
        "value.converter.apicurio.registry.url": registry_v3,
        "value.converter.apicurio.registry.as-confluent": "true",
        "value.converter.apicurio.registry.find-latest": "true",
        "value.converter.apicurio.registry.use-id": "contentId",
        "value.converter.apicurio.registry.auto-register": "false",
        "value.converter.schemas.enable": "true",
    }


def _iceberg_catalog_props(svc: "AppService") -> Dict[str, str]:
    """The REST-catalog + S3FileIO property set, mirrored from the alpha
    `services/iceberg_sink_config.py::build_connector_config` (the proven
    production generator) and fed from the sink service's own config
    (`catalogUrl`/`warehouse`/`oauthClientId`/`oauthClientSecret`/
    `s3Endpoint`/`s3AccessKey`/`s3SecretKey`/`s3Region`/`s3PathStyle`)."""
    cfg = svc.config or {}
    catalog_url = str(cfg.get("catalogUrl", "")).rstrip("/")
    s3_region = str(cfg.get("s3Region") or "us-east-1")
    props: Dict[str, str] = {
        "iceberg.catalog.type": "rest",
        "iceberg.catalog.uri": catalog_url,
        "iceberg.catalog.warehouse": str(cfg.get("warehouse", "bronze")),
        "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "iceberg.catalog.client.region": s3_region,
        "iceberg.catalog.s3.region": s3_region,
        "iceberg.catalog.s3.path-style-access": "false" if cfg.get("s3PathStyle") is False else "true",
    }
    client_id = cfg.get("oauthClientId")
    client_secret = cfg.get("oauthClientSecret")
    if client_id and client_secret:
        props["iceberg.catalog.rest.auth.type"] = "oauth2"
        props["iceberg.catalog.credential"] = f"{client_id}:{client_secret}"
        props["iceberg.catalog.scope"] = str(cfg.get("oauthScope") or "PRINCIPAL_ROLE:ALL")
        # Same default the alpha generator (and the live Polaris catalog) use.
        props["iceberg.catalog.oauth2-server-uri"] = f"{catalog_url}/v1/oauth/tokens"
        props["iceberg.catalog.token-refresh-enabled"] = "true"
    if cfg.get("s3Endpoint"):
        props["iceberg.catalog.s3.endpoint"] = str(cfg.get("s3Endpoint"))
    if cfg.get("s3AccessKey"):
        props["iceberg.catalog.s3.access-key-id"] = str(cfg.get("s3AccessKey"))
    if cfg.get("s3SecretKey"):
        props["iceberg.catalog.s3.secret-access-key"] = str(cfg.get("s3SecretKey"))
    return props


def _opensearch_credential_props(svc: "AppService") -> Dict[str, str]:
    """OpenSearch basic-auth credentials, when the service has them (E2)."""
    cfg = svc.config or {}
    if not cfg.get("username"):
        return {}
    return {
        "connection.username": str(cfg.get("username")),
        "connection.password": str(cfg.get("password") or ""),
    }


def build_kafka_kc_connector(
    *, flow: "Flow", block: FlowBlock, ctx: "CompileContext", flow_token: str, topic: str, entity_token: str
) -> ConnectorSpec:
    svc = _sink_service(ctx, block)
    kind = svc.config.get("kind", "iceberg_catalog")
    user_sink = block.config.get("sinkConfig") or {}

    if kind == "iceberg_catalog":
        base: Dict[str, str] = {
            "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
            "topics": topic,
            "tasks.max": "1",
            "consumer.override.auto.offset.reset": _offset_reset(block),
            "iceberg.tables": f"bronze.{entity_token}",
            "iceberg.tables.auto-create-enabled": "true",
            **_iceberg_catalog_props(svc),
            "iceberg.control.commit.interval-ms": "60000",
            **_avro_converter_props(ctx),
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            # DLQ config OFF — Connect keeps its own error path (spec §5).
        }
    elif kind == "opensearch":
        base = {
            "connector.class": "io.aiven.kafka.connect.opensearch.OpensearchSinkConnector",
            "topics": topic,
            "tasks.max": "1",
            "consumer.override.auto.offset.reset": _offset_reset(block),
            "connection.url": str(svc.config.get("url", "")),
            **_opensearch_credential_props(svc),
            "type.name": "_doc",
            "key.ignore": "false" if svc.config.get("writeMode") == "upsert" else "true",
            "schema.ignore": "true",
            **_avro_converter_props(ctx),
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
    user_sink = block.config.get("sinkConfig") or {}

    if topic_is_governed:
        value_converter = _avro_converter_props(ctx)
    else:
        value_converter = {"value.converter": "org.apache.kafka.connect.json.JsonConverter",
                            "value.converter.schemas.enable": "false"}

    if kind == "iceberg_catalog":
        base = {
            "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
            "topics": topic,
            "tasks.max": "1",
            "consumer.override.auto.offset.reset": _offset_reset(block),
            "iceberg.tables": f"bronze.{entity_token}",
            "iceberg.tables.auto-create-enabled": "true",
            **_iceberg_catalog_props(svc),
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            **value_converter,
        }
    elif kind == "opensearch":
        base = {
            "connector.class": "io.aiven.kafka.connect.opensearch.OpensearchSinkConnector",
            "topics": topic,
            "tasks.max": "1",
            "consumer.override.auto.offset.reset": _offset_reset(block),
            "connection.url": str(svc.config.get("url", "")),
            **_opensearch_credential_props(svc),
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
