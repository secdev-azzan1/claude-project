"""Pure-function generator for Iceberg Sink Kafka Connect connector configs.

No I/O, no Mongo, no HTTP calls — every function here is deterministic given its
inputs so the golden fixture tests can assert exact-dict equality against real
production connector configs.
"""
import re
from typing import Any, Dict

from services.apicurio_client import _normalize_apicurio_endpoints

ICEBERG_CONNECTOR_CLASS = "org.apache.iceberg.connect.IcebergSinkConnector"
TOPIC_PREFIX = "bronze."
DEFAULT_COMMIT_INTERVAL_MS = 60000
DEFAULT_ERRORS_TOLERANCE = "all"
DEFAULT_DLQ_REPLICATION_FACTOR = 1
DEFAULT_ICEBERG_CATALOG = "bronze"


def normalize_iceberg_identifier(value: str, fallback: str) -> str:
    """Normalize a display name into a stable Iceberg namespace/table token."""
    text = (value or "").strip().lower()
    token = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return token or fallback


def _strip_history_suffix(value: str) -> str:
    return value[:-len("__history")] if value.endswith("__history") else value


def derive_table_name(topic: str) -> str:
    """Derive a default "namespace.table" Iceberg name from a Kafka topic.

    This is a fallback for older sink rows. New sink sync uses the flow/source
    name as namespace and stream/entity name as table.
    """
    if not topic:
        raise ValueError("topic must not be empty")

    parts = topic.split(".")
    valid_bronze_topic = (
        topic.startswith(TOPIC_PREFIX)
        and len(parts) == 3
        and parts[0] == "bronze"
        and bool(parts[1])
        and bool(parts[2])
    )
    valid_two_part_table = (
        not topic.startswith(TOPIC_PREFIX)
        and len(parts) == 2
        and bool(parts[0])
        and bool(parts[1])
    )
    if not valid_bronze_topic and not valid_two_part_table:
        raise ValueError(
            f"topic '{topic}' does not derive to a valid Iceberg table name"
        )
    if valid_bronze_topic:
        return ".".join(
            (
                normalize_iceberg_identifier(parts[1], "source"),
                normalize_iceberg_identifier(_strip_history_suffix(parts[2]), "entity"),
            )
        )
    return ".".join(
        (
            normalize_iceberg_identifier(parts[0], "source"),
            normalize_iceberg_identifier(_strip_history_suffix(parts[1]), "entity"),
        )
    )


def derive_table_name_from_names(namespace_name: str, table_name: str, topic: str = "") -> str:
    """Derive the primary Iceberg table name from flow/source and entity names."""
    namespace = normalize_iceberg_identifier(namespace_name, "")
    table = normalize_iceberg_identifier(table_name, "")
    if namespace and table:
        return f"{namespace}.{table}"
    if topic:
        return derive_table_name(topic)
    raise ValueError("namespace and table names must not both be empty")


def derive_connector_name(topic: str) -> str:
    """Derive the Kafka Connect connector name for a topic."""
    if not topic:
        raise ValueError("topic must not be empty")
    return f"{topic}__iceberg"


def _derive_control_group_id_prefix(topic: str) -> str:
    token = re.sub(r"[^A-Za-z0-9]+", "-", topic).strip("-").lower()
    return f"cg-iceberg-{token or 'topic'}"


def build_connector_config(
    topic: str,
    table_name: str,
    apicurio_conn: Dict[str, Any],
    iceberg_conn: Dict[str, Any],
) -> Dict[str, str]:
    """Build the full Iceberg Sink Kafka Connect connector config.

    Every value in the returned dict is a str, matching the Kafka Connect REST
    API's config shape.
    """
    catalog_uri = (iceberg_conn.get("endpoint") or "").rstrip("/")
    warehouse = DEFAULT_ICEBERG_CATALOG
    oauth2_server_uri = (
        iceberg_conn.get("iceberg_oauth2_server_uri") or f"{catalog_uri}/v1/oauth/tokens"
    )
    scope = iceberg_conn.get("iceberg_scope") or "PRINCIPAL_ROLE:ALL"
    endpoint = _normalize_apicurio_endpoints(apicurio_conn.get("endpoint") or "")["root"]
    registry_url = f"{endpoint}/apis/registry/v3"
    s3_region = iceberg_conn.get("s3_region") or "us-east-1"
    s3_path_style_access = iceberg_conn.get("s3_path_style_access", True)

    config: Dict[str, str] = {
        "connector.class": ICEBERG_CONNECTOR_CLASS,
        "consumer.override.auto.offset.reset": "earliest",
        "tasks.max": "1",
        "topics": topic,
        "iceberg.tables": table_name,
        "iceberg.tables.auto-create-enabled": "true",
        "iceberg.tables.evolve-schema-enabled": "true",
        "iceberg.tables.schema-force-optional": "true",
        "iceberg.control.topic": "control-iceberg",
        "iceberg.control.group-id-prefix": _derive_control_group_id_prefix(topic),
        "iceberg.control.commit.interval-ms": str(DEFAULT_COMMIT_INTERVAL_MS),
        "iceberg.catalog": "polaris",
        "iceberg.catalog.type": "rest",
        "iceberg.catalog.uri": catalog_uri,
        "iceberg.catalog.warehouse": warehouse,
        "iceberg.catalog.rest.auth.type": "oauth2",
        "iceberg.catalog.scope": scope,
        "iceberg.catalog.oauth2-server-uri": oauth2_server_uri,
        "iceberg.catalog.token-refresh-enabled": "true",
        "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "iceberg.catalog.client.region": s3_region,
        "iceberg.catalog.s3.region": s3_region,
        "iceberg.catalog.s3.path-style-access": "true" if s3_path_style_access else "false",
        "value.converter": "io.apicurio.registry.utils.converter.AvroConverter",
        "value.converter.apicurio.registry.as-confluent": "true",
        "value.converter.apicurio.registry.auto-register": "false",
        "value.converter.apicurio.registry.find-latest": "true",
        "value.converter.apicurio.registry.url": registry_url,
        "value.converter.apicurio.registry.use-id": "contentId",
        "value.converter.schemas.enable": "true",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "errors.tolerance": DEFAULT_ERRORS_TOLERANCE,
        "errors.log.enable": "true",
        "errors.log.include.messages": "true",
        "errors.deadletterqueue.topic.name": f"dlq.{topic}.iceberg",
        "errors.deadletterqueue.context.headers.enable": "true",
        "errors.deadletterqueue.topic.replication.factor": str(DEFAULT_DLQ_REPLICATION_FACTOR),
    }

    credential = iceberg_conn.get("iceberg_credential")
    if credential:
        config["iceberg.catalog.credential"] = credential

    s3_endpoint = iceberg_conn.get("s3_endpoint")
    if s3_endpoint:
        config["iceberg.catalog.s3.endpoint"] = s3_endpoint

    s3_access_key_id = iceberg_conn.get("s3_access_key_id")
    if s3_access_key_id:
        config["iceberg.catalog.s3.access-key-id"] = s3_access_key_id

    s3_secret_access_key = iceberg_conn.get("s3_secret_access_key")
    if s3_secret_access_key:
        config["iceberg.catalog.s3.secret-access-key"] = s3_secret_access_key

    return config


_REDACTED = "***REDACTED***"
_EXPLICIT_SECRET_KEYS = {
    "iceberg.catalog.credential",
    "iceberg.catalog.s3.secret-access-key",
    "iceberg.catalog.s3.access-key-id",
}


def redact_connector_config(config: Dict[str, str]) -> Dict[str, str]:
    """Return a copy of config with sensitive values masked. Never mutates input."""
    redacted: Dict[str, str] = {}
    for key, value in config.items():
        lowered = key.lower()
        if (
            key in _EXPLICIT_SECRET_KEYS
            or any(
                token in lowered
                for token in ("credential", "secret", "password", "access-key", "access_key")
            )
        ):
            redacted[key] = _REDACTED
        else:
            redacted[key] = value
    return redacted
