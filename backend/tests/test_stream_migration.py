import sys
from pathlib import Path


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services.stream_migration import migrate_source


def test_migrate_source_does_not_add_primary_entity_when_explicit_entity_exists():
    source = {
        "kafka_output": {
            "topic": "bronze.source.login__history",
            "schema_artifact_id": "bronze.source.login__history-value",
            "schema_version": 1,
        },
        "streams": [
            {
                "id": "login",
                "name": "login",
                "is_primary": True,
            },
            {
                "id": "orders",
                "name": "orders",
                "is_primary": False,
                "entity": {
                    "kafka": {
                        "topic": "bronze.source.orders__history",
                        "schema_artifact_id": "bronze.source.orders__history-value",
                        "schema_version": 1,
                    },
                    "schema_artifact_id": "bronze.source.orders__history-value",
                    "schema_version": 1,
                },
            },
        ],
    }

    migrated = migrate_source(source)
    streams = {stream["id"]: stream for stream in migrated["streams"]}

    assert "entity" not in streams["login"]
    assert streams["orders"]["entity"]["kafka"]["topic"] == "bronze.source.orders__history"


def test_migrate_source_adds_primary_entity_for_legacy_source_without_entities():
    source = {
        "kafka_output": {
            "topic": "bronze.legacy.users__history",
            "schema_artifact_id": "bronze.legacy.users__history-value",
            "schema_version": 1,
        },
        "streams": [
            {
                "id": "users",
                "name": "users",
                "is_primary": True,
            },
        ],
    }

    migrated = migrate_source(source)
    stream = migrated["streams"][0]

    assert stream["entity"]["kafka"]["topic"] == "bronze.legacy.users__history"
