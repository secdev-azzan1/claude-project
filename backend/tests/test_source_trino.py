import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from models.source import SourceCreate


def test_source_create_accepts_trino_source_config():
    source = SourceCreate(
        name="Asset history snapshots",
        type="trino",
        trino_endpoint="https://trino.example.com",
        trino_user="admin",
        trino_catalog="bronze",
        trino_schema="rapid7_securado",
        trino_table="asset__history",
        trino_checkpoint_table="bronze.flow_checkpoint.trino_snapshots",
        trino_columns=["id", "object_id", "ip", "hostname"],
        streams=[
            {
                "id": "asset__history",
                "name": "asset__history",
                "entity": {"kafka": {"topic": "bronze.asset_history.snapshot_rows"}},
            }
        ],
        kafka_output={"topic": "bronze.asset_history.snapshot_rows"},
    )

    assert source.type == "Trino"
    assert source.trino_endpoint == "https://trino.example.com"
    assert source.trino_columns == ["id", "object_id", "ip", "hostname"]


def test_source_create_accepts_split_trino_checkpoint_table_config():
    source = SourceCreate(
        name="FortiSIEM device snapshots",
        type="Trino",
        trino_endpoint="https://trino.example.com",
        trino_user="admin",
        trino_catalog="bronze",
        trino_schema="fortisiem",
        trino_table="device__history",
        trino_checkpoint_catalog="bronze",
        trino_checkpoint_schema="checkpoints",
        trino_checkpoint_table_name="trino_snapshot_checkpoints",
        trino_auto_create_checkpoint_table=True,
        trino_columns=["accessip", "object_id", "name", "naturalid"],
        streams=[
            {
                "id": "device__history",
                "name": "device__history",
                "entity": {"kafka": {"topic": "bronze.fortisiem.device_history.rows"}},
            }
        ],
        kafka_output={"topic": "bronze.fortisiem.device_history.rows"},
    )

    assert source.type == "Trino"
    assert source.trino_checkpoint_catalog == "bronze"
    assert source.trino_checkpoint_schema == "checkpoints"
    assert source.trino_checkpoint_table_name == "trino_snapshot_checkpoints"
    assert source.trino_auto_create_checkpoint_table is True


def test_source_create_accepts_trino_all_column_mode_without_manual_columns():
    source = SourceCreate(
        name="FortiSIEM device all columns",
        type="Trino",
        trino_endpoint="https://trino.example.com",
        trino_user="admin",
        trino_catalog="bronze",
        trino_schema="fortisiem",
        trino_table="device__history",
        trino_checkpoint_catalog="bronze",
        trino_checkpoint_schema="checkpoints",
        trino_checkpoint_table_name="trino_snapshot_checkpoints",
        trino_column_mode="all",
        trino_columns=[],
        streams=[
            {
                "id": "device__history",
                "name": "device__history",
                "entity": {"kafka": {"topic": "bronze.fortisiem.device_history.rows"}},
            }
        ],
        kafka_output={"topic": "bronze.fortisiem.device_history.rows"},
    )

    assert source.type == "Trino"
    assert source.trino_column_mode == "all"
    assert source.trino_columns == []


def test_source_create_requires_trino_table_details():
    with pytest.raises(ValidationError) as exc:
        SourceCreate(
            name="Bad Trino",
            type="Trino",
            trino_endpoint="https://trino.example.com",
            trino_catalog="bronze",
            trino_schema="rapid7_securado",
            trino_columns=["id"],
        )

    assert "trino_table is required for Trino sources" in str(exc.value)
