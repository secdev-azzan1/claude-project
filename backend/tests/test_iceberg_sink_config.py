import json
from pathlib import Path
import sys

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from services import iceberg_sink_config as isc

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def _load_fixture(name: str) -> dict:
    with open(FIXTURES_DIR / name, "r", encoding="utf-8") as f:
        return json.load(f)


def _assert_configs_equal(actual: dict, expected: dict):
    if actual != expected:
        only_actual = {k: v for k, v in actual.items() if expected.get(k) != v}
        only_expected = {k: v for k, v in expected.items() if actual.get(k) != v}
        print("Keys/values only in actual:", only_actual)
        print("Keys/values only in expected:", only_expected)
    assert actual == expected


ICEBERG_CONN = {
    "endpoint": "http://polaris:8181/api/catalog",
    "iceberg_warehouse": "bronze",
    "iceberg_credential": "root:s3cr3t",
    "s3_endpoint": "http://s3g:9878",
    "s3_access_key_id": "admin",
    "s3_secret_access_key": "password",
    "s3_region": "us-east-1",
}

APICURIO_CONN = {
    "endpoint": "http://apicurio:8080",
}


def test_golden_sentinelone_config_matches_fixture():
    expected = _load_fixture("iceberg_connector_bronze.sentinelone.agent__history.json")
    topic = "bronze.sentinelone.agent__history"
    table_name = isc.derive_table_name(topic)

    actual = isc.build_connector_config(
        topic=topic,
        table_name=table_name,
        apicurio_conn=APICURIO_CONN,
        iceberg_conn=ICEBERG_CONN,
    )

    _assert_configs_equal(actual, expected)


def test_golden_fileshare_config_matches_fixture():
    expected = _load_fixture("iceberg_connector_bronze.fileshare.asset__history.json")
    topic = "bronze.fileshare.asset__history"
    table_name = isc.derive_table_name(topic)

    actual = isc.build_connector_config(
        topic=topic,
        table_name=table_name,
        apicurio_conn=APICURIO_CONN,
        iceberg_conn=ICEBERG_CONN,
    )

    _assert_configs_equal(actual, expected)


def test_derive_table_name():
    assert isc.derive_table_name("bronze.sentinelone.agent__history") == "sentinelone.agent"
    assert isc.derive_table_name("fileshare.asset__history") == "fileshare.asset"
    assert isc.derive_table_name_from_names("DummyJson Flow", "User", "") == "dummyjson_flow.user"

    with pytest.raises(ValueError):
        isc.derive_table_name("")

    with pytest.raises(ValueError):
        isc.derive_table_name("bronze.onlyonepart")

    with pytest.raises(ValueError):
        isc.derive_table_name("bronze.too.many.dots")

    with pytest.raises(ValueError):
        isc.derive_table_name("bronze.")


def test_derive_connector_name():
    assert isc.derive_connector_name("bronze.sentinelone.agent__history") == (
        "bronze.sentinelone.agent__history__iceberg"
    )

    with pytest.raises(ValueError):
        isc.derive_connector_name("")


def test_platform_owned_defaults_are_fixed():
    topic = "bronze.sentinelone.agent__history"
    table_name = isc.derive_table_name(topic)

    actual = isc.build_connector_config(
        topic=topic,
        table_name=table_name,
        apicurio_conn=APICURIO_CONN,
        iceberg_conn=ICEBERG_CONN,
    )

    assert actual["iceberg.control.commit.interval-ms"] == "60000"
    assert "iceberg.control.commit.threads" not in actual
    assert actual["errors.tolerance"] == "all"
    assert actual["errors.deadletterqueue.topic.replication.factor"] == "1"


def test_build_connector_config_uses_platform_owned_defaults():
    topic = "bronze.sentinelone.agent__history"
    table_name = isc.derive_table_name(topic)

    actual = isc.build_connector_config(
        topic=topic,
        table_name=table_name,
        apicurio_conn=APICURIO_CONN,
        iceberg_conn=ICEBERG_CONN,
    )
    assert actual["iceberg.tables"] == "sentinelone.agent"
    assert actual["iceberg.control.commit.interval-ms"] == "60000"


def test_redact_masks_secrets_and_does_not_mutate_input():
    config = {
        "iceberg.catalog.credential": "root:s3cr3t",
        "iceberg.catalog.s3.secret-access-key": "password",
        "iceberg.catalog.s3.access-key-id": "admin",
        "topics": "bronze.sentinelone.agent__history",
    }
    original = dict(config)

    redacted = isc.redact_connector_config(config)

    assert redacted["iceberg.catalog.credential"] == "***REDACTED***"
    assert redacted["iceberg.catalog.s3.secret-access-key"] == "***REDACTED***"
    assert redacted["iceberg.catalog.s3.access-key-id"] == "***REDACTED***"
    assert redacted["topics"] == "bronze.sentinelone.agent__history"

    # Input must not be mutated.
    assert config == original
