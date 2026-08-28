"""Tests for backend/models/adapter — the Pydantic domain models that mirror
frontend/src/prototype/types.ts 1:1.

Covers:
  1. A realistic Flow JSON (modeled on the "flow-rapid7" seed in
     frontend/src/prototype/seeds.ts) parses and round-trips with camelCase
     keys preserved and unknown keys retained.
  2. AppService.redact() masks each secret key type and emits has* flags.
  3. PlatformConnection.redact() does the same.
  4. Enum (Literal) fields reject bad values.
"""

import copy
from pathlib import Path
import sys

import pytest
from pydantic import ValidationError

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from models.adapter import (  # noqa: E402
    AppService,
    Flow,
    FlowBlock,
    PlatformConnection,
)
from models.adapter._secrets import SECRET_CONFIG_KEYS  # noqa: E402

# --------------------------------------------------------------------------
# A realistic Flow, modeled directly on seeds.ts's "flow-rapid7":
#   simple source (http read) -> governed destination (kafka_kc sink),
#   Running, one materialized+sealed topic, one non-secret flow variable,
#   servicePins, deployedAt/lastRunAt/createdAt/updatedAt all set.
# Plus a couple of keys the current Python models don't know about, to prove
# `extra="allow"` really does keep unknown keys alive through a round trip.
# --------------------------------------------------------------------------
RAPID7_FLOW_JSON = {
    "id": "flow-rapid7",
    "name": "Rapid7 Assets",
    "description": "Nightly asset inventory from Rapid7 InsightVM into the bronze lakehouse.",
    "state": "Running",
    "enabled": True,
    "cron": "0 */6 * * *",
    "blocks": [
        {
            "id": "b-r7-list",
            "adapter": "http",
            "mode": "read",
            "name": "List Assets",
            "parentId": None,
            "serviceId": "svc-rapid7",
            "config": {
                "method": "GET",
                "path": "/api/3/assets",
                "responseFormat": "json",
                "recordPath": "$.resources[*]",
                "split": True,
                "pagination": {
                    "type": "page",
                    "fields": {
                        "pageParam": "page",
                        "sizeParam": "size",
                        "sizeValue": "500",
                        "firstPage": "1",
                        "stop": "empty_response",
                    },
                },
            },
            "transforms": [
                {"id": "t-r7-1", "kind": "extract", "config": {"attribute": "site_id", "path": "$.siteId", "default": ""}},
                {"id": "t-r7-2", "kind": "remove_field", "config": {"field": "links"}},
            ],
            "testResult": {
                "ok": True,
                "records": [
                    {"id": 1204, "hostName": "srv-dc01.corp.local", "os": "Windows Server 2022", "riskScore": 7211, "siteId": 3},
                    {"id": 1205, "hostName": "srv-web02.dmz.corp", "os": "Ubuntu 22.04", "riskScore": 18342, "siteId": 3},
                ],
                "detectedFields": ["id", "hostName", "os", "riskScore", "siteId"],
                "testedAt": "2026-08-11T04:00:00.000Z",
            },
            # Not a field any current model knows about — must survive anyway.
            "futureBlockField": {"nested": True, "n": 7},
        },
        {
            "id": "b-r7-sink",
            "adapter": "kafka_kc",
            "name": "Assets to Iceberg",
            "parentId": "b-r7-list",
            "serviceId": "svc-iceberg",
            "entity": "asset",
            "config": {
                "sinkServiceId": "svc-iceberg",
                "sinkConfig": {
                    "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
                    "tasks.max": "2",
                    "iceberg.catalog.type": "rest",
                    "iceberg.control.commit.interval-ms": "60000",
                    "consumer.override.max.poll.records": "2000",
                },
            },
            "transforms": [],
        },
    ],
    "topics": [
        {"id": "t-flow-r7", "kind": "materialized", "name": "raw.rapid7_assets.asset", "sealed": True, "writerBlockId": "b-r7-sink"},
    ],
    "variables": [{"name": "rapid7_region", "value": "us2", "secret": False}],
    "servicePins": {"svc-rapid7": 1, "svc-iceberg": 1},
    "deployedAt": "2026-07-31T12:00:00.000Z",
    "lastRunAt": "2026-08-13T00:00:00.000Z",
    "createdAt": "2026-07-14T12:00:00.000Z",
    "updatedAt": "2026-08-13T00:00:00.000Z",
    # Not a field any current model knows about — must survive anyway.
    "futureFrontendField": "some-value-the-backend-does-not-model-yet",
}


def _assert_subset_preserved(original, dumped):
    """Recursively assert every key/value in `original` is present, unchanged,
    in `dumped` (which may legitimately carry additional default-valued keys
    the original omitted)."""
    if isinstance(original, dict):
        assert isinstance(dumped, dict)
        for key, value in original.items():
            assert key in dumped, f"key {key!r} missing from round-tripped output"
            _assert_subset_preserved(value, dumped[key])
    elif isinstance(original, list):
        assert isinstance(dumped, list)
        assert len(original) == len(dumped)
        for o_item, d_item in zip(original, dumped):
            _assert_subset_preserved(o_item, d_item)
    else:
        assert original == dumped


class TestFlowRoundTrip:
    def test_parses_realistic_seed_modeled_flow(self):
        flow = Flow.model_validate(RAPID7_FLOW_JSON)
        assert flow.id == "flow-rapid7"
        assert flow.state == "Running"
        assert flow.blocks[0].adapter == "http"
        assert flow.blocks[0].mode == "read"
        assert flow.blocks[1].adapter == "kafka_kc"
        assert flow.topics[0].sealed is True
        assert flow.servicePins == {"svc-rapid7": 1, "svc-iceberg": 1}
        assert flow.variables[0].secret is False

    def test_round_trip_preserves_camelcase_keys(self):
        flow = Flow.model_validate(RAPID7_FLOW_JSON)
        dumped = flow.model_dump()
        # Spot-check literal camelCase keys survive (not snake_case).
        for key in (
            "servicePins",
            "deployedAt",
            "lastRunAt",
            "createdAt",
            "updatedAt",
        ):
            assert key in dumped
        for key in ("parentId", "serviceId", "testResult", "topicOverride"):
            assert key in dumped["blocks"][0]
        for key in ("writerBlockId",):
            assert key in dumped["topics"][0]
        _assert_subset_preserved(RAPID7_FLOW_JSON, dumped)

    def test_round_trip_retains_unknown_keys(self):
        flow = Flow.model_validate(RAPID7_FLOW_JSON)
        dumped = flow.model_dump()
        assert dumped["futureFrontendField"] == "some-value-the-backend-does-not-model-yet"
        assert dumped["blocks"][0]["futureBlockField"] == {"nested": True, "n": 7}

    def test_server_only_additive_fields_default_none_and_do_not_break_parse(self):
        flow = Flow.model_validate(RAPID7_FLOW_JSON)
        assert flow.runtimeScopeMap is None
        assert flow.nifiProcessGroupId is None
        assert flow.provenance is None
        # And can be set / round-tripped like any other field.
        flow2 = Flow.model_validate({**RAPID7_FLOW_JSON, "nifiProcessGroupId": "pg-123"})
        assert flow2.nifiProcessGroupId == "pg-123"

    def test_partial_doc_from_mongo_parses_with_defaults(self):
        # A bare-minimum document — only the Mongo-required id — must still parse.
        flow = Flow.model_validate({"id": "flow-x"})
        assert flow.name == ""
        assert flow.blocks == []
        assert flow.servicePins == {}
        assert flow.state == "Draft"


class TestAppServiceRedact:
    SERVICE_TYPE_FOR_KEY = {
        "password": "database",
        "token": "http",
        "clientSecret": "http",
        "keyValue": "http",
        "saslPassword": "external_kafka",
        "adminKey": "http",  # type doesn't matter for redact(); any is fine
        "s3AccessKey": "sink_destination",
        "s3SecretKey": "http",
        "oauthClientSecret": "sink_destination",
        "kafbatPassword": "http",  # connection-level key; type irrelevant for redact()
    }

    @pytest.mark.parametrize("secret_key", SECRET_CONFIG_KEYS)
    def test_redact_masks_each_known_secret_key_and_emits_has_flag(self, secret_key):
        svc = AppService(
            id="svc-1",
            type=self.SERVICE_TYPE_FOR_KEY[secret_key],
            name="Test Service",
            config={"baseUrl": "https://api.example.corp", secret_key: "super-secret-value"},
            hasSecret=True,
        )
        redacted = svc.redact()
        assert redacted["config"][secret_key] is None
        has_flag = "has" + secret_key[0].upper() + secret_key[1:]
        assert redacted[has_flag] is True
        # Non-secret config keys are untouched.
        assert redacted["config"]["baseUrl"] == "https://api.example.corp"
        # The model's own TS-shape hasSecret flag is left as-is.
        assert redacted["hasSecret"] is True
        # The original model is never mutated.
        assert svc.config[secret_key] == "super-secret-value"

    def test_redact_flags_absent_keys_false(self):
        svc = AppService(id="svc-2", type="http", name="No Secret", config={"baseUrl": "https://x"}, hasSecret=False)
        redacted = svc.redact()
        for key in SECRET_CONFIG_KEYS:
            has_flag = "has" + key[0].upper() + key[1:]
            assert redacted[has_flag] is False
        assert redacted["config"] == {"baseUrl": "https://x"}

    def test_redact_masks_multiple_secrets_in_one_config(self):
        # Not realistic for one auth mode, but exercises multiple keys at once.
        svc = AppService(
            id="svc-3",
            type="http",
            name="Multi",
            config={"password": "p", "token": "t", "clientSecret": "c", "keyValue": "k"},
        )
        redacted = svc.redact()
        for key in ("password", "token", "clientSecret", "keyValue"):
            assert redacted["config"][key] is None
            assert redacted["has" + key[0].upper() + key[1:]] is True

    def test_revision_field_matches_types_ts_not_rev(self):
        # types.ts's AppService names the counter `revision`, not `rev`.
        svc = AppService(id="svc-4", type="http", name="X", revision=3)
        assert svc.revision == 3
        assert "revision" in svc.model_dump()


class TestPlatformConnectionRedact:
    @pytest.mark.parametrize("secret_key", SECRET_CONFIG_KEYS)
    def test_redact_masks_each_known_secret_key_and_emits_has_flag(self, secret_key):
        conn = PlatformConnection(
            id="conn-1",
            type="nifi",
            name="Production NiFi",
            config={"url": "https://nifi.internal.corp", secret_key: "super-secret-value"},
            hasSecret=True,
        )
        redacted = conn.redact()
        assert redacted["config"][secret_key] is None
        has_flag = "has" + secret_key[0].upper() + secret_key[1:]
        assert redacted[has_flag] is True
        assert redacted["config"]["url"] == "https://nifi.internal.corp"
        assert redacted["hasSecret"] is True
        assert conn.config[secret_key] == "super-secret-value"

    def test_redact_apisix_admin_key(self):
        conn = PlatformConnection(
            id="conn-2",
            type="apisix",
            name="APISIX Gateway",
            config={"adminUrl": "https://apisix-admin.internal.corp", "runtimeUrl": "https://apisix.internal.corp", "adminKey": "topsecret"},
            hasSecret=True,
        )
        redacted = conn.redact()
        assert redacted["config"]["adminKey"] is None
        assert redacted["hasAdminKey"] is True
        assert redacted["config"]["adminUrl"] == "https://apisix-admin.internal.corp"

    def test_redact_does_not_mutate_original(self):
        original_config = {"host": "redis.internal.corp", "port": 6379, "password": "swordfish"}
        conn = PlatformConnection(id="conn-3", type="redis", name="Redis", config=copy.deepcopy(original_config))
        conn.redact()
        assert conn.config == original_config


class TestLiteralValidation:
    def test_flow_block_rejects_bad_adapter(self):
        with pytest.raises(ValidationError):
            FlowBlock(id="b-1", adapter="ftp", name="Bad Adapter")

    def test_flow_rejects_bad_state(self):
        with pytest.raises(ValidationError):
            Flow(id="flow-bad", state="Zombie")

    def test_app_service_rejects_bad_type(self):
        with pytest.raises(ValidationError):
            AppService(id="svc-bad", type="ftp", name="Bad")

    def test_platform_connection_rejects_bad_type(self):
        with pytest.raises(ValidationError):
            PlatformConnection(id="conn-bad", type="ftp", name="Bad")

    def test_flow_block_rejects_bad_branch_op(self):
        with pytest.raises(ValidationError):
            FlowBlock(
                id="b-2",
                adapter="http",
                name="Branch",
                branch={"name": "branch-1", "rules": [{"field": "x", "op": "not_a_real_op", "value": "y"}]},
            )
