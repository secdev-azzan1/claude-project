"""Tests for backend/services/adapter/compiler -- the T7.1 flow COMPILER
(`Flow -> DeploymentPlan`, pure, no network I/O).

Flow/service/connection fixtures are modeled on `frontend/src/prototype/
seeds.ts`'s `flow-rapid7` (http read -> kafka_kc, api_key auth, iceberg sink)
and `flow-fortisiem` (session_token auth), same convention
`test_adapter_rules.py` already uses for this domain.
"""

from pathlib import Path
import json
import sys

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from models.adapter import (  # noqa: E402
    AppService,
    ApprovedSchema,
    BranchCondition,
    BranchInfo,
    Flow,
    FlowBlock,
    PlatformConnection,
    TransformRule,
)
from services.adapter.compiler import CompileContext, CompileError, compile_flow  # noqa: E402

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "compiler"


# --------------------------------------------------------------------------
# Builders
# --------------------------------------------------------------------------


def make_service(**over) -> AppService:
    defaults = dict(id="svc", type="http", name="Svc", config={})
    defaults.update(over)
    return AppService(**defaults)


def make_connection(**over) -> PlatformConnection:
    defaults = dict(id="conn", type="kafka", name="Conn", config={})
    defaults.update(over)
    return PlatformConnection(**defaults)


def golden_flow() -> Flow:
    """http read (api_key auth, offset pagination) -> transforms(extract,
    add_field) -> kafka_kc (dedup last) -- modeled on seeds.ts's flow-rapid7,
    with an added `add_field` transform and a dedup on the sink so all four
    scoped transform families (extract/add_field/dedup + routing-free direct
    chain) show up in one golden fixture."""
    return Flow(
        id="flow-golden",
        name="Golden Flow",
        cron="0 */6 * * *",
        state="Draft",
        enabled=True,
        createdAt="2026-01-01T00:00:00.000Z",
        updatedAt="2026-01-01T00:00:00.000Z",
        blocks=[
            FlowBlock(
                id="b-read",
                adapter="http",
                mode="read",
                name="List Assets",
                parentId=None,
                serviceId="svc-http",
                config={
                    "method": "GET",
                    "path": "/api/3/assets",
                    "responseFormat": "json",
                    "recordPath": "$.resources[*]",
                    "split": True,
                    "pagination": {
                        "type": "offset",
                        "fields": {"offsetParam": "offset", "limitParam": "limit", "limitValue": "200", "stop": "empty_response"},
                    },
                },
                transforms=[
                    TransformRule(id="t-1", kind="extract", config={"attribute": "site_id", "path": "$.siteId", "default": ""}),
                    TransformRule(id="t-2", kind="add_field", config={"field": "source", "value": "rapid7"}),
                ],
            ),
            FlowBlock(
                id="b-sink",
                adapter="kafka_kc",
                name="Assets to Iceberg",
                parentId="b-read",
                serviceId="svc-iceberg",
                entity="asset",
                config={"sinkServiceId": "svc-iceberg"},
                transforms=[
                    TransformRule(id="t-3", kind="dedup", config={"identityFields": ["id"], "excludedFields": [], "windowHours": 24}),
                ],
            ),
        ],
        topics=[],
        variables=[],
        servicePins={"svc-http": 1, "svc-iceberg": 1},
    )


def golden_ctx() -> CompileContext:
    services = {
        "svc-http": make_service(
            id="svc-http", type="http", name="Rapid7 InsightVM API",
            config={"baseUrl": "https://insightvm.corp.local:3780", "authMode": "api_key",
                    "keyLocation": "header", "keyName": "X-Api-Key", "keyValue": "s3cr3t"},
            hasSecret=True,
        ),
        "svc-iceberg": make_service(
            id="svc-iceberg", type="sink_destination", name="Iceberg Bronze Catalog",
            config={"kind": "iceberg_catalog", "catalogUrl": "http://polaris.internal.corp:8181/api/catalog", "warehouse": "bronze"},
            hasSecret=False,
        ),
    }
    connections = {
        "kafka": make_connection(id="conn-kafka", type="kafka", name="Primary Kafka Cluster", config={"bootstrapServers": "kafka:9092"}),
        "apicurio": make_connection(id="conn-apicurio", type="apicurio", name="Apicurio Schema Registry", config={"url": "http://apicurio.internal.corp:8081"}),
        "redis": make_connection(id="conn-redis", type="redis", name="Dedup Redis", config={"host": "redis.internal.corp", "port": 6379, "dedupDb": 2, "password": "redispw"}),
    }
    schemas = {
        "b-sink": ApprovedSchema(
            id="schema-1", subject="raw.golden_flow.asset-value", entity="asset",
            flowId="flow-golden", blockId="b-sink", approvedAt="2026-01-01T00:00:00.000Z",
        ),
    }
    return CompileContext(services=services, connections=connections, gateway_proxies={}, approved_schemas=schemas)


def routing_flow() -> Flow:
    """One unconditional child + one any-match (2 rules) + one all-match (2
    rules) child, all off the same http-read parent."""
    return Flow(
        id="flow-routing", name="Routing Flow", cron="0 * * * *", state="Draft", enabled=True,
        createdAt="2026-01-01T00:00:00.000Z", updatedAt="2026-01-01T00:00:00.000Z",
        blocks=[
            FlowBlock(
                id="b-read", adapter="http", mode="read", name="Read", parentId=None, serviceId="svc-http",
                config={"method": "GET", "path": "/x", "responseFormat": "json", "recordPath": "$.items[*]",
                        "split": True, "pagination": {"type": "none", "fields": {}}},
            ),
            FlowBlock(
                id="b-any", adapter="kafka", mode="write", name="Any Branch", parentId="b-read", entity="e1",
                branch=BranchInfo(name="any-branch", match="any", rules=[
                    BranchCondition(field="sev", op="equals", value="HIGH"),
                    BranchCondition(field="sev", op="equals", value="CRIT"),
                ]),
                config={},
            ),
            FlowBlock(
                id="b-all", adapter="kafka", mode="write", name="All Branch", parentId="b-read", entity="e2",
                branch=BranchInfo(name="all-branch", match="all", rules=[
                    BranchCondition(field="region", op="equals", value="us"),
                    BranchCondition(field="env", op="not_equals", value="dev"),
                ]),
                config={},
            ),
            FlowBlock(id="b-uncond", adapter="kafka", mode="write", name="Uncond Branch", parentId="b-read", entity="e3", config={}),
        ],
        topics=[], variables=[], servicePins={},
    )


def routing_ctx() -> CompileContext:
    services = {"svc-http": make_service(id="svc-http", type="http", name="X", config={"baseUrl": "https://x.example", "authMode": "none"})}
    connections = {"kafka": make_connection(id="conn-kafka", type="kafka", name="K", config={"bootstrapServers": "kafka:9092"})}
    return CompileContext(services=services, connections=connections, gateway_proxies={}, approved_schemas={})


def session_token_flow() -> Flow:
    """Modeled on seeds.ts's flow-fortisiem: session_token auth http read."""
    return Flow(
        id="flow-session", name="Session Token Flow", cron="*/15 * * * *", state="Draft", enabled=True,
        createdAt="2026-01-01T00:00:00.000Z", updatedAt="2026-01-01T00:00:00.000Z",
        blocks=[
            FlowBlock(
                id="b-fetch", adapter="http", mode="read", name="Fetch Incidents", parentId=None, serviceId="svc-fs",
                config={"method": "GET", "path": "/phoenix/rest/incident/list", "responseFormat": "json",
                        "recordPath": "$.incidents[*]", "split": True, "pagination": {"type": "none", "fields": {}}},
            ),
            FlowBlock(id="b-write", adapter="kafka", mode="write", name="All Events", parentId="b-fetch", entity="event", config={}),
        ],
        topics=[], variables=[], servicePins={},
    )


def session_token_ctx() -> CompileContext:
    services = {
        "svc-fs": make_service(
            id="svc-fs", type="http", name="FortiSIEM Events API",
            config={"baseUrl": "https://fortisiem.internal.corp", "authMode": "session_token",
                    "loginPath": "/phoenix/rest/h5/sec/login", "tokenPath": "$.sessionToken", "tokenHeader": "Authorization"},
            hasSecret=True,
        ),
    }
    connections = {"kafka": make_connection(id="conn-kafka", type="kafka", name="K", config={"bootstrapServers": "kafka:9092"})}
    return CompileContext(services=services, connections=connections, gateway_proxies={}, approved_schemas={})


# --------------------------------------------------------------------------
# 1. Golden test
# --------------------------------------------------------------------------


def test_golden_flow_matches_fixture():
    plan = compile_flow(golden_flow(), golden_ctx())
    actual = plan.to_dict()
    fixture_path = FIXTURES_DIR / "golden_flow.json"

    if not fixture_path.exists():
        FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
        fixture_path.write_text(json.dumps(actual, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        pytest.fail(f"Golden fixture generated at {fixture_path} -- review it, then re-run to assert equality.")

    expected = json.loads(fixture_path.read_text(encoding="utf-8"))
    assert actual == expected


# --------------------------------------------------------------------------
# 2. Dedup
# --------------------------------------------------------------------------


def test_dedup_processor_shape_and_ordering():
    plan = compile_flow(golden_flow(), golden_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-sink")
    keys = [p.key for p in group.processors]

    # ExecuteGroovyScript + DetectDuplicate are the LAST two processors
    # before the publish step (envelope -> dedupe__hash -> dedupe__detect -> publish).
    idx_publish = keys.index("publish")
    assert keys[idx_publish - 2 : idx_publish] == ["dedupe__hash", "dedupe__detect"]

    hash_p = next(p for p in group.processors if p.key == "dedupe__hash")
    detect_p = next(p for p in group.processors if p.key == "dedupe__detect")

    assert detect_p.type == "org.apache.nifi.processors.standard.DetectDuplicate"
    assert detect_p.properties["Cache Entry Identifier"] == "${dedupe.key}"
    assert detect_p.properties["Age Off Duration"] == "24 hours"  # windowHours=24
    assert detect_p.autoTerminate == ["duplicate"]  # duplicate auto-terminated; failure is NOT

    dlq_from_detect = [c for c in group.connections if c.from_ == "dedupe__detect" and c.to == "dlq"]
    assert len(dlq_from_detect) == 1
    assert dlq_from_detect[0].relationships == ["failure"]

    excludes = hash_p.properties["EXCLUDES"].split(",")
    assert {"ingest_id", "ingest_ts", "op"} <= set(excludes)
    assert hash_p.properties["SRC"] == "golden_flow__b-sink"  # <flowToken>__<blockId>

    redis_cache = next(cs for cs in group.controllerServices if cs.key == "cs_redis_cache")
    assert redis_cache.type == "org.apache.nifi.redis.service.RedisDistributedMapCacheClientService"
    assert redis_cache.properties["TTL"] == "24 hours"
    redis_pool = next(cs for cs in group.controllerServices if cs.key == "cs_redis_pool")
    assert redis_pool.type == "org.apache.nifi.redis.service.RedisConnectionPoolService"


# --------------------------------------------------------------------------
# 3. Routing
# --------------------------------------------------------------------------


def test_routing_any_all_unconditional():
    plan = compile_flow(routing_flow(), routing_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-read")

    route_procs = [p for p in group.processors if p.type == "org.apache.nifi.processors.standard.RouteOnAttribute"]

    any_procs = [p for p in route_procs if p.key == "route__any_branch"]
    assert len(any_procs) == 1, "exactly one RouteOnAttribute for the any-match branch"
    any_proc = any_procs[0]
    dynamic = {k: v for k, v in any_proc.properties.items() if k.startswith("rule_")}
    assert len(dynamic) == 2  # one dynamic property per rule
    assert dynamic["rule_0"] == "${sev:equals('HIGH')}"
    assert dynamic["rule_1"] == "${sev:equals('CRIT')}"
    assert any_proc.autoTerminate == ["unmatched"]

    all_procs = sorted((p for p in route_procs if p.key.startswith("route__all_branch__rule_")), key=lambda p: p.key)
    assert len(all_procs) == 2, "a 2-processor chain for the all-match branch"
    assert all_procs[0].properties["matched"] == "${region:equals('us')}"
    assert all_procs[1].properties["matched"] == "${env:equals('dev'):not()}"  # not_equals mapping

    # unconditional child: direct connection, no RouteOnAttribute involved.
    uncond_conns = [c for c in group.connections if c.to == "outputPort:b-uncond"]
    assert len(uncond_conns) == 1
    assert "RouteOnAttribute" not in next(
        (p.type for p in group.processors if p.key == uncond_conns[0].from_), ""
    )

    # Route processors live in the PARENT's PG, attributed to the parent in the scope map.
    assert plan.scopeMap["b-read"].groupName == group.name
    for key in ("route__any_branch", "route__all_branch__rule_0", "route__all_branch__rule_1"):
        assert key in plan.scopeMap["b-read"].components
    assert "b-any" not in plan.scopeMap or plan.scopeMap.get("b-any").groupName != group.name


# --------------------------------------------------------------------------
# 4. Terminal placement
# --------------------------------------------------------------------------


def test_block_after_kafka_kc_raises():
    flow = Flow(
        id="flow-term", name="Terminal Flow", state="Draft", enabled=True,
        createdAt="2026-01-01T00:00:00.000Z", updatedAt="2026-01-01T00:00:00.000Z",
        blocks=[
            FlowBlock(id="b-read", adapter="http", mode="read", name="Read", parentId=None, serviceId="svc-http", config={"path": "/x"}),
            FlowBlock(id="b-sink", adapter="kafka_kc", name="Sink", parentId="b-read", serviceId="svc-iceberg", entity="e", config={"sinkServiceId": "svc-iceberg"}),
            FlowBlock(id="b-after", adapter="kafka", mode="write", name="After", parentId="b-sink", entity="e2", config={}),
        ],
        topics=[], variables=[], servicePins={},
    )
    services = {"svc-http": make_service(id="svc-http", type="http", name="X", config={"baseUrl": "https://x", "authMode": "none"})}
    ctx = CompileContext(services=services, connections={}, gateway_proxies={}, approved_schemas={})
    with pytest.raises(CompileError, match="terminal"):
        compile_flow(flow, ctx)


# --------------------------------------------------------------------------
# 5. kafka_kc
# --------------------------------------------------------------------------


def test_kafka_kc_publish_and_connector():
    plan = compile_flow(golden_flow(), golden_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-sink")

    publish = next(p for p in group.processors if p.key == "publish")
    assert publish.type == "org.apache.nifi.kafka.processors.PublishKafka"
    assert publish.properties["Record Reader"] == "cs_json_reader"
    assert publish.properties["Record Writer"] == "cs_avro_writer"

    avro_writer = next(cs for cs in group.controllerServices if cs.key == "cs_avro_writer")
    assert avro_writer.type == "org.apache.nifi.avro.AvroRecordSetWriter"
    assert avro_writer.properties["Schema Registry"] == "cs_schema_registry"
    assert avro_writer.properties["Schema Write Strategy"] == "schema-reference-writer"
    assert avro_writer.properties["Schema Reference Writer"] == "cs_schema_ref_writer"

    registry = next(cs for cs in group.controllerServices if cs.key == "cs_schema_registry")
    assert registry.type == "org.apache.nifi.confluent.schemaregistry.ConfluentSchemaRegistry"
    assert registry.properties["Schema Registry URLs"] == "#{apicurio_ccompat_url}"
    ccompat_param = next(p for p in plan.parameterContext.parameters if p.name == "apicurio_ccompat_url")
    assert ccompat_param.value == "http://apicurio.internal.corp:8081/apis/ccompat/v7"

    ref_writer = next(cs for cs in group.controllerServices if cs.key == "cs_schema_ref_writer")
    assert ref_writer.type == "org.apache.nifi.confluent.schemaregistry.ConfluentEncodedSchemaReferenceWriter"

    connector = next(c for c in plan.connectors if c.ownerBlockId == "b-sink")
    assert connector.name == "golden_flow.b-sink.kafka_kc"  # <flowToken>.<blockId>.kafka_kc

    topic_names = {t.name: t.kind for t in plan.topics}
    assert topic_names.get("raw.golden_flow.asset") == "data"
    assert topic_names.get("dlq.golden_flow") == "dlq"

    # deploy gate: approved schema required.
    ctx_no_schema = golden_ctx()
    ctx_no_schema.approved_schemas = {}
    with pytest.raises(CompileError, match="approved schema"):
        compile_flow(golden_flow(), ctx_no_schema)


# --------------------------------------------------------------------------
# 6. session_token http
# --------------------------------------------------------------------------


def test_session_token_login_ahead_of_fetch():
    plan = compile_flow(session_token_flow(), session_token_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-fetch")
    keys = [p.key for p in group.processors]

    assert "login" in keys and "extract_token" in keys and "fetch" in keys
    assert keys.index("login") < keys.index("extract_token") < keys.index("fetch")

    login = next(p for p in group.processors if p.key == "login")
    assert login.type == "org.apache.nifi.processors.standard.InvokeHTTP"
    assert login.properties["HTTP URL"] == "#{svc_svc-fs_base_url}/phoenix/rest/h5/sec/login"

    extract_token = next(p for p in group.processors if p.key == "extract_token")
    assert extract_token.properties["session.token"] == "$.sessionToken"

    fetch = next(p for p in group.processors if p.key == "fetch")
    assert fetch.properties["Authorization"] == "${session.token}"  # injection header on the fetch call

    # Login/extract_token failures are run failures, not record failures -- no DLQ record.
    dlq_from_login = [c for c in group.connections if c.from_ in ("login", "extract_token") and c.to == "dlq"]
    assert dlq_from_login == []
    assert any(c.to == "run_failure__log" for c in group.connections if c.from_ == "login")
    assert any(c.to == "run_failure__log" for c in group.connections if c.from_ == "extract_token")


# --------------------------------------------------------------------------
# 7. Determinism
# --------------------------------------------------------------------------


def test_determinism():
    flow = golden_flow()
    ctx = golden_ctx()
    plan1 = compile_flow(flow, ctx)
    plan2 = compile_flow(flow, ctx)
    assert plan1.to_json() == plan2.to_json()

    routing_result_1 = compile_flow(routing_flow(), routing_ctx()).to_json()
    routing_result_2 = compile_flow(routing_flow(), routing_ctx()).to_json()
    assert routing_result_1 == routing_result_2
