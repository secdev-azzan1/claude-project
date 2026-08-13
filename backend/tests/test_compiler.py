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
    FlowTopic,
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


# --------------------------------------------------------------------------
# 8. jdbc
# --------------------------------------------------------------------------


def jdbc_flow() -> Flow:
    """Root jdbc read (incremental, watermark) -> child jdbc write, both on
    the same postgres service -- exercises the shared DBCPConnectionPool CS
    reuse across two block groups' own `_ensure_db_pool` calls too."""
    return Flow(
        id="flow-jdbc", name="Jdbc Flow", cron="0 */2 * * *", state="Draft", enabled=True,
        createdAt="2026-01-01T00:00:00.000Z", updatedAt="2026-01-01T00:00:00.000Z",
        blocks=[
            FlowBlock(
                id="b-read", adapter="jdbc", mode="read", name="Read Assets", parentId=None, serviceId="svc-db",
                config={"table": "cmdb_assets", "columns": ["id", "hostname", "updated_at"],
                        "incremental": True, "watermarkColumn": "updated_at", "initialPosition": "oldest"},
            ),
            FlowBlock(
                id="b-write", adapter="jdbc", mode="write", name="Write Assets", parentId="b-read",
                serviceId="svc-db", entity="asset", config={"table": "assets_mirror"},
            ),
        ],
        topics=[], variables=[], servicePins={},
    )


def jdbc_ctx() -> CompileContext:
    services = {
        "svc-db": make_service(
            id="svc-db", type="database", name="Ops Postgres",
            config={"dialect": "postgresql", "host": "db.internal.corp", "port": 5432, "database": "ops",
                    "username": "svc_reader", "password": "s3cret"},
            hasSecret=True,
        ),
    }
    connections = {"kafka": make_connection(id="conn-kafka", type="kafka", name="K", config={"bootstrapServers": "kafka:9092"})}
    return CompileContext(services=services, connections=connections, gateway_proxies={}, approved_schemas={})


def test_jdbc_read_incremental_golden_checks():
    plan = compile_flow(jdbc_flow(), jdbc_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-read")

    query = next(p for p in group.processors if p.key == "query")
    assert query.type == "org.apache.nifi.processors.standard.QueryDatabaseTableRecord"
    assert query.properties["Table Name"] == "cmdb_assets"
    assert query.properties["Columns to Return"] == "id, hostname, updated_at"
    assert query.properties["Maximum-value Columns"] == "updated_at"
    assert query.properties["Initial Load Strategy"] == "Start at Beginning"
    assert query.runOnPrimary is True
    assert query.schedulingStrategy == "CRON_DRIVEN"  # cron on root
    assert query.schedulingPeriod == "0 0 */2 * * *"  # "0 */2 * * *" -> "sec min hour dom mon dow"

    pool = next(cs for cs in group.controllerServices if cs.key == "cs_db_pool")
    assert pool.type == "org.apache.nifi.dbcp.DBCPConnectionPool"
    assert pool.properties["Database Driver Class Name"] == "org.postgresql.Driver"

    url_param = next(p for p in plan.parameterContext.parameters if p.name == "svc_svc-db_db_url")
    assert url_param.value == "jdbc:postgresql://db.internal.corp:5432/ops"
    pw_param = next(p for p in plan.parameterContext.parameters if p.name == "svc_svc-db_db_password")
    assert pw_param.sensitive is True and pw_param.value == "s3cret"

    split = next(p for p in group.processors if p.key == "split")
    assert split.type == "org.apache.nifi.processors.standard.SplitRecord"
    assert split.properties["Records Per Split"] == "1"
    dlq_from_query = [c for c in group.connections if c.from_ == "query" and c.to == "dlq"]
    assert dlq_from_query and dlq_from_query[0].relationships == ["failure"]


def test_jdbc_write():
    plan = compile_flow(jdbc_flow(), jdbc_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-write")

    write = next(p for p in group.processors if p.key == "write")
    assert write.type == "org.apache.nifi.processors.standard.PutDatabaseRecord"
    assert write.properties["Statement Type"] == "INSERT"
    assert write.properties["Table Name"] == "assets_mirror"
    assert write.properties["Database Connection Pooling Service"] == "cs_db_pool"

    dlq_from_write = [c for c in group.connections if c.from_ == "write" and c.to == "dlq"]
    assert dlq_from_write and dlq_from_write[0].relationships == ["failure"]

    # b-write is wired as b-read's child (jdbc is never forced terminal).
    port_link = next((pl for pl in plan.rootGroup.connections if pl.toBlockId == "b-write"), None)
    assert port_link is not None and port_link.fromBlockId == "b-read"


# --------------------------------------------------------------------------
# 9. kafka read
# --------------------------------------------------------------------------


def kafka_read_flow(parse_format: str = "json", initial_position: str = "beginning") -> Flow:
    """A bare kafka-read root, attached to an ADOPTED topic (root_block()'s
    special case), with no children -- see blocks_kafka.py's module
    docstring for why children aren't exercised here (compile_flow.py's
    `terminal=True` for every kafka block means none would actually get
    wired to a PortLink)."""
    return Flow(
        id="flow-kread", name="Kafka Read Flow", cron=None, state="Draft", enabled=True,
        createdAt="2026-01-01T00:00:00.000Z", updatedAt="2026-01-01T00:00:00.000Z",
        blocks=[
            FlowBlock(
                id="b-kread", adapter="kafka", mode="read", name="Consume Events", parentId="t-adopted",
                config={"parseFormat": parse_format, "initialPosition": initial_position},
            ),
        ],
        topics=[FlowTopic(id="t-adopted", kind="adopted", name="partner.threatfeed.indicators", sealed=False)],
        variables=[], servicePins={},
    )


def kafka_read_ctx() -> CompileContext:
    connections = {"kafka": make_connection(id="conn-kafka", type="kafka", name="K", config={"bootstrapServers": "kafka:9092"})}
    return CompileContext(services={}, connections=connections, gateway_proxies={}, approved_schemas={})


def test_kafka_read_json_split_and_offset_reset():
    plan = compile_flow(kafka_read_flow(parse_format="json", initial_position="new"), kafka_read_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-kread")
    assert group.inputPort is False  # ConsumeKafka is a source processor, no upstream port needed

    consume = next(p for p in group.processors if p.key == "consume")
    assert consume.type == "org.apache.nifi.kafka.processors.ConsumeKafka"
    assert consume.properties["Group ID"] == "kafka_read_flow__b-kread"
    assert consume.properties["Auto Offset Reset"] == "latest"  # initialPosition "new"

    topic_param = next(p for p in plan.parameterContext.parameters if p.name == "topic_b-kread")
    assert topic_param.value == "partner.threatfeed.indicators"  # adopted topic's own name, not config.topicName

    split = next(p for p in group.processors if p.key == "split")
    assert split.type == "org.apache.nifi.processors.standard.SplitJson"
    assert split.properties["JsonPath Expression"] == "$[*]"
    assert "original" in split.autoTerminate
    dlq_from_split = [c for c in group.connections if c.from_ == "split" and c.to == "dlq"]
    assert dlq_from_split and dlq_from_split[0].relationships == ["failure"]


def test_kafka_read_raw_no_transforms_or_split():
    plan = compile_flow(kafka_read_flow(parse_format="raw", initial_position="beginning"), kafka_read_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-kread")
    keys = [p.key for p in group.processors]

    assert "split" not in keys and "convert" not in keys  # R8: byte passthrough, no record processing
    consume = next(p for p in group.processors if p.key == "consume")
    assert consume.properties["Auto Offset Reset"] == "earliest"  # initialPosition "beginning"
    assert "success" in consume.autoTerminate  # nothing downstream -- auto-terminated tail


# --------------------------------------------------------------------------
# 10. http write / lookup
# --------------------------------------------------------------------------


def http_svc_ctx() -> CompileContext:
    services = {"svc-http": make_service(id="svc-http", type="http", name="Ticketing API",
                                          config={"baseUrl": "https://ticketing.corp.local", "authMode": "none"})}
    connections = {"kafka": make_connection(id="conn-kafka", type="kafka", name="K", config={"bootstrapServers": "kafka:9092"})}
    return CompileContext(services=services, connections=connections, gateway_proxies={}, approved_schemas={})


def http_write_flow(write_forwards: str) -> Flow:
    return Flow(
        id="flow-hwrite", name="Http Write Flow", cron=None, state="Draft", enabled=True,
        createdAt="2026-01-01T00:00:00.000Z", updatedAt="2026-01-01T00:00:00.000Z",
        blocks=[
            FlowBlock(
                id="b-write", adapter="http", mode="write", name="Post Incident", parentId=None, serviceId="svc-http",
                config={"method": "POST", "path": "/incidents",
                        "bodyTemplate": '{"title": "${title}", "severity": "${sev}"}',
                        "writeForwards": write_forwards, "responseFormat": "json", "recordPath": "$.data[*]",
                        "split": True},
            ),
            FlowBlock(
                id="b-continue", adapter="kafka", mode="write", name="Log Result", parentId="b-write",
                entity="incident_result", config={},
            ),
        ],
        topics=[], variables=[], servicePins={},
    )


def test_http_write_replace_text_method_and_original_continuation():
    plan = compile_flow(http_write_flow("original"), http_svc_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-write")
    keys = [p.key for p in group.processors]
    assert keys.index("extract_body_fields") < keys.index("render_body") < keys.index("write")
    assert "split" not in keys  # "original" continuation never touches the response-parse chain

    extract = next(p for p in group.processors if p.key == "extract_body_fields")
    assert extract.properties["title"] == "$.title"
    assert extract.properties["sev"] == "$.sev"

    render = next(p for p in group.processors if p.key == "render_body")
    assert render.type == "org.apache.nifi.processors.standard.ReplaceText"
    assert render.properties["Replacement Strategy"] == "Always Replace"
    assert render.properties["Replacement Value"] == '{"title": "${title}", "severity": "${sev}"}'

    write = next(p for p in group.processors if p.key == "write")
    assert write.type == "org.apache.nifi.processors.standard.InvokeHTTP"
    assert write.properties["HTTP Method"] == "POST"
    assert write.properties["Request Body Enabled"] == "true"
    assert "Original" not in write.autoTerminate  # forwarded onward
    assert "Response" in write.autoTerminate  # unused on this branch

    onward = [c for c in group.connections if c.from_ == "write" and c.to == "outputPort:b-continue"]
    assert onward and onward[0].relationships == ["Original"]


def test_http_write_response_continuation_reuses_parse_chain():
    plan = compile_flow(http_write_flow("response"), http_svc_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-write")

    write = next(p for p in group.processors if p.key == "write")
    assert "Response" not in write.autoTerminate  # forwarded onward
    assert "Original" in write.autoTerminate  # unused on this branch

    split = next(p for p in group.processors if p.key == "split")
    assert split.type == "org.apache.nifi.processors.standard.SplitJson"
    assert split.properties["JsonPath Expression"] == "$.data[*]"
    wr_to_split = [c for c in group.connections if c.from_ == "write" and c.to == "split"]
    assert wr_to_split and wr_to_split[0].relationships == ["Response"]

    onward = [c for c in group.connections if c.from_ == "split" and c.to == "outputPort:b-continue"]
    assert onward and onward[0].relationships == ["split"]


def http_lookup_flow() -> Flow:
    return Flow(
        id="flow-hlookup", name="Http Lookup Flow", cron="0 * * * *", state="Draft", enabled=True,
        createdAt="2026-01-01T00:00:00.000Z", updatedAt="2026-01-01T00:00:00.000Z",
        blocks=[
            FlowBlock(
                id="b-read", adapter="http", mode="read", name="List Hosts", parentId=None, serviceId="svc-http",
                config={"method": "GET", "path": "/hosts", "responseFormat": "json", "recordPath": "$[*]",
                        "split": True, "pagination": {"type": "none", "fields": {}}},
            ),
            FlowBlock(
                id="b-lookup", adapter="http", mode="lookup", name="Enrich Host", parentId="b-read",
                serviceId="svc-http", config={"path": "/hosts/${host_id}/details", "lookupJoinField": "details"},
            ),
        ],
        topics=[], variables=[], servicePins={},
    )


def test_http_lookup_join_merge():
    plan = compile_flow(http_lookup_flow(), http_svc_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-lookup")
    keys = [p.key for p in group.processors]
    assert keys.index("extract_path_fields") < keys.index("lookup_fetch") < keys.index("lookup_extract") < keys.index("lookup_merge")

    extract = next(p for p in group.processors if p.key == "extract_path_fields")
    assert extract.properties["host_id"] == "$.host_id"

    fetch = next(p for p in group.processors if p.key == "lookup_fetch")
    assert fetch.type == "org.apache.nifi.processors.standard.InvokeHTTP"
    assert fetch.properties["HTTP Method"] == "GET"
    assert fetch.properties["HTTP URL"].endswith("/hosts/${host_id}/details")

    lookup_extract = next(p for p in group.processors if p.key == "lookup_extract")
    assert lookup_extract.properties["lookup_value"] == "$"

    merge = next(p for p in group.processors if p.key == "lookup_merge")
    assert merge.type == "org.apache.nifi.processors.standard.UpdateRecord"
    assert merge.properties["/details_lookup"] == "${lookup_value}"


# --------------------------------------------------------------------------
# 11. http read: csv response parsing
# --------------------------------------------------------------------------


def http_csv_read_flow() -> Flow:
    return Flow(
        id="flow-csv", name="Csv Read Flow", cron="0 * * * *", state="Draft", enabled=True,
        createdAt="2026-01-01T00:00:00.000Z", updatedAt="2026-01-01T00:00:00.000Z",
        blocks=[
            FlowBlock(
                id="b-read", adapter="http", mode="read", name="List Rows", parentId=None, serviceId="svc-http",
                config={"method": "GET", "path": "/rows.csv", "responseFormat": "csv", "recordPath": "$[*]",
                        "split": True, "pagination": {"type": "none", "fields": {}}},
            ),
        ],
        topics=[], variables=[], servicePins={},
    )


def test_http_read_csv_response_converts_then_splits():
    plan = compile_flow(http_csv_read_flow(), http_svc_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-read")
    keys = [p.key for p in group.processors]
    assert keys.index("fetch") < keys.index("convert") < keys.index("split")

    convert = next(p for p in group.processors if p.key == "convert")
    assert convert.type == "org.apache.nifi.processors.standard.ConvertRecord"
    csv_reader = next(cs for cs in group.controllerServices if cs.type == "org.apache.nifi.csv.CSVReader")
    assert convert.properties["Record Reader"] == csv_reader.key

    fetch_to_convert = [c for c in group.connections if c.from_ == "fetch" and c.to == "convert"]
    assert fetch_to_convert and fetch_to_convert[0].relationships == ["Response"]
    dlq_from_convert = [c for c in group.connections if c.from_ == "convert" and c.to == "dlq"]
    assert dlq_from_convert and dlq_from_convert[0].relationships == ["failure"]

    split = next(p for p in group.processors if p.key == "split")
    assert split.type == "org.apache.nifi.processors.standard.SplitJson"
    assert split.properties["JsonPath Expression"] == "$[*]"
    assert "original" in split.autoTerminate  # pagination.type == "none"

def test_kafka_read_with_child_gets_port_link():
    """R3/compile fix: kafka blocks are not terminal — a kafka read's children
    must be wired via a PortLink (was silently skipped when every kafka block
    compiled as terminal)."""
    base = kafka_read_flow(parse_format="json", initial_position="beginning")
    child = FlowBlock(id="b-kwrite", adapter="kafka", mode="write", name="events out",
                      parentId="b-kread", entity="event", config={}, transforms=[])
    flow = base.model_copy(update={"blocks": list(base.blocks) + [child]})
    plan = compile_flow(flow, kafka_read_ctx())
    links = [(l.fromBlockId, l.toBlockId) for l in plan.rootGroup.connections]
    assert ("b-kread", "b-kwrite") in links
    read_group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-kread")
    assert any("ConsumeKafka" in p.type for p in read_group.processors)
    assert read_group.outputPort
    # the promised split tail feeds the output port, not an auto-terminate
    split = next(p for p in read_group.processors if p.key == "split")
    assert "split" not in split.autoTerminate


def test_kafka_write_childless_publish_consumes_tail_without_auto_terminate():
    base = kafka_read_flow(parse_format="json", initial_position="beginning")
    child = FlowBlock(id="b-kwrite", adapter="kafka", mode="write", name="events out",
                      parentId="b-kread", entity="event", config={}, transforms=[])
    flow = base.model_copy(update={"blocks": list(base.blocks) + [child]})
    plan = compile_flow(flow, kafka_read_ctx())
    kw = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-kwrite")
    pub = next(p for p in kw.processors if "PublishKafka" in p.type)
    assert pub is not None
    assert not kw.outputPort
    # the input-port tail feeds publish exactly once, no dangling auto-termination clash
    feeds = [c for c in kw.connections if c.to == "publish"]
    assert len(feeds) == 1
