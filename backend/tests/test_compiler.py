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
from services.adapter.compiler.inference import build_inference_plan  # noqa: E402

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
                config={
                    "sinkServiceId": "svc-iceberg",
                    "sinkConfig": {
                        "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
                        "topics": "raw.golden_flow.asset",
                        "tasks.max": "1",
                        "consumer.override.auto.offset.reset": "earliest",
                        "iceberg.tables": "bronze.asset",
                        "iceberg.tables.auto-create-enabled": "true",
                        "iceberg.catalog.type": "rest",
                        "iceberg.catalog.uri": "http://polaris.internal.corp:8181/api/catalog",
                        "iceberg.catalog.warehouse": "bronze",
                        "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                        "iceberg.catalog.client.region": "us-east-1",
                        "iceberg.catalog.s3.region": "us-east-1",
                        "iceberg.catalog.s3.path-style-access": "true",
                        "iceberg.control.commit.interval-ms": "60000",
                        "value.converter": "io.apicurio.registry.utils.converter.AvroConverter",
                        "value.converter.apicurio.registry.url": "http://apicurio.internal.corp:8081/apis/registry/v3",
                        "value.converter.apicurio.registry.as-confluent": "true",
                        "value.converter.apicurio.registry.find-latest": "true",
                        "value.converter.apicurio.registry.use-id": "contentId",
                        "value.converter.apicurio.registry.auto-register": "false",
                        "value.converter.schemas.enable": "true",
                        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                    },
                },
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

    # Only the configured exclusions participate.  The compiler must not add
    # hidden platform metadata exclusions behind the user's back.  An empty
    # optional EXCLUDES property is omitted because NiFi rejects blank dynamic
    # properties; the Groovy script treats a missing property as no excludes.
    assert "EXCLUDES" not in hash_p.properties
    assert hash_p.properties["SRC"] == "golden_flow__b-sink"  # <flowToken>__<blockId>

    redis_cache = next(cs for cs in group.controllerServices if cs.key == "cs_redis_cache")
    assert redis_cache.type == "org.apache.nifi.redis.service.RedisDistributedMapCacheClientService"
    assert redis_cache.properties["TTL"] == "24 hours"
    redis_pool = next(cs for cs in group.controllerServices if cs.key == "cs_redis_pool")
    assert redis_pool.type == "org.apache.nifi.redis.service.RedisConnectionPoolService"


def test_dedup_preserves_configured_exclusions():
    flow = golden_flow()
    flow.blocks[1].transforms[0].config["excludedFields"] = ["updatedAt"]

    plan = compile_flow(flow, golden_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-sink")
    hash_p = next(p for p in group.processors if p.key == "dedupe__hash")

    assert hash_p.properties["EXCLUDES"] == "updatedAt"


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
    # M1: 'matched'-if-any strategy — N genuine rule expressions, but ONE
    # matched relationship so a record matching several rules is delivered
    # to the child exactly once (Route to Property name cloned per property).
    assert any_proc.properties["Routing Strategy"] == "Route to 'matched' if any matches"
    dynamic = {k: v for k, v in any_proc.properties.items() if k.startswith("rule_")}
    assert len(dynamic) == 2  # one dynamic property per rule
    assert dynamic["rule_0"] == "${sev:equals('HIGH')}"
    assert dynamic["rule_1"] == "${sev:equals('CRIT')}"
    assert any_proc.autoTerminate == ["unmatched"]
    any_to_child = [c for c in group.connections if c.from_ == "route__any_branch" and c.to == "outputPort:b-any"]
    assert len(any_to_child) == 1, "a single connection to the child, never one per rule"
    assert any_to_child[0].relationships == ["matched"]

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


def test_schema_inference_plan_reuses_chain_but_bypasses_avro_and_connect():
    ctx = golden_ctx()
    ctx.approved_schemas = {}
    plan = build_inference_plan(
        golden_flow(),
        ctx,
        target_block_id="b-sink",
        inference_topic="dmp.schema_inference.golden.asset.job-1",
        job_id="schema-inference-job-1",
    )

    assert plan.flowId.startswith("flow-golden__schema_inference__")
    assert plan.rootGroup.name.startswith("golden_flow_schema_inference_")
    assert plan.connectors == []
    assert {topic.name for topic in plan.topics} >= {
        "dmp.schema_inference.golden.asset.job-1",
        "dlq.golden_flow_schema_inference_schema_inference_job_1",
    }

    sink = next(group for group in plan.rootGroup.childGroups if group.blockId == "b-sink")
    publish = next(processor for processor in sink.processors if processor.key == "publish")
    assert publish.properties["Record Writer"] == "cs_json_writer"
    assert "cs_avro_writer" not in {service.key for service in sink.controllerServices}
    assert "cs_schema_registry" not in {service.key for service in sink.controllerServices}
    assert "envelope" in {processor.key for processor in sink.processors}
    assert {"dedupe__hash", "dedupe__detect"}.issubset({processor.key for processor in sink.processors})


# --------------------------------------------------------------------------
# 6. session_token http
# --------------------------------------------------------------------------


def test_session_token_login_ahead_of_fetch():
    plan = compile_flow(session_token_flow(), session_token_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-fetch")
    keys = [p.key for p in group.processors]

    # R3 (journey-r-reverify.md): ONE ExecuteGroovyScript login step. The
    # previous ReplaceText-JSON-body chain was PROVEN UNDEPLOYABLE live —
    # NiFi refuses a `#{sensitive param}` reference inside a non-sensitive
    # STATIC property ("the Sensitivity of the parameter does not match the
    # Sensitivity of the property"), and `sensitiveDynamicPropertyNames`
    # cannot cover a static descriptor. Only a SENSITIVE DYNAMIC property
    # (ExecuteGroovyScript's binding mechanism, same as the dedup hash
    # script) can legally carry the password.
    assert "login" in keys and "fetch" in keys
    assert "login_body" not in keys and "extract_token" not in keys
    assert keys.index("login") < keys.index("fetch")

    login = next(p for p in group.processors if p.key == "login")
    assert login.type == "org.apache.nifi.processors.groovyx.ExecuteGroovyScript"
    assert login.properties["LOGIN_URL"] == "#{svc_svc-fs_base_url}/phoenix/rest/h5/sec/login"
    assert login.properties["TOKEN_PATH"] == "$.sessionToken"
    assert login.properties["USERNAME"] == "#{svc_svc-fs_username}"
    assert login.properties["PASSWORD"] == "#{svc_svc-fs_password}"
    assert "Request Username" not in login.properties  # no Basic-Auth props
    assert "Request Password" not in login.properties

    pw_param = next(p for p in plan.parameterContext.parameters if p.name == "svc_svc-fs_password")
    assert pw_param.sensitive is True

    # The deployer lists PASSWORD (and ONLY it) in the processor's
    # sensitiveDynamicPropertyNames — assert with the real nifi_apply helper
    # so the compiler test breaks if the two modules ever drift apart.
    from services.adapter.deployer.nifi_apply import _sensitive_dynamic_props
    sensitive_names = {p.name for p in plan.parameterContext.parameters if p.sensitive}
    assert _sensitive_dynamic_props(login.properties, sensitive_names) == ["PASSWORD"]

    # The Groovy script does the whole login: POST JSON body, parse, resolve
    # the token dot-path, set `session.token`, REL_FAILURE on any run failure.
    script = login.properties["Script Body"]
    assert "JsonOutput.toJson([username: username, password: password])" in script
    assert "JsonSlurper" in script
    assert "session.token" in script
    assert "REL_SUCCESS" in script and "REL_FAILURE" in script

    # success feeds fetch; failure is a RUN failure -> run_failure__log,
    # never a DLQ record. Exactly one disposition per relationship (M7).
    assert any(c.from_ == "login" and c.to == "fetch" and c.relationships == ["success"] for c in group.connections)
    fail_conns = [c for c in group.connections if c.from_ == "login" and c.to == "run_failure__log"]
    assert fail_conns and fail_conns[0].relationships == ["failure"]
    assert not any(c.from_ == "login" and c.to == "dlq" for c in group.connections)

    fetch = next(p for p in group.processors if p.key == "fetch")
    assert fetch.properties["Authorization"] == "${session.token}"  # injection header on the fetch call


def test_session_token_header_honors_token_template():
    ctx = session_token_ctx()
    ctx.services["svc-fs"].config["tokenTemplate"] = "Bearer ${token}"
    plan = compile_flow(session_token_flow(), ctx)
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-fetch")
    fetch = next(p for p in group.processors if p.key == "fetch")
    # `${token}` in the template is replaced with the extracted-token EL.
    assert fetch.properties["Authorization"] == "Bearer ${session.token}"


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
    connections = {
        "kafka": make_connection(id="conn-kafka", type="kafka", name="K", config={"bootstrapServers": "kafka:9092"}),
        "redis": make_connection(id="conn-redis", type="redis", name="Redis", config={"host": "redis", "port": 6379, "bookmarksDb": 1}),
    }
    return CompileContext(services=services, connections=connections, gateway_proxies={}, approved_schemas={})


def test_jdbc_read_incremental_golden_checks():
    plan = compile_flow(jdbc_flow(), jdbc_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-read")

    assert not any(p.type == "org.apache.nifi.processors.standard.QueryDatabaseTableRecord" for p in group.processors)
    trigger = next(p for p in group.processors if p.key == "trigger")
    assert trigger.type == "org.apache.nifi.processors.standard.GenerateFlowFile"
    assert trigger.runOnPrimary is True
    assert trigger.schedulingStrategy == "CRON_DRIVEN"
    assert trigger.schedulingPeriod == "0 0 */2 * * ?"

    query = next(p for p in group.processors if p.key == "query")
    assert query.type == "org.apache.nifi.processors.standard.ExecuteSQLRecord"
    assert query.properties["SQL Query"] == "${jdbc.query}"
    assert "SQL select query" not in query.properties
    assert query.properties["Max Rows Per FlowFile"] == "0"
    assert "LIMIT 1" not in next(p for p in group.processors if p.key == "bookmark_oldest").properties["jdbc.query"]
    batch_writer = next(cs for cs in group.controllerServices if cs.key == "cs_incremental_json_writer")
    assert batch_writer.type == "org.apache.nifi.json.JsonRecordSetWriter"
    assert batch_writer.properties["Output Grouping"] == "output-array"
    query_seed = next(p for p in group.processors if p.key == "bookmark_existing_query")
    assert query_seed.properties["sql.args.1.type"] == "93"
    assert query_seed.properties["sql.args.1.value"] == "${jdbc.bookmark.value}"
    assert "WHERE updated_at > ?" in query_seed.properties["jdbc.query"]

    fetch = next(p for p in group.processors if p.key == "bookmark_fetch")
    assert fetch.type == "org.apache.nifi.processors.standard.FetchDistributedMapCache"
    assert fetch.properties["Distributed Cache Service"] == "cs_jdbc_bookmark_cache"
    assert fetch.properties["Put Cache Value In Attribute"] == "jdbc.bookmark.raw"

    bookmark_cache = next(cs for cs in group.controllerServices if cs.key == "cs_jdbc_bookmark_cache")
    assert bookmark_cache.type == "org.apache.nifi.redis.service.RedisDistributedMapCacheClientService"
    bookmark_pool = next(cs for cs in group.controllerServices if cs.key == "cs_jdbc_bookmark_pool")
    assert bookmark_pool.properties["Database Index"] == "1"
    bookmark_param = next(p for p in plan.parameterContext.parameters if p.name == "jdbc_bookmark_key_b-read")
    assert bookmark_param.value == "dmp:jdbc:bookmark:flow-jdbc:b-read"

    pool = next(cs for cs in group.controllerServices if cs.key == "cs_db_pool")
    assert pool.type == "org.apache.nifi.dbcp.DBCPConnectionPool"
    assert pool.properties["Database Driver Class Name"] == "org.postgresql.Driver"
    # postgresql keeps the standard trailing /{database} path segment.
    assert pool.properties["Database Connection URL"] == "#{svc_svc-db_db_url}"
    # No `driverLocations` configured on this fixture's service -> the
    # property is left unset entirely (not invented/guessed).
    assert "Database Driver Locations" not in pool.properties

    url_param = next(p for p in plan.parameterContext.parameters if p.name == "svc_svc-db_db_url")
    assert url_param.value == "jdbc:postgresql://db.internal.corp:5432/ops"
    pw_param = next(p for p in plan.parameterContext.parameters if p.name == "svc_svc-db_db_password")
    assert pw_param.sensitive is True and pw_param.value == "s3cret"

    assert not any(p.key == "split" for p in group.processors)
    capture = next(p for p in group.processors if p.key == "bookmark_capture")
    assert capture.properties["jdbc.bookmark.candidate"] == "$[-1].updated_at"
    assert any(c.from_ == "query" and c.to == "bookmark_capture" and c.relationships == ["success"] for c in group.connections)
    dlq_from_query = [c for c in group.connections if c.from_ == "query" and c.to == "dlq"]
    assert dlq_from_query and dlq_from_query[0].relationships == ["failure"]
    dlq_from_capture = [c for c in group.connections if c.from_ == "bookmark_capture" and c.to == "dlq"]
    assert dlq_from_capture and dlq_from_capture[0].relationships == ["failure"]


def test_jdbc_incremental_requires_redis_at_compile_time():
    ctx = jdbc_ctx()
    del ctx.connections["redis"]
    with pytest.raises(CompileError, match="requires an active Redis connection"):
        compile_flow(jdbc_flow(), ctx)


def test_jdbc_write():
    plan = compile_flow(jdbc_flow(), jdbc_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-write")

    write = next(p for p in group.processors if p.key == "write")
    assert write.type == "org.apache.nifi.processors.standard.PutDatabaseRecord"
    assert write.properties["Statement Type"] == "INSERT"
    assert write.properties["Table Name"] == "assets_mirror"
    assert write.properties["Database Connection Pooling Service"] == "cs_db_pool"

    commit = next(p for p in group.processors if p.key == "jdbc_write__bookmark_commit")
    payload = next(p for p in group.processors if p.key == "jdbc_write__bookmark_payload")
    assert commit.type == "org.apache.nifi.processors.standard.PutDistributedMapCache"
    assert commit.properties["Distributed Cache Service"] == "cs_jdbc_bookmark_cache"
    assert "success" in commit.autoTerminate
    assert any(c.from_ == "write" and c.to == payload.key and c.relationships == ["success"] for c in group.connections)
    assert any(c.from_ == payload.key and c.to == commit.key and c.relationships == ["success"] for c in group.connections)

    assert "retry" in write.autoTerminate
    dlq_from_write = [c for c in group.connections if c.from_ == "write" and c.to == "dlq"]
    assert dlq_from_write and dlq_from_write[0].relationships == ["failure"]
    port_link = next((pl for pl in plan.rootGroup.connections if pl.toBlockId == "b-write"), None)
    assert port_link is not None and port_link.fromBlockId == "b-read"


def test_jdbc_incremental_new_position_snapshots_without_publishing():
    flow = jdbc_flow()
    flow.blocks[0].config["initialPosition"] = "new"
    plan = compile_flow(flow, jdbc_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-read")

    initial = next(p for p in group.processors if p.key == "bookmark_initial_query")
    assert initial.type == "org.apache.nifi.processors.standard.ExecuteSQLRecord"
    assert initial.properties["SQL Query"] == "${jdbc.query}"
    seed = next(p for p in group.processors if p.key == "bookmark_initial_seed")
    assert "updated_at IS NOT NULL" in seed.properties["jdbc.query"]
    assert "ORDER BY updated_at DESC LIMIT 1" in seed.properties["jdbc.query"]
    assert initial.properties["Max Rows Per FlowFile"] == "0"
    initial_extract = next(p for p in group.processors if p.key == "bookmark_initial_extract")
    assert initial_extract.properties["jdbc.bookmark.candidate"] == "$[-1].__dmp_watermark"
    assert not any(c.from_ == "bookmark_initial_query" and c.to == "outputPort:b-write" for c in group.connections)


def test_jdbc_incremental_tie_breaker_uses_compound_cursor():
    flow = jdbc_flow()
    flow.blocks[0].config.update({"bookmarkTieBreaker": "id", "bookmarkTieBreakerType": "bigint"})
    plan = compile_flow(flow, jdbc_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-read")
    seed = next(p for p in group.processors if p.key == "bookmark_existing_query")
    assert "(updated_at > ?) OR (updated_at = ? AND id > ?)" in seed.properties["jdbc.query"]
    assert "LIMIT 1" not in seed.properties["jdbc.query"]
    assert seed.properties["sql.args.3.type"] == "-5"


def test_jdbc_lookup_respects_join_field():
    from services.adapter.compiler.blocks_jdbc import _compile_lookup
    from services.adapter.compiler.ir import BlockBuilder

    flow = Flow(
        id="flow-jdbc-lookup", name="Jdbc Lookup Flow", cron=None, state="Draft", enabled=True,
        createdAt="2026-01-01T00:00:00.000Z", updatedAt="2026-01-01T00:00:00.000Z",
        blocks=[
            FlowBlock(id="b-root", adapter="http", mode="read", name="Root", parentId=None, serviceId=None, config={}),
            FlowBlock(
                id="b-lookup", adapter="jdbc", mode="lookup", name="Lookup", parentId="b-root", serviceId="svc-db",
                config={"table": "cmdb_assets", "lookupJoinField": "asset_id"},
            ),
        ],
        topics=[], variables=[], servicePins={},
    )
    ctx = jdbc_ctx()
    builder = BlockBuilder()
    params = []
    _compile_lookup(
        builder,
        flow=flow,
        block=flow.blocks[1],
        ctx=ctx,
        flow_token="jdbc_lookup",
        is_root=False,
        add_param=lambda name, value, sensitive: params.append((name, value, sensitive)),
    )

    lookup = next(p for p in builder.processors if p.key == "lookup")
    assert lookup.type == "org.apache.nifi.processors.standard.LookupRecord"
    assert lookup.properties["Lookup Service"] == "cs_db_lookup"
    assert lookup.properties["Result RecordPath"] == "/asset_id_lookup"
    assert lookup.properties["asset_id"] == "/asset_id"

    lookup_service = next(cs for cs in builder.controller_services if cs.key == "cs_db_lookup")
    assert lookup_service.type == "org.apache.nifi.lookup.db.DatabaseRecordLookupService"
    assert lookup_service.properties["Lookup Key Column"] == "asset_id"


def test_jdbc_read_mid_chain_is_rejected():
    from services.adapter.compiler.blocks_jdbc import _compile_read
    from services.adapter.compiler.ir import BlockBuilder

    flow = Flow(
        id="flow-jdbc-invalid", name="Jdbc Invalid Flow", cron="0 */2 * * *", state="Draft", enabled=True,
        createdAt="2026-01-01T00:00:00.000Z", updatedAt="2026-01-01T00:00:00.000Z",
        blocks=[
            FlowBlock(id="b-parent", adapter="http", mode="read", name="Root", parentId=None, serviceId=None, config={}),
            FlowBlock(
                id="b-read", adapter="jdbc", mode="read", name="Read Assets", parentId="b-parent", serviceId="svc-db",
                config={"table": "cmdb_assets"},
            ),
        ],
        topics=[], variables=[], servicePins={},
    )
    builder = BlockBuilder()
    with pytest.raises(CompileError, match="cannot be placed mid-chain"):
        _compile_read(
            builder,
            flow=flow,
            block=flow.blocks[1],
            ctx=jdbc_ctx(),
            flow_token="jdbc_invalid",
            is_root=False,
            add_param=lambda *args: None,
        )


def test_compile_flow_rejects_non_root_jdbc_read():
    flow = Flow(
        id="flow-jdbc-public", name="Jdbc Public Invalid Flow", cron="0 */2 * * *", state="Draft", enabled=True,
        createdAt="2026-01-01T00:00:00.000Z", updatedAt="2026-01-01T00:00:00.000Z",
        blocks=[
            FlowBlock(id="b-parent", adapter="http", mode="read", name="Root", parentId=None, serviceId="svc-http", config={"path": "/x"}),
            FlowBlock(
                id="b-read", adapter="jdbc", mode="read", name="Read Assets", parentId="b-parent", serviceId="svc-db",
                config={"table": "cmdb_assets"},
            ),
        ],
        topics=[], variables=[], servicePins={},
    )
    ctx = CompileContext(
        services={"svc-http": make_service(id="svc-http", type="http", name="X", config={"baseUrl": "https://x", "authMode": "none"})},
        connections={},
        gateway_proxies={},
        approved_schemas={},
    )
    with pytest.raises(CompileError, match="jdbc read is only legal as a root block"):
        compile_flow(flow, ctx)


def trino_flow() -> Flow:
    """Root jdbc read against a Trino lakehouse service -- exercises the
    catalog/schema-aware URL branch (`_ensure_db_pool`) for a fully-qualified
    Trino table."""
    return Flow(
        id="flow-trino", name="Trino Flow", cron="0 */2 * * *", state="Draft", enabled=True,
        createdAt="2026-01-01T00:00:00.000Z", updatedAt="2026-01-01T00:00:00.000Z",
        blocks=[
            FlowBlock(
                id="b-read", adapter="jdbc", mode="read", name="Read Assets", parentId=None, serviceId="svc-trino",
                config={"table": "gold.asset.asset__xref"},
            ),
        ],
        topics=[], variables=[], servicePins={},
    )


def trino_ctx(*, driver_locations: str | None = None) -> CompileContext:
    config = {"dialect": "trino", "url": "https://trino.datapasc.com",
              "username": "admin", "password": "s3cret"}
    if driver_locations is not None:
        config["driverLocations"] = driver_locations
    services = {
        "svc-trino": make_service(id="svc-trino", type="database", name="Lakehouse Trino", config=config, hasSecret=True),
    }
    connections = {"kafka": make_connection(id="conn-kafka", type="kafka", name="K", config={"bootstrapServers": "kafka:9092"})}
    return CompileContext(services=services, connections=connections, gateway_proxies={}, approved_schemas={})


def test_jdbc_trino_pool_uses_url_tls_and_table_catalog_schema():
    """Trino's coordinator URL and the block's fully-qualified table determine
    the JDBC URL; the processor itself receives the leaf table."""
    plan = compile_flow(trino_flow(), trino_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-read")

    pool = next(cs for cs in group.controllerServices if cs.key == "cs_db_pool")
    assert pool.type == "org.apache.nifi.dbcp.DBCPConnectionPool"
    assert pool.properties["Database Driver Class Name"] == "io.trino.jdbc.TrinoDriver"
    assert "Database Driver Locations" not in pool.properties

    url_param = next(p for p in plan.parameterContext.parameters if p.name == "svc_svc-trino_db_url")
    assert url_param.value == "jdbc:trino://trino.datapasc.com:443/gold/asset?SSL=true"
    query = next(p for p in group.processors if p.key == "query")
    assert query.properties["Table Name"] == "asset__xref"


def test_jdbc_trino_pool_sets_driver_locations_when_configured():
    """When the database service config carries an explicit `driverLocations`
    string, `_ensure_db_pool` sets `Database Driver Locations` on the
    DBCPConnectionPool verbatim -- needed for non-bundled drivers like
    Trino's (Publish3.json: `/opt/nifi/nifi-current/nar_extensions/
    trino-jdbc-480.jar`)."""
    jar_path = "/opt/nifi/nifi-current/nar_extensions/trino-jdbc-480.jar"
    plan = compile_flow(trino_flow(), trino_ctx(driver_locations=jar_path))
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-read")

    pool = next(cs for cs in group.controllerServices if cs.key == "cs_db_pool")
    assert pool.properties["Database Driver Locations"] == jar_path
    # Catalog/schema and TLS remain part of the URL when driverLocations is set.
    url_param = next(p for p in plan.parameterContext.parameters if p.name == "svc_svc-trino_db_url")
    assert url_param.value == "jdbc:trino://trino.datapasc.com:443/gold/asset?SSL=true"


def test_jdbc_trino_rejects_unqualified_table():
    flow = trino_flow()
    flow.blocks[0].config["table"] = "asset__xref"
    with pytest.raises(CompileError, match="catalog.schema.table"):
        compile_flow(flow, trino_ctx())


def test_jdbc_trino_passwordless_service_omits_empty_password_property():
    ctx = trino_ctx()
    ctx.services["svc-trino"].config.pop("password")
    plan = compile_flow(trino_flow(), ctx)
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-read")
    pool = next(cs for cs in group.controllerServices if cs.key == "cs_db_pool")

    assert "Password" not in pool.properties
    assert not any(p.name == "svc_svc-trino_db_password" for p in plan.parameterContext.parameters)


def test_jdbc_trino_incremental_uses_qualified_table_and_redis_bookmark():
    flow = trino_flow()
    flow.blocks[0].config.update({
        "incremental": True,
        "watermarkColumn": "updated_at",
        "initialPosition": "oldest",
    })
    ctx = trino_ctx()
    ctx.connections["redis"] = make_connection(
        id="conn-redis", type="redis", name="Redis", config={"host": "redis", "port": 6379, "bookmarksDb": 1}
    )

    plan = compile_flow(flow, ctx)
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-read")
    query_seed = next(p for p in group.processors if p.key == "bookmark_existing_query")

    assert "FROM gold.asset.asset__xref" in query_seed.properties["jdbc.query"]
    assert any(cs.key == "cs_jdbc_bookmark_cache" for cs in group.controllerServices)


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

    # M17: record-based splitting (SplitRecord + JsonTreeReader), never
    # SplitJson `$[*]` — a Kafka message is normally one JSON OBJECT, and
    # `$[*]` over an object shreds it into its scalar VALUES. SplitRecord
    # handles both object-shaped (1 record) and array-shaped (N records)
    # messages.
    split = next(p for p in group.processors if p.key == "split")
    assert split.type == "org.apache.nifi.processors.standard.SplitRecord"
    assert split.properties["Records Per Split"] == "1"
    assert split.properties["Record Reader"] == "cs_json_reader"
    assert split.properties["Record Writer"] == "cs_json_writer"
    assert "original" in split.autoTerminate
    dlq_from_split = [c for c in group.connections if c.from_ == "split" and c.to == "dlq"]
    assert dlq_from_split and dlq_from_split[0].relationships == ["failure"]
    # the record tail is SplitRecord's `splits` relationship
    consume_to_split = [c for c in group.connections if c.from_ == "consume" and c.to == "split"]
    assert consume_to_split and consume_to_split[0].relationships == ["success"]


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


def http_write_paginated_flow(
    *, write_forwards="response", ptype="offset", body_template='{"target": "USER"}', extra_fields=None,
) -> Flow:
    """A write-mode pagination fixture whose `bodyTemplate` holds only the
    caller's own payload -- no hand-typed `${offset}`/`${limit}`/`${page}`/
    `${page_size}` tokens. The compiler (`_auto_fill_pagination_body`) splices
    those on automatically from the same named fields the UI's offset/page
    pagination boxes already write, exactly like `_build_query` does for a
    read block's URL."""
    base_fields = {"limitValue": 500} if ptype == "offset" else {"sizeValue": 250}
    fields = {**base_fields, **(extra_fields or {})}
    return Flow(
        id="flow-hwrite-page", name="Http Write Paginated Flow", cron=None, state="Draft", enabled=True,
        createdAt="2026-01-01T00:00:00.000Z", updatedAt="2026-01-01T00:00:00.000Z",
        blocks=[
            FlowBlock(
                id="b-write", adapter="http", mode="write", name="Query CMDB", parentId=None, serviceId="svc-http",
                config={"method": "POST", "path": "/query/cmdb",
                        "bodyTemplate": body_template,
                        "writeForwards": write_forwards, "responseFormat": "json", "recordPath": "$.data[*]",
                        "split": True, "pagination": {"type": ptype, "fields": fields}},
            ),
            FlowBlock(
                id="b-continue", adapter="kafka", mode="write", name="Log Result", parentId="b-write",
                entity="cmdb_result", config={},
            ),
        ],
        topics=[], variables=[], servicePins={},
    )


def test_http_write_offset_pagination_seeds_counters_and_loops_to_render_body():
    plan = compile_flow(http_write_paginated_flow(), http_svc_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-write")
    keys = [p.key for p in group.processors]
    assert {"init", "render_body", "write", "split", "page_meta", "has_more", "next"} <= set(keys)

    init = next(p for p in group.processors if p.key == "init")
    assert init.properties["offset"] == "0"
    assert init.properties["limit"] == "500"
    assert init.properties["page_count"] == "1"

    # the user's own body is untouched apart from the two auto-appended
    # pagination fields -- no hand-typed ${offset}/${limit} in the fixture.
    render = next(p for p in group.processors if p.key == "render_body")
    assert render.properties["Replacement Value"] == '{"target": "USER", "offset": ${offset}, "limit": ${limit}}'

    next_proc = next(p for p in group.processors if p.key == "next")
    assert next_proc.properties["offset"] == "${offset:toNumber():plus(500)}"

    # loop-back target is render_body (re-renders the body from the updated
    # counters), not write directly -- write has no per-page state of its own.
    loop_edge = [c for c in group.connections if c.from_ == "next" and c.to == "render_body"]
    assert loop_edge and loop_edge[0].relationships == ["success"]

    # "original" no longer auto-terminates once pagination needs it for page_meta.
    split = next(p for p in group.processors if p.key == "split")
    assert "original" not in split.autoTerminate


def test_http_write_page_pagination_auto_fills_body_and_loops_to_render_body():
    plan = compile_flow(http_write_paginated_flow(ptype="page"), http_svc_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-write")
    keys = [p.key for p in group.processors]
    assert {"init", "render_body", "write", "split", "page_meta", "has_more", "next"} <= set(keys)

    init = next(p for p in group.processors if p.key == "init")
    assert init.properties["page"] == "1"
    assert init.properties["page_size"] == "250"
    assert init.properties["page_count"] == "1"

    render = next(p for p in group.processors if p.key == "render_body")
    assert render.properties["Replacement Value"] == '{"target": "USER", "page": ${page}, "size": ${page_size}}'

    next_proc = next(p for p in group.processors if p.key == "next")
    assert next_proc.properties["page"] == "${page:toNumber():plus(1)}"

    loop_edge = [c for c in group.connections if c.from_ == "next" and c.to == "render_body"]
    assert loop_edge and loop_edge[0].relationships == ["success"]


def test_http_write_pagination_body_field_collision_raises():
    # the body already hand-writes "offset" -- the same name the (default)
    # Offset parameter field would also splice in.
    flow = http_write_paginated_flow(body_template='{"target": "USER", "offset": 0}')
    with pytest.raises(CompileError, match="offset"):
        compile_flow(flow, http_svc_ctx())


def test_http_write_pagination_body_field_collision_raises_custom_param_name():
    # collision still fires when the pagination parameter is renamed away
    # from the default -- the guard matches on the configured field name.
    flow = http_write_paginated_flow(
        body_template='{"target": "USER", "start": 0}',
        extra_fields={"offsetParam": "start"},
    )
    with pytest.raises(CompileError, match="start"):
        compile_flow(flow, http_svc_ctx())


def test_http_write_offset_pagination_total_count_stop_from_body_path():
    flow = http_write_paginated_flow(
        extra_fields={"offsetStop": "total_count", "offsetTotalCountSource": "body",
                      "offsetTotalCountPath": "$.meta.total"},
    )
    plan = compile_flow(flow, http_svc_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-write")

    page_meta = next(p for p in group.processors if p.key == "page_meta")
    assert page_meta.type == "org.apache.nifi.processors.standard.EvaluateJsonPath"
    assert page_meta.properties["total_count"] == "$.meta.total"

    has_more = next(p for p in group.processors if p.key == "has_more")
    assert has_more.properties["continue"] == (
        "${total_count:isEmpty():or(${offset:toNumber():plus(500):lt(${total_count:toNumber()})})}"
    )


def test_http_write_offset_pagination_total_count_stop_from_header():
    flow = http_write_paginated_flow(
        extra_fields={"offsetStop": "total_count", "offsetTotalCountSource": "header",
                      "offsetTotalCountHeader": "X-Total-Count"},
    )
    plan = compile_flow(flow, http_svc_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-write")

    page_meta = next(p for p in group.processors if p.key == "page_meta")
    assert page_meta.type == "org.apache.nifi.processors.attributes.UpdateAttribute"
    assert page_meta.properties["total_count"] == (
        "${'X-Total-Count':replaceEmpty(${'x-total-count'})"
        ":replaceEmpty(${'invokehttp.response.header.X-Total-Count'})"
        ":replaceEmpty(${'invokehttp.response.header.x-total-count'})}"
    )

    has_more = next(p for p in group.processors if p.key == "has_more")
    assert has_more.properties["continue"] == (
        "${total_count:isEmpty():or(${offset:toNumber():plus(500):lt(${total_count:toNumber()})})}"
    )


def test_http_write_page_pagination_total_count_stop_from_body_path():
    flow = http_write_paginated_flow(
        ptype="page",
        extra_fields={"stop": "total_count", "totalCountSource": "body", "totalCountPath": "$.meta.total"},
    )
    plan = compile_flow(flow, http_svc_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-write")

    page_meta = next(p for p in group.processors if p.key == "page_meta")
    assert page_meta.type == "org.apache.nifi.processors.standard.EvaluateJsonPath"
    assert page_meta.properties["total_count"] == "$.meta.total"

    has_more = next(p for p in group.processors if p.key == "has_more")
    assert has_more.properties["continue"] == (
        "${total_count:isEmpty():or(${page_count:toNumber():multiply(250):lt(${total_count:toNumber()})})}"
    )


def test_http_write_page_pagination_total_count_stop_from_header():
    flow = http_write_paginated_flow(
        ptype="page",
        extra_fields={"stop": "total_count", "totalCountSource": "header", "totalCountHeader": "X-Total-Count"},
    )
    plan = compile_flow(flow, http_svc_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-write")

    page_meta = next(p for p in group.processors if p.key == "page_meta")
    assert page_meta.type == "org.apache.nifi.processors.attributes.UpdateAttribute"
    assert page_meta.properties["total_count"] == (
        "${'X-Total-Count':replaceEmpty(${'x-total-count'})"
        ":replaceEmpty(${'invokehttp.response.header.X-Total-Count'})"
        ":replaceEmpty(${'invokehttp.response.header.x-total-count'})}"
    )

    has_more = next(p for p in group.processors if p.key == "has_more")
    assert has_more.properties["continue"] == (
        "${total_count:isEmpty():or(${page_count:toNumber():multiply(250):lt(${total_count:toNumber()})})}"
    )


def test_http_write_pagination_requires_write_forwards_response():
    with pytest.raises(CompileError, match="writeForwards"):
        compile_flow(http_write_paginated_flow(write_forwards="original"), http_svc_ctx())


def test_http_write_offset_pagination_also_rides_in_the_query_string():
    """Counters must reach the URL, not just the body.

    FortiSIEM's `/query/cmdb` (verified live) ignores `start`/`size` in the POST
    body and paginates only off the query string: body `{"start": 0}` and
    `{"start": 3}` return byte-identical rows, while `?start=0` and `?start=3`
    return different pages. Body-only paging there re-fetches page 1 forever.
    """
    flow = http_write_paginated_flow(extra_fields={"offsetParam": "start", "limitParam": "size"})
    plan = compile_flow(flow, http_svc_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-write")

    write = next(p for p in group.processors if p.key == "write")
    assert write.properties["HTTP URL"] == (
        "#{svc_svc-http_base_url}/query/cmdb?start=${offset}&size=${limit}"
    )
    # ...and still in the body, for APIs that read it there instead.
    render = next(p for p in group.processors if p.key == "render_body")
    assert render.properties["Replacement Value"] == '{"target": "USER", "start": ${offset}, "size": ${limit}}'


def test_http_write_page_pagination_also_rides_in_the_query_string():
    plan = compile_flow(http_write_paginated_flow(ptype="page"), http_svc_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-write")
    write = next(p for p in group.processors if p.key == "write")
    assert write.properties["HTTP URL"] == (
        "#{svc_svc-http_base_url}/query/cmdb?page=${page}&size=${page_size}"
    )


def test_http_write_pagination_query_joins_an_existing_literal_query_with_ampersand():
    """A path that already carries its own `?...` must not grow a second `?`."""
    flow = http_write_paginated_flow()
    flow.blocks[0].config["path"] = "/query/cmdb?organization=Super"
    plan = compile_flow(flow, http_svc_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-write")
    write = next(p for p in group.processors if p.key == "write")
    assert write.properties["HTTP URL"] == (
        "#{svc_svc-http_base_url}/query/cmdb?organization=Super&offset=${offset}&limit=${limit}"
    )


def test_http_write_pagination_loop_restores_mime_type():
    """`next` must re-set `mime.type` on every iteration.

    `init` seeds it once, but the loop path carries the flowfile that came off
    `write`'s Response relationship — and InvokeHTTP overwrites `mime.type` there
    with the RESPONSE's Content-Type. Since the baseline sets
    "Request Content-Type": "${mime.type}", page 2 onward would otherwise POST a
    JSON body advertised as whatever the API replied with.
    """
    for ptype in ("offset", "page"):
        plan = compile_flow(http_write_paginated_flow(ptype=ptype), http_svc_ctx())
        group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-write")
        next_proc = next(p for p in group.processors if p.key == "next")
        assert next_proc.properties["mime.type"] == "application/json", ptype


def test_http_read_pagination_loop_does_not_set_mime_type():
    """The mime.type reset is write-only — read mode's `next` is unchanged."""
    plan = compile_flow(golden_flow(), golden_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-read")
    next_proc = next(p for p in group.processors if p.key == "next")
    assert "mime.type" not in next_proc.properties


@pytest.mark.parametrize("ptype", ["cursor", "next_url"])
def test_http_write_rejects_cursor_and_next_url_pagination(ptype):
    """Both compiled into a loop that never advanced.

    `next_url` set the `request.url` attribute, which only read mode's `fetch`
    reads — a write block's URL is the concrete `{base}{path}`. `cursor` never
    reached the body, because `_auto_fill_pagination_body` splices offset/page
    pairs only. Either way the request was byte-identical every iteration and the
    flow re-POSTed page 1 against the source API forever.
    """
    flow = http_write_paginated_flow(ptype=ptype)
    flow.blocks[0].config["pagination"] = {"type": ptype, "fields": {}}
    with pytest.raises(CompileError, match=ptype):
        compile_flow(flow, http_svc_ctx())


# --------------------------------------------------------------------------
# Columnar (FortiSIEM `/query/cmdb`-style) response transform
# --------------------------------------------------------------------------


def http_columnar_read_flow(*, ptype="none", extra_fields=None, split=True, columns=None) -> Flow:
    fields = {"limitValue": 500, **(extra_fields or {})}
    return Flow(
        id="flow-hcolumnar", name="Http Columnar Read Flow", cron="0 * * * *", state="Draft", enabled=True,
        createdAt="2026-01-01T00:00:00.000Z", updatedAt="2026-01-01T00:00:00.000Z",
        blocks=[
            FlowBlock(
                id="b-read", adapter="http", mode="read", name="Query CMDB", parentId=None, serviceId="svc-http",
                config={"method": "GET", "path": "/query/cmdb", "responseFormat": "json", "recordPath": "$.data[*]",
                        "split": split, "pagination": {"type": ptype, "fields": fields},
                        "columnar": {"enabled": True, "rowsField": "data",
                                     "columns": columns if columns is not None else ["name", "ip Address", "1status"]}},
            ),
        ],
        topics=[], variables=[], servicePins={},
    )


def test_http_columnar_read_inserts_jolt_before_split_with_sanitized_names():
    plan = compile_flow(http_columnar_read_flow(), http_svc_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-read")
    keys = [p.key for p in group.processors]
    assert keys.index("fetch") < keys.index("columnar_transform") < keys.index("split")

    transform = next(p for p in group.processors if p.key == "columnar_transform")
    assert transform.type == "org.apache.nifi.processors.jolt.JoltTransformJSON"
    assert transform.properties["Jolt Transform"] == "jolt-transform-shift"
    spec = json.loads(transform.properties["Jolt Specification"])
    # "ip Address" -> "ip_Address" (space sanitized); "1status" doesn't start
    # with a letter/underscore -> prefixed with its column index.
    assert spec == {"data": {"*": {"0": "[&1].name", "1": "[&1].ip_Address", "2": "[&1].col_2_1status"}}}

    fetch_to_transform = [c for c in group.connections if c.from_ == "fetch" and c.to == "columnar_transform"]
    assert fetch_to_transform and fetch_to_transform[0].relationships == ["Response"]

    split = next(p for p in group.processors if p.key == "split")
    assert split.properties["JsonPath Expression"] == "$.[*]"
    transform_to_split = [c for c in group.connections if c.from_ == "columnar_transform" and c.to == "split"]
    assert transform_to_split and transform_to_split[0].relationships == ["success"]

    dlq_from_transform = [c for c in group.connections if c.from_ == "columnar_transform" and c.to == "dlq"]
    assert dlq_from_transform and dlq_from_transform[0].relationships == ["failure"]


def test_http_columnar_read_pagination_probe_reads_raw_pre_jolt_response():
    # empty_response stop condition (default): page_meta's probe must read
    # the RAW $.data[*] path off the un-transformed response, not the Jolt
    # output -- the Jolt shift only keeps "data", but more importantly the
    # probe/JSONPath here is evaluated against content that still has its
    # original shape (bare row-arrays), which is exactly why this must be
    # forked off BEFORE the transform rather than off the split's "original".
    plan = compile_flow(http_columnar_read_flow(ptype="offset"), http_svc_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-read")

    fetch_to_page_meta = [c for c in group.connections if c.from_ == "fetch" and c.to == "page_meta"]
    assert fetch_to_page_meta and fetch_to_page_meta[0].relationships == ["Response"]
    # page_meta is NOT fed from the split's "original" relationship when columnar is active.
    split_to_page_meta = [c for c in group.connections if c.from_ == "split" and c.to == "page_meta"]
    assert not split_to_page_meta

    probe = next(p for p in group.processors if p.key == "page_meta")
    assert probe.properties["probe"] == "$.data[0]"

    split = next(p for p in group.processors if p.key == "split")
    assert "original" in split.autoTerminate  # nothing consumes it -- page_meta reads pre-Jolt instead


def test_http_columnar_write_total_count_stop_reads_raw_totalcount_field():
    # The real FortiSIEM shape: POST /query/cmdb pagination (offset/limit
    # auto-filled body) PLUS a columnar response whose totalCount sibling
    # key the Jolt shift would otherwise drop.
    flow = http_write_paginated_flow(
        ptype="offset", extra_fields={"offsetStop": "total_count", "offsetTotalCountPath": "$.totalCount"},
    )
    block = flow.blocks[0]
    block.config["columnar"] = {"enabled": True, "rowsField": "data", "columns": ["name", "status"]}
    block.config["recordPath"] = "$.data[*]"

    plan = compile_flow(flow, http_svc_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-write")

    write_to_page_meta = [c for c in group.connections if c.from_ == "write" and c.to == "page_meta"]
    assert write_to_page_meta and write_to_page_meta[0].relationships == ["Response"]

    page_meta = next(p for p in group.processors if p.key == "page_meta")
    assert page_meta.properties["total_count"] == "$.totalCount"

    split = next(p for p in group.processors if p.key == "split")
    assert split.properties["JsonPath Expression"] == "$.[*]"

    transform = next(p for p in group.processors if p.key == "columnar_transform")
    assert transform.type == "org.apache.nifi.processors.jolt.JoltTransformJSON"


def test_http_columnar_requires_split():
    with pytest.raises(CompileError, match="split into records"):
        compile_flow(http_columnar_read_flow(split=False), http_svc_ctx())


def test_http_columnar_requires_at_least_one_column():
    with pytest.raises(CompileError, match="at least one column"):
        compile_flow(http_columnar_read_flow(columns=[]), http_svc_ctx())


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
# 10b. http read: chained (non-root) read promotes parent-record fields
# --------------------------------------------------------------------------
#
# Regression coverage for a bug where a non-root http-read block's `path`
# could reference a parent record field (e.g. "/sites/${site_id}/assets")
# but the field was never extracted from the incoming record into a
# flowfile attribute -- so the URL silently compiled with an empty value.
# _compile_write and _compile_lookup already did this extraction; compile_read
# did not. Two cases matter because they freeze the URL at different points:
#   - pagination "page" (and offset/cursor): the template lives directly on
#     `fetch`'s own "HTTP URL" property, evaluated fresh per FlowFile.
#   - pagination "none" (and next_url): `init` evaluates the template ONCE
#     and freezes the result into the `request.url` ATTRIBUTE -- so the
#     field must already be a resolvable attribute by the time `init` runs,
#     not merely by the time `fetch` runs.


def http_chained_read_flow(*, child_pagination: dict) -> Flow:
    return Flow(
        id="flow-hchain", name="Http Chained Read Flow", cron="0 * * * *", state="Draft", enabled=True,
        createdAt="2026-01-01T00:00:00.000Z", updatedAt="2026-01-01T00:00:00.000Z",
        blocks=[
            FlowBlock(
                id="b-site", adapter="http", mode="read", name="List Sites", parentId=None, serviceId="svc-http",
                config={"method": "GET", "path": "/sites", "responseFormat": "json", "recordPath": "$[*]",
                        "split": True, "pagination": {"type": "none", "fields": {}}},
            ),
            FlowBlock(
                id="b-asset", adapter="http", mode="read", name="List Site Assets", parentId="b-site",
                serviceId="svc-http",
                config={"method": "GET", "path": "/sites/${site_id}/assets", "responseFormat": "json",
                        "recordPath": "$[*]", "split": True, "pagination": child_pagination},
            ),
        ],
        topics=[], variables=[], servicePins={},
    )


def test_http_read_chained_child_promotes_field_paginated():
    """pagination: page -- the template lives on fetch's own property."""
    plan = compile_flow(
        http_chained_read_flow(child_pagination={"type": "page", "fields": {"pageParam": "page", "sizeParam": "size", "sizeValue": "50"}}),
        http_svc_ctx(),
    )
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-asset")
    keys = [p.key for p in group.processors]
    assert keys.index("extract_path_fields") < keys.index("init") < keys.index("fetch")

    extract = next(p for p in group.processors if p.key == "extract_path_fields")
    assert extract.properties["site_id"] == "$.site_id"
    gate = next(p for p in group.processors if p.key == "extract_path_fields__check_0")
    assert gate.type == "org.apache.nifi.processors.standard.RouteOnAttribute"
    assert gate.properties["present"] == "${site_id:isEmpty():not()}"
    assert gate.properties["missing"] == "${site_id:isEmpty()}"
    entry_link = [c for c in group.connections if c.from_ == "inputPort" and c.to == "extract_path_fields__check_0"]
    assert entry_link and entry_link[0].relationships == []
    present_to_merge = [c for c in group.connections
                        if c.from_ == "extract_path_fields__check_0" and c.to == "extract_path_fields__merge_0"]
    missing_to_extract = [c for c in group.connections
                          if c.from_ == "extract_path_fields__check_0" and c.to == "extract_path_fields"]
    extract_to_merge = [c for c in group.connections
                        if c.from_ == "extract_path_fields" and c.to == "extract_path_fields__merge_0"]
    merge_to_init = [c for c in group.connections
                     if c.from_ == "extract_path_fields__merge_0" and c.to == "init"]
    assert present_to_merge and present_to_merge[0].relationships == ["present"]
    assert missing_to_extract and missing_to_extract[0].relationships == ["missing"]
    assert extract_to_merge and extract_to_merge[0].relationships == ["matched"]
    assert merge_to_init and merge_to_init[0].relationships == ["success"]

    fetch = next(p for p in group.processors if p.key == "fetch")
    assert fetch.properties["HTTP URL"] == "#{svc_svc-http_base_url}/sites/${site_id}/assets?page=${page}&size=${page_size}"


def test_http_read_chained_child_promotes_field_unpaginated():
    """pagination: none -- init freezes the URL into request.url, so the
    field must be extracted BEFORE init, not merely before fetch."""
    plan = compile_flow(
        http_chained_read_flow(child_pagination={"type": "none", "fields": {}}),
        http_svc_ctx(),
    )
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-asset")
    keys = [p.key for p in group.processors]
    assert keys.index("extract_path_fields") < keys.index("init") < keys.index("fetch")

    init = next(p for p in group.processors if p.key == "init")
    assert init.properties["request.url"] == "#{svc_svc-http_base_url}/sites/${site_id}/assets"

    fetch = next(p for p in group.processors if p.key == "fetch")
    assert fetch.properties["HTTP URL"] == "${request.url}"


def test_http_read_root_rejects_path_field_reference():
    """A root read has no incoming record to extract from -- a ${field} in
    its path is a config error, not a silently-empty URL."""
    flow = Flow(
        id="flow-hroot-bad", name="Bad Root Flow", cron="0 * * * *", state="Draft", enabled=True,
        createdAt="2026-01-01T00:00:00.000Z", updatedAt="2026-01-01T00:00:00.000Z",
        blocks=[
            FlowBlock(
                id="b-root", adapter="http", mode="read", name="Bad Root", parentId=None, serviceId="svc-http",
                config={"method": "GET", "path": "/sites/${site_id}/assets", "responseFormat": "json",
                        "recordPath": "$[*]", "split": True, "pagination": {"type": "none", "fields": {}}},
            ),
        ],
        topics=[], variables=[], servicePins={},
    )
    with pytest.raises(CompileError, match="site_id"):
        compile_flow(flow, http_svc_ctx())


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
    # the promised split tail (SplitRecord `splits`) feeds the output port,
    # not an auto-terminate
    split = next(p for p in read_group.processors if p.key == "split")
    assert "splits" not in split.autoTerminate


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


# --------------------------------------------------------------------------
# 12. C1 — cron translation (5-field UTC -> NiFi/Quartz 6-field)
# --------------------------------------------------------------------------


def test_cron_presets_translate_to_quartz():
    from services.adapter.compiler.transforms import cron_or_period
    from services.adapter.naming import CRON_PRESETS

    expected = {
        "*/5 * * * *": "0 */5 * * * ?",
        "*/15 * * * *": "0 */15 * * * ?",
        "0 * * * *": "0 0 * * * ?",
        "0 */6 * * *": "0 0 */6 * * ?",
        "0 2 * * *": "0 0 2 * * ?",
        # Weekly Mon: standard-cron DOW 1 (Monday) -> Quartz 2 (1=Sunday)
        "0 6 * * 1": "0 0 6 ? * 2",
    }
    # every UI preset is covered by this table
    assert {p["value"] for p in CRON_PRESETS} == set(expected)
    for cron, quartz in expected.items():
        period, strategy = cron_or_period(cron)
        assert strategy == "CRON_DRIVEN"
        assert period == quartz, f"{cron!r} -> {period!r}, expected {quartz!r}"


def test_cron_dow_conversion_ranges_lists_steps_and_dom():
    from services.adapter.compiler.transforms import cron_or_period

    # DOW range 1-5 (Mon-Fri) -> 2-6, DOM becomes ?
    assert cron_or_period("0 6 * * 1-5")[0] == "0 0 6 ? * 2-6"
    # list incl. Sunday-as-0 and Sunday-as-7 (both -> Quartz 1)
    assert cron_or_period("0 6 * * 0,3,7")[0] == "0 0 6 ? * 1,4,1"
    # a step count is NOT a day value and never shifts
    assert cron_or_period("0 6 * * 1-5/2")[0] == "0 0 6 ? * 2-6/2"
    assert cron_or_period("0 6 * * */2")[0] == "0 0 6 ? * */2"
    # named days pass through (same meaning in both dialects)
    assert cron_or_period("0 6 * * MON")[0] == "0 0 6 ? * MON"
    # DOM specified, DOW wildcard -> DOW becomes ?
    assert cron_or_period("30 4 15 * *")[0] == "0 30 4 15 * ?"
    # both specified -> DOW wins, DOM becomes ? (Quartz forbids both)
    assert cron_or_period("0 2 15 * 1")[0] == "0 0 2 ? * 2"
    # no cron -> timer fallback unchanged
    assert cron_or_period(None) == ("1 hour", "TIMER_DRIVEN")


# --------------------------------------------------------------------------
# 13. C4 — pagination: EL evaluated where NiFi evaluates it (all 4 styles)
# --------------------------------------------------------------------------


def _paginated_read_flow(pagination: dict) -> Flow:
    return Flow(
        id="flow-pg", name="Pg Flow", cron="0 * * * *", state="Draft", enabled=True,
        createdAt="2026-01-01T00:00:00.000Z", updatedAt="2026-01-01T00:00:00.000Z",
        blocks=[
            FlowBlock(
                id="b-read", adapter="http", mode="read", name="Read", parentId=None, serviceId="svc-http",
                config={"method": "GET", "path": "/items", "responseFormat": "json", "recordPath": "$.items[*]",
                        "split": True, "pagination": pagination},
            ),
        ],
        topics=[], variables=[], servicePins={},
    )


def _read_group(plan):
    return next(g for g in plan.rootGroup.childGroups if g.blockId == "b-read")


def _paginated_read_flow_with_path(pagination: dict, path: str) -> Flow:
    flow = _paginated_read_flow(pagination)
    flow.blocks[0].config["path"] = path
    return flow


def test_pagination_offset_template_on_fetch_url():
    plan = compile_flow(_paginated_read_flow(
        {"type": "offset", "fields": {"offsetParam": "offset", "limitParam": "limit", "limitValue": "30"}}
    ), http_svc_ctx())
    group = _read_group(plan)
    fetch = next(p for p in group.processors if p.key == "fetch")
    # the EL lives on the InvokeHTTP property, evaluated per FlowFile
    assert fetch.properties["HTTP URL"] == "#{svc_svc-http_base_url}/items?offset=${offset}&limit=${limit}"
    init = next(p for p in group.processors if p.key == "init")
    assert "request.url" not in init.properties  # no frozen URL string anywhere
    assert init.properties["offset"] == "0"
    assert init.properties["limit"] == "30"
    nxt = next(p for p in group.processors if p.key == "next")
    # `next` recomputes the literal counter AT the UpdateAttribute
    assert nxt.properties["offset"] == "${offset:toNumber():plus(30)}"
    assert any(c.from_ == "next" and c.to == "fetch" for c in group.connections)  # loop re-entry


def test_pagination_page_template_on_fetch_url():
    plan = compile_flow(_paginated_read_flow(
        {"type": "page", "fields": {"pageParam": "page", "sizeParam": "size", "firstPage": "1", "sizeValue": "50"}}
    ), http_svc_ctx())
    group = _read_group(plan)
    fetch = next(p for p in group.processors if p.key == "fetch")
    assert fetch.properties["HTTP URL"] == "#{svc_svc-http_base_url}/items?page=${page}&size=${page_size}"
    init = next(p for p in group.processors if p.key == "init")
    assert "request.url" not in init.properties
    assert init.properties["page"] == "1"
    assert init.properties["page_size"] == "50"
    nxt = next(p for p in group.processors if p.key == "next")
    assert nxt.properties["page"] == "${page:toNumber():plus(1)}"


def test_pagination_cursor_template_on_fetch_url():
    plan = compile_flow(_paginated_read_flow(
        {"type": "cursor", "fields": {"cursorParam": "cursor", "cursorPath": "$.meta.next"}}
    ), http_svc_ctx())
    group = _read_group(plan)
    fetch = next(p for p in group.processors if p.key == "fetch")
    assert fetch.properties["HTTP URL"] == "#{svc_svc-http_base_url}/items?cursor=${cursor}"
    init = next(p for p in group.processors if p.key == "init")
    assert "request.url" not in init.properties
    assert init.properties["cursor"] == ""
    page_meta = next(p for p in group.processors if p.key == "page_meta")
    assert page_meta.properties["next_cursor"] == "$.meta.next"
    nxt = next(p for p in group.processors if p.key == "next")
    assert nxt.properties["cursor"] == "${next_cursor}"


def test_pagination_cursor_with_size_on_fetch_url():
    plan = compile_flow(_paginated_read_flow(
        {"type": "cursor", "fields": {"cursorParam": "cursor", "cursorPath": "$.meta.next", "sizeValue": "100"}}
    ), http_svc_ctx())
    group = _read_group(plan)
    fetch = next(p for p in group.processors if p.key == "fetch")
    assert fetch.properties["HTTP URL"] == "#{svc_svc-http_base_url}/items?cursor=${cursor}&limit=${page_size}"
    init = next(p for p in group.processors if p.key == "init")
    assert init.properties["page_size"] == "100"


def test_pagination_cursor_with_custom_size_param_name():
    plan = compile_flow(_paginated_read_flow(
        {"type": "cursor", "fields": {"cursorParam": "cursor", "sizeParam": "pageSize", "sizeValue": "25"}}
    ), http_svc_ctx())
    group = _read_group(plan)
    fetch = next(p for p in group.processors if p.key == "fetch")
    assert fetch.properties["HTTP URL"] == "#{svc_svc-http_base_url}/items?cursor=${cursor}&pageSize=${page_size}"
    init = next(p for p in group.processors if p.key == "init")
    assert init.properties["page_size"] == "25"


def test_pagination_cursor_with_static_query_filter_in_path():
    """A block `path` may embed its own literal "?..." query (e.g. a static
    date-math lookback filter) ahead of cursor pagination's own params — the
    two must join with "&", not clash on a second "?"."""
    plan = compile_flow(_paginated_read_flow_with_path(
        {"type": "cursor", "fields": {"cursorParam": "cursor", "cursorPath": "$.meta.next", "sizeValue": "100"}},
        "/items?updatedAt__gte=${now():toNumber():minus(3600000)}",
    ), http_svc_ctx())
    group = _read_group(plan)
    fetch = next(p for p in group.processors if p.key == "fetch")
    assert fetch.properties["HTTP URL"] == (
        "#{svc_svc-http_base_url}/items?updatedAt__gte=${now():toNumber():minus(3600000)}"
        "&cursor=${cursor}&limit=${page_size}"
    )


def test_pagination_offset_page_meta_probe_return_type_json():
    """R2-D1 regression (journey-r-reverify.md): the offset/page probe
    (`$.items[0]`) evaluates to a JSON OBJECT — EvaluateJsonPath with
    `Return Type: scalar` routes any non-scalar result to `failure`
    (attribute destination), so the continuation check never ran: every
    offset/page-paginated read froze after page 1 AND fabricated the raw
    page into the DLQ on every run. Proven live (30-and-frozen), then
    194/194 with `Return Type: json`."""
    plan = compile_flow(_paginated_read_flow(
        {"type": "offset", "fields": {"offsetParam": "offset", "limitParam": "limit", "limitValue": "30"}}
    ), http_svc_ctx())
    page_meta = next(p for p in _read_group(plan).processors if p.key == "page_meta")
    assert page_meta.properties["Return Type"] == "json"
    assert page_meta.properties["probe"] == "$.items[0]"  # object-valued probe


def test_pagination_page_page_meta_probe_return_type_json():
    """Same R2-D1 regression guard for the `page` style — it shares the
    offset branch's object-valued probe in `_build_pagination`."""
    plan = compile_flow(_paginated_read_flow(
        {"type": "page", "fields": {"pageParam": "page", "sizeParam": "size", "firstPage": "1", "sizeValue": "50"}}
    ), http_svc_ctx())
    page_meta = next(p for p in _read_group(plan).processors if p.key == "page_meta")
    assert page_meta.properties["Return Type"] == "json"


def test_pagination_cursor_and_next_url_page_meta_stay_scalar():
    """The cursor/next_url probes (`next_cursor`/`next_url`) genuinely ARE
    scalars — the R2-D1 fix must not leak into those branches."""
    plan = compile_flow(_paginated_read_flow(
        {"type": "cursor", "fields": {"cursorParam": "cursor", "cursorPath": "$.meta.next"}}
    ), http_svc_ctx())
    page_meta = next(p for p in _read_group(plan).processors if p.key == "page_meta")
    assert page_meta.properties["Return Type"] == "scalar"

    plan = compile_flow(_paginated_read_flow({"type": "next_url", "fields": {"urlPath": "$.next"}}), http_svc_ctx())
    page_meta = next(p for p in _read_group(plan).processors if p.key == "page_meta")
    assert page_meta.properties["Return Type"] == "scalar"


def test_pagination_next_url_keeps_request_url_attribute():
    plan = compile_flow(_paginated_read_flow(
        {"type": "next_url", "fields": {"urlPath": "$.next"}}
    ), http_svc_ctx())
    group = _read_group(plan)
    fetch = next(p for p in group.processors if p.key == "fetch")
    # next_url has no counter placeholder: the concrete URL is stored and
    # `next` overwrites it with the server-given absolute URL each iteration.
    assert fetch.properties["HTTP URL"] == "${request.url}"
    init = next(p for p in group.processors if p.key == "init")
    assert init.properties["request.url"] == "#{svc_svc-http_base_url}/items"
    nxt = next(p for p in group.processors if p.key == "next")
    assert nxt.properties["request.url"] == "${next_url}"


def test_pagination_cursor_uses_ui_page_size_field_names():
    plan = compile_flow(_paginated_read_flow({
        "type": "cursor",
        "fields": {
            "cursorParam": "after",
            "cursorSizeParam": "limit",
            "cursorSizeValue": "5",
            "cursorSource": "body",
            "cursorPath": "$.cursor",
            "maxPages": "2",
        },
    }), http_svc_ctx())
    group = _read_group(plan)
    fetch = next(p for p in group.processors if p.key == "fetch")
    init = next(p for p in group.processors if p.key == "init")
    has_more = next(p for p in group.processors if p.key == "has_more")
    assert fetch.properties["HTTP URL"].endswith("?after=${cursor}&limit=${page_size}")
    assert init.properties["page_size"] == "5"
    assert init.properties["page_count"] == "1"
    assert has_more.properties["continue"] == (
        "${next_cursor:trim():isEmpty():not():and(${page_count:toNumber():lt(2)})}"
    )


def test_pagination_has_more_body_flag_is_compiled_for_page():
    plan = compile_flow(_paginated_read_flow({
        "type": "page",
        "fields": {
            "pageParam": "page", "sizeParam": "pagesize", "sizeValue": "100",
            "stop": "has_more", "hasMoreSource": "body", "hasMorePath": "$.has_more",
            "maxPages": "10",
        },
    }), http_svc_ctx())
    group = _read_group(plan)
    page_meta = next(p for p in group.processors if p.key == "page_meta")
    has_more = next(p for p in group.processors if p.key == "has_more")
    assert page_meta.properties["has_more_flag"] == "$.has_more"
    assert has_more.properties["continue"] == (
        "${has_more_flag:equals('false'):not():and(${page_count:toNumber():lt(10)})}"
    )


def test_pagination_has_more_header_flag_is_compiled_for_offset():
    plan = compile_flow(_paginated_read_flow({
        "type": "offset",
        "fields": {
            "limitValue": "25", "offsetStop": "has_more", "hasMoreSource": "header",
            "hasMoreHeader": "X-Has-More", "maxPages": "4",
        },
    }), http_svc_ctx())
    group = _read_group(plan)
    page_meta = next(p for p in group.processors if p.key == "page_meta")
    next_page = next(p for p in group.processors if p.key == "next")
    assert page_meta.type == "org.apache.nifi.processors.attributes.UpdateAttribute"
    assert "X-Has-More" in page_meta.properties["has_more_flag"]
    assert next_page.properties["X-Has-More"] == ""
    assert next_page.properties["invokehttp.response.header.x-has-more"] == ""


def test_pagination_cursor_header_uses_ui_header_field_name():
    plan = compile_flow(_paginated_read_flow({
        "type": "cursor",
        "fields": {"cursorParam": "cursor", "cursorSource": "header", "cursorHeader": "X-Cursor"},
    }), http_svc_ctx())
    group = _read_group(plan)
    page_meta = next(p for p in group.processors if p.key == "page_meta")
    next_page = next(p for p in group.processors if p.key == "next")
    assert page_meta.type == "org.apache.nifi.processors.attributes.UpdateAttribute"
    assert "X-Cursor" in page_meta.properties["next_cursor"]
    assert next_page.properties["X-Cursor"] == ""


def test_pagination_next_url_uses_ui_body_path():
    plan = compile_flow(_paginated_read_flow({
        "type": "next_url",
        "fields": {"nextUrlSource": "body", "nextUrlPath": "$.paging.next"},
    }), http_svc_ctx())
    page_meta = next(p for p in _read_group(plan).processors if p.key == "page_meta")
    assert page_meta.properties["next_url"] == "$.paging.next"


def test_pagination_next_url_raw_header_and_link_header_compile():
    raw_plan = compile_flow(_paginated_read_flow({
        "type": "next_url",
        "fields": {"nextUrlSource": "header", "nextUrlHeader": "X-Next-Url"},
    }), http_svc_ctx())
    raw_group = _read_group(raw_plan)
    raw_meta = next(p for p in raw_group.processors if p.key == "page_meta")
    raw_next = next(p for p in raw_group.processors if p.key == "next")
    assert raw_meta.type == "org.apache.nifi.processors.attributes.UpdateAttribute"
    assert "X-Next-Url" in raw_meta.properties["next_url"]
    assert raw_next.properties["X-Next-Url"] == ""
    assert raw_next.properties["invokehttp.response.header.x-next-url"] == ""

    link_plan = compile_flow(_paginated_read_flow({
        "type": "next_url",
        "fields": {"nextUrlSource": "link_header", "linkRel": "next", "maxPages": "4"},
    }), http_svc_ctx())
    link_group = _read_group(link_plan)
    link_meta = next(p for p in link_group.processors if p.key == "page_meta")
    link_route = next(p for p in link_group.processors if p.key == "has_more")
    link_next = next(p for p in link_group.processors if p.key == "next")
    assert link_meta.type == "org.apache.nifi.processors.attributes.UpdateAttribute"
    assert "rel=" in link_meta.properties["next_url"]
    assert "replaceAll" in link_meta.properties["next_url"]
    assert link_route.properties["continue"].endswith(":and(${page_count:toNumber():lt(4)})}")
    assert link_next.properties["Link"] == ""
    assert link_next.properties["invokehttp.response.header.link"] == ""


def test_pagination_invalid_max_pages_is_refused_by_compiler():
    with pytest.raises(CompileError, match="maximum pages"):
        compile_flow(_paginated_read_flow({
            "type": "next_url", "fields": {"nextUrlSource": "body", "maxPages": "0"},
        }), http_svc_ctx())


# --------------------------------------------------------------------------
# 14. M4 — extract `default` materialized via UpdateAttribute
# --------------------------------------------------------------------------


def _single_read_flow_with_transforms(transforms) -> Flow:
    return Flow(
        id="flow-tx", name="Tx Flow", cron="0 * * * *", state="Draft", enabled=True,
        createdAt="2026-01-01T00:00:00.000Z", updatedAt="2026-01-01T00:00:00.000Z",
        blocks=[
            FlowBlock(
                id="b-read", adapter="http", mode="read", name="Read", parentId=None, serviceId="svc-http",
                config={"method": "GET", "path": "/items", "responseFormat": "json", "recordPath": "$.items[*]",
                        "split": True, "pagination": {"type": "none", "fields": {}}},
                transforms=transforms,
            ),
        ],
        topics=[], variables=[], servicePins={},
    )


def test_extract_default_materialized_as_update_attribute():
    flow = _single_read_flow_with_transforms([
        TransformRule(id="t-1", kind="extract", config={"attribute": "status", "path": "$.status", "default": "unknown"}),
    ])
    plan = compile_flow(flow, http_svc_ctx())
    group = _read_group(plan)

    extract = next(p for p in group.processors if p.key == "t0__extract")
    # M4: EvaluateJsonPath dynamic properties are JsonPaths only — the old
    # "Default Value (informational)" property made the processor invalid.
    assert not any("informational" in k.lower() for k in extract.properties)

    dflt = next(p for p in group.processors if p.key == "t0__extract__default")
    assert dflt.type == "org.apache.nifi.processors.attributes.UpdateAttribute"
    assert dflt.properties["status"] == "${status:isEmpty():ifElse('unknown', ${status})}"
    link = [c for c in group.connections if c.from_ == "t0__extract" and c.to == "t0__extract__default"]
    assert link and link[0].relationships == ["matched"]
    # the default step IS the new tail (childless flow -> auto-terminated)
    assert "success" in dflt.autoTerminate


def test_extract_without_default_adds_no_extra_processor():
    flow = _single_read_flow_with_transforms([
        TransformRule(id="t-1", kind="extract", config={"attribute": "status", "path": "$.status"}),
    ])
    plan = compile_flow(flow, http_svc_ctx())
    group = _read_group(plan)
    assert not any(p.key == "t0__extract__default" for p in group.processors)
    extract = next(p for p in group.processors if p.key == "t0__extract")
    assert "matched" in {r for c in group.connections if c.from_ == "t0__extract" for r in c.relationships} or (
        "matched" in extract.autoTerminate
    )


# --------------------------------------------------------------------------
# 15. M5 — coerce: RecordPath property name, field.value EL cast
# --------------------------------------------------------------------------


def test_coerce_recordpath_name_and_field_value_el():
    flow = _single_read_flow_with_transforms([
        TransformRule(id="t-1", kind="coerce", config={"field": "age", "type": "integer"}),
    ])
    plan = compile_flow(flow, http_svc_ctx())
    group = _read_group(plan)
    coerce = next(p for p in group.processors if p.key == "t0__coerce")
    assert coerce.type == "org.apache.nifi.processors.standard.UpdateRecord"
    assert coerce.properties["Replacement Value Strategy"] == "literal-value"
    # dynamic property NAME is a RecordPath; VALUE recomputes the field via
    # UpdateRecord's own `field.value` EL variable
    assert coerce.properties["/age"] == "${field.value:toNumber()}"
    assert not any("informational" in k.lower() for k in coerce.properties)


def test_coerce_el_per_target_type():
    from services.adapter.compiler.transforms import _coerce_el

    assert _coerce_el("integer") == "${field.value:toNumber()}"
    assert _coerce_el("number") == "${field.value:toNumber()}"
    assert _coerce_el("double") == "${field.value:toDecimal()}"
    assert _coerce_el("boolean") == "${field.value:toLower():equals('true')}"
    assert _coerce_el("string") == "${field.value:toString()}"
    assert _coerce_el("something_else") == "${field.value}"


# --------------------------------------------------------------------------
# 16. M15 — dedup requires a per-record stream
# --------------------------------------------------------------------------


def _dedup_rule() -> TransformRule:
    return TransformRule(id="t-d", kind="dedup",
                         config={"identityFields": ["id"], "excludedFields": [], "windowHours": 24})


def test_dedup_on_unsplit_http_collection_read_refused():
    from services.adapter.validation import validate_flow

    flow = Flow(
        id="flow-nosplit", name="NoSplit Flow", cron="0 * * * *", state="Draft", enabled=True,
        createdAt="2026-01-01T00:00:00.000Z", updatedAt="2026-01-01T00:00:00.000Z",
        blocks=[
            FlowBlock(
                id="b-read", adapter="http", mode="read", name="Read", parentId=None, serviceId="svc-http",
                config={"method": "GET", "path": "/items", "responseFormat": "json", "recordPath": "$.items[*]",
                        "split": False, "pagination": {"type": "none", "fields": {}}},
                transforms=[_dedup_rule()],
            ),
            FlowBlock(id="b-write", adapter="kafka", mode="write", name="Out", parentId="b-read", entity="e", config={}),
        ],
        topics=[], variables=[], servicePins={},
    )
    ctx = http_svc_ctx()

    # validation surfaces it as an issue...
    issues = validate_flow(flow, list(ctx.services.values()), [], None)
    assert any("one record per FlowFile" in i.message for i in issues)
    # ...and the compiler refuses outright (defense in depth)
    with pytest.raises(CompileError, match="one record per FlowFile"):
        compile_flow(flow, ctx)


def test_dedup_downstream_of_unsplit_collection_read_also_refused():
    flow = Flow(
        id="flow-nosplit2", name="NoSplit Flow 2", cron="0 * * * *", state="Draft", enabled=True,
        createdAt="2026-01-01T00:00:00.000Z", updatedAt="2026-01-01T00:00:00.000Z",
        blocks=[
            FlowBlock(
                id="b-read", adapter="http", mode="read", name="Read", parentId=None, serviceId="svc-http",
                config={"method": "GET", "path": "/items", "responseFormat": "json", "recordPath": "$.items[*]",
                        "split": False, "pagination": {"type": "none", "fields": {}}},
            ),
            FlowBlock(id="b-write", adapter="kafka", mode="write", name="Out", parentId="b-read", entity="e",
                      config={}, transforms=[_dedup_rule()]),
        ],
        topics=[], variables=[], servicePins={},
    )
    with pytest.raises(CompileError, match="one record per FlowFile"):
        compile_flow(flow, http_svc_ctx())


def test_dedup_on_split_http_read_allowed():
    flow = _single_read_flow_with_transforms([_dedup_rule()])
    ctx = http_svc_ctx()
    ctx.connections["redis"] = make_connection(id="conn-redis", type="redis", name="R",
                                               config={"host": "redis", "port": 6379})
    plan = compile_flow(flow, ctx)  # split=True -> fine
    group = _read_group(plan)
    assert any(p.key == "dedupe__detect" for p in group.processors)


def test_dedup_on_unsplit_root_object_http_read_allowed():
    flow = _single_read_flow_with_transforms([_dedup_rule()])
    flow.blocks[0].config.update({"recordPath": "$", "split": False})
    ctx = http_svc_ctx()
    ctx.connections["redis"] = make_connection(id="conn-redis", type="redis", name="R",
                                               config={"host": "redis", "port": 6379})
    plan = compile_flow(flow, ctx)
    assert any(p.key == "dedupe__detect" for p in _read_group(plan).processors)


def test_temporary_parent_values_are_removed_after_branch_routing():
    flow = routing_flow()
    flow.blocks[0].transforms = [
        TransformRule(id="t-attr", kind="extract", config={"attribute": "route_hint", "path": "$.route_hint", "retention": "block"}),
        TransformRule(id="t-field", kind="add_field", config={"field": "temporary_helper", "value": "${route_hint}", "retention": "block"}),
    ]
    plan = compile_flow(flow, routing_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-read")
    assert {p.key for p in group.processors} >= {
        "egress__b_any__remove_record_fields",
        "egress__b_any__remove_attributes",
        "egress__b_uncond__remove_record_fields",
        "egress__b_uncond__remove_attributes",
    }
    any_route = next(p for p in group.processors if p.key == "route__any_branch")
    any_cleanup = next(p for p in group.processors if p.key == "egress__b_any__remove_record_fields")
    assert any(c.from_ == any_route.key and c.to == any_cleanup.key and c.relationships == ["matched"] for c in group.connections)


def test_temporary_terminal_values_are_removed_after_dedup_before_publish():
    flow = golden_flow()
    sink = next(b for b in flow.blocks if b.id == "b-sink")
    sink.transforms.insert(0, TransformRule(
        id="t-temp", kind="add_field", config={"field": "temporary_helper", "value": "internal", "retention": "block"}
    ))
    plan = compile_flow(flow, golden_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-sink")
    keys = [p.key for p in group.processors]
    assert keys.index("dedupe__detect") < keys.index("egress__publish__remove_record_fields") < keys.index("publish")
    cleanup = next(p for p in group.processors if p.key == "egress__publish__remove_record_fields")
    assert cleanup.type == "org.apache.nifi.processors.standard.RemoveRecordField"
    assert cleanup.properties["field_to_remove_1"] == "/temporary_helper"


# --------------------------------------------------------------------------
# 17. M16 — api_key in query location
# --------------------------------------------------------------------------


def api_key_query_ctx() -> CompileContext:
    services = {"svc-http": make_service(
        id="svc-http", type="http", name="Keyed API",
        config={"baseUrl": "https://keyed.example", "authMode": "api_key",
                "keyLocation": "query", "keyName": "api_key", "keyValue": "s3cr3t"},
        hasSecret=True,
    )}
    connections = {"kafka": make_connection(id="conn-kafka", type="kafka", name="K",
                                            config={"bootstrapServers": "kafka:9092"})}
    return CompileContext(services=services, connections=connections, gateway_proxies={}, approved_schemas={})


def test_api_key_query_read_folds_into_url_once():
    plan = compile_flow(_paginated_read_flow(
        {"type": "offset", "fields": {"offsetParam": "offset", "limitParam": "limit", "limitValue": "30"}}
    ), api_key_query_ctx())
    group = _read_group(plan)
    fetch = next(p for p in group.processors if p.key == "fetch")
    url = fetch.properties["HTTP URL"]
    assert url == "#{svc_svc-http_base_url}/items?offset=${offset}&limit=${limit}&api_key=#{svc_svc-http_key_value}"
    assert url.count("#{svc_svc-http_key_value}") == 1  # never doubled
    # M16: no malformed "informational" header property, and the key value is
    # never emitted as a header-shaped dynamic property
    assert not any("informational" in k.lower() for k in fetch.properties)
    header_like = {k: v for k, v in fetch.properties.items()
                   if v == "#{svc_svc-http_key_value}" and k != "HTTP URL"}
    assert header_like == {}


def test_api_key_query_read_unpaginated_stays_in_request_url_only():
    plan = compile_flow(_paginated_read_flow({"type": "none", "fields": {}}), api_key_query_ctx())
    group = _read_group(plan)
    fetch = next(p for p in group.processors if p.key == "fetch")
    assert fetch.properties["HTTP URL"] == "${request.url}"  # untouched — no double-append
    init = next(p for p in group.processors if p.key == "init")
    assert init.properties["request.url"].endswith("?api_key=#{svc_svc-http_key_value}")
    assert not any("informational" in k.lower() for k in fetch.properties)


def test_api_key_query_write_appends_to_url():
    flow = Flow(
        id="flow-wq", name="Wq Flow", cron=None, state="Draft", enabled=True,
        createdAt="2026-01-01T00:00:00.000Z", updatedAt="2026-01-01T00:00:00.000Z",
        blocks=[
            FlowBlock(id="b-write", adapter="http", mode="write", name="Post", parentId=None, serviceId="svc-http",
                      config={"method": "POST", "path": "/incidents", "bodyTemplate": "{}",
                              "writeForwards": "original"}),
        ],
        topics=[], variables=[], servicePins={},
    )
    plan = compile_flow(flow, api_key_query_ctx())
    group = next(g for g in plan.rootGroup.childGroups if g.blockId == "b-write")
    write = next(p for p in group.processors if p.key == "write")
    assert write.properties["HTTP URL"] == "#{svc_svc-http_base_url}/incidents?api_key=#{svc_svc-http_key_value}"
    assert not any("informational" in k.lower() for k in write.properties)


# --------------------------------------------------------------------------
# 18. M6/M7 — relationship-disposition invariant
# --------------------------------------------------------------------------


def test_relationship_disposition_invariant_raises():
    from services.adapter.compiler.ir import BlockBuilder, ProcessorSpec

    b = BlockBuilder()
    b.add_processor(ProcessorSpec(key="p", name="p", type="T", autoTerminate=["x"]))
    b.add_processor(ProcessorSpec(key="q", name="q", type="T"))
    b.link("p", "q", ["x"])  # `x` now both connected and auto-terminated
    with pytest.raises(CompileError, match="both connected and auto-terminated"):
        b.build_group("b", "b", input_port=False, output_port=False)


def test_no_emitted_group_violates_disposition_invariant():
    # Compiling every fixture flow in this module exercises the builder-level
    # invariant on the real graphs (build_group runs it) — reaching here
    # without CompileError IS the assertion, but re-check explicitly anyway.
    for flow, ctx in [
        (golden_flow(), golden_ctx()),
        (routing_flow(), routing_ctx()),
        (session_token_flow(), session_token_ctx()),
        (jdbc_flow(), jdbc_ctx()),
        (trino_flow(), trino_ctx()),
        (kafka_read_flow(), kafka_read_ctx()),
        (http_write_flow("original"), http_svc_ctx()),
        (http_write_flow("response"), http_svc_ctx()),
        (http_lookup_flow(), http_svc_ctx()),
    ]:
        plan = compile_flow(flow, ctx)
        for group in plan.rootGroup.childGroups:
            connected = {}
            for c in group.connections:
                connected.setdefault(c.from_, set()).update(c.relationships)
            for p in group.processors:
                overlap = set(p.autoTerminate) & connected.get(p.key, set())
                assert not overlap, f"{flow.id}/{group.blockId}/{p.key}: {overlap}"


# --------------------------------------------------------------------------
# 19. sinkConfig pass-through — the compiler no longer derives connector
# configs; every kc/kafka_kc block's config.sinkConfig goes straight to the
# ConnectorSpec, unchanged.
# --------------------------------------------------------------------------


_FULL_ICEBERG_SINK_CONFIG = {
    "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
    "topics": "raw.golden_flow.asset",
    "tasks.max": "1",
    "consumer.override.auto.offset.reset": "latest",
    "iceberg.tables": "bronze.asset",
    "iceberg.tables.auto-create-enabled": "true",
    "iceberg.catalog.type": "rest",
    "iceberg.catalog.uri": "http://polaris.internal.corp:8181/api/catalog",
    "iceberg.catalog.warehouse": "bronze",
    "iceberg.catalog.credential": "cid:csecret",
    "iceberg.catalog.rest.auth.type": "oauth2",
    "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "iceberg.catalog.s3.endpoint": "http://minio.corp:9000",
    "iceberg.catalog.s3.access-key-id": "AK",
    "iceberg.catalog.s3.secret-access-key": "SK",
    "value.converter": "io.apicurio.registry.utils.converter.AvroConverter",
    "value.converter.apicurio.registry.url": "http://apicurio.internal.corp:8081/apis/registry/v3",
    "value.converter.apicurio.registry.use-id": "contentId",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "tasks.max.value": 1,  # a non-string value, to prove stringification
}


def test_kafka_kc_connector_config_is_exact_passthrough_of_sink_config():
    """A block whose sinkConfig holds a full, migration-authored config
    compiles to EXACTLY that config, key for key — the compiler derives
    nothing from the bound service any more."""
    flow = golden_flow()
    sink = next(b for b in flow.blocks if b.id == "b-sink")
    sink.config["sinkConfig"] = dict(_FULL_ICEBERG_SINK_CONFIG)
    plan = compile_flow(flow, golden_ctx())
    cfg = next(c for c in plan.connectors if c.ownerBlockId == "b-sink").config

    # every value is stringified (tasks.max.value: 1 -> "1")...
    expected = {k: str(v) for k, v in _FULL_ICEBERG_SINK_CONFIG.items()}
    assert cfg == expected
    # ...and the connector name is still the live-evidence convention.
    connector = next(c for c in plan.connectors if c.ownerBlockId == "b-sink")
    assert connector.name == "golden_flow.b-sink.kafka_kc"


def test_kafka_kc_connector_config_empty_sink_config_compiles_to_empty_config():
    """An empty sinkConfig compiles to an empty connector config — the
    compiler doesn't refuse this; that's validation's job (see
    validation.py::_sink_config_refusals)."""
    flow = golden_flow()
    sink = next(b for b in flow.blocks if b.id == "b-sink")
    sink.config["sinkConfig"] = {}  # explicitly empty, unlike golden_flow()'s default full config
    plan = compile_flow(flow, golden_ctx())
    cfg = next(c for c in plan.connectors if c.ownerBlockId == "b-sink").config
    assert cfg == {}


def test_build_kc_connector_passthrough():
    """`build_kc_connector` (previously untested) has the same pass-through
    contract as `build_kafka_kc_connector`, but names the connector `.kc`."""
    from services.adapter.compiler.connectors import build_kc_connector

    flow = golden_flow()
    block = next(b for b in flow.blocks if b.id == "b-sink").model_copy(
        update={"adapter": "kc", "config": {"sinkConfig": {"connector.class": "com.example.Sink", "topics": "raw.x", "batch.size": 500}}}
    )
    ctx = golden_ctx()
    spec = build_kc_connector(
        flow=flow, block=block, ctx=ctx, flow_token="golden_flow", topic="raw.x", entity_token="asset",
        topic_is_governed=True,
    )
    assert spec.name == "golden_flow.b-sink.kc"
    assert spec.config == {"connector.class": "com.example.Sink", "topics": "raw.x", "batch.size": "500"}
    assert spec.ownerBlockId == "b-sink"


def test_http_path_normalization_join_safety():
    """User-reported live failure: {baseUrl}{path} blind concatenation. The
    compiler must strip a base-matching full URL, slash-join a bare path,
    and refuse a foreign full URL."""
    from services.adapter.compiler.blocks_http import _normalize_path
    svc = make_service(id="s", type="http", name="S",
                       config={"baseUrl": "https://dummyjson.com", "authMode": "none"})
    assert _normalize_path("/users", svc) == "/users"
    assert _normalize_path("users", svc) == "/users"
    assert _normalize_path("https://dummyjson.com/users", svc) == "/users"
    assert _normalize_path("HTTPS://DUMMYJSON.COM/users", svc) == "/users"
    assert _normalize_path("${dynamic.path}", svc) == "${dynamic.path}"
    import pytest as _pytest
    with _pytest.raises(CompileError):
        _normalize_path("https://other.example.com/x", svc)
