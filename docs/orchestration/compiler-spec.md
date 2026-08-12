# Flow Compiler Specification (WS7)

Translates a `Flow` document (adapter model, `frontend/src/prototype/types.ts` shapes) into
live NiFi + Kafka Connect artifacts. Two stages: **compile** (pure: Flow → DeploymentPlan
JSON) and **apply** (DeploymentPlan → NiFi/Connect REST). Golden-fixture unit tests target
the compile stage. References: `analysis/nifi-reference-flows.md` (ingest/publish configs),
`analysis/dedup-reference-flow.md` (dedup), `analysis/alpha-backend.md` §4 (client/lifecycle
helpers), decisions D5–D9, D16, D17.

## 1. DeploymentPlan IR

```json
{
  "flowId": "...", "flowToken": "<tokenize(flow.name)>",
  "parameterContext": {"name": "<flowToken>__params",
    "parameters": [{"name": "...", "value": "...", "sensitive": false}]},
  "rootGroup": {"name": "<flowToken>",
    "childGroups": [BlockGroup...], "connections": [PortLink...]},
  "topics": [{"name": "...", "kind": "data|dlq", "ownerBlockId": "..."}],
  "connectors": [{"name": "<flowToken>.<blockId>.<kind>", "config": {...},
                  "ownerBlockId": "..."}],
  "scopeMap": {"<blockId>": {"adapter": "http", "engine": "nifi|connect",
               "groupName": "...", "components": []}}
}
BlockGroup = {"blockId", "name": "<blockNameToken>__<adapter>",
  "processors": [{"key", "name", "type", "properties": {...}, "schedulingPeriod?",
                  "schedulingStrategy?", "executionNode?", "autoTerminate": ["rel"...],
                  "penalty?", "runOnPrimary?"}],
  "controllerServices": [{"key", "name", "type", "properties"}],
  "connections": [{"from": "key|inputPort", "to": "key|outputPort|dlq",
                   "relationships": ["success"]}],
  "inputPort": bool, "outputPort": bool, "dlqPort": bool}
PortLink = {"fromGroup/blockId", "toGroup/blockId"}   // parent output port -> child input port
```

`key` is a stable local id; apply() maps keys → NiFi component UUIDs and stores the result
in `flow.runtimeScopeMap` (per-block: nifi ids + processor types + connector names + topics).

Determinism: no timestamps/random values inside plan bodies. Everything environment-specific
(endpoints, credentials, topic prefix) resolves through parameters; secrets are
`sensitive: true` parameters valued at apply time from service/connection records.

## 2. Naming (parity with frontend `src/prototype/naming.ts` — port it exactly)

- `tokenize(s)`: trim → lowercase → non-alnum → `_` (NO collapse of repeats).
- Flow PG: `tokenize(flow.name)`. Block PG: `tokenize(block.name)__<adapter>`.
- Topic: use `deriveTopicName` parity (naming.ts) incl. `topicOverride` (R7) and branch
  variant tokens; DLQ: `dlq.<flowToken>` (naming.ts `dlqName`).
- Connector: `<flowToken>.<blockId>.<kafka_kc|kc>` (live-evidence convention).
- Parameters: `<snake>` names; service-derived: `svc_<serviceId>_<field>`.

## 3. Per-adapter compilation

### 3.1 http (read / write / lookup)
Chain inside the block PG (reference: nifi-reference-flows §9.1, DummyJson_Dedup):
1. **Trigger** (only if this block is the flow root — R1): `GenerateFlowFile`,
   TIMER_DRIVEN/CRON_DRIVEN from `flow.cron` (5-field → NiFi cron `sec min hour dom mon dow`
   = `0 <min> <hour> <dom> <mon> <dow>`), Batch Size 1, runOnPrimary. Non-root blocks get an
   input port instead.
2. `UpdateAttribute` `init` — seeds pagination attrs (offset/page/cursor per config) and
   static request meta.
3. *(session_token services only)* `InvokeHTTP` `login` (method/path/body from service
   config) → `EvaluateJsonPath` `extract_token` (token JSONPath → attr `session.token`) →
   failure of either → run-failure path (LogAttribute + terminate; NO DLQ record — run
   failure, not record failure).
4. `InvokeHTTP` `fetch`: baseline config from reference flows (§9.1 timeouts etc.,
   Response Redirects **False** per MVP), URL = `#{svc_<id>_base_url}` + block path with
   `${...}` params; auth per service: basic → Request Username/Password(sensitive param);
   bearer → `Authorization: Bearer #{...}` dynamic header property; api_key header/query;
   session_token → `Authorization`(or configured header) = `${session.token}`; oauth2 →
   `StandardOauth2AccessTokenProvider` CS referenced via InvokeHTTP's OAuth2 provider
   property. Proxy egress: when the bound service selects a gateway proxy, base URL becomes
   `#{apisix_runtime_url}/<proxyToken>` (path prefix routing) — target host resolution then
   happens in APISIX.
5. Response parsing: `responseFormat`:
   - json + split: `SplitJson` (JsonPath = recordPath or `$[*]`) → per-record FlowFiles.
   - json no-split: `EvaluateJsonPath`/pass-through.
   - csv/xml/text: record-reader based `SplitRecord`/`ConvertRecord` per reference flows.
6. Pagination (config.pagination.type): compile the reference loop shapes —
   offset/page/cursor/next_url: `EvaluateJsonPath` `page_meta` → `RouteOnAttribute`
   `has_more` (continue → `UpdateAttribute` `next` → back to fetch; done → auto-term) with
   stop conditions (empty page / total count field / has-more flag, body-or-header source).
   Max-pages safety attr respected.
7. write mode: `InvokeHTTP` method POST/PUT/PATCH with body template (`ReplaceText` to
   materialize `${field}` body from record) ahead of the call; "chain continues with"
   original vs parsed response honored on the outbound connection.
8. lookup mode: fetch result merged onto parent record: `EvaluateJsonPath` response fields →
   `UpdateRecord` join (joinField config).
9. Transforms/dedup/routing stages appended per §4. Output port emits per-record FlowFiles.
Failure relationships (fetch non-2xx after retries, parse errors) → block DLQ path (§6).

### 3.2 jdbc
- read: `QueryDatabaseTableRecord` (DBCPConnectionPool CS from database service; Table,
  Columns, Maximum-value Columns = watermark when incremental; initial position honored),
  runOnPrimary, trigger = cron scheduling on the processor itself when root.
- write: `PutDatabaseRecord` (INSERT/UPDATE per `change_type` note — statement type from
  config; JsonTreeReader).
- lookup: `LookupRecord` + `DatabaseRecordLookupService` (join field config).

### 3.3 kafka (read / write)
- read: `ConsumeKafka` (Kafka3ConnectionService CS → `#{kafka_bootstrap}` in-cluster
  `kafka:9092`; group id `<flowToken>__<blockId>`; topic from config/adopted topic; offset
  reset from initialPosition; parse per parseFormat — raw mode: no record processing at all
  downstream (R8 quarantine enforced at validation, compiler emits byte passthrough)).
- write: `PublishKafka` JSON passthrough (JsonRecordSetWriter) or raw bytes on R8 branch;
  kafka key from an `extract`-transform-designated attribute when present.

### 3.4 kafka_kc (terminal, governed)
`UpdateRecord` `envelope` (`/ingest_id=${uuid}`, `/ingest_ts=${now():toNumber()}`) →
[transforms per §4 incl. dedup LAST] → `PublishKafka` with JsonTreeReader reader +
`AvroRecordSetWriter` writer (Schema Access = schema-name `<topic>-value`,
`ConfluentSchemaRegistry` CS → `#{apicurio_ccompat_url}`, Schema Write Strategy =
schema-reference-writer + `ConfluentEncodedSchemaReferenceWriter`), acks=all, Failure
Strategy = Route to Failure → DLQ path. Topic auto-created at deploy (Kafka admin via
Kafbat). PLUS one Connect sink connector (§5). Deploy gate: approved schema required.

### 3.5 kc (terminal subscription)
No NiFi components. One Connect connector consuming `config.attachTopicId`'s topic from
`initialPosition`, sink config per service + user `sinkConfig` (locked keys enforced:
`topics`, converters, iceberg.tables* for lakehouse). Save-is-live: connector upserted on
flow save when the topic exists.

## 4. Transforms, dedup, routing (per block)

Compiled AFTER response parsing, per-record:
- `extract` → `EvaluateJsonPath` (destination attribute, default value support).
- `add_field`/`set_from_attribute`/`rename`/`coerce` → `UpdateRecord` steps
  (literal-value strategy; rename via field move; coerce via CAST-style writer schema or
  `UpdateRecord` + expression).
- `remove_field` → `RemoveRecordField`.
- User order preserved 1:1; each rule = its own processor named `t<idx>__<kind>` (keeps
  scope-map attribution and honest structure).
- **dedup** (always last — validation guarantees): per `analysis/dedup-reference-flow.md`:
  `ExecuteGroovyScript` `dedupe__hash` (SRC=`<flowToken>__<blockId>`, EXCLUDES=user excludes
  + `ingest_id,ingest_ts,op`; missing identity field → route to failure/DLQ path with
  `dlq.reason=dedup_identity_missing`) → `DetectDuplicate` `dedupe__detect`
  (`${dedupe.key}`, Age Off = TTL, Redis cache CS with same TTL → shared
  `RedisConnectionPoolService` from Redis connection). non-duplicate → continue; duplicate →
  auto-terminate (counted); failure → DLQ path (fail-stop).
- **routing/branches**: children of block B attach at B's output:
  - unconditional branch child: direct PortLink (NiFi fans out copies).
  - conditional branch (rules, match=any): ONE `RouteOnAttribute` `route__<branchToken>`
    (Route to Property strategy) with one dynamic property per rule (EL over extracted
    attrs); any matched property relationship → child; unmatched → auto-term (counted).
  - match=all: CHAIN `route__<branchToken>__rule_<i>` RouteOnAttribute per rule;
    matched→next; last matched→child; each unmatched→auto-term. (Genuine multi-processor
    decisions — D7.)
  - Rule fields referenced must exist as attributes: compiler auto-prepends an
    `EvaluateJsonPath` `route_fields` extracting every referenced field.
  - Routing processors live in the PARENT block's PG (they are the parent's egress
    decision); scope-map attributes them to the parent.
  - EL mapping: equals `${f:equals('v')}`; not_equals `${f:equals('v'):not()}`; contains
    `${f:contains('v')}`; starts_with `${f:startsWith('v')}`; regex `${f:matches('v')}`;
    is_empty `${f:isEmpty()}`.

## 5. Kafka Connect connector configs

From sink_destination service (D11 fields) + alpha `iceberg_sink_config.py` reference:
- Iceberg: `connector.class=org.apache.iceberg.connect.IcebergSinkConnector`, topics=<topic>,
  iceberg.tables=`bronze.<entityToken>` (naming parity), catalog uri/warehouse + OAuth2
  client credentials, S3 endpoint/keys/path-style, Avro converter → Apicurio ccompat URL,
  `iceberg.tables.auto-create-enabled=true`, DLQ config OFF (Connect keeps own error path).
- OpenSearch: `connector.class=...OpensearchSinkConnector` equivalents (connection.url,
  index prefix, write mode, key.ignore etc.), value converter JSON or Avro+registry
  depending on source topic (kafka_kc → Avro; kc on schemaless topic → JSON).
Created stopped at deploy; started/stopped/deleted with flow verbs; kc saves upsert live.

## 6. DLQ wiring (per block PG)

`UpdateAttribute` `dlq__meta` (dlq.block=<blockId>, dlq.reason=<from failure source>) →
`PublishKafka` `dlq__publish` (topic `dlq.<flowToken>`, JSON/raw bytes as-is, headers from
attributes). Every record-failure relationship in the block routes here. DLQ publish failure
→ the FlowFile stays queued (connection backpressure) — matches "park in own queue".

## 7. Apply stage (NiFi deployer)

Order: ensure parameter context → create flow PG (or locate for redeploy) → create child
PGs + ports → controller services (create, set properties, ENABLE with poll-wait; reuse
alpha `_wait_cs_enabled` pattern) → processors (properties incl. sensitive dynamic props)
→ connections (incl. cross-PG port links) → write scopeMap + fingerprints to flow doc →
leave everything STOPPED. On partial failure: best-effort delete created PG, surface error.
Verbs: start = start PG (+ start connectors + cron enabled); pause = stop
trigger/ingest processors only; resume = start them; stop = stop PG (queues retained);
stop&clear = stop + drop all queued FlowFiles (audited counts); redeploy = stop+clear →
delete PG → full deploy; undeploy = delete PG + delete connectors + empty owned data
topics + keep DLQ + reset scope map (state Draft); delete = undeploy + delete DLQ + owned
topics + flow doc. NiFi REST specifics (2.9): reuse alpha nifi_client (JWT, revision 409
retry) and nifi_flow_manager start/stop helpers.

## 8. Preflight (deploy)

Mirror prototype `deployPreflight()` rows exactly (config valid; per-required-connection
active+healthy: nifi/kafka/apicurio always, kafka_connect if kc|kafka_kc, redis if dedup or
jdbc-incremental, apisix if proxied; gateway proxies Reconciled + host allowlisted; schema
approved per kafka_kc; bound services healthy; no retired pinned services) + naming
collision check (topic reservation) + Connect plugin installed check (live
/connector-plugins). All-or-nothing; every failing row reported.
