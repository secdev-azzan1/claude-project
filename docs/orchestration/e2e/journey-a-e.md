# E2E Verification — Journey A (dedup flagship) + Journey E (lifecycle)

Agent: e2ea · Backend: http://localhost:8010 (/api/v2) · Date: 2026-08-13 (UTC timestamps from API responses)

Naming (derived): flowToken `e2ea_users` · topic `raw.e2ea_users.e2ea_user` · DLQ `dlq.e2ea_users` · subject `raw.e2ea_users.e2ea_user-value` · connector `e2ea_users.b2.kafka_kc`

---

## Journey A

### Step 0 — Preconditions
- `GET /api/v2/flows/` → 200. All 6 platform connections (nifi, kafka, apicurio, kafka_connect, redis, apisix) active + Healthy (`GET /api/v2/connections/` at 01:21Z).
- **PASS**

### Step 1 — HTTP application service
- `POST /api/v2/services/` `{type:"http", name:"e2ea dummyjson", config:{baseUrl:"https://dummyjson.com", authMode:"none"}}` → 200, id `svc-vxgkzn`, health "Not Tested" (01:21:10.052Z).
- `POST /api/v2/services/svc-vxgkzn/test` → 200, health **Healthy**, audit detail "Reachable — HTTP 200." (01:21:16.265Z).
- **PASS**

### Step 2 — Iceberg sink_destination service
- NOTE (mission-brief deviation, not a backend defect): the backend's `_validate_service` accepts `config.kind` of `"opensearch" | "iceberg_catalog"` only — the brief's `kind:"iceberg"` would 422. Used `kind:"iceberg_catalog"` (matches `types.ts` `SinkKind`).
- `POST /api/v2/services/` → 200, id `svc-lrogto`, rev 1. Secrets redacted in response (`hasS3SecretKey:true, hasOauthClientSecret:true`) — correct write-only secret behaviour.
- First `POST /{id}/test` → health **Failed**, audit: "Oauth2 token request failed with status 404."
  - Diagnosis (direct curl): `POST https://polaris.datapasc.com/v1/oauth/tokens` → 404 (`NotFoundException`); `POST https://polaris.datapasc.com/api/catalog/v1/oauth/tokens` → 200 with access_token. Polaris serves the REST catalog under the `/api/catalog` prefix; the brief's `catalogUrl` lacked it. Environment/config fact, not a code defect.
- Fixed at the cause: re-saved service with `catalogUrl:"https://polaris.datapasc.com/api/catalog"` (secrets left blank → kept, correct SecretField semantics; revision bumped to 2).
- Retest → 200, health **Healthy** (01:21:55.085Z) via OAuth + `/v1/config`.
- **PASS** (with documented config correction)

### Step 3 — Flow creation
- `POST /api/v2/flows/` flow id `flow-e2ea1`, name "e2ea users", cron `*/2 * * * *`, enabled:true, blocks b1 (http read, dummyjson `/users`, split on `$.users[*]`, transforms: extract user_id←$.id, dedup identity=[id] window 24h) + b2 (kafka_kc, entity `e2ea_user`, sinkServiceId svc-lrogto) → 200 (01:22:33.762Z).
- First `POST /{id}/validate` returned 2 issues: b2 "Select a service…" + "Schema ceremony required". Finding (minor, frontend-parity note): kafka_kc block requires **both** `serviceId` and `config.sinkServiceId` — `needs_service` includes kafka_kc. Set `b2.serviceId = svc-lrogto`, re-saved → 200.
- Re-validate → only remaining issue: "Schema ceremony required" (expected before step 4).
- **PASS**

### Step 4 — Schema infer + approve + registry proof
- Sampled `https://dummyjson.com/users?limit=3` (3 records, first id=1).
- `POST /api/v2/schemas/infer` (multipart, recordPath `$.users[*]`, name `e2ea_user`) → 200, report `{recordCount:3, fieldCount:28}`, suggestedPaths `["$.users[*]"]`.
- `POST /api/v2/schemas/approve` `{flowId:flow-e2ea1, blockId:b2, entity:e2ea_user, topic:raw.e2ea_users.e2ea_user, subject:raw.e2ea_users.e2ea_user-value, avro, provenance:"uploaded"}` → 200, id `schema-ijbes6`, nullable unions normalized null-first with `default:null`.
- Registry proof: `GET https://apicurio.datapasc.com/apis/ccompat/v7/subjects` includes `raw.e2ea_users.e2ea_user-value`.
- Flow validate now clean of the ceremony issue.
- **PASS**

### Step 5 — Deploy + NiFi/Connect/topic verification

**Attempt 1** — `POST /verbs/deploy` → **502** `{"detail":"Failed to create topic(s): raw.e2ea_users.e2ea_user, dlq.e2ea_users"}`.

**DEFECT 1 (blocking, backend):** `backend/services/kafka_client.py::ensure_topic_exists` — when `kafka_connection_mode == "kafbat"` (the only mode that works here; the broker `kafka:9092` is not TCP-reachable from the app host), the kafbat branch only **verifies** topic existence via `_kafbat_topic_message_count` and returns `TOPIC_NOT_FOUND` as a failure. It never **creates** the topic through the Kafbat API (`POST /api/clusters/{cluster}/topics` exists and works — proven below). compiler-spec.md §3.4 requires "Topic auto-created at deploy (Kafka admin via Kafbat)". Net effect: **deploying any flow with a new topic always fails in this environment.** Not a one-line fix (needs a Kafbat create call + re-verify), so not patched; worked around as an operator would.

- Workaround: created both topics via Kafbat REST (`POST https://kafbat.datapasc.com/api/clusters/local/topics`, partitions 1, RF 1 — single-broker cluster) → 200 for `raw.e2ea_users.e2ea_user` and `dlq.e2ea_users` (both then GET 200, messagesCount 0).

**Attempt 2** — deploy → **200**, state `Stopped`, `deployedAt 01:26:08.212Z`, servicePins `{svc-vxgkzn:1, svc-lrogto:2}`, full runtimeScopeMap. NiFi REST verification (JWT via POST /nifi-api/access/token):
- Flow PG `e2ea_users` (id f8b93f7d…) under root (parent 657f40bd… = recorded fingerprint); parameter context **`e2ea_users__params`** bound. Child PGs **`b1__http`** + **`b2__kafka_kc`**.
- b1__http chain (connections dump): `trigger(GenerateFlowFile) → init(UpdateAttribute) → fetch(InvokeHTTP) → split(SplitJson) → t0__extract(EvaluateJsonPath) → dedupe__hash(ExecuteGroovyScript) → dedupe__detect(DetectDuplicate) → non-duplicate → output port b1__http__out__b2`. **Dedup is after the transform chain, hash before detect — ordering correct.**
- `dedupe__detect`: autoTerminated `['duplicate']` (counted drop), `Cache Entry Identifier ${dedupe.key}`, `Age Off Duration 24 hours`, Distributed Cache Service = dedupe_redis_cache. `failure → dlq__meta` (DLQ path — fail-stop honoured, reference deviation corrected).
- `dedupe__hash`: `SRC=e2ea_users__b1`, `EXCLUDES=ingest_id,ingest_ts,op`, `IDENTITY=id`; `failure → dlq__meta`. All other failure rels (fetch/split/extract) also → dlq__meta → dlq__publish (topic dlq path).
- Controller services all **ENABLED**: `redis_pool` (RedisConnectionPoolService), `dedupe_redis_cache` (RedisDistributedMapCacheClientService), 2× Kafka3ConnectionService, JsonTreeReader, JsonRecordSetWriter, ConfluentSchemaRegistry, ConfluentEncodedSchemaReferenceWriter, AvroRecordSetWriter.
- b2 `publish` = PublishKafka: Topic `#{topic_b2}`, acks=all, Failure Strategy "Route to Failure", Reader=json_reader, Writer=avro_writer; avro_writer: Schema Access `schema-name` `#{schema_b2}`, Schema Write Strategy `schema-reference-writer` → ConfluentEncodedSchemaReferenceWriter, Registry → ConfluentSchemaRegistry. Matches spec §3.4.
- Kafka Connect: connector `e2ea_users.b2.kafka_kc` exists, connector state **PAUSED** (created stopped ✓).

**DEFECT 2 (blocking for the destination, backend compiler):** `backend/services/adapter/compiler/connectors.py::build_kafka_kc_connector` (iceberg_catalog branch) generates only `iceberg.catalog.uri` + `iceberg.catalog.warehouse` — it omits `iceberg.catalog.type=rest` (Iceberg then defaults to **HiveCatalog** → task 0 FAILED with `java.lang.ClassNotFoundException: org.apache.iceberg.hive.HiveCatalog` — full trace captured from /status), the OAuth2 client credentials, and every S3 key. compiler-spec.md §5 explicitly requires "catalog uri/warehouse + OAuth2 client credentials, S3 endpoint/keys/path-style". The service record HAS all these fields (oauthClientId/Secret, s3Endpoint/Keys/Region/PathStyle) — the compiler just never emits them. Multi-line fix → not patched; worked around via user `sinkConfig` pass-through (the `iceberg.catalog.*` prefix is not platform-locked).

**FINDING 3 (parity gap, medium):** `GET /{id}/messages?topic=raw.e2ea_users.e2ea_user` → **404 "does not belong to this flow"** right after a successful deploy. `runtime.py::get_topic_messages` derives ownership from `flow.topics`, which only the **frontend** materializes (api.ts `syncTopics`); the v2 save endpoint does not sync it server-side, and deploy records topics only into `runtimeScopeMap`. An API client that saves a legal flow without the `topics` array gets a deployable flow that cannot view its own topic. Also: the DLQ topic is never in `flow.topics`, so `messages?topic=dlq.e2ea_users` 404s by design (DLQ inspection is `GET /{id}/dlq`).

- Mirror-the-frontend fix applied to the flow doc: added materialized topic entry `{id:t-b2, kind:materialized, name:raw.e2ea_users.e2ea_user, sealed:true, writerBlockId:b2}` + `sinkConfig` with the 10 missing `iceberg.catalog.*` keys.
- First redeploy attempt → 422 preflight: "Configuration valid: 1 issue" → validate: "Set connector.class" (a non-empty sinkConfig must name its plugin — UI parity, reasonable). Added `connector.class=org.apache.iceberg.connect.IcebergSinkConnector`, saved → 200.
- `POST /verbs/redeploy` → **200**, state Stopped, deployedAt 01:31:11.727Z, new PG ids (root f8bde0ca…), connector re-upserted. Connector config verified via Connect REST: full REST-catalog config (type=rest, credential, oauth2-server-uri, scope, S3FileIO, endpoint/keys/path-style/region) + platform-locked keys correct (topics, iceberg.tables=bronze.e2ea_user, Avro converter → Apicurio ccompat, as-confluent=true).
- `GET /{id}/messages?topic=raw.e2ea_users.e2ea_user` → **200** `{"messages":[]}` after the topics-array fix.
- **PASS (with 2 defects + 1 parity finding documented; workarounds applied through public API only)**
