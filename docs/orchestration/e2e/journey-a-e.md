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

### Step 6 — Start + run 1
- `POST /verbs/start` → 200, state **Running**, lastRunAt 01:32:47.239Z. NiFi: all 13 components RUNNING; trigger = CRON_DRIVEN `0 */2 * * * *` on PRIMARY (correct 5→6-field conversion). Connect connector resumed: connector RUNNING, task 0 RUNNING.
- Cron fired 01:34:00Z. `GET /metrics` → `{available:true, perBlock:[b1 out=30, b2 in=30], topicCounts:[raw.e2ea_users.e2ea_user=30, dlq.e2ea_users=0]}`. **30 records after run 1, exactly as expected** (dummyjson default page).
- **PASS**

### Step 7 — DEDUP PROOF (run 2)
NiFi processor + connection statistics (REST `/flow/processors/{id}/status`, `/flow/process-groups/{id}/status`), DetectDuplicate id f8be7c69…:

| Moment | detect flowFilesIn | detect flowFilesOut | non-duplicate conn (detect→out port b2) | detect→dlq | topic count |
|---|---|---|---|---|---|
| after run 1 (01:35Z) | 30 | 30 | 30 | 0 | 30 |
| after run 2 (01:36Z) | **60** | **30** | **30** (unchanged) | 0 | **30** (unchanged) |

- Poll log: detect in/out was `30 30` at 01:35:48Z and `60 30` at 01:35:59Z — the second fetch of the same 30 users entered DetectDuplicate and **all 30 were routed to `duplicate` (auto-terminated, counted)**: in−out = 30 duplicates suppressed in NiFi itself.
- Topic message count after run 2: **still 30** (metrics topicCounts). DLQ still 0. `dedupe__hash → dlq` and `dedupe__detect → dlq` connections both at 0 — no dedup failures.
- **PASS — dedup suppression proven with NiFi component counters**

### Step 8 — Kafka Connect + Iceberg destination
- After start: connector `e2ea_users.b2.kafka_kc` RUNNING; task 0 then **FAILED** with: `Tolerance exceeded in error handler … Caused by: com.microsoft.kiota.ApiException: service returned status code 404 but no response body was found` at `io.apicurio.registry.resolver…getByContentId`.

**DEFECT 2b (blocking for destination, backend compiler — no user workaround):** `build_kafka_kc_connector` sets `value.converter.apicurio.registry.url` to the **ccompat** URL (`…/apis/ccompat/v7`). The Apicurio `AvroConverter` (registry 3.x kiota SDK resolver) requires the **core registry API** URL. Proof: ccompat schema id for our subject = 13; `GET https://apicurio.datapasc.com/apis/ccompat/v7/ids/contentIds/13` → **404** (exactly what the resolver hits), while `GET https://apicurio.datapasc.com/apis/registry/v3/ids/contentIds/13` → **200** with our schema. Because `value.converter*` is a platform-locked prefix (stripped from user sinkConfig by `_merge_locked`), the platform API offers **no** user-level workaround — must be fixed in `connectors.py` (emit `https://…/apis/registry/v3` for the converter while NiFi's ConfluentSchemaRegistry CS correctly keeps ccompat).
- Diagnostic surgery (out-of-band, documented): `PUT /connectors/e2ea_users.b2.kafka_kc/config` with only that URL corrected → 200; `POST /restart?includeTasks=true&onlyFailed=true` → 202. Task 0 → **RUNNING** and stayed RUNNING (polled 01:38:27–01:39:11Z).
- Destination proof: Polaris `GET /api/catalog/v1/bronze/namespaces/bronze/tables` → `bronze.e2ea_user` **auto-created**; table metadata shows 1 snapshot, op `append`, **`total-records: 30`** (ts 1786585186435 ≈ 01:39:46Z). End-to-end delivery: dummyjson → NiFi (dedup) → Kafka (Avro+registry) → Connect → Iceberg, 30 unique records.
- **PASS (destination proven; connector config defect documented — our defect, not environment)**

### Step 9 — UI state
- `GET /flows/flow-e2ea1` → state **Running**, enabled true.
- `GET /metrics` → `available:true`, per-block numbers (b1 http out=30, b2 kafka_kc in=30), topicCounts present.
- `GET /dlq` → `{"records":[]}` (empty — correct).
- `GET /runtime` → reachable:true, 13 components grouped b1=9/b2=4, all RUNNING, 9 controller services, 1 connector, **drift findings: 0**.
- **PASS**

---

## Journey E (same flow)

### Step 10 — pause / resume
- `POST /verbs/pause` → 200, state **Paused** (01:41:41Z audit). NiFi states after pause: `trigger STOPPED`; `init/fetch/dedupe__detect/b2 publish` all still RUNNING — exactly "trigger only" semantics.
- `POST /verbs/resume` → 200, state **Running**; trigger RUNNING again (01:41:52Z audit).
- **PASS**

### Step 11 — stop_clear
- `POST /verbs/stop_clear` → 200, state **Stopped**. NiFi flow PG status: `queued: 0 (0 bytes)`, trigger STOPPED.
- Audit 01:42:37.275Z: **"Flow stopped and cleared … Dropped 0 queued record(s) across 21 connection(s)."** — the drop count is audited (0 because the flow was idle at stop).
- **PASS**

### Step 12 — redeploy from stopped
- `POST /verbs/redeploy` → 200, state Stopped, deployedAt 01:44:13.130Z. **PG recreated with new component ids** (root PG f8bde0ca… → f8c9c940…; DetectDuplicate f8be7c69… → f8ca6735…).
- Topic **PRESERVED with data**: `raw.e2ea_users.e2ea_user` still **30 messages** after redeploy (metrics topicCounts) — redeploy does not wipe data topics. DLQ intact at 0.
- **PASS**

### Step 13 — drift detection
**13a — out-of-band rename:** `PUT /nifi-api/process-groups/{pg}` name → `e2ea_drifted` (200). `GET /runtime` → reachable:true, **NO drift findings**.

**FINDING 5 (gap, medium):** the runtime reader (`runtime.py::read_runtime`) locates the PG **by id** and only ever emits `process_group_missing` findings — an out-of-band PG rename (and by extension the `property_edited` / `component_state_changed` / `out_of_band_edit` kinds declared in types.ts `DriftKind`) is never detected. Verdict recorded: **rename invisible to drift detection**. (Renamed back to `e2ea_users` — 200.)

**13b — out-of-band delete:** deleting the PG required stop-all + disable-all-CS first (NiFi 409: "…schema_ref_writer… cannot be deleted because it is not disabled" — captured), then `DELETE /nifi-api/process-groups/{pg}` → 200.
- `GET /runtime` → drift finding: kind **`process_group_missing`**, verdict **`really_deleted`**, repairable true, verdictDetail: "The active NiFi connection points at the same instance (root process group fingerprint matches) … the process group is gone there — it was deleted outside the platform." Fingerprint disambiguation works.
- `POST /runtime/repair` → 200: `clearedFindings: 1`, orphans `[]` (correct — really_deleted leaves nothing to orphan), runtime `processGroupId: null`.
- Flow doc after repair: state **Draft**, deployedAt null, nifiProcessGroupId null. Audit 01:46:44.786Z: **"Runtime force repaired … 1 finding(s) resolved, 0 orphan(s) recorded."**
- **PASS (delete/repair path); rename detection recorded as FINDING 5**

### Step 14 — cleanup
- `DELETE /api/v2/flows/flow-e2ea1` → 200 `{ok:true}`; `GET /flows/flow-e2ea1` → 404. Audit "Flow deleted".
- Kafka: after fresh Kafbat login, **no e2ea topics remain** — both `raw.e2ea_users.e2ea_user` and `dlq.e2ea_users` deleted by the flow delete (earlier 302 responses were unauthenticated-session redirects, not topic state).
- NiFi: **no e2ea PGs under root**.
- Kafka Connect: connector `e2ea_users.b2.kafka_kc` **still existed** after flow delete (GET /status → 200).

**DEFECT 6 (teardown residue, backend):** `lifecycle.delete()` only runs `undeploy()` — the sole place connectors are deleted — when `deployedAt` or `nifiProcessGroupId` is set. After `runtime/repair` returns a flow to Draft (both nulled, `runtimeScopeMap` retained), DELETE removes topics + docs but **leaves the flow's Kafka Connect connectors orphaned**, and no orphan record is written for them. Evidence: connector 200 post-delete. Cleaned out-of-band (`DELETE /connectors/e2ea_users.b2.kafka_kc` → 204); Connect then shows no e2ea connectors.
- Services: `POST /services/{id}/retire` → both `svc-vxgkzn` and `svc-lrogto` retired:true, audited.
- Schema: flow delete leaves the approved-schema doc + registry subject behind (by design — schemas have their own lifecycle); cleaned via `DELETE /api/v2/schemas/schema-ijbes6` → 200 `{ok:true, registryDeleted:true}`; ccompat subjects now contain **no e2ea entries**.
- **PASS (with DEFECT 6 documented; environment left fully clean)**

---

## Final summary

| # | Step | Result |
|---|------|--------|
| 1 | HTTP service create + test | PASS |
| 2 | Iceberg sink service create + test | PASS (config fix: `kind=iceberg_catalog`, catalogUrl needs `/api/catalog`) |
| 3 | Flow creation + validation | PASS |
| 4 | Schema infer/approve + Apicurio proof | PASS |
| 5 | Deploy + NiFi structure verification | PASS after workaround (DEFECT 1) |
| 6 | Start + run 1 (30 records) | PASS |
| 7 | Dedup proof (run 2 suppressed) | **PASS — 60 in / 30 out, 30 duplicates dropped, topic stays 30** |
| 8 | Connect + Iceberg destination | PASS after out-of-band fix (DEFECTS 2/2b); 30 records committed to `bronze.e2ea_user` |
| 9 | UI state (flow/metrics/dlq/runtime) | PASS |
| 10 | pause / resume | PASS |
| 11 | stop_clear (audited drop counts) | PASS |
| 12 | redeploy (topic preserved) | PASS |
| 13 | drift rename / delete / repair | PASS for delete+repair; rename undetected (FINDING 5) |
| 14 | delete + retire + sweep | PASS with connector residue (DEFECT 6), cleaned |

## Defects & findings

1. **DEFECT (blocking)** — `kafka_client.py::ensure_topic_exists`: kafbat mode never creates topics (verify-only). Every deploy with a new topic fails 502 in this environment. Spec §3.4 requires auto-create via Kafbat (`POST /api/clusters/{cluster}/topics` works — proven).
2. **DEFECT (blocking, destination)** — `compiler/connectors.py::build_kafka_kc_connector` (iceberg): omits `iceberg.catalog.type=rest`, OAuth (`credential`/`oauth2-server-uri`/`scope`) and all S3 keys (`io-impl`, endpoint, keys, path-style, region) despite the service record carrying them and spec §5 requiring them → task fails with `ClassNotFoundException: org.apache.iceberg.hive.HiveCatalog`. Workaroundable via user `sinkConfig` (prefix not locked).
   - **2b (no workaround)** — same builder sets `value.converter.apicurio.registry.url` to the ccompat URL; Apicurio AvroConverter needs the core API (`/apis/registry/v3`). `{ccompat}/ids/contentIds/13` → 404 vs `{v3}/ids/contentIds/13` → 200 proven. `value.converter*` is platform-locked, so only a compiler fix can resolve it.
3. **FINDING (parity, medium)** — `GET /{id}/messages` ownership comes from `flow.topics`, which only the frontend materializes (api.ts `syncTopics`); server-side save doesn't sync it, so API-created flows 404 on their own topics until the client mirrors the frontend. DLQ topic is never viewable through this endpoint (by design; `/dlq` covers it).
4. **NOTE (mission-brief)** — sink kind must be `iceberg_catalog` (not `iceberg`); Polaris catalogUrl needs the `/api/catalog` prefix; kafka_kc blocks need `serviceId` set in addition to `config.sinkServiceId`; non-empty `sinkConfig` must include `connector.class`.
5. **FINDING (gap, medium)** — drift detection only emits `process_group_missing`; out-of-band renames/property edits are invisible (types.ts declares richer DriftKind values the reader never produces).
6. **DEFECT (teardown)** — flow delete after runtime/repair leaves Kafka Connect connectors orphaned (undeploy — the only connector-deleting step — is skipped when deployedAt/nifiProcessGroupId are null), with no orphan record.

Out-of-band interventions (all documented above, none touching source code): Kafbat topic pre-creation (defect 1), Connect connector URL patch + restart (defect 2b), NiFi PG rename/delete (drift test itself), leftover connector deletion (defect 6). Zero source files modified; uvicorn untouched.
