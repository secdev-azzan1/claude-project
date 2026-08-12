# Execution Plan — Data Mobility Platform (full-stack)

> **For agentic workers:** each task below is a bounded objective executed by a focused
> implementation agent, then independently verified. Decisions referenced as D# live in
> `decisions.md`. Analysis reports live in `analysis/`. Working tree:
> `C:\Users\kaifm\Desktop\claude-project` (frontend/, backend/). NEVER touch the original
> projects under `C:\Users\kaifm\Desktop\Project\`.

**Goal:** turn the finalized frontend prototype into a fully functional full-stack
application on the real infrastructure (NiFi 2.9, Kafka via Kafbat, Apicurio 3.2 ccompat,
Kafka Connect 4.2, APISIX, Redis-via-NiFi), per the goal prompt.

**Environment facts (verified):** NiFi API ok (basic auth→JWT); Apicurio ccompat v7 ok;
Kafka Connect ok (OpenSearch+Iceberg sink plugins installed); APISIX admin ok (X-API-KEY);
Kafka broker TCP unreachable from dev machine (use Kafbat REST); Redis only reachable from
NiFi (`redis:6379`); reference dedup PG `DummyJson_Dedup` exported to
`reference/nifi-flows/DummyJson_Dedup.json`; Docker Desktop available (Mongo via compose);
Node 20 / Python 3.13.

**Build order:** WS0 → WS1 (contract+scaffold) → WS2..WS6 in parallel tracks → WS7 (compiler)
→ WS8 (frontend API swap + UI edits, can start once WS1 contract fixed) → WS9 (E2E) → WS10
(review/final audit). UI-only tasks (WS8.U*) are independent of backend and can run early.

---

## WS0 — Environment & scaffold

- **T0.1** Docker: start Docker Desktop engine, `docker compose up -d mongo` with a compose
  file adapted from alpha (mongo only; backend/frontend run natively for dev).
  *Done when:* `mongosh ping` ok on localhost:27018.
- **T0.2** Backend boots: venv (py3.13; bump motor/pymongo/fastapi pins as needed),
  `.env` from infra credentials (never committed), `uvicorn server:app` starts against
  Mongo, `GET /api` healthy. Strip alpha startup pieces that block boot (seed defaults ok).
  *Done when:* server starts clean; /api returns version.
- **T0.3** Frontend dev server runs (`npm run dev`), existing vitest suite green baseline
  (record failures pre-existing vs introduced).
  *Done when:* dev server serves the app; `npx vitest run` result recorded.

## WS1 — Backend contract & core model (D3, D4)

- **T1.1** Pydantic models mirroring `frontend/src/prototype/types.ts` (Flow, FlowBlock,
  BranchInfo/Condition, TransformRule, FlowTopic, FlowVariable, AppService(+revisions),
  PlatformConnection(6 types), GatewayProxy/CertProfile, ApprovedSchema(+approvals),
  SchemaTemplate, FlowRuntime/NifiComponent/ConnectConnectorRuntime/DriftFinding,
  DlqRecord, AuditEvent, DashboardSummary). camelCase JSON (D4). New module
  `backend/models/adapter/*.py`. Unit tests: round-trip serialization matches TS field names.
- **T1.2** New router family mounted under `/api/v2`: flows CRUD + verbs skeleton,
  connections, services, gateway, schemas, dashboard, audit — request/response contracts
  finalized (mirroring api.ts function signatures); handlers initially wire CRUD to Mongo
  with validation, verbs stubbed with 501 where compiler is pending. Server-side legality
  validation module `backend/services/adapter/legality.py` porting `legality.ts` +
  `validation.ts` rules (R1–R8, terminal rule D9, entity-required, dedup-last).
  Unit tests for legality parity with the TS tests.
- **T1.3** Audit helper + Mongo indexes for new collections; seed connections from env at
  boot (nifi/kafka/apicurio/kafka_connect/redis/apisix) with immediate health test (D15).

## WS2 — Platform Connections backend (D15)

- **T2.1** Type-specific config/auth per prototype fields (nifi url+bearer/basic; kafka
  bootstrap+mode(native/kafbat)+security+sasl; apicurio url+none/basic/bearer; kafka_connect
  url; redis host/port/dedupDb/bookmarksDb/password; apisix adminUrl/runtimeUrl/adminKey).
  Secrets write-only + `hasSecret`. Test dispatch per type (adapt alpha clients; redis test
  = via NiFi controller-service validation or socket test from NiFi host — implement as
  "verified via NiFi" status; kafka test via Kafbat when mode=kafbat or TCP fails).
- **T2.2** activate (one-active per type, dependents check), repoint (adopt/migrate/reset —
  adapt alpha lifecycle runner to new dependents = deployed flows via provenance), delete
  with impact; audit all.

## WS3 — Application Services backend (D11)

- **T3.1** CRUD with revisions (edit → rev n+1; flows pin `servicePins`), retire/reinstate
  (logical), private services, per-type validation. Secrets write-only.
- **T3.2** Test per type: http (request probe honoring auth mode incl. session_token login
  round-trip + oauth2 client-credentials token fetch), database (TCP/driver probe where
  possible — trino via HTTP; postgres/mysql socket probe), external_kafka (Kafbat/TCP),
  sink_destination (OpenSearch URL probe; Iceberg catalog v1/config OAuth probe — alpha
  `iceberg_catalog_client.py`).
- **T3.3** Sink destination extended fields (D11): Iceberg (catalog URL, warehouse, OAuth2
  client id/secret, S3 endpoint/keys/region/path-style), OpenSearch (+optional basic auth).

## WS4 — APISIX backend (D13)

- **T4.1** APISIX admin client (upstreams/routes/ssl CRUD, X-API-KEY, list/diff).
- **T4.2** Proxy model → reconcile: create/update upstream + prefix routes
  (`/<proxy-token>` + `/<proxy-token>/*` with proxy-rewrite), status Pending/Reconciled/
  Failed + statusDetail; test = request through runtime URL; delete refused while flows
  depend (server check).
- **T4.3** Cert profiles (APISIX ssl objects / upstream client-cert config) + host
  allowlist storage with admin-confirm semantics + audit; reconcile order certs→upstreams→
  routes on gateway repoint (D13).

## WS5 — Schemas backend (D10)

- **T5.1** Approved schemas + templates CRUD; ceremony approve endpoint = register to
  Apicurio ccompat under `<topic>-value` (alpha apicurio_client) + record approval history;
  re-approve idempotent.
- **T5.2** Independent verify endpoint (Avro parse via fastavro + ccompat compatibility
  check, NO registration) and independent register endpoint (direct ccompat register).
- **T5.3** Granular delete: DELETE approval version (ccompat/native version delete
  best-effort + local removal; last-version → whole delete rule), DELETE whole schema
  (subject delete + local), DELETE template. Audited, with dependent-flow guards
  (409 when a deployed kafka_kc flow references it).
- **T5.4** File-upload inference endpoint: multipart upload (json/ndjson/csv/xlsx/xml),
  record-path resolution, `schema_inferencer.infer_avro_schema` — returns Avro + report
  (field count, notes). Used by new-schema creation and ceremony "uploaded" path
  server-side parity.

## WS6 — OpenAPI backend (D12)

- **T6.1** Port alpha `openapi_parser.py` + `openapi_specs` router under /api/v2
  (parse/upload with checksum dedupe, get, operations search paginated). Spec linkage =
  `FlowBlock.config.openapiSpecId`.

## WS7 — Flow compiler & lifecycle (D5–D9, D16, D17) — THE CORE

- **T7.1** Compiler IR: `Flow` → deployment plan (per-block PG specs: processors,
  controller services, connections, ports; parameter context; topics; connectors;
  runtime-scope map). Pure function, unit-tested via golden JSON fixtures (adapt alpha
  generator patterns + reference-flow configs).
  Covers: http read (+pagination 4 styles, auth modes incl. session_token pre-step,
  proxy egress base-url swap), http write/lookup, jdbc read (incremental/watermark)
  /write/lookup, kafka read (json/csv/xml/raw)/write, kafka_kc (Avro publish + registry +
  Connect sink), kc (Connect sink from existing topic), transforms chain (user order),
  dedup (D6 template), routing (D7), DLQ failure paths (D17), entity/topic naming
  (`naming.ts` parity: tokenize + collision rules).
- **T7.2** NiFi deployer: plan → live NiFi via REST (create PG hierarchy, controller
  services enable-wait, processors, connections, parameter context, start/stop). Adapt
  alpha `nifi_client`/generator lifecycle helpers (JWT, 409 retry, cleanup on failure).
- **T7.3** Connect deployer: kafka_kc/kc → connector configs (OpenSearch/Iceberg from sink
  service, D11 fields; alpha `iceberg_sink_config.py` reference), created stopped; start/
  stop/delete with flow verbs.
- **T7.4** Verbs: deploy (preflight per prototype `deployPreflight` rows + MVP checklist →
  compile → apply → runtime-scope map + provenance fingerprints), start/pause/resume/stop/
  stop_clear (queue drop audited)/redeploy/undeploy/delete (D16); topic + DLQ management
  via Kafka admin (Kafbat REST first); dedup cache clear action; enable/disable.
- **T7.5** Runtime/observability: metrics endpoint (NiFi PG status → per-block via scope
  map + topic counts), messages endpoint (group-less viewer via Kafbat), DLQ endpoint,
  runtime refresh (live NiFi/Connect state + drift findings vs stored fingerprints),
  force-repair (clear dead refs, record orphans). Block test endpoint: bounded live probe
  for http/jdbc/kafka read (adapt alpha test-stream resolver; ≤10 records, no commits) —
  feeds `BlockTestResult`.

## WS8 — Frontend: API swap + required UI changes

**API swap (after WS1 contract):**
- **T8.1** `src/prototype/api.ts` reimplemented as HTTP client against `/api/v2` (same
  exported function names/types; remove sleep/store). `store.ts`/`seeds.ts` retired from
  runtime (kept only for tests that exercise pure logic). Env `VITE_BACKEND_URL`.
  Loading/error behavior via existing react-query usage. Import-connector wizard gets a
  real file picker (D22).

**UI tasks (independent, can run before backend):**
- **T8.U1** Dashboard: trim Sink connectors card verbosity (D20).
- **T8.U2** APISIX page: remove ConnectionHeader card & "Manage on Platform Connections"
  button; polished Add-Certificate dialog; polished Add-Host flow keeping admin confirm
  (D13). Keep Proxies untouched.
- **T8.U3** Schemas: remove Duplicate + Check only; unified editor experience (Add Field
  in top action row at extreme right beside Structured Edit/Raw Avro JSON tabs; always
  available), remove Discard changes; single consistent Save; approved schemas directly
  editable (D10 UI part); granular Delete dialog (version vs whole) restored.
- **T8.U4** Schemas: new-schema creation with file upload + inference (reuse ceremony
  upload components; backend T5.4 when available, client-side inference until then).
- **T8.U5** Schemas: independent Verify + Register actions wired (needs T5.2).
- **T8.U6** Flows page: Deploy/Redeploy as direct row button; remove Root column & other
  root exposure; Overview tab rebuild per D14 (keep Metrics/DLQ/Messages/Runtime).
- **T8.U7** Flow Builder Identity/Service: explicit "Existing service | Set up here"
  choice; manual mode renders inline ServiceFormFields (creates private service; D11).
- **T8.U8** Flow Builder HTTP adapter: OpenAPI upload control at top of Adapter settings
  (upload → summary → searchable endpoint dropdown on Path field; defaults toward manual
  endpoint mode; never locks the user; needs T6.1).
- **T8.U9** Flow Builder: remove EgressLine block from HTTP Advanced accordion (D13/prompt
  8.3). Proxy remains visible on the service form only.
- **T8.U10** Flow Builder dedup polish (D6): TTL window field honoring 1min–365d
  (default 24h), identity-fields validation messaging, "cache clears on config change"
  note, Clear-dedup-cache action surfaced (Flows detail), R8/kc hosting already enforced.
- **T8.U11** Test block wired to real block-test endpoint (T7.5); ceremony live-sample path
  consumes real test results.

## WS9 — End-to-end verification (mandatory, real infra)

- **T9.1** Journey A (HTTP→dedup→kafka_kc→Iceberg or OpenSearch): create services via UI
  API, build flow (http read dummyjson-like source w/ pagination → transforms → dedup →
  kafka_kc), ceremony w/ uploaded samples, deploy, verify NiFi PG structure (block PGs,
  dedup processors, ordering), start, feed duplicates, verify: non-dupes in topic (Kafbat),
  dupes suppressed, missing-identity → DLQ, Connect sink running, Apicurio subject
  registered, UI state matches.
- **T9.2** Journey B (routing): flow with ≥2 conditional branches (+1 unconditional),
  deploy, verify one RouteOnAttribute per branch decision (chained for match=all), records
  land per-branch topics correctly, non-matching counted-drop.
- **T9.3** Journey C (APISIX egress + session token): http service with proxy → deploy →
  verify APISIX route/upstream created + traffic flows through gateway; session_token
  service login-once-per-run visible in NiFi structure.
- **T9.4** Journey D (schemas): create template via upload-inference, verify independently,
  register independently, delete version, delete schema; Apicurio state checked each step.
- **T9.5** Journey E (lifecycle + connections): pause/resume/stop&clear/redeploy/undeploy/
  delete against live NiFi; connection test-all; runtime drift detection after manual NiFi
  edit; force repair. Audit entries verified throughout.

## WS10 — Reviews & closure

- **T10.1** Opus-class review per completed workstream (schemas, services+connections,
  APISIX, flow builder+compiler, dedup, routing, lifecycle) with findings → correction
  tasks.
- **T10.2** Final audit vs goal prompt §-by-§; update orchestration state; final summary.
