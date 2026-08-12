# Architectural Decisions

Status legend: each decision is FINAL unless marked provisional. Source-of-truth priority
(per the goal prompt): (1) finalized UI prototype + explicit prompt requirements,
(2) MVP/architecture docs, (3) alpha implementation.

Analysis inputs: `docs/orchestration/analysis/*.md` (architecture-mvp, product-requirements,
alpha-backend, alpha-frontend, prototype-ui, nifi-reference-flows, dedup-reference-flow).

---

## D1 — Project layout

```
claude-project/
  frontend/    <- copy of lovable_ui_adapter_ui_prototype (React/Vite/TS), modified here
  backend/     <- copy of lovable_ui/backend (FastAPI + Mongo), heavily adapted here
  reference/   <- read-only reference material (NiFi flow exports incl. DummyJson_Dedup.json)
  docs/orchestration/  <- plan, decisions, state
```

## D2 — Backend stack: keep FastAPI + MongoDB (Motor)

The alpha backend (FastAPI, Motor/Mongo, 95 endpoints) is close to fully functional and its
infrastructure clients (NiFi, Kafka+Kafbat fallback, Kafka Connect, Apicurio ccompat/native)
are directly reusable. Mongo runs via docker-compose (`mongo:7`). Python 3.13 locally; bump
pinned deps if 3.11-era pins fail to install.

## D3 — API contract: the prototype's `src/prototype/api.ts` IS the contract

The prototype audit's conclusion is decisive: every page consumes `src/prototype/api.ts`
(1589 lines of typed mock functions over `types.ts` domain model). `docs/BACKEND_API_ENDPOINTS.md`
is stale (predates the adapter rework) and is IGNORED as a contract.

**Integration strategy**: create `frontend/src/prototype/api.ts` replacement that calls a real
backend implementing the same function-level semantics (same types, same return shapes,
same guard-reason behavior). Pages and components remain untouched except for the specific
UI changes required by the prompt. The backend gets a new router family (`/api/v2/...`,
"adapter model" API) whose response shapes serialize `types.ts` structures 1:1:

- Flows: list/get/save/delete, verbs (deploy/start/pause/resume/stop/stop_clear/redeploy/
  undeploy), enable/disable, metrics, dlq, messages, runtime (refresh/force-repair), block test.
- Connections: list/save/test/activate/repoint/delete (6 types: nifi, kafka, apicurio,
  kafka_connect, redis, apisix).
- Services: list/save (revisioned)/test/retire/reinstate (4 types: http, database,
  external_kafka, sink_destination).
- Gateway: proxies CRUD/test/reconcile, cert profiles, host allowlist (admin-confirmed).
- Schemas: approved list, templates CRUD, ceremony approve (=register), plus NEW independent
  verify/register/delete endpoints (D10).
- Dashboard summary, Audit list.
- OpenAPI: parse/upload + operations search (restored from alpha, D12).

Alpha's Mongo collections and reusable services are adapted underneath; legacy alpha routers
that conflict with the new model are not mounted.

## D4 — Domain model: prototype `types.ts` shapes stored in Mongo

`Flow` (blocks[], topics[], variables[], servicePins, state, cron, drift...), `FlowBlock`
(adapter, mode, parentId, branch{name,rules,match}, serviceId, entity, config, transforms[],
topicOverride, testResult), `AppService` (4 types, revisions, retired, private), 
`PlatformConnection` (6 types), `GatewayProxy`, `CertProfile`, host allowlist,
`ApprovedSchema` (+approvals[] history), `SchemaTemplate`, `FlowRuntime`, DLQ records,
`AuditEvent`. Pydantic models mirror the TS types field-for-field (camelCase preserved via
alias or stored as-is; decision: **store and serve camelCase JSON exactly as types.ts
defines** to keep the frontend swap 1:1).

## D5 — Flow compiler: NiFi process-group-per-adapter inside one flow PG

- One top-level NiFi PG per flow, named `<tokenized flow name>`.
- **One child PG per block/adapter** (prompt §8.9: "each adapter must exist as its own NiFi
  Processor Group"), named `<blockName>__<adapterId>`, wired via input/output ports.
  The alpha generator already builds child-PG-per-stream with ports; that machinery is the
  reference implementation.
- Trigger: `GenerateFlowFile` seed (cron → NiFi cron schedule) in the root block's PG (R1:
  http/jdbc roots only; kafka reads are continuous).
- HTTP ingest per reference flows: `UpdateAttribute` (init) → `InvokeHTTP` (baseline config
  from nifi-reference-flows §9.1) → response parse (`SplitJson`/`EvaluateJsonPath` or record
  readers) with pagination loops per reference patterns (offset/page/cursor/next-url).
- Publish per reference flows: `PublishKafka` + `Kafka3ConnectionService`
  (bootstrap `kafka:9092` in-cluster) + `AvroRecordSetWriter` with Confluent-compatible
  Apicurio registry (`ConfluentSchemaRegistry` CS → Apicurio ccompat URL) for kafka_kc;
  plain JSON writer for schemaless kafka writes.
- Parameter contexts: one per flow (`<flow>__params`) carrying endpoint/topic/schedule values;
  secrets applied as sensitive parameters. Global platform endpoints resolved from the active
  Platform Connections at compile time.
- Controller services created inside the flow PG (reference flows show per-PG copies; simpler
  and avoids cross-flow coupling).
- kafka_kc / kc blocks additionally compile to Kafka Connect connectors named
  `<flow>.<blockId>.<sinkKind>` (matches live evidence `dmw3gatec0f913.blk_...opensearch`),
  created stopped at deploy, started on flow start.
- Runtime-scope map: compiler records `{blockId -> [nifi component ids / connector names /
  topics]}` on the flow document; the Runtime tab and metrics attribution read this map.

## D6 — Deduplication (MVP-authoritative; reference = live `DummyJson_Dedup` PG)

UI: already present in the prototype as the `dedup` transform kind, pinned last, single
instance, with identityFields/excludedFields/windowHours. Keep; extend config to full MVP:
TTL window bounds 1min–365d (default 24h), validation (≥1 identity field), R8/kc hosting
rules (already enforced). Add "Clear dedup cache" as an audited per-flow/stream action in the
Flows detail panel (Runtime/actions), and warn-at-deploy when dedup config changed
(cache cleared).

NiFi translation (per `analysis/dedup-reference-flow.md`), compiled as the LAST step of the
block's transform chain, immediately before Avro conversion/publish:
1. `ExecuteGroovyScript` (hash): parametrized `SRC=<flow>__<blockId>`, `EXCLUDES=<user
   excluded fields>+ingest_id,ingest_ts,op`, identity fields checked explicitly — record
   missing an identity field routes to the DLQ/failure path (MVP §11.6a), else sets
   `dedupe.key = <SRC>:<identity-values>:<sha256(record minus excludes)>`.
2. `DetectDuplicate`: `Cache Entry Identifier=${dedupe.key}`, `Age Off Duration=<TTL>`,
   cache = `RedisDistributedMapCacheClientService` (TTL=<TTL>) →
   `RedisConnectionPoolService` (connection string/password/db from the active Redis
   platform connection; in-cluster `redis:6379`).
3. Relationships: `non-duplicate` → continue; `duplicate` → auto-terminated (counted,
   intentional outcome); `failure` → DLQ/failure path (fail-stop — NEVER auto-terminated;
   deviation from reference corrected per MVP).
Per-branch dedup = per-block dedup cache key prefix (flow+block) — one cache per stream.

## D7 — Routing translation: one genuine NiFi decision processor per conditional branch

UI semantics (prototype, kept per prompt): branches are evaluated INDEPENDENTLY (a record may
take several); each branch has 0..N rules with `all`/`any` match; a record matching no
conditional branch (and no unconditional branch exists) is a counted drop.

NiFi translation (structural, no imitation shortcuts):
- Fork point: the parent block's output port feeds each branch's entry.
- Unconditional branch: direct connection (full copy — NiFi connections duplicate FlowFiles
  to multiple destinations naturally).
- Conditional branch: a dedicated `RouteOnAttribute` processor **per branch**, named
  `route__<branchName>`, `Routing Strategy = Route to Property name`, one dynamic property
  per rule compiled from the rule (field/op/value → NiFi EL over the record's extracted
  attributes/JSON), with:
  - `match=any`: one dynamic property per rule (`rule_1..rule_N`), all routed to the branch
    (any property match forwards) — genuinely N decision expressions on one processor.
  - `match=all`: a CHAIN of `RouteOnAttribute` processors, one per rule
    (`route__<branch>__rule_<i>`), matched → next rule / final matched → branch entry,
    unmatched → counted-drop (auto-terminate). Multiple decisions = multiple processors,
    exactly as the prompt requires.
- Rule field values are made routable by an `EvaluateJsonPath` attribute-extraction step
  compiled ahead of the routing stage (fields referenced by rules → attributes).
- Op mapping: equals→`equals`, not_equals→`equals():not()`, contains→`contains`,
  starts_with→`startsWith`, regex→`matches`, is_empty→`isEmpty()` (null-safe).

## D8 — Ordering: transforms (user order) → routing fan-out → per-branch chain → dedup last → serialize/publish

Within one block: extract/add/remove/rename/coerce/set_from_attribute compile in user
order (UpdateRecord/RemoveRecordField/EvaluateJsonPath steps); dedup (if present) compiles
last (D6). Branch routing happens at the block boundary (children attach to the block's
output). On a kafka_kc branch, dedup sits immediately before ConvertRecord(JSON→Avro) →
PublishKafka. Metadata injection (`ingest_id`, `ingest_ts`) happens in the enrich step
before dedup hash (which excludes them) — mirroring the reference flow.

## D9 — Terminal rule enforcement (R3/R5) — both sides

Client-side already enforced by `legality.ts` (computeAddMenu returns [] after kafka_kc/kc).
Server adds authoritative validation: flow save + deploy reject any block whose parent is a
terminal adapter (`kafka_kc`, `kc`), and kc must attach to an unsealed topic. Keep UI as
reference behavior, unchanged.

## D10 — Schemas: prototype ceremony KEPT + alpha-style independent operations RESTORED

The prompt overrides the MVP's "ceremony is the only door" rule:
- Keep the ceremony (Declare→Orchestrate→Review→Approve) for kafka_kc blocks — approve
  registers under `<topic>-value` via Apicurio ccompat (alpha `apicurio_client.register_schema`).
- Schemas page changes:
  - REMOVE "Duplicate" and "Check only" template actions.
  - RESTORE granular delete: delete a specific approval version (an entry in `approvals[]`,
    server-side deletes that Apicurio version when possible) vs delete the whole schema
    (artifact + registry subject; template delete stays trivial). Confirmations name what is
    removed; last-version delete of an approved schema = whole-schema delete (alpha edge rule).
  - INDEPENDENT Verify: `POST /schemas/verify` — structural Avro validation + (when registry
    reachable) ccompat compatibility check against the subject, WITHOUT registering. Shown as
    a "Verify" action on templates and edited schemas.
  - INDEPENDENT Register: `POST /schemas/register` — direct registration of the current
    buffer under a chosen/derived subject without the 4-step ceremony. Exposed on templates
    ("Register") and on approved-schema edit flow.
  - Editor consistency: Add Field ALWAYS available (registered or not) — moved into the
    editor header action row (Structured Edit | Raw Avro JSON | ... | Add Field at extreme
    right); approved schemas become directly editable (edit-in-place buffer) without the
    separate "Edit → new version" gate; REMOVE "Discard changes"; ONE consistent "Save"
    button (templates: saves template; approved: saves as new draft version via
    verify/register path).
  - NEW-SCHEMA creation: "New schema" dialog gains a file-upload/inference path reusing the
    ceremony's `parseSampleFile`/`inferAvroFromRecords` frontend machinery + backend
    inference (alpha `schema_inferencer.py`) — upload JSON/NDJSON/CSV(+XLSX server-side),
    pick record path, infer, land in the editor.

## D11 — Services & auth

HTTP app services keep the prototype's 6 auth modes — `none`, `basic`, `bearer`,
`api_key` (header/query), `oauth2` (client credentials), `session_token` (login path,
token JSONPath, token header) — exactly the MVP's six. Backend stores secrets server-side
(write-only, `has_*` presence flags), applies them at compile/test time:
- basic/bearer/api_key → InvokeHTTP request properties / headers / query params.
- session_token → per-run login step compiled ahead of the chain (InvokeHTTP login →
  EvaluateJsonPath token → header injection), failed login = run failure.
- oauth2 → `StandardOauth2AccessTokenProvider` controller service.
Manual/inline config in Flow Builder (Option B) = creates a `private: true` service under
the hood (MVP §15.2 exactly) — UI presents it as "Set up here / Configure manually" with an
explicit toggle; APISIX proxy/service pickers are irrelevant in that mode.
Sink destination services: extend fields to what deployment actually needs (from alpha
`iceberg_sink_config.py` + live connectors): OpenSearch (URL, index prefix, write mode,
username/password optional), Iceberg (catalog URL, warehouse, OAuth2 client id/secret,
S3 endpoint/access key/secret/region/path-style). Database service: dialect postgresql/
trino/mysql; External Kafka receiver unchanged.

## D12 — OpenAPI restored (alpha subsystem, http adapter)

Reuse alpha `openapi_parser.py` + `openapi_specs` router (parse/store/search operations).
Flow Builder HTTP adapter gets an "API documentation" upload control at the TOP of the
HTTP settings section: upload → parse → operations become a searchable dropdown on the
Path field; selecting an operation fills method/path (and known params); uploading switches
the service section toward manual/direct-endpoint mode by default but NEVER locks the user
in (both "existing service" and "manual" remain switchable). Spec stored per-flow-block
(`config.openapiSpecId`). Accept JSON (OAS3 + Swagger2 per alpha parser; YAML rejected with
clear message — matches MVP's JSON-only stance while reusing alpha capability).

## D13 — APISIX integration (net-new backend, prompt-directed UI edits)

Backend module using APISIX Admin API (env: APISIX_ADMIN_URL + X-API-KEY):
- Proxy = one managed unit compiling to APISIX upstream + route(s) (`/‹proxy-name›/*` prefix
  → target host:port with proxy-rewrite), mirroring the existing live route conventions.
- Cert profiles → APISIX SSL objects (client cert on upstream where referenced).
- Host allowlist stored app-side; gates proxy creation/reconcile.
- Reconcile = desired-vs-observed diff + apply; status Pending/Reconciled/Failed persisted.
- Egress: http blocks whose bound service selects a proxy compile InvokeHTTP to the APISIX
  runtime URL (`https://apisix.datapasc.com/<route-prefix>/...`).
UI: remove the ConnectionHeader card + "Manage on Platform Connections" button from the
APISIX page; keep Proxies as-is; rebuild cert-profile add and host-allowlist add as
polished dialogs consistent with the design language (keep admin-confirm semantics for
allowlist).

## D14 — Flows page changes

- Promote **Deploy** (and Redeploy when applicable) out of the overflow menu to a direct
  row button (alpha precedent: all verbs were direct buttons). Row: primary verb, Stop,
  Deploy/Redeploy, Edit, overflow (Stop&Clear, Undeploy, Enable/Disable, Save as Connector,
  Delete).
- REMOVE the Root column (and any other user-facing "root" concept on this page).
- Overview tab rebuilt: keep Metrics/DLQ tabs untouched; new Overview combines alpha's
  useful flow-level info (entity outputs/topics + schema links, NiFi PG id when deployed,
  deploy/run timestamps, block chain) presented in the new design language; validation
  issues stay. Runtime and Messages tabs kept (they're good and backend-supported).

## D15 — Platform connections (backend semantics)

Six types with one-active-per-type (partial unique index), health vs reachability facts,
Test/Test All, activate (blocked while active peer has dependents → repoint), repoint
adopt/migrate/reset reusing alpha `connection_lifecycle_runner` adapted to the new
dependents model, delete with impact preview. Redis/APISIX skip fingerprint blocking
(warn+confirm only). Seed from env at boot (NiFi/Kafka/Apicurio/Kafka Connect/Redis/APISIX
— all available in this deployment), test immediately.
Kafka connectivity from the dev machine: broker TCP is NOT reachable (verified); backend
uses Kafbat REST (https://kafbat.datapasc.com) as the Kafka data path (alpha kafka_client
already implements the fallback); NiFi reaches kafka:9092 in-cluster. Redis likewise only
reachable through NiFi — the app never dials Redis directly; preflight "Redis reachable"
is verified via NiFi controller-service validation.

## D16 — Verbs & lifecycle semantics (MVP-informed, prototype-shaped)

deploy: validate + preflight → compile → create PG (stopped) + connectors (stopped) + DLQ
topic `dlq.<flow>` + data topics; start: start PG + connectors + cron; pause/resume: NiFi
PG stop-start of non-source processors is NOT how pause works — implement pause as NiFi
"stop root trigger, keep rest running" approximation: stop the trigger/ingest processors,
downstream keeps draining; resume restarts them. stop: stop everything (queues retained);
stop_clear: stop + drop queued FlowFiles (audited); redeploy: stop+clear → delete PG →
recompile (fresh deploy path); undeploy: delete PG + connectors, keep DLQ topic, empty
generated data topics, clear dedup caches (flag for next deploy), state → Draft;
delete: undeploy + delete flow doc + delete DLQ + delete owned topics.

## D17 — DLQ & metrics

DLQ topic per flow `dlq.<flow-tokenized>`; failure paths in generated flows route to a
`PublishKafka` DLQ publisher in each block PG (headers: block id, error class) — correcting
the reference flows' auto-terminate gap, per MVP. DLQ tab reads the topic via Kafka client
(Kafbat). Metrics: NiFi PG status snapshot (per-component → per-block via runtime-scope
map) + topic counts; "unavailable", never fake zeros (prototype behavior kept).

## D18 — Audit

Reuse alpha audit collection/router shape; every verb, destructive action, admin allowlist
change, service edit/retire, connection lifecycle, schema approve/register/delete audited.
Frontend Audit page unchanged (its `listAudit` contract implemented server-side; CSV export
stays client-side).

## D19 — Secrets & env

All infra credentials in `backend/.env` (gitignored; `.env.example` committed with blank
values). Never sent to the browser; write-only fields with `has_*` flags (alpha pattern).
Frontend uses `VITE_BACKEND_URL`.

## D20 — Dashboard

Keep as-is except the Sink connectors card: value + label only ("Sink connectors" with
`running/total`), single short hint retained only when undeployed connectors exist; drop
the verbose "on the Connect cluster" hint line.

## D21 — Testing strategy

- Backend: pytest unit tests for compiler (graph→NiFi plan JSON, dedup/routing translation,
  terminal validation), schema endpoints, services/connections; integration tests hit real
  infra where reachable (NiFi, Apicurio, Kafka Connect, APISIX admin) guarded by env flag.
- Frontend: vitest suites for changed components; existing prototype tests for
  legality/mutations/naming/inference must keep passing (they define the model).
- E2E: scripted journeys through the real backend + real NiFi/Kafka/Apicurio/Connect/APISIX
  (details in execution-plan phase E2E), including a dedup duplicate/non-duplicate proof and
  a multi-branch routing proof, inspecting NiFi structure and Kafka topics (via Kafbat).

## D22 — Out of scope (not invented)

No RBAC/auth (alpha+prototype both authless "admin"); no Iceberg platform-connection type;
no webhook/syslog/CDC adapters; no NoSQL/file-share adapters (greyed "coming later" in the
picker stays); no connector marketplace. Legacy alpha routers not needed by the new UI are
not mounted (sources/flow_import legacy model, nifi_services passthrough, iceberg_sinks
legacy lifecycle) — superseded by the adapter model. Connector export/import stays at the
prototype's level (Save as Connector → JSON download; Import wizard gets a REAL file picker
replacing the canned-bundle simulation, since backend can now validate).
