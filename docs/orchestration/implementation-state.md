# Implementation State

Statuses: NOT STARTED | IN PROGRESS | IMPLEMENTED | VERIFICATION FAILED |
CORRECTION IN PROGRESS | VERIFIED | BLOCKED

Last updated: 2026-08-13 — COMPLETE. Backend 653/0, frontend 157/0; journeys A/E, B, C/D, R1-R8 all PASS; final audit done.

| Task | Status | Notes |
|---|---|---|
| Analysis (6 reports + dedup reference) | VERIFIED | docs/orchestration/analysis/ |
| Plan + decisions | IMPLEMENTED | this folder |
| T0.1 Mongo via docker | VERIFIED | dmp-mongo container Up, port 27018 |
| T0.2 Backend boots | VERIFIED | uvicorn :8010, /api healthy, NiFi seed test Healthy (2.9.0); kafka TCP seed test slow-fails as expected |
| T0.3 Frontend dev + vitest baseline | VERIFIED | vitest 27 files/148 tests green; tsc pre-existing errors only in dead legacy files (FlowDesigner.tsx, schemaCreate.test.ts) |
| T1.1 Adapter models | IMPLEMENTED | 29/29 model tests pass; full suite 340 pass (1 pre-existing unrelated fail: test_connection_fingerprint). Field fixes vs types.ts: `revision`, `targetHost`; secret keys in models/adapter/_secrets.py |
| T1.2 /api/v2 routers + legality | VERIFIED | all 8 v2 routers mounted in server.py; full suite 538/538; live endpoints verified |
| T1.3 Audit + seed connections | VERIFIED | seed_v2_connections at startup: 6 connections seeded from env, audit trail live |
| T2.1 Connections config/test | VERIFIED | live: nifi/apicurio/kafka_connect/redis(indirect)/apisix Healthy; kafka Healthy via Kafbat w/ creds (1 broker/20 topics) |
| T2.2 Activate/repoint/delete | IMPLEMENTED | activate/adopt/delete+impact done; migrate/reset 501 pending engine (by design until T7.x) |
| T3.1 Services CRUD/revisions | IMPLEMENTED | routers/v2/services.py; 36 tests; live http/session_token/oauth2/trino/iceberg probes |
| T3.2 Services test | IMPLEMENTED | with T3.1 |
| T3.3 Sink dest fields | IMPLEMENTED | iceberg oauth+s3 fields; oauthClientSecret added to secret keys |
| T4.1 APISIX client | IMPLEMENTED | services/apisix_client.py; 13/13 unit tests; live smoke ok (10 routes) |
| T4.2 Proxy reconcile | IMPLEMENTED | routers/v2/gateway.py; 40 tests; real upstream+routes+ssl bodies, allowlist gate |
| T4.3 Certs + allowlist | IMPLEMENTED | with T4.2; PEMs write-only, admin-gated allowlist |
| T5.1 Schemas CRUD + approve | IMPLEMENTED | routers/v2/schemas.py; 16 tests; register-failure atomicity verified |
| T5.2 Independent verify/register | IMPLEMENTED | /verify (ccompat compat check, no registration) + /register standalone |
| T5.3 Granular delete | IMPLEMENTED | version delete (registry best-effort, last-version rule) + whole delete (deployed-flow 409) |
| T5.4 Upload inference | IMPLEMENTED | /infer multipart json/ndjson/csv/xlsx/xml + recordPath + suggestions |
| T6.1 OpenAPI port | IMPLEMENTED | routers/v2/openapi.py; 9/9 tests; suite 362 pass (1 pre-existing env fail) |
| T7.1 Compiler IR | IMPLEMENTED | 7/7 tests; stub-fill agent running (jdbc/kafka-read/http-write/lookup/csv-xml) |
| T7.2 NiFi deployer | VERIFIED | LIVE apply+teardown on nifi.datapasc.com (dmp_ci_smoke: 2 block PGs, 8 procs, CS enabled); recursive CS-disable fix |
| T7.3 Connect deployer | IMPLEMENTED | connect_apply.py; live check in E2E |
| T7.4 Verbs | IMPLEMENTED | lifecycle.py all verbs + preflight + dedup epoch clear; suite 554 pass |
| T7.5 Runtime/metrics/test | VERIFIED | 19 tests; live: metrics/dlq/messages/runtime honest states; block test live: ok=true, 10 records, fields detected |
| T8.1 api.ts swap | VERIFIED | 66 exports mapped; 14-test live smoke incl. full flow write path; vitest 157 green |
| T8.U1 Dashboard sink card | IMPLEMENTED | label+hint trimmed; vitest 148 green |
| T8.U2 APISIX UI edits | IMPLEMENTED | ConnectionHeader card removed; cert dialog; allowlist polish w/ admin gate |
| T8.U3 Schemas editor consistency | IMPLEMENTED | all 5 changes; new api fns saveApprovedSchemaDraft/deleteApprovedSchemaVersion/deleteApprovedSchema; vitest 148 green |
| T8.U4 New-schema upload/infer | IMPLEMENTED | SampleInferencePanel shared w/ ceremony; two-path New template; vitest 148 green |
| T8.U5 Verify/Register actions | VERIFIED | live: verify+compat checks, register -> global id 45; vitest 157 green |
| T8.U6 Flows page changes | IMPLEMENTED | Deploy direct button; Root column removed; Overview rebuilt (Deployment/Entity outputs/Blocks/Topics) |
| T8.U7 Service "Set up here" | IMPLEMENTED | ToggleGroup 2-mode ServiceSelector; sink pickers share it; vitest 148 green |
| T8.U8 OpenAPI upload UI | VERIFIED | live parse/search/detail verified; combobox path picker; operationsCount projection bug found+fixed w/ regression test |
| T8.U9 Remove Egress block | IMPLEMENTED | EgressLine removed from BlockForm; validation.ts untouched |
| T8.U10 Dedup polish | IMPLEMENTED | window unit control + bounds validators + captions + clear-cache action; vitest 157 green |
| T8.U11 Test block wiring | VERIFIED | delivered via T8.1 (testBlock->real endpoint) + T7.5 (endpoint); live probe: 10 records, detectedFields |
| T9.1–T9.5 E2E journeys | VERIFIED | A+E PASS (dedup proven live; Iceberg snapshot; lifecycle+drift); B PASS (routing structural+behavioral, 90/39/45 exact); C+D PASS (APISIX egress, schema lifecycle). Defect corrections landed; live re-verification R1-R8 in flight |
| T10.1 Reviews | VERIFIED | Opus review triaged: C2 refuted w/ live evidence; all other C/M findings fixed in correction wave (644 backend tests green) or documented as intended semantics (M3) |
| T10.2 Final audit | VERIFIED | final-audit.md — every prompt § mapped to evidence; R1-R8 all PASS (R3 live-proven post-fix) |

## Blockers

- None currently. Kafka broker TCP + Redis unreachable from dev machine by design —
  mitigations decided (Kafbat REST; Redis via NiFi) — see decisions D15.
| User-reported fixes (path UX, test-save, topic self-own, InvokeHTTP) + schema registration UX | VERIFIED | diag-invokehttp.md, ui-playwright-journey.md, schema-registration-verify.md; suites backend 660/0, frontend 161/0 |
