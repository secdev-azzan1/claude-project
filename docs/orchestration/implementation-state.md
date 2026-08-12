# Implementation State

Statuses: NOT STARTED | IN PROGRESS | IMPLEMENTED | VERIFICATION FAILED |
CORRECTION IN PROGRESS | VERIFIED | BLOCKED

Last updated: 2026-08-13 (planning complete)

| Task | Status | Notes |
|---|---|---|
| Analysis (6 reports + dedup reference) | VERIFIED | docs/orchestration/analysis/ |
| Plan + decisions | IMPLEMENTED | this folder |
| T0.1 Mongo via docker | VERIFIED | dmp-mongo container Up, port 27018 |
| T0.2 Backend boots | VERIFIED | uvicorn :8010, /api healthy, NiFi seed test Healthy (2.9.0); kafka TCP seed test slow-fails as expected |
| T0.3 Frontend dev + vitest baseline | VERIFIED | vitest 27 files/148 tests green; tsc pre-existing errors only in dead legacy files (FlowDesigner.tsx, schemaCreate.test.ts) |
| T1.1 Adapter models | IMPLEMENTED | 29/29 model tests pass; full suite 340 pass (1 pre-existing unrelated fail: test_connection_fingerprint). Field fixes vs types.ts: `revision`, `targetHost`; secret keys in models/adapter/_secrets.py |
| T8.U1 status note | IMPLEMENTED | see row below |
| T1.2 /api/v2 routers + legality | NOT STARTED | |
| T1.3 Audit + seed connections | NOT STARTED | |
| T2.1 Connections config/test | NOT STARTED | |
| T2.2 Activate/repoint/delete | NOT STARTED | |
| T3.1 Services CRUD/revisions | NOT STARTED | |
| T3.2 Services test | NOT STARTED | |
| T3.3 Sink dest fields | NOT STARTED | |
| T4.1 APISIX client | IMPLEMENTED | services/apisix_client.py; 13/13 unit tests; live smoke ok (10 routes) |
| T4.2 Proxy reconcile | NOT STARTED | |
| T4.3 Certs + allowlist | NOT STARTED | |
| T5.1 Schemas CRUD + approve | NOT STARTED | |
| T5.2 Independent verify/register | NOT STARTED | |
| T5.3 Granular delete | NOT STARTED | |
| T5.4 Upload inference | NOT STARTED | |
| T6.1 OpenAPI port | IN PROGRESS | agent running |
| T7.1 Compiler IR | NOT STARTED | core |
| T7.2 NiFi deployer | NOT STARTED | core |
| T7.3 Connect deployer | NOT STARTED | |
| T7.4 Verbs | NOT STARTED | |
| T7.5 Runtime/metrics/test | NOT STARTED | |
| T8.1 api.ts swap | NOT STARTED | |
| T8.U1 Dashboard sink card | IMPLEMENTED | label+hint trimmed; vitest 148 green |
| T8.U2 APISIX UI edits | IMPLEMENTED | ConnectionHeader card removed; cert dialog; allowlist polish w/ admin gate |
| T8.U3 Schemas editor consistency | IN PROGRESS | agent running |
| T8.U4 New-schema upload/infer | NOT STARTED | |
| T8.U5 Verify/Register actions | NOT STARTED | |
| T8.U6 Flows page changes | IMPLEMENTED | Deploy direct button; Root column removed; Overview rebuilt (Deployment/Entity outputs/Blocks/Topics) |
| T8.U7 Service "Set up here" | IN PROGRESS | agent running |
| T8.U8 OpenAPI upload UI | NOT STARTED | |
| T8.U9 Remove Egress block | IMPLEMENTED | EgressLine removed from BlockForm; validation.ts untouched |
| T8.U10 Dedup polish | NOT STARTED | |
| T8.U11 Test block wiring | NOT STARTED | |
| T9.1–T9.5 E2E journeys | NOT STARTED | |
| T10.1 Reviews | NOT STARTED | |
| T10.2 Final audit | NOT STARTED | |

## Blockers

- None currently. Kafka broker TCP + Redis unreachable from dev machine by design —
  mitigations decided (Kafbat REST; Redis via NiFi) — see decisions D15.
