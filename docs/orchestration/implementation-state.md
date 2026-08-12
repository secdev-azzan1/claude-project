# Implementation State

Statuses: NOT STARTED | IN PROGRESS | IMPLEMENTED | VERIFICATION FAILED |
CORRECTION IN PROGRESS | VERIFIED | BLOCKED

Last updated: 2026-08-13 (planning complete)

| Task | Status | Notes |
|---|---|---|
| Analysis (6 reports + dedup reference) | VERIFIED | docs/orchestration/analysis/ |
| Plan + decisions | IMPLEMENTED | this folder |
| T0.1 Mongo via docker | NOT STARTED | Docker Desktop launched, engine warming |
| T0.2 Backend boots | NOT STARTED | venv install ran in background |
| T0.3 Frontend dev + vitest baseline | NOT STARTED | npm install done |
| T1.1 Adapter models | NOT STARTED | |
| T1.2 /api/v2 routers + legality | NOT STARTED | |
| T1.3 Audit + seed connections | NOT STARTED | |
| T2.1 Connections config/test | NOT STARTED | |
| T2.2 Activate/repoint/delete | NOT STARTED | |
| T3.1 Services CRUD/revisions | NOT STARTED | |
| T3.2 Services test | NOT STARTED | |
| T3.3 Sink dest fields | NOT STARTED | |
| T4.1 APISIX client | NOT STARTED | |
| T4.2 Proxy reconcile | NOT STARTED | |
| T4.3 Certs + allowlist | NOT STARTED | |
| T5.1 Schemas CRUD + approve | NOT STARTED | |
| T5.2 Independent verify/register | NOT STARTED | |
| T5.3 Granular delete | NOT STARTED | |
| T5.4 Upload inference | NOT STARTED | |
| T6.1 OpenAPI port | NOT STARTED | |
| T7.1 Compiler IR | NOT STARTED | core |
| T7.2 NiFi deployer | NOT STARTED | core |
| T7.3 Connect deployer | NOT STARTED | |
| T7.4 Verbs | NOT STARTED | |
| T7.5 Runtime/metrics/test | NOT STARTED | |
| T8.1 api.ts swap | NOT STARTED | |
| T8.U1 Dashboard sink card | NOT STARTED | |
| T8.U2 APISIX UI edits | NOT STARTED | |
| T8.U3 Schemas editor consistency | NOT STARTED | |
| T8.U4 New-schema upload/infer | NOT STARTED | |
| T8.U5 Verify/Register actions | NOT STARTED | |
| T8.U6 Flows page changes | NOT STARTED | |
| T8.U7 Service "Set up here" | NOT STARTED | |
| T8.U8 OpenAPI upload UI | NOT STARTED | |
| T8.U9 Remove Egress block | NOT STARTED | |
| T8.U10 Dedup polish | NOT STARTED | |
| T8.U11 Test block wiring | NOT STARTED | |
| T9.1–T9.5 E2E journeys | NOT STARTED | |
| T10.1 Reviews | NOT STARTED | |
| T10.2 Final audit | NOT STARTED | |

## Blockers

- None currently. Kafka broker TCP + Redis unreachable from dev machine by design —
  mitigations decided (Kafbat REST; Redis via NiFi) — see decisions D15.
