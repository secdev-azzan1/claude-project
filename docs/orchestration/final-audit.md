# Final Audit — goal prompt vs delivered system

Date: 2026-08-13. Verdicts cite evidence in docs/orchestration/e2e/*, analysis/*, reviews/*,
and the test suites (backend 645+/0 failed, frontend 157/0 at audit time).

## Prompt requirements

| § | Requirement | Verdict | Evidence |
|---|---|---|---|
| 1 | Dashboard kept; Sink Connector card verbosity reduced | DONE | T8.U1; browser pass (live KPIs, trimmed card) |
| 2 | Audit Log unchanged, genuinely functional | DONE | listAudit live; every verb/admin action audited (journeys show entries) |
| 3 | APISIX: gateway card + Manage button removed; Proxies kept & real; cert-profile creation polished; allowlist polished (admin-gated) | DONE | T8.U2 + T4.x; Journey C: reconcile → live upstream/routes; browser pass |
| 4 | Platform Connections: correct auth/config per type | DONE | 6 types incl. Redis host/port/2 DBs/password (matches infra), kafka native/kafbat (+kafbat creds added), apisix admin+runtime+key; all 6 live-Healthy |
| 5 | App services: HTTP auth incl. session token; sink destinations complete | DONE | 6 auth modes; session_token live (login-per-run in NiFi; Groovy sensitive-props design after NiFi constraint); iceberg_catalog full OAuth+S3 fields (live snapshots); opensearch +creds |
| 6 | Schemas: Duplicate/Check-only removed; granular delete (version vs whole); consistent registered/unregistered editing; Add Field top-right; no Discard; consistent Save; upload-inference at creation; independent Verify/Register | DONE | T8.U3/U4/U5 + T5.x; Journey D + R6 (registry version arithmetic proven); browser pass (editor layout) |
| 7 | Flows page: direct actions (Deploy); root removed; Overview rebuilt; Metrics/DLQ kept | DONE | T8.U6; browser pass; Metrics/DLQ/Messages/Runtime all backend-real (T7.5) |
| 8 | Builder: layout/graph/+/Destinations preserved | DONE | untouched; browser pass (canvas nodes verified via DOM) |
| 8.1 | Service section: existing service OR set-up-here (private service; backend distinction) | DONE | T8.U7; private:true services server-side |
| 8.2 | OpenAPI upload at top of HTTP adapter; searchable endpoint dropdown; never locks in | DONE | T6.1 + T8.U8 (live parse/search; 5MB/YAML guards) |
| 8.3 | Adapter Settings kept; Egress block removed | DONE | T8.U9 |
| 8.4 | Generic Transform kept | DONE | + dedup polish (T8.U10) |
| 8.5 | Test block real | DONE | live probe: ok=true, 10 records, detectedFields |
| 8.6 | Dedup per MVP; reference flows studied; NiFi-verified with real duplicates | DONE | MVP rules implemented (last-in-chain, SHA-256, Redis, TTL 1min–365d, fail-stop, identity→DLQ, epoch cache-clear, config-change warning); live DummyJson_Dedup reference adapted; Journey A: 30 dupes suppressed in NiFi (DetectDuplicate 60 in/30 out), topic stable, DLQ 0 |
| 8.7 | Routing: UI as reference; multiple conditions = genuine NiFi decision processors | DONE | chained RouteOnAttribute (all-match), single-with-per-rule-props 'matched if any' (any-match), unconditional fan-out; Journey B live: structure read back from NiFi + exact counts 90/39/45 |
| 8.8 | Adapter architecture per MVP | DONE | 5 adapters + modes; contract-driven config respected |
| 8.9 | Each adapter = its own NiFi Processor Group | DONE | flow PG + child PG per block (`<block>__<adapter>`), port-linked; verified in every journey |
| 8.10 | Kafka+KC terminal rule enforced | DONE | legality.ts (UI) + validate_placement (server) + compiler fail-fast; kafka read/write correctly non-terminal |
| 9 | Alpha backend reused/adapted; new product model served | DONE | FastAPI+Mongo kept; NiFi/Kafka(+Kafbat)/Apicurio/Connect/iceberg clients reused; new /api/v2 adapter-model API mirrors prototype api.ts (66 exports) |
| 10 | E2E verification mandatory | DONE | Journeys A+E, B, C+D, R1–R8 + browser pass, all with infra-level evidence |
| 11 | Real infrastructure used | DONE | NiFi/Kafka(Kafbat)/Connect/Apicurio/APISIX/Redis(via NiFi)/Polaris/Ozone all exercised |
| A | No secrets in frontend/committed/logs | DONE | .env gitignored; write-only secrets + has* flags; sensitive NiFi params |
| B | Originals unmodified | DONE | all work in claude-project; originals read-only throughout |
| C–G | Verification/autonomy constraints | DONE | independent verification per task; review + E2E + corrections cycles |

## Known limitations (honest)

1. Connection repoint `migrate`/`reset` return 501 (adopt works) — deep repoint automation
   deferred; documented in-product.
2. Drift detection covers PG-missing, fingerprint-mismatch, rename (as out-of-band-edit);
   arbitrary property-level out-of-band edits are not diffed.
3. jdbc adapter compiles (tested) but had no live E2E journey (no reachable JDBC database in
   the provided infra); property names flagged for first live use.
4. Alpha-only concepts intentionally retired: legacy routers (sources/flow_import/nifi_services/
   iceberg_sinks lifecycle) not mounted; connector export/import stays at prototype scope with a
   real file picker.
5. Session-token final design (Groovy sensitive-props login): unit/compile-tested + NiFi-accepted
   mechanism (same as dedup hash); full live token round-trip re-run: see journey-r addendum.
6. Kafka broker TCP + Redis are cluster-internal by design; app uses Kafbat REST and
   NiFi-mediated Redis (documented, preflight-aware).
