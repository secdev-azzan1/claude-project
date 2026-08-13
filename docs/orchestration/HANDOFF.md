# HANDOFF — continuation guide (written 2026-08-13, mid-task)

Purpose: if the current session stops abruptly, continue from here. Older work is
summarized; the ACTIVE tasks are detailed. Read order for a new agent:
this file → implementation-state.md → decisions.md → the e2e/ evidence logs.

## 1. What this project is

Full-stack "Data Mobility Platform": React/Vite frontend (`frontend/`, from the
finalized prototype) + FastAPI/MongoDB backend (`backend/`, adapted from the alpha)
that compiles user-built flows (adapter blocks: http/jdbc/kafka/kafka_kc/kc) into live
NiFi process groups + Kafka Connect connectors against REAL infrastructure
(nifi/kafka(+kafbat)/apicurio/kafka-connect/apisix/redis/polaris/ozone @ *.datapasc.com;
credentials in `backend/.env`, gitignored). Orchestration state lives in
`docs/orchestration/` (decisions.md, compiler-spec.md, implementation-state.md,
verification-state.md, analysis/, reviews/, e2e/).

## 2. How to run

- Mongo: docker container `dmp-mongo` (port 27018). `docker start dmp-mongo`.
  ⚠ Docker Desktop is flaky on this machine (hung engine 3×): recover with
  `wsl --shutdown` + relaunch Docker Desktop.
  ⚠ A leftover compose stack `data-mobility-platform-*` (the user's FIRST-attempt app)
  auto-starts with Docker and squats ports 8010/3000 — STOP those containers
  (`docker stop data-mobility-platform-backend-1 ...frontend-1 ...mongo-1`); restart
  policy already set to `no`, but it has resurrected once — always check
  `docker ps` + that `GET :8010/api` returns `{"message":"NIF Abstractor API",...}`
  (the imposter returns `{"service":"nif-abstractor-api",...}`).
- Backend: `cd backend; .venv\Scripts\python.exe -m uvicorn server:app --port 8010`
  (boot ≈60-90s: startup live-tests connections). Restart procedure: kill ONLY
  processes whose command line matches `uvicorn server:app` (Get-CimInstance filter);
  NEVER blanket-kill netstat PIDs (that killed Docker once).
- Frontend: `cd frontend; npm run dev` → http://localhost:3001 (VITE_BACKEND_URL in
  frontend/.env.local → :8010).
- Suites: backend `cd backend; .venv\Scripts\python.exe -m pytest tests/ -q`
  (baseline **671 passed, 1 deselected** — the deselected is a live-marked NiFi test);
  frontend `npx vitest run` (**161 passed**) and `npx tsc -p tsconfig.app.json --noEmit`
  (pre-existing errors ONLY in dead legacy `FlowDesigner.tsx` + `schemaCreate.test.ts`).
- Playwright: specs in `frontend/e2e/` (`playwright.config.ts`, `ui-journey.spec.ts` is
  the PROVEN full lifecycle spec with hard-won selectors — reuse its patterns).
  Run: `cd frontend; npx playwright test -c e2e/playwright.config.ts`.
  Kafka from this dev machine: broker TCP unreachable — everything goes through Kafbat
  REST (handled inside backend/services/kafka_client.py). Redis reachable only from NiFi.

## 3. Completed (compressed — details in implementation-state.md + git log)

- Full v2 backend (`/api/v2/*`) mirroring the frontend's `src/prototype/api.ts` contract
  (66 exports): flows CRUD+verbs, connections (6 types, env-seeded), services (4 types,
  6 http auth modes incl session_token via sensitive-prop Groovy login), gateway
  (APISIX proxies/certs/allowlist w/ real reconcile+teardown), schemas (templates +
  approved, independent verify/register, ccompat-only writes, version browsing),
  openapi, dashboard, audit.
- Flow compiler+deployer: Flow → NiFi PGs (child PG per block) + Connect connectors;
  dedup (Groovy SHA-256 + DetectDuplicate + Redis, pinned last, epoch cache-clear);
  routing (chained RouteOnAttribute all-match / 'matched-if-any' any-match);
  pagination (4 styles, EL on URL property); DLQ per block; deploy preflight incl.
  topic reservation (own-topics never self-collide) + post-apply validation gate;
  verbs deploy/start/pause/resume/stop/stop_clear/redeploy/undeploy/delete.
- E2E proven live (evidence in docs/orchestration/e2e/): dedup suppression in NiFi
  (30→60 in / 30 out), routing exact counts (90/39/45), Iceberg snapshot landing,
  APISIX egress, session-token login (20 authed fetches), schema lifecycle vs Apicurio,
  full Playwright UI lifecycle journey (ui-playwright-journey.md).
- User-batch fixes (all committed): http path join-safety (base-URL context + auto-strip
  + validation both sides), test-saves-first, topic self-ownership on redeploy, schema
  registration visibility (Registered · #id · vN pill; filters All/Registered/Not
  registered), registry version browsing dropdown, dedup discoverability panel
  ("Enable deduplication" in Generic transformations), Clear topic / Clear DLQ buttons
  (audited counts), Flows row Eye/Overview button (row-click no longer opens sheet),
  sidebar renamed "Proxies", jdbc trino URL + driverLocations fixes.
- Latest commit at time of writing: 33268fa (all the above landed).

## 4. ACTIVE RIGHT NOW (detailed — this is where to resume)

Two background E2E agents are running (started ~13:5x local). **USER RULE: DO NOT
DELETE/CLEAN UP ANYTHING they create — flows/schemas/topics/proxies/connectors/tables
stay as user-visible evidence. Stop flows after verification; never undeploy/delete.**

### A. Datatypes journey (spec being written to `frontend/e2e/datatypes.spec.ts`)
Goal: prove JSON, CSV, XML processing end-to-end THROUGH THE UI. Three flows (prefix
`dt`, cron */3): (1) "dt json products" — dummyjson /products, recordPath
$.products[*], split, USES THE NEW Deduplication panel (identity field `id`) and must
also prove suppression (second cron fire → topic count unchanged); (2) "dt csv
addresses" — a probed-reachable public CSV url (candidates: fsu addresses.csv, titanic
csv), responseFormat csv → verify topic messages are JSON objects w/ column fields;
(3) "dt xml feed" — RSS/XML url (bbc rss or w3schools cd_catalog.xml), responseFormat
xml → verify parsed records; XML parse failures must be evidenced via NiFi bulletins
(possible real defect — our XML path is the least live-tested). Evidence log →
docs/orchestration/e2e/datatypes-journey.md + screenshots in
frontend/e2e/artifacts/datatypes/. Status when writing: agent mid-work.
If resuming manually: the spec may be partial; run it, fix selectors only (app bugs
get reported/fixed separately), flows stay in place.

### B. Gateway + Iceberg journey (prefix `gw`/`ice`) — ✅ COMPLETE (PASS/PASS)
Goal: (a) route an API through APISIX via the UI (allowlist dummyjson.com admin-confirm
→ proxy "gw dummyjson" → reconcile → verify dmp_* upstream/routes on
apisix-admin → service w/ gateway egress → flow "gw via gateway" deploy/start →
records via https://apisix.datapasc.com/gw_dummyjson) and (b) governed Iceberg sink:
sink service "ice polaris" (catalogUrl https://polaris.datapasc.com/api/catalog,
warehouse bronze, oauth root/s3cr3t, s3 ozones3g eltadmin/OzoneS3Key123 path-style),
flow "ice users" http→kafka_kc entity ice_user, schema via UI ceremony (uploaded
samples path; API-approve fallback allowed+documented), deploy/start, connector
`ice_users.<blockId>.kafka_kc` RUNNING, then DATA LANDING check via Trino
(POST https://trino.datapasc.com/v1/statement, X-Trino-User header, follow nextUri;
discover catalog/schema via SHOW CATALOGS/SCHEMAS/TABLES; expect ~30 rows in the
ice_user table after the Connect commit interval ~60-180s). Evidence log →
docs/orchestration/e2e/gateway-iceberg-journey.md. RESULT: PASS both parts. Gateway: proxy dmp_gw-proxy-dw0e0l reconciled on live APISIX,
flow `gw via gateway` routed through https://apisix.datapasc.com/gw_dummyjson, 30
records; Iceberg: flow `ice users`, connector ice_users.b-wm9mil.kafka_kc RUNNING,
94 rows verified via Trino `SELECT count(*) FROM bronze.bronze.ice_user` (catalog for
warehouse bronze is `bronze`, NOT `iceberg`), Apicurio subject raw.ice_users.ice_user-value
gid 27. Everything left in place (flows Stopped, deployed). Log:
e2e/gateway-iceberg-journey.md; artifacts frontend/e2e/artifacts/gwice/.
DEFECTS found → a fix agent is running for the two real ones:
(1) schema inference emits duplicate Avro named types for same-named nested objects
(fix = path-derived type names in BOTH frontend/src/prototype/inference.ts and
backend/services/schema_inferencer.py + uniqueness pass + tests);
(2) Iceberg sink service UI form missing OAuth/S3 fields (ServiceFormFields.tsx).
(3-4 minor: Trino catalog naming doc'd above; cosmetic DOM-nesting warning in
SampleInferencePanel.)

### Watch-outs learned running these journeys
- Playwright: "Entity & derived names" section is FORCED-OPEN (disabled trigger) for a
  fresh write block — don't click it; new canvas nodes need explicit click-to-select;
  Undeploy menuitem needs exact:true; add-block via node "+" → menuitem "kafka · write".
- One backend restart mid-run breaks polling loops — spec API polls tolerate ~90s.

## 5. NEXT after the active tasks complete

1. Compile both evidence logs + screenshots into the final user report (per-datatype
   PASS/FAIL, gateway + Iceberg verdicts, everything left in place; list the flow
   names/topics/subjects/tables so the user can inspect).
2. Commit specs + evidence; update implementation-state.md; mark task #7 complete.
3. If XML (or CSV) parsing FAILED: fix the compiler's xml/csv response path
   (backend/services/adapter/compiler/blocks_http.py `_parse_response`,
   `_ensure_xml_reader` — XMLReader infer-schema quirks are the prime suspect), add
   compiler tests, redeploy the dt flow, re-verify. Same treatment for any gateway or
   Iceberg defect found.
4. Still-unverified NiFi property names (flagged, need a live flow to confirm):
   `QueryDatabaseTableRecord` "Initial Load Strategy", `PutDatabaseRecord` statement
   config, `ConsumeKafka` property set, `LookupRecord`/`DatabaseRecordLookupService` —
   a live jdbc read (Trino service exists: https://trino.datapasc.com, driver
   io.trino.jdbc.TrinoDriver, URL without /db suffix, maybe needs driverLocations JAR
   path on the NiFi host) and a kafka-read flow would close these.
5. Known accepted limitations (docs/orchestration/final-audit.md): connection repoint
   migrate/reset = 501; drift detection covers missing/renamed PG + fingerprint only;
   connector export/import at prototype scope.

## 6. Conventions for continuation

- Commit style: conventional prefix + body, Co-Authored-By Claude line (or your own).
- Never modify `C:\Users\kaifm\Desktop\Project\*` (read-only references).
- Secrets stay in backend/.env (gitignored) — never in frontend/source/commits.
- Every fix needs a test; suites must stay green (671/161 baseline).
- Evidence-preserving mode is ACTIVE: do not delete user-visible artifacts.
