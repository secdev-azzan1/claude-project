# Gateway + Iceberg Sink — UI-first E2E Verification Log

Run date: 2026-08-13 (UTC ~11:30–12:20)
Backend: http://localhost:8010 (fresh process, includes commit `f9fe0ac` — the
Kafbat topic auto-create fix from the earlier journeys, confirmed live by both
deploys creating their topics first try).
Frontend: http://localhost:3001, driven by Playwright (specs under
`frontend/e2e/gateway-iceberg.spec.ts` + `frontend/e2e/gateway-iceberg-2.spec.ts`,
screenshots/JSON evidence under `frontend/e2e/artifacts/gwice/`).
Live infra touched directly (curl / node fetch): APISIX admin
`https://apisix-admin.datapasc.com` (X-API-KEY from backend/.env), APISIX
runtime `https://apisix.datapasc.com`, NiFi REST `https://nifi.datapasc.com`,
Kafka Connect `https://kafkaconnect.datapasc.com`, Apicurio
`https://apicurio.datapasc.com`, Polaris `https://polaris.datapasc.com/api/catalog`,
Trino `https://trino.datapasc.com`.

Prefixes: `gw` (gateway objects), `ice` (governed Iceberg flow).

Shared-backend note: another agent's `dt *` flows ran concurrently on the same
backend; nothing `dt`-prefixed was touched.

**EVIDENCE POLICY (per the mission): NOTHING was deleted.** Both flows are
STOPPED (cron quiesced) but remain deployed. The proxy, allowlist entry,
services, approved schema + registry subject, topics, the (paused) connector
and the Iceberg table all remain in place.

---

## JOURNEY A — APISIX gateway routing ("use apisix gateway to route some api's and use in our application")

### A1 — Sidebar rename evidence
The sidebar's System group now reads **"Proxies"** (`/apisix`), page title
"APISIX Gateway". Screenshots: `A1-sidebar-proxies.png`, `A1b-proxies-page.png`.
**PASS.**

### A2 — Admin-gated allowlist
`dummyjson.com` typed into "Host to allowlist" → **Administrator action**
confirm dialog ("Allow egress to dummyjson.com … written to the audit log") —
screenshot `A2-admin-confirm-dialog.png` — confirmed as admin; host chip
appears (`A2b-host-allowlisted.png`). **PASS.**

### A3 — Proxy "gw dummyjson" create → Reconcile → APISIX admin proof → Test
Add Proxy dialog: target `dummyjson.com:443`, SNI `dummyjson.com`, path `/`,
methods GET. Created (status Pending) → **Reconcile** → toast
`"gw dummyjson" is live on the gateway`; card shows *Reconciled and
allowlisted* (`A3-proxy-reconciled-card.png`).

Live APISIX admin dump (`A3-apisix-objects.json`, all HTTP 200), proxy id
`gw-proxy-dw0e0l`:

- upstream `dmp_gw-proxy-dw0e0l`: roundrobin, nodes `{"dummyjson.com:443": 1}`,
  scheme https, pass_host node, timeouts connect 5s / send+read 30s
- route `dmp_gw-proxy-dw0e0l_root`: uri `/gw_dummyjson`, GET,
  proxy-rewrite → `/`
- route `dmp_gw-proxy-dw0e0l_wild`: uri `/gw_dummyjson/*`, GET,
  proxy-rewrite regex `^/gw_dummyjson/(.*)` → `/$1`

Card **Test** → probe through the RUNTIME gateway succeeded
(`Reached gw_dummyjson/ — HTTP 200`, `A3b-proxy-test-ok.png`). **PASS.**

### A4 — Service "gw proxied dummyjson" with API gateway egress
Add Application Service → HTTP service, baseUrl `https://dummyjson.com`, auth
None, **"API gateway egress" select set to `gw dummyjson`** — helper text
confirms "Every block using this service calls dummyjson.com through
'gw dummyjson'". Screenshots `A4-service-egress-select.png`,
`A4b-service-created.png`. **PASS.**

### A5 — Flow "gw via gateway" (cron `*/3 * * * *`)
Built in the flow-builder UI: http·read root bound to the proxied service,
path `/users`, recordPath `$.users[*]`, split ON → kafka·write child, entity
`gw_user` (derived topic `raw.gw_via_gateway.gw_user`). `A5-flow-built.png`.
Saved (flow id `flow-yxhj2r`). **PASS.**

### A6 — Deploy + THE gateway-routing proof
Preflight all green (`A6-preflight.png`) → Deployed (`A6b-deployed.png`,
NiFi PG `fae927c5-019f-1000-1bd8-83bec9bd682e`).

Routing evidence, three independent reads:
1. **Backend runtime API** (`A6-runtime-fetch-property.json`): the http block's
   `init` (UpdateAttribute) property
   `request.url = #{apisix_runtime_url}/gw_dummyjson/users`.
2. **Live NiFi REST** (`A6-nifi-params.json`): parameter context
   `gw_via_gateway__params` resolves `apisix_runtime_url =
   https://apisix.datapasc.com` — i.e. the fetch goes to
   `https://apisix.datapasc.com/gw_dummyjson/users`, never to dummyjson
   directly.
3. **UI Runtime tab** (`A6c-runtime-tab-gateway-url.png`): the same property,
   read-only, grouped under the http block.
**PASS.**

### A7 — Start, records, stop
Enabled + Started (`A7-started.png`). First cron firing landed **exactly 30
messages** on `raw.gw_via_gateway.gw_user` after 62s of polling
(`A7-topic-poll.txt`; the 30 = dummyjson `/users` default page — records that
by construction travelled dummyjson→APISIX→NiFi→Kafka). Messages tab shows the
topic with offsets (`A7b-messages-tab.png`). Flow **STOPPED** — "Stopped —
queues retained" (`A7c-stopped-still-deployed.png`); still deployed,
`deployedAt 2026-08-13T11:36:54Z`. **PASS.**

**JOURNEY A VERDICT: PASS — end-to-end gateway routing proven live** (admin
objects on APISIX, NiFi fetch URL through the runtime gateway, 30 real records
delivered through it).

---

## JOURNEY B — Iceberg sink ("making few sink connectors and sending data to iceberg and verifying if data landed there perfectly")

### B1 — Sink service "ice polaris" (documented API fallback) + UI Test
**FALLBACK (documented):** the Add Service UI form for kind *Iceberg catalog*
only exposes **Catalog URL + Warehouse** (`ServiceFormFields.tsx`); the OAuth
client and S3/FileIO fields the live connector needs (`oauthClientId/Secret`,
`s3Endpoint/AccessKey/SecretKey/Region/PathStyle`) are backend-accepted but
have no UI inputs. The service was therefore created via
`POST /api/v2/services/` with the full config (catalogUrl
`https://polaris.datapasc.com/api/catalog`, warehouse `bronze`, oauth
root/s3cr3t, s3 `https://ozones3g.datapasc.com` eltadmin, us-east-1,
path-style) — `B1-sink-service-created.json` — then **Tested through the UI**:
card Test → **Healthy** ("Iceberg catalog reachable", audit `Service tested /
ice polaris / Success`). Screenshot `B1-sink-service-healthy.png`. **PASS.**

### B2/B3 — Flow "ice users" (cron `*/3 * * * *`)
Plain http service `ice dummyjson` created via UI. Flow built in the UI:
http·read `/users` (recordPath `$.users[*]`) → **kafka+connect · governed
write** block, entity `ice_user` (governed sealed topic
`raw.ice_users.ice_user`), Destination service `ice polaris · rev 1` — the
sink section shows the service-derived locked rows `iceberg.catalog.uri =
https://polaris.datapasc.com/api/catalog`, `iceberg.catalog.warehouse =
bronze` (`B3-kafka-kc-configured.png`). Saved: flow `flow-5r8v22`, kafka_kc
block `b-wm9mil`. **PASS.**

### B4 — Schema ceremony (UI, uploaded sample file) — with one defect found
Sample: 3 real users saved from `https://dummyjson.com/users?limit=3` to
`ice-users-sample.json` (wrapper shape kept so the ceremony's record path is
the flow's own `$.users[*]`).

Ceremony (all in the browser): **Declare** (entity pre-filled `ice_user`,
evidence path "Uploaded sample files", `B4a-ceremony-declare.png`) →
**Orchestrate** (file uploaded, record path `$.users[*]`, "3 record(s)
matched", `B4b-ceremony-upload.png`, Infer schema → 28 top-level fields) →
**Review** (`B4c-ceremony-review.png`).

**DEFECT 1 (real product bug, found by attempt 1+2):** Approve & register
returned **422 — `Invalid Avro schema: redefined named type:
raw.ice_users.Address`** (captured in `B4-approve-response.json` history).
Root cause: the sample-inference engine emits a FULL record definition at
every nested-object site, named after the field — dummyjson users carry
`address` and `company.address` (and `coordinates` under both), so the
generated Avro defines `raw.ice_users.Address` / `raw.ice_users.Coordinates`
twice. Avro forbids redefining a named type; the backend's
fastavro validation correctly rejects it. Any uploaded/live sample whose
records contain two same-named nested objects will 422 the ceremony's
approve. Fix suggestion: the inference should either uniquify nested record
names (e.g. parent-qualified) or emit a by-name reference for repeats.

**WORKAROUND (in-UI, attempt 3 — the ceremony's own sanctioned surface):** on
the Review step's **Raw Avro JSON** tab the duplicate definitions were renamed
uniquely (`Address -> Address2`, `Coordinates -> Coordinates2`;
`B4-raw-avro-renames.json`, full schema `B4-fixed-avro.json`,
`B4c2-ceremony-raw-avro-fixed.png`) — the dialog re-validated "Edits still fit
the uploaded samples · All 3 sample record(s) still fit this schema" →
**Approve** summary (subject `raw.ice_users.ice_user-value`, evidence
"uploaded samples — 1 file(s), 3 record(s) at $.users[*]",
`B4d-ceremony-approve-summary.png`) → **Approve & register → 200**
(`B4-approve-response.json`): schema `schema-ufgi50`, **registry global id
27**; Apicurio ccompat subject `raw.ice_users.ice_user-value` versions `[1]`
confirmed live. Block form shows Approved (`B4e-schema-approved.png`).
**PASS (after documented workaround).**

Automation note: one Playwright-side selector ambiguity (label "Raw Avro JSON"
matches tabpanel + textarea) and two assertion-text misses ("Approved #N" is
the collapsed-section summary; the expanded section says "global id #N") were
test-code fixes, not product issues.

### B5 — Deploy + start
Preflight — every row green (`B5-preflight-connect-rows.png`): Configuration
valid, NiFi/Kafka/Schema registry/**Kafka Connect connections active**,
**"Schema approved — New kafka+connect · Approved and registered"**, bound
services reachable, no retired services. (Note: the *installed-plugin* check
is a server-side deploy gate, not a UI dialog row — it passed, as proven by
the connector instantiating.) Deployed (`B5b-deployed.png`,
`deployedAt 2026-08-13T12:00:36Z`) → Enabled → Started (`B5c-started.png`).
**PASS.**

### B6 — Connector live on Kafka Connect
`GET https://kafkaconnect.datapasc.com/connectors/ice_users.b-wm9mil.kafka_kc/status`
→ connector **RUNNING**, task 0 **RUNNING** (worker kafka-connect:8083, sink;
`B6-connector-status.json`). Committed consumer offsets later showed
partitions 5/2/4 at 30+56+4 = **90 records consumed**. **PASS.**

### B7 — DATA LANDED (Trino) — with a naming discovery
First firing: **30 Avro records** on `raw.ice_users.ice_user` after 97s
(`B7-topic-poll.txt`).

Trino discovery (mission's own instruction, `B7-trino-results.json`):
- `SHOW CATALOGS` → `bronze, gold, iceberg, silver, system`
- `SHOW TABLES FROM iceberg.bronze` → empty — the alpha-convention guess
  `iceberg.bronze.ice_user` is wrong in the **catalog** position on this
  cluster
- `SHOW TABLES FROM bronze.bronze` → **`ice_user`** (and journey A's
  `e2ea_user`) — Trino maps the Polaris warehouse `bronze` to the catalog
  named `bronze`
- Polaris REST cross-check: `GET /api/catalog/v1/bronze/namespaces/bronze/tables`
  → `bronze.ice_user` exists (auto-created by the connector,
  `iceberg.tables.auto-create-enabled=true`)

**Row count:** `SELECT count(*) FROM bronze.bronze.ice_user` → **94** —
exactly 3 complete firings × 30 + 4 records of the 4th commit cycle in flight
when verification closed (the connector's 60s commit interval; committed
offsets at the earlier read summed to 90). Sample rows verified as the real
upstream users (`SELECT id, firstName, … LIMIT 5`: Emily Johnson ×4 — one per
firing — then Michael Williams), i.e. genuine dummyjson records, one append
per firing, zero corruption. **PASS — data landed and is queryable.**

### B8 — Stop
Flow **STOPPED** via UI ("Stopped — queues retained",
`B7-stopped-still-deployed.png`); the platform also paused the managed
connector (Connect status now PAUSED/PAUSED — stop semantics). Everything
left deployed/in place. **PASS.**

**JOURNEY B VERDICT: PASS — governed Avro topic → managed Iceberg sink
connector → Polaris/Ozone table, verified queryable through Trino.**

---

## Defects & findings

1. **DEFECT (product, schema inference → ceremony approve):** sample
   inference emits duplicate Avro named types for repeated same-named nested
   objects (`Address`, `Coordinates` in dummyjson users) →
   `POST /api/v2/schemas/approve` 422 `redefined named type`. Worked around
   in-UI on the ceremony's Raw Avro JSON tab (renames; evidence above).
   Frontend `inferAvroFromRecords` should uniquify nested record names.
2. **FINDING (UI gap, documented fallback):** the sink-destination service
   form exposes only `catalogUrl`/`warehouse` for Iceberg — no OAuth/S3
   fields, though the backend accepts and the compiler requires them.
   Service creation had to go through the API.
3. **FINDING (docs/convention):** on this Trino, the landed table is
   `bronze.bronze.ice_user` (catalog = warehouse name), not
   `iceberg.bronze.ice_user`.
4. **NOTE (cosmetic, console):** React `validateDOMNesting` warning from
   `SampleInferencePanel` (Badge `<div>` inside `<p>`) — harmless, logged in
   `console-errors.txt`.
5. **CONFIRMED FIXED:** the earlier journeys' blocking Kafbat topic-creation
   defect (commit `f9fe0ac`) is live — both flows' topics were auto-created
   on first deploy.

## What remains live (deliberately — evidence)

| Object | Where | State |
|---|---|---|
| Allowlist host `dummyjson.com` | platform gateway resources | present |
| Proxy `gw dummyjson` (`gw-proxy-dw0e0l`) | platform + APISIX (`dmp_gw-proxy-dw0e0l{,_root,_wild}`) | Reconciled / live |
| Service `gw proxied dummyjson` (egress → proxy) | Application Services | active |
| Service `ice dummyjson`, `ice polaris` | Application Services | active / Healthy |
| Flow `gw via gateway` (`flow-yxhj2r`) | platform + NiFi PG `fae927c5…` | **Stopped**, deployed |
| Flow `ice users` (`flow-5r8v22`) | platform + NiFi | **Stopped**, deployed |
| Topics `raw.gw_via_gateway.gw_user` (30 msgs), `raw.ice_users.ice_user` (~120 msgs), DLQs | Kafka | retained |
| Schema `raw.ice_users.ice_user-value` (global id 27) | platform + Apicurio | Approved / registered v1 |
| Connector `ice_users.b-wm9mil.kafka_kc` | Kafka Connect | PAUSED (stopped with the flow) |
| Table `bronze.ice_user` (94 rows at verification) | Polaris/Ozone, queryable via Trino `bronze.bronze.ice_user` | present |

## Screenshot index (frontend/e2e/artifacts/gwice/)

A1(-b) sidebar/page rename · A2(-b) admin allowlist dialog/result ·
A3(-b) reconciled card + Test ok · A4(-b) egress select + service card ·
A5 flow built · A6/A6b preflight/deployed · A6c Runtime tab gateway URL ·
A7/A7b/A7c started/messages/stopped · B1 sink Healthy ·
B3 kafka_kc configured · B4a–B4e full ceremony incl. raw-Avro fix ·
B5* preflight/deployed/started · B7 stopped. JSON/txt: APISIX objects, NiFi
params, runtime property, poll logs, approve response, fixed Avro + renames,
connector status, Trino results.
