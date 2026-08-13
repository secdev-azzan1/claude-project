# Journey C + D — End-to-End Verification Log

Run started: 2026-08-13T01:23Z
Backend: http://localhost:8010/api/v2
Prefix: `e2ec`

Real infra used directly for verification:
- APISIX admin: https://apisix-admin.datapasc.com (X-API-KEY from backend/.env)
- APISIX runtime: https://apisix.datapasc.com
- Apicurio (ccompat v7): https://apicurio.datapasc.com/apis/ccompat/v7
- NiFi: https://nifi.datapasc.com

Contract sources read before executing: backend/routers/v2/gateway.py, backend/routers/v2/schemas.py,
backend/routers/v2/services.py, backend/services/adapter/compiler/blocks_http.py,
backend/services/adapter/compiler/blocks_kafka.py, backend/services/adapter/naming.py,
backend/models/adapter/flow.py, docs/orchestration/compiler-spec.md §3.1, frontend/src/prototype/types.ts.

Pre-flight: GET /api/v2/gateway/ → `{"proxies":[],"certProfiles":[],"allowlist":[]}` (clean).
GET /api/v2/flows/ → only pre-existing `e2ea users` (Draft, another journey's artifact, untouched).
GET /api/v2/schemas/ → empty (clean).

---

## JOURNEY C — APISIX egress + session token

### Step 1 — Allowlist add
`POST /api/v2/gateway/allowlist {"host":"dummyjson.com","action":"add","adminConfirmed":true}`
→ 200 `{"allowlist":["dummyjson.com"]}`
`GET /api/v2/gateway/` → `allowlist` contains `"dummyjson.com"`.
**PASS**

### Step 2 — Create proxy
`POST /api/v2/gateway/proxies` body:
```json
{"name":"e2ec dummyjson","targetHost":"dummyjson.com","port":443,"path":"/","sni":"dummyjson.com","methods":["GET","POST"],"connectTimeoutMs":5000,"readTimeoutMs":15000}
```
→ 201, `id: "gw-proxy-0v4vpw"`, `status: "Pending"`, `statusDetail: "Created — not yet reconciled onto the gateway."`
**PASS** (matches expected Pending status)

### Step 3 — Reconcile + APISIX direct verify + test
`POST /api/v2/gateway/proxies/gw-proxy-0v4vpw/reconcile` → 200, `status: "Reconciled"`, `statusDetail: null`.

Direct APISIX admin verification (`curl -H "X-API-KEY: ..." https://apisix-admin.datapasc.com/apisix/admin/...`):
- `GET /apisix/upstreams/dmp_gw-proxy-0v4vpw` → `nodes: {"dummyjson.com:443":1}`, `scheme: "https"`, `pass_host: "node"`, `timeout: {connect:5, send:15, read:15}` — matches connectTimeoutMs/readTimeoutMs converted to seconds.
- `GET /apisix/routes/dmp_gw-proxy-0v4vpw_root` → `uri: "/e2ec_dummyjson"`, `methods: ["GET","POST"]`, `upstream_id: "dmp_gw-proxy-0v4vpw"`, `plugins.proxy-rewrite.uri: "/"`.
- `GET /apisix/routes/dmp_gw-proxy-0v4vpw_wild` → `uri: "/e2ec_dummyjson/*"`, `plugins.proxy-rewrite.regex_uri: ["^/e2ec_dummyjson/(.*)", "/$1"]`.

Proxy token confirmed as `tokenize(name)` = `e2ec_dummyjson` (matches `services/adapter/naming.tokenize`).

`POST /api/v2/gateway/proxies/gw-proxy-0v4vpw/test` → 200 `{"ok":true,"status":200,"ms":1480,"message":"Reached e2ec_dummyjson/ — HTTP 200."}`.
Direct sanity probe `curl https://apisix.datapasc.com/e2ec_dummyjson/` → HTTP 200.
**PASS**

### Step 4 — Egress flow (http read via proxy → kafka write)

`POST /api/v2/services/` `{"type":"http","name":"e2ec proxied","config":{"baseUrl":"https://dummyjson.com","authMode":"none","proxyId":"gw-proxy-0v4vpw"}}` → 200, `id: "svc-l4obpb"`.

Flow `flow-e2ec-gw` "e2ec via gateway", cron `*/5 * * * *`, blocks:
- `b1` http·read, serviceId `svc-l4obpb`, config `{path:"/products", recordPath:"$.products[*]", split:true, responseFormat:"json"}`
- `b2` kafka·write, parentId `b1`, entity `e2ec_product`

`POST /api/v2/flows/` → 200 created (Draft).
`POST /api/v2/flows/flow-e2ec-gw/verbs/deploy` → **first attempt 502** `"Failed to create topic(s): raw.e2ec_via_gateway.e2ec_product, dlq.e2ec_via_gateway"`.

**DEFECT-1 (blocking, all new-topic deploys)** — see Findings section below for full root-cause analysis. Workaround applied (direct Kafbat REST calls, not a source change): pre-created both topics via `POST https://kafbat.datapasc.com/api/clusters/local/topics` with `replicationFactor:1` (the cluster only has 1 broker registered). Re-ran deploy.

`POST /api/v2/flows/flow-e2ec-gw/verbs/deploy` (retry) → 200, `state: "Stopped"`, `deployedAt` set, `nifiProcessGroupId: "f8bc930d-019f-1000-1bac-614f38fcc1e8"`.

NiFi direct verification (bearer token from `POST /nifi-api/access/token`, admin/Nifiadmin@123):
- Processor `init` (UpdateAttribute, id `f8bce66a-...`) property `request.url` = `"#{apisix_runtime_url}/e2ec_dummyjson/products"`.
- Processor `fetch` (InvokeHTTP, id `f8bce829-...`) property `HTTP URL` = `"${request.url}"` (the EL template evaluates to the APISIX-routed URL above at runtime).
- Parameter context `e2ec_via_gateway__params` (id `f8bc8f39-...`): `apisix_runtime_url = "https://apisix.datapasc.com"`, `svc_svc-l4obpb_base_url = "https://dummyjson.com"` (present only as the informational service param — NOT what `fetch` actually calls).
- **Confirmed: the fetch request resolves to `https://apisix.datapasc.com/e2ec_dummyjson/products` — through the gateway, never directly to `dummyjson.com`.**

`POST /api/v2/flows/flow-e2ec-gw/verbs/start` → 200, `state: "Running"`.

After the next `*/5 * * * *` firing (01:35:00 UTC):
- `GET /api/v2/flows/flow-e2ec-gw/metrics` → `perBlock: [{blockId:"b1", recordsOut:30}, {blockId:"b2", recordsIn:30, recordsOut:0}]`, `topicCounts: [{topic:"dlq.e2ec_via_gateway", messages:0}]` (no DLQ traffic — clean run).
- Direct Kafbat verification: `GET https://kafbat.datapasc.com/api/clusters/local/topics/raw.e2ec_via_gateway.e2ec_product` → summed partition offsets (`offsetMax - offsetMin`) = **30 messages**, matching dummyjson's `/products` default page size exactly.
- `POST /api/v2/flows/flow-e2ec-gw/verbs/stop` → run at cleanup (see Step 6).

**PASS** — records fetched through the APISIX gateway (never touching `dummyjson.com` directly per the NiFi param evidence above) landed in `raw.e2ec_via_gateway.e2ec_product` end-to-end.

### Step 5 — Session-token auth

`POST /api/v2/services/` `{"type":"http","name":"e2ec session","config":{"baseUrl":"https://dummyjson.com","authMode":"session_token","loginPath":"/auth/login","tokenPath":"$.accessToken","tokenHeader":"Authorization","username":"emilys","password":"emilyspass"}}` → 200, `id: "svc-jkbzyv"`, `hasPassword:true` (password redacted in response, per `_secrets.py`).

`POST /api/v2/services/svc-jkbzyv/test` → 200, `health: "Healthy"`. This exercises `routers/v2/services.py::_test_http_session_token()`, which correctly POSTs a JSON body `{"username":..., "password":...}` to `/auth/login`, extracts `$.accessToken`, then GETs the base URL with the raw token in the `Authorization` header (no `Bearer` prefix — see Finding F1 below).

Direct dummyjson probe to characterize the target API (for the Finding below):
- `POST /auth/login {"username":"emilys","password":"emilyspass"}` → 200, `accessToken` present.
- `GET /auth/me` with `Authorization: Bearer <token>` → 200 (full profile).
- `GET /auth/me` with `Authorization: <token>` (no `Bearer` prefix) → **also 200** — dummyjson tolerates both forms.

Flow `flow-e2ec-authed` "e2ec authed", cron `*/5 * * * *`, blocks:
- `b1` http·read, serviceId `svc-jkbzyv`, config `{path:"/auth/me", split:false, responseFormat:"json"}`
- `b2` kafka·write, parentId `b1`, entity `e2ec_me`

Pre-created topics `raw.e2ec_authed.e2ec_me` / `dlq.e2ec_authed` directly via Kafbat (same DEFECT-1 workaround as Step 4).

`POST /api/v2/flows/flow-e2ec-authed/verbs/deploy` → 200, `nifiProcessGroupId: "f8bf1ed1-019f-1000-85cd-d6309ab40157"`.

NiFi direct verification confirms the STRUCTURE is exactly per compiler-spec §3.1 item 3:
- `login` (InvokeHTTP, id `f8bf74f6-...`): `HTTP Method: POST`, `HTTP URL: #{svc_svc-jkbzyv_base_url}/auth/login`, `Request Username: #{svc_svc-jkbzyv_username}`, `Request Password: ********`. **`Request Body Enabled` is absent from the dumped properties → false (the `_INVOKE_HTTP_BASELINE` default, never overridden by `_build_session_login`).**
- `extract_token` (EvaluateJsonPath, id `f8bf76d1-...`): `session.token = $.accessToken`, wired off `login`'s `Response` relationship.
- `fetch` (InvokeHTTP, id `f8bf7a82-...`): dynamic property `Authorization: ${session.token}` — confirms the fetch step DOES carry the session header, ahead of which `login`+`extract_token` are wired, exactly as required.
- All three processors sit ahead of `fetch` in `runtimeScopeMap`, matching the "login InvokeHTTP + EvaluateJsonPath token steps exist AHEAD of the fetch" requirement structurally.

`POST /api/v2/flows/flow-e2ec-authed/verbs/start` → 200, `state: "Running"`.

After the next `*/5 * * * *` firing (01:35:00 UTC): **records did NOT arrive.**
- `GET /api/v2/flows/flow-e2ec-authed/metrics` → `perBlock: [{blockId:"b1", recordsIn:0, recordsOut:0}, {blockId:"b2", recordsIn:0, recordsOut:0}]`, DLQ topic messages: 0.
- Direct Kafbat verification: `raw.e2ec_authed.e2ec_me` partition offsets sum to **0 messages**.
- NiFi process-group status (`GET /nifi-api/flow/process-groups/{fetch_me__http PG}/status`): connection `login → run_failure__log` shows `flowFilesIn:1, flowFilesOut:1` (i.e. the login call took the **Failure** path, not `Response`) — `extract_token` and `fetch` both show 0 in/0 out (never executed).
- NiFi bulletin board (`GET /nifi-api/flow/bulletin-board?groupId=...`) captured the actual failed request:
  ```
  sourceName: run_failure__log, level: ERROR
  invokehttp.request.url: https://dummyjson.com/auth/login
  invokehttp.status.code: 400
  invokehttp.status.message: Bad Request
  invokehttp.response.body: {"message":"Username and password required"}
  request.url: https://dummyjson.com/auth/me   (the attribute the *next* fetch would have used, proving this is the same flowfile lineage)
  ```
- Confirmed independently against the real API: `curl -X POST https://dummyjson.com/auth/login -u "emilys:emilyspass"` (HTTP Basic auth, empty body — the exact shape NiFi's `login` processor sends) → 400 `{"message":"Username and password required"}`, byte-for-byte matching the bulletin.

**Root cause (DEFECT-3, see Findings):** the compiled `login` InvokeHTTP sends the service's username/password as NiFi's `Request Username`/`Request Password` properties (HTTP **Basic Authentication**), never as a JSON request body — but dummyjson's `/auth/login` (like the mission spec itself states) requires a JSON body `{"username":..., "password":...}` and ignores Basic-Auth headers entirely. The login call fails every time, so `extract_token`/`fetch` never run and the topic never receives a record.

**Step 5 result: PARTIAL.** Service creation/test (backend-level session_token support) — PASS. NiFi structural wiring (login+extract ahead of fetch, fetch carries session header) — PASS. Live end-to-end record delivery through a real JSON-body login API — **FAIL**, blocked by DEFECT-3. Not worked around (would require a source change to `_build_session_login`, out of scope per this task's read-only mandate) — reported as a defect instead of forced to a false pass.

`POST /api/v2/flows/flow-e2ec-authed/verbs/stop` → run at cleanup (see Step 6).

### Step 6 — Cleanup

`POST .../verbs/stop` on both flows → 200, `state: "Stopped"`.
`DELETE /api/v2/flows/flow-e2ec-gw` → 200 `{"ok":true}`.
`DELETE /api/v2/flows/flow-e2ec-authed` → 200 `{"ok":true}`.

Proxy-delete 409 guard: both flows were already deleted by the time I got here, which would have made the dependents check trivially pass (it scans live `flows`, not services) — so I recreated a minimal Draft flow (`flow-e2ec-guard-probe`, http·read on `svc-l4obpb` → kafka·write) purely to re-arm the guard for a clean test:
- `DELETE /api/v2/gateway/proxies/gw-proxy-0v4vpw` (while `flow-e2ec-guard-probe` exists) → **409** `"Cannot delete: 1 flow(s) route through \"e2ec dummyjson\" (e2ec guard probe). Repoint them first."` — **PASS**, matches the required refusal.
- `DELETE /api/v2/flows/flow-e2ec-guard-probe` → 200 (removed the reference).
- `DELETE /api/v2/gateway/proxies/gw-proxy-0v4vpw` (retry) → 200 `{"ok":true,...}`.

**DEFECT-4 found here** (see Findings) — the proxy delete only touched Mongo; the live APISIX `dmp_gw-proxy-0v4vpw*` upstream/routes were still present after a 200 delete response. Confirmed via direct APISIX admin GET (still 200 with full objects), then manually deleted them directly via the APISIX admin API (routes first, then upstream) as cleanup — re-verified 404 on all three afterward.

`POST /api/v2/services/svc-l4obpb/retire` ("e2ec proxied") → 200, `retired:true`.
`POST /api/v2/services/svc-jkbzyv/retire` ("e2ec session") → 200, `retired:true`.
`POST /api/v2/gateway/allowlist {"host":"dummyjson.com","action":"remove","adminConfirmed":true}` → 200, `allowlist: []`.
`GET /api/v2/gateway/` → `{"proxies":[],"certProfiles":[],"allowlist":[]}` — clean.

**Step 6: PASS**, modulo DEFECT-4 (orphaned APISIX objects after proxy delete — worked around manually, not a source fix).

Post-cleanup NiFi verification: `GET /nifi-api/process-groups/{pg}` for both flows' root PGs → **404** (confirms `DELETE /api/v2/flows/{id}` really did undeploy — tear down the NiFi PG — before removing the flow doc, per `lifecycle.delete()`'s documented behavior; audit log shows `Flow undeployed` immediately before each `Flow deleted`).

---

## JOURNEY D — Schema lifecycle (Apicurio)

### Step 7 — Infer + template
Two sample JSON files written (`e2ec_sample1.json`, `e2ec_sample2.json`), each `{"things":[{id,name,price,inStock,tags[]}, ...]}`, 2 records each.

`POST /api/v2/schemas/infer` (multipart, `files=` x2, `recordPath=$.things[*]`, `name=E2ecThing`, `namespace=com.e2ec`) → 200:
```json
{"avro":{"type":"record","name":"E2ecThing","fields":[
  {"name":"id","type":["null","long"],"default":null},
  {"name":"name","type":["null","string"],"default":null},
  {"name":"price","type":["null","double"],"default":null},
  {"name":"inStock","type":["null","boolean"],"default":null},
  {"name":"tags","type":["null",{"type":"array","items":["null","string"]}],"default":null}
],"namespace":"com.e2ec"},
 "report":{"recordCount":4,"fieldCount":5,"notes":["e2ec_sample1.json: 2 record(s) parsed as json.","e2ec_sample2.json: 2 record(s) parsed as json."]},
 "suggestedPaths":["$.things[*]"]}
```
Correctly merged both files (4 total records), inferred nullable-union types per field. **PASS**

`POST /api/v2/schemas/templates` `{"name":"e2ec inferred", "avro": <above>}` → 201, `id: "tpl-yhs0vy"`. **PASS**

### Step 8 — Verify (valid / invalid)
`POST /api/v2/schemas/verify {"avro": <inferred avro>}` → `{"ok":true,"issues":[]}`. **PASS**
`POST /api/v2/schemas/verify {"avro": {"type":"string"}}` → `{"ok":false,"issues":["Root Avro schema must be a record (type: \"record\")."]}`. **PASS**

### Step 9 — Register + evolve + verify-with-subject
`POST /api/v2/schemas/register {"subject":"e2ec-topic-value","templateId":"tpl-yhs0vy","avro":<inferred avro>}` → 200 `{"globalId":49,"subject":"e2ec-topic-value","version":"2"}`.

Direct Apicurio ccompat verification: `GET /apis/ccompat/v7/subjects/e2ec-topic-value/versions` → `[1,2]` (both content id 16 in Apicurio's own numbering — see Finding F2 on why one `/register` call produces two ccompat versions).

Evolved avro (added nullable `description` field) registered under the same subject: `POST /api/v2/schemas/register {"subject":"e2ec-topic-value","avro":<evolved>}` → 200 `{"globalId":51,"subject":"e2ec-topic-value","version":"4"}`. `GET .../versions` → `[1,2,3,4]` (v3/v4 both content id 17 = the evolved schema, `description` field present). **New version created — PASS.**

`POST /api/v2/schemas/verify {"subject":"e2ec-topic-value","avro":<a further-evolved, additionally-nullable-field avro>}` → `{"ok":true,"issues":[],"compatibility":{"checked":true,"compatible":true,"message":"Compatible with the latest registered version."}}` — confirms `/verify` runs a real compat check against Apicurio (`services/apicurio_client`'s ccompat `/compatibility/subjects/.../versions/latest`) when a subject is given, not just structural validation. **PASS**

### Step 10 — Approved-schema surrogate (ceremony lifecycle)
`POST /api/v2/schemas/approve` `{"flowId":"e2ec-fake-flow","blockId":"bX","entity":"e2ec_thing","topic":"e2ec.thing","subject":"e2ec.thing-value","provenance":"manual","avro":<E2ecThingV1: id,name>}` → 200, `id:"schema-hoebly"`, `approvals:[{version:1, registryGlobalId:53}]`. **PASS**

Re-approve with a changed avro (added `note` field): same endpoint → 200, `approvals:[{version:1,...,supersededAt:<ts>}, {version:2, registryGlobalId:55, supersededAt:null}]` — **version 2 created in history. PASS**

`DELETE /api/v2/schemas/schema-hoebly/versions/2` → 200 `{"registryDeleted":true, "approvals":[{version:1, supersededAt:null, ...}], "avro": <back to id,name only>}` — app-side history correctly reverted to v1 as latest. **PASS (app-side)**

**DEFECT-2 found here** (see Findings) — cross-checked directly against Apicurio: `GET /apis/ccompat/v7/subjects/e2ec.thing-value/versions` before delete = `[1,2,3,4]` (v1/v2 = approval-1 content id 16, v3/v4 = approval-2 content-with-`note` id 17); after the delete-version-2 call, `GET .../versions` = `[1,3,4]` — ccompat version "2" (a duplicate of approval-1's content) was removed, but v3/v4 (the actual approval-2 `note`-field content that the app claims is now retired) are **still live in the registry**. `registryDeleted:true` is therefore misleading — the wrong registry version was deleted; the superseded schema content remains registered indefinitely.

`DELETE /api/v2/schemas/schema-hoebly` → 200 `{"ok":true,"registryDeleted":true}`. Verified: `GET /api/v2/schemas/` no longer lists `schema-hoebly`; direct Apicurio `GET /apis/ccompat/v7/subjects/e2ec.thing-value/versions` → **404** `"No artifact with ID 'e2ec.thing-value' ... was found"` — the whole-schema delete DOES correctly wipe the entire subject (unlike the single-version delete). **PASS**

Template cleanup: `DELETE /api/v2/schemas/templates/tpl-yhs0vy` → 200 `{"ok":true}`. **PASS**

`e2ec-topic-value` subject cleanup: **no API endpoint exposes deleting a bare registered subject that isn't backed by an approved-schema record** (`DELETE /api/v2/schemas/{id}` operates on the `schemas` collection keyed by approval id; a template-linked `/register` call never creates such a record). This is an acceptable, expected gap per the mission brief — noted, not treated as a defect. For registry hygiene (this Apicurio instance is shared with other agents) I deleted it directly via `DELETE https://apicurio.datapasc.com/apis/ccompat/v7/subjects/e2ec-topic-value` (200, then confirmed 404 on re-GET) — a direct-infra cleanup action, not a source change.

### Step 11 — Audit
`GET /api/v2/audit/?search=e2ec` → 33 matching entries, one per action taken across both journeys end-to-end: allowlist/proxy create+reconcile+test, both service creates+test, both flow create+deploy(+first failed attempt)+start+stop+undeploy+delete, schema template create+delete, schema register x2, schema verify, approve x2, schema version delete, schema delete, gateway proxy delete, both service retires. Every action in this log has a corresponding audit row. **PASS**

---

## Findings

**DEFECT-1 (blocking — Journey C, generalizes to ANY new-topic flow deploy) — Kafbat-mode topic creation is a no-op.**
`backend/services/kafka_client.py::ensure_topic_exists()` (lines 1047-1134): when the active Kafka connection's `kafka_connection_mode` is `"kafbat"` (config `{"mode":"kafbat", ...}` — the ONLY mode viable in this environment, per the module's own docstring: "the broker is not reachable by TCP from this host"), the function takes the branch at lines 1067-1086, which calls `_kafbat_topic_message_count()` (a GET on `/api/clusters/{cluster}/topics/{topic}`) purely to check existence — there is **no POST/create call anywhere on this path**. If the topic doesn't already exist, it 404s and `ensure_topic_exists` returns `ok:false`. `services/adapter/deployer/lifecycle.py::deploy()` (line 315) calls this for every topic in the plan (data + DLQ) and aborts the whole deploy with a 502 if any fail (line 320) — silently dropping the underlying error message, too (only topic *names* are surfaced, not `error`/`error_code`).
- Live evidence: `POST /api/v2/flows/flow-e2ec-gw/verbs/deploy` (first attempt) → 502 `"Failed to create topic(s): raw.e2ec_via_gateway.e2ec_product, dlq.e2ec_via_gateway"`. Direct `GET https://kafbat.datapasc.com/api/clusters/local/topics/raw.e2ec_via_gateway.e2ec_product` → 404 `TopicNotFoundException` before creation.
- Compounding issue: even the native (non-Kafbat) creation path in the same function defaults to `replication_factor=3`, but the live cluster has only 1 broker registered — confirmed by hitting Kafbat's own topic-create endpoint directly with `replicationFactor:3` → 400 `InvalidReplicationFactorException: ... only 1 broker(s) are registered`. So even if the Kafbat short-circuit were removed, the default RF would still need to drop to 1 for this environment.
- **Impact**: every brand-new flow deploy in this environment fails at the topic-creation step. This is not specific to Journey C — it would block any flow (Journey C or otherwise) whose data/DLQ topics don't already exist.
- **Workaround used for this verification** (test methodology only, not a source change): pre-created the required topics directly via `POST https://kafbat.datapasc.com/api/clusters/local/topics` with `replicationFactor:1`, before each deploy.

**DEFECT-2 (data integrity — Journey D) — `DELETE /schemas/{id}/versions/{version}` deletes the wrong Apicurio registry version.**
`services/apicurio_client.py::register_schema()` performs a dual write per logical registration: a Confluent-compatible POST to `/subjects/{subject}/versions` AND a native POST to `/apis/registry/v3/groups/{group}/artifacts/{artifact_id}/versions` (lines 157-243) — both land on the same underlying Apicurio subject, and BOTH increment the subject's ccompat version counter even when content is byte-identical, so **one `approve()`/`register()` call always consumes two ccompat version numbers**, not one. The app's own `ApprovedSchema.approvals[].version` field, however, increments once per approval. `routers/v2/schemas.py::delete_approved_schema_version()` (line 559) naively forwards the app's own version integer straight through as the literal ccompat version to delete (`.../subjects/{subject}/versions/{version}`) — from the second approval onward these two numbering schemes are no longer aligned, so the delete removes the wrong registry version.
- Live evidence: approval v1 (content id 16, no `note` field) and v2 (content id 17, `note` field) were created. Ccompat's actual versions before the delete: `[1,2,3,4]` (v1/v2 → content 16; v3/v4 → content 17). `DELETE /api/v2/schemas/schema-hoebly/versions/2` returned `"registryDeleted":true`, and ccompat afterward showed `[1,3,4]` — version "2" (a duplicate of the v1 content) was removed, while v3 AND v4 (the actual v2-approval `note`-field content) are both still live and registered.
- **Impact**: "deleting" a superseded schema approval leaves its content permanently orphaned in the registry (a stale, retired schema version stays resolvable/fetchable indefinitely), while the app-side UI/API reports success and a clean rollback. A full `DELETE /api/v2/schemas/{id}` (whole-record delete) does NOT have this problem — it deletes the entire subject in one ccompat call and was verified clean (404 after).

**DEFECT-3 (blocking, live-confirmed — Journey C step 5) — compiled `session_token` login sends HTTP Basic Auth, not the JSON body the target API requires.**
`backend/services/adapter/compiler/blocks_http.py::_build_session_login()` (lines 387-401) sets the service's username/password as InvokeHTTP's `Request Username`/`Request Password` properties — NiFi's HTTP **Basic Authentication** mechanism — and never enables `Request Body Enabled` or wires a body-rendering step (unlike `_compile_write`'s `render_body`/`ReplaceText` step, which exists for exactly this purpose on the write path). dummyjson.com's `/auth/login` (and the mission spec itself: `POST /auth/login {"username":"emilys","password":"emilyspass"}`) requires a JSON body and ignores Basic-Auth headers entirely.
- Live evidence (from the actually-deployed-and-fired `e2ec authed` flow): NiFi's bulletin board captured the real login attempt: `invokehttp.request.url: https://dummyjson.com/auth/login`, `invokehttp.status.code: 400`, `invokehttp.response.body: {"message":"Username and password required"}`. Process-group connection status confirmed `login → run_failure__log` fired (the Failure/Retry/No-Retry path), while `login → extract_token` (the `Response` path) never did — `extract_token` and `fetch` both show 0 in/0 out. Reproduced independently: `curl -X POST https://dummyjson.com/auth/login -u "emilys:emilyspass"` (Basic auth, empty body — the exact shape NiFi sent) → 400, byte-identical response body.
- Contrast: `routers/v2/services.py::_test_http_session_token()` (backing `POST /services/{id}/test`) does this CORRECTLY — it POSTs a real JSON body (`json={"username":..., "password":...}`) — so the service-level "Test" button is not representative of what the compiled NiFi flow will actually do at runtime. This is a genuine inconsistency between the two code paths, and the compiler path is the broken one.
- **Impact**: every `session_token`-authed http-read root block targeting a JSON-body login API (the standard shape for this auth pattern) silently produces zero records forever — no DLQ record either, since login failures are explicitly treated as "run failures, not record failures" (by design) and only surface via a NiFi bulletin, never in flow metrics (`errors24h` stayed 0).
- Not worked around (would require a source change to the compiler, out of scope for this read-only verification task) — Step 5's live end-to-end delivery is reported FAIL, not forced to a false PASS.

**DEFECT-4 (security-relevant — Journey C step 6) — proxy delete never tears down the live APISIX objects.**
`backend/routers/v2/gateway.py::delete_proxy()` (lines 341-363) only removes the proxy from the platform's own Mongo `gateway_v2` document; it never calls `services/apisix_client.py` to delete the `dmp_{proxy_id}` upstream or the `dmp_{proxy_id}_root`/`_wild` routes that `reconcile_proxy()` pushed to the live APISIX Admin API.
- Live evidence: `DELETE /api/v2/gateway/proxies/gw-proxy-0v4vpw` → 200 `{"ok":true,...}`. Immediately after, direct `GET https://apisix-admin.datapasc.com/apisix/admin/{upstreams,routes}/dmp_gw-proxy-0v4vpw*` still returned 200 with the full live objects — the real gateway kept routing `/e2ec_dummyjson/*` → `dummyjson.com:443` even though the platform believed the proxy was gone and its allowlist host had been revoked.
- **Impact**: every proxy delete leaks a live, unmanaged upstream+route pair on the shared APISIX instance — a real orphaned-egress/security concern, not just cosmetic drift, since the route keeps functioning indefinitely.
- Cleaned up manually as part of this run's own hygiene (`DELETE` calls directly against the APISIX admin API for the two routes then the upstream; verified 404 afterward) — not a source fix.

**Finding F1 (non-blocking, code-level observation) — session_token header injection has no `Bearer` prefix.**
Both `blocks_http.py::_apply_auth()` (line 153) and the session-token branches of `compile_read`/`_compile_write`/`_compile_lookup` set `props[header] = "${session.token}"` verbatim — the raw extracted token, never `f"Bearer {token}"`. This matches what compiler-spec.md §3.1 item 4 itself documents (`session_token → Authorization(or configured header) = ${session.token}`), so it's spec-conformant, not a deviation — but it is a departure from the OAuth2/Bearer-scheme convention most JSON APIs (including dummyjson) expect. Verified directly against dummyjson.com that **both** `Authorization: Bearer <token>` and `Authorization: <token>` return 200 on `/auth/me` — so this specific target API tolerates the missing prefix and it caused no failure here. Flagged for the record since a stricter API (one that requires the literal `Bearer ` scheme prefix and rejects a bare token) would fail against this code path with no compiler support for adding it.

**Finding F2 (non-blocking, explains DEFECT-2's root cause) — one logical schema registration always creates two Apicurio ccompat versions.**
`register_schema()`'s dual-write design (ccompat POST + native v3 POST, both against the same subject) is intentional and documented in the code ("NiFi's ApicurioSchemaRegistry controller service reads native Apicurio artifacts by group/artifact id, not ccompat subjects") — but as a side effect, every single `/schemas/register` or `/schemas/approve` call consumes 2 ccompat version slots instead of 1, even when the same content is written both times. This inflates the registry's version history and is the direct root cause of DEFECT-2's version-number misalignment.

---

## Summary

| Step | Result |
|---|---|
| C.1 Allowlist add | PASS |
| C.2 Proxy create | PASS |
| C.3 Reconcile + APISIX verify + test | PASS |
| C.4 Egress flow deploy/run/verify | PASS (blocked initially by DEFECT-1, worked around) |
| C.5 Session-token service+structure | PASS; live record delivery FAIL (DEFECT-3) |
| C.6 Cleanup | PASS (409 guard verified; DEFECT-4 found + manually remediated) |
| D.7 Infer + template | PASS |
| D.8 Verify valid/invalid | PASS |
| D.9 Register + evolve + verify w/ subject | PASS (dual-version side effect noted, F2) |
| D.10 Approve/version-delete/schema-delete/template-delete | PASS overall; DEFECT-2 found in version-delete's registry cleanup |
| D.11 Audit | PASS |

4 defects (1 blocking a step outright, 3 with live-verified incorrect/incomplete behavior against real infra) and 2 non-blocking findings. Full request/response and direct-infra evidence inline above. All e2ec-prefixed platform state (flows, services, gateway proxy/allowlist, schemas, templates) is cleaned up; all e2ec-prefixed live infra objects (APISIX routes/upstream, Kafka topics, Apicurio subjects) are cleaned up or accounted for.

Final infra sweep: all 4 e2ec Kafka topics (`raw.e2ec_via_gateway.e2ec_product`, `dlq.e2ec_via_gateway`, `raw.e2ec_authed.e2ec_me`, `dlq.e2ec_authed`) confirmed gone (404 via direct Kafbat GET) — `DELETE /api/v2/flows/{id}` did correctly delete (not just empty) owned topics via `lifecycle.delete()`, unlike the proxy-delete path (DEFECT-4). Both flows' NiFi process groups confirmed gone (404). No Kafka Connect connectors were ever created (`0 connector(s)` in both deploy audit entries), so no KC cleanup was needed.

