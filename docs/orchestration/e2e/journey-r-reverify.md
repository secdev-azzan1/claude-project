# Journey R — Final re-verification of the correction wave (live infra)

Agent: e2er · Backend: http://localhost:8010 (/api/v2) · Date: 2026-08-13 (all times UTC, from API responses / live polls)
Prefix: `e2er`. Pre-flight sweep at 06:17Z: zero e2er topics/subjects/connectors/NiFi PGs; gateway `{proxies:[],certProfiles:[],allowlist:[]}`; all 6 platform connections active + Healthy.

Verdict table up front:

| # | Fix under test | Result |
|---|---|---|
| R1 | E1 — Kafbat-mode topic auto-create at deploy | **PASS** |
| R2 | C4 — pagination advances the URL | **PASS after a sanctioned one-line fix** (new defect found in the shipped correction — see R2-D1) |
| R3 | E5 — session-token JSON-body login chain | **BLOCKED-BY-DESIGN-CONSTRAINT** (NiFi sensitive-parameter rule, exactly the flagged risk) |
| R4 | E2/E2b — Iceberg connector config + converter URL | **PASS** |
| R5 | E4 — proxy delete tears down live APISIX objects | **PASS** |
| R6 | E6 — registry version accounting + version delete | **PASS** |
| R7 | M10/M11 — dedup epoch on config change + undeploy | **PASS** |
| R8 | M12 — name freeze at deploy | **PASS** |

---

## R1 — Fresh-topic deploy WITHOUT pre-creating topics (fix E1)

- `POST /api/v2/services/` http "e2er dummyjson" (none auth) → `svc-wrd0dl`.
- Flow `flow-e2er-fresh` "e2er fresh": b1 http·read `/products` recordPath `$.products[*]` split, **dedup transform** (identity `[id]`, window 24h — doubles as the R7 vehicle), b2 kafka·write entity `e2er_product`, cron `*/3 * * * *`. Saved → 200 Draft.
- Pre-deploy Kafbat GETs: `raw.e2er_fresh.e2er_product` → **404**, `dlq.e2er_fresh` → **404**. Nothing pre-created.
- `POST /verbs/deploy` → **200**, state Stopped, deployedAt 06:20:00.921Z, PG `f9c6fa1f-…`. (Before the fix this exact scenario 502'd — Journey A/C DEFECT-1.)
- Post-deploy Kafbat GETs: both topics **EXIST** (partitions 6, replicationFactor **1** — single-broker cluster respected).
- **PASS** — `_kafbat_create_topic` (kafka_client.py) creates via `POST /api/clusters/{cluster}/topics` and treats "already exists" as success.

## R2 — Pagination live (fix C4) — the critical proof

Flow `flow-e2er-paged` "e2er paged": b1 http·read `/products`, recordPath `$.products[*]`, split, `pagination: {type:"offset", fields:{offsetParam:"skip", limitParam:"limit", limitValue:30}}` (exact keys per `blocks_http.py::_build_query`/`compile_read`; stop condition "empty page" is the built-in `${probe:isEmpty():not()}`), b2 kafka·write `e2er_paged`, cron `*/3`. Deploy → 200, PG `f9c7db5f-…`.

C4 shape verified in NiFi (processor property dump): `fetch.HTTP URL = #{svc_svc-wrd0dl_base_url}/products?skip=${offset}&limit=${limit}` (EL evaluated per FlowFile on fetch itself), `init` seeds `offset=0 / limit=30 / page_count=0` with **no** frozen `request.url`, `next.offset = ${offset:toNumber():plus(30)}`, `has_more.continue = ${probe:isEmpty():not()}`, `page_meta.probe = $.products[0]`. The frozen-URL defect is gone.

**Run 1 (shipped code): FAIL — frozen at page 1 for a NEW reason.** Started 06:21:34Z; cron fired 06:24:00Z; topic count reached **30 and froze** (polled 15s-granularity 06:24:09→06:26:11, never advanced). NiFi connection counters (PG status snapshot):
```
split → out_port b2          in=30 out=30      (page 1 delivered)
split(original) → page_meta  in=1  out=1
page_meta → has_more         in=0  out=0       ← continue check NEVER ran
page_meta → dlq__meta        in=1  out=1       ← original routed to FAILURE
has_more → next              in=0; next → fetch in=0
```
`dlq.e2er_paged` received the **raw page-1 response as a fabricated DLQ record** (payloadPreview captured via `GET /{id}/dlq`, errorClass `unspecified_failure`; topic offset sum showed 2 messages for the single traversal — possible duplicate publish, not investigated further since the path itself is defective). Flow stopped at 06:26:11Z before the next firing.

**NEW DEFECT R2-D1 (blocking offset/page pagination, in the shipped C4 correction):** `blocks_http.py::_build_pagination` (offset/page branch) emits `page_meta` = EvaluateJsonPath with `Return Type: scalar` and probe `_probe_path(recordPath)` = `$.products[0]` — which evaluates to a JSON **object**. NiFi's EvaluateJsonPath routes a non-scalar result to `failure` when Return Type is scalar (attribute destination). So the continuation check never executes: every offset/page-paginated read stops after page 1 **and** DLQs the raw page on every run. (The C4 URL mechanics themselves are correct — proven below.)

**Sanctioned one-line fix applied** (mission clause: re-verification otherwise impossible): `blocks_http.py:508` `"Return Type": "scalar"` → `"Return Type": "json"` (offset/page probe branch only; cursor branch untouched — `next_cursor` genuinely is a scalar). uvicorn restarted (`backend/.venv\Scripts\python.exe -m uvicorn server:app --port 8010`, detached, PID replaced 24720→25328). Both e2er_paged topics purged via Kafbat (`DELETE …/messages` → 200, count 0) for a clean count.

**Run 2 (patched): PASS.** Redeploy → new PG `f9d020c2-…`, `page_meta Return Type: json` confirmed VALID in NiFi. Started 06:30:15Z; cron fired 06:33:00Z; poll log: `06:32:45 count=0 → 06:33:01 count=194 → 06:33:14 count=194` (all 7 pages — 6×30+14 — fetched inside one second-long run). **Exactly 194**: not 30 (frozen), not >250 (no loop; the empty page at skip=210 stopped it). `dlq.e2er_paged` = **0**. Flow stopped by the poll guard immediately after stability.

## R3 — Session token live redux (fix E5)

- Service `svc-pw4309` http authMode `session_token` `{baseUrl:https://dummyjson.com, loginPath:/auth/login, tokenPath:$.accessToken, tokenHeader:Authorization, tokenTemplate:"Bearer ${token}", username:emilys, password:emilyspass}` (keys confirmed against `routers/v2/services.py` + `blocks_http.py::_session_header_value`). `POST /{id}/test` → **Healthy** (service-level login works).
- Flow `flow-e2er-authed` "e2er authed": b1 http·read `/auth/me` (split:false, no recordPath) → b2 kafka·write `e2er_me`, cron `*/3`. Deploy → **200**, PG `f9c95b8f-…`.
- NiFi processor dump of `read_me__http`: chain compiled exactly per the E5 rewrite — `login_body` (ReplaceText, Replacement Value `{"username":"#{svc_svc-pw4309_username}","password":"#{svc_svc-pw4309_password}"}`) → `login` (InvokeHTTP POST `#{…}/auth/login`, `Request Body Enabled: true`, Content-Type application/json, autoTerminate Original only — M7 fixed) → `extract_token` (`session.token = $.accessToken`) → `fetch` (`Authorization: Bearer ${session.token}` — tokenTemplate honoured).
- **BUT the flagged risk fired exactly as predicted.** `login_body` is **INVALID** in NiFi:
  > `'Replacement Value' is invalid because The property 'Replacement Value' cannot reference Parameter 'svc_svc-pw4309_password' because the Sensitivity of the parameter does not match the Sensitivity of the property.`
  `Replacement Value` is a static (non-dynamic) non-sensitive property, so NiFi's sensitive-parameter rule rejects the reference; `sensitiveDynamicPropertyNames` cannot help a static descriptor. The flow can never start.
- **Verdict: BLOCKED-BY-DESIGN-CONSTRAINT** (per mission instruction — orchestrator has a fallback design). Not started; skipped to R4.
- **FINDING R3-F1 (new, medium):** `deploy()` returned **200 / state Stopped** despite the INVALID processor — nothing in `nifi_apply`/lifecycle reads post-apply `validationStatus`, so a flow that can never start reports a successful deploy. The failure would only surface at `start` (NiFi refuses to start an invalid processor). Suggest a post-apply validation gate or a preflight row.

## R4 — Iceberg connector config (fix E2/E2b)

- Sink service `svc-il1wlv` `{type:sink_destination, kind:iceberg_catalog, catalogUrl:https://polaris.datapasc.com/api/catalog, warehouse:bronze, oauthClientId:root, oauthClientSecret:s3cr3t, s3Endpoint:https://ozones3g.datapasc.com, s3AccessKey:eltadmin, s3SecretKey:…, s3Region:us-east-1, s3PathStyle:true}` → test **Healthy** (06:23:32Z).
- Flow `flow-e2er-gov` "e2er gov": b1 http·read `/users` `$.users[*]` split → b2 kafka_kc entity `e2er_user` (serviceId + config.sinkServiceId both set). Schema ceremony: `/schemas/infer` (3-user sample, 28 fields) → `/schemas/approve` (topic `raw.e2er_gov.e2er_user`, subject `raw.e2er_gov.e2er_user-value`) → 200, ccompat shows `[1]` (single write — E6 side-benefit visible here already).
- Deploy → 200, PG `f9cb6334-…`, connector `e2er_gov.b2.kafka_kc` created.
- `GET https://kafkaconnect.datapasc.com/connectors/e2er_gov.b2.kafka_kc/config` — **every required key present, no sinkConfig workaround involved**:
  - `iceberg.catalog.type = rest` ✓ · `iceberg.catalog.credential = root:s3cr3t` ✓ · `iceberg.catalog.oauth2-server-uri = https://polaris.datapasc.com/api/catalog/v1/oauth/tokens` ✓ (+ `rest.auth.type=oauth2`, `scope=PRINCIPAL_ROLE:ALL`, `token-refresh-enabled=true`)
  - S3: `io-impl=…S3FileIO`, `s3.endpoint=https://ozones3g.datapasc.com`, `s3.access-key-id=eltadmin`, `s3.secret-access-key` set, `s3.path-style-access=true`, `s3.region=us-east-1`, `client.region` ✓
  - `value.converter.apicurio.registry.url = https://apicurio.datapasc.com/apis/registry/v3` ✓ (E2b — core API, not ccompat) with `as-confluent=true`, `use-id=contentId`
  - plus `iceberg.tables=bronze.e2er_user`, `topics=raw.e2er_gov.e2er_user`, `consumer.override.auto.offset.reset=earliest` (M14's initialPosition mapping).
- Start 06:25:21Z → connector **RUNNING**, task 0 **RUNNING** (stayed RUNNING across checks at 06:25/06:29/06:32 — no restart surgery needed this time). Cron firings 06:27/06:30 → topic 60.
- Destination proof (Polaris REST): table `bronze.e2er_user` **auto-created**; metadata shows **2 append snapshots — total-records 30 then 60** — matching the topic counts exactly. End-to-end http→NiFi→Kafka(Avro/registry)→Connect→Iceberg live.
- **PASS**

## R5 — Proxy delete teardown (fix E4)

- Allowlist add `dummyjson.com` (adminConfirmed) → 200. Proxy "e2er proxy" (`targetHost dummyjson.com:443, sni, path /, GET`) → 201 `gw-proxy-ikqhzj` Pending.
- Reconcile → Reconciled. Direct APISIX admin (X-API-KEY from backend/.env): `upstreams/dmp_gw-proxy-ikqhzj` 200 (`nodes {dummyjson.com:443}: 1`, scheme https), `routes/dmp_gw-proxy-ikqhzj_root` 200, `…_wild` 200.
- `DELETE /api/v2/gateway/proxies/gw-proxy-ikqhzj` → 200 **`{"ok":true,"id":"gw-proxy-ikqhzj","apisixCleaned":true}`**.
- Direct APISIX admin after delete: routes `_root`/`_wild` → **404**, upstream → **404**. No orphaned egress (Journey C DEFECT-4 gone).
- Allowlist host removed → `[]`.
- **PASS**

## R6 — Registry version-delete correctness (fix E6)

- `POST /schemas/approve` ×2 for fake flow `e2er-fake`/`bX`, topic `e2er.thing`, subject `e2er.thing-value` (v1: id,name · v2: +note) → approvals `[(1, gid 19), (2, gid 20)]`.
- ccompat `GET /subjects/e2er.thing-value/versions` → **`[1,2]` — EXACTLY 2** (was 4 pre-fix; `register_schema(ccompat_only=True)` writes once per approval, so app-version numbering aligns 1:1 with ccompat).
- `DELETE /schemas/{id}/versions/2` → 200 `registryDeleted:true`; app approvals now `[(1, current)]` with avro fields `[id,name]`; ccompat → **`[1]`**, and `versions/latest` content = fields `[id,name]` — **the v1 content, verified byte-level by field list** (pre-fix this deleted a duplicate of v1 and left the retired v2 content live).
- `DELETE /schemas/{id}` → 200; ccompat subject GET → **404**. Subject fully gone.
- **PASS**

## R7 — Dedup config-change + undeploy cache semantics (fixes M10/M11)

On the R1 flow (deployed with dedup, epoch 0):
- Baseline NiFi: `dedupe__hash` `SRC = e2er_fresh__b1` (no epoch suffix), `IDENTITY_FIELDS=id`, `EXCLUDES=ingest_id,ingest_ts,op`; `dedupe__detect` `Age Off Duration = 24 hours`.
- Changed `windowHours` 24→12, saved (200), `POST /verbs/redeploy` → 200 with **`preflightWarnings: [{label:"Dedup cache impact", ok:true, detail:"Dedup settings changed — the cache resets and previously suppressed records may reappear. (block(s): read products.)"}]`** — the M11 warning row.
- Flow doc: `dedupEpoch: 1`. NiFi after redeploy: **`SRC = e2er_fresh__b1__e1`** (epoch suffix live in the compiled processor), `Age Off Duration = 12 hours`.
- `POST /verbs/undeploy` → 200, state Draft, and the flow doc shows **`dedupEpoch: 2`** — M10's undeploy bump, verified via the persisted doc (the next deploy would compile `__e2`).
- **PASS**

## R8 — Name freeze (fix M12)

- `POST /api/v2/flows/` re-saving the deployed `flow-e2er-fresh` with name "e2er fresh RENAMED" → **409** `{"detail":"Names freeze at deploy — undeploy first to rename."}`; GET confirms name unchanged.
- **PASS**

## Cleanup

- Flows: all four `DELETE` → 200 `{ok:true, orphans:[]}`. Services `svc-wrd0dl`/`svc-pw4309`/`svc-il1wlv` retired:true. Gateway already empty.
- Residue found & handled:
  1. Approved-schema doc `schema-a7owr9` (+ subject `raw.e2er_gov.e2er_user-value`) survives flow delete **by design** (own lifecycle) — deleted via `DELETE /api/v2/schemas/{id}` → 200, subject 404.
  2. **Topic `raw.e2er_fresh.e2er_product` survived** the delete of `flow-e2er-fresh` — that flow was deleted from **Draft** (post-undeploy) state: undeploy nulls `runtimeScopeMap`, so `delete()`'s owned-topic capture finds no data topics (the DLQ, derived independently, WAS deleted). Same family as Journey A DEFECT-6 (connector residue after repair-to-Draft), still open for **topics after undeploy-to-Draft**. Cleaned via direct Kafbat `DELETE /topics/{name}` → 200.
  3. Iceberg table `bronze.e2er_user` dropped best-effort via Polaris (`DELETE …?purgeRequested=true` → 204).
- Final sweep: e2er topics `[]` · e2er subjects `[]` · e2er connectors `[]` · e2er NiFi PGs `[]` · e2er flows `[]` · gateway clean · bronze tables back to pre-run `['e2ea_user']`. **Nothing undeletable.**

## New defects & findings from this run

1. **R2-D1 (was blocking, now fixed in-tree + live-verified)** — offset/page pagination's `page_meta` used `Return Type: scalar` on an object-valued probe (`$.products[0]`) → EvaluateJsonPath routes to `failure`: loop dies after page 1 and the raw page is fabricated into the DLQ every run. One-line fix `blocks_http.py:508` (`scalar`→`json`) applied under the mission's re-verification clause and proven live (194/194, DLQ 0). **Needs adoption by the correction wave + a regression test** (none of the compiler tests execute the probe against an object).
2. **R3-F1 (medium)** — deploy() reports success (200/Stopped) while a compiled processor is INVALID in NiFi (no post-apply validationStatus gate); the failure only surfaces at start. Seen on the session-token flow.
3. **Teardown gap (medium, Journey-A DEFECT-6 family)** — deleting a flow that is in Draft because it was undeployed leaves its **data topics** on the cluster (scope map nulled at undeploy; DLQ still deleted). Evidence: `raw.e2er_fresh.e2er_product` 200 after flow delete.
4. **Minor anomaly (noted only)** — the defective pre-fix R2 run put **2** messages on `dlq.e2er_paged` for a single `page_meta` failure traversal (possible duplicate publish on the DLQ path); post-fix runs wrote 0. Not investigated further.
5. **R3 constraint (design input, not a code defect)** — NiFi refuses `#{sensitive param}` inside ReplaceText's non-sensitive static `Replacement Value`; the E5 login-body approach cannot work as compiled. Fallback design required (e.g. body via `login`'s own request-body with a sensitive-capable mechanism, or an ExecuteGroovyScript/param-provider approach).

Out-of-band interventions this run (all documented above): one-line compiler fix + uvicorn restart (sanctioned), Kafbat message purge + one orphan-topic delete, Polaris table drop. NiFi/APISIX/Apicurio/Connect were only read, never mutated directly.
