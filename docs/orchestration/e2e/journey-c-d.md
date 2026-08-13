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
(Cron fires on 5-minute wall-clock boundaries; message-arrival verification logged below after the next boundary.)

