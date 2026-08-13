# Diagnosis — InvokeHTTP / "no records reached Kafka" on `testflow`

Agent: e2ediag · Backend: http://localhost:8010 (/api/v2) · Live NiFi: https://nifi.datapasc.com
Date: 2026-08-13 (UTC timestamps from API/NiFi responses)

---

## Verdict (read this first)

**Root cause confirmed — H1 is correct, and it is a real backend defect, not user error the platform should have allowed.**

The user typed the *full URL* `https://dummyjson.com/users` into the HTTP-read block's **Path** field instead of just `/users`, while also having a service bound whose **Base URL** is `https://dummyjson.com`. The Path field's UI/API placed no restriction on this. The compiler then builds the request URL by **raw string concatenation of `{service.baseUrl}{block.path}` with no separator, no scheme check, and no URL parsing/validation anywhere in the stack** — not in the flow builder's save/validate, not in NiFi's own processor validation (the value arrives via an Expression-Language attribute, so NiFi can't statically check it), and not even in the "Test" block's live-probe path. The result is a garbled URL:

```
https://dummyjson.com  +  https://dummyjson.com/users
= https://dummyjson.comhttps://dummyjson.com/users
```

Live NiFi bulletin captured for the reproduction (`e2ediag fullurl`, InvokeHTTP `fetch` processor `fa58a210-019f-1000-e764-4adcae6247b1`):

```
InvokeHTTP[id=fa58a210-019f-1000-e764-4adcae6247b1] Request Processing failed:
FlowFile[filename=d141466d-f178-4689-ade7-97009a28a224]:
java.net.UnknownHostException: dummyjson.comhttps: Name or service not known
```

(URL parsing takes the host up to the first `:` after `//`, so the DNS lookup is literally for the mangled host `dummyjson.comhttps`.) This fired at *every* cron tick (captured twice, `09:00:00 UTC` and `09:03:00 UTC`), the flow file was routed to InvokeHTTP's `Failure` relationship → `dlq__meta` → `dlq__publish`, and **zero records ever reached the raw topic** — `raw.e2ediag_fullurl.diag_users` stayed at 0 messages while `dlq.e2ediag_fullurl` accumulated failure records. This is exactly the symptom the user reported: flow ran, InvokeHTTP errored, nothing landed in Kafka.

**Happy-path verdict: PASS.** The identical flow shape with a correct relative Path (`/users`) delivered all 30 records to Kafka with 0 errors and 0 DLQ entries (evidence below). The platform's core http-read → kafka-write path works correctly; the defect is specifically the base-URL/path concatenation with no user-input validation.

---

## Evidence

### 0. The user's own `testflow` (inspected first, left untouched)

`GET /api/v2/flows/` → flow `flow-1yswut`, name `TestFlow`, currently `state: "Draft"`, `deployedAt: null`, `nifiProcessGroupId: null`, `runtimeScopeMap: null` (`GET /api/v2/flows/flow-1yswut/runtime` → `{"detail":"No runtime record for this flow"}`).

**Block config — direct proof of H1**, block `b-418xbf` ("HTTP_Read"):
```json
{"method":"GET","path":"https://dummyjson.com/users","responseFormat":"json","split":true,"recordPath":"$.users"}
```
bound to `serviceId: svc-worhyc`, whose config is `{"baseUrl":"https://dummyjson.com","authMode":"none"}`. The user typed the **entire URL into Path** while a service with the same base URL was already selected — exactly hypothesis H1.

**Audit trail** (`GET /api/v2/audit/`) reconstructs exactly what the user did:

| time (UTC) | action | result |
|---|---|---|
| 08:28:10 | Flow created | Success |
| 08:29:21 | Flow deployed | Success — "2 block group(s), 0 connector(s)." |
| 08:29:35 | Flow enabled | Success |
| 08:30:18 | Flow started | Success |
| 08:30:47 | Flow stopped | Success (~29s after start) |
| 08:33:57 | Flow undeployed | Success |
| 08:42:38 / 08:42:45 | Flow deploy refused (preflight) | Failed — "1 failing check(s)" (generic; not re-diagnosable without redeploying the user's own flow, which was intentionally avoided — see "Left in place" below) |

**DLQ proof of the failure the user saw**: `GET /api/v2/flows/flow-1yswut/dlq` →
```json
{"records":[{"id":"dlq.testflow-3-0","flowId":"flow-1yswut","ts":"2026-08-13T08:32:24.233Z","blockName":"HTTP_Read","errorClass":"unspecified_failure","payloadPreview":"{}"}]}
```
Same shape (`blockName: HTTP_Read`, `errorClass: unspecified_failure`, empty payload) as the DLQ records produced by our controlled `e2ediag fullurl` reproduction below — the leftover state is consistent with the same root cause, deployed and started for real (NiFi PG existed 08:29–08:33), then torn down by the user before we could inspect it, hence no live NiFi bulletins remain for `testflow` itself. `GET /api/v2/flows/flow-1yswut/messages?topic=raw.testflow.users` → `{"messages":[]}` confirms zero records ever reached the raw topic. `POST /api/v2/flows/flow-1yswut/validate` → `[]` — confirms the platform's flow validation does **not** flag a full URL typed into Path (see Bug #2 below).

**`testflow` was left exactly as found** — not deployed, not started, not deleted, not edited.

### 1. Happy-path control — `e2ediag good` (PASS)

- Service `POST /api/v2/services/` `{type:"http", name:"e2ediag svc", config:{baseUrl:"https://dummyjson.com", authMode:"none"}}` → `svc-rsou8m`; `POST /svc-rsou8m/test` → **Healthy**.
- Flow `flow-e2ediag-good`, cron `*/3 * * * *`: `b1` http-read (`path:"/users"`, `responseFormat:"json"`, `recordPath:"$.users[*]"`, `split:true`) → `b2` kafka-write (`entity:"diag_users"`).
- `POST /validate` → `[]`. `POST /verbs/deploy` → **200 on first attempt** (`state:"Stopped"`, PG `fa56bafd-…`) — confirms the previously-documented Kafbat "topic never auto-created" defect (journey-a-e.md DEFECT 1) is now fixed; no manual Kafbat topic workaround was needed this time.
- `POST /verbs/start` → `state:"Running"`, immediate run fired (`lastRunAt` set at start, not waiting for the next cron tick).
- NiFi `fetch` (InvokeHTTP `fa56d5c8-…`): `validationStatus: VALID`, ran 1 task, `HTTP URL` property = `${request.url}`, and the upstream UpdateAttribute (`init`) set `request.url = "#{svc_svc-rsou8m_base_url}/users"` → resolves to `https://dummyjson.com/users`. All processors in the chain (`trigger`, `init`, `fetch`, `split`) show `tasks=1`, zero queued items anywhere, and the failure connections (`fetch→dlq__meta`, `split→dlq__meta`) stayed at 0.
- `GET /api/v2/flows/flow-e2ediag-good/messages?topic=raw.e2ediag_good.diag_users` → 30 messages (dummyjson's default `/users` page size), first record `id:1 firstName:"Emily" …`.
- `GET /api/v2/flows/flow-e2ediag-good/metrics` → `{"records24h":0,"errors24h":0,"perBlock":[{"blockId":"b1","recordsOut":30},{"blockId":"b2","recordsIn":30}],"topicCounts":[{"topic":"dlq.e2ediag_good","messages":0},{"topic":"raw.e2ediag_good.diag_users","messages":30}]}`.
- The bounded live-probe "Test" button (`POST /flows/{id}/blocks/b1/test`) also succeeded: `{"ok":true,"records":[…30 users…]}`.

**Happy path: 30/30 records delivered, 0 errors, 0 DLQ, both the deployed/NiFi path and the "Test" probe path agree.**

### 2. H1 repro — `e2ediag fullurl` (fails exactly as hypothesized)

Same flow shape, only `b1.config.path` changed to `"https://dummyjson.com/users"` (full URL, service base URL unchanged at `https://dummyjson.com`).

- `POST /validate` → `[]` — **no validation error at save/validate time**, exactly as with the user's `testflow`.
- `POST /verbs/deploy` → **200**, `state:"Stopped"` — NiFi component-level validation also passes (`validationStatus: VALID`, no `validationErrors`) because the URL only exists as an Expression-Language attribute value (`${request.url}`) at deploy time; NiFi cannot statically detect the malformed host until the expression is evaluated at runtime.
- Live NiFi processor property confirms the exact concatenation NiFi received — `init` (UpdateAttribute) property `request.url`:
  ```
  #{svc_svc-rsou8m_base_url}https://dummyjson.com/users
  ```
  i.e. the compiled parameter `#{svc_svc-rsou8m_base_url}` (= `https://dummyjson.com`) glued directly onto the user's full-URL Path value, with **zero separator and zero scheme sanity-check**.
- `POST /verbs/start` → `state:"Running"`.
- NiFi bulletin board (`GET /nifi-api/flow/bulletin-board?groupId=fa587c2a-…`), captured twice (once per cron tick, `09:00:00` and `09:03:00 UTC`):
  ```
  InvokeHTTP[id=fa58a210-019f-1000-e764-4adcae6247b1] Request Processing failed:
  FlowFile[filename=...]: java.net.UnknownHostException: dummyjson.comhttps: Name or service not known
  ```
  Full Java stack trace confirms this is thrown from `InvokeHTTP.onTrigger` via OkHttp's DNS resolver (`okhttp3.Dns$Companion$DnsSystem.lookup`) — a hard connection failure, not an HTTP-level 4xx/5xx.
- Process-group connection dump shows the flow file took the **Failure** path, not Response: `fetch --[Failure]--> dlq__meta --[success]--> dlq__publish` both ran (`tasks=1`); `split` never ran (`tasks=0`) because no successful response ever reached it.
- `GET /api/v2/flows/flow-e2ediag-fullurl/dlq` → DLQ record `blockName:"HTTP_Read"`, `errorClass:"unspecified_failure"`, `payloadPreview:"{}"` — **same signature as the user's leftover `testflow` DLQ record.**
- `GET /api/v2/flows/flow-e2ediag-fullurl/metrics` → `{"records24h":0,"errors24h":1,"topicCounts":[{"topic":"dlq.e2ediag_fullurl","messages":4},{"topic":"raw.e2ediag_fullurl.diag_users","messages":0}]}` — **0 records ever reached the raw topic**, exactly mirroring the user's report.
- The "Test" block probe (`POST /flows/{id}/blocks/b1/test`) hits the *same* bug in a different code path (Python/httpx, not NiFi) and surfaces it even more legibly:
  ```json
  {"ok":false,"reason":"Cannot connect to https://dummyjson.comhttps://dummyjson.com/users: [Errno 11001] getaddrinfo failed","testedAt":"2026-08-13T09:04:16.879Z"}
  ```
  This is what the user would have seen **if they had clicked "Test" on the block before deploying** — it literally shows the mangled URL in the error text, which is actually a better diagnostic than the NiFi bulletin (which never surfaces the URL, only the DNS host fragment).

### 3. Other observations on the happy path / platform behavior

- **No layer validates a full URL typed into Path.** Checked all three: (a) `POST /{id}/validate` (flow-level `validate_flow`) → `[]` in both the good and bad case; (b) NiFi's own processor validation → `VALID` because the value only exists behind an EL attribute; (c) the deploy preflight (`deploy_preflight` in `backend/services/adapter/validation.py`) only checks connection health / schema approval / retired services / gateway allowlisting — nothing about block-config sanity for `http` blocks. This is a **three-layer gap**, not a single missed check.
- **Redirects are intentionally not followed.** Live NiFi processor property on the InvokeHTTP `fetch` node: `"Response Redirects Enabled": "False"`. Traced to `backend/services/adapter/compiler/blocks_http.py:78`, which is a deliberate, commented MVP choice ("MVP explicitly pins this False (unlike the general reference baseline's True)") — not a bug, but worth flagging as a real limitation: any source API that 301/302s (common for `http://`→`https://` upgrades or trailing-slash normalization) will silently return the redirect response body to the parser instead of the intended payload, producing a confusing downstream "Response was not valid JSON" / split failure rather than a connection-level error. Not reproduced live (dummyjson's `/users` doesn't redirect); flagged from source + live config as a design gap worth a UI/docs callout, not a fresh live-tested defect.
- **Kafbat topic auto-create defect from a prior journey (journey-a-e.md DEFECT 1) is now fixed.** Both `e2ediag` flows deployed successfully on the *first* attempt with brand-new topics (`raw.e2ediag_good.diag_users`, `raw.e2ediag_fullurl.diag_users`, plus their `dlq.*` topics) — no 502 "Failed to create topic(s)" and no manual Kafbat REST workaround was needed, unlike the earlier journey. Consistent with the already-committed `_kafbat_create_topic` function found in `backend/services/kafka_client.py`.
- `testflow`'s two `Flow deploy refused (preflight)` audit entries (08:42:38/45, "1 failing check(s)") could not be further diagnosed without calling `deploy` again on the user's own live flow, which was deliberately avoided per the "leave it in place" instruction. `Configuration valid` would have passed (validate returns `[]`); the most likely candidates from `deploy_preflight`'s checklist are a transient platform-connection health blip or the bound service's `health` status, but this is not confirmed with live evidence and should not be treated as diagnosed.

---

## Root cause (code-level)

Both failure surfaces trace to the same pattern — literal string concatenation of the service base URL and the block's `path` field, with no separator, scheme check, or `urljoin`/parsing:

- **NiFi compiler** — `backend/services/adapter/compiler/blocks_http.py:344` (http-read fetch URL):
  ```python
  base_expr = _base_url_expr(block=block, service=service, ctx=ctx, add_param=add_param)
  initial_url = f"{base_expr}{path}" + (f"?{query}" if query else "")
  ```
  The same pattern recurs for http-write (`blocks_http.py:731`, `"HTTP URL": f"{base_expr}{path}"`) and for lookup-fetch (`blocks_http.py:797`, same expression) — i.e. **every** http adapter mode is exposed to this, not just read.
- **"Test" block bounded live-probe** — `backend/services/adapter/runtime.py:1251`:
  ```python
  url = f"{base_url}{path}"
  ```
  Independent implementation, same bug, same missing guard.
- **No validation catches it** — `validate_flow` (used by both `POST /validate` and as the first `deploy_preflight` check in `backend/services/adapter/validation.py`) has no rule inspecting `http` block `config.path` for an absolute URL / scheme prefix while a service is bound.

A one-line, low-risk mitigation (not applied — this is a read-only diagnosis) would be a validation rule flagging `path` values that start with `http://` or `https://` when `serviceId` is set, surfaced at both `POST /validate` (so it shows in the builder before deploy) and inside `deploy_preflight` (so a bad deploy is refused with an actionable message instead of silently DLQing every run).

---

## Cleanup performed

- Stopped, undeployed, and deleted flows `flow-e2ediag-good` and `flow-e2ediag-fullurl` (`DELETE /api/v2/flows/{id}` → 200 for both).
- Retired service `e2ediag svc` (`svc-rsou8m`) via `POST /api/v2/services/svc-rsou8m/retire` → 200.
- Final `GET /api/v2/flows/` shows only the user's original `TestFlow` (`flow-1yswut`, still `Draft`, untouched).
- No source files were modified; the backend was not restarted.
