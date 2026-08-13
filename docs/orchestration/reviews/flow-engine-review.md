# Flow-engine senior review — compiler / deployer / lifecycle

Scope: `backend/services/adapter/compiler/**`, `backend/services/adapter/deployer/**`,
`runtime.py`, `legality.py`, `validation.py`, `naming.py`, `routers/v2/flows.py`, and the four
test modules. Requirements read in the mandated order: `decisions.md` (D5–D9, D16, D17),
`compiler-spec.md`, `analysis/architecture-mvp.md` §2/§4/§6/§7, `analysis/dedup-reference-flow.md`.

Verdict up front: the **structure** is right almost everywhere — dedup really is last, really is
SHA-256 over the post-transform record minus excludes minus platform metadata, really is
per-stream namespaced, really does fail-stop to a visible path; routing really is
multi-processor; the terminal rule really is enforced on both sides; lifecycle verb semantics
really do follow D16. What is wrong is almost entirely at the **NiFi-contract** layer: several
emitted graphs reference relationships that do not exist, schedule with a cron dialect NiFi
rejects, or set dynamic properties whose validators will refuse them. Those are the findings
that will stop a real deploy, and the current test suite cannot see any of them because
`nifi_apply` is 100 % monkeypatched everywhere except the `live`-marked suite.

Counts: **4 CRITICAL**, **17 MAJOR**, **16 MINOR**.

---

## CRITICAL

### C1 — Cron translation emits an expression NiFi/Quartz rejects (and shifts day-of-week by one)

**Where**: `backend/services/adapter/compiler/transforms.py:47-65` (`cron_or_period`), consumed at
`blocks_http.py:378` and `blocks_jdbc.py:167`. Frozen in
`backend/tests/fixtures/compiler/golden_flow.json:73` as `"0 0 */6 * * *"`.

**What's wrong**: the mapping is a literal field-shift — `0 <min> <hour> <dom> <mon> <dow>` — which
reproduces the standard-cron convention that day-of-month and day-of-week may both be `*`. NiFi's
`CRON_DRIVEN` scheduling period is a Quartz expression, and Quartz refuses an expression that
specifies *both* day-of-month and day-of-week: exactly one must be `?`
("Support for specifying both a day-of-week AND a day-of-month parameter is not implemented").
Second defect in the same three lines: standard cron day-of-week is `0-6` with `0 = Sunday`;
Quartz is `1-7` with `1 = Sunday`. Every DOW value is therefore off by one day.

**Failure scenario**: the prototype's own preset "Daily at 02:00 UTC" (`0 2 * * *`, `CRON_PRESETS`
in `naming.py:228-235`) compiles to `0 0 2 * * *`. `POST /nifi-api/process-groups/{id}/processors`
returns 400 on the `GenerateFlowFile` trigger, `_create_processor` returns `None`,
`_apply_processors` raises `NifiApplyError`, `apply_plan` deletes the half-built PG — **no
cron-scheduled flow can deploy at all**. If a build accepts it, the preset "Weekly (Mon 06:00 UTC)"
(`0 6 * * 1`) fires on **Sunday**.

**Fix**: translate properly, not positionally.
```python
minute, hour, dom, mon, dow = fields
if dow == "*" and dom == "*":       dom, dow = "*", "?"
elif dow != "*":                     dom = "?"        # DOW wins; Quartz needs ? in DOM
else:                                dow = "?"
dow = shift_dow(dow)                 # 0-6 Sun-Sat  ->  1-7 Sun-Sat (also names: SUN..SAT pass through)
return f"0 {minute} {hour} {dom} {mon} {dow}", "CRON_DRIVEN"
```
and assert it in `test_compiler.py` for all six `CRON_PRESETS`, not just `0 */2 * * *`.

---

### C2 — Every conditional branch wires a `failure` relationship that `RouteOnAttribute` does not have

**Where**: `backend/services/adapter/compiler/routing.py:135` (`_wire_any_match`) and
`routing.py:158` (`_wire_all_match`) — `builder.to_dlq(key, "failure")`.

**What's wrong**: `RouteOnAttribute`'s relationship set is `unmatched` plus one relationship per
dynamic property (or `matched`). There is no `failure` relationship, ever, in any routing
strategy. `nifi_apply._apply_intra_connections` (`nifi_apply.py:515-527`) resolves both endpoints
fine (they are real processor ids) and posts the connection with
`selectedRelationships: ["failure"]`; NiFi's connection endpoint validates selected relationships
against the source processor and rejects unknown ones.

Note the same module *already knows* this class of bug elsewhere — `blocks_http.py:479-481` has an
explicit comment "UpdateAttribute has no `failure` relationship to route to DLQ" and skips it.
Routing was not given the same treatment.

**Failure scenario**: any flow with a branch that has ≥1 rule (i.e. the entire D7 feature under
review). `_create_connection` returns `None` → `NifiApplyError("Failed to create connection
'route__hot' -> 'dlq' ...")` → `apply_plan` deletes the PG → `lifecycle.deploy` raises
`LifecycleError` → 502. **No routed flow can deploy.**

**Fix**: delete both `builder.to_dlq(key, "failure")` calls. RouteOnAttribute cannot fail a record
— an EL that cannot evaluate simply yields `false` and the record goes to `unmatched`, which is
already the counted-drop path.

---

### C3 — Every jdbc read wires a `failure` relationship `QueryDatabaseTableRecord` does not have

**Where**: `backend/services/adapter/compiler/blocks_jdbc.py:180` — `builder.to_dlq("query", "failure")`.

**What's wrong**: `QueryDatabaseTableRecord` is a source processor with exactly one relationship,
`success`. Same mechanism as C2: the connection POST is rejected.

**Failure scenario**: `test_jdbc_read_incremental_golden_checks` (`test_compiler.py:456`) asserts
this edge exists (`:483`) — so the test suite actively locks in the defect. Any jdbc-rooted flow
fails at `apply_plan`.

**Fix**: remove the DLQ edge. A JDBC query failure is a **run failure** in MVP §7.14 terms (no
record exists yet), so it must not fabricate a DLQ record anyway — NiFi yields/penalizes the
processor and the failure surfaces as a bulletin. If a visible artifact is wanted, follow the
session-token precedent (`blocks_http.py:412-419`) and log, don't DLQ.

---

### C4 — http pagination never advances the URL: offset/page/cursor loop forever on page 1

**Where**: `backend/services/adapter/compiler/blocks_http.py:305-339` (the `init` `UpdateAttribute`)
plus `_build_pagination` at `:427-497`. Frozen at `golden_flow.json:85`:
`"request.url": "#{svc_svc-http_base_url}/api/3/assets?offset=${offset}&limit=${limit}"`.

**What's wrong**: the module docstring (`blocks_http.py:7-13`) claims "NiFi re-evaluates the
template fresh each time the looped flowfile re-enters `fetch`". It does not. Two separate reasons:

1. `UpdateAttribute` evaluates each property value against the **incoming** FlowFile's attributes.
   `offset` is being set by the *same* processor, so `${offset}` resolves to empty at `init` time —
   the stored `request.url` is literally `.../assets?offset=&limit=`.
2. `fetch`'s `HTTP URL` is `${request.url}`, which yields the **stored string**. NiFi EL is
   single-pass; it never re-evaluates EL found inside an attribute's value. So even if `offset`
   had been seeded first, the URL would be frozen at its first value.

Meanwhile `next` (`:492-495`) faithfully bumps the `offset`/`page`/`cursor` attribute that nothing
reads any more, and `has_more`'s condition `${probe:isEmpty():not()}` stays true because the same
first page keeps coming back.

**Failure scenario**: an `http · read` with offset pagination and no `maxPages` re-requests page 1
in a tight loop for as long as the flow runs — unbounded outbound request volume against the
customer's API, unbounded duplicate FlowFiles into the transform chain (and, with dedup on, an
unbounded stream of duplicate suppressions that look like the flow is "working"). `next_url`
pagination is the only style that works, because `next` overwrites `request.url` with an absolute
URL (`:465`).

**Fix**: build the URL where it is evaluated per iteration, not once. Put the template on
`fetch`'s `HTTP URL` property directly —
`HTTP URL = #{svc_x_base_url}/path?offset=${offset}&limit=${limit}` — and let `init`/`next` maintain
only the counter attributes. `next_url` then becomes the special case (set `request.url`, and make
`HTTP URL` `${request.url:isEmpty():ifElse('<template>', ${request.url})}`), rather than the only
correct case.

---

## MAJOR

### M1 — `match=any` clones the record to every matching rule: duplicate delivery down the branch

**Where**: `routing.py:118-138`, specifically the loop at `:136-137` creating one connection per
`rule_i` relationship into the same child port.

**What's wrong**: with `Routing Strategy = Route to Property name`, `RouteOnAttribute` transfers a
**copy** of the FlowFile to *each* dynamic property whose expression is true. D7/compiler-spec §4
describe this as "any property match forwards", but NiFi's semantics are "every property match
forwards, once each".

**Failure scenario**: branch `hot` with rules `severity equals critical` (rule_0) and
`priority equals p1` (rule_1), `match=any`. A record that is both critical and p1 is delivered to
the child block **twice** — two rows written to the sink, two Avro messages on a governed
`kafka_kc` topic. Dedup does not save you: dedup on the parent runs *before* the fork, and dedup on
the child would suppress the second copy only if the child happens to have dedup configured with an
identity that covers it.

**Fix**: use `Routing Strategy = Route to 'matched' if any matches` and keep one dynamic property
per rule (this still satisfies D7's "N genuine decision expressions on one processor" — only the
relationship set changes, from N to `matched`/`unmatched`), then link `matched` → child once.

### M2 — Blocks that never read their input port still get one, and topic-attached reads get no PortLink

**Where**: `compile_flow.py:179` (`input_port=not is_root`) against `blocks_jdbc.py:120-191`
(`_compile_read` never links from `"inputPort"`) and `blocks_kafka.py:206-266` (`ConsumeKafka` is a
source; nothing links from `"inputPort"`). Port creation at `nifi_apply.py:465-470`.

**What's wrong**: two coupled problems.
(a) A non-root `jdbc · read` (offered mid-chain by `compute_add_menu`, `legality.py:164`) or any
non-root `kafka · read` gets an input port created with **zero outgoing connections**. NiFi marks a
port with no connections invalid, and `start_process_group` then fails for the whole flow PG.
(b) `compile_flow.py:55-58` keys `by_parent` by `parentId`, but a kafka read attached to a topic
node has a **topic id** as its `parentId`. No block ever claims it as a child, so no `PortLink` is
created into it — while `root_block` (`legality.py:265-277`) only elects it as root when the topic
is `kind == "adopted"`. A mid-chain read off an internally materialized topic is therefore neither
root nor child: `is_root=False` → input port created → dangling.

**Failure scenario**: `http read → kafka write (materializes topic T) → kafka read on T → jdbc write`.
Deploy succeeds; Start fails with a port validation error and the flow is stuck in `Stopped` with a
built PG.

**Fix**: derive `input_port` from the graph, not from `is_root` — `input_port = any(c.from_ ==
"inputPort" for c in builder.connections)`. Separately, decide explicitly whether a topic-attached
read is a root (it is, per §7.11 "first `kafka` read in a topic-rooted flow") regardless of the
topic's `kind`.

### M3 — `kafka · write` with children forwards records that were never published

**Where**: `compile_flow.py:155-169`. `compile_publish` consumes the tail (`blocks_kafka.py:202`)
and `autoTerminate=["success"]` (`:200`); then `elif children:` wires `routing.wire_children` off
the **same** pre-publish tail.

**What's wrong**: NiFi duplicates a FlowFile across multiple outbound connections from one
relationship, so the children get a copy in parallel with the publish attempt — including copies of
records whose publish subsequently failed and went to the DLQ (`:203`). Publish's own `success`
relationship, which is the only relationship that means "this record was actually written", is
thrown away.

**Failure scenario**: `http read → kafka write → http write`. Kafka is unreachable for 30 s; every
record in that window is DLQ'd *and* POSTed to the downstream API. R3's "the chain may continue
after a kafka write" is documented nowhere as "continue with records that failed to write".

**Fix**: when a kafka write has children, wire children off `("publish", "success")` and drop the
`success` auto-terminate. (The same question applies to `http · write`, where it is at least an
explicit user choice via `writeForwards` — `blocks_http.py:613-630`. Kafka write has no such
config and silently picks the pre-publish record.)

### M4 — An `extract` transform with a `default` value produces an invalid processor

**Where**: `transforms.py:170-181` — `props["Default Value (informational)"] = str(default)` on an
`EvaluateJsonPath`.

**What's wrong**: every dynamic property on `EvaluateJsonPath` is a JsonPath expression, validated
by `JsonPathValidator`. `"N/A"` (or any default that is not a JsonPath) fails validation → the
processor is invalid → the PG cannot start. The property is *also* semantically wrong: its name
would become an output attribute name.

**Failure scenario**: user adds `extract $.status → status, default "unknown"`. Deploy reports
success; Start fails with `'Default Value (informational)' is invalid because ... is not a valid
JSON Path expression`.

**Fix**: don't emit it. Either materialise the default honestly (an `UpdateAttribute` after the
extract with `attr = ${attr:isEmpty():ifElse('<default>', ${attr})}` — the same EL the comment
already describes), or record it in the scope map / plan metadata, never as a processor property.

### M5 — Every `coerce` transform emits a non-RecordPath property name on `UpdateRecord`

**Where**: `transforms.py:260-270` — `"Coerce Target Type (informational)": target_type`.

**What's wrong**: `UpdateRecord` dynamic properties are `<RecordPath> = <replacement>`; the strategy
here is `record-path-value`, so the *value* must also be a valid RecordPath. `"Coerce Target Type
(informational)" = "integer"` fails on both counts — the value validator rejects `integer`, and if
a build lets it through, `RecordPath.compile()` throws at runtime and every record routes to
`failure` → DLQ.

**Failure scenario**: any flow using the `coerce` transform (a first-class member of the closed
six-transform set) either cannot start or DLQs 100 % of its records.

**Fix**: same as M4 — informational data does not belong in processor properties. Keep the
pass-through `UpdateRecord` (or drop the processor entirely and rely on the writer schema, which is
what the docstring says actually performs the coercion) and put the target type in the scope map.

### M6 — `PutDatabaseRecord`'s `retry` relationship is neither connected nor auto-terminated

**Where**: `blocks_jdbc.py:220-232`.

**What's wrong**: NiFi requires every relationship to be connected or auto-terminated; `retry`
(present on `PutDatabaseRecord` since 1.15) is neither. The processor is invalid → PG won't start.

**Fix**: `autoTerminate=["retry"]` is wrong (silent loss); self-loop `retry` back into the
processor with a penalty, matching the DLQ publisher's own park-in-queue precedent
(`dlq.py:93`), or route it to the DLQ path after the capped retries MVP §7.14 requires.

### M7 — session_token login auto-terminates and connects the same two relationships

**Where**: `blocks_http.py:398-419`: `autoTerminate=list(_INVOKE_HTTP_AUTOTERMINATE)` = `["No Retry",
"Retry", "Original"]`, then `builder.link("login", "run_failure__log", ["Failure", "Retry", "No Retry"])`.

**What's wrong**: `Retry` and `No Retry` are declared auto-terminated at creation and then given a
connection. NiFi treats auto-termination and connection as mutually exclusive for a relationship;
at best one silently wins, at worst the connection POST is rejected and deploy fails.

**Failure scenario**: every http service using `session_token` auth (one of the six mandated
D11 modes).

**Fix**: build `login`'s auto-terminate list explicitly as `["Original"]` and let
`Failure`/`Retry`/`No Retry` all go to `run_failure__log`.

### M8 — A `None`-valued parameter deletes itself from the parameter context on redeploy

**Where**: `compile_flow.py:63-67` (`value=(None if value is None else str(value))`) →
`nifi_apply.py:188-189` → `_update_parameter_context` at `:207-232`.

**What's wrong**: `add_param("redis_password", None, True)` (`transforms.py:344`) and every optional
service secret emit `{"name": ..., "value": null, "sensitive": true}`. In NiFi's parameter-context
**update-request** payload, a parameter with a null value is the *delete* instruction. On the second
deploy (redeploy path — `deploy()` reuses `_ensure_parameter_context`, which takes the update branch
when the context already exists) the parameter is removed while
`RedisConnectionPoolService.Password = #{redis_password}` still references it.

**Failure scenario**: in-cluster Redis with no password (the documented deployment, D15 —
`redis:6379`). First deploy is fine; **redeploy** removes `redis_password`, the Redis pool CS never
validates, `_wait_all_cs_enabled` raises after 45 s, deploy fails and the PG is torn down. Same for
any http service with basic auth and an empty password.

**Fix**: coerce `None` → `""` in `add_param`, or skip emitting both the parameter and the property
that references it.

### M9 — Pause/Resume are broken for every root that is not `http`

**Where**: `deployer/lifecycle.py:379-394` — `_TRIGGER_KEYS = ("trigger",)`, with a stale comment
claiming jdbc "is a NotImplementedError stub as of T7.1".

**What's wrong**: jdbc roots carry their schedule on the `query` processor
(`blocks_jdbc.py:176-179`) and kafka-read roots ingest through `consume`
(`blocks_kafka.py:233-243`). Neither key is in `_TRIGGER_KEYS`, so `_trigger_component_ids`
returns `[]`.

**Failure scenario**: `POST /flows/{id}/verbs/pause` on a jdbc-rooted running flow → 502
"Could not resolve the flow's trigger processor(s) to pause". D16's pause semantics
("stop the trigger/ingest processors, downstream keeps draining") are unavailable for two of the
three root adapters.

**Fix**: `_TRIGGER_KEYS = ("trigger", "query", "consume")`, or better, have the compiler mark the
trigger key per block in the scope map instead of hard-coding conventions in the lifecycle layer.

### M10 — Undeploy does not clear dedup caches

**Where**: `lifecycle.py:535-562`.

**What's wrong**: D16 ("undeploy: ... clear dedup caches (flag for next deploy)") and MVP §7.9
("clears dedup caches") both require it. `undeploy()` deletes the PG, connectors and empties data
topics, but never bumps `dedupEpoch` — the mechanism `clear_dedup_cache` already implements
(`:593-628`).

**Failure scenario**: operator undeploys to reset a stream, redeploys, starts — and the first 24 h
of re-ingested records are silently suppressed as duplicates against the pre-undeploy cache.
Exactly the situation MVP §2.8(b) calls out as needing a remedy.

**Fix**: in `undeploy()`, bump `dedupEpoch` on every dedup transform of every block (the same
`$set` path `clear_dedup_cache` uses), and audit it.

### M11 — A dedup config change neither clears the cache nor warns at deploy

**Where**: nothing implements it. Expected by MVP §2.8(c), restated in D6
("warn-at-deploy when dedup config changed (cache cleared)").

**What's wrong**: `deploy()` has no comparison against the previously deployed dedup config, so
changing `identityFields`/`excludedFields`/`windowHours`/on-off produces no cache clear and no
warning row. The `windowHours` case is the substantive one: `Age Off Duration` and the Redis CS
`TTL` change for *new* entries only; entries already written under the old TTL keep suppressing
for their original lifetime.

**Fix**: persist the deployed dedup config (it already fits in `provenance`), diff it in
`deploy()`, bump the epoch on change, and return a warning row alongside the preflight rows.

### M12 — Names are not frozen at Deploy; renaming a deployed flow orphans its topics and mis-targets Delete

**Where**: `routers/v2/flows.py:196-244` (`save_flow_v2` allows any edit while state is `Stopped`)
vs `lifecycle.py:518-519` (`_dlq_topic_name` derives from the **current** `flow_doc["name"]`) and
`compile_flow.py:52` (`flow_token = tokenize(flow.name)`).

**What's wrong**: MVP §7.1 invariant 2 — "Names freeze at Deploy... after Deploy, names are frozen
for the flow's lifetime". Nothing enforces it. `_owned_data_topic_names` reads real names from the
scope map (good), but the DLQ name is recomputed from the live flow name (bad).

**Failure scenario**: deploy flow "Asset Sync" (creates `dlq.asset_sync`, `raw.asset_sync.asset`),
stop, rename to "Asset Sync v2", delete. `delete()` deletes `dlq.asset_sync_v2` (which does not
exist — Kafbat 404, treated as success at `topics.py:92-95`) and leaves `dlq.asset_sync` on the
cluster forever. Redeploy instead of delete, and you get a second PG, a second topic set, and a
silently abandoned first set.

**Fix**: store `flowToken`/`dlqTopic` on the flow doc at first successful deploy and read those
everywhere after; refuse (or explicitly re-identify) a rename while `deployedAt` is set.

### M13 — Topic reservation is checked against a hard-coded mock list, and only for overrides

**Where**: `naming.py:63-70` + `:189-194` (`RESERVED_TOPIC_NAMES` — the prototype's canned six),
called from `validation.py:249-252` only when `block.topicOverride` is set **and** does not match
the derived name. Preflight (`validation.py:341-474`) adds no topic row of its own.

**What's wrong**: MVP §7.12 row 3 ("Topic reservations valid — no silent suffix on collision") and
compiler-spec §8 ("naming collision check (topic reservation)") both expect a real check. Derived
names are never checked against anything, and nothing checks against *other flows*.

**Failure scenario**: two flows both named "Asset Sync" (nothing forbids duplicate flow names) each
derive `raw.asset_sync.asset`. Both deploy; `ensure_topic_exists` is create-or-verify, so the second
flow happily publishes into the first flow's governed topic — under the first flow's registered
Avro schema.

**Fix**: query owned topics from `flows_v2` (`runtimeScopeMap.*.topics`) plus the live cluster
listing, and add a preflight row per derived name.

### M14 — Iceberg/OpenSearch connector configs omit the credentials the connectors need

**Where**: `compiler/connectors.py:68-103` (`build_kafka_kc_connector`) and `:136-158`
(`build_kc_connector`).

**What's wrong**: compiler-spec §5 and D11 specify, for Iceberg, "catalog uri/warehouse + OAuth2
client credentials, S3 endpoint/keys/path-style"; the sink service model already carries
`oauthClientId`/`oauthClientSecret` (`models/adapter/_secrets.py:43-47`,
`routers/v2/services.py:610`) and `s3SecretKey`. None of it is emitted. OpenSearch's optional
username/password (D11) is likewise dropped, and `initialPosition` — spec §3.5's "consuming ...
from `initialPosition`" — never becomes
`consumer.override.auto.offset.reset`.

**Failure scenario**: deploy a `kafka_kc` block bound to an Iceberg sink service. Preflight passes
(the plugin is installed), the connector is created and paused, Start resumes it, and the task dies
immediately with an auth error against the REST catalog / S3. The flow reports Running.

**Fix**: emit `iceberg.catalog.credential` / `iceberg.catalog.oauth2-server-uri`,
`iceberg.catalog.s3.*` (endpoint, access-key-id, secret-access-key, region, path-style-access), and
`connection.username`/`connection.password` for OpenSearch, from the bound service config.

### M15 — Dedup is per-FlowFile, but nothing guarantees one record per FlowFile

**Where**: the Groovy script at `transforms.py:89` — `def rec = (parsed instanceof List) ? parsed[0] : parsed`
— combined with `DetectDuplicate`, which is a per-FlowFile processor.

**What's wrong**: the invariant "one record per FlowFile at dedup time" is real for the paths the
UI produces (http `SplitJson`, jdbc/kafka `SplitRecord` with `Records Per Split = 1`) but is
enforced nowhere. When it does not hold, the script hashes **record 0** and `DetectDuplicate`
suppresses (or passes) the **entire batch** on that one fingerprint.

**Failure scenario**: `config.split = false` (`blocks_http.py:294` — no UI control today, but the
API accepts it and the compiler honours it) on a dedup-enabled http read: run 1 publishes the whole
page; run 2, page unchanged, the whole page is silently dropped as one "duplicate" — a counted,
intentional-looking outcome hiding N records of data loss.

**Fix**: make it structural — refuse to compile a dedup transform on a block whose chain does not
end in a per-record split (a `CompileError`, since MVP §2.10 makes ordering/shape a
compiler-enforced invariant), or emit a `SplitRecord`(1) immediately before `dedupe__hash`.

### M16 — `api_key` in query location sends the secret as a malformed HTTP header

**Where**: `blocks_http.py:126-130` — `props[f"API Key Query Param (informational): {key_name}"] =
f"#{{svc_{sid}_key_value}}"`.

**What's wrong**: every dynamic property on `InvokeHTTP` is emitted as a request **header**. There
is no "informational" dynamic property. So the compiler adds a header literally named
`API Key Query Param (informational): X-Api-Key` whose value is the API key. Header names may not
contain spaces, parentheses or colons — the HTTP client rejects it, so every request fails; and the
intent (documenting where the key went) is achieved by leaking the secret into a second location.
The key is *already* correctly folded into the query string by `_build_query` (`:171-173`), so the
property is pure downside.

**Fix**: delete those four lines.

### M17 — `kafka · read` (json) hard-codes `SplitJson $[*]`, shredding object-shaped messages

**Where**: `blocks_kafka.py:248-250`.

**What's wrong**: a Kafka message is normally one JSON **object**. Jayway's `$[*]` over an object
returns the list of its **values**, and `SplitJson` happily splits that list. There is no
`recordPath` config for kafka read (unlike http, `blocks_http.py:295`).

**Failure scenario**: topic carries `{"id":1,"name":"asset-a","severity":"high"}`. The block emits
three FlowFiles containing `1`, `"asset-a"`, `"high"`. Every downstream `UpdateRecord`/dedup then
operates on scalars; dedup's `rec instanceof Map` check fails and every record goes to the DLQ with
`dedup_identity_missing`.

**Fix**: only split when the payload is an array — e.g. `EvaluateJsonPath` a type probe and
`RouteOnAttribute`, or simply do not split (one message = one record is the correct default) and
expose an optional record path for array-batched topics.

---

## MINOR

1. **Redis-down routes to the DLQ, which marks the record "handled"** — `transforms.py:394`
   (`to_dlq(detect_key, "failure")`). D6 sanctions the DLQ path, but MVP §2.7/§7.14 classify Redis
   unavailability as an *infrastructure* failure that must be fail-stop and "never routed around";
   reaching the DLQ successfully means the run succeeds and bookmarks advance. Worth an explicit
   ruling: park the record (self-loop with penalty, like `dlq__publish`) rather than DLQ it.
2. **Root `fetch` failures fabricate DLQ records for run failures** — `blocks_http.py:361`
   (`to_dlq("fetch", "Failure")`). MVP §7.14: a root request error is a run failure and "no DLQ
   record [is] fabricated". The session-token path gets this right (`:412-419`); the fetch path does
   not. Non-root http reads/lookups are genuinely record failures, so this needs to branch on
   `is_root`.
3. **Identity-missing check rejects legitimately empty values** — `transforms.py:91-93`:
   `rec.get(f).toString().trim().isEmpty()` treats `""` as *missing*. An empty-string identity is
   present-but-empty; MVP §2.8(a) is about *missing* fields. Records with an empty identity value go
   to the DLQ instead of being fingerprinted.
4. **Identity fields and excludes are top-level only** — `transforms.py:92` (`rec.containsKey(f)`)
   and `:102` (`m.remove(it)`). A nested identity field (`user.id`) can never be found → 100 % of
   records DLQ'd with `dedup_identity_missing`; a nested exclude is silently ignored, so a volatile
   nested timestamp keeps every record unique. Note the transform compiler *does* support dotted
   paths (`_to_record_path`, `:115-123`), so users have every reason to expect dots to work here.
5. **Fingerprint depends on field order** — `JsonOutput.toJson(new LinkedHashMap(rec))`
   (`transforms.py:101-103`). The reference flow has the same property and the doc calls it
   "canonical JSON (source field order)", but any upstream reordering (a rename, a writer that
   re-emits by inferred schema) changes the hash and silently disables suppression. Sorting keys
   recursively before hashing costs nothing and makes the fingerprint honest.
6. **Identity values are joined with `,` without escaping** — `transforms.py:99`. Identities
   `("a,b", "c")` and `("a", "b,c")` collide in the cache key.
7. **`op` is never added to the envelope** — `blocks_kafka_kc.py:44-49` sets `/ingest_id` and
   `/ingest_ts` only, while `DEDUP_PLATFORM_EXCLUDES` lists `op`. Harmless today, but the Record
   Envelope described in MVP §2.4 is not fully materialised.
8. **Dedup suppressions and routing drops are counted by NiFi but never surfaced** —
   `runtime.py:144-235` reports `records24h`/`errors24h`/`queued` and per-block in/out only. MVP
   §2.6/§2.11(8) and §4.2 both require these intentional outcomes to be *counted* and visibly
   distinct from failures. The scope map already carries `dedupe__detect` and `route__*` ids, so
   per-processor auto-terminated counts are one status read away.
9. **Preflight is missing two MVP §7.12 rows** — no "DLQ limits (110 MiB) supported" row, and the
   topic-reservation row (M13). Also `topics.ensure_topics` (`topics.py:35-43`) creates the DLQ with
   default broker settings — MVP §7.13's 7-day retention and 110 MiB ceiling are never configured.
10. **The Connect-plugin row is skipped when any other row fails** — `lifecycle.py:197-205`
    computes the plan (and therefore the plugin check) only when `all(r["ok"])`. MVP §7.12 says
    "every failing check found, not just the first"; an operator fixing a NiFi-connection failure
    will discover the missing plugin only on the next attempt.
11. **Registry-repair self-heal is absent** — MVP §7.4 step 3 ("re-checks every referenced approved
    schema's registry presence; re-registers from local copy if missing"). Preflight only checks
    that a local `ApprovedSchema` doc exists (`validation.py:430-439`).
12. **Kafka CS hard-codes `PLAINTEXT`** — `dlq.py:36-44` ignores the active Kafka connection's
    `securityProtocol`/SASL config (which `_kafka_conn_dict` already extracts,
    `lifecycle.py:125-138`). Correct for this deployment, wrong for any secured cluster.
13. **`delete_topic` failures are swallowed** — `lifecycle.py:576-582` ignores the result, and
    `topics.delete_topic` returns `ok: False` when the connection is in native (non-Kafbat) mode
    (`topics.py:80-83`). Delete then reports success while leaving the DLQ and data topics on the
    cluster, contradicting MVP §7.10's "deletes (not empties) generated topics with ownership proof".
14. **`compiler-spec.md` §2 contradicts the code and the frontend on `tokenize`** — the spec says
    "NO collapse of repeats" (echoing MVP §3.1's `asset___active` example), but `naming.ts:8-13` and
    `naming.py:43-47` both collapse runs (`[^a-z0-9]+` → one `_`) and strip edges. The code has
    parity with the prototype, which is the higher authority (D3); the **spec text** is what needs
    fixing, before someone "corrects" the tokenizer and renames every topic on the platform.
15. **Three latent naming-port divergences** (none currently reachable, all worth closing):
    `override_matches_derived(flow, block, None)` returns `False` where TS returns `true`
    (`naming.py:174-186`); `topic_name_collision(name, None)` returns a message where TS throws
    (`:189-194`); `is_valid_cron`/`cron_preview` split on Python's whitespace set rather than
    `/\s+/`, so a BOM- or NEL-containing cron validates differently on the two sides (`:216`, `:225`).
    Also `cron_preview` returns the shared module-level list by reference (`:213`).
16. **`architecture-mvp.md` §4.1 (first-match-wins) vs D7 (independent branches)** — the
    implementation follows D7 (independent evaluation, a record may take several branches), which is
    correct per the source-of-truth order, but the divergence from §4.1 is not recorded in D7 itself.
    One sentence in D7 ("this supersedes architecture-mvp §4.1's first-match-wins reading, which
    described the pre-prototype model") would stop this being re-litigated.

---

## Area verdicts

**DEDUP — structurally correct; the defects are at the edges.** Position is right
(`compile_flow.py:151` runs `build_chain` between the envelope and the publish, so
`dedupe__hash → dedupe__detect → PublishKafka(Avro)` is exactly the reference ordering, and
`test_compiler.py:240` locks it). Hash is right: SHA-256 over the parsed record minus user excludes
minus `ingest_id,ingest_ts,op` (`transforms.py:101-104`, `:309`). Namespace is right:
`SRC = <flowToken>__<blockId>` (`:329`) — one cache per stream, per D6/MVP §2.4. TTL propagates to
**both** `Age Off Duration` and the Redis CS `TTL` from one `format_duration_hours` call
(`:330, :361, :387`), and the sub-hour rendering is correct down to `1 mins`. Duplicate is
auto-terminated as a counted intentional drop (`:390`); `failure` on both processors goes to the DLQ
rather than being auto-terminated, correcting the reference's documented deviation (`:379, :394`).
The Groovy identity check is *logically* right (missing → `dlq.reason=dedup_identity_missing` →
`REL_FAILURE`, before any hashing) — it fails only on nested paths (MINOR 4) and empty strings
(MINOR 3). The epoch namespace is sound as a design: keys are prefixed by `SRC`, so bumping the
epoch orphans the whole block's namespace atomically, epoch 0 omits the suffix for fixture
stability, and the API is honest about `redeployRequired` (`lifecycle.py:622-628`,
`flows.py:312-326`). Its gap is not soundness but coverage: it is never triggered by undeploy (M10)
or by a config change (M11).

**ROUTING — genuinely multi-processor, but undeployable and duplicating.** The shape follows D7
exactly: one `RouteOnAttribute` per any-match branch with one dynamic property per rule
(`routing.py:118-138`), a real chain of one processor per rule for all-match (`:141-162`),
unconditional children as bare connections letting NiFi fan out (`:108`), a shared `route_fields`
`EvaluateJsonPath` prepended once (`:85-104`), `unmatched` auto-terminated as the counted drop, and
routing processors attributed to the **parent's** scope entry (`compile_flow.py:90` via the returned
keys — actually attributed because the processors live in the parent's builder). EL translation is
correct per operator and null-safe by construction: `Path Not Found Behavior: ignore` leaves the
attribute unset, and NiFi EL on a null subject yields `false` for `equals`/`contains`/`startsWith`/
`matches` and `true` for `isEmpty()` — which is the desired semantics for all six.
`escape_el_literal` handles the quote case (`ir.py:330-332`). What breaks it: C2 (cannot deploy at
all) and M1 (duplicate delivery). Fix those two and this area is done.

**TERMINAL RULE — correct on all three surfaces.** `legality.is_terminal` (`legality.py:92-94`)
covers `kafka_kc`/`kc`; `compute_add_menu` returns `[]` after a terminal parent (`:156-157`);
`validate_placement` raises a violation for any block whose block-parent is terminal, for `kc` with a
block parent, and for anything attaching to a **sealed** topic (`:444-449`, `:431-437`) — satisfying
D9's "kc must attach to an unsealed topic". It is invoked at save (`flows.py:224`, tested at
`test_v2_flows.py:150`) and at the top of compile (`compile_flow.py:47-50`, tested at
`test_compiler.py:309`), so deploy inherits it. `kafka_kc` as root is refused twice
(`legality.py:425`, `blocks_kafka_kc.py:38-39`).
On the recent kafka non-terminal fix: `compile_flow.py:139-144` no longer sets `terminal=True` for
kafka, and the `tail_consumed_by_publish` flag is *correct for three of the four combinations* —
read+childless auto-terminates the split tail (`:170-174`), read+children wires children off the
transform tail (`:167-169`, tested at `test_compiler.py:726`), write+childless lets the publish
consume the tail without a spurious auto-terminate (`:161`, tested at `:745`). The fourth,
**write+children**, is wrong (M3): `elif children:` wins over the publish branch, so both consume the
same pre-publish relationship and children receive records regardless of publish outcome. Note also
that `blocks_kafka.py`'s module docstring (lines 15-56) still describes the *old* behaviour
("unconditionally sets `terminal = True` for EVERY kafka block", "KNOWN GAP ... NO PortLink is ever
created") — it is now actively misleading and should be rewritten with the fix.

**LIFECYCLE — verb semantics match D16 closely.** deploy → preflight → compile → topics → NiFi →
connectors, ending `Stopped` with connectors paused (`lifecycle.py:285-361`, `connect_apply.py:43-58`);
start = start PG + resume connectors; stop = stop PG + pause connectors, **queues retained**
(`:459-482`); stop_clear adds `drop_all_queues` and audits the counts (`:485-515`); redeploy tears
down the PG and rebuilds while `ensure_topics` preserves both data topics and the DLQ (`:312-315`);
undeploy deletes PG + connectors, **empties** owned data topics and **keeps** the DLQ
(`:535-562`, `_owned_data_topic_names` excludes it at `:526-531`), resets state to `Draft`; delete
captures the topic list *before* undeploy nulls the scope map, then deletes the DLQ and the owned
topics (`:565-587`). Audit coverage is complete across verbs. `servicePins` are recomputed at each
deploy from the blocks' bound + sink services (`:231-237`) and preflight refuses retired pinned
services (`validation.py:461-472`) — though nothing makes the *compile* use the pinned revision, so
the pin is a staleness detector, not a pin. Gaps: M9 (pause), M10 (dedup caches), M11 (config-change
warn), M12 (name freeze), MINOR 9-11, 13.

**GENERAL.** Secret handling is sound end-to-end: every secret enters as a `sensitive: true`
parameter and is referenced as `#{...}` (`blocks_http.py:112,117,121,135`, `blocks_jdbc.py:103`,
`transforms.py:344`); `nifi_apply._sensitive_dynamic_props` (`:303-313`) computes
`sensitiveDynamicPropertyNames` for any *dynamic* property referencing a sensitive parameter — which
is exactly what makes bearer/api-key headers legal in NiFi — and correctly excludes statically
sensitive descriptors (`:283`) that NiFi would refuse to see in that list. No secret value appears in
any processor property literal; the only leak-shaped construct is the "informational" api-key
property (M16), and the plan itself never leaves the process (compile is called only from
`_preflight_rows`). Scope-map fidelity is good: block → PG id, plan-key → component id, connector
names, topics (`lifecycle.py:240-260`), consumed correctly by metrics attribution
(`runtime.py:190-207`) and drift/repair. Determinism holds — no timestamps or random ids in plan
bodies, ordering follows declaration order, parameters are first-wins over a deterministic block
walk. DLQ wiring is per-block and complete (`dlq.py`), with the self-loop park-in-own-queue behaviour
spec §6 asks for, `dlq.*` attributes promoted to headers, and a genuinely honest DLQ reader. Naming
has real parity with `naming.ts` (verified function by function; only the three latent divergences in
MINOR 15).

---

## Test gaps that matter

The suite is broad and its assertions are specific — but it cannot catch a single one of the four
CRITICALs, because `nifi_apply`/`connect_apply`/`topics` are monkeypatched in every non-`live` test
and no test validates the emitted graph against NiFi's component contracts.

1. **No relationship-legality check.** A cheap, high-value unit test: a table of
   `processor type → legal relationships`, asserting every `ConnectionSpec.relationships` entry and
   every `autoTerminate` entry is legal for its source, and that every relationship of every emitted
   processor is either connected or auto-terminated (never both). That single test catches C2, C3,
   M6 and M7 at once.
2. **No cron-translation test beyond one expression.** Assert all six `CRON_PRESETS` and, ideally,
   validate with a Quartz-compatible parser. Catches C1.
3. **kafka write with children is untested** — `test_kafka_write_childless_...`
   (`test_compiler.py:745`) uses a leaf block despite its section header. Catches M3.
4. **Dedup epoch → compiled `SRC` is untested.** `test_compiler.py:256` pins only the epoch-0 form;
   `test_deployer.py:454` tests the bump in isolation. Nothing verifies the two halves connect.
5. **The Groovy script's behaviour is never executed or asserted** — the identity-missing branch,
   `dedup_identity_missing`, `dedup_script_error`, and `IDENTITY_FIELDS` exist only frozen inside
   the golden fixture's `Script Body` string. A Groovy-free unit test of the same logic (or a
   `live` test) would protect the single most load-bearing behaviour in the platform.
6. **Only two of six routing operators are asserted** (`equals`, `not_equals`). `contains`,
   `starts_with`, `regex`, `is_empty`, the unknown-operator `CompileError`, and quote-escaping are
   untested; `route_fields` has **zero** references in any test file.
7. **The all-match chain's `unmatched` auto-termination, DLQ edges and final port link are
   unasserted** — only the processor count and EL are.
8. **No test asserts secrets never appear as property literals.** A one-line scan of
   `plan.rootGroup.childGroups[*].processors[*].properties` for every sensitive parameter's *value*
   would lock in the property the design depends on.
9. **`kc` blocks are never compiled by any test** — `_compile_kc` (`compile_flow.py:107-122`), the
   governed-topic converter switch, and the "not attached to a topic" refusal are all uncovered.
10. **`start`/`pause`/`resume`/`stop`/`redeploy` bodies are never invoked** — only their router
    guards are. M9 would have been caught by one pause test on a jdbc-rooted flow.
11. **Parameter-context update path is untested** — no test redeploys, which is why M8 is invisible.
12. **Undeploy's DLQ-retention test passes vacuously** (`test_deployer.py:397-401`): the DLQ never
    appears in any block's `topics` list, so the `name != dlq` filter at `lifecycle.py:530` is never
    actually exercised. `delete()`'s DLQ deletion is never asserted at all.
