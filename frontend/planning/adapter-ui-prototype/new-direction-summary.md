# New Product Direction — Digest of concept.html

> Produced by analysis Agent B (2026-08-11) from
> `C:\Users\kaifm\Desktop\Project\DataPASC-DataMobility\plan\concept.html`
> ("Data Mobility MVP — Complete Specification", 2026-08-05), with secondary
> clarifications marked **[plan.md]**. This is reference material for the UI
> prototype; the prototype deliberately deviates where the user's brief fixes
> a different UX decision (form-centric, compact visual — see
> `adapter-flow-ui-design.md`).

## Terminology correction up front

The dotted adapter identities from older planning rounds (`nifi.http.in`,
`nifi.http.out`, `nifi.kafka.bridge`, `kafka_connect.iceberg.out`, …) and the
encoding enums (`WHOLE_PAYLOAD`/`PARSE_AND_SPLIT`,
`PRESERVE_SOURCE_FORMAT`/`NORMALIZE_TO_AVRO`) appear **nowhere** in
concept.html or plan.md. They are superseded. The new direction has exactly
**five adapters with short names**: `http`, `jdbc`, `kafka`, `kafka_kc`, `kc`,
plus an invisible shared parent `base`.

## 1. The four terms (§2)

Positioning (§1): *"Users describe where data comes from, how it should be
shaped, and where it should go."* Governing rule: **engines are invisible** —
users see adapters, streams, entities, and flows, never NiFi processors or
Connect configs. The platform "only moves and lightly reshapes data — one
record at a time, no reference data" (joins/aggregation belong to the separate
Refinement platform).

| Term | Meaning |
|---|---|
| **Adapter** | A building block that ships with the platform. Five exist: `http`, `jdbc`, `kafka`, `kafka_kc`, `kc` — plus invisible parent `base`. Hardcoded; not user-extensible. |
| **Stream** | One block the user actually places — an adapter filled in with config (e.g. "List All Sites" is an http stream). Hosts/credentials are **selected** from saved services, never typed into a block. |
| **Connector** | A saved chain of streams describing one data path (e.g. "Rapid7 to Iceberg"). Travels as a file — no secrets. Named + versioned (`rapid7-to-iceberg@1`), immutable once published. |
| **Flow** | A connector brought to life with real services bound. The unit you deploy, start, pause, stop. Owns its dead-letter queue. |

### The five adapters (§5)

- **`http`** — read / write / lookup against APIs. Richest adapter: request
  builder, OpenAPI import, response parsing (JSON/XML/CSV/plain text),
  pagination (only here), chained requests, lookup mode, write mode, APISIX
  `proxy: on` egress.
- **`jdbc`** — read / write / lookup against databases. Dialects: PostgreSQL,
  Trino, MySQL/MariaDB ("Trino is a service choice, not its own adapter"). No
  custom SQL — everything generated from picked tables/columns. Incremental
  reads with watermark+bookmark; history-driven writes mapping `change_type`
  → INSERT/UPDATE/DELETE.
- **`kafka`** — read (any cluster) / write (platform cluster only), always
  **schemaless** (JSON bytes). Reads parse JSON/CSV/XML/raw; supports topic
  **adoption**.
- **`kafka_kc`** — "the structured write — one unit, always terminal": a new
  governed Avro topic + a Kafka Connect sink created and managed together.
  The only place Avro and schemas exist. Requires the mandatory **schema
  ceremony**; targets lakehouse-class sinks (Iceberg first).
- **`kc`** — "a sink over an existing topic — subscription only": moves a
  topic's bytes untouched into any installed Connect sink (OpenSearch upsert
  first). No transforms, no schema surface. Attaches to topic nodes with a
  **dashed line**; "Save is live."
- **`base`** *(invisible)* — contributes the shared Generic-transformations
  section (§6) and the per-block Test contract (§8) to every adapter except
  kc. [plan.md: also owns the Record Envelope (`ingest_id`, `ingest_ts`, `op`
  as message headers), entity labeling, and the failure taxonomy.]

Other first-class vocabulary: **entity label** (one word for what the data
*is* — `asset`, `incident`, `order`; mandatory on every write: "No write
without an entity, ever"), **topic node** (the topic rendered as its own
node), **adopted topic** ("sampled · never renamed"), **fork/branch names**,
**the naming walk**, **the schema ceremony**, **source name** (the flow's
name — first half of every derived topic/table/DLQ name), **Platform
Connections** vs **Application Services**, **DLQ**, **drift**.

## 2. Workflow semantics (§3, §4, §6, §7)

A flow is "a left-to-right chain of blocks." The builder is **guided**: every
block's output shows a **+** button offering only the blocks legal at that
position — an illegal flow cannot even be drawn. Build sequence: ① name the
flow (= source name) → ② place root, pick service, fill form, set the flow's
single cron → ③ Test the block → ④ chain & shape with + (adapter settings
first, then Generic transformations) → ⑤ add writes with entity labels
(kafka_kc opens the ceremony) → ⑥ Deploy & Start.

### The eight graph rules (R1–R8), condensed

- **R1 — One root, one schedule.** One root per flow; the single **cron**
  trigger (UTC, presets; only trigger type) lives on the first runnable
  block. Mid-chain kafka reads are continuous consumers, unscheduled. A
  topic-rooted flow with only kc sinks has no trigger at all. "Want two
  sources? Make two flows."
- **R2 — Legal roots:** http read, http write (a POST whose *response* is the
  data), jdbc read, kafka read, or an adopted topic node. `kafka_kc` can
  never be root.
- **R3 — Writes are not dead ends**, except the two Connect-backed ones. The
  chain continues after http/jdbc/kafka writes (http write chooses per block
  whether original records or parsed response flows on). **`kafka_kc` and
  `kc` are always terminal.**
- **R4 — Forks fork, never merge.** Any record-carrying output can split into
  N branches; **every fork is named** (user name, routing-rule name, or auto
  `fork-1`/`fork-2` defaults, always editable). A plain fork is unconditional.
  Branches never re-join.
- **R5 — kc attaches to topics only** (dashed), and is terminal.
- **R6 — Kafka: read anywhere, write home only** (platform cluster).
- **R7 — Names derived by default; every Kafka topic name may be overridden**
  (tokenized + reserved + collision-checked; warned if it strays into
  `raw.*`). Table and DLQ names stay derived.
- **R8 — Raw bytes are quarantined.** On a raw-mode kafka read branch the
  Generic-transformations section is absent and kafka_kc is refused.

### Topic nodes & Destinations panel

Topics appear as pill-shaped nodes whenever a real topic exists (adopted at
root, or materialized by a kafka-family write). Exactly two things attach:
kafka read (solid arrow — chain continues) and kc sinks (dashed —
independent subscription). kafka_kc topics are **sealed** — visible, never
attachable. A **Destinations panel** lists every topic with its attached
sinks ("the dashed edges, as a list").

### Generic transformations (§6)

One identical section inside each hosting block (http r/w/l, jdbc r/w/l,
kafka read/write, kafka_kc before Avro; never kc, never raw branches),
applied after adapter parsing, in user order: **Extract/project · Add field ·
Remove field · Set from attribute · Rename · Coerce · Route/filter/drop/
forward · Dedup**. Flatten deliberately removed. Routing is conditional
first-match-wins; "dropped records are intentional outcomes — counted, never
errors." **Fork ≠ route.**

**Dedup** is an in-stream transform, **always last**: identity fields,
excluded fields, time window 1 min–365 d (default 24 h); SHA-256 fingerprints
in Redis, one cache per stream; "Redis down = records fail rather than sneak
through"; labeled "duplicate suppression, not a delivery guarantee." Missing
identity field → DLQ; audited per-stream "Clear dedup cache" action.

### Naming (§7)

One tokenizer (trim → lowercase → non-alphanumeric → `_`). Derived names:
topic `raw.<source>.<entity>`, variant `raw.<source>.<entity>.<variant>`,
lakehouse table `bronze.<source>.<entity>__raw`, DLQ `dlq.<flow>`. Names
preview read-only while building, are reserved per cluster before creation
(collisions block with the reason), and freeze at deploy. The **naming walk**
(4 deterministic steps): sole writer per kind keeps the bare name → identical
populations collapse into one shared topic → differing populations take
variant tokens pre-filled from branch labels → on cross-kind collision the
governed (kafka_kc) write holds the bare name.

**Multi-destination:** fork after the last shaping block and give each branch
a write.

### Scheduling & error handling (§13)

Cron: standard 5-field UTC with presets, validated at save with a
next-3-occurrences preview; overlapping occurrences skipped and counted.
**DLQ**: on by default, one per flow (`dlq.<flow>`, 7-day retention); failing
record gets 3 retries then lands with original bytes + headers naming block
and error class; inspect/download in UI; **no automated replay**. Failure
taxonomy: record failures (retryable/permanent), run failures, infrastructure
failures (fail-stop). Record Envelope: `ingest_id`/`ingest_ts`/`op` as
message headers, excluded from dedup identity and inference.

### Testing (§8)

"Test is per block, never per flow" — one real bounded probe (max 10
records). `${placeholder}` values prompted in a dialog. Mutating methods
double-confirmed. One test result feeds the response explorer,
field-suggestion chips, extraction buttons, and pagination Detect. "Sample
runs never commit anything." Failures return as data (`ok:false` + reason).

### Schema ceremony (§9)

Triggered by exactly one thing: configuring a `kafka_kc` block. Four steps:
**Declare → Orchestrate → Review → Approve**. Three evidence paths: live
sample run (throwaway `-schema-inference` topic, ~10 messages), uploaded
sample files (JSON/XML/CSV/XLSX), or author by hand (provenance-flagged).
Pre-fill from any approved schema. Editor: synchronized structured field
table (depth 5) + raw Avro JSON tabs. **Approve = register** (subject
`<derived topic>-value`); registration failure fails approval. **No
evolution** — re-run the ceremony deliberately. Flow undeployable until
approval. The **Schemas screen** becomes a read-only browser whose single
action is "start a pre-filled ceremony from this schema."

## 3. Platform/service connections (§11, §12, §17)

**Platform Connections** (admin; six types, exactly one active each):
**NiFi** · **Kafka** (native or Kafbat-proxy mode) · **Apicurio** · **Kafka
Connect** · **Redis (new)** — "dedup caches and jdbc bookmarks, in separate
logical databases", standalone only · **API Gateway / APISIX (new)** — admin
URL (backend-only) + runtime URL for `proxy: on` egress. **The old Iceberg
connection type is removed** — catalog details move to a Sink destination
service.

Machinery: health vs reachability are separate recorded facts; no background
polling; Activate blocked while current connection has dependents;
**Repoint** (adopt/migrate/reset) with impact preview, fingerprint identity
checks, audited progress. Redis switches are never blocked — warned with real
counts + explicit confirmation; gateway switches re-reconcile managed
resources. Registry connection cannot be edited/deleted while approved
schemas depend on it.

**Application Services** (§12) — reusable endpoint+credential profiles;
"every adapter that needs credentials selects a service." Four types: **HTTP
service** (auth: none/basic/bearer/api-key/OAuth2/**session token** — "logins
are never modeled as data streams anymore"), **Database service** (dialect,
curated driver, host/db, credentials, read/write capability), **External
Kafka receiver** (input-only), **Sink destination service** (endpoint +
credentials for kc/kafka_kc sinks — OpenSearch, Iceberg catalog…). Manual
mode = inline (private) service creation; **edits create revisions** — linked
flows show "service update available" and adopt at next deploy; deletion is
logical retirement flagging dependent flows "action required."

**APISIX egress (§17):** `proxy: on` exists because NiFi refuses endpoints
with broken/nonstandard TLS. Gateway mode unlocks those endpoints plus client
certificates — only for admin-allowlisted hosts, with the UI stating plainly
that upstream certificates are not verified. Egress only. Admin-managed
gateway resources: Client Certificate Profiles, Upstreams, Routes, host
allowlist.

**Variables:** global admin screen (name, value, secret flag) + per-flow
section; flow overrides global. "Ask-at-runtime" values exist only at
Test/ceremony time — deploy refuses flows with unresolved ones.

## 4. UI doctrine from the spec

- Guided composition, not freehand: filtered + menus; refusals explain
  themselves in R-rule words.
- Field pickers over tested shapes; response-tree explorer; OpenAPI import
  with per-parameter bindings.
- Future scope signposted: greyed "coming later" entries — NoSQL family,
  File-share family, extra JDBC dialects. Webhook/syslog/CDC absent entirely.
- Lifecycle verbs: Deploy · Start · Pause (queues) · Resume · Stop · Stop &
  Clear · Redeploy · Undeploy · Delete. Editing a deployed flow is refused
  until stopped — except kc's Save-is-live.
- Statuses: connection Healthy/Failed/Not Tested; service revisions +
  "update available"/"action required"; flow drift; ceremony lock; per-run
  history.
- Initial-position prompts wherever data comes from existing storage
  (beginning vs new + backlog estimate).
- Honesty patterns: no dead knobs, refusals with reasons, warnings with real
  counts, destructive confirmations naming exact resources, redacted secrets
  ("blank keeps existing").

## 5. Conceptual delta vs the stream-based model

1. **"Stream" is redefined** — one configured block, not an end-to-end path.
   Pipeline concepts are now Connector (definition) and Flow (instance).
2. **Wizard → guided composition** (the prototype maps this onto forms +
   compact visual per the user's fixed UX decision).
3. **Source-type ladder → closed adapter registry** (5 adapters × modes).
4. **Graph, not line**: named forks, conditional routing, topic nodes, dashed
   kc subscriptions, terminal rules.
5. **Schema handling inverted**: Avro only at kafka_kc; ceremony replaces the
   Draft→Needs-Verification→Verified pipeline; Schemas screen read-only.
6. **Encoding enums gone** — replaced by per-adapter parse settings and the
   kafka vs kafka_kc choice.
7. **Credentials move off blocks** into revisioned Application Services.
8. **Cron-only triggers**; token bootstrap becomes session-token auth on the
   HTTP service.

## 6. Explicit non-goals / deferred

Shelved (return later as adapter packages): MongoDB, SMB, webhook receiver,
manual file upload, syslog, CDC/Debezium. Greyed in pickers: NoSQL family,
File-share family, extra JDBC dialects. Dropped on purpose: Flatten,
fixed-attempt polling, user-facing NiFi controller-services manager, live
editing of deployed processors, fan-out barrier, schema versions/verify
pipeline, interval & on-change triggers, per-message Kafka deletes, DLQ
replay, fan-in, multi-source flows, RBAC, user-authored adapters. Also: no
rate-limiting knobs, OpenAPI 3.0/3.1 JSON only, no Avro decode in the
message viewer, no external-cluster Kafka writes, no background health
polling.
