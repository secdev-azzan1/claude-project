# Adapter Flow UI Design — Prototype Blueprint

2026-08-11 · governs the implementation in this prototype folder.

## 1. Design mandate (fixed decisions, from the brief)

1. The existing application is **evolved, not rebuilt** — same product feel,
   same shell, same interaction vocabulary.
2. **Form-centric UX is preserved.** Forms configure the workflow; a
   **compact visual flow** represents what the forms built. No full-screen
   canvas editor, no freehand node graph.
3. The flow architecture becomes **adapter-based** (five adapters:
   `http`, `jdbc`, `kafka`, `kafka_kc`, `kc`), replacing the six
   stringly-typed source types and the linear source→streams→destination
   wizard model.
4. Frontend-only: every operation is mocked; state persists in
   `localStorage`; realistic security-domain dummy data in multiple states.

### Resolving the spec-vs-brief tension

The greenfield spec (concept.html §3) makes the canvas the primary authoring
surface. This prototype deliberately does **not** follow that: per the
brief, forms stay primary. What we keep from the spec's builder is
everything *conceptual*:

- the four nouns (Adapter → Stream/block → Connector → Flow),
- the guided **legality model** (rules R1–R8: what may follow what),
- topic nodes, named forks, entity labels, derived names,
- per-block Test, the schema ceremony, cron-only scheduling, lifecycle verbs.

The compact visual keeps exactly three interactions — select a block, add a
legal next block via a filtered `+` menu, and that's it (delete/fork/rename
live in the forms). Everything else is read-only representation. An illegal
flow still cannot be built, because the only way to add blocks is through
the legality-filtered menus (offered both in the visual and in the outline).

## 2. Domain model adopted by the UI

- **Adapter** — closed set: `http`, `jdbc`, `kafka`, `kafka_kc`, `kc`.
  Future families (NoSQL, File share, extra JDBC dialects) appear greyed
  "coming later" in pickers; webhook/syslog/CDC absent entirely.
- **Block (stream)** — one placed adapter with config: id, adapter, mode
  (read/write/lookup where applicable), name, service reference, adapter
  config, generic transformations, entity label (writes), branch/fork
  linkage, test result, validation state.
- **Flow** — named first (name = source name; first half of every derived
  name), single cron trigger on the first runnable block, blocks + edges,
  DLQ `dlq.<flow>`, lifecycle state: Draft · Deploying · Running · Paused ·
  Stopped · Degraded · Error, enabled flag.
- **Connector** — versioned export of a flow (`rapid7-to-iceberg@1`), no
  secrets; represented by Save-as-Connector / Import dialogs (mocked).
- **Topic node** — first-class: adopted (root) or materialized by
  kafka/kafka_kc writes; kc sinks attach to topic nodes (dashed edge);
  kafka_kc topics are sealed (never attachable).
- Legality matrix (drives every `+` menu):
  - Roots: http read, http write, jdbc read, kafka read, adopted topic.
  - After a record-carrying block: http (r/w/l), jdbc (r/w/l), kafka write,
    kafka_kc, fork.
  - kafka_kc and kc are terminal. kc only attaches to (non-sealed) topic
    nodes. Kafka write → platform cluster only. Raw-mode branch: transforms
    absent, kafka_kc refused (R8). Forks never merge; branches are named
    (`fork-N` default, editable).
- Naming: tokenizer (lowercase, non-alnum → `_`); topic
  `raw.<source>.<entity>`; variant `raw.<source>.<entity>.<variant>`
  (variant pre-filled from branch label when multiple same-kind writes);
  table `bronze.<source>.<entity>__raw`; DLQ `dlq.<flow>`. Kafka-family
  topic names overridable (tokenized live + collision check + `raw.*`
  warning); table/DLQ names always derived.

## 3. The Flow Builder page (new: `/flow-builder/:id`)

Replaces the 6-step FlowDesigner wizard. Layout, top to bottom:

1. **Header bar** — flow name + state badge + enabled switch, and the
   lifecycle verb bar: Validate · Save · Deploy · Start · Pause · Resume ·
   Stop · Stop & Clear · Redeploy · Undeploy · Delete. Every verb uses the
   existing block-reason pattern (disabled + tooltip explaining why).
   Editing a deployed, non-stopped flow shows a lock banner (form fields
   disabled) — the kc "Save is live" exception noted inline.
2. **Compact visual flow** (collapsible, ~320px) — reuses the existing
   StreamFlowMap architecture (pure graph derivation → d3-hierarchy layout →
   display-only React Flow). Node types: **block nodes** (adapter icon +
   mode chip, name, entity label, error badge) and **topic pill nodes**
   (mono name, "adopted · sampled, never renamed" or sealed-lock for
   kafka_kc). Edge styles: solid = record flow, blue = fork branches (with
   branch-name label), purple = routing branches (with rule name), dashed =
   kc subscription. Hovering a block/topic output shows **+** → a menu of
   only the legal next blocks (greyed future-scope entries included, with
   reasons); clicking a node selects it and scrolls its form into view.
3. **Working area, two columns:**
   - **Left — outline navigator** (the form-centric spine): "Flow
     settings" entry, then the block tree (indentation mirrors branches;
     topic nodes and kc attachments shown inline). Each row: adapter icon,
     name, chips (entity, tested, error count). `+ Add block` affordances
     appear per insertion point with the same legality menu. This is the
     revival of the never-shipped hierarchy-navigator idea from the audit.
   - **Right — the form panel** for the current selection:
     - **Flow settings form**: name (locked after deploy), description,
       cron schedule (presets + 5-field editor + mocked next-3-occurrences
       preview; hidden entirely for topic-rooted kc-only flows), DLQ name
       preview (read-only), flow variables (name/value/secret rows),
       validation summary.
     - **Block forms** (per adapter, sections in order):
       a. *Identity*: block name, adapter + mode, service selector
          (Application Services filtered by type; "＋ create private
          service" inline dialog). Credentials never appear on the block.
       b. *Adapter settings* (per type):
          - **http**: method, path (with `${placeholder}` support),
            OpenAPI operation picker (seeded spec), headers/query rows,
            body template (writes), response parsing (format
            JSON/XML/CSV/text; record path with picker-from-test; split
            toggle), pagination (off/page/cursor/offset/next-url — dynamic
            per-type fields + **Detect from test**), `proxy: on` toggle
            (gateway note: allowlisted hosts only, upstream certs not
            verified), lookup-mode fields (join field), write-mode choice
            "continue with: original records | parsed response".
          - **jdbc**: table picker (from the service's mocked catalog),
            column multi-select, incremental read (watermark column +
            bookmark note + initial-position choice), write mapping note
            (`change_type` → INSERT/UPDATE/DELETE).
          - **kafka read**: cluster (platform or External Kafka receiver
            service), topic (adopt — picker with backlog estimate), parse
            format (JSON/CSV/XML/**raw** — raw shows the R8 quarantine
            notice), initial position (beginning/new — immutable after
            first start).
          - **kafka write**: derived topic preview + custom-name override
            (tokenized live, collision check, `raw.*` warning).
          - **kafka_kc**: sink service (Iceberg catalog), derived topic +
            table previews (no override on table), schema panel — Approved
            chip (with view link) or "Schema ceremony required" warning +
            **Start ceremony** button.
          - **kc**: attach-to-topic selector (only attachable topic nodes),
            sink service (e.g. OpenSearch), backlog estimate + initial
            position, "Save is live" notice. No transforms, no Test.
       c. *Entity label* (writes only, required — "No write without an
          entity, ever").
       d. *Generic transformations* (absent on kc and raw branches):
          ordered rule list — Extract/project · Add field · Remove field ·
          Set from attribute · Rename · Coerce · Route/filter · Dedup —
          with move up/down, dedup pinned last (identity fields, excluded
          fields, window with 24h default, Redis fail-stop note). Routing
          rules create named branches (first-match-wins; route/drop/forward
          actions; "dropped records are counted, never errors").
       e. *Test* (all except kc): Test button → simulated probe (max 10
          records) → response tree with click-to-set record path, field
          suggestion chips, failures-as-data (seeded failing example),
          `${placeholder}` prompt dialog, double-confirm for mutating
          methods. The stored result feeds downstream field pickers.
       f. *Branches* (on forks/routes): branch list with editable names,
          which pre-fill topic variant tokens.
       g. *Danger zone*: delete block (removes its subtree after a naming
          confirmation), fork here.
4. **Destinations panel** (bottom card): every topic in the flow with its
   attached sinks — "the dashed edges, as a list."
5. **Validation & preflight**: per-block issues badge in outline + node;
   flow Validate lists issues (click → jump to block/form section); Deploy
   opens a mocked **preflight checklist** dialog (active connections,
   services reachable, schemas approved, cron valid, no unresolved
   ask-at-runtime values) that blocks with named reasons.
6. **Schema ceremony** (modal wizard from a kafka_kc block): Declare
   (entity, evidence path: live sample run | upload samples | author by
   hand; pre-fill from approved schema) → Orchestrate (simulated sample
   collection progress) → Review (structured field table ⇄ raw Avro JSON,
   editable, reusing the existing editor pattern) → Approve (= register;
   simulated registration; failure fails approval). Approval unlocks
   Deploy; provenance recorded (sample-run / uploaded / manual).

New-flow entry: `/flow-builder/new` opens a small **"Name your flow"**
panel first (explicit naming step; live tokenized preview of
`raw.<name>.…` / `dlq.<name>`), then root selection via the legality menu.

## 4. Other pages

- **Flows** (`/flows`) — keep table + detail-sheet structure. Columns:
  State · Flow · Root · Entities · Topics · Schema · Actions. New verbs
  (Pause/Resume, Stop & Clear, Redeploy) with block-reasons; bulk actions
  kept. Detail sheet tabs: Overview (blocks read-only, services with
  revision pins + "update available"), Metrics (mocked, per-block
  attribution), **DLQ** (replaces Runtime Issues: records with block +
  error class, download, no replay), Messages (newest-first cap 50, plain
  text, "binary payload (N bytes)"), Connect (kafka_kc/kc sink states).
  Export → **Save as Connector** dialog (name@version, "no secrets"
  notice); Import → simplified connector import wizard (file → preview →
  bind services → rename → finalize), mocked.
- **Platform Connections** (`/connections`, promoted to its own nav item;
  Settings page retired) — six types: NiFi, Kafka, Apicurio, Kafka
  Connect, **Redis (new)**, **API Gateway/APISIX (new)**; Iceberg type
  removed (moved to Sink destination services). Multiple connections per
  type with exactly one **Active** (Activate blocked while the active one
  has dependents — block-reason + impact preview); Test (simulated,
  deterministic per seed) recording health + reachability as two facts;
  no background polling (manual Test/Test All); Edit (blank keeps secret);
  Delete guarded by impact preview; **Repoint** dialog (adopt / migrate /
  reset + impact list + simulated progress log). Redis switch: never
  blocked, warned with real counts (dedup windows, bookmarks). APISIX
  connection detail includes the **Gateway resources** manager (client
  certificate profiles, upstreams, routes, host allowlist —
  reference-counted deletes).
- **Application Services** (`/application-services`, replaces "Service
  Manager"; NiFi controller-services UI dropped) — four types: **HTTP**
  (auth: none/basic/bearer/api-key/OAuth2/**session token**), **Database**
  (dialect PostgreSQL/Trino/MySQL; greyed future dialects), **External
  Kafka receiver** (input-only), **Sink destination** (OpenSearch, Iceberg
  catalog…). Lifecycle: list/create/edit (edits create **revisions**;
  dependent flows show "service update available — adopts at next
  deploy")/test/**retire** (logical; dependents flagged "action
  required"). Secrets write-only.
- **Schemas** (`/schemas`) — becomes a **read-only browser**: search;
  cards/table with subject, entity, provenance badge (sample run /
  uploaded / manual — "not sample-validated"), Approved date, linked
  flow/block; detail dialog with structured field table + raw Avro tabs
  (read-only); single action: **Start a pre-filled ceremony** (jumps into
  the owning flow's kafka_kc ceremony). Old Draft/Verify workspace removed.
- **Dashboard** (`/`) — kept (existence recorded as an open product
  decision): KPIs → Flows, Running, Approved Schemas, Connections healthy
  x/y; Flow Status panel; Recent Activity. "Total Sources" and "Verified
  Schemas" retired with the old nouns.
- **Audit** (`/audit`) — unchanged page; seeds extended with new verb
  events (Pause, Redeploy, ceremony Approve, Activate/Repoint, service
  revision, gateway change).
- **Variables** (`/variables`, new) — global variables table
  (name/value/secret flag, masked display), CRUD; note that flow-level
  overrides live in the Flow settings form.
- **Sidebar** — Workspace: Dashboard, Flows, Schemas, Application
  Services, Audit Log. System: Platform Connections, Variables. Footer:
  "Adapter UI Prototype · mock data" badge + **Reset demo data** button.
- **Removed routes**: `/flow-designer` (wizard → redirects to new
  builder), `/nifi-services` (redirects to `/application-services`),
  `/settings` (redirects to `/connections`). Old page files stay on disk,
  unrouted, for reference/diffing.

## 5. Prototype architecture

```
UI components (pages + components/)
  ↓
prototypeApi (typed service layer; simulated latency + failures)
  ↓
store (mock repository: load/save/mutate, one localStorage document)
  ↓
seeds (realistic security-domain dataset; versioned; Reset restores)
```

- `src/prototype/types.ts` — the adapter domain model.
- `src/prototype/seeds.ts` — seed dataset (see §6).
- `src/prototype/store.ts` — localStorage-backed repository
  (`dmp-adapter-prototype-v1`), seed-version stamped, reset support.
- `src/prototype/api.ts` — typed async service layer used with
  react-query; deterministic simulated Test/Deploy/latency.
- `src/prototype/naming.ts`, `legality.ts`, `validation.ts` — pure logic
  (tokenizer + naming walk; `+`-menu computation; block/flow validation).
- `src/lib/api.ts` — the legacy HTTP core is hard-disabled (throws an
  offline-only error), so no network request can leave the app even from a
  stray legacy import; every routed page reads the mock store through
  `src/prototype/api.ts`.

## 6. Seed data (all interactive, multiple states)

- **Platform connections**: Production NiFi (Healthy, active) · Staging
  NiFi (Not Tested) · Primary Kafka Cluster (Healthy, active) · DR Kafka
  Cluster (Not Tested) · Apicurio Schema Registry (Healthy, active) ·
  Kafka Connect Cluster (Healthy, active) · Legacy Kafka Connect (Failed)
  · Dedup Redis (Healthy, active) · APISIX Gateway (Healthy, active; cert
  profile "FortiSIEM Client Cert", 1 upstream, 1 route, allowlist).
- **Application services**: Rapid7 InsightVM API (api-key) · FortiSIEM
  Events API (session token) · ServiceNow CMDB API (OAuth2; **revision 3
  pending** → "update available" on its flow) · Security Postgres
  (PostgreSQL) · Trino Lakehouse (Trino) · Partner SIEM Kafka (external
  receiver) · Iceberg Bronze Catalog (sink dest) · SOC OpenSearch (sink
  dest) · Retired: Legacy Qualys API (→ "action required" on a flow).
- **Flows** (seven, covering the brief's §18 list):
  1. *Rapid7 Assets to Lakehouse* — http read → transforms → kafka_kc
     (Iceberg) — Running, schema Approved. Simple source→destination.
  2. *FortiSIEM Security Events* — http read (session-token svc) → route
     fork (`critical`/`routine`) → kafka_kc + filtered kafka write; kc
     (SOC OpenSearch) attached to the raw topic — Running. Branching +
     multiple destinations + kc.
  3. *CMDB Asset Sync* — jdbc incremental → kafka write → topic → kc
     OpenSearch — Stopped; shows "service update available".
  4. *Partner Threat Feed Relay* — adopted topic root → kafka read →
     transforms → kafka write — **Paused**.
  5. *Vulnerability Scan Delta* — http read + pagination + http lookup —
     **Draft with validation issues** (missing entity label, ceremony not
     run, unresolved `${scan_id}`).
  6. *Asset Retirement Notices* — http read → named fork (all / active /
     decommissioned) → kafka_kc + two kafka writes, one with custom topic
     `asset_retired` — Stopped, valid. The naming-walk showcase.
  7. *Mirror Raw Audit Topic* — kafka read (**raw**) → kafka write —
     Running. R8 quarantine showcase.
- **Schemas**: `raw.rapid7_assets.asset-value` (sample run) ·
  `raw.fortisiem_events.incident-value` (uploaded) ·
  `raw.asset_retirement.asset-value` (manual — flagged). All Approved.
- **Connectors**: `rapid7-to-iceberg@1` (published from flow 1).
- **Variables**: `fortisiem_base_path`, `rapid7_region`, secret
  `scan_api_token` (masked).
- **DLQ**: ~6 records on flow 2 (error classes: parse failure, missing
  identity field). **Metrics**: canned per flow/block. **Audit**: ~30
  events across all verbs.

## 7. Deliberate prototype simplifications

- Deploy/Test/Repoint/ceremony run on timers with deterministic outcomes
  (seeded per entity), not real calls.
- The naming walk implements steps 1, 3, 4 (bare name, variant tokens,
  governed-write-wins); step 2's "identical populations collapse" is
  approximated (same-entity writes on unforked paths share the topic).
- Cron next-3 preview is computed for presets and common expressions, not
  a full cron engine.
- Connector import previews canned bundles rather than parsing arbitrary
  files.
- Ops metrics are static snapshots with light jitter, not time series.
- No RBAC, no dark mode work, no mobile pass (matches existing app).
