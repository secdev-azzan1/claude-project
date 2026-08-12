# Existing UI Summary — lovable_ui frontend

> Produced by analysis Agent A (2026-08-11) from
> `C:\Users\kaifm\Desktop\Project\lovable_ui\frontend`. React 18 + Vite 5 +
> TS 5.8 + shadcn/ui + TanStack Query v5 + React Router v6 + @xyflow/react +
> d3-hierarchy. ~22k lines in src/, dominated by `pages/FlowDesigner.tsx`
> (10,543 lines) and `pages/Flows.tsx` (2,806 lines).

## Shell & navigation

`App.tsx` routes: `/` Dashboard · `/nifi-services` + `/application-services`
→ NifiServices ("Service Manager") · `/schemas` · `/flows` ("Flow Runner") ·
`/flow-designer` (`?new=1`, `?editFlowId=`) · `/audit` ·
`/settings(/connections)` → Platform Connections panel · `/connections`
redirect. `pages/ApplicationServices.tsx` (481 lines) is orphaned/unrouted;
`lib/mockData.ts` unused. Layout: `AppLayout` (sidebar + max-w-[1400px] main
+ title/description/actions header) used by every page; `AppSidebar`
(collapsible; Workspace group: Dashboard, Service Manager, Schema Manager,
Flow Runner, Audit Log; System: Settings; hardcoded admin avatar). Theme:
shadcn HSL tokens + semantic `success`/`warning`/`info` (+`-muted`) tokens;
dark tokens exist but nothing toggles dark mode.

## Pages

- **Dashboard** (199 ln): 4 KPI cards (Total Sources/Total Flows/Running
  Flows/Verified Schemas) from `/api/dashboard/summary` @30s; conditional
  Iceberg Sinks tile; Flow Status panel; Recent Activity (last 6 audit).
- **Audit** (174 ln): table from `/api/audit/?limit=100&search=` @15s,
  debounced search, client-side CSV export, refresh.
- **Connections** (1,506 ln; wrapped by Settings): card grid per connection;
  5 types — kafka (native/kafbat), apicurio, nifi, kafka_connect, iceberg;
  Add (type-select dialog) / Edit / Test / Delete (confirm); health
  StatusBadge (Healthy/Failed/Not Tested) + last-tested; secrets write-only
  ("has_password" booleans). Runtime gating: FlowDesigner/Flows compute
  `missingRuntimeServices` from `/api/connections/`.
- **NifiServices** "Service Manager" (907 ln): (1) Application services —
  REST API/SMB/Webhook credential profiles used by FlowDesigner's
  `application_service` mode; (2) NiFi global controller services — live
  descriptor-driven property dialogs, enable/disable/set-default/delete.
- **Schemas** "Schema Manager" (1,154 ln + lib/schemaEditor.ts 547 ln):
  master-detail; version select ("v3 - Verified"), Verify Version, Save
  Draft, delete; Draft → Needs Verification → Verified lifecycle (Verify
  pushes to Apicurio; editing Verified forks a draft); Linked Flows box;
  two-tab editor — recursive structured Avro field tree (depth-capped,
  logical types, unions, advanced raw-JSON per node) ⇄ raw Avro JSON
  textarea in two-way sync.
- **Flows** "Flow Runner" (2,806 ln): 15s-polled table (State/Name/Source/
  Entities/Topic/Schema/Actions), debounced search, bulk actions with
  per-row eligibility; **the block-reason pattern** — `get*BlockReason(flow)`
  per action returns human reason or null, driving disabled+tooltip+toast
  (getStartBlockReason etc., Flows.tsx:496-551). Detail Sheet with 6 tabs:
  Metrics (10s/30s), Runtime Issues, Kafka tail (50, clear-topic), NiFi
  Processors/Services (descriptor-driven live editing), Iceberg sink
  lifecycle. Flowpack export (blob download) + 3-phase import wizard
  (preview → credentials → schema/service conflict resolution → finalize),
  pure logic tested in `lib/flowApi.ts`.
- **FlowDesigner** (10,543 ln): 6-step wizard — Source Type (6 cards: REST
  API, PostgreSQL [disabled], MongoDB, SMB, Webhook, Trino) → Configure
  Source (per-type forms; manual vs Application Service; OpenAPI upload) →
  **Streams** (the heart: collapsible 320px React Flow map, display-only —
  `nodesDraggable=false`, layout via d3-hierarchy from
  `lib/streamGraph.ts`/`lib/streamFlowMap.ts`, hover "+" adds parallel
  branch; stream cards below with Entity/Iceberg switches; Request card w/
  OpenAPI operation picker; **Test & Shape Response** card — test-stream →
  ResponseTreeViewer explorer, array/field suggestion chips, one-click
  extraction rules; Advanced accordion: Extraction / Pagination (5 types,
  Detect button) / Transformations (ADD/REMOVE/SET_FROM_ATTRIBUTE) / Routing
  (rules + default action + parallel-branch mgmt)) → Destination (per-entity
  cards: derived topic `bronze.<source>.<stream>__history`, partitions, key
  strategy, Avro locked; Use Existing Schema vs Auto Inference with live
  progress + Accept) → Schedule (interval or cron) → Review (summary +
  unverified-schema warnings). Validation `validateDesignerState(step)`
  returns {step,message}[]. Dual persistence: full wizard state to
  localStorage (`nif-flow-designer-draft-v2`) AND `designer_payload` +
  compiled snake_case `streams[]` round-tripped through the backend. Deploy
  happens on the Flows page, not in the designer. Branching: parent-child
  fan-out, conditional routes (purple), parallel branches (blue).
- **Settings** (13 ln wrapper) · **NotFound**.

## API/state layer

`lib/api.ts`: fetch wrapper, base `VITE_BACKEND_URL` else same-origin, no
dev proxy; FastAPI error normalization; get/post/put/patch/delete/postForm +
`timeAgo`. Typed clients: flowApi (632 ln, ~25 types + tested import-wizard
helpers), schemaApi, inferenceApi, icebergSinksApi, nifiServicesApi,
applicationServicesApi, openapiApi. Key shapes: `ApiFlow` {id, name,
source_id, source_type, state: Draft|Deploying|Running|Stopped|Degraded|
Error, enabled, entity_destinations[], …}; `ApiSource` — one flat bag of
~70 nullable per-type columns + `designer_payload` (the pre-adapter model's
clearest artifact). react-query: stable keys, per-tab polling 10-30s,
tab-gated `enabled`, mutations with toast + invalidate. ~25 colocated vitest
files on pure lib modules.

## Reusable components

`AppLayout` / `AppSidebar` / `NavLink` — use as-is. **`StatusBadge`** — the
app's single status vocabulary (~25 statuses → variant+icon pill, compact
mode); extend, don't fork. **`flow-map/StreamFlowMap.tsx`** +
`StreamFlowNode` + pure `lib/streamGraph.ts`/`lib/streamFlowMap.ts` —
auto-layout display-only DAG; adapter prototype can reuse wholesale. Big
in-page editors (pagination/routing/transformations/extraction editors,
ResponseTreeViewer, SchemaFieldRow tree) are file-local but extractable.

## Build/run

Vite port 3000 (prototype: 3001), vitest (jsdom, ~27 test files), lint.
Standalone with mocked backend: all calls go through `lib/api.ts` absolute
`${BASE}/api/...` — intercepting the wrapper is the single choke point.
Pages degrade gracefully (error cards, empty states).

## Preservation notes

Keep: block-reason pattern; StatusBadge; derived flow map (data → pure graph
→ auto-layout → display-only React Flow); test-first configuration
(test → explorer → click-to-configure); entity/destination cards; structured
Avro editor; descriptor-driven property dialogs; import wizard; app shell +
tokens; react-query conventions; lib/ pure-logic + vitest discipline.

Redesign targets: the sourceType union ladder (70-field flat Stream record,
isRestSource/… branching); 10.5k-line page file; dual persistence quirk;
orphaned ApplicationServices page; disabled PostgreSQL type; linear wizard
vs DAG domain (the Streams step already outgrew the wizard).
