# Prototype UI Audit — `lovable_ui_adapter_ui_prototype`

Read-only audit of the frontend prototype at
`C:\Users\kaifm\Desktop\Project\lovable_ui_adapter_ui_prototype`. This is a
**frontend-only, mock-backed** React/Vite/TS app: `README.md` states plainly
"No backend. Every operation — test, deploy, ceremony, repoint — is
simulated in the browser." All file paths below are relative to that project
root unless given in full.

---

## 1. App structure

### 1.1 Routing

Routing is defined in `src/App.tsx:24-42` (React Router v6, `BrowserRouter`):

| Path | Page component |
|---|---|
| `/` | `Dashboard` (`src/pages/Dashboard.tsx`) |
| `/flows` | `Flows` (`src/pages/Flows.tsx`) |
| `/flow-builder/new` | `FlowBuilder` (`src/pages/FlowBuilder.tsx`) |
| `/flow-builder/:flowId` | `FlowBuilder` |
| `/schemas` | `Schemas` (`src/pages/Schemas.tsx`) |
| `/application-services` | `AppServices` (`src/pages/AppServices.tsx`) |
| `/audit` | `Audit` (`src/pages/Audit.tsx`) |
| `/connections` | `Connections` (`src/pages/Connections.tsx`) |
| `/apisix` | `Apisix` (`src/pages/Apisix.tsx`) |
| `/flow-designer` | redirects to `/flows` |
| `/nifi-services` | redirects to `/application-services` |
| `/settings`, `/settings/connections` | redirect to `/connections` |
| `/variables` | redirects to `/` (global variables were removed) |
| `*` | `NotFound` (`src/pages/NotFound.tsx`) |

**Dead/unrouted pages** — present on disk but never imported by `App.tsx` and
unreachable by any route:

- `src/pages/FlowDesigner.tsx` — **10,543 lines**, the entire legacy
  "stream-based" flow editor (streams, sources, pagination-as-a-stage, its
  own OpenAPI/service pickers).
- `src/pages/ApplicationServices.tsx` and `src/pages/NifiServices.tsx` —
  legacy service-management pages superseded by `AppServices.tsx`.
- `src/pages/Settings.tsx` — a thin duplicate of `Connections.tsx` (renders
  `PlatformConnectionsPanel`), also unrouted (the `/settings` route redirects
  to `/connections` instead of rendering this component).

`README.md:40-42` confirms this explicitly: *"Legacy pages from the
stream-based app (`FlowDesigner.tsx`, `NifiServices.tsx`, `Settings.tsx`,
`ApplicationServices.tsx`) remain on disk unrouted, for reference and
diffing."* Their exclusive supporting libraries are equally dead:
`src/lib/flowApi.ts`, `src/lib/streamGraph.ts`, `src/lib/streamFlowMap.ts`,
`src/lib/nifiServicesApi.ts`, `src/lib/openapiApi.ts`,
`src/lib/applicationServicesApi.ts`, `src/lib/serviceManagerOptions.ts`,
`src/lib/flowDesignerConnectionState.ts`,
`src/lib/flowDesignerSchemaRequirement.ts`, `src/lib/icebergSinksApi.ts`
(entirely unimported — orphaned even from the dead pages), plus
`src/components/flow-map/StreamFlowMap.tsx` / `StreamFlowNode.tsx`. A large
fraction of the test suite exercises this dead code (see §11). None of it
should be treated as current UX truth; the live flow editor is
`FlowBuilder.tsx` + `src/components/flow-builder/*`.

Also dead: `src/lib/mockData.ts` (169 lines) — not imported anywhere; the
real mock data layer is `src/prototype/*`.

### 1.2 Layout

`src/components/AppLayout.tsx` wraps every routed page: a `SidebarProvider`
+ `AppSidebar` (left nav) + a `<main>` column with a title/description/
actions header row (`title`, `description`, `actions` props) and the page
body below, capped at `max-w-[1400px]`.

`src/components/AppSidebar.tsx` renders two nav groups:
- **Workspace**: Dashboard, Flows, Schemas, Application Services, Audit Log.
- **System**: Platform Connections, APISIX Gateway.

The sidebar footer also owns:
- A **migration notice** banner (when a stale seed blob was archived and
  reseeded, `consumeMigrationNotice()` shown once — `AppSidebar.tsx:54-76`).
- A **"Reset demo data"** button (`AppSidebar.tsx:166-184`) that calls
  `resetDemoData()` and reloads the page — this is the one global "start
  over" affordance.
- A static "admin / Platform Admin" identity block (not a real auth
  session — there is no login).

### 1.3 State management approach

- **Server-state cache**: `@tanstack/react-query` (`QueryClient` created once
  in `App.tsx:16`, `QueryClientProvider` wraps the whole app). Every page
  fetches through `useQuery`/`useMutation` against the functions in
  `src/prototype/api.ts`, and invalidates specific `queryKey`s
  (`["flows"]`, `["schemas"]`, `["connections"]`, `["services"]`,
  `["gateway-proxies"]`, `["gateway"]`, `["audit"]`, `["dashboard"]`, etc.)
  after a mutation. There is no global client-side store (no Redux/Zustand);
  react-query's cache *is* the client-side state layer, always sourced from
  the mock API.
- **Local component state**: plain `useState` for form drafts, dialog open
  state, per-block accordion open-section state, etc. (e.g. `FlowBuilder.tsx`
  holds an entire draft `Flow` in `useState` and only persists it to the
  store on explicit **Save**).
- **Toasts**: `sonner` (`<Sonner position="bottom-left">` in `App.tsx:22`)
  plus the shadcn `<Toaster />` — both mounted; `sonner`'s `toast.*` is what
  every mutation success/error path actually calls.
- **Routing-driven state**: `FlowBuilder.tsx` reads `?ceremony=<blockId>` and
  `?prefill=<templateId>` query params to deep-link into the schema
  ceremony from the Schemas page.

### 1.4 The mock/prototype data layer (`src/prototype/`)

This is the actual "backend" of the prototype:

- **`src/prototype/types.ts`** (563 lines) — the entire domain model
  (`Flow`, `FlowBlock`, `FlowTopic`, `PlatformConnection`, `GatewayProxy`,
  `AppService`, `ApprovedSchema`, `SchemaTemplate`, runtime types, etc.).
  Full field-level breakdown of the flow model is in §9.7.
- **`src/prototype/seeds.ts`** (2072 lines) — builds the seed dataset
  (`buildSeedState()`), stamped with `SEED_VERSION = 4`. Seeded with
  realistic security-integration data: Rapid7, FortiSIEM, ServiceNow,
  APISIX, Redis, Iceberg/OpenSearch — flows in Running/Paused/Stopped/
  Draft-with-issues/Degraded-drift states, healthy & failed connections,
  retired services, all three schema provenances. Also defines
  `CONNECT_PLUGIN_CATALOG` (the 4 known Kafka Connect sink plugins).
- **`src/prototype/store.ts`** (154 lines) — a localStorage-backed
  single-document repository (`STORAGE_KEY = "dmp-adapter-prototype-v1"`).
  `getState()` / `mutate(fn)` are the only read/write primitives; a
  seed-version mismatch or a structurally-incomplete blob triggers an
  automatic reseed, archiving the old blob under
  `dmp-adapter-prototype-backup-v<N>` and setting a one-time
  `migrationNotice` (surfaced by `AppSidebar`, §1.2).
- **`src/prototype/api.ts`** (1589 lines) — "Typed mock service layer.
  Simulates the eventual backend: latency, deterministic test results,
  lifecycle transitions, audit trail. All state lives in the localStorage
  store; nothing touches the network." (file header, lines 1-3). Every
  exported function is `async`, awaits an artificial `sleep(ms)`, and
  reads/writes through `store.ts`. This is the file every page's
  react-query hooks call into (`listFlows`, `getFlow`, `saveFlow`,
  `runFlowVerb`, `listConnections`, `testConnection`, `listServices`,
  `saveService`, `listGatewayProxies`, `reconcileGatewayProxy`,
  `approveSchema`, `getDashboardSummary`, `listAudit`, etc.).
- **`src/prototype/mutations.ts`** (431 lines) — the single home for
  *structural* flow edits (add/reparent/delete a block, set a branch).
  Pure functions: `(Flow, ...) → Flow | {ok:false, reason}`. Owns the edit
  lock, the R1–R8 legality rules and the cycle guard so the map, the form
  and any other surface cannot drift apart (file header, lines 1-14).
  Exports include `addBlock`, `setBranch`, `deleteBlockCascade`,
  `reparentBlock`, `previewReparentRenames`.
- **`src/prototype/legality.ts`** (333 lines) — the placement-rule engine
  (rules R1–R8): `computeAddMenu`, `computeRootMenu`, `computeTopicMenu`,
  `canReparent`, `isTerminal`, `rootBlock`, `flowHasTrigger`. Every "+ add"
  menu in the UI is a rendering of these functions (§9.5).
- **`src/prototype/branches.ts`** (124 lines) — the branch/routing
  vocabulary (`branchesOf`, `rulesOf`, `matchOf`, `isConditional`,
  `describeBranch`, `BRANCH_OPS`).
- **`src/prototype/naming.ts`** (174 lines) — the derived-name "naming
  walk" (`deriveTopicName`, `tableName`, `dlqName`, `tokenize`,
  `topicNameCollision`, cron helpers).
- **`src/prototype/validation.ts`** (344 lines) — `validateBlock`,
  `validateFlow`, `deployPreflight` (§9.6, §9.9).
- **`src/prototype/inference.ts`** (972 lines) — Avro schema inference from
  sample records/files (`inferAvroFromRecords`, `parseSampleFile`,
  `resolveRecordPath`, `validateRecordsAgainstAvro`) — powers the schema
  ceremony's "uploaded" and "sample_run" evidence paths (§9.8, §7).
- **`src/prototype/migrate.ts`** — one-time in-place shape migrations that
  must not cost the user their saved data (kept separate from the
  version-bump reseed path).

### 1.5 Which pages use which data

Every routed page consumes `src/prototype/api.ts` via react-query; there is
no page-specific store. Rough map of primary query keys per page:

| Page | Primary react-query keys / api.ts calls |
|---|---|
| Dashboard | `dashboard`, `flows`, `audit`, `services` (`getDashboardSummary`, `listFlows`, `listAudit`, `listServices`) |
| Flows | `flows`, `metrics`, `dlq`, `runtimes` (flow list + side panel data) |
| FlowBuilder | `flow` (single), `services`, `schemas`; mutates via `saveFlow`, `runFlowVerb`, `mutations.ts` |
| Schemas | `schemas` (approved), `schemaTemplates`, `flows` |
| AppServices | `services` |
| Connections | `connections` |
| Apisix | `gateway-proxies`, `gateway`, `connections`, `flows` |
| Audit | `audit` (polls every 15s via `refetchInterval`) |

`docs/BACKEND_API_ENDPOINTS.md` is **stale relative to the current app** —
see §2.

---

## 2. Expected API contract (`docs/BACKEND_API_ENDPOINTS.md`)

The document is titled "NIF Abstractor Backend API Endpoints (Simple List)"
and describes a **different, older shape of the app** than what
`src/pages/*` currently implements — it predates the adapter-model rework
(it still talks about "Flow Designer + Flow Runner", "Schema Manager" with
versioned artifacts, and drafts endpoints; none of these concepts exist in
the current routed pages). Every endpoint, by section:

- **Dashboard**: `GET /dashboard/summary` (KPI cards — sources, running
  flows, verified schemas, failed connections, recent runs), `GET
  /dashboard/flow-status`, `GET /dashboard/recent-activity`.
- **Connections (Service-Level Only)**: `GET/POST /connections`, `GET
  /connections/{connectionId}`, `PATCH /connections/{connectionId}`, `POST
  /connections/{connectionId}/test`. Doc note: "SMB is not a service-level
  connection endpoint here; SMB belongs to source configuration."
- **Schema Manager**: `GET/POST /schema-artifacts`, `GET
  /schema-artifacts/{artifactId}`, `GET .../versions/{version}`, `PATCH
  .../versions/{version}` (save edits, "fork if editing a verified
  version"), `POST .../versions/{version}/generate` (needs verification),
  `POST .../versions/{version}/verify`, `GET .../versions/{version}/linked-
  flows`, `GET /schema-links`, `GET/PUT/DELETE /drafts/schema-manager`.
- **Flows (Flow Designer + Flow Runner)**: `GET/POST /flows`, `GET/PUT/
  PATCH/DELETE /flows/{flowId}`, `POST /flows/{flowId}/deploy|start|stop`,
  `GET /flows/{flowId}/metrics|bulletins|runs`, `GET/PUT/DELETE
  /drafts/flow-designer`.
- **Audit**: `GET /audit`, `GET /audit/export`.
- **Settings**: `GET/PUT /settings`.
- **System**: `GET /health`, `GET /meta/enums`.

### Gaps vs. the actual current app

The doc has **no endpoints at all** for: Application Services (auth modes,
sink destinations), the APISIX Gateway page (proxies, certificate profiles,
host allowlist), Platform Connections' `activate`/`repoint`/`Test All`
flows, the schema **ceremony** (Declare/Orchestrate/Review/Approve, file
upload, live-sample inference), flow verbs beyond deploy/start/stop
(`pause`/`resume`/`stop_clear`/`redeploy`/`undeploy`), DLQ records, block-
level Test, or runtime/drift (`FlowRuntime`, `NifiComponent`,
`ConnectConnectorRuntime`, `DriftFinding`). Conversely it references
concepts the current app does not have (`schema-artifacts` with numbered
versions and separate generate/verify steps, `/drafts/*` save-and-resume,
`bulletins`/`runs` sub-resources). Treat this doc as **background only** —
it is not an accurate contract for `src/prototype/api.ts`'s current surface.

---

## 3. Dashboard (`src/pages/Dashboard.tsx`)

Layout: a row of 4 KPI stat cards, then a 2-column grid below (`Flow
Status` list spanning 2/3 width; `Recent Activity` + `Needs attention`
stacked in the remaining 1/3).

### KPI stat cards (`Dashboard.tsx:125-163`, rendered `168-197`)

1. **Flows** — `summary.totalFlows`, `Workflow` icon.
2. **Running Flows** — `summary.runningFlows`, `Activity` icon.
3. **Approved Schemas** — `summary.approvedSchemas`, `ShieldCheck` icon.
4. **Connections healthy** — `"{healthy}/{total}"`, `Plug` icon, turns
   warning-tinted when `connectionsHealthy < connectionsTotal`.
5. **Sink connectors running** — this is the "Sink Connector" card the task
   asked about. Exact rendering (`Dashboard.tsx:151-162`):
   - Value: `"{summary.sinkConnectorsRunning}/{summary.sinkConnectorsTotal}"`.
   - Icon: `PlugZap`.
   - Tone: success-tinted normally, warning-tinted
     (`text-warning bg-warning-muted`) when
     `sinkConnectorsRunning < sinkConnectorsTotal`.
   - A **hint line** underneath the number/label
     (`Dashboard.tsx:157-161`, rendered via the generic `s.hint` block at
     `193`): `"on the Connect cluster"`, or, when
     `summary.sinkConnectorsUndeployed > 0`, the longer
     `"on the Connect cluster · {N} more configured but not deployed"`.
   - Code comment justifying the metric's precision
     (`Dashboard.tsx:119-121`): *"A sink that is not RUNNING is the failure
     people care about — a paused or failed connector means data has
     stopped landing while the flow still reads as Running, because the
     connector lives on the Connect cluster, not in NiFi."*
   - This is 5 stat cards total (statDefs array), not 4 — every card shares
     the same generic tile markup (icon chip, big number, label, optional
     hint line, a small spinner while `sumLoading`).

### Flow Status panel (`Dashboard.tsx:200-241`)

- Card header: title "Flow Status" / description "Operational state of all
  flows", with a "View all →" link to `/flows`.
- Empty state: centered `Workflow` icon + "No flows yet" + a
  "Create your first flow →" link to `/flow-builder/new`.
- Otherwise: one row per flow — flow name (links to `/flows`), an
  `AdapterChip` for the flow's **root block** (via local helper
  `rootBlockOf(flow)`, `Dashboard.tsx:33-40` — same root concept as
  `legality.ts`'s `rootBlock`, computed locally here rather than imported),
  and a `StatusBadge` for `flow.state` on the right.

### Recent Activity (`Dashboard.tsx:244-272`)

Last 8 audit events (`audit.slice(0, 8)`). Each row: a colored status dot
(`statusDotClass`, `Dashboard.tsx:42-48`: destructive for
failed/error, warning for warning, success for success, info otherwise),
`action` (bold) + `target` (muted), and a `"{timeAgo} · {user}"` line.

### Needs attention (`Dashboard.tsx:274-306`)

Built by local `computeAttention(flows, services)` (`Dashboard.tsx:56-87`),
which flags a flow when any of:
- `flow.state === "Draft"` → `"Draft — has validation issues ({n})"` or
  `"Draft — not yet deployed"`.
- `flow.drift` is set → the drift string itself (explicitly **not** gated
  on state, so a Running/Stopped-but-drifted flow still shows here).
- A pinned/referenced service is retired → `"Action required — retired
  service \"{name}\""`.
- `serviceUpdateAvailable(flow, services)` is non-empty → `"Service update
  available — {names} (adopts at next deploy)"`.

Each row is a link to `/flow-builder/{flowId}` showing the flow name and a
bulleted list of its reasons. Empty state: "Nothing needs attention."

A manual **Refresh** button (`Dashboard.tsx:172-176`) invalidates all four
query keys at once — there is no polling on this page.

---

## 4. APISIX page (`src/pages/Apisix.tsx`, 1289 lines)

Routed at `/apisix`. Sections top-to-bottom: connection header, Proxies,
Certificate profiles, Host allowlist.

### 4.1 The "APISIX Gateway card" and "Manage on Platform" button

There isn't a separate small "APISIX Gateway card" nested inside the page —
the whole page *is* titled "APISIX Gateway" (`AppLayout title`,
`Apisix.tsx:414`). What matches the task's description is the
**`ConnectionHeader` component** (`Apisix.tsx:995-1088`), rendered first in
the page body (`Apisix.tsx:424`):

- If no active APISIX connection exists: a destructive-bordered card,
  `XCircle` icon, "No active APISIX connection" title, explanatory
  description, and a single button **"Go to Platform Connections"**
  (`Cable` icon) linking to `/connections` (`Apisix.tsx:1002-1027`).
- If one exists: a card with the connection name, an "Active" `StatusBadge`,
  health/reachability badges, a small `Admin API` / `Runtime` URL summary
  block, an optional warning strip if unhealthy, and the button
  **"Manage on Platform Connections"** (`Cable` icon, `Apisix.tsx:1080-1084`)
  linking to `/connections`. This is the literal "Manage on Platform…"
  button the task refers to.

Page-level "Add Proxy" button lives in the `AppLayout` `actions` slot
(`Apisix.tsx:416-420`), top-right of the page header, not inside this card.

### 4.2 Proxies section (`Apisix.tsx:426-481`, cards `1090-1288`)

- Section heading via `SectionHeading` (`Apisix.tsx:967-993`): icon,
  title "Proxies", count, and blurb "One proxy is one egress definition. An
  http block references it by id — deploy needs it reconciled and its host
  allowlisted."
- Loading: two `Skeleton` blocks. Empty: a card with `Globe` icon, "No
  proxies yet", and an "Add Proxy" button.
- Each proxy renders as a `ProxyCard` (`Apisix.tsx:1090-1288`) in a 2-column
  grid:
  - Header: `Globe` icon tile, proxy name, description (or "No
    description."), a `StatusBadge` for `proxy.status`
    (`Pending`/`Reconciled`/`Failed`).
  - Summary rows: Target (`host:port + path`), Methods, SNI, Timeouts
    (`connect {ms} · read {ms}`), Client cert (profile name or "none").
  - Conditional banners: not-allowlisted warning (with an inline **"Add to
    allowlist (admin)"** button), not-Reconciled warning
    (`statusDetail` or generic), and a green "Reconciled and allowlisted"
    confirmation when both are true.
  - Last-test result banner (ok/fail, colored, with relative time), shown
    only after a Test has been run this session.
  - A **"Used by N flows"** popover button (disabled when 0) listing
    dependent flows with links to their builder + a `StatusBadge`.
  - "Updated {time}" text.
  - Row of action buttons: **Test**, **Reconcile**, **Edit**, **Delete**
    (delete disabled — with a tooltip explaining why — when any flow
    depends on the proxy).
- **Add/Edit dialog** (`Apisix.tsx:678-820`): Name, Description, Target
  host + Port, an inline warning if the host is not on the allowlist
  (informational, does not block Save), SNI, Connect/Read timeout (ms),
  Route path prefix, Allowed methods (toggle chips: GET/POST/PUT/PATCH/
  DELETE/HEAD), Client certificate profile select. Client-side validation
  in `proxySaveBlockReason()` (`Apisix.tsx:147-170`) blocks Save with a
  specific reason (bad hostname, scheme/path/port embedded in host, bad
  port range, missing path or leading slash, no methods selected, bad
  timeouts). Submit button label: "Create Proxy" / "Save proxy".
- **Delete confirm**: `AlertDialog` listing dependent flows; delete is
  **refused** (button disabled) if any flow routes through the proxy
  (`Apisix.tsx:822-868`).

### 4.3 Certificate Profiles — current add/create interaction

Not a modal — an **inline row form at the bottom of the Certificate
profiles card** (`Apisix.tsx:537-588`): three inputs (`Name`, `Subject
(CN=…)`, an HTML `type="date"` expiry picker) plus an **"Add profile"**
button. Button is disabled until name+subject are non-empty; expiry
defaults to "+365 days" if left blank. Adding calls the shared gateway-
resources mutation and shows a success toast. Each existing profile is
listed above the form as a row (name, subject, expiry — red+"expired" text
if past, a ref-count badge, and a delete icon button that is disabled with
a tooltip when `refCount > 0`, i.e. still used by a proxy). Deleting opens
a confirm `AlertDialog` (`Apisix.tsx:870-901`).

### 4.4 Host Allow List — current add/create interaction

Also inline, at the bottom of the Host allowlist card
(`Apisix.tsx:641-671`): a single `host.example.corp`-placeholder text
input (Enter key also submits) + an **"Add host (admin)"** button
(`ShieldCheck` icon). Unlike proxies/certs, this is gated as an
**administrator action**: clicking Add (or pressing Enter) does **not**
write directly — it opens a confirmation `AlertDialog` ("Administrator
action", `ShieldAlert` icon, `Apisix.tsx:904-960`) stating the consequence
(which proxies become reconcilable / stranded) and requiring an explicit
**"Confirm as admin — add host"** click before the allowlist changes; the
mutation is audited (`gwMut` → `updateGatewayResources`, message "Admin
action recorded — ..."). Each allowlisted host is a `Badge` with a usage
count (`"· N proxies"`) and an inline `X` remove button that opens the
same admin-confirm dialog in "revoke" mode. A static warning banner above
the list reminds the user this section is administrator-managed
(`Apisix.tsx:605-612`).

---

## 5. Connections page (`src/pages/Connections.tsx`, 1154 lines)

Routed at `/connections`, titled "Platform Connections." The page body is
`PlatformConnectionsPanel` (also reused, unrouted, by the dead
`Settings.tsx`). Connections are grouped into 6 fixed type sections, in
this order (`TYPE_ORDER`, `Connections.tsx:80`): **nifi, kafka, apicurio,
kafka_connect, redis, apisix**. Each section header shows an icon, label,
and description from `TYPE_META` (`Connections.tsx:82-92`); multiple
connections of the same type are allowed, but **exactly one may be
`active`** per type (enforced by the Activate/Repoint flow).

Per-type exact form fields (from `defaultDraft()` / the dynamic
`ConnectionFormFields` component, `Connections.tsx:102-125`, `328-505`) —
every type also has a plain-text **Name** field:

- **NiFi** (`nifi`): URL (hint: "NiFi API base URL, e.g.
  https://nifi.internal:8443"); Auth mode select — **Bearer token** (one
  secret field, password-masked) or **Basic** (Username text field +
  Password secret field).
- **Kafka** (`kafka`): Bootstrap servers (comma-separated host:port); Mode
  select — **Native Kafka** or **Kafbat proxy** (the latter reveals a
  "Kafbat proxy URL" field); Security protocol select
  (PLAINTEXT/SSL/SASL_SSL/SASL_PLAINTEXT) — when it starts with `SASL`,
  reveals SASL username (text) + SASL password (secret).
- **Apicurio** (`apicurio`): URL; Auth mode select — **None** (no fields),
  **Basic** (Username + Password secret), or **Bearer token** (secret).
- **Kafka Connect** (`kafka_connect`): URL only (hint: "Kafka Connect REST
  endpoint"). No auth fields at all — `noSecretPossible` is always true for
  this type (`Connections.tsx:245-246`), so it can never carry a secret.
- **Redis** (`redis`): Host + Port (3-col grid, host spans 2), Dedup
  logical DB + Bookmarks logical DB (two number fields — validated to be
  distinct non-negative integers), Password (secret, optional), plus a
  static note "Standalone mode only. Dedup caches and jdbc bookmarks live
  in separate logical databases." So Redis exposes **5 fields, not just
  password** (host, port, 2 logical-DB numbers, password).
- **APISIX** (`apisix`): Admin URL (hint: "backend-only — never exposed to
  flows"), Runtime URL, Admin key (secret).

Validation (`validateDraft()`, `Connections.tsx:161-203`) is per-type and
distinguishes create vs. edit (secrets are only *required* on create —
"Secrets are write-only — leave secret fields blank to keep the existing
values" on edit, `SECRET_PLACEHOLDER` constant `Connections.tsx:318`).

### Per-card summary + actions

Each connection card (`Connections.tsx:858-935`) shows: icon, name, an
"Active" badge if applicable, "Last tested: {time}", health + reachability
`StatusBadge`s, a summary block (`summaryRows()`, `Connections.tsx:260-298`
— type-specific key/value pairs, e.g. URL/Auth for nifi, Bootstrap/Mode/
Security for kafka) plus a "Secret: stored (write-only)" row when
`hasSecret`. Action buttons: **Test**, **Activate** (hidden once already
active), **Edit**, **Repoint**, and — **only for `apisix`-type
connections** — a **"Gateway resources"** button linking to `/apisix`
(`Connections.tsx:911-922`); a destructive icon-only **Delete** button sits
at the far right.

- **Test All** button (`Connections.tsx:802-818`) runs every connection's
  test via `Promise.allSettled` and reports `"{healthy}/{total} healthy"`.
- **Activate**: blocked with a toast offering a **"Repoint"** action button
  if a different connection of that type is already active
  (`Connections.tsx:694-716`); Redis specifically gets an extra confirm
  dialog first (`Connections.tsx:1087-1130`) warning that dedup/bookmark
  state on the old instance will be lost.
- **Repoint** dialog (`Connections.tsx:517-629`): pick one of 3 modes —
  **Adopt** ("assume resources already exist... verify and take
  ownership"), **Migrate** ("re-create the managed resources... then
  switch dependents over"), **Reset** ("rebuild platform state from
  scratch — dependents must redeploy") — shows an "impact preview" of
  dependent deployed flows, then a step-by-step progress list
  (`RepointStep[]`, done/active/pending icons) while `repointConnection()`
  runs.
- **Delete**: confirm dialog; blocked (button disabled) if any deployed
  flow depends on the connection.
- **Add Connection** flow is two steps: a type-picker dialog
  (`Connections.tsx:944-974`, with a footnote "Iceberg moved to Sink
  destination services.") then the per-type form dialog, with a **Back**
  button returning to the type picker.

---

## 6. Application Services page(s)

### 6.1 Which page is actually routed

`src/App.tsx:30` routes `/application-services` to **`AppServices.tsx`**
(560 lines). `ApplicationServices.tsx` and `NifiServices.tsx` are **not
imported anywhere** and are dead legacy code (§1.1) — they should be
ignored for UX purposes; anything below describes `AppServices.tsx` only.

### 6.2 Service types

Four types, in fixed order (`TYPE_ORDER`, `AppServices.tsx:114`), each its
own labeled section on the page (`SERVICE_TYPE_META`,
`AppServices.tsx:84-112`):

1. **HTTP service** (`http`) — "Base URL + authentication profile used by
   http adapter blocks."
2. **Database service** (`database`) — "Host, credentials and capabilities
   used by jdbc adapter blocks."
3. **External Kafka receiver** (`external_kafka`) — "Input only — external
   clusters are never a destination."
4. **Sink destination** (`sink_destination`) — "Endpoint + credentials the
   managed Connect sinks write to — selected by kafka+connect and kc
   blocks."

### 6.3 The service form (`src/components/service-form/ServiceFormFields.tsx`, 525 lines)

Deliberately shared between this page **and** the Flow Builder's inline
"create a private service" dialog (§9) — "ONE definition, two mount
points" (file header). Fields, exactly:

**HTTP** — Base URL; Auth mode select with **6 modes**, each revealing
different fields:
- `none` — nothing.
- `basic` — Username + Password (secret).
- `bearer` — Token (secret).
- `api_key` — Key name, Key location (Header/Query parameter), Key value
  (secret).
- `oauth2` — Token URL, Client id, Client secret (secret).
- `session_token` — Login path, Token JSONPath, Token header, plus a note
  "Logins are never modeled as data streams — session bootstrap lives here
  now."
Always present regardless of auth mode: an **"API gateway egress"**
select (`ProxyField`, `ServiceFormFields.tsx:269-298`) — "No proxy — call
the host directly" or one of the APISIX proxy catalog entries, with a link
to "Manage proxies" (`/apisix`). This is where a block's egress proxy is
actually configured now (see the read-only `EgressLine` in §9).

**Database** — Dialect select (PostgreSQL / Trino / MySQL-MariaDB enabled;
Oracle / SQL Server shown but disabled "coming later"); Host + Port; 
Database name; Username; Password (secret); Capabilities checkboxes
(Read / Write, at least one required).

**External Kafka** — Bootstrap servers; Security protocol select
(SASL_SSL/SASL_PLAINTEXT/SSL/PLAINTEXT); SASL username; SASL password
(secret); static note "Input only — external clusters are never a
destination (R6)."

**Sink destination** — Kind select: **OpenSearch** (URL, Index prefix,
Write mode select Upsert/Index) or **Iceberg catalog** (Catalog URL,
Warehouse).

### 6.4 Cards + actions (`AppServices.tsx:452-560`)

Grouped by type, each a `ServiceCard`: icon, name, `"rev {n}"` badge,
"Retired" `StatusBadge` if applicable, service-type label, health badge, a
type-specific `configSummary()` block (`AppServices.tsx:116-155` — e.g. for
HTTP: Base URL + Auth [+ extra row for api_key/session_token/oauth2
sub-fields]), a "N dependent flows" popover, "Last tested" text, and
action buttons: **Test**, **Edit**, and either **Retire** (destructive,
opens confirm dialog listing dependents that will be flagged "action
required") or **Reinstate** (if already retired). Editing always creates a
new **revision** — "Save as revision {n+1}" — dialog copy explains "Linked
flows keep their pinned revision until the next deploy." Retirement is
explicitly logical/soft-delete, never a hard delete.

---

## 7. Schemas page (`src/pages/Schemas.tsx`, 1232 lines)

Master-detail layout: a left **artifact rail** (search + filter chips +
list) and a right **detail pane**, sized by `schemaWorkspaceLayout` from
`src/lib/schemaLayout.ts`.

### 7.1 Two record kinds, one list

The left rail merges two collections into one sorted list (`Artifact`
union type, `Schemas.tsx:271-273`, sorted newest-first):
- **Approved schemas** (`ApprovedSchema`) — born only from the ceremony on
  a `kafka_kc` block; read-only here; carry a registry global id and an
  `approvals[]` history.
- **Library templates** (`SchemaTemplate`) — hand-authored, unregistered,
  bound to nothing; fully editable in place.

Filter chips: **All / Approved / Templates** (kind), plus an "Evidence"
chip row for provenance (`sample_run` "live run" / `uploaded` "sample
files" / `manual` "manual") — provenance filtering narrows to Approved only
and hides templates. A search box filters by subject/entity/registry id/
flow name/block name (approved) or name/description (templates), debounced
250ms.

### 7.2 Actions — "Duplicate" and "Check Only", located exactly

Both live in the **template detail header's action row**
(`Schemas.tsx:989-1019`, only rendered when `selectedTemplate` is the
active artifact — i.e. these two actions exist **only for library
templates**, not for approved schemas):
- **"Check & register…"** (`Wand2` icon) — runs the same structural checks,
  then (if they pass) opens the ceremony-target picker dialog.
- **"Check only"** (`ShieldCheck` icon, `Schemas.tsx:1005-1007`) — runs
  `checkAvroRecord()` against the current buffer and renders a
  `CheckPanel` inline above the editor; does **not** navigate anywhere.
- **"Duplicate"** (`Copy` icon, `Schemas.tsx:1008-1015`) — calls
  `duplicateSchemaTemplate(selectedTemplate.id)`, selects the new copy on
  success, toast "Duplicated as \"{name}\"."
- **"Delete"** (`Trash2` icon) — opens a confirm dialog; deleting a
  template is always allowed (bound to nothing); text notes that any
  approval that was pre-filled from it keeps the template's name as a
  frozen history line.

Approved-schema detail header instead shows: **"Edit → new version"**
(enters an in-place edit mode, `PenLine` icon), **"Re-run ceremony"**
(navigates to the owning flow's builder with `?ceremony=<blockId>`), and
**"Save as template"** (copies the approved schema into the library,
independent of the approval).

### 7.3 The editor (`src/components/schema-editor/*`)

`AvroEditorTabs` (`AvroEditorTabs.tsx`) renders two tabs, exact labels:
**"Structured Editor"** and **"Raw Avro JSON"** — both driven by one
`useAvroBuffer()` buffer so they cannot drift (`useAvroBuffer.ts`: a raw-
JSON parse failure keeps the last valid structured record and only
surfaces an inline error banner rather than destroying the draft).

- **Structured Editor tab**: optional Record name / Namespace inputs (off
  when `showIdentity=false`, e.g. inside the ceremony where identity is
  derived), then `SchemaFieldList` (`SchemaFieldList.tsx`) — a recursive
  field list. **Add Field placement**: a single **"Add Field"** button
  (`Plus` icon) rendered **at the bottom of each field list**, after the
  existing rows, at every nesting depth (`SchemaFieldList.tsx:145-156`);
  disabled past `MAX_STRUCTURED_SCHEMA_DEPTH` with a tooltip explaining the
  depth cap. Each field row: Name input, a `SchemaTypeSelect` dropdown
  (scalar/nested/named/advanced types, depth-filtered), a Nullable switch,
  a Remove (trash) icon, and an optional Doc text input below. Object/
  array/map fields recurse into nested `SchemaFieldList`/`SchemaNodeEditor`
  blocks; enum/fixed/union/reference/"advanced" (anything beyond depth
  cap) fields render a "preserved exactly — use Raw Avro JSON to edit"
  notice instead of controls.
- **Raw Avro JSON tab**: a `Textarea` when editable, a `<pre>` block when
  `readOnly`. **This tab is genuinely editable** (not the read-only view
  the task's phrasing questioned) whenever the caller passes
  `readOnly={false}` — on a library template, and inside the ceremony's
  Review step. On an approved schema it is read-only unless "Edit → new
  version" has been clicked, matching the registered/unregistered
  behavior split below. A footer note warns that round-tripping through
  the structured editor rewrites non-null field `default` values to
  nullable-only defaults.

### 7.4 Registered vs. unregistered behavior differences

- **Approved (registered)**: read-only by default (`AvroEditorTabs
  readOnly={!editingApproval}`, `Schemas.tsx:962`) — "Read-only until you
  edit it. There is no evolution: a change is registered as a new approval
  by the ceremony, never by writing over this one." Clicking "Edit → new
  version" (disabled if the owning flow no longer exists, or a historical
  — superseded — approval is being viewed) unlocks the editor in place;
  the only way out is **"Register new version…"** (stages the edit via
  `stageCeremonyDraft()` and navigates into the ceremony, which is the
  only thing that actually registers it) or **"Discard edits"**. There is
  also an **Approval history** dropdown (`Schemas.tsx:898-923`) selecting
  among `approvals[]` entries (`v{n} · #{globalId} · {provenance-short} ·
  {relativeTime} · superseded/current`); viewing a historical entry shows
  a warning banner and disables "Edit → new version".
- **Library template (unregistered)**: name and description are editable
  inline in the header at all times (`Input`/`Textarea`, no separate edit
  mode); the editor tabs are always editable. **Save/Discard buttons**:
  the footer shows **"Discard changes"** (ghost, `RotateCcw` icon) and
  **"Save template"** (primary, `Save` icon) — both disabled unless
  `templateDirty` (name, description, or buffer changed from the saved
  version); Save is also disabled while `buffer.rawError` is set. There is
  **no separate "Save Draft" button** distinct from "Save" — templates
  themselves function as the draft/scratch tier (they are inherently
  unregistered), and "Save template" simply overwrites in place. A
  "New template" button (top of the rail) opens a small dialog (Name +
  optional Description) that seeds a legal empty Avro record via
  `createEmptyAvroTemplate()`.

### 7.5 File-upload / inference

**None on the Schemas page itself.** File upload and schema inference from
samples exist only inside the **schema ceremony** in the Flow Builder
(`CeremonyDialog.tsx` — see §9.8), which the Schemas page reaches via
"Re-run ceremony" / "Check & register…" deep links.

### 7.6 Verify / register actions

There is no separate "Verify" step anywhere in this app (that concept only
exists in `docs/BACKEND_API_ENDPOINTS.md`'s stale Schema-Manager section,
§2). "Check" / "Check only" (§7.2) run the same structural checks the
ceremony's Approve step enforces, purely as a local preview — they never
touch the registry. The **only door that registers a schema is the
ceremony's Approve step** (`CeremonyDialog.tsx`, §9.8); comment in
`Schemas.tsx:17-23` states this explicitly: "Approval IS registration...
Approve there is what registers it in the registry."

---

## 8. Flows page (`src/pages/Flows.tsx`, 2349 lines)

Header comment states the page's own framing: built against the mock/
localStorage-backed `src/prototype/api.ts`, all actions are "guard-reason-
driven," and `listFlows` polling every 15s (`Flows.tsx:1737`) is the only
polling on the page.

### 8.1 The flow list

The table (`Flows.tsx:1988-2222`, `min-w-[1250px] table-fixed`) has these
columns, in order:

| Column | Row cell | Data source |
|---|---|---|
| Select | `Flows.tsx:2028-2041` | local `selectedIds` state, not a `Flow` field |
| State | `Flows.tsx:2042-2056` | `Flow.state` via `<StatusBadge compact>`; a drift `AlertTriangle` with a tooltip when `flow.drift` is set |
| Flow Name | `Flows.tsx:2057-2076` | `Flow.name`/`description`; "Update available" badge from `serviceUpdateAvailable()`; "Action required" badge from local `retiredPinnedServices(flow, services)` (`Flows.tsx:224-228`) |
| Root | `Flows.tsx:2077-2090` | `rootBlock(flow)` (§8.3) → `AdapterChip`; a "topic" badge if `isAdoptedRooted(flow)` |
| Entities | `Flows.tsx:2091-2095` | local `flowEntities(flow)` (`Flows.tsx:148-152`, write blocks' `.entity`, deduped), summarized "a, b +2 more" by local `summarize()` (`Flows.tsx:155-159`) |
| Topics | `Flows.tsx:2096-2107` | `Flow.topics[].name`, first + "+N more" |
| Schema | `Flows.tsx:2108-2120` | local `schemaStatus(flow, schemas)` (`Flows.tsx:175-181`) — count of `kafka_kc` blocks vs. matching `ApprovedSchema`s |
| Actions | `Flows.tsx:2121-2216` | row buttons + overflow menu (§8.2) |

**Search**: one free-text `Input` (`Flows.tsx:1952-1960`), debounced 180ms,
filtering via local `flowMatchesSearch()` (`Flows.tsx:161-173`) against
name/description/state/topic names/block name+entity+adapter. **No
sortable columns exist.** **Bulk selection** is local state (a tri-state
header checkbox, `Flows.tsx:1774-1789`; per-row checkboxes,
`Flows.tsx:2029-2040`); a bulk action bar appears once ≥1 row is selected
(`Flows.tsx:1894-1947`) with Start/Stop/Deploy/Enable/Disable, Delete, and
Clear.

Notably, **none of the four purpose-built helper libraries the file
structure suggests are actually imported**: `src/lib/flowTableSummary.ts`,
`src/lib/flowBulkSelection.ts`, and `src/lib/pageSearch.ts` are all
reimplemented inline on `Flows.tsx` instead of being called (confirmed
zero references); `src/lib/defaultRouteAction.ts` is unrelated to this
page entirely (it normalizes a transform's default-route action for the
flow-builder). This is worth flagging for a rebuild: the "shared logic"
layer for the flow list is aspirational, not actually wired up.

### 8.2 Row-level actions vs. the overflow menu

**Row-level** (`Flows.tsx:2121-2216`, `e.stopPropagation()`'d so they don't
also open the detail sheet):
1. Primary verb icon button (Pause / Resume / Start depending on state) —
   real mutation via `runFlowVerb()`.
2. Stop icon button — real mutation.
3. **Edit** icon button — `navigate('/flow-builder/{id}')`; **UI-only**,
   no API call; tooltip differs ("Opens read-only — stop to edit" vs.
   "Edit flow") based on whether the flow is locked.
4. Overflow ("more") trigger — opens the dropdown below (shows a spinner
   instead if a different verb is mid-flight for this row).

**Overflow / three-dot menu** (`Flows.tsx:2164-2213`) — the complete,
exhaustive list, in order, each item wrapped in a local `GuardedMenuItem`
that disables itself and shows the block reason as a tooltip:
1. **Deploy** — real mutation (`runFlowVerb`, sets `Deploying` → `Stopped`
   after a simulated 1.6s, sets `deployedAt`, pins service revisions,
   synthesizes a `FlowRuntime`, clears drift, audits).
2. **Redeploy** — same code path as Deploy.
3. **Stop & Clear** — real mutation, audits "Flow stopped & cleared" with
   a Warning-status entry noting queued records were discarded.
4. **Undeploy** — real mutation, state → `Draft`, clears `deployedAt`/
   `drift`, removes the flow's `FlowRuntime` record, audits.
5. *(separator)*
6. **Disable** / **Enable** (label flips on `flow.enabled`) — real
   mutation via `setFlowEnabled()`; Disable is refused unless the flow is
   stopped.
7. **Save as Connector** — opens the `SaveConnectorDialog`; the menu item
   itself is UI-only (dialog open), the real `publishConnector()` mutation
   fires from that dialog's own Publish button.
8. *(separator)*
9. **Delete** (destructive styling) — opens a confirmation `AlertDialog`;
   the real delete mutation fires only on that dialog's Confirm button,
   which removes the flow and its runtime record and audits.

### 8.3 The "root" concept

`rootBlock(flow)` is defined in `src/prototype/legality.ts:224-230` (not
locally in `Flows.tsx` — the same function §9.6 already documents for the
Flow Builder side), imported at `Flows.tsx:127`: the flow's one parentless
block, or, for topic-rooted flows, the `kafka`/`read` block attached to an
adopted topic (a `kc` subscription never counts as root). Used in three
places on this page:
- Local helper `isAdoptedRooted(flow)` (`Flows.tsx:190-193`) — root plus a
  check that its parent topic is `kind === "adopted"`.
- The list table's **Root** column (`Flows.tsx:2005, 2077-2090`): an
  `AdapterChip` for the root block's adapter/mode, plus a "topic" badge
  when `isAdoptedRooted`, or an em-dash when there is no root yet.
- The detail sheet's Overview tab (`Flows.tsx:1049`): `root.id === block.id`
  marks the root row in the block-chain list, and that row's meta line
  shows the trigger — `"cron {flow.cron} (UTC)"` or `"continuous — no
  trigger"` (`Flows.tsx:1227-1231`).
- A related but separate computation, `chainRows(flow)` (`Flows.tsx:196-
  218`), builds the Overview tab's DFS block ordering with its own inline
  "parentless OR attached-to-adopted-topic" root filter rather than
  calling `rootBlock()` directly.

### 8.4 The flow-details side panel

**Opens** on clicking anywhere on a flow's row (`Flows.tsx:2026`; the
checkbox and Actions cells stop propagation so they don't also open it).
Renders as a `Sheet` wrapping `FlowDetailSheet` (`Flows.tsx:999-1466`);
closes by `Sheet onOpenChange`.

**Header** (`Flows.tsx:1066-1090`, shown regardless of tab): flow name +
`StatusBadge`, an enabled/disabled `Switch`, "last run {time}" / "deployed
{time|never}", the description paragraph, and — if `flow.drift` is set —
a drift `Alert` with an inline "Open the Runtime tab" text-button.

**Shared action-button row** (`Flows.tsx:1110-1171`, above the Tabs, so it
applies no matter which tab is open): primary verb (Start/Pause/Resume),
Stop, Deploy-or-Redeploy, Stop & Clear, Undeploy, Edit (navigate, UI-only),
Save as Connector (opens dialog) — identical real-mutation behavior to
§8.2's row/menu actions, funneled through the same `onVerb` handler.

**Tabs** (exact labels, in order): **Overview, Metrics, DLQ, Messages,
Runtime**. Switching flows resets the active tab back to Overview.

#### Overview tab — full detail (`Flows.tsx:1183-1273`)

1. **Validation issues alert** (only if any exist): warning `Alert`,
   `"{N} validation issue(s)"`, up to 6 bullets of `**{where}** —
   {message}` (from `validateFlowNow(flow)`), "…and N more" beyond that.
2. **Block chain list** (`Flows.tsx:1201-1237`): `chainRows(flow)` in DFS
   order (kc sinks excluded), each block as a bordered card indented
   `depth × 16px`:
   - Top line: `AdapterChip`, block name, a "branch: {name}" badge if
     branched, a mono entity badge if set.
   - Second line (muted): the resolved Application Service name; a
     `ServicePinChips` indicator (nothing / "retired — action required" /
     "rev X pinned · rev Y available" / muted "rev X pinned"); and, only
     on the root's row, the schedule text (cron expression or "continuous
     — no trigger").
   - Empty state: "The flow has no blocks yet."
3. **Topics card**: per topic — name (mono), "Sealed"/"Adopted"
   `StatusBadge`s as applicable, and a dashed pill per attached `kc` block
   reading `"kc · {name}"` (optionally `" → {sinkServiceName}"`). Footer:
   `DLQ {dlqName(flow.name)}`. Empty state: "No topics — the flow writes
   to no kafka-family destination yet."

No other content or buttons render inside the Overview tab body itself —
every action button lives in the shared row above the Tabs, not inside
this tab.

#### Metrics tab — kept as-is, full reference detail (`Flows.tsx:1276-1348`)

Query enabled only while this tab is active. States: loading text; an
explicit **no-fake-data** empty state ("Metrics unavailable... We never
fake zeros.") when the flow has never reported metrics; otherwise a 4-cell
stat grid (**Records (24h)**, **Errors (24h)** — red if >0, **Queued**,
**Last run** `StatusBadge`), a per-block table (Block / In / Out / Queued
from `metrics.perBlock[]`), and a "Topic message counts" card
(`metrics.topicCounts[]`).

#### DLQ tab — kept as-is, full reference detail (`Flows.tsx:1351-1402`)

Header caption states the DLQ policy inline: `"One DLQ per flow: {name} ·
3 retries then here · 7-day retention · no automated replay."`, plus a
**Download** button (disabled when empty) that triggers a real client-side
JSON file download of the mocked DLQ records. Table columns: Time
(relative), Block, Error class (code chip), Payload preview (truncated,
full text in a tooltip). Empty state: "No dead-lettered records."

#### Messages tab — briefly (`Flows.tsx:1405-1457`)

A topic `Select` (from `flow.topics`) drives a query for that topic's
messages, sorted newest-first and capped at 50. Caption: "Group-less
viewer — nothing is committed. Avro payloads are not decoded here." Each
row: offset, relative time, key (or `—`), and either the raw text value or
`"binary payload (N bytes)"`.

#### Runtime tab — briefly (`Flows.tsx:1459-1462`, sub-view `491-995`)

A large, explicitly **read-only** sub-view with a permanent "Read-only, on
purpose" banner explaining that live edits are exactly what produces
drift, plus a link back into the builder. Shows: last-read time, NiFi
connection, process-group id (or "reference cleared"), an unreachable-
runtime warning, a "Refresh from NiFi" button (real read-and-persist
mutation), a drift-findings section with a "Force repair" button behind a
confirmation dialog (real mutation — clears the dead reference, records an
orphan, never touches the live runtime), an accordion of generated NiFi
components grouped by owning block, a compiled-controller-services
accordion, a Kafka Connect connector/task list (failed-task counts,
truncated error traces), and a "Recorded orphans" ledger.

### 8.5 What is UI-only on this page

**Real mutations** (persist to the mock store, survive reload, and — for
lifecycle verbs — write an audit entry): Start/Pause/Resume/Stop/Stop &
Clear/Deploy/Redeploy/Undeploy/Delete (`runFlowVerb`); Enable/Disable
(`setFlowEnabled`); the bulk action bar (sequential real per-flow calls);
"Save as Connector" → dialog Publish (`publishConnector`, also triggers a
real JSON file download); "Import Connector" wizard's final "Import & open
builder" step (`importConnectorFlow`, creates a real new draft flow and
navigates to it); the Runtime tab's Refresh/Force-repair buttons; the DLQ
tab's Download button (a genuine browser file download, not decorative).

**UI-only / no persisted mutation**: both **Edit** buttons (pure
`navigate()`, no API call); row click / tab switching / the Messages
topic selector (local component state only); search input and selection
checkboxes (never persisted); inline "Open the Runtime tab" / "open this
flow in the builder" text-links (navigation only). The single clearest
purely-decorative control on the page: the **Import Connector wizard's
"Choose file…" button does not open a real file picker at all** — it
flips a boolean and displays a hardcoded canned bundle
(`IMPORT_BUNDLE` constant, `Flows.tsx:283-293`); the code comments this
explicitly: *"UI prototype — the file picker is simulated; a canned
bundle is selected for you."* No `console.log`-only handlers exist
anywhere in the file.

### 8.6 Locally defined types worth knowing about

`BulkAction` (`"start"|"stop"|"deploy"|"enable"|"disable"|"delete"`,
distinct from the imported `FlowVerb`), `VERB_META` (per-verb button
label/success-toast/detail), `RUNTIME_LABELS`, `DRIFT_KIND_LABEL` /
`VERDICT_LABEL` / `ORPHAN_KIND_LABEL` (runtime-tab display maps), and
inline (unnamed/unexported) prop-object types for every internal
sub-component (`GuardedIconButton`, `GuardedActionButton`,
`GuardedMenuItem`, `ServicePinChips`, `DisclosureRow`, `PropertyRows`,
`RuntimeTab`, `FlowDetailSheet`, `SaveConnectorDialog`,
`ImportConnectorDialog`) are all defined locally in `Flows.tsx` rather
than in `src/prototype/types.ts`.

---

## 9. Flow Builder (`src/pages/FlowBuilder.tsx` + `src/components/flow-builder/*`)

This is the centerpiece of the prototype (per `README.md:31`) and the most
detailed section of this audit.

### 9.1 Overall layout

`FlowBuilder.tsx:406-656`. Top-to-bottom:

1. **Lifecycle bar** (`FlowBuilder.tsx:436-536`, one `Card`): a
   `StatusBadge` for `draft.state`, a **Save** button, verb buttons
   (**Deploy**, and once deployed, **Start**/**Pause or Resume**/**Stop**),
   a **More** dropdown (overflow verbs: Start/Pause/Stop when never
   deployed, Stop & Clear, Redeploy, Undeploy, Enable/Disable, and
   destructive **Delete flow**), an "Unsaved changes" pill when dirty, a
   "Deployed once / Never deployed" label, and a DLQ-name readout with an
   info popover. If `deployReason` is set (Deploy currently blocked), a
   sub-row below the card explains why in plain text (not just a disabled-
   button tooltip).
2. **Two-column grid** (`xl:grid-cols-[5fr,7fr]`,
   `FlowBuilder.tsx:544-654`) — comment explicitly frames this as "graph
   left, config right": left column is the interactive **Flow map**
   (`FlowMapView`, in a fixed `h-[420px]` band, toggleable Hide/Show but
   kept mounted to preserve pan/zoom) plus a **Destinations** panel below
   it; right column is the single dominant **form pane**, which shows
   exactly one of three things depending on `selectedId`: `BlockForm` (a
   block is selected), `TopicDetails` (a topic node is selected), or
   `FlowSettingsForm` (the default — flow-level settings). A small header
   row above the form pane has a "Flow settings" button (with an error-
   count badge) and, when a block is selected, a breadcrumb-style
   `chevron → block name`.
3. Two dialogs mounted at the page level: `PreflightDialog` (Deploy gate)
   and `CeremonyDialog` (schema ceremony, opened via block Schema section
   or the `?ceremony=` deep link).

Design comment (`FlowBuilder.tsx:1-13`) states the elevation hierarchy
explicitly: the block form is the one *dominant* surface, the outline
(block list) rail was removed entirely (map + form now cover its jobs),
and the map is a *recessed* canvas band, never a third competing panel.
**Structure is never edited on the canvas or form directly** — every
create/re-parent/delete routes through `src/prototype/mutations.ts`.

### 9.2 The graph model (`src/components/flow-builder/graph.ts`, 148 lines)

Pure derivation `buildFlowGraph(flow: Flow) → { nodes: MapNode[], edges:
MapEdge[] }`:
- `MapNode = { id, kind: "block"|"topic", block?, topic?, x, y }` — a
  simple left-to-right tree layout (`X_GAP=290, Y_GAP=104`; leaves get
  consecutive rows, parents center over their children's row range).
- `MapEdge = { id, source, target, kind, label? }`, where
  `MapEdgeKind = "flow" | "branch" | "materialize" | "subscription"`, each
  with its own stroke style (`EDGE_STYLE`, `graph.ts:38-46`): plain flow
  edges are grey, named/conditional branches are blue, a topic-
  materialization edge (derived from `topic.writerBlockId`, never a real
  parent link, never draggable) is a thin light-grey line, and a `kc`
  subscription edge is a dashed grey line. A branch edge's label is either
  its rule description (`describeBranch()`) when conditional, or just its
  name when not.
- Roots = nodes that are nobody's child; a cycle (should never legally
  happen — `canReparent` guards it) still gets a safe fallback layout
  rather than an infinite loop.
- `chainTipIds(flow)` — nodes with no outgoing edge; used to keep the "+"
  add-button pinned visible on chain tips even without hover/selection.

### 9.3 Canvas nodes and the "+" button branching options

Rendered by `src/components/flow-builder/FlowMapView.tsx` (745 lines) on
top of `@xyflow/react` (React Flow). Three node types
(`FlowMapView.tsx:305`): `blockNode`, `topicNode`, `placeholderNode`
(shown only when the flow has zero nodes — "No root block yet" + a
**"Place the root"** button).

Each block node (`BlockNode`, `FlowMapView.tsx:125-237`) shows: adapter
icon+label+mode, a branch-attention warning icon (half-written condition),
a cron-clock icon (only on the scheduling root), a tested-checkmark, an
issue-count red pill, the block name, and small `entity:` / branch-name
chips. A `NodeToolbar` (visible only when selected) offers a **Delete**
button. A **"+"** circular button (visible when selected, on a chain tip,
or on hover) opens `AddBlockMenu` — the *same* legality-filtered menu
everywhere (`src/components/flow-builder/AddBlockMenu.tsx`): entries come
from `computeAddMenu()` (after a block) or `computeRootMenu()` (no root
yet) or `computeTopicMenu()` (off a topic node), each entry either
clickable, or greyed with a `Lock` icon and its refusal reason verbatim
(and a separate "Coming later" group for shelved adapter families —
NoSQL, file-share, more JDBC dialects). Per the code comment
(`AddBlockMenu.tsx:1-12`): *"There is nothing here but adapters. Adding a
block IS creating a branch off its parent."* — there is no separate "add a
branch" affordance; every "+ add block" click *is* a new branch, and
whether it carries a condition is decided afterward in the block's own
Routing section (§9.4 "Routing conditions UI").

Gestures (file header, `FlowMapView.tsx:7-16`): click a node to select
it → its form opens; "+" on a node/canvas opens the add menu; dragging a
source handle onto empty canvas opens the same add menu at the drop point
(`onConnectEnd`); dragging a handle onto another node re-parents (validated
live via `isValidConnection`/`canReparent`, refused moves toast the exact
reason); dragging an existing edge's endpoint re-parents that edge
specifically (`onReconnect`); the node toolbar's Delete or the Delete key
opens a cascade-delete confirm dialog that **previews** every block/topic
that will be removed with it (`deletePreview()`, `FlowMapView.tsx:324-330`).
Nothing on the canvas writes to the flow directly — every gesture calls
back up to `FlowBuilder.tsx`, which applies `src/prototype/mutations.ts`.

### 9.4 "Destination section" — located exactly

Two distinct things answer to "Destination":
1. **`src/components/flow-builder/DestinationsPanel.tsx`** (57 lines) — a
   `Card` titled "Destinations", rendered directly under the flow map on
   the FlowBuilder page (`FlowBuilder.tsx:586`), listing **every topic**
   in the flow with its writer (if materialized), adopted/sealed badges,
   backlog estimate, and its attached `kc` sink subscriptions as clickable
   chips ("the dashed edges, as a list" — file header). This is the
   canvas-adjacent destinations overview.
2. Inside `BlockForm`, the block accordion is organized under three
   `GroupHeading`s (`BlockForm.tsx:442, 546, 638`): **"Connection"**
   (Identity, Adapter settings), **"Records"** (Generic transformations,
   Test), and **"Destination"** ("where the records end up, and what
   follows" — Entity & derived names, Schema, Sink configuration,
   Routing). So per-block, "Destination" is the third grouping header,
   containing the Entity/Schema/Sink/Routing sections (§9.5–9.9 below).

### 9.5 The configuration form (`src/components/flow-builder/BlockForm.tsx`, 1607 lines)

One `Accordion type="multiple"` per block, sections computed per-adapter.
Three sections are **force-open** and cannot be collapsed
(`forcedSections`, `BlockForm.tsx:323-329`): **Identity** while the block
has validation issues, **Entity & derived names** while a name warning/
collision exists, and **Schema** always (it's the ceremony's only entry
point, and the target of the `?ceremony=` deep link).

**Identity block** (`BlockForm.tsx:445-508`, title "Identity", `IdCard`
icon): validation-issue list (if any), Block name input, Branch name input
(if this block starts a branch — with a note it feeds the naming walk),
and, for every adapter *except* the sink adapters (`kc`/`kafka_kc`, which
render their service picker inside Sink configuration instead), a
**Service selection** control (`ServiceSelector`, `BlockForm.tsx:950-1073`):
a `Select` of eligible non-retired services of the matching
`ServiceType` (label is "Service", or "Cluster" for a kafka·read block,
where a "Platform cluster (default)" no-service option is also offered),
plus an inline **"+ private service"** button that opens the *same*
`ServiceFormFields` used on the Application Services page in a dialog,
creating a service flagged `private: true` without leaving the builder.

**HTTP adapter config fields** (`HttpSettings`, `BlockForm.tsx:1150-1358`):
an optional "OpenAPI operation" picker (only if the bound service has
canned operations in `OPENAPI_OPERATIONS`, a small hardcoded map keyed by
seeded service id — applying one fills method/path/record-path at once);
Method select (GET/POST for read+lookup, POST/PUT/PATCH for write, via
`METHODS_FOR_MODE`); Path text input; a Join field input (lookup mode
only); Response parsing row (Format select JSON/XML/CSV/TEXT, Record path
JSONPath input, "split into records" switch — plus a CSV-is-UTF-8 note).
An **"Advanced"** nested accordion (auto-open if any advanced field is
set) holds: Headers (`KvRows`), Query parameters (`KvRows`), and — write
mode only — a Body template textarea and a "Chain continues with (R3)"
select (Original records / Parsed response), then `PaginationFields`
(§ below) and, last, the **`EgressLine`** (§9.6).

**Adapter Settings block** — this is literally the accordion section
titled **"Adapter settings"** (`Sliders` icon, `BlockForm.tsx:510-543`),
which is where `HttpSettings`/`JdbcSettings`/`KafkaReadSettings`/
`KcSettings` all mount (the adapter-specific fields above/below are all
inside this one section):
- `JdbcSettings` (`BlockForm.tsx:1360-1442`): Table select (from a
  hardcoded mock table list), Columns (comma-separated text), and — read
  mode only — an "Incremental reads" switch revealing a Watermark column
  input and an Initial-position select (oldest / only-new), with a note
  that bookmarks live in Redis and fail rather than silently lose their
  place; write mode shows a static note about `change_type`-driven
  INSERT/UPDATE/DELETE.
- `KafkaReadSettings` (`BlockForm.tsx:1444-1521`): Topic input (only if
  not consuming an adopted topic, in which case it's read-only text
  instead), Parse-as select (JSON/CSV/XML/"Raw bytes (quarantined — R8)" —
  picking Raw shows an `Alert` explaining the R8 quarantine consequence),
  Initial position select (immutable and disabled once the flow has first
  started).
- `KcSettings` (`BlockForm.tsx:1523-1606`): Subscribed topic select
  (unsealed topics only), Entity label input, Initial position select, and
  a permanent "Save is live" `Alert` explaining this block saves
  independently of flow deploys and never blocks the chain.
- `kafka·write` and `kafka_kc` adapters show only a static explanatory
  paragraph in this section (no configurable fields beyond what's derived
  elsewhere).

**The Egress block — located exactly, will be removed**: it is the
**`EgressLine` component** (`BlockForm.tsx:1084-1148`), a small bordered
"Egress" info panel mounted as the **last item inside HTTP's "Advanced"
sub-accordion** (`BlockForm.tsx:1352`, inside the "Adapter settings"
section — *not* its own top-level accordion section). It is **read-only
by design already** — the comment (`BlockForm.tsx:1075-1083`) explains it
used to be a picker on the block but "let two blocks calling the same API
disagree about how to reach it," so the proxy choice moved onto the HTTP
**service** (`ServiceFormFields`'s `ProxyField`, §6.3) and this component
now only *displays* the consequence: which proxy (name, target host:port
+path, status badge, client-cert badge) will actually be used, a dangling-
reference error if the configured proxy id no longer exists, a warning if
the proxy isn't Reconciled yet, a legacy-compat warning if the block still
carries the pre-migration `config.proxyId`/`config.proxy` boolean, and a
link to edit the bound service. It reads `blockProxyId()` from
`src/prototype/validation.ts`.

**Generic Transform section** — accordion item titled **"Generic
transformations"** (`Fingerprint` icon, `BlockForm.tsx:548-580`), rendered
for every adapter except `kc`. Body is `TransformsEditor`
(`src/components/flow-builder/TransformsEditor.tsx`, 269 lines): an
ordered list of `TransformRule`s, an **"Add transformation"** dropdown
with 7 kinds (`extract`/`add_field`/`remove_field`/`set_from_attribute`/
`rename`/`coerce`/`dedup`), each with its own inline fields (e.g. `dedup`
gets identity-fields/excluded-fields/window-hours-with-a-Redis-caveat via
a `DedupFields` sub-component); rows have Move-up/Move-down/Remove icon
buttons; **`dedup` is pinned last** and cannot be reordered past, and only
one `dedup` rule is allowed at a time. If the block is on a raw-bytes
branch (R8 quarantine), this whole section is replaced by a `Lock`-icon
`Alert` "Quarantined (R8)" instead of the editor.

**Test block** — accordion item titled **"Test"** (`FlaskConical` icon,
`BlockForm.tsx:582-626`), shown only when `hostsTest(block)` is true (reads
+ lookups on http/jdbc, kafka·read only — never any write, never `kc`,
because "no write is test-run... a probe against a write commits real
data"). When absent, a one-line `FlaskConicalOff` explanation takes its
place (§9.8 below covers `TestPanel` behavior in detail as this doubles as
the schema-ceremony's live-sample evidence source).

**Routing conditions UI (multiple conditions)** — accordion item titled
**"Routing"** (`Shuffle` icon, only for non-terminal blocks,
`BlockForm.tsx:811-828`), body is `BranchesCard`
(`src/components/flow-builder/BranchesCard.tsx`, 253 lines). It edits, per
existing child branch, a name field, a rule-count badge ("N rules" or "all
records"), and a jump-to-child button. For a branch with ≥2 rules, a
**"Take a record when"** select toggles **`all rules match`** vs. **`any
rule matches`** (`BranchMatch`), and each rule row is: a leading `where` /
`and` / `or` label, a Field text input, an Operator select (`BRANCH_OPS`:
equals / does not equal / contains / starts with / matches regex / is
empty — the last needs no value field), a Value input (hidden for
`is_empty`), and a per-rule delete icon; below the rules, **"Add rule"**
and (if any rules exist) **"Clear rules"** buttons. A footer note explains
the independence model explicitly: branches are evaluated independently
(a record can take several at once), there is no ordering/first-match-
wins, an all-unconditional-branches case gets "X branches have no rules,
so each receives every record," an all-conditional case gets "a record
matching none of them stops at this block — a counted outcome," and a
reminder that *new* branches are created via the map's "+", not from
here. **Creating a branch is not a Routing-section action at all** — it
is always "add a block."

`PaginationFields` (`src/components/flow-builder/PaginationFields.tsx`,
245 lines, mounted inside HTTP's Advanced accordion): Pagination-type
select (No pagination / Page increment / Cursor-based / Offset-limit /
Next-URL), each revealing its own field set (page/cursor/offset params +
sizes, a shared "stop condition" concept — Empty page / Total count field
/ API "has more" flag — each of which, when it needs metadata, asks
whether that metadata comes from the response Body (JSONPath) or a
Header (name, case-sensitive)), Next-URL specifically supports Link-header
(RFC 5988)/Header/Body sourcing.

`KvRows` (`src/components/flow-builder/KvRows.tsx`, 125 lines) is the
shared key/value row editor reused by HTTP headers, HTTP query params, and
the sink-config property editor; it also renders **locked/platform-owned
rows** distinctly (dashed border, `Lock` icon, disabled inputs, an
explanatory reason line, optional trailing action button).

**Sink configuration** (`src/components/flow-builder/SinkConfigEditor.tsx`,
549 lines) mounted for `kc`/`kafka_kc` blocks: a `connector.class` picker
(4 known plugins from `CONNECT_PLUGIN_CATALOG`, or a "Custom connector
class…" free-text escape hatch — flagged with a warning if outside the
catalog, but never blocked, since the cluster may have plugins the UI
doesn't know about), an **"Upload .json"** button that reads a Kafka
Connect properties file (or REST envelope `{name, config:{...}}`) from
disk via `FileReader`, applies scalar properties, silently drops non-
scalar ones (reported), and ignores/reports platform-owned keys; a
`KvRows`-based property editor with a fixed set of **locked rows**
(`topics`, converters, and, for lakehouse sinks, `iceberg.tables` /
`iceberg.tables.auto-create-enabled` / `iceberg.tables.evolve-schema-
enabled`) computed live and never persisted; and, above it, the
destination-service picker (shared with Identity, mounted here instead
for sink adapters) plus derived connection-property rows read straight
off that service (e.g. `iceberg.catalog.uri`/`iceberg.catalog.warehouse`,
or `connection.url`/`index.prefix`).

### 9.6 Validation rules already enforced

`src/prototype/legality.ts` is the placement-rule engine (rules R1–R8),
consumed by every "+" menu:
- **`isTerminal(block)`** (`legality.ts:64-66`): true for `kafka_kc` and
  `kc` — "Terminal blocks (R3/R5): the chain never continues after these."
- **`computeAddMenu(flow, afterBlockId)`** (`legality.ts:105-144`): returns
  `[]` (no menu entries at all) when the parent block `isTerminal()` — the
  concrete mechanism preventing any node from being added after a
  Kafka+Kafka Connect (`kafka_kc`) governed write or a `kc` sink
  subscription (`legality.ts:108`). `FlowMapView.tsx:542-549` surfaces the
  same rule as a toast if a drag-to-connect gesture is attempted from a
  terminal block's handle: `"{name}" is terminal (R3/R5) — the chain
  never continues after it."`
- **`canReparent(flow, blockId, newParentId)`** (`legality.ts:280-333`)
  re-checks the same terminal rule (plus root immutability, self-parenting,
  cycle detection via `subtreeIds`, topic-attach rules for `kc`, and R8
  raw-byte quarantine propagation) whenever a block is dragged to a new
  parent — refusal strings are shown verbatim in a toast.
- **R8 (raw-byte quarantine)**: `isRawBranch()` walks upward from a block
  for a `kafka·read` ancestor parsing `raw` bytes; if found, the add menu
  refuses every entry except `kafka·write` (byte-preserving), specifically
  refusing `kafka_kc` ("a governed Avro write cannot follow a raw kafka
  read"); transforms are also disabled on a raw branch (`hostsTransforms()`).
- **R2**: `kafka_kc` can never be the flow's root (`computeRootMenu()`
  ships it pre-disabled). **R5**: only a `kafka·read` or a `kc` sink
  subscription may attach to a topic node (`computeTopicMenu()`); a sealed
  topic offers nothing attachable. **R1**: only `http`/`jdbc` roots get a
  cron trigger; kafka reads are continuous, never scheduled. **R7**: a
  custom topic-name override is legal on the whole kafka family including
  `kafka_kc`.
- Deploy-time checks live separately in `src/prototype/validation.ts`
  (`validateBlock`, `validateFlow`, `deployPreflight` — §9.9) — e.g. no
  write without an entity ever, schema-ceremony-required for `kafka_kc`,
  gateway-proxy reconciliation/allowlist refusals, dangling service/
  proxy references, retired-service refusals, dedup-must-be-last.

### 9.7 The flow model in state — type definitions (`src/prototype/types.ts`)

The whole domain model lives in this one file (563 lines); the flow/block
subset (most relevant to the builder) is:

```
type AdapterId = "http" | "jdbc" | "kafka" | "kafka_kc" | "kc";
type BlockMode = "read" | "write" | "lookup";
type FlowState = "Draft" | "Deploying" | "Running" | "Paused" | "Stopped"
                | "Degraded" | "Error";
type TransformKind = "extract" | "add_field" | "remove_field"
                    | "set_from_attribute" | "rename" | "coerce" | "dedup";
type BranchOp = "equals" | "not_equals" | "contains" | "starts_with"
               | "regex" | "is_empty";
type BranchMatch = "all" | "any";

interface BranchCondition { field: string; op: BranchOp; value: string; }

interface TransformRule {
  id: string;
  kind: TransformKind;
  config: Record<string, unknown>;   // shape depends on `kind`, see below
}

interface BlockTestResult {
  ok: boolean;
  reason?: string;
  records?: unknown[];
  detectedFields?: string[];
  testedAt: string;
}

interface BranchInfo {
  name: string;               // branch-1 / user label
  rules?: BranchCondition[];  // empty/absent = every record takes this branch
  match?: BranchMatch;        // absent reads as "all"
}

interface FlowBlock {
  id: string;
  adapter: AdapterId;
  mode?: BlockMode;                 // undefined for kafka_kc/kc (always sinks)
  name: string;
  parentId: string | null;          // block id, topic node id, or null (root)
  branch?: BranchInfo;
  serviceId?: string | null;        // Application Service reference
  entity?: string | null;           // required on every write, kc included
  config: Record<string, unknown>;  // per-adapter loosely-typed payload
  transforms: TransformRule[];
  topicOverride?: string | null;    // kafka-family writes only (R7)
  testResult?: BlockTestResult | null;
}

interface FlowTopic {
  id: string;
  kind: "adopted" | "materialized";
  name: string;
  sealed: boolean;                 // kafka_kc-owned topics — never attachable
  writerBlockId?: string;          // materialized topics
  backlogEstimate?: number;
}

interface FlowVariable { name: string; value: string; secret: boolean; }

interface Flow {
  id: string;
  name: string;                    // the "source name" — first half of every derived name
  description?: string;
  state: FlowState;
  enabled: boolean;
  cron: string | null;             // 5-field UTC; null = no trigger
  blocks: FlowBlock[];
  topics: FlowTopic[];
  variables: FlowVariable[];
  servicePins: Record<string, number>;  // serviceId -> revision pinned at last deploy
  drift?: string | null;
  deployedAt?: string | null;
  lastRunAt?: string | null;
  createdAt: string;
  updatedAt: string;
}
```

`config`'s per-adapter conventions (documented in a code comment,
`types.ts:88-105`, "grep, never trust tsc, when renaming one" since it's
untyped `Record<string, unknown>`):
- `http`: `method, path, responseFormat, recordPath, split, pagination,
  proxyId? (legacy — see §9.6 EgressLine), lookupJoinField`.
- `jdbc`: `table, columns, incremental, watermarkColumn, initialPosition`.
- `kafka`: `topicName, parseFormat, initialPosition`.
- `kafka_kc`: `sinkServiceId, sinkConfig?: Record<string,string>`.
- `kc`: `attachTopicId, initialPosition, sinkConfig?: Record<string,string>`.

`TransformRule.config` shapes (comment, `types.ts:46-51`): `extract:
{attribute, path, default?}` · `add_field: {field, value}` ·
`remove_field: {field}` · `set_from_attribute: {field, attribute}` ·
`rename: {from, to}` · `coerce: {field, type}` · `dedup: {identityFields[],
excludedFields[], windowHours}`.

Structural edits are centralized in `src/prototype/mutations.ts`
(`MutationResult = {ok:true, flow, selectId?} | {ok:false, reason}`) —
`addBlock`, `setBranch`, `deleteBlockCascade`, `reparentBlock`,
`previewReparentRenames` — all pure `Flow → Flow` transforms so every
surface (map, form) stays in sync by construction rather than discipline.

### 9.8 Schema inference / file-upload behavior in the builder

Lives entirely in the **schema ceremony**
(`src/components/flow-builder/CeremonyDialog.tsx`, 982 lines), opened from
a `kafka_kc` block's Schema section (§9.5) or the Schemas page. Four
labeled steps (`STEPS` constant, `CeremonyDialog.tsx:71`): **Declare →
Orchestrate → Review → Approve**.

- **Declare** (step 0): Entity name input; **Evidence path** radio group
  with three real options: **Live sample run** (`FlaskConical` icon —
  "Runs the real upstream chain into a throwaway `-schema-inference` topic
  (~10 messages)"), **Uploaded sample files** (`Upload` icon — "JSON /
  NDJSON / CSV files you provide, with a record path"), **Author by hand**
  (`PenLine` icon — flagged forever "manually authored — not
  sample-validated"); an optional "Pre-fill the Review step" select
  listing existing approved schemas and library templates.
- **Orchestrate** (step 1):
  - *Live sample run*: a simulated progress bar (`setInterval`, no real
    network call) over the **parent block's already-stored `testResult`
    records** — i.e. it reuses whatever the parent's Test block (§9.5)
    last captured, inferring from *all* of them, not just the first; if
    the parent has no probe recorded, Review falls back to one placeholder
    field.
  - *Uploaded sample files*: a real **file upload** — hidden `<input
    type=file multiple accept=".json,.ndjson,.jsonl,.csv,.txt,...">`
    triggered by an **"Add sample files"** button; each file is read
    client-side via `FileReader.readAsText` (never sent anywhere — max 2 MB
    per file, refused with a note above that, not truncated) and parsed by
    `parseSampleFile()` from `src/prototype/inference.ts` (JSON/NDJSON/CSV
    supported; XML/XLSX explicitly refused with a note to export as CSV/
    JSON first). A **Record path** input (same JSONPath-with-`[*]` syntax
    as the HTTP adapter's response record path) resolves records across
    all uploaded files at once, with clickable **suggested paths**
    (`suggestRecordPaths()`) offered as badges; a live "N record(s)
    matched across M file(s)" readout; an **"Infer schema & continue"**
    button (disabled until ≥1 record matches) runs
    `inferAvroFromRecords()` and jumps straight to Review.
  - *Manual*: no evidence collected at all — just an explanatory note and
    a "Continue to Review" button.
- **Review** (step 2): the same `AvroEditorTabs` structured/raw editor as
  the Schemas page (`showIdentity={false}` — name/namespace are always
  derived from entity+topic here); an **inference report** panel showing
  field count, record count, and up to 6 inference notes (type widening,
  nullability decisions, etc., truncated with "…and N more"); if evidence
  was retained (uploaded or live-sample path), a live **re-validation**
  banner (`validateRecordsAgainstAvro()`) checks the current edited record
  against every retained sample and **blocks "Continue to Approve" if it
  no longer fits** — "Approve stays locked until every retained sample
  record fits the schema again."
- **Approve** (step 3): a read-only summary table (Subject, Record,
  Entity, Top-level fields, Evidence description, sample re-validation
  result, pre-fill source if any) and one final button, **"Approve &
  register"** — calls `approveSchema()`, which is the sole path that
  actually registers a schema (toast: "Approved & registered — {subject}
  (global id {n})"). If registration fails, the whole approval fails with
  it (explicit copy in the UI).

### 9.9 Deploy validation (`src/prototype/validation.ts`)

`validateBlock`/`validateFlow` (consumed live by `FlowBuilder.tsx` to
badge sections/nodes and populate the Flow-settings Validation card) check,
per block: name required; service required + not-retired for adapters that
need one; entity required for every write (including `kc`); HTTP path
required + unresolved `${…}` placeholders flagged + APISIX gateway
refusals (`gatewayRefusals()` — unreconciled proxy or non-allowlisted host,
worded exactly as shown on the Apisix page); JDBC table required; Kafka
read needs a topic; kafka-family topic-name collisions; `kafka_kc` needs
an approved schema **and** a sink destination service; `kc` needs an
attached topic; sink-config sanity (`connector.class` must look like a
class name, platform-owned keys must not be persisted); incomplete branch
rules; dedup-must-be-last; R8 transform quarantine. Flow-level: name
required, at least one block/topic, cron required+valid when the flow has
a trigger, a legal root must exist, and data must go somewhere (at least
one write/sink).

`deployPreflight()` (`FlowBuilder.tsx`'s `PreflightDialog`, §9 layout) adds
deploy-specific infra checks on top: "Configuration valid" (rolls up
`validateFlow`), one row per required-and-active platform connection type
(NiFi/Kafka/Apicurio always; +Kafka Connect if any kc/kafka_kc block;
+Redis if any dedup transform or jdbc-incremental block; +APISIX if any
proxied block), a reconciled/allowlisted pair per referenced gateway
proxy, one "Schema approved" row per `kafka_kc` block, "Bound services
reachable" (fails if any bound service's health is `Failed`), and "No
retired services" (fails if any pinned service is retired). Deploy is
disabled until every row is `ok`.

---

## 10. What is UI-only

The prototype's own framing (`README.md:11-12`, `src/prototype/api.ts:1-3`)
is that the **entire application is UI-only** by design — there is no real
backend at all; every "real" action is simulated:

- `src/prototype/api.ts` is an artificial async layer: every exported
  function `await sleep(ms)`s and then reads/writes the localStorage-backed
  store (`src/prototype/store.ts`). Deploy, Start/Stop/Pause/Resume,
  Redeploy, Undeploy, Test (block/connection/service/proxy), Reconcile,
  Repoint, Activate, and schema Approve **all** follow this pattern —
  none of them contact a real NiFi/Kafka/Apicurio/Kafka-Connect/APISIX
  instance. Success/failure outcomes are largely deterministic based on
  seeded/mock state (e.g. connection health, service health) rather than
  random, so behavior is repeatable, but it is still entirely simulated.
- No literal `console.log`-only stubs or unimplemented handlers were found
  anywhere in `src/` (grepped for `console.log`/`TODO`/"not implemented"/
  "no-op") — the prototype's own design principle (visible in
  `feedback.txt`, an older feedback document, and honored by the current
  code) is that every visible action must at least mutate mock state and
  surface a toast/dialog, never silently do nothing. This is itself a
  finding worth carrying forward: there is no cheap way to distinguish
  "wired to something real" from "wired to the mock" by looking for dead
  buttons — **the entire mock layer needs to be swapped for real API calls
  wholesale**, not patched button-by-button.
- Genuinely real, non-simulated browser behavior (will keep working as-is
  against a real backend): the Audit page's **Export CSV** button
  (`Audit.tsx:34-56`) builds a real `Blob` and triggers a real file
  download client-side; file reads in the schema ceremony
  (`CeremonyDialog.tsx`) and the sink-config JSON import
  (`SinkConfigEditor.tsx`) use the real `FileReader` API and never leave
  the browser; the **Reset demo data** button
  (`AppSidebar.tsx:78-82`) really does wipe and reseed `localStorage`.
- The one place a comment explicitly frames itself as **not** a dry run:
  `TestPanel.tsx` warns that a mutating (POST/PUT/PATCH) block Test
  "sends a real {method}" *in the sense that the mock layer will actually
  run its simulated side effects and record a test result* — still not a
  real network call, but the UI treats it with the same double-confirm
  weight a real one would need, which is useful signal for how the real
  version should behave.

---

## 11. Build / test setup

`package.json` scripts:
```
"start": "vite --port 3000 --host 0.0.0.0"
"dev": "vite"
"build": "vite build"
"build:dev": "vite build --mode development"
"lint": "eslint ."
"preview": "vite preview"
"test": "vitest run"
"test:watch": "vitest"
```

Stack: React 18.3, Vite 5.4 (`@vitejs/plugin-react-swc`), TypeScript 5.8,
Tailwind 3.4 + shadcn/radix UI primitives (`src/components/ui/*`),
`@tanstack/react-query` 5.8, `react-router-dom` 6.30, `@xyflow/react` 12
(the flow canvas), `zod` (present but forms mostly hand-roll validation —
`react-hook-form`/`@hookform/resolvers` are dependencies but not obviously
used in the pages read), `sonner` for toasts, `d3-hierarchy` (declared,
purpose not confirmed in this audit), `recharts` (declared; no chart usage
observed on the audited pages — likely legacy/`FlowDesigner.tsx`-only).

Test runner: **Vitest** (`vitest.config.ts`), `environment: "jsdom"`,
`globals: true`, setup file `src/test/setup.ts` (jest-dom matchers,
`matchMedia` and `ResizeObserver` stubs for jsdom), include pattern
`src/**/*.{test,spec}.{ts,tsx}`, `@` alias resolved to `src/`.

27 test files exist. A significant fraction test the **dead/unrouted**
legacy stream-based code (§1.1) rather than the live adapter app:
`flowApiExport.test.ts`, `flowDesignerConnectionState.test.ts`,
`flowDesignerSchemaRequirement.test.ts`, `flowImportCredentials.test.ts`,
`flowImportFinalize.test.ts`, `flowImportPreview.test.ts`,
`serviceManagerOptions.test.ts`, `streamFlowMap.test.ts`,
`streamGraph.test.ts`, and both `src/components/flow-map/*.test.tsx`
files. The tests actually covering the live prototype are concentrated in
`src/prototype/*.test.ts` (`inference`, `legality`, `migrate`, `mutations`,
`naming`) and a handful of `src/lib/*.test.ts` files
(`defaultRouteAction`, `flowBulkSelection`, `flowTableSummary`,
`inferencePreflight`, `pageSearch`, `schemaCreate`, `schemaEditor`,
`schemaLayout`, `useDebouncedValue`) plus `src/test/smbPathNormalization.test.ts`
and `src/test/example.test.ts`. There is no Playwright/e2e config wired
into `package.json` scripts despite `playwright` being a devDependency —
it appears unused by the current test/build pipeline (worth confirming
before relying on it).

`dist/` contains a pre-built production bundle (checked into the working
tree at audit time) — not relevant to source-level UX analysis.
