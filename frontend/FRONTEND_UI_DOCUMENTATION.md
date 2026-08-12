# NIF Abstractor — Frontend UI Documentation

> Handoff document for an external AI coding agent or frontend developer.
> Source of truth: the current Lovable project source code under `src/` plus the NIF Abstractor PRD context.

---

## 1. Project Overview

- **Product name:** NIF Abstractor
- **Purpose:** A web UI that lets a technical admin configure data ingestion sources (REST API, PostgreSQL, MongoDB, SMB) and **auto-generate Apache NiFi flows** that publish data into Kafka — **without ever touching the NiFi canvas**. Schemas are managed as Avro in Apicurio Schema Registry.
- **Target user:** Single admin persona ("admin") on a data-platform / SecOps engineering team. No multi-tenant or RBAC.
- **Problem the UI solves:** Building NiFi flows by hand on the NiFi canvas is slow, error-prone, and requires deep NiFi expertise. NIF Abstractor abstracts that into:
  1. A **connection registry** for shared platform backends (NiFi, Kafka, Apicurio), with SMB handled in source configuration.
  2. A **guided wizard** for source/schedule/stream/Kafka configuration.
  3. A **schema lifecycle workflow** so flows can never run with a non-verified Avro contract.
  4. A **flow runner** that exposes deploy/start/stop and basic NiFi telemetry.
- **Mock-only behavior:** Everything is mocked. No HTTP, no DB, no NiFi/Kafka/Apicurio/SMB calls. Buttons mutate **local React state** and emit `sonner` toasts.
- **Frontend-only project:** The codebase contains no backend code (no Edge Functions, no Supabase client, no Lovable Cloud). It is a pure Vite + React + Tailwind SPA.
- **Fake data:** All entities (sources, streams, schemas, flows, audit rows, metrics, bulletins) come from `src/lib/mockData.ts` or are inlined in the page components.
- **No authentication:** A static "admin" user is shown in the sidebar footer and audit log. No login screen, no session, no roles.

---

## 2. High-Level User Journey

The UI is built around this end-to-end journey:

1. **Open Dashboard** (`/`) — see overall health (sources, running flows, verified schemas, failed connections, recent runs) and recent admin activity.
2. **Configure external service connections** (`/connections`) — Kafka, Apicurio, NiFi. Test each connection.
3. **Create a source** via the Flow Designer (`/flow-designer`) — pick source type (REST / Postgres / Mongo / SMB).
4. **Define source-level flow schedule** in step 3 of the wizard (`Interval` or `Cron`).
5. **Configure one or more streams** in step 4 of the wizard (e.g. `sites`, `agents`).
6. **Mark exactly one stream as Primary** — establishes the primary output stream context used for downstream Kafka/schema configuration.
7. **Configure stream-level behavior** in step 4 — request settings, response extraction, attribute extraction, pagination, fan-out/parent input, transformations, and routing.
8. **Configure Kafka output** in step 5 and review in step 6.
9. **Select and verify schema version** in Schema Manager (`/schemas`) — flows must link to an exact schema artifact + version, and the linked version must be verified before run.
10. **Deploy & start the flow** in Flow Runner (`/flows`) — Start is eligibility-gated by linked schema version verification and flow readiness rules.
11. **Operate flow state** — enable/disable scheduled execution, stop a flow when needed, and edit existing flow configuration only when state is `Stopped`.
12. **Monitor** — open a flow's side panel for processor metrics, queued FlowFiles, throughput, NiFi bulletins, Kafka topic msg count, and run history. All admin actions are recorded in **Audit Log** (`/audit`).

The current UI represents every one of these steps visually. The **transitions between steps are not enforced** (e.g. you can land directly on `/flows` without configuring a connection first); the journey is illustrated rather than gated.

---

## 3. Application Structure

### Tech stack
- **Framework:** React 18 + TypeScript + Vite 5
- **Routing:** `react-router-dom` v6 (BrowserRouter)
- **Styling:** Tailwind CSS v3 with semantic CSS variables in `src/index.css`
- **Component library:** shadcn/ui (Radix primitives) under `src/components/ui/*`
- **Icons:** `lucide-react`
- **Notifications:** `sonner` (and shadcn `toaster`)
- **Data fetching shell:** `@tanstack/react-query` provider mounted but not currently used to fetch anything

### Folder map (current code)

```
src/
├── main.tsx                    # React root, imports App + index.css
├── App.tsx                     # QueryClientProvider, Tooltip, Toasters, Router, Routes
├── index.css                   # Tailwind layers + design tokens (HSL CSS vars)
├── App.css                     # Legacy CSS, effectively unused
├── vite-env.d.ts
│
├── components/
│   ├── AppLayout.tsx           # Page shell: sidebar + sticky header + title + actions slot
│   ├── AppSidebar.tsx          # Left navigation (workspace + system groups)
│   ├── NavLink.tsx             # Wrapper around react-router NavLink with activeClassName API
│   ├── StatusBadge.tsx         # Single source of truth for all status pills/colors/icons
│   └── ui/                     # shadcn primitives (button, card, table, dialog, sheet, ...)
│
├── pages/
│   ├── Dashboard.tsx           # /  — KPI cards, flow status, recent activity
│   ├── Connections.tsx         # /connections  — connection cards + edit dialogs
│   ├── FlowDesigner.tsx        # /flow-designer — 6-step wizard
│   ├── Schemas.tsx             # /schemas — schema list + editor + raw Avro tab
│   ├── Flows.tsx               # /flows — table + side sheet with metrics/bulletins/history
│   ├── Audit.tsx               # /audit — audit table with filter bar
│   ├── Settings.tsx            # /settings — platform defaults + safety toggles
│   └── NotFound.tsx            # 404 fallback
│
├── lib/
│   ├── mockData.ts             # All shared fake data + TypeScript types
│   ├── schemaStore.ts          # Schema artifacts/versions + flow-schema links (localStorage)
│   ├── flowStore.ts            # Flow catalog/readiness store (localStorage)
│   └── utils.ts                # `cn()` className helper
│
├── hooks/
│   ├── use-mobile.tsx          # shadcn mobile breakpoint hook
│   └── use-toast.ts            # shadcn toast hook
│
└── test/                       # vitest setup + example test
```

### File-by-file responsibility

| File | Responsibility | Type | Consumed by |
|---|---|---|---|
| `src/main.tsx` | Mounts `<App />` into `#root`. | Entry | — |
| `src/App.tsx` | Wraps app in `QueryClientProvider`, `TooltipProvider`, both toasters, `BrowserRouter`, and declares all routes. | Routing + providers | All pages |
| `index.html` | Sets `<title>`, meta description, Open Graph tags. | SEO | — |
| `src/index.css` | Defines all design tokens (HSL vars) for light + dark + sidebar; Tailwind base layer. | Styling | Every component |
| `tailwind.config.ts` | Maps semantic color names (`primary`, `success`, `warning`, `info`, `destructive`, `sidebar.*`) onto CSS vars. | Styling | Every component |
| `src/components/AppLayout.tsx` | Standard page chrome: sidebar provider, sticky topbar with global search + bell, page title, optional `actions` slot. | UI | Every page except `NotFound` |
| `src/components/AppSidebar.tsx` | Left collapsible sidebar with two groups: Workspace (Dashboard, Connections, Sources/Flow Designer, Schema Manager, Flow Runner, Audit Log) and System (Settings). Footer shows "admin". | UI | `AppLayout` |
| `src/components/NavLink.tsx` | Thin wrapper that adds `activeClassName` / `pendingClassName` props on top of react-router's render-prop API. | UI helper | `AppSidebar` |
| `src/components/StatusBadge.tsx` | Maps a status string (Healthy, Running, Verified, Draft, Failed, Schema Outdated, etc.) to a colored pill with an icon. **Single source of truth for status visuals.** | UI | Dashboard, Connections, Schemas, Flows, Audit |
| `src/lib/mockData.ts` | Exports dashboard/activity seeds and initial mock entities. | Mock data seed | Pages + stores |
| `src/lib/schemaStore.ts` | Persists schema artifacts with versions, version statuses, and flow-schema links; handles legacy key migration. | Local store | Flow Designer, Schemas, Flows |
| `src/lib/flowStore.ts` | Persists flow catalog with linked schema artifact/version and eligibility metadata; seeded from `mockData`. | Local store | Flow Designer, Flows |
| `src/lib/utils.ts` | `cn()` utility (clsx + tailwind-merge). | Util | Everywhere |
| `src/pages/Dashboard.tsx` | KPI cards, "Flow Status" list, "Recent Activity" feed. | UI + mock data | Route `/` |
| `src/pages/Connections.tsx` | Service connection cards (Kafka/Apicurio/NiFi), Add/Edit dialogs, and simulated `Test Connection`. Holds local state for the connection list. | UI + local state | Route `/connections` |
| `src/pages/FlowDesigner.tsx` | 6-step wizard with source/schedule/streams/Kafka config plus schema artifact+version selection and verification warnings; local draft save/restore. | UI + local state/store | Route `/flow-designer` |
| `src/pages/Schemas.tsx` | Schema artifact/version manager with version selector, per-version editing, verified-version forking behavior, and linked-flow visibility. | UI + local state/store | Route `/schemas` |
| `src/pages/Flows.tsx` | Flow table with eligibility-gated start/stop state mutations and side `Sheet` with metrics/bulletins/history. | UI + local state/store | Route `/flows` |
| `src/pages/Audit.tsx` | Audit table + non-functional filter bar + Export CSV button. | UI | Route `/audit` |
| `src/pages/Settings.tsx` | Platform defaults form + safety switches + Save Changes button. | UI | Route `/settings` |
| `src/pages/NotFound.tsx` | 404 page; logs to console. | UI | Route `*` |

---

## 4. Routing and Navigation

Defined in `src/App.tsx` using `BrowserRouter` and `<Routes>`. The default landing page is the **Dashboard** at `/`.

| Route / Page | URL/Path | Component / File | Purpose | Notes |
|---|---|---|---|---|
| Dashboard | `/` | `src/pages/Dashboard.tsx` | Platform overview | Default landing page; sidebar marks active with `end` prop |
| Connections | `/connections` | `src/pages/Connections.tsx` | Manage Kafka/Apicurio/NiFi service connections | Add/Edit both open `Dialog` forms |
| Sources / Flow Designer | `/flow-designer` | `src/pages/FlowDesigner.tsx` | Guided 6-step wizard | "New Source" CTA on Dashboard links here |
| Schema Manager | `/schemas` | `src/pages/Schemas.tsx` | Schema artifact/version lifecycle (`Draft` / `Needs Verification` / `Verified`) | Last wizard step deep-links here |
| Flow Runner | `/flows` | `src/pages/Flows.tsx` | Deploy, run, monitor flows with start eligibility checks | Row click opens detail `Sheet` |
| Audit Log | `/audit` | `src/pages/Audit.tsx` | Immutable history table | Filter UI is visual-only |
| Settings | `/settings` | `src/pages/Settings.tsx` | Platform defaults + safety toggles | "System" group in sidebar |
| Not Found | `*` | `src/pages/NotFound.tsx` | 404 fallback | Logs the bad path |

**Sidebar items** (defined in `AppSidebar.tsx`):
- Workspace group: Dashboard, Connections, Sources / Flow Designer, Schema Manager, Flow Runner, Audit Log
- System group: Settings
- Header: NIF Abstractor logo + "Data Ingestion Platform" subtitle
- Footer: "admin" / "Platform Admin" avatar

**Header / topbar** (`AppLayout.tsx`) is shared by every page and contains:
- `SidebarTrigger` (toggles collapsed sidebar)
- A non-functional global search input (`md:` and up only)
- A bell icon button (no behavior)

There are **no nested routes**. The only "detail" surface is the **side `Sheet`** on `/flows` triggered by clicking a row (state held locally as `openId`). No modal-driven page navigation.

---

## 5. Page-by-Page UI Documentation

### 5.1 Dashboard — `/`

#### Purpose
Single-screen overview of platform health and the most recent admin activity.

#### Main UI Elements
- **Page header** with title "Dashboard" and CTA `New Source` linking to `/flow-designer`.
- **5 KPI cards** in a 2/3/5-column responsive grid: Total Sources, Running Flows, Verified Schemas, Failed Connections, Recent Runs (24h). Each card has an icon chip in a tone-specific muted background.
- **Flow Status card** (2/3 width on desktop): list of flows with name + Kafka topic + `StatusBadge`. "View all" link → `/flows`.
- **Recent Activity card** (1/3 width): vertical feed of last admin actions with a colored dot (success / error / info), action label, target, time, and user.

#### User Actions
- Click `New Source` → navigates to `/flow-designer`.
- Click `View all` on Flow Status → navigates to `/flows`.

#### Current Behavior
- **Real:** Routing links work. Layout is responsive.
- **Mock:** All numbers and rows come from `dashboardStats`, `flowSummary`, `recentActivity` in `mockData.ts`. No data fetch.
- **Static UI-only:** KPI cards have no drill-through; activity rows are not clickable.

#### Data Displayed
- `dashboardStats` (counts), `flowSummary` (5 flows), `recentActivity` (6 entries).

#### Important Components
- `AppLayout`, `Card`, `Button`, `StatusBadge`, lucide icons.

#### Current Limitations
- KPI cards are not links/filters.
- No empty state — list is always populated.
- No loading skeleton (no async work).

---

### 5.2 Connection Manager — `/connections`

#### Purpose
Manage shared service-level connections used by generated flows: Kafka, Apicurio, and NiFi.

#### Main UI Elements
- Page header with working `Add Connection` button.
- 2-column grid of **service connection cards** (Kafka, Apicurio, NiFi). Each card shows: icon chip, name, description, current `StatusBadge` (Healthy / Not Tested / Failed), endpoint string in a muted box, last-tested timestamp, and actions: `Test Connection`, `Edit`.
- **Add dialog** (`Dialog`) — choose type (Kafka / Apicurio / NiFi), then fill type-specific fields.
- **Edit dialog** (`Dialog`) — type-specific fields based on the selected card.

#### User Actions
- Click `Test Connection` → spinner appears on the button for ~1.2s, then state mutates and a toast appears.
- Click `Edit` → opens type-specific edit dialog. `Save Changes` updates local card state and toasts success.
- Click `Add Connection` → opens a dialog; `Create Connection` appends a new local card.

#### Current Behavior
- **Real (local state only):** `Test Connection` simulates async with `setTimeout(1200ms)` and marks tested connection as healthy. Health badge and `lastTested` update in `useState`.
- **Real (local state only):** Add/Edit forms are controlled in page state and update the card endpoint/config in-memory.
- **Dynamic auth fields:** Apicurio and NiFi forms switch visible fields immediately by auth type (`None`, `Bearer Token`, `Basic`, `Client Certificate`) in both Add and Edit flows.
- **Scope decision:** SMB is intentionally removed from this page; SMB remains part of source configuration concepts elsewhere.
- **Static:** "View Details" is still not built.

#### Data Displayed
`connections[]` seed data from `src/lib/mockData.ts`, then transformed to service-only page state (SMB filtered out on this page).

#### Important Components
`AppLayout`, `Card`, `Dialog`, `Input`, `Label`, `Select`, `Button`, `StatusBadge`, local `Field` helper, local `ConnectionFormFields`, local `AuthFields`.

#### Current Limitations
- No form validation, no required fields.
- No "View Details" surface.
- Refresh resets all changes.

---

### 5.3 Source / Flow Designer — `/flow-designer`

#### Purpose
A guided wizard that supports both creating new flows and editing existing stopped flows, with source-level scheduling, stream configuration, schema-link selection, and final review.

#### Main UI Elements
- **Stepper** at the top — 6 clickable steps: `Source Type`, `Configure Source`, `Flow Schedule`, `Streams`, `Kafka Output`, `Review`. Visited steps show a check.
- **Step 1 — Source Type:** 4 selectable cards: REST API, PostgreSQL, MongoDB, SMB. Selection is highlighted.
- **Step 2 — Configure Source:** source-specific forms:
  - REST: source name, base URL, dynamic auth (`None`, `Bearer`, `Basic`, `API Key` with key location), timeout fields, rate limit.
  - PostgreSQL / MongoDB: host/port/database/credentials/SSL mode/timeout.
  - SMB: connection selector, base directory, file filter, format options, completion strategy.
- **Step 3 — Flow Schedule:** source-level schedule mode with dynamic fields:
  - `Interval`: run every + unit (`seconds` / `minutes` / `hours` / `days`)
  - `Cron`: cron expression input with example helper text
- **Step 4 — Streams:** 2-card layout:
  - Left: stream list with a "Primary" badge and a dashed footer note explaining the primary-stream rule.
  - Right: stream editor with sections for request configuration, response extraction, attribute extraction (repeatable rules), pagination (dynamic by type), fan-out/parent input, transformations, and routing rules.
- **Add Stream dialog:** opens from step 4 and creates a new stream in local state.
- **Step 5 — Kafka Output:** Kafka Topic, Partitions, Replication Factor, Key Strategy, Value Format (Avro/JSON), and schema linking mode:
  - `Use Existing Schema` → select schema artifact + version (status visible)
  - `Auto-Generate Schema` → artifact derived from source naming
- **Step 6 — Review:** Definition list summarizing source/schedule/stream/pagination/fan-out/Kafka config + schema link summary, with verification status callout:
  - unverified version → warning + `Go to Schema Verification`
  - verified version → success confirmation
- **Footer nav:** Back / Next buttons, `Save Draft` button, and final CTA `Save & Continue to Schema`.
- **Edit-mode CTA text:** final CTA changes to `Save Changes & Continue to Schema` when editing an existing flow.
- **Edit mode banner:** when opened via `/flow-designer?editFlowId=...`, page shows that an existing stopped flow is being edited.

#### User Actions
- Click any step in the stepper to jump to it.
- Select source type, configure source form fields, and switch REST auth to dynamically change visible credential inputs.
- Set source schedule mode (`Interval`/`Cron`) and edit only the active mode fields.
- Open `Add Stream`, create stream entries, and toggle the Primary Stream switch (mutually exclusive).
- Add/remove/edit extraction, transformation, and routing rules within the stream editor.
- Configure stream-level pagination and fan-out settings with dynamic form changes by selected options.
- Click `Save Draft` to persist the wizard payload in `localStorage`.
- Click `Save & Continue to Schema` on Review → navigates to `/schemas`, toasts "Flow saved as draft".
- Open Flow Designer from Flow Runner edit action (stopped-only) to edit an existing flow payload.

#### Current Behavior
- **Real (local state):** Stepper state, source config, source schedule, streams, stream editor sections, and Kafka output are all stateful and editable.
- **Real (dynamic UI):** REST auth, schedule type (interval/cron), pagination type, stop-condition branches, fan-out inputs, extraction actions, and routing destination inputs change visible fields immediately.
- **Real (local persistence):** Draft save/restore is implemented via `localStorage`.
- **Real (stopped-only editing):** Existing flow edit mode is loaded via `editFlowId` query parameter. If targeted flow is not stopped, edit is blocked and user is redirected back to Flow Runner.
- **Real (cross-page persistence):** Saving writes flow linkage/readiness and full designer payload to `flowStore` for future reopen/edit.
- **Mock-only:** No backend calls are made. Refresh restores only what was saved via draft actions.

#### Data Displayed
- `initialStreams` (sites, agents) defined inline in `FlowDesigner.tsx`.
- `sourceTypes` array defined inline.
- Default source schedule is defined inline (`interval`: every 15 minutes, plus cron example).
- Available schema artifact/version options are pulled from `schemaStore`.

#### Important Components
`AppLayout`, `Card`, `Input`, `Label`, `Select`, `Switch`, `Textarea`, `Badge`, `Button`, `cn()`, `sonner`.

#### Current Limitations
- Stream delete is not implemented yet (add/edit is implemented).
- No field-level validation or required-field enforcement.
- No backend persistence or API integration yet.
- If a flow has no saved designer payload, edit mode uses a minimal fallback hydration path from flow summary data.

---

### 5.4 Schema Manager — `/schemas`

#### Purpose
Manage schema artifacts with explicit **version lifecycle** (`Draft` / `Needs Verification` / `Verified`), edit Avro content, and gate flow execution by linked schema version verification.

#### Main UI Elements
- **Schema artifacts list (left):** clickable artifact list with latest version + latest status.
- **Create button/dialog:** manual schema artifact creation (`artifact id` + optional stream) with initial `v1 Draft`.
- **Schema detail (right):**
  - Header: artifact id, stream, active version badge, and contextual labels (`Latest`, `In Use`).
  - **Version selector:** dropdown for `vN - status` per artifact.
  - Action buttons: `Generate`, `Verify Version`, `Save Draft`.
  - **Conditional warning banner** for non-verified version: flows cannot run with that version.
  - **Linked Flows panel:** shows flows linked to the selected exact version.
  - **Tabs:** `Structured Editor` (editable table of fields: name, type, nullable, doc) and `Raw Avro JSON` (editable JSON textarea with validation errors).

#### User Actions
- Select a schema artifact in the left list.
- Select a specific version from version dropdown.
- Click `Create` and add a schema artifact manually.
- Click `Generate` to mark selected version as `Needs Verification`.
- Click `Verify Version` to mark selected version as `Verified`.
- Edit fields in Structured Editor or Raw JSON.
- Click `Save Draft` to persist schema workspace in `localStorage`.

#### Current Behavior
- **Real (local state):** Version-level state transitions, version selection, structured/raw syncing, linked-flow visibility, and draft save/restore work in frontend state.
- **Version immutability rule implemented:** Editing a `Verified` version forks a **new** `Needs Verification` version; editing `Draft`/`Needs Verification` updates the same version.
- **Mock-only:** No backend registration or schema inference API calls are made.

#### Data Displayed
- Artifact/version catalog from `schemaStore` (`nif-schema-artifacts-v2` localStorage key).
- Per-version workspace data (`avro`, `rawText`, `rawError`) in component state.
- Version-linked flows from `flow-schema-links` local storage.

#### Important Components
`AppLayout`, `Card`, `Tabs`, `Table`, `Input`, `Switch`, `Textarea`, `Select`, `Button`, `StatusBadge`, lucide icons (`Sparkles`, `ShieldCheck`, `AlertTriangle`, `Save`, `Plus`, `Trash2`).

#### Current Limitations
- No diff view between schema versions.
- Delete artifact/version actions are not implemented.
- No backend persistence or server-side validation.

---

### 5.5 Flow Runner — `/flows`

#### Purpose
List deployed flows, enforce run eligibility, control execution (Start/Stop), support stopped-only editing, and manage enable/disable scheduled execution.

#### Main UI Elements
- **Flow table** with columns: State, Flow Name, Source, Primary Stream, Kafka Topic, Schema Link (artifact + version + status), Enabled, Last Run, Records, Errors, Actions.
- **Per-row actions:** Play/Stop (with eligibility enforcement), Edit (stopped-only), Enable/Disable Schedule, Deploy (rocket icon), Delete (trash, destructive).
- **Side `Sheet`** opened by clicking a row:
  - Header: flow name + current state badge + topic + linked schema reference.
  - Quick actions: `Edit Flow` (stopped-only) and `Enable/Disable Schedule`.
  - Eligibility warning banner (when start is blocked).
  - Tabs: `Metrics`, `Bulletins`, `Run History`.
  - **Metrics tab:** 4 metric cards (Throughput, Queued FlowFiles, Records Processed, Kafka Topic Msgs) + Processor Metrics card with `Progress` bars for InvokeHTTP, SplitJson, UpdateAttribute, PublishKafkaRecord_2_6.
  - **Bulletins tab:** 3 hardcoded bulletins (WARN backpressure, ERROR 429, INFO schema cache refreshed).
  - **Run History tab:** 4-row table (Started, Duration, Records, Status).

#### User Actions
- Click a row → opens side sheet for that flow.
- Click Play button → starts only when eligibility checks pass.
- Click Stop button → mutates flow state to stopped.
- Click Edit button → navigates to Flow Designer in edit mode only when flow is stopped.
- Click Enable/Disable Schedule → toggles flow `enabled` state in local store.
- Click Deploy/Delete → no behavior (icon only).
- Click outside / close the sheet → resets `openId`.

#### Current Behavior
- **Real:** Start is blocked with explicit reasons when readiness rules fail (missing schema, missing version, unverified version, disabled flow, incomplete config). Stop works in local state.
- **Real:** Existing flow editing is restricted to stopped flows; non-stopped edit attempts are blocked with user feedback.
- **Real:** Enable/Disable schedule is persisted in local `flowStore`; disabled flows cannot be started manually.
- **Real:** Flow list is backed by local `flowStore` (`nif-flow-catalog-v1`) and schema status is resolved via `schemaStore`.
- **Mock:** Metrics, bulletins, and run history remain hardcoded constants. Deploy/Delete have no effect.

#### Data Displayed
Flow catalog from `flowStore` (seeded from `mockData.ts` + flow/schema links). Metrics and bulletins are inlined in `Flows.tsx`.

#### Important Components
`AppLayout`, `Card`, `Table`, `Sheet`, `Tabs`, `Progress`, `Button`, `StatusBadge`, local `MetricCard` helper.

#### Current Limitations
- No filtering, no sorting.
- Deploy and Delete are decorative.
- Sheet content is the same for every flow (only header values are bound to the row).
- No real-time / polling behavior.

---

### 5.6 Audit Log — `/audit`

#### Purpose
Show an immutable, filterable history of admin actions on connections, sources, schemas, and flows.

#### Main UI Elements
- Page header with `Export CSV` button.
- **Filter bar card:** search input + `Last 7 days` button + `All users` button.
- **Audit table:** Timestamp (mono), User (avatar + name), Action, Object, Target (mono), Status badge.

#### User Actions
- Type into the search input — has no effect.
- Click filter buttons / Export CSV — no behavior.

#### Current Behavior
- **Real:** Renders the full `auditLog` array.
- **Mock / static:** All filtering and export are visual stubs.

#### Data Displayed
`auditLog[]` from `mockData.ts` (10 rows spanning 2 days).

#### Important Components
`AppLayout`, `Card`, `Table`, `Input`, `Button`, `StatusBadge`.

#### Current Limitations
No pagination, no time-range picker logic, no actual filtering, no row drill-in.

---

### 5.7 Settings — `/settings`

#### Purpose
Platform-wide defaults applied to new flows + safety guardrails.

#### Main UI Elements
- **Platform card:** Default Topic Prefix, Default Schema Group, Default Partitions, Default Replication.
- **Safety card:** three switch rows — "Require schema verification before deploy" (on), "Auto-pause flow on schema drift" (on), "Send alerts on connection failure" (off).
- `Save Changes` button at the bottom.

#### User Actions
- Edit inputs and toggle switches — uncontrolled inputs / `defaultChecked`, no state.
- Click `Save Changes` — no handler.

#### Current Behavior
Pure visual mock. Nothing persists.

#### Data Displayed
All hardcoded inside `Settings.tsx`.

#### Important Components
`AppLayout`, `Card`, `Input`, `Label`, `Switch`, `Button`.

#### Current Limitations
Entire page is non-functional; `Save Changes` is a no-op.

---

## 6. Component Inventory

| Component | File Path | Used In | Purpose | Props / Inputs | Notes |
|---|---|---|---|---|---|
| `AppLayout` | `src/components/AppLayout.tsx` | Every page | Page shell: sidebar + sticky header + page title + actions slot | `{ title, description?, actions?, children }` | Centralizes layout responsiveness |
| `AppSidebar` | `src/components/AppSidebar.tsx` | `AppLayout` | Left collapsible nav | — | Two groups: Workspace + System; static "admin" footer |
| `NavLink` | `src/components/NavLink.tsx` | `AppSidebar` | Wraps react-router's NavLink to expose `activeClassName` | `{ to, end?, className?, activeClassName?, pendingClassName? }` | Used so sidebar items get an active style |
| `StatusBadge` | `src/components/StatusBadge.tsx` | Dashboard, Connections, Schemas, Flows, Audit | Maps a status string to a colored, icon'd pill | `{ status: string, className? }` | **Single source of truth for status visuals** |
| `Field` (local) | inline in `Connections.tsx` | Connection edit dialog | Label + Input + hint helper | `{ label, hint?, ...inputProps }` | Not exported |
| `MetricCard` (local) | inline in `Flows.tsx` | Flow Sheet metrics tab | Compact metric tile | `{ label, value, icon }` | Not exported |
| `Card`, `CardHeader`, `CardContent`, `CardTitle`, `CardDescription` | `src/components/ui/card.tsx` | Most pages | Card primitives | shadcn defaults | — |
| `Button` | `src/components/ui/button.tsx` | Everywhere | Variant-based button | `variant`, `size`, `asChild` | shadcn defaults |
| `Input`, `Label`, `Textarea`, `Switch`, `Select*` | `src/components/ui/*.tsx` | Forms | Form primitives | shadcn defaults | — |
| `Table*` | `src/components/ui/table.tsx` | Flows, Audit, Schemas | Table primitives | shadcn defaults | — |
| `Dialog*` | `src/components/ui/dialog.tsx` | Connections | Modal for editing connections | shadcn defaults | — |
| `Sheet*` | `src/components/ui/sheet.tsx` | Flows | Right-side drawer for flow details | shadcn defaults | — |
| `Tabs*` | `src/components/ui/tabs.tsx` | Schemas, Flows | Tab navigation | shadcn defaults | — |
| `Badge` | `src/components/ui/badge.tsx` | Flow Designer (Primary marker) | Inline labels | shadcn defaults | — |
| `Progress` | `src/components/ui/progress.tsx` | Flows (processor metrics) | Linear progress bar | shadcn defaults | — |
| `Sidebar*` | `src/components/ui/sidebar.tsx` | `AppSidebar` | shadcn collapsible sidebar | shadcn defaults | Provides `SidebarProvider`, `SidebarTrigger`, etc. |
| `Toaster` (`toast`) | `src/components/ui/toaster.tsx` | App.tsx | Radix-based toast | — | Mounted but `sonner` is what pages call |
| `Sonner` | `src/components/ui/sonner.tsx` | App.tsx | Sonner toast renderer | — | Pages call `toast.success/error` from `sonner` |

All other files in `src/components/ui/*` are unmodified shadcn primitives — available but not necessarily used yet.

---

## 7. State Management and Data Flow

### Approach
- **All state is local React `useState`** inside the page components. No Redux, Zustand, Jotai, or Context (besides the shadcn `SidebarProvider` and `TooltipProvider`).
- **`@tanstack/react-query`** is wired up in `App.tsx` but **no queries or mutations are defined** — there is no data fetching anywhere.
- **Local draft persistence:** Flow Designer and Schema Manager both implement explicit draft save/restore via `localStorage`.
- **Local cross-page stores:** `schemaStore.ts` persists schema artifacts + versions and flow-schema links; `flowStore.ts` persists flow catalog + readiness metadata.
- **Form control style:** Connections, Flow Designer, and Schema Manager use controlled inputs for core state. Audit and Settings remain primarily visual/uncontrolled.

### Where state lives

| Page | State variable | Purpose |
|---|---|---|
| Connections | `conns` | Connection list (mutated by Test) |
| Connections | `testing` | Currently-spinning connection id |
| Connections | `editing` | Object with connection id + controlled edit form state |
| Connections | `adding` | Boolean controlling Add Connection dialog visibility |
| Connections | `addForm` | Controlled Add Connection form state |
| Flow Designer | `step` | Active wizard step (0–5) |
| Flow Designer | `sourceType` | Selected source card |
| Flow Designer | `restSource` / `postgresSource` / `mongoSource` / `smbSource` | Controlled source-type form state |
| Flow Designer | `schedule` | Source-level flow schedule (`interval`/`cron`) |
| Flow Designer | `streams` | List of stream objects (controlled inputs) |
| Flow Designer | `editingStream` | Id of the stream shown in the right pane |
| Flow Designer | `editingFlow` | Flow metadata when opened in edit mode (`?editFlowId=`) |
| Flow Designer | `kafkaOutput` | Controlled Kafka output settings |
| Flow Designer | `availableSchemaArtifacts` | Artifact + version options loaded from `schemaStore` |
| Schemas | `artifacts` | Artifact/version workspace including Avro + raw + validation per version |
| Schemas | `selectedArtifactId` / `selectedVersion` | Current artifact and version selection |
| Flows | `data` | Flow list (mutated by Start/Stop) |
| Flows | `openId` | Id of flow whose Sheet is open |

### Example flow: "Test Connection"

```
User clicks "Test Connection" on a service card (e.g., NiFi)
→ handleTest("nifi") sets `testing = "nifi"` → button shows spinner
→ setTimeout(1200ms) fires
→ setConns(...) updates that connection's health to "Healthy" + lastTested = "just now"
→ StatusBadge re-renders as healthy
→ toast.success(...)  // sonner
→ NO real network call is made
```

### Example flow: "Mark stream as Primary"

```
User flips the Primary switch on stream "sites"
→ setPrimaryStream("s1") runs over `streams` and sets `isPrimary: true` only for that stream
→ React re-renders → "sites" gets the warning-tone "Primary" Badge,
   "agents" loses it
→ Review summary and stream-level sections reflect the updated primary state
```

---

## 8. Mock Data Documentation

Mock data lives in **`src/lib/mockData.ts`** (centralized) and inline in some page components (decorative metrics/bulletins/history).

### Central mock data table

| Mock Data Entity | File Path | Fields | Used By | Notes |
|---|---|---|---|---|
| `dashboardStats` | `src/lib/mockData.ts` | `totalSources`, `runningFlows`, `verifiedSchemas`, `failedConnections`, `recentRuns` | Dashboard KPI cards | 5 numbers |
| `recentActivity` | `src/lib/mockData.ts` | `id`, `time`, `user`, `action`, `target`, `status` (`success`/`info`/`error`) | Dashboard activity feed | 6 entries |
| `flowSummary` | `src/lib/mockData.ts` | `name`, `state` (`FlowState`), `topic` | Dashboard "Flow Status" | 5 entries |
| `connections` | `src/lib/mockData.ts` | `id`, `name`, `description`, `health` (`ConnHealth`), `endpoint`, `lastTested` | Connections page | Seed includes Kafka, Apicurio, NiFi, SMB; `/connections` currently filters out SMB |
| `schemas` | `src/lib/mockData.ts` | `name`, `version`, `state` (`SchemaState`), `updated`, `stream` | Schema Manager seed data | Migrated into artifact/version local store (`schemaStore`) |
| `rapid7AssetsAvro` | `src/lib/mockData.ts` | Avro record `{ type, name, namespace, fields[] }` | Schema Manager template | Used as the base template for each schema item, then edited per-subject in local state |
| `flows` | `src/lib/mockData.ts` | `id`, `name`, `state`, `source`, `primaryStream`, `topic`, `schemaVersion`, `lastRun`, `records`, `errors` | Flow Runner seed data | Migrated into flow catalog local store (`flowStore`) |
| `schemaStore` | `src/lib/schemaStore.ts` | schema artifacts, versions, per-version status, flow-schema links | Flow Designer, Schemas, Flows | LocalStorage keys: `nif-schema-artifacts-v2`, `nif-flow-schema-links-v2` |
| `flowStore` | `src/lib/flowStore.ts` | flow readiness metadata + linked schema artifact/version + enable flag + saved designer payload | Flows (and save points from Flow Designer) | LocalStorage key: `nif-flow-catalog-v1` |
| `auditLog` | `src/lib/mockData.ts` | `id`, `time`, `user`, `action`, `object`, `target`, `status` | Audit page | 10 rows over 2 days |

### Inline / hardcoded mock data inside components

| Where | What |
|---|---|
| `Schemas.tsx` | Artifact-version workspace with per-version `avro`, `rawText`, `rawError`, and status |
| `FlowDesigner.tsx` `initialStreams` | `sites` (supporting) + `agents` (primary) |
| `FlowDesigner.tsx` `sourceTypes` | REST / Postgres / Mongo / SMB cards |
| `FlowDesigner.tsx` | Controlled default state objects for source configs, streams, and Kafka output |
| `FlowDesigner.tsx` | Schema artifact/version selection + verification warning UX for existing schema linking |
| `Flows.tsx` MetricCards | Throughput "1,240 rec/s", "84" queued FlowFiles, etc. |
| `Flows.tsx` processor list | `InvokeHTTP 72%`, `SplitJson 41%`, `UpdateAttribute 18%`, `PublishKafkaRecord_2_6 64%` |
| `Flows.tsx` bulletins | WARN backpressure, ERROR 429, INFO schema cache |
| `Flows.tsx` run history | 4 rows (10:42, 10:30, 10:18, 10:06) |
| `Settings.tsx` | All defaults and switch labels |

### Example values used everywhere
- **Source / flow names:** `rapid7__assets`, `sentinelone__agents`, `fortisiem__devices`, `smb__daily_reports`, `rapid7__vulns`
- **Connection ids:** `kafka`, `apicurio`, `nifi`, `smb`
- **Topics:** `rapid7.assets.v2`, `sentinelone.agents.v3`, `fortisiem.devices.v1`, `smb.daily_reports.v1`, `rapid7.vulns.v1`
- **Schema subjects:** `<source>__<stream>-value`
- **States:** see § 11

### Replace-with-backend expectations
Every entity above must be replaceable with a real API response. Suggested mapping:
- `dashboardStats` → `GET /metrics/summary`
- `flowSummary`, `flows` → `GET /flows`
- `connections` → `GET /connections`
- `schemas` → `GET /schemas`
- `auditLog` → `GET /audit?from=&to=`
- Flow Sheet metrics/bulletins/history → `GET /flows/:id/metrics`, `/bulletins`, `/runs`

---

## 9. Domain Model as Reflected in the UI

| Entity | Meaning in UI | Fields shown | Screens | Notes vs PRD |
|---|---|---|---|---|
| **Connection** | Reusable service backend used by flows | id, name, description, health, endpoint, lastTested | Connections, Dashboard ("Failed Connections" KPI) | `/connections` currently shows Kafka/Apicurio/NiFi only; SMB is treated as source-level in UI |
| **Source** | A configured data source (REST/Postgres/Mongo/SMB) — one per flow | source-specific fields (REST auth/timeouts/rate limit, DB connection fields, SMB file fields) | Flow Designer step 2, Flow Runner column | Source-type branching is implemented |
| **Flow Schedule** | Source-level run schedule for the generated flow/process group | `type` (`interval`/`cron`), interval value+unit, or cron expression | Flow Designer step 3, Review step 6 | Exactly one schedule per source in local state |
| **Stream** | A logical endpoint/collection inside a source | name, endpointPath, method, responseDataPath, primaryKeyFields, isPrimary, pagination config, parent injection config, extraction/transformation/routing rules | Flow Designer step 4 | Modeled in code as `Stream` type |
| **Primary Stream** | Exactly one stream per source used as the main output stream context | `isPrimary: true` | Flow Designer (badge + switch + helper text) | Mutual exclusion enforced in state |
| **Supporting Stream** | Stream whose values can be injected into other streams | `parentStreamId`, `parentField`, `injectAs`, `injectFieldName`, `injectTemplate` | Flow Designer step 4 (Fan-out section) | Configured per stream (no standalone relationship panel) |
| **Pagination** | How NiFi iterates pages | `paginationEnabled`, `paginationType`, plus type-specific fields (page/cursor/offset strategies) | Flow Designer step 4 (Pagination section) | Stream-level and dynamic |
| **Fan-Out** | Parent → child stream expansion | parent stream selection + inject target fields | Flow Designer step 4 (Fan-out section) | Stream-level and dynamic |
| **Attribute Extraction** | Promote values from response payload for reuse/injection | repeatable rules: attribute name, path expression, default value, action, optional target/inject fields | Flow Designer step 4 (Attribute Extraction section) | Implemented as first-class UI |
| **Routing Rule** | Content-based routing conditions for a stream | repeatable rules: attribute, condition, value, destination, optional topic suffix | Flow Designer step 4 (Routing Rules section) | Implemented as first-class UI |
| **Kafka Output** | Topic config for the flow | topic, partitions, replication, key strategy, value format, schema link mode (existing/auto-generate) | Flow Designer step 5 | Aligned |
| **Schema Artifact** | Named schema entity containing multiple versions | artifact id, stream, versions[], version status, fields[], raw Avro JSON | Schema Manager, Flow Designer, Flows | Version-level lifecycle is enforced |
| **Schema Version** | Exact version linked by a flow | version number, status (`Draft` / `Needs Verification` / `Verified`), updated | Schema Manager, Flow Designer, Flows | Flows link to one exact version |
| **Flow** | Deployed NiFi process group with execution eligibility metadata | id, name, state, source, primaryStream, topic, linkedSchemaArtifactId, linkedSchemaVersion, readiness flags, enabled | Flow Runner table, Dashboard seed, stores | Start is eligibility-gated |
| **Flow Designer Payload** | Persisted editable configuration snapshot for an existing flow | step, sourceType, source configs, schedule, streams, kafkaOutput, editingStream | Flow Designer + flowStore | Enables stopped-only reopen/edit cycles |
| **Run Log** | Per-run record | started, duration, records, status | Flow Sheet → Run History tab | Hardcoded; not per-flow |
| **Audit Log** | Admin action history | time, user, action, object, target, status | Audit page | Aligned |

---

## 10. Design System and Styling

### Style direction
Modern enterprise SaaS dashboard inspired by Airbyte / Confluent Cloud / Supabase — clean light theme, soft neutral background, white cards, subtle borders, generous rounded corners, compact forms, monospace for technical strings (topics, endpoints, subjects).

### Theme
- **Light theme is default.** A `.dark` token set is defined in `index.css` but no UI toggle exists.
- All colors are **HSL CSS variables** in `src/index.css` and exposed to Tailwind in `tailwind.config.ts`.

### Color tokens (HSL, light)

| Token | Value (HSL) | Use |
|---|---|---|
| `--background` | `220 20% 98%` | App background |
| `--foreground` | `222 25% 14%` | Body text |
| `--card` | `0 0% 100%` | Card surfaces |
| `--primary` / `--primary-muted` | `221 83% 53%` / `221 83% 96%` | Brand blue + muted backdrop |
| `--success` / `--success-muted` | `142 71% 36%` / `142 76% 96%` | Healthy / Running / Verified |
| `--warning` / `--warning-muted` | `38 92% 50%` / `48 96% 95%` | Pending / Degraded / Outdated / Primary marker |
| `--destructive` / `--destructive-muted` | `0 72% 51%` / `0 86% 97%` | Failed / Error |
| `--info` / `--info-muted` | `199 89% 48%` / `204 94% 96%` | Informational labels and banners |
| `--muted` / `--muted-foreground` | `220 14% 96%` / `220 9% 46%` | Subtle surfaces and helper text |
| `--border` / `--input` / `--ring` | `220 13% 91%` / `220 13% 91%` / `221 83% 53%` | Strokes and focus ring |
| `--sidebar-*` | `0 0% 100%` background, `222 18% 30%` foreground, blue accent | Sidebar surface |
| `--radius` | `0.625rem` | Drives `lg/md/sm` border radii |

### Typography
- System default font stack (no custom Google Font wired up).
- `font-feature-settings: "cv11", "ss01"` enables Inter-style alt forms when available.
- Headings: `text-2xl font-semibold tracking-tight` for page titles, `text-base` for card titles.
- Mono (`font-mono`) used for topics, endpoints, schema artifact IDs, timestamps, IDs.

### Spacing
- Card padding `p-4`/`p-6`, page padding `p-4 md:p-6 lg:p-8` with `max-w-[1400px]`.
- Grid gaps usually `gap-3` or `gap-4`.

### Card / Button / Table styles
- Cards: white background, subtle border, `--radius` rounding, no/low shadow.
- Buttons: shadcn variants (`default`, `outline`, `ghost`, `secondary`, `destructive`).
- Tables: shadcn defaults; tabular-nums for numeric columns; mono for IDs/topics; destructive text for non-zero error counts.

### Badges (status)
All managed by `StatusBadge` (see § 11). 5 visual variants: success, warning, destructive, info, muted. Each pill = soft tinted background + matching foreground + colored border + 12px lucide icon.

### Icons
`lucide-react`, sized 3.5–5 (h-/w-3.5 to h-/w-5) consistently.

### Responsive behavior
- Sidebar collapses to icon mode (shadcn collapsible="icon").
- Search input hidden on mobile (`hidden md:flex`).
- Grid breakpoints `grid-cols-1 md:grid-cols-2 lg:grid-cols-3` etc.
- Tables wrapped in `overflow-x-auto`.

### Tailwind / shadcn usage
- shadcn primitives in `src/components/ui/*` (unmodified except for theming via tokens).
- `tailwindcss-animate` plugin enabled.
- `cn()` helper from `src/lib/utils.ts` used consistently.

### Inconsistencies noticed
- Two toaster systems are mounted (`Toaster` from shadcn + `Sonner`). Pages only call `sonner`'s `toast`.
- `App.css` exists but is effectively unused.
- Connections card doesn't include the "View Details" action that the PRD calls for.
- Flow Runner `Deploy` and `Delete` actions are visual-only (no mutation yet).
- Audit filters and export controls are visual-only.

---

## 11. Status, Badge, and State Definitions

All status labels render through **`StatusBadge`** (`src/components/StatusBadge.tsx`), which maps a string to one of five variants.

| Status | Used For | Meaning in UI | Visual Style | Real or Mock |
|---|---|---|---|---|
| `Healthy` | Connection | Last test succeeded | success (green) + check icon | Mock (toggled by Test Connection) |
| `Not Tested` | Connection | Never tested | muted (gray) + circle | Mock |
| `Failed` | Connection, Run history | Test/connection failure | destructive (red) + X | Mock |
| `Running` | Flow | Flow is actively processing | success + activity icon | Mock (toggled by Start) |
| `Stopped` | Flow | Flow is paused / not running | muted + pause icon | Mock (toggled by Stop) |
| `Degraded` | Flow | Running with partial errors | warning (amber) + alert | Mock |
| `Error` | Flow | Failed run state | destructive + X | Mock |
| `Draft` | Schema | Newly created, no fields generated yet | muted + circle | Mock |
| `Needs Verification` | Schema Version | Version exists but has not been verified for flow run | warning + clock | Mock |
| `Verified` | Schema | Approved — flows allowed to run | success + check | Mock |
| `Schema Generated` | Schema (legacy) | Legacy seed status still recognized for migration compatibility | info + check | Legacy/mock |
| `Schema Registered` | Schema (legacy) | Legacy seed status still recognized for migration compatibility | info + check | Legacy/mock |
| `Pending Verification` | Schema (legacy) | Legacy seed status still recognized for migration compatibility | warning + clock | Legacy/mock |
| `Schema Outdated` | Schema | Source data drifted from registered schema | warning + alert | Mock |
| `Success` | Audit, Run history | Past action succeeded | success + check | Mock |

The `StatusBadge` is **the single styling contract for status across the app** — adding a new status only requires adding an entry in `labelMap` inside `StatusBadge.tsx`.

---

## 12. Forms and Input Fields

> Current reality: core authoring forms in Connections, Flow Designer, and Schema Manager are **controlled** and update local state immediately. Draft persistence exists for Flow Designer and Schema Manager via `localStorage`. Audit/Settings controls remain mostly visual.

### 12.1 Connections — Kafka edit dialog

| Field | Type | Default | Required | Functional? |
|---|---|---|---|---|
| Bootstrap Servers | text | `kafka-prod-01:9093,kafka-prod-02:9093` | implied | Yes (local state) |
| Security Protocol | select (PLAINTEXT/SSL/SASL_SSL/SASL_PLAINTEXT) | `SASL_SSL` | — | Yes (local state) |
| SASL Mechanism | select (PLAIN/SCRAM-SHA-256/SCRAM-SHA-512) | `SCRAM-SHA-512` | — | Yes (local state) |
| SASL Username | text | `nifi-producer` | — | Yes (local state) |
| SASL Password | password | empty | — | Yes (local state) |
| Default Topic Prefix | text | `nif.` | — | Yes (local state) |

### 12.2 Connections — Apicurio edit dialog

| Field | Type | Default |
|---|---|---|
| Registry URL | text | `https://apicurio.internal/api` |
| Auth Type | select (None/Bearer Token/Basic/Client Certificate) | `None` |
| Group ID | text | `nif-platform` |
| Auth fields | dynamic | changes immediately by auth type |

**Dynamic auth behavior (Apicurio):**
- `None` → no credential fields
- `Bearer Token` → token field
- `Basic / Username + Password` → username + password
- `Client Certificate` → certificate + key fields

### 12.3 Connections — NiFi edit dialog

| Field | Type | Default |
|---|---|---|
| NiFi API URL | text | `https://nifi.internal/nifi-api` |
| Auth Type | select (None/Bearer Token/Basic/Client Certificate) | `None` |
| Auth fields | dynamic | changes immediately by auth type |

**Dynamic auth behavior (NiFi):**
- `None` → no credential fields
- `Bearer Token` → token field
- `Basic / Username + Password` → username + password
- `Client Certificate` → certificate + key fields

**Removed from Connections page in Phase 2:** `Target Process Group` field is no longer shown in NiFi form.

### 12.4 Connections — SMB edit dialog

SMB edit UI is no longer rendered on `/connections` as of Phase 2. SMB remains part of source-level configuration in Flow Designer concepts.

### 12.5 Flow Designer — Step 2 (Configure Source, REST)

| Field | Type | Default |
|---|---|---|
| Source Name | text | `sentinelone__agents` |
| Base URL | text | `https://usea1-partners.sentinelone.net` |
| Auth Type | select (None/API Key/Bearer/Basic) | `API_KEY` |
| API Key Name / Value / Location | dynamic by auth type | `Authorization` / empty / `HEADER` |
| Bearer Token or Basic Username/Password | dynamic by auth type | empty |
| Connection Timeout (s) | number | `30` |
| Socket Read Timeout (s) | number | `30` |
| Socket Write Timeout (s) | number | `30` |
| Socket Idle Timeout (s) | number | `30` |
| Rate Limit (req/min) | number | `120` |

Flow Designer step 2 also includes dedicated PostgreSQL, MongoDB, and SMB forms with source-specific fields.

### 12.6 Flow Designer — Step 3 (Flow Schedule, **controlled**)

Step 3 is source-level schedule configuration and includes:
- Schedule Type selector: `Interval` or `Cron`.
- Dynamic interval fields: `Run Every` + `Unit` (`seconds`, `minutes`, `hours`, `days`) when `Interval` is selected.
- Dynamic cron field: `Cron Expression` input with helper example when `Cron` is selected.
- Review summary output: schedule text is rendered in Step 6 review.

### 12.7 Flow Designer — Step 4 (Stream form, **controlled**)

Step 4 is stream-centric and includes these controlled sections:
- Request configuration (name, method, endpoint, headers/query params/body template).
- Response extraction (data path, primary key, primary-stream toggle).
- Attribute extraction (repeatable rules with action-dependent fields).
- Pagination (enable/disable, type selector, dynamic fields for Page Increment/Cursor/Offset-Limit).
- Fan-out / parent stream input (parent stream, parent field, inject target, template).
- Transformations (repeatable rules: Add Field, Remove Field, Set from Extracted Attribute).
- Routing rules (repeatable condition/destination rules, optional separate-topic suffix).

### 12.8 Flow Designer — Step 5 (Kafka Output)

Kafka Topic, Partitions, Replication Factor, Key Strategy, Value Format, and Schema Linking mode. Existing-schema mode requires artifact + version selection with status-aware warning. All are controlled and persisted in draft saves.

### 12.9 Flow Designer — Step 6 (Review)

Read-only summary of source, flow schedule, streams, pagination, fan-out, Kafka topic, and linked schema artifact/version with verification warning when selected version is unverified. Final CTA: `Save & Continue to Schema`.

### 12.10 Schema Manager — Structured + Raw Editors

| Area | Behavior |
|---|---|
| Version Selector | User selects artifact version (`vN - status`) before reviewing/editing |
| Version Mutation Rule | Editing `Verified` creates a new higher version in `Needs Verification`; editing `Draft`/`Needs Verification` stays in-place |
| Structured Editor | Fully editable field table (name, type, nullable, doc), add/remove fields |
| Raw Avro JSON | Editable textarea |
| Sync | Structured edits update raw JSON; valid raw JSON updates structured model |
| Validation | Invalid raw JSON shows error and preserves the last valid structured model |
| Actions | `Generate`, `Verify Version`, `Save Draft` all mutate local UI state at version-level |

### 12.11 Audit — Filter bar

Search input + "Last 7 days" + "All users" buttons. All non-functional.

### 12.12 Settings

| Section | Fields |
|---|---|
| Platform | Default Topic Prefix (`nif.`), Default Schema Group (`nif-platform`), Default Partitions (`6`), Default Replication (`3`) |
| Safety | 3 switches (verification before deploy ON, auto-pause on drift ON, alerts on connection failure OFF) |

Settings inputs are uncontrolled visual defaults. `Save Changes` is currently a no-op.

### Validation
**No form validation is implemented anywhere.** Required-field markers, regex/format checks, and min/max constraints would all need to be added.

---

## 13. Important User Flows

### 13.1 Configure Connection Flow
1. **Start:** `/connections`.
2. User locates a card (e.g., Kafka).
3. User clicks **Test Connection** → button shows spinner for ~1.2s → `StatusBadge` updates to `Healthy` → toast appears.
4. User clicks **Edit** → `Dialog` opens with type-specific form (Kafka/Apicurio/NiFi), including dynamic auth fields for Apicurio/NiFi.
5. User edits fields and clicks **Save Changes** → dialog closes, local state updates for endpoint/config, success toast.
6. User clicks **Add Connection** → `Dialog` opens; user picks type (Kafka/Apicurio/NiFi), fills fields, and creates a new local card.
- **Components:** `Connections.tsx`, `Dialog`, `Field`, `StatusBadge`, `sonner`.
- **Backend integration points:** Replace `handleTest` simulation with `POST /connections/:id/test`; persist edits with `PATCH /connections/:id`.

### 13.2 Create Source Flow
1. **Start:** Dashboard `New Source` button → `/flow-designer`.
2. Step 1 — choose source type card.
3. Step 2 — fill out source-specific connection details (REST, PostgreSQL, MongoDB, or SMB).
4. Step 3 — configure source-level schedule (`Interval` or `Cron`) with dynamic fields.
5. Click **Next** to advance through steps.
- **Components:** `FlowDesigner.tsx`, `Card`, `Input`, `Select`, `cn()`.
- **Backend points:** `POST /sources` on Save/Continue.

### 13.3 Configure Stream Flow
1. **Start:** Step 4 of the Flow Designer.
2. User selects a stream in the left list → right pane edits it.
3. User toggles **Primary Stream** switch → flips primary exclusively to that stream.
4. User configures stream-level sections: attribute extraction, pagination, fan-out, transformations, and routing rules.
5. User adds new streams through the `Add Stream` dialog.
- **Components:** `FlowDesigner.tsx`, `Switch`, `Badge`, `Input`, `Select`, `Textarea`, `Dialog`.
- **Backend points:** `PUT /sources/:id/streams`; persist nested stream configuration payloads.

### 13.4 Schema Approval Flow
1. **Start:** `/schemas` (or via Flow Designer's `Save & Continue to Schema`).
2. User picks a schema artifact in the left list and selects a specific version.
3. User clicks **Generate** → selected version becomes `Needs Verification`.
4. User edits Structured/Raw schema:
   - editing `Verified` version creates a new `Needs Verification` version
   - editing `Draft`/`Needs Verification` updates the same version.
5. User clicks **Verify Version** → selected version becomes `Verified`.
6. Warning banner appears for non-verified versions; linked flows are shown per exact selected version.
- **Components:** `Schemas.tsx`, `Tabs`, `Table`, `StatusBadge`, lucide icons.
- **Backend points:** `POST /schema-artifacts/:artifactId/versions/:version/generate`, `POST /schema-artifacts/:artifactId/versions/:version/verify` (or equivalent). Real lifecycle transitions must mutate **version-level** status.

### 13.5 Flow Execution Flow
1. **Start:** `/flows`.
2. User scans the table to find a flow.
3. User clicks **Play** to start a flow in `Stopped` or `Error` state:
   - UI validates readiness (linked schema artifact/version exists, selected version is verified, flow enabled, config complete)
   - if checks fail, Start is blocked with clear reason
   - otherwise state mutates to `Running` + toast.
4. User can toggle **Enable/Disable Schedule** from row actions or sheet quick actions; disabled flows remain blocked from manual start.
5. User can click **Edit** only when flow state is `Stopped` → opens `/flow-designer?editFlowId=...` with hydrated payload from local flow store.
6. User clicks the row → side `Sheet` opens.
7. User reviews **Metrics**, **Bulletins**, **Run History** tabs and sees linked schema version + any blocking reason.
- **Components:** `Flows.tsx`, `Table`, `Sheet`, `Tabs`, `Progress`, `MetricCard`, `StatusBadge`.
- **Backend points:** `POST /flows/:id/start|stop|deploy`, `DELETE /flows/:id`, `GET /flows/:id/metrics`, `/bulletins`, `/runs`.

### 13.6 Edit Existing Flow Flow
1. **Start:** `/flows`.
2. User clicks **Edit** on a stopped flow (row action or sheet quick action).
3. App navigates to `/flow-designer?editFlowId=<id>`.
4. Flow Designer hydrates existing flow config from local `flowStore` payload.
5. User updates configuration and saves; flow entry is updated in local store.
6. If flow is not stopped, edit is blocked with clear feedback.
- **Components:** `Flows.tsx`, `FlowDesigner.tsx`, `flowStore.ts`.
- **Backend points:** `GET /flows/:id`, `PUT /flows/:id`.

### 13.7 Enable/Disable Flow Flow
1. **Start:** `/flows`.
2. User toggles schedule state with **Enable/Disable Schedule** action.
3. Flow `enabled` flag updates in local flow store.
4. While disabled, Start action remains blocked by eligibility guard.
- **Components:** `Flows.tsx`, `flowStore.ts`.
- **Backend points:** `PATCH /flows/:id` with `{ enabled: boolean }`.

---

## 14. Backend Integration Expectations

| Frontend Area | Current Behavior | Future Backend / API Needed | Expected Data |
|---|---|---|---|
| Test Kafka connection | `setTimeout` always Healthy | `POST /connections/kafka/test` | `{ ok, latencyMs, error? }` |
| Test Apicurio connection | Same | `POST /connections/apicurio/test` | Same shape |
| Test NiFi connection | Same | `POST /connections/nifi/test` | Same shape |
| Test SMB connection | Not exposed on `/connections` page | Configure/test under source-level SMB UX when added | Same shape |
| Save connection | Toast only | `PATCH /connections/:id` | Updated connection object |
| Save source | Local wizard state + review CTA toast + optional draft save to `localStorage` | `POST /sources` | Source id + echo of payload |
| Save streams | Local state only | `PUT /sources/:id/streams` | List of streams |
| Generate schema version | Local state transition to `Needs Verification` for selected version | `POST /schema-artifacts/:artifactId/versions/:version/generate` | Updated version state |
| Verify schema version | Local state transition to `Verified` for selected version | `POST /schema-artifacts/:artifactId/versions/:version/verify` | `status: Verified`, `verifiedBy`, `verifiedAt` |
| Deploy NiFi flow | No-op | `POST /flows/:id/deploy` | NiFi process-group id, status |
| Start flow | Eligibility-gated local state transition (blocked when rules fail) | `POST /flows/:id/start` | New flow state or blocking reason |
| Stop flow | Local state | `POST /flows/:id/stop` | New flow state |
| Edit flow | Stopped-only UI navigation to Flow Designer edit mode | `GET /flows/:id` + `PUT /flows/:id` | Full flow config payload |
| Enable/Disable schedule | Local state toggle persisted in flow store | `PATCH /flows/:id` | `{ enabled: boolean }` |
| Delete flow | No-op | `DELETE /flows/:id` | 204 |
| Fetch processor metrics | Hardcoded | `GET /flows/:id/metrics` | Per-processor load %, throughput, queued FlowFiles |
| Fetch bulletins | Hardcoded | `GET /flows/:id/bulletins` | NiFi bulletin objects |
| Fetch run history | Hardcoded | `GET /flows/:id/runs?limit=` | Run records |
| Fetch audit log | Static array | `GET /audit?from=&to=&user=` | Paged audit events |
| Dashboard KPIs | Static | `GET /metrics/summary` | Counts |
| Save settings | No-op | `PUT /settings` | Updated platform settings |

When wiring real APIs, prefer `@tanstack/react-query` (already provided) for caching and optimistic updates.

---

## 15. Known Gaps, Inaccuracies, and Mismatches

| Area | Current UI Behavior | Expected Behavior | Priority | Notes |
|---|---|---|---|---|
| Stream delete | Streams can be added/edited but not deleted from the stream list | Add stream delete action with safeguards for primary stream reassignment | Medium | `FlowDesigner.tsx` |
| Flow Runner Deploy / Delete | Buttons are decorative | Implement deploy + delete with confirmation dialog for delete | Medium | `Flows.tsx` |
| Flow Sheet content | Same metrics/bulletins/history regardless of selected flow | Bind to per-flow data | Medium | `Flows.tsx` |
| Connections "View Details" | Missing | Add per PRD | Medium | `Connections.tsx` |
| Audit filters | Search & buttons non-functional | Wire up text filter + date range + user filter | Medium | `Audit.tsx` |
| Settings save | `Save Changes` is a no-op and inputs uncontrolled | Controlled form + persist (backend or localStorage) | Medium | `Settings.tsx` |
| Form validation | Absent everywhere | Add required-field markers, type checks, helpful errors | Medium | All forms |
| Empty / loading / error states | Not implemented (no fetch yet) | When backend is wired, add skeletons + empty illustrations + error retry | Medium | All list pages |
| Two toasters mounted | `Toaster` (shadcn) + `Sonner` both mounted; only `sonner` is used | Pick one; remove the unused | Low | `App.tsx` |
| Unused `App.css` | Legacy file | Remove | Low | — |
| Dark mode | Tokens defined but no toggle | Add a toggle (or remove `.dark` tokens) | Low | `index.css` |

### 15.1 Feedback Progress Tracker (Updated: 2026-04-25)

#### Phase 2 — Connections Page
- ✅ **Add Connection works**  
  Implemented Add dialog with type selection (Kafka/Apicurio/NiFi), type-specific fields, and local-state card creation.
- ✅ **SMB removed from `/connections` service cards**  
  SMB seed data is filtered out for this page; Connections UI now focuses on service-level entries.
- ✅ **Dynamic auth fields implemented (Add + Edit)**  
  Apicurio and NiFi forms now switch fields immediately for `None`, `Bearer Token`, `Basic / Username + Password`, and `Client Certificate`.
- ✅ **NiFi Target Process Group field removed**  
  No longer shown in NiFi connection form UI.
- ✅ **SMB version selector removed from Connections page scope**  
  SMB connection form is not rendered on `/connections`.

#### Phase 3 — Sources / Flow Designer
- ✅ **Source-type-specific Configure Source forms implemented (`3.1`)**  
  Step 2 now renders distinct forms for REST API, PostgreSQL, MongoDB, and SMB.
- ✅ **Source auth fields are dynamic (`3.2`)**  
  REST auth switches visible fields for `None`, `Bearer`, `Basic`, and `API Key` with key location.
- ✅ **Add Stream is functional (`3.3`)**  
  `Add Stream` opens a dialog, creates a stream in local state, and supports primary designation.
- ✅ **Attribute Extraction section added (`3.4`)**  
  Per-stream extraction rules are repeatable and include attribute name, JSONPath/path expression, and default value.
- ✅ **Attribute extraction actions aligned to approved UX (`3.5`)**  
  Removed `Insert into Payload` and `Rename Before Insert`; kept `Store as Flow Attribute Only`, `Use Later in Another Stream`, and request injection behavior.
- ✅ **Transformation section aligned to approved UX (`3.6`)**  
  Kept only `Add Field`, `Remove Field`, and `Set from Extracted Attribute`.
- ✅ **Parent-to-child stream value passing configured in-stream (`3.7`)**  
  Parent stream, parent field, inject-as target, inject field name, and optional template are now per-stream settings.
- ✅ **Standalone Stream Relationship section removed (`3.8`)**  
  Relationship/fan-out setup now lives inside each stream configuration.
- ✅ **Pagination moved to stream level (`3.9`, `4.1`)**  
  Removed global pagination stage; each stream has its own pagination enablement and strategy.
- ✅ **Pagination forms are dynamic (`4.2`)**  
  UI now switches fields for `None`, `Page Increment`, `Cursor`, and `Offset/Limit`.
- ✅ **Explicit total-count stop logic added**  
  For both `Page Increment` and `Offset/Limit`, when `Stop Condition = Total Count Field`, UI includes `Total Count JSONPath` plus explicit `Stop When` condition selection.
- ✅ **Review CTA renamed (`6.1`)**  
  Final button label is now `Save & Continue to Schema`.

#### Phase 4 — Schema Manager
- ✅ **Register + Verify actions merged (`4.1`)**  
  Kept `Generate Schema` as a standalone action and replaced separate `Register` / `Verify & Approve` with single `Verify & Register Schema`.
- ✅ **Sampled-records banner text removed (`4.2`)**  
  The explicit “Sampled 50 records…” message is no longer shown in visible UI.
- ✅ **Raw Avro JSON is editable (`4.3`)**  
  Raw tab now uses an editable text area for direct Avro JSON edits.
- ✅ **Structured and Raw editors are synchronized (`4.4`)**  
  Structured edits update raw JSON, valid raw JSON updates structured fields, and invalid raw JSON shows validation errors without overwriting structured state.
- ✅ **Save Draft / Return later behavior added (`4.5`)**  
  Added local draft save/restore for both Schema Manager and Flow Designer using browser `localStorage`, with confirmation toasts.

#### Phase A — Source-Level Flow Scheduling
- ✅ **Dedicated source-level schedule step added**  
  Flow Designer now includes `Flow Schedule` as a separate wizard step after source configuration and before streams.
- ✅ **Dynamic schedule form behavior implemented**  
  Selecting `Interval` shows only interval fields (`Run Every` + `Unit`), and selecting `Cron` shows only cron expression input.
- ✅ **Source-level schedule draft persistence added**  
  Schedule configuration is included in Flow Designer local draft save/restore payload (`localStorage`).
- ✅ **Schedule summary included in final Review**  
  Step 6 review now displays a readable schedule summary (`Runs every ...` / `Runs using cron expression: ...`).

#### Phase C — Schema Versioning, Verification, Selection, and Flow Eligibility
- ✅ **Schema artifact + version model implemented (`C1`, `C5`, `C6`)**  
  Replaced single mutable schema behavior with artifact-level versions. Users now select exact schema artifact and version; statuses are version-level (`Draft`, `Needs Verification`, `Verified`).
- ✅ **Flow links now persist exact schema version (`C1`, `C4`)**  
  Flow linking stores `schemaArtifactId + schemaVersion` explicitly, and flows do not auto-switch to newer versions.
- ✅ **Flow creation schema selection upgraded (`C1`)**  
  In Flow Designer, existing-schema mode now requires both artifact and version selection and displays version status inline.
- ✅ **Unverified-version blocking guidance added (`C1`, `C7`)**  
  If selected version is unverified, UI shows warning plus direct action: `Go to Schema Verification`.
- ✅ **Verified schema edit now forks new version (`C2`)**  
  Editing a `Verified` version creates a newer `Needs Verification` version while preserving the old verified version.
- ✅ **Draft/unverified edit stays on same version (`C2`)**  
  Editing `Draft` or `Needs Verification` updates that same version instead of creating endless new versions.
- ✅ **Version-linked flow visibility in Schema Manager (`C5`)**  
  Schema Manager now shows linked flows for the selected exact version.
- ✅ **Flow start eligibility enforcement added (`C7`)**  
  Flow Runner start action is blocked with explicit reasons when conditions fail (no schema link, no version, unverified version, disabled flow, incomplete config).

#### Phase D — Editing Existing Flows + Enable/Disable Scheduled Execution
- ✅ **Stopped-only flow editing implemented (`D1`, `D2`)**  
  Flow Runner now exposes `Edit` actions (row + sheet), but editing is allowed only when flow state is `Stopped`; otherwise UI blocks and explains why.
- ✅ **Flow Designer edit-mode hydration added (`D1`)**  
  Flow Designer supports `?editFlowId=` and hydrates existing flow configuration from local `flowStore` payload.
- ✅ **Enable/Disable scheduled execution controls added (`D3`)**  
  Flow Runner now includes explicit schedule toggle controls and persists `enabled` state in local flow store.
- ✅ **Manual Start remains blocked when disabled (`D4`)**  
  Start eligibility guard now clearly blocks disabled flows until re-enabled.
- ✅ **Editable flow payload persistence added (`D5`, `D6`)**  
  Flow saves now persist `designerPayload` in `flowStore`, enabling reliable reopen/edit cycles.

#### Phase Completion Snapshot
- ✅ Phase 1: Dashboard
- ✅ Phase 2: Connections Page
- ✅ Phase 3: Sources / Flow Designer
- ✅ Phase 4: Schema Manager
- ✅ Phase C: Schema Versioning + Flow Eligibility
- ✅ Phase 5: Flow Runner (eligibility gating updated in Phase C; deploy/delete still pending)
- ✅ Phase 6: Audit Log (no major requested changes)
- ✅ Phase A: Source-Level Flow Scheduling
- ✅ Phase D: Edit Existing Flows + Enable/Disable Scheduled Execution

> **User notes placeholder:** The original prompt included `[PASTE MY NOTES HERE]` but no specific notes were provided. When notes are supplied, list each one here with the same five columns: which area of the UI it relates to, what the current UI likely does, what should change, the likely files/components involved, and explicit confirmation that the change is **not yet implemented**.

---

## 16. Recommendations for External Agent

When working on this project, please:

1. **Start by reading this document end-to-end** before opening source files.
2. **Then inspect the listed page components** in `src/pages/*` and shared components in `src/components/*`.
3. **Make changes page by page** — each page is self-contained; cross-page coupling is minimal except via `mockData.ts` and `StatusBadge`.
4. **Avoid changing unrelated components.** In particular, do not modify `src/components/ui/*` (shadcn primitives) unless explicitly asked.
5. **Keep mock data** unless backend integration is explicitly requested. When wiring real APIs, prefer `@tanstack/react-query` (already provided).
6. **Preserve design consistency:**
   - Always go through `StatusBadge` for any new status visuals.
   - Use semantic color tokens (`bg-success-muted`, `text-warning`, `border-info/30`, …). **Never hardcode raw colors.**
   - Match the rounded/bordered/white-card aesthetic.
7. **Preserve existing routing** in `App.tsx` unless explicitly asked to change it.
8. **Do not introduce authentication** unless requested. The "admin" identity is a static placeholder.
9. **Do not add destinations other than Kafka** unless requested. The product position is "REST/DB/SMB → Kafka via NiFi/Apicurio".
10. **Do not build a raw NiFi canvas editor.** That is explicitly out of scope per the PRD.
11. **Keep the product aligned with the PRD.** When in doubt, prefer adding a small, focused UI surface over expanding scope.
12. **Refresh resets state.** If you add features that need persistence in a demo, prefer `localStorage` until a backend exists.

---

## 17. Suggested Next Implementation Order

1. **Flow Runner operational completeness**
   - Implement `Deploy` and `Delete` local-state behavior (with delete confirmation).
   - Bind sheet metrics/bulletins/history to per-flow data.
2. **Flow Designer stream management polish**
   - Add stream delete and enforce safe primary-stream reassignment rules.
3. **Connections and observability UX**
   - Add "View Details" surface on connection cards.
   - Wire Audit filters/search/export behavior in local mock state.
4. **Settings and validation**
   - Make Settings controlled and persist to local storage.
   - Add form validation across key forms (connections, source config, stream config, schema edits).
5. **Platform polish**
   - Remove unused `App.css` and keep one toaster system.
   - Decide dark-mode strategy (add toggle or remove dormant `.dark` tokens).
6. **Backend readiness (when requested)**
   - Introduce `@tanstack/react-query` queries/mutations matching § 14 endpoints.
   - Keep current mocks as fallback handlers for demo/test environments.

---

## 18. Glossary

- **NiFi (Apache NiFi):** A data-flow orchestration tool. Flows are visual graphs of processors that move and transform data. NIF Abstractor generates and deploys these flows on the user's behalf.
- **Process Group:** A NiFi container that holds a related set of processors and connections. Each generated flow lives in its own process group.
- **FlowFile:** NiFi's unit of work — a piece of content plus attributes, moving through the flow graph.
- **Kafka:** A distributed event-streaming platform. Every generated flow ends with a Kafka publish.
- **Apicurio:** A schema registry. NIF Abstractor stores Avro schemas for each Kafka topic here.
- **Avro:** A row-oriented binary serialization format with a JSON-defined schema. Used for the value of every Kafka message.
- **Source:** A configured external system that NIF Abstractor pulls from (REST, Postgres, Mongo, SMB).
- **Stream:** A logical endpoint or collection inside a source — e.g. `/agents` REST endpoint, a Postgres table, or a daily-reports SMB file pattern.
- **Primary Stream:** Exactly one stream per source used as the main output stream context.
- **Supporting Stream:** A stream whose values feed parameters of the primary stream's requests (e.g. fetching `sites` first to pass `siteIds` into `agents`).
- **Pagination:** How NiFi iterates through pages of a paginated API.
- **Cursor Pagination:** Server returns an opaque cursor / next-token; client passes it on the next request.
- **Offset / Limit Pagination:** Client requests pages by `offset` and `limit` until an empty page.
- **Page Increment Pagination:** Client increments a `page` (or page number) parameter.
- **Fan-Out:** Expanding one parent record into many child requests (e.g. for each `site`, fetch its `agents`).
- **Attribute Extraction:** Picking values out of a record (e.g. `dataPath`, `primaryKey`) to use as NiFi attributes.
- **Content-Based Routing:** Splitting flow paths based on record content. Implemented as stream-level routing rules in Flow Designer step 4.
- **Schema Verification:** The human-approval gate. Until a schema is `Verified`, flows that depend on it cannot run.
- **Flow Runner:** The page that lists deployed NiFi flows and exposes deploy/start/stop/delete + monitoring.
- **Audit Log:** Append-only history of admin actions (create source, test connection, generate/approve schema, start/stop flow, …).

## Documentation Accuracy Notes

- **Audit completed:** `2026-04-25 20:23:43 UTC`
- **Verification basis:** This document was audited against the current frontend source code under `src/` and updated to match implemented UI behavior.
- **PRD usage:** PRD was treated as product intent/context only; frontend code was treated as the behavior source of truth.
- **Assumptions made:**
  - Local storage stores (`schemaStore`, `flowStore`) are authoritative for cross-page mock state.
  - Backend integration rows remain forward-looking and intentionally non-implemented in this frontend-only mock.
- **Areas not fully verifiable from code alone:**
  - Real backend endpoint contracts and response payloads (documented as integration expectations only).
  - Runtime behavior of external systems (NiFi/Kafka/Apicurio/SMB), which are not connected in this mock.
- **Known limitations remaining:** Deploy/Delete flow actions, Audit filters/export, and Settings persistence remain visual/mock gaps; see §15 for the maintained gap list.
