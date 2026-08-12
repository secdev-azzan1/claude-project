# Alpha Frontend Behavioral Audit — `lovable_ui/frontend`

Source root: `C:\Users\kaifm\Desktop\Project\lovable_ui\frontend`

## 0. Ground-truth note

`FRONTEND_UI_DOCUMENTATION.md` and `docs/BACKEND_API_ENDPOINTS.md` in this repo describe an **earlier, mock-only** version of the app ("everything is local React state, no HTTP calls"). That is **no longer true**. The current source code has a real API client (`src/lib/api.ts`) and every page listed below fetches/mutates through TanStack React Query against a live backend. This report is based entirely on reading the actual current source, not the stale docs. Where the stale docs are wrong in a way worth flagging (route list, "mock-only" claims), it's called out inline.

### Current route table (`src/App.tsx`)

| Path | Component | Notes |
|---|---|---|
| `/` | `src/pages/Dashboard.tsx` | Live KPI/activity/flow-status data |
| `/nifi-services` | `src/pages/NifiServices.tsx` | "Service Manager" — NiFi controller services + HTTP application services |
| `/application-services` | `src/pages/NifiServices.tsx` | Same component, second route alias |
| `/connections` | `<Navigate to="/settings/connections" replace />` | Legacy path, redirects |
| `/settings`, `/settings/connections` | `src/pages/Settings.tsx` | Thin wrapper rendering `PlatformConnectionsPanel` from `Connections.tsx` |
| `/flow-designer` | `src/pages/FlowDesigner.tsx` | 6-step wizard, create + edit modes |
| `/schemas` | `src/pages/Schemas.tsx` | Schema artifact/version manager |
| `/flows` | `src/pages/Flows.tsx` | Flow Runner list + details panel |
| `/audit` | `src/pages/Audit.tsx` | Live audit log, search + CSV export |
| `*` | `src/pages/NotFound.tsx` | 404 |

Note: `src/pages/ApplicationServices.tsx` exists and is fully functional (full CRUD for REST/SMB/Webhook application services) but **is not routed anywhere in `App.tsx`** — only reachable if a route were added, or dead code currently. Its functionality (application-service CRUD) is otherwise exposed through `NifiServices.tsx`'s "HTTP Service" sub-flow. Also, `Connections.tsx`'s own default-exported `Connections` component (bottom of file) is defined but unrouted — superseded by `Settings.tsx` reusing its `PlatformConnectionsPanel` export.

### Sidebar (`src/components/AppSidebar.tsx`)

Header: "Data Mobility Platform". **Workspace**: Dashboard (`/`), Service Manager (`/nifi-services`), Schema Manager (`/schemas`), Flow Runner (`/flows`), Audit Log (`/audit`). **System**: Settings (`/settings`). No sidebar entry for `/flow-designer` (reached via "Add Flow"/"Edit" actions) or `/application-services`.

---

## 1. Flows / Streams Page — `/flows`, `src/pages/Flows.tsx` (2,806 lines)

Data: `useQuery(["flows"], flowsApi.list)` → `GET /api/flows/`, polled every 15s. Also loads `["schemas"]` and `["connections"]` for gating logic. Fully real, not mock.

### Table

Fixed-width table, one row per flow: checkbox, **State** (`StatusBadge`, compact), **Flow Name** (mono), **Source** (`source_type`), **Entities** (derived stream/entity names, truncated with "See N more"), **Kafka Topic** (derived, same overflow pattern), **Schema Link** (artifact + version + status per entity), **Actions**.

`getFlowDestinations(flow)` prefers `flow.entity_destinations[]` (multi-entity flows); falls back to a synthetic single destination from `kafka_topic`/`schema_artifact_id`/`primary_stream_id` for legacy/simple flows.

### Row actions — all direct icon buttons, no kebab/dropdown menu

Every action is always rendered, disabled with a tooltip explaining the block reason when ineligible:

1. **Start/Stop** (Play/Square icon, single toggle button)
2. **Deploy/Undeploy** (Rocket/Trash2, single toggle button, spins `RefreshCw` while in-flight)
3. **Edit** (Pencil) — always enabled
4. **Export** (Download, spinner while exporting)
5. **Enable/Disable** (ToggleRight/ToggleLeft)
6. **Delete** (Trash2, destructive)

#### Start — step by step
1. Click Start. `getStartBlockReason(flow)` checks, in order: missing `primary_stream_id`; schema not fully verified (`requiresSchemaWorkflow(source_type)` — Trino sources are exempt); flow disabled; missing NiFi/Kafka/Apicurio connections; not yet deployed.
2. If blocked → `toast.error(reason)`, no API call.
3. If eligible → `POST /api/flows/{id}/start`. No confirmation dialog either way.
4. Success → `toast.success("Flow started")` (or `toast.error(res.reason)` if `res.ok` is false); `["flows"]`/`["dashboard"]` query caches invalidated (no manual optimistic patch — refetch drives the UI).

#### Stop — step by step
1. `getStopBlockReason`: Draft flows can't be stopped; deployed-but-NiFi-connection-missing is blocked.
2. `POST /api/flows/{id}/stop` → toast "Flow stopped" → invalidate. No confirmation.

#### Deploy / Undeploy (single toggle button)
- Deploy blocked for Draft-state flows, missing runtime services, or unverified/unlinked schemas (`getSchemaBlockReason(flow, "deploy")`).
- Undeploy blocked only if the NiFi connection is missing.
- `POST /api/flows/{id}/deploy` or `POST /api/flows/{id}/undeploy`. **No confirmation dialog for undeploy** even though it removes NiFi resources. Toast: "Flow deployed to NiFi" / "Flow removed from NiFi".

#### Edit
- Button label/tooltip implies "stopped-only" editing, but `canEdit` is a **hardcoded `true`** constant — edit is always enabled regardless of flow state in the current code (the disabled branch is unreachable). `navigate(/flow-designer?editFlowId={id})` — pure client nav, no direct API call from this page.

#### Export
- Blocked only if `flow.state === "Draft"`.
- `GET /api/flows/{id}/export` (raw `fetch`, not the shared wrapper, to read the blob + `Content-Disposition` filename). Downloads a `.flowpack.json` file via a hidden `<a download>`. No confirmation. Toast "Flow export downloaded".

#### Enable/Disable schedule
- `PATCH /api/flows/{id}` with `{enabled: !flow.enabled}`. Disabling shows toast **"Flow disabled — NiFi resources stopped"** (disabling actively stops NiFi resources server-side, not just a flag). Disabled entirely for Draft-state flows. No confirmation.

#### Delete
1. Native `confirm()`: `Delete flow "{name}"? This will also remove it from NiFi.`
2. Confirm → `DELETE /api/flows/{id}`.
3. Success → toast "Flow deleted"; closes the details Sheet if the deleted flow was open; invalidate.

### Bulk selection

Header checkbox + per-row checkboxes drive a bulk toolbar (`src/lib/flowBulkSelection.ts` for pure selection-state helpers). Toolbar shows "{N} selected" and buttons: **Start, Stop, Deploy, Undeploy ("Remove from NiFi"), Enable, Disable, Delete, Clear**. Each button disabled if zero selected flows are eligible for it (same eligibility functions as row actions). `runBulkAction` filters to eligible flows, confirms via `confirm()` for delete (naming the count), then runs the same per-flow API calls **sequentially** (not parallel), tracking success/failure independently per flow. Final toast: `"{Action} ran on N flows. X skipped. Y failed."`

### Search / filter

Real (not visual-only): debounced (180ms) text input, `filterFlowRows` (`src/lib/pageSearch.ts`) substring-matches flow name, source_type, state, primary stream, kafka topic, schema artifact id, and every `entity_destinations[]` field. Header shows "X of Y flows". **No column sorting exists anywhere.**

### Import Flow (header button, not per-row)

"Add Flow" → `navigate("/flow-designer?new=1")`. "Import" → opens `<input type="file" accept=".flowpack.json,application/json">`.

1. File picked → `POST /api/flows/import/preview` (multipart) → `FlowImportPreview` (summary counts, credential requirements, schema conflicts, NiFi service resolution options, errors/warnings).
2. Dialog: editable flow name (auto-suggested, appends " copy" on name collision).
3. Per referenced NiFi service (Kafka/Schema Registry): "Use local default service" vs. "Use imported service definition" — **note: even choosing "imported" only captures values for review; finalize always resolves to local default services** (an explicit product behavior, not a bug).
4. Per conflicting schema artifact: "Create renamed schema" (default) or "Use existing schema" (only if compatible).
5. Required re-entered credentials (masked in the export) shown as password/text inputs with show/hide; "Validate Details" → `POST /api/flows/import/credentials/validate`.
6. "Import Flow" enabled only when `isFlowImportReady(...)` (preview present, name non-empty, credentials validated if required, all schema conflicts resolved) → `POST /api/flows/import/finalize`. Success → toast "Flow imported", caches invalidated, the newly imported flow auto-opens in the details Sheet. **Import always creates a new draft flow — never overwrites an existing one.**

The import helper functions (`isFlowImportReady`, `createImportSchemaResolutions`, `getSuggestedImportFlowName`, `groupImportCredentialRequirements`, etc.) live in `src/lib/flowApi.ts` (lines ~193–393), not in `FlowDesigner.tsx` — despite `flowImportCredentials.test.ts` / `flowImportPreview.test.ts` / `flowImportFinalize.test.ts` existing as standalone test files with no matching source file of the same name.

### Missing-services banner

If NiFi/Kafka/Apicurio connections are absent (from the `["connections"]` query), a warning banner above the table lists which are missing and links to `/settings/connections`.

### Flow map / graph

None on this page. `StreamFlowMap`/`StreamFlowNode` (`src/components/flow-map/*`) are used only inside `FlowDesigner.tsx`, not `Flows.tsx`.

---

## 2. Flow Details Interface (right-side `Sheet`)

Opens on row click (not action-icon clicks — those `stopPropagation`). `detailTab` resets to `"metrics"` on open.

### Header
- Flow name, `StatusBadge`, topic count, schema count.
- **Entity Outputs** block: per-destination entity/stream name, Kafka topic, schema artifact id + version (real data from `entity_destinations`).
- Same action buttons as the row (Start/Stop, Edit, Export, Deploy/Undeploy, Enable/Disable) — literally the same handlers, same eligibility rules, same real API calls.
- If deployed, shows the raw NiFi Process Group ID.
- Inline warning banner with the exact block reason if Start is currently blocked.

### Tabs: Metrics, Runtime Issues, Kafka Messages, Processors, Services, Iceberg Sync

Per your instructions, **Metrics is documented only briefly** since it will come from the new UI: it's `GET /api/flows/{id}/metrics`, polled 10s while Running / 30s otherwise, real data (throughput, queued flowfiles, records processed, bytes in/out) shown as cards; "Deploy the flow to NiFi to see live metrics" when undeployed. There is no tab literally named "DLQ" in this alpha — **Runtime Issues** is the closest analog and is documented below since it isn't the excluded DLQ tab.

#### Runtime Issues (real, backend-wired)
`GET /api/flows/{id}/runtime-errors?limit=50`, polls 10s while tab active. Shows failure topic name, data source (`kafka`/`kafbat`/`unverified`), whether it's complete Kafka history or "Not end-to-end verified". Each error is an accordion item: stream name, stage, HTTP status, processor name, error category, request method+URL, pagination page count, event time, topic:partition:offset, and a raw JSON attributes dump. Manual refresh button.

#### Kafka Messages (real, backend-wired)
`GET /api/flows/{id}/kafka-messages?limit=50`, polled 10s while active. Total message count, topic list, accordion of latest 50 messages (partition/offset/timestamp, expandable pretty-printed key+value). **Clear Topics** button (destructive) → `confirm("Clear all retained messages from N Kafka topic(s)? This cannot be undone.")` → `POST /api/flows/{id}/kafka-messages/clear`, refetches messages+metrics, toasts how many were cleared.

#### Processors (real, backend-wired)
`GET /api/flows/{flowId}/processors`, enabled only when deployed. Lists NiFi processor name/type/owning PG/state badge/validation-error warning. **Configure** opens a dialog (`GET .../processors/{processorId}` for full config, dynamic form from `descriptors`: dropdown for enumerated allowable values, else text/password honoring `sensitive`). **Configure is disabled while the flow is enabled** ("Disable the flow first to configure processors"). Save → `PUT .../processors/{processorId}` with `{properties}`; dialog notes "Processor will be briefly stopped during save" if currently RUNNING.

#### Services (Controller Services, real, backend-wired)
Same pattern as Processors but for NiFi controller services scoped to the flow's process group: `GET /api/flows/{flowId}/controller-services`, Configure disabled while flow enabled, save via `PUT .../controller-services/{serviceId}` with `{properties}`. Tab trigger itself is disabled when the flow has no `nifi_process_group_id`.

#### Iceberg Sync (real, backend-wired, via `icebergSinksApi`)
`GET /api/flows/{flowId}/iceberg-sinks?live=true`, polled 10s. Shows Kafka-Connect-unreachable warning if `connect_reachable === false`. Per-sink card: `topic → bronze.{table_name}` mapping, connector name, `StatusBadge` for derived state (RUNNING/PAUSED/FAILED/UNASSIGNED/RESTARTING/Disabled/Not Deployed/Unknown), per-task states, expandable error/trace. Action buttons conditional on state: Enable, Disable, Pause, Resume, Restart → `POST /api/iceberg-sinks/{id}/{action}`. Preflight-check failures render as a pass/fail checklist. Footer: "Data lands in Iceberg in ~60s commit batches." If no sinks exist, instructs the user to enable "Sync to Iceberg" on an entity stream in Flow Designer — **sink config is not editable here or anywhere in the Services pages; it's lifecycle-management only** (see §4).

### Real vs. mock summary
Every part of the details panel is wired to real endpoints via React Query polling — there is no hardcoded/mock data anywhere in it. The only "soft" non-backend-affecting UI is the import dialog's NiFi-service resolution radio (documented in §1), which is an intentional always-resolves-to-default behavior, not a stub bug.

---

## 3. Schema Pages — `/schemas`, `src/pages/Schemas.tsx` (1,153 lines)

Real backend throughout (`schemasApi` in `src/lib/schemaApi.ts`). Two-pane layout: left "Schema Artifacts" list, right detail pane.

### Schema list & version display
- Left panel: debounced search (artifact_id/stream/namespace/version/status), Verified/Unverified checkboxes (filter by the **latest** version's status; both-checked or both-unchecked = no filter). Each row: `artifact_id` (mono), `Latest v{n} · {relative time}`, `StatusBadge` for latest version status. No sort control.
- Clicking a row selects the artifact and jumps to its latest version.
- **Version display**: a `<Select>` dropdown in the detail header (`data-testid="schema-version-select"`), options read `"v{n} - {status}"` — not tabs, not a list.
- Detail header pills: `Latest` (if selected = newest), `In Use` (if any flow references this exact artifact+version), `Apicurio #{global_id}` (if registered), `Unsaved` (if local edits are dirty).

### Add Field — step by step
1. In the "Structured Editor" tab, each nesting level shows existing fields + an "Add Field" button.
2. Click → appends a new field `{name: "new_field_{depth}", type: "string", nullable: false, doc: ""}` to the in-memory Avro record.
3. Each field row exposes: Field name (text), Type (select — scalars, logical types, object/array/map, enum/fixed/union/reference, advanced), Nullable (switch), remove (trash) button, Doc (text).
4. Max nesting depth is 5 (`MAX_STRUCTURED_SCHEMA_DEPTH`); the Add Field button disables past that depth, and the type dropdown only offers object/array/map while nesting is still allowed.
5. `object` fields recursively render a nested field list; `array`/`map` render one nested editor for the item/value type; `enum`/`fixed`/`union`/`reference`/`advanced` show a static notice: *"This field is preserved exactly. Use Raw Avro JSON to edit unsupported Avro shapes."*

### Structured Editor vs. Raw Avro JSON
Two tabs ("Structured Editor" / "Raw Avro JSON") operating on **one shared in-memory buffer**, not independent state:
- Structured edits call `updateAvro(next)` → updates `editAvro`, re-serializes to `rawText`, clears `rawError`, sets `dirty=true`. Raw tab reflects structured edits immediately.
- Raw tab edits (`Textarea`, `data-testid="schema-raw-textarea"`) set `rawText` immediately; on each keystroke, attempts `JSON.parse` + `normalizeAvroRecord` validation (root must be `record` type or an array wrapping one; `fields` must be an array; each field needs `name` and `type`). On success, flows back into `editAvro`/Structured tab. **On failure, `rawError` is set and `editAvro` is left unchanged** — the Structured tab silently keeps showing the last valid parse.
- Error text under the textarea: *"Invalid Avro JSON: {rawError}. Structured Editor remains unchanged until JSON is valid."*
- Both Save Draft and Verify check `rawError` first and block with toast (`"Cannot save: {rawError}"` / `"Cannot verify: {rawError}"`) if the raw text is currently invalid.

### Save Draft vs. Verify — different actions, different endpoints
- **Save Draft** (`data-testid="schema-save-draft-btn"`): enabled only when `dirty`. → `PUT /api/schemas/{artifactId}/versions/{version}` with `editAvro`. Response `{forked, version?, new_version?}` — if `forked`, toast "Created new draft version v{new_version}" and the UI switches to that version; else toast "Draft saved".
- **Verify Version** (`data-testid="schema-verify-btn"`): disabled while pending or if already Verified.
  1. If `dirty` edits exist and the version isn't already Verified, the frontend **first silently persists** them via the same `PUT .../versions/{version}` call (no separate toast for this step) — explicit code comment: *"Persist only deliberate edits before verify. Loading an inferred schema must not rewrite it."*
  2. Then `POST /api/schemas/{artifactId}/versions/{version}/verify`.
  3. Response `{status: "Verified", version, verified_at, apicurio: {ok, api_version?, global_id?, version?, error?} | null}`.
  4. Toast branches three ways: full success mentions the Apicurio global ID (`"Schema verified and registered in Apicurio (globalId=...)"`); `apicurio.ok===false` → **warning** toast `"Schema verified locally, but Apicurio registration failed: {error}"`; no `apicurio` info → plain `"Schema verified"`.

### Version immutability for Verified versions
No client-side block on editing a Verified version's fields — the editor stays interactive. Instead: a green banner above the tabs when Verified reads *"Verified version. To make changes, save edits — a new draft version will be forked automatically."* The fork happens **server-side**: user edits a Verified version → clicks Save Draft → backend detects immutability and returns `{forked: true, new_version: N}` → frontend switches to the new Draft version and toasts "Created new draft version v{N}". The Verify button itself is simply disabled while the selected version is already Verified, so there's no path to re-verify in place.

### VERIFY vs REGISTER — important finding

**There is no separate "Register" action anywhere in this app.** `schemasApi` exposes `list/get/create/remove/removeVersion/updateVersion/generate/verify` — no `register` method exists. The single `POST .../versions/{version}/verify` call does double duty: it marks the version Verified **and** attempts Apicurio registration server-side in the same request, reporting both outcomes via the `apicurio` sub-object in the response (handled by the three-way toast above). If the new UI is meant to treat Verify and Register as genuinely separate user-facing steps, that is a **behavioral change from this alpha**, not something to preserve as-is — flag this explicitly to whoever designs the new UI's schema actions.

Also note: `schemasApi.generate` (`POST .../versions/{version}/generate`, meant to flip status to "Needs Verification") is defined in `schemaApi.ts` but **is never called anywhere in the codebase** — dead/orphaned API hook.

### Delete — version vs. entire artifact

Single shared dialog (title "Delete Schema", opened by one trash-icon button, `data-testid="schema-delete-btn"`, no separate menu). Body text: *"Choose whether to delete only version v{n} or the entire schema artifact."* Shows informational counts: "Linked flows to v{version}" and "Linked flows to artifact" (**informational only, not enforced client-side**).

- **Delete Version** button: `disabled` when the artifact has only one version (caption: *"This artifact has only one version; delete version is disabled."*) or while a delete is pending. → `DELETE /api/schemas/{artifactId}/versions/{version}`. On success: toast "Schema version deleted", auto-selects the next-highest remaining version. **No client-side guard against deleting a version linked to a flow or a Verified version** — only the "last remaining version" guard exists client-side; anything else would have to be a backend rejection surfaced as a generic error toast.
- **Delete Entire Artifact** button: disabled only while a delete is pending (no linked-flow or Verified-content guard at all). → `DELETE /api/schemas/{artifactId}`. On success: toast "Schema artifact deleted", selection resets to auto-pick another artifact.
- Both delete paths have a defensive re-check: if the DELETE call throws but a follow-up GET shows the item is actually gone, it's treated as success anyway (guards against a network-layer error masking a real server-side success). Otherwise `toast.error(message || "Failed to delete schema version/artifact")` and the dialog stays open for retry.

### Schema inference from uploaded files — important finding

**There is no file-upload-based schema inference on the Schemas page**, and no CSV/JSON/Parquet upload control exists anywhere in `Schemas.tsx` (confirmed by full-project grep — the only file inputs in the app are for OpenAPI spec upload and `.flowpack.json` flow import, both unrelated). Instead, inference is a **live Kafka-sampling job**, entirely implemented in **`src/pages/FlowDesigner.tsx`** using `src/lib/inferenceApi.ts`:

- Per-entity-stream "Start Inference" (or "Retry Inference") button in the Flow Designer's Destination step: *"Runs a temporary NiFi inference flow and samples only this entity branch."*
- Persists the draft flow, then `POST /api/schema-inference/start` (`target_messages` default 10, or 1 for REST POST streams).
- For REST sources, a preflight scan (`src/lib/inferencePreflight.ts`) checks for unresolved `${placeholder}` runtime variables in the endpoint template; a 422 response triggers a UI prompt to collect missing values rather than a hard failure.
- Blocked entirely for REST streams using PUT/PATCH/DELETE methods (toast explains "Use an existing schema" instead).
- While running: status panel (`GET /api/schema-inference/{jobId}`, polled), progress bar, `{collected}/{target}` messages, Stop button.
- On complete: shows the raw generated Avro JSON, with **"Accept Schema"** (`POST /api/schema-inference/{jobId}/accept` → creates/updates the schema artifact+version, updates the entity's destination config to reference it) and **"Verify in Schema Manager"** (simply `navigate("/schemas")`, handing off to the Verify flow described above). Because no edits have been made yet at that point, `dirty` is false, so clicking Verify there skips the pre-verify save and verifies the inferred schema exactly as accepted.

### Real vs. mock in Schemas.tsx
Everything is real (`useQuery`/`useMutation` against live `/api/schemas/...` endpoints). Purely local/in-memory: the edit buffer (`editAvro`/`rawText`/`dirty`, so keystrokes don't hit the API), the structured-field representation (`src/lib/schemaEditor.ts`, a pure client-side transform layer never sent directly to the backend), and the search/status-checkbox filtering (client-side only, no server-side search endpoint).

---

## 4. Services Pages

Two pages, both real, both hitting `applicationServicesApi`/`nifiServicesApi` (`src/lib/*.ts`).

### Conceptual split
- **`src/pages/NifiServices.tsx`** ("Service Manager", routed at `/nifi-services` and `/application-services`): manages **live NiFi controller-service instances** (Kafka connection services, Schema Registry services, MongoDB services, or any other/"custom" NiFi controller-service type) side-by-side with a filtered view of **Application Services of type "REST API"**, branded "HTTP Service" in this page's UI.
- **`src/pages/ApplicationServices.tsx`** (full CRUD, all 3 types, but **currently unrouted in `App.tsx`** — see §0): manages `ApplicationService` records — reusable, application-owned (not NiFi) connection profiles of type **REST API**, **SMB**, or **Webhook**, selected during source setup in the Flow Designer wizard.
- Both pages hit the **same backend collection** for REST API type services (`/api/application-services/`) — a service created on one page is visible/editable from the other.

### NifiServices.tsx — card grid, two card types

**NiFi Global Service card**: name, "Default" star badge, kind description, live state badge (ENABLED green / DISABLED muted / other warning), NiFi Type (last segment of Java class), Controller Service ID, validation-error box, error box. Actions: **Set Default** (only for kafka/schema_registry kinds, not already default), **Configure** (opens dynamic property-descriptor form — see below), **Enable/Disable** toggle, **Delete** (`confirm()` dialog: `Delete NiFi global service "{name}"?`).

**HTTP Service card** (Application Service, REST API type): name, `StatusBadge` (Healthy/Failed/Not Tested), Base URL, "Last tested" timestamp. Actions: **Configure**, **Test** (spinner while pending), **Delete** (`confirm()`: `Delete HTTP Service "{name}"?`).

**Add Service dialog**: searchable combobox (debounced 180ms) listing every live NiFi controller-service type from `GET /api/nifi-services/types` **plus** one synthetic `"HTTP Service"` entry. Selecting a NiFi type + Name → `POST /api/nifi-services/` (`{name, nifi_type}`), then immediately opens the Configure dialog for the new service. Selecting "HTTP Service" instead routes to the HTTP dialog.

**Configure NiFi Service dialog**: fully dynamic — one control per NiFi property descriptor returned live by NiFi (`DynamicPropertiesForm`): allowable-values → `Select`; `sensitive` → password input (placeholder "Leave blank to keep unchanged"); else → `Textarea`. Save → `PUT /api/nifi-services/{id}` with `{properties}` (note: "Saving may temporarily disable and re-enable the service").

### ApplicationServices.tsx — full CRUD, 3 types

Card per service; type-specific description text; `StatusBadge`; Configure/Test/Delete actions identical in style to NifiServices' HTTP cards. Add/Configure dialog: Name, **Service Type** select (REST API / SMB / Webhook, **disabled when editing** — type is immutable after creation), Description, then type-specific fields:

- **SMB fields**: SMB Server Address (`smbHostname`), SMB Share (`smbShare`), Domain (`smbDomain`), Username (`smbUsername`), Password (`smbPassword`, secret input, "Leave blank to keep unchanged"), Connection Timeout (s) (default `"30"`).
- **Webhook fields**: Signature Header (`webhookSignatureHeader`, default `"X-Signature"`), Shared Secret (`webhookSecret`, secret input). `webhook_signature_algo` is always sent as `"hmac-sha256"` — no UI control for algorithm choice.

### HTTP Authentication Methods — exact enumeration

Defined **identically in two places**: `HttpServiceFields` (`NifiServices.tsx`) and `RestFields` (`ApplicationServices.tsx`). This applies only to REST API / "HTTP Service" type — **NiFi controller services have no fixed auth-type selector** (their "auth" is whatever arbitrary property descriptors NiFi reports, rendered by `DynamicPropertiesForm`).

`RestAuthType = "NONE" | "API_KEY" | "BEARER" | "BASIC"`. **Auth Type** select, exact option labels: **None**, **Bearer Token**, **Basic Auth**, **API Key**.

| Auth Type | Fields shown | Field name → input type |
|---|---|---|
| None | (none) | — |
| Bearer Token | Bearer Token | `bearerToken` → secret input, "Leave blank to keep unchanged" |
| Basic Auth | Username, Password | `restUsername` → text; `restPassword` → secret input |
| API Key | API Key Name, API Key Value, API Key Location | `apiKeyName` → text; `apiKeyValue` → secret input; `apiKeyLocation` → select (`HEADER`→"Header", `QUERY`→"Query Param") |

Always-present fields regardless of auth type: Base URL (`baseUrl`), Connection Timeout (s) (`connectionTimeout`, default `"30"`), Socket Read Timeout (s) (`readTimeout`, default `"30"`), Socket Write Timeout (s) (`writeTimeout`, default `"30"`), Socket Idle Timeout (s) (`idleTimeout`, default `"30"`), Rate Limit (req/min) (`rateLimit`, default `"120"`).

Payload keys sent: `base_url, auth_type, api_key_name, api_key_value, api_key_location, bearer_token, rest_username, rest_password, connection_timeout, read_timeout, write_timeout, idle_timeout, rate_limit`.

**No OAuth2, Session/Login-token, arbitrary Custom Headers list, or Client Certificate auth exists for Application Services.** (Client Certificate auth *does* exist, but only for **platform Connections** — Apicurio/NiFi/Kafka Connect — see §6, a separate concept from Application Services.)

### Sink/destination service config

**Neither Services page configures sink/destination services.** Iceberg sinks (`src/lib/icebergSinksApi.ts`) are not imported by either page — they're managed entirely from the Flows details panel (§2, Iceberg Sync tab) and surfaced read-only on the Dashboard. The Iceberg sinks API itself exposes **no create/edit config form** — sinks are auto-generated per flow/stream; the client only offers lifecycle operations (`enable/disable/pause/resume/restart`), a masked read-only `getConfig`, a `preflight` check, and Kafka Connect cluster health (`cluster()`). There is no per-sink-type field enumeration to document because none is user-editable in this alpha.

### Test Connection (Application Services only — NiFi services have no test action)
Button → `POST /api/application-services/{id}/test` → `{ok, health, message}`. Toast: `success` if `ok`, else neutral `"message"` style toast, using the backend's message or a fallback like "HTTP Service test finished". Spinner replaces the icon while pending; on completion the list refetches, updating the `StatusBadge` and "Last tested" text. No inline log/detail beyond the toast.

### Delete
Both pages use native `window.confirm()` (not a styled dialog) before deleting. **No client-side "in use by a flow" guard on either page** — deletion is attempted unconditionally; any backend rejection surfaces only as a generic `toast.error(err.message)`, with no special "service in use" messaging or dependency listing (contrast with platform Connections' delete, which does show an impact preview — §6).

---

## 5. OpenAPI Upload Flow (in Flow Designer, Step "Configure Source")

**Location**: appears only when `sourceType === "rest"`, in a bordered "OpenAPI Documentation (Optional)" panel in Step 1 (`FlowDesigner.tsx` ~line 6466). Shown regardless of whether the REST connection is set to Manual or Application Service mode.

**Step-by-step:**

1. **Upload control**: a plain "Upload" button triggers a hidden `<input type="file" accept=".json,.yaml,.yml,application/json,text/yaml,application/yaml">`. **File-picker only — no drag-and-drop, no paste-URL option.**
2. On file selection: `POST /api/openapi/parse` (multipart). On success, stores `spec_id`, filename, title, version, format, `warnings[]`, `servers[]`, defaults the "preferred server" to the first one. Upload button shows a spinner while parsing; toast confirms success/failure.
3. **No endpoint list loads at upload time.** A summary panel appears instead: title/filename, "version • format • operations count", a **Remove** button (detaches the spec entirely, `resetOpenApiSelection`), a **Preferred Server** dropdown (from `servers[]`), and an amber warnings box if the parse produced any.
4. **Endpoint discovery happens later, per-stream, in the Streams step** — not in Configure Source. When editing a stream on a REST source with a spec attached, the endpoint-path field becomes:
   - A free-text input (typing here clears any selected operation link).
   - Below it, a bordered mini-panel: a search box ("Search endpoints from OpenAPI", debounced 250ms) driving `GET /api/openapi/{specId}/operations?search=...&page=1&pageSize=200`, and a scrollable list of matching operations rendered as `{METHOD} {path}` buttons (with summary text), highlighting the current selection. "Loading endpoints…" / "No endpoint matches your search." are the loading/empty states. No method/tag filter UI is exposed even though the backend endpoint supports it.
5. **Selecting an operation** (`applyOpenApiOperationToStream`) sets the stream's method and endpoint path (normalized against the source base URL to strip overlapping path segments), and links `openApiOperationId`.
6. **Auto-population beyond method/path**: fetching the full operation detail (`GET /api/openapi/{specId}/operations/{operationId}`) drives a parameter-mapping build for every path/query parameter — auto-guessing which parent-stream extracted attribute each maps to via fuzzy name matching (confidence scored high/medium/low), and defaulting query params that have a spec-provided default value. This renders as an **"OpenAPI Parameter Mapping"** card: per binding, a path/query badge, name, required badge, Enable switch (path params locked-enabled), and a value input accepting a literal or a `${template}` token. **Body, headers, and auth are NOT auto-populated from the spec** — those remain whatever is configured at the source level.
7. **Interaction with Application Service selection**: these are independent, not mutually exclusive. There are three separate axes: (a) source-level "Connection Setup" (Manual vs. Application Service — controls where base URL/auth/timeouts come from), (b) OpenAPI spec attachment (works under either connection mode), (c) per-stream endpoint entry (manual typing vs. picking from the OpenAPI list — shown together, not tabbed; typing overrides/clears the OpenAPI link).
8. **Error states**: `openApiUploadError` for parse failure (invalid spec/malformed JSON-YAML), non-fatal `warnings[]` shown even on success, and the "no endpoint matches" empty state for narrow/failed searches.

Note: `openApiApi.attachToSource`/`detachFromSource` (`POST`/`DELETE /api/openapi/sources/{sourceId}/attach`) are defined in `src/lib/openapiApi.ts` but **not called anywhere in FlowDesigner.tsx** — the spec link is instead carried directly as `openapi_spec_id` on the Source payload.

---

## 6. Platform Connections / Settings — `/settings`, `src/pages/Connections.tsx` (1,506 lines, exports `PlatformConnectionsPanel`)

Fully real (`useQuery`/`useMutation` against `/api/connections/*`), despite the stale doc's "mock, local state" claim. `src/pages/Settings.tsx` is a 13-line wrapper (`AppLayout title="Platform Connections"`) that just renders `<PlatformConnectionsPanel showHeading={false} />` — "Settings" in this app **is** the connections panel; there's no separate theme/notification/user config.

### Connection types
`ServiceType = "kafka" | "apicurio" | "nifi" | "kafka_connect" | "iceberg"`. **Exactly one connection per type is allowed** — "Add Connection" is blocked once all 5 exist (toast: "All platform connection types are already configured."), and already-configured types show disabled in the Add dialog's type selector.

| Type | Label | Notable fields |
|---|---|---|
| `kafka` | Apache Kafka | Connection Method (Native / HTTP via Kafbat), then mode-specific fields (Bootstrap Servers + Security Protocol + SASL Mechanism/Username/Password for native; Kafbat URL + Username + Password for kafbat), Default Topic Prefix (default `"nif."`) |
| `apicurio` | Apicurio Schema Registry | Registry URL, Auth Type, Group ID (default `"nif-platform"`), + auth fields |
| `nifi` | Apache NiFi | NiFi API URL, Auth Type, + auth fields |
| `kafka_connect` | Kafka Connect | Kafka Connect URL, Auth Type (**None/Bearer/Basic only — Client Certificate intentionally omitted**, per code comment: the client doesn't support it and cert material isn't persisted), + auth fields |
| `iceberg` | Iceberg Catalog | Catalog URI, Warehouse (default `"bronze"`), Credential, OAuth2 Server URI, Scope (default `"PRINCIPAL_ROLE:ALL"`); S3 section: Endpoint, Access Key ID, Secret Access Key, Region (default `"us-east-1"`), Path-Style Access (Enabled/Disabled, default Enabled) |

### Shared Auth Type fields (apicurio, nifi, kafka_connect) — `AuthType = "NONE"|"BEARER"|"BASIC"|"CLIENT_CERT"`
- **None** → no fields.
- **Bearer Token** → "Bearer Token" (secret, "Leave blank to keep existing token").
- **Basic / Username + Password** → "Username" (text, required) + "Password" (secret, "Leave blank to keep existing password").
- **Client Certificate** → "Client Certificate" (text, hint "PEM text or certificate path") + "Client Key" (text, hint "PEM key text or key path") — **not offered for kafka_connect**.

### Test Connection — real
Click "Test Connection" on a card → `POST /api/connections/{id}/test` → `{ok, health, message}`. Spinner on that card's button; success/error toast from `result.message`. On success, invalidates `["connections"]` and `["dashboard"]`. A **"Test All"** button loops `testMutation.mutate(id)` for every connection (client-side sequential loop, not a batched server call).

### Add / Edit — real
Add: pick type (only not-yet-configured types selectable) → fill fields → client-side `validateConnectionForm` (URL format checks, required fields, comma-separated host:port validation for Kafka bootstrap servers, topic-prefix regex `^[A-Za-z0-9._-]+$`) → `POST /api/connections/`. Edit: secret fields always start blank (never pre-filled) so "leave blank to keep existing" semantics apply; `PUT /api/connections/{id}` (type field stripped from the update payload — immutable after creation).

### Delete — real, with an impact-check gate
1. Click Delete → immediately `GET /api/connections/{id}/impact?operation=delete` → `{flows, global_services, schema_versions, iceberg_sinks}` counts.
2. Confirmation dialog shows each count under labels "Flows depending on this", "Global services depending on this", "Schema versions depending on this", "Iceberg sinks depending on this".
3. If **any** count is > 0: warning banner "Warning: This connection has dependent items and cannot be deleted." and the confirm button is **disabled** — deletion is blocked entirely until nothing depends on it.
4. Only when all counts are 0 can the user confirm → `DELETE /api/connections/{id}` → invalidates `["connections"]`/`["dashboard"]`, toast "Connection deleted".

This is the **only** delete flow in the whole app with a pre-emptive dependency-count gate — contrast with Schemas (informational counts only, no block) and Services (no check at all).

### Status display
`StatusBadge` for `health` (Healthy/Failed/Not Tested); green "Active" pill if `is_active`; blue "Reachable"/"Unreachable"/"Unknown" pill if `reachability` is present; Kafka cards show "Connection Method" (Native/Kafbat); endpoint/URL box; "Last tested: {relative time}".

### Dashboard & Audit (brief, for completeness)
- `src/pages/Dashboard.tsx`: fully live now — `GET /api/dashboard/summary`, `/api/audit/?limit=6`, `/api/dashboard/flow-summary` (30s refetch), plus an optional Iceberg-sinks tile (`retry:false`, silently disappears if the endpoint is unavailable).
- `src/pages/Audit.tsx`: fully live — `GET /api/audit/?limit=100&search=...` with 400ms-debounced search, 15s refetch, manual refresh, and a **client-side CSV export** (builds the CSV from already-loaded rows; no dedicated export endpoint despite one being listed in the stale `BACKEND_API_ENDPOINTS.md` doc).

---

## 7. API Client Layer

### Base configuration — `src/lib/api.ts`
- Base URL: `VITE_BACKEND_URL` env var (`.env.local` sets `http://localhost:8010` for local dev), falling back to legacy `REACT_APP_BACKEND_URL`, falling back to `window.location.origin` (comment notes a Kubernetes ingress routes `/api/*` to backend port 8001 in deployed environments).
- Hand-rolled `fetch` wrapper, not axios: `api.get/post/put/patch/delete/postForm`. Auto-sets `Content-Type: application/json` unless the body is `FormData`.
- **No auth header injection anywhere** — this is an unauthenticated client (no bearer token, no cookies, no interceptor).
- Response handling: 204 → `null`; non-OK → parses FastAPI/Pydantic-style `detail`/`message` (including array-of-`{msg,loc}` validation errors) into a thrown `ApiError(status, message)`; a defensive check throws a 502 `ApiError` if the response body looks like an HTML page (guards against a misconfigured `VITE_BACKEND_URL` silently returning the SPA's own `index.html`).
- `timeAgo(iso)` relative-time helper used across the app.

### Endpoints by client file

**`src/lib/flowApi.ts`** — sources + flows (largest client): `GET/POST /api/sources/`, `GET/PUT/DELETE /api/sources/{id}`; `GET/POST /api/flows/`, `GET/PUT/PATCH/DELETE /api/flows/{id}`; `POST /api/flows/{id}/start|stop|deploy|undeploy`; `GET /api/flows/{id}/export`; `POST /api/flows/import/preview|credentials/validate|finalize`; `GET /api/flows/{id}/metrics|kafka-messages|runtime-errors`; `POST /api/flows/{id}/kafka-messages/clear`; `GET/PUT /api/flows/{flowId}/controller-services[/{id}]`; `GET/PUT /api/flows/{flowId}/processors[/{id}]`.

**`src/lib/schemaApi.ts`**: `GET/POST /api/schemas/`; `GET/DELETE /api/schemas/{artifactId}`; `DELETE /api/schemas/{artifactId}/versions/{version}`; `PUT .../versions/{version}`; `POST .../versions/{version}/generate` (unused/dead); `POST .../versions/{version}/verify`.

**`src/lib/nifiServicesApi.ts`**: `GET/POST /api/nifi-services/`; `GET /api/nifi-services/types?service_kind=`; `GET/PUT/DELETE /api/nifi-services/{id}`; `POST /api/nifi-services/{id}/enable|disable|set-default`.

**`src/lib/applicationServicesApi.ts`**: `GET/POST /api/application-services/`; `GET/PUT/DELETE /api/application-services/{id}`; `POST /api/application-services/{id}/test`.

**`src/lib/openapiApi.ts`**: `POST /api/openapi/parse`; `GET /api/openapi/{specId}`; `GET /api/openapi/{specId}/operations[?search&method&tag&page&page_size]`; `GET /api/openapi/{specId}/operations/{operationId}`; `POST/DELETE /api/openapi/sources/{sourceId}/attach` (defined, unused).

**`src/lib/inferenceApi.ts`**: `POST /api/schema-inference/start`; `GET /api/schema-inference/{jobId}`; `GET /api/schema-inference/flow/{flowId}?entity_stream_id=`; `POST /api/schema-inference/{jobId}/stop|accept`.

**`src/lib/icebergSinksApi.ts`**: `GET /api/iceberg-sinks/`; `GET /api/flows/{flowId}/iceberg-sinks?live=`; `POST /api/iceberg-sinks/{id}/preflight|enable|disable|pause|resume|restart`; `GET /api/iceberg-sinks/{id}/config`; `GET /api/kafka-connect/cluster`.

### Endpoints called directly from pages (bypassing a dedicated `*Api.ts` module)
- **`Connections.tsx`** (no `connectionsApi.ts` file exists): `GET /api/connections/` (30s poll), `POST /api/connections/{id}/test`, `PUT/POST /api/connections/`, `DELETE /api/connections/{id}`, `GET /api/connections/{id}/impact?operation=delete`.
- **`Audit.tsx`**: `GET /api/audit/?limit=100&search=` (15s poll) + client-side CSV export.
- **`Dashboard.tsx`**: `GET /api/dashboard/summary`, `/api/audit/?limit=6`, `/api/dashboard/flow-summary`.
- **`Flows.tsx`** and **`FlowDesigner.tsx`**: both also call `GET /api/connections/` directly (for populating Kafka/schema-registry pickers).
- **`FlowDesigner.tsx`** raw `fetch()` (not the `api` wrapper) to `POST /api/sources/{sourceId}/test-stream` for the "Test Stream" feature.

### Error handling & loading conventions
TanStack React Query (`useQuery`/`useMutation`) everywhere, with `refetchInterval` polling (commonly 10–30s, sometimes conditional on tab/state). Errors surface as `sonner` toasts (`toast.error(e.message)` pattern); a few endpoints return `{ok, message}` without throwing, handled via `toast[result.ok ? "success" : "message"](...)`. `ApiError.status` is checked via `instanceof` in exactly one place (FlowDesigner's inference-preflight 422 handling) to branch into a params-collection UI instead of a generic error. No retry/backoff logic beyond React Query defaults (one query explicitly sets `retry: false` for an optional Iceberg tile). The raw `fetch()` in FlowDesigner's stream-test feature is the one outlier that manages its own try/catch/local-state instead of using React Query.

---

## 8. Flow Creation Wizard — `src/pages/FlowDesigner.tsx` (10,543 lines)

### Steps (constants, `STEP_SOURCE_TYPE=0` .. `STEP_REVIEW=5`)
1. **Source Type** — cards: REST API, PostgreSQL (hard-disabled, "Temporarily unavailable"), MongoDB, SMB, Webhook, Trino.
2. **Configure Source** — source-type-specific fields (below), including the OpenAPI upload panel for REST.
3. **Streams** — the largest step: flow map, per-stream request/dependency config, response test/explorer, Advanced Behavior accordion (extraction, pagination, transformations, routing).
4. **Destination** — per-entity Kafka topic/partitions/replication/key-strategy + schema link/inference.
5. **Flow Schedule** — Interval vs. Cron (code-ordered before Streams in the file, but the visible step label/order is Streams-then-Destination-then-Schedule per the constants above — double-check against the live UI if exact on-screen order matters).
6. **Review** — summary + per-entity schema verification status.

**Stepper**: clicking any step label jumps directly (`setStep(index)`) with **no validation gating** on free navigation. "Next" validates only the step being left; the final step's save button validates **all** steps at once.

**Draft persistence**: `localStorage["nif-flow-designer-draft-v2"]`, loaded on mount unless `editFlowId` or `?new=1` is present. "Save Draft" writes to localStorage **and** calls `persistDraftToBackend()` (upserts Source + Flow via `sourcesApi`/`flowsApi`, forcing flow state to `"Draft"`). First backend save of a new flow navigates to `/flow-designer?editFlowId=<newId>` (`replace:true`), converting the session into edit mode.

**Edit mode**: `?editFlowId=<id>` → `flowsApi.get` → `sourcesApi.get`. If the source's `designer_payload` is "complete" (non-empty streams/kafkaOutput/schedule/valid sourceType), it's applied wholesale; otherwise the UI reconstructs state field-by-field from the legacy `Source`/`Flow` records and forces the user to `STEP_CONFIGURE_SOURCE`. A banner always reads *"...currently Stopped"* regardless of the flow's actual state — a static/cosmetic inaccuracy worth fixing in the new UI, not a behavior to preserve literally.

**Primary stream**: `setPrimaryStream` is defined but **never wired to any UI control** — dead code. Primary-stream assignment is implicit: the first-created stream is primary; deleting it reassigns the first remaining stream.

### Source-type config fields (Step "Configure Source")
- **REST**: Source Name; Connection Setup (Manual / HTTP Service); Manual mode — Base URL, Auth Type (None/Bearer/Basic/API Key, same shape as §4's enumeration) with conditional fields, 4 timeout fields, Rate Limit; OpenAPI upload panel (§5) always shown.
- **PostgreSQL/Mongo** (shared code path): Source Name; Mongo Connection Mode (Basic host/port, Mongo URI, or NiFi Global Service of kind `mongodb`); Database Name; Username/Password/SSL Mode when not URI/global-service; Connection Timeout.
- **SMB**: Source Name; Connection Setup (Manual / Service filtered to `service_type==="SMB"`); Manual mode — Server Address, Share, Domain, Username, Password; always — Base Directory Path, File Filter Pattern, File Format (CSV/JSON/JSONL/XLSX/EXCEL/BINARY); CSV mode adds Delimiter + Has Header Row.
- **Webhook**: Source Name; Connection Setup (Manual / Service); Webhook Endpoint ID (regex-validated, resolves to `/api/webhooks/<id>`, shown read-only); Content Type (auto/json/xml/text); Enabled switch; Manual mode adds Signature Header (default `X-Signature`), Signature Secret, read-only Signature Algorithm (`hmac-sha256`).
- **Trino**: Source Name, Endpoint, User; Iceberg source table (Catalog/Schema/Table); Checkpoint tracking table (Catalog/Schema/Table + auto-create switch); Column Selection (All / Manual with a textarea). **Trino is the only source type exempt from the schema-verification gate** (`requiresSchemaWorkflow` returns false only for `"trino"`).

### Streams step mechanics
Per-stream: **Flow Map** (embeds `StreamFlowMap`/`StreamFlowNode`, a read-only/select-only React Flow canvas — no dragging/connecting, click-to-edit); **Request & Dependency card** (name, method — filtered to methods the OpenAPI spec actually declares for the path, endpoint path with OpenAPI search per §5, Mongo query/projection/sort/limit, parent-child path-parameter mapping with auto-match status, optional body/headers/query-params accordion with `${var}` detection); **Test & Shape Response card** ("Test Stream" button posting to `/api/sources/{id}/test-stream`, prompting for template values and mutation confirmation on non-GET methods; response-format toggle; split-into-records; a recursive Response Structure explorer with click-to-select "Smart Suggestions" for record-list/record-root/field extraction; Selected Fields chip list); **Advanced Behavior accordion** with 4 sub-editors:
- **Extraction**: manual attribute-extraction rules (name, JSONPath/XPath, default value) with an exclusive "Kafka Key" switch per stream.
- **Pagination** (REST only): a **"Detect pagination"** button heuristically inspects the last test response (Link header → next-URL → cursor → offset/limit cascade) and auto-fills the form; manual controls for pagination type (Page Increment/Cursor/Offset-Limit/Next URL), stop conditions, max-pages safety limit, and where to send pagination values (Query vs. Body).
- **Transformations**: Add Field / Remove Field / Set-from-extracted-attribute rules.
- **Routing**: per-rule attribute/condition (equals/contains/starts_with/ends_with/regex/is_empty)/value/action (Include/Exclude/Route to Stream), a default-action for non-matching records (Drop / Route to default), and a "Parallel Branches" panel (create new branch, attach an existing independent root stream, remove children).

### Destination step
One card per entity stream: read-only generated Kafka topic (`bronze.<source>.<stream>__history`), Partitions, Replication Factor, Key Strategy (None / Primary Key), Value Format (fixed "Avro (Apicurio)"). **Schema Option** select: "Use Existing Schema" (artifact + version dropdowns) vs. "Auto Schema Inference" (the wizard-side inference UI described in §3). Trino entities show an informational note instead of the schema gate.

### Review step
Definition list of source/schedule/stream/pagination/fan-out summary + a card per entity showing topic/schema artifact/version and a verification badge; a warning banner with a "Back to Destination" shortcut if any entity's schema isn't verified (skipped for Trino).

### Supporting lib files
- `src/lib/flowDesignerConnectionState.ts` — reconciles REST/SMB/Webhook connection mode + application-service id across defaults, loaded Source, and draft payload.
- `src/lib/flowDesignerSchemaRequirement.ts` — `requiresSchemaWorkflow(sourceType)` (false only for Trino), status-label helpers.
- `src/lib/streamGraph.ts` — builds parent/child maps from fan-out, route, and default-route edges for branching/cycle logic.
- `src/lib/inferencePreflight.ts` — scans for unresolved `${var}` runtime placeholders before allowing inference to start.
- `src/lib/smbPathNormalization.ts` — sanitizes a clicked JSON path for SMB field-extraction suggestions.
- `src/lib/flowImportCache.ts` — cache-invalidation helper for the **Flow Import** feature, which despite its test files' naming (`flowImport*.test.ts`) actually lives entirely in `src/lib/flowApi.ts` and `src/pages/Flows.tsx`, not in `FlowDesigner.tsx` (see §1's Import Flow section for the full step-by-step).

---

## Appendix: files read for this audit

- `src/App.tsx`, `src/components/AppSidebar.tsx`, `src/components/StatusBadge.tsx`
- `src/pages/Flows.tsx`, `src/pages/FlowDesigner.tsx`, `src/pages/Schemas.tsx`, `src/pages/NifiServices.tsx`, `src/pages/ApplicationServices.tsx`, `src/pages/Connections.tsx`, `src/pages/Settings.tsx`, `src/pages/Dashboard.tsx`, `src/pages/Audit.tsx`
- `src/lib/api.ts`, `flowApi.ts`, `schemaApi.ts`, `nifiServicesApi.ts`, `applicationServicesApi.ts`, `openapiApi.ts`, `inferenceApi.ts`, `inferencePreflight.ts`, `icebergSinksApi.ts`, `schemaEditor.ts`, `schemaCreate.ts`, `schemaLayout.ts`, `flowTableSummary.ts`, `flowBulkSelection.ts`, `flowDesignerConnectionState.ts`, `flowDesignerSchemaRequirement.ts`, `streamGraph.ts`, `flowImportCache.ts`, `smbPathNormalization.ts`, `serviceManagerOptions.ts`, `pageSearch.ts`
- `src/components/flow-map/StreamFlowMap.tsx`, `StreamFlowNode.tsx`
