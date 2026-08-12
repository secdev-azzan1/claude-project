# Prototype Changes — Migration / UI Summary

2026-08-11 · what the prototype changed relative to the original `lovable_ui`
frontend, and why. The original application is untouched; this folder is an
isolated copy.

## Final classification per UI area

| Area | Classification | Notes |
|---|---|---|
| App shell / layout / theming | **UNCHANGED** | Same `AppLayout`, tokens, header contract, react-query + sonner conventions. |
| Sidebar navigation | **MODIFIED** | Workspace: Dashboard, Flows, Schemas, Application Services, Audit Log · System: Platform Connections, Variables. "Service Manager"/"Settings" entries retired; prototype badge + Reset demo data in footer. |
| Dashboard | **MODIFIED** | KPIs re-based on the new nouns (Flows / Running / Approved Schemas / Connections healthy x/y); Needs-attention panel added; "Total Sources" and "Verified Schemas" retired. Page's continued existence is an open decision (see unresolved-decisions.md). |
| Flow Designer (6-step wizard, 10.5k lines) | **REPLACED** | By the Flow Builder: outline + per-block forms + compact guided visual. Old file kept on disk, unrouted; `/flow-designer` redirects. |
| Flow Builder | **NEW** | `/flow-builder/:id` — see `adapter-flow-ui-design.md` §3 and the explanation below. |
| Flow map (visual) | **EXTENDED** | Same display-only React Flow architecture (data → pure graph → auto-layout); extended with topic pill nodes (adopted/sealed), dashed kc edges, branch-name edge labels, legality-filtered ＋ menus, error badges, cron/tested chips. |
| Flows list ("Flow Runner") | **EXTENDED** | Table/bulk/detail-sheet patterns and the block-reason guard system preserved; new verbs (Pause/Resume, Stop & Clear, Redeploy), Runtime Issues → DLQ tab, live NiFi Processors/Services editing → read-only Blocks & Services, flowpack → Connector export/import, per-flow "Update available"/"Action required" chips. |
| Schema Manager | **REPLACED** | Draft→Needs-Verification→Verified workspace removed. Schemas is now a read-only browser (search, provenance, registry id, owning flow/block, View dialog) whose one action starts a pre-filled ceremony. |
| Schema ceremony | **NEW** | 4-step modal (Declare → Orchestrate → Review → Approve=register) with three evidence paths, structured⇄raw Avro editor (mechanism carried over from the old Schema Manager), provenance flags, deploy gate. |
| Platform Connections | **EXTENDED** | All good patterns kept (typed dynamic forms, Test, write-only secrets, impact-guarded delete). Extended: multiple-per-type + Active marker, health *and* reachability as separate facts, **Redis** and **APISIX** types, Iceberg type removed, Repoint (adopt/migrate/reset) with live progress, gateway resources manager (cert profiles/upstreams/routes/allowlist), no background polling. Promoted to its own nav entry. |
| Service Manager (NifiServices) | **REPLACED** | NiFi controller-services management dropped (out of scope by direction). Replaced by Application Services. |
| Application Services | **NEW** (concept **EXTENDED**) | Four types (HTTP incl. session-token & OAuth2 auth, Database with dialects + greyed future dialects, External Kafka receiver, Sink destination); revisions on edit; logical retirement + reinstate; dependent-flow popovers; "manual mode" = inline private service creation from the builder. |
| Variables | **NEW** | Global variables admin (secret masking, blank-keeps-existing); flow-level overrides live in Flow settings. |
| Audit Log | **UNCHANGED** (data source swapped) | Same table/search/CSV/refresh; now reads the mock store; seed events extended with the new verbs. |
| Settings page | **REMOVED** | Was only a wrapper; `/settings` redirects to `/connections`. |
| Iceberg sink toggle + tabs | **REPLACED** | Iceberg is a kafka+connect block + Sink destination service + ceremony; Connect states shown in the flow detail Connect tab. |
| Old source types | **REMOVED / repositioned** | Mongo + SMB → greyed "coming later" families; Webhook/syslog absent by direction; PostgreSQL/Trino live on as Database-service dialects. |
| StatusBadge / block-reason pattern | **EXTENDED** | New statuses (Approved, Active, Reachable/Unreachable, Retired, Update available, Action required, Ceremony required, Sealed, Adopted…). Block-reason contract now also drives every flow verb. |
| localStorage persistence | **EXTENDED** | Whole mock state persists (`dmp-adapter-prototype-v1`); Reset restores seeds. |

## Flow-builder: what changed from streams to adapters

- **Model**: six stringly-typed source types + one flat 70-field Stream record
  → five adapters (`http`, `jdbc`, `kafka`, `kafka_kc`, `kc`) × modes, each
  block a small typed record with per-adapter config + shared transforms.
- **Structure**: linear 6-step wizard → one builder page. The wizard's
  step-gated validation became per-block validation with an outline badge +
  clickable flow-level summary.
- **Form-centric behaviour preserved**: every block is a form; the outline is
  the navigation spine; flow-level settings (name/cron/variables/validation)
  are a form. The visual is display-only apart from select and the
  legality-filtered ＋ menus — nodes cannot be dragged, edges cannot be drawn.
- **Adapters are configured** through: identity section (name, service
  selector — credentials never on the block, private-service inline
  creation), adapter settings section (dynamic per adapter+mode: request/
  parsing/pagination/proxy for http; table/columns/incremental for jdbc;
  adopt/parse/initial-position for kafka; sink service for kafka_kc/kc),
  entity + derived-name section (naming walk, custom topic override with
  collision/`raw.*` warnings), shared Generic-transformations section
  (ordered, dedup pinned last, absent on kc/raw branches), per-block Test
  (placeholder prompts, mutating double-confirm, failures-as-data, field
  chips → extraction rules).
- **Branching**: adding a second child to a block creates named fork branches
  (`fork-N` defaults, editable in the branch's form; labels appear on the
  visual's edges and pre-fill topic variant tokens). Routing rules inside the
  transforms section create purple route branches bound to the rule. Topics
  are first-class pill nodes; kc sinks attach to topics with dashed edges;
  kafka_kc topics are sealed. A Destinations panel lists topics + sinks.
- **Lifecycle**: verb bar (Deploy via preflight checklist · Start · Pause ·
  Resume · Stop · Stop & Clear · Redeploy · Undeploy · Delete) with
  block-reasons on every verb; edit-lock while deployed & not stopped (kc
  "Save is live" exempt); cron-only trigger with presets + next-occurrences
  preview, hidden for continuous/topic-rooted flows.

## What was deliberately NOT built (documented simplifications)

- Repoint runs a simulated 4-step progress log, not a real migration engine.
- Connector import uses a canned bundle (no real file parsing); finalizing
  creates a bound Draft flow and opens it in the builder.
- Cron preview is canned for presets; not a cron engine.
- Naming-walk step 2 ("identical populations collapse") is approximated.
- Ops metrics are static seed snapshots; no time series.
- No dirty-navigation route blocker (only `beforeunload`) — BrowserRouter
  (non-data-router) limitation; noted for the real migration.
- No dark mode / mobile pass (parity with the existing app).
