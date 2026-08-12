# Rework Plan — Review Round 1

2026-08-11 · Design plan for the approved rework. Verified against the real
code by a six-agent read-only mechanics pass. **No code changed yet.**

Decisions locked with the user:
- Global **Variables** page removed (per-flow variables stay).
- **APISIX** becomes its own top-level page with a catalog of named proxies;
  the http adapter links one by reference.
- **Schemas** = old master-detail workspace + old deep Avro editor, over
  ceremony semantics, **plus standalone hand-authored library templates** that
  a ceremony can pre-fill from (the "middle path").
- **Flow Builder** gets a real elevation system and progressive disclosure.
- **kc / kafka_kc** get a shared sink-config editor with `.json` upload.
- **Branching** becomes first-class on forms; the graph gains create / delete /
  re-parent — both driven by the same mutation code.

---

## 0 · The five findings that changed the plan

1. **The deep Avro editor is already in the prototype.** `src/lib/schemaEditor.ts`
   (547 lines) and its test file are byte-identical to the original app — the
   fork copied the whole frontend and only `pages/Schemas.tsx` was swapped.
   Its only import is a type-only import, erased at build. **Porting cost is
   zero.** The editor *components* (`SchemaTypeSelect`, `SchemaNodeEditor`,
   `SchemaFieldList`, `SchemaFieldRow`) live in the original page at lines
   65–407, are self-contained, and cut-paste into a new
   `src/components/schema-editor/` module.
2. **There is no cycle guard anywhere in the codebase.** `buildFlowGraph.visit`,
   `outlineDepths.depthOf`, `isRawBranch`, and `branchPathLabels` all walk
   parent/child links unguarded. One bad re-parent that puts a node under its
   own descendant is an infinite recursion — a hung browser tab, not a
   validation error. The guard must live in the mutation, before the write.
3. **The prototype has never read a real file.** `CeremonyDialog`'s "upload"
   only appends invented filenames; there is no `FileReader` in the codebase.
   The `.json` sink-config upload will be the first genuine file read — which
   is what the user asked for, so it gets built properly rather than faked.
4. **`cn()` will not dedupe custom shadow names.** tailwind-merge classifies
   `shadow-<unknown>` as a shadow *color*, so `cn("shadow-sm","shadow-e1")`
   emits both and CSS source order decides. The elevation scale therefore
   **overrides the stock keys** (`shadow-sm/md/lg/xl`) instead of inventing
   new ones.
5. **Seeded kc blocks have no entity.** Adding the spec-required entity label
   to kc retroactively invalidates seeded *Running* flows unless the seeds are
   backfilled in the same change.

---

## 1 · Architecture decision: lift the mutations out of the page

Today `addBlockFromEntry`, `createRouteBranch`, and `deleteBlock` are
`useCallback`s inside `FlowBuilder.tsx`. They are not exported, they call
`setSelectedId` *inside* a `setDraft` updater (double-fires under StrictMode),
and none of them check the edit lock — enforcement is "don't render the
button", which is exactly the guarantee that breaks the moment a second
surface (the graph, or the new Branches card) can trigger them.

**Move them to `src/prototype/mutations.ts` as pure functions:**

```
addBlock(flow, parentId|null, entry)      -> { flow, selectId }  | { error }
createRouteBranch(flow, blockId, ruleId, entry) -> { flow, selectId } | { error }
reparentBlock(flow, blockId, newParentId) -> { flow }            | { error }
deleteBlock(flow, blockId)                -> { flow, removed[] } | { error }
```

Every caller — page, form, graph — passes the current flow and applies the
returned one. This is what makes "a change in the graph shows in the form and
vice versa" true by construction rather than by discipline, and it gives the
lock/cycle/legality guards exactly one home.

Bugs to fix while the code moves (all pre-existing):
- `createRouteBranch` calls `defaultConfigFor` without `parentTopicId`, so a
  `kc` route branch is born with `attachTopicId: ""` and is then silently
  deleted by `syncFlowTopics`.
- The fork sibling counter filters only `adapter !== "kc"`, so route children
  count as forks — adding a fork beside two route branches produces `fork-3`
  with no `fork-1`/`fork-2`.
- `registryGlobalId = 3100 + state.schemas.length` collides after any delete;
  becomes a monotonic counter on state.
- Selection moves out of the state updater: the mutation returns the id, the
  caller selects.

---

## 2 · Foundations (Phase 0 — do first, everything depends on it)

### 2.1 Elevation & spacing
- Add a `boxShadow` scale to `tailwind.config.ts` **overriding** `sm/md/lg/xl`,
  with values driven by CSS custom properties so `.dark` can redefine them
  (a flat black shadow is invisible on `--card: 222 25% 11%`).
- Surface roles in the builder: **form = dominant** (raised), **outline = flat
  rail** (loses its Card), **map = recessed canvas** (inset, reads as a
  different kind of surface, not another panel), dialogs/popovers highest.
- Whitespace: 24–32px between major zones, 12–16px within; minimum annotation
  size 12px (currently 9–11px everywhere); max two metadata tokens per row.
- Do **not** touch `ui/card.tsx`'s structure — 25 live Cards across six pages
  consume it. Redefining what `shadow-sm` *means* upgrades them all coherently.
- Note (pre-existing, out of scope but recorded): `.dark` never defines
  `--success/--warning/--info`, and `bg-primary-muted` / `bg-destructive-muted`
  are dead classes — the CSS vars exist but no utility maps to them.

### 2.2 State migration — one bump, one wipe
`SEED_VERSION` 2 → 3, covering **all** model changes at once: drop
`globalVariables`; add `gatewayProxies`; add `schemaTemplates` (separate
collection — see 3.2); add `ApprovedSchema.approvals[]`; add `config.proxyId`,
`config.sinkConfig`, kc `config.entity`; backfill entity on every seeded kc
block; keep `seeds.ts:441`'s `topicOverride: "asset_retired"` and its
`topicMessages` key stable.

`store.ts` checks the version number only and reseeds silently — no shape
validation, no prompt. So: archive the previous blob under a backup key, show a
one-time notice explaining the reset, and fix the fact that **Reset demo data
is invisible when the sidebar is collapsed to the icon rail**.

Reminder for the whole rework: `block.config` is `Record<string, unknown>`, so
**no config-key change produces a TypeScript error**. Every rename must be
grepped, not trusted to `tsc`.

### 2.3 Mutations module + cycle guard
Section 1, plus `canReparent(flow, blockId, newParentId): string | null` in
`legality.ts` checking: block is not the root · new parent is not inside the
block's own subtree · adapter+mode is legal at the new position per
`computeAddMenu` · terminal and raw-branch rules · kc attaches to topics only.

---

## 3 · Schemas (the middle path)

### 3.1 What gets restored
From the original page, **lines 65–407** (the editor components) and
**454–596** (the structured⇄raw sync buffer), plus the master-detail shell:
left rail with search and filter chips, right detail pane with header actions
and a scrolled body holding `Structured Editor | Raw Avro JSON` tabs.

The sync pattern is adopted wholesale: `AvroRecord` is the single source of
truth, the structured tree is derived every render via `avroToStructuredFields`,
and a raw-JSON parse failure **keeps the last valid record** and only shows an
error — never destroys the user's work.

**Explicitly not restored:** the version/verify apparatus (original lines
806–874, 894–910, 958–1059). Porting it would reintroduce a second, contradictory
authoring model next to the ceremony.

### 3.2 Two record kinds

| | **Approved schema** | **Library template** |
|---|---|---|
| Born from | a ceremony on a kafka_kc block | the Schemas page, by hand |
| Registered | yes — `registryGlobalId` | no — "Template · not registered" |
| Bound to | one flow + block | nothing |
| Editing | read-only; **Re-run ceremony** to change | fully editable in place, Save |
| Delete | no (survives its block, per spec) | yes, freely |
| History | `approvals[]` — every past approval, read-only | none; Save overwrites |
| Actions | Re-run ceremony · Save as template | Start a ceremony from this · Duplicate · Delete |

**Templates live in their own `state.schemaTemplates` collection, never in
`state.schemas`.** Three things key off `state.schemas`: the Apicurio
connection's edit guard, its delete guard, and registry-id allocation. Mixing
unregistered templates in would lock the registry connection for the wrong
reason and corrupt global ids.

Spec position: this stays *inside* §9's existing pre-fill rule (*"A ceremony
may start pre-filled … from any approved schema picked in the Schemas
browser"*) — we widen the pre-fill source set to include unregistered
templates. Approval still happens per stream, inside the ceremony. Nothing
about approval-is-registration, one-flow binding, or no-evolution changes.

### 3.3 Ceremony changes
The Review step's flat 6-type editor (`CeremonyDialog` lines 340–410, plus
helpers `AVRO_TYPES`, `fieldsToAvro`, `avroToFields`) is replaced by the same
ported components — closing spec ledger item 24 (depth-5 structured editing
with raw-JSON protected nodes). The dialog widens to `max-w-4xl` with a
scrolling editor body; at depth 4–5 the indent guides eat ~80px of the grid.

"Start a ceremony from this template" needs a target: a picker listing
`kafka_kc` blocks grouped by flow, then navigating
`/flow-builder/<flowId>?ceremony=<blockId>&prefill=<templateId>`. The existing
`?ceremony=<blockId>` deep-link must keep working — it is the only path from
the Schemas page into authoring.

### 3.4 Landmines
- `AvroField` is declared twice with **incompatible shapes** (`types.ts:117`
  vs `schemaEditor.ts:50`). Any file importing both must alias.
- Converting the prototype's flat `AvroField[]` back into an `AvroRecord` is
  lossy and ambiguous — `type` is a free string and `children` can't
  distinguish record fields from array elements. **Always parse
  `rawAvro` with `normalizeAvroRecord` instead**, and treat `fields` as
  write-only derived display output.
- `normalizeAvroRecord` **throws** on a non-record root, a missing fields
  array, or an unnamed field. Template creation goes through
  `createEmptyAvroTemplate`; saves are wrapped.
- `approveSchema` deletes the prior record for `(flowId, blockId)` before
  pushing the new one — history must be read off the outgoing record inside
  the same mutation, or every re-run wipes it.
- Namespace: the ceremony derives it from the topic; `createEmptyAvroTemplate`
  defaults to `com.nif`. Re-plumb, or new approvals stop matching the seeded
  `raw.<flow>` convention.
- `structuredToAvroFields` silently drops any non-null Avro `default` on a
  round trip. Invisible in the seeds (all defaults are null); real on uploads.
- `bg-primary-muted` on the original page's selected-artifact row is a dead
  class — cut-pasting it ships an invisible selection state.

---

## 4 · APISIX

New top-level nav page **APISIX Gateway**:
- **Proxies** (primary catalog) — create/edit/test/delete named proxies:
  name, target host + port, SNI, timeouts, path + methods, optional client
  certificate profile, reconciliation status, "used by N flows".
- **Certificate profiles** and **Host allowlist** as secondary sections;
  allowlist stays admin-gated (spec: hosts are admin-allowlisted), proxy
  creation is self-serve.
- Header card showing which APISIX platform connection is Active, linking back
  to Platform Connections. The connection itself stays there — it is
  infrastructure identity; the new page is the catalog reconciled onto it. The
  gateway-resources modal retires; its button becomes a link.

**http adapter:** the boolean switch becomes a proxy picker writing
`config.proxyId`. `config.proxy === true` has exactly two logic readers —
`api.ts:387` (APISIX dependents → the activate/delete guard) and
`validation.ts:151` (deploy requires an active APISIX connection). Both move to
`proxyId`, and both get *stronger*: the referenced proxy must exist, be
reconciled, and its host allowlisted. Miss either and the guard silently
disappears with no type error.

Open, low-stakes: where the seeded `allowlist` lives after the modal retires
(state-level list, recommended) and whether cert profiles stay a shared
referenced collection (yes — `refCount` implies reuse).

---

## 5 · Flow Builder

### 5.1 Progressive disclosure
Block form becomes one surface with `Accordion type="multiple"` (the accordion
has open/close animations wired; `ui/collapsible.tsx` is a bare 9-line re-export
that would snap). Sections and default state:

| Section | Default | Collapsed summary |
|---|---|---|
| Identity | open | — |
| Adapter settings | open (required fields only; Advanced nested) | method + path / table / topic |
| **Branches & routing** (new) | open when it has children | "2 forks · 1 route" |
| Entity & derived names | open | entity → derived topic |
| Schema (kafka_kc) | always reachable | Approved #id / Ceremony required |
| Sink configuration (kc, kafka_kc — new) | open | plugin + N keys |
| Generic transformations | collapsed | "3 rules" |
| Test | collapsed | "Tested ✓ 2h ago" |
| Danger zone | collapsed | — |

**Force-open rules (must not be hideable):** Identity when the block has
validation issues (the issue list renders inside its header) · Entity when a
derived-name warning or topic collision exists (both are deploy blockers
surfaced nowhere else) · Schema always, because it is the ceremony entry point
and the `?ceremony=` deep-link target.

**Cross-link required:** "Detect from test" (in pagination, heading to
Advanced) is disabled until a test has run, and Test is a different, collapsed
section — burying both severs the discovery path. Pagination's Detect button
links to Test when no result exists.

### 5.2 Layout
`lg:grid-cols-[300px,1fr]` and the map's `h-[320px]` are load-bearing — a map
inside a 300px column is an unusable canvas, so map and outline do **not**
become tabs in one pane. Instead: outline is a flat rail (no Card), form is the
single raised surface, map is a recessed canvas band with real breathing room.
If the map is ever unmounted for a toggle, keep it mounted-and-hidden or xyflow
loses viewport state on every switch.

Verb bar demotes to status + Save + Deploy for never-deployed drafts, with the
full set behind the existing More menu — but its meta span is currently the
**only** place the DLQ name and the unsaved-changes indicator appear, and
`verbReason()` ("Save the draft first.") is exposed only through a `title`
attribute. Both must be relocated, not dropped.

Rule jargon (R1–R8) moves behind info popovers; refusal messages stay verbatim
per spec (*"every refusal explains itself in these words"*).

---

## 6 · Branches & routing (forms) + graph parity

### 6.1 The card
Sits **directly below adapter settings, above Generic transformations** — one
instance, so there is exactly one `AddBlockMenu` in the form. It lists the
block's children grouped as continuation / fork branches (inline renamable) /
route branches (with condition summary), each row selecting that child. Two
create actions: **Add parallel branch (fork)** and **Add conditional branch
(route)**.

Route rules stay inside Generic transformations for ordering semantics
("first match wins", dedup pinned last); the card is the discovery front door
and deep-links into the rule for condition editing. The transform entry gets
renamed so the vocabulary matches what users search for.

### 6.2 Landmines
- **Multiple `route` transforms per block are legal** — only dedup is
  uniqueness-guarded. A card reading `transforms.find(kind === "route")` hides
  branches; it must flatMap across all of them.
- **Orphan route branches**: deleting a rule, or flipping its action off
  "route", leaves a block whose `branch.ruleId` points at nothing — still drawn
  as a purple route edge and labelled `route "name"` in the outline.
- **Two independent name editors** for one concept: `RouteRule.name` and
  `FlowBlock.branch.name`, copied once at creation and never re-synced. The
  card exposes this immediately → rule name becomes the single source for route
  branches; the branch-name field goes read-only for them.
- The fork path of `addBlockFromEntry` is **not a mutation** — it sets state
  and opens a page-level dialog. A form-side fork button must funnel through
  the same dialog state or there will be two pickers.
- kc is lock-exempt while the rest of the block is not
  (`locked = flowLocked && adapter !== "kc"`), and kc is terminal — so the card
  simply does not render on kc blocks.

### 6.3 Graph gestures
Flip `nodesConnectable` and restore `deleteKeyCode` (both currently hard-off,
which makes every connection handler silently never fire). Then:

1. **Empty-state root node** on a blank canvas, using `computeRootMenu`.
   `onAdd` is typed `(parentNodeId: string, …)` in the map but `string | null`
   everywhere else — that mismatch blocks the root path today.
2. **Persistent ＋** on the selected node and chain tips (currently
   `opacity-0` until hover).
3. **Drag from a handle → release on empty canvas** → the same legality-gated
   menu opens at the drop point → creates and connects in one gesture.
4. **Re-parent**: dragging an existing edge endpoint (`onReconnect`, which is
   the only gesture that says *which* edge is moving) and node-to-node
   `onConnect`, both validated by the same pure `canReparent`.
   `isValidConnection` receives only `{source, target, …}` — no event, no node
   objects — so legality must be a pure function of ids plus the flow.
5. **Delete** via node toolbar and the Delete key, reusing the cascade with a
   preview. `NodeToolbar` keys off xyflow's internal `node.selected`, which
   this codebase never sets (selection is a bespoke `data.selected`), so it
   needs an explicit `isVisible`.

Also: a kc re-parent must patch `attachTopicId` in the *same* mutation —
`syncFlowTopics` deletes any kc block whose attach target no longer resolves.
Re-parenting silently renames derived topics (variant tokens come from branch
labels above the block), so the mutation should surface that. And the map
refits the camera on every node-count change with a 300ms animation — that
must be suppressed for user-initiated mutations or the camera yanks mid-gesture.

---

## 7 · kc / kafka_kc sink configuration

One shared `SinkConfigEditor` mounted on both, with `locked` passed **per call
site** (kafka_kc freezes at deploy; kc stays editable because Save is live):
- `connector.class` first-class, validated against a seeded mock catalog of
  installed Connect plugins (spec: the platform checks the plugin is installed).
- Arbitrary key/value rows below — reusing the existing headers/query kv-row
  pattern already in `BlockForm`.
- **`.json` upload**: a real `<input type="file">` + `FileReader` + parse,
  prefilling the editor, which stays the source of truth afterwards.
- **Locked keys rendered inline as disabled rows**: `topics=` bound to the
  attached/derived topic, the Avro converter, and — on recognized lakehouse
  sinks — the table name and auto-create/evolution. The locked `topics=` value
  is **computed at render from `deriveTopicName`, never persisted**, or it goes
  stale the moment a name changes.

Two spec violations fixed alongside:
- **kc gains its entity label.** Do *not* fix this by giving kc a `mode` — that
  leaks into service-type mapping, `hostsTransforms`, and legality. Add a
  targeted entity field, and backfill the seeded kc blocks or seeded Running
  flows start failing preflight.
- **kafka_kc gains its topic override.** `naming.ts:72` gates `topicOverride`
  on `adapter === "kafka"`; enabling the input without fixing that produces an
  override that is stored, displayed, and completely ignored — and the collision
  check would then run against the derived name instead of the typed one.
  Overriding also unlocks the sink's `topics=` key, per spec.

---

## 8 · Order of work

| # | Work | Why here |
|---|---|---|
| 0 | Elevation tokens · state migration (single SEED_VERSION bump) · mutations module + cycle guard · reset-visibility fix | Every later item is built inside these |
| 1 | Flow Builder disclosure + layout restructure | Answers the loudest complaint; is the container the new sections are born into |
| 2 | Branches & routing card | Verbatim ask; no model change; establishes the shared mutation path |
| 3 | Graph parity (root node, persistent ＋, delete, drag-to-create, re-parent) | Verbatim ask; reuses #2's paths; needs the cycle guard from #0 |
| 4 | Sink config editor + kc entity + kafka_kc override | Verbatim ask; one editor closes two spec gaps |
| 5 | Schemas page + editor components + ceremony swap | Bigger lift; uses #0's tokens |
| 6 | APISIX page + `proxyId` linking + Connections cleanup | Widest blast radius (nav, validation, seeds, http form) |
| 7 | Remove Variables page | Trivial; do it with the nav change in #6 so the sidebar changes once |
| 8 | Three spec contradictions (root http write default, mandatory max-pages) | Cheap correctness; no added complexity |
| 9 | Everything else from the spec sweep | **Documented backlog, not built** — the user's "too complicated" verdict is the scope constraint |

**Governing constraint:** every new section from items 2–7 must be born inside
item 1's disclosure system. Net visible surface should go *down*, not up, even
as capability goes up.

---

## 9 · Small calls made without asking

- Templates have no version history (they are freely editable; versioning
  existed in the old app only to serve the Draft→Verify pipeline). Approved
  schemas keep an approval history.
- "Save an approved schema as a library template" is in scope — ~10 lines, and
  without it the library starts empty forever.
- Approved schemas render in the detail pane **read-only** using the same
  editor components; changing one means re-running its ceremony.
- Deleting a template that pre-filled an approval is allowed; the approval
  keeps the template's name as a frozen string for its history line.
- The dead unrouted pages (`FlowDesigner.tsx`, `NifiServices.tsx`,
  `ApplicationServices.tsx`, `Settings.tsx`) are **not** touched in this round
  — unrelated churn.
