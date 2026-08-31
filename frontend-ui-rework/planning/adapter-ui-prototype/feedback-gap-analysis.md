# Feedback Gap Analysis — Review Round 1

2026-08-11 · Synthesis of a 10-agent read-only audit run against the prototype,
`concept.html` (the authoritative spec), and the original `lovable_ui` app.
**No code was changed.** This document classifies every feedback point, adds
what the sweep found beyond the feedback, and lists the decisions needed
before rework starts.

---

## The headline

Your nine feedback points split into three kinds:

| Kind | Items |
|---|---|
| **A. Spec-vs-you conflicts** — the prototype followed `concept.html`, and what you're reacting to is *in the spec you made authoritative*. Needs your decision, not a bug fix. | Variables page · Schemas page · (partially) APISIX placement |
| **B. Real gaps** — you're right, it's missing, and in two cases the spec itself demands it. | kc sink config + JSON upload · branching from forms · conditional-branching discoverability |
| **C. Design failures** — the feature exists but the presentation fails. | Builder congestion / no shadows · graph creation affordances |

Plus: the full spec sweep found ~19 additional gaps (2 outright
contradictions), and the old-app parity sweep found 4 high-value patterns the
rebuild dropped.

---

## A. Spec-vs-you conflicts (decide before touching code)

### A1 · Variables page — it IS in your MVP

concept.html §12 (line 694): *"A small admin screen for global variables
(name, value, secret flag) plus a per-flow variables section in the builder;
the flow overrides the global"* — and requirement row 45 repeats it:
*"Variables screen (global + per-flow, secret-flagged, compiled to parameter
contexts)"*. The page wasn't invented; it was built from the spec.

That said, two honest observations from the audit:
- The page never explains its job (shared endpoints/regions across flows,
  secret values feeding parameter contexts and OpenAPI bindings), and it shows
  no usage ("used by N flows"), so it looks orphaned — which is why it read as
  pointless.
- Removal is cheap: flow-level variables (already in Flow settings) fully
  absorb the role; `validation.ts` placeholder resolution keeps working with
  the global union deleted.

**Recommendation:** demote, don't delete the concept — drop the global
Variables page + nav entry, keep per-flow variables in Flow settings, and
record the spec amendment (req 45 loses its "global" clause). Keep only if you
have real cross-flow shared values (one tenant region used by many flows).

### A2 · Schemas page — your request contradicts the spec's own decision

concept.html §9 is blunt: the Schemas screen is *"a read-only browser"* and
*"The old Draft → Needs-Verification → Verified pipeline, standalone
artifacts, version dropdowns, and delete cascades are gone"*; *"Exactly one
thing triggers schema work: configuring a kafka_kc block"*; *"approval is
registration, and re-running the ceremony is versioning"*.

So "keep the schema page just like the old application" has two readings:

1. **Old LOOK + EDITOR** (master-detail workspace, search + filter chips,
   status badges, version dropdown, Linked-Flows panel, the deep structured
   Avro editor ⇄ raw JSON sync) — **fully restorable** on top of ceremony
   semantics. Mapping: "Verify Version" → "Re-run ceremony" (deep-link to the
   owning kafka_kc block, pre-filled); Verified/Unverified filters →
   provenance filters (sample-validated vs manually authored); version
   dropdown → approval-history dropdown (each past ceremony approval as a
   read-only version).
2. **Old LIFECYCLE** (standalone Create Schema, Draft→Verify as a separate
   step, per-version/artifact delete) — **directly contradicts** the spec and
   would need a deliberate spec amendment.

**Bonus finding:** the ceremony's Review-step editor is far weaker than the
old app's — 6 flat scalar types vs the old editor's 8 primitives + 11 logical
types + object/array/map nesting to depth 5 + protected raw nodes. Spec ledger
24 *requires* the depth-5 editor, so **porting the original `schemaEditor.ts`
is mandatory regardless of which reading you pick** — it closes a spec gap AND
most of what you miss about the old page.

**Recommendation:** reading 1 — old master-detail workspace + ported deep
editor, keyed to ApprovedSchema records with an approval-history "versions"
array (`approveSchema` appends instead of replaces).

### A3 · APISIX — user override of the spec's placement (fine, but record it)

The spec keeps the gateway deliberately small and admin-scoped: connection in
§13, *"Gateway resources (admin)"* in line 695 (cert profiles → upstreams →
routes → allowlist, reference-counted deletion), and even says *"Redis and the
gateway are simpler on purpose"* (line 669). A top-level APISIX tab with
self-serve proxy creation is *your addition on top of the spec* — legitimate,
just record it as a deliberate override.

The good news: **the data model already contains your "proxy"** — an
upstream + route pair (types.ts:164-184). The design that fits your vision:

- New top-level nav entry **"APISIX Gateway"**: a **Proxies catalog** as the
  primary list (create/edit/test like a service), plus Certificate Profiles
  and Host Allowlist sections, plus a header card showing the Active apisix
  connection (linking back to Platform Connections).
- A first-class **GatewayProxy** object (name, target host/port, SNI,
  timeouts, path+methods, optional cert profile, reconciliation status, real
  refCount) composing today's upstream+route.
- **http adapter: replace the `proxy: on/off` Switch with a reference** —
  "Route via gateway proxy: [none | picker]" storing `proxyId`; picker filters
  by endpoint host, shows reconcile status, offers "create proxy" deep-link.
  Validation upgrades from "an apisix connection exists" to "the referenced
  proxy exists, is Reconciled, and the host is allowlisted" — making refCount
  real and delete-guarding work like Platform Connections.
- **Platform Connections keeps the connection itself** (admin/runtime URLs,
  credentials, Test, Active, Repoint); the Gateway-resources modal retires in
  favor of the new page.
- **Application Services: do NOT add a proxy field now** (you said "idk for
  sure for now") — recorded as a deferred option in unresolved-decisions.md.

---

## B. Real gaps (you're right — and mostly the spec agrees)

### B1 · kc sink configuration + JSON upload — biggest genuine miss

The spec is explicit — §5 kc: *"Config = key/value editor + .json upload"*,
*"Save is live"*, *"pending — created at deploy"*, post-create
Pause/Resume/Restart/Stop/Delete. The kc form today has only: subscribed
topic, initial position, a static "Save is live" note. The audit also found
three sibling violations:

- **kafka_kc has no sink-config surface either** (spec ruling 4/44:
  user-authored config, four locked keys on lakehouse sinks, topic override
  unlocks `topics=`) — today it's one descriptive paragraph.
- **kc never asks for its entity label** — spec: *"Entity label still
  required, so lineage never has holes"*; the `isWrite` gate excludes kc so
  the Entity card never renders.
- **kafka_kc topic-name override is missing entirely** — contradicts R7/ruling
  1 ("any kafka-family write offers an advanced field").

**Plan:** build ONE reusable sink-config editor (key/value rows with
`connector.class` first-class + "Upload .json" that parses and prefills +
locked/derived keys shown read-only, incl. `topics=` bound to the attached
topic) and mount it on both kc and kafka_kc; validate `connector.class`
against a mock installed-plugins list; add the entity field to kc; add the
kafka_kc topic override wired into naming.ts.

### B2 · Branching from forms — correct, zero affordance exists

Fork creation lives only in the outline's per-row ＋ menu and the graph node ＋.
The BlockForm — the primary surface of a form-centric UX — has only a passive
branch badge and a rename field for an *existing* branch. A user working
purely in forms never encounters forking.

**Plan:** a first-class **"Branches & routing" card** on every record-carrying
block's form (between adapter settings and transforms):
- lists all children grouped as continuation / fork branches (inline
  renamable) / route branches (condition summary shown), each row jumping to
  the child;
- two create actions: **"Add parallel branch (fork)"** (reuses the existing
  fork dialog + legality menu) and **"Add conditional branch (route)"**
  (creates/reuses the route transform, appends a rule, opens the branch-block
  picker);
- both write to the exact same model as the graph/outline paths, so sync is
  automatic;
- parent header badge ("2 forks · 3 routes"), and route children show the
  driving condition with a link back to the parent rule;
- the silent fork-on-second-child gets a one-line explanatory notice.

### B3 · Conditional branching — exists, but 5–6 clicks deep under the wrong name

Today's path: select block → scroll to "Generic transformations" → "Add
transformation" → pick **"Route / filter"** (1 of 8 kinds) → "Add rule" → set
action to "route" → only then a "+ branch block" button appears. The words
"branch", "branching", "conditional" appear nowhere until the final step.
That's why you couldn't find it — the spec actually places routing inside the
transforms section (line 132), but its assumed discoverability failed contact
with a real user.

**Plan:** the Branches & routing card (B2) becomes the discovery front door
for the same rules; the transforms entry stays for ordering semantics but gets
renamed ("Route / filter — conditional branching") and cross-linked.

---

## C. Design failures

### C1 · Congestion + no shadows — confirmed, with root causes

- **No elevation system exists at all**: `Card` is hard-coded `shadow-sm`
  (ui/card.tsx:6); tailwind.config.ts extends no boxShadow scale; index.css
  defines no elevation tokens; `--background` (220 20% 98%) vs `--card`
  (white) barely differ → every surface reads as one flat plane. This is
  precisely "no shadows so everything looks mixed up".
- **BlockForm renders 5–7 fully-expanded cards, no progressive disclosure** —
  an http block's form is ~3,000px tall with ~15 controls visible at once in
  HttpSettings alone.
- **Three simultaneous representations** of the same flow (map + outline +
  form), each repeating name/adapter/entity/branch/issues, each with its own
  ＋ menu; the map is open by default despite being subordinate to forms.
- Micro-typography (9–11px) everywhere; uniform 16px spacing with no
  whitespace hierarchy; R1–R8 rule jargon inlined in nearly every section.

**Plan:** (1) a 3–4 step elevation token scale + slightly darkened page
background, one dominant surface per screen; (2) BlockForm → single card with
accordion sections (identity + required adapter fields open; the rest
collapsed with one-line summaries); (3) HttpSettings splits required from an
"Advanced" disclosure (headers/query/body/pagination/proxy); (4) map and
outline become alternatives, not siblings; (5) whitespace hierarchy 24–32px
between zones; min 12px annotations; max 2 metadata tokens per row; (6) R-rule
citations behind info-popovers (refusal messages stay verbatim per spec); (7)
staged first-run: name → "Place the root" → builder reveals as blocks exist;
verb bar demoted to Save + Deploy until first deployment.

### C2 · Graph creation — sync already works; the gap is affordances

Verified: there is exactly ONE draft state (`FlowBuilder.tsx:96`) and map,
outline, and forms all render from it — **two-way reflection already holds by
construction**. What's missing is creation parity:

- **No root placement from the graph** — a new flow's map is an empty dead
  canvas (root menu lives only in the outline). → Add an empty-state "+ Place
  the root" node.
- **＋ buttons are hover-only** (opacity-0 until hover). → Persistently
  visible on selected + chain-tip nodes.
- **No delete from the graph** (deleteKeyCode nulled; delete lives only in the
  form's danger zone). → Node toolbar/context-menu + Delete key, reusing the
  existing cascade, with a confirm preview.
- **Drag gesture**: the spec forbids freehand edges (*"you never draw lines by
  hand"*, line 126) and forks never merge (R4) — so the reconcilable design is
  **drag-from-handle → release on empty canvas → opens the same
  legality-gated AddBlockMenu** (xyflow v12 `onConnectEnd`); node-to-node
  connections stay refused (`isValidConnection` false). The gesture creates
  via menu, never a raw edge.
- Configuration stays forms-only: every graph creation lands on
  `addBlockFromEntry`, which already auto-selects the new block so its form
  opens — that handoff is the core loop.
- Nodes stay non-draggable/auto-laid-out (positions aren't in the model).

**Open reading to confirm:** did "make connections through the graph" also
mean binding *service* connections from the graph (pick an AppService when
dropping an http node)? Assumed structural wiring only.

---

## D. Beyond your feedback — spec sweep + old-app parity highlights

### Hard contradictions (cheap fixes, should do regardless)
1. **kafka_kc topic override missing** (R7/ruling 1) — see B1.
2. **Root http write defaults to "original"**; spec says it *"defaults to
   parsed response"*.
3. **Pagination lacks the mandatory max-pages/max-records hard limit** — spec
   calls it mandatory ("a flow can never loop forever"); Detect must always
   set it.

### Notable omissions (candidate backlog, NOT all for this round)
- OpenAPI parameter **bindings with value sources** (parent field / static /
  variable / **ask-at-runtime**) + preflight refusal on unresolved
  ask-at-runtime — currently absent end to end.
- **jdbc write** reduced to a sentence: no INSERT/UPSERT choice, no match
  columns, no field→column mapping editor (Story 4 unconfigurable).
- **Record key field picker** on kafka-family writes (ruling 35) — absent.
- **Preflight missing half the spec checklist** (topic reservations, gateway
  routes, plugin installed, DLQ 110 MiB, ask-at-runtime).
- **Ops verbs**: run history per run, Clear Topics w/ ownership proof,
  offset-skip, Clear dedup cache, drift force-repair, reader-registry warning
  on undeploy/delete.
- **Connector sharing depth**: dual export (rendered artifacts + manifest),
  block-by-block update diff, "modified from X" lineage.
- Redis switch warn-with-counts; auto-promotion banner; installed-plugins
  read-only list; lookup failure toggle; ceremony live-run preconditions.

### Old-app patterns worth carrying over (parity sweep)
- **Field pickers over the tested shape** — old app let you click the response
  tree to fill fields; new transforms/route/dedup/binding fields are all
  free-typed. This is ALSO a spec promise ("Downstream blocks then offer field
  pickers over the tested shape") and is exactly the hands-on richness you
  missed in Schemas.
- **Key-field click-to-toggle** from the tested tree (old `primaryKeyFields`).
- **Draft persistence**: the old designer autosaved drafts to localStorage;
  the new builder loses ALL work on an in-app sidebar click (only
  `beforeunload` guards a tab close). Needs draft-per-flow persistence +
  in-app navigation blocker.
- **Import ceremony**: old flowpack import had preview → credential validation
  → naming; the new connector import jumps straight to binding.
- XML/CSV sample parsing in the response explorer (spec: "JSON, CSV, XML,
  raw").

---

## E. Cross-cutting constraints (critic findings)

1. **The complexity paradox**: almost every fix above ADDS form surface while
   your loudest complaint is "too complicated". Governing rule for the rework:
   **every new section must be born inside the C1 accordion/elevation system**,
   and most of section D gets *deferred to a documented backlog*, not built
   now. Your "too complicated" verdict is the scope constraint.
2. **"Process" complexity vs pixel complexity**: the audits fixed pixels; if
   after the redesign the *workflow* still feels heavy (nouns, steps to first
   flow), that's a separate conversation about the model itself.
3. **Sequencing**: the elevation/token system must land FIRST — the restored
   Schemas page and the new APISIX page must be built with it, or they get
   styled twice.
4. **One coordinated migration**: APISIX proxies, schema versions, drafts, and
   kc connector state all extend PrototypeState — one SEED_VERSION bump with a
   single migration, not piecemeal.
5. **Shared mutation paths**: form-side branch creation (B2) and graph-side
   creation (C2) must call the identical `addBlockFromEntry` / route-rule
   code so your bidirectional-sync requirement holds for free.
6. **Edit-lock parity**: graph create/delete affordances must show the same
   refusal ceremony as forms when a flow is deployed.

---

## Proposed execution order (pending your decisions)

1. Answer the decisions below (5 minutes, gates three work streams).
2. Elevation tokens + Flow Builder progressive-disclosure restructure (C1).
3. Branches & routing form section (B2 + B3).
4. Graph creation parity (C2).
5. kc/kafka_kc sink-config editor + JSON upload + entity/override fixes (B1).
6. Schemas page rebuild (old workspace + ported deep editor) (A2).
7. APISIX page + proxyId linking + Connections cleanup (A3).
8. Spec hard contradictions (D1–D3).
9. Selected old-app parity items (tested-shape pickers first — three features
   depend on it).
10. Everything else in D → documented deferred backlog.

---

## Decisions needed from you

1. **Variables**: demote to flow-level only (amend spec req 45) — recommended
   — or keep the global page but make it earn its place (usage counts,
   delete-guarding)?
2. **Schemas**: old look + deep editor on ceremony semantics (recommended), or
   do you want the full old *lifecycle* back (standalone create, Draft→Verify,
   deletes) — which means overriding concept.html §9?
3. **APISIX**: confirm self-serve proxy creation with the host allowlist
   staying admin-gated? And must every proxied http block explicitly pick a
   proxy, or should plain `proxy: on` survive as an auto-match fallback?
4. **Graph "connections"**: structural wiring only (assumed), or also binding
   Application Services from the graph?
5. **Scope**: agree that section D beyond the three hard contradictions is
   deferred backlog for this round?
