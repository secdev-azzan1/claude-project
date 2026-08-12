# Prior Audit & Work Notes — Digest

> Produced by analysis Agent C (2026-08-11) from
> `DataPASC-DataMobility\plan\alpha-ui-feature-audit.md` (470 KB, 2026-08-10)
> and `DataPASC-DataMobility\plan\work.md` (37 KB, 2026-08-09).
> Reference material for the UI prototype. Note: where these documents assume
> a full-screen canvas-primary rebuild, the prototype deliberately follows
> the user's fixed decision instead (form-centric + compact visual).

## What the audit is

A complete behavioural reference for the **Alpha** frontend (the lovable_ui
app), compiled from ten specialist audit reports, source-code-only, with
evidence tags and file:line citations. 24 chapters; classification system:
**A** = matches spec, port (~83) · **B** = spec-silent but useful (~36) ·
**C** = obsolete, do not port (~56) · **D** = ambiguous, needs product
decision (~39) · plus 21 explicit conflicts. Meta-finding: Flow Runner and
Connections are the strongest direct carry-overs; Service Manager, Schemas,
and the Flow Builder's structural model are being replaced wholesale.
`FRONTEND_UI_DOCUMENTATION.md` in the repo is stale and untrustworthy.

## Per-area verdicts

**Shell/nav.** Eleven routes → nine screens; Flow Builder has no sidebar
entry; `ApplicationServices.tsx` fully built but unrouted. No error boundary;
everything polls. Keep Schemas/Flows/Audit entries; drop "Service Manager";
replace "Settings" with direct "Platform Connections"; add entries for Flow
Builder, Application Services, Variables, Gateway Resources.

**Dashboard.** The spec never names a Dashboard (open product question #1 —
does the Flows list become the landing page?). "Total Sources" has no
countable entity in the new model; "Verified Schemas" state no longer exists.

**Audit Log.** Strongest simple keep (search, CSV export all A). Gaps:
100-row cap, CSV covers only loaded rows. Needs net-new event types
(Pause/Resume/Redeploy/Stop&Clear, ceremony Approve, Repoint/Activate,
service revisions, gateway changes).

**Connections.** Alpha's best area — per-type validation, per-row Test +
Test All, write-only secrets ("blank keeps existing"), health vs reachability
as two separately-recorded facts, impact-preview-guarded delete. Conflicts:
singleton-per-type → must become **multiple-per-type with an active marker**;
**Iceberg connection type removed**; 30s background polling → spec says none.
Net-new: **Redis and APISIX types**, **Repoint** (adopt/migrate/reset),
activation with progress, registry-connection protection.

**Service Manager.** Entire surface obsolete (live NiFi controller-service
management dropped). Survives conceptually as the HTTP service type; three
net-new service types (Database, External Kafka receiver, Sink destination).
Delete becomes logical retirement.

**Schema Manager.** Draft→Needs-Verification→Verified model replaced by the
four-step ceremony triggered only by kafka_kc. Schemas page survives as
read-only browser with one action ("start a pre-filled ceremony"). Big keep:
the structured recursive field-table editor (depth-5) synchronized with a raw
Avro JSON tab — same mechanism, relocated into the ceremony's Review step.

**Flow Runner / Flows list.** Richest screen — 16 A items: polling table
with client-side search, bulk verbs with per-row eligibility, import wizard,
visibility-gated tab polling, metrics cadence (verbatim spec match), Kafka
messages newest-first cap 50, and the block-reason guard functions ("the only
documentation of the business rules in the codebase"). Conflicts: Stop drops
queued data (spec: Stop retains; Stop & Clear separate); live processor/
service editing dropped; Runtime Issues → per-flow DLQ panel; `.flowpack`
export → versioned Connector export. Missing verbs: Pause/Resume, Redeploy,
Stop & Clear.

## Flow-builder findings

Central verdict: Alpha's builder is a **six-step wizard** (`FlowDesigner.tsx`,
10,543 lines: Source Type → Configure Source → Streams → Destination →
Schedule → Review) with a 320px collapsible React Flow display inside step 3
(nodes not draggable/connectable, positions from d3-hierarchy, edges derived
from config). The greenfield spec inverts this (canvas-primary). **The
prototype's resolution:** keep forms primary per the user's decision, but
restructure from linear wizard to per-block forms + always-visible compact
guided visual (see adapter-flow-ui-design.md).

Structurally obsolete in Alpha regardless of layout choice: ungated stepper
jumps; validate-after-construct model; flow name derived from Source Name
(spec: naming is the explicit first step); read-only derived topic names
(spec R7: custom override on kafka-family writes); Iceberg as a boolean
(spec: kafka_kc + Sink service + ceremony); five of six source types gone or
repositioned (Postgres/Trino → jdbc dialects; Mongo/SMB shelved-greyed;
Webhook absent); **credentials typed into blocks and persisted to
localStorage in plaintext** (spec reverses this exact pattern); append-only
rule lists (spec: user-ordered transforms, dedup last); `canEdit=true`
hardcoded.

**Strengths to preserve deliberately:** one-test-feeds-everything; test
failures as data; pagination auto-detect; OpenAPI param-binding fuzzy
matching with confidence scoring; response-tree click-to-configure;
`${placeholder}` prompting; confidence-annotated suggestion chips;
block-reason tooltips; honest microcopy. Fork model carries over (parallel
fan-out vs conditional routing, no-merge, no-cycles) but **fork naming is
new**. Usability lessons: no undo / no draft isolation; toast-only errors;
navigation doesn't scale (revive the never-rendered hierarchy navigator
idea); silent unsaved-work loss everywhere (no dirty flag, no route blocker).

**Net-new elements Alpha lacks:** topic nodes (incl. sealed kafka_kc ones),
Destinations panel, raw-branch quarantine (R8), kc dashed attachment (R5),
greyed future-scope entries, Deploy preflight as a named gate.

## work.md

The "Work Breakdown — Data Mobility Platform MVP": 104 tasks across four
weeks + 3 workstreams, derived from plan2.md + mvp.html (rulings 1–46, laws
R1–R8); "never overrides the spec"; a clean build with Alpha as reference.
Key frontend framings: W1.2 nav slots for Flow Builder / Schemas /
Connections / Application Services / Flows ("no source-type wizard"); W1.9
contract-generated block forms; W1.24-27 guided + menu from the placement
table, placement laws, topic nodes, greyed future-scope catalog; W2.19
Schemas read-only. Pins: alpha's Kafka-Connect→Iceberg connector shape is
authoritative; backend port 8010. No explicit "UI prototype phase" exists in
work.md — this prototype is a newer initiative.

## Direct implications adopted for the prototype

1. Model the five adapters — not source types; grey out NoSQL/File-share/
   extra-dialect families; omit webhook/syslog entirely.
2. Credentials always selected from an Application Service (manual mode =
   inline private service); never secrets in block forms or local drafts.
3. Include topic nodes, Destinations panel, named forks (`fork-N` defaults),
   raw-branch quarantine, kc dashed attachment.
4. Every kafka-family write: derived name preview + custom override
   (`raw.<source>.<entity>[.<variant>]`, tables `bronze.<source>.<entity>__raw`).
5. Iceberg is never a toggle — kafka_kc + Sink destination service +
   four-step ceremony (Approve = register).
6. Schemas screen read-only ("Approved" terminology), one action.
7. Connections: six types, multiple-per-type + active marker, health vs
   reachability badges, Test/Test All, impact preview, Repoint
   (adopt/migrate/reset), no background polling.
8. Flow naming is an explicit first step.
9. Lifecycle verbs incl. Pause/Resume/Stop & Clear/Redeploy; deploy
   preflight checklist; edit-lock on deployed flows (kc Save-is-live
   exception).
10. Ops honesty: DLQ panel (no replay), message viewer plain-text cap 50,
    "unavailable" never fake zero, three distinct clear verbs.
11. Carry over Alpha's proven interaction patterns (test-driven pickers,
    failures-as-data, block-reason tooltips, blank-keeps-secret).
12. Fix guard-rail gaps by design: styled confirmations with real impact,
    dirty-state tracking + navigation guards, per-block error badges,
    validation summary.
13. Don't invent beyond scope (no Flatten, fan-in, multi-source, DLQ replay,
    interval triggers, RBAC, user-authored adapters, NiFi services manager).
14. Open D-questions the prototype consciously answers are recorded in
    `unresolved-decisions.md` (e.g. Dashboard existence #1, config-panel
    placement #9, block-deletion model #14, Deploy/Start reachable from
    builder #26).
