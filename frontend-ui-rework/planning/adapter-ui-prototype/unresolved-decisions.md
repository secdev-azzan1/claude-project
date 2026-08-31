# Unresolved Product Decisions

2026-08-11 · Things the prototype had to pick a side on (or leave open) that
deserve an explicit decision before the real migration. Where the prototype
made a choice, it is stated — review it while clicking through.

1. **Does the Dashboard survive?** The new direction never names a Dashboard;
   the greenfield work-breakdown's nav has none (open question #1 in the
   prior audit). *Prototype keeps it* with re-based KPIs, on the brief's
   "preserve what works" default. Decide: keep, or make Flows the landing
   page.
2. **Form-centric builder vs canvas-primary.** The greenfield spec makes the
   canvas the authoring surface; the brief fixes forms + compact visual.
   *Prototype follows the brief* (outline + per-block forms, display-only
   map with ＋ menus). If the canvas ever needs to grow, the legality engine
   and graph modules carry over unchanged — but that's a later decision.
3. **Where Deploy/Start live.** *Prototype puts the full verb bar in the
   builder* AND keeps all verbs on the Flows list (audit open question #26).
   Alternative: builder only validates/saves, ops verbs live only in Flows.
4. **Fork creation UX.** *Prototype*: adding a second child auto-names both
   branches (`fork-1`/`fork-2`, editable), plus an explicit "Fork into
   branches" menu entry. Alternative: an explicit fork dialog naming all
   branches up front.
5. **Route branches as blocks.** *Prototype* binds a route rule to a child
   block (branch chip shows the rule); passive routed records without a
   destination block are not representable. Confirm this matches intended
   routing semantics (the old app allowed passive route children).
6. **kafka read root without adoption.** *Prototype* allows a kafka read
   root with a typed topic name (platform cluster) in addition to
   adopted-topic roots. Confirm whether every consumed topic should instead
   be forced through adoption (a visible topic node).
7. **Schemas page lifetime.** Read-only browser kept as its own nav entry.
   Alternative: fold it into the flow detail (schema chips) and drop the
   page.
8. **Application Services vs "Connections" naming.** Two nav entries
   ("Platform Connections" admin vs "Application Services" user-facing)
   follow the spec, but the words are close; confirm the labels.
9. **Variables scope.** *Prototype*: global Variables page + per-flow
   overrides in Flow settings. Ask-at-runtime values exist only in the Test
   dialog. Confirm that's the full intended surface for MVP.
10. **Connector import ending.** The mocked wizard stops before creating a
    flow. Decide the real import's last step: create Draft flow immediately
    vs land in the builder for review-before-save.
11. **Degraded/drift representation.** *Prototype* shows drift text on a
    Degraded flow (banner + tooltip). The direction also describes explicit
    audited repair actions ("really deleted / deployed elsewhere /
    unreachable") — not prototyped; needs product wording.
12. **Dedup UI granularity.** Dedup is per-stream (one cache per block) per
    the direction; the old open question "per stream vs per flow" is treated
    as settled = per stream. Confirm.
13. **Edit-lock strictness.** *Prototype* refuses edits in Running, Paused,
    Deploying AND Degraded (any not-stopped state), kc excepted. Confirm
    Degraded should be locked too.
14. **Navigation guard depth.** Only `beforeunload` guards unsaved builder
    changes (BrowserRouter limitation). The real app should adopt the data
    router + in-app route blocking — decide during migration.
