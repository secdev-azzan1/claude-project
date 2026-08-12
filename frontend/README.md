# Data Mobility Platform — Adapter UI Prototype

A **frontend-only, interactive prototype** of the adapter-based UI direction
for the Data Mobility Platform, built as an isolated copy of the existing
`lovable_ui` frontend. The original application is untouched.

## What this is

- The existing form-centric app, evolved to the new **adapter-based flow
  model** (five adapters: `http`, `jdbc`, `kafka`, `kafka_kc`, `kc`).
- **No backend.** Every operation — test, deploy, ceremony, repoint — is
  simulated in the browser. State persists in `localStorage` and survives
  refreshes. Use **Reset demo data** (sidebar footer) to restore the seeds.
- Seeded with realistic security-integration dummy data (Rapid7, FortiSIEM,
  ServiceNow, APISIX gateway, Redis dedup, Iceberg/OpenSearch sinks) in many
  states: Running, Paused, Stopped, Draft-with-issues, Degraded-drift,
  failed connections, retired services, and all three schema provenances.

## Run it

```bash
npm install
npm run dev        # http://localhost:3001  (original app keeps port 3000)
npm run test       # vitest — pure-logic and page tests
```

## Where to look

| Area | Path |
|---|---|
| Flow Builder (the centerpiece) | `src/pages/FlowBuilder.tsx` + `src/components/flow-builder/` |
| Legality rules (R1–R8 → + menus) | `src/prototype/legality.ts` |
| Naming walk / derived names | `src/prototype/naming.ts` |
| Validation & deploy preflight | `src/prototype/validation.ts` |
| Mock service layer | `src/prototype/api.ts` |
| Seed dataset | `src/prototype/seeds.ts` |
| Store (localStorage) | `src/prototype/store.ts` |
| Planning & design docs | `planning/adapter-ui-prototype/` |

Legacy pages from the stream-based app (`FlowDesigner.tsx`, `NifiServices.tsx`,
`Settings.tsx`, `ApplicationServices.tsx`) remain on disk unrouted, for
reference and diffing.

## Review guide (suggested walk)

1. **Flows** → open *FortiSIEM Events* → see branching, topic nodes, a kc
   subscription, DLQ tab, metrics.
2. **Flow Builder** → *Vulnerability Scan Delta* (Draft) → validation issues,
   fix them, run the schema ceremony, deploy via preflight.
3. **Flow Builder** → *Asset Retirement* → named forks + the naming walk +
   a custom topic name.
4. **Flow Builder** → *Audit Mirror* → raw-branch quarantine (R8). The flow
   is seeded Degraded (drift showcase), which locks editing — click **Stop**
   first to explore the quarantined ＋ menus.
5. **New Flow** → naming-first step, guided root selection, + menus.
6. **Platform Connections** → Redis & APISIX types, Activate/Repoint,
   gateway resources, impact-preview delete.
7. **Application Services** → four types, revisions, retire/reinstate.
8. **Schemas** → read-only browser → "Start pre-filled ceremony".
