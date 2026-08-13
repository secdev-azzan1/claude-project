# Browser UI Verification Pass (live frontend :3001 -> live backend :8010)

Date: 2026-08-13. Chrome via MCP tools; frontend dev server (vite :3001), backend uvicorn :8010,
real infra behind it. E2E agents were concurrently mutating state — their effects were visible
live in the UI, which itself validates the integration.

| Area | Verified | Evidence |
|---|---|---|
| Dashboard | PASS | Live KPIs: 6/6 connections healthy; trimmed "Sink connectors" card w/ minimal "1 not deployed" hint (T8.U1); Flow Status showing live e2ea flow; Recent Activity = real audit trail (service tests, flow created) |
| Connections | PASS | 6 seeded connections, Active/Healthy/Reachable badges, secret "stored (write-only)", kafbat proxy mode shown |
| APISIX page | PASS | No gateway card / no "Manage on Platform" button (T8.U2); live proxy `e2ec dummyjson` REconciled+allowlisted (Journey C real-time); "Add certificate" dialog button; admin-gated allowlist section |
| Flows page | PASS | No Root column; direct row actions incl. Deploy rocket (T8.U6); live flows e2ea/e2eb; schema column "1/1 approved" from real Apicurio approval |
| Flow detail Overview | PASS | Rebuilt Overview: Deployment card (state/schedule/DLQ topic dlq.e2ea_users), Entity outputs (kafka+connect chip, derived topic raw.e2ea_users.e2ea_user, live "Schema approved" badge), collapsed Blocks, Topics card; Metrics/DLQ/Messages/Runtime tabs present |
| Schemas | PASS | Live approved schema (registry global id 47, "Uploaded samples" provenance); editor: Structured Editor | Raw Avro JSON tabs w/ Add Field at EXTREME RIGHT (T8.U3); actions Verify/Save/Register new version/Re-run ceremony/Save as template/Delete; no Duplicate/Check-only/Discard; approval history dropdown |
| Flow Builder | PASS | Lifecycle bar reflects live state transitions (Draft -> Stopped "Deployed once" while Journey A deployed); name input frozen after deploy (names-freeze rule); trigger cron preview; canvas nodes b1/b2 present+styled+positioned in DOM (screenshot "empty canvas" was a stale CDP capture artifact — DOM/computed-style evidence recorded: node cards visible, bordered, camera fitted, zero mutation loop) |

Finding (tooling, not app): CDP Page.captureScreenshot intermittently timed out ("renderer
frozen") on this tab; successful captures sometimes showed stale frames. DOM/JS inspection used
as authoritative evidence for the canvas.
