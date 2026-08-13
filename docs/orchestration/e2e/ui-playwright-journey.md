# Playwright UI Journey — full flow lifecycle through the real browser

Date: 2026-08-13. Spec: frontend/e2e/ui-journey.spec.ts (chromium, headless), app at
:3001 -> backend :8010 -> live NiFi/Kafka. Screenshots/traces: frontend/e2e/artifacts/.
Final run: 8 passed / 1 selector-flake / 1 completed via API — every product behavior PASS.

| Step | Result | Evidence |
|---|---|---|
| 1 Create HTTP service via UI | PASS | card rendered, toast |
| 2 New flow "e2eui users" | PASS | builder opened, flow persisted |
| 3 Root http·read: service bind + PATH UX | PASS | typed FULL URL https://dummyjson.com/users -> AUTO-STRIPPED to /users w/ toast; "Base URL — https://dummyjson.com (from service)" context line + resolved preview visible |
| 4 kafka·write child + entity | PASS | derived topic raw.e2eui_users.e2eui_user rendered |
| 5 Block Test on UNSAVED flow | PASS | no "engine pending" — real result (records + detected fields); "Testing saves the flow first." |
| 6 Deploy via preflight dialog | PASS | all rows ok, Deployed |
| 7 Start -> records -> Messages tab | PASS | topic populated after cron fire; rows render in UI |
| 8 REGRESSION undeploy -> redeploy | PASS | preflight clean — NO self-topic "not owned by this flow" collision |
| 9 UI cleanup (delete flow, retire service) | PASS (flake) | delete+retire succeeded; assertion locked onto a stale duplicate card from earlier aborted runs — completed verification via API |
| 10 API sweep | PASS | e2eui flows: NONE; active e2eui services: NONE |

Spec-hardening fixed during runs (harness, not app): duplicate tracing start; collapsed
"Entity & derived names" accordion is FORCED-OPEN (disabled trigger) for fresh write
blocks; new canvas node needs explicit selection; Undeploy menuitem needs exact match
(regex also hit "Delete flow — Undeploy the flow first").

Environment note: one mid-run backend outage (Docker/WSL hang, fixed by the schema
workflow's verifier via wsl --shutdown) invalidated one earlier attempt; final run clean.
