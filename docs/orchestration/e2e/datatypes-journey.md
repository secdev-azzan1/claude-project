# Datatypes Journey Report

Date: 2026-08-15

## Scope

This report captures the verified state of the datatypes continuation work under `frontend/e2e/datatypes-continuation.spec.ts` and the preserved runtime resources created during verification.

## Verified Passes

- The continuation spec exists and is wired for JSON, CSV, and XML coverage.
- The artifact path is in place at `frontend/e2e/artifacts/datatypes-continuation/`.
- The flow-sheet UI assertions are encoded for:
  - row click not opening the overview
  - the eye `Overview` action opening the flow overview
  - `Clear topic` on Messages
  - `Clear DLQ` on DLQ
- Runtime connection fixes were applied and left in place:
  - NiFi connection `conn-kk8a1v` named `codex15aug26 NiFi`
  - Kafka connection `conn-g7huwg` named `Apache Kafka`
  - Redis connection `conn-eduhk7`
- These connections were verified healthy on backend `http://localhost:8011`.

## Blockers

- The live JSON flow path on `http://localhost:8011` still does not emit Kafka messages in the verified run.
- The last verified JSON flow runtime showed:
  - flow id `flow-nfxn4y`
  - `records24h: 0`
  - `errors24h: 2`
  - `queued: 4` at the API metrics layer
  - NiFi processor evidence showing the HTTP read path advancing, but downstream Kafka publish never producing topic messages
- Verified topic/message checks for that run were empty:
  - main topic messages: `[]`
  - DLQ records: `[]`
- CSV and XML were not carried through to completion because the JSON blocker consumed the available verification window.

## Preserved State

- No created flow, schema, topic, connector, proxy, table, or evidence resource was deleted or cleared.
- The live connection changes remain visible for follow-up debugging.
- The E2E artifacts directory remains populated for inspection.

## Notes

- The work is intentionally left in a preserved, inspectable state.
- This report reflects the exact verified state reached before interruption.
