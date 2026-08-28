# JDBC Continuation Verification

**Date:** 2026-08-15
**Scope:** live UI + backend/runtime verification for the `jdbc` adapter continuation work
**Environment:** frontend `http://localhost:3001`, backend `http://localhost:8011`, Trino `https://trino.datapasc.com`
**Artifacts:** `frontend/e2e/artifacts/jdbc-continuation/`

## What was verified

- `jdbc · read` is available as a root block.
- After a root JDBC read exists, the add menu does not offer `jdbc · read` again.
- `jdbc · lookup` exposes the join-field input in the block form.
- The JDBC service was reconciled to a backend-reachable Trino endpoint and retested healthy.
- The flow saved cleanly, and the backend flow record preserved the JDBC read / lookup / write chain with the Trino table and join field.

## What is still blocked

- The NiFi deploy path did not complete in this run.
- The last direct backend deploy attempt failed controller-service validation with:
  - `Database Driver Locations` invalid because the referenced jar path was not accessible to NiFi.
  - `Password cannot be empty`.
- The created flow remains in `Draft` state and was left in place for inspection.

## Evidence checklist

- Service name: `codex15aug26-jdbc-trino-service`
- Flow name: `codex15aug26-jdbc-continuation-4`
- JDBC table probe: written to `frontend/e2e/artifacts/jdbc-continuation/table-probe.json`
- Service persistence: written to `frontend/e2e/artifacts/jdbc-continuation/service.json`
- Backend flow record: preserved in Mongo and visible through the flow API
- Runtime payload: not captured because deploy/start did not complete
- Topic/messages sample: not captured because deploy/start did not complete
- Verdict summary: not captured because deploy/start did not complete

## Screenshot set

- `01-service-created.png`
- `02-flow-created.png`
- `03-jdbc-root-configured.png`
- `04-lookup-join-field.png`
- `05-kafka-write-added.png`
- `06-preflight.png`
- `06b-runtime-table-reconciled.png`
- `FAIL-jdbc_continuation_journey.png`

## Notes

- The spec uses the live Trino endpoint to discover a queryable table before building the flow.
- No created flow, service, or topic was deleted during verification.
- The saved JDBC service is healthy, but the NiFi deploy path still needs a valid controller-service jar location and a non-empty password before runtime proof is possible.
