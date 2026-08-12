# Alpha Backend Functional Map — `lovable_ui/backend`

Audit target: `C:\Users\kaifm\Desktop\Project\lovable_ui\backend` (+ `docker-compose.yml`, `docs/` at repo root).
Purpose: complete functional map of the "alpha" NiFi-abstraction backend, to inform reuse/adaptation in a new architecture. This backend generates and manages Apache NiFi flows, Kafka topics, Apicurio schemas, and Kafka-Connect Iceberg sinks from a UI-driven "Source → Stream → Flow" configuration model.

---

## 1. Framework & Runtime

**Framework:** FastAPI (`fastapi==0.110.1`) served by a single Uvicorn worker. Entry point `backend/server.py` (322 lines), `FastAPI(title="NIF Abstractor API", version="1.0.0")`. The app description explicitly states *"Authentication is intentionally disabled for the current MVP test environment"* — **there is no auth/authz layer anywhere in this backend.**

**Python / packaging:** `Dockerfile` base image `python:3.11-slim`. No Python version pin in `requirements.txt`. Container installs from `requirements.local.txt` only (`requirements.txt` additionally lists `emergentintegrations==0.1.0`, an unused/non-standard package — likely leftover AI-app-builder scaffolding, not actually shipped). Key deps: `motor==3.3.1` / `pymongo==4.5.0` (async Mongo), `httpx>=0.28.1`, `kafka-python>=2.0.0`, `fastavro>=1.12.2`, `boto3`, `pysmb>=1.2.10` (SMB), `openpyxl` (XLSX), `PyYAML`, `pandas`/`numpy`; JWT/auth libs (`python-jose`, `pyjwt`, `passlib`) are present but unused given auth is disabled.

**Startup (`server.py`):**
- CORS is the only middleware registered (`allow_origins` from `CORS_ORIGINS` env, default `'*'`, combined with `allow_credentials=True` — a permissive default).
- 16 routers mounted via `app.include_router(...)` with **no additional prefix** — each router's own `APIRouter(prefix=...)` is authoritative: `connections`, `application_services`, `nifi_services`, `dashboard`, `audit`, `settings`, `sources`, `schemas`, `flows`, `flow_import`, `schema_inference`, `webhooks`, `openapi_specs`, `content_store`, `iceberg_sinks`, `kafka_connect`.
- `GET /api` — health/version endpoint, also the Docker healthcheck target.
- `@app.on_event("startup")` (deprecated FastAPI API, not lifespan-context-based) does, in order: `database.init_db()`; fire `asyncio.create_task(recover_runtime_state_background())` (crash recovery, §4); fire `asyncio.create_task(ensure_indexes())` (~20 Mongo indexes across 13 collections, 5s timeout each, failures swallowed); `await seed_default_connections()` (synchronous — on first boot, if `db.connections` is empty, live-tests NiFi/Kafka/Apicurio and inserts 3 default connection docs with hard-coded dev-tunnel fallback URLs, seeds `PlatformSettings`, inserts an initial `AuditEvent`).
- No exception handlers registered (default FastAPI error handling). No scheduler library (no APScheduler/Celery) — all "background" work is ad-hoc `asyncio.create_task()` fire-and-forget calls, not a managed job queue.

**Database:** `backend/db.py` (24 lines, full file) — **MongoDB via Motor**, no SQL, no ORM, no SQLAlchemy anywhere in the repo. Module-level singleton `_client`/`_db`. `MONGO_URL` (default `mongodb://localhost:27017`), `DB_NAME` (default `nif_abstractor`). Schemaless; only indexes are created programmatically, no table/collection creation step.

**Migrations:** `backend/migrations/` has no real framework (Alembic doesn't apply to Mongo). `backfill_connection_provenance.py` (188 lines) is a standalone, manually-invoked, idempotent backfill script (sync `pymongo.MongoClient`, not the app's async client) that stamps connection-provenance fields onto pre-existing docs. **No migration runner, no version tracking, no automatic execution at startup** — a formalization gap for the rewrite.

**Docker Compose** (`docker-compose.yml`, repo root, 70 lines) defines only the app's own stack — **NiFi, Kafka, Apicurio, Kafka Connect are external/provided services, not containerized here**:
- `mongo` (image `mongo:7`, port `27018:27017`, healthcheck via `mongosh ping`).
- `backend` (build `./backend`, port `8011:8010`, `depends_on: mongo` healthy; env vars for `MONGO_URL`, `CONTENT_STORE_ROOT=/app/.content-store`, `CORS_ORIGINS`, `ALLOW_INSECURE_TLS` (default `true`), `STRICT_TLS_VERIFY` (default `false`), `NIFI_URL`/`NIFI_USERNAME`/`NIFI_PASSWORD`, `KAFKA_BOOTSTRAP_SERVERS`, `APICURIO_URL`, `KAFBAT_URL`/`KAFBAT_USERNAME`/`KAFBAT_PASSWORD`; volume `nif-abstractor-content-store:/app/.content-store`).
- `frontend` (build `./frontend`, port `3001:80`, `depends_on: backend` healthy).
- **Notable default:** TLS certificate verification is disabled by default (`ALLOW_INSECURE_TLS=true`, `STRICT_TLS_VERIFY=false`) for all outbound calls to NiFi/Kafka/Apicurio/Kafka Connect — controlled centrally by `services/http_tls.py:tls_verify_enabled()` (17 lines).

**Config / secrets:** Only a repo-root `.env` exists (no `backend/.env`, no `.env.example` anywhere). Config categories: TLS posture, Mongo, content-store root, CORS, NiFi/Apicurio/Kafka/Kafbat connection settings. The root `.env` contains **real plaintext credentials** (`NIFI_PASSWORD`, `KAFBAT_PASSWORD`) — a secrets-hygiene finding independent of environment.

**Docs context** (`docs/application-feature-list/*.md`, `docs/claude plan/*`): confirm this alpha's real scope — REST API sources are the most complete (pagination, fan-out, parent-child chaining, OpenAPI import); MongoDB/SMB/Webhook are partial; PostgreSQL generation and async REST polling are **deprecated**; full NiFi flow lifecycle, Kafka inspection, Avro/Apicurio schema lifecycle, and flow import/export are built. The docs also describe a ground-up rewrite plan ("Data Mobility Platform") that explicitly treats this alpha's REST generator, Kafka-Connect→Iceberg path, and connections/lifecycle machinery as reusable references while dropping MongoDB/SMB/webhook/manual-upload sources and the current schema-verify pipeline to "shelved" status.

---

## 2. Complete API Surface

All paths below are the full, final route (router prefixes only — no extra nesting in `server.py`). 95 endpoints across 16 router files.

### 2.1 `routers/sources.py` — prefix `/api/sources` (7 endpoints)

| Method | Path | Function | Purpose |
|---|---|---|---|
| GET | `/api/sources/` | `list_sources` | List all sources (200 cap), migrates legacy shape on read, redacts secrets. |
| GET | `/api/sources/{source_id}` | `get_source` | Fetch one source, same migration/redaction. |
| POST | `/api/sources/` | `create_source` | Create REST API/MongoDB/SMB/Webhook/PostgreSQL/Trino source with streams. Body is raw dict normalized by `_prepare_source_payload` then validated against `SourceCreate`. 409 on name clash. |
| PUT | `/api/sources/{source_id}` | `update_source` | Full replace; preserves blanked secrets via `_preserve_existing_secrets` unless application-service-managed. |
| DELETE | `/api/sources/{source_id}` | `delete_source` | 409 if any flow still references the source. |
| PUT | `/api/sources/{source_id}/streams` | `update_streams` | Replace just the `streams` array; Webhook sources enforced to exactly one stream, no fan-out/pagination. |
| POST | `/api/sources/{source_id}/test-stream` | `test_stream` | Live, read-only "test this stream" preview: branches per source type (Webhook → latest `webhook_samples` doc; MongoDB → fresh raw `AsyncIOMotorClient` query; SMB → `pysmb` fetch+parse; REST → live `httpx` call via `rest_stream_request_resolver`). Largest single endpoint in the codebase (~490 lines). No DB writes. |

Zero direct NiFi calls in this file — purely source CRUD + live connectivity preview. Local model: `TestStreamRequest`.

### 2.2 `routers/flows.py` — prefix `/api/flows` (23 endpoints)

The deepest NiFi-integration surface in the API layer.

| Method | Path | Function | Purpose |
|---|---|---|---|
| GET | `/api/flows/` | `list_flows` | List flows, enriched with `entity_destinations` (per-stream Kafka topics/schema links). |
| POST | `/api/flows/` | `create_flow` | Create a Flow linking `source_id`+`primary_stream_id`; validates schema-link pairing; derives legacy Kafka topic name if none set. No NiFi call. |
| PUT | `/api/flows/{flow_id}` | `update_flow` | Full replace; re-syncs Iceberg sinks for the flow. |
| GET | `/api/flows/{flow_id}` | `get_flow` | Fetch one flow. |
| GET | `/api/flows/{flow_id}/export` | `export_flow` | Downloads a `.flowpack.json` via `services.flow_export.build_flow_export_package`. |
| PATCH | `/api/flows/{flow_id}` | `patch_flow` | Partial update (`enabled`, schema link, `state` Draft/Stopped only). Disabling a deployed flow live-deactivates its NiFi PG. No audit log call (gap). |
| DELETE | `/api/flows/{flow_id}` | `delete_flow` | Deletes flow; best-effort NiFi PG cleanup (non-blocking); deletes Iceberg sinks; **cascades to delete the source itself if no other flow references it.** |
| POST | `/api/flows/{flow_id}/start` | `start_flow` | Guards: NiFi reachable, not locked, enabled, not already running, runtime connections present, schema Verified. Atomically claims state via `claim_flow_state`. Webhook flows flip state only; others call `nifi_flow_manager.prepare_process_group_for_start`+`start_process_group`. Records a new `FlowRun`. |
| POST | `/api/flows/{flow_id}/stop` | `stop_flow` | Mirrors start; calls `stop_and_clear_process_group` (also **purges queued NiFi FlowFiles** — data-loss-relevant). Closes the run record. |
| POST | `/api/flows/{flow_id}/deploy` | `deploy_flow` | **Most complex endpoint in the codebase.** Idempotency/staleness check; auto-repairs missing REST fan-out edges and (hardcoded) Rapid7-specific pagination defaults; validates pagination/polling/schema-verification; atomically claims `state="Deploying"`; delegates to `services.nifi_flow_generator.generate_and_deploy_flow`; stamps NiFi/Kafka instance fingerprints on success; deploys linked Iceberg sinks; best-effort cleans up partial PG on failure. |
| POST | `/api/flows/{flow_id}/undeploy` | `undeploy_flow` | Deletes the NiFi PG (strict mode) without deleting the flow record; Iceberg sinks intentionally left running. |
| GET | `/api/flows/{flow_id}/iceberg-sinks` | `get_flow_iceberg_sinks` | Live/cached sink status list. |
| GET | `/api/flows/{flow_id}/metrics` | `get_flow_metrics` | Live NiFi PG metrics merged with Kafka topic counts; falls back to zeroed defaults if undeployed/unreachable. |
| GET | `/api/flows/{flow_id}/kafka-messages` | `get_flow_kafka_messages` | Recent messages across all of the flow's entity topics, merged/sorted. |
| GET | `/api/flows/{flow_id}/runtime-errors` | `get_flow_runtime_errors` | Reads a dedicated Kafka error topic (`rest-flow-failures` by default) via `flow_runtime_errors`. |
| POST | `/api/flows/{flow_id}/kafka-messages/clear` | `clear_flow_kafka_messages` | **Destructively** clears all messages on the flow's Kafka topic(s); audit-logs before/after counts. |
| GET | `/api/flows/{flow_id}/runs` | `get_flow_runs` | Returns `flow.runs[]` (embedded run history), most recent first. |
| GET | `/api/flows/{flow_id}/controller-services` | `list_flow_controller_services` | Live NiFi controller services scoped to the PG plus inherited "global" services. |
| GET | `/api/flows/{flow_id}/processors` | `list_flow_processors` | Live NiFi processor list for the PG. |
| GET | `/api/flows/{flow_id}/processors/{processor_id}` | `get_flow_processor` | Full live processor config. |
| PUT | `/api/flows/{flow_id}/processors/{processor_id}` | `update_flow_processor` | **Live-edits a deployed processor's properties directly in NiFi**, bypassing the Source/Flow config → redeploy cycle (config-drift risk, no sync-back). |
| GET | `/api/flows/{flow_id}/controller-services/{service_id}` | `get_flow_controller_service` | Full live controller-service config. |
| PUT | `/api/flows/{flow_id}/controller-services/{service_id}` | `update_flow_controller_service` | Live-edits a controller service (disable→update→re-enable cycle, same drift risk). |

Local models: `FlowPatchRequest`, `ControllerServiceUpdateRequest`. A substantial "stale-deployment reconciliation" subsystem (`_nifi_pg_status`, `_clear_nifi_pg_if_missing`, `_stale_nifi_deployment_error`, etc.) distinguishes "PG deleted in the same NiFi instance" from "flow was deployed against a now-replaced NiFi instance" via fingerprint comparison, and is reused by nearly every live-NiFi endpoint.

### 2.3 `routers/connections.py` — prefix `/api/connections` (9 endpoints)

Generic external-connection registry (types: `nifi`, `kafka`, `apicurio`, `kafka_connect`, `iceberg`) — the most cleanly reusable router in the codebase (see §11).

| Method | Path | Function | Purpose |
|---|---|---|---|
| GET | `/api/connections/` | `list_connections` | List, redacted. |
| GET | `/api/connections/{conn_id}` | `get_connection` | Fetch one. |
| POST | `/api/connections/` | `create_connection` | Create; auto-activates if it's the first connection of its type. |
| PUT | `/api/connections/{conn_id}` | `update_connection` | Blocks (409) endpoint/auth changes if the connection has live dependents — directs caller to `/repoint` instead. |
| DELETE | `/api/connections/{conn_id}` | `delete_connection` | Inactive+no-dependents deletes freely; active+dependents still allowed (resolver self-heals); inactive+dependents blocked. |
| POST | `/api/connections/{conn_id}/test` | `test_connection` | Live reachability probe, dispatched by type; persists `health`/`reachability`. |
| POST | `/api/connections/{conn_id}/activate` | `activate_connection` | Makes this the active connection for its type; refuses if currently-active one has dependents, or if target is unreachable. |
| POST | `/api/connections/{conn_id}/repoint` | `repoint_connection` | Tracked endpoint/auth change with dependent migration — delegates to `connection_lifecycle_runner.run_repoint` (strategies: adopt/migrate/reset). |
| GET | `/api/connections/{conn_id}/impact` | `get_connection_impact` | Read-only blast-radius preview via `connection_impact.compute_impact`. |

### 2.4 `routers/flow_import.py` — prefix `/api/flows/import` (3 endpoints)

| Method | Path | Function | Purpose |
|---|---|---|---|
| POST | `/api/flows/import/preview` | `preview_flowpack_import` | Uploads a `.flowpack.json`, previews import (no writes) via `flow_import_preview`. |
| POST | `/api/flows/import/credentials/validate` | `validate_flowpack_import_credentials` | Validates re-entered secret credentials (multipart file + `credentials_json` form field) via `flow_import_credentials`. |
| POST | `/api/flows/import/finalize` | `finalize_flowpack_import` | Performs the import as a new Draft flow via `flow_import_finalize` (file + `credentials_json` + `options_json`). |

No local Pydantic models — raw `UploadFile`/`Form` fields, JSON sub-payloads manually parsed.

### 2.5 `routers/application_services.py` — prefix `/api/application-services` (6 endpoints)

Reusable credential/config bundles (REST API / SMB / Webhook) referenced by Sources.

| Method | Path | Function | Purpose |
|---|---|---|---|
| GET | `/api/application-services/` | `list_services` | List (redacted). |
| POST | `/api/application-services/` | `create_service` | Create; 409 on name clash; validates config per `service_type`. |
| GET | `/api/application-services/{service_id}` | `get_service` | Fetch one. |
| PUT | `/api/application-services/{service_id}` | `update_service` | Update; preserves blanked secrets. |
| POST | `/api/application-services/{service_id}/test` | `test_service` | Live REST/SMB probe (Webhook trivially succeeds if security config present). |
| DELETE | `/api/application-services/{service_id}` | `delete_service` | 409 if referenced by any source/flow. |

### 2.6 `routers/content_store.py` — prefix `/api/content-store` (2 endpoints)

| Method | Path | Function | Purpose |
|---|---|---|---|
| GET | `/api/content-store/validate` | `validate_content_store_status` | Diffs the file-based content-store mirror against Mongo. |
| POST | `/api/content-store/rematerialize` | `rematerialize_content_store_endpoint` | Full rebuild of the mirror from Mongo. |

### 2.7 `routers/dashboard.py` — prefix `/api/dashboard` (2 endpoints)

| Method | Path | Function | Purpose |
|---|---|---|---|
| GET | `/api/dashboard/summary` | `get_summary` | KPI counts (total sources/flows/running flows/verified schemas/failed connections/24h runs). |
| GET | `/api/dashboard/flow-summary` | `get_flow_summary` | Last 10 flows updated, with state/topic. |

### 2.8 `routers/iceberg_sinks.py` — prefix `/api/iceberg-sinks` (8 endpoints)

| Method | Path | Function | Purpose |
|---|---|---|---|
| GET | `/api/iceberg-sinks/` | `list_sinks` | Cached/last-known status list. |
| POST | `/api/iceberg-sinks/{sink_id}/preflight` | `preflight_sink` | 7-point readiness check (connect reachable, cluster match, plugin installed, schema available, topic exists, config valid). |
| POST | `/api/iceberg-sinks/{sink_id}/enable` | `enable_sink` | Locks via `lifecycle_locks`, upserts Kafka Connect connector. |
| POST | `/api/iceberg-sinks/{sink_id}/disable` | `disable_sink` | Deletes the connector (Iceberg table left in place — see §8). |
| POST | `/api/iceberg-sinks/{sink_id}/pause` | `pause_sink` | Kafka Connect pause. |
| POST | `/api/iceberg-sinks/{sink_id}/resume` | `resume_sink` | Kafka Connect resume. |
| POST | `/api/iceberg-sinks/{sink_id}/restart` | `restart_sink` | Kafka Connect restart. |
| GET | `/api/iceberg-sinks/{sink_id}/config` | `get_sink_config` | Builds and returns the effective connector config, redacted. |

### 2.9 `routers/kafka_connect.py` — prefix `/api/kafka-connect` (2 endpoints, read-only diagnostics)

| Method | Path | Function | Purpose |
|---|---|---|---|
| GET | `/api/kafka-connect/cluster` | `get_cluster` | Cluster info + Iceberg plugin presence + cross-checks Kafka Connect's reported `kafka_cluster_id` against a known-good one from a flow doc. |
| GET | `/api/kafka-connect/orphans` | `get_orphans` | Diffs live `__iceberg`-suffixed connectors against enabled `iceberg_sinks` docs → `unmanaged_connectors` / `missing_connectors`. |

### 2.10 `routers/nifi_services.py` — prefix `/api/nifi-services` (9 endpoints)

Manages shared NiFi Controller Services (kinds: `kafka`, `schema_registry`, `mongodb`, `custom`).

| Method | Path | Function | Purpose |
|---|---|---|---|
| GET | `/api/nifi-services/types` | `list_types` | Live NiFi `controller-service-types`, filtered by kind hint. |
| GET | `/api/nifi-services/` | `list_services` | List, merged with live NiFi state. |
| POST | `/api/nifi-services/` | `create_service` | Creates the controller service **directly in NiFi** under root PG, stamps instance fingerprint, auto-promotes to default. |
| GET | `/api/nifi-services/{service_id}` | `get_service` | Live-fetches config from NiFi. |
| PUT | `/api/nifi-services/{service_id}` | `update_service` | Pushes property updates live to NiFi. |
| POST | `/api/nifi-services/{service_id}/enable` | `enable_service` | NiFi run-status → ENABLED. |
| POST | `/api/nifi-services/{service_id}/disable` | `disable_service` | NiFi run-status → DISABLED. |
| POST | `/api/nifi-services/{service_id}/set-default` | `set_default_service` | Marks default for its kind. |
| DELETE | `/api/nifi-services/{service_id}` | `delete_service` | Blocked if referenced by any source or live NiFi component, or is the last live default of a platform-default kind (`kafka`/`schema_registry`). |

### 2.11 `routers/openapi_specs.py` — prefix `/api/openapi` (6 endpoints)

| Method | Path | Function | Purpose |
|---|---|---|---|
| POST | `/api/openapi/parse` | `parse_openapi` | Upload+parse a JSON OpenAPI doc (10MB cap), dedup by SHA-256 checksum, stores raw bytes gzip+base64 in Mongo. |
| GET | `/api/openapi/{spec_id}` | `get_openapi_spec` | Summary (no raw content/operations). |
| GET | `/api/openapi/{spec_id}/operations` | `list_operations` | Paginated/filterable in-memory search over parsed operations. |
| GET | `/api/openapi/{spec_id}/operations/{operation_id}` | `get_operation_detail` | Full parsed operation object. |
| POST | `/api/openapi/sources/{source_id}/attach` | `attach_openapi_to_source` | Links a spec+server URL to a REST API source. |
| DELETE | `/api/openapi/sources/{source_id}/attach` | `detach_openapi_from_source` | Unlinks. |

### 2.12 `routers/schema_inference.py` — prefix `/api/schema-inference` (5 endpoints)

| Method | Path | Function | Purpose |
|---|---|---|---|
| POST | `/api/schema-inference/start` | `start_inference` | Extensive pre-validation (blocked HTTP methods, unresolved template params, missing runtime bindings), atomically locks the flow, fires a background task running the full deploy→sample→infer pipeline (§5). Returns 202. |
| GET | `/api/schema-inference/flow/{flow_id}` | `get_inference_job_for_flow` | Latest job for a flow (+ optional `entity_stream_id`); best-effort cleans up stale NiFi PGs for terminal jobs. |
| GET | `/api/schema-inference/{job_id}` | `get_inference_job` | Fetch one job. |
| POST | `/api/schema-inference/{job_id}/stop` | `stop_inference` | Stops an active job, releases the flow lock, deletes the temp NiFi PG. |
| POST | `/api/schema-inference/{job_id}/accept` | `accept_inference_schema` | Links the generated schema artifact/version onto the flow (and stream, if entity-scoped); deletes the temp PG. |

### 2.13 `routers/schemas.py` — prefix `/api/schemas` (8 endpoints)

CRUD + lifecycle for Avro `SchemaArtifact`/`SchemaVersion` (Draft → Needs Verification → Verified).

| Method | Path | Function | Purpose |
|---|---|---|---|
| GET | `/api/schemas/` | `list_artifacts` | List all (200 cap). |
| GET | `/api/schemas/{artifact_id}` | `get_artifact` | Fetch one with all versions. |
| POST | `/api/schemas/` | `create_artifact` | Create with initial v1 Draft; validates via `fastavro.parse_schema`. |
| DELETE | `/api/schemas/{artifact_id}` | `delete_artifact` | **Delete whole artifact.** 409 if any linked flow is Running/Deploying/deployed. Unlinks flows/streams first. |
| DELETE | `/api/schemas/{artifact_id}/versions/{version}` | `delete_artifact_version` | **Delete a single version.** 400 if it's the only remaining version ("delete the whole schema artifact instead"). |
| PUT | `/api/schemas/{artifact_id}/versions/{version}` | `update_version_schema` | Edits a version's Avro body. If the target is already `Verified`, **forks a new version** rather than mutating in place. |
| POST | `/api/schemas/{artifact_id}/versions/{version}/generate` | `generate_version` | Marks a version `Needs Verification`. |
| POST | `/api/schemas/{artifact_id}/versions/{version}/verify` | `verify_version` | Idempotent if already Verified. Otherwise, if an `apicurio` connection is configured, calls `apicurio_client.register_schema` and **blocks verification (400) if Apicurio rejects it**; if no registry configured, verifies locally-only. |

Per-artifact `asyncio.Lock` (in-process only — not distributed) serializes delete/verify operations; `update_version_schema` is **not** covered by this lock (race risk).

### 2.14 `routers/settings.py` — prefix `/api/settings` (2 endpoints)

| Method | Path | Function | Purpose |
|---|---|---|---|
| GET | `/api/settings/` | `get_settings` | Singleton `PlatformSettings` doc (`id="platform"`), migrates legacy no-id doc if found. |
| PUT | `/api/settings/` | `update_settings` | Partial update. **Hard-coded policy lock**: `require_schema_verification` can never be set `False` via the API (422). |

### 2.15 `routers/webhooks.py` — prefix `/api/webhooks` (2 endpoints; 726-line file, mostly payload-processing helpers)

| Method | Path | Function | Purpose |
|---|---|---|---|
| GET | `/api/webhooks/{endpoint_id}/health` | `webhook_health` | Checks the matching Webhook source/flow is enabled and Running. |
| POST | `/api/webhooks/{endpoint_id}` | `receive_webhook` | Public inbound receiver: HMAC-SHA256 signature check (skipped entirely if no secret configured), 1MB payload cap, idempotency via `Idempotency-Key`/similar headers or body hash (with **partial-batch resume** via `processed_indices`), JSON/XML/text parsing with record-splitting, per-record extraction/transformation/routing rules, publishes each record to Kafka via `kafka_client.produce_kafka_message`. |

### 2.16 `routers/audit.py` — prefix `/api/audit` (1 endpoint)

| Method | Path | Function | Purpose |
|---|---|---|---|
| GET | `/api/audit/` | `get_audit_log` | Paginated, filterable (`user`, `search`, `from_date`/`to_date`) read of `audit_events`. |

This is the read side of an audit trail every other router writes to via ad-hoc, per-router `AuditEvent(...)` + `insert_one` calls (duplicated pattern, not a shared decorator/middleware).

### 2.17 Key request/response shapes (illustrative, not exhaustive)

`POST /api/flows/{flow_id}/deploy` response (success):
```json
{"ok": true, "process_group_id": "<nifi-uuid>", "iceberg_sinks": [{"sink_id": "...", "enabled": true}]}
```

`POST /api/schemas/{artifact_id}/versions/{version}/verify` response:
```json
{"status": "Verified", "verified_at": "...", "verified_by": "admin",
 "apicurio": {"ok": true, "api_version": "v3", "global_id": 123, "version": 1} }
```
(`apicurio: null` when no registry connection is configured, or when the version was already Verified — idempotent short-circuit.)

`POST /api/schema-inference/start` request (`StartInferenceRequest`, `extra="forbid"`):
```json
{"flow_id": "...", "target_messages": 10, "runtime_values": {"userId": "42"}, "entity_stream_id": null}
```
Returns 202 with the created `SchemaInferenceJob` doc (`status: "idle"` initially, transitions through `deploying_nifi → nifi_running → collecting → inferring → complete|failed|stopped`).

`POST /api/connections/{conn_id}/repoint` request (`ConnectionRepoint`):
```json
{"endpoint": "https://nifi2.example.com/", "auth_type": "BASIC", "username": "admin",
 "password": "...", "strategy": "migrate", "pace": "one_by_one", "abandon_old": false}
```

`POST /api/sources/` accepts a raw dict validated against `SourceCreate` — minimal REST API example:
```json
{"name": "DummyJson Users", "type": "REST API", "base_url": "https://dummyjson.com",
 "auth_type": "NONE", "streams": [{"name": "users", "endpoint_path": "/users",
 "method": "GET", "response_format": "json", "is_primary": true,
 "entity": {"kafka": {"topic": "", "schema_mode": "auto_generate"}}}]}
```

`POST /api/webhooks/{endpoint_id}` response envelope:
```json
{"ok": true, "accepted": 5, "published": 5, "excluded": 0, "routed": 1,
 "topic": "bronze.example.events__history", "request_key": "..."}
```
(A duplicate/replayed request returns the same envelope with the fields as originally computed, annotated `duplicate: true` rather than reprocessing.)

Every list/detail response across all 16 routers strips Mongo's `_id`, ISO-formats `datetime` fields, and — for `Source`/`Connection`/`ApplicationService` — replaces secret field values with `null` plus a companion `has_<field>: bool` flag (e.g. `has_rest_password`) so the UI can show "configured" state without ever receiving the secret back.

---

## 3. Domain Models

All entities are **Pydantic `BaseModel`s serialized as MongoDB documents** — there is no ORM, no SQLAlchemy, no formal FK/constraint enforcement anywhere in `models/`. "Relationships" are plain `str` UUID fields validated only by application code. Full inventory (13 files):

- **`connection.py`** — `Connection`/`ConnectionCreate`/`ConnectionUpdate`/`ConnectionRepoint`. Central hub entity. `type ∈ {nifi, kafka, apicurio, kafka_connect, iceberg}`, `auth_type ∈ {NONE, BEARER, BASIC, CLIENT_CERT}`, plus per-type fields (Kafka SASL/Kafbat, Apicurio group_id, Iceberg OAuth2/S3). `health ∈ {Healthy, Failed, Not Tested}`, `reachability ∈ {Reachable, Unreachable, Unknown}`, `is_active: bool`. Note: `ConnectionUpdate` must be field-for-field kept in sync with `ConnectionCreate` (explicit comment warning of this maintenance hazard, since `extra="forbid"`).
- **`source.py`** (939 lines, largest model file) — `Source`/`SourceCreate` + a deep nested structure: `Stream` (embedded, one per data-fetch node), `StreamPagination` (`type ∈ {none, page, cursor, offset, next_url}`), `StreamPolling` (deprecated — see §5/§8), `StreamFanOut` (parent→child request chaining), `ExtractionRule`, `StreamParameterBinding`, `TransformationRule`, `RoutingRule` (legacy), and the newer "Phase 4" routing model `FilterRule`/`RouteCondition`/`Route`/`RouteSource`/`EntityConfig`/`EntityIcebergConfig`. `Source.type ∈ {REST API, PostgreSQL, MongoDB, SMB, Webhook, Trino}`. `Source.streams: List[Stream]` is fully embedded/denormalized — Streams are never a separate collection. A `designer_payload: Dict` field stores the raw frontend wizard state verbatim for edit-reload — a UI/backend coupling point worth flagging.
- **`flow.py`** — `Flow`/`FlowCreate`/`FlowRun`. `Flow.state ∈ {Starting, Running, Stopping, Stopped, Degraded, Error}` (deployment/runtime state, distinct from `Source.state ∈ {Draft, Saved, Deployed}`). Holds `nifi_process_group_id`, `nifi_connection_id`+`nifi_instance_fingerprint`, `kafka_connection_id`+`kafka_cluster_id` (drift-detection pattern), `schema_artifact_id`+`schema_version`, embedded `runs: List[FlowRun]`.
- **`schema_artifact.py`** — `SchemaArtifact` (embeds `versions: List[SchemaVersion]`), `SchemaVersion` (`status ∈ {Draft, Needs Verification, Verified}`, `apicurio_global_id`, `apicurio_connection_id`), `FlowSchemaLink` (a thin join entity that appears to duplicate the schema fields already embedded on `Flow` — possible legacy/redundant mechanism).
- **`schema_inference.py`** — `SchemaInferenceJob`. `status ∈ {idle, deploying_nifi, nifi_running, collecting, inferring, complete, failed, stopped}`. Carries `worker_instance_id`/`heartbeat_at` for multi-instance job-ownership/crash-recovery.
- **`application_service.py`** — `ApplicationService`/`Create`/`Update`. `service_type ∈ {REST API, SMB, Webhook}`. `REST_AUTH_TYPES = {NONE, API_KEY, BEARER, BASIC}`, `API_KEY_LOCATIONS = {HEADER, QUERY}`.
- **`nifi_global_service.py`** — `NifiGlobalService`/`Create`/`Update`. `VALID_SERVICE_KINDS = {kafka, schema_registry, mongodb, custom}`. Holds `nifi_controller_service_id`+`nifi_process_group_id` (external NiFi identity) and `nifi_instance_fingerprint`.
- **`iceberg_sink.py`** — `IcebergSink` (`extra="allow"`, notably permissive vs. most other models). Three separate `Connection` FKs (Kafka Connect, Iceberg catalog, Kafka broker). `overrides: Dict` is an explicit change-detection cache of last-synced designer intent (only `enabled` is authoritative going forward). `ICEBERG_SINK_DISPLAY_STATES = (Running, Paused, Failed, Degraded, Disabled, Not Deployed, Unknown)`.
- **`connection_lifecycle_job.py`** — `ConnectionLifecycleJob`. `operation ∈ {delete, repoint, migrate, reset, activate}`, `status ∈ {pending, running, interrupted, failed, completed}`, `owner_instance_id`/`heartbeat_at` for takeover detection, embedded `per_object: List[Dict]` progress tracking.
- **`orphaned_artifact.py`** — `OrphanedArtifact`. `system ∈ {nifi, kafka, apicurio}`, `kind ∈ {process_group, controller_service, topic, schema}` — a cleanup ledger for external artifacts abandoned by lifecycle operations (notably does not cover `kafka_connect`/`iceberg` systems, unlike `Connection.type`).
- **`audit.py`** — `AuditEvent`/`AuditEventCreate`. `user` defaults hardcoded `"admin"` (no real auth/user context anywhere). `object_type` and `target` are free-text/loosely-typed references, not FKs.
- **`settings.py`** — `PlatformSettings` (singleton, `id="platform"`)/`PlatformSettingsUpdate`. Defaults mirrored elsewhere (`default_partitions=6`/`default_replication=3` match `KafkaOutput` defaults). `require_schema_verification` and `auto_pause_on_drift` toggles.
- **`__init__.py`** — empty package marker.

**Entity relationship summary:** `ApplicationService` (optional shared creds) → `Source` (embeds many `Stream`s, which can reference sibling streams via fan-out/route/parameter-binding string IDs, forming an in-document DAG) → `Flow` (one per Source+primary Stream; owns NiFi/Kafka deployment state) → `SchemaArtifact`/`SchemaVersion` (governance) → `IcebergSink` (optional downstream table sync, spans three `Connection`s). `Connection` is the hub every external-system-facing entity points at. `NifiGlobalService` is a shared NiFi controller-service registry referenced by Sources via `nifi_service_refs`. `ConnectionLifecycleJob` and `SchemaInferenceJob` are async job records that can produce `OrphanedArtifact`s. `AuditEvent` logs against everything generically. **Cross-cutting pattern:** several entities carry "instance fingerprint" fields (`Flow.nifi_instance_fingerprint`/`kafka_cluster_id`, `IcebergSink.connect_cluster_fingerprint`, `NifiGlobalService.nifi_instance_fingerprint`) implementing an implicit drift-detection subsystem, and `owner_instance_id`/`heartbeat_at` pairs on two job types implement implicit multi-instance job-lease coordination — neither is modeled as an explicit first-class entity.

---

## 4. NiFi Integration

**Client** (`services/nifi_client.py`, 295 lines): no hardcoded base URL — every call resolves a `Connection` doc's `endpoint`. `_normalize_nifi_base_url()` deliberately preserves a `/nifi` path suffix for reverse-proxied deployments. Auth: `NONE` (no header) / `BASIC` (exchanges username+password for a JWT via `POST /nifi-api/access/token`, cached in-process for 8 minutes, keyed `{base_url}:{username}` — lost on restart) / `BEARER` (stored token used directly). All authenticated calls send `Authorization: Bearer <jwt>`. Error classification distinguishes NiFi-UI-vs-API-path mistakes, "Invalid SNI" TLS errors, connect/timeout errors, and 401/403 — no built-in retry (callers implement their own 409-revision-conflict retries).

**Flow generation** (`services/nifi_flow_generator.py`, 6576 lines — the single largest file in the backend) translates a `Source`+`Stream` config into a NiFi **process-group tree**: one top-level PG per flow under NiFi root, shared/inherited controller services (Kafka connection, schema registry, readers/writers), and one child PG per REST stream wired via input/output ports for cross-PG fan-out/routing. No NiFi parameter contexts are created for production flows (only the throwaway schema-inference PG looks up an existing "global-infra" parameter context). Distinct NiFi processor/controller-service types referenced include `InvokeHTTP`, `PublishKafka`, `UpdateAttribute`, `RouteOnAttribute`, `SplitJson`/`SplitXml`/`SplitRecord`/`SplitText`, `ConvertRecord`, `UpdateRecord`/`RemoveRecordField`, `MergeContent`, `QueryDatabaseTableRecord`, `ListSmb`/`FetchSmb`, `GetMongo`/`GetMongoRecord`, plus controller services `Kafka3ConnectionService`, `ConfluentSchemaRegistry`, `JsonTreeReader`/`AvroRecordSetWriter`/`CSVReader`/`XMLReader`/`ExcelReader`, `SmbjClientProviderService`, `DBCPConnectionPool`, `MongoDBControllerService`.

**Lifecycle functions:**
- `generate_and_deploy_flow(...)` (`nifi_flow_generator.py:5980`) — warms JWT, resolves root PG, detects redeploy (deletes old PG first via `_delete_old_pg`), creates PG, creates/enables standard controller services (polling `_wait_cs_enabled`, hard-fails and cleans up on timeout), dispatches to per-type builders (`_build_rest_api_flow`/`_build_smb_flow`/`_build_postgres_flow`/`_build_mongodb_flow`/`_build_trino_iceberg_flow`), best-effort deletes a partially-built PG on failure. **Deployed flows are left stopped** — starting is a separate step.
- `_delete_old_pg(nifi_url, pg_id, ..., strict=False)` (`:63`) — the undeploy primitive: stop all processors → disable owned controller services → purge queued FlowFiles → delete PG (retried once on 409).
- `nifi_flow_manager.py` (840 lines): `start_process_group`/`stop_process_group`/`stop_and_clear_process_group`/`deactivate_process_group`/`prepare_process_group_for_start` (re-enable services in up to 3 passes, then reset processors to STOPPED-but-enabled) — all via `PUT /nifi-api/flow/process-groups/{id}` state changes; `_set_component_state` retries once on 409 with a fresh revision.
- Dead code: lines 6557–6576 of `nifi_flow_generator.py` are unreachable (after a `return`), duplicating logic already present at line 3813 — a leftover refactor fragment.

**Global NiFi services** (`services/nifi_global_services.py`, 630 lines): a shared-controller-service registry ("global" = shared across generated flows within one NiFi instance, not cross-instance pooling). `PLATFORM_DEFAULT_KINDS = {kafka, schema_registry}` always need exactly one live default; `delete_global_service` is heavily guarded (refuses if referenced by any source/flow or live NiFi component, or would remove the last live default of a platform kind).

**Runtime state & crash recovery:** DB-tracked via `flows.state`, `nifi_process_group_id`, `lifecycle_lock_job_id`, `schema_inference_active`; `connection_lifecycle_jobs` and `schema_inference_jobs` collections track long-running async operations with `owner_instance_id`/`worker_instance_id` + `heartbeat_at`. `services/runtime_recovery.py:reconcile_runtime_state()` (called at startup) does three passes: (1) marks stale/orphaned `connection_lifecycle_jobs` `"interrupted"` and releases their locks; (2) sweeps terminal `schema_inference_jobs` for leftover NiFi PGs; (3) marks orphaned active inference jobs `"failed"` and cleans up their NiFi PG/Kafka topic. **Notably, a crash mid `deploy_flow()` for a normal flow is not covered by this recovery** — only schema-inference jobs and connection-lifecycle jobs are heartbeat-tracked; a normal flow deploy interrupted by a restart could leave an orphaned partial PG with no automatic cleanup.

**Repoint/migrate/reset** (`services/connection_lifecycle_runner.py`, 726 lines): `run_repoint()` computes blast radius via `connection_impact.compute_impact()`, probes the new endpoint's identity (`connection_fingerprint.py` — NiFi via root-PG-id, Kafka via `cluster_id`, Kafka Connect borrows Kafka's cluster id, Apicurio/Iceberg have **no stable identifier** and only report reachability), refuses to proceed on unknown identity unless `abandon_old=True`, locks all affected records (`lifecycle_locks.lock_records`), and dispatches to `_strategy_adopt` (rebind only), `_strategy_reset` (abandon old artifacts to `orphan_registry`, demote dependents to Stopped), or `_strategy_migrate` (demote then redeploy each flow onto the new instance, resumable via `per_object` progress tracking).

**Concurrency control** is DB-only (no Redis/distributed lock manager): `services/flow_operation_claims.py` (atomic `find_one_and_update` state-machine claim/release per flow, used by start/stop) and `services/lifecycle_locks.py` (coarser multi-record `lifecycle_lock_job_id` locking across `flows`/`nifi_global_services`/`iceberg_sinks`, used during connection lifecycle operations). Both are single-Mongo-primary-safe but not distributed-lock-manager-grade.

**`services/stream_migration.py`** (126 lines) — a **data-model** migration (not NiFi-instance migration): upgrades legacy `Source.streams` shapes (old `routing_rules` → new `filters`/`routes`, old `is_primary` flag → explicit `entity.kafka` block) so older saved sources are compatible with the current multi-stream/routing topology the flow generator expects.

---

## 5. Schema Subsystem

**Apicurio client** (`services/apicurio_client.py`, 338 lines, pure async functions, no class): base URL normalized into two forms — a "root" registry URL and a "ccompat" (Confluent-compatible) URL — since NiFi's `ConfluentSchemaRegistry` controller service reads via ccompat while its `ApicurioSchemaRegistry` service reads native v3/v2 artifacts. Auth: `NONE`/`BEARER`/`BASIC` (no API-key/mTLS mode; inline per-function, no shared helper).

- `test_apicurio_connection()` — probes ccompat `/subjects` then `API_PROBE_PATHS` (v3 admin/config, v2 groups, legacy `/api/artifacts`); 200 or 404 both count as reachable.
- `register_schema()` — three-tier: (1) best-effort ccompat sync (`POST {ccompat}/subjects/{artifact_id}/versions`, failure logged only, not fatal); (2) v3 native `POST .../groups/{group}/artifacts/{artifact_id}/versions`, falling back to create-with-first-version on 404; (3) v2 fallback. Artifact type hardcoded `AVRO`; group defaults `"nif-platform"` (router-level default) / `"default"` (client-level default).
- `schema_available()` — checks native (v3-then-v2) and ccompat presence **independently**; overall `ok` requires **both**.
- **No delete function exists in this client at all.** Deletion is entirely local/Mongo-only, implemented in `routers/schemas.py` (§2.13) — deleting a schema artifact or version **never calls back into Apicurio** to remove the registered content there, meaning Apicurio and Mongo can diverge after a delete.
- Registration is only ever triggered from `routers/schemas.py:verify_version()` — there is no standalone "register" endpoint; "Verify" (local Draft→Needs Verification→Verified transition) and "Register" (the Apicurio API call) are conceptually distinct but coupled as one operation, gated so that Apicurio rejection blocks local verification when a registry connection exists.

**Schema inference** — two-layer design:
- `services/schema_inferencer.py` (315 lines, pure, no I/O) — a **custom hand-rolled** Avro-schema-inference algorithm (not a third-party lib), lifted from a standalone script per its docstring. Walks sample Python objects into a `Node` tree (type counts, nested fields, array-item type), then `AvroBuilder` converts it to Avro: conservative scalar-type resolution (mixed types widen to `string`), heuristic **timestamp logical-type detection** (field-name keywords + epoch-range plausibility check on ≥3 samples), depth-capped recursion (`MAX_COMPLEX_DEPTH=5`, beyond which nested structures collapse to `string`), all fields made nullable unions preserving first-seen field order (important for CSV/XLSX column order). Entry point: `infer_avro_schema(samples, name, namespace)`.
- `services/schema_inference_runner.py` (1049 lines) — async orchestration invoked as a **fire-and-forget `asyncio` background task** (not a queue/worker), per §2.12's `start_inference`. For Webhook sources: polls `db.webhook_samples` for up to 90s. For other source types: requires NiFi+Kafka connections, deploys a temporary NiFi PG via `nifi_flow_generator.generate_and_deploy_inference_flow` writing plain JSON to a scratch `{topic}-schema-inference` Kafka topic (created via Kafbat REST or `kafka-python` `KafkaAdminClient`), polls the topic every 5s (re-reading from the beginning each time, keeping the max observed batch) up to a 120s hard budget with a 2-poll stability check before stopping, then calls `infer_avro_schema()`, normalizes an array-rooted result into a top-level record (`_normalize_inferred_schema_root`), and **forks a new schema version with status `"Needs Verification"`** (never auto-Verified) linked back to the flow. A `finally` block unconditionally deletes the temp NiFi PG and scratch topic and clears the flow's `schema_inference_active` flag.
- `services/kafka_schema_consumer.py` (630 lines) — the sample-collection layer feeding inference: consumes via direct `kafka-python` first, falling back to Kafbat's SSE-streamed REST message endpoint; auto-detects JSON/XML/CSV/key-value/plain-text per message and parses to Python objects, using a cross-sample **XML hinting pass** to correctly infer arrays that only appear once in a given sample.

**Verify vs. Register / delete-version vs. delete-whole-schema** (the two semantics called out in the audit brief):
- *Verify* is a local Mongo state transition, side-effected by an Apicurio *register* call only when a registry connection is configured; there is no separate register endpoint.
- *Delete version* (`DELETE /api/schemas/{id}/versions/{v}`) explicitly refuses to remove the last remaining version (400, directs to whole-artifact delete); *delete whole artifact* (`DELETE /api/schemas/{id}`) removes everything and unlinks all dependents. Neither path touches Apicurio.

---

## 6. OpenAPI Subsystem

**Parser** (`services/openapi_parser.py`, 269 lines, no network calls): accepts OpenAPI 3.x or Swagger 2.0 JSON (UTF-8/UTF-16 sniffing), inlines local `$ref`s only (rejects remote refs, cycle-protected), extracts servers (direct for OAS3, synthesized `scheme://host+basePath` for Swagger2), and per-operation: merges path+operation parameters, normalizes types, synthesizes `operationId` when missing, and — critically — **rewrites OpenAPI's `{param}` path templates into this app's own `${param}` runtime-template syntax**, the same convention consumed by `rest_stream_value_mapping.substitute_runtime_values()`. This is the actual bridge from "user uploads a spec" to "REST stream endpoint_path field." Output: `{checksum_sha256, format, openapi_version, title, version, servers[], operations[], warnings[], errors[]}`.

**Endpoint discovery / serving to frontend:** `routers/openapi_specs.py` (§2.11) — upload dedupes by checksum, stores raw bytes gzip+base64 in Mongo (fully recoverable original), and serves operations via in-memory search/pagination over the already-parsed `operations` array (no re-parse per request). Attach/detach binds a spec+server-URL to a specific `REST API` source.

**`openapi_snapshot.json` / `openapi_ops_summary.json`** (backend root): confirmed **unrelated to the upload/parse feature** — these are developer/CI artifacts describing **this backend's own API** (a raw FastAPI-generated OpenAPI doc, and a flattened `{Method,Path,OperationId,...}` operation-inventory table respectively), not fixtures or examples of `parse_openapi_document()`'s output shape. Worth noting for the rewrite: these look like snapshot-testing/doc-export tooling output, not application logic, and could be regenerated or dropped rather than ported.

---

## 7. Service/Auth Configuration

Two **separate, non-unified** auth vocabularies exist in this codebase:

1. **Application Service / Source auth** (`models/application_service.py`, `models/source.py`) — for REST API/SMB/Webhook credentials a Source or reusable `ApplicationService` holds: `service_type ∈ {REST API, SMB, Webhook}`; REST auth `REST_AUTH_TYPES = {NONE, API_KEY, BEARER, BASIC}` with `API_KEY_LOCATIONS = {HEADER, QUERY}`; SMB requires hostname/share/username/password(+domain); Webhook requires a secret + `webhook_signature_algo` (only `"hmac-sha256"` currently allowed). `SECRET_CONFIG_FIELDS` fixed set drives redaction (`api_key_value`, `bearer_token`, `rest_password`, `smb_password`, `webhook_secret`).
2. **Platform Connection auth** (`models/connection.py`) — for the backend's own calls *to* NiFi/Kafka/Apicurio/Kafka Connect/Iceberg: `auth_type ∈ {NONE, BEARER, BASIC, CLIENT_CERT}` (has `CLIENT_CERT` but no `API_KEY`, the inverse gap from vocabulary #1). Plus per-type extras: Kafka SASL mechanism/username/password + `kafka_connection_mode ∈ {native, kafbat}`; Apicurio `group_id`; Iceberg OAuth2 client-credentials (`iceberg_credential` formatted `client_id:client_secret`) + S3 access key/secret/region/path-style.

**Credential storage:** inline on the `Source`/`ApplicationService`/`Connection` document (plaintext in Mongo — no field-level encryption observed anywhere in the reviewed code), redacted only at the API response layer (`has_<field>: bool` flags replacing values) and in the content-store export path (`flow_secret_refs.py`, §10). No secrets manager/vault integration.

**NiFi Global Services** (`models/nifi_global_service.py`) add a third, narrower dimension — `VALID_SERVICE_KINDS = {kafka, schema_registry, mongodb, custom}` — these are NiFi-side shared controller-service *instances*, not an auth-method enum, but they carry their own `properties: Dict` (arbitrary NiFi property bag, which may itself embed credentials, masked in API responses via any-key-containing `password/secret/token/key` heuristic redaction).

---

## 8. Other Integrations

**Kafka** (`services/kafka_client.py`, 1174 lines) — two-tier: direct TCP via `kafka-python` (`KafkaAdminClient`/`KafkaConsumer`/`KafkaProducer`, `SASL_SSL`/`SASL_PLAINTEXT` support), falling back to "Kafbat" (a Kafka UI/proxy) REST API (session-cookie auth via `POST /login`) when direct TCP isn't reachable. Detailed error classification (`_classify_kafka_error`) maps low-level exceptions to codes like `DNS_RESOLUTION_FAILED`, `SASL_AUTH_FAILED`, `SSL_MISMATCH_OR_CERT_ERROR`. Functions: connection test, topic message count/recent-messages/clear/produce/ensure-exists.

**Kafka Connect** (`services/kafka_connect_client.py`, 444 lines) — thin async REST wrapper (`_request()` central helper) with uniform error-code mapping (`CONNECT_AUTH_FAILED`, `CONNECT_REBALANCING`, `CONNECT_CONFIG_INVALID`, `CONNECT_NOT_FOUND`, `CONNECT_UNREACHABLE`, `CONNECT_TIMEOUT`). Full connector CRUD + plugin listing + config validation.

**Iceberg** — `services/iceberg_catalog_client.py` (188 lines, OAuth2 client-credentials probe against `/v1/config`), `services/iceberg_sink_config.py` (201 lines, **pure function**, no I/O — builds the `org.apache.iceberg.connect.IcebergSinkConnector` Kafka Connect config: REST/Polaris catalog, Apicurio Avro converter, S3 settings, DLQ topic `dlq.{topic}.iceberg`), `services/iceberg_sinks.py` (756 lines — full reconciliation/orchestration: `preflight_sink`'s 7-point check, enable/disable/pause/resume/restart, and a documented design decision that **Iceberg tables are never dropped** on sink deletion — the table is recorded as an orphan via `orphan_registry.record_orphan(system="iceberg", kind="table", ...)` instead).

**APISIX / Redis:** confirmed via exhaustive case-insensitive grep across the entire backend tree — **zero references to either system anywhere in code, config, or docstrings.** Neither is integrated in any form (the docs describe both as *planned/pending* features for the future rewrite, not implemented here).

**TLS:** all outbound HTTPS calls (Kafka, Kafka Connect, Iceberg, NiFi, Apicurio) route cert-verification through the single `services/http_tls.py:tls_verify_enabled()` toggle — except connection-fingerprint probing code, which was found to hardcode `verify=False` for Apicurio/Iceberg probes in places, an inconsistency versus the shared helper.

---

## 9. Tests

48 files under `backend/tests/` (37 top-level + 7 in `tests/resilience/` + `conftest.py`/`__init__.py` + 2 golden JSON fixtures in `tests/fixtures/`). Rough distribution: REST-stream/pagination/routing behavior ~13 files (largest cluster), flow import/export/content-store ~6, resilience/fault-injection ~7, schema inference ~3, NiFi flow generator ~3, connections/NiFi client ~3, Kafka/Iceberg sinks ~4, misc (dashboard, runtime errors, stream migration, env loading) ~7.

**Framework:** plain `pytest` (`pytest>=8.0.0`), no `pytest-asyncio` — async tests use a hand-rolled `async_test` decorator (`asyncio.run` wrapper) **redefined in nearly every file** rather than shared via `conftest.py`. No root-level `pytest.ini`/`pyproject.toml`/`conftest.py` (only `tests/resilience/conftest.py` exists); tests manually `sys.path`-insert the backend dir, implying invocation as `pytest tests/` from `backend/`.

**Mocking:** `monkeypatch.setattr` targeting service/router functions directly, plus hand-written fake `httpx.AsyncClient` classes returning real `httpx.Response` objects — no respx/httpx-mock library, no real network calls. **No real MongoDB** — an in-memory fake DB pattern (`FaultInjectingCollection`/`ResilienceFakeDB` in `tests/resilience/conftest.py`, implementing a meaningful subset of Mongo query/update operators plus deliberate fault injection via a `FaultPoint` enum) is the core mechanism for the resilience suite; other files use a similar `FakeDB` (defined in `test_content_store.py`, reused elsewhere). `iceberg_sink_config.py`'s pure functions are tested via golden-fixture dict-equality against `tests/fixtures/*.json`.

**Resilience suite** (`tests/resilience/`) specifically exercises: deploy-recovery, export-snapshot correctness, fault-fixture behavior, import-atomicity (rollback on partial failure), runtime concurrency (state-claim races), and schema-service concurrency — i.e., this is where the crash-recovery and locking mechanisms described in §4 are actually validated.

---

## 10. Other Notable Items

**Content store** (`services/content_store.py`, 678 lines) — a file-based mirror of Mongo state under `backend/.content-store/flows/<sanitized-name>/`, confirmed by the on-disk layout (`flow.json`, `secrets.json`, `source/source.json`, `source/streams/<id>.json`, `services/{application,nifi}.json`, `schemas/<artifact_id>/{artifact.json, versions/<v>.json}`, `openapi/<spec_id>/spec.json`, plus a root `manifest.json`). Writes are atomic (tempfile+`os.replace`+fsync) and path-bounded (rejects writes outside the configured root). `services/content_store_reconcile.py` validates and, on drift, fully rematerializes it. Purpose appears to be a human-diffable/exportable snapshot layer rather than a functional dependency of the live API (routers still read/write Mongo as source of truth and sync the store as a side effect).

**Secrets handling** (`services/flow_secret_refs.py`, 128 lines): a fixed key-pattern classifier (`is_secret_key` / `SECRET_KEYS` + suffix rules) replaces secret values with `{"$secretRef": "<path>#<field>"}` markers and collects real values into the bundle's `secrets.json`. **This is plaintext-on-disk, not encryption** — separation is by file convention only. `flow_export.py` explicitly nulls `secrets.json` when building a downloadable `.flowpack.json`, so exports never carry live secret values; import (`flow_import_preview.py`/`flow_import_credentials.py`/`flow_import_finalize.py`) enforces that every referenced secret is null in the uploaded package and requires exact re-entry of the required credential set before finalizing (full ID remapping, schema-conflict resolution via `link_existing`/`rename`, and rollback-on-exception with content-store cleanup).

**`cleanup_orphan_sources.py`** (backend root) — a confirmed **one-off migration hack**, self-documented in its own docstring as a one-time fix for a missing cascade-delete (deleting a flow used to leave its source behind). It's bare top-level script code (not `if __name__ == "__main__"` guarded), uses a raw synchronous `pymongo.MongoClient` bypassing the app's own DB/connection-resolver patterns, and — critically — its output artifact **`sources_backup_20260714_064304.json`** (3.47MB, ~106K lines, full un-redacted `sources` collection dump) was found to contain **plaintext credentials and internal hostnames/IPs** (e.g. a `rest_password` value and an internal `172.16.x.x` `base_url`) committed to the backend source tree, entirely bypassing the app's own secret-redaction discipline. This is a concrete security/hygiene finding to flag, and this pattern (ad-hoc backup scripts writing unredacted dumps to the shipped source tree) should not be repeated in the rewrite.

**Audit logging:** `services/audit.py` (37 lines) — a single `log_audit()` helper writing `AuditEvent` docs, called throughout routers/services with Success/Failed status and a JSON-serialized `details` blob. **No real user attribution anywhere** — `user` defaults to the hardcoded string `"admin"`; `verify_version()` similarly hardcodes `verified_by: "admin"`. Coverage is inconsistent: e.g. `flows.py`'s `patch_flow` performs a live NiFi deactivation without any audit call.

**Error handling patterns:** consistently HTTPException-based with specific status codes (404/409/422/502/423-Locked); "best-effort" cleanup (NiFi PG deletion, Kafka topic deletion) is pervasive and deliberately non-blocking — failures are logged/appended to a report rather than raised, favoring availability of the primary operation over cleanup completeness. External-system probes uniformly distinguish "unreachable" vs. "reachable but errored" vs. "identity unprovable" (see `connection_fingerprint.py`'s `FingerprintResult` contract).

**Background jobs:** none use a real task queue (no Celery/RQ/APScheduler). All async work is `asyncio.create_task()` fire-and-forget from within request handlers or the startup event, coordinated after the fact via Mongo-persisted job documents with heartbeat/instance-id fields for crash detection — a lightweight, DB-only job system rather than a dedicated worker architecture.

**Stray files/hygiene:** two files literally named `=0.28.1` and `=2.0.0` sit at `backend/` root (artifacts of an unquoted `pip install pkg>=x.y.z` in a shell that treated `>` as redirection) — trivial but indicative of low CI/dev hygiene during this alpha's development.

---

## 11. Reusability Assessment

**Cleanly reusable (generic, well-isolated, minimal NiFi coupling):**
- `routers/connections.py` + `services/connection_resolver.py` + `services/connection_impact.py` + `services/connection_fingerprint.py` + `services/connection_lifecycle_runner.py` + `services/lifecycle_locks.py` + `services/flow_operation_claims.py` — the entire generic "external connection registry with health/activation/repoint/impact-analysis lifecycle" is type-agnostic (NiFi is one of five `type`s) and could be carried into a new architecture with minimal change, just adding/removing connection types.
- `routers/sources.py` — zero direct NiFi calls; pure source CRUD plus a live REST/Mongo/SMB test-preview harness. The REST-preview logic in particular (`rest_stream_request_resolver.py`, `rest_stream_value_mapping.py`) is cleanly factored and reusable independent of NiFi.
- `services/rest_stream_behavior.py`, `rest_stream_branch_validation.py`, `rest_stream_pagination_validation.py`, `rest_stream_request_resolver.py`, `rest_stream_value_mapping.py` — pure, well-tested (13 dedicated test files), NiFi-agnostic graph/validation/templating logic describing *what* a REST source should do; the NiFi-specific part is only the final compile step (`to_nifi_runtime_template`).
- `services/schema_inferencer.py` — pure, dependency-free Avro-inference algorithm; fully portable.
- `services/apicurio_client.py`, `services/kafka_client.py`, `services/kafka_connect_client.py`, `services/iceberg_catalog_client.py`, `services/iceberg_sink_config.py` — thin, well-scoped external-API clients with uniform error-code conventions; reusable as-is or with light refactoring.
- `services/audit.py`, `models/audit.py` — trivial, reusable as-is (though needs real user-attribution wiring for a production rewrite).
- `models/connection.py`, `models/application_service.py`, `models/settings.py` — generic config/credential models, low NiFi coupling.

**Entangled with NiFi-specific concepts (expect a substantial rewrite, not a port):**
- `services/nifi_flow_generator.py` (6576 lines) and `services/nifi_flow_manager.py` — the entire "compile Source/Stream config into a NiFi process-group graph" engine is inherently NiFi-shaped (processor types, controller-service enable/disable cycles, process-group nesting) and is the piece most likely to be replaced wholesale by whatever the new architecture's execution engine is, though the *conceptual* mapping (stream → fetch/parse/transform/route/publish stages) is a useful reference.
- `routers/flows.py`'s deploy/start/stop/undeploy endpoints, the stale-PG/instance-fingerprint reconciliation subsystem, and the live processor/controller-service passthrough endpoints (§2.2, nos. 19–23) — deeply NiFi-specific; the live-edit-bypasses-source-of-truth pattern in particular is a design smell (config drift) that a rewrite should deliberately avoid rather than port.
- `routers/nifi_services.py` + `services/nifi_global_services.py` — entirely NiFi-controller-service-shaped; not portable to a different execution engine without a full redesign.
- The hardcoded Rapid7-specific pagination-defaults heuristic in `flows.py:deploy_flow` (`_repair_rest_pagination_for_deploy`) — vendor-specific scope creep that should not be ported; if similar smart-defaults are wanted, they belong in a general capability, not a name-substring special case.

**Mixed / needs conscious decision:**
- `services/schema_inference_runner.py` and `services/kafka_schema_consumer.py` — the *sampling* concept (deploy a temp pipeline, collect N messages, infer, discard) is reusable, but the current implementation is tightly bound to deploying a temporary NiFi PG; a rewrite would need an equivalent "temporary sampling pipeline" primitive in whatever the new execution engine is.
- `models/source.py`'s `Stream` model and its routing/fan-out sub-models — the *data model* (parent/child streams, filters, routes, pagination strategies) is a solid, reusable design; the deprecated fields within it (`RoutingRule` alongside the newer `Route`/`RouteCondition`, `StreamPolling` which is fully deprecated per `rest_stream_polling_validation.py`) should be pruned rather than carried forward as-is.
- `services/content_store.py` and its `sync_*`/`reconcile` machinery — a reasonable pattern (file-mirror for diffability/export) but adds real complexity (every mutating router call must remember to sync); worth evaluating whether the new architecture needs this at all versus deriving exports on demand.
- `services/flow_export.py` / `flow_import_*.py` — the secret-redaction-on-export and re-entry-on-import design is sound and worth keeping conceptually, but should be re-implemented with real encryption/secrets-manager backing rather than the current plaintext-file-with-naming-convention approach (see §10's `sources_backup_*.json` finding as a cautionary example of what happens when that discipline is bypassed).

**Do not carry forward:** `cleanup_orphan_sources.py` and `sources_backup_20260714_064304.json` (one-off hack + leaked plaintext-credential artifact — should be deleted from the repo, not migrated); the stray `=0.28.1`/`=2.0.0` files; `emergentintegrations==0.1.0` in `requirements.txt`; the deprecated `StreamPolling`/legacy `RoutingRule` code paths once their replacements are confirmed complete.
