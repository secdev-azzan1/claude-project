# Schema Registration Fix — Verification Evidence

**Date:** 2026-08-13
**Verifier:** verification agent (no source changes made)
**Scope:** backend `routers/v2/schemas.py` (`registeredVersion` persistence fix) + frontend `Schemas.tsx` / `prototype/api.ts` / `prototype/types.ts` (registration badge + filter fix)
**Live infra:** backend `http://localhost:8010` (local venv, not the docker-compose backend on :8011), Mongo via `dmp-mongo` docker container (`localhost:27018`), Apicurio ccompat v7 at `https://apicurio.datapasc.com`

---

## 1. Backend restart

- No local `uvicorn server:app` process was found running at task start (`Get-CimInstance Win32_Process` filter on `python.exe` + `uvicorn` returned nothing; `netstat` showed no listener on 8010, only a stale `SYN_SENT` client). Nothing to stop.
- First start attempt failed: Mongo at `localhost:27018` was unreachable (`ServerSelectionTimeoutError`). Root cause: Docker Desktop's engine was not responsive (`docker ps` → named-pipe error `dockerDesktopLinuxEngine` not found; `com.docker.service` Windows service was `Stopped`).
- Restarted Docker Desktop (`Stop-Process` on all `*docker*` processes → `wsl --shutdown` → relaunch `Docker Desktop.exe`). After a full WSL VM cold start (~2.5 min, `vm.lifecycle-server.docker` log showed `wait for engine: still waiting for API to respond after 6m59s` before the reset), the engine came up and resumed its `restart: unless-stopped` containers automatically:
  - `dmp-mongo` → `0.0.0.0:27018->27017/tcp`
  - `data-mobility-platform-backend-1` → `0.0.0.0:8011->8010/tcp` (docker-compose backend, untouched — different port from our target)
  - `data-mobility-platform-frontend-1` → `0.0.0.0:3000->80/tcp`
- Started the local backend: `cd backend; .venv\Scripts\python.exe -m uvicorn server:app --port 8010` (stdout/stderr → `uvicorn-out.log`/`uvicorn-err.log`). Confirmed a single clean process tree (venv redirector PID → real interpreter PID) and a clean log:
  ```
  INFO:     Started server process [27448]
  2026-08-13 13:54:54,492 - server - INFO - Database connection initialized.
  2026-08-13 13:54:54,530 - server - INFO - v2 connections seeded/verified.
  INFO:     Application startup complete.
  INFO:     Uvicorn running on http://127.0.0.1:8010 (Press CTRL+C to quit)
  ```
- `GET http://localhost:8010/api` → `200 {"message":"NIF Abstractor API","version":"1.0.0"}`.

## 2. Live round-trip (prefix `e2ewf`)

| Step | Call | Result |
|---|---|---|
| Create template | `POST /api/v2/schemas/templates` `{name:"e2ewf template", avro: E2ewfDemo{id:string, value:int}}` | `201`, `id: tpl-fjih5j` |
| Verify | `POST /api/v2/schemas/verify` `{avro, subject:"e2ewf.demo-value"}` | `200 {"ok":true,"issues":[],"compatibility":{"checked":true,"compatible":true,...}}` |
| Register | `POST /api/v2/schemas/register` `{subject:"e2ewf.demo-value", avro, templateId:"tpl-fjih5j"}` | `200 {"globalId":23,"subject":"e2ewf.demo-value","version":1,"registeredAt":"2026-08-13T09:56:10.418Z"}` |
| List | `GET /api/v2/schemas/` | template `tpl-fjih5j` entry carries **all four** fields: `registeredSubject:"e2ewf.demo-value"`, `registryGlobalId:23`, `registeredVersion:1` (JSON number, i.e. int), `registeredAt:"2026-08-13T09:56:10.418Z"` |
| ccompat cross-check | `GET https://apicurio.datapasc.com/apis/ccompat/v7/subjects/e2ewf.demo-value/versions` | `200 [1]` |

**PASS** — matches the backend fix's claim exactly: `registeredVersion` now persists and round-trips through the list endpoint.

## 3. Re-register with changed avro

Added a nullable field (`note: ["null","string"], default:null`) to the avro, re-registered same `subject`+`templateId`:

- `POST /api/v2/schemas/register` → `200 {"globalId":24,"subject":"e2ewf.demo-value","version":2,"registeredAt":"2026-08-13T09:56:36.045Z"}`
- Template doc after: `registryGlobalId:24` (was 23), `registeredVersion:2` (was 1), `registeredSubject` unchanged, `registeredAt`/`updatedAt` refreshed.
- ccompat: `GET .../versions` → `[1,2]`

**PASS** — `registeredVersion` incremented, `registryGlobalId` changed, ccompat versions `[1,2]`.

## 4. Idempotent re-register (unchanged avro)

Registered the same (now-current, v2) avro again under the same subject+template:

- `POST /api/v2/schemas/register` → `200 {"globalId":24,"subject":"e2ewf.demo-value","version":2,"registeredAt":"2026-08-13T09:56:55.317Z"}` — no error.
- Template doc: `registryGlobalId:24`, `registeredVersion:2` — unchanged from step 3 (ccompat did not mint a new global id/version for byte-identical content; only `registeredAt`/`updatedAt` advanced, which is correct — the router refreshes those unconditionally on every call).
- ccompat: `GET .../versions` → still `[1,2]`.

**PASS** — idempotent, no error, `registeredVersion` still 2, ccompat versions unchanged.

## 5. Test suites

| Suite | Command | Result |
|---|---|---|
| Backend, schemas-only | `pytest tests/test_v2_schemas.py -q` | **18 passed** (matches backend agent's report) |
| Backend, full | `pytest tests/ -q` | **660 passed, 1 deselected** (matches backend agent's report exactly — baseline 658 + 2 new test functions) |
| Frontend unit | `npx vitest run` | **29 test files passed, 161 tests passed**, 0 failures |
| Frontend typecheck | `npx tsc -p tsconfig.app.json --noEmit` | **14 errors, confined to exactly 2 pre-existing files** not touched by either fix: `src/lib/schemaCreate.test.ts` (1 error) and `src/pages/FlowDesigner.tsx` (13 errors). Confirmed via `git status --short` that neither file is part of this change set. Zero errors in `Schemas.tsx`, `prototype/api.ts`, or `prototype/types.ts` (the three files the frontend fix touched). |

**PASS** — all suites green; no new typecheck regressions introduced by either fix.

## 6. Cleanup

- `DELETE https://apicurio.datapasc.com/apis/ccompat/v7/subjects/e2ewf.demo-value` → `200 [1,2]`. Confirmed gone: subsequent `GET .../versions` → `404 No artifact with ID 'e2ewf.demo-value'...`.
- `DELETE /api/v2/schemas/templates/tpl-fjih5j` → `200 {"ok":true}`. Confirmed gone: `GET /api/v2/schemas/` no longer contains `tpl-fjih5j`.

No residue left in either the registry or the app database from this verification run.

---

## Diff review (source-of-truth check against both fix agents' reports)

- `backend/routers/v2/schemas.py`: diff matches the backend report 1:1 — `_coerce_int` helper added, `register_schema_standalone` now computes `version = _coerce_int(result.get("version"))`, hoists a single `registered_at`, and the template `$set` includes `registeredVersion` unconditionally on every register call. Response body now also includes `registeredAt`.
- `frontend/src/prototype/types.ts` / `api.ts`: diff matches the frontend report — additive `registeredVersion?`/`registeredAt?` on `SchemaTemplate`, mapped in `toSchemaTemplate` following the existing optional-spread pattern.
- `frontend/src/pages/Schemas.tsx`: diff matches the frontend report's description — new `TemplateRegistrationBadge` component (compact/full), rail row and detail header wired to real `registryGlobalId`/`registeredVersion`, `RegistrationFilter` replacing `KindFilter` ("All"/"Registered"/"Not registered"), `templateEditedSinceRegistration` derived warning, and the reworded page description.

## Conclusion

Both fixes verified working end-to-end against live infra (Mongo via Docker, Apicurio ccompat v7). The `registeredVersion` field now persists on register, refreshes correctly on re-register (both changed and unchanged avro / idempotent path), and round-trips through the list endpoint into the frontend's `SchemaTemplate` type and the new `TemplateRegistrationBadge` rendering path. All backend and frontend test suites pass with no newly introduced failures or typecheck regressions. Environment note (not a code defect): Docker Desktop required a full restart (`wsl --shutdown` + relaunch) before Mongo was reachable — the engine was hung, unrelated to either code fix.

**Backend process left running** at `http://localhost:8010` (PID tree rooted at the venv `python.exe`) for handoff. Logs: `backend/uvicorn-out.log`, `backend/uvicorn-err.log`.
