# NIF Abstractor Backend API Endpoints (Simple List)

This is the minimal backend endpoint list for the current frontend.

## Dashboard
- `GET /dashboard/summary` — Get KPI cards (sources, running flows, verified schemas, failed connections, recent runs).
- `GET /dashboard/flow-status` — Get compact flow status list for the Dashboard card.
- `GET /dashboard/recent-activity` — Get recent admin activity for the Dashboard feed.

## Connections (Service-Level Only)
- `GET /connections` — List service connections shown on Connections page.
- `POST /connections` — Create a new service connection (Kafka/Apicurio/NiFi).
- `GET /connections/{connectionId}` — Get one connection’s details.
- `PATCH /connections/{connectionId}` — Update connection settings.
- `POST /connections/{connectionId}/test` — Test connection and update health/last tested.

Note: SMB is not a service-level connection endpoint here; SMB belongs to source configuration.

## Schema Manager
- `GET /schema-artifacts` — List schema artifacts with versions.
- `POST /schema-artifacts` — Create a schema artifact (initial draft version).
- `GET /schema-artifacts/{artifactId}` — Get one artifact with all versions.
- `GET /schema-artifacts/{artifactId}/versions/{version}` — Get one exact schema version for editing/view.
- `PATCH /schema-artifacts/{artifactId}/versions/{version}` — Save schema edits (fork if editing a verified version).
- `POST /schema-artifacts/{artifactId}/versions/{version}/generate` — Mark version as needs verification.
- `POST /schema-artifacts/{artifactId}/versions/{version}/verify` — Mark version as verified.
- `GET /schema-artifacts/{artifactId}/versions/{version}/linked-flows` — List flows linked to that exact version.
- `GET /schema-links` — Lookup flow-to-schema-version links (optional helper endpoint).
- `GET /drafts/schema-manager` — Load saved Schema Manager draft.
- `PUT /drafts/schema-manager` — Save/update Schema Manager draft.
- `DELETE /drafts/schema-manager` — Clear Schema Manager draft.

## Flows (Flow Designer + Flow Runner)
- `GET /flows` — List flows for Flow Runner.
- `GET /flows/{flowId}` — Get full flow details (including designer payload for edit mode).
- `POST /flows` — Create a new flow from Flow Designer.
- `PUT /flows/{flowId}` — Update an existing flow definition (stopped-only edit).
- `PATCH /flows/{flowId}` — Partial update (for example enable/disable schedule).
- `POST /flows/{flowId}/deploy` — Deploy flow to NiFi.
- `POST /flows/{flowId}/start` — Start flow (eligibility checks apply).
- `POST /flows/{flowId}/stop` — Stop flow.
- `DELETE /flows/{flowId}` — Delete flow.
- `GET /flows/{flowId}/metrics` — Get metrics for Flow Runner side panel.
- `GET /flows/{flowId}/bulletins` — Get bulletins for Flow Runner side panel.
- `GET /flows/{flowId}/runs` — Get run history for Flow Runner side panel.
- `GET /drafts/flow-designer` — Load saved Flow Designer draft.
- `PUT /drafts/flow-designer` — Save/update Flow Designer draft.
- `DELETE /drafts/flow-designer` — Clear Flow Designer draft.

## Audit
- `GET /audit` — List audit events with filters.
- `GET /audit/export` — Export audit events (CSV).

## Settings
- `GET /settings` — Get platform settings.
- `PUT /settings` — Save platform settings.

## System
- `GET /health` — API health check.
- `GET /meta/enums` — Return enums/reference values used by frontend forms.
