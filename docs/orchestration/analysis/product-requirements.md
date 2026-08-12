# Product / Feature Requirements — Data Mobility Platform MVP

**Scope of this report.** This report extracts every PRODUCT-LEVEL requirement stated in the source documents that sits **outside** the core flow-engine/adapter architecture (adapters, dedup, routing, and NiFi mechanics are covered by a sibling analysis). It covers: Application-Level Services, Platform Connections, Schemas/Schema Ceremony, APISIX/Gateway, Dashboard & Audit Log, DLQ & Metrics, OpenAPI upload/parsing, UI/UX workflows, and Security/credential handling.

**Source documents:**
- `C:\Users\kaifm\Desktop\Project\Fullstack\documents\plan.md` — "Data Mobility Platform — Engineering Specification" (referred to below as **plan2.md**, its own self-reference), the implementation-depth elaboration of the MVP. Section numbers below (`§NN.n`) refer to plan.md's own numbered sections unless stated otherwise.
- `C:\Users\kaifm\Desktop\Project\Fullstack\documents\concept.html` — **this is `mvp.html`** itself ("Data Mobility MVP — Complete Specification," dated 2026-08-05), the authoritative source plan2.md elaborates and MUST NOT contradict. Cited below as `mvp.html §N`.
- `C:\Users\kaifm\Desktop\Project\Fullstack\documents\work.md` — "Work Breakdown — Data Mobility Platform MVP," a 104-task, four-week plan derived from plan2.md/mvp.html. Cited below as work.md task IDs (e.g., `W4.1`).

Per plan2.md's own precedence rule (§01.9), mvp.html is the tiebreaker any time plan2.md and mvp.html appear to disagree; plan2.md is subordinate to it. Both were used here since plan2.md's normative language (MUST/SHALL) is more precise, while mvp.html's original prose is sometimes quoted directly for traceability.

Markers `[INFERRED]` and `OPEN:` are plan2.md's own provenance markers (§01.7–§01.9, §28) — an `[INFERRED]` point is a reasonable engineering completion of an MVP rule, not itself an MVP guarantee; an `OPEN:` point is a genuine gap plan2.md deliberately did not invent an answer for. Both are preserved below where load-bearing.

---

## 1. Application-Level Services

/ Source: `plan2.md §15 Application Services & Variables`, `mvp.html §12 Application Services`, `plan2.md §06.1–§06.2 (http)`, `plan2.md §07.2 (jdbc)` /

### 1.1 What they are and why they exist

Application Services are "the platform's single mechanism for holding endpoint and credential information outside of stream/block configuration" (§15.1). Every adapter that reaches an external system (`http`, `jdbc`, `kafka` reads against an external cluster, `kc`, `kafka_kc`) **MUST** reference a saved service; **no adapter form field may hold a raw hostname-plus-credential pair as free text**. A stream document (a block's persisted config) MUST NOT ever contain a credential value, bearer token, or connection string with an embedded password — it holds a service reference (an ID) and nothing else pertaining to auth. This is explicitly what makes a connector export secret-free "for free" — there is nothing to strip.

Platform Kafka (the platform's own cluster) needs **no** Application Service at all — it is reached through the Kafka **Platform Connection** directly, because it is infrastructure the platform itself operates, not an external system a flow author is credentialing into (§15.1, mvp.html §12).

### 1.2 Exactly four service types (no fifth exists)

| Service type | Consuming adapters | Fields it holds |
|---|---|---|
| **HTTP service** | `http` read / write / lookup | base URL; auth method (`none · basic · bearer · api_key [header or query param] · oauth2_client_credentials · session_token`); TLS policy; timeouts |
| **Database service** | `jdbc` read / write / lookup | `dialect` (`postgresql`, `trino`, `mysql_mariadb`); platform-curated driver reference (never a user-uploaded jar); host/port; `database`; credentials; `read_write_capability` flag (admin-set, declares whether this instance may back read blocks, write blocks, or both) |
| **External Kafka receiver** | `kafka` read against an **external** cluster only | brokers; security protocol/SASL mechanism; credentials — **input-only, never usable as a write destination** |
| **Sink destination service** | `kc`, `kafka_kc` | endpoint + credentials the sink configuration references (e.g., an OpenSearch endpoint, an Iceberg catalog endpoint) |

(§15.1 table; identical table restated in mvp.html §12 and in work.md `W4.1`.) Each row is exhaustive as written: an HTTP service MUST NOT be extended to carry a Kafka broker list, and a Database service MUST NOT be repurposed to authenticate an `http` block — enforced at the picker level (a block's service picker lists only its own adapter family's service type).

### 1.3 HTTP service — auth methods in full (my focus area's headline item)

Per `§06.1 The HTTP service binding`, auth method is selected **once on the service** and applied to every request the service originates. The method set is exhaustive at exactly six values; the adapter form MUST NOT expose additional auth types in the MVP:

| Method | Behavior |
|---|---|
| `none` | No credential material attached to requests. |
| `basic` | Username/password sent as `Authorization: Basic` header, resolved server-side only. |
| `bearer` | Static bearer token attached as `Authorization: Bearer <token>`. |
| `api_key` | Key/value pair injected as **either** a request header **or** a query parameter — user's choice at service configuration time. |
| `oauth2_client_credentials` | Compiles to the runtime engine's native token-provider construct (e.g., a NiFi OAuth2 token provider controller service). The platform MUST NOT persist issued tokens — acquisition/refresh delegated entirely to the compiled component. |
| `session_token` | See §1.4 below — the MVP's replacement for the alpha's ad hoc "token bootstrap" stream. |

### 1.4 Session-token auth — the MVP's headline HTTP auth mechanism

This is decision-ledger **ruling 8**: "Token bootstrap is the **session token** auth method on the HTTP service — never a chained stream" (mvp.html §16). Per `§06.2 Session-token bootstrap`:

- Session-token auth is a **service-level configuration**, never a separate chained stream, never a distinct block type.
- The service definition holds a **login request config**: HTTP method, path, and body template for the login call; the **token location in the response** (e.g., a JSON body field path, or a specific response header); and the **injection template** describing how the extracted token is inserted into every subsequent request (e.g., `Authorization: Bearer ${token}`).
- The login request MUST fire **exactly once per run**, before the block's own request chain begins.
- **A failed login MUST fail the run as a whole** — never any individual record — because at the point of failure no records exist yet to fail individually (this is the run-failure branch of the failure taxonomy, §18.5.2).
- Logins MUST NOT be modeled, represented, or compiled as data-producing streams — no record shape, no field pickers, invisible to the flow's record space entirely.
- `[INFERRED]` The extracted token SHOULD be held only for the run's duration (in-memory) and MUST NOT be persisted to any datastore/log/export — consistent with "the platform never stores OAuth2 tokens."
- `[INFERRED]` If a session-token service is shared by multiple concurrently-running flows, each run performs its **own independent login** (no cross-run token caching) — the MVP states "once per run" but doesn't describe cross-run reuse; this is the safest reading.

The glossary (§27, "session token") restates: "An `http`-service authentication method (replacing the alpha's ad hoc 'token bootstrap' stream)... The login fires once per run, before the chain starts. A failed login is classified as a run failure, never a record failure."

work.md task `W1.18` ("Session-token auth (token bootstrap)") captures this as a `[partial]` — a working alpha implementation exists and is ported/checked against the new design.

### 1.5 Manual mode = inline private-service creation, never a bypass

`§15.2`: a stream's service-selection control offers two paths that both terminate at the same underlying object — pick an existing saved service, or "manual" entry, which visually looks like typing credentials directly but **on Save creates a real Application Service record**, marked private/manual (excluded from the general picker). This collapses a named alpha defect: previously, manual credentials were copied onto every source block using manual entry, producing N copies of the same secret with N independent rotation/redaction paths. The MVP's fix gives exactly **one storage path**, **one redaction path**, **one place to rotate a password** (§15.2). A private/manual service:
- Still goes through the same secret storage-as-reference discipline as a named service.
- `[INFERRED]` MAY later be promoted to a shared, named service by an explicit rename/re-flag action (no promotion UI is specified).
- Is otherwise indistinguishable from a named service — gets revisions on edit, logical retirement on delete, is independently Test-able.

### 1.6 Edits create revisions — never a live mutation

`§15.3`: editing any Application Service (any of the four types) MUST NOT mutate the record a running flow is pinned to — it MUST create a new **revision**. A flow's compiled binding is pinned to a specific **revision ID**, not the service's mutable "current" pointer:
1. Every flow referencing the service is flagged `service_update_available`.
2. A flow already Started/Running on the prior revision continues unaffected — the edit never propagates into a live runtime.
3. The new revision is adopted **only at that flow's next Deploy/Redeploy**.
4. `service_update_available` clears once the flow redeploys and adopts the new revision. `OPEN:` whether it can be dismissed without a redeploy, or persists as a nag indefinitely.

Revisions are append-only and retained at minimum for as long as any flow is pinned to one, so "which revision is this running flow actually using" is always answerable from the ops view (§15.3, §19.9).

### 1.7 Delete = logical retirement, never a hard delete

`§15.4`: deleting a service is **always permitted** and always resolves to **logical retirement** (a deliberate reversal of the alpha's "simply refuse deletion with an error"):
- Disappears from every service picker (new streams can't select it; existing streams being edited can't re-select it).
- A running flow on a pinned revision of the retired service keeps running unaffected.
- Any referencing flow (deployed or not) is flagged **`action required`** and blocked from redeploying until rebound to a different, non-retired service of the same type.
- Retirement is itself audited.
- `OPEN:` whether logical retirement is reversible (un-retire) or permanent once invoked.

### 1.8 Service Test vs. block Test — two different questions

`§15.5`: **Service Test** proves the service itself is reachable and credentials are valid (broker metadata for a receiver, a login round-trip for a Database service, a reachability probe for an HTTP service) — recorded on the service (health status + last-tested timestamp), independent of any flow. **Block Test** (§13/§8 of the MVP) proves a specific block's *full configuration* works end-to-end using its currently-selected service, capped at 10 sample records, committing nothing. The two are never conflated: Service Test never runs a block's parsing/records path; Block Test never writes back to the service's own health record.

### 1.9 External clusters sit outside the fingerprint system (by design)

`§15.6`: an External Kafka receiver is explicitly **not** a Platform Connection and does not participate in fingerprint/repoint machinery — the platform has no way to prove "same physical cluster" for infrastructure it doesn't own, so it doesn't try. Moving/rotating an external cluster = edit the service (new revision); flows adopt at next deploy; position continuity is carried by per-run offset snapshots and the audited seek action — "out of scope for machines we don't own," stated explicitly. A Sink destination service similarly sits outside the fingerprint system `[INFERRED — structural, since fingerprinting is scoped to Platform Connections only]`.

### 1.10 Variables (adjacent to Application Services)

`§15.7`, mvp.html §12: two coupled surfaces — a **global admin screen** (name, value, `secret` boolean flag) and a **per-flow variables section** in the builder (same shape). Flow-scoped value overrides a same-named global for that flow. Variables compile into the **parameter contexts** the export mechanism mandates (§14/§20), making them first-class named parameters resolvable at deploy time, not just a builder-UI convenience. They are one of four recognized value sources for OpenAPI-derived parameter bindings on `http` blocks (§1.11 / §6.4 below) and usable inside `${...}` body/header templates. **Ask-at-runtime is never a variable** — it is a value-source designation meaning "prompt the user," existing only at block-Test time and schema-ceremony time; it never appears on the Variables screens, and Deploy preflight MUST refuse a flow with an unresolved required ask-at-runtime binding. Secret-flagged variables follow the same secret-handling discipline as service credentials.

---

## 2. Platform Connections

/ Source: `plan2.md §16 Platform Connections & Lifecycle (Repoint)`, `mvp.html §11 Platform Connections`, decision-ledger rulings 25–29 /

### 2.1 Exactly six connection types (no seventh; legacy Iceberg type removed)

| # | Type | Backs | Fingerprintable identity |
|---|---|---|---|
| 1 | `nifi` | Flow Builder compile/deploy target, ops-view runtime | root process-group ID |
| 2 | `kafka` | Platform Kafka cluster (writes; reads when no external receiver selected) | Kafka cluster ID |
| 3 | `apicurio` | Schema registry for `kafka_kc` | none — registry has no stable identity |
| 4 | `kafka_connect` | Connect worker hosting all `kc`/`kafka_kc` sink connectors | `[INFERRED]` Connect worker cluster ID |
| 5 | `redis` | Dedup fingerprint cache + jdbc incremental bookmarks | none — simplified path |
| 6 | `api_gateway` | APISIX-managed proxy egress (`proxy: on`) | none — simplified path |

(§16.2; mvp.html §11 table.) "The old Iceberg connection type is removed — catalog/table details for lakehouse sinks now live on a Sink destination Application Service, not on a platform connection." Each type is **DB-enforced to have exactly one active connection at a time** — a database constraint (a partial unique index on `(type) WHERE active = true`), never an application-layer habit a bug can bypass (§16.2). Multiple *inactive* connections of a type may coexist (repoint targets, archived connections, pre-registered future connections).

### 2.2 Connection auth/config per type (from mvp.html §11's table, restated)

- **NiFi**: API URL, auth, TLS.
- **Kafka**: brokers, security protocol, credentials (native mode or Kafbat-proxy mode).
- **Apicurio**: registry URL, auth, group.
- **Kafka Connect**: worker URL, auth.
- **Redis** *(new vs. alpha)*: host, database, credentials — **standalone mode only**.
- **API Gateway / APISIX** *(new vs. alpha)*: admin URL (backend-only) + runtime URL, credentials.

### 2.3 Seeding, health vs. reachability

On fresh install, the **essential** connections — NiFi, Kafka, Apicurio (registry) — MUST be seeded from environment variables and tested immediately at boot; resulting health MUST resolve to `healthy` or `failed`, **never** left `not_tested`. There is **no hardcoded fallback address** — if the env var is absent, no connection record is created at all (§16.4). `[INFERRED]` Redis, Kafka Connect, and API Gateway MAY also be seeded where present, under the same rule. The first connection of any type auto-activates; every subsequent connection of that type is created inactive.

Two independently-recorded facts, never conflated: **Health** (from pressing Test/Test-All — validates credentials + reachability, `healthy`/`failed`) and **Reachability** (from the identity probe Activate runs, `reachable`/`unreachable`). **Neither changes on its own — there is no background polling of any connection, of any type, ever.** Test never changes which connection is active (§16.5).

### 2.4 Secret handling on connections

Secrets (passwords, tokens, TLS client keys) are stored as references, never plaintext, and **never returned** to any client — API responses carry only presence flags (`has_password: true/false`). A blank secret field on edit means "keep the existing one," never "clear it." Per fixed-vs-alpha ruling 29: platform Kafka's **full security config** (protocol, SASL mechanism, credentials) MUST reach compiled NiFi/Connect components as **secret-flagged parameters**, not merely the bootstrap-server endpoint — the alpha's endpoint-only propagation is explicitly disallowed going forward (§16.6).

### 2.5 The one shared dependents check (ruling 25)

"One dependents-check implementation MUST be used identically by four call sites: Activate, Edit, Delete, and Impact Preview" (§16.7). The alpha shipped four inconsistent implementations, producing bugs; this MVP forbids recurrence by construction. Four buckets, applicable per type:

| Bucket | Counted for | Definition |
|---|---|---|
| Flows | nifi, kafka, kafka_connect | Flows whose deployed runtime is pinned by provenance to this connection |
| Schema approvals | apicurio | Approved `kafka_kc` schemas registered against this registry connection |
| Connect connectors | kafka_connect | Live `kc`/`kafka_kc` connectors running on this worker |
| Redis scopes | redis | Dedup caches + jdbc incremental bookmarks keyed against this Redis |

Redis and API Gateway do **not** go through the dependents-blocking path (ruling 27) — the shared check still computes their counts for display, but never blocks Activate/Edit/Delete for those two.

### 2.6 Activation rules

- **Activate is blocked** while the currently-active connection of that type has dependents (§16.7) — remedy is Repoint, not force-activation. Applies to `nifi`, `kafka`, `apicurio`, `kafka_connect`.
- **Activate is blocked** if the **target** connection fails its reachability probe.
- **Every connection type is probeable** — the alpha left two types unable to activate because probe branches were simply missing; this MVP requires a probe for all six, no gaps.
- **Endpoint/credential edits are refused** on a connection with dependents — must go through Repoint instead.
- **Impact Preview** is available before Activate/Edit/Delete on any connection, rendering **actual dependent lists with counts**, never an unrenderable blob (fixing a named alpha bug) (§16.8).

### 2.7 Redis and API Gateway — the simpler path (ruling 27)

Neither has a machine identity to fingerprint, so both skip Repoint entirely (§16.9):
- **Redis switch is never blocked.** It computes and displays real counts (dedup fingerprint windows that will be lost; jdbc incremental bookmarks that will be lost), requires **explicit operator confirmation**, and relies on **fail-stop dedup** (Redis down ⇒ records needing dedup fail to DLQ rather than pass unsuppressed) to protect correctness during/after the switch.
- **Gateway switch** triggers automatic **re-reconciliation** of every managed gateway resource in dependency order: **certificates → upstreams → routes**. Nothing else about flows changes.
- Both still populate the dependents buckets for Impact Preview display — the simplification is that the count never **blocks**, only **warns**.

### 2.8 Repoint — the lifecycle engine (the highest-bug-surface subsystem per work.md)

Repoint is the **only** supported mechanism for moving a connection with live dependents, for the four fingerprintable types (`nifi`, `kafka`, `apicurio`, `kafka_connect`) (§16.10). Fingerprints prove "same physical service": NiFi's root process-group ID, Kafka's cluster ID, `[INFERRED]` Connect worker cluster ID; Apicurio has **none** (repoint against a registry relies on operator intent, not proof); Redis/Gateway don't apply (bypass entirely).

If the platform cannot prove the new endpoint is the same physical service, Repoint **blocks** unless the operator explicitly chooses **reset** (abandon).

| Strategy | When | Effect |
|---|---|---|
| `adopt` | Same physical service, address changed | Provenance rebinds only — **nothing is redeployed**. Licensed by fingerprint match. |
| `migrate` | Genuinely new/different service | Dependent flows **undeployed, rebound, redeployed** (all-at-once or one-by-one). For Kafka Connect specifically, **every live connector (kc and kafka_kc) is re-created on the new worker** — a full recreate, not a rebind, because connector state lives on the worker. |
| `reset` | Abandoning old runtime outright | Old artifacts recorded as **orphans** (never deleted). Flows **rebound** but **left stopped/undeployed for manual redeploy** — no auto-redeploy. |

Load-bearing internal rules: Repoint **never mutates the old connection document** — always creates a new one; the old is marked `archived: true`, remains visible, removable later. **Migrate activates the new connection before redeploying** dependents. Every repoint runs under a **lock** scoped to the connection type. Repoint maintains a **resumable, per-item progress log**. Every repoint operation, per-item result, and failure is **fully audited**. Provenance pins are for impact/drift detection **only**, never for routing — runtime always uses whichever connection is currently active.

### 2.9 Loud self-healing when no active connection exists (ruling 26)

If any operation requires an active connection of a type and finds none, the platform **auto-promotes** the most-recently-updated connection of that type — audited (actor = system, reason = "no active connection found") and **surfaced as a UI banner**, never silent. If no connection of that type exists at all, the operation fails outright with a clear error naming the missing type (§16.11).

### 2.10 Registry (Apicurio) protection

The active Apicurio connection **cannot be edited or deleted while any approved schema depends on it** — the Connections screen displays this as a live dependency count (§16.12). See also §3 below for the registry-repair mechanics this protects.

### 2.11 Connections screen requirements (ruling 29)

The screen (admin-facing "by convention" — this MVP has **no roles/RBAC**, stated plainly) MUST (§16.13, mvp.html §11):
- Show **multiple connections per type**, each row with an explicit **active** marker.
- Let operators **name connections themselves**, name preserved across saves (fixes the alpha's name-overwrite-on-save bug).
- Offer **Activate** and a **Repoint dialog** (strategy picker: adopt/migrate/reset; pace picker for migrate: all-at-once vs. one-by-one; explicit abandon confirmation) with a **live lifecycle-job progress view**.
- Render **Impact Preview** with real dependent lists/counts before Activate/Edit/Delete.
- Carry over, now fixed: **Test All** (bulk health check), **per-connection Test**, edit with blank-keeps-existing-secret, delete-with-impact-confirmation.
- `[INFERRED]` Each row SHOULD show both `health` and `reachable` (with timestamps) as visually distinct indicators.

`OPEN:` no concrete polling/refresh cadence is stated for the Connections **screen** itself (distinct from the underlying no-background-polling rule for the facts themselves) — assume manual refresh only. `OPEN:` whether migrate's "one-by-one" pace exposes a pause/resume control mid-run, or is merely slower sequential execution.

work.md maps this subsystem to `W1.14`–`W1.16` (foundation: six types, one-active constraint, health/reachability, minimal screen) and `W4.5`–`W4.9` (full repoint engine, self-heal, audit, full screen) — flagged as "the highest-bug-surface subsystem" (Milestone 4.2 outcome).

---

## 3. Schemas — Ceremony, Registry, Inference

/ Source: `plan2.md §14 Schema Ceremony & the Registry`, `mvp.html §9 The schema ceremony & the Schemas screen` /

### 3.1 The single trigger

The schema ceremony SHALL be initiated by **exactly one** event in the entire application: configuring a `kafka_kc` block. No other action MAY start a ceremony. Plain `kafka` writes stay schemaless forever (never surface a schema editor); `kc` sinks never present any schema surface at all (they move bytes they didn't produce); `http`/`jdbc` write blocks never reference schemas — their Test-time shape is a building convenience only, never a governance artifact (§14.1).

### 3.2 The four-step state machine

| Step | Name | Entry | Exit |
|---|---|---|---|
| 1 | **Declare** | Block placed, entity not fixed | Entity typed/validated (tokenized); topic name `raw.<source>.<entity>[.<variant>]` and table name `bronze.<source>.<entity>__raw` computed read-only |
| 2 | **Orchestrate** | Entity fixed | Candidate schema produced via exactly one of 3 evidence paths |
| 3 | **Review** | Candidate exists | Accepted as-is, or edited and re-validated |
| 4 | **Approve** | Reviewed schema valid | Registry registration succeeds |

(§14.2.) No live data flows before Approval — absolute, no exceptions, no partial-deploy state.

### 3.3 The three evidence paths (§14.3)

1. **Live sample run** — the platform deploys a **temporary copy of the flow's real upstream chain**, output redirected to a throwaway topic `<derived topic>-schema-inference` (plain JSON, never Avro), sampling toward **10 messages** with a **hard cap of 100**, polling until stable under one time budget. Zero messages is a first-class legible failure (a 3-point checklist naming the throwaway topic), not a hang. **Cleanup is unconditional** on success/failure/Stop/backend restart: temp runtime, temp topic, and ceremony lock all torn down. Only **one ceremony per flow at a time**. **Commits nothing** — no jdbc bookmark, no Kafka offset, no dedup marker (§14.5). Preconditions: flow saved; NiFi/Kafka/registry connections configured; every `${placeholder}` prompts for a value; if the upstream chain contains a mutating write (POST/PUT/PATCH/DELETE), an **explicit extra confirmation** beyond "run this ceremony" is required, and the UI SHOULD steer toward uploaded sample files as the safe alternative.
2. **Uploaded sample files** — JSON, XML, CSV, or XLSX, with a mandatory explicit **record-boundary choice** (each file = one record, vs. rows inside each file = records) — never inferred/defaulted.
3. **Manual authoring** — blank record or pasted raw Avro JSON, no sample data at all. **MAY be approved with zero sample evidence** — a first-class supported path, not a degraded fallback — but MUST carry a persistent, visible provenance flag reading exactly `"manually authored — not sample-validated"` everywhere the schema is displayed post-approval (§14.4).

These three paths are mutually exclusive **per ceremony run**; a user MAY abandon one path and re-run Orchestrate with a different path before Review.

### 3.4 Inference rules (deterministic, apply to live-run and uploaded-file paths — not manual authoring)

Per `§14.6`:
- **Every field is nullable with a null default** — no inferred field may be required.
- **Objects become Avro records, never Avro maps** — a nested JSON object always maps to a nested `record` type.
- **Field order is first-seen** — never alphabetized or reordered by type.
- **Mixed types degrade safely**, in precedence: string beats everything; any float observed → `double`; integers-only → `long`.
- **Timestamp detection**: integer fields whose name looks time-related (`_ts`, `_time`, `date`, …) AND whose values look like plausible epoch magnitudes are promoted to a timestamp logical type.
- **Names are sanitized and deduplicated** — Avro-identifier legality, collision resolution.
- **Depth-5 collapse** — nesting infers to max depth 5; deeper structure collapses to a single `string` field at depth 5 (a hard cap, not a soft warning).

### 3.5 The editor (Review step)

Two synchronized tabs over one buffer (§14.7): (1) a **structured editor** — recursive field table to depth 5 (`name`, `type` dropdown covering scalars/logical types/nested constructs, `nullable` switch, per-field `doc`); fields beyond depth 5 or exotic constructs appear as **protected "advanced" nodes**, not editable structurally. (2) a **raw Avro JSON tab** — the only surface for edits the structured view can't express; invalid JSON blocks saving with an inline parse error, never silently discarded. Normalization on every save: nullable unions ordered **null-first** with `default: null`; the **schema root MUST be a record**.

### 3.6 Edits, re-validation, no evolution

`§14.8`: with sample evidence (live run or uploaded files), every edit is re-checked against the **same sample set** before Approve unlocks; failures show inline with counts and a first-failure example ("3 of 10 records fail; first failure: …"). With no evidence (pure manual), validation is **Avro-validity only**. **No schema evolution, ever** — once Approved, a schema MUST NOT drift, auto-widen, or silently accept out-of-shape records; a record that doesn't fit at runtime is a genuine DLQ failure. The only sanctioned remedy for a real shape change is a **deliberate, explicit re-run of the ceremony** followed by a redeploy.

### 3.7 Approve = register

`§14.9`: Approve (1) validates the final schema one last time, (2) **registers** it under subject `<derived topic>-value` using the **Confluent-compatible registry endpoint first**, native fallback if unavailable, (3) **records the approval** (schema, approving actor, timestamp, an **evidence fingerprint**, and provenance classification). **Registration failure fails the approval** — there MUST NOT exist an "approved locally but not registered" state; this is a hard invariant. Re-approving an unchanged, already-registered schema is a **no-op** (idempotent). On success: the deploy gate for the owning flow opens.

### 3.8 Registry repair, deploy-time self-healing, connection protection

`§14.10`: NiFi/Connect resolve schemas from the registry at runtime with **auto-registration off** — the platform is the sole author of registrations. **Every deploy re-checks** every approved schema still exists in the registry; if missing, deploy **re-registers it from the platform's locally stored copy** automatically; deploy blocks only if that re-registration itself fails. If the active Apicurio connection is repointed/reset, every dependent approval is marked **"needs re-registration"**, self-healed at each affected flow's next deploy. The registry connection **cannot be edited/deleted while any approved schema depends on it**. **Start never re-checks schemas** — Deploy is the only schema gate (ruling 24).

### 3.9 Approval scope, pre-fill, forked-branch ceremonies

- An approval belongs to **one stream, in one flow** — never a connector/template/shareable artifact. A connector export **never carries a schema**; importing always re-runs the ceremony fresh (§14.11).
- **Pre-fill** copies a candidate from a sibling variant's approved schema, or any approved schema picked from the Schemas browser — **strictly a copy**; editing the pre-filled candidate never alters the source, and Review/Approve still happen per-stream, independently (§14.12).
- **Identical (plain, unconditional) sibling branches** carrying the same entity **share one topic and run exactly ONE ceremony** (one schema, one Approve, N Connect sinks). **Each diverged branch** (a route/filter/transform sits between the common ancestor and the write) **runs its OWN ceremony**, approved separately, with pre-fill offered by default from the nearest sibling (§14.13).

### 3.10 The Schemas screen — strictly read-only

`§14.14`: lists every approved schema — Flow, Stream, Entity, Topic, Approval date, Provenance (with the "not sample-validated" flag when applicable). Actions: **View** the Avro definition; **Jump to the registry** (deep link into Apicurio/Confluent-compatible view); **Search and filter**; and the **single mutating action** — *"start a pre-filled ceremony from this schema."* **Explicitly removed/absent**: the prior Draft → Needs-Verification → Verified pipeline, standalone schema artifacts, version-number dropdowns, delete-cascade operations. "Approval *is* registration, and re-running the ceremony *is* versioning" — no separate versioning concept is exposed.

### 3.11 Open items specific to the ceremony (from plan2.md §14's own open list)

- `OPEN:` exact upload size/file-count caps for the uploaded-sample-files evidence path are not stated in the MVP.
- `OPEN:` precise "sample set stability" predicate and overall time-budget duration for the live sample run's polling loop are not stated.
- `OPEN:` provenance labeling behavior when a manually authored schema later gains attached samples without a full ceremony re-run.

work.md maps this subsystem across `W1.31`–`W1.34` (minimal ceremony vertical slice) and `W2.14`–`W2.20` (full ceremony: all evidence paths, full editor, pre-fill, registry repair, read-only screen, forked-branch sharing).

---

## 4. APISIX / Gateway

/ Source: `plan2.md §15.8 Gateway resources admin`, `§06.10 Gateway egress (proxy: on)`, `§22.7 APISIX egress`, `mvp.html §12/§17` /

### 4.1 What the product does with APISIX

APISIX is a **Platform Connection** (§2 above) backing `proxy: on` egress for `http` blocks. It exists **specifically** because NiFi's HTTP client refuses connections to endpoints with broken/nonstandard TLS (self-signed certs, mismatched hostnames, deprecated cipher suites) and has no native client-certificate presentation model — rather than weaken NiFi's TLS posture globally, such calls route through the gateway as an isolated, explicitly-opted-into egress path (§22.7.1).

Setting `proxy: on` on an `http` block unlocks two things a direct call cannot: (a) reaching endpoints with broken/nonstandard TLS, and (b) presenting a **client certificate** for mutual-TLS endpoints (§22.7.2, §06.10).

### 4.2 Gateway resource admin — four resource kinds

Configured via a **dedicated admin surface**, distinct from both Application Services and Platform Connections (§15.8):

| Resource | Holds | Notes |
|---|---|---|
| **Client Certificate Profile** | certificate chain + private key (secret references); expiry surfaced | referenced optionally by an Upstream |
| **Upstream** | approved host/port; SNI; timeouts; optional Client Certificate Profile reference | the thing a Route points at |
| **Route** | path + HTTP method(s) → exactly one Upstream | the unit `proxy: on` resolves through |
| **Host allowlist** | the set of hosts admins have approved for gateway egress at all | gates which Upstreams may even be created |

**Deletion is reference-counted and enforced in reverse dependency order**: **Route → Upstream → Certificate Profile** (delete children before parents); the platform rejects a deletion that would orphan a dependent resource — never a silent cascade (§15.8).

**Each resource independently shows its reconciliation status** against the live gateway — desired-state-vs-observed, mirroring the platform's drift-detection posture elsewhere. `OPEN:` the exact reconciliation-state enum/state-machine (`reconciled`/`pending`/`drift`/`error`?) is not specified — left to implementation, constrained only by "never silent."

### 4.3 Constraints and security posture on `proxy: on`

- **Admin-allowlisted hosts only** — a `proxy: on` call cannot route to an arbitrary host; the target must resolve to a configured, approved Upstream+Route pair. A block targeting a host with no matching Upstream/Route **MUST fail deploy preflight** with a clear reason (§22.7.3).
- **Upstream certificates are not verified in this mode — stated plainly.** The UI MUST state this at the point the user enables `proxy: on` — a deliberate, disclosed trust reduction for an admin-allowlisted, egress-only path, not a general bypass; direct (non-proxied) `http` blocks retain normal TLS verification (§22.7.4).
- **Redirects are always refused**, proxied or not (§06.10, §22.7.6) — closes an SSRF-adjacent risk (an allowlisted upstream can't be used as a stepping-stone via a 3xx response).
- **Egress only, never inbound** — the gateway MUST NEVER be configured/exposed as an inbound listener or webhook receiver (§22.7.5) — consistent with webhook/syslog being fully shelved and absent from every picker.
- Deploy preflight MUST verify gateway routes are resolvable for every `http` block with `proxy: on` before the flow may run.

### 4.4 Reconciliation on connection switch

Switching the active Gateway/APISIX Platform Connection triggers full **re-reconciliation** in dependency order: **certificates → upstreams → routes** (§15.8, §22.7.7, §16.9). Nothing else about flows using `proxy: on` changes — they transparently pick up the new routes.

work.md maps this to `W4.4` (gateway resources admin: Cert Profiles, Upstreams, Routes, host allowlist, reference-counted reverse-order deletion, reconciliation status) and `W4.10`–`W4.11` (egress routing through APISIX + reconcile/TLS-honesty), landing in Week 4 / Milestone 4.3.

---

## 5. Dashboard and Audit Log

/ Source: searched exhaustively; `plan2.md §19 Observability & the Ops View`, `§22.8 Audit`, `mvp.html §13` /

### 5.1 Finding: there is no standalone cross-flow "Dashboard" screen specified

Both source documents were searched for the term "dashboard." It appears exactly **once** in plan2.md, generically ("dashboards continue to show honest live FlowFile/offset figures," §19.5) — not as a named product screen — and **zero times** in mvp.html/concept.html. **The MVP does not specify a dedicated, cross-flow "Dashboard" feature.** The closest analogues the spec actually names are:
- The **Flow inventory/list** (work.md `W3.18`; §17.10): search/filter/sort, create/rename/duplicate/delete, bulk actions.
- The per-flow **Ops view** (§19, mvp.html §13 "Running flows: verbs, safety & watching") — the only place engines become visible, scoped to one deployed flow.
- The **Connections screen**, **Application Services screen**, and **Schemas screen** — each its own admin surface, not unified into one dashboard.
- The **Audit log** (§22.8) — searchable, exportable, but not framed as a "dashboard."

Any downstream design work assuming a unified cross-flow dashboard should treat this as a gap to raise, not an existing requirement — see Open Questions below.

### 5.2 Audit log requirements

`§22.8` (elaborating mvp.html §17's one-line "every verb, destructive action, approval, and service edit — searchable in-app, exportable as CSV"):

**Scope — at minimum:**
- Every flow lifecycle verb (Deploy, Start, Pause, Resume, Stop, Stop & Clear, Redeploy, Undeploy, Delete).
- Every destructive/state-clearing action (Clear Queues, Clear dedup cache, Clear Topics with per-topic before/after counts including partial failures, consumer-group offset-skip).
- Every schema-ceremony approval (schema, actor, time, evidence fingerprint, provenance).
- Every Application Service edit (creation, revision, logical retirement).
- Every Platform Connection lifecycle action (Activate, Repoint with strategy/pace/per-item results, self-heal auto-promotion banners).
- Every gateway resource change (Client Certificate Profile / Upstream / Route create-edit-delete).

**Fields** `[INFERRED]`: actor (whatever identity the auth layer establishes — no RBAC/permission levels in this MVP), timestamp, action/verb type, target entity identifier, outcome (success/failure with reason), and a structured payload of what changed (before/after counts, repoint strategy chosen, schema evidence fingerprint, etc.).

**Searchable and exportable**: the audit trail MUST be searchable in-app (by actor, target, action type, time range `[INFERRED]`) and **exportable as CSV** on demand. The CSV export MUST itself respect secret-masking — only action metadata is ever recorded, never a secret value even masked.

**Reads are not audited as actions** — opening the ops view, viewing an impact preview, browsing Schemas never itself mutates state or gets an audit entry; only actions with effect are logged, keeping the trail meaningful rather than noisy.

work.md maps this to `W4.16` ("Audit + CSV export," `[partial]` — carried from alpha, extended to the new event set).

---

## 6. DLQ and Metrics

/ Source: `plan2.md §18 Reliability, DLQ & the Failure Taxonomy`, `§19.5 Per-block metric attribution`, `mvp.html §13` /

### 6.1 The per-flow DLQ — identity, provisioning, contract

Every flow owns **exactly one** DLQ topic named `dlq.<flow>` (flow's source name), never shared across flows, provisioned with **7-day retention** (§18.3). DLQ topics are created at **Deploy**; **Undeploy leaves the DLQ topic and contents intact** (only generated *data* topics are emptied) `[INFERRED, but corroborated directly by ruling 18's Delete-specific language]`; **Delete removes it** (with ownership proof); **Redeploy leaves it unchanged**. DLQ provisioning MUST configure `max.message.bytes` to accommodate the **110 MiB ceiling**, verified at deploy preflight.

### 6.2 DLQ entry shape

`§18.4`: Value = **original bytes**, unmodified (raw HTTP response bytes / raw Kafka message bytes / raw row bytes). Headers = which **block** produced the failure + which **error class**. Key = `[INFERRED]` preserved from the source record's key when one exists. The entry is **never re-parsed, re-shaped, or coerced** before writing — DLQ inspection shows the literal input.

### 6.3 The failure taxonomy — three classes, precisely

`§18.5` / ruling 41, restated in full in mvp.html §13:

- **Record failures** — one record, single block. **Retryable** (transient network error, momentary connection blip) get **3 capped-backoff retries** before DLQ; **permanent** (schema violation, unresolvable field mapping, un-coercible value) go to DLQ **immediately, zero retries**. A record reaching successful `DLQ_WRITE` is **HANDLED** — counted as a failure metric, but the run succeeds and positions advance (**handled-includes-DLQ**, ruling 42).
- **Run failures** — no record exists yet to fail (session-token login failure; a root request erroring, including an **HTTP 500 on page 3 of a paginated read**; connection refused at root). Marked in run history with a failure class; **never fabricated as a DLQ record**; positions/bookmarks untouched — next cron starts fresh.
- **Infrastructure failures** — a required dependency (Redis, NiFi, and by extension the Connect worker `[INFERRED]`) is down. **Fail-stop** — the flow/block stops rather than routing around it — and surfaces as a runtime event, never silently swallowed or retried indefinitely.

### 6.4 Own-queue parking (last line of defense) and the 110 MiB ceiling

If the **DLQ write itself fails** (topic unreachable, over quota, broker rejects), the record MUST **park in the flow's own queue** rather than be dropped or block the platform globally — "one poison record can only jam its own flow" (§18.6). The stated repair is **Clear Queues** — explicit, audited, and a deliberate data-loss action, never a side effect of Stop. A record whose original bytes **exceed 110 MiB** cannot be written to the DLQ and follows the **same parking path directly** (§18.7). `OPEN:` whether an oversized-record poison event is distinctly labeled from a generic DLQ-write-failure parking event.

### 6.5 Intentional outcomes are never failures

Filtered drops, dedup suppressions, and empty runs (a source legitimately producing zero records) MUST be counted in metrics but **never classified as, or reported alongside, failures**. Tombstones (null-value Kafka messages) belong to the same bucket by extension — skipped as counted, successful no-ops (§18.9).

### 6.6 Metrics — the ops view's honesty requirements

`§19.5`: The compiler's **runtime-scope map** (block → generated components) is the **sole** attribution mechanism — the Metrics panel MUST use it, never naming-convention guesswork at read time. Numbers sourced from NiFi MUST be labeled **FlowFile counts**, never silently renamed "records processed" ("FlowFiles: 1,204" not "Records: 1,204"). A failed metrics query renders an explicit **"unavailable"** state — MUST NOT render `0` (a genuine zero and an unreachable read are different facts). No metric carries an unverifiable time-window label (e.g., "last 24h") unless the query genuinely bounds itself to that window. **Topic counts** derive from broker offset-spread (low/high watermark), with the **compaction caveat stated** wherever shown for a topic that could be compacted. **Live numbers and stored history are visually separated** — run history and DLQ count sit beside, never inside, the live grid; clearing topics MUST NOT retroactively alter run-history rows.

### 6.7 Ops-view panel loading, cadence, transport (metrics-adjacent, from §19.2–§19.4)

- **Nothing fetched until visible** (ruling 30) — no eager warming on ops-view open.
- Cadence: Metrics 10s while Running / 30s otherwise; Kafka message viewer and DLQ view 10s while visible; Blocks & services and installed-plugins list load **once**, manual refresh only; Connect panel cadence `[INFERRED]` 10s-while-visible (not explicitly stated by the MVP).
- **Idle backoff** — a Live panel stops auto-refreshing once not visible, resumes from zero (no burst-fire catch-up on return).
- **Manual refresh is universal** on every panel, Live or read-only.
- **One transport layer** for all NiFi/Kafka/Connect/registry calls, with classified errors (`ok` / `unavailable [unreachable|timeout|auth|not_found]` / `partial`), bounded timeouts, and **server-side credential resolution** — the browser never holds infrastructure credentials.

### 6.8 Kafka message viewer and DLQ view (harmless by construction)

`§19.6`, `§19.8`: **group-less, non-committing** consumer — provably incapable of disturbing a real consumer's position (ruling 31); **50-message cap**, merged newest-first across partitions; **Kafbat fallback** only on connectivity failure, honestly stamped (`source: "kafbat"` + original error); **no Avro decoding, ever, by design** (ruling 22) — even for `kafka_kc`'s governed topics, values render as plain text, non-printable payloads show `binary payload (N bytes)`. The DLQ view reuses this exact mechanism against `dlq.<flow>`, adds **redacted attributes** (secret-looking keys and credential-bearing URL query strings stripped before reaching the browser) and an honest **"could not read — history incomplete"** flag on partial read failure rather than a fake error page or silent truncation.

work.md maps DLQ to `W1.37` (minimal, Week 1) and `W3.11`–`W3.12` (full taxonomy + ceiling/poison path); metrics/ops-view to `W3.19`–`W3.23`.

---

## 7. OpenAPI Document Upload / Parsing (http adapter, HTTP services)

/ Source: `plan2.md §06.4 OpenAPI import`, ruling 36/43, `mvp.html §5` /

OpenAPI import is an **`http`-only** capability — no other adapter accepts a spec upload. It is a **design-time convenience**, not a distinct request mode: after import the block still compiles to the same request-builder primitives.

**Accepted formats and limits:**
- **OpenAPI 3.0 or 3.1, JSON serialization only.**
- **No external `$ref`s** — every schema/parameter reference must resolve within the uploaded document; an external `$ref` is rejected at import with a message identifying the offending reference.
- **5 MB hard cap** — rejected before parsing if exceeded.
- **Swagger 2.0 specs are rejected**, as are **YAML specs of any version** — both with clear rejection messages. This is a **deliberate regression** vs. the alpha (which presumably best-effort-parsed these); rejection MUST be explicit, never a partial/degraded import.

**Import mechanics:**
1. Upload occurs once per block `[INFERRED per-block, not per-service]` — different blocks against the same service may target different operations/parameter bindings.
2. The **endpoint field becomes a searchable operation list** (drawn from the spec's `paths` × `operations`), replacing free-text path entry.
3. Selecting an operation **narrows the method dropdown** to only the methods the spec declares for that path.
4. Every declared parameter (path, query, header, body-schema fields) is surfaced with a binding to one of **four value sources**:

| Value source | Description |
|---|---|
| `parent_record_field` | Bound to a field on the parent record (chained/lookup blocks only) |
| `static_value` | Literal value typed at design time |
| `global_variable` | Resolved from the Variables screen, compiled into the parameter context |
| `ask_at_runtime` | Left unresolved at design time; user prompted at Test time or ceremony time |

- **Required parameters cannot be disabled** — a required parameter always carries an active binding.
- **Required, unfilled ask-at-runtime bindings prompt the user before Test or ceremony execution** — the same placeholder-prompt dialog used for `${field}` placeholders.
- **Ask-at-runtime is design-time-only** — never a persisted runtime input source. **Deploy preflight MUST refuse a flow with a required ask-at-runtime binding still unresolved.**
- `[INFERRED]` Re-uploading a spec on a block with existing bindings SHOULD preserve bindings for unchanged parameters and flag parameters that disappeared/changed shape.

work.md maps this to `W1.22` ("OpenAPI import," `[partial]` — alpha precedent ported and re-checked against the new contract-driven form engine).

---

## 8. Explicit UI/UX Requirements & Workflows

/ Source: distributed across plan2.md; consolidated here /

### 8.1 Engines are invisible while building (the platform-wide UX invariant)

The build-time UI (canvas, block forms, Test panel, schema ceremony, Connections, Application Services, Schemas browser) MUST reference **only platform-level nouns** — no NiFi processor names, no Kafka Connect connector JSON anywhere in the build-time surface. The **ops view of a deployed flow is the sole exception**, and even there it is strictly **read-only** — no editing surface (§02.1). Every block's `+` menu shows **only legal next blocks** — an engine-aware user cannot even attempt an illegal topology (ruling 14, guided canvas, §05).

### 8.2 The guided canvas

A left-to-right chain of blocks; every block's output exposes a single `+` button whose menu lists only legal blocks at that position, computed from the adapter registry and placement laws R1–R8. **No freehand edge drawing.** Topic nodes are first-class canvas objects distinct from processing blocks; a `kafka_kc` topic node renders **sealed** — visible everywhere, never an attachment target (§05, glossary "guided canvas"/"sealed node").

### 8.3 Application Services screen (implied UI requirements)

- Service picker per adapter-family, listing only matching-type services.
- "Manual" entry mode visually indistinguishable from typing credentials directly, but produces a real (private) service record.
- Per-service **health + last-tested** display, independent of any flow.
- `service_update_available` badge on any flow referencing a service that has a newer revision.
- `action required` flag on any flow referencing a retired (logically deleted) service.
- Global Variables admin screen (name/value/secret flag) plus a per-flow Variables section in the builder.
- Gateway resources admin screen (Client Certificate Profiles, Upstreams, Routes, host allowlist) — §4.2 above.

### 8.4 Connections screen (see §2.11 above for full detail)

Multiple connections per type with an active marker; user-editable, persistent names; Activate + Repoint dialogs (strategy/pace/abandon-confirmation pickers) with a live lifecycle-job progress view; Impact Preview with real dependent lists; Test All / per-connection Test; blank-keeps-existing-secret edit semantics; delete-with-impact-confirmation.

### 8.5 Schemas screen (see §3.10 above)

Strictly read-only browse/search/filter surface; single mutating action is "start a pre-filled ceremony from this schema"; explicit removal of the alpha's Draft→Needs-Verification→Verified pipeline and version dropdowns.

### 8.6 Ops view (per deployed flow) — panel behavior

Tab/panel set: Metrics, Kafka message viewer, DLQ view, Blocks & services (read-only), Connect panel, Drift/runtime-state banner. Lazy-loaded, visibility-gated, idle-backoff, universal manual refresh (§19.2, detailed in §6.7 above). **A read never mutates** — repairing drift is a distinct, explicit, audited "force" action, never a side effect of viewing (§19.10).

### 8.7 Three distinct, never-merged "clear" verbs (§19.7)

- **Clear Topics** (Kafka records) — owned topics only, adopted refused outright; confirmation dialog names the exact topics (never a bare count); count → clear → count execution order per topic, auditing a genuine before/after pair even on partial failure.
- **Consumer-group offset-skip** (the poison-record verb) — moves a consumer group's committed offset past a bad message without deleting anything.
- **Clear Queues** (NiFi FlowFiles) — a fully separate action from Clear Topics; must never be conflated in UI copy or code.

### 8.8 Import wizard — four hard-gated stages (§20.3)

**Preview → Bind services / re-enter credentials → Name the new flow (reservation preflight) → Finalize (with rollback).** Each is a hard gate; back-navigation preserves entered state `[INFERRED]`. Naming stage: pre-filled from the connector's `<name>`; a collision with an existing flow name **prompts the user to choose a different name** — never silent auto-suffixing. Finalize rolls back completely on any partial failure (no orphaned flow record, no partially-reserved names, no half-bound services).

### 8.9 Testing a block — UX contract (§13, elaborated further in the adapter-focused sibling report but touching product UX directly)

- Test fires a **bounded, side-effect-free probe** (≤10 records), showing raw→parsed→fields for downstream field pickers.
- Testing a block with `${placeholder}`s **prompts the user to type probe values** (path placeholders required; others fall back to saved defaults).
- Testing a **mutating** method (POST/PUT/PATCH/DELETE) requires **double confirmation** — a UI prompt **plus** an independent backend refusal, not merely a client-side dialog.
- One Test result feeds every downstream consumer (explorer, field chips, extraction buttons, pagination Detect) with **no extra calls**; sensitive headers redacted, previews size-capped.
- Sample runs (Test and ceremony live-runs alike) **commit nothing** — no offsets, bookmarks, or dedup markers.

### 8.10 Cron contract UX

Standard 5-field cron in UTC, with friendly presets; validated at save with a **next-3-occurrences preview**; the compiler translates to the target engine's dialect. If a cron occurrence fires while the previous run is still executing, the new occurrence is **skipped and counted** as a runtime event — never queued, never concurrent (§17.12, mvp.html §13).

---

## 9. Security / Credential-Handling Requirements

/ Source: `plan2.md §22 Security & Egress` /

### 9.1 Secrets — storage, resolution, masking, travel (§22.1)

- **Storage as references** — a secret (password, API key, bearer token, OAuth2 client secret, session-token credentials, TLS client-cert private key, DB password, Kafka SASL credential, Connect sink credential) MUST NEVER be persisted as a literal value inside a flow document, stream config, connector export, or any browser-visible payload. Stored exactly once, as a reference, on the owning Application Service or Platform Connection record.
- **Resolution boundary** — a secret reference is dereferenced to its literal value **only inside the backend process**, and only for: Block Test (one bounded probe), schema-ceremony live sample run (one temporary execution), Flow Deploy compile (embedded as secret-flagged parameters, never literal values), Connection/Service Test (one reachability probe), and runtime execution (resolved by the owning engine's own controller-service/worker credential provider). A secret value MUST NEVER cross into the browser, a log line, a run-history record, a DLQ entry, or any exported artifact.
- **Masking in every response** — any surface that would otherwise show a secret returns a presence flag (`has_password: true`) or masked placeholder — never partial characters, never a brute-forceable hash, never the literal value. Blank secret field on edit = "keep existing," never "clear."
- **Stripped from exports, re-entered on import** — a connector export carries zero secrets and zero service definitions, only service *references*; the importer explicitly re-binds/re-enters credentials from scratch. The dual export's rendered-artifacts half likewise contains no literal environment values.
- **Compiled artifacts carry no literal environment values** — brokers/URLs/credentials resolve entirely through parameter contexts; a leaked compiled artifact contains no live credential, only parameter names.

### 9.2 Shared files carry no executable content (§22.2)

Every file the platform accepts or produces (schema-ceremony sample uploads, connector export/import bundles, OpenAPI spec uploads, raw-Avro-JSON paste) MUST be treated as **inert data, never code**: never executed, never `eval`'d, never deserialized via a mechanism capable of instantiating arbitrary types (no unsafe pickle/YAML-tags/Java-serialization-style deserialization). A connector/export bundle MUST NEVER embed a jar, native binary, script, or Connect plugin. `[INFERRED]` Uploaded sample files parsed only by the platform's own bounded parsers, XXE-safe configuration required.

### 9.3 Connect plugins and JDBC drivers — admin-installed, checksum-pinned (§22.3)

**No user uploads, ever** — users configuring a `kafka_kc` sink, `kc` sink, or `jdbc` Database service are never offered a path to upload a jar/driver/plugin archive; every Connect sink plugin and JDBC driver is installed by an **administrator** directly onto the worker/backend driver directory, outside the user-facing surface. Each is **checksum-pinned** `[INFERRED mechanism: recorded SHA-256 digest compared at a verification point, natural fit = deploy preflight]`. A **read-only "installed plugins" list** is populated via **live worker introspection**, never a static app-side registry — this is what the `kafka_kc`/`kc` config UI offers as selectable sink plugins and what deploy preflight checks against (ruling 45f).

### 9.4 Split credentials — pipeline vs. controller (§22.4)

**Pipeline credentials** (a compiled flow's controller services, a Connect sink's configured credentials) MUST be scoped to **data-plane operations only** — they MUST NOT be able to create a topic, delete a topic, alter topic config, or delete/modify another flow's consumer groups. **Topic lifecycle operations the platform performs on the user's behalf** (name reservation, materializing a topic at Deploy, emptying owned topics at Undeploy, deleting at Delete, Clear Topics) MUST be executed exclusively under one distinct **backend controller credential**, held only by the backend service, never distributed to a compiled flow. Rationale, stated directly: "a compromised or malicious flow configuration... cannot itself create rogue topics, cannot delete another flow's topic, and cannot damage cluster-level state" — the blast radius of a bad flow config is confined to what it can legitimately produce/consume.

### 9.5 All SQL is generated (§22.5)

The platform MUST **never** accept, store, or execute a user-typed or user-pasted SQL string against a Database service, for any purpose (read/write/lookup). Every query is generated from structured configuration validated against live metadata — this closes SQL injection as an attack class by construction; there is no code path where a raw string reaches the JDBC driver as a statement. `[INFERRED]` All user-supplied values pass as bound parameters, never string-interpolated.

### 9.6 Sample uploads — size-bounded, never logged (§22.6)

Schema-ceremony sample-file uploads MUST be subject to an enforced size cap (`[INFERRED]`, exact byte ceiling not stated — `OPEN`), rejected with a clear error **before parsing begins** if exceeded. Sample-upload **content** MUST NEVER be written to application logs, audit logs, or run-history records — logging is metadata-only (which ceremony, which stream, actor, timestamp, evidence *fingerprint*, never evidence *content*).

### 9.7 Audit (§22.8) — see §5.2 above for full detail

---

## Open Questions / Ambiguities

These are drawn primarily from plan2.md's own `OPEN:` markers (§28.3), filtered to items touching this report's focus areas, plus one structural observation of my own (the first item).

1. **No dedicated cross-flow "Dashboard" is specified anywhere in either source document.** The term appears once, generically, in plan2.md, and never in mvp.html. The nearest analogues (flow inventory/list, per-flow ops view, Connections/Services/Schemas screens, audit log) are each scoped narrowly. If a unified operator dashboard is expected in the product, it is not currently a documented requirement and should be raised explicitly with stakeholders. *(My own observation, not a source-flagged OPEN item.)*

2. **Application Services (§15):**
   - Whether `service_update_available` can be dismissed without a redeploy, or persists indefinitely as a nag until the flow actually redeploys.
   - Whether logical retirement of a service is reversible (un-retire) or permanent once invoked.
   - The precise reconciliation-status state machine for gateway resources (state names, transition triggers, polled vs. event-driven) — left to implementation.
   - Whether a lookup response field name colliding with an existing record field overwrites, renames/prefixes, or requires explicit aliasing (http lookup, §06.8 — adjacent to service-consumption UX).

3. **Platform Connections (§16):**
   - No concrete polling/refresh cadence stated for the Connections **screen** itself (vs. the underlying no-background-polling rule for health/reachability facts) — assume manual refresh only, unconfirmed.
   - Whether Repoint's "one-by-one" migrate pace exposes a pause/resume control mid-run, or is just slower automatic sequential execution.
   - The retention/visibility duration of an "archived" connection document created by repoint — purged eventually, or persists indefinitely for audit.

4. **Schema Ceremony (§14):**
   - Exact upload size/file-count caps for the uploaded-sample-files evidence path (JSON/XML/CSV/XLSX) — not stated.
   - Precise "sample set stability" predicate and overall time-budget duration for the live sample run's polling loop — not stated.
   - Provenance labeling/display treatment when a manually authored schema later has sample files attached without a full ceremony re-run.
   - The exact plugin-classification mechanism for "recognized lakehouse-class" sinks beyond Iceberg (allowlist config? plugin manifest flag?) — relevant to Sink destination service UX.

5. **APISIX / Gateway (§15.8, §22.7):**
   - The precise reconciliation-status state machine (see #2 above — repeated here since it's core to this focus area).
   - No numeric SLA or explicit cap stated for how many Upstreams/Routes/Cert Profiles the admin screen must support at once — not addressed at all.

6. **DLQ & Metrics (§18–§19):**
   - Whether an oversized-record poison event (>110 MiB) emits a distinct, differently-labeled runtime/audit event from a generic DLQ-write-failure parking event.
   - No maximum message size, truncation policy, or per-message body-size cap stated for the Kafka message viewer's rendering of very large values.
   - Whether the Connect panel's refresh cadence follows the 10s-visible rule (messages/DLQ) or the metrics running/not-running split — marked as inferred, not stated.
   - No numeric SLA given anywhere for "lazy, on-demand" ops-view panel load time / staleness window.

7. **Security (§22):**
   - No concrete byte ceiling stated for schema-ceremony sample uploads (distinct from the OpenAPI spec's explicit 5 MB cap) — must be set during implementation.
   - Verification cadence for plugin/driver checksum pinning (at worker startup, at each deploy preflight, or both) is left to implementation.

8. **Sharing/Export (§20) — adjacent to Application Services since service *references* travel in connectors:**
   - What parts of a legacy `.flowpack` are structurally translatable to the current connector model, and whether translation failure is per-block (partial import) or all-or-nothing.

9. **Audit (§22.8):**
   - Exact field-level schema for audit entries is `[INFERRED]`, not verbatim-stated by the MVP — only the *scope* (which actions must be audited) is explicit.
   - Whether audit search supports fields beyond actor/target/action-type/time-range is `[INFERRED]`, not explicitly enumerated.

10. **General provenance note:** plan2.md logs **224** total `[INFERRED]` markers across the whole document (§28.1); this report has flagged the ones falling within its focus areas, but any implementation team should treat plan2.md §28 as the master list and re-verify against mvp.html (the tiebreaker document) before building.
