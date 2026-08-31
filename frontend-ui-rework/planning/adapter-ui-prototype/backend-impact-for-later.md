# Backend Impact — For After UI Approval

2026-08-11 · Everything the approved UI will eventually require from the
backend, intentionally NOT implemented in this phase. The prototype mocks all
of it in `src/prototype/api.ts`; that file is effectively the contract sketch
for the future API surface.

## Data model

1. **Flow document** replaces Source+Flow pair: name (frozen at deploy),
   description, state, enabled, cron (nullable), `blocks[]` (adapter, mode,
   parentId, branch{kind,name,ruleId}, serviceId, entity, config, transforms,
   topicOverride), `topics[]` (adopted/materialized, sealed, writer),
   flow-level variables, `servicePins` (serviceId→revision captured at
   deploy), drift info. The old `ApiSource` 70-nullable-column bag and
   `designer_payload` round-trip disappear.
2. **Adapter registry** as a closed compile-time set (http/jdbc/kafka/
   kafka_kc/kc + base): placement flags (root/writable/terminal/
   hosts_transforms) that the UI's legality menus consume — ideally served,
   not duplicated client-side.
3. **Application Services** with four types, **revisions on edit**, logical
   retirement, private (inline-created) services, secret storage (write-only
   from the UI). Old NiFi controller-service management endpoints retire.
4. **Platform Connections**: multiple per type + exactly-one-active
   (DB-enforced), health *and* reachability as separately recorded facts,
   Redis + APISIX types, Iceberg type removed (its details move to Sink
   destination services).
5. **Schemas**: approval-only records (subject, entity, flow+block binding,
   provenance, fields, raw Avro, registry global id). The version/verify
   pipeline (Draft → Needs Verification → Verified) is deleted.
6. **Connectors**: versioned immutable exports (name@version, no secrets) +
   import preview/bind/finalize endpoints.
7. **Global variables** (secret flag) + per-flow overrides compiled into
   parameter contexts.

## Behaviour / services

8. **Compiler**: blocks → NiFi flows + Kafka Connect configs, replacing the
   per-source-type generator ladders (`nifi_flow_generator.py:6135/6486`).
   Includes the naming walk (tokenizer, bare/variant/table/DLQ names, per-
   cluster reservation + collision refusal), R1–R8 enforcement server-side.
9. **Lifecycle verbs**: Pause/Resume (capture-and-queue), Stop (retain
   queues) vs Stop & Clear (audited), Redeploy (stopped+cleared gate),
   Undeploy (empty generated topics, clear dedup caches, reset positions),
   deploy preflight endpoint returning named checks.
10. **Per-block Test**: bounded probe (max 10 records), `${placeholder}`
    values passed per-run, mutating-method server-side confirmation,
    stored test result served to downstream field pickers.
11. **Schema ceremony orchestration**: live sample run into a throwaway
    `-schema-inference` topic, uploaded-file inference, approve=register
    transaction against Apicurio (registration failure fails approval).
12. **DLQ**: one per flow (`dlq.<flow>`, 3 retries, 7-day retention),
    inspection + download endpoints, no replay. Failure taxonomy (record /
    run / infrastructure) surfaced in run history.
13. **Redis integration**: per-stream dedup caches (SHA-256, windowed,
    fail-stop) + jdbc bookmarks in separate logical DBs; audited
    "clear dedup cache"; deploy preflight reachability check.
14. **APISIX egress**: managed cert profiles/upstreams/routes/host allowlist,
    reconciliation status per resource, `proxy: on` request routing,
    admin-URL kept backend-only.
15. **Repoint engine** (adopt/migrate/reset) with fingerprint identity
    checks, impact preview, per-item audited progress; Redis switches warned
    (never blocked); gateway switches re-reconcile resources.
16. **Kafka read semantics**: adopted topics (never renamed), initial
    position immutable after first start, audited offset-skip, group-less
    non-committing message viewer (newest-first, cap 50, no Avro decode).
17. **Metrics attribution**: compiler-emitted runtime-scope map so ops
    metrics attribute to blocks ("http · Asset Details"), never NiFi
    processor classes; honest "unavailable" semantics.
18. **Audit**: new event types (pause/resume/redeploy/stop-clear, ceremony
    approve, activate/repoint, service revision/retire, gateway changes,
    dedup-cache clear, connector publish).

## Deletions the migration should plan

- Webhook runtime (`routers/webhooks.py`), SMB/Mongo/Trino/Postgres source
  ladders (shelved or folded into jdbc dialects), schema verify pipeline,
  Iceberg-sink toggle endpoints, NiFi controller-services CRUD for users,
  interval/on-change scheduling, flowpack import/export (superseded by
  connectors; legacy import quarantined).
