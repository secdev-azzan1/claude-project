# Verification State

Records actual evidence for anything marked VERIFIED. No entry = not verified.

| Item | Evidence | Date |
|---|---|---|
| Infra reachability | NiFi 2.9.0 about OK (JWT auth); Apicurio 3.2.1 (v2/v3/ccompat v7 subjects listed); Kafka Connect 4.2.0 (plugins: OpenSearchSink, IcebergSink); APISIX admin routes listed (10 routes, fortisiem/rapid7 upstreams); Kafka TCP 9092 FAIL from dev machine (Test-NetConnection False) | 2026-08-13 |
| Dedup reference flow | `DummyJson_Dedup` PG exported from live NiFi → reference/nifi-flows/DummyJson_Dedup.json; pattern documented in analysis/dedup-reference-flow.md (Groovy SHA-256 hash + DetectDuplicate + RedisDistributedMapCacheClientService TTL 24h + RedisConnectionPoolService redis:6379) | 2026-08-13 |
| Demo_flows contain no dedup | grep across 5 exports: no DetectDuplicate/Redis/hash processors (analysis/nifi-reference-flows.md §6) | 2026-08-13 |

## Pending corrections (from peer hygiene review of compiler, pre-completion)

- routing.py `_branch_token()` builds processor keys from branch NAME only — two sibling
  branches with the same name collide silently (add child.id disambiguator to keys AND/OR
  forbid duplicate sibling branch names in validation). MUST FIX after T7.1 lands.
- transforms.py: unused `List` import; connectors.py: unused `Optional` import.
- dlq.py build() + routing.py wire_children(): unused `flow` params (tidy or use).
- blocks_jdbc.py compile_entry(): stub signature parity noted as intentional — keep.

## Flow-engine review triage (Opus review, 2026-08-13)

Full review: reviews/flow-engine-review.md (4 CRITICAL / 17 MAJOR / 16 MINOR).
Confirmed-correct areas: dedup ordering/hash/TTL/namespace, routing multi-processor
structure+EL, terminal enforcement, secrets, scope map, determinism, naming parity.

Empirical cross-checks pending from E2E journeys:
- C1 (cron translation malformed): Journey A DID deploy+later verbs with cron */2 — severity
  may be "wrong schedule" rather than "cannot deploy"; verify actual GenerateFlowFile
  scheduling + firing behavior from Journey A evidence, then fix translation regardless.
- C2 (RouteOnAttribute failure rel): Journey B (routing) will confirm live.
- C3 (QueryDatabaseTableRecord failure rel): no jdbc journey — fix from review directly.
- C4 (pagination attr never re-evaluated): journeys don't use pagination — fix from review
  + targeted live retest of one paginated flow.
Correction wave to follow E2E completion; M-findings triaged with it.
