# Reference NiFi Deduplication Flow — `DummyJson_Dedup` (live NiFi)

Discovered on the live NiFi instance (https://nifi.datapasc.com), root canvas, PG `DummyJson_Dedup`
(id `b2240253-019f-1000-3668-4c64c07337a1`). Flow definition exported to
`reference/nifi-flows/DummyJson_Dedup.json`. This is the user-provided reference implementation for
deduplication (the Demo_flows JSON exports contain **no** dedup — see `nifi-reference-flows.md` §6).

## Processor chain

```
trigger (GenerateFlowFile, timer)
  → set_request_meta (UpdateAttribute)
  → user__fetch (InvokeHTTP)
  → user__split (SplitJson — ONE RECORD PER FLOWFILE)
  → user__extract (EvaluateJsonPath — promotes $.id etc. to attributes)
  → user__enrich__set_metadata (UpdateRecord):
        /ingest_id = ${uuid}
        /ingest_ts = ${now():toNumber()}
        /object_id = ${id}
  → user__dedupe__hash (ExecuteGroovyScript)      ← fingerprint computation
  → user__dedupe__detect (DetectDuplicate)        ← cache check
      ├─ non-duplicate → user__convert_avro (ConvertRecord JSON→Avro) → user__publish (PublishKafka)
      ├─ duplicate  → auto-terminated (counted drop)
      └─ failure    → auto-terminated  ⚠ deviation from MVP (see below)
```

## Fingerprint computation (`user__dedupe__hash`, ExecuteGroovyScript)

- Dynamic properties: `SRC = dummyjson`, `EXCLUDES = ingest_id,ingest_ts`
- Script logic:
  1. Read the (single-record) JSON FlowFile content.
  2. Copy the record map, **remove EXCLUDES keys** (platform metadata `ingest_id`,`ingest_ts` —
     exactly the MVP's "platform metadata structurally excluded" rule).
  3. Serialize remaining map as canonical JSON (source field order, stable per fetch).
  4. `hash = SHA-256(json)` hex.
  5. Set attribute `dedupe.key = <SRC>:<object_id>:<hash>` (`object_id` = identity field promoted
     earlier from `$.id`).
- `Failure Strategy = rollback`; script errors → `failure` relationship.

Key structure: `dedupe.key = <source>:<identity>:<sha256-of-record-minus-excludes>` —
i.e. identity fields select the *cache key prefix*, while the hash covers the full record body minus
excluded fields, so a changed record with the same identity is NOT considered a duplicate
(new hash → new cache key). Suppression happens only for identical (identity+content) records.

## Cache check (`user__dedupe__detect`, DetectDuplicate)

- `Cache Entry Identifier = ${dedupe.key}`
- `Cache The Entry Identifier = true`
- `Age Off Duration = 24 hours` (matches MVP default TTL)
- `Distributed Cache Service` → `dummyjson__dedupe__redis_cache`

## Controller services

- `dummyjson__dedupe__redis_cache` — `RedisDistributedMapCacheClientService`, `TTL = 24 hours`,
  → `dummyjson__redis_pool`
- `dummyjson__redis_pool` — `RedisConnectionPoolService`, `Connection String = redis:6379`,
  `Redis Mode = Standalone`, `Database Index = 0` (password redacted in export; set at deploy).
- A `MapCacheServer`/`MapCacheClientService` pair (localhost:4557) also exists in the PG but is NOT
  referenced by DetectDuplicate — leftover alternative; do not reproduce.

## Ordering vs MVP

Order observed: extract → metadata/normalize (UpdateRecord) → **hash → detect** → Avro convert → publish.
This matches MVP §11.2/§9.8: dedup evaluates LAST in the transform chain, immediately before
Avro serialization/publish. Generated flows must preserve this ordering.

## Deviations our generated flows must correct

1. **`failure` auto-terminated** on DetectDuplicate/hash — MVP requires fail-stop on Redis
   unavailability (records fail visibly, never silently dropped, never pass-through). Generated
   flows must route dedup `failure` to a visible failure path (penalize/park or flow failure port),
   not auto-terminate.
2. Per-stream cache scoping: reference key prefix is `<source>` only; MVP requires one cache per
   stream — generated key prefix must include flow+stream identity (e.g. `<flow>:<stream>:<identity>:<hash>`).
3. TTL must be user-configurable (1 min – 365 days, default 24h) — set both DetectDuplicate
   `Age Off Duration` and Redis cache TTL from stream config.
4. Missing identity field → record must go to DLQ (MVP §11.6(a)); the reference script would emit
   key `src:null:<hash>` — generated hash script must explicitly route records with missing
   identity fields to the failure/DLQ path instead.

## Implementation template for the flow compiler

Two processors + two controller services per dedup-enabled stream:
- `ExecuteGroovyScript` (hash): parametrize `SRC` → `<flow>__<stream>`, `EXCLUDES` → user excluded
  fields + always `ingest_id,ingest_ts,op`; identity extraction from configured identity fields
  (fail → DLQ route).
- `DetectDuplicate`: `${dedupe.key}`, Age Off = user TTL, Redis cache client CS (TTL = user TTL)
  → shared RedisConnectionPoolService (from platform Redis connection).
- Relationships: `non-duplicate` → next step; `duplicate` → auto-terminate (counted, intentional);
  `failure` → failure/park path (fail-stop).
