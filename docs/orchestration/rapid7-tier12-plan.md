# Rapid7 Tier 1 + Tier 2 Implementation Plan

Applies identically to **`rapid7_asyad.maximum_useful`** and **`rapid7_securado.maximum_useful`**.
Both are generated from the same instance-parameterised builder, so every change lands twice via env
overrides. Asyad is always done first (707 assets) and Securado second (75,893 assets).

Planning document. Nothing in here has been executed.

## Decisions taken

| # | Decision | Choice |
|---|---|---|
| 1 | Flow layout | **Nested process groups** with input/output ports |
| 2 | `ingest_ts` for historical rows | **Backfill** `ingest_ts := epoch_millis(extraction_timestamp)` |
| 3 | Dedupe key | **Fix in Phase 0** — prepend `source_platform`, clear Redis |
| 4 | Vulnerability catalogue | **ID-gated from observed findings only**, no full sweep |

Backfilled rows stay identifiable: a backfilled row has `ingest_ts` exactly equal to
`extraction_timestamp` converted to millis, whereas a live row differs by the enrichment delay. So
the backfill is reconstructable, not fabricated.

---

## 1. Target architecture

Each flow becomes a parent PG containing five children. The parameter context binds at the parent
and is inherited.

```
rapid7_<instance>.maximum_useful
├── 00_spine            trigger(2h) -> run_metadata -> OUT site_seed
│                       trigger(daily)  -> OUT catalog_seed
│                       trigger(weekly) -> OUT vuln_catalog_seed
├── 10_site_asset       the existing 5 entities
│                       OUT software_ids | os_ids | vuln_ids | solution_ids
├── 20_catalog          Tier 1  (IN catalog_seed, software_ids, os_ids)
├── 30_vuln_catalog     Tier 2  (IN vuln_catalog_seed, vuln_ids, solution_ids)
└── 90_replay           every ConsumeKafka replay consumer, isolated
```

`90_replay` exists specifically so the replay consumers can never be started by a
"start everything in this PG" action. Replay and the live fetch chain feeding the same
`__avro__publish` is what produced the 2x Iceberg duplication on 2026-08-18.

Ports carry FlowFile attributes, so `extraction_timestamp` / `ingestion_run_batch_identity` /
`ingest_ts` propagate across PG boundaries without extra work.

## 2. Entity register — 22 total

`scan` and `scan_engine` remain excluded.

### Existing (5) — unchanged endpoints, Phase 0 touches metadata only

| Entity | Endpoints | object_id |
|---|---|---|
| `site` | `/sites` -> `/sites/{id}` | `${site_id}` |
| `asset` | `/sites/{id}/assets` -> `/assets/{id}` | `${site_id}_${asset_id}` |
| `asset_software` | `/assets/{id}/software` | `${asset_id}_${software_id}` |
| `asset_service` | `/assets/{id}/services` -> `/{protocol}/{port}` | `${asset_id}_${protocol}_${port}` |
| `asset_vulnerability` | `/assets/{id}/vulnerabilities` -> `/{vulnId}` | `${asset_id}_${vulnerability_id}` |

### Tier 1 (9) — `20_catalog`

| Entity | Endpoints | object_id | Pattern | Cadence |
|---|---|---|---|---|
| `agent` | `/agents` | `${agent_id}` | paged list | daily |
| `tag` | `/tags` -> `/tags/{id}` | `${tag_id}` | paged + detail | daily |
| `tag_asset` | `/tags/{id}/assets` | `${tag_id}_${asset_id}` | rooted on `tag` | daily |
| `tag_site` | `/tags/{id}/sites` | `${tag_id}_${site_id}` | rooted on `tag` | daily |
| `asset_group` | `/asset_groups` -> `/{id}` | `${asset_group_id}` | paged + detail | daily |
| `asset_group_asset` | `/asset_groups/{id}/assets` | `${asset_group_id}_${asset_id}` | rooted | daily |
| `site_organization` | `/sites/{id}/organization` | `${site_id}` | singleton, rooted on `site` | 2h |
| `software` | `/software/{id}` | `${software_id}` | **ID-gated** from `asset_software` | event |
| `operating_system` | `/operating_systems/{id}` | `${os_id}` | **ID-gated** from `asset` | event |

### Tier 2 (8) — `30_vuln_catalog`

| Entity | Endpoints | object_id | Pattern | Cadence |
|---|---|---|---|---|
| `vulnerability` | `/vulnerabilities/{id}` | `${vulnerability_id}` | **ID-gated** from `asset_vulnerability` | event |
| `asset_vulnerability_solution` | `/assets/{id}/vulnerabilities/{vulnId}/solution` | `${asset_id}_${vulnerability_id}_${solution_id}` | per finding | 2h |
| `solution` | `/solutions/{id}` | `${solution_id}` | **ID-gated** from above | event |
| `vulnerability_reference` | `/vulnerability_references` -> `/{id}` | `${reference_id}` | paged + detail | weekly |
| `vulnerability_category` | `/vulnerability_categories` -> `/{id}` | `${category_id}` | paged + detail | weekly |
| `exploit` | `/exploits` -> `/{id}` | `${exploit_id}` | paged + detail | weekly |
| `malware_kit` | `/malware_kits` -> `/{id}` | `${malware_kit_id}` | paged + detail | weekly |
| `vulnerability_exception` | `/vulnerability_exceptions` -> `/{id}` | `${exception_id}` | paged + detail | daily |

**Deferred to a later phase** (join edges, all per-vulnerability and therefore gated but additive):
`/vulnerabilities/{id}/exploits`, `/malware_kits`, `/references`, `/solutions`.

## 3. The ID-gate pattern

The single most important addition. Without it, `vulnerability` detail costs one call per *finding*;
Asyad has 9,946 findings over roughly 500 distinct CVEs.

```
asset_vulnerability__extract ──clone──> vuln__gate_key   (UpdateAttribute)
                                          dedupe.key = <src>:<tenant>:vuln_gate:${vulnerability_id}
                                        vuln__gate       (DetectDuplicate, 24h)
                                          └─non-duplicate─> /vulnerabilities/{id} -> standard tail
```

Keyed on the **ID alone**, not a content hash — one fetch per distinct ID per 24h window. Applies to
`vulnerability`, `solution`, `software`, `operating_system`.

Verification that the gate works: `distinct vulnerability_id in bronze.<inst>.vulnerability`
should be far below `count(*) in bronze.<inst>.asset_vulnerability`, and the gate's
`duplicate` counter should carry the bulk of the traffic.

## 4. Cross-cutting changes

### 4.1 `ingest_ts`

Follows `fileshare.asset` (`/ingest_ts = ${now():toNumber()}`, epoch millis, excluded from the hash).

- **Attribute:** set in `<entity>__set_ids` (UpdateAttribute), which runs *after*
  `CryptographicHashContent` — so it is structurally outside the fingerprint. No `EXCLUDES` list.
- **Value:** `<entity>__set_metadata` (UpdateRecord) gains `/ingest_ts = ${ingest_ts}`.
- **Header:** extend `STANDARD_HEADER_PATTERN` in `build_fortisiem_maximum_useful.py` to include
  `ingest_ts`. Safe for FortiSIEM — the pattern only emits headers for attributes that exist.
- **Type:** UpdateRecord writes EL output as a JSON string, so the schema generator must pin
  `ingest_ts` to `["null","long"]`; `JsonTreeReader` then coerces string -> long and Iceberg gets
  `bigint`, not `varchar`.

### 4.2 Backfill (decision 2)

Replayed messages predate the field. The replay path in `90_replay` gains one UpdateRecord between
`ConsumeKafka` and `__avro__publish`:

```
/ingest_ts = ${extraction_timestamp:toDate("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"):toNumber()}
```

`extraction_timestamp` is already a Kafka header and `ConsumeKafka` restores it as an attribute via
`Header Name Pattern`, so the EL resolves. Live-path records are unaffected.

### 4.3 Dedupe key (decision 3)

`raw.md` section 5B requires **Source + Tenant + Object Type + Source Object ID + Content Hash**.
Current key omits Source.

```
before:  ${SOURCE_INSTANCE}:{entity}:${object_id}:${'content_SHA-256'}
after:   ${source_platform}:${customer_tenant_organization}:{entity}:${object_id}:${'content_SHA-256'}
         e.g. rapid7:rapid7_asyad:asset:15_28092:<sha256>
```

Every key changes, so the first run republishes everything once. Redis is cleared by prefix as part
of Phase 0 to make that deliberate rather than a surprise. `source_object_id` values themselves
already match the `plan.md` table for all 5 existing entities and need no change.

## 5. Builder work items

`tools/build_rapid7_asyad_maximum_useful.py` (instance-parameterised, drives both flows):

1. Nested PG creation + input/output ports + cross-PG connections.
2. New pattern: `id_gate(entity, id_attr, detail_url)`.
3. New pattern: `singleton_branch(entity, url)` — no paging (`site_organization`,
   `asset_vulnerability_solution`).
4. `ingest_ts` in `set_ids` / `set_metadata`; header pattern extended in the shared library.
5. Dedupe key rewritten to the 5-part form.
6. `infer_register()` override pinning `ingest_ts` to `["null","long"]`.
7. Replay backfill UpdateRecord.
8. Multi-trigger support (2h / daily / weekly) in `00_spine`.
9. Entity table driven — all 22 declared as data, not code.

## 6. Phases

Every phase ends at the same gate and does not proceed until green:

> **raw count == avro count == Iceberg rows**, all DLQ topics 0, `count(distinct source_object_id)`
> sane vs `count(*)`, nested types intact (arrays not collapsed to varchar), all processors
> VALID + STOPPED afterwards.

Asyad first, then Securado, for every phase.

| Phase | Content | Extra verification |
|---|---|---|
| **0** | `ingest_ts` + dedupe-key fix + nested-PG restructure of the existing 5. Schemas -> v3. Redis clear. Rebuild avro topics/tables, replay with backfill. | `ingest_ts` present as header **and** value; Iceberg column is `bigint`; backfilled rows satisfy `ingest_ts == millis(extraction_timestamp)`; live rows do not |
| **1** | Tier 1 standalone catalogues: `agent`, `tag`, `asset_group`, `site_organization` | 4 new topics -> schemas -> connectors -> Iceberg |
| **2** | Tier 1 memberships: `tag_asset`, `tag_site`, `asset_group_asset` | Referential: every `asset_id` in `tag_asset` exists in `asset` |
| **3** | ID-gates: `software`, `operating_system` | Gate ratio: distinct fetches << source row count |
| **4** | Tier 2 catalogues: `vulnerability_reference`, `vulnerability_category`, `exploit`, `malware_kit`, `vulnerability_exception` | 5 new topics end to end |
| **5** | Tier 2 gated: `vulnerability`, then `asset_vulnerability_solution`, then `solution` | Cost measured on Asyad before Securado; `asset_vulnerability_solution` is the only genuinely per-finding addition |

Phase 0 is the only destructive one — it drops and rebuilds the 10 existing avro topics and Iceberg
tables. Raw topics are never touched and remain the source of truth throughout.

## 7. Cost model

Per-asset calls are the only thing that scales with 75,893 assets.

| Class | Calls | Notes |
|---|---|---|
| Per asset (today) | 6 | detail, software, services, service detail, vulns, vuln detail |
| Per asset (after) | 7 | +1: `asset_vulnerability_solution`, per *finding* |
| ID-gated | ~distinct IDs | ~500 vulns, not 9,946 findings |
| Catalogues | O(catalogue) | tens to low thousands, off the asset path |
| Reverse-keyed memberships | O(tags), O(groups) | dozens, not 75,893 |

Processor count per flow: ~83 today -> ~317. Split across nested PGs:
`00_spine` ~5, `10_site_asset` ~78, `20_catalog` ~114, `30_vuln_catalog` ~98, `90_replay` ~22.

## 8. Standing rules

1. Replay and the live fetch chain **never** run simultaneously — `90_replay` isolation enforces this.
2. Every phase starts from a fully STOPPED PG and ends fully STOPPED.
3. Bump ConsumeKafka `Group ID` (`-v3`, `-v4`, ...) whenever a replay must re-read from the start.
4. Never write a masked `********` back to a NiFi sensitive property.
5. Secrets stay in the parameter context and env vars; never in generated schemas, Kafka values,
   or committed files.
6. Connectors use `errors.tolerance=none` so sink failures surface instead of vanishing.
7. Securado only runs against `http://apisix:9080/rapid7_securado/api/3`; never the raw console IP.

## 9. Open items to confirm before Phase 3

- Whether `/assets/{id}` exposes a usable operating-system **id** for the `operating_system` gate.
  The `Asset` schema has `os` (string) and `osFingerprint` (object); the id must be confirmed by
  sampling one real payload before the gate is built. If absent, `operating_system` drops to a
  full `/operating_systems` catalogue sweep instead.
- Whether `/agents` is permitted on both credentials. `plan.md` records several admin-scoped
  endpoints returning 404 on the Securado credential; `agent` may be one of them. Build it, leave
  it disabled if it 404s, exactly as `plan.md` prescribes for permission-gated roots.
