# SentinelOne maximum-useful ingestion — E2E evidence

Date: 2026-08-19
Flow: `sentinelone.maximum_useful` (`14ab82fd-01a0-1000-47d6-db7896347cfc`)
Parent: `Ingest(3) (1)` (`0a00e822-01a0-1000-68b7-f28e69779c95`)
Builder: `tools/build_sentinelone_maximum_useful.py`
Source: `https://euce1-120-mssp.sentinelone.net/web/api/v2.1` (site-scoped token, valid to 2026-09-16)

**28 entities · 318 processors · 26 Apicurio subjects · 26 Iceberg connectors · 29,762 Iceberg rows**

This run added 11 entities found by sweeping the full API reference, introduced `ingest_ts` as an
11th standard field, and realigned `source_object_id` with raw.md. Kafka, Iceberg and the schema
registry were wiped first, so this is a clean rebuild rather than a migration.

---

## Design conformance

| Rule | Implementation | Status |
|---|---|---|
| Pagination NiFi-native, not Groovy | `init_cursor → fetch → split → page_meta → has_more → next_cursor → fetch` | PASS |
| Single trigger at 2 hours | `sentinelone.maximum__trigger`, `2 hours`, `TIMER_DRIVEN` | PASS |
| **11** standard fields as Kafka headers | `FlowFile Attribute Header Pattern` on all 52 publishers | PASS |
| **11** standard fields inside message value | injected by the local `JSON_NORMALIZE_SCRIPT` | PASS |
| `ingest_ts` present and per-record | epoch millis, stamped at hash time | PASS |
| `source_object_id` = native vendor ID | verified 120/120 per entity | PASS |
| Schema generated and registered | 26 subjects, all containing `ingest_ts` | PASS |
| Avro branch to a second topic | 26 `*__raw.avro` topics | PASS |
| Iceberg sink + Trino verification | 26 connectors, 26 tables, delta 0 | PASS |

`ingest_ts` is **SentinelOne-only**. `tools/build_fortisiem_maximum_useful.py` was deliberately not
edited — fortisiem and both rapid7 flows remain on 10 fields.

## 1. `ingest_ts`

Modelled on `fileshare.asset__enrich__set_key`, which sets `/ingest_ts = ${now():toNumber()}`
(epoch millis) with `EXCLUDES = ingest_id,ingest_ts` on its hash processor.

It is deliberately different from `extraction_timestamp`: that is ISO-8601, set once per run in
`maximum__run_metadata`. `ingest_ts` is epoch millis stamped **per record**, so it measures
per-record latency through the flow. Proven, not assumed:

```
entity                 msgs   distinct ingest_ts   distinct extraction_timestamp
installed_application   150                  150                              1
xdr_asset               150                  150                              1
activity                150                  150                              1
                                      e.g. 1787121057885, 1787121058284
```

150 distinct values against 1 — exactly the intended semantics.

It can never enter the content hash: the SHA-256 is computed over the source payload *before*
metadata is attached, and `ingest_ts` is set after. Confirmed by the dedupe check in §8.

Implemented as local constants (`STANDARD_VALUE_FIELDS` = 11, `STANDARD_HEADER_PATTERN`,
`JSON_NORMALIZE_SCRIPT`, `add_standard_value_fields`) so the shared library is untouched.

## 2. `source_object_id` per raw.md

raw.md §4 lists `Source object ID` beside `Source platform`, `Customer/tenant/organization` and
`Source object type` — as a separate field, not a merged one. §5B then defines change detection as:

> **Source + Tenant + Object Type + Source Object ID + Payload/Relevant-Content Hash**

So the ID is the vendor's **native** ID and uniqueness comes from the composite key. Two fixes:

**Composite IDs removed.** Checked against 400 Kafka samples each before changing anything —
`installed_application` gave 400 distinct native ids across 37 agents, `threat_timeline` 307 across
39 threats, so both are globally-unique snowflakes and the parent prefix added nothing.

| Entity | Was | Now |
|---|---|---|
| `installed_application` | `${agentId}_${id}` | `${id}` |
| `threat_timeline` | `${threatId}_${id}` | `${id}` |
| `threat_note` | `${threatId}_${id}` | `${id}` |

Verified after the rebuild — `source_object_id` equals the payload's native `id`:

```
installed_application  matched 120/120   e.g. 2188456283986479846
threat_timeline        matched 120/120   e.g. 2549221788456858968
agent                  matched 120/120   e.g. 2328792223987411623
xdr_asset              matched 120/120   e.g. vw4kvf4qnzippwnlpdnkilij6i
```

**Documented exceptions.** `site_policy`, `group_policy`, `tenant_policy` and `system_info` return a
bare settings object with **no native ID anywhere in the payload**, so a deterministic key from the
parent scope is the only stable choice (`group_policy_${groupId}`, `tenant_policy_sentinelone`, …).

**Tenant added to the dedupe key**, per §5B:

```
sentinelone:<customer_tenant_organization>:<entity>:<source_object_id>:<content_hash>
```

Parent IDs (`agentId`, `threatId`, `siteId`) remain ordinary payload fields — which is what raw.md
asks for.

## 3. New entities — what the API reference sweep found

The full reference (3,509 pages) yielded **827 endpoints, 387 GET**. Every tier-1–6 candidate was
probed live. 11 are reachable and now ingested:

| Entity | Endpoint | Iceberg rows |
|---|---|---:|
| `xdr_asset` | `/xdr/assets` | 3,904 |
| `xdr_asset_tag` | `/xdr/assets/tags` | 2 |
| `agent_tag` | `/agents/tags` | 8 |
| `group_policy` | `/groups/{id}/policy` | 146 |
| `tenant_policy` | `/tenant/policy` | 1 |
| `cloud_detection_rule` | `/cloud-detection/rules` | 3 |
| `service_user` | `/service-users` | 1 |
| `system_info` | `/system/info` | 1 |
| `agent_package` | `/update/agent/packages` | 190 |
| `location` | `/locations` | 1 |
| `ioc` | `/threat-intelligence/iocs` | 0 (empty at source) |

**`xdr_asset` is the significant one.** It is SentinelOne's own unified asset inventory and a
superset of the typed sub-endpoints, so the 12 routes `/xdr/assets/{device,server,workstation,…}`
were deliberately **not** ingested — they are filtered views of the same rows. Proven by the
`surfaces` and `category` breakdown:

```
surfaces:    Endpoint 3,828   |   Network Discovery 76
categories:  Workstation 3,742 |  Server 162
rows carrying serialNumber: 3,704 of 3,904 (95%)
```

Two consequences worth noting. It delivers **unmanaged-device discovery even though
`/ranger/table-view` is 403** — raw.md family 4, previously written off. And it carries
`serialNumber` plus `identity.adMachineDistinguishedName` / `adUserDistinguishedName`, giving
hardware identity (raw.md §8 ranks serial as a *strong* correlation signal) and AD identity context
(family 14) that our `agent` entity does not have and that every `/identity/*` route refuses.

Structure survived inference — `agent` and `identity` are records, `surfaces`, `riskFactors`,
`missingCoverage` and `idSecondary` are arrays. No premature string collapse despite `xdr_asset`
being the deepest JSON ingested so far.

## 4. Still blocked — re-probed, not assumed

| Endpoint | Result |
|---|---|
| `/accounts` | 403 — site-scoped token |
| `/application-management/inventory`, `/risks`, `/risks/applications`, `/risks/aggregated-applications` | **403 with and without a scope filter** |
| `/application-management/risks/endpoints`, `/risks/cves` | 400 — require `applicationName`+`applicationVendor`; per-app lookups whose feeder inventory is 403 |
| `/ranger/table-view` | 403 |
| `/rogues/table-view` | 400 — needs single-account filter |
| `/cloudnative/cloud-rogues` | 404 |
| `/mobile-integration/devices` | 403 — Mobile Security not enabled |
| all `/identity/*` (AD domains, DCs, service accounts, admins) | 403 — "Scope details required" |
| `/threats/{id}/quarantined-files` | 403 |
| `/detection-library/rules` | 400 — site user cannot query tenant-scope rules |
| `/system/configuration` | 403 |
| `/content-updates-inventory` | 400 — requires `agentId`, per-agent only |

The `Endpoint → Software → CVE` chain remains the one important raw.md requirement we cannot
satisfy. It needs the **Application Risk** module on an account-scoped token.

## 5. NiFi state

```
processors: 318   invalid: 0   states: {'STOPPED': 318}
```

## 6. Secrets

```
service_user   sampled=2   apiToken          CLEAN
site           sampled=38  registrationToken CLEAN
group          sampled=40  registrationToken CLEAN
agent          sampled=40  licenseKey        CLEAN
user           sampled=40  apiToken          CLEAN
```

`service_user` is new and exposes an `apiToken`; the existing suffix rule
`.*(token|apikey|api_key|credential|passphrase)$` catches it. The rule deliberately excludes a bare
`password` contains-match so it cannot delete the legitimate policy toggle
`dvEventTypeOpenDirectoryModifyPassword`.

## 7. Kafka — 11 headers and 11 value fields

```
topics checked: 56  |  with 11/11 headers and values: 52  |  failures: 0
no data: threat_note (raw+avro), ioc (raw+avro)  -- both empty at source
```

## 8. Iceberg via Trino

`avro` = messages on the `.avro` topic · `rows` = Iceberg rows · `delta` = drops.

```
table                       avro    rows    meta  ing_ts  delta
site                          19      19      19      19      0
group                        146     146     146     146      0
agent                       3899    3899    3899    3885      0
installed_application       4725    4725    4725    4725      0
application_cve             2063    2063    2063    2063      0
threat                        43      43      43      43      0
activity                    5301    5301    5301    5301      0
site_policy                   19      19      19      19      0
user                          99      99      99      99      0
role                           8       8       8       8      0
exclusion                   1293    1293    1293    1293      0
restriction                 6931    6931    6931    6931      0
config_override                6       6       6       6      0
activity_type                760     760     760     760      0
alert                          1       1       1       1      0
threat_timeline              192     192     192     192      0
xdr_asset                   3904    3904    3904    3904      0
xdr_asset_tag                  2       2       2       2      0
agent_tag                      8       8       8       8      0
cloud_detection_rule           3       3       3       3      0
service_user                   1       1       1       1      0
agent_package                190     190     190     190      0
location                       1       1       1       1      0
tenant_policy                  1       1       1       1      0
system_info                    1       1       1       1      0
group_policy                 146     146     146     146      0
TOTAL                             29762
```

**`delta = 0` on all 26 tables** and `metadata_rows = total_rows` everywhere. Connectors were
recreated with **`errors.tolerance=none`** rather than `all`, so sink write failures surface instead
of being silently swallowed — the suspected cause of the FortiSIEM one-run defect.

All 26 connectors and tasks `RUNNING`, no traces.

## 9. Cross-entity integrity

```
installed_application.agentId -> agent.id            0 orphans
agent.siteId          -> site.id                     0 orphans
group.siteId          -> site.id                     0 orphans
threat_timeline.threatId -> threat.id                0 orphans
activity.activityType -> activity_type.id            0 orphans
NEW xdr_asset.s1SiteId -> site.id                    0 orphans
NEW group_policy      -> group.id                    0 orphans
```

Zero orphans everywhere, including both new relationships. `xdr_asset.s1SiteId` joining cleanly to
`site.id` is what makes the XDR inventory usable alongside the agent estate rather than a parallel
island.

---

## Known gaps and caveats

**This run is a partial sweep.** Fetch processors were stopped mid-pagination rather than waiting
for full drains. Complete entities: `site`, `group`, `user`, `role`, `exclusion`, `activity_type`,
`application_cve`, `agent_package`, `group_policy`, and every singleton. Partial: `agent` 3,899 of
4,465 · `xdr_asset` 3,904 of 4,548 · `restriction` 6,931 of 17,152 · `installed_application` 4,725
of ~136,000. Scheduled 2-hour runs will complete them; dedupe makes the repeats cheap.

**14 `agent` rows have NULL `ingest_ts`** (0.36%). All carry the same older
`extraction_timestamp 2026-08-19T06:08:07.793Z`. These are FlowFiles left parked in queues by the
*previous* build, which stamped attributes before `ingest_ts` existed; they flushed through the new
Avro branch on the first run after the rebuild. A one-off artifact, not a code defect — the latest
500 messages on both `agent__raw` and `agent__raw.avro` have the header and the value field with
zero misses. They will age out.

**`threat_note` and `ioc` have no data.** The tenant holds exactly one threat note (on a threat
outside the incremental window) and zero IOCs. Both lanes are built and VALID; schema and connector
are deferred because a schema cannot be inferred from zero samples. `threat_timeline` uses an
identical lane shape and works (192 rows), so the pattern is proven.

**`group_policy` costs 146 calls per run.** Steady state is now ~310 calls per 2-hour run
(~3,700/day), dominated by `installed_application` pagination and `group_policy`. Well inside
SentinelOne's limits, but it is the obvious thing to gate if that changes.

**Transport workaround.** At 318 processors the shared library's `curl.exe` transport intermittently
died with `0xC0000005`, killing builds mid-way. `n.run_curl` is now wrapped in a retry inside the
SentinelOne builder. A `requests`-based transport was tried first and rejected: the reverse proxy in
front of NiFi returns 403 for non-curl `PUT /processors/{id}`, `PUT /controller-services/{id}/run-status`
and `POST /parameter-contexts/{id}/update-requests`. Controller services are now reused rather than
disabled-and-rewritten, which also avoids that 403 path.
