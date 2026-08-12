# NiFi Reference Flow Analysis

**Scope:** Read-only structural analysis of five exported NiFi flow definitions in
`C:\Users\kaifm\Desktop\Project\lovable_ui\Demo_flows\`, plus their companion Avro
`bronze/history` schemas. Purpose: extract exact processor/controller-service/connection
patterns so a new application can regenerate equivalent flows programmatically via the
NiFi REST API.

**File format:** All five `.json` files are NiFi **Registry `VersionedFlowSnapshot`**
exports (`flowEncodingVersion: "1.0"`), not the raw `flow-definition` REST download
format. Top-level keys: `flowContents` (a `VersionedProcessGroup`), `externalControllerServices`
(empty `{}` in all five files), `parameterContexts`, `flowEncodingVersion`, `parameterProviders`
(empty in all five), `latest` (`false` in all five). Processor/controller-service/connection
objects are flat `Versioned*` records — no `component`/`revision` wrapper as in the live REST API
representation (`GET /process-groups/{id}`); when regenerating via REST, each `Versioned*` object's
fields map directly onto the `component` body of a `POST /process-groups/{id}/processors` (etc.) call.
All processors/controller services use bundle `{"group":"org.apache.nifi","version":"2.9.0"}` — this
is a **NiFi 2.9.0** environment.

**Headline finding (read before anything else):** none of the five reference flows contain
a `DetectDuplicate` processor, a `DeduplicateRecord` processor, any Redis-backed cache service, any
`DistributedMapCacheClient`/`DistributedMapCacheServer`, or any hash-based duplicate-suppression
logic. A full-text grep of all five raw JSON files for `Duplicate`, `Redis`, `DistributedMapCache`,
`dedup`, `MapCacheClient`, `MapCacheServer`, `DetectDuplicate`, `HashContent`, `CryptographicHash`
returns **zero real hits** (the single `Duplicate` occurrence in `fileshare__assets` is the
CSVReader property `Allow Duplicate Header Names`, unrelated to record dedup). See §6 for the
full analysis of what these flows do instead, and what that implies for a "reference pattern."

---

## 1. `fileshare__assets` — SMB/Excel CMDB ingest

### 1.1 Top-level structure
Single flat process group, no nesting: `/fileshare__assets`
(`flowFileConcurrency=UNBOUNDED`, `executionEngine=INHERITED`).
7 processors, 6 connections, 8 controller services, 0 sub-groups.

### 1.2 Parameter context
`fileshare__assets__params` (no inheritance):
| Parameter | Value | Sensitive |
|---|---|---|
| `topic_name` | `fileshare__asset__bronze__history` | no |
| `schema_name` | `fileshare__asset__bronze__history-value` | no |

Referenced but **not defined in this context** (resolved from a broader/global parameter
context not included in this export — inferred from usage): `#{SMB__username}`,
`#{SMB__endpoint}`, `#{SMB_Domain}`, `#{Kafka__endpoint}`, `#{Apicurio__endpoint}`.

### 1.3 Processors (execution order)
1. **`List CMDB Asset Files (SMB)`** — `org.apache.nifi.processors.smb.ListSmb`
   TIMER_DRIVEN, `30 min`, executionNode=`PRIMARY`.
   - `Input Directory` = `/`, `File Filter` = `^.+__Assets\.xlsx$`
   - `Listing Strategy` = `none`, `Initial Listing Strategy` = `ALL_FILES`,
     `Entity Tracking Initial Listing Target` = `all`, `Entity Tracking Time Window` = `3 hours`
   - `Minimum File Age` = `5 secs`, `SMB Client Provider Service` = `Global SMB Client Service`
   - This is the source-side incremental-listing mechanism — see §6.2.
2. **`Fetch CMDB Asset File (SMB)`** — `org.apache.nifi.processors.smb.FetchSmb`. `Remote File` =
   `${path}/${filename}`, `Completion Strategy`=`NONE`, auto-terminates `failure`.
3. **`Excel To CSV (CMDB Assets)`** — `org.apache.nifi.processors.standard.ConvertRecord`.
   Reader=`Excel Reader`, Writer=`CSV Writer`, `Include Zero Record FlowFiles`=`true`.
4. **`Rewrite CMDB CSV Header`** — `org.apache.nifi.processors.standard.ReplaceText`.
   `Evaluation Mode`=`Entire text`, regex `Search Value` = `(?s)^[^\r\n]*(\r?\n)` replaces the
   Excel-derived header row with a fixed literal CSV header (`sl_no,hostname,ip_address,...`).
5. **`Split CMDB Rows`** — `org.apache.nifi.processors.standard.SplitRecord`. `Records Per Split`=`1`
   (one FlowFile per CMDB row), Reader=`CSV Reader`, Writer=`CSV Writer`, auto-terminates
   `original`,`failure`.
6. **`Inject Organization Name (CMDB Assets)`** — `org.apache.nifi.processors.standard.UpdateRecord`.
   `Replacement Value Strategy`=`literal-value`; sets `/organization_name` =
   `${filename:substringBefore('__Assets.xlsx')}` (org derived from source filename) and
   `/ingest_ts` = `${now():toNumber()}`.
7. **`Publish CMDB Assets Kafka`** — `org.apache.nifi.kafka.processors.PublishKafka`. See §10 for
   the common PublishKafka config; **this flow does not set a `Kafka Key`** (property value is
   `null`) — records publish with no key, unlike the API-sourced flows.

### 1.4 Controller services
| Name | Type | Key properties |
|---|---|---|
| `Global SMB Client Service` | `org.apache.nifi.services.smb.SmbjClientProviderService` | `Hostname`=`#{SMB__endpoint}`, `Port`=`445`, `Share`=`cmdb`, `Domain`=`#{SMB_Domain}`, `Username`=`#{SMB__username}`, `Password`=`***`, `SMB Dialect`=`AUTO`, `Use Encryption`=`false`, `Enable DFS`=`false`, `Timeout`=`5 sec` |
| `Excel Reader` | `org.apache.nifi.excel.ExcelReader` | `Schema Access Strategy`=`Use Starting Row`, `Starting Row`=`1`, `Input File Type`=`XLSX` |
| `CSV Reader` | `org.apache.nifi.csv.CSVReader` | `Schema Access Strategy`=`schema-name`, `Schema Registry`=Global Confluent Schema Registry, `Schema Name`=`#{schema_name}`, `Treat First Line as Header`=`true` |
| `CSV Writer` | `org.apache.nifi.csv.CSVRecordSetWriter` | `Schema Access Strategy`=`inherit-record-schema`, `Schema Write Strategy`=`no-schema` |
| `AvroWriter` | `org.apache.nifi.avro.AvroRecordSetWriter` | `Schema Access Strategy`=`schema-name`, `Schema Name`=`#{schema_name}`, `Schema Write Strategy`=`schema-reference-writer`, `Schema Reference Writer`=Global Confluent Encoded Schema Ref Writer |
| `Global Confluent Schema Registry` | `org.apache.nifi.confluent.schemaregistry.ConfluentSchemaRegistry` | `Schema Registry URLs`=`#{Apicurio__endpoint}`, `Cache Size`=`1000`, `Cache Expiration`=`1 hour` |
| `Global Confluent Encoded Schema Ref Writer` | `org.apache.nifi.confluent.schemaregistry.ConfluentEncodedSchemaReferenceWriter` | (no properties — Confluent 5-byte magic-byte+schema-ID wire format) |
| `Global Kafka3ConnectionService` | `org.apache.nifi.kafka.service.Kafka3ConnectionService` | see §10 shared config |

### 1.5 Connections
```
List CMDB Asset Files (SMB) --[success]--> Fetch CMDB Asset File (SMB)
Fetch CMDB Asset File (SMB) --[success]--> Excel To CSV (CMDB Assets)
Excel To CSV (CMDB Assets) --[success]--> Rewrite CMDB CSV Header
Rewrite CMDB CSV Header --[success]--> Split CMDB Rows
Split CMDB Rows --[splits]--> Inject Organization Name (CMDB Assets)
Inject Organization Name (CMDB Assets) --[success]--> Publish CMDB Assets Kafka
```
All connections use default back-pressure (10,000 objects / 1 GB), 0 sec expiration, no prioritizers.

---

## 2. `fortisiem__devices` — two-tier XML API ingest (orgs → devices → detail)

### 2.1 Top-level structure
Single flat process group `/fortisiem__devices`. 16 processors, 16 connections, 5 controller
services, 0 sub-groups. No `GenerateFlowFile`/trigger processor is present in this export —
`List FortiSIEM Orgs (Domains)` itself is TIMER_DRIVEN on `30 min` and acts as the polling
trigger (unlike the other API flows, which use a separate `GenerateFlowFile` seed).

### 2.2 Parameter context
`fortisiem__devices__params`:
| Parameter | Value | Sensitive |
|---|---|---|
| `base_url` | `http://apisix:9080/fortisiem/phoenix/rest/` | no |
| `organization_endpoint_domains` | `config/Domain` | no |
| `device_endpoint_devices_by_org` | `cmdbDeviceInfo/devices?organization=${org_name}` | no |
| `device_endpoint_device_details` | `cmdbDeviceInfo/device?organization=${org_name}&naturalId=${natural_id}&loadDepend=false` | no |
| `username` | `super/CMDBAPI` | no |
| `password` | `***` | **yes** |
| `topic_name` | `fortisiem__device__bronze__history` | no |
| `schema_name` | `fortisiem__device__bronze__history-value` | no |

Note: requests go through an internal gateway (`apisix:9080`), i.e. an API-gateway/reverse-proxy
sits in front of the vendor API — same `apisix` gateway pattern appears in `rapid7-securado__assets`.

### 2.3 Processors (logical stages)
**Stage A — list orgs, resolve names:**
1. `List FortiSIEM Orgs (Domains)` — `InvokeHTTP`, GET `#{base_url}#{organization_endpoint_domains}`,
   TIMER_DRIVEN `30 min`, Basic Auth via `Request Username`=`#{username}`, `Request Password`=`***`.
2. `Split Orgs XML` — `SplitXml`, `Split Depth`=`3`.
3. `Extract Org Name (XPath)` — `EvaluateXPath`, `org_name` = `/domain/name/text()`, `Destination`=`flowfile-attribute`.
4. `Route FortiSIEM Blocked Orgs (Devices)` — `RouteOnAttribute`, `Routing Strategy`=`Route to Property name`,
   auto-terminates `blocked_org`. **No dynamic blocklist property is currently set** — the
   `blocked_org` relationship exists as a wired-but-currently-empty tenant-exclusion hook (org-blocking
   placeholder pattern; same shape recurs in Rapid7 and SentinelOne — see §8).

**Stage B — list devices per org:**
5. `Get FortiSIEM Devices (by org)` — `InvokeHTTP`, GET `#{base_url}#{device_endpoint_devices_by_org}`
   (org_name interpolated). No page/offset parameter — full pull per org, no pagination.
6. `Split Devices XML` — `SplitXml`, `Split Depth`=`1`.
7. `Extract FortiSIEM Access IP (XPath)` — `EvaluateXPath`, `natural_id` = `/device/naturalId/text()`.
8. `Set FortiSIEM Kafka Key` — `UpdateAttribute`, `kafka.key` = `${org_name}|${natural_id}` — a
   **composite natural key** (see §6.3).
9. `Route FortiSIEM Has Access IP` — `RouteOnAttribute`, `has_access_ip` =
   `${natural_id:trim():length():gt(0)}`.

**Stage C — conditional detail fetch:**
10. `Get FortiSIEM Device Details` — `InvokeHTTP`, GET `#{base_url}#{device_endpoint_device_details}`
    (org_name + natural_id interpolated) — only reached via the `has_access_ip` relationship.
11. `Route FortiSIEM Detail XML` — `RouteOnContent`. Two dynamic regex relationships:
    `valid_devices` = `(?s)^\s*(<\?xml[^>]*\?>\s*)?<devices(\s|>)`,
    `valid_device` = `(?s)^\s*(<\?xml[^>]*\?>\s*)?<device(\s|>)`. `Match Requirement`=`content must contain match`.
12. `Check FortiSIEM Detail XML` — `EvaluateXPath`, `has_children` = `count(/device/* | /devices/*)`.
13. `Route FortiSIEM Detail XML (XPath)` — `RouteOnAttribute`, `valid_xml` =
    `${has_children:toNumber():gt(0)}` — guards against empty/error XML detail responses.

**Stage D — normalize + publish:**
14. `Add FortiSIEM Ingest Timestamp` — `UpdateRecord`, `/ingest_ts`=`${now():toNumber()}`
    (path for devices that got detail-enriched, valid_xml branch).
15. `Add FortiSIEM Ingest Timestamp (Missing IP)` — `UpdateRecord`, same `/ingest_ts` logic
    (path for devices with no access IP — skips detail fetch, publishes list-only data).
16. `Publish FortiSIEM Devices Raw` — `PublishKafka`. **`Topic Name` = literal `'hehe'`**, not
    `#{topic_name}` — this looks like a leftover test/debug value rather than the intended
    `#{topic_name}` parameter reference used by all other flows; flag before using this file as
    a template. `Kafka Key` = `${kafka.key}`.

### 2.4 Controller services
Same shared set pattern as fileshare (`AvroWriter fortisim`, `XMLReader`, `Global Confluent Encoded
Schema Ref Writer`, `Global Confluent Schema Registry`, `Global Kafka3ConnectionService`), except
the reader here is `org.apache.nifi.xml.XMLReader` (`Field Name for Content`=`original_content`,
`Parse XML Attributes`=`true`, `Attribute Prefix`=`attr_`, `Schema Access Strategy`=`schema-name`,
`Schema Name`=`#{schema_name}`) instead of CSV/JSON, because the source payloads are XML.

### 2.5 Connections
```
List FortiSIEM Orgs (Domains) --[Response]--> Split Orgs XML
Split Orgs XML --[split]--> Extract Org Name (XPath)
Extract Org Name (XPath) --[matched]--> Route FortiSIEM Blocked Orgs (Devices)
Route FortiSIEM Blocked Orgs (Devices) --[unmatched]--> Get FortiSIEM Devices (by org)
Get FortiSIEM Devices (by org) --[Response]--> Split Devices XML
Split Devices XML --[split]--> Extract FortiSIEM Access IP (XPath)
Extract FortiSIEM Access IP (XPath) --[matched]--> Set FortiSIEM Kafka Key
Set FortiSIEM Kafka Key --[success]--> Route FortiSIEM Has Access IP
Route FortiSIEM Has Access IP --[has_access_ip]--> Get FortiSIEM Device Details
Route FortiSIEM Has Access IP --[unmatched]--> Add FortiSIEM Ingest Timestamp (Missing IP)
Get FortiSIEM Device Details --[Response]--> Route FortiSIEM Detail XML
Route FortiSIEM Detail XML --[valid_devices, valid_device]--> Check FortiSIEM Detail XML
Check FortiSIEM Detail XML --[matched]--> Route FortiSIEM Detail XML (XPath)
Route FortiSIEM Detail XML (XPath) --[valid_xml]--> Add FortiSIEM Ingest Timestamp
Add FortiSIEM Ingest Timestamp --[success]--> Publish FortiSIEM Devices Raw
Add FortiSIEM Ingest Timestamp (Missing IP) --[success]--> Publish FortiSIEM Devices Raw
```

---

## 3. `pokeapi-offset-test` — offset/limit pagination reference pattern

### 3.1 Top-level structure
Two process groups:
```
/pokeapi-offset-test                      (flowFileConcurrency=UNBOUNDED, executionEngine=INHERITED)
  /pokeapi-offset-test/PG_OFFSET_PAGINATOR (flowFileConcurrency=UNBOUNDED, executionEngine=INHERITED,
                                             statelessFlowTimeout=1 min, flowFileOutboundPolicy=STREAM_WHEN_AVAILABLE)
```
This is the only flow of the five that factors pagination into a **reusable child process group**
wired through an input port (`in`) and output port (`page_responses`) rather than inlining the loop
in the top-level canvas. 8 processors total (5 inside `PG_OFFSET_PAGINATOR`), 10 connections,
0 controller services (no publish/serialization stage — this flow is a pagination test harness only,
it never reaches Kafka).

### 3.2 Parameter context
`pokeapi-offset-test__params`: `poke_base_url`=`https://pokeapi.co/api/v2`,
`poke_endpoint_path`=`/pokemon`, `poke_limit`=`20`.

### 3.3 Processors
Outer group:
1. `Seed` — `GenerateFlowFile`, TIMER_DRIVEN `60 min`, `Batch Size`=`1`, `Unique FlowFiles`=`false` — the poll trigger.
2. `Split Pokemon Results` — `SplitJson`, `JsonPath Expression`=`$.results[*]`.
3. `Log Pokemon Item` — `LogAttribute`, `Attributes to Log Regular Expression` =
   `^(name|pokemon_url|offset|limit|invokehttp\..*)$`, auto-terminates `success` (this flow is a
   diagnostic/test flow — logs instead of publishing).

`PG_OFFSET_PAGINATOR` (input port `in` → output port `page_responses`):
4. `Init Offset` — `UpdateAttribute`, `offset`=`0`, `limit`=`#{poke_limit}` (runs once, on the
   initial flowfile arriving from `in`).
5. `Invoke Page` — `InvokeHTTP`, GET `#{poke_base_url}#{poke_endpoint_path}?limit=${limit}&offset=${offset}`.
6. `Extract First Result` — `EvaluateJsonPath`, `first_result_name`=`$.results[0].name`,
   `Path Not Found Behavior`=`ignore` (so an empty page doesn't route to `failure`, it routes to
   `unmatched` with the attribute simply absent).
7. `Has Data?` — `RouteOnAttribute`, `has_data` = `${first_result_name:trim():length():gt(0)}`,
   auto-terminates `unmatched` (this is the loop-exit condition).
8. `Next Offset` — `UpdateAttribute`, `offset` = `${offset:toNumber():plus(#{poke_limit})}`.

### 3.4 The pagination loop (exact wiring — this is the template to copy)
```
in (INPUT_PORT) ----------------------------> Init Offset
Init Offset --[success]--------------------> Invoke Page
Next Offset --[success]--------------------> Invoke Page          <-- loop re-entry
Invoke Page --[Response]-------------------> Extract First Result
Extract First Result --[matched, unmatched]-> Has Data?
Has Data? --[has_data]----------------------> page_responses (OUTPUT_PORT)   <-- fan-out #1
Has Data? --[has_data]----------------------> Next Offset                    <-- fan-out #2 (loop continues)
Has Data? --[unmatched]---------------------> (auto-terminated: loop stops)
```
Key detail: **the single `has_data` relationship is wired to two separate destinations** (both the
output port and `Next Offset`) — NiFi clones the FlowFile once per outbound connection on a matched
relationship, so each successful page is simultaneously (a) emitted downstream for processing and
(b) used to advance the loop and fetch the next page. Loop termination is a `RouteOnAttribute`
`unmatched` relationship, auto-terminated, with no destination — that starves the loop and the
process group returns to waiting for the next `in` trigger. Outer group:
```
Seed --[success]--> in (INPUT_PORT, into PG_OFFSET_PAGINATOR)
page_responses (OUTPUT_PORT, out of PG_OFFSET_PAGINATOR) --> Split Pokemon Results
Split Pokemon Results --[split]--> Log Pokemon Item
```

---

## 4. `rapid7-securado__assets` — nested two-level pagination (sites → assets) + detail fetch

### 4.1 Top-level structure
Single flat process group `/rapid7-securado__assets`. 19 processors, 20 connections,
6 controller services, 0 sub-groups (pagination is inlined on the canvas here, unlike pokeapi's
reusable child group).

### 4.2 Parameter context
`rapid7-securado__assets__params`:
| Parameter | Value | Sensitive |
|---|---|---|
| `base_url` | `http://apisix:9080/rapid7/api/3/` | no |
| `asset_endpoint_sites` | `sites?page=${sites_page}&size=500` | no |
| `asset_endpoint_site_assets` | `sites/${site_id}/assets?page=${page}&size=500` | no |
| `asset_endpoint_detail` | `assets/${asset_id}` | no |
| `username` | `apiuser` | no |
| `password` | `***` | **yes** |
| `topic_name` | `rapid7-securado__asset__bronze__history` | no |
| `schema_name` | `rapid7-securado__asset__bronze__history-value` | no |

### 4.3 Processors — outer loop: enumerate sites (page/size pagination)
1. `GenerateFlowFile` — TIMER_DRIVEN `30 min`, seed trigger.
2. `Init Sites Page` — `UpdateAttribute`, `sites_page`=`0`.
3. `List Rapid7 Sites (All)` — `InvokeHTTP`, GET `#{base_url}#{asset_endpoint_sites}`, Basic Auth.
4. `Extract Sites Page Meta` — `EvaluateJsonPath`, `sites_page_number`=`$.page.number`,
   `sites_page_total`=`$.page.totalPages`.
5. `Has More Sites Pages?` — `RouteOnAttribute`. `has_more_sites` =
   `${sites_page_total:trim():isEmpty():not():and(${sites_page_number:toNumber():lt(${sites_page_total:toNumber():minus(1)})})}`
   — classic "current page index < totalPages‑1" pagination-continue check. Also defines
   `to_build` = `${uuid:isEmpty():not()}` (always true — effectively an unconditional pass-through
   used to route into `Split Sites`).
6. `Next Sites Page` — `UpdateAttribute`, `sites_page` = `${sites_page_number:toNumber():plus(1)}`.
7. `Split Sites` — `SplitJson`, `JsonPath Expression`=`$.resources[*]`.
8. `Extract Rapid7 Site Meta` — `EvaluateJsonPath`, `site_id`=`$.id`, `site_name`=`$.name`.
9. `Route Rapid7 Blocked Sites` — `RouteOnAttribute`, `blocked` =
   `${site_name:equals('CCED Windows QUARTER')}`, auto-terminates `blocked` — an **active** tenant/site
   exclusion filter (unlike FortiSIEM/SentinelOne's empty placeholder — this one names a real site).

### 4.4 Processors — inner loop: enumerate assets per site (page/size pagination)
10. `Init Page` — `UpdateAttribute`, `page`=`0`, `ingest_ts`=`${now():toNumber()}` (timestamp captured
    at the start of the per-site pagination run, not per-record).
11. `List Site Assets` — `InvokeHTTP`, GET `#{base_url}#{asset_endpoint_site_assets}` (site_id, page interpolated).
12. `Extract Page Meta` — `EvaluateJsonPath`, `page_number`=`$.page.number`, `page_total`=`$.page.totalPages`.
13. `Has More Pages?` — `RouteOnAttribute`, `has_more` = same `lt(total-1)` pattern as sites-level.
14. `Next Page` — `UpdateAttribute`, `page` = `${page:toNumber():plus(1)}`, also refreshes `ingest_ts`.
15. `Split Assets` — `SplitJson`, `JsonPath Expression`=`$.resources[*]`.
16. `Extract Asset ID` — `EvaluateJsonPath`, `asset_id`=`$.id`, **`kafka.key`=`$.id`** — the Kafka
    message key is set directly to the vendor's native asset ID, no composite/prefix.

### 4.5 Processors — per-asset detail + publish
17. `Get Asset Details` — `InvokeHTTP`, GET `#{base_url}#{asset_endpoint_detail}` (asset_id interpolated).
18. `Add Entity Date Fields (Rapid7)` — `UpdateRecord`, sets `/site_id`, `/site_name`, `/ingest_ts`
    from flowfile attributes onto the record (JsonTreeReader → JsonRecordSetWriter).
19. `Publish Rapid7 Assets Raw` — `PublishKafka`, `Topic Name`=`#{topic_name}`, `Kafka Key`=`${kafka.key}`.

### 4.6 Controller services
`JsonTreeReader` (`Schema Access Strategy`=`schema-name`, `Schema Name`=`#{schema_name}`,
`Schema Application Strategy`=`SELECTED_PART`) + `JsonRecordSetWriter` (`Output Grouping`=
`output-oneline`, `Suppress Null Values`=`suppress-missing`, `Schema Write Strategy`=`no-schema`)
used for the record-level `Add Entity Date Fields` step; `AvroWriter` (schema-reference-writer via
Confluent) used for the final `PublishKafka` Record Writer — i.e. **records are read/transformed as
JSON but published as Avro** (two different Record Writer controller services in the same flow, one
per stage). Plus the same shared `Global Confluent Schema Registry`, `Global Confluent Encoded
Schema Ref Writer`, `Global Kafka3ConnectionService` seen in every flow.

### 4.7 Connections
```
GenerateFlowFile --[success]--> Init Sites Page
Init Sites Page --[success]--> List Rapid7 Sites (All)
Next Sites Page --[success]--> List Rapid7 Sites (All)
List Rapid7 Sites (All) --[Response]--> Extract Sites Page Meta
Extract Sites Page Meta --[matched]--> Has More Sites Pages?
Has More Sites Pages? --[has_more_sites]--> Next Sites Page
Has More Sites Pages? --[to_build]--> Split Sites
Split Sites --[split]--> Extract Rapid7 Site Meta
Extract Rapid7 Site Meta --[matched]--> Route Rapid7 Blocked Sites
Route Rapid7 Blocked Sites --[unmatched]--> Init Page
Init Page --[success]--> List Site Assets
Next Page --[success]--> List Site Assets
List Site Assets --[Response]--> Split Assets
Split Assets --[split]--> Extract Asset ID
Split Assets --[original]--> Extract Page Meta
Extract Page Meta --[matched]--> Has More Pages?
Has More Pages? --[has_more]--> Next Page
Extract Asset ID --[matched]--> Get Asset Details
Get Asset Details --[Response]--> Add Entity Date Fields (Rapid7)
Add Entity Date Fields (Rapid7) --[success]--> Publish Rapid7 Assets Raw
```
Note `Split Assets` fans its input out via two relationships: `split` (each individual asset row)
drives the detail-fetch chain, while `original` (the whole unsplit page) feeds back into
`Extract Page Meta` to drive the pagination-continue check — decoupling "does this page have more
pages" from "process each record," so pagination advances independent of per-record detail-fetch latency.

---

## 5. `sentinelone__agents` — nested two-level cursor pagination (sites → agents)

### 5.1 Top-level structure
Single flat process group `/sentinelone__agents`. 18 processors, 19 connections,
6 controller services, 0 sub-groups.

### 5.2 Parameter context
`sentinelone__agents__params`:
| Parameter | Value | Sensitive |
|---|---|---|
| `base_url` | `https://euce1-120-mssp.sentinelone.net/web/api/v2.1/` | no |
| `site_endpoint_sites` | `sites?limit=200&cursor=${cursor}` | no |
| `agent_endpoint_agents_by_site` | `agents?siteIds=${s1_site_id}&limit=200&cursor=${cursor}` | no |
| `authentication_token` | `***` | **yes** |
| `topic_name` | `sentinelone__agent__bronze__history` | no |
| `schema_name` | `sentinelone__agent__bronze__history-value` | no |

Auth is `Authorization` header (bearer/API-token style — redacted) rather than the Basic-Auth
`Request Username`/`Request Password` pair used by FortiSIEM/Rapid7 — SentinelOne is the only one
of the three API flows using header-based auth instead of InvokeHTTP's built-in Basic Auth properties.

### 5.3 Processors — outer loop: enumerate sites (cursor pagination)
1. `S1 Sites Seed` — `GenerateFlowFile`, TIMER_DRIVEN `30 min`, `Custom Text`=`[]`.
2. `Init S1 Sites Cursor` — `UpdateAttribute`, `cursor`=`''`, `site_batch_id`=`${uuid}` (a batch
   correlation ID generated once per polling cycle, not persisted/used for cache lookups elsewhere —
   informational only, not a dedup key).
3. `List S1 Sites (All)` — `InvokeHTTP`, GET `#{base_url}#{site_endpoint_sites}`.
4. `Extract S1 Sites Page Meta` — `EvaluateJsonPath`, `next_cursor`=`$.pagination.nextCursor`.
5. `Has More S1 Sites Cursor?` — `RouteOnAttribute`, `has_more` = `${next_cursor:trim():isEmpty():not()}`
   — presence-of-cursor check (simpler than Rapid7's page-index math, typical of cursor APIs).
6. `Next S1 Sites Cursor` — `UpdateAttribute`, `cursor`=`${next_cursor}`.
7. `Split S1 Sites Data` — `SplitJson`, `JsonPath Expression`=`$.data.sites[*]`.
8. `Extract S1 Site` — `EvaluateJsonPath`, `s1_site_id`=`$.id`, `s1_site_name`=`$.name`.
9. `Route S1 Blocked Sites (Agents)` — `RouteOnAttribute`, auto-terminates `blocked` — same **empty
   placeholder** blocklist pattern as FortiSIEM (no dynamic property currently defined).

### 5.4 Processors — inner loop: enumerate agents per site (cursor pagination)
10. `Init Cursor` — `UpdateAttribute`, `cursor`=`''`.
11. `Get S1 Agents (by site)` — `InvokeHTTP`, GET `#{base_url}#{agent_endpoint_agents_by_site}`
    (s1_site_id + cursor interpolated).
12. `Extract S1 Page Meta` — `EvaluateJsonPath`, `next_cursor`=`$.pagination.nextCursor`.
13. `Has More Cursor?` — `RouteOnAttribute`, `has_more` = same presence check.
14. `Next Cursor` — `UpdateAttribute`, `cursor`=`${next_cursor}`.
15. `Split S1 Agents` — `SplitJson`, `JsonPath Expression`=`$.data[*]`.
16. `Extract S1 Agent Key` — `EvaluateJsonPath`, `kafka.key`=`$.id` (agent's native ID, same
    direct-ID-as-key pattern as Rapid7).

### 5.5 Processors — normalize + publish
17. `Add Entity Date Fields (SentinelOne)` — `UpdateRecord`, `/ingest_ts`=`${now():toNumber()}`
    (JsonTreeReader → JsonRecordSetWriter, same reader/writer pair pattern as Rapid7).
18. `Publish SentinelOne Agents Raw` — `PublishKafka`, `Topic Name`=`#{topic_name}`,
    `Kafka Key`=`${kafka.key}`.

### 5.6 Controller services
Same 6-service shape as Rapid7: `JsonTreeReader`, `JsonRecordSetWriter` (here `Output Grouping`=
`output-array`, vs. Rapid7's `output-oneline` — cosmetic difference, both still feed a
per-record `UpdateRecord`), `AvroWriter`, `Global Confluent Schema Registry`,
`Global Confluent Encoded Schema Ref Writer`, `Global Kafka3ConnectionService`.

### 5.7 Connections
```
S1 Sites Seed --[success]--> Init S1 Sites Cursor
Init S1 Sites Cursor --[success]--> List S1 Sites (All)
Next S1 Sites Cursor --[success]--> List S1 Sites (All)
List S1 Sites (All) --[Response]--> Extract S1 Sites Page Meta
Extract S1 Sites Page Meta --[matched, unmatched]--> Has More S1 Sites Cursor?
Extract S1 Sites Page Meta --[matched, unmatched]--> Split S1 Sites Data
Has More S1 Sites Cursor? --[has_more]--> Next S1 Sites Cursor
Split S1 Sites Data --[split]--> Extract S1 Site
Extract S1 Site --[matched]--> Route S1 Blocked Sites (Agents)
Route S1 Blocked Sites (Agents) --[unmatched]--> Init Cursor
Init Cursor --[success]--> Get S1 Agents (by site)
Next Cursor --[success]--> Get S1 Agents (by site)
Get S1 Agents (by site) --[Response]--> Split S1 Agents
Split S1 Agents --[split]--> Extract S1 Agent Key
Split S1 Agents --[original]--> Extract S1 Page Meta
Extract S1 Page Meta --[matched]--> Has More Cursor?
Has More Cursor? --[has_more]--> Next Cursor
Extract S1 Agent Key --[matched]--> Add Entity Date Fields (SentinelOne)
Add Entity Date Fields (SentinelOne) --[success]--> Publish SentinelOne Agents Raw
```
Same fan-out shape as Rapid7's `Split Assets`: `Extract S1 Sites Page Meta` routes on **both**
`matched` and `unmatched` to two different downstream processors simultaneously (`Has More S1 Sites
Cursor?` AND `Split S1 Sites Data`) — the pagination-continue check and the per-record processing
happen off the same page fetch, in parallel branches, regardless of whether the JsonPath extraction
matched.

---

## 6. SYNTHESIS — DEDUPLICATION PATTERN

### 6.1 Direct answer
**There is no in-flow, processor-level deduplication in any of the five reference flows.**
Specifically absent from all five, confirmed by full-text search of the raw exports:
- `org.apache.nifi.processors.standard.DetectDuplicate` (or any `DeduplicateRecord` processor)
- `RedisDistributedMapCacheClientService` / `RedisConnectionPoolService` / any Redis controller service
- `DistributedMapCacheClientService` / `DistributedMapCacheServer` (in-memory or otherwise)
- Any hash-construction processor (`CryptographicHashContent`, `CryptographicHashAttribute`) feeding
  a cache-lookup step
- Any `Wait`/`Notify` processor pair (sometimes used for a different kind of coalescing/dedup in NiFi)

If your generator is expected to reproduce "the dedup pattern used by these reference flows," there
is nothing to copy — **do not fabricate a `DetectDuplicate`+Redis pipeline and attribute it to these
files.** What follows (§6.2–§6.4) is what these flows do instead; treat §6.5 as a separate,
clearly-labeled recommendation, not an observed pattern.

### 6.2 What exists instead, mechanism 1: source-side incremental listing (`fileshare__assets` only)
`List CMDB Asset Files (SMB)` (`org.apache.nifi.processors.smb.ListSmb`) is configured with:
- `Listing Strategy` = `none` (this is the *listing algorithm* property — "none" here means the
  processor falls back to its default entity-tracking behavior rather than timestamp-window or
  no-tracking modes)
- `Entity Tracking Initial Listing Target` = `all`
- `Entity Tracking Time Window` = `3 hours`
- `Initial Listing Strategy` = `ALL_FILES`
- `Minimum File Age` = `5 secs`

This is NiFi's **built-in processor state** ("Entity Tracking") mechanism: `ListSmb` persists which
files (by path + timestamp/size) it has already emitted in its own component state (via NiFi's
`StateManager`, cluster-wide if clustered), and on each `30 min` trigger only emits *new or changed*
entries within the tracking window. This is dedup **at the file-listing layer** — it prevents
re-processing the same unchanged SMB file — but it is not a record-level or content-hash dedup of
the CMDB rows that come out of that file. To regenerate via REST API: this requires no separate
processor; it's just `ListSmb` properties plus NiFi's native processor-state persistence (no
external cache service is involved — state lives in the NiFi cluster's own state provider, e.g.
ZooKeeper/embedded state provider, not something the flow definition configures).

### 6.3 What exists instead, mechanism 2: natural-key Kafka message keying
Every API-sourced flow (`fortisiem__devices`, `rapid7-securado__assets`, `sentinelone__agents`) sets
a `kafka.key` FlowFile attribute from the vendor's own entity identifier, then reads it into
`PublishKafka`'s `Kafka Key` property as `${kafka.key}`:

| Flow | Key construction step | Key expression |
|---|---|---|
| `fortisiem__devices` | `UpdateAttribute` "Set FortiSIEM Kafka Key" | `${org_name}\|${natural_id}` (composite: tenant + device natural ID, pipe-delimited) |
| `rapid7-securado__assets` | `EvaluateJsonPath` "Extract Asset ID" | `$.id` (vendor asset ID, direct) |
| `sentinelone__agents` | `EvaluateJsonPath` "Extract S1 Agent Key" | `$.id` (vendor agent ID, direct) |
| `fileshare__assets` | *(none set)* | `Kafka Key` property is `null` — no key, default partitioner distributes round-robin/hash-of-nothing |

`PublishKafka`'s `Kafka Key Attribute Encoding` = `utf-8` in all four flows that publish. This keying
is the closest thing to a "dedup key" present in these flows, but it does not suppress anything
inside NiFi — it only determines the Kafka record key. Whether that produces deduplication depends
entirely on **Kafka-side topic configuration** (e.g. `cleanup.policy=compact` on the
`*__bronze__history` topics, which is not visible in these NiFi exports — it would be set on the
Kafka topic itself, outside NiFi's flow definition), or on a downstream consumer doing
key + `max(ingest_ts)` upsert resolution.

### 6.4 Why "bronze / history" implies append-only, not deduplicated
Every flow's `topic_name` parameter and matching `.avsc` schema follow the convention
`<source>__<entity>__bronze__history` / `<source>__<entity>__bronze__history-value`
(Confluent Schema Registry `<topic>-value` subject naming convention). Every schema's record type
includes an `ingest_ts` field (long, injected by an `UpdateRecord`/`EvaluateJsonPath`-fed step
immediately before publish, always `${now():toNumber()}` epoch millis) in addition to the vendor's
own natural-key field(s) (`naturalId` in FortiSIEM, `id` in Rapid7 and SentinelOne). The word
"history" plus the presence of `ingest_ts` on every row strongly indicates this is a **medallion
architecture bronze layer that intentionally retains every poll's full snapshot as a new event**,
not a deduplicated table — i.e. the same device/asset/agent will appear as multiple Kafka records
over time, one per poll cycle, each with a different `ingest_ts`. Deduplication (if any) is deferred
to a downstream silver/gold transformation that resolves "latest row per natural key" — that logic
is out of scope for (and absent from) these five NiFi flows.

### 6.5 Recommendation if a genuine NiFi-native dedup stage is required (not an observed pattern)
Since no reference implementation exists in this corpus, if the target application needs actual
duplicate suppression *inside* NiFi (e.g. to avoid republishing an unchanged record within a polling
window), the idiomatic NiFi 2.9 building blocks — to be introduced net-new, not copied from these
files — would be:
1. A `CryptographicHashRecord`/`CryptographicHashContent` processor (or an `UpdateRecord` computing a
   hash across selected fields) to compute a stable content hash per record, using the same natural
   key already extracted (`kafka.key` equivalent) plus a hash of the mutable payload.
2. `org.apache.nifi.processors.standard.DetectDuplicate`, configured with a
   `Distributed Cache Service` property pointing at either:
   - `org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService` +
     `DistributedMapCacheServer` (in-cluster, no external dependency), or
   - a Redis-backed equivalent (`RedisDistributedMapCacheClientService` bound to a
     `RedisConnectionPoolService`) if cross-restart/shared-across-flows persistence is required.
3. `Cache Entry Identifier` = the natural key (or key+hash) expression; `Age Off Duration` = the
   dedup TTL/window; route `duplicate` to a drop/log path and `non-duplicate` to the existing publish
   chain.
This is a from-scratch design suggestion, clearly outside what these five flows demonstrate — call
this out explicitly to whoever consumes this report so it isn't mistaken for an extracted pattern.

---

## 7. SYNTHESIS — NORMALIZATION PATTERN

Normalization stages and their position relative to publish (there being no dedup stage, "relative
to dedup" collapses to "relative to publish," which is the meaningful boundary in every flow):

- **`fileshare__assets`**: `Excel To CSV` (`ConvertRecord`, format normalization XLSX→CSV) →
  `Rewrite CMDB CSV Header` (`ReplaceText`, fixes a broken/inconsistent header row) →
  `Split CMDB Rows` (`SplitRecord`, 1 row per FlowFile) → `Inject Organization Name`
  (`UpdateRecord`, adds `organization_name` + `ingest_ts`) → publish. All normalization happens
  **before** the record-splitting granularity settles and immediately before publish; there is no
  post-split validation step.
- **`fortisiem__devices`**: normalization is entirely attribute-level (`EvaluateXPath` extractions,
  `RouteOnContent`/`RouteOnAttribute` structural validation of the XML) until the very last step,
  where `UpdateRecord` ("Add FortiSIEM Ingest Timestamp[…]") injects `/ingest_ts` immediately before
  `PublishKafka`. Reader is `XMLReader`, so the "normalization" from XML to the Avro output schema
  happens implicitly at read/write time via the record reader/writer pair, not an explicit
  transform processor (no `JoltTransformRecord`/`JoltTransformJSON` appears in any of the five flows).
- **`rapid7-securado__assets`** / **`sentinelone__agents`**: identical shape —
  `EvaluateJsonPath` (extract key/meta attributes) → `UpdateRecord` ("Add Entity Date Fields[…]",
  injects `ingest_ts` and, for Rapid7, `site_id`/`site_name` denormalized from the parent
  pagination context) → `PublishKafka`. Note the reader/writer pair for this `UpdateRecord` step is
  `JsonTreeReader`/`JsonRecordSetWriter` (JSON in, JSON out), while the *subsequent* `PublishKafka`
  uses a **separate** `AvroWriter` Record Writer — i.e. normalization happens in JSON, then the
  publish step itself performs the JSON→Avro conversion via `PublishKafka`'s own Record Reader
  (`JsonTreeReader`, reading the just-normalized JSON) + Record Writer (`AvroWriter`, schema-registry-backed).
- No flow uses `JoltTransformJSON`, `JoltTransformRecord`, `ScriptedTransformRecord`, or
  `QueryRecord` for normalization — all transformation is done via `UpdateRecord` dynamic properties
  (`/field = expression`) with `Replacement Value Strategy = literal-value`, or `EvaluateXPath`/
  `EvaluateJsonPath` promoting values to FlowFile attributes for later interpolation.

**General rule observed:** normalization (adding `ingest_ts`, resolving parent-context fields like
org/site name) is always the *last* record-shaping step immediately upstream of `PublishKafka`, after
all splitting/pagination/detail-fetch logic has resolved. Since there is no dedup stage, this
ordering question ("normalize before or after dedup?") doesn't arise in these flows — normalization
is simply the final step before publish, full stop.

---

## 8. SYNTHESIS — ROUTING PATTERN

Three routing processor types appear, no `QueryRecord` anywhere:

| Processor type | Uses across the 5 flows |
|---|---|
| `RouteOnAttribute` | Pagination-continue checks (`has_more`, `has_data`, `has_more_sites`, `has_more`), tenant/org/site blocklists (`blocked`, `blocked_org`), post-detail-fetch conditionals (`has_access_ip`, `valid_xml`) |
| `RouteOnContent` | Only in `fortisiem__devices` ("Route FortiSIEM Detail XML") — regex-matches the *body* of the XML response against `<devices…` / `<device…` root-element patterns to distinguish singular vs. collection detail responses before XPath-parsing them |
| `EvaluateXPath` / `EvaluateJsonPath` used as implicit routers | `matched`/`unmatched` relationships downstream of these extraction processors are routed differently in several places (e.g. pokeapi's `Extract First Result` routes **both** `matched` and `unmatched` into the same `Has Data?` router, deferring the actual branch to the dedicated `RouteOnAttribute`) |

**Blocklist pattern (recurs 3×, worth calling out as a template):** `RouteOnAttribute` with
`Routing Strategy = Route to Property name`, a relationship named `blocked`/`blocked_org`, that
relationship auto-terminated (silently dropped, no logging/DLQ), and the `unmatched` relationship
continuing the main pipeline. Two of the three instances (FortiSIEM `Route FortiSIEM Blocked Orgs
(Devices)`, SentinelOne `Route S1 Blocked Sites (Agents)`) currently have **no dynamic property
defined** — they are wired placeholders that pass everything through unmatched; only Rapid7's `Route
Rapid7 Blocked Sites` has an active rule: `blocked = ${site_name:equals('CCED Windows QUARTER')}`.
If generating this pattern programmatically, the dynamic property name becomes the relationship name
and its expression is the block condition; leaving zero dynamic properties defined yields a
functional no-op blocklist with the hook already wired for later use.

**Structural validation via routing:** FortiSIEM chains three routing decisions in sequence purely
to validate an XML API response before parsing it as a record (`RouteOnContent` content-shape check
→ `EvaluateXPath` element-count check → `RouteOnAttribute` on that count) — a defense-in-depth
pattern against malformed/empty upstream XML rather than trusting `XMLReader` to fail cleanly.

---

## 9. SYNTHESIS — INGEST PATTERN

### 9.1 HTTP polling (`InvokeHTTP`) — common baseline config
Every `InvokeHTTP` instance across all four API-touching flows shares this baseline (only URL,
method, and auth differ):
```
HTTP Method = GET
Connection Timeout = 5 secs
Socket Read Timeout = 15 secs
Socket Write Timeout = 15 secs
Socket Idle Timeout = 5 mins
Socket Idle Connections = 5
Response Cache Enabled = false
Response Cache Size = 10MB
Response FlowFile Naming Strategy = RANDOM
Response Redirects Enabled = True
Request Date Header Enabled = True
Request Content-Type = ${mime.type}
Request Content-Encoding = DISABLED
Request Body Enabled = false
Request Multipart Form-Data Filename Enabled = true
Request Chunked Transfer-Encoding Enabled = false
HTTP/2 Disabled = True   (rapid7's "List Rapid7 Sites (All)" is the one exception: False)
auto-terminated relationships: No Retry, Retry, Original, Failure   (only "Response" is wired onward)
```
Basic Auth (FortiSIEM, Rapid7): `Request Username` = `#{username}`, `Request Password` = `***`
(parameter-context-backed, `Request Digest Authentication Enabled` left at redacted/default-off).
Header-based auth (SentinelOne): a redacted `Authorization` property set directly on the processor
(no username param in that flow's parameter context) — this is the one flow with no
`Request Username`/`Request Password`, consistent with token/bearer auth instead of Basic.
All three vendor APIs are fronted by an internal API gateway parameterized as `#{base_url}` (FortiSIEM
and Rapid7 both route through `apisix:9080`); PokeAPI hits the public API directly (`pokeapi.co`).

### 9.2 Pagination styles observed (three distinct styles, one per API's native scheme)
1. **Offset/limit** (`pokeapi-offset-test`): `?limit=${limit}&offset=${offset}`, loop-exit by
   checking whether the response's first result exists (`$.results[0].name` non-empty). See §3.4 for
   the exact wiring — this is the cleanest, most reusable pagination sub-flow of the five (packaged
   as its own process group with in/out ports).
2. **Page-index + total-pages** (`rapid7-securado__assets`, both its site-level and asset-level
   loops): `?page=${page}&size=500`, response includes `page.number`/`page.totalPages`, loop
   continues while `page_number < page_total - 1`. Implemented twice at two nesting levels (sites,
   then assets-within-site) with structurally identical processor chains
   (`Init*Page → List* → Extract*PageMeta → Has More*Pages? → Next*Page`, looping back to `List*`).
3. **Cursor-based** (`sentinelone__agents`, both its site-level and agent-level loops):
   `?limit=200&cursor=${cursor}`, response includes `pagination.nextCursor`, loop continues while
   `next_cursor` is non-empty. Simpler continuation check than page-index math since the API itself
   signals "no more data" via an absent/empty cursor rather than requiring the client to track totals.
4. **No pagination, full pull per parent** (`fortisiem__devices`): both the orgs list and the
   devices-by-org list are unpaginated single GETs; only the third-level per-device detail fetch is
   additionally conditional (`has_access_ip` gate) rather than paginated.

**Nested pagination shape** (Rapid7, SentinelOne): both flows paginate an outer collection (sites),
and for *each* site returned, run an independent inner pagination loop (assets, or agents) scoped by
that site's ID interpolated into the inner endpoint's URL parameter. The outer and inner loops are
structurally parallel pagination sub-chains built from the same primitives (`Init*`,
`List/Get*`, `Extract*PageMeta`, `Has More*?`, `Next*`) rather than a shared reusable component
(contrast with pokeapi's `PG_OFFSET_PAGINATOR`, which IS a reusable component but is only used once
in that test flow).

### 9.3 File-share (SMB) ingest (`fileshare__assets`)
`ListSmb` (state-tracked incremental listing, §6.2, `PRIMARY`-node-only execution to avoid duplicate
listing across a NiFi cluster) → `FetchSmb` (streams the actual file bytes, `${path}/${filename}`
built from the listing attributes, `Completion Strategy=NONE` meaning source files are left in place
after fetch, not moved/deleted) → format conversion (`ConvertRecord` XLSX→CSV via `ExcelReader`/
`CSVRecordSetWriter`). This is the only non-HTTP ingest mechanism among the five flows.

---

## 10. SYNTHESIS — PUBLISH PATTERN

### 10.1 `PublishKafka` common config (all 4 publishing flows — fileshare, fortisiem, rapid7, sentinelone)
```
compression.type = none
acks = all
partitioner.class = org.apache.kafka.clients.producer.internals.DefaultPartitioner
Record Metadata Strategy = FROM_PROPERTIES
Publish Strategy = USE_VALUE
Failure Strategy = Route to Failure
Transactions Enabled = false
Kafka Key Attribute Encoding = utf-8
Header Encoding = UTF-8
max.request.size = 1 MB   (fortisiem overrides to 500 MB — likely to accommodate large XML device payloads)
Topic Name = #{topic_name}   (fortisiem hardcodes literal 'hehe' instead — see §2.3 item 16, flag as a bug/test leftover before templating)
Kafka Key = ${kafka.key}   (fileshare leaves this null — no key)
Kafka Connection Service -> Global Kafka3ConnectionService (shared, same UUID d8714b37-954b-356a-9c1f-32925bc0abf2 in all 4 flows)
Record Writer -> flow-local AvroWriter controller service instance (schema-reference-writer strategy)
autoTerminatedRelationships: success, failure   (both terminated directly on the processor — no DLQ, see §11)
```

### 10.2 `Global Kafka3ConnectionService` (`org.apache.nifi.kafka.service.Kafka3ConnectionService`)
Identical configuration duplicated (same controller-service UUID) into every flow's own process
group — i.e. not a true cross-flow "external controller service" reference (`externalControllerServices`
is `{}` in every export) but a template that was copy/pasted with a stable ID:
```
bootstrap.servers = #{Kafka__endpoint}
security.protocol = PLAINTEXT
sasl.mechanism = GSSAPI          <-- inconsistent with PLAINTEXT; looks like an unused template default, not an active SASL/GSSAPI handshake
ack.wait.time = 5 sec
max.block.ms = 5 sec
default.api.timeout.ms = 60 sec
max.poll.records = 10000
isolation.level = read_committed
```

### 10.3 Avro + Confluent Schema Registry integration
Every publishing flow's `AvroWriter` (`org.apache.nifi.avro.AvroRecordSetWriter`) uses:
```
Schema Access Strategy = schema-name
Schema Name = #{schema_name}                     (e.g. fortisiem__device__bronze__history-value)
Schema Registry -> Global Confluent Schema Registry (org.apache.nifi.confluent.schemaregistry.ConfluentSchemaRegistry)
Schema Write Strategy = schema-reference-writer
Schema Reference Writer -> Global Confluent Encoded Schema Ref Writer (ConfluentEncodedSchemaReferenceWriter)
Encoder Pool Size = 32
Cache Size = 1000
Compression Format = NONE
```
`Global Confluent Schema Registry`: `Schema Registry URLs = #{Apicurio__endpoint}` (i.e. the
registry is actually Apicurio, addressed through NiFi's Confluent-compatible schema registry client —
`Cache Size=1000`, `Cache Expiration=1 hour`, `Communications Timeout=30 secs`, redacted
`Authentication Type`). `ConfluentEncodedSchemaReferenceWriter` has no configurable properties — it
writes the standard Confluent wire format (magic byte `0x0` + 4-byte schema ID prefix) ahead of the
Avro-encoded payload, matching how a Confluent/Apicurio-compatible consumer expects to deserialize.

### 10.4 Topic / schema naming convention (confirmed against all 4 `.avsc` files)
Pattern: **`<source>__<entity>__bronze__history`** for the Kafka topic, and
**`<source>__<entity>__bronze__history-value`** for the registered schema/subject name (the `-value`
suffix is the standard Confluent Schema Registry "subject for a topic's value" convention). Observed:
| Flow | topic_name | schema_name | Avro record `name` / `namespace` |
|---|---|---|---|
| fileshare | `fileshare__asset__bronze__history` | `fileshare__asset__bronze__history-value` | `prod__cmdb__assets` / `bronze` |
| fortisiem | `fortisiem__device__bronze__history` (**but processor hardcodes `hehe`**) | `fortisiem__device__bronze__history-value` | `fortisim` / `bronze` |
| rapid7-securado | `rapid7-securado__asset__bronze__history` | `rapid7-securado__asset__bronze__history-value` | `rapid7` / `bronze` |
| sentinelone | `sentinelone__agent__bronze__history` | `sentinelone__agent__bronze__history-value` | `sentinal` [sic] / `bronze` |

Every schema's field list is a near-verbatim flattening of the vendor API's own JSON/XML response
shape (FortiSIEM: 39 fields incl. nested `components`/`interfaces`/`softwarePatches` as
complex/array types; Rapid7: 26 fields incl. `vulnerabilities`, `services`, `software` arrays;
SentinelOne: 86 fields, by far the widest schema), **plus exactly one flow-injected field,
`ingest_ts`, appended last**, with no other flow-injected fields except fileshare's
`organization_name` and rapid7's `site_id`/`site_name` (both denormalized parent-context fields
folded into the record, not just `ingest_ts`). No schema includes a `_hash`, `_dedup_key`, or
`is_duplicate` field of any kind, reinforcing §6's finding.

---

## 11. SYNTHESIS — COMMON STRUCTURAL CONVENTIONS

### 11.1 Processor-level defaults (uniform across essentially every processor in all 5 flows)
```
schedulingStrategy = TIMER_DRIVEN
schedulingPeriod = 0 sec               (event-driven within the flow graph; only the seed/GenerateFlowFile
                                         or the top-of-chain InvokeHTTP for FortiSIEM carries a real period)
concurrentlySchedulableTaskCount = 1
executionNode = ALL   (except ListSmb: PRIMARY — cluster-safe listing, see §6.2)
penaltyDuration = 30 sec
yieldDuration = 1 sec
bulletinLevel = WARN
retryCount = 10
backoffMechanism = PENALIZE_FLOWFILE
maxBackoffPeriod = 10 mins
retriedRelationships = []              (NiFi 2.x per-processor auto-retry feature is available but
                                         not actually opted into via retriedRelationships on any processor
                                         in these flows — retryCount/backoff are set but inert without
                                         a populated retriedRelationships list)
```
Connections (all, in all 5 flows) use identical back-pressure/expiration defaults:
`backPressureObjectThreshold=10000`, `backPressureDataSizeThreshold=1 GB`,
`flowFileExpiration=0 sec` (no FlowFile-age-based expiration anywhere), no prioritizers configured.

### 11.2 Trigger pattern
Every flow (except FortiSIEM, see §2.1) begins with a `GenerateFlowFile` "seed" processor on a
`TIMER_DRIVEN` schedule of 30 or 60 minutes, `Batch Size=1`, feeding the first `UpdateAttribute`
"Init*" step that seeds loop-control attributes (offset/page/cursor = 0/''), which then feeds the
first `InvokeHTTP`/`ListSmb` call. This is the canonical "one clock tick → one full poll-and-paginate
run" shape.

### 11.3 Naming conventions
- Processors: `Verb Source/Entity (Qualifier)` — e.g. `List CMDB Asset Files (SMB)`,
  `Get FortiSIEM Device Details`, `Extract S1 Sites Page Meta`, `Route Rapid7 Blocked Sites`.
  Vendor/source name is embedded in almost every processor name (aids readability when multiple
  flows are open in the same NiFi canvas / when generating names programmatically from a
  `{source}` + `{verb}` + `{entity}` template).
- Parameters: `snake_case`, flow-scoped context named `<flow_name>__params`; shared/global values
  referenced via `#{Service__property}` (`#{SMB__endpoint}`, `#{Kafka__endpoint}`,
  `#{Apicurio__endpoint}`) that are **not** present in the flow's own parameter context — implying a
  separate, un-exported global parameter context assigned to the process group hosting the shared
  controller services, outside the scope of a single-flow "Download flow definition" export.
- Controller services: shared/reusable ones prefixed `Global ` (`Global Kafka3ConnectionService`,
  `Global Confluent Schema Registry`, `Global Confluent Encoded Schema Ref Writer`,
  `Global SMB Client Service`); flow-local ones are plain type-derived names (`AvroWriter`,
  `CSV Reader`, `JsonTreeReader`, `XMLReader`).
- Kafka topic/schema: `<source>__<entity>__bronze__history[-value]`, double-underscore delimited
  (see §10.4).

### 11.4 Reuse / componentization
Only `pokeapi-offset-test` factors a sub-pattern into its own child process group
(`PG_OFFSET_PAGINATOR`) with input/output ports as its interface. All other flows — including the
two with structurally near-identical nested pagination logic repeated at two levels
(Rapid7 sites/assets, SentinelOne sites/agents) — inline every copy of the pattern directly on the
top-level canvas rather than factoring it into a reusable child group. If generating flows
programmatically, the pokeapi shape (process group + 1 input port + 1 output port + internal
Init/Invoke/Extract/Route/Next processors) is the template to reuse for any single-parameter
(offset, page, or cursor) pagination primitive; the other four flows show that this factoring is
*not* consistently applied even within the same flow (i.e. don't assume the generator needs to
detect and dedupe repeated inline sub-chains — these reference flows themselves don't do that).

### 11.5 Error handling / DLQ — notable gap
**No flow routes `failure` (or the InvokeHTTP `Retry`/`No Retry`/`Original`/`Failure` relationships)
to any logging, alerting, or dead-letter destination.** Every failure-type relationship across all
five flows is either left with no outbound connection (which, per the `autoTerminatedRelationships`
list on each processor, is explicitly auto-terminated — i.e. the FlowFile is deliberately dropped)
or, for `PublishKafka`, routed via `Failure Strategy = Route to Failure` into the processor's own
`failure` relationship, which is then *also* auto-terminated. There is no `PutFile`, no dedicated
"error" or "DLQ" Kafka topic, no `LogAttribute` on a failure path (the one `LogAttribute` present,
in pokeapi, sits on the successful `split` path, not a failure path), and no bulletin-based alerting
beyond the ambient `bulletinLevel=WARN` default. If the target application's flows need actual
failure visibility/replay, that is new design work — these five flows are not evidence of an
existing DLQ convention to replicate.

### 11.6 Funnels / labels / remote process groups
None of the five flows use a `Funnel`, `Label`, or `RemoteProcessGroup` — confirmed empty in every
file's hierarchy walk. Multi-path fan-in (e.g. FortiSIEM's two `UpdateRecord` branches both feeding
`Publish FortiSIEM Devices Raw`) is done with plain multiple incoming connections to the same
processor, not a funnel.

---

## Appendix: quick counts

| Flow | Processors | Connections | Controller Services | Process Groups |
|---|---|---|---|---|
| fileshare__assets | 7 | 6 | 8 | 1 |
| fortisiem__devices | 16 | 16 | 5 | 1 |
| pokeapi-offset-test | 8 | 10 | 0 | 2 |
| rapid7-securado__assets | 19 | 20 | 6 | 1 |
| sentinelone__agents | 18 | 19 | 6 | 1 |
