# User Reference Flows 2 — `Ingest3.json` (dedup) & `Publish3.json` (JDBC/converters)

**Scope.** Read-only structural analysis of two additional user-supplied NiFi flow exports:
`reference/nifi-flows/Ingest3.json` (577 KB, user: "an example flow that does de duplication") and
`reference/nifi-flows/Publish3.json` (275 KB, user: an example for "how to build jdbc adapter" /
converters). Both are NiFi Registry `VersionedFlowSnapshot` exports (`flowEncodingVersion: "1.0"`),
same shape as the five flows in `nifi-reference-flows.md`, all processors/services on NiFi bundle
`2.9.0`/`2.7.2` (parameter `NIFI_VERSION`). Parsed programmatically (Python, full JSON walk of
`processGroups`/`processors`/`controllerServices`/`connections`, not sampled) rather than read as raw
text — both files are single-line minified JSON, `Ingest3.json` alone is ~2,200 lines pretty-printed.

Compared against: `docs/orchestration/analysis/dedup-reference-flow.md` (our existing dedup template,
derived from the older `DummyJson_Dedup.json`), `backend/services/adapter/compiler/transforms.py`
(`_compile_dedup` + `GROOVY_HASH_SCRIPT`), `blocks_jdbc.py`, `blocks_kafka.py`, `connectors.py`, and
`architecture-mvp.md` §2 (MVP dedup rules).

**Headline findings:**
1. `Ingest3.json` is a **newer, evolved version of the same demo topology** as `nifi-reference-flows.md`'s
   five flows (`fileshare.asset`, `rapid7_securado.asset`, `rapid7_asyad.asset` [new], `fortisiem.device`,
   `sentinelone.agent`) — but now **every one of the 5 source flows has a full dedup mechanism wired in**,
   confirming and extending (not contradicting) the older `DummyJson_Dedup.json`-derived
   `dedup-reference-flow.md`.
2. `Publish3.json` is **not** a "Kafka → database" or "database → Kafka via CDC-log" adapter example.
   It is a **Trino/Iceberg lakehouse → Kafka replication publisher** (`ExecuteSQLRecord` + `PutSQL` +
   `PublishKafka`, snapshot-diff CDC via Iceberg `FOR VERSION AS OF`). It contains **zero** occurrences
   of `ConsumeKafka`, `PutDatabaseRecord`, `QueryDatabaseTable*`, `LookupRecord`, or
   `DatabaseRecordLookupService` — every one of those four flagged-as-unverified processor types in
   `blocks_jdbc.py`/`blocks_kafka.py` remains **unconfirmed** by this reference. What it does confirm,
   with byte-exact evidence, is the `DBCPConnectionPool` controller service (driver class, URL shape,
   property names) — the exact thing `blocks_jdbc.py::_ensure_db_pool` flagged as needing verification.

---

## 1. `Ingest3.json` — topology

```
/Ingest  (root PG — no processors, 4 shared controller services, 1 shared DLQ fan-in)
├── fileshare.asset          13 processors, 6 controller services  — SMB/XLSX ingest
├── rapid7_securado.asset    25 processors, 4 controller services  — paginated REST (sites→assets)
├── rapid7_asyad.asset       25 processors, 4 controller services  — identical shape, 2nd tenant
├── fortisiem.device         15 processors, 4 controller services  — two-tier XML REST (orgs→devices)
├── sentinelone.agent        22 processors, 4 controller services  — cursor-paginated REST
└── global__dlq                2 processors, 0 controller services — shared DLQ sink
```

Root-level shared controller services: `global__redis_pool` (`RedisConnectionPoolService`,
`Connection String=redis:6379`, `Redis Mode=Standalone`, `Database Index=0`), `global__kafka_connection`
(`Kafka3ConnectionService`, `bootstrap.servers=#{KAFKA_BOOTSTRAP_SERVERS}`), `global__schema_registry`
(`ConfluentSchemaRegistry`, `Schema Registry URLs=#{SCHEMA_REGISTRY_URL}`), `global__schema_ref_writer`
(`ConfluentEncodedSchemaReferenceWriter`, no properties — Confluent 5-byte wire format). Every
per-flow dedupe cache and every per-flow Avro writer references these same 4 shared services — one
Redis **connection pool**, one Kafka connection, one schema registry, shared platform-wide; each stream
gets its own dedupe **cache client** and its own Avro **writer** instance pointed at that shared pool/registry.

Parameter contexts: one `global-infra` context (`SCHEMA_REGISTRY_URL`, `KAFKA_BOOTSTRAP_SERVERS`,
`KAFKA_SECURITY_PROTOCOL`, `DEFAULT_SCHEDULE_PERIOD`, `DLQ_TOPIC_PREFIX=__dlq__`) inherited by 5
per-flow contexts, each with `SCHEMA_NAME`, `KAFKA_TOPIC`, `DLQ_TOPIC`, `SCHEDULE_PERIOD`, plus
source-specific creds/pagination params.

---

## 2. `Ingest3.json` — the complete dedup mechanism

### 2.1 Chain, verbatim example (`fileshare.asset`)

```
scan_files__list (ListSmb, CRON #{DEFAULT_SCHEDULE_PERIOD})
  --success--> scan_files__set_error_attrs (UpdateAttribute: error.source_flow, error.dlq_topic)
  --success--> scan_files__fetch (FetchSmb)
  --success--> scan_files__convert (ConvertRecord: ExcelReader -> JsonRecordSetWriter)
  --success--> renamefield (RenameRecordField: 15 Excel headers -> snake_case, e.g. 'IP Address'->ip_address)
  --success--> scan_files__split (SplitRecord, Records Per Split=1, autoterm 'original')
  --splits--> scan_files__inject_meta (UpdateRecord: /organization_name = filename-derived)
  --success--> enrich__extract_key (EvaluateJsonPath: sl_no, location, organization_name -> attrs)
  --matched--> enrich__set_key (UpdateRecord: /object_id=${organization_name}_${sl_no},
                                 /ingest_id=${uuid}, /ingest_ts=${now():toNumber()})
  --success--> dedupe__hash (ExecuteGroovyScript)        <- fingerprint, sets 'dedupe.key' attribute
  --success--> dedupe__detect (DetectDuplicate)          <- cache check
      ├─ non-duplicate --> load__publish (PublishKafka, Topic=#{KAFKA_TOPIC}, gzip, Avro+registry)
      ├─ duplicate     --> auto-terminated (counted, silent — MVP-correct)
      └─ failure       --> fileshare.asset__error_out (OUTPUT_PORT) --> root dlq_in --> global DLQ
  dedupe__hash --failure--> auto-terminated (see §2.6 — NOT routed to DLQ, unlike detect's failure)
```

Every one of the 5 flows follows **exactly this shape**: `[source-specific fetch/parse/enrich]` →
one `UpdateRecord` that computes `/object_id` (a synthetic composite key, e.g.
`${site_id}_${asset_id}`, `${naturalId}_${organization__attr_id}`, `${agent_id}`) plus
`/ingest_id`/`/ingest_ts` → `dedupe__hash` → `dedupe__detect` → `non-duplicate` → `load__publish`.
Dedup is the **last** step before publish in every single stream — no exceptions.

### 2.2 Fingerprint computation — JSON variant (4 of 5 flows: fileshare, rapid7×2, sentinelone)

Dynamic properties: `SRC` (short source name, e.g. `fileshare`, `rapid7_securado`), `EXCLUDES =
ingest_id,ingest_ts` (verbatim in all 4). Script body, byte-identical across all 4 flows:

```groovy
import groovy.json.*
import java.security.MessageDigest
import org.apache.nifi.processor.io.InputStreamCallback

def flowFile = session.get()
if (!flowFile) return
try {
    def exclStr = (binding.hasVariable('EXCLUDES') ? (EXCLUDES.value ?: '') : '')
    def excl = exclStr.split(',').collect { it.trim() }.findAll { it }
    def src  = (binding.hasVariable('SRC') ? (SRC.value ?: 'src') : 'src')
    def holder = [t: '']
    session.read(flowFile, { inp -> holder.t = inp.getText('UTF-8') } as InputStreamCallback)
    def parsed = new JsonSlurper().parseText(holder.t)
    def rec = (parsed instanceof List) ? parsed[0] : parsed
    def oid = rec.get('object_id')
    // copy, drop excluded keys, serialize as-is (source field order is stable per fetch)
    def m = new LinkedHashMap(rec)
    excl.each { m.remove(it) }
    def cj = JsonOutput.toJson(m)
    def h = MessageDigest.getInstance('SHA-256').digest(cj.getBytes('UTF-8')).encodeHex().toString()
    flowFile = session.putAttribute(flowFile, 'dedupe.key', (src + ':' + oid + ':' + h).toString())
    session.transfer(flowFile, REL_SUCCESS)
} catch (Exception e) {
    log.error('dedupe hash failed: ' + e.message, e)
    session.transfer(flowFile, REL_FAILURE)
}
```

This is **identical** (modulo `SRC` value) to the script `dedup-reference-flow.md` already documented
from `DummyJson_Dedup.json` — confirms it was not a one-off, it's the platform's standing JSON-content
dedup pattern. Key structure unchanged: `<SRC>:<object_id>:<sha256-of-record-minus-EXCLUDES>`.

### 2.3 Fingerprint computation — Record variant (`fortisiem.device` only, XML source)

Same `SRC`/`EXCLUDES` convention (`EXCLUDES = ingest_id,ingest_ts,bios,deviceStatus` — two extra
volatile fields excluded here), plus a **new dynamic property not seen before**: `CTL.reader` =
`079b001d-...` (the flow's own `XMLReader` controller-service id, bound directly into the script).
Rather than text-parsing JSON, it reads the FlowFile through the bound `RecordReader` and canonicalizes
`Record`/array/map values into a `TreeMap` before hashing:

```groovy
import java.security.MessageDigest
import groovy.json.JsonOutput
import org.apache.nifi.processor.io.InputStreamCallback

def ff = session.get()
if (!ff) return
try {
    def exclStr = (binding.hasVariable('EXCLUDES') ? (EXCLUDES.value ?: '') : '')
    def excl = exclStr.split(',').collect { it.trim() }.findAll { it } as Set
    def src  = (binding.hasVariable('SRC') ? (SRC.value ?: 'src') : 'src')
    def holder = [oid: null, hash: null]
    def toPlain
    toPlain = { v ->
        if (v instanceof org.apache.nifi.serialization.record.Record) {
            def m = new TreeMap(); v.rawFieldNames.each { m.put(it, toPlain(v.getValue(it))) }; return m
        }
        if (v instanceof Object[]) return v.collect { toPlain(it) }
        if (v instanceof java.util.Map) { def m = new TreeMap(); v.each { k, val -> m.put(k, toPlain(val)) }; return m }
        return v
    }
    session.read(ff, { inStream ->
        def rr = CTL.reader.createRecordReader(ff, inStream, log)
        def rec = rr.nextRecord()
        holder.oid = rec.getValue('object_id')
        def m = new TreeMap()
        rec.rawFieldNames.each { fn -> if (!excl.contains(fn)) m.put(fn, toPlain(rec.getValue(fn))) }
        def cj = JsonOutput.toJson(m)
        holder.hash = MessageDigest.getInstance('SHA-256').digest(cj.getBytes('UTF-8')).encodeHex().toString()
        rr.close()
    } as InputStreamCallback)
    ff = session.putAttribute(ff, 'dedupe.key', (src + ':' + holder.oid + ':' + holder.hash).toString())
    session.transfer(ff, REL_SUCCESS)
} catch (Exception e) {
    log.error('record-dedupe hash failed: ' + e.message, e)
    session.transfer(ff, REL_FAILURE)
}
```

Why this variant exists: `fortisiem.device` keeps records as **native XML all the way to publish**
(its `load__publish` PublishKafka uses `Record Reader=xml_reader`, converting straight to Avro at the
very last step) — it never normalizes to JSON mid-chain, so the hash step must read XML via a real
`RecordReader` (a naive `JsonSlurper().parseText()` would throw on XML content). Also notice it uses
`TreeMap` (sorted keys) instead of the JSON variant's `LinkedHashMap` (insertion order) — XML record
field order isn't guaranteed stable the way a JSON API response's field order is, so it sorts before
hashing to keep the fingerprint stable run-to-run. **This is a real, non-cosmetic difference** — see
§6 item 5 for why our compiler doesn't need to replicate it.

### 2.4 `DetectDuplicate` (identical shape in all 5 flows, only the CS id differs)

```
type: org.apache.nifi.processors.standard.DetectDuplicate
Cache Entry Identifier   = ${dedupe.key}
Cache The Entry Identifier = true
Age Off Duration         = 24 hours          (all 5 — hardcoded, not parametrized)
Distributed Cache Service = <per-flow>.dedupe__cache
autoTerminatedRelationships = [duplicate]     (counted, silent drop)
non-duplicate --> load__publish
failure       --> <flow>.error_out            (routed to DLQ — see §2.6)
```

### 2.5 Controller services

Each of the 5 flows has its own `<flow>.dedupe__cache`
(`org.apache.nifi.redis.service.RedisDistributedMapCacheClientService`, `TTL=24 hours`, pointing at
the single shared `global__redis_pool`). One shared `RedisConnectionPoolService` (`Connection
String=redis:6379`, `Redis Mode=Standalone`, `Database Index=0`) backs all 5 per-stream cache clients —
i.e. **one Redis TCP connection pool, five separate cache-client namespaces**, one per stream. This is
exactly the MVP's "one dedup cache per stream" requirement (§2.4) realized as: shared connection,
per-stream cache client object with its own TTL. `RedisConnectionPoolService`'s property set confirms
the exact property name `Password` exists (sensitive, present in `propertyDescriptors` though redacted
in this export) — matches `transforms.py::_compile_dedup`'s `"Password": "#{redis_password}"` exactly.

### 2.6 Relationships / failure handling — refines `dedup-reference-flow.md`'s old finding

The **old** `DummyJson_Dedup.json`-derived doc flagged: *"`failure` auto-terminated on
DetectDuplicate/hash — MVP requires fail-stop... generated flows must route dedup `failure` to a
visible failure path."* `Ingest3.json` shows this was **half-fixed** in the newer reference:

| Processor | `failure` relationship in Ingest3 | MVP-correct? |
|---|---|---|
| `dedupe__detect` (DetectDuplicate) | routed to `<flow>.error_out` → DLQ | Yes — matches §2.7 fail-stop |
| `dedupe__hash` (ExecuteGroovyScript) | **auto-terminated** (silently dropped) | **No** — still a gap |

So the reference now correctly fails-visibly on the Redis-touching step (`DetectDuplicate`'s `failure`
is the one that would fire if Redis were unreachable — that's the fail-stop-relevant relationship per
MVP §2.7) but still silently drops hash-script exceptions (malformed content, digest errors — a
record-level failure, not an infra failure, but still a record that vanishes with no signal). Our
compiler already routes **both** to DLQ (`builder.to_dlq(hash_key, "failure")` and
`builder.to_dlq(detect_key, "failure")` in `_compile_dedup`) — this is confirmed as the **more
correct** behavior, not something to walk back.

### 2.7 Ordering — confirms MVP §2.3 exactly

Observed order in every flow: `[source fetch/parse]` → `[field enrichment: object_id/ingest_id/
ingest_ts]` → **hash → detect** → `[Avro convert]` → publish. Dedup is unconditionally the last
transform before the terminal publish/serialize step, matching MVP §2.3 ("dedup always runs last")
and `dedup-reference-flow.md`'s original finding — now corroborated across 5 independent streams
instead of 1.

### 2.8 A vestigial artifact worth noting (not a template item)

Every one of the 5 flows also contains a `<flow>.raw__publish` `PublishKafka` processor (hardcoded
topic `<flow>.raw`, no dedup upstream) that is **`DISABLED`** and has **no incoming connection** in
all 5 flows (confirmed via `scheduledState` + connection graph, not just visual inspection) — a
leftover pre-dedup raw-passthrough path from before dedup was added, left in place but dead. Not a
pattern to reproduce; noted so it isn't mistaken for an intentional "raw + deduped" dual-publish design.

---

## 3. `Ingest3.json` — ingest patterns, error/DLQ, naming

**Sources/formats:** SMB file listing + XLSX (`fileshare.asset`, via `ExcelReader` →
`JsonRecordSetWriter`, **not** CSV — see below); paginated JSON REST, page-number style
(`rapid7_securado.asset`, `rapid7_asyad.asset` — identical shape, second tenant); paginated JSON REST,
cursor style (`sentinelone.agent`); two-tier XML REST, list-then-detail (`fortisiem.device`, via
`XMLReader`/`XMLRecordSetWriter`, `EvaluateXPath`, `SplitXml`). **No CSV reader/writer anywhere in this
file** (`CSVReader`/`CSVRecordSetWriter` count = 0) — this **updates** `nifi-reference-flows.md`'s
older `fileshare__assets` writeup, which described an Excel→CSV round-trip via `ReplaceText` header
rewriting; `Ingest3`'s `fileshare.asset` instead converts XLSX straight to JSON with `ConvertRecord`
and does header renaming with `RenameRecordField` (15 explicit `/'Excel Header'` → `snake_case`
mappings) — no CSV intermediate at all.

**Error/DLQ handling:** every per-record failure relationship in every flow (HTTP retry/failure,
XPath/JsonPath unmatched, dedup failure, publish failure) routes to that flow's own `<flow>.error_out`
output port, which all 5 flows wire to a single root-level input port `dlq_in` feeding one shared
`global__dlq` process group: `dlq_in` → `global__dlq__enrich` (UpdateAttribute: adds
`error.processor=${provenance.processor.name}`, `error.timestamp=${now():toNumber()}`) →
`global__dlq__publish` (PublishKafka, `Topic Name=${error.dlq_topic}` — dynamic, set per-flow earlier
via each flow's `trigger`/`set_error_attrs` processor to `#{DLQ_TOPIC}`, e.g.
`bronze.fileshare.asset__raw__dlq`), with `failure` **self-looping back onto itself** (park-in-own-queue
retry, not a drop) — the same self-loop-on-DLQ-publish-failure pattern our `dlq.py::build()` already
implements (`builder.link("dlq__publish", "dlq__publish", ["failure"])`). Difference: Ingest3
centralizes DLQ publish in one shared cross-process-group sink; our compiler gives each block its own
`dlq__meta`/`dlq__publish` pair. Both achieve the same fail-visibly/self-retry outcome — not a
correctness gap, just a topology choice (see §6 item 6).

**Naming:** processor names follow `<flow>.<stage>__<step>` (e.g. `fileshare.asset__dedupe__hash`),
consistent across all 5 flows — same convention already reflected in our `t<idx>__<kind>`/
`dedupe__hash`/`dedupe__detect` processor keys.

---

## 4. `Publish3.json` — topology

```
/Publish  (root PG — empty, 0 processors, 0 controller services)
├── asset.publish__full          18 processors, 3 controller services  — full re-push / backfill
└── asset.publish__incremental   16 processors, 3 controller services  — snapshot-diff CDC
```

Both sub-groups share the same 3 controller services (defined independently in each PG, identical
config): `KafkaService` (`Kafka3ConnectionService`, `bootstrap.servers=kafka:29092`), `TrinoJDBC`
(`DBCPConnectionPool` — see §5.2), `JsonWriter` (`JsonRecordSetWriter`, schemaless). **No parameter
contexts at all** (`parameterContexts: {}`) — every source-table binding (`src.table.catalog/schema/
name`, `key.expr`, `change.expr`, `kafka.topic`, `enrich_mode`, `checkpoint.table`) is instead a
**dynamic property on a per-source `GenerateFlowFile` "feed" processor** (`feed_fileshare`,
`feed_fortisiem`, `feed_r7asyad`, `feed_r7securado`, `feed_sentinelone`, `feed_gold` — 6 sources,
hourly `TIMER_DRIVEN`), read downstream via `${attribute}` EL rather than `#{param}` — a materially
different binding mechanism from `Ingest3`/`DummyJson_Dedup`'s parameter-context style, worth noting
if this "point the same generic sub-flow at N sources via per-source trigger properties" pattern is
ever useful for our own compiler's multi-entity fan-out.

**What this flow actually does** (per its own process-group `comments`, corroborated by the SQL):
a **generic Iceberg-table → Kafka publisher over Trino JDBC**, in two modes — `asset.publish__full`
unconditionally republishes every row at the current Iceberg snapshot ("head") each run (backfill/
disabled-by-default), `asset.publish__incremental` diffs the current head snapshot against a
checkpointed prior snapshot using Trino's Iceberg time-travel (`FOR VERSION AS OF <snapshot_id>`),
publishing one Kafka message per **changed** key (`upsert` with full-row JSON, or a delete-style
tombstone key derived from an `EXCEPT`-based key-set diff), then advances a checkpoint table
(`bronze.checkpoints.codex_nifi_snapshot_messages_checkpoint_v1`) via `PutSQL` — **only after** the
last `SplitJson` fragment of that batch has published (`route_last_fragment`'s
`${fragment.index:toNumber():plus(1):equals(${fragment.count})}` gate), so the checkpoint never
advances past records that haven't actually been delivered yet.

---

## 5. `Publish3.json` — JDBC-relevant patterns

### 5.1 Processor inventory — direction is the opposite of what was guessed

A full-text scan of the raw JSON for `ConsumeKafka`, `PutDatabaseRecord`, `QueryDatabaseTable`,
`LookupRecord`, `DatabaseRecordLookupService`, `AvroRecordSetWriter`, `ConfluentSchemaRegistry`,
`CSVReader`, `XMLReader`, `ExcelReader`, `ConvertRecord`, `RedisDistributedMapCache`, `DetectDuplicate`
returns **zero hits for every one of them**. The only DB/Kafka-relevant processor types present are:

| Processor type | Role in this flow |
|---|---|
| `org.apache.nifi.processors.standard.ExecuteSQLRecord` | runs the setup query (head snapshot id, checkpoint id, column-projection list) and the diff/full-select query against Trino; writes results via `JsonWriter` |
| `org.apache.nifi.processors.standard.PutSQL` | writes the checkpoint row back to the same Trino pool (static SQL text built via NiFi EL, not a record-oriented insert) |
| `org.apache.nifi.kafka.processors.PublishKafka` | publishes each diffed/full row (content = SQL-built `value_json` string, not a RecordWriter-serialized row) |
| `org.apache.nifi.processors.standard.GenerateFlowFile` | per-source trigger + dynamic-property parameter carrier (not record generation) |
| `EvaluateJsonPath`, `RouteOnAttribute`, `SplitJson`, `UpdateAttribute` | plumbing (attribute promotion, branching, per-row split, state stamping) |

So: **the direction is inverted from what the task brief speculated.** This is a
lakehouse(Trino/Iceberg)-**read** → Kafka-**write** publisher, not a Kafka-read → database-write
adapter. None of `blocks_jdbc.py`'s `QueryDatabaseTableRecord`/`PutDatabaseRecord`/`LookupRecord`/
`DatabaseRecordLookupService` guesses, and none of `blocks_kafka.py`'s `ConsumeKafka` guesses, are
confirmed **or** corrected by this file — they remain exactly as unverified as before this analysis.

### 5.2 `DBCPConnectionPool` ("TrinoJDBC") — the one thing this file does confirm, byte-exact

```
type: org.apache.nifi.dbcp.DBCPConnectionPool
Database Connection URL        = jdbc:trino://trino:8080          <- NO trailing /<database>
Database Driver Class Name     = io.trino.jdbc.TrinoDriver
Database User                  = admin
Password                       = (sensitive, present in propertyDescriptors, redacted in export)
Database Driver Locations      = /opt/nifi/nifi-current/nar_extensions/trino-jdbc-480.jar
Maximum Idle Connections = 8 | Minimum Idle Connections = 0 | Max Total Connections = 4
Max Wait Time = 500 millis | Time Between Eviction Runs = -1 | Minimum Evictable Idle Time = 30 mins
Maximum Connection Lifetime = -1 | Soft Minimum Evictable Idle Time = -1 | Validation Query = (unset)
```

Identical in both `asset.publish__full` and `asset.publish__incremental` (2 independent instances,
same config). See §7 for exactly which of `blocks_jdbc.py::_ensure_db_pool`'s guesses this confirms,
corrects, or leaves open.

### 5.3 Record writer — no Avro, no schema registry, on this branch

The only RecordSetWriter in this file is `JsonWriter` (`org.apache.nifi.json.JsonRecordSetWriter`,
`Schema Access Strategy=inherit-record-schema`, `Schema Write Strategy=no-schema`,
`Output Grouping=output-array`). No `AvroRecordSetWriter`, no `ConfluentSchemaRegistry`, no
`ConfluentEncodedSchemaReferenceWriter` anywhere — this publish path is **deliberately schemaless**
(plain JSON on the wire, no registry). This is architecturally consistent with our platform's own
`kafka write` (plain/JSON, schemaless) vs `kafka_kc` (governed/Avro+registry) split — Publish3's
pattern maps to the former, not the latter. It does **not** add new evidence toward the
Avro/registry-converter properties `blocks_kafka_kc.py`/`connectors.py` already use (those remain
sourced from `Ingest3`/the alpha production generator, unaffected by this file).

Notably, the "conversion" from Iceberg row → Kafka payload doesn't happen via a NiFi RecordSetWriter's
per-field schema mapping at all — it happens **inside the Trino SQL itself**, via `json_object(...)`/
`json_format(...)` functions that build the exact target JSON string server-side; NiFi's role is only
to extract that pre-built string (`EvaluateJsonPath $.value_json → flowfile-content`) and republish it.
This is a fundamentally different "conversion" mechanism than the Reader/Writer-pair pattern the rest
of the platform (and the task brief) assumes — worth knowing if a future block ever needs
SQL-engine-side JSON shaping instead of NiFi-side record conversion, but out of scope for correcting
`blocks_jdbc.py` today (our `read` mode has no SQL-templating surface, by design).

### 5.4 Iceberg / warehouse

Confirms Trino is the platform's SQL engine of choice **over** Iceberg (queries hit
`<catalog>.<schema>.<table>` triples like `silver.asset.fortisiem__device__current`,
`gold.asset.asset__xref`, and Iceberg's `"<table>$snapshots"` metadata table), i.e. Iceberg tables are
read via **standard JDBC against Trino**, not via any NiFi-native Iceberg processor and not via Kafka
Connect. This is the **read-side mirror** of `connectors.py::build_kafka_kc_connector`'s
`IcebergSinkConnector` (write-side, Kafka→Iceberg via Kafka Connect) — no code changes implied for
`connectors.py`, but it does validate that `_DIALECT_DRIVERS["trino"] = "io.trino.jdbc.TrinoDriver"`
in `blocks_jdbc.py` is the **correct** driver class for a future `jdbc read` block pointed at this same
lakehouse.

### 5.5 Error handling — much weaker than Ingest3, not a pattern to copy

Every `ExecuteSQLRecord`/`PublishKafka` in both process groups auto-terminates its `failure`
relationship (`exec_full_plain`, `exec_pass_setup`, `exec_full_portal`, `publish_to_kafka`,
`write_payload_to_content` all list `failure` in `autoTerminatedRelationships`), and `put_checkpoint`
(`PutSQL`) auto-terminates **all three** of `success`/`failure`/`retry`. There is **no DLQ port, no
error output, anywhere in this file** — a query or publish failure is silently dropped. Noted for
completeness (the task asked about converters/DBCP, not failure handling), but this is a materially
weaker failure posture than `Ingest3`'s DLQ-everywhere design and than our own `dlq.py`; nothing here
should be adopted.

---

## 6. DIFF — Dedup: adopt vs. ignore

| # | Finding | Verdict | Reason |
|---|---|---|---|
| 1 | Hash script (JSON variant) is byte-identical to the one `dedup-reference-flow.md`/`transforms.py::GROOVY_HASH_SCRIPT` were already built from, now corroborated across 4 independent flows | **Confirmed, no change** | Our `GROOVY_HASH_SCRIPT` already matches this shape (extended with `IDENTITY_FIELDS`/DLQ-on-missing — see #4) |
| 2 | `DetectDuplicate.failure` now routes to DLQ in Ingest3 (was auto-terminated in the older `DummyJson_Dedup` reference) | **Confirms our existing behavior is correct** | Our `to_dlq(detect_key, "failure")` already does this; the *newer* reference caught up to what MVP §2.7 already required |
| 3 | `dedupe__hash` (ExecuteGroovyScript)'s `failure` is **still** auto-terminated in Ingest3, even in the newer reference | **Do NOT adopt — keep our stricter behavior** | Our compiler routes hash-script failures to DLQ too (`to_dlq(hash_key, "failure")`); MVP's "records fail visibly" posture applies to record-level failures generally, not just Redis-down — Ingest3 silently dropping malformed-content records is a gap in the reference, not a pattern to import |
| 4 | Missing/null `object_id` → reference hashes `<src>:null:<hash>` (no identity validation at all, in any of the 5 flows) | **Do NOT adopt — keep our stricter behavior** | Directly the deviation MVP §2.8(a) forbids ("silent partial-identity dedup"); our `IDENTITY_FIELDS` + `dlq.reason=dedup_identity_missing` routing is a required correctness fix, still validated as necessary by this newer reference not doing it either |
| 5 | `Age Off Duration`/cache `TTL` hardcoded to `24 hours` in all 5 flows (no per-stream configurability) | **Do NOT adopt — keep our stricter behavior** | MVP §2.5 requires user-configurable TTL (1 min–365 days, default 24h); our `windowHours` param already implements this; reference is a static deployed instance, not a generator, so hardcoding there is expected and not evidence against configurability |
| 6 | Cache-key prefix (`SRC`) is flow-only (e.g. `fileshare`), not flow+stream-scoped | **Do NOT adopt — keep our stricter behavior** | Coincidentally correct here only because each PG happens to host exactly one dedup instance; our `src = f"{flow_token}__{block.id}"` is strictly more correct per MVP §2.4 ("one dedup cache per stream") and would remain correct even if a future flow had multiple dedup-enabled streams in one process group |
| 7 | Record-reader-based hash variant (`fortisiem.device`, using `CTL.reader` to read native XML instead of `JsonSlurper`) | **Do NOT adopt — architecturally unnecessary for us** | Ingest3 needs this because it keeps non-JSON streams in their native format until the final publish step. Our compiler normalizes `csv`/`xml` kafka-read content to JSON immediately at the read/split step (`blocks_kafka.py::_compile_read_terminal` pairs a csv/xml `Record Reader` with a JSON `Record Writer`) — by the time any stream reaches the `dedup` transform, content is uniformly JSON, so a single `JsonSlurper`-based script is architecturally sufficient. Adding a second `CTL.reader`-based script variant would be solving a problem our pipeline design doesn't have |
| 8 | Composite identity is pre-flattened into a real, persisted `/object_id` **data field** (via a dedicated `UpdateRecord` step) before hashing, in every flow — and Publish3's gold-layer xref logic (`asset_identity_key`, `source_object_id`, etc.) suggests this materialized identity field is later relied on for cross-source entity resolution | **Out of scope for the `dedup` transform — flag as a separate, larger design question, not a dedup fix** | MVP's Record Envelope (`architecture-mvp.md` §2/glossary) defines only `ingest_id`/`ingest_ts`/`op` as platform metadata; "identity fields" are explicitly just a **fingerprinting input**, not a mandate to materialize a new persisted field. Our `_compile_dedup` computing identity transiently inside the Groovy script (never writing it back to the record) is MVP-consistent as-is. If a future entity-resolution/xref feature needs a stable persisted identity key, that's a new capability to design deliberately, not something to bolt onto the dedup transform by copying this reference |
| 9 | Redis: one shared connection pool serves all 5 streams' cache clients in Ingest3; our compiler gives each dedup-enabled block (fresh `BlockBuilder()` per block — confirmed via `compile_flow.py::_compile_block`) its own `RedisConnectionPoolService`, not a shared one | **Minor, optional — low priority** | Not a correctness issue (every pool points at the same Redis, cache clients are already correctly per-stream either way), just more controller-service/connection objects than strictly necessary. Could be revisited as a resource-efficiency cleanup later; does not block anything and isn't required by MVP |

**Verdict in one line:** nothing from `Ingest3.json` requires changing `transforms.py::_compile_dedup`
or `GROOVY_HASH_SCRIPT`. Every place our compiler differs from the reference, our version is either
already the MVP-mandated fix for a gap the reference still has (items 3, 4, 5, 6), or a deliberate,
correct architectural choice that makes the reference's variant unnecessary (item 7). Item 8 is a
real, interesting observation but belongs to a different feature (entity resolution), not dedup.

---

## 7. DIFF — JDBC/converters: confirm, correct, or still open

| Flagged guess (in `blocks_jdbc.py`/`blocks_kafka.py`) | Verdict | Evidence / reasoning |
|---|---|---|
| `DBCPConnectionPool` property names: `Database Connection URL`, `Database Driver Class Name`, `Database User`, `Password` | **CONFIRMED, exact** | `Publish3`'s `TrinoJDBC` CS uses these exact 4 property names (§5.2); `Password` confirmed present via `propertyDescriptors` even though redacted in the properties dict |
| `_DIALECT_DRIVERS["trino"] = "io.trino.jdbc.TrinoDriver"` | **CONFIRMED, exact** | `Database Driver Class Name = io.trino.jdbc.TrinoDriver` verbatim |
| `_DIALECT_DRIVERS["postgresql"]`/`["mysql"]` | **Still unverified** | Neither reference file uses Postgres or MySQL; only Trino appears. Class names (`org.postgresql.Driver`, `com.mysql.cj.jdbc.Driver`) are standard/well-known outside NiFi specifically, but no in-flow evidence either way |
| JDBC URL template `jdbc:{dialect}://{host}:{port}/{database}` (always appends `/{database}`) | **CORRECTION for `trino`** | `Publish3`'s live URL is `jdbc:trino://trino:8080` — **no** trailing path; Trino resolves catalog/schema per-query (`FROM ${catalog}.${schema}.${table}`), not via a URL path segment. Recommend: for `dialect == "trino"`, omit the trailing `/{database}` (or make it conditional on dialect) rather than always appending it — appending an unused/undefined catalog segment risks an invalid or mismatched URL. `postgresql`/`mysql` should keep the `/{database}` suffix (standard for both) |
| `Database Driver Locations` (JAR path) — **not currently set at all** in `_ensure_db_pool` | **NEW finding — recommend ADOPT** | `Publish3` sets `Database Driver Locations = /opt/nifi/nifi-current/nar_extensions/trino-jdbc-480.jar` because Trino's JDBC driver isn't bundled in NiFi's standard `DBCPConnectionPool` NAR. Our compiler never sets this property for any dialect, which would leave the pool unable to load the driver class on a real NiFi instance unless the jar happens to be pre-placed in a global lib directory outside our control. Recommend: add a `Database Driver Locations` property, at minimum for `trino` (confirmed needed), and check whether the deployed NiFi image bundles `postgresql`/`mysql` drivers or needs the same treatment — flagged for live E2E |
| `Database Connection Pooling Service` property name, used by `QueryDatabaseTableRecord`/`PutDatabaseRecord`/`DatabaseRecordLookupService` | **Confidence raised, not fully confirmed** | Not directly evidenced (none of those 3 processor types appear in either file), but `ExecuteSQLRecord` — a sibling `standard`-bundle DB processor — uses this **exact** property name/spelling for its pool reference. Since `QueryDatabaseTableRecord`/`PutDatabaseRecord` are also `Record`-suffixed `standard`-bundle DB processors, this is reasonable (not certain) corroboration. Contrast: `PutSQL` (non-Record processor) uses a **different** name, `JDBC Connection Pool`, for the same concept — property-name spelling is NOT uniform across the bundle, so this remains a should-verify-live item, just lower priority than before |
| `QueryDatabaseTableRecord`'s `Initial Load Strategy` property + `"Start at Beginning"`/`"Start at Current Maximum Values"` values | **Still completely unverified** | `Publish3` doesn't use `QueryDatabaseTableRecord` or any watermark-column incremental pattern at all — its "incremental" mode is a from-scratch Iceberg-snapshot-diff CDC design (§4), unrelated to this processor. No new evidence either way; still needs live E2E |
| `PutDatabaseRecord`'s `Statement Type` (static) and the RecordPath-driven per-record statement-type mode | **Still completely unverified** | `Publish3` writes via `PutSQL` with a fully static, EL-templated `SQL Statement` string — not `PutDatabaseRecord` at all. No new evidence |
| `ConsumeKafka` property names (`Kafka Connection Service`, `Group ID`, `Topics`, `Topic Format`, `Auto Offset Reset`) | **Still completely unverified** | Zero occurrences of `ConsumeKafka` in either file — `Publish3` never reads from Kafka in any direction; `Ingest3` only ever publishes. Neither of the two "reference" flows this task was given actually contains a Kafka-consumer processor |
| `LookupRecord` / `DatabaseRecordLookupService` property names | **Still completely unverified** | Zero occurrences in either file |
| Record converters: Avro/JSON/CSV, schema access strategies, registry wiring | **No new evidence from `Publish3`** (already covered by `Ingest3`/older refs) | `Publish3`'s only writer is a schemaless `JsonRecordSetWriter` (no Avro, no registry) — a different, legitimate "plain JSON replication topic" pattern that maps to our `kafka write` (not `kafka_kc`) block, but doesn't confirm or correct anything in `blocks_kafka_kc.py`'s Avro/registry property set, which was already sourced from `Ingest3`/the alpha generator |

---

## 8. Summary

- **Dedup**: `Ingest3.json` fully corroborates the existing `dedup-reference-flow.md`/`transforms.py`
  design across 5 independent streams and 2 script variants. No code changes recommended — every
  divergence is either the reference having a gap our compiler already fixes (MVP-mandated), or a
  reference-only technique (record-reader hash variant) our different pipeline architecture makes
  unnecessary.
- **JDBC/converters**: `Publish3.json` is the mirror-image adapter (warehouse→Kafka, not Kafka→
  warehouse) from what was expected, so it leaves `QueryDatabaseTableRecord`, `PutDatabaseRecord`,
  `ConsumeKafka`, and `LookupRecord`/`DatabaseRecordLookupService` exactly as unverified as before —
  those still need live NiFi E2E confirmation. It does confirm `DBCPConnectionPool`'s property names
  and the `trino` driver class exactly, surfaces one real correction (Trino's JDBC URL should not get
  a trailing `/{database}`), and one clear gap to close (`Database Driver Locations` is required for
  non-bundled drivers like Trino's and is currently never set).
