# Core Architecture Analysis — Data Mobility Platform MVP

**Source documents:**
- `plan.md` — "Data Mobility Platform — Engineering Specification" (plan2.md-equivalent; 28 numbered sections, a 46-ruling decision ledger, a glossary, and an assumptions/provenance appendix). Cited below as `plan.md §N`.
- `concept.html` — titled "Data Mobility MVP — Complete Specification" (kicker: "DataPASC · Data Mobility Platform"). This **is** the `mvp.html` document that `plan.md` cites throughout as the authoritative source (`plan.md`'s own header states: "Where this disagrees with `plan.md` v6, the decision ledger (§16) wins" — confirming concept.html is the MVP source of truth plan.md builds on and defers to). Cited below as `concept.html` / `mvp.html`.
- `work.md` — "Work Breakdown — Data Mobility Platform MVP," a 104-task, 4-week plan derived from plan.md + mvp.html. Cited for cross-checking scope and task-to-section mapping.

**Note on provenance markings.** plan.md marks every claim as either directly sourced (`[FROM: mvp.html ...]`), a normative restatement of an mvp.html ruling, or `[INFERRED]` (an engineering completion the MVP text doesn't spell out). This report preserves that distinction wherever it matters — `[INFERRED]` items are engineering inferences, not confirmed MVP requirements, and are flagged as such below.

---

## 1. The Adapter Model

### 1.1 Definition

An **Adapter** is "a hardcoded, platform-shipped building block that defines one *type* of stream" — never instantiated directly by a user, always instantiated as a **Stream** (one block placed on the canvas). (`plan.md §03.2.1`, `§04`)

The MVP defines **exactly five concrete adapters plus one invisible shared parent**, a closed, compile-time registry with no runtime extension mechanism:

| Adapter id | Family | Role |
|---|---|---|
| `base` | shared parent (invisible, never instantiable) | Contributes Generic Transformations (§11) and the Test contract to every inheriting adapter |
| `http` | record block | Read, write, or lookup against HTTP/REST APIs |
| `jdbc` | record block | Read, write, or lookup against relational databases |
| `kafka` | kafka family | Read from / write to Kafka topics, schemaless |
| `kafka_kc` | kafka family | Governed write: new Avro topic + Kafka Connect sink, created as one unit |
| `kc` | kc (subscription) | Untouched-bytes sink attached to an existing topic node |

There is no sixth adapter and no user/administrator path to register a new one — "user-authored or code-defined adapters are explicitly dropped scope" (ruling 46). (`plan.md §04.3`)

### 1.2 `base@1` — the shared parent

`base` is invisible and non-instantiable: never in the adapter picker, never selected, produces no block by itself. Every one of the five adapters MUST inherit `base@1`. `base` supplies, unconditionally, to every inheriting adapter:

| Capability | Guarantee |
|---|---|
| Generic transformations | Extract/project, add/remove field, set-from-attribute, rename, coerce, route/filter/drop/forward, dedup — one identical section, applied after the adapter's own parsing |
| Test contract | Per-block bounded probe (≤10 records), placeholder prompting, mutating-method double-confirmation, `ok:false`-as-data failures, redaction, one-test-feeds-everything, commits nothing |
| Record Envelope | `ingest_id`/`ingest_ts`/`op` metadata — travel as headers, excluded from dedup identity and schema inference |
| Entity labeling | Every write block's mandatory entity-label field |
| Failure taxonomy | Record/run/infrastructure failure classes |

"An adapter MUST NOT reimplement any row of this table independently." (`plan.md §04.2`) `kc` inherits `base` like the others, but its record-space surface is **gated entirely off** — "no transforms, no schema surface, and no Test contract in the block-testing sense." (`plan.md §03.2.1`, `§04.2`)

### 1.3 Registry flags (root / writable / terminal / hosts-transforms)

```
registry@1 = {
  http:      { root: true,  writable: true,  terminal: false, hosts_generic_transforms: true  },
  jdbc:      { root: true,  writable: true,  terminal: false, hosts_generic_transforms: true  },
  kafka:     { root: true,  writable: true,  terminal: false, hosts_generic_transforms: "read/write; absent on raw branch (R8)" },
  kafka_kc:  { root: false, writable: true,  terminal: true,  hosts_generic_transforms: true  },
  kc:        { root: false, writable: false, terminal: true,  hosts_generic_transforms: false }
}
```
(`plan.md §04.3` — `[INFERRED]` JSON rendering; the underlying constraints are verbatim from mvp.html §4/§5/§6.)

`kc` is `writable: false` because "it has no record space: it moves an existing topic's bytes untouched into a Connect sink" — but it is still a first-class registry entry because its lifecycle (Save-is-live, independent of Deploy) and attachment rule (dashed subscription off a topic node) differ structurally from every record-carrying adapter. (`plan.md §04.3`)

### 1.4 Streams, Connectors, Flows — the containment hierarchy

- **Stream**: one canvas block — an adapter, filled in. Owns its adapter type (immutable once placed), its config, a *reference* (never a copy) to one Application Service, zero-or-one Generic Transformations pipeline, an entity label (for writes), and its graph position/edges. (`plan.md §03.2.2`)
- **Connector**: a saved, named, versioned (`name@version`), immutable chain of Streams — "carries no secrets and no environment-specific detail" (chain topology, block configs, entity labels, parameter *names*, service *references* only). (`plan.md §03.2.3`)
- **Flow**: a Connector brought to life — real bound services, a real source name, a real cron (or none), a real DLQ. The unit that is Deployed/Started/Paused/Resumed/Stopped/Undeployed/Deleted. Import is copy-on-import: a Flow's Streams are independent editable copies, so retiring the source Connector "breaks nothing." (`plan.md §03.2.4`)

```
Adapter (5 hardcoded + base) → Stream (one canvas block) → Flow (source name, cron, DLQ, resolved services)
                                                                ↑ saved-as
                                                          Connector (named, versioned, shareable)
```
(`plan.md §03.2.5`)

### 1.5 Contract-driven configuration

Every adapter MUST publish a declarative **contract** (config surface, legal-position flags, transform hosting) so builder forms are generated, not hand-coded — this is explicitly what replaces the alpha's per-adapter bespoke forms (P1 build phase). (`plan.md §04.4`, `§04.1`) The contract declares modes, service type, fields (with `mode_visibility`), `hosts_generic_transforms` per mode, and the widgets (response-tree explorer, pagination Detect, etc.) that read from the one stored Test result rather than issuing their own calls.

### 1.6 The entity concept (cross-cutting on every write)

"No write without an entity, ever." (`plan.md §03.3`, `§12.2`) Every write block — regardless of adapter — MUST declare a one-word entity. For `kafka_kc` the entity is fixed during the schema ceremony's Declare step; every other write type asks directly on the block. The entity passes through the platform's one tokenizer and feeds every derived name (topic, table, DLQ) — see §3 below.

### 1.7 The future-scope catalog (adapter picker)

The picker renders the five selectable adapters plus a **greyed, unclickable, "coming later"** catalog: the NoSQL family (MongoDB, graph DB, key-value store), the File-share family (upload, S3, SMB), and non-MVP JDBC dialects. Greyed entries carry **zero runtime meaning** — they must not configure, compile, or run anything. Webhook, syslog, and CDC/Debezium are excluded even from the greyed catalog — "simply absent from the pickers — not even greyed" (ruling 46). (`plan.md §04.5`)

---

## 2. Deduplication (top priority)

Dedup is specified in `plan.md §11.5` ("Dedup mechanics") and `§11.6` ("The three dedup edge rules"), inside **§11 Generic Transformations & Routing**, and cross-referenced by rulings 12, 16, and 45(e). `concept.html` (mvp.html) states the same rules concisely at its dedup table row and at ruling rows 12/45.

### 2.1 Where dedup lives in the architecture

Dedup is **not a canvas block type** — it is one of the closed set of transforms inside the single, shared "Generic transformations" section inherited from `base` (ruling 12: "Dedup and routing are transforms within the shared Generic transformations section, not canvas block types"). (`plan.md §26.6`) It is configured **per-stream**, exactly like any other transform, but its mechanism is fixed by the platform rather than user-composed. (`plan.md §11.5`)

### 2.2 Which adapters/legs it applies to

Dedup is available wherever the Generic transformations section is hosted, which is every adapter/mode **except**:

| Block/branch | Hosts dedup? |
|---|---|
| `http` read/write/lookup | Yes |
| `jdbc` read/write/lookup | Yes |
| `kafka` read/write (JSON/CSV/XML) | Yes |
| `kafka_kc` | Yes — applied **before** Avro serialization |
| `kafka` read in **raw** mode, and everything downstream on that branch | **No — entire transforms section absent** (rule R8) |
| `kc` | **No — never.** No record space at all. |

(`plan.md §11.1`, table; `§04.2`) On a raw branch the quarantine is total and structural: "not merely disabled or hidden behind a toggle, but structurally not rendered." (`plan.md §11.1`)

### 2.3 Ordering: dedup always runs last

Within Generic Transformations, all other transforms (Extract/project, Add field, Remove field, Set-from-attribute, Rename, Coerce, Route/filter) are **user-arranged** — any order, any repetition, freely interleaved. **Dedup is the sole exception: it always runs last**, after every other transform and after any routing/filtering the user has arranged (ruling 12; `plan.md §11.2`). This is stated as load-bearing, not a UI convenience:

> "dedup fingerprints the record **exactly as it will be emitted**, so any field shaping that happens after dedup would make the fingerprint stale and let differently-shaped duplicates slip through undetected." (`plan.md §11.2`)

`[INFERRED]` — plan.md notes the MVP doesn't specify *how* the UI enforces last-position (pin visually vs. silently re-sequence), only that dedup *evaluates* last as engine behavior. (`plan.md §11.2`)

For `kafka_kc` specifically: "Dedup, per the platform-wide rule, always runs last in the transform chain, immediately before the record is handed to Avro serialization — meaning the SHA-256 fingerprint used for suppression is computed on the record shape that will actually be serialized." (`plan.md §9.8`)

Because routing can fork the stream, each branch's dedup (if configured) evaluates last **on that branch**, against **that branch's own cache** — dedup state after a fork is per-branch, not shared. (`plan.md §11.2`)

### 2.4 Mechanism — fingerprinting and storage

- **Algorithm**: SHA-256 over the record content **as it will be emitted** (i.e., after every other user-arranged transform has run). (`plan.md §11.5`)
- **Storage backend**: **Redis**.
- **Cache scope**: **one dedup cache per stream** — "dedup state is never shared across streams, flows, or branches, even if two branches happen to carry structurally identical records." `[INFERRED]` exact Redis key-namespacing is not specified beyond "one cache per stream"; implementers should namespace by `(flow, stream/block)`, mirroring the deterministic-consumer-group pattern (ruling 44), but the exact key format is left open. (`plan.md §11.5`)

### 2.5 Required and optional configuration

| Field | Required/Optional | Detail |
|---|---|---|
| **Identity fields** | Required | Which field(s) constitute the record's identity for fingerprinting |
| **Excluded fields** | Optional | User-declared volatile fields (e.g. a per-request timestamp) excluded from the fingerprint even though present in the record |
| **Time window (TTL)** | Configurable | Min **1 minute**, max **365 days**, **default 24 hours** |

**Platform metadata is excluded by default and cannot be manually included**: the Record Envelope's `ingest_id`, `ingest_ts`, and `op` fields are metadata (headers), not data, and are structurally excluded from the fingerprint — "a user does not need to (and cannot meaningfully) manually exclude these — the default exclusion is structural." (`plan.md §11.5`, `§03.4.1`) This is the direct reason the strict data/metadata envelope split exists: "if [ingest_id/ingest_ts] did, every record would be unique by construction... and dedup would never suppress anything." (`plan.md §03.4.1`)

### 2.6 Behavior and labeling

Dedup suppresses a record whose fingerprint (identity fields minus exclusions) matches one already in the stream's Redis cache within the TTL; the suppressed record is **dropped as an intentional outcome** — counted, not an error, does not proceed further down that branch. (`plan.md §11.5`)

The platform is explicit that dedup MUST be documented/understood as **duplicate suppression, not a delivery guarantee** — "best-effort narrowing of the stream based on a fingerprint cache with a bounded TTL," not exactly-once semantics, not a substitute for idempotent writes. (`plan.md §11.5`, glossary entry `dedup`)

### 2.7 Redis-down behavior — fail-stop, not pass-through

If the active Redis connection is unreachable when a dedup-enabled stream needs to fingerprint-check a record, **the record fails rather than being allowed to pass through unchecked**. Stated as "an explicit correctness-over-availability choice": "silently skipping the dedup check to keep records flowing would risk delivering duplicates downstream with no signal that dedup was bypassed." (`plan.md §11.5`) Classified as an **infrastructure failure** (fail-stop, surfaced as a runtime event) rather than a per-record retry-then-DLQ record failure — `[INFERRED]` the MVP states the fail-stop behavior directly but plan.md infers the specific three-way taxonomy label. (`plan.md §11.5`, `§18.5.3`)

### 2.8 The three dedup edge rules (explicitly load-bearing, not incidental)

**(a) Missing identity field → DLQ, never silent partial-identity dedup.** A record missing one or more configured identity fields fails to `dlq.<flow>` rather than being fingerprinted on whatever partial identity is present — "the platform explicitly forbids 'silent partial-identity dedup'" because it could either over-suppress or under-suppress unpredictably. (`plan.md §11.6(a)`)

**(b) Audited per-stream Clear dedup cache.** Each stream's dedup cache can be explicitly cleared via a **Clear dedup cache** action, scoped to one stream, **audited**, and stated as the sanctioned remedy to make previously-suppressed records eligible for reprocessing *before* TTL naturally expires (rather than disabling dedup or waiting). This is a standalone action — not part of preflight, not a side effect of any verb. (`plan.md §11.6(b)`, `§17.13`)

**(c) Config change clears the cache and warns at next deploy.** Changing identity fields, excluded fields, TTL, or the dedup on/off toggle takes effect at the stream's next deploy, **clears that stream's dedup cache as a direct consequence**, and the deploy flow **warns** the operator that previously-suppressed records may reappear. This mirrors the platform's general "warn, don't silently mutate behavior" pattern used for shape-changing Redeploy edits. (`plan.md §11.6(c)`, `§45(e)` restated at `§26.19`)

### 2.9 Interaction with normalization/inference and routing

- Dedup fingerprints the **data** portion of the Record Envelope only — never the metadata (`ingest_id`/`ingest_ts`/`op`), which live exclusively as headers. (`plan.md §03.4.1`)
- Dedup evaluates strictly **after** any routing/filter rules the user has arranged upstream of it on the same branch — since routing can fork the stream, dedup (if configured per-branch) evaluates independently, per branch, against that branch's own cache. (`plan.md §11.2`)
- Dedup interacts with schema inference only insofar as both exclude the same metadata: tombstones (null-value Kafka messages) are skipped as counted no-ops and explicitly **never write a dedup marker and never enter schema inference** — uniform, deterministic tombstone handling that "MUST NOT vary by downstream block type." (`plan.md §08.10`, `§18.9`)
- Test/ceremony sample runs are explicitly non-committing with respect to dedup: "a ceremony run MUST NOT... write any dedup fingerprint marker" and block Tests are likewise guaranteed to leave no dedup markers (ruling 42(a)). (`plan.md §14.5`, `§26.4`)

### 2.10 How dedup should be represented in NiFi / ordering restrictions

plan.md's compilation section does not name a specific NiFi processor for dedup, but establishes the governing constraints the compiler must satisfy:

- Dedup is compiled as the terminal step of a stream's transform chain, immediately preceding the block's write/emit behavior (or, for `kafka_kc`, immediately preceding Avro serialization). (`plan.md §11.1`, `§9.8`)
- The compiler's Redis access for dedup goes through the **pipeline controller-service family** (§23.7) like any other produce/consume-time credential — never the topic-admin controller, which is reserved for topic management operations only.
- No processor-level ordering override is permitted: the compiler must not allow a compiled graph where dedup precedes a user-arranged transform, regardless of the canvas's visual arrangement (§11.2's normative "always last" is a compiler-enforced invariant, not merely a UI suggestion). `[INFERRED]` — the MVP states the outcome, not the specific NiFi processor topology; §23 (Compilation & Runtime Mapping) is silent on a dedup-specific processor type.

### 2.11 Summary — dedup invariants restated for enforceability

1. Configured inside Generic Transformations, per-stream; never a standalone canvas block.
2. Not hosted on `kc` (no record space) or on any block downstream of a raw-mode `kafka` read (R8 quarantine).
3. Always evaluates last in the transform chain — after every field transform and every routing/filter rule on that branch, and (for `kafka_kc`) before Avro serialization.
4. SHA-256 fingerprint, computed over data fields only (post-transform shape), stored in Redis, one cache per stream.
5. Config: identity fields (required), excluded fields (optional; platform metadata excluded by default and structurally, not by user action), TTL (1 min – 365 days, default 24h).
6. Missing identity field → DLQ (never partial-identity dedup). Redis down → fail-stop (records fail, never silently pass). Config change → cache clears, deploy warns. Clear-dedup-cache is a standalone audited per-stream action.
7. Labeled "duplicate suppression, not a delivery guarantee" — never described as exactly-once.
8. A dedup suppression is a counted, intentional outcome — never classified as, or reported alongside, a failure (ruling 41/42, `plan.md §18.9`).

---

## 3. Normalization

The word "normalization" is used in the source documents in two distinct, narrower senses than "general data normalization" — engineers should not conflate them:

### 3.1 Naming normalization — the one tokenizer

"The single normalization rule every generated name (topic, table, DLQ, custom override) passes through before use: **trim → lowercase → any character that is not a letter or digit becomes `_`**. There is exactly one tokenizer in the platform; no adapter or naming path MUST implement a divergent normalization rule." (`plan.md` glossary, `tokenizer`; full spec at `§12.3`)

```
tokenize(s) = replace_non_alnum_with_underscore(lowercase(trim(s)))
```

Every name-bearing input — entity labels, flow/source names, fork and routing-rule labels used as variant tokens, and custom topic names — MUST pass through this single function before entering a derived name template. (`plan.md §12.3`) `[INFERRED]` The tokenizer does not collapse consecutive underscores (`"Asset - Active"` → `asset___active`, three separate substitutions) — plan.md explicitly warns engineers not to silently collapse repeats absent a future ruling. (`plan.md §12.3`)

### 3.2 Avro-schema normalization — the ceremony editor

Distinct from the tokenizer: at the schema ceremony's **Review** step (§14.7), "**Normalization**, applied on every save regardless of which tab was used":

- Nullable unions MUST be ordered **null-first**, with `default: null` attached.
- The schema **root MUST be a record** — a bare scalar or map at the root is not a legal ceremony output.

(`plan.md §14.7`) This normalization is applied automatically to whatever the structured editor or the raw Avro JSON tab produces, so the two tabs stay convergent on one underlying, canonically-shaped buffer.

### 3.3 Ordering relative to dedup and routing

Normalization in the naming sense (§3.1 above) is orthogonal to dedup/routing — it operates on *names* (entity labels, fork/route branch labels, flow names), not on record payloads, so it has no direct sequencing relationship with the transform chain. It is applied once, at the point a name-bearing field is captured (entity declaration, fork naming, flow creation), and again idempotently on every recomputation before Deploy freezes it. (`plan.md §12.9`)

Avro-schema normalization (§3.2) happens once per ceremony Review-step save, entirely outside the record-processing path — it shapes the *schema*, not individual records, and precedes (governs) what `kafka_kc`'s post-dedup Avro serialization step will validate against. It has no direct interaction with the per-record dedup/routing pipeline other than this: the shape it fixes is exactly the shape a record must already be coerced into (via the `coerce` transform) by the time it reaches dedup and then serialization on a `kafka_kc` branch. (`plan.md §9.8`, `§11.3` coerce transform)

There is **no third, general "data normalization" transform** in the closed six-transform set (§11.3) — shaping a record's *data* fields is accomplished only through Extract/project, Add field, Remove field, Set-from-attribute, Rename, and Coerce; "normalization" is never itself a named record-level transform in these documents.

---

## 4. Routing

Routing is specified in `plan.md §11.4` ("Routing vs. plain fork") and `§11.4.1` ("Routing/filter patterns"), governed by ruling 12 (routing is an in-stream transform, not a block type) and ruling 46-adjacent placement rule R4.

### 4.1 The routing conditions model

Routing (also called **Route/filter/drop/forward**) is an **ordered list of rules**, each tested against a record **in the order arranged**, with **first-match-wins** semantics: "the record follows the **first** rule whose condition it satisfies — it does not fan out to every matching rule, and it is not re-evaluated against later rules once a match is found." (`plan.md §11.4`) This is the defining contrast with a **plain fork** (unconditional, every branch gets a full copy).

Each routing rule that creates a branch is itself a **user-named branch**: "routing rules sprout user-named forks" and "rule names automatically name the branches they create." A name is required on every branch-producing rule (only unnamed *plain* forks fall back to an automatic `fork-N` default). (`plan.md §11.4`)

### 4.2 How multiple conditions behave

Four named usage patterns exist over the same first-match-wins engine (not separate mechanisms):

| Pattern | Behavior |
|---|---|
| **Split-by-value** | Ordered rules, each testing a field, each producing its own named branch; record goes to the first matching branch |
| **Pure filter (no branches)** | Single rule (or ordered set) deciding pass/drop only; unmatched records continue unchanged, matched records drop |
| **Stacked filters** | Multiple filter rules in sequence, narrowing funnel, no branch tree |
| **Whitelist style** | "Keep only records matching X" — inverse framing of drop-style filter, same engine |

(`plan.md §11.4.1`)

**Dropped records are intentional outcomes**: "counted, never treated as an error or a failure — it does not retry, does not go to the DLQ, and does not fail the run," mirroring the platform's "handled includes intentional outcomes" stance (ruling 41/42). (`plan.md §11.4.1`)

Routing/filter and the five field transforms may be freely interleaved per the user-arranged ordering rule (§11.2): a routing rule may test a field an earlier Add-field/Coerce produced, and a branch created by routing may carry its own subsequent transforms and its own dedup, arranged independently of any sibling branch. (`plan.md §11.4.1`)

**Explicit exclusions**: routing/forking exist only to shape/redirect the single record-at-a-time stream — never for joins, enrichment-by-merging-other-datasets, aggregation, windowing, or multi-record merges (that is Refinement-platform turf, out of scope). "Forks fork; they never merge" (R4) — no re-join/merge node type exists anywhere in the system; a union of two branches at one destination requires placing the write **above** the fork point. (`plan.md §11.4.1`, `§05.4 R4`)

### 4.3 Translation into NiFi processors

`plan.md §23` (Compilation & Runtime Mapping) does not name a specific NiFi processor type for routing rules — it states only the general compilation posture: NiFi is used "for every `http`, `jdbc`, and `kafka` stream, the generic-transformations chain, routing/dedup, and the wiring between them — a **process group** per flow." (`plan.md §23.1`) The runtime-scope map (§23.3) attributes whatever processors the compiler emits for a block's transform chain (including routing rules) back to that block, so ops-view metrics can be shown per-block rather than as raw NiFi component names — but the specific processor type(s) used to implement first-match-wins routing are not named in either source document. This is a gap the report flags explicitly in Open Questions (§7 below).

---

## 5. NiFi Representation

Specified primarily in `plan.md §23` ("Compilation & Runtime Mapping"), which is explicitly flagged as **"engineering design inferred from MVP behavior, not a restatement of an explicit MVP subsystem"** — the MVP establishes *observable guarantees* (engines invisible, per-block metrics, deterministic exports, self-healing schema registration, drift not silent merge) but does not specify compiler internals. (`plan.md §23` preamble)

### 5.1 Flow → NiFi mapping

A **Flow** compiles to one or both of two live engines at Deploy:

- **NiFi** — for every `http`, `jdbc`, and `kafka` stream, the generic-transformations chain, routing/dedup, and the wiring between them, as **one process group per flow**. `[INFERRED structural mapping]` — MVP states engines are invisible but doesn't literally name "process group" as the unit; this is the natural NiFi mapping, used consistently with the ops view's "generated components grouped under owning block." (`plan.md §23.1`)
- **Kafka Connect** — for every `kafka_kc` block (governed sink + owned topic) and every `kc` block (subscription sink), **one Connect connector per block**. (`plan.md §23.1`, ruling 39)

A flow whose only content is a topic node with `kc` sinks compiles to **Connect connectors only — no NiFi process group, no cron** ("topic-rooted kc-only flows have no cron and map verbs to connector operations," ruling 39). All other flows compile to a NiFi process group, optionally paired with Connect connectors. The compiler decides which artifacts to emit **purely from the flow's block graph — never from flags or environment** — so the same connector definition compiled twice against the same bound services produces the same artifact set (determinism requirement). (`plan.md §23.1`)

### 5.2 Naming/tracking conventions

`[INFERRED]` — the MVP fixes data-plane naming (topics/tables/DLQ, §12) but says nothing about engine-artifact names. plan.md's inferred templates:

```
NiFi process group name      : <flowSourceName>
NiFi controller service name : <serviceType>__<applicationServiceId>__rev<N>
Kafka Connect connector name : <flowSourceName>.<streamId>.<sinkKind>   # sinkKind = kc | kafka_kc
Consumer group id            : (flow, block)          for kafka reads       [explicit, ruling 44]
                                (flow, stream, sink)   for Connect          [explicit, ruling 44]
```
(`plan.md §23.2`) All names pass the single tokenizer before use as engine identifiers. The consumer-group cardinality rule — `(flow, block)` for reads, `(flow, stream, sink)` for Connect — **is** explicitly stated by the MVP (ruling 44), not inferred; only the literal string templates around it are inferred.

### 5.3 The runtime-scope map (per-block metric attribution)

This is an **explicit MVP requirement**, only its data shape is inferred: "the compiler emits a runtime-scope map (which block owns which generated components) → the ops view attributes NiFi's per-component numbers to blocks" (ruling 33). (`plan.md §23.3`) The compiler emits exactly one map per successful compile, keyed by canvas block id:

```json
{
  "flow_id": "f_9c21...",
  "compiled_at": "2026-08-06T14:02:11Z",
  "artifact_digest": "sha256:…",
  "blocks": {
    "blk_http_asset_details": {
      "adapter": "http", "engine": "nifi",
      "components": [
        {"type": "processor", "nifi_id": "b7e1…", "processor_type": "InvokeHTTP"},
        {"type": "processor", "nifi_id": "4a02…", "processor_type": "EvaluateJsonPath"},
        {"type": "controller_service", "nifi_id": "0af9…", "ref": "svc_http_1__rev3"}
      ]
    },
    "blk_kafka_kc_asset_publish": {
      "adapter": "kafka_kc", "engine": "connect",
      "components": [
        {"type": "connector", "connect_name": "rapid7.asset_publish.kafka_kc"},
        {"type": "topic", "kafka_topic": "bronze.rapid7.asset__raw"}
      ]
    }
  }
}
```
(`plan.md §23.3`) The ops view MUST read this map — **never NiFi's flow definition directly** — and MUST treat a block with zero mapped components (e.g. an as-yet-undeployed `kc` block) as legitimately empty, not an error.

### 5.4 Deterministic, secret-free, environment-free compilation

Two MVP guarantees force the parameterization discipline: (1) export "lands anywhere" — "compiled artifacts contain no literal environment values — brokers/URLs/credentials all resolve through parameters"; (2) connectors carry no secrets/environment ids. (`plan.md §23.4`) The compiler routes every environment-dependent/secret value through **two parameter contexts per flow** `[INFERRED cardinality]`:

- **Global parameter context** — one platform-wide instance: global Variables + platform connection endpoints/security config, reaching compiled components as secret-flagged parameters (ruling 29 — Platform Kafka's full security config, not endpoint-only).
- **Per-flow parameter context** — one per flow: that flow's own Variables (overriding same-named globals) + every Application-Service-derived credential reference, expressed as parameter references, never literals.

Compilation MUST be a **pure function** of `(connector definition, bound service references, bound service revisions, global parameter set, flow parameter set)` — recompiling unchanged inputs reproduces byte-identical artifacts and the same digest. Wall-clock timestamps or regenerated random UUIDs embedded in processor properties are disallowed inside the compiled artifact body. (`plan.md §23.4`)

### 5.5 Artifact digests and drift

The compiler's own digest is the **desired** state; a runtime fingerprint probe is the **observed** state; drift is any mismatch. Digest = `sha256` over the compiled artifact body **after parameter substitution is stripped back to references** (digests must not depend on secret values, or a credential rotation alone would falsely read as drift). Drift resolution table:

| Desired digest | Observed state | Verdict |
|---|---|---|
| present | fingerprint matches | in sync |
| present | fingerprint differs | drift — "edited out-of-band" |
| present | resource missing, same engine instance | drift — "really deleted" |
| present | resource missing, different engine instance | drift — "deployed elsewhere" |
| present | engine unreachable | drift — "unknown/unreachable" |

Computing this table is side-effect-free ("a read never mutates anything"); repair is a separate, explicit, audited action. (`plan.md §23.5`)

### 5.6 Controller services per Application-Service revision

The compiler MUST NOT compile Application-Service credentials directly into processors. For every distinct `(applicationServiceId, revisionNumber)` a deployed flow references, a corresponding NiFi **controller service** must exist (created if absent, reused if a prior flow already compiled that revision), bound by reference. Two flows pinned to different revisions compile to **two distinct controller services**, never one mutated in place. Redeploy is the only point a flow's processors rebind to a newer controller service. (`plan.md §23.6`)

### 5.7 Split-credential controllers (security-relevant NiFi detail)

Two disjoint controller-service families for platform Kafka access:

- **Pipeline controller service(s)** — bound into every processor/connector that produces/consumes records; credentials scoped to produce/consume/describe ACLs only.
- **Topic-admin controller** — a single backend-owned credential, **never referenced by any user-authored processor or connector config**, used exclusively for topic reservation/creation, DLQ topic creation, owned-topic emptying/deletion, and Clear Topics.

"A compiled artifact that somehow referenced the topic-admin credential is a compiler defect... and deploy preflight SHOULD reject it." (`plan.md §23.7`)

### 5.8 Kafka Connect connector compilation

- **`kafka_kc`** compiles only after schema approval, only as part of flow **Deploy**, created **stopped**; config includes topic name, Avro converter bound to the registered subject, pipeline-family security config, and (for recognized lakehouse sinks) the four locked keys.
- **`kc`** compiles independently of flow Deploy, **on Save** ("Save is live"). If its target topic doesn't yet exist, config is recorded **pending** and materializes automatically when the owning flow's Deploy creates the topic. A rejected Save leaves the **last-applied** config authoritative.

Both connector kinds enter the same flow-scoped runtime-scope map. (`plan.md §23.8`)

### 5.9 Compile-triggering events

| Verb | Compilation action |
|---|---|
| Deploy | Full compile: NiFi PG (if any) built stopped, Connect connectors for `kafka_kc`/pending `kc` created stopped, registry re-checked/re-registered, runtime-scope map + digests written |
| Redeploy | Recompile after stop+clear; shape-change prompt offered before new artifact goes live |
| `kc` Save | Immediate, independent connector compile/update — not gated by any flow verb |
| Start/Pause/Resume/Stop/Undeploy/Delete | No recompilation |

Every compile MUST be atomic w.r.t. the runtime-scope map and digest — a partial-failure compile must never publish a stale/partial map. (`plan.md §23.9`)

### 5.10 Deployment model / hosting

Per `work.md`'s scope guards: "Infrastructure hosting — NiFi, Kafka, Kafka Connect, Apicurio, Redis, and APISIX are provided services we consume, operated by the platform team. The only stack we host is the app compose: backend + frontend + Mongo." (`work.md`, Scope guards) No task stands up infrastructure; environment tasks only configure connections against provided endpoints. This is corroborated by `plan.md §16` (Platform Connections), which models NiFi/Kafka/Apicurio/Kafka Connect/Redis/APISIX as six centrally-managed, admin-configured **connections** (one active per type, enforced as a DB constraint) rather than infrastructure the application provisions.

---

## 6. The Kafka + Kafka Connect Terminal Rule

This is one of the most heavily cross-referenced rules in the specification (placement law R3/R5, ruling 5, glossary entries for `kafka_kc` and `kc`).

### 6.1 Exact spec language

From `plan.md §05.4`, rule **R3 — Writes are not dead ends (except one)**:

> "The **two Connect-backed adapters — `kafka_kc` and `kc` — are terminal**: nothing is ever placed after either, for the same underlying reason (a Connect sink hands records to an external system and returns nothing to the flow)."

From `concept.html` (mvp.html), the corresponding rule box, quoted verbatim:

> "**R3 — Writes are not dead ends, except the two Connect-backed ones.** The chain may continue after an `http` write, `jdbc` write, or `kafka` write — including **http write after http write**... **Both Connect-backed adapters — `kafka_kc` and `kc` — are terminal**: nothing is ever placed after either. A branch reaching one of them ends there; sibling branches keep going. (They are terminal for the same underlying reason: a Connect sink hands records to an external system and returns nothing to the flow.)"

Decision ledger, ruling 5 (`plan.md §26.1`), restated normatively:

> "**5 — Both Connect-backed adapters are terminal.** Nothing is ever placed after `kafka_kc` **or** `kc` (a Connect sink hands records to an external system and returns nothing to the flow). `kafka_kc` is *additionally* never the root — nothing would fill its topic, so targeting an existing topic MUST route the user to `kc` instead."

Glossary entry `kafka_kc` (`plan.md §27`):

> "`kafka_kc` MUST NOT ever be root and is **always terminal** — nothing may follow it in the chain."

Glossary entry `kc`:

> "`kc` MUST NOT sit inside the record chain and nothing MUST ever follow it — it is a subscription, not a processing step."

### 6.2 Enforcement mechanism (structural, not just refused-at-save)

- For `kafka_kc`: the `+` menu MUST NOT be rendered on the block's output at all — "there is no output to attach to — its downstream is the sealed topic node." (`plan.md §05.4 R3`)
- For `kc`: terminality is expressed through R5 — it is a subscription off a topic node, never a chain step, so it has no "output" in the record-chain sense at all.
- The placement summary table (`plan.md §05.7`) states this as an enforceable row: "After `kafka_kc` → nothing — terminal, always (R3) → any block or fork [illegal]" and "After `kc` → nothing — terminal, always (R3); it is a subscription, not a step (R5) → any block or fork [illegal]."
- `kafka_kc`'s own materialized topic node is additionally **sealed**: visible everywhere (canvas/viewer/ops view) but **never attachable** — not even by a different flow — reinforcing terminality at the topic level, not just the block level (§05.6, §9.5). This is stricter than the general topic-node attachability rule (ruling 6) and is explicitly called out as an *exception* to it (ruling 39: "stricter than, and an exception to, ruling 6").

### 6.3 Per-branch scope

Terminality is per-branch, not per-flow: "A branch that reaches `kafka_kc` ends at that block; sibling branches from the same fork are unaffected and continue normally." (`plan.md §9.2`) The canvas must render this per-branch — one branch of a fork can end at `kafka_kc` while a sibling branch continues through further writes.

### 6.4 Raw-branch and root exclusions

- `kafka_kc` MUST additionally be refused on a raw-mode `kafka` read branch (R8) — "a governed schema cannot meaningfully be inferred over or enforced against bytes the platform has declared opaque." (`plan.md §9.2`)
- `kafka_kc` MUST NEVER be offered as a root — "its topic does not exist until the block itself creates it during ceremony/deploy — there is nothing pre-existing for it to read." If the intent is to consume an existing topic, `kc` (attached to an adopted topic node) is the correct block, not `kafka_kc`. (`plan.md §05.4 R2`, `§9.2`)

### 6.5 Why: the shared rationale

Both terminal for the identical underlying reason, stated consistently across every citation: **a Connect sink hands records to an external system and returns nothing to the flow.** `kafka_kc` and `kc` differ only in *how* they are terminal — "`kafka_kc` forecloses continuation of a live record chain, whereas `kc` was never part of a chain to begin with — it is a leaf subscription off a topic node." (`plan.md §10.2`)

---

## 7. Flow Lifecycle

Specified in `plan.md §17` ("Flow Lifecycle, Verbs & Scheduling") and `§18` ("Reliability, DLQ & the Failure Taxonomy").

### 7.1 Two governing identity invariants

1. **Deploy is the only schema gate.** No verb other than Deploy may perform schema-registry validation or block on approval state; Start/Resume run whatever was compiled at the last successful Deploy without re-checking. **Redeploy is itself a Deploy** and re-runs the gate. (`plan.md §17.1`, ruling 24: "Deploy is the only lifecycle verb that re-validates schema state — Start MUST NOT re-check.")
2. **Names freeze at Deploy.** Before first Deploy, derived names recompute freely; after Deploy, names are frozen for the flow's lifetime — a post-deploy rename is an identity change requiring a new Deploy under a new name. (`plan.md §17.1`, `§12.9`)

### 7.2 State machine

```
draft → [Deploy] → deployed → [Start] → running → [Pause] → paused → [Resume] → running
                       ↑                    ↓ [Stop]              ↓ [Stop]
                       |                 stopped                stopped
                  [Redeploy]                ↓ [Clear Queues]
              stopped_cleared ← [Stop & Clear]
any post-Deploy state → [Undeploy] → draft
any state → [Delete] → (flow removed)
```
(`[INFERRED]` enum rendering of prose transitions; `plan.md §17.2`)

Flow definition (blocks, configs, entity labels, cron) is editable only in `draft`. Once deployed and not currently `stopped`/`stopped_cleared`, definition edits are refused, citing the Redeploy rule. **Sole exception**: `kc`'s Save-is-live sink config — editable at any time regardless of flow status; this exception covers *only* the sink-config surface, not the `kc` block's entity label, canvas position, or topic binding. (`plan.md §17.2`, `§17.9`)

### 7.3 Create / Save

Not separately verbed in the lifecycle table — a flow exists in `draft` from creation and is freely editable there; "Save" is implicit in builder edits while `draft`/`stopped_cleared`.

### 7.4 Deploy

Preconditions: `draft` (first deploy) or `stopped_cleared` (redeploy). Effect — builds **stopped**, no data moves as a side effect:
1. Runs full preflight (§17.13) — any failure blocks before any artifact is touched, all-or-nothing.
2. Confirms every `kafka_kc` block has an approved schema (hard precondition, not a warning).
3. Re-checks every referenced approved schema's registry presence; re-registers from local copy if missing (registry-repair self-heal); registration failure blocks Deploy.
4. Compiles every block into engine artifacts (NiFi PG + controller services; Connect connector configs for `kc`/`kafka_kc`, created but not running).
5. Reserves every derived name per the naming walk; collision blocks with the specific reason.
6. Freezes all derived names.
7. Leaves the runtime **stopped**: no cron firing, no NiFi processors running, Connect connectors created but not started.
(`plan.md §17.3`)

### 7.5 Start

Preconditions: `deployed` or post-Start/Stop-cycle `stopped`/`stopped_cleared`; refused on never-deployed `draft`. Registers the cron trigger, starts NiFi processors, resumes `kafka_kc` connectors. For a topic-rooted kc-only flow with no cron, Start is equivalent to ensuring `kc` connectors are running. Does **not** re-run schema validation or re-derive names. (`plan.md §17.3.1`)

### 7.6 Pause / Resume

**Pause** is "capture-and-queue," not a full halt: trigger and root acquisition keep running; every downstream block is held, queuing bounded by NiFi backpressure. `kafka_kc` connectors pause (native Connect pause). **Resume misses nothing** — every record acquired during Pause is queued and drains. Critical rule: "**the trigger's timer does not re-fire for missed occurrences**" — the cron simply continues from its natural schedule; nothing is "caught up." **Stop is the verb that halts the trigger too** — this is the load-bearing Pause/Stop distinction. (`plan.md §17.4`)

### 7.7 Stop / Stop & Clear / Clear Queues

Stop (from `running`/`paused`) halts completely: cron deregistered, NiFi processors stopped, Connect connectors stopped (tasks released, offsets kept — not deleted). **Queues retained by default** — an explicit fix vs. the alpha, which silently dropped queued data. **Stop & Clear** is Stop plus an explicit audited queue-purge, offered as a distinct action, never Stop's silent default. **Standalone Clear Queues** — available from `stopped`, audited (queue depths before/after), operates on **NiFi FlowFiles only**, never conflated with the ops-view's Clear Topics (Kafka records). (`plan.md §17.5`)

### 7.8 Redeploy

Requires `stopped_cleared`. Re-runs the full Deploy sequence against the current edited definition. If output shape changed (transform edits altering emitted fields, or a re-approved `kafka_kc` schema), surfaces a warning dialog: states the topic will mix old/new-shape messages, offers to clear the owned topic first — **default ON for `kafka_kc`-owned topics only**, never offered for adopted topics — and **respects a decline** (old-shape messages remain; a mismatched message later fails visibly to DLQ, never silently coerced). Redeploy never alters frozen names. (`plan.md §17.6`)

### 7.9 Undeploy

Valid from any post-Deploy state; does not require stopped-first (`[INFERRED]` — Undeploy is itself the halting+teardown action). Effect: deletes the NiFi process group + controller services; deletes every Connect connector (`kafka_kc` and `kc`) owned by the flow; **empties** (not deletes) the flow's generated topics (low-watermark advance; topic/config/consumer-groups survive) — **never touches adopted topics**; clears dedup caches; resets read positions/bookmarks (re-asked at next Deploy+Start). **Untouched**: registered schemas, adopted topics, already-delivered destination data, application services. Flow definition survives — returns to `draft`, freely editable/redeployable. Per ruling 18 (refined by 45): the flow's **DLQ topic is NOT emptied by Undeploy** — only Delete removes it. Cross-flow topic readers must be warned by name before Undeploy proceeds (informed consent, not a hard block). (`plan.md §17.7`, `§26.16`)

### 7.10 Delete

Same reachability as Undeploy. Performs everything Undeploy does, plus: removes the flow record; **deletes** (not empties) generated topics with **ownership proof**; **deletes the DLQ topic** (`dlq.<flow>`); leaves registry schema entries in place; leaves adopted topics and application services completely untouched. Branch/block-level delete is narrower — only that branch's solely-owned resources (its topic if unshared, its dedup cache prefix) are removed; siblings unaffected. (`plan.md §17.8`)

### 7.11 The cron trigger contract

Standard 5-field cron, **always UTC**, no seconds field. Friendly presets are UI convenience only; persisted form is always the 5-field expression. Lives on the flow's **first runnable block** (root, or first `kafka` read in a topic-rooted flow — a topic node cannot run itself). A mid-chain `kafka` read on an internally materialized topic has **no schedule of its own** — runs as a continuous consumer while `running`. A topic-rooted kc-only flow has **no cron at all**. Validated at save with a **next-3-occurrences preview**. Compiler translates the canonical expression into the target engine's dialect at Deploy — explicitly to prevent the alpha's bug where a cron was saved but never actually wired to the runtime trigger. **Overlap policy**: an occurrence firing while the previous run is still executing is **skipped and counted** — never queued, never concurrent, no exceptions. (`plan.md §17.12`)

### 7.12 Deploy preflight — the full checklist (all-or-nothing, every failure surfaced)

| # | Check |
|---|---|
| 1 | Required connections reachable (only those the flow's blocks actually use) |
| 2 | Schema approvals present + registry repair attempted automatically |
| 3 | Topic reservations valid (no silent suffix on collision) |
| 4 | Redis reachable (only if dedup or jdbc bookmarks are used) |
| 5 | Cron valid (trivially passes for kc-only flows) |
| 6 | Gateway routes resolvable (only if `proxy: on` used) |
| 7 | Connect worker reachable + plugin installed (per `kc`/`kafka_kc` block) |
| 8 | DLQ limits (110 MiB) supported |
| 9 | No unresolved ask-at-runtime bindings |

(`plan.md §17.13`) Any single failure blocks the entire Deploy with the specific reason, before any runtime artifact is touched. `[INFERRED]` preflight SHOULD report every failing check found, not just the first.

### 7.13 DLQ

One DLQ per flow, `dlq.<flow>` (source name), **never shared across flows**, **7-day retention**, **110 MiB ceiling** (deploy-preflight-verified). Created at Deploy; **not** deleted by Undeploy (only emptied topics are the data topics); **deleted only by Delete**, ownership-proven. Entry shape: original bytes (unmodified) as value; headers naming the failing block and error class; key preserved from source when present. **No automated replay** — inspect/download only. If the DLQ write itself fails, the record **parks in the flow's own queue** ("one poison record can only jam its own flow"); Clear Queues is the stated repair (a deliberate data-loss action). Oversized (>110 MiB) failures follow the same parking path directly. (`plan.md §18.3`, `§18.4`, `§18.6`, `§18.7`)

### 7.14 The failure taxonomy (governs all of the above)

Exactly three classes:

- **Record failure** (parse/coerce/lookup/single-record-delivery) — retryable → 3 capped retries then DLQ; permanent (schema violation, unresolvable mapping) → immediate DLQ, zero retries. Reaching DLQ successfully = **handled**, run still succeeds, positions still advance.
- **Run failure** (no record yet exists — session-token login failure, root request error including "HTTP 500 on page 3 of pagination," connection refused) — marked in run history by class; **no DLQ record fabricated**; positions/bookmarks untouched (pre-ack); next cron starts fresh.
- **Infrastructure failure** (Redis down, NiFi down, Connect worker unreachable `[INFERRED extension]`) — fail-stop, surfaced as a runtime event, never routed around.

Intentional outcomes (filtered drops, dedup suppressions, empty runs, tombstones) are **counted, never classified as failures**. Checkpoints (jdbc bookmarks, Kafka positions) advance **only post-ack**; handled-includes-DLQ is what stops one poison record from re-reading a batch forever; only an unhandleable failure (the DLQ write itself failing) holds a bookmark back. Connect sinks (`kc`/`kafka_kc`) **keep their own error handling** — the per-flow DLQ is never injected into Connect's task-level error path. (`plan.md §18.5`, `§18.8`–`§18.10`, ruling 41/42)

### 7.15 Metrics / test behavior (brief cross-reference)

Ops-view panels are lazy/visibility-gated with idle backoff (metrics 10s Running / 30s otherwise; messages+DLQ 10s while visible); per-block metrics are attributed via the runtime-scope map (§5.3 above), labeled honestly as FlowFile counts, "unavailable" rather than a fake zero on failure. Block **Test** (§13 of plan.md, not itself part of the lifecycle verb set) is a bounded (≤10 record), non-committing, per-block probe that feeds every downstream field picker and commits nothing — no offsets, bookmarks, or dedup markers, ever, even for a mutating method (which additionally requires double-confirmation: UI prompt *and* independent backend refusal). (`plan.md §19`, `§13`)

---

## 8. Open Questions / Ambiguities

The following points are either explicitly flagged `OPEN:` in plan.md (§28.3, reproduced/organized here by topic) or identified independently while researching this report. Items marked "independently identified" are this report's own observations, not MVP-stated OPEN items.

### 8.1 Dedup

- No numeric spec exists for the Redis key-namespacing scheme that guarantees "one cache per stream" isolation — `[INFERRED]` only that it should follow the `(flow, stream/block)` pattern used elsewhere. (`plan.md §11.5`)
- Whether the classification of Redis-down-during-dedup as an "infrastructure failure" (vs. some other bucket) is the MVP's own vocabulary or plan.md's inference is not fully settled — the MVP states fail-stop behavior directly but doesn't use the phrase "infrastructure failure" in that exact sentence. (`plan.md §11.5`)
- **Independently identified**: neither source document names a specific NiFi processor (or processor pattern) used to implement dedup at compile time — §23 (Compilation & Runtime Mapping) is silent on this specific mapping, unlike its treatment of controller services and connector compilation.

### 8.2 Adapter model

- No migration/compatibility policy is specified for `contract_version` bumps to `base` itself (e.g. what happens to flows deployed against `base@1` if a future `base@2` changes the generic-transformations contract). (`plan.md §04`, OPEN)
- `kafka_kc`: exact plugin-classification mechanism for "recognized lakehouse-class" beyond Iceberg (allowlist config? manifest flag?) is unspecified. (`plan.md §9.11`, OPEN)
- `kafka_kc`: whether `tasks.max` or other performance-tuning sink keys are user-configurable within the locked-key envelope, or covered by "entire sink configuration" freedom, is unspecified. (`plan.md §9.11`, OPEN)
- `kc`: whether the entity label is surfaced anywhere in the generated Connect connector's own config (e.g. connector-name component, custom header) is unstated. (`plan.md §10.3`, OPEN)
- `kc`: whether partial-merge vs. full-replace is the semantics when a `.json` upload conflicts with existing key/value editor state is unstated; plan.md assumes full replace as the safer default `[INFERRED]`. (`plan.md §10.4`)
- `kc`: whether an audited-seek-equivalent mechanism exists for Connect-managed offsets, or whether changing initial position requires Delete+recreate, is unstated. (`plan.md §10.5`, OPEN)

### 8.3 Routing

- **Independently identified**: neither source document names the specific NiFi processor type(s) used to compile first-match-wins routing rules (e.g., RouteOnAttribute or an equivalent) — §23 states only that routing is compiled "into the wiring between" NiFi components generically, without naming a processor pattern the way it names `InvokeHTTP`/`EvaluateJsonPath` illustratively for http.

### 8.4 Normalization / naming

- Whether the per-cluster name-reservation table is exposed as its own admin/browser screen distinct from the Destinations panel and Schemas screen is unstated — plan.md does not invent one. (`plan.md §12.11`, OPEN)
- Provenance-label text/display when a manually-authored schema later gains attached samples without a full ceremony re-run is unstated (affects the "manually authored — not sample-validated" flag's exact hybrid-case behavior). (`plan.md §14.4`, `§14.8`, OPEN — appears twice)
- Exact upload size/file-count caps for schema-ceremony sample files, and the exact "sample set stability" predicate / time budget for the live-run polling loop, are unstated. (`plan.md §14.5`, `§14.6`, OPEN)

### 8.5 NiFi representation / compilation

- Whether a controller service backing an Application-Service revision no longer referenced by any deployed flow is garbage-collected automatically or left for an admin sweep is unspecified — no GC policy is invented. (`plan.md §23.6`, OPEN)
- The exact on-the-wire/on-disk schema (field names/types) of the parameter-context compilation artifact is unspecified beyond "it exists and resolves brokers/URLs/credentials at runtime." (`plan.md §27` glossary, OPEN)
- **Independently identified**: the entirety of §23 (Compilation & Runtime Mapping) — process-group naming templates, controller-service naming templates, the runtime-scope map's JSON shape, and the digest/fingerprint mechanism — is explicitly self-flagged by plan.md as engineering inference rather than MVP-stated fact. Any implementation should treat this section as a reasonable starting design, not a literal spec to match byte-for-byte, and should expect concept.html/mvp.html to be silent on these specifics (confirmed by this report's grep of concept.html, which discusses NiFi/Connect only at the behavioral/guarantee level, never at the processor-graph level).

### 8.6 Flow lifecycle

- Whether bulk actions extend beyond Start/Stop/Deploy to Pause/Resume/Undeploy/Delete is unstated — the MVP names only "start/stop/deploy many flows." (`plan.md §17.10`, OPEN)
- Whether an oversized-record poison event (>110 MiB) emits an audit/runtime event distinctly labeled from a generic DLQ-write-failure parking event is unstated. (`plan.md §18.7`, OPEN)
- Ops-view panel latency SLAs (concrete milliseconds for "lazy, on-demand" / staleness windows) are not given anywhere. (`plan.md §25.14`-adjacent, OPEN)
- The Connect panel's own refresh cadence (same 10s-visible rule as messages/DLQ, or the metrics panel's running/not-running split) is unspecified. (`plan.md §19`, OPEN)

### 8.7 Document-relationship note (not a spec ambiguity, but relevant to future research)

`concept.html` and the `mvp.html` cited throughout `plan.md` appear to be the same document (confirmed by concept.html's own header: "Where this disagrees with `plan.md` v6, the decision ledger (§16) wins" — the exact relationship plan.md describes for mvp.html). Future analysis tasks against this document set can treat `concept.html` as the authoritative, more concise MVP source, and `plan.md` as the elaborated engineering specification built on top of it — with `plan.md`'s own `[FROM:]`/`[INFERRED]`/`OPEN:` markers as the reliable way to distinguish confirmed MVP requirements from engineering completions.
