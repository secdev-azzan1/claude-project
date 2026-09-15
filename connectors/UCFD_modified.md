# Unified Connector Feature — Canonical Product Design

## 1. Purpose

The Connector feature turns a fully built source integration into a **plug-and-play product**.

Instead of rebuilding the same ingestion and refinement pipelines for every customer, the platform keeps one reusable **Blueprint** and allows users to create many configured **Connector instances** from it.

For example:

- **Source Type:** Rapid7 InsightVM
- **Blueprint:** Rapid7 InsightVM Blueprint v1
- **Connector:** Rapid7 — Securado
- **Connector:** Rapid7 — Asyad
- **Connector:** Rapid7 — Customer X

The complex integration logic is built once. Each Connector only supplies the small amount of information that genuinely changes between instances.

---

## 2. Core Product Model

The complete model is:

```mermaid
flowchart LR
    ST[Source Type<br/>Vendor / Product Family]
    B[Blueprint<br/>Reusable End-to-End Integration]
    C[Connector<br/>Configured Instance]
    R[Runtime Materialization<br/>Deployed Physical Flows and Resources]

    ST --> B --> C --> R
```

### Source Type

A Source Type represents the vendor or product family independently of any customer.

Examples:

- Rapid7 InsightVM
- SentinelOne
- FortiSIEM
- PostgreSQL

There is one Source Type definition even when many customers use it.

### Blueprint

A Blueprint is the reusable definition of how that source works from end to end.

It contains the complete prebuilt behavior for the flow or flows that belong to that source package.

A Blueprint may contain exactly one of these three compositions:

- **one Mobility flow only**;
- **one Refinement flow only**; or
- **one Mobility flow and one Refinement flow together**.

A Blueprint never contains more than one Mobility flow or more than one Refinement flow.

Depending on which flow types are present, the Blueprint can also contain the supporting behavior required by those flows, including:

- schema handling;
- Kafka and sink setup;
- shared resource relationships;
- models and mappings;
- scripts;
- source entities;
- tenancy capability for a Mobility flow;
- expected lineage and health behavior;
- version and compatibility information.

The Blueprint is created by someone designing the integration.

### Connector

A Connector is a configured instance of a Blueprint.

It contains only the values that genuinely change for that customer, source instance, site, tenant, or environment.

Examples:

```text
Rapid7 Blueprint
├── Rapid7 - Securado
├── Rapid7 - Asyad
└── Rapid7 - Customer X
```

### Runtime Materialization

The runtime layer is what the unified platform physically deploys from the Blueprint:

- the Mobility flow, when the Blueprint contains one;
- Kafka topics, schemas and sink configuration required by that Mobility flow;
- datasets / tables and other logical resources;
- the Refinement flow, when the Blueprint contains one;
- tenant-specific runtime copies where multi-tenancy requires them.

The normal user should manage the **Connector**, not the lower-level runtime objects.

---

## 3. Main User Experience

The intended experience is:

```mermaid
flowchart TD
    A[Choose or Import Blueprint]
    B[Create Connector]
    C[Enter Only Required Instance Values]
    T[Choose Single-Tenant or Multi-Tenant]
    D[Test Connection]
    E[Platform Materializes the Blueprint Automatically]
    F[Connector is Ready - Stopped]
    G[User Presses Start]
    H[Data Flows End to End]

    A --> B --> C --> T --> D --> E --> F --> G --> H
```

A mature Connector should not require the user to:

- rebuild NiFi flows;
- infer schemas manually;
- create Kafka sync configuration manually;
- discover table names;
- reconnect Mobility output tables to Refinement inputs;
- recreate mappings;
- rebuild normalization flows;
- configure internal pagination;
- configure internal read/write behavior;
- manually duplicate flows for tenants.

All of that belongs to the Blueprint or is generated automatically by the platform.

---

## 4. Flow Granularity

Blueprint flow granularity is fixed and intentionally simple.

A Blueprint can contain only one of the following three compositions:

```text
Case 1
Blueprint
└── One Mobility Flow

Case 2
Blueprint
└── One Refinement Flow

Case 3
Blueprint
├── One Mobility Flow
└── One Refinement Flow
```

A Blueprint **never contains more than one Mobility flow and never contains more than one Refinement flow**.

This means the valid counts are:

| Mobility Flow | Refinement Flow | Valid |
|---:|---:|---|
| 1 | 0 | Yes |
| 0 | 1 | Yes |
| 1 | 1 | Yes |
| 0 | 0 | No |
| >1 | Any | No |
| Any | >1 | No |

Entities remain part of the source definition and may be produced or refined inside the single flow of that type. An entity does not require its own separate flow.

For example, one Rapid7 Mobility flow can contain the Site, Asset, Service, Software and Vulnerability branches while still being **one Mobility flow**.

Where both flow types are present, the Blueprint also defines the shared resources and bindings that connect the single Mobility flow to the single Refinement flow.

---

## 5. What the User Configures

The guiding rule is:

> **Expose only values that genuinely differ between instances of the same Blueprint.**

For a Rapid7 Connector, this may include:

- Base URL
- Credential reference
- Collection schedule
- Connectivity or proxy settings only when genuinely instance-specific
- Single-tenant or multi-tenant mode
- Tenant definitions and tenant rules when multi-tenancy is enabled
- Minimal Refinement controls

Different sources can expose different fields, but the Connector structure itself remains the same.

### Example

A user creating **Rapid7 — Securado** may only need to provide:

```text
Connector Name: Rapid7 - Securado
Base URL:       https://...
Credentials:    Rapid7 Securado Credential
Schedule:       Every 6 hours
Tenancy:        Single Tenant
```

Everything else is inherited from the Rapid7 Blueprint.

---

## 6. Typed Parameter Model

Every user-facing Connector input is declared by the Blueprint through a typed parameter definition.

A parameter can define:

- field name;
- label and description;
- data type;
- required or optional;
- secret or non-secret;
- default value;
- visibility;
- validation rules;
- whether it can be changed after deployment.

For example:

```text
Base URL
Type: URL
Required: Yes
Secret: No

Credential
Type: Credential Reference
Required: Yes
Secret: Yes

Schedule
Type: Schedule
Required: No
Default: Blueprint Default
```

The parameter system is generic, but the Blueprint should expose only the minimum parameters that genuinely vary by Connector instance.

Internal capabilities do **not** automatically become user parameters.

---

## 7. What Stays Inside the Blueprint

The user should not be asked to configure internal implementation details such as:

- API paths
- pagination logic
- cursor handling
- response extraction rules
- routing implementation
- NiFi processor configuration
- Kafka topic internals
- schema registration behavior
- sink implementation
- Iceberg write details
- deduplication rules
- normalization rules
- field mappings
- model versions
- lookup rules
- flattening logic
- merge logic
- survivorship rules
- SQL or script internals
- rebuild behavior

These define **how the integration works**, not **which instance is being created**.

If two deployments genuinely require different integration logic, that should normally result in a different Blueprint version or variant rather than exposing more low-level configuration to the Connector user.

---

## 8. Shared Resources Between Mobility and Refinement

The unified application must not duplicate information that different parts of the Blueprint use.

When the Blueprint contains both one Mobility flow and one Refinement flow, Mobility may produce a dataset that Refinement consumes.

That dataset should be defined once as a shared logical resource.

```mermaid
flowchart LR
    S[Source]
    M[Data Mobility]
    B[(Shared Logical Resource)]
    R[Data Refinement]
    O[(Next Logical Resource)]

    S --> M --> B --> R --> O
```

The platform resolves the real physical resource name once.

For example:

```text
Logical resource:
asset.bronze.raw

Resolved physical table:
bronze.rapid7_securado.asset__raw
```

The Mobility flow writes to it.

The Refinement flow reads from the exact same resolved resource.

For Mobility-only or Refinement-only Blueprints, the same logical-resource model is still used for the flow's external outputs or inputs.

The user never has to enter the same table name twice.

The same principle applies to:

- Kafka topics
- Bronze datasets
- Current datasets
- Silver datasets
- Gold / canonical datasets
- generated flow names
- tenant-specific namespaces
- other cross-platform resource identities

Bronze and Silver are examples, not hard-coded limits. The resource model must be able to represent any logical dataset required by the Blueprint.

---

## 9. Binding and Resolution Layer

The platform needs an internal mechanism that connects Connector values and shared resources to the native Mobility and Refinement artifacts.

That is the **binding / resolution layer**.

```mermaid
flowchart LR
    P[Connector Parameters]
    R[Shared Logical Resources]

    P --> B[Binding / Resolution Layer]
    R --> B

    B --> M[Mobility Artifacts]
    B --> F[Refinement Artifacts]
```

Examples:

```text
Connector Base URL
→ bound into the Mobility source connection

asset.bronze.raw
→ bound as Mobility output
→ bound as Refinement input
```

The user does not configure bindings manually.

They are Blueprint-owned and allow the platform to reuse native Mobility and Refinement artifacts without inventing a second flow language.

---

## 10. Data Refinement Control

Data Refinement should remain highly automated, but it should not be completely uncontrollable.

These controls are shown only when the Blueprint contains a Refinement flow.

The Connector should expose only a very small number of high-level controls.

### Enable / Disable Refinement

Allows the user to intentionally stop the pipeline at ingestion when required.

### Run Mode

Two choices:

- **Automatic**
- **Manual**

Automatic means the Blueprint uses its predefined downstream trigger behavior.

The user does not configure individual Refinement schedules or nodes.

### Initial Load

A small advanced option for how Refinement should begin processing:

- Blueprint Default
- Latest
- Beginning
- From Timestamp
- Last N Snapshots

Everything below this level remains Blueprint-owned.

---

## 11. Entity Model and Capability Manifest

A source can contain many different entities.

For example, a Rapid7 Blueprint may include:

```text
Site
Asset
Vulnerability
Asset Vulnerability
Software
Service
Tag
```

Each entity is described independently because different entities can have different semantics and downstream value.

The Blueprint therefore declares, per entity:

- expected normalized / OCSF class;
- capabilities contributed;
- identifiers carried;
- expected identifier coverage where relevant;
- collection semantics such as snapshot, event, or re-observed data;
- logical resources associated with the entity;
- the Mobility flow and/or Refinement flow associated with the entity, depending on which flow types the Blueprint contains.

Conceptually:

```text
Entity: Asset
Class:  <normalized / OCSF class>
Capability: Asset Inventory

Identifiers
- object_id
- hostname
- IP
```

The manifest is an expectation declared by the Blueprint.

Runtime observations are tracked separately.

---

# 12. Single-Tenant and Multi-Tenant Design

The Connector supports exactly two tenancy modes:

```text
Single Tenant
Multi-Tenant
```

There is no separate "shared multi-tenant" mode.

Single-tenant is the default.

Multi-tenancy is explicitly enabled only when one logical source instance contains data for more than one customer or tenant.

The chosen multi-tenant model is:

> **One logical multi-tenant Connector in the UI, materialized internally as separate tenant-scoped runtime copies of the flow types present in the Blueprint.**

If the Blueprint contains both flow types, each tenant receives one Mobility copy and one Refinement copy. If the Blueprint is Mobility-only, each tenant receives only the Mobility copy. A Refinement-only Blueprint does not create a new tenant split because the Tenant Boundary is a Mobility-flow concept.

Each tenant materialization writes or consumes its own tenant-specific resources as defined by the Blueprint.

---

# 13. Single-Tenant Mode

This is the normal case.

When the Blueprint contains both flow types, the single-tenant case looks like this:

```mermaid
flowchart LR
    C[One Connector]
    M[One Mobility Flow]
    B[(Shared Logical Resource)]
    R[One Refinement Flow]
    S[(Output Resource)]

    C --> M --> B --> R --> S
```

For a Mobility-only Blueprint, only the Mobility side is materialized. For a Refinement-only Blueprint, only the Refinement side is materialized.

Example:

```text
Rapid7 - Securado
```

The platform materializes exactly the flow type or flow types contained in the Blueprint.

No tenant routing is required.

No tenant-based flow duplication is required.

---

# 14. Multi-Tenant Mode

Multi-tenancy is used when one logical source connection contains data for more than one tenant.

The user still sees **one Connector**.

The platform creates a tenant-specific backend copy for every configured tenant.

When the Blueprint contains both one Mobility flow and one Refinement flow, the materialization looks like this:

```mermaid
flowchart TD
    C[One Logical Multi-Tenant Connector]
    B[Blueprint Tenant Boundary]

    C --> B

    B --> T1[Tenant A]
    B --> T2[Tenant B]
    B --> TN[Tenant N]

    T1 --> M1[Mobility Copy A]
    T2 --> M2[Mobility Copy B]
    TN --> MN[Mobility Copy N]

    M1 --> D1[(Tenant A Resources)]
    M2 --> D2[(Tenant B Resources)]
    MN --> DN[(Tenant N Resources)]

    D1 --> R1[Refinement Copy A]
    D2 --> R2[Refinement Copy B]
    DN --> RN[Refinement Copy N]
```

The user does not manually clone either application.

If the Blueprint is Mobility-only, only the Mobility copy and its resources are created for each tenant. If the Blueprint contains both flow types, the corresponding Refinement copy follows the tenant-specific Mobility output automatically.

The Connector remains one logical object in the unified platform.

---

## 15. One Tenancy Boundary Per Flow

Multi-tenancy begins at one clearly defined point in a Mobility flow.

The Blueprint author selects the adapter where tenant identity first becomes available.

For example:

```text
List Sites
```

That adapter becomes the **Tenant Boundary**.

From that point onward, the tenant rule applies to every downstream branch that belongs to that tenant-scoped part of the flow.

For any individual flow:

> **There can be zero or one Tenant Boundary. Tenancy never starts a second time later in the same flow.**

This keeps the behavior predictable and avoids nested or repeated tenancy logic.

If the Blueprint contains a Mobility flow, that single Mobility flow may either remain single-tenant or define one Tenant Boundary when multi-tenancy is required. The same flow can never contain multiple tenancy start points.

---

## 16. What the Blueprint Stores for Multi-Tenancy

The Blueprint stores only the reusable tenancy capability.

If the Blueprint contains a Mobility flow that supports multi-tenancy, the Blueprint defines:

- whether the flow supports multi-tenancy;
- the single Tenant Boundary adapter;
- the field used to distinguish tenants;
- the data type of that field;
- the allowed rule operators;
- the fact that the rule applies to the downstream graph;
- how tenant identity should be written into generated records.

For example:

```text
Flow: Rapid7 Site / Asset Flow
Tenant Boundary: List Sites
Selector Field: name
Allowed Rules:
- equals
- not equals
- in
- not in
- contains
- matches

Scope:
All downstream branches after List Sites
```

The Blueprint does **not** contain customer-specific tenant values.

---

## 17. What the Connector Stores for Multi-Tenancy

The Connector stores the actual tenant configuration for that instance.

For example:

```text
Connector: Rapid7 Production
Tenancy: Multi-Tenant

Tenant: Securado
Rule: site name != "CCED Windows QUARTER"

Tenant: CCED
Rule: site name == "CCED Windows QUARTER"
```

The Connector user only supplies:

- tenant ID;
- tenant display name;
- tenant slug if needed;
- the rule value or expression supported by the Blueprint.

The user does not re-enter:

- the adapter;
- the selector field;
- the downstream branches;
- the internal NiFi routing implementation.

Those are already defined by the Blueprint.

---

## 18. How the Tenant Rule Works

The user defines the rule once per tenant.

The platform applies it automatically to all downstream branches after the Tenant Boundary.

Conceptually:

```text
List Sites
   |
   | Tenant Rule
   |
   +--> List Site Assets
   |       |
   |       +--> Asset Detail
   |       +--> Asset Services
   |       +--> Asset Vulnerabilities
   |
   +--> Site Organization
   |
   +--> Publishing branches
```

The user does not manually configure the same condition on every branch.

The platform compiles the tenant rule into the generated Mobility flow.

---

## 19. Tenant Rule Validation

Before deployment, the platform validates the tenant configuration.

It should check that:

- the selected Blueprint actually supports multi-tenancy;
- the Tenant Boundary still exists in the Blueprint version being used;
- the selector field is valid;
- every rule uses an allowed operator;
- rules are not obviously ambiguous or overlapping where this can be determined;
- required tenant information is present.

This prevents incorrect routing from silently creating duplicate or missing tenant data.

---

## 20. Rapid7 Example

Suppose `List Sites` returns multiple Rapid7 sites.

The user wants:

- CCED data separated into one tenant;
- all remaining sites treated as Securado.

The Connector contains:

```text
Tenant: Securado
Rule: site name != "CCED Windows QUARTER"

Tenant: CCED
Rule: site name == "CCED Windows QUARTER"
```

The platform materializes:

```mermaid
flowchart TD
    C[Rapid7 Production<br/>One Logical Connector]
    B[List Sites<br/>Single Tenant Boundary]

    C --> B

    B --> P1[Securado Rule]
    B --> P2[CCED Rule]

    P1 --> M1[Rapid7 Mobility Copy - Securado]
    P2 --> M2[Rapid7 Mobility Copy - CCED]

    M1 --> BR1[(Securado Resources)]
    M2 --> BR2[(CCED Resources)]

    BR1 --> R1[Rapid7 Refinement Copy - Securado]
    BR2 --> R2[Rapid7 Refinement Copy - CCED]

    R1 --> S1[(Securado Outputs)]
    R2 --> S2[(CCED Outputs)]
```

From the user's perspective, this is still one Connector.

From the runtime perspective, it is two tenant-specific end-to-end materializations.

---

## 21. Tenant Identity

Each materialized tenant pipeline receives its tenant identity automatically.

For example:

```text
Securado materialization
customer_tenant_organization = securado

CCED materialization
customer_tenant_organization = cced
```

The user should not manually maintain this value across multiple Mobility or Refinement flows.

It is derived from the Connector's tenant definition.

---

## 22. Refinement Under Multi-Tenancy

This section applies when the Blueprint contains both one Mobility flow and one Refinement flow.

When the Mobility flow is materialized into tenant-specific copies, the single Refinement flow follows automatically for each tenant.

For example:

```text
Tenant A Mobility Output
        |
        v
Tenant A Shared Logical Dataset
        |
        v
Tenant A Refinement Copy
        |
        v
Tenant A Refined Output
```

and independently:

```text
Tenant B Mobility Output
        |
        v
Tenant B Shared Logical Dataset
        |
        v
Tenant B Refinement Copy
        |
        v
Tenant B Refined Output
```

The Connector user does not configure the Refinement copies independently.

The same single Refinement flow from the Blueprint is reused with tenant-specific resolved resources.

If the Blueprint is Mobility-only, no Refinement copy is created.

Because every tenant gets separate physical resources in this model, tenant isolation happens through separate materialization rather than through shared-table Row-Level Security.

---

## 23. Where Tenancy Information Lives

The responsibilities are deliberately separated.

| Concern | Blueprint | Connector | Platform-Derived |
|---|---:|---:|---:|
| Flow supports multi-tenancy | ✓ | | |
| Default mode is single | ✓ | | |
| Single Tenant Boundary adapter | ✓ | | |
| Tenant selector field | ✓ | | |
| Allowed routing operators | ✓ | | |
| Downstream routing scope | ✓ | | |
| Tenant metadata field | ✓ | | |
| Single or multi for this Connector | | ✓ | |
| Tenant names and IDs | | ✓ | |
| Tenant selection rules | | ✓ | |
| Number of backend copies | | | ✓ |
| Per-tenant Mobility flow copies, if Mobility is present | | | ✓ |
| Per-tenant Refinement flow copies, if Refinement is present | | | ✓ |
| Per-tenant topics / datasets | | | ✓ |
| Tenant metadata values | | | ✓ |

This prevents the same information from being entered or stored in multiple places.

---

# 24. Versioning and Compatibility

Blueprints and their important supporting definitions must be versioned.

At minimum, the platform should be able to identify versions for:

- Source Type / source definition;
- Blueprint / package;
- parameter schema;
- Mobility bundle;
- Refinement bundle;
- mappings;
- entity manifest.

A Connector points to a specific Blueprint version.

When a Connector is imported later, the platform should be able to determine:

- what Blueprint version it expects;
- what parameter definition it expects;
- which mappings and artifacts it was built with;
- whether the target platform supports the required capabilities.

This makes Blueprint / Source Pack imports predictable rather than dependent on whatever happens to exist in the target environment.

---

## 25. Test Connection

Before the platform materializes the complete Connector, the user should be able to run **Test Connection**.

The test validates the instance-specific source configuration, including:

- endpoint reachability;
- authentication;
- required source-specific inputs;
- any Blueprint-defined connectivity checks.

The result should be simple:

```text
PASS
or
FAIL: <clear reason>
```

A failed Test Connection should not leave partially materialized runtime flows behind.

---

## 26. Complete Connector Lifecycle

```mermaid
flowchart TD
    I[Choose or Import Blueprint]
    V[Validate Blueprint Version and Compatibility]
    P[Enter Minimal Connector Values]
    T{Tenancy Mode}

    T -->|Single| S[Single-Tenant]
    T -->|Multi| M[Define Tenant Rules]

    S --> TC[Test Connection]
    M --> TC

    TC --> RES[Resolve Logical Resources]
    RES --> MAT[Create Required Tenant Runtime Contexts]
    MAT --> FLOW[Materialize the Blueprint Flow Type or Flow Types]
    FLOW --> VAL[Validate Required Bindings]
    VAL --> READY[Ready - Stopped]
    READY --> START[User Starts Connector]
    START --> RUN[Data Flows End to End]
```

The target experience remains plug-and-play.

Multi-tenancy changes the runtime materialization, not the simplicity of the user experience.

---

## 27. Platform Configuration vs Connector Configuration

The Connector should describe the source instance.

Infrastructure belongs to the unified platform.

### Connector-Level

Examples include:

- source URL;
- source credentials;
- schedule;
- instance-specific proxy or connectivity inputs when required;
- tenancy mode;
- tenant rules;
- minimal Refinement controls.

### Platform-Level

Examples include:

- NiFi connection;
- Kafka brokers;
- Schema Registry;
- Redis;
- Airflow;
- Polaris;
- S3 / MinIO;
- Trino;
- FileBrowser;
- other shared infrastructure.

The Blueprint declares which platform capabilities it requires.

The target environment resolves those capabilities to its own infrastructure.

This keeps Blueprints portable and allows Connectors to be recreated consistently even when the underlying services change.

---

## 28. Export and Portability

There is only **one exportable integration artifact**:

```text
Blueprint / Source Pack
```

There is no separate "Configured Connector Export" format.

A user may start the export action from either:

- the Blueprint itself; or
- a deployed/configured Connector created from that Blueprint.

In both cases, the exported result is the **Blueprint for that flow composition** — nothing new is created from the Connector instance.

Therefore:

```text
Export Blueprint
→ Blueprint / Source Pack

Export Deployed Connector
→ its underlying Blueprint / Source Pack
```

The Connector's instance-specific configuration is not exported as a new package.

Depending on the Blueprint composition, the export contains:

- one Mobility flow only; or
- one Refinement flow only; or
- one Mobility flow and one Refinement flow.

It can also contain the supporting artifacts required by those flows, including, where applicable:

- schemas;
- sink definitions;
- models;
- mappings;
- scripts;
- typed parameter definitions;
- entity definitions;
- capability and lineage manifest;
- shared-resource relationships;
- binding definitions;
- tenancy capability and Tenant Boundary definition;
- health expectations;
- version and compatibility information.

The exported Blueprint must not contain Connector-instance values such as:

- customer credentials;
- customer endpoint values;
- tenant IDs or tenant names;
- tenant routing values;
- runtime IDs;
- deployed physical topic/table names;
- environment infrastructure configuration.

When that Blueprint is imported into another environment, the user creates a new Connector instance and supplies the required instance-specific values again.

This keeps the nomenclature and behavior simple:

```text
Blueprint / Source Pack
= reusable exportable integration

Connector
= configured runtime instance of that Blueprint
```

---

## 29. Runtime Status

The Connector definition describes the desired configuration.

Runtime status is generated and maintained by the platform.

Runtime status may include:

- configuration state;
- deployment state;
- test connection result;
- resolved logical-to-physical resource names;
- materialized Mobility flows;
- materialized Refinement flows;
- tenant materializations;
- execution status;
- freshness;
- schema drift;
- failures;
- actual observed lineage.

The user should not author this information manually.

---

## 30. Connector Health

The Connector should be monitored as one logical product object, even when a multi-tenant Connector creates several tenant-specific physical pipelines.

Health should include:

- expected versus actual execution;
- data freshness;
- source schema drift;
- failures;
- tenant materialization health;
- entity health;
- Mobility status;
- Refinement status.

Example:

```text
Rapid7 Production
Multi-Tenant
Overall: Healthy

Tenants
- Securado: Healthy
- CCED:     Healthy
```

The user can inspect lower-level runtime flows when needed, but normal management should stay at the Connector level.

---

## 31. Expected vs Actual Lineage

The Blueprint declares what it **expects** each entity to produce.

Runtime status records what the platform **actually observed**.

Conceptually:

```text
Blueprint Entity Manifest
        |
        | expected
        v
OCSF / normalized class
Capabilities
Identifiers
Expected coverage

Runtime Actual Lineage
        |
        | observed
        v
Actual class
Actual identifiers
Observed coverage
Last verification
```

This allows the platform to detect when the real source data does not match the assumptions encoded in the Blueprint.

---

## 32. Canonical Ownership Rules

The feature should follow these ownership rules consistently.

### User-Configurable

Only values that genuinely vary by Connector instance:

- endpoint;
- credentials;
- schedule;
- necessary source-specific operational inputs;
- single or multi tenancy;
- tenant definitions and rules;
- minimal Refinement controls.

### Blueprint-Owned

Reusable integration behavior:

- source flow logic;
- API paths;
- pagination;
- extraction;
- routing implementation;
- schemas;
- sink behavior;
- Refinement logic;
- mappings;
- models;
- scripts;
- entity semantics;
- Tenant Boundary;
- binding definitions;
- health expectations.

### Derived Once by the Platform

Values that should not be manually repeated:

- physical flow names;
- Kafka topics;
- table / dataset names;
- tenant-specific namespaces;
- tenant metadata values;
- materialized Mobility flow IDs;
- materialized Refinement flow IDs;
- resolved shared resources.

### Platform-Owned

Environment infrastructure:

- NiFi;
- Kafka;
- Redis;
- Schema Registry;
- Airflow;
- Polaris;
- MinIO / S3;
- Trino;
- FileBrowser;
- other shared services.

---

## 33. Final Design Principles

The feature should follow these rules consistently:

1. **Source Types identify vendor/product families; Blueprints define reusable integrations; Connectors are configured instances.**
2. **The user only configures values that genuinely vary between instances.**
3. **Mobility and Refinement are treated as one end-to-end product.**
4. **A Blueprint contains at most one Mobility flow and at most one Refinement flow: Mobility-only, Refinement-only, or one of each.**
5. **Shared logical resources are defined once and referenced by both Mobility and Refinement.**
6. **The binding layer injects parameters and shared resources into native artifacts automatically.**
7. **Derived values such as topic names and dataset names are generated once and reused.**
8. **Refinement remains configurable only at a minimal, high-level layer.**
9. **Entity capabilities, identifiers, and expected lineage are declared per entity.**
10. **Single-tenant is the default.**
11. **The only supported multi-tenant behavior is tenant-specific materialization into separate physical pipelines and resources.**
12. **Each individual Mobility flow may contain at most one Tenant Boundary. Tenancy never starts again downstream in the same flow.**
13. **The Blueprint defines where tenancy starts; the Connector defines the actual tenants and rules.**
14. **One logical multi-tenant Connector can materialize multiple tenant-specific Mobility and Refinement pipelines automatically.**
15. **The user never manually duplicates those pipelines.**
16. **Tenant rules are validated before deployment.**
17. **Infrastructure configuration stays outside the Connector.**
18. **Blueprints, parameters, mappings, manifests, and packaged artifacts are versioned.**
19. **Expected lineage and actual observed lineage are stored separately.**
20. **Importing a mature Blueprint should allow the platform to create a complete ready-to-start Connector for the flow type or flow types contained in that Blueprint.**

---

## 34. Final Product Direction

The Connector should feel like installing and configuring a finished integration, not rebuilding a data pipeline.

The normal experience should be:

> **Choose or import a Blueprint → provide the few values unique to this source instance → choose single or multi-tenancy → define tenant rules only when needed → test → deploy → start.**

Behind that simple workflow, the platform is responsible for:

- resolving parameters;
- resolving shared resources;
- materializing the Blueprint's single Mobility flow when present;
- registering schemas and creating sink configuration when required;
- creating tenant-specific runtime copies of that Mobility flow when multi-tenancy is enabled;
- materializing the Blueprint's single Refinement flow when present;
- connecting all datasets automatically;
- applying the correct tenant identity;
- validating the resulting pipeline;
- monitoring the Connector as one logical product object.

This is what makes the Connector feature genuinely reusable, portable, minimally configurable, and plug-and-play.
