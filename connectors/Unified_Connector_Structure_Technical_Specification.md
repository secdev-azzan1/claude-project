# Unified Connector Structure — Technical Specification

**Status:** Proposed v1 canonical technical structure  
**Focus:** Connector instance structure first; Blueprint only where needed to define/resolve a Connector  
**Applies to:** Unified Data Mobility + Data Refinement platform

---

## 1. Scope and fixed decisions

This document defines the **Connector contract**: what a Connector stores, what it references from a Blueprint, what the user configures, and what the platform derives.

The design follows these fixed rules:

1. **Source Type** identifies a vendor/product family.
2. **Blueprint** is the reusable end-to-end integration definition.
3. **Connector** is one configured instance of a Blueprint.
4. A Connector exposes only values that genuinely vary between instances.
5. Mobility and Refinement are deployed as one end-to-end Connector.
6. Cross-platform values such as topics and datasets are represented once as logical resources and resolved automatically.
7. Refinement has only minimal Connector-level controls.
8. Tenancy has only two modes: `single` and `multi`.
9. Multi-tenancy always materializes **separate tenant-specific pipelines/resources**. There is no shared multi-tenant mode.
10. A Mobility flow can contain **zero or one tenant boundary**. Multi-tenancy never starts a second time later in the same flow.
11. Flow granularity remains **per source**: a Blueprint may contain one or more Mobility flows and one or more Refinement flows. It is not flow-per-entity.
12. Imported Connectors are materialized but remain `stopped` until the user starts them.

---

# 2. Object relationship

```text
Source Type
    |
    v
Blueprint
    |
    v
Connector Instance
    |
    v
Runtime Materialization
```

Example:

```text
Source Type:  rapid7_insightvm
Blueprint:    rapid7_insightvm@1.0.0
Connector:    Rapid7 Production
Runtime:      Securado materialization + CCED materialization
```

The Connector is the object the user configures and manages.

---

# 3. Canonical Connector structure

The Connector should be a small instance document that references a Blueprint.

```json
{
  "apiVersion": "rp360.io/connector/v1",
  "kind": "Connector",

  "metadata": {
    "connectorId": "con_<id>",
    "displayName": "<display name>",
    "description": "<optional>",
    "revision": 1
  },

  "blueprintRef": {
    "blueprintId": "<blueprint id>",
    "blueprintVersion": "<semver>",
    "digest": "sha256:<digest>"
  },

  "instance": {
    "connectorSlug": "<stable slug>",

    "parameters": {},

    "tenancy": {},

    "controls": {
      "entities": {},
      "refinement": {}
    },

    "desiredState": "stopped"
  },

  "status": {
    "managedByPlatform": true
  }
}
```

`metadata`, `blueprintRef`, and `instance` are portable Connector configuration.

`status` is platform-managed and is not user-authored.

---

# 4. Connector field contract

## 4.1 `metadata`

```json
{
  "connectorId": "con_rapid7_production",
  "displayName": "Rapid7 Production",
  "description": "Rapid7 InsightVM production source",
  "revision": 1
}
```

Purpose:

- stable Connector identity;
- display name;
- revision of this Connector configuration.

It does not identify the reusable integration version; that belongs to `blueprintRef`.

---

## 4.2 `blueprintRef`

```json
{
  "blueprintId": "rapid7_insightvm",
  "blueprintVersion": "1.0.0",
  "digest": "sha256:<exact-blueprint-digest>"
}
```

The Connector must point to one exact Blueprint version.

The Blueprint supplies:

- the typed parameter definitions;
- Mobility flows;
- schemas;
- sinks;
- Refinement flows;
- mappings/models/scripts;
- logical resources;
- bindings;
- entity manifest;
- tenant-boundary definition;
- validation and health expectations.

A portable Connector export may bundle a snapshot of the referenced Blueprint, but the Connector itself should not duplicate the full Blueprint definition.

---

# 5. Connector parameters

`instance.parameters` contains only instance-specific values allowed by the Blueprint's `parameterSchema`.

Example:

```json
{
  "parameters": {
    "source.base_url": "https://<rapid7-endpoint>",
    "source.credential": "credential://rapid7-production",
    "runtime.schedule": "0 */6 * * *"
  }
}
```

The Blueprint defines the type and behavior of each field.

For example:

```json
{
  "id": "source.base_url",
  "type": "url",
  "required": true,
  "secret": false
}
```

```json
{
  "id": "source.credential",
  "type": "credential_ref",
  "required": true,
  "secret": true
}
```

```json
{
  "id": "runtime.schedule",
  "type": "schedule",
  "required": false,
  "default": "0 */6 * * *"
}
```

A Connector must not contain internal Blueprint values such as:

```text
API paths
pagination implementation
NiFi processor settings
Kafka broker address
Redis address
Apicurio address
mapping rules
dedupe keys
read/write modes
```

Those are Blueprint- or platform-owned.

---

# 6. Tenancy structure

The Connector supports:

```text
single
multi
```

There is no third tenancy strategy.

## 6.1 Single tenant

```json
{
  "tenancy": {
    "mode": "single",

    "tenant": {
      "tenantId": "securado",
      "tenantSlug": "securado",
      "tenantName": "Securado"
    }
  }
}
```

This creates one end-to-end materialization.

---

## 6.2 Multi-tenant

```json
{
  "tenancy": {
    "mode": "multi",

    "tenants": [
      {
        "tenantId": "securado",
        "tenantSlug": "securado",
        "tenantName": "Securado",

        "rules": {
          "mobility.flow.rapid7_site_assets": {
            "operator": "not_equals",
            "value": "CCED Windows QUARTER"
          }
        }
      },

      {
        "tenantId": "cced",
        "tenantSlug": "cced",
        "tenantName": "CCED",

        "rules": {
          "mobility.flow.rapid7_site_assets": {
            "operator": "equals",
            "value": "CCED Windows QUARTER"
          }
        }
      }
    ]
  }
}
```

The Connector stores the **actual tenant values/rules**.

It does not store:

```text
which adapter is the tenant boundary
which field is evaluated
which downstream branches are affected
how NiFi implements the routing
```

Those are defined once by the Blueprint.

---

# 7. Multi-tenancy rule

For each Mobility flow:

```text
number of tenant boundaries = 0 or 1
```

Never:

```text
boundary A -> downstream -> boundary B
```

If a Blueprint contains several Mobility flows, each flow may independently define zero or one tenant boundary.

The Connector uses the same tenant identities across the source, while each tenant-aware flow reads the rule for its own `flowRef`.

For the Rapid7 example:

```text
Flow:
mobility.flow.rapid7_site_assets

Tenant boundary:
List Sites

Selector field:
name

Scope:
all downstream branches
```

The Connector therefore only needs:

```text
Securado -> name != "CCED Windows QUARTER"
CCED     -> name == "CCED Windows QUARTER"
```

---

# 8. Connector controls

## 8.1 Entity controls

Where the Blueprint marks entities as optional:

```json
{
  "entities": {
    "site": { "enabled": true },
    "asset": { "enabled": true },
    "asset_service": { "enabled": true },
    "asset_software": { "enabled": true },
    "asset_vulnerability": { "enabled": true },
    "asset_vulnerability_solution": { "enabled": true },
    "site_organization": { "enabled": true }
  }
}
```

The user can enable/disable Blueprint-defined entities.

The user does not edit the underlying flow graph.

---

## 8.2 Minimal Refinement controls

```json
{
  "refinement": {
    "enabled": true,
    "runMode": "automatic",

    "initialLoad": {
      "mode": "blueprint_default",
      "timestamp": null,
      "lastSnapshots": null
    }
  }
}
```

Supported `runMode`:

```text
automatic
manual
```

Supported `initialLoad.mode`:

```text
blueprint_default
latest
beginning
timestamp
last_snapshots
```

The Connector does not expose normal Refinement internals such as:

```text
dedupe match fields
volatile fields
mapping definitions
model selection
write modes
latest_by
rebuild strategy
SQL/script content
merge/survivorship rules
```

Those stay inside the Blueprint.

---

# 9. Derived values are not Connector fields

The platform derives values that can be calculated from the Blueprint + Connector context.

For Rapid7, the current Mobility export contains instance-specific values such as:

```text
rapid7_securado
customer_tenant_organization = rapid7_securado
rapid7:rapid7_securado:asset:${site_id}_${id}
bronze.rapid7_securado.asset
bronze.rapid7_securado.asset_service
bronze.rapid7_securado.asset_software
bronze.rapid7_securado.asset_vulnerability
bronze.rapid7_securado.asset_vulnerability_solution
bronze.rapid7_securado.site_organization
dlq.rapid7_securado_site_assets
```

These should not be entered individually by the Connector user.

For a tenant materialization the platform derives a namespace, for example:

```text
sourceSlug = rapid7
tenantSlug = securado

namespace = rapid7_securado
```

or:

```text
sourceSlug = rapid7
tenantSlug = cced

namespace = rapid7_cced
```

The Blueprint then derives topics, table names, object-ID prefixes, batch IDs, and tenant-context values from that namespace.

---

# 10. Shared logical resources

Mobility and Refinement must not independently store the same physical dataset name.

Blueprint logical resource:

```text
asset.bronze.raw
```

Runtime resolution:

```text
Securado:
bronze.rapid7_securado.asset__raw

CCED:
bronze.rapid7_cced.asset__raw
```

Mobility writes:

```text
asset.bronze.raw
```

Refinement reads:

```text
asset.bronze.raw
```

The physical name is resolved once per tenant materialization.

The same model applies to:

```text
Kafka topics
Bronze raw/history/current datasets
Silver history/current datasets
other downstream logical datasets
```

---

# 11. Runtime status

`status` is read-only platform state.

Recommended shape:

```json
{
  "status": {
    "managedByPlatform": true,

    "configurationState": "validated",
    "deploymentState": "ready",

    "testConnection": {
      "status": "pass",
      "checkedAt": "2026-09-15T10:00:00Z",
      "reason": null
    },

    "materializations": {
      "securado": {
        "resolvedResources": {},
        "mobilityFlowRefs": [],
        "sinkRefs": [],
        "refinementFlowRefs": []
      },

      "cced": {
        "resolvedResources": {},
        "mobilityFlowRefs": [],
        "sinkRefs": [],
        "refinementFlowRefs": []
      }
    },

    "health": {},

    "actualLineage": {}
  }
}
```

Single-tenant Connectors contain one materialization.

Multi-tenant Connectors contain one materialization per tenant.

---

# 12. Complete Rapid7 Connector example

This example intentionally uses **multi-tenancy** because it exercises the complete Connector structure.

```json
{
  "apiVersion": "rp360.io/connector/v1",
  "kind": "Connector",

  "metadata": {
    "connectorId": "con_rapid7_production",
    "displayName": "Rapid7 Production",
    "description": "Rapid7 InsightVM Connector",
    "revision": 1
  },

  "blueprintRef": {
    "blueprintId": "rapid7_insightvm",
    "blueprintVersion": "1.0.0",
    "digest": "sha256:<rapid7-blueprint-digest>"
  },

  "instance": {
    "connectorSlug": "production",

    "parameters": {
      "source.base_url": "https://<rapid7-endpoint>",
      "source.credential": "credential://rapid7-production",
      "runtime.schedule": "0 */6 * * *"
    },

    "tenancy": {
      "mode": "multi",

      "tenants": [
        {
          "tenantId": "securado",
          "tenantSlug": "securado",
          "tenantName": "Securado",

          "rules": {
            "mobility.flow.rapid7_site_assets": {
              "operator": "not_equals",
              "value": "CCED Windows QUARTER"
            }
          }
        },

        {
          "tenantId": "cced",
          "tenantSlug": "cced",
          "tenantName": "CCED",

          "rules": {
            "mobility.flow.rapid7_site_assets": {
              "operator": "equals",
              "value": "CCED Windows QUARTER"
            }
          }
        }
      ]
    },

    "controls": {
      "entities": {
        "site": { "enabled": true },
        "asset": { "enabled": true },
        "asset_service": { "enabled": true },
        "asset_software": { "enabled": true },
        "asset_vulnerability": { "enabled": true },
        "asset_vulnerability_solution": { "enabled": true },
        "site_organization": { "enabled": true }
      },

      "refinement": {
        "enabled": true,
        "runMode": "automatic",

        "initialLoad": {
          "mode": "blueprint_default",
          "timestamp": null,
          "lastSnapshots": null
        }
      }
    },

    "desiredState": "stopped"
  },

  "status": {
    "managedByPlatform": true
  }
}
```

### What the user actually enters

For this example the UI only needs to ask for:

```text
Connector name
Base URL
Credential
Schedule

Tenancy:
Multi

Tenant 1:
Securado
Rule value:
not equals "CCED Windows QUARTER"

Tenant 2:
CCED
Rule value:
equals "CCED Windows QUARTER"

Optional entity choices
Minimal Refinement controls
```

The user does not type topic names, table names, `customer_tenant_organization`, object-ID prefixes, Refinement inputs, or Refinement outputs.

---

# 13. What this Connector materializes

From the Connector above, the platform produces:

```text
Rapid7 Production
|
+-- Securado materialization
|   |
|   +-- Rapid7 Mobility flow copy
|   |   tenant rule:
|   |   name != "CCED Windows QUARTER"
|   |
|   +-- namespace:
|   |   rapid7_securado
|   |
|   +-- tenant context:
|   |   customer_tenant_organization = rapid7_securado
|   |
|   +-- Kafka / sink resources
|   |
|   +-- Bronze datasets
|   |
|   +-- Rapid7 Refinement flow copies
|   |
|   +-- Silver / downstream datasets
|
+-- CCED materialization
    |
    +-- Rapid7 Mobility flow copy
    |   tenant rule:
    |   name == "CCED Windows QUARTER"
    |
    +-- namespace:
    |   rapid7_cced
    |
    +-- tenant context:
    |   customer_tenant_organization = rapid7_cced
    |
    +-- Kafka / sink resources
    |
    +-- Bronze datasets
    |
    +-- Rapid7 Refinement flow copies
    |
    +-- Silver / downstream datasets
```

The frontend still shows one logical Connector: `Rapid7 Production`.

---

# 14. Minimal Blueprint structure required by the Connector

The Blueprint is not the main object in this specification, but the Connector depends on the following Blueprint contracts:

```json
{
  "metadata": {},
  "sourceType": {},
  "compatibility": {},
  "parameterSchema": [],
  "tenancy": {},
  "naming": {},
  "entities": {},
  "resources": {},
  "artifacts": {
    "mobility": {},
    "refinement": {}
  },
  "bindings": [],
  "testConnection": {},
  "deployment": {},
  "healthContract": {}
}
```

The Connector never recreates these sections.

---

# 15. Complete Rapid7 Blueprint example

The following example is structurally complete but keeps native artifact payloads as package references rather than embedding the full NiFi/Refinement JSON.

The exact OCSF class values are intentionally marked as mapping-owned values because they are not established by the Rapid7 Mobility export itself.

```json
{
  "apiVersion": "rp360.io/blueprint/v1",
  "kind": "Blueprint",

  "metadata": {
    "blueprintId": "rapid7_insightvm",
    "displayName": "Rapid7 InsightVM",
    "version": "1.0.0",
    "digest": "sha256:<digest>"
  },

  "sourceType": {
    "sourceTypeId": "rapid7_insightvm",
    "vendor": "Rapid7",
    "product": "InsightVM",
    "category": "vulnerability_management"
  },

  "compatibility": {
    "platformMinVersion": "1.0.0",

    "requiredCapabilities": [
      "mobility-runtime",
      "kafka",
      "schema-registry",
      "sink-runtime",
      "iceberg-catalog",
      "refinement-runtime"
    ],

    "versions": {
      "parameterSchema": "1.0.0",
      "mobilityBundle": "1.0.0",
      "refinementBundle": "1.0.0",
      "mappingBundle": "1.0.0",
      "manifest": "1.0.0"
    }
  },

  "parameterSchema": [
    {
      "id": "source.base_url",
      "label": "Base URL",
      "type": "url",
      "required": true,
      "secret": false,
      "exposure": "required"
    },

    {
      "id": "source.credential",
      "label": "Credentials",
      "type": "credential_ref",
      "required": true,
      "secret": true,
      "exposure": "required"
    },

    {
      "id": "runtime.schedule",
      "label": "Collection Schedule",
      "type": "schedule",
      "required": false,
      "secret": false,
      "default": "0 */6 * * *",
      "exposure": "optional"
    }
  ],

  "tenancy": {
    "defaultMode": "single",
    "supportedModes": [
      "single",
      "multi"
    ],

    "tenantContext": {
      "recordField": "customer_tenant_organization",
      "valueTemplate": "${sourceSlug}_${tenant.tenantSlug}"
    },

    "flowBoundaries": [
      {
        "flowRef": "mobility.flow.rapid7_site_assets",

        "boundary": {
          "adapterRef": "list_sites",
          "displayName": "List Sites"
        },

        "selector": {
          "field": "name",
          "type": "string",

          "allowedOperators": [
            "equals",
            "not_equals",
            "in",
            "not_in",
            "contains",
            "matches"
          ]
        },

        "scope": "all_downstream_branches",
        "maxBoundariesPerFlow": 1
      }
    ]
  },

  "naming": {
    "sourceSlug": "rapid7",

    "templates": {
      "singleNamespace": "${sourceSlug}_${connector.connectorSlug}",
      "tenantNamespace": "${sourceSlug}_${tenant.tenantSlug}",

      "bronzeTopic": "bronze.${namespace}.${entityId}",
      "bronzeRawTable": "bronze.${namespace}.${entityId}__raw",
      "bronzeHistoryTable": "bronze.${namespace}.${entityId}__history",
      "bronzeCurrentTable": "bronze.${namespace}.${entityId}__current",
      "silverHistoryTable": "silver.${namespace}.${entityId}__history",
      "silverCurrentTable": "silver.${namespace}.${entityId}__current"
    }
  },

  "entities": {
    "site": {
      "enabledByDefault": true,

      "manifest": {
        "ocsfClass": "<mapping-owned-value>",
        "capabilities": [
          "site_inventory"
        ],

        "identifiers": [
          "object_id"
        ],

        "collectionMode": "snapshot"
      },

      "mobilityFlowRefs": [
        "mobility.flow.rapid7_site_assets"
      ],

      "refinementFlowRefs": [
        "refinement.flow.rapid7_site_normalize"
      ]
    },

    "asset": {
      "enabledByDefault": true,

      "manifest": {
        "ocsfClass": "<mapping-owned-value>",
        "capabilities": [
          "asset_inventory"
        ],

        "identifiers": [
          "object_id",
          "hostname",
          "ip"
        ],

        "collectionMode": "snapshot"
      },

      "mobilityFlowRefs": [
        "mobility.flow.rapid7_site_assets"
      ],

      "refinementFlowRefs": [
        "refinement.flow.rapid7_asset_dedupe",
        "refinement.flow.rapid7_asset_normalize"
      ]
    },

    "asset_service": {
      "enabledByDefault": true,
      "mobilityFlowRefs": [
        "mobility.flow.rapid7_site_assets"
      ]
    },

    "asset_software": {
      "enabledByDefault": true,
      "mobilityFlowRefs": [
        "mobility.flow.rapid7_site_assets"
      ]
    },

    "asset_vulnerability": {
      "enabledByDefault": true,
      "mobilityFlowRefs": [
        "mobility.flow.rapid7_site_assets"
      ]
    },

    "asset_vulnerability_solution": {
      "enabledByDefault": true,
      "mobilityFlowRefs": [
        "mobility.flow.rapid7_site_assets"
      ]
    },

    "site_organization": {
      "enabledByDefault": true,
      "mobilityFlowRefs": [
        "mobility.flow.rapid7_site_assets"
      ]
    }
  },

  "resources": {
    "asset.kafka.bronze": {
      "kind": "kafka_topic",
      "nameTemplate": "bronzeTopic",
      "entityId": "asset"
    },

    "asset.bronze.raw": {
      "kind": "iceberg_table",
      "nameTemplate": "bronzeRawTable",
      "entityId": "asset"
    },

    "asset.bronze.history": {
      "kind": "iceberg_table",
      "nameTemplate": "bronzeHistoryTable",
      "entityId": "asset"
    },

    "asset.bronze.current": {
      "kind": "iceberg_table",
      "nameTemplate": "bronzeCurrentTable",
      "entityId": "asset"
    },

    "asset.silver.history": {
      "kind": "iceberg_table",
      "nameTemplate": "silverHistoryTable",
      "entityId": "asset"
    },

    "asset.silver.current": {
      "kind": "iceberg_table",
      "nameTemplate": "silverCurrentTable",
      "entityId": "asset"
    }
  },

  "artifacts": {
    "mobility": {
      "flows": [
        {
          "artifactId": "mobility.flow.rapid7_site_assets",
          "format": "native_mobility_flow",
          "payloadRef": "mobility/flows/rapid7_site_assets.json",

          "producesEntities": [
            "site",
            "asset",
            "asset_service",
            "asset_software",
            "asset_vulnerability",
            "asset_vulnerability_solution",
            "site_organization"
          ]
        }
      ],

      "schemas": [
        {
          "artifactId": "schema.asset",
          "format": "avro",
          "payloadRef": "mobility/schemas/asset.avsc"
        }
      ],

      "sinks": [
        {
          "artifactId": "mobility.sink.asset",
          "format": "native_sink_configuration",

          "inputResourceRef": "asset.kafka.bronze",
          "outputResourceRef": "asset.bronze.raw",

          "payloadRef": "mobility/sinks/asset.json"
        }
      ]
    },

    "refinement": {
      "flows": [
        {
          "artifactId": "refinement.flow.rapid7_asset_dedupe",
          "format": "data_refinement_flow",

          "inputResourceRefs": [
            "asset.bronze.raw"
          ],

          "outputResourceRefs": [
            "asset.bronze.history",
            "asset.bronze.current"
          ],

          "payloadRef": "refinement/flows/dedupe_rapid7_asset.json"
        },

        {
          "artifactId": "refinement.flow.rapid7_asset_normalize",
          "format": "data_refinement_flow",

          "inputResourceRefs": [
            "asset.bronze.history"
          ],

          "outputResourceRefs": [
            "asset.silver.history",
            "asset.silver.current"
          ],

          "payloadRef": "refinement/flows/normalize_rapid7_asset.json"
        }
      ],

      "models": [
        {
          "artifactId": "refinement.model.asset",
          "version": "v1",
          "payloadRef": "refinement/models/asset_observation.v1.json"
        }
      ],

      "mappings": [
        {
          "artifactId": "refinement.mapping.rapid7_asset",
          "version": "v1",
          "payloadRef": "refinement/mappings/rapid7_asset.mapping.json"
        }
      ],

      "scripts": []
    }
  },

  "bindings": [
    {
      "bindingId": "source.base_url",
      "targetArtifactRef": "mobility.flow.rapid7_site_assets",
      "valueFrom": {
        "type": "parameter",
        "ref": "source.base_url"
      }
    },

    {
      "bindingId": "source.credential",
      "targetArtifactRef": "mobility.flow.rapid7_site_assets",
      "valueFrom": {
        "type": "parameter",
        "ref": "source.credential"
      }
    },

    {
      "bindingId": "tenant.context",
      "targetArtifactRef": "mobility.flow.rapid7_site_assets",
      "valueFrom": {
        "type": "tenant_context"
      }
    },

    {
      "bindingId": "asset.sink.topic",
      "targetArtifactRef": "mobility.sink.asset",
      "valueFrom": {
        "type": "resource",
        "ref": "asset.kafka.bronze"
      }
    },

    {
      "bindingId": "asset.dedupe.input",
      "targetArtifactRef": "refinement.flow.rapid7_asset_dedupe",
      "valueFrom": {
        "type": "resource",
        "ref": "asset.bronze.raw"
      }
    },

    {
      "bindingId": "asset.normalize.input",
      "targetArtifactRef": "refinement.flow.rapid7_asset_normalize",
      "valueFrom": {
        "type": "resource",
        "ref": "asset.bronze.history"
      }
    }
  ],

  "testConnection": {
    "required": true,

    "checks": [
      "endpoint_reachability",
      "authentication",
      "required_source_inputs"
    ]
  },

  "deployment": {
    "startAfterImport": false,

    "phases": [
      "validate",
      "test_connection",
      "resolve_tenants",
      "resolve_resources",
      "materialize_mobility",
      "register_schemas",
      "materialize_sinks",
      "materialize_refinement",
      "validate_bindings",
      "ready"
    ]
  },

  "healthContract": {
    "execution": true,
    "freshness": true,
    "schemaDrift": true,
    "actualLineage": true
  }
}
```

---

# 16. Rapid7 source facts used by this example

The current Mobility export contains:

```text
flow:
rapid7_securado_site_assets

process group:
list_sites__http

tenant exclusion currently repeated as:
${name:equals('CCED Windows QUARTER'):not()}

downstream List Sites outputs include:
asset-list
site-organization
site publishing / related branches
```

The export also currently hard-codes:

```text
/customer_tenant_organization = rapid7_securado

/object_id examples:
rapid7:rapid7_securado:site:${site_id}
rapid7:rapid7_securado:asset:${site_id}_${id}
rapid7:rapid7_securado:asset_service:${asset_id}_${protocol}_${port}
rapid7:rapid7_securado:asset_vulnerability:${asset_id}_${vulnerability_id}
```

Its parameter context currently contains source-specific and platform values together, including:

```text
source base URL
source username/password
Kafka bootstrap
Redis connection
Apicurio URL
Kafka topic names
schema subject names
DLQ topic
```

Under the Connector model:

```text
source base URL       -> Connector parameter
source credential     -> Connector parameter/reference
Kafka/Redis/Apicurio  -> Platform settings
topic/schema names    -> Blueprint-derived
tenant namespace      -> Platform-derived from Connector tenant
```

---

# 17. Rapid7 Refinement facts used by this example

The current Rapid7 Securado asset dedupe job uses:

```text
input:
bronze.rapid7_securado.asset__raw

outputs:
bronze.rapid7_securado.asset__history
bronze.rapid7_securado.asset__current

operator:
dedupe

match:
object_id

volatile:
history
riskscore
rawriskscore
vulnerabilities
```

The current normalize job uses:

```text
input:
bronze.rapid7_securado.asset__history

outputs:
silver.rapid7_securado.asset__history
silver.rapid7_securado.asset__current

operator:
normalize

model:
asset.v1
```

The corresponding Asyad normalize job uses the same processing logic with the namespace changed from:

```text
rapid7_securado
```

to:

```text
rapid7_asyad
```

This is why the namespace and physical dataset names should be derived from the Connector instance/tenant while the dedupe/normalize logic stays inside the Blueprint.

---

# 18. Connector validation rules

A Connector should fail validation when:

```text
blueprintRef cannot be resolved;
Blueprint version/digest does not match;
required parameter is missing;
parameter type is invalid;
credential reference is missing;
tenancy mode is not supported by the Blueprint;
multi mode has fewer than two tenants;
tenant ID or tenant slug is duplicated;
required tenant-aware flow has no rule for a tenant;
rule operator is not allowed by the Blueprint;
an enabled entity is not defined by the Blueprint;
Refinement control value is invalid;
Test Connection fails.
```

For tenancy specifically, the platform should also detect obvious overlapping or invalid rules where possible.

---

# 19. Materialization algorithm

```text
Input:
Blueprint B
Connector C

1. Resolve and validate B.
2. Validate C against B.parameterSchema.
3. Validate C.tenancy against B.tenancy.
4. Run Test Connection.

5. Build runtime contexts.

   single:
   -> one context

   multi:
   -> one context per tenant

6. For each context:

   a. derive namespace;
   b. resolve logical resources;
   c. materialize Mobility flow(s);
   d. inject source parameter bindings;
   e. inject tenant context;
   f. apply the tenant rule at the one Blueprint-defined boundary;
   g. register schemas;
   h. materialize sink(s);
   i. materialize Refinement flow(s);
   j. bind Refinement inputs/outputs to the same logical resources;
   k. record runtime IDs.

7. Validate the final end-to-end graph.
8. Set deploymentState = ready.
9. Leave desiredState = stopped until the user starts the Connector.
```

---

# 20. Final ownership table

| Concern | Connector | Blueprint | Platform-derived |
|---|---:|---:|---:|
| Connector name | ✓ | | |
| Source endpoint | ✓ | declares field | |
| Credential reference | ✓ | declares field | |
| Schedule | ✓ | default/type | |
| Single vs multi | ✓ | supported modes | |
| Tenant IDs/names/slugs | ✓ | | |
| Tenant rule values | ✓ | selector contract | |
| Tenant boundary adapter | | ✓ | |
| Tenant selector field | | ✓ | |
| API paths/pagination | | ✓ | |
| Mobility flow logic | | ✓ | |
| Entity definitions | | ✓ | |
| Schemas | | ✓ | |
| Sink logic | | ✓ | |
| Refinement flow logic | | ✓ | |
| Models/mappings/scripts | | ✓ | |
| Logical resources | | ✓ | |
| Bindings | | ✓ | |
| Topic names | naming rule | ✓ | ✓ resolved |
| Table names | naming rule | ✓ | ✓ resolved |
| `customer_tenant_organization` | tenant data | template | ✓ |
| Object-ID namespace | | template | ✓ |
| Runtime Mobility IDs | | | ✓ |
| Runtime Refinement IDs | | | ✓ |
| Number of tenant copies | tenant list | | ✓ |
| Test result | | check definition | ✓ |
| Health | | expectation | ✓ |
| Actual lineage | | expected manifest | ✓ |

---

# 21. Final contract

The implementation should preserve this boundary:

```text
CONNECTOR
=
instance-specific configuration only

BLUEPRINT
=
reusable integration behavior and contracts

PLATFORM
=
resolution, materialization, runtime state, and infrastructure
```

For the Rapid7 example:

```text
Connector supplies:
endpoint
credential
schedule
single/multi
tenants
tenant rule values
optional entity choices
minimal Refinement controls

Blueprint supplies:
Rapid7 Mobility flow
List Sites tenant boundary
selector field = name
schemas/sinks
entity definitions
dedupe/normalize flows
models/mappings
resource graph
bindings
naming rules

Platform derives:
rapid7_securado / rapid7_cced namespaces
tenant routing
customer_tenant_organization
topic names
table names
object-ID prefixes
tenant-specific Mobility copies
tenant-specific Refinement copies
runtime IDs
health and observed lineage
```

That is the canonical v1 Connector structure.
