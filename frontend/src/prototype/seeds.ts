// Seed dataset for the adapter UI prototype. Realistic security-integration
// dummy data, no real secrets. Covers the states the review needs to see:
// Running / Paused / Stopped / Draft-with-issues / Degraded-drift, healthy &
// failed connections, service revisions & retirement, approved schemas of all
// three provenances, branches with and without conditions, custom topic names,
// raw-branch quarantine.

import type {
  ConnectPlugin,
  ControllerServiceRuntime,
  FlowRuntime,
  PrototypeState,
  RuntimeProperty,
} from "./types";

// v3 (rework round 1): global variables dropped · gatewayProxies added ·
// schemaTemplates + ApprovedSchema.approvals[] added · http `config.proxy`
// boolean replaced by `config.proxyId` · kc/kafka_kc `config.sinkConfig` added ·
// every kc block backfilled with an entity.
// v4 (rework round 2): `runtimes` added — per-deployed-flow generated NiFi
// components, compiled controller services, Connect connector/task states and
// drift findings. A saved v3 blob has no `runtimes`, so the Runtime tab would
// read `undefined` and throw: the bump is mandatory, not cosmetic.
export const SEED_VERSION = 4;

const now = Date.now();
const minutesAgo = (m: number) => new Date(now - m * 60_000).toISOString();
const hoursAgo = (h: number) => new Date(now - h * 3_600_000).toISOString();
const daysAgo = (d: number) => new Date(now - d * 86_400_000).toISOString();

/**
 * Mock inventory of Kafka Connect plugins installed on the active Connect
 * cluster. The sink-config editor validates a block's `connector.class` against
 * this list; `lakehouseSink` marks the connectors whose table name /
 * auto-create / schema-evolution keys the platform owns and renders locked.
 */
export const CONNECT_PLUGIN_CATALOG: ConnectPlugin[] = [
  {
    connectorClass: "io.aiven.kafka.connect.opensearch.OpensearchSinkConnector",
    displayName: "OpenSearch Sink",
    lakehouseSink: false,
  },
  {
    connectorClass: "org.apache.iceberg.connect.IcebergSinkConnector",
    displayName: "Apache Iceberg Sink",
    lakehouseSink: true,
  },
  {
    connectorClass: "io.confluent.connect.jdbc.JdbcSinkConnector",
    displayName: "JDBC Sink",
    lakehouseSink: false,
  },
  {
    connectorClass: "io.confluent.connect.s3.S3SinkConnector",
    displayName: "Amazon S3 Sink",
    lakehouseSink: false,
  },
];

const pretty = (value: unknown) => JSON.stringify(value, null, 2);

// ---- raw Avro shared by an approved schema and its approval history -------
// Every one of these parses cleanly through normalizeAvroRecord: record root,
// named record, a fields array, and every field carries a name and a type.

const R7_ASSET_AVRO_V1 = pretty({
  type: "record",
  name: "asset",
  namespace: "raw.rapid7_assets",
  fields: [
    { name: "id", type: "long", doc: "InsightVM asset id" },
    { name: "hostName", type: "string" },
    { name: "os", type: ["null", "string"], default: null },
  ],
});

const R7_ASSET_AVRO_V2 = pretty({
  type: "record",
  name: "asset",
  namespace: "raw.rapid7_assets",
  fields: [
    { name: "id", type: "long", doc: "InsightVM asset id" },
    { name: "hostName", type: "string" },
    { name: "os", type: ["null", "string"], default: null },
    { name: "riskScore", type: "double" },
    { name: "siteId", type: "int" },
  ],
});

const FS_INCIDENT_AVRO = pretty({
  type: "record",
  name: "incident",
  namespace: "raw.fortisiem_events",
  fields: [
    { name: "incidentId", type: "long" },
    { name: "incidentTitle", type: "string" },
    { name: "eventSeverityCat", type: "string" },
    { name: "srcIp", type: ["null", "string"], default: null },
    { name: "reportingDevice", type: ["null", "string"], default: null },
  ],
});

const RET_ASSET_AVRO = pretty({
  type: "record",
  name: "asset",
  namespace: "raw.asset_retirement",
  fields: [
    { name: "sys_id", type: "string" },
    { name: "name", type: "string" },
    { name: "install_status", type: "string" },
    { name: "decommission_date", type: ["null", "string"], default: null },
  ],
});

// ---- library templates (unregistered, hand-authored) ----------------------

const TEMPLATE_INCIDENT_AVRO = pretty({
  type: "record",
  name: "IncidentEnvelope",
  namespace: "com.datapasc.templates",
  doc: "Vendor-neutral security incident envelope.",
  fields: [
    { name: "incident_id", type: "string", doc: "Stable id assigned by the source system." },
    { name: "source_system", type: "string" },
    { name: "observed_at", type: { type: "long", logicalType: "timestamp-millis" } },
    {
      name: "severity",
      type: { type: "enum", name: "Severity", symbols: ["LOW", "MEDIUM", "HIGH", "CRITICAL"] },
    },
    { name: "title", type: "string" },
    { name: "description", type: ["null", "string"], default: null },
    {
      name: "source_address",
      type: [
        "null",
        {
          type: "record",
          name: "NetworkEndpoint",
          fields: [
            { name: "ip", type: "string" },
            { name: "port", type: ["null", "int"], default: null },
            { name: "hostname", type: ["null", "string"], default: null },
          ],
        },
      ],
      default: null,
    },
    { name: "indicators", type: { type: "array", items: "string" } },
    { name: "labels", type: { type: "map", values: "string" } },
  ],
});

const TEMPLATE_ASSET_AVRO = pretty({
  type: "record",
  name: "AssetRecord",
  namespace: "com.datapasc.templates",
  doc: "Canonical asset shape shared by the CMDB and scanner feeds.",
  fields: [
    { name: "asset_id", type: "string" },
    { name: "hostname", type: "string" },
    { name: "environment", type: ["null", "string"], default: null },
    { name: "ip_addresses", type: { type: "array", items: "string" } },
    {
      name: "ownership",
      type: {
        type: "record",
        name: "AssetOwnership",
        fields: [
          { name: "team", type: "string" },
          { name: "contact_email", type: ["null", "string"], default: null },
          { name: "business_unit", type: ["null", "string"], default: null },
        ],
      },
    },
    { name: "risk_score", type: ["null", "double"], default: null },
    { name: "last_seen_at", type: { type: "long", logicalType: "timestamp-millis" } },
    { name: "retired", type: "boolean" },
  ],
});

const TEMPLATE_INDICATOR_AVRO = pretty({
  type: "record",
  name: "ThreatIndicator",
  namespace: "com.datapasc.templates",
  doc: "Partner-feed threat indicator, normalised.",
  fields: [
    { name: "indicator_id", type: "string" },
    {
      name: "indicator_type",
      type: { type: "enum", name: "IndicatorType", symbols: ["IP", "DOMAIN", "URL", "FILE_HASH", "EMAIL"] },
    },
    { name: "value", type: "string" },
    { name: "confidence", type: "int" },
    { name: "first_seen_at", type: { type: "long", logicalType: "timestamp-millis" } },
    { name: "last_seen_at", type: ["null", { type: "long", logicalType: "timestamp-millis" }], default: null },
    { name: "sources", type: { type: "array", items: "string" } },
  ],
});

// ---------------------------------------------------------------- runtime
// What the deployed flows look like on the runtime. Everything here is read
// live and rendered READ-ONLY: the spec traded live NiFi editing away for
// drift detection, so there is no editing surface anywhere on this data.

/** NiFi root-group ids — the fingerprint that disambiguates a missing group. */
const NIFI_PROD_FINGERPRINT = "9d1f2c40-0193-1000-8a4b-6f1c3d7e2b55";
const NIFI_STAGING_FINGERPRINT = "3ab77e10-0193-1000-b012-88f4c0aa1d09";

const CONNECT_WORKER_A = "connect-1.internal.corp:8083";
const CONNECT_WORKER_B = "connect-2.internal.corp:8083";

const p = (name: string, value: string): RuntimeProperty => ({ name, value });
/** Sensitive descriptors never leave the backend — the value is absent, not blanked. */
const secret = (name: string): RuntimeProperty => ({ name, value: null, sensitive: true });
/** A property whose live value no longer matches what the platform compiled. */
const diverged = (name: string, observed: string, compiled: string): RuntimeProperty => ({
  name,
  value: observed,
  divergedFrom: compiled,
});

/**
 * Platform-level controller services: compiled once, referenced by every flow
 * that needs them, and reported by NiFi under the same component id everywhere.
 * They are pinned to no Application Service — the platform owns them.
 */
function sharedServices(sharedWith: string[]): ControllerServiceRuntime[] {
  return [
    {
      id: "cs-shared-registry",
      name: "Platform · Schema Registry",
      type: "ConfluentSchemaRegistry",
      state: "ENABLED",
      appServiceId: null,
      pinnedRevision: null,
      scope: "shared",
      sharedWith,
      properties: [
        p("Schema Registry URLs", "http://apicurio.internal.corp:8081/apis/ccompat/v6"),
        p("Cache Size", "1000"),
        p("Cache Expiration", "1 hour"),
        secret("Authentication Password"),
      ],
    },
    {
      id: "cs-shared-writer",
      name: "Platform · Avro Record Writer",
      type: "AvroRecordSetWriter",
      state: "ENABLED",
      appServiceId: null,
      pinnedRevision: null,
      scope: "shared",
      sharedWith,
      properties: [
        p("Schema Write Strategy", "Confluent Schema Registry Reference"),
        p("Schema Access Strategy", "Use 'Schema Name' Property"),
        p("Schema Registry", "Platform · Schema Registry"),
      ],
    },
    {
      id: "cs-shared-kafka-ssl",
      name: "Platform · Kafka SSL Context",
      type: "StandardRestrictedSSLContextService",
      state: "ENABLED",
      appServiceId: null,
      pinnedRevision: null,
      scope: "shared",
      sharedWith,
      properties: [
        p("Truststore Filename", "/opt/nifi/conf/kafka-truststore.p12"),
        p("Truststore Type", "PKCS12"),
        secret("Truststore Password"),
        p("TLS Protocol", "TLS"),
      ],
    },
  ];
}

/** The Redis service every dedup / bookmark-using flow compiles against. */
function redisService(sharedWith: string[]): ControllerServiceRuntime {
  return {
    id: "cs-shared-redis",
    name: "Platform · Redis Connection Pool",
    type: "RedisConnectionPoolService",
    state: "ENABLED",
    appServiceId: null,
    pinnedRevision: null,
    scope: "shared",
    sharedWith,
    properties: [
      p("Connection String", "redis.internal.corp:6379"),
      p("Redis Mode", "Standalone"),
      p("Database Index", "2"),
      secret("Password"),
    ],
  };
}

/**
 * NiFi root-group id per platform connection — the fingerprint a runtime read
 * compares against `FlowRuntime.deployedFingerprint`. Same id = same instance
 * (a missing group was really deleted); different id = deployed elsewhere.
 */
export const NIFI_INSTANCE_FINGERPRINTS: Record<string, string> = {
  "conn-nifi-prod": NIFI_PROD_FINGERPRINT,
  "conn-nifi-staging": NIFI_STAGING_FINGERPRINT,
};

const JSON_READER: ControllerServiceRuntime = {
  id: "cs-shared-json-reader",
  name: "Platform · JSON Tree Reader",
  type: "JsonTreeReader",
  state: "ENABLED",
  appServiceId: null,
  pinnedRevision: null,
  scope: "shared",
  properties: [p("Schema Access Strategy", "Infer Schema"), p("Starting Field Strategy", "Root Node")],
};

/**
 * The platform-owned controller services every compiled flow references. Also
 * used when a flow is deployed at runtime (api.ts synthesizes its runtime from
 * the same building blocks the seed uses).
 */
export function platformControllerServices(sharedWith: string[] = []): ControllerServiceRuntime[] {
  return [JSON_READER, ...sharedServices(sharedWith)];
}

/** The Redis pool, for flows that dedup or keep jdbc bookmarks. */
export function platformRedisService(sharedWith: string[] = []): ControllerServiceRuntime {
  return redisService(sharedWith);
}

const KAFKA_BROKERS = "kafka-1.internal.corp:9094,kafka-2.internal.corp:9094";

/** The Kafka client properties every generated publish/consume processor carries. */
const kafkaClientProps = (): RuntimeProperty[] => [
  p("Kafka Brokers", KAFKA_BROKERS),
  p("Security Protocol", "SASL_SSL"),
  p("SASL Mechanism", "SCRAM-SHA-512"),
  p("SSL Context Service", "Platform · Kafka SSL Context"),
  secret("sasl.jaas.config"),
];

const publishProps = (topic: string, extra: RuntimeProperty[] = []): RuntimeProperty[] => [
  p("Topic Name", topic),
  p("Record Reader", "Platform · JSON Tree Reader"),
  p("Record Writer", "Platform · Avro Record Writer"),
  p("Delivery Guarantee", "Guarantee Replicated Delivery"),
  p("Compression Type", "snappy"),
  ...kafkaClientProps(),
  ...extra,
];

const ICEBERG_CLASS = "org.apache.iceberg.connect.IcebergSinkConnector";
const OPENSEARCH_CLASS = "io.aiven.kafka.connect.opensearch.OpensearchSinkConnector";

/**
 * A truncated worker trace — exactly what the Connect panel shows so a FAILED
 * sink task is visible here instead of buried in the worker's logs.
 */
const OPENSEARCH_FLUSH_TRACE = `org.apache.kafka.connect.errors.ConnectException: Flush timeout expired with unflushed records: 1843
\tat io.aiven.kafka.connect.opensearch.OpensearchSinkTask.flush(OpensearchSinkTask.java:212)
\tat org.apache.kafka.connect.runtime.WorkerSinkTask.commitOffsets(WorkerSinkTask.java:405)
\tat org.apache.kafka.connect.runtime.WorkerSinkTask.execute(WorkerSinkTask.java:203)
Caused by: org.opensearch.client.ResponseException: method [POST], host [https://opensearch.soc.corp:9200], URI [/_bulk],
\tstatus line [HTTP/1.1 429 Too Many Requests]
\t{"error":{"type":"es_rejected_execution_exception","reason":"rejected execution of coordinating operation [coordinating_and_primary_bytes=0, max_coordinating_and_primary_bytes=1073741824]"}}
… trace truncated at 8 lines — the full stack stays on ${CONNECT_WORKER_B}`;

function buildRuntimes(): FlowRuntime[] {
  const prodRuntime = (
    flowId: string,
    processGroupId: string,
    parts: Partial<FlowRuntime> & Pick<FlowRuntime, "components" | "controllerServices" | "connectors">,
  ): FlowRuntime => ({
    flowId,
    nifiConnectionId: "conn-nifi-prod",
    processGroupId,
    deployedFingerprint: NIFI_PROD_FINGERPRINT,
    observedFingerprint: NIFI_PROD_FINGERPRINT,
    reachable: true,
    lastReadAt: minutesAgo(4),
    drift: [],
    orphans: [],
    ...parts,
  });

  return [
    // ── Rapid7 Assets · Running, clean ────────────────────────────────────
    prodRuntime("flow-rapid7", "0193a41c-7f10-1000-b8d2-1f0c44ae7701", {
      components: [
        {
          id: "0193a41c-7f10-1000-9a01-2c81ff30b101",
          name: "List Assets · InvokeHTTP",
          type: "org.apache.nifi.processors.standard.InvokeHTTP",
          blockId: "b-r7-list",
          state: "RUNNING",
          properties: [
            p("HTTP Method", "GET"),
            p("Remote URL", "https://insightvm.corp.local:3780/api/3/assets?page=${page}&size=500"),
            p("SSL Context Service", "Rapid7 InsightVM API · SSL Context"),
            secret("Request Header api-key"),
            p("Connection Timeout", "5 secs"),
            p("Read Timeout", "30 secs"),
            p("Proxy Configuration Service", "No value set"),
          ],
        },
        {
          id: "0193a41c-7f10-1000-9a01-2c81ff30b102",
          name: "List Assets · SplitJson",
          type: "org.apache.nifi.processors.standard.SplitJson",
          blockId: "b-r7-list",
          state: "RUNNING",
          properties: [p("JsonPath Expression", "$.resources[*]"), p("Null Value Representation", "empty string")],
        },
        {
          id: "0193a41c-7f10-1000-9a01-2c81ff30b103",
          name: "List Assets · Extract site_id",
          type: "org.apache.nifi.processors.standard.EvaluateJsonPath",
          blockId: "b-r7-list",
          state: "RUNNING",
          properties: [p("Destination", "flowfile-attribute"), p("site_id", "$.siteId"), p("Path Not Found Behavior", "warn")],
        },
        {
          id: "0193a41c-7f10-1000-9a01-2c81ff30b104",
          name: "List Assets · Remove links",
          type: "org.apache.nifi.processors.standard.JoltTransformJSON",
          blockId: "b-r7-list",
          state: "RUNNING",
          properties: [p("Jolt Transformation DSL", "Remove"), p("Jolt Specification", '{ "links": "" }')],
        },
        {
          id: "0193a41c-7f10-1000-9a01-2c81ff30b105",
          name: "Assets to Iceberg · PublishKafkaRecord",
          type: "org.apache.nifi.processors.kafka.pubsub.PublishKafkaRecord_2_6",
          blockId: "b-r7-sink",
          state: "RUNNING",
          properties: publishProps("raw.rapid7_assets.asset"),
        },
      ],
      controllerServices: [
        {
          id: "cs-r7-ssl",
          name: "Rapid7 InsightVM API · SSL Context",
          type: "StandardRestrictedSSLContextService",
          state: "ENABLED",
          appServiceId: "svc-rapid7",
          pinnedRevision: 1,
          scope: "flow",
          properties: [
            p("Truststore Filename", "/opt/nifi/conf/rapid7-truststore.p12"),
            p("Truststore Type", "PKCS12"),
            secret("Truststore Password"),
            p("TLS Protocol", "TLS"),
          ],
        },
        JSON_READER,
        ...sharedServices(["FortiSIEM Events", "CMDB Asset Sync", "Asset Retirement", "Partner Threat Feed"]),
      ],
      connectors: [
        {
          name: "dmp.rapid7_assets.asset.iceberg",
          blockId: "b-r7-sink",
          connectorClass: ICEBERG_CLASS,
          state: "RUNNING",
          workerId: CONNECT_WORKER_A,
          recordsSent: 35_644,
          recordsFailed: 0,
          tasks: [
            { id: 0, state: "RUNNING", workerId: CONNECT_WORKER_A },
            { id: 1, state: "RUNNING", workerId: CONNECT_WORKER_B },
          ],
        },
      ],
    }),

    // ── FortiSIEM Events · Running, one FAILED sink task ──────────────────
    prodRuntime("flow-fortisiem", "0193a41c-7f10-1000-b8d2-1f0c44ae7702", {
      lastReadAt: minutesAgo(2),
      components: [
        {
          id: "0193a41c-7f10-1000-9a02-2c81ff30b201",
          name: "Fetch Incidents · InvokeHTTP",
          type: "org.apache.nifi.processors.standard.InvokeHTTP",
          blockId: "b-fs-fetch",
          state: "RUNNING",
          properties: [
            p("HTTP Method", "GET"),
            p("Remote URL", "https://fortisiem.internal.corp/phoenix/rest/incident/list?nextToken=${next_token}"),
            p("SSL Context Service", "FortiSIEM Events API · SSL Context"),
            p("Proxy Configuration Service", "FortiSIEM egress · APISIX Proxy"),
            secret("Request Header Authorization"),
            p("Connection Timeout", "5 secs"),
            p("Read Timeout", "30 secs"),
          ],
        },
        {
          id: "0193a41c-7f10-1000-9a02-2c81ff30b202",
          name: "Fetch Incidents · SplitJson",
          type: "org.apache.nifi.processors.standard.SplitJson",
          blockId: "b-fs-fetch",
          state: "RUNNING",
          properties: [p("JsonPath Expression", "$.incidents[*]"), p("Null Value Representation", "empty string")],
        },
        {
          id: "0193a41c-7f10-1000-9a02-2c81ff30b203",
          name: "Fetch Incidents · Extract severity",
          type: "org.apache.nifi.processors.standard.EvaluateJsonPath",
          blockId: "b-fs-fetch",
          state: "RUNNING",
          properties: [p("Destination", "flowfile-attribute"), p("severity", "$.eventSeverityCat"), p("Path Not Found Behavior", "warn")],
        },
        {
          id: "0193a41c-7f10-1000-9a02-2c81ff30b204",
          name: "Fetch Incidents · Route critical",
          type: "org.apache.nifi.processors.standard.RouteOnAttribute",
          blockId: "b-fs-fetch",
          state: "RUNNING",
          properties: [
            p("Routing Strategy", "Route to Property name"),
            p("critical", "${severity:equals('HIGH')}"),
            p("Unmatched Relationship", "forwarded"),
          ],
        },
        {
          id: "0193a41c-7f10-1000-9a02-2c81ff30b205",
          name: "Critical to Lakehouse · PublishKafkaRecord",
          type: "org.apache.nifi.processors.kafka.pubsub.PublishKafkaRecord_2_6",
          blockId: "b-fs-critical",
          state: "RUNNING",
          properties: publishProps("raw.fortisiem_events.incident"),
        },
        {
          id: "0193a41c-7f10-1000-9a02-2c81ff30b206",
          name: "All Events Topic · DetectDuplicate",
          type: "org.apache.nifi.processors.standard.DetectDuplicate",
          blockId: "b-fs-all",
          state: "RUNNING",
          properties: [
            p("Distributed Cache Service", "Platform · Redis Connection Pool"),
            p("Cache Entry Identifier", "${dmp.dedup.fingerprint}"),
            p("Age Off Duration", "24 hours"),
            p("Cache The Entry Identifier", "true"),
          ],
        },
        {
          id: "0193a41c-7f10-1000-9a02-2c81ff30b207",
          name: "All Events Topic · PublishKafkaRecord",
          type: "org.apache.nifi.processors.kafka.pubsub.PublishKafkaRecord_2_6",
          blockId: "b-fs-all",
          state: "RUNNING",
          properties: publishProps("raw.fortisiem_events.event"),
        },
      ],
      controllerServices: [
        {
          id: "cs-fs-token",
          name: "FortiSIEM Events API · Session Token",
          type: "DmpSessionTokenProvider",
          state: "ENABLED",
          appServiceId: "svc-fortisiem",
          pinnedRevision: 2,
          scope: "flow",
          properties: [
            p("Login URL", "https://fortisiem.internal.corp/phoenix/rest/h5/sec/login"),
            p("Token JSON Path", "$.sessionToken"),
            p("Token Header", "Authorization"),
            p("Refresh Window", "5 mins"),
            secret("Login Password"),
          ],
        },
        {
          id: "cs-fs-ssl",
          name: "FortiSIEM Events API · SSL Context",
          type: "StandardRestrictedSSLContextService",
          state: "ENABLED",
          appServiceId: "svc-fortisiem",
          pinnedRevision: 2,
          scope: "flow",
          properties: [
            p("Truststore Filename", "/opt/nifi/conf/fortisiem-truststore.p12"),
            secret("Truststore Password"),
            p("TLS Protocol", "TLS"),
          ],
        },
        {
          id: "cs-fs-proxy",
          name: "FortiSIEM egress · APISIX Proxy",
          type: "StandardProxyConfigurationService",
          state: "ENABLED",
          appServiceId: null,
          pinnedRevision: null,
          scope: "flow",
          properties: [
            p("Proxy Type", "HTTP"),
            p("Proxy Server Host", "apisix.internal.corp"),
            p("Proxy Server Port", "9080"),
            p("Proxy User", "dmp-egress"),
            secret("Proxy Password"),
          ],
        },
        JSON_READER,
        redisService(["Partner Threat Feed", "CMDB Asset Sync"]),
        ...sharedServices(["Rapid7 Assets", "CMDB Asset Sync", "Asset Retirement", "Partner Threat Feed"]),
      ],
      connectors: [
        {
          name: "dmp.fortisiem_events.incident.iceberg",
          blockId: "b-fs-critical",
          connectorClass: ICEBERG_CLASS,
          state: "RUNNING",
          workerId: CONNECT_WORKER_A,
          recordsSent: 3_709,
          recordsFailed: 2,
          tasks: [
            { id: 0, state: "RUNNING", workerId: CONNECT_WORKER_A },
            { id: 1, state: "RUNNING", workerId: CONNECT_WORKER_B },
          ],
        },
        {
          name: "dmp.fortisiem_events.event.opensearch",
          blockId: "b-fs-os",
          connectorClass: OPENSEARCH_CLASS,
          state: "RUNNING",
          workerId: CONNECT_WORKER_A,
          recordsSent: 114_489,
          recordsFailed: 1_843,
          lastErrorTrace: OPENSEARCH_FLUSH_TRACE,
          tasks: [
            { id: 0, state: "RUNNING", workerId: CONNECT_WORKER_A },
            { id: 1, state: "RUNNING", workerId: CONNECT_WORKER_A },
            { id: 2, state: "FAILED", workerId: CONNECT_WORKER_B, lastErrorTrace: OPENSEARCH_FLUSH_TRACE },
          ],
        },
      ],
    }),

    // ── CMDB Asset Sync · Stopped, pinned an older service revision ───────
    prodRuntime("flow-cmdb", "0193a41c-7f10-1000-b8d2-1f0c44ae7703", {
      lastReadAt: hoursAgo(6),
      components: [
        {
          id: "0193a41c-7f10-1000-9a03-2c81ff30b301",
          name: "Read CMDB Assets · QueryDatabaseTableRecord",
          type: "org.apache.nifi.processors.standard.QueryDatabaseTableRecord",
          blockId: "b-cmdb-read",
          state: "STOPPED",
          properties: [
            p("Database Connection Pooling Service", "Security Postgres · Connection Pool"),
            p("Table Name", "cmdb_assets"),
            p("Columns to Return", "asset_id, hostname, owner_group, environment, updated_at"),
            p("Maximum-value Columns", "updated_at"),
            p("Initial Load Strategy", "Start at Beginning"),
            p("Fetch Size", "1000"),
            p("Record Writer", "Platform · Avro Record Writer"),
          ],
        },
        {
          id: "0193a41c-7f10-1000-9a03-2c81ff30b302",
          name: "Read CMDB Assets · Rename owner_group",
          type: "org.apache.nifi.processors.standard.UpdateRecord",
          blockId: "b-cmdb-read",
          state: "STOPPED",
          properties: [
            p("Replacement Value Strategy", "Record Path Value"),
            p("/team", "/owner_group"),
            p("Record Reader", "Platform · JSON Tree Reader"),
          ],
        },
        {
          id: "0193a41c-7f10-1000-9a03-2c81ff30b303",
          name: "Assets Topic · PublishKafkaRecord",
          type: "org.apache.nifi.processors.kafka.pubsub.PublishKafkaRecord_2_6",
          blockId: "b-cmdb-write",
          state: "STOPPED",
          properties: publishProps("raw.cmdb_asset_sync.asset"),
        },
      ],
      controllerServices: [
        {
          id: "cs-cmdb-dbcp",
          name: "Security Postgres · Connection Pool",
          type: "DBCPConnectionPool",
          state: "ENABLED",
          appServiceId: "svc-postgres",
          pinnedRevision: 1,
          scope: "flow",
          properties: [
            p("Database Connection URL", "jdbc:postgresql://pg-sec.internal.corp:5432/secops"),
            p("Database Driver Class Name", "org.postgresql.Driver"),
            p("Database User", "dmp_reader"),
            secret("Password"),
            p("Max Total Connections", "8"),
            p("Validation Query", "SELECT 1"),
          ],
        },
        JSON_READER,
        redisService(["FortiSIEM Events", "Partner Threat Feed"]),
        ...sharedServices(["Rapid7 Assets", "FortiSIEM Events", "Asset Retirement", "Partner Threat Feed"]),
      ],
      connectors: [
        {
          name: "dmp.cmdb_asset_sync.asset.opensearch",
          blockId: "b-cmdb-os",
          connectorClass: OPENSEARCH_CLASS,
          state: "PAUSED",
          workerId: CONNECT_WORKER_A,
          recordsSent: 191_004,
          recordsFailed: 0,
          tasks: [{ id: 0, state: "PAUSED", workerId: CONNECT_WORKER_A }],
        },
      ],
    }),

    // ── Partner Threat Feed · Paused (processing held, trigger alive) ─────
    prodRuntime("flow-partner", "0193a41c-7f10-1000-b8d2-1f0c44ae7704", {
      lastReadAt: hoursAgo(2),
      components: [
        {
          id: "0193a41c-7f10-1000-9a04-2c81ff30b401",
          name: "Consume Partner Feed · ConsumeKafkaRecord",
          type: "org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_2_6",
          blockId: "b-pt-read",
          state: "STOPPED",
          properties: [
            p("Topic Name(s)", "partner.threatfeed.indicators"),
            p("Group ID", "dmp-partner-threat-feed"),
            p("Offset Reset", "earliest"),
            p("Kafka Brokers", "kafka.partner-siem.example:9093"),
            p("Security Protocol", "SASL_SSL"),
            p("SSL Context Service", "Partner SIEM Kafka · SSL Context"),
            secret("sasl.jaas.config"),
          ],
        },
        {
          id: "0193a41c-7f10-1000-9a04-2c81ff30b402",
          name: "Consume Partner Feed · Add feed_source",
          type: "org.apache.nifi.processors.standard.UpdateRecord",
          blockId: "b-pt-read",
          state: "STOPPED",
          properties: [p("Replacement Value Strategy", "Literal Value"), p("/feed_source", "partner-siem")],
        },
        {
          id: "0193a41c-7f10-1000-9a04-2c81ff30b403",
          name: "Consume Partner Feed · DetectDuplicate",
          type: "org.apache.nifi.processors.standard.DetectDuplicate",
          blockId: "b-pt-read",
          state: "STOPPED",
          properties: [
            p("Distributed Cache Service", "Platform · Redis Connection Pool"),
            p("Cache Entry Identifier", "${dmp.dedup.fingerprint}"),
            p("Age Off Duration", "24 hours"),
          ],
        },
        {
          id: "0193a41c-7f10-1000-9a04-2c81ff30b404",
          name: "Indicators Topic · PublishKafkaRecord",
          type: "org.apache.nifi.processors.kafka.pubsub.PublishKafkaRecord_2_6",
          blockId: "b-pt-write",
          state: "STOPPED",
          properties: publishProps("raw.partner_threat_feed.indicator"),
        },
      ],
      controllerServices: [
        {
          id: "cs-pt-ssl",
          name: "Partner SIEM Kafka · SSL Context",
          type: "StandardRestrictedSSLContextService",
          state: "ENABLED",
          appServiceId: "svc-partner-kafka",
          pinnedRevision: 1,
          scope: "flow",
          properties: [
            p("Truststore Filename", "/opt/nifi/conf/partner-truststore.p12"),
            secret("Truststore Password"),
            p("TLS Protocol", "TLS"),
          ],
        },
        JSON_READER,
        redisService(["FortiSIEM Events", "CMDB Asset Sync"]),
        ...sharedServices(["Rapid7 Assets", "FortiSIEM Events", "CMDB Asset Sync", "Asset Retirement"]),
      ],
      connectors: [],
    }),

    // ── Asset Retirement · Stopped, one out-of-band property edit ─────────
    prodRuntime("flow-retirement", "0193a41c-7f10-1000-b8d2-1f0c44ae7705", {
      lastReadAt: hoursAgo(9),
      components: [
        {
          id: "0193a41c-7f10-1000-9a05-2c81ff30b501",
          name: "Fetch Retirement Notices · InvokeHTTP",
          type: "org.apache.nifi.processors.standard.InvokeHTTP",
          blockId: "b-ret-fetch",
          state: "STOPPED",
          properties: [
            p("HTTP Method", "GET"),
            p("Remote URL", "https://corp.service-now.com/api/now/table/cmdb_ci_retired?sysparm_limit=1000"),
            p("OAuth2 Access Token Provider", "ServiceNow CMDB API · OAuth2"),
            p("Connection Timeout", "5 secs"),
            p("Read Timeout", "30 secs"),
          ],
        },
        {
          id: "0193a41c-7f10-1000-9a05-2c81ff30b502",
          name: "Fetch Retirement Notices · Extract lifecycle_state",
          type: "org.apache.nifi.processors.standard.EvaluateJsonPath",
          blockId: "b-ret-fetch",
          state: "STOPPED",
          properties: [p("Destination", "flowfile-attribute"), p("lifecycle_state", "$.install_status")],
        },
        {
          id: "0193a41c-7f10-1000-9a05-2c81ff30b503",
          name: "All Notices to Lakehouse · PublishKafkaRecord",
          type: "org.apache.nifi.processors.kafka.pubsub.PublishKafkaRecord_2_6",
          blockId: "b-ret-all",
          state: "STOPPED",
          properties: [
            p("Topic Name", "raw.asset_retirement.asset"),
            p("Record Reader", "Platform · JSON Tree Reader"),
            p("Record Writer", "Platform · Avro Record Writer"),
            p("Delivery Guarantee", "Guarantee Replicated Delivery"),
            // Someone changed this in the NiFi canvas. Read, never repaired.
            diverged("Compression Type", "gzip", "snappy"),
            ...kafkaClientProps(),
          ],
        },
        {
          id: "0193a41c-7f10-1000-9a05-2c81ff30b504",
          name: "Active Assets · RouteOnAttribute",
          type: "org.apache.nifi.processors.standard.RouteOnAttribute",
          blockId: "b-ret-active",
          state: "STOPPED",
          properties: [
            p("Routing Strategy", "Route to Property name"),
            p("keep-active", "${lifecycle_state:equals('in_use')}"),
            p("Unmatched Relationship", "dropped"),
          ],
        },
        {
          id: "0193a41c-7f10-1000-9a05-2c81ff30b505",
          name: "Active Assets · PublishKafkaRecord",
          type: "org.apache.nifi.processors.kafka.pubsub.PublishKafkaRecord_2_6",
          blockId: "b-ret-active",
          state: "STOPPED",
          properties: publishProps("raw.asset_retirement.asset.active"),
        },
        {
          id: "0193a41c-7f10-1000-9a05-2c81ff30b506",
          name: "Decommissioned Assets · RouteOnAttribute",
          type: "org.apache.nifi.processors.standard.RouteOnAttribute",
          blockId: "b-ret-decom",
          state: "STOPPED",
          properties: [
            p("Routing Strategy", "Route to Property name"),
            p("keep-retired", "${lifecycle_state:equals('retired')}"),
            p("Unmatched Relationship", "dropped"),
          ],
        },
        {
          id: "0193a41c-7f10-1000-9a05-2c81ff30b507",
          name: "Decommissioned Assets · PublishKafkaRecord",
          type: "org.apache.nifi.processors.kafka.pubsub.PublishKafkaRecord_2_6",
          blockId: "b-ret-decom",
          state: "STOPPED",
          properties: publishProps("asset_retired"),
        },
      ],
      controllerServices: [
        {
          id: "cs-ret-oauth",
          name: "ServiceNow CMDB API · OAuth2",
          type: "StandardOauth2AccessTokenProvider",
          state: "ENABLED",
          appServiceId: "svc-servicenow",
          pinnedRevision: 1,
          scope: "flow",
          properties: [
            p("Authorization Server URL", "https://corp.service-now.com/oauth_token.do"),
            p("Grant Type", "Client Credentials"),
            p("Client ID", "dmp-cmdb-reader"),
            secret("Client Secret"),
            p("Refresh Window", "1 min"),
          ],
        },
        JSON_READER,
        ...sharedServices(["Rapid7 Assets", "FortiSIEM Events", "CMDB Asset Sync", "Partner Threat Feed"]),
      ],
      connectors: [
        {
          name: "dmp.asset_retirement.asset.iceberg",
          blockId: "b-ret-all",
          connectorClass: ICEBERG_CLASS,
          state: "PAUSED",
          workerId: CONNECT_WORKER_B,
          recordsSent: 42_118,
          recordsFailed: 0,
          tasks: [{ id: 0, state: "PAUSED", workerId: CONNECT_WORKER_B }],
        },
      ],
      drift: [
        {
          id: "drift-ret-compression",
          kind: "property_edited",
          summary: 'Compression Type was changed to "gzip" on the live processor',
          where: "All Notices to Lakehouse · PublishKafkaRecord",
          expected: "snappy",
          observed: "gzip",
          verdict: "out_of_band_edit",
          verdictDetail:
            "Same NiFi instance, same process group — someone edited the property on the canvas. The platform did not merge it and will not: the next Redeploy compiles the block config and overwrites it.",
          observedAt: hoursAgo(9),
          repairable: false,
        },
      ],
    }),

    // ── Audit Mirror · Degraded, process group gone from the same NiFi ────
    prodRuntime("flow-auditmirror", "0193a41c-7f10-1000-b8d2-1f0c44ae7706", {
      lastReadAt: minutesAgo(6),
      // The read found nothing under the recorded process-group id, so there
      // are no live components or connectors to report. Shared services are
      // platform-level and survive.
      components: [],
      controllerServices: [
        // Compiled for this flow but registered on the ROOT group, so it
        // outlived the deleted process group. It references nothing now — and
        // a force repair records it as an orphan rather than deleting it.
        {
          id: "cs-am-legacy-ssl",
          name: "Legacy Audit Kafka · SSL Context",
          type: "StandardRestrictedSSLContextService",
          state: "DISABLED",
          appServiceId: null,
          pinnedRevision: null,
          scope: "flow",
          properties: [
            p("Truststore Filename", "/opt/nifi/conf/legacy-audit-truststore.p12"),
            secret("Truststore Password"),
            p("TLS Protocol", "TLS"),
            p("Referencing Components", "0 — the process group that used it is gone"),
          ],
        },
        ...sharedServices(["Rapid7 Assets", "FortiSIEM Events", "CMDB Asset Sync", "Asset Retirement"]),
      ],
      connectors: [],
      drift: [
        {
          id: "drift-am-pg-missing",
          kind: "process_group_missing",
          summary: "Process group 0193a41c-7f10-1000-b8d2-1f0c44ae7706 is not on Production NiFi",
          where: "Audit Mirror (process group)",
          expected: `root group ${NIFI_PROD_FINGERPRINT}`,
          observed: `root group ${NIFI_PROD_FINGERPRINT}`,
          verdict: "really_deleted",
          verdictDetail:
            "The instance fingerprint matches the one recorded at deploy, so this is the same NiFi — the group was deleted out-of-band, not moved to another instance. Nothing was re-created by this read.",
          observedAt: minutesAgo(6),
          repairable: true,
        },
      ],
    }),
  ];
}

export function buildSeedState(): PrototypeState {
  return {
    seedVersion: SEED_VERSION,

    // ------------------------------------------------------------- flows
    flows: [
      // 1 — simple source → governed destination, Running, valid
      {
        id: "flow-rapid7",
        name: "Rapid7 Assets",
        description: "Nightly asset inventory from Rapid7 InsightVM into the bronze lakehouse.",
        state: "Running",
        enabled: true,
        cron: "0 */6 * * *",
        blocks: [
          {
            id: "b-r7-list",
            adapter: "http",
            mode: "read",
            name: "List Assets",
            parentId: null,
            serviceId: "svc-rapid7",
            config: {
              method: "GET",
              path: "/api/3/assets",
              responseFormat: "json",
              recordPath: "$.resources[*]",
              split: true,
              pagination: { type: "page", fields: { pageParam: "page", sizeParam: "size", sizeValue: "500", firstPage: "1", stop: "empty_response" } },
            },
            transforms: [
              { id: "t-r7-1", kind: "extract", config: { attribute: "site_id", path: "$.siteId", default: "" } },
              { id: "t-r7-2", kind: "remove_field", config: { field: "links" } },
            ],
            testResult: {
              ok: true,
              records: [
                { id: 1204, hostName: "srv-dc01.corp.local", os: "Windows Server 2022", riskScore: 7211, siteId: 3 },
                { id: 1205, hostName: "srv-web02.dmz.corp", os: "Ubuntu 22.04", riskScore: 18342, siteId: 3 },
              ],
              detectedFields: ["id", "hostName", "os", "riskScore", "siteId"],
              testedAt: daysAgo(2),
            },
          },
          {
            id: "b-r7-sink",
            adapter: "kafka_kc",
            name: "Assets to Iceberg",
            parentId: "b-r7-list",
            serviceId: "svc-iceberg",
            entity: "asset",
            config: {
              sinkServiceId: "svc-iceberg",
              // `topics`, the Avro converter and the Iceberg table name are
              // platform-owned: rendered as locked rows, never persisted here.
              sinkConfig: {
                "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
                "tasks.max": "2",
                "iceberg.catalog.type": "rest",
                "iceberg.control.commit.interval-ms": "60000",
                "consumer.override.max.poll.records": "2000",
              },
            },
            transforms: [],
          },
        ],
        topics: [
          { id: "t-flow-r7", kind: "materialized", name: "raw.rapid7_assets.asset", sealed: true, writerBlockId: "b-r7-sink" },
        ],
        variables: [{ name: "rapid7_region", value: "us2", secret: false }],
        servicePins: { "svc-rapid7": 1, "svc-iceberg": 1 },
        deployedAt: daysAgo(12),
        lastRunAt: hoursAgo(4),
        createdAt: daysAgo(30),
        updatedAt: hoursAgo(4),
      },

      // 2 — conditional branch + schemaless topic + kc subscription, Running
      {
        id: "flow-fortisiem",
        name: "FortiSIEM Events",
        description: "Security incidents from FortiSIEM: critical incidents governed to the lakehouse, the full feed to a topic consumed by the SOC OpenSearch.",
        state: "Running",
        enabled: true,
        cron: "*/15 * * * *",
        blocks: [
          {
            id: "b-fs-fetch",
            adapter: "http",
            mode: "read",
            name: "Fetch Incidents",
            parentId: null,
            serviceId: "svc-fortisiem",
            config: {
              method: "GET",
              path: "/phoenix/rest/incident/list",
              responseFormat: "json",
              recordPath: "$.incidents[*]",
              split: true,
              pagination: {
                type: "cursor",
                fields: { cursorParam: "nextToken", cursorSource: "body", cursorPath: "$.nextToken" },
              },
            },
            transforms: [
              { id: "t-fs-1", kind: "extract", config: { attribute: "severity", path: "$.eventSeverityCat", default: "LOW" } },
            ],
            testResult: {
              ok: true,
              records: [
                { incidentId: 88121, eventSeverityCat: "HIGH", incidentTitle: "Brute-force attempt on vpn-gw01", srcIp: "203.0.113.44" },
                { incidentId: 88122, eventSeverityCat: "LOW", incidentTitle: "Interface flap on sw-edge07", srcIp: "10.4.2.7" },
              ],
              detectedFields: ["incidentId", "eventSeverityCat", "incidentTitle", "srcIp"],
              testedAt: daysAgo(1),
            },
          },
          {
            id: "b-fs-critical",
            adapter: "kafka_kc",
            name: "Critical to Lakehouse",
            parentId: "b-fs-fetch",
            branch: { name: "critical", rules: [{ field: "severity", op: "equals", value: "HIGH" }] },
            serviceId: "svc-iceberg",
            entity: "incident",
            config: { sinkServiceId: "svc-iceberg" },
            transforms: [],
          },
          {
            id: "b-fs-all",
            adapter: "kafka",
            mode: "write",
            name: "All Events Topic",
            parentId: "b-fs-fetch",
            entity: "event",
            config: {},
            transforms: [
              {
                id: "t-fs-3",
                kind: "dedup",
                config: { identityFields: ["incidentId"], excludedFields: ["lastSeen"], windowHours: 24 },
              },
            ],
          },
          {
            id: "b-fs-os",
            adapter: "kc",
            name: "SOC OpenSearch Feed",
            parentId: "t-flow-fs-events",
            serviceId: "svc-opensearch",
            entity: "event",
            config: {
              attachTopicId: "t-flow-fs-events",
              initialPosition: "beginning",
              sinkConfig: {
                "connector.class": "io.aiven.kafka.connect.opensearch.OpensearchSinkConnector",
                "tasks.max": "3",
                "key.ignore": "false",
                "schema.ignore": "true",
                "batch.size": "2000",
                "flush.timeout.ms": "30000",
                "behavior.on.malformed.documents": "warn",
                "errors.tolerance": "all",
              },
            },
            transforms: [],
          },
        ],
        topics: [
          { id: "t-flow-fs-incident", kind: "materialized", name: "raw.fortisiem_events.incident", sealed: true, writerBlockId: "b-fs-critical" },
          { id: "t-flow-fs-events", kind: "materialized", name: "raw.fortisiem_events.event", sealed: false, writerBlockId: "b-fs-all", backlogEstimate: 48210 },
        ],
        variables: [{ name: "fortisiem_base_path", value: "/phoenix/rest", secret: false }],
        servicePins: { "svc-fortisiem": 2, "svc-iceberg": 1, "svc-opensearch": 1 },
        deployedAt: daysAgo(8),
        lastRunAt: minutesAgo(9),
        createdAt: daysAgo(21),
        updatedAt: minutesAgo(9),
      },

      // 3 — jdbc incremental → topic → kc, Stopped, "service update available"
      {
        id: "flow-cmdb",
        name: "CMDB Asset Sync",
        description: "Incremental replication of the CMDB asset table into a topic mirrored to the SOC OpenSearch.",
        state: "Stopped",
        enabled: true,
        cron: "0 2 * * *",
        blocks: [
          {
            id: "b-cmdb-read",
            adapter: "jdbc",
            mode: "read",
            name: "Read CMDB Assets",
            parentId: null,
            serviceId: "svc-postgres",
            config: {
              table: "cmdb_assets",
              columns: ["asset_id", "hostname", "owner_group", "environment", "updated_at"],
              incremental: true,
              watermarkColumn: "updated_at",
              initialPosition: "oldest",
            },
            transforms: [
              { id: "t-cmdb-1", kind: "rename", config: { from: "owner_group", to: "team" } },
              { id: "t-cmdb-2", kind: "coerce", config: { field: "asset_id", type: "string" } },
            ],
            testResult: {
              ok: true,
              records: [
                { asset_id: 40122, hostname: "db-prod-03", owner_group: "dba", environment: "production", updated_at: "2026-08-09T18:22:10Z" },
              ],
              detectedFields: ["asset_id", "hostname", "owner_group", "environment", "updated_at"],
              testedAt: daysAgo(5),
            },
          },
          {
            id: "b-cmdb-write",
            adapter: "kafka",
            mode: "write",
            name: "Assets Topic",
            parentId: "b-cmdb-read",
            entity: "asset",
            config: {},
            transforms: [],
          },
          {
            id: "b-cmdb-os",
            adapter: "kc",
            name: "OpenSearch Assets Index",
            parentId: "t-flow-cmdb",
            serviceId: "svc-opensearch",
            entity: "asset",
            config: {
              attachTopicId: "t-flow-cmdb",
              initialPosition: "beginning",
              sinkConfig: {
                "connector.class": "io.aiven.kafka.connect.opensearch.OpensearchSinkConnector",
                "tasks.max": "1",
                "key.ignore": "false",
                "schema.ignore": "true",
                "batch.size": "1000",
                "flush.timeout.ms": "20000",
                "behavior.on.malformed.documents": "warn",
              },
            },
            transforms: [],
          },
        ],
        topics: [
          { id: "t-flow-cmdb", kind: "materialized", name: "raw.cmdb_asset_sync.asset", sealed: false, writerBlockId: "b-cmdb-write", backlogEstimate: 191_004 },
        ],
        variables: [],
        servicePins: { "svc-postgres": 1, "svc-opensearch": 1 },
        deployedAt: daysAgo(15),
        lastRunAt: daysAgo(1),
        createdAt: daysAgo(40),
        updatedAt: daysAgo(1),
      },

      // 4 — adopted-topic root, continuous, Paused
      {
        id: "flow-partner",
        name: "Partner Threat Feed",
        description: "Relay of the partner SIEM's indicator topic onto the platform cluster, deduplicated.",
        state: "Paused",
        enabled: true,
        cron: null,
        blocks: [
          {
            id: "b-pt-read",
            adapter: "kafka",
            mode: "read",
            name: "Consume Partner Feed",
            parentId: "t-flow-pt-adopted",
            serviceId: "svc-partner-kafka",
            config: { parseFormat: "json", initialPosition: "beginning" },
            transforms: [
              { id: "t-pt-1", kind: "add_field", config: { field: "feed_source", value: "partner-siem" } },
              { id: "t-pt-2", kind: "dedup", config: { identityFields: ["indicator_id"], excludedFields: [], windowHours: 24 } },
            ],
            testResult: {
              ok: true,
              records: [{ indicator_id: "ioc-7781", type: "ip", value: "198.51.100.23", confidence: 82 }],
              detectedFields: ["indicator_id", "type", "value", "confidence"],
              testedAt: daysAgo(3),
            },
          },
          {
            id: "b-pt-write",
            adapter: "kafka",
            mode: "write",
            name: "Indicators Topic",
            parentId: "b-pt-read",
            entity: "indicator",
            config: {},
            transforms: [],
          },
        ],
        topics: [
          { id: "t-flow-pt-adopted", kind: "adopted", name: "partner.threatfeed.indicators", sealed: false, backlogEstimate: 182_400 },
          { id: "t-flow-pt-out", kind: "materialized", name: "raw.partner_threat_feed.indicator", sealed: false, writerBlockId: "b-pt-write" },
        ],
        variables: [],
        servicePins: { "svc-partner-kafka": 1 },
        deployedAt: daysAgo(6),
        lastRunAt: hoursAgo(31),
        createdAt: daysAgo(19),
        updatedAt: hoursAgo(31),
      },

      // 5 — Draft with validation issues (missing entity, ceremony, ${scan_id}, retired service)
      {
        id: "flow-vulnscan",
        name: "Vulnerability Scan Delta",
        description: "Draft: per-scan vulnerability deltas into the lakehouse.",
        state: "Draft",
        enabled: false,
        cron: null,
        blocks: [
          {
            id: "b-vs-list",
            adapter: "http",
            mode: "read",
            name: "List Scans",
            parentId: null,
            serviceId: "svc-rapid7",
            config: {
              method: "GET",
              path: "/api/3/scans",
              responseFormat: "json",
              recordPath: "$.resources[*]",
              split: true,
              pagination: { type: "offset", fields: { offsetParam: "offset", limitParam: "limit", limitValue: "200", stop: "empty_response" } },
            },
            transforms: [],
            testResult: null,
          },
          {
            id: "b-vs-detail",
            adapter: "http",
            mode: "lookup",
            name: "Scan Details",
            parentId: "b-vs-list",
            serviceId: "svc-qualys",
            config: {
              method: "GET",
              path: "/api/2.0/fo/scan/${scan_id}",
              responseFormat: "json",
              recordPath: "$.scan",
              split: false,
              pagination: { type: "none", fields: {} },
              lookupJoinField: "scan_id",
            },
            transforms: [],
            testResult: { ok: false, reason: "Service is retired — 410 Gone from the gateway.", testedAt: daysAgo(2) },
          },
          {
            id: "b-vs-sink",
            adapter: "kafka_kc",
            name: "Scan Deltas to Lakehouse",
            parentId: "b-vs-detail",
            serviceId: "svc-iceberg",
            entity: null,
            config: { sinkServiceId: "svc-iceberg" },
            transforms: [],
          },
        ],
        topics: [],
        variables: [],
        servicePins: {},
        deployedAt: null,
        lastRunAt: null,
        createdAt: daysAgo(4),
        updatedAt: hoursAgo(20),
      },

      // 6 — three named branches (two conditional, one taking everything),
      //     custom topic override, Stopped
      {
        id: "flow-retirement",
        name: "Asset Retirement",
        description: "Retirement notices fanned out: everything governed to the lakehouse, active assets to a topic, decommissioned assets to a custom-named topic.",
        state: "Stopped",
        enabled: true,
        cron: "0 6 * * 1",
        blocks: [
          {
            id: "b-ret-fetch",
            adapter: "http",
            mode: "read",
            name: "Fetch Retirement Notices",
            parentId: null,
            serviceId: "svc-servicenow",
            config: {
              method: "GET",
              path: "/api/now/table/cmdb_ci_retired",
              responseFormat: "json",
              recordPath: "$.result[*]",
              split: true,
              pagination: { type: "offset", fields: { offsetParam: "sysparm_offset", limitParam: "sysparm_limit", limitValue: "1000", stop: "empty_response" } },
            },
            transforms: [{ id: "t-ret-0", kind: "extract", config: { attribute: "lifecycle_state", path: "$.install_status", default: "unknown" } }],
            testResult: {
              ok: true,
              records: [
                { sys_id: "a91f", name: "srv-legacy-11", install_status: "retired", decommission_date: "2026-07-30" },
                { sys_id: "b23c", name: "srv-app-04", install_status: "in_use", decommission_date: null },
              ],
              detectedFields: ["sys_id", "name", "install_status", "decommission_date"],
              testedAt: daysAgo(9),
            },
          },
          {
            id: "b-ret-all",
            adapter: "kafka_kc",
            name: "All Notices to Lakehouse",
            parentId: "b-ret-fetch",
            branch: { name: "all" },
            serviceId: "svc-iceberg",
            entity: "asset",
            config: { sinkServiceId: "svc-iceberg" },
            transforms: [],
          },
          {
            id: "b-ret-active",
            adapter: "kafka",
            mode: "write",
            name: "Active Assets",
            parentId: "b-ret-fetch",
            branch: { name: "active", rules: [{ field: "install_status", op: "equals", value: "in_use" }] },
            entity: "asset",
            config: {},
            transforms: [],
          },
          {
            id: "b-ret-decom",
            adapter: "kafka",
            mode: "write",
            name: "Decommissioned Assets",
            parentId: "b-ret-fetch",
            branch: { name: "decommissioned", rules: [{ field: "install_status", op: "equals", value: "retired" }] },
            entity: "asset",
            topicOverride: "asset_retired",
            config: {},
            transforms: [],
          },
        ],
        topics: [
          { id: "t-flow-ret-all", kind: "materialized", name: "raw.asset_retirement.asset", sealed: true, writerBlockId: "b-ret-all" },
          { id: "t-flow-ret-active", kind: "materialized", name: "raw.asset_retirement.asset.active", sealed: false, writerBlockId: "b-ret-active" },
          { id: "t-flow-ret-decom", kind: "materialized", name: "asset_retired", sealed: false, writerBlockId: "b-ret-decom" },
        ],
        variables: [],
        servicePins: { "svc-servicenow": 1, "svc-iceberg": 1 },
        drift:
          'Compression Type edited out-of-band on "All Notices to Lakehouse" (snappy → gzip). Shown, never merged — Redeploy compiles it back.',
        deployedAt: daysAgo(20),
        lastRunAt: daysAgo(7),
        createdAt: daysAgo(45),
        updatedAt: daysAgo(7),
      },

      // 7 — raw-branch quarantine (R8), continuous, Running with drift warning
      {
        id: "flow-auditmirror",
        name: "Audit Mirror",
        description: "Byte-for-byte mirror of the legacy audit topic. Raw mode — no transformations are possible on this branch.",
        state: "Degraded",
        enabled: true,
        cron: null,
        blocks: [
          {
            id: "b-am-read",
            adapter: "kafka",
            mode: "read",
            name: "Read Legacy Audit",
            parentId: null,
            config: { topicName: "audit.raw.legacy", parseFormat: "raw", initialPosition: "beginning" },
            transforms: [],
            testResult: {
              ok: true,
              records: ["(binary payload · 412 bytes)", "(binary payload · 388 bytes)"],
              detectedFields: [],
              testedAt: daysAgo(14),
            },
          },
          {
            id: "b-am-write",
            adapter: "kafka",
            mode: "write",
            name: "Mirror Topic",
            parentId: "b-am-read",
            entity: "audit_event",
            config: {},
            transforms: [],
          },
        ],
        topics: [
          { id: "t-flow-am-out", kind: "materialized", name: "raw.audit_mirror.audit_event", sealed: false, writerBlockId: "b-am-write" },
        ],
        variables: [],
        servicePins: {},
        drift:
          "Process group missing on Production NiFi — the instance fingerprint matches the one recorded at deploy, so it was deleted out-of-band (not moved). Reads never repair: see Runtime → Drift.",
        deployedAt: daysAgo(60),
        lastRunAt: minutesAgo(1),
        createdAt: daysAgo(70),
        updatedAt: minutesAgo(1),
      },
    ],

    // ----------------------------------------------------------- schemas
    schemas: [
      {
        id: "schema-r7-asset",
        subject: "raw.rapid7_assets.asset-value",
        entity: "asset",
        flowId: "flow-rapid7",
        blockId: "b-r7-sink",
        provenance: "sample_run",
        fields: [
          { name: "id", type: "long", doc: "InsightVM asset id" },
          { name: "hostName", type: "string" },
          { name: "os", type: "string", nullable: true },
          { name: "riskScore", type: "double" },
          { name: "siteId", type: "int" },
        ],
        rawAvro: R7_ASSET_AVRO_V2,
        approvedAt: daysAgo(12),
        registryGlobalId: 3011,
        approvals: [
          {
            version: 1,
            approvedAt: daysAgo(26),
            provenance: "manual",
            registryGlobalId: 2951,
            rawAvro: R7_ASSET_AVRO_V1,
            supersededAt: daysAgo(12),
          },
          {
            version: 2,
            approvedAt: daysAgo(12),
            provenance: "sample_run",
            registryGlobalId: 3011,
            rawAvro: R7_ASSET_AVRO_V2,
          },
        ],
      },
      {
        id: "schema-fs-incident",
        subject: "raw.fortisiem_events.incident-value",
        entity: "incident",
        flowId: "flow-fortisiem",
        blockId: "b-fs-critical",
        provenance: "uploaded",
        fields: [
          { name: "incidentId", type: "long" },
          { name: "incidentTitle", type: "string" },
          { name: "eventSeverityCat", type: "string" },
          { name: "srcIp", type: "string", nullable: true },
          { name: "reportingDevice", type: "string", nullable: true },
        ],
        rawAvro: FS_INCIDENT_AVRO,
        approvedAt: daysAgo(8),
        registryGlobalId: 3025,
        approvals: [
          {
            version: 1,
            approvedAt: daysAgo(8),
            provenance: "uploaded",
            registryGlobalId: 3025,
            rawAvro: FS_INCIDENT_AVRO,
            prefilledFromLabel: "Security incident envelope",
          },
        ],
      },
      {
        id: "schema-ret-asset",
        subject: "raw.asset_retirement.asset-value",
        entity: "asset",
        flowId: "flow-retirement",
        blockId: "b-ret-all",
        provenance: "manual",
        fields: [
          { name: "sys_id", type: "string" },
          { name: "name", type: "string" },
          { name: "install_status", type: "string" },
          { name: "decommission_date", type: "string", nullable: true },
        ],
        rawAvro: RET_ASSET_AVRO,
        approvedAt: daysAgo(20),
        registryGlobalId: 2988,
        approvals: [
          {
            version: 1,
            approvedAt: daysAgo(20),
            provenance: "manual",
            registryGlobalId: 2988,
            rawAvro: RET_ASSET_AVRO,
          },
        ],
      },
    ],

    // ------------------------------------------- library schema templates
    // Unregistered, hand-authored, bound to nothing. Deliberately NOT in
    // `schemas` — that array's length guards the Apicurio connection.
    schemaTemplates: [
      {
        id: "tpl-incident-envelope",
        name: "Security incident envelope",
        description: "Vendor-neutral incident shape — the starting point for any SIEM feed.",
        rawAvro: TEMPLATE_INCIDENT_AVRO,
        createdAt: daysAgo(24),
        updatedAt: daysAgo(9),
      },
      {
        id: "tpl-asset-record",
        name: "Canonical asset",
        description: "Shared asset shape used by the CMDB sync and the scanner inventories.",
        rawAvro: TEMPLATE_ASSET_AVRO,
        createdAt: daysAgo(18),
        updatedAt: daysAgo(18),
      },
      {
        id: "tpl-threat-indicator",
        name: "Threat indicator",
        description: "Normalised partner-feed indicator (IP / domain / URL / hash / email).",
        rawAvro: TEMPLATE_INDICATOR_AVRO,
        createdAt: daysAgo(11),
        updatedAt: daysAgo(5),
      },
    ],

    // Next Apicurio global id to allocate — strictly above every seeded id
    // (highest seeded: 3025). Never derive an id from an array length.
    registryGlobalIdSeq: 3101,

    // ------------------------------------------------------- connections
    connections: [
      {
        id: "conn-nifi-prod",
        type: "nifi",
        name: "Production NiFi",
        active: true,
        health: "Healthy",
        reachability: "Reachable",
        lastTestedAt: hoursAgo(3),
        config: { url: "https://nifi.internal.corp:8443", authMode: "bearer" },
        hasSecret: true,
      },
      {
        id: "conn-nifi-staging",
        type: "nifi",
        name: "Staging NiFi",
        active: false,
        health: "Not Tested",
        reachability: "Unknown",
        lastTestedAt: null,
        config: { url: "https://nifi-staging.internal.corp:8443", authMode: "bearer" },
        hasSecret: true,
      },
      {
        id: "conn-kafka-primary",
        type: "kafka",
        name: "Primary Kafka Cluster",
        active: true,
        health: "Healthy",
        reachability: "Reachable",
        lastTestedAt: hoursAgo(3),
        config: { bootstrapServers: "kafka-1.internal.corp:9094,kafka-2.internal.corp:9094", mode: "native", securityProtocol: "SASL_SSL" },
        hasSecret: true,
      },
      {
        id: "conn-kafka-dr",
        type: "kafka",
        name: "DR Kafka Cluster",
        active: false,
        health: "Not Tested",
        reachability: "Unknown",
        lastTestedAt: null,
        config: { bootstrapServers: "kafka-dr.internal.corp:9094", mode: "native", securityProtocol: "SASL_SSL" },
        hasSecret: true,
      },
      {
        id: "conn-apicurio",
        type: "apicurio",
        name: "Apicurio Schema Registry",
        active: true,
        health: "Healthy",
        reachability: "Reachable",
        lastTestedAt: hoursAgo(3),
        config: { url: "http://apicurio.internal.corp:8081", authMode: "none" },
        hasSecret: false,
      },
      {
        id: "conn-connect-prod",
        type: "kafka_connect",
        name: "Kafka Connect Cluster",
        active: true,
        health: "Healthy",
        reachability: "Reachable",
        lastTestedAt: hoursAgo(3),
        config: { url: "http://connect.internal.corp:8083" },
        hasSecret: false,
      },
      {
        id: "conn-connect-legacy",
        type: "kafka_connect",
        name: "Legacy Kafka Connect",
        active: false,
        health: "Failed",
        reachability: "Unreachable",
        lastTestedAt: daysAgo(2),
        config: { url: "http://connect-old.internal.corp:8083" },
        hasSecret: false,
      },
      {
        id: "conn-redis",
        type: "redis",
        name: "Dedup Redis",
        active: true,
        health: "Healthy",
        reachability: "Reachable",
        lastTestedAt: hoursAgo(3),
        config: { host: "redis.internal.corp", port: 6379, dedupDb: 2, bookmarksDb: 3, mode: "standalone" },
        hasSecret: true,
      },
      {
        id: "conn-apisix",
        type: "apisix",
        name: "APISIX Gateway",
        active: true,
        health: "Healthy",
        reachability: "Reachable",
        lastTestedAt: hoursAgo(6),
        config: { adminUrl: "http://apisix-admin.internal.corp:9180", runtimeUrl: "http://apisix.internal.corp:9080" },
        hasSecret: true,
      },
    ],

    gateway: {
      certProfiles: [
        { id: "gw-cert-fortisiem", name: "FortiSIEM Client Cert", subject: "CN=dmp-egress,O=DataPASC", expiresAt: daysAgo(-200), refCount: 1 },
        { id: "gw-cert-partner", name: "Partner mTLS Cert", subject: "CN=dmp-partner-egress,O=DataPASC", expiresAt: daysAgo(-46), refCount: 1 },
      ],
      // Admin-gated. A proxy whose targetHost is missing here cannot deploy.
      allowlist: ["fortisiem.internal.corp", "legacy-scanner.dmz.corp"],
    },

    // ------------------------------------------------- APISIX proxy catalog
    gatewayProxies: [
      {
        id: "gw-proxy-fortisiem",
        name: "FortiSIEM egress",
        description: "Client-cert egress to the FortiSIEM appliance. Referenced by the FortiSIEM Events flow.",
        targetHost: "fortisiem.internal.corp",
        port: 443,
        sni: "fortisiem.internal.corp",
        connectTimeoutMs: 5_000,
        readTimeoutMs: 30_000,
        path: "/phoenix/rest",
        methods: ["GET", "POST"],
        certProfileId: "gw-cert-fortisiem",
        status: "Reconciled",
        createdAt: daysAgo(28),
        updatedAt: daysAgo(9),
      },
      {
        id: "gw-proxy-partner",
        name: "Partner threat API",
        description: "Outbound to the partner threat-exchange REST API. Host is NOT allowlisted yet — deploy will refuse.",
        targetHost: "api.threatexchange.partner.example",
        port: 443,
        sni: "api.threatexchange.partner.example",
        connectTimeoutMs: 4_000,
        readTimeoutMs: 20_000,
        path: "/v2/indicators",
        methods: ["GET"],
        certProfileId: "gw-cert-partner",
        status: "Reconciled",
        createdAt: daysAgo(13),
        updatedAt: daysAgo(13),
      },
      {
        id: "gw-proxy-legacy-scanner",
        name: "Legacy scanner egress",
        description: "DMZ scanner API used by the retired Qualys integration.",
        targetHost: "legacy-scanner.dmz.corp",
        port: 8443,
        sni: "legacy-scanner.dmz.corp",
        connectTimeoutMs: 3_000,
        readTimeoutMs: 15_000,
        path: "/api/2.0/fo",
        methods: ["GET", "POST"],
        certProfileId: null,
        status: "Failed",
        statusDetail: "APISIX admin API rejected the upstream: TLS handshake failed — the scanner presents an expired self-signed certificate.",
        createdAt: daysAgo(21),
        updatedAt: daysAgo(2),
      },
    ],

    // ---------------------------------------------------------- services
    services: [
      {
        id: "svc-rapid7",
        type: "http",
        name: "Rapid7 InsightVM API",
        revision: 1,
        retired: false,
        health: "Healthy",
        lastTestedAt: daysAgo(1),
        config: { baseUrl: "https://insightvm.corp.local:3780", authMode: "api_key", keyLocation: "header", keyName: "X-Api-Key" },
        hasSecret: true,
        createdAt: daysAgo(31),
        updatedAt: daysAgo(31),
      },
      {
        id: "svc-fortisiem",
        type: "http",
        name: "FortiSIEM Events API",
        revision: 2,
        retired: false,
        health: "Healthy",
        lastTestedAt: daysAgo(1),
        config: {
          baseUrl: "https://fortisiem.internal.corp",
          authMode: "session_token",
          loginPath: "/phoenix/rest/h5/sec/login",
          tokenPath: "$.sessionToken",
          tokenHeader: "Authorization",
          // Egress is a property of the host, so it lives on the service: every
          // block calling FortiSIEM leaves through the same proxy.
          proxyId: "gw-proxy-fortisiem",
        },
        hasSecret: true,
        createdAt: daysAgo(22),
        updatedAt: daysAgo(3),
      },
      {
        id: "svc-servicenow",
        type: "http",
        name: "ServiceNow CMDB API",
        revision: 1,
        retired: false,
        health: "Healthy",
        lastTestedAt: daysAgo(9),
        config: { baseUrl: "https://corp.service-now.com", authMode: "oauth2", tokenUrl: "https://corp.service-now.com/oauth_token.do" },
        hasSecret: true,
        createdAt: daysAgo(46),
        updatedAt: daysAgo(46),
      },
      {
        id: "svc-postgres",
        type: "database",
        name: "Security Postgres",
        revision: 2,
        retired: false,
        health: "Healthy",
        lastTestedAt: daysAgo(2),
        config: { dialect: "postgresql", host: "pg-sec.internal.corp", port: 5432, database: "secops", username: "dmp_reader", capabilities: ["read"] },
        hasSecret: true,
        createdAt: daysAgo(41),
        updatedAt: daysAgo(2),
      },
      {
        id: "svc-trino",
        type: "database",
        name: "Trino Lakehouse",
        revision: 1,
        retired: false,
        health: "Not Tested",
        lastTestedAt: null,
        config: { dialect: "trino", host: "trino.internal.corp", port: 8080, database: "iceberg/bronze", username: "dmp", capabilities: ["read"] },
        hasSecret: false,
        createdAt: daysAgo(18),
        updatedAt: daysAgo(18),
      },
      {
        id: "svc-partner-kafka",
        type: "external_kafka",
        name: "Partner SIEM Kafka",
        revision: 1,
        retired: false,
        health: "Healthy",
        lastTestedAt: daysAgo(3),
        config: { bootstrapServers: "kafka.partner-siem.example:9093", securityProtocol: "SASL_SSL", note: "Input only — never a destination." },
        hasSecret: true,
        createdAt: daysAgo(20),
        updatedAt: daysAgo(20),
      },
      {
        id: "svc-iceberg",
        type: "sink_destination",
        name: "Iceberg Bronze Catalog",
        revision: 1,
        retired: false,
        health: "Healthy",
        lastTestedAt: daysAgo(1),
        config: { kind: "iceberg_catalog", catalogUrl: "http://polaris.internal.corp:8181/api/catalog", warehouse: "bronze" },
        hasSecret: true,
        createdAt: daysAgo(35),
        updatedAt: daysAgo(35),
      },
      {
        id: "svc-opensearch",
        type: "sink_destination",
        name: "SOC OpenSearch",
        revision: 1,
        retired: false,
        health: "Healthy",
        lastTestedAt: daysAgo(1),
        config: { kind: "opensearch", url: "https://opensearch.soc.corp:9200", indexPrefix: "dmp-", writeMode: "upsert" },
        hasSecret: true,
        createdAt: daysAgo(28),
        updatedAt: daysAgo(28),
      },
      {
        id: "svc-qualys",
        type: "http",
        name: "Legacy Qualys API",
        revision: 4,
        retired: true,
        health: "Failed",
        lastTestedAt: daysAgo(2),
        config: { baseUrl: "https://qualysapi.qualys.eu", authMode: "basic" },
        hasSecret: true,
        createdAt: daysAgo(300),
        updatedAt: daysAgo(2),
      },
    ],

    // ------------------------------------------------------------- audit
    audit: [
      { id: "a-1", ts: minutesAgo(9), user: "admin", action: "Flow run completed", object: "Flow", target: "FortiSIEM Events", status: "Success", details: "1,204 records · 2 to DLQ" },
      { id: "a-2", ts: hoursAgo(4), user: "admin", action: "Flow run completed", object: "Flow", target: "Rapid7 Assets", status: "Success", details: "8,911 records" },
      { id: "a-3", ts: hoursAgo(20), user: "j.okafor", action: "Draft saved", object: "Flow", target: "Vulnerability Scan Delta", status: "Success" },
      { id: "a-4", ts: hoursAgo(31), user: "admin", action: "Flow paused", object: "Flow", target: "Partner Threat Feed", status: "Success", details: "Trigger keeps firing; records queue until Resume" },
      { id: "a-5", ts: daysAgo(1), user: "s.lindgren", action: "Service revision created", object: "Application Service", target: "Security Postgres (rev 2)", status: "Success", details: "Host changed — linked flows adopt at next deploy" },
      { id: "a-6", ts: daysAgo(2), user: "admin", action: "Service retired", object: "Application Service", target: "Legacy Qualys API", status: "Warning", details: "1 dependent flow flagged: action required" },
      { id: "a-7", ts: daysAgo(2), user: "admin", action: "Connection test failed", object: "Platform Connection", target: "Legacy Kafka Connect", status: "Failed", details: "Connection refused" },
      { id: "a-8", ts: daysAgo(3), user: "admin", action: "Dedup cache cleared", object: "Stream", target: "Consume Partner Feed", status: "Success", details: "24h window reset" },
      { id: "a-9", ts: daysAgo(6), user: "admin", action: "Flow deployed", object: "Flow", target: "Partner Threat Feed", status: "Success" },
      { id: "a-10", ts: daysAgo(8), user: "admin", action: "Schema approved", object: "Schema", target: "raw.fortisiem_events.incident-value", status: "Success", details: "Registered as global id 3025 · evidence: uploaded samples" },
      { id: "a-11", ts: daysAgo(8), user: "admin", action: "Flow deployed", object: "Flow", target: "FortiSIEM Events", status: "Success" },
      { id: "a-12", ts: daysAgo(9), user: "admin", action: "Gateway proxy reconciled", object: "Gateway", target: "FortiSIEM egress", status: "Success" },
      { id: "a-12b", ts: daysAgo(2), user: "admin", action: "Gateway proxy reconcile failed", object: "Gateway", target: "Legacy scanner egress", status: "Failed", details: "TLS handshake failed — expired self-signed certificate on legacy-scanner.dmz.corp" },
      { id: "a-13", ts: daysAgo(12), user: "admin", action: "Schema approved", object: "Schema", target: "raw.rapid7_assets.asset-value", status: "Success", details: "Registered as global id 3011 · evidence: live sample run" },
      { id: "a-14", ts: daysAgo(12), user: "admin", action: "Flow deployed", object: "Flow", target: "Rapid7 Assets", status: "Success" },
      { id: "a-15", ts: daysAgo(12), user: "admin", action: "Connector published", object: "Connector", target: "rapid7-to-iceberg@1", status: "Success" },
      { id: "a-16", ts: daysAgo(14), user: "admin", action: "Connection activated", object: "Platform Connection", target: "Dedup Redis", status: "Success" },
      { id: "a-17", ts: daysAgo(15), user: "m.haddad", action: "Flow stopped", object: "Flow", target: "CMDB Asset Sync", status: "Success", details: "Queues retained" },
      { id: "a-17b", ts: daysAgo(16), user: "m.haddad", action: "Flow redeployed", object: "Flow", target: "CMDB Asset Sync", status: "Success", details: "Shape unchanged · pinned service revisions refreshed" },
      { id: "a-18", ts: daysAgo(20), user: "admin", action: "Schema approved", object: "Schema", target: "raw.asset_retirement.asset-value", status: "Success", details: "Manually authored — not sample-validated" },
      { id: "a-19", ts: daysAgo(21), user: "admin", action: "Connection change completed", object: "Platform Connection", target: "Production NiFi", status: "Success", details: "6 flows verified" },
      { id: "a-19b", ts: daysAgo(26), user: "admin", action: "Schema approved", object: "Schema", target: "raw.rapid7_assets.asset-value", status: "Success", details: "Registered as global id 2951 · evidence: manually authored — not sample-validated" },
      { id: "a-20", ts: daysAgo(28), user: "admin", action: "Connection created", object: "Platform Connection", target: "APISIX Gateway", status: "Success" },
    ],

    // --------------------------------------------------------------- dlq
    dlq: [
      { id: "d-1", flowId: "flow-fortisiem", ts: minutesAgo(12), blockName: "Fetch Incidents", errorClass: "PARSE_FAILURE", payloadPreview: '{"incidentId": "not-a-number", "incidentTitle": "Malformed record from collector 7"…' },
      { id: "d-2", flowId: "flow-fortisiem", ts: minutesAgo(41), blockName: "All Events Topic", errorClass: "MISSING_IDENTITY_FIELD", payloadPreview: '{"incidentTitle": "Heartbeat", "eventSeverityCat": "LOW"…  (no incidentId — dedup identity missing)' },
      { id: "d-3", flowId: "flow-fortisiem", ts: hoursAgo(3), blockName: "Fetch Incidents", errorClass: "PARSE_FAILURE", payloadPreview: '<?xml version="1.0"?><error>Session expired</error>' },
      { id: "d-4", flowId: "flow-fortisiem", ts: hoursAgo(7), blockName: "Critical to Lakehouse", errorClass: "SINK_REJECTED", payloadPreview: '{"incidentId": 87001, "incidentTitle": "…", "srcIp": "203.0.113.9"} — Iceberg commit conflict, retried 3×' },
      { id: "d-5", flowId: "flow-fortisiem", ts: hoursAgo(9), blockName: "Fetch Incidents", errorClass: "PARSE_FAILURE", payloadPreview: '{"incidents": null} — record path $.incidents[*] matched nothing' },
      { id: "d-6", flowId: "flow-fortisiem", ts: daysAgo(1), blockName: "All Events Topic", errorClass: "MISSING_IDENTITY_FIELD", payloadPreview: '{"eventSeverityCat": "LOW", "reportingDevice": "sw-edge07"…' },
    ],

    // ----------------------------------------------------------- metrics
    metrics: [
      {
        flowId: "flow-rapid7",
        records24h: 35_644,
        errors24h: 0,
        queued: 0,
        perBlock: [
          { blockId: "b-r7-list", label: "http · List Assets", recordsIn: 0, recordsOut: 35_644, queued: 0 },
          { blockId: "b-r7-sink", label: "kafka+connect · Assets to Iceberg", recordsIn: 35_644, recordsOut: 35_644, queued: 0 },
        ],
        topicCounts: [{ topic: "raw.rapid7_assets.asset", messages: 1_204_112 }],
        lastRunOutcome: "Success",
      },
      {
        flowId: "flow-fortisiem",
        records24h: 118_202,
        errors24h: 6,
        queued: 40,
        perBlock: [
          { blockId: "b-fs-fetch", label: "http · Fetch Incidents", recordsIn: 0, recordsOut: 118_202, queued: 40 },
          { blockId: "b-fs-critical", label: "kafka+connect · Critical to Lakehouse", recordsIn: 3_711, recordsOut: 3_709, queued: 0 },
          { blockId: "b-fs-all", label: "kafka · All Events Topic", recordsIn: 114_491, recordsOut: 114_489, queued: 0 },
          { blockId: "b-fs-os", label: "kc · SOC OpenSearch Feed", recordsIn: 114_489, recordsOut: 114_489, queued: 0 },
        ],
        topicCounts: [
          { topic: "raw.fortisiem_events.incident", messages: 48_003 },
          { topic: "raw.fortisiem_events.event", messages: 2_301_552 },
        ],
        lastRunOutcome: "Success",
      },
      {
        flowId: "flow-cmdb",
        records24h: 0,
        errors24h: 0,
        queued: 0,
        perBlock: [
          { blockId: "b-cmdb-read", label: "jdbc · Read CMDB Assets", recordsIn: 0, recordsOut: 0, queued: 0 },
          { blockId: "b-cmdb-write", label: "kafka · Assets Topic", recordsIn: 0, recordsOut: 0, queued: 0 },
          { blockId: "b-cmdb-os", label: "kc · OpenSearch Assets Index", recordsIn: 0, recordsOut: 0, queued: 0 },
        ],
        topicCounts: [{ topic: "raw.cmdb_asset_sync.asset", messages: 191_004 }],
        lastRunOutcome: "Success",
      },
      {
        flowId: "flow-partner",
        records24h: 0,
        errors24h: 0,
        queued: 12_882,
        perBlock: [
          { blockId: "b-pt-read", label: "kafka · Consume Partner Feed", recordsIn: 0, recordsOut: 0, queued: 12_882 },
          { blockId: "b-pt-write", label: "kafka · Indicators Topic", recordsIn: 0, recordsOut: 0, queued: 0 },
        ],
        topicCounts: [{ topic: "raw.partner_threat_feed.indicator", messages: 977_310 }],
        lastRunOutcome: "Skipped (overlap)",
      },
      {
        flowId: "flow-retirement",
        records24h: 0,
        errors24h: 0,
        queued: 0,
        perBlock: [
          { blockId: "b-ret-fetch", label: "http · Fetch Retirement Notices", recordsIn: 0, recordsOut: 0, queued: 0 },
          { blockId: "b-ret-all", label: "kafka+connect · All Notices to Lakehouse", recordsIn: 0, recordsOut: 0, queued: 0 },
          { blockId: "b-ret-active", label: "kafka · Active Assets", recordsIn: 0, recordsOut: 0, queued: 0 },
          { blockId: "b-ret-decom", label: "kafka · Decommissioned Assets", recordsIn: 0, recordsOut: 0, queued: 0 },
        ],
        topicCounts: [
          { topic: "raw.asset_retirement.asset", messages: 42_118 },
          { topic: "raw.asset_retirement.asset.active", messages: 39_004 },
          { topic: "asset_retired", messages: 3_114 },
        ],
        lastRunOutcome: "Success",
      },
      {
        flowId: "flow-auditmirror",
        records24h: 402_118,
        errors24h: 0,
        queued: 104_552,
        perBlock: [
          { blockId: "b-am-read", label: "kafka · Read Legacy Audit", recordsIn: 0, recordsOut: 402_118, queued: 104_552 },
          { blockId: "b-am-write", label: "kafka · Mirror Topic", recordsIn: 402_118, recordsOut: 402_118, queued: 0 },
        ],
        topicCounts: [{ topic: "raw.audit_mirror.audit_event", messages: 88_204_331 }],
        lastRunOutcome: "Success",
      },
    ],

    // -------------------------------------------------------- connectors
    connectors: [
      {
        id: "connector-r7",
        name: "rapid7-to-iceberg",
        version: 1,
        flowId: "flow-rapid7",
        description: "Rapid7 InsightVM asset inventory into an Iceberg bronze table.",
        createdAt: daysAgo(12),
      },
    ],

    // --------------------------------------------------- runtime records
    // One per deployed flow. Read-only everywhere in the UI — the replacement
    // for the alpha's controller-services manager and live processor editing.
    runtimes: buildRuntimes(),

    // Global variables were removed in seed v3 — per-flow `Flow.variables` stays.

    // ---------------------------------------------------- topic messages
    topicMessages: {
      "raw.fortisiem_events.event": [
        { offset: 2301551, ts: minutesAgo(2), key: "88129", value: '{"incidentId":88129,"incidentTitle":"Port scan detected on dmz segment","eventSeverityCat":"MEDIUM","srcIp":"198.51.100.7"}', bytes: 121 },
        { offset: 2301550, ts: minutesAgo(3), key: "88128", value: '{"incidentId":88128,"incidentTitle":"Failed login burst on vpn-gw01","eventSeverityCat":"HIGH","srcIp":"203.0.113.44"}', bytes: 118 },
        { offset: 2301549, ts: minutesAgo(5), key: "88127", value: '{"incidentId":88127,"incidentTitle":"Interface flap on sw-edge07","eventSeverityCat":"LOW","srcIp":"10.4.2.7"}', bytes: 109 },
      ],
      "raw.rapid7_assets.asset": [
        { offset: 1204111, ts: hoursAgo(4), key: "1205", value: '{"id":1205,"hostName":"srv-web02.dmz.corp","os":"Ubuntu 22.04","riskScore":18342.0,"siteId":3}', bytes: 96 },
        { offset: 1204110, ts: hoursAgo(4), key: "1204", value: '{"id":1204,"hostName":"srv-dc01.corp.local","os":"Windows Server 2022","riskScore":7211.0,"siteId":3}', bytes: 101 },
      ],
      "partner.threatfeed.indicators": [
        { offset: 182399, ts: hoursAgo(31), key: "ioc-7781", value: '{"indicator_id":"ioc-7781","type":"ip","value":"198.51.100.23","confidence":82}', bytes: 84 },
      ],
      "raw.cmdb_asset_sync.asset": [
        { offset: 191003, ts: daysAgo(1), key: "40123", value: '{"asset_id":"40123","hostname":"app-prod-11","team":"platform","environment":"production"}', bytes: 92 },
        { offset: 191002, ts: daysAgo(1), key: "40122", value: '{"asset_id":"40122","hostname":"db-prod-03","team":"dba","environment":"production"}', bytes: 86 },
      ],
      "asset_retired": [
        { offset: 3113, ts: daysAgo(7), key: "a91f", value: '{"sys_id":"a91f","name":"srv-legacy-11","install_status":"retired","decommission_date":"2026-07-30"}', bytes: 101 },
      ],
      "raw.audit_mirror.audit_event": [
        { offset: 88204330, ts: minutesAgo(1), key: null, value: null, bytes: 412 },
        { offset: 88204329, ts: minutesAgo(1), key: null, value: null, bytes: 388 },
      ],
    },
  };
}
