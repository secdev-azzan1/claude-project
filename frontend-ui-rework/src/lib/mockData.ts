export type FlowState = "Running" | "Stopped" | "Degraded" | "Error";
export type SchemaState =
  | "Draft"
  | "Schema Generated"
  | "Schema Registered"
  | "Pending Verification"
  | "Verified"
  | "Schema Outdated";
export type ConnHealth = "Healthy" | "Not Tested" | "Failed";
export type SourceType = "REST API" | "PostgreSQL" | "MongoDB" | "SMB" | "Webhook" | "Trino";

export const dashboardStats = {
  totalSources: 12,
  runningFlows: 7,
  verifiedSchemas: 9,
  failedConnections: 1,
  recentRuns: 184,
};

export const recentActivity = [
  { id: 1, time: "2m ago", user: "admin", action: "Started flow", target: "rapid7__assets", status: "success" },
  { id: 2, time: "14m ago", user: "admin", action: "Approved schema", target: "sentinelone__agents-value v3", status: "success" },
  { id: 3, time: "42m ago", user: "admin", action: "Generated schema", target: "fortisiem__devices", status: "info" },
  { id: 4, time: "1h ago", user: "admin", action: "Tested Kafka connection", target: "kafka-prod", status: "success" },
  { id: 5, time: "3h ago", user: "admin", action: "Created source", target: "smb__daily_reports", status: "info" },
  { id: 6, time: "5h ago", user: "admin", action: "Connection failed", target: "smb-archive", status: "error" },
];

export const flowSummary: { name: string; state: FlowState; topic: string }[] = [
  { name: "rapid7__assets", state: "Running", topic: "rapid7.assets.v2" },
  { name: "sentinelone__agents", state: "Running", topic: "sentinelone.agents.v3" },
  { name: "fortisiem__devices", state: "Degraded", topic: "fortisiem.devices.v1" },
  { name: "smb__daily_reports", state: "Stopped", topic: "smb.daily_reports.v1" },
  { name: "rapid7__vulns", state: "Error", topic: "rapid7.vulns.v1" },
];

export const connections = [
  {
    id: "kafka",
    name: "Kafka",
    description: "Bootstrap broker cluster (prod)",
    health: "Healthy" as ConnHealth,
    endpoint: "kafka-prod-01:9093,kafka-prod-02:9093",
    lastTested: "2 min ago",
  },
  {
    id: "apicurio",
    name: "Apicurio Schema Registry",
    description: "Avro schema registry",
    health: "Healthy" as ConnHealth,
    endpoint: "https://apicurio.internal/api",
    lastTested: "12 min ago",
  },
  {
    id: "nifi",
    name: "Apache NiFi",
    description: "NiFi cluster API for flow deployment",
    health: "Healthy" as ConnHealth,
    endpoint: "https://nifi.internal/nifi-api",
    lastTested: "5 min ago",
  },
  {
    id: "smb",
    name: "SMB Share",
    description: "Daily reports file share",
    health: "Failed" as ConnHealth,
    endpoint: "\\\\fileserver01\\reports",
    lastTested: "1 hr ago",
  },
];

export const schemas: { name: string; version: number; state: SchemaState; updated: string; stream: string }[] = [
  { name: "rapid7__assets-value", version: 2, state: "Verified", updated: "2 days ago", stream: "assets" },
  { name: "sentinelone__agents-value", version: 3, state: "Verified", updated: "5 hours ago", stream: "agents" },
  { name: "fortisiem__devices-value", version: 1, state: "Pending Verification", updated: "1 hour ago", stream: "devices" },
  { name: "smb__daily_reports-value", version: 1, state: "Schema Generated", updated: "3 hours ago", stream: "daily_reports" },
  { name: "rapid7__vulns-value", version: 1, state: "Schema Outdated", updated: "21 days ago", stream: "vulns" },
  { name: "sentinelone__sites-value", version: 1, state: "Draft", updated: "20 min ago", stream: "sites" },
];

export const rapid7AssetsAvro = {
  type: "record",
  name: "Asset",
  namespace: "com.rapid7.assets",
  fields: [
    { name: "id", type: "long", doc: "Unique asset identifier" },
    { name: "ip", type: "string", doc: "Primary IP address" },
    { name: "hostname", type: ["null", "string"], default: null, doc: "Resolved hostname" },
    { name: "os", type: ["null", "string"], default: null, doc: "Operating system" },
    { name: "risk_score", type: "double", doc: "Composite risk score" },
    { name: "last_scan", type: { type: "long", logicalType: "timestamp-millis" }, doc: "Last scan timestamp" },
  ],
};

export const flows = [
  {
    id: "f1",
    name: "rapid7__assets",
    state: "Running" as FlowState,
    source: "REST API",
    primaryStream: "assets",
    topic: "rapid7.assets.v2",
    schemaVersion: "v2",
    lastRun: "32s ago",
    records: 482310,
    errors: 0,
  },
  {
    id: "f2",
    name: "sentinelone__agents",
    state: "Running" as FlowState,
    source: "REST API",
    primaryStream: "agents",
    topic: "sentinelone.agents.v3",
    schemaVersion: "v3",
    lastRun: "1m ago",
    records: 91204,
    errors: 0,
  },
  {
    id: "f3",
    name: "fortisiem__devices",
    state: "Degraded" as FlowState,
    source: "PostgreSQL",
    primaryStream: "devices",
    topic: "fortisiem.devices.v1",
    schemaVersion: "v1",
    lastRun: "4m ago",
    records: 12048,
    errors: 14,
  },
  {
    id: "f4",
    name: "smb__daily_reports",
    state: "Stopped" as FlowState,
    source: "SMB",
    primaryStream: "daily_reports",
    topic: "smb.daily_reports.v1",
    schemaVersion: "v1",
    lastRun: "2 hr ago",
    records: 3890,
    errors: 0,
  },
  {
    id: "f5",
    name: "rapid7__vulns",
    state: "Error" as FlowState,
    source: "REST API",
    primaryStream: "vulns",
    topic: "rapid7.vulns.v1",
    schemaVersion: "v1",
    lastRun: "18m ago",
    records: 2210,
    errors: 47,
  },
];

export const auditLog = [
  { id: 1, time: "2025-04-24 10:42:11", user: "admin", action: "Started flow", object: "Flow", target: "rapid7__assets", status: "Success" },
  { id: 2, time: "2025-04-24 10:28:54", user: "admin", action: "Approved schema", object: "Schema", target: "sentinelone__agents-value v3", status: "Success" },
  { id: 3, time: "2025-04-24 10:01:30", user: "admin", action: "Generated schema", object: "Schema", target: "fortisiem__devices", status: "Success" },
  { id: 4, time: "2025-04-24 09:43:02", user: "admin", action: "Tested Kafka connection", object: "Connection", target: "kafka-prod", status: "Success" },
  { id: 5, time: "2025-04-24 09:21:18", user: "admin", action: "Created source", object: "Source", target: "smb__daily_reports", status: "Success" },
  { id: 6, time: "2025-04-24 08:55:00", user: "admin", action: "Tested SMB connection", object: "Connection", target: "smb-archive", status: "Failed" },
  { id: 7, time: "2025-04-24 08:12:44", user: "admin", action: "Deployed flow", object: "Flow", target: "fortisiem__devices", status: "Success" },
  { id: 8, time: "2025-04-23 17:39:09", user: "admin", action: "Registered schema", object: "Schema", target: "rapid7__assets-value v2", status: "Success" },
  { id: 9, time: "2025-04-23 16:02:21", user: "admin", action: "Stopped flow", object: "Flow", target: "smb__daily_reports", status: "Success" },
  { id: 10, time: "2025-04-23 14:48:55", user: "admin", action: "Created connection", object: "Connection", target: "apicurio-prod", status: "Success" },
];
