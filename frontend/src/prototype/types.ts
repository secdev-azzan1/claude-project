// Adapter-based domain model for the UI prototype (frontend-only).
// This is the mock counterpart of what the backend would eventually persist.

export type AdapterId = "http" | "jdbc" | "kafka" | "kafka_kc" | "kc";

export type BlockMode = "read" | "write" | "lookup";

export type FlowState =
  | "Draft"
  | "Deploying"
  | "Running"
  | "Paused"
  | "Stopped"
  | "Degraded"
  | "Error";

export type TransformKind =
  | "extract"
  | "add_field"
  | "remove_field"
  | "set_from_attribute"
  | "rename"
  | "coerce"
  | "dedup";

/** The operators a branch condition can use. */
export type BranchOp = "equals" | "not_equals" | "contains" | "starts_with" | "regex" | "is_empty";

/** One test against one field. A branch holds a list of them. */
export interface BranchCondition {
  field: string;
  op: BranchOp;
  value: string;
}

/**
 * How a branch's rules combine. `all` (the default) is an AND — every rule has
 * to hold; `any` is an OR. This is a property of ONE branch: it never orders or
 * ranks branches against each other, which is what keeps them independent.
 */
export type BranchMatch = "all" | "any";

export interface TransformRule {
  id: string;
  kind: TransformKind;
  // Loosely typed per-kind payload; the form components know the shapes:
  // extract: {attribute, path, default?}
  // add_field: {field, value} · remove_field: {field}
  // set_from_attribute: {field, attribute} · rename: {from, to}
  // coerce: {field, type} · dedup: {identityFields[], excludedFields[], windowHours}
  // route: {rules: RouteRule[], defaultAction: "forward"|"drop"}
  config: Record<string, unknown>;
}

export interface BlockTestResult {
  ok: boolean;
  reason?: string; // failures come back as data, never thrown
  records?: unknown[];
  detectedFields?: string[];
  testedAt: string;
}

/**
 * A branch is the edge from a parent block to one of its children. There is only
 * one kind: no rules means every record takes it, rules mean only matching
 * records do. Branches are evaluated independently, so a record can travel down
 * several of them at once.
 */
export interface BranchInfo {
  name: string; // branch-1 / user label — feeds topic variant tokens
  /** Empty or absent ⇒ every record takes this branch. */
  rules?: BranchCondition[];
  /** How the rules combine. Absent reads as "all". */
  match?: BranchMatch;
}

export interface FlowBlock {
  id: string;
  adapter: AdapterId;
  mode?: BlockMode; // http/jdbc: read|write|lookup · kafka: read|write · kafka_kc/kc: undefined (always sinks)
  name: string;
  /** Upstream node: a block id, a topic node id, or null for the root. */
  parentId: string | null;
  branch?: BranchInfo; // present when this block starts a named branch off its parent
  serviceId?: string | null; // Application Service reference (credentials never live here)
  entity?: string | null; // entity label — required on every write (kc included)
  /**
   * Loosely typed per-adapter payload. Conventions the forms rely on
   * (`config` is `Record<string, unknown>`, so none of these are type-checked —
   * grep, never trust tsc, when renaming one):
   * - http:      method, path, responseFormat, recordPath, split, pagination,
   *              `proxyId?: string | null` — reference into `state.gatewayProxies`
   *              (replaced the old boolean `proxy`), lookupJoinField
   * - jdbc:      table, columns, incremental, watermarkColumn, initialPosition
   * - kafka:     topicName, parseFormat, initialPosition
   * - kafka_kc:  sinkServiceId, `sinkConfig?: Record<string, string>`
   * - kc:        attachTopicId, initialPosition, `sinkConfig?: Record<string, string>`
   *
   * `sinkConfig` is the Kafka Connect connector configuration edited by the
   * shared sink-config editor: flat string→string, `connector.class` validated
   * against CONNECT_PLUGIN_CATALOG. Keys the platform owns are rendered as
   * disabled rows and are NEVER persisted here — `topics` is computed at render
   * from `deriveTopicName`, and the Avro converter / lakehouse table name are
   * derived too. Persisting them would go stale on the next rename.
   */
  config: Record<string, unknown>;
  transforms: TransformRule[];
  topicOverride?: string | null; // kafka-family writes only (R7)
  testResult?: BlockTestResult | null;
}

export interface FlowTopic {
  id: string;
  kind: "adopted" | "materialized";
  name: string;
  sealed: boolean; // kafka_kc-owned topics are sealed — never attachable
  writerBlockId?: string; // materialized topics
  backlogEstimate?: number; // adopted topics / kc attach prompts
}

export interface FlowVariable {
  name: string;
  value: string;
  secret: boolean;
}

export interface Flow {
  id: string;
  name: string; // the "source name" — first half of every derived name
  description?: string;
  state: FlowState;
  enabled: boolean;
  /** Cron (5-field, UTC). null = no trigger (topic-rooted, kc-only flows). */
  cron: string | null;
  blocks: FlowBlock[];
  topics: FlowTopic[];
  variables: FlowVariable[];
  /** serviceId -> revision pinned at last deploy. */
  servicePins: Record<string, number>;
  drift?: string | null; // e.g. "Process group missing on Production NiFi"
  deployedAt?: string | null;
  lastRunAt?: string | null;
  createdAt: string;
  updatedAt: string;
}

// ---------------------------------------------------------------- schemas

export type SchemaProvenance = "sample_run" | "uploaded" | "manual";

export interface AvroField {
  name: string;
  type: string; // simplified for the prototype ("string", "long", "record", ...)
  doc?: string;
  nullable?: boolean;
  children?: AvroField[]; // record/array element fields
}

/**
 * One past (or current) approval of an approved schema. Approve = register, so
 * every entry carries the registry global id it was registered under. Read-only
 * history — the newest entry is LAST and mirrors the parent record's
 * `provenance` / `registryGlobalId` / `rawAvro` / `approvedAt`.
 */
export interface SchemaApproval {
  version: number; // 1-based, monotonic within the record
  approvedAt: string;
  provenance: SchemaProvenance;
  registryGlobalId: number;
  rawAvro: string; // pretty-printed Avro JSON as approved at that moment
  supersededAt?: string; // set on every entry except the latest
  prefilledFromLabel?: string; // frozen template name, if the ceremony was pre-filled
}

export interface ApprovedSchema {
  id: string;
  subject: string; // "<topic>-value"
  entity: string;
  flowId: string;
  blockId: string;
  provenance: SchemaProvenance;
  fields: AvroField[];
  rawAvro: string; // pretty-printed Avro JSON
  approvedAt: string;
  registryGlobalId: number;
  /** Approval history, oldest first, latest LAST. Never empty. */
  approvals: SchemaApproval[];
}

/**
 * Hand-authored library schema. NOT registered, NOT bound to a flow/block, and
 * deliberately kept OUT of `state.schemas` — `state.schemas.length` guards the
 * Apicurio connection's edit/delete and used to drive registry-id allocation.
 * A ceremony may pre-fill from one; the template itself is freely editable.
 */
export interface SchemaTemplate {
  id: string;
  name: string;
  description?: string;
  rawAvro: string; // pretty-printed Avro JSON; MUST parse via normalizeAvroRecord
  createdAt: string;
  updatedAt: string;
}

// ------------------------------------------------------------ connections

export type ConnectionType =
  | "nifi"
  | "kafka"
  | "apicurio"
  | "kafka_connect"
  | "redis"
  | "apisix";

export type ConnectionHealth = "Healthy" | "Failed" | "Not Tested";
export type Reachability = "Reachable" | "Unreachable" | "Unknown";

export interface PlatformConnection {
  id: string;
  type: ConnectionType;
  name: string;
  active: boolean; // exactly one active per type
  health: ConnectionHealth;
  reachability: Reachability;
  lastTestedAt?: string | null;
  /** Non-secret config fields, shape depends on type. */
  config: Record<string, unknown>;
  hasSecret: boolean; // secrets are write-only; UI shows presence only
}

export interface GatewayCertProfile {
  id: string;
  name: string;
  subject: string;
  expiresAt: string;
  refCount: number;
}

export type GatewayReconcileStatus = "Reconciled" | "Pending" | "Failed";

/**
 * A named APISIX proxy in the catalog. Supersedes the old upstream/route pair:
 * one proxy is one egress definition, and an http block references it by id via
 * `config.proxyId`. Deploy requires the referenced proxy to exist, be
 * "Reconciled", and have `targetHost` in `gateway.allowlist`.
 */
export interface GatewayProxy {
  id: string;
  name: string;
  description?: string;
  targetHost: string; // must be admin-allowlisted to deploy
  port: number;
  sni?: string;
  connectTimeoutMs?: number;
  readTimeoutMs?: number;
  path: string; // route path prefix, e.g. "/phoenix/rest"
  methods: string[]; // allowed HTTP methods, e.g. ["GET", "POST"]
  certProfileId?: string | null; // client-cert profile in gateway.certProfiles
  status: GatewayReconcileStatus;
  statusDetail?: string; // why it is Pending/Failed
  createdAt: string;
  updatedAt: string;
}

/** Secondary, admin-gated gateway resources. Proxies live in `state.gatewayProxies`. */
export interface GatewayResources {
  certProfiles: GatewayCertProfile[];
  allowlist: string[];
}

// --------------------------------------------------------------- services

export type ServiceType = "http" | "database" | "external_kafka" | "sink_destination";

export type HttpAuthMode = "none" | "basic" | "bearer" | "api_key" | "oauth2" | "session_token";
export type JdbcDialect = "postgresql" | "trino" | "mysql";
export type SinkKind = "opensearch" | "iceberg_catalog";

export interface AppService {
  id: string;
  type: ServiceType;
  name: string;
  revision: number; // edits create revisions; flows pin a revision at deploy
  retired: boolean; // logical retirement — dependents flagged "action required"
  private?: boolean; // created inline from a block ("manual mode")
  health: ConnectionHealth;
  lastTestedAt?: string | null;
  config: Record<string, unknown>; // non-secret; type-specific
  hasSecret: boolean;
  createdAt: string;
  updatedAt: string;
}

// --------------------------------------------------------------- the rest

export interface AuditEvent {
  id: string;
  ts: string;
  user: string;
  action: string;
  object: string;
  target: string;
  status: "Success" | "Failed" | "Warning";
  details?: string;
}

export interface DlqRecord {
  id: string;
  flowId: string;
  ts: string;
  blockName: string;
  errorClass: string;
  payloadPreview: string;
}

export interface BlockMetric {
  blockId: string;
  label: string; // "http · Asset Details"
  recordsIn: number;
  recordsOut: number;
  queued: number;
}

export interface FlowMetrics {
  flowId: string;
  records24h: number;
  errors24h: number;
  queued: number;
  perBlock: BlockMetric[];
  topicCounts: { topic: string; messages: number }[];
  lastRunOutcome?: "Success" | "Failed" | "Skipped (overlap)";
}

export interface ConnectorExport {
  id: string;
  name: string; // "rapid7-to-iceberg"
  version: number;
  flowId: string;
  description?: string;
  createdAt: string;
}

/**
 * A Kafka Connect plugin the platform believes is installed on the active
 * Connect cluster. The sink-config editor validates `connector.class` against
 * this catalog (spec: the platform checks the plugin is installed) and unlocks
 * the lakehouse-only locked keys (table name, auto-create, schema evolution)
 * when the chosen plugin is `lakehouseSink`.
 */
export interface ConnectPlugin {
  connectorClass: string; // the exact value of the `connector.class` key
  displayName: string;
  lakehouseSink: boolean;
}

// --------------------------------------------------------------- runtime
// What a deployed flow actually looks like on the runtime, read live and shown
// READ-ONLY. The spec removed the user-facing controller-services manager and
// live processor editing on purpose: live edits are exactly what produces the
// drift this model detects. Tuning = adapter config + redeploy.

/** A property as it comes back from a live NiFi descriptor. */
export interface RuntimeProperty {
  name: string;
  /**
   * The live value — or `null` when the descriptor is sensitive. Sensitive
   * values are never shipped to the browser at all; the UI renders a mask, it
   * does not mask a value it was given.
   */
  value: string | null;
  sensitive?: boolean;
  /** Set when the live value differs from the one the platform compiled. */
  divergedFrom?: string;
}

export type NifiComponentState = "RUNNING" | "STOPPED" | "DISABLED" | "INVALID";

/** One generated NiFi component, attributed to the block that produced it. */
export interface NifiComponent {
  id: string; // NiFi component uuid
  name: string;
  /** Fully-qualified processor type, as NiFi reports it. */
  type: string;
  /** The flow block this component was generated from (the runtime-scope map). */
  blockId: string;
  state: NifiComponentState;
  /** Validation errors reported by NiFi for this component. */
  invalidReasons?: string[];
  properties: RuntimeProperty[];
}

export type ControllerServiceState = "ENABLED" | "DISABLED" | "INVALID";

/**
 * A compiled NiFi controller service. Generated per Application Service
 * revision — never authored by the user, which is why this carries the
 * `appServiceId` + `pinnedRevision` it was compiled from.
 */
export interface ControllerServiceRuntime {
  id: string;
  name: string;
  type: string; // e.g. "StandardRestrictedSSLContextService"
  state: ControllerServiceState;
  /** Application Service it was compiled from, null for platform-owned ones. */
  appServiceId: string | null;
  /** Revision pinned at the flow's last deploy. */
  pinnedRevision: number | null;
  /** "flow" = generated inside this flow's process group · "shared" = platform-level, referenced by many flows. */
  scope: "flow" | "shared";
  /** Names of the other deployed flows referencing a shared service. */
  sharedWith?: string[];
  properties: RuntimeProperty[];
}

export type ConnectRunState = "RUNNING" | "PAUSED" | "FAILED" | "UNASSIGNED" | "RESTARTING";

export interface ConnectTaskRuntime {
  id: number;
  state: ConnectRunState;
  workerId: string;
  /** Truncated stack trace — the worker's log line, not the whole log. */
  lastErrorTrace?: string;
}

export interface ConnectConnectorRuntime {
  /** Connector name on the worker. */
  name: string;
  /** Owning kc / kafka_kc block. */
  blockId: string;
  connectorClass: string;
  state: ConnectRunState;
  workerId: string;
  tasks: ConnectTaskRuntime[];
  /** Connect-only counters — deliberately separate from NiFi FlowFile counts. */
  recordsSent: number;
  recordsFailed: number;
  lastErrorTrace?: string;
}

export type DriftKind =
  | "process_group_missing"
  | "property_edited"
  | "component_state_changed"
  | "connector_missing"
  | "consumer_lag";

/**
 * The fingerprint disambiguation, kept from the alpha: a missing process group
 * on the SAME instance means really deleted, on a DIFFERENT one means deployed
 * elsewhere, unreachable means exactly that — unknown.
 */
export type DriftVerdict = "really_deleted" | "deployed_elsewhere" | "unknown" | "out_of_band_edit";

export interface DriftFinding {
  id: string;
  kind: DriftKind;
  /** One-line statement of what diverged. */
  summary: string;
  /** Where it diverged — component / block / connector name. */
  where: string;
  /** Expected (compiled) vs observed (live), when the divergence is a value. */
  expected?: string;
  observed?: string;
  verdict: DriftVerdict;
  /** Plain-language reading of the verdict, shown next to it. */
  verdictDetail: string;
  observedAt: string;
  /** Only findings that hold a dead reference can be force-repaired. */
  repairable: boolean;
}

/** Left behind on a runtime the platform no longer manages. Never deleted for you. */
export interface RuntimeOrphan {
  id: string;
  kind: "process_group" | "connector" | "controller_service";
  ref: string; // id or name on the runtime
  instance: string; // which NiFi / Connect instance it was left on
  recordedAt: string;
}

/** Per-deployed-flow runtime record. Absent for flows that were never deployed. */
export interface FlowRuntime {
  flowId: string;
  /** Platform connection the runtime was deployed onto. */
  nifiConnectionId: string;
  /** NiFi process group id, or null once a force repair cleared the reference. */
  processGroupId: string | null;
  /** NiFi root-group id recorded at deploy. */
  deployedFingerprint: string;
  /** Root-group id seen on the last read — null when the instance was unreachable. */
  observedFingerprint: string | null;
  reachable: boolean;
  /** Why the last read could not complete, when it could not. */
  unreachableReason?: string;
  lastReadAt: string | null;
  components: NifiComponent[];
  controllerServices: ControllerServiceRuntime[];
  connectors: ConnectConnectorRuntime[];
  drift: DriftFinding[];
  orphans: RuntimeOrphan[];
}

export interface TopicMessage {
  offset: number;
  ts: string;
  key: string | null;
  /** Plain text body, or null when binary — UI renders "binary payload (N bytes)". */
  value: string | null;
  bytes: number;
}

// ------------------------------------------------------------- root state

export interface PrototypeState {
  seedVersion: number;
  flows: Flow[];
  /** Approved (= registered) schemas only. Never holds templates. */
  schemas: ApprovedSchema[];
  /** Hand-authored, unregistered library schemas. Separate on purpose. */
  schemaTemplates: SchemaTemplate[];
  /** Next Apicurio global id to hand out. Monotonic — never derived from array length. */
  registryGlobalIdSeq: number;
  connections: PlatformConnection[];
  /** Cert profiles + host allowlist. Proxies are a sibling collection. */
  gateway: GatewayResources;
  /** The APISIX proxy catalog. `config.proxyId` on http blocks points here. */
  gatewayProxies: GatewayProxy[];
  services: AppService[];
  audit: AuditEvent[];
  dlq: DlqRecord[];
  metrics: FlowMetrics[];
  connectors: ConnectorExport[];
  /**
   * Live runtime records, one per deployed flow. Read-only in the UI: a read
   * never mutates them beyond states/properties/timestamps, and repairing a
   * dead reference is an explicit force action.
   */
  runtimes: FlowRuntime[];
  /** topic name -> messages, for the message viewer */
  topicMessages: Record<string, TopicMessage[]>;
  /**
   * Set once by the store when a seed-version mismatch wiped saved data.
   * The UI shows it a single time and calls `consumeMigrationNotice()`.
   */
  migrationNotice?: string;
  /**
   * A schema edited on the Schemas page and handed to the ceremony that will
   * register it. It exists because registration has exactly one door — the
   * ceremony — while editing has to be possible where the schema is read. So
   * the edit travels to the door rather than a second door being cut. Consumed
   * once, by the ceremony it names.
   */
  pendingCeremonyDraft?: CeremonyDraft;
}

/** An edited-but-unregistered Avro record on its way to a ceremony. */
export interface CeremonyDraft {
  flowId: string;
  blockId: string;
  rawAvro: string;
  /** What was edited, for the ceremony's own banner (e.g. the old subject). */
  label: string;
}
