// Real HTTP service layer, talking to the live backend at
// `${import.meta.env.VITE_BACKEND_URL}`. This replaces the old localStorage
// mock (store.ts / seeds.ts / migrate.ts are retired from runtime — nothing
// here imports them). Every function keeps the exact name and type signature
// the pages were built against, so no page needed an import change.
//
// A few pure, synchronous helpers (getVerbBlockReason, getEditLockReason,
// validateFlowNow, connectionDependents, proxyDependents, serviceDependents)
// are called directly from render code and cannot become async without
// breaking their signature. They read from a small in-memory cache instead
// of localStorage — the cache is populated as a side effect of the list*/
// get* fetches the pages already perform (via react-query), and each of
// those six helpers also opportunistically "warms" any collection it needs
// that nothing has fetched yet, so a page that never happens to list e.g.
// flows still gets a real answer shortly after mount.

import { deriveTopicName } from "./naming";
import {
  blockProxyId,
  deployPreflight,
  validateFlow,
  type GatewaySnapshot,
  type PreflightCheck,
  type ValidationIssue,
} from "./validation";
import { recordToDisplayFields } from "@/components/schema-editor";
import type { AvroRecord } from "@/lib/schemaEditor";
import type {
  AppService,
  ApprovedSchema,
  AuditEvent,
  AvroField,
  CeremonyDraft,
  ConnectorExport,
  DlqRecord,
  Flow,
  FlowBlock,
  FlowMetrics,
  FlowRuntime,
  GatewayCertProfile,
  GatewayProxy,
  GatewayResources,
  PlatformConnection,
  PrototypeState,
  RuntimeOrphan,
  SchemaApproval,
  SchemaProvenance,
  SchemaTemplate,
  TopicMessage,
} from "./types";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const nowIso = () => new Date().toISOString();

function uid(prefix: string): string {
  return `${prefix}-${Math.random().toString(36).slice(2, 8)}`;
}

/** The gateway facts validation needs, read off one state snapshot. */
function gatewayOf(state: PrototypeState): GatewaySnapshot {
  return { proxies: state.gatewayProxies, allowlist: state.gateway.allowlist };
}

// -------------------------------------------------------------- HTTP client

const BASE = ((import.meta.env.VITE_BACKEND_URL as string | undefined) ?? "").replace(/\/+$/, "");

export class ApiRequestError extends Error {
  status: number;
  constructor(message: string, status: number) {
    super(message);
    this.name = "ApiRequestError";
    this.status = status;
  }
}

/** FastAPI convention: `{detail: "..."}` or `{detail: {issues: [...]}}`, plus
 *  the stock pydantic validation shape `{detail: [{loc, msg, type}, ...]}`. */
function normalizeDetail(detail: unknown): string {
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail)) {
    const first = detail[0];
    if (first && typeof first === "object") {
      const rec = first as Record<string, unknown>;
      if (typeof rec.msg === "string") {
        const loc = Array.isArray(rec.loc) ? rec.loc.filter((p) => p !== "body").join(".") : "";
        return loc ? `${loc}: ${rec.msg}` : rec.msg;
      }
    }
    try {
      return detail.map(String).join("; ");
    } catch {
      return JSON.stringify(detail);
    }
  }
  if (detail && typeof detail === "object") {
    const rec = detail as Record<string, unknown>;
    if (Array.isArray(rec.issues)) {
      const joined = (rec.issues as Array<{ message?: string }>)
        .map((i) => i.message)
        .filter((m): m is string => !!m)
        .join("; ");
      return joined || "Validation failed.";
    }
    // Deploy preflight failure shape: {rows: [{label, ok, detail}, ...]}
    if (Array.isArray(rec.rows)) {
      const joined = (rec.rows as Array<{ label?: string; ok?: boolean; detail?: string }>)
        .filter((r) => !r.ok)
        .map((r) => (r.label ? `${r.label}: ${r.detail ?? ""}` : r.detail))
        .filter((m): m is string => !!m)
        .join("; ");
      return joined || "Preflight failed.";
    }
    if (typeof rec.message === "string") return rec.message;
    try {
      return JSON.stringify(detail);
    } catch {
      return String(detail);
    }
  }
  return String(detail);
}

interface RequestOpts {
  method?: string;
  body?: unknown;
  form?: FormData;
}

/** Small fetch wrapper: JSON in/out, FormData, 204, and FastAPI detail extraction. */
async function request<T>(path: string, opts: RequestOpts = {}): Promise<T> {
  const { method = "GET", body, form } = opts;
  const url = `${BASE}${path}`;
  const init: RequestInit = { method };
  if (form) {
    init.body = form;
  } else if (body !== undefined) {
    init.headers = { "Content-Type": "application/json" };
    init.body = JSON.stringify(body);
  }

  let res: Response;
  try {
    res = await fetch(url, init);
  } catch (err) {
    throw new ApiRequestError(
      `Could not reach the backend at ${url}: ${err instanceof Error ? err.message : String(err)}`,
      0,
    );
  }

  if (res.status === 204) return undefined as T;

  const text = await res.text();

  if (!res.ok) {
    if (res.status === 501) {
      throw new ApiRequestError("Deployment engine pending — this verb will be live shortly.", 501);
    }
    let message = text || res.statusText || `Request failed with status ${res.status}`;
    try {
      const json = JSON.parse(text);
      message = normalizeDetail(json.detail ?? json.message ?? text);
    } catch {
      // non-JSON error body — keep the raw text
    }
    throw new ApiRequestError(message, res.status);
  }

  if (!text) return undefined as T;

  const trimmed = text.trim().toLowerCase();
  if (trimmed.startsWith("<!doctype html") || trimmed.startsWith("<html")) {
    throw new ApiRequestError(`Unexpected HTML response from ${url}. Check VITE_BACKEND_URL and backend availability.`, 502);
  }

  try {
    return JSON.parse(text) as T;
  } catch {
    throw new ApiRequestError(`Invalid JSON response from ${url}`, 502);
  }
}

/** forceRepairRuntime: this route doesn't exist on the backend yet (404) or
 *  exists but 501 — both read as "engine pending". NOT used by testBlock or
 *  clearDedupCache any more — those routes are real now, and a 404 from them
 *  means something concrete ("Flow not found"), not "unimplemented"; blanket-
 *  mapping their 404s to the pending message hid that real detail. */
async function requestPendingAware<T>(path: string, opts: RequestOpts = {}): Promise<T> {
  try {
    return await request<T>(path, opts);
  } catch (err) {
    if (err instanceof ApiRequestError && err.status === 404) {
      throw new Error("Deployment engine pending — this verb will be live shortly.");
    }
    throw err;
  }
}

// ------------------------------------------------ small in-memory read cache

interface ReadCache {
  flows: Flow[];
  services: AppService[];
  schemas: ApprovedSchema[];
  connections: PlatformConnection[];
  gatewayProxies: GatewayProxy[];
  gateway: GatewayResources;
}
const cache: ReadCache = {
  flows: [],
  services: [],
  schemas: [],
  connections: [],
  gatewayProxies: [],
  gateway: { certProfiles: [], allowlist: [] },
};
const loaded = { flows: false, services: false, schemas: false, connections: false, gateway: false };

/** Kick off a fetch for any collection nothing has populated yet. Fire-and-forget. */
function warmCache(): void {
  if (!loaded.flows) {
    loaded.flows = true;
    listFlows().catch(() => {
      loaded.flows = false;
    });
  }
  if (!loaded.services) {
    loaded.services = true;
    listServices().catch(() => {
      loaded.services = false;
    });
  }
  if (!loaded.schemas) {
    loaded.schemas = true;
    listSchemas().catch(() => {
      loaded.schemas = false;
    });
  }
  if (!loaded.connections) {
    loaded.connections = true;
    listConnections().catch(() => {
      loaded.connections = false;
    });
  }
  if (!loaded.gateway) {
    loaded.gateway = true;
    listGatewayProxies().catch(() => {
      loaded.gateway = false;
    });
  }
}

/** A PrototypeState-shaped view over the read cache, for the pure helpers
 *  below that were written against `PrototypeState` and only ever touch a
 *  handful of its fields. */
function stateSnapshot(): PrototypeState {
  warmCache();
  return {
    seedVersion: 0,
    flows: cache.flows,
    schemas: cache.schemas,
    schemaTemplates: [],
    registryGlobalIdSeq: 0,
    connections: cache.connections,
    gateway: cache.gateway,
    gatewayProxies: cache.gatewayProxies,
    services: cache.services,
    audit: [],
    dlq: [],
    metrics: [],
    connectors: [],
    runtimes: [],
    topicMessages: {},
  };
}

// ------------------------------------------------------------------ flows

export type FlowVerb =
  | "deploy"
  | "start"
  | "pause"
  | "resume"
  | "stop"
  | "stop_clear"
  | "redeploy"
  | "undeploy"
  | "delete";

export async function listFlows(): Promise<Flow[]> {
  const data = await request<Flow[]>("/api/v2/flows/");
  cache.flows = data;
  loaded.flows = true;
  return data;
}

/**
 * `createFlow` mints an all-new flow with zero blocks and zero topics. The
 * live backend's flow-level validation refuses to persist exactly that
 * shape ("The flow is empty — add a root block."), because a flow really is
 * required to have SOME structure — but this app's own "New flow" page
 * intentionally creates the empty shell first and lets the user place the
 * root block on the next screen, saving explicitly afterward. Rather than
 * fight that validation (or bend the backend, which is out of scope), the
 * freshly minted flow is staged in memory here and served back by `getFlow`
 * until the user's first real `saveFlow` succeeds — at which point it
 * really exists server-side and this staging entry is dropped.
 */
const stagedNewFlows = new Map<string, Flow>();

export async function getFlow(id: string): Promise<Flow | null> {
  const staged = stagedNewFlows.get(id);
  if (staged) return { ...staged };
  try {
    return await request<Flow>(`/api/v2/flows/${id}`);
  } catch (err) {
    if (err instanceof ApiRequestError && err.status === 404) return null;
    throw err;
  }
}

/** Keep flow.topics in sync with its kafka-family write blocks. */
export function syncFlowTopics(flow: Flow): void {
  const keep = new Set<string>();
  for (const b of flow.blocks) {
    const isWrite = (b.adapter === "kafka" && b.mode === "write") || b.adapter === "kafka_kc";
    if (!isWrite) continue;
    const name = deriveTopicName(flow, b).value;
    let topic = flow.topics.find((t) => t.writerBlockId === b.id);
    if (!topic) {
      topic = { id: uid("t"), kind: "materialized", name, sealed: b.adapter === "kafka_kc", writerBlockId: b.id };
      flow.topics.push(topic);
    } else {
      topic.name = name;
      topic.sealed = b.adapter === "kafka_kc";
    }
    keep.add(topic.id);
  }
  flow.topics = flow.topics.filter((t) => t.kind === "adopted" || keep.has(t.id));
  // Drop kc blocks whose topic disappeared
  flow.blocks = flow.blocks.filter(
    (b) => b.adapter !== "kc" || flow.topics.some((t) => t.id === (b.config.attachTopicId as string)),
  );
}

export async function createFlow(name: string, description?: string): Promise<Flow> {
  const now = nowIso();
  const flow: Flow = {
    id: uid("flow"),
    name,
    description,
    state: "Draft",
    enabled: false,
    cron: null,
    blocks: [],
    topics: [],
    variables: [],
    servicePins: {},
    deployedAt: null,
    lastRunAt: null,
    createdAt: now,
    updatedAt: now,
  };
  stagedNewFlows.set(flow.id, flow);
  return { ...flow };
}

export async function saveFlow(updated: Flow): Promise<Flow> {
  const next = { ...updated };
  syncFlowTopics(next);
  next.updatedAt = nowIso();
  // No separate v2 create endpoint — a flow whose id is not yet on record is
  // created by the same POST /api/v2/flows/ an update uses.
  const saved = await request<Flow>("/api/v2/flows/", { method: "POST", body: next });
  stagedNewFlows.delete(updated.id);
  return saved;
}

/** The block-reason contract: null = allowed, string = why not. */
export function getVerbBlockReason(flow: Flow, verb: FlowVerb, state?: PrototypeState): string | null {
  const s = state ?? stateSnapshot();
  const deployed = !!flow.deployedAt;
  const editVerbs: Record<FlowVerb, () => string | null> = {
    deploy: () => {
      if (flow.state === "Running" || flow.state === "Paused") return "Stop the flow before deploying.";
      if (flow.state === "Deploying") return "A deploy is already in progress.";
      const issues = validateFlow(flow, s.services, s.schemas, gatewayOf(s));
      if (issues.length > 0) return `${issues.length} validation issue(s) — run Validate for details.`;
      return null;
    },
    start: () => {
      if (!deployed) return "Deploy the flow first.";
      if (!flow.enabled) return "The flow is disabled.";
      if (flow.state === "Running") return "Already running.";
      if (flow.state === "Paused") return "Use Resume — the flow is paused, its trigger still fires.";
      const missing = ["nifi", "kafka", "apicurio"].filter(
        (t) => !s.connections.some((c) => c.type === t && c.active && c.health === "Healthy"),
      );
      if (missing.length > 0) return `Runtime connections unavailable: ${missing.join(", ")}.`;
      return null;
    },
    pause: () => (flow.state !== "Running" ? "Only a running flow can be paused." : null),
    resume: () => (flow.state !== "Paused" ? "Only a paused flow can be resumed." : null),
    stop: () => (flow.state !== "Running" && flow.state !== "Paused" && flow.state !== "Degraded" ? "The flow is not running." : null),
    stop_clear: () =>
      flow.state !== "Running" && flow.state !== "Paused" && flow.state !== "Degraded"
        ? "The flow is not running."
        : null,
    redeploy: () => {
      if (!deployed) return "The flow has never been deployed.";
      if (flow.state !== "Stopped") return "Redeploy requires the flow stopped (and queues cleared).";
      return null;
    },
    undeploy: () => {
      if (!deployed) return "The flow is not deployed.";
      if (flow.state === "Running" || flow.state === "Paused") return "Stop the flow before undeploying.";
      return null;
    },
    // Deletion is queued and the backend lifecycle performs a safe undeploy
    // first when required.
    delete: () => null,
  };
  return editVerbs[verb]();
}

/** Editing a deployed, non-stopped flow is refused (kc "Save is live" is the sole exception). */
export function getEditLockReason(flow: Flow): string | null {
  if (flow.state === "Running" || flow.state === "Paused" || flow.state === "Deploying" || flow.state === "Degraded") {
    return "Editing a deployed flow is refused until it is stopped. (kc sink subscriptions save live.)";
  }
  return null;
}

export async function runFlowVerb(flowId: string, verb: FlowVerb): Promise<Flow | null> {
  if (verb === "delete") {
    await request(`/api/v2/flows/${flowId}`, { method: "DELETE" });
    return null;
  }
  return request<Flow>(`/api/v2/flows/${flowId}/verbs/${verb}`, { method: "POST" });
}

export async function setFlowEnabled(flowId: string, enabled: boolean): Promise<void> {
  await request(`/api/v2/flows/${flowId}/enabled`, { method: "POST", body: { enabled } });
}

// ------------------------------------------------------------- bulk jobs
//
// A bulk run happens on the server, not in this tab. Deploy alone can poll
// NiFi for 30s (parameter context) plus 45s (controller services) per flow,
// so twenty flows is minutes of work that used to die if you closed the page.

export type BulkJobStatus = "queued" | "pending" | "running" | "completed" | "failed" | "cancelled" | "interrupted";
export type BulkItemStatus = "pending" | "running" | "succeeded" | "failed" | "skipped" | "cancelled";

export interface BulkJobItem {
  id: string;
  flowId: string;
  flowName: string;
  status: BulkItemStatus;
  error: string | null;
  startedAt: string | null;
  finishedAt: string | null;
  cancellable?: boolean;
}

export interface BulkJob {
  id: string;
  verb: string;
  status: BulkJobStatus;
  total: number;
  completed: number;
  succeeded: number;
  failed: number;
  cancelRequested: boolean;
  items: BulkJobItem[];
  error: string | null;
  createdAt: string;
  updatedAt: string;
  finishedAt: string | null;
  label?: string;
  cancellable?: boolean;
}

/** A job in one of these will never change again — stop polling. */
export function isBulkJobTerminal(job: BulkJob | null | undefined): boolean {
  if (!job) return false;
  return job.status === "completed" || job.status === "failed" || job.status === "cancelled" || job.status === "interrupted";
}

/** 0-100, for the Progress bar. Guards total=0 so it never yields NaN. */
export function bulkJobPercent(job: BulkJob | null | undefined): number {
  if (!job || job.total <= 0) return 0;
  return Math.round((job.completed / job.total) * 100);
}

export interface FlowDeleteOptions {
  /** Remove the flow-owned DLQ and generated data topics. Defaults to true. */
  deleteTopics?: boolean;
  /** Invalidate every dedup cache namespace associated with the flow. */
  clearCache?: boolean;
}

export async function startBulkJob(
  verb: string,
  flowIds: string[],
  deleteOptions?: FlowDeleteOptions,
): Promise<string> {
  const res = await request<{ jobId: string }>("/api/v2/flows/bulk", {
    method: "POST",
    body: {
      verb,
      flowIds,
      ...(verb === "delete" && deleteOptions ? { deleteOptions } : {}),
    },
  });
  return res.jobId;
}

export async function getBulkJob(jobId: string): Promise<BulkJob> {
  return request<BulkJob>(`/api/v2/flows/bulk/${jobId}`);
}

export async function waitForBulkJob(jobId: string): Promise<BulkJob> {
  for (;;) {
    const job = await getBulkJob(jobId);
    if (isBulkJobTerminal(job)) return job;
    await new Promise((resolve) => window.setTimeout(resolve, 800));
  }
}

/** The run still in flight, if any. This is what lets a refresh reattach. */
export async function getActiveBulkJob(): Promise<BulkJob | null> {
  return request<BulkJob | null>("/api/v2/flows/bulk/active");
}

/** All queued/running/recent flow operations for the global queue panel. */
export async function getBulkQueue(): Promise<BulkJob[]> {
  return request<BulkJob[]>("/api/v2/flows/bulk/queue");
}

export async function cancelBulkJob(jobId: string): Promise<BulkJob> {
  return request<BulkJob>(`/api/v2/flows/bulk/${jobId}/cancel`, { method: "POST" });
}

export async function cancelBulkItem(jobId: string, itemId: string): Promise<BulkJob> {
  return request<BulkJob>(`/api/v2/flows/bulk/${jobId}/items/${itemId}/cancel`, { method: "POST" });
}

export interface KafkaConnectSync {
  id: string;
  name: string;
  description: string;
  direction: "sink" | "source";
  connectorClass: string;
  connectorName: string;
  config: Record<string, string>;
  enabled: boolean;
  retired: boolean;
  remotePresent: boolean;
  configurationState: "draft" | "needs_review" | "synced" | "changes_pending";
  pendingChanges: boolean;
  linkedFlowId: string | null;
  linkedBlockId: string | null;
  hasSecrets?: boolean;
  lastStatus?: Record<string, unknown> | null;
  lastError?: string | null;
  createdAt?: string;
  updatedAt?: string;
}

export interface KafkaConnectSyncInput {
  id?: string;
  name: string;
  description?: string;
  direction?: "sink" | "source";
  connectorClass: string;
  connectorName?: string;
  config: Record<string, string>;
  linkedFlowId?: string | null;
  linkedBlockId?: string | null;
}

const mapKafkaConnectSync = (raw: Record<string, unknown>): KafkaConnectSync => ({
  id: String(raw.id ?? ""),
  name: String(raw.name ?? ""),
  description: String(raw.description ?? ""),
  direction: raw.direction === "source" ? "source" : "sink",
  connectorClass: String(raw.connector_class ?? ""),
  connectorName: String(raw.connector_name ?? ""),
  config: (raw.config && typeof raw.config === "object" ? raw.config : {}) as Record<string, string>,
  enabled: Boolean(raw.enabled),
  retired: Boolean(raw.retired),
  remotePresent: Boolean(raw.remote_present),
  configurationState: ["synced", "changes_pending", "needs_review"].includes(String(raw.configuration_state))
    ? String(raw.configuration_state) as KafkaConnectSync["configurationState"]
    : "draft",
  pendingChanges: Boolean(raw.pending_changes),
  linkedFlowId: typeof raw.linked_flow_id === "string" ? raw.linked_flow_id : null,
  linkedBlockId: typeof raw.linked_block_id === "string" ? raw.linked_block_id : null,
  hasSecrets: Boolean(raw.has_secrets),
  lastStatus: (raw.last_status as Record<string, unknown> | null | undefined) ?? null,
  lastError: typeof raw.last_error === "string" ? raw.last_error : null,
  createdAt: typeof raw.created_at === "string" ? raw.created_at : undefined,
  updatedAt: typeof raw.updated_at === "string" ? raw.updated_at : undefined,
});

export async function listKafkaConnectSyncs(): Promise<KafkaConnectSync[]> {
  const data = await request<Record<string, unknown>[]>("/api/kafka-connect/syncs");
  return data.map(mapKafkaConnectSync);
}

export async function refreshKafkaConnectSyncStatuses(): Promise<KafkaConnectSync[]> {
  const data = await request<Record<string, unknown>[]>('/api/kafka-connect/syncs/statuses');
  return data.map(mapKafkaConnectSync);
}

export async function saveKafkaConnectSync(input: KafkaConnectSyncInput): Promise<KafkaConnectSync> {
  const raw = await request<Record<string, unknown>>("/api/kafka-connect/syncs", {
    method: "POST",
    body: {
      id: input.id,
      name: input.name,
      description: input.description ?? "",
      direction: input.direction ?? "sink",
      connector_class: input.connectorClass,
      connector_name: input.connectorName,
      config: input.config,
      linked_flow_id: input.linkedFlowId ?? null,
      linked_block_id: input.linkedBlockId ?? null,
    },
  });
  return mapKafkaConnectSync(raw);
}

export async function deleteKafkaConnectSync(id: string): Promise<void> {
  await request(`/api/kafka-connect/syncs/${id}`, { method: "DELETE" });
}

export async function linkKafkaConnectSync(id: string, flowId: string, blockId: string): Promise<KafkaConnectSync> {
  const raw = await request<Record<string, unknown>>(`/api/kafka-connect/syncs/${id}/link`, {
    method: "POST",
    body: { flow_id: flowId, block_id: blockId },
  });
  return mapKafkaConnectSync(raw);
}

export async function unlinkKafkaConnectSync(id: string): Promise<KafkaConnectSync> {
  const raw = await request<Record<string, unknown>>(`/api/kafka-connect/syncs/${id}/unlink`, { method: "POST" });
  return mapKafkaConnectSync(raw);
}

export async function retireKafkaConnectSync(id: string): Promise<KafkaConnectSync> {
  const raw = await request<Record<string, unknown>>(`/api/kafka-connect/syncs/${id}/retire`, { method: "POST" });
  return mapKafkaConnectSync(raw);
}

export async function reinstateKafkaConnectSync(id: string): Promise<KafkaConnectSync> {
  const raw = await request<Record<string, unknown>>(`/api/kafka-connect/syncs/${id}/reinstate`, { method: "POST" });
  return mapKafkaConnectSync(raw);
}

export async function validateKafkaConnectSync(id: string): Promise<Record<string, unknown>> {
  return request<Record<string, unknown>>(`/api/kafka-connect/syncs/${id}/validate`, { method: "POST" });
}

export async function applyKafkaConnectSync(id: string): Promise<KafkaConnectSync> {
  const raw = await request<Record<string, unknown>>(`/api/kafka-connect/syncs/${id}/apply`, { method: "POST" });
  return mapKafkaConnectSync(raw);
}

export async function kafkaConnectSyncAction(
  id: string,
  action: "pause" | "resume" | "restart" | "start" | "stop",
): Promise<KafkaConnectSync> {
  const raw = await request<Record<string, unknown>>(`/api/kafka-connect/syncs/${id}/${action}`, { method: "POST" });
  return mapKafkaConnectSync(raw);
}

export async function refreshKafkaConnectSyncStatus(id: string): Promise<KafkaConnectSync> {
  const raw = await request<Record<string, unknown>>(`/api/kafka-connect/syncs/${id}/status`);
  return mapKafkaConnectSync(raw);
}

// -------------------------------------------------- flow cascade deletion

export type CascadeKind = "schema" | "service" | "proxy" | "kafka_connect_sync";

export interface CascadeTarget {
  kind: CascadeKind;
  id: string;
  name: string;
  /** Names of OTHER flows that also use this. Non-empty means it cannot be
   *  deleted along with this flow without breaking them. */
  sharedWith: string[];
  /** Services and Kafka Connect syncs: the resource is still active, and its
   *  delete endpoint refuses an active resource. Deleting it here has to
   *  retire it first -- the same way deleting a deployed flow has to undeploy
   *  first. */
  needsRetire?: boolean;
}

/**
 * Everything that hangs off one flow and could be deleted with it.
 *
 * The three associations are each expressed differently, which is why this
 * has to be computed rather than read off the flow:
 *   - schemas: reverse pointer, ApprovedSchema.flowId -> flow
 *   - services: block.serviceId OR block.config.sinkServiceId
 *   - proxies: two hops, block.serviceId -> AppService.config.proxyId,
 *              with the legacy block.config.proxyId still honoured
 *   - Kafka Connect syncs: block.config.syncId, with shared references
 *     discovered from every flow in the current catalog
 *
 * Anything shared with another flow is returned with a populated `sharedWith`
 * so the dialog can show it but refuse to delete it.
 */
export function flowCascadeTargets(
  flow: Flow,
  state?: PrototypeState,
  kafkaConnectSyncs: KafkaConnectSync[] = [],
): CascadeTarget[] {
  const s = state ?? stateSnapshot();
  const targets: CascadeTarget[] = [];

  // Schemas are owned outright: ApprovedSchema.flowId identifies exactly one
  // flow, so a schema found this way is never shared by construction.
  for (const schema of s.schemas) {
    if (schema.flowId !== flow.id) continue;
    targets.push({ kind: "schema", id: schema.id, name: schema.subject, sharedWith: [] });
  }

  const serviceIds = new Set<string>();
  for (const block of flow.blocks) {
    if (block.serviceId) serviceIds.add(block.serviceId);
    const sink = block.config?.sinkServiceId;
    if (typeof sink === "string" && sink) serviceIds.add(sink);
  }
  for (const serviceId of serviceIds) {
    const svc = s.services.find((x) => x.id === serviceId);
    if (!svc) continue;
    const sharedWith = serviceDependents(serviceId, s)
      .filter((f) => f.id !== flow.id)
      .map((f) => f.name);
    targets.push({
      kind: "service",
      id: svc.id,
      name: svc.name,
      sharedWith,
      needsRetire: !svc.retired,
    });
  }

  const proxyIds = new Set<string>();
  for (const block of flow.blocks) {
    const proxyId = blockProxyId(block, s.services);
    if (proxyId) proxyIds.add(proxyId);
  }
  for (const proxyId of proxyIds) {
    const proxy = s.gatewayProxies.find((p) => p.id === proxyId);
    if (!proxy) continue;
    const sharedWith = proxyDependents(proxyId, s).filter((name) => name !== flow.name);
    targets.push({ kind: "proxy", id: proxy.id, name: proxy.name, sharedWith });
  }

  const syncIds = new Set<string>();
  for (const block of flow.blocks) {
    const syncId = block.config?.syncId;
    if (typeof syncId === "string" && syncId.trim()) syncIds.add(syncId.trim());
  }
  for (const syncId of syncIds) {
    const sync = kafkaConnectSyncs.find((candidate) => candidate.id === syncId);
    if (!sync) continue;
    const sharedWith = s.flows
      .filter((candidate) => candidate.id !== flow.id)
      .filter((candidate) =>
        candidate.blocks.some((block) => block.config?.syncId === syncId),
      )
      .map((candidate) => candidate.name);
    targets.push({
      kind: "kafka_connect_sync",
      id: sync.id,
      name: sync.name,
      sharedWith,
      needsRetire: !sync.retired,
    });
  }

  return targets;
}

/**
 * Delete one cascade target, routing to the right endpoint for its kind.
 *
 * Services get retired first when they are still active: the v2 delete
 * endpoint refuses an active service with a 409 ("Retire the service first"),
 * so without this a cascade delete of a live service would fail even though
 * the user explicitly asked for it. Retiring here is a mechanical
 * prerequisite, exactly like undeploying a flow before deleting it -- the
 * user's intent to remove it entirely has already been confirmed.
 */
export async function deleteCascadeTarget(target: CascadeTarget): Promise<void> {
  if (target.kind === "schema") return deleteApprovedSchema(target.id);
  if (target.kind === "service") {
    if (target.needsRetire) await retireService(target.id);
    return deleteService(target.id);
  }
  if (target.kind === "kafka_connect_sync") {
    if (target.needsRetire) await retireKafkaConnectSync(target.id);
    return deleteKafkaConnectSync(target.id);
  }
  return deleteGatewayProxy(target.id);
}

export function validateFlowNow(flow: Flow): ValidationIssue[] {
  const s = stateSnapshot();
  return validateFlow(flow, s.services, s.schemas, gatewayOf(s));
}

export async function getPreflight(flow: Flow): Promise<PreflightCheck[]> {
  const [services, schemas, connections, gatewayProxies, gatewayResources] = await Promise.all([
    listServices(),
    listSchemas(),
    listConnections(),
    listGatewayProxies(),
    getGatewayResources(),
  ]);
  const active = connections.filter((c) => c.active).map((c) => ({ type: c.type, name: c.name, health: c.health }));
  const gateway: GatewaySnapshot = { proxies: gatewayProxies, allowlist: gatewayResources.allowlist };
  return deployPreflight(flow, services, schemas, active, gateway);
}

// -------------------------------------------------------------- block test

export async function testBlock(flowId: string, blockId: string): Promise<FlowBlock["testResult"]> {
  // Plain `request`, not `requestPendingAware`: the route is real (T7.5), so
  // a 404 here means the backend's actual detail — most commonly "Flow not
  // found" when the draft hasn't been saved yet — and that must reach the
  // caller verbatim rather than being flattened into the "engine pending"
  // message. `request` already turns a genuine 501 (adapter/mode not test-
  // runnable yet) into that pending message on its own.
  return request<FlowBlock["testResult"]>(`/api/v2/flows/${flowId}/blocks/${blockId}/test`, {
    method: "POST",
  });
}

// ------------------------------------------------------------------ dedup

export async function clearDedupCache(flowId: string, blockId: string): Promise<{ cleared: boolean }> {
  // Real endpoint (T7.4): blockId is a query param, not a body field. It
  // bumps a dedup epoch (see lifecycle.py's clear_dedup_cache docstring —
  // Redis isn't dialable from this host, so this is a deferred-to-redeploy
  // flush, not an instant one) and returns {blockId, dedupEpoch,
  // redeployRequired, message}; the mock's {cleared} shape is preserved by
  // reporting success as `cleared: true`.
  // Plain `request` — see testBlock's comment above; the route is real (T7.4)
  // so a 404's real detail (e.g. flow/block not found) must reach the caller.
  await request(`/api/v2/flows/${flowId}/dedup-cache/clear?blockId=${encodeURIComponent(blockId)}`, {
    method: "POST",
  });
  return { cleared: true };
}

// --------------------------------------------------------------- ceremony

function safeDisplayFields(avro: unknown): AvroField[] {
  try {
    return recordToDisplayFields(avro as AvroRecord);
  } catch {
    return [];
  }
}

/** Map a backend approved-schema doc (`avro` dict) onto the frontend's
 *  `ApprovedSchema` shape (`rawAvro` string + a derived `fields` display
 *  tree — `fields` is write-only derived output, never read back). */
function toApprovedSchema(doc: Record<string, unknown>): ApprovedSchema {
  const avro = doc.avro ?? {};
  const approvals: SchemaApproval[] = ((doc.approvals as Record<string, unknown>[]) ?? []).map((a) => ({
    version: a.version as number,
    approvedAt: a.approvedAt as string,
    provenance: a.provenance as SchemaProvenance,
    registryGlobalId: a.registryGlobalId as number,
    rawAvro: JSON.stringify(a.avro ?? {}, null, 2),
    ...(a.supersededAt ? { supersededAt: a.supersededAt as string } : {}),
    ...(a.evidence ? { prefilledFromLabel: String(a.evidence) } : {}),
  }));
  return {
    id: doc.id as string,
    subject: doc.subject as string,
    entity: doc.entity as string,
    flowId: doc.flowId as string,
    blockId: doc.blockId as string,
    provenance: doc.provenance as SchemaProvenance,
    fields: safeDisplayFields(avro),
    rawAvro: JSON.stringify(avro, null, 2),
    approvedAt: doc.approvedAt as string,
    registryGlobalId: doc.registryGlobalId as number,
    approvals,
    ...(doc.draftAvro != null ? { draftAvro: doc.draftAvro } : {}),
    ...(doc.draftUpdatedAt ? { draftUpdatedAt: doc.draftUpdatedAt as string } : {}),
  };
}

function toSchemaTemplate(doc: Record<string, unknown>): SchemaTemplate {
  const avro = doc.avro;
  return {
    id: doc.id as string,
    name: doc.name as string,
    description: (doc.description as string) ?? undefined,
    rawAvro: avro ? JSON.stringify(avro, null, 2) : "",
    createdAt: doc.createdAt as string,
    updatedAt: doc.updatedAt as string,
    ...(doc.registeredSubject ? { registeredSubject: doc.registeredSubject as string } : {}),
    ...(doc.registryGlobalId != null ? { registryGlobalId: doc.registryGlobalId as number } : {}),
    ...(doc.registeredVersion != null ? { registeredVersion: doc.registeredVersion as number } : {}),
    ...(doc.registeredAt ? { registeredAt: doc.registeredAt as string } : {}),
  };
}

function parseAvroOrThrow(rawAvro: string): unknown {
  try {
    return JSON.parse(rawAvro);
  } catch {
    throw new Error("The Avro record is not valid JSON.");
  }
}

export async function approveSchema(
  flowId: string,
  blockId: string,
  input: {
    entity: string;
    provenance: SchemaProvenance;
    fields: AvroField[];
    rawAvro: string;
    /** Frozen name of the library template the ceremony was pre-filled from. */
    prefilledFromLabel?: string;
  },
): Promise<ApprovedSchema> {
  const flow = await getFlow(flowId);
  if (!flow) throw new Error("Flow not found");
  const block = flow.blocks.find((b) => b.id === blockId);
  if (!block) throw new Error("Block not found");
  const topic = deriveTopicName(flow, block).value;
  const avro = parseAvroOrThrow(input.rawAvro);
  const doc = await request<Record<string, unknown>>("/api/v2/schemas/approve", {
    method: "POST",
    body: {
      flowId,
      blockId,
      entity: input.entity,
      topic,
      subject: `${topic}-value`,
      provenance: input.provenance,
      avro,
      ...(input.prefilledFromLabel ? { evidence: input.prefilledFromLabel } : {}),
    },
  });
  return toApprovedSchema(doc);
}

// --------------------------------------------------------- V2 schema ceremony

export type V2SchemaInferenceStatus =
  | "queued"
  | "deploying"
  | "running"
  | "collecting"
  | "inferring"
  | "cleaning_up"
  | "stopping"
  | "complete"
  | "failed"
  | "stopped";

export interface V2SchemaInferenceJob {
  id: string;
  flowId: string;
  targetBlockId: string;
  flowName: string;
  targetTopic: string;
  inferenceTopic: string;
  status: V2SchemaInferenceStatus;
  messagesCollected: number;
  targetMessages: number;
  nifiProcessGroupId?: string | null;
  generatedSchema?: unknown | null;
  schemaStatus?: string;
  error?: string | null;
  cleanupError?: string | null;
  createdAt: string;
  updatedAt: string;
}

export async function startSchemaInferenceV2(
  flowId: string,
  targetBlockId: string,
  targetMessages = 10,
): Promise<V2SchemaInferenceJob> {
  return request<V2SchemaInferenceJob>("/api/v2/schema-inference/start", {
    method: "POST",
    body: { flowId, targetBlockId, targetMessages },
  });
}

export async function getSchemaInferenceV2(jobId: string): Promise<V2SchemaInferenceJob> {
  return request<V2SchemaInferenceJob>(`/api/v2/schema-inference/${encodeURIComponent(jobId)}`);
}

export async function stopSchemaInferenceV2(jobId: string): Promise<V2SchemaInferenceJob> {
  return request<V2SchemaInferenceJob>(`/api/v2/schema-inference/${encodeURIComponent(jobId)}/stop`, { method: "POST" });
}

export async function listSchemas(): Promise<ApprovedSchema[]> {
  const data = await request<{ approved: Record<string, unknown>[]; templates: Record<string, unknown>[] }>(
    "/api/v2/schemas/",
  );
  const schemas = data.approved.map(toApprovedSchema);
  cache.schemas = schemas;
  loaded.schemas = true;
  return schemas;
}

/**
 * Save an edited-but-unregistered buffer onto an approved schema. This is
 * NOT registration — it never touches `approvals`, `rawAvro` or the registry
 * global id.
 */
export async function saveApprovedSchemaDraft(schemaId: string, avro: unknown): Promise<ApprovedSchema> {
  const doc = await request<Record<string, unknown>>(`/api/v2/schemas/${schemaId}/draft`, {
    method: "POST",
    body: { avro },
  });
  return toApprovedSchema(doc);
}

/**
 * Delete one approval from an approved schema's history. Refused when it is
 * the only one left.
 */
export async function deleteApprovedSchemaVersion(schemaId: string, version: number): Promise<ApprovedSchema> {
  const doc = await request<Record<string, unknown>>(`/api/v2/schemas/${schemaId}/versions/${version}`, {
    method: "DELETE",
  });
  return toApprovedSchema(doc);
}

/** Delete an approved schema entirely — every approval, gone with it. */
export async function deleteApprovedSchema(schemaId: string): Promise<void> {
  await request(`/api/v2/schemas/${schemaId}`, { method: "DELETE" });
}

// ---------------------------------------- verify / register (independent)
// Neither of these goes through the ceremony: `verify` is a read-only check
// (structural + optional registry compatibility), `register` writes straight
// to the registry. Both operate on whatever buffer the caller hands them.

export interface VerifySchemaResult {
  ok: boolean;
  /** Structural Avro problems, if any — empty when `ok`. */
  issues: string[];
  compatibility: {
    /** False when no subject was given, the schema was structurally invalid, or no registry connection exists. */
    checked: boolean;
    compatible: boolean | null;
    message: string;
  };
}

/** Structural Avro validation, plus — when `subject` is given — a registry
 *  compatibility check against its latest version. Registers nothing. */
export async function verifySchema(avro: unknown, subject?: string): Promise<VerifySchemaResult> {
  return request<VerifySchemaResult>("/api/v2/schemas/verify", {
    method: "POST",
    body: { avro, ...(subject ? { subject } : {}) },
  });
}

export interface RegisterSchemaResult {
  globalId: number;
  subject: string;
  /** The registry's own version marker — shape varies by registry backend (string or number). */
  version: string | number | null;
}

/** Register `avro` under `subject` directly, no ceremony. When `templateId`
 *  is given, the backend also stamps the template with `registeredSubject` /
 *  `registryGlobalId` so a later Register pre-fills from it. */
export async function registerSchema(
  subject: string,
  avro: unknown,
  templateId?: string,
): Promise<RegisterSchemaResult> {
  return request<RegisterSchemaResult>("/api/v2/schemas/register", {
    method: "POST",
    body: { subject, avro, ...(templateId ? { templateId } : {}) },
  });
}

// ------------------------------------------- registry version browsing (alpha parity)
// A registered template's local doc only ever tracks the CURRENT registry
// version (`registeredVersion`). These read straight from the registry's own
// ccompat subject history so older versions are reachable too — the template
// counterpart of an approved schema's `approvals` history.

export interface RegistrySubjectVersion {
  version: number;
}

/** Every version registered under `subject`, ascending. 404s (unknown subject)
 *  surface as a thrown `ApiRequestError` with status 404. */
export async function listRegistrySubjectVersions(subject: string): Promise<RegistrySubjectVersion[]> {
  return request<RegistrySubjectVersion[]>(
    `/api/v2/schemas/registry-subject/${encodeURIComponent(subject)}/versions`,
  );
}

export interface RegistrySubjectVersionDetail {
  version: number;
  globalId: number | null;
  avro: unknown;
}

/** One specific registered version's Avro, read straight from the registry. */
export async function getRegistrySubjectVersion(
  subject: string,
  version: number,
): Promise<RegistrySubjectVersionDetail> {
  const doc = await request<Record<string, unknown>>(
    `/api/v2/schemas/registry-subject/${encodeURIComponent(subject)}/versions/${version}`,
  );
  return {
    version: doc.version as number,
    globalId: (doc.globalId as number) ?? null,
    avro: doc.avro,
  };
}

// ------------------------------------------------------- library templates

export async function listSchemaTemplates(): Promise<SchemaTemplate[]> {
  const data = await request<{ approved: Record<string, unknown>[]; templates: Record<string, unknown>[] }>(
    "/api/v2/schemas/",
  );
  return data.templates.map(toSchemaTemplate);
}

export async function createSchemaTemplate(input: {
  name: string;
  description?: string;
  rawAvro: string;
}): Promise<SchemaTemplate> {
  const avro = parseAvroOrThrow(input.rawAvro);
  const doc = await request<Record<string, unknown>>("/api/v2/schemas/templates", {
    method: "POST",
    body: { name: input.name, description: input.description, avro },
  });
  return toSchemaTemplate(doc);
}

export async function saveSchemaTemplate(tpl: SchemaTemplate): Promise<SchemaTemplate> {
  const avro = parseAvroOrThrow(tpl.rawAvro);
  const doc = await request<Record<string, unknown>>(`/api/v2/schemas/templates/${tpl.id}`, {
    method: "PUT",
    body: { name: tpl.name, description: tpl.description, avro },
  });
  return toSchemaTemplate(doc);
}

/** Templates are bound to nothing, so deleting one is always allowed. */
export async function deleteSchemaTemplate(id: string): Promise<void> {
  await request(`/api/v2/schemas/templates/${id}`, { method: "DELETE" });
}

/** Lift an approved schema into the library so it can pre-fill later ceremonies. */
export async function saveApprovedAsTemplate(schemaId: string, name: string): Promise<SchemaTemplate> {
  const schemas = await listSchemas();
  const schema = schemas.find((s) => s.id === schemaId);
  if (!schema) throw new Error("Approved schema not found.");
  return createSchemaTemplate({
    name,
    description: `Copied from approved schema ${schema.subject} (global id ${schema.registryGlobalId}).`,
    rawAvro: schema.rawAvro,
  });
}

/**
 * Hand an edited schema to the ceremony that will register it. Kept
 * client-side (module-scoped, in-memory) — one page navigates to the
 * builder that reads it a moment later, all within the same SPA session.
 */
let pendingCeremonyDraft: CeremonyDraft | undefined;

export async function stageCeremonyDraft(draft: CeremonyDraft): Promise<void> {
  pendingCeremonyDraft = { ...draft };
}

/** Read the staged draft for this block and clear it — it is used once. */
export async function consumeCeremonyDraft(flowId: string, blockId: string): Promise<CeremonyDraft | null> {
  const draft = pendingCeremonyDraft;
  if (!draft || draft.flowId !== flowId || draft.blockId !== blockId) return null;
  pendingCeremonyDraft = undefined;
  return { ...draft };
}

// ------------------------------------------------------------ connections

export async function listConnections(): Promise<PlatformConnection[]> {
  const data = await request<PlatformConnection[]>("/api/v2/connections/");
  cache.connections = data;
  loaded.connections = true;
  return data;
}

export function connectionDependents(conn: PlatformConnection, state?: PrototypeState): string[] {
  const s = state ?? stateSnapshot();
  const deployed = s.flows.filter((f) => f.deployedAt);
  switch (conn.type) {
    case "nifi":
    case "kafka":
    case "apicurio":
      return conn.active ? deployed.map((f) => f.name) : [];
    case "kafka_connect":
      return conn.active ? deployed.filter((f) => f.blocks.some((b) => b.adapter === "kafka_kc" || b.adapter === "kc")).map((f) => f.name) : [];
    case "redis":
      return conn.active
        ? deployed
            .filter((f) => f.blocks.some((b) => b.transforms.some((t) => t.kind === "dedup") || (b.adapter === "jdbc" && b.config.incremental)))
            .map((f) => f.name)
        : [];
    case "apisix": {
      const proxyIds = new Set(s.gatewayProxies.map((p) => p.id));
      return conn.active
        ? deployed
            .filter((f) => f.blocks.some((b) => { const id = blockProxyId(b, s.services); return !!id && proxyIds.has(id); }))
            .map((f) => f.name)
        : [];
    }
  }
}

export async function saveConnection(conn: PlatformConnection): Promise<PlatformConnection> {
  return request<PlatformConnection>("/api/v2/connections/", { method: "POST", body: conn });
}

export type ConnectionTestResult = PlatformConnection & { message?: string };

export async function testConnection(id: string): Promise<ConnectionTestResult> {
  return request<ConnectionTestResult>(`/api/v2/connections/${id}/test`, { method: "POST" });
}

export async function activateConnection(id: string): Promise<void> {
  await request(`/api/v2/connections/${id}/activate`, { method: "POST" });
}

export interface NifiPlatformServiceResult {
  kind: string;
  name: string;
  type: string;
  status: "healthy" | "created" | "repaired" | "failed" | "blocked";
  serviceId?: string;
  state?: string;
  changedProperties?: string[];
  validationErrors?: string[];
  message?: string;
}

export interface NifiServiceReadinessResult {
  ok: boolean;
  connectionId: string;
  connectionName: string;
  rootProcessGroupId: string;
  services: NifiPlatformServiceResult[];
  summary: Record<string, number>;
  flowScopedServicesUntouched: boolean;
  message: string;
}

export async function checkNifiPlatformServices(connectionId: string): Promise<NifiServiceReadinessResult> {
  return request<NifiServiceReadinessResult>(`/api/v2/connections/${connectionId}/nifi-services/readiness`, { method: "POST" });
}

export async function deleteConnection(id: string): Promise<void> {
  await request(`/api/v2/connections/${id}`, { method: "DELETE" });
}

export async function getGatewayResources(): Promise<GatewayResources> {
  const data = await request<{ proxies: GatewayProxy[]; certProfiles: GatewayCertProfile[]; allowlist: string[] }>(
    "/api/v2/gateway/",
  );
  cache.gateway = { certProfiles: data.certProfiles, allowlist: data.allowlist };
  cache.gatewayProxies = data.proxies;
  loaded.gateway = true;
  return cache.gateway;
}

/**
 * The mock's `updateGatewayResources` replaced the whole {certProfiles,
 * allowlist} document in one write. The real backend has three separate
 * endpoints instead (cert-profile create/delete, allowlist add/remove), so
 * this diffs the caller's desired `next` against the server's current state
 * and issues exactly the calls implied by the difference — every call site
 * in Apisix.tsx only ever changes one cert or one host per call, so the
 * diff is always unambiguous. No call site needed to change.
 */
export async function updateGatewayResources(next: GatewayResources): Promise<GatewayResources> {
  const current = await getGatewayResources();

  const currentCertIds = new Set(current.certProfiles.map((c) => c.id));
  const nextCertIds = new Set(next.certProfiles.map((c) => c.id));
  for (const cert of next.certProfiles) {
    if (!currentCertIds.has(cert.id)) {
      await request("/api/v2/gateway/cert-profiles", {
        method: "POST",
        body: { name: cert.name, subject: cert.subject, expiresAt: cert.expiresAt },
      });
    }
  }
  for (const cert of current.certProfiles) {
    if (!nextCertIds.has(cert.id)) {
      await request(`/api/v2/gateway/cert-profiles/${cert.id}`, { method: "DELETE" });
    }
  }

  const currentHosts = new Set(current.allowlist);
  const nextHosts = new Set(next.allowlist);
  for (const host of next.allowlist) {
    if (!currentHosts.has(host)) {
      await request("/api/v2/gateway/allowlist", { method: "POST", body: { host, action: "add", adminConfirmed: true } });
    }
  }
  for (const host of current.allowlist) {
    if (!nextHosts.has(host)) {
      await request("/api/v2/gateway/allowlist", { method: "POST", body: { host, action: "remove", adminConfirmed: true } });
    }
  }

  return getGatewayResources();
}

// ------------------------------------------------------ APISIX proxy catalog

export async function listGatewayProxies(): Promise<GatewayProxy[]> {
  const data = await request<{ proxies: GatewayProxy[]; certProfiles: GatewayCertProfile[]; allowlist: string[] }>(
    "/api/v2/gateway/",
  );
  cache.gatewayProxies = data.proxies;
  cache.gateway = { certProfiles: data.certProfiles, allowlist: data.allowlist };
  loaded.gateway = true;
  return cache.gatewayProxies;
}

export async function getGatewayProxy(id: string): Promise<GatewayProxy | null> {
  const proxies = await listGatewayProxies();
  return proxies.find((p) => p.id === id) ?? null;
}

/**
 * Flows whose http blocks route through this proxy — every flow, not only the
 * deployed ones: a Draft that references a deleted proxy is broken too.
 */
export function proxyDependents(proxyId: string, state?: PrototypeState): string[] {
  const s = state ?? stateSnapshot();
  return s.flows.filter((f) => f.blocks.some((b) => blockProxyId(b, s.services) === proxyId)).map((f) => f.name);
}

export async function saveGatewayProxy(proxy: GatewayProxy): Promise<GatewayProxy> {
  return request<GatewayProxy>("/api/v2/gateway/proxies", { method: "POST", body: proxy });
}

export async function deleteGatewayProxy(id: string): Promise<void> {
  await request(`/api/v2/gateway/proxies/${id}`, { method: "DELETE" });
}

export interface ProxyTestResult {
  ok: boolean;
  detail: string;
  testedAt: string;
}

export async function testGatewayProxy(id: string): Promise<ProxyTestResult> {
  const data = await request<{ ok: boolean; message: string; testedAt: string }>(`/api/v2/gateway/proxies/${id}/test`, {
    method: "POST",
  });
  return { ok: data.ok, detail: data.message, testedAt: data.testedAt };
}

export async function reconcileGatewayProxy(id: string): Promise<GatewayProxy> {
  return request<GatewayProxy>(`/api/v2/gateway/proxies/${id}/reconcile`, { method: "POST" });
}

// --------------------------------------------------------------- services

export async function listServices(): Promise<AppService[]> {
  const data = await request<AppService[]>("/api/v2/services/");
  cache.services = data;
  loaded.services = true;
  return data;
}

export function serviceDependents(serviceId: string, state?: PrototypeState): Flow[] {
  const s = state ?? stateSnapshot();
  return s.flows.filter((f) => f.blocks.some((b) => b.serviceId === serviceId || b.config.sinkServiceId === serviceId));
}

export async function saveService(svc: AppService): Promise<AppService> {
  return request<AppService>("/api/v2/services/", { method: "POST", body: svc });
}

export async function testService(id: string): Promise<AppService> {
  return request<AppService>(`/api/v2/services/${id}/test`, { method: "POST" });
}

export async function retireService(id: string): Promise<void> {
  await request(`/api/v2/services/${id}/retire`, { method: "POST" });
}

export async function reinstateService(id: string): Promise<void> {
  await request(`/api/v2/services/${id}/reinstate`, { method: "POST" });
}

export async function deleteService(id: string): Promise<void> {
  await request(`/api/v2/services/${id}`, { method: "DELETE" });
  // Drop it from the read cache immediately. serviceDependents and friends
  // read this cache synchronously from render code, so leaving the deleted
  // service in place would hand out a ghost until the next listServices().
  cache.services = cache.services.filter((s) => s.id !== id);
}

/**
 * Why a delete may be refused, or null when it is allowed.
 *
 * Deletion is deliberately the second stage after retirement: retiring is
 * reversible and keeps the record, deleting is neither. The same two
 * conditions are enforced server-side in backend/routers/v2/services.py --
 * this exists to disable the button and explain why, not to be the guard.
 */
export function serviceDeleteBlockReason(svc: AppService): string | null {
  // Retirement is the only gate. Linked flows do NOT block deletion any more:
  // the consequence is carried by the flows instead (an undeployed one fails
  // validation with "The selected service no longer exists."; a deployed one
  // keeps running and gets a drift warning). The delete dialog spells that out
  // before the user confirms.
  if (!svc.retired) return "Retire the service before deleting it. Retirement is reversible; deletion is not.";
  return null;
}

/** What deleting this service will do to the flows using it — the two groups
 *  behave differently, so the dialog shows them separately. */
export function serviceDeleteImpact(
  svc: AppService,
  state?: PrototypeState,
): { deployed: Flow[]; undeployed: Flow[] } {
  const deps = serviceDependents(svc.id, state);
  return {
    deployed: deps.filter((f) => !!f.deployedAt),
    undeployed: deps.filter((f) => !f.deployedAt),
  };
}

/** flows that pinned an older revision than the service's current one */
export function serviceUpdateAvailable(flow: Flow, services: AppService[]): AppService[] {
  return Object.entries(flow.servicePins)
    .map(([id, rev]) => {
      const svc = services.find((s) => s.id === id);
      return svc && !svc.retired && svc.revision > rev ? svc : null;
    })
    .filter((s): s is AppService => !!s);
}

// ---------------------------------------------------------------- runtime
// The read-only ops view of a deployed flow. The deployment engine (NiFi /
// Kafka Connect) hasn't landed server-side yet, so these mostly read 404s
// today — that is surfaced honestly rather than simulated.

export async function getFlowRuntime(flowId: string): Promise<FlowRuntime | null> {
  try {
    return await request<FlowRuntime>(`/api/v2/flows/${flowId}/runtime`);
  } catch (err) {
    if (err instanceof ApiRequestError && err.status === 404) return null;
    throw err;
  }
}

/**
 * Simulated live read of NiFi + Connect in the old mock; now just GETs the
 * same runtime endpoint the read-only ops view reads. Real incremental
 * refresh (states/properties/timestamps only, drift never auto-clears)
 * lands with the deployment engine.
 */
export async function refreshFlowRuntime(flowId: string): Promise<FlowRuntime> {
  try {
    return await request<FlowRuntime>(`/api/v2/flows/${flowId}/runtime`);
  } catch (err) {
    if (err instanceof ApiRequestError && err.status === 404) {
      throw new Error("The flow has no runtime — deploy it before reading NiFi.");
    }
    throw err;
  }
}

export interface ForceRepairResult {
  runtime: FlowRuntime;
  orphans: RuntimeOrphan[];
  clearedFindings: number;
}

/** The explicit force path — only reachable as a confirmed user action. */
export async function forceRepairRuntime(flowId: string): Promise<ForceRepairResult> {
  return requestPendingAware<ForceRepairResult>(`/api/v2/flows/${flowId}/runtime/repair`, { method: "POST" });
}

// ------------------------------------------------------- everything else

export async function listAudit(search?: string): Promise<AuditEvent[]> {
  const params = new URLSearchParams();
  params.set("limit", "500");
  if (search?.trim()) params.set("search", search.trim());
  return request<AuditEvent[]>(`/api/v2/audit/?${params.toString()}`);
}

export async function getDlq(flowId: string): Promise<DlqRecord[]> {
  const data = await request<{ records: DlqRecord[] }>(`/api/v2/flows/${flowId}/dlq`);
  return data.records ?? [];
}

export async function getMetrics(flowId: string): Promise<FlowMetrics | null> {
  const data = await request<{ available: boolean } & Partial<FlowMetrics>>(`/api/v2/flows/${flowId}/metrics`);
  if (!data.available) return null;
  return data as FlowMetrics;
}

/**
 * NOTE (call-site adjustment): the mock's `getTopicMessages(topic)` took
 * only a topic name because the mock's single-document store had one global
 * `topicMessages` map. The real endpoint is flow-scoped
 * (`GET /api/v2/flows/{id}/messages?topic=`), so this needed a flowId too —
 * the one call site (Flows.tsx's flow detail sheet, which always has `flow`
 * in scope) was updated to `getTopicMessages(flow.id, msgTopic!)`.
 */
export async function getTopicMessages(flowId: string, topic: string): Promise<TopicMessage[]> {
  const data = await request<{ messages: TopicMessage[] }>(
    `/api/v2/flows/${flowId}/messages?topic=${encodeURIComponent(topic)}`,
  );
  return data.messages ?? [];
}

export interface ClearTopicResult {
  topic: string;
  before: number;
  after: number;
}

/**
 * Alpha parity: the ops-view "Clear Topics" destructive action (MVP §19.7).
 * Owned topics only — an unowned topic 404s, an adopted one 409s ("Adopted
 * topics are never cleared from here."); a successful clear returns the
 * real before/after retained-message counts the backend audited.
 */
export async function clearFlowTopic(flowId: string, topic: string): Promise<ClearTopicResult> {
  return request<ClearTopicResult>(`/api/v2/flows/${flowId}/topics/clear`, {
    method: "POST",
    body: { topic },
  });
}

// ---------------------------------------------------------- connectors
// Kept fully client-side (in-memory, per the task brief) — there is no
// backend persistence for published connector bundles.

let connectorsCache: ConnectorExport[] = [];

export async function listConnectors(): Promise<ConnectorExport[]> {
  return connectorsCache;
}

export async function publishConnector(flowId: string, name: string, description?: string): Promise<ConnectorExport> {
  const existing = connectorsCache.filter((c) => c.name === name);
  const version = existing.length > 0 ? Math.max(...existing.map((c) => c.version)) + 1 : 1;
  const connector: ConnectorExport = { id: uid("connector"), name, version, flowId, description, createdAt: nowIso() };
  connectorsCache = [...connectorsCache, connector];
  return connector;
}

/**
 * Finalize the (canned) connector import: creates a Draft flow with the
 * bundle's chain bound to the chosen services, persisted via `saveFlow`
 * against the live backend.
 */
export async function importConnectorFlow(input: {
  flowName: string;
  httpServiceId: string;
  sinkServiceId: string;
}): Promise<Flow> {
  const now = nowIso();
  const readId = uid("b");
  const sinkId = uid("b");
  const flow: Flow = {
    id: uid("flow"),
    name: input.flowName,
    description: "Imported from connector fortisiem-to-opensearch@1 — services re-bound at import.",
    state: "Draft",
    enabled: false,
    cron: "*/15 * * * *",
    blocks: [
      {
        id: readId,
        adapter: "http",
        mode: "read",
        name: "Fetch Incidents",
        parentId: null,
        serviceId: input.httpServiceId,
        entity: null,
        config: {
          method: "GET",
          path: "/phoenix/rest/incident/list",
          responseFormat: "json",
          recordPath: "$.incidents[*]",
          split: true,
          pagination: { type: "cursor", fields: { cursorParam: "nextToken", cursorPath: "$.nextToken", stop: "cursor_empty" } },
          proxyId: null,
        },
        transforms: [],
        testResult: null,
      },
      {
        id: sinkId,
        adapter: "kafka",
        mode: "write",
        name: "Incidents Topic",
        parentId: readId,
        serviceId: null,
        entity: "incident",
        config: {},
        transforms: [],
        testResult: null,
      },
    ],
    topics: [],
    variables: [],
    servicePins: {},
    deployedAt: null,
    lastRunAt: null,
    createdAt: now,
    updatedAt: now,
  };
  syncFlowTopics(flow);
  // The bundle's kc subscription attaches to the materialized topic.
  const topic = flow.topics[0];
  if (topic) {
    flow.blocks.push({
      id: uid("b"),
      adapter: "kc",
      name: "OpenSearch Incident Index",
      parentId: topic.id,
      serviceId: input.sinkServiceId,
      entity: "incident",
      config: {
        attachTopicId: topic.id,
        initialPosition: "beginning",
        sinkConfig: {
          "connector.class": "io.aiven.kafka.connect.opensearch.OpensearchSinkConnector",
          "connection.url": "https://opensearch.internal.corp:9200",
          "behavior.on.malformed.documents": "warn",
        },
      },
      transforms: [],
      testResult: null,
    });
  }
  return saveFlow(flow);
}

// Global variables are gone: `${...}` placeholders now resolve from upstream
// extractions and per-flow variables only, which is the only scope a flow can
// reason about without a second, invisible namespace.

export interface DashboardSummary {
  totalFlows: number;
  runningFlows: number;
  approvedSchemas: number;
  connectionsHealthy: number;
  connectionsTotal: number;
  /** Active application-managed sink connectors whose state is RUNNING. */
  sinkConnectorsRunning: number;
  /** Every sink connector a deployed flow owns — the denominator. */
  sinkConnectorsTotal: number;
  /**
   * kc / kafka_kc blocks in flows that were never deployed. They are real sinks
   * the user configured, but no connector exists for them yet, so they are
   * counted separately instead of quietly widening the denominator.
   */
  sinkConnectorsUndeployed: number;
}

export async function getDashboardSummary(): Promise<DashboardSummary> {
  return request<DashboardSummary>("/api/v2/dashboard/summary");
}

export type { PreflightCheck, ValidationIssue };
