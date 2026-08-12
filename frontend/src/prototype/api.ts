// Typed mock service layer. Simulates the eventual backend: latency,
// deterministic test results, lifecycle transitions, audit trail. All state
// lives in the localStorage store; nothing touches the network.

import { getState, mutate, resetDemoData, uid } from "./store";
import { deriveTopicName } from "./naming";
import { NIFI_INSTANCE_FINGERPRINTS, platformControllerServices, platformRedisService } from "./seeds";
import {
  blockProxyId,
  deployPreflight,
  validateFlow,
  type GatewaySnapshot,
  type PreflightCheck,
  type ValidationIssue,
} from "./validation";
import type {
  AppService,
  ApprovedSchema,
  AuditEvent,
  AvroField,
  CeremonyDraft,
  ConnectConnectorRuntime,
  ConnectorExport,
  ControllerServiceRuntime,
  DlqRecord,
  Flow,
  FlowBlock,
  FlowMetrics,
  FlowRuntime,
  GatewayProxy,
  GatewayResources,
  NifiComponent,
  NifiComponentState,
  PlatformConnection,
  PrototypeState,
  RuntimeOrphan,
  RuntimeProperty,
  SchemaApproval,
  SchemaProvenance,
  SchemaTemplate,
  TopicMessage,
} from "./types";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const nowIso = () => new Date().toISOString();

/** The gateway facts validation needs, read off one state snapshot. */
function gatewayOf(state: PrototypeState): GatewaySnapshot {
  return { proxies: state.gatewayProxies, allowlist: state.gateway.allowlist };
}

function audit(state: PrototypeState, action: string, object: string, target: string, status: AuditEvent["status"] = "Success", details?: string) {
  state.audit.unshift({ id: uid("a"), ts: nowIso(), user: "admin", action, object, target, status, details });
}

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
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
  await sleep(120);
  return clone(getState().flows);
}

export async function getFlow(id: string): Promise<Flow | null> {
  await sleep(80);
  const flow = getState().flows.find((f) => f.id === id);
  return flow ? clone(flow) : null;
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
  await sleep(150);
  return mutate((state) => {
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
      createdAt: nowIso(),
      updatedAt: nowIso(),
    };
    state.flows.unshift(flow);
    audit(state, "Flow created", "Flow", name);
    return clone(flow);
  });
}

export async function saveFlow(updated: Flow): Promise<Flow> {
  await sleep(200);
  return mutate((state) => {
    const idx = state.flows.findIndex((f) => f.id === updated.id);
    if (idx === -1) throw new Error("Flow not found");
    const next = clone(updated);
    syncFlowTopics(next);
    next.updatedAt = nowIso();
    state.flows[idx] = next;
    audit(state, "Draft saved", "Flow", next.name);
    return clone(next);
  });
}

/** The block-reason contract: null = allowed, string = why not. */
export function getVerbBlockReason(flow: Flow, verb: FlowVerb, state?: PrototypeState): string | null {
  const s = state ?? getState();
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
    delete: () => (deployed ? "Undeploy the flow before deleting it." : null),
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
  const pre = getState().flows.find((f) => f.id === flowId);
  if (!pre) throw new Error("Flow not found");
  const reason = getVerbBlockReason(pre, verb);
  if (reason) throw new Error(reason);

  if (verb === "deploy" || verb === "redeploy") {
    mutate((state) => {
      const f = state.flows.find((x) => x.id === flowId)!;
      f.state = "Deploying";
    });
    await sleep(1600);
    return mutate((state) => {
      const f = state.flows.find((x) => x.id === flowId)!;
      f.state = "Stopped";
      f.deployedAt = nowIso();
      f.updatedAt = nowIso();
      for (const b of f.blocks) if (b.serviceId) f.servicePins[b.serviceId] = state.services.find((sv) => sv.id === b.serviceId)?.revision ?? 1;
      // The compiler emits the runtime: generated components, compiled
      // controller services, Connect connectors. A redeploy compiles the block
      // config back over anything edited out of band, so property drift dies
      // here — the orphan ledger is carried forward, because it never does.
      const previous = state.runtimes.find((r) => r.flowId === f.id);
      const runtime = synthesizeRuntime(f, state, previous);
      state.runtimes = [...state.runtimes.filter((r) => r.flowId !== f.id), runtime];
      f.drift = null;
      audit(state, verb === "deploy" ? "Flow deployed" : "Flow redeployed", "Flow", f.name);
      return clone(f);
    });
  }

  await sleep(500);
  return mutate((state) => {
    const f = state.flows.find((x) => x.id === flowId)!;
    switch (verb) {
      case "start":
        f.state = "Running";
        f.lastRunAt = nowIso();
        audit(state, "Flow started", "Flow", f.name);
        break;
      case "pause":
        f.state = "Paused";
        audit(state, "Flow paused", "Flow", f.name, "Success", "Trigger keeps firing; records queue until Resume");
        break;
      case "resume":
        f.state = "Running";
        audit(state, "Flow resumed", "Flow", f.name);
        break;
      case "stop":
        f.state = "Stopped";
        audit(state, "Flow stopped", "Flow", f.name, "Success", "Queues retained");
        break;
      case "stop_clear":
        f.state = "Stopped";
        audit(state, "Flow stopped & cleared", "Flow", f.name, "Warning", "Queued records discarded — audited");
        break;
      case "undeploy":
        f.state = "Draft";
        f.deployedAt = null;
        f.drift = null;
        // A clean undeploy removes the runtime properly — no orphans, because
        // the platform did the removal itself.
        state.runtimes = state.runtimes.filter((r) => r.flowId !== flowId);
        audit(state, "Flow undeployed", "Flow", f.name, "Warning", "Generated topics emptied · dedup caches cleared · positions reset");
        break;
      case "delete": {
        const name = f.name;
        state.flows = state.flows.filter((x) => x.id !== flowId);
        state.runtimes = state.runtimes.filter((r) => r.flowId !== flowId);
        audit(state, "Flow deleted", "Flow", name, "Warning");
        return null;
      }
    }
    f.updatedAt = nowIso();
    return clone(f);
  });
}

export async function setFlowEnabled(flowId: string, enabled: boolean): Promise<void> {
  await sleep(200);
  mutate((state) => {
    const f = state.flows.find((x) => x.id === flowId);
    if (!f) return;
    f.enabled = enabled;
    audit(state, enabled ? "Flow enabled" : "Flow disabled", "Flow", f.name);
  });
}

export function validateFlowNow(flow: Flow): ValidationIssue[] {
  const s = getState();
  return validateFlow(flow, s.services, s.schemas, gatewayOf(s));
}

export async function getPreflight(flow: Flow): Promise<PreflightCheck[]> {
  await sleep(700);
  const s = getState();
  const active = s.connections.filter((c) => c.active).map((c) => ({ type: c.type, name: c.name, health: c.health }));
  return deployPreflight(flow, s.services, s.schemas, active, gatewayOf(s));
}

// -------------------------------------------------------------- block test

const TEST_SAMPLES: Record<string, unknown[]> = {
  "svc-rapid7": [
    { id: 1204, hostName: "srv-dc01.corp.local", os: "Windows Server 2022", riskScore: 7211, siteId: 3 },
    { id: 1205, hostName: "srv-web02.dmz.corp", os: "Ubuntu 22.04", riskScore: 18342, siteId: 3 },
    { id: 1206, hostName: "wks-fin-114.corp.local", os: "Windows 11", riskScore: 903, siteId: 5 },
  ],
  "svc-fortisiem": [
    { incidentId: 88121, eventSeverityCat: "HIGH", incidentTitle: "Brute-force attempt on vpn-gw01", srcIp: "203.0.113.44" },
    { incidentId: 88122, eventSeverityCat: "LOW", incidentTitle: "Interface flap on sw-edge07", srcIp: "10.4.2.7" },
  ],
  "svc-servicenow": [
    { sys_id: "a91f", name: "srv-legacy-11", install_status: "retired", decommission_date: "2026-07-30" },
    { sys_id: "b23c", name: "srv-app-04", install_status: "in_use", decommission_date: null },
  ],
  "svc-postgres": [
    { asset_id: 40122, hostname: "db-prod-03", owner_group: "dba", environment: "production", updated_at: "2026-08-09T18:22:10Z" },
    { asset_id: 40123, hostname: "app-prod-11", owner_group: "platform", environment: "production", updated_at: "2026-08-10T02:11:44Z" },
  ],
  "svc-partner-kafka": [{ indicator_id: "ioc-7781", type: "ip", value: "198.51.100.23", confidence: 82 }],
};

export async function testBlock(flowId: string, blockId: string): Promise<FlowBlock["testResult"]> {
  await sleep(1100);
  return mutate((state) => {
    const flow = state.flows.find((f) => f.id === flowId);
    const block = flow?.blocks.find((b) => b.id === blockId);
    if (!flow || !block) throw new Error("Block not found");
    const svc = state.services.find((s) => s.id === block.serviceId);
    let result: FlowBlock["testResult"];
    if (svc?.retired) {
      result = { ok: false, reason: `Service "${svc.name}" is retired — 410 Gone from the gateway.`, testedAt: nowIso() };
    } else if (block.adapter === "kafka" && block.config.parseFormat === "raw") {
      result = { ok: true, records: ["(binary payload · 412 bytes)", "(binary payload · 388 bytes)"], detectedFields: [], testedAt: nowIso() };
    } else {
      const records = TEST_SAMPLES[block.serviceId ?? ""] ?? [{ sample: true, note: "10-record bounded probe (simulated)" }];
      result = {
        ok: true,
        records: records.slice(0, 10),
        detectedFields: Object.keys((records[0] as Record<string, unknown>) ?? {}),
        testedAt: nowIso(),
      };
    }
    block.testResult = result;
    audit(state, "Block tested", "Stream", `${flow.name} · ${block.name}`, result.ok ? "Success" : "Failed", result.ok ? "Bounded probe, max 10 records — nothing committed" : result.reason);
    return clone(result);
  });
}

// --------------------------------------------------------------- ceremony

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
  await sleep(1200);
  return mutate((state) => {
    const registry = state.connections.find((c) => c.type === "apicurio" && c.active);
    if (!registry || registry.health !== "Healthy") {
      audit(state, "Schema approval failed", "Schema", input.entity, "Failed", "Registry registration failed — approval fails with it");
      throw new Error("Registration failed: no healthy active schema registry connection. Approve = register — the approval fails with it.");
    }
    const flow = state.flows.find((f) => f.id === flowId)!;
    const block = flow.blocks.find((b) => b.id === blockId)!;
    block.entity = input.entity;
    const topic = deriveTopicName(flow, block).value;
    const approvedAt = nowIso();
    // Approve = register: the id is handed out from a monotonic counter, never
    // derived from array length (that collides after any delete).
    const registryGlobalId = state.registryGlobalIdSeq;
    state.registryGlobalIdSeq += 1;

    // History has to be read off the OUTGOING record before the filter below
    // removes it — otherwise every re-run wipes the record's own past.
    const outgoing = state.schemas.find((s) => s.flowId === flowId && s.blockId === blockId);
    const history: SchemaApproval[] = (outgoing?.approvals ?? []).map((a, idx, all) =>
      idx === all.length - 1 ? { ...a, supersededAt: a.supersededAt ?? approvedAt } : a,
    );
    history.push({
      version: history.length + 1,
      approvedAt,
      provenance: input.provenance,
      registryGlobalId,
      rawAvro: input.rawAvro,
      ...(input.prefilledFromLabel ? { prefilledFromLabel: input.prefilledFromLabel } : {}),
    });

    const schema: ApprovedSchema = {
      id: outgoing?.id ?? uid("schema"),
      subject: `${topic}-value`,
      entity: input.entity,
      flowId,
      blockId,
      provenance: input.provenance,
      fields: input.fields,
      rawAvro: input.rawAvro,
      approvedAt,
      registryGlobalId,
      approvals: history,
    };
    state.schemas = state.schemas.filter((s) => !(s.flowId === flowId && s.blockId === blockId));
    state.schemas.push(schema);
    audit(
      state,
      "Schema approved",
      "Schema",
      schema.subject,
      "Success",
      `Registered as global id ${schema.registryGlobalId} · approval ${history.length}${history.length > 1 ? ` (supersedes #${history[history.length - 2].registryGlobalId})` : ""} · evidence: ${input.provenance === "sample_run" ? "live sample run" : input.provenance === "uploaded" ? "uploaded samples" : "manually authored — not sample-validated"}${input.prefilledFromLabel ? ` · pre-filled from "${input.prefilledFromLabel}"` : ""}`,
    );
    return clone(schema);
  });
}

export async function listSchemas(): Promise<ApprovedSchema[]> {
  await sleep(100);
  return clone(getState().schemas);
}

// ------------------------------------------------------- library templates
// Hand-authored, unregistered, bound to nothing. They live in their own
// collection: `state.schemas.length` guards the registry connection's edit and
// delete, so an unregistered template in there would lock it for the wrong
// reason and corrupt global-id allocation.

export async function listSchemaTemplates(): Promise<SchemaTemplate[]> {
  await sleep(100);
  return clone(getState().schemaTemplates);
}

export async function createSchemaTemplate(input: {
  name: string;
  description?: string;
  rawAvro: string;
}): Promise<SchemaTemplate> {
  await sleep(250);
  return mutate((state) => {
    const tpl: SchemaTemplate = {
      id: uid("tpl"),
      name: input.name,
      description: input.description,
      rawAvro: input.rawAvro,
      createdAt: nowIso(),
      updatedAt: nowIso(),
    };
    state.schemaTemplates.push(tpl);
    audit(state, "Schema template created", "Schema", tpl.name, "Success", "Library template — not registered, bound to no flow");
    return clone(tpl);
  });
}

export async function saveSchemaTemplate(tpl: SchemaTemplate): Promise<SchemaTemplate> {
  await sleep(250);
  return mutate((state) => {
    const idx = state.schemaTemplates.findIndex((t) => t.id === tpl.id);
    if (idx === -1) throw new Error("Template not found.");
    const next: SchemaTemplate = { ...clone(tpl), createdAt: state.schemaTemplates[idx].createdAt, updatedAt: nowIso() };
    state.schemaTemplates[idx] = next;
    audit(state, "Schema template saved", "Schema", next.name);
    return clone(next);
  });
}

export async function duplicateSchemaTemplate(id: string): Promise<SchemaTemplate> {
  await sleep(200);
  return mutate((state) => {
    const source = state.schemaTemplates.find((t) => t.id === id);
    if (!source) throw new Error("Template not found.");
    const copy: SchemaTemplate = {
      ...clone(source),
      id: uid("tpl"),
      name: `${source.name} (copy)`,
      createdAt: nowIso(),
      updatedAt: nowIso(),
    };
    state.schemaTemplates.push(copy);
    audit(state, "Schema template duplicated", "Schema", copy.name);
    return clone(copy);
  });
}

/** Templates are bound to nothing, so deleting one is always allowed. */
export async function deleteSchemaTemplate(id: string): Promise<void> {
  await sleep(200);
  mutate((state) => {
    const tpl = state.schemaTemplates.find((t) => t.id === id);
    state.schemaTemplates = state.schemaTemplates.filter((t) => t.id !== id);
    if (tpl)
      audit(
        state,
        "Schema template deleted",
        "Schema",
        tpl.name,
        "Warning",
        "Approvals pre-filled from it keep its name as a frozen history line",
      );
  });
}

/** Lift an approved schema into the library so it can pre-fill later ceremonies. */
export async function saveApprovedAsTemplate(schemaId: string, name: string): Promise<SchemaTemplate> {
  await sleep(250);
  return mutate((state) => {
    const schema = state.schemas.find((s) => s.id === schemaId);
    if (!schema) throw new Error("Approved schema not found.");
    const tpl: SchemaTemplate = {
      id: uid("tpl"),
      name,
      description: `Copied from approved schema ${schema.subject} (global id ${schema.registryGlobalId}).`,
      rawAvro: schema.rawAvro,
      createdAt: nowIso(),
      updatedAt: nowIso(),
    };
    state.schemaTemplates.push(tpl);
    audit(state, "Schema saved as template", "Schema", tpl.name, "Success", `Copied from ${schema.subject} — the template itself is not registered`);
    return clone(tpl);
  });
}

/**
 * Hand an edited schema to the ceremony that will register it.
 *
 * Only one draft is ever in flight: it is written by the click that navigates to
 * the builder and read by the ceremony that opens a moment later, so a second
 * one can only mean the first was abandoned. Keeping a queue would preserve
 * exactly the drafts nobody asked for.
 */
export async function stageCeremonyDraft(draft: CeremonyDraft): Promise<void> {
  await sleep(60);
  mutate((state) => {
    state.pendingCeremonyDraft = { ...draft };
  });
}

/** Read the staged draft for this block and clear it — it is used once. */
export async function consumeCeremonyDraft(flowId: string, blockId: string): Promise<CeremonyDraft | null> {
  await sleep(30);
  return mutate((state) => {
    const draft = state.pendingCeremonyDraft;
    if (!draft || draft.flowId !== flowId || draft.blockId !== blockId) return null;
    delete state.pendingCeremonyDraft;
    return clone(draft);
  });
}

// ------------------------------------------------------------ connections

export async function listConnections(): Promise<PlatformConnection[]> {
  await sleep(100);
  return clone(getState().connections);
}

export function connectionDependents(conn: PlatformConnection, state?: PrototypeState): string[] {
  const s = state ?? getState();
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
      // `config.proxyId` replaced the boolean `config.proxy`. A block only
      // depends on the gateway when its reference actually resolves.
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
  await sleep(250);
  return mutate((state) => {
    const idx = state.connections.findIndex((c) => c.id === conn.id);
    if (idx !== -1 && conn.type === "apicurio" && state.connections[idx].active && state.schemas.length > 0) {
      throw new Error(
        `The registry connection cannot be edited while ${state.schemas.length} approved schema(s) are registered through it.`,
      );
    }
    const next = clone(conn);
    if (idx === -1) {
      next.id = next.id || uid("conn");
      next.active = !state.connections.some((c) => c.type === next.type && c.active);
      state.connections.push(next);
      audit(state, "Connection created", "Platform Connection", next.name);
    } else {
      state.connections[idx] = next;
      audit(state, "Connection updated", "Platform Connection", next.name);
    }
    return clone(next);
  });
}

export async function testConnection(id: string): Promise<PlatformConnection> {
  await sleep(1000);
  return mutate((state) => {
    const conn = state.connections.find((c) => c.id === id);
    if (!conn) throw new Error("Connection not found");
    const fails = conn.name.toLowerCase().includes("legacy");
    conn.health = fails ? "Failed" : "Healthy";
    conn.reachability = fails ? "Unreachable" : "Reachable";
    conn.lastTestedAt = nowIso();
    audit(state, fails ? "Connection test failed" : "Connection tested", "Platform Connection", conn.name, fails ? "Failed" : "Success", fails ? "Connection refused" : undefined);
    return clone(conn);
  });
}

export async function activateConnection(id: string): Promise<void> {
  await sleep(600);
  mutate((state) => {
    const conn = state.connections.find((c) => c.id === id);
    if (!conn) throw new Error("Connection not found");
    const current = state.connections.find((c) => c.type === conn.type && c.active && c.id !== id);
    if (current) {
      const deps = connectionDependents(current, state);
      if (deps.length > 0 && conn.type !== "redis") {
        throw new Error(
          `"${current.name}" has ${deps.length} dependent flow(s) — use Repoint (adopt / migrate / reset) instead of a bare activation.`,
        );
      }
      current.active = false;
    }
    conn.active = true;
    audit(state, "Connection activated", "Platform Connection", conn.name, "Success", conn.type === "redis" ? "Redis switch: dedup windows and bookmarks on the old instance are lost" : undefined);
  });
}

export async function deleteConnection(id: string): Promise<void> {
  await sleep(300);
  mutate((state) => {
    const conn = state.connections.find((c) => c.id === id);
    if (!conn) return;
    const deps = connectionDependents(conn, state);
    if (deps.length > 0) throw new Error(`Cannot delete: ${deps.length} deployed flow(s) depend on it (${deps.slice(0, 3).join(", ")}${deps.length > 3 ? "…" : ""}).`);
    if (conn.type === "apicurio" && state.schemas.length > 0 && conn.active)
      throw new Error(`Cannot delete: ${state.schemas.length} approved schema(s) are registered through this registry connection.`);
    state.connections = state.connections.filter((c) => c.id !== id);
    audit(state, "Connection deleted", "Platform Connection", conn.name, "Warning");
  });
}

export interface RepointStep {
  label: string;
  status: "done" | "active" | "pending";
}

export async function repointConnection(id: string, mode: "adopt" | "migrate" | "reset", onStep: (steps: RepointStep[]) => void): Promise<void> {
  const conn = getState().connections.find((c) => c.id === id);
  if (!conn) throw new Error("Connection not found");
  const steps = [
    "Fingerprint identity check",
    mode === "adopt" ? "Adopting existing resources" : mode === "migrate" ? "Re-creating managed resources" : "Resetting platform state",
    "Verifying dependents",
    "Recording audit trail",
  ];
  for (let i = 0; i <= steps.length; i++) {
    onStep(steps.map((label, idx) => ({ label, status: idx < i ? "done" : idx === i ? "active" : "pending" })));
    await sleep(700);
  }
  mutate((state) => {
    const c = state.connections.find((x) => x.id === id);
    if (c) {
      const previous = state.connections.find((x) => x.type === c.type && x.active && x.id !== id);
      if (previous) previous.active = false;
      c.active = true;
    }
    audit(state, "Repoint completed", "Platform Connection", `${conn.name} (${mode})`, "Success", "Per-item progress recorded");
  });
}

export async function getGatewayResources(): Promise<GatewayResources> {
  await sleep(80);
  return clone(getState().gateway);
}

export async function updateGatewayResources(next: GatewayResources): Promise<GatewayResources> {
  await sleep(250);
  return mutate((state) => {
    state.gateway = clone(next);
    audit(state, "Gateway resources updated", "Gateway", "APISIX Gateway");
    return clone(state.gateway);
  });
}

// ------------------------------------------------------ APISIX proxy catalog

export async function listGatewayProxies(): Promise<GatewayProxy[]> {
  await sleep(100);
  return clone(getState().gatewayProxies);
}

export async function getGatewayProxy(id: string): Promise<GatewayProxy | null> {
  await sleep(80);
  const proxy = getState().gatewayProxies.find((p) => p.id === id);
  return proxy ? clone(proxy) : null;
}

/**
 * Flows whose http blocks route through this proxy — every flow, not only the
 * deployed ones: a Draft that references a deleted proxy is broken too.
 */
export function proxyDependents(proxyId: string, state?: PrototypeState): string[] {
  const s = state ?? getState();
  return s.flows.filter((f) => f.blocks.some((b) => blockProxyId(b, s.services) === proxyId)).map((f) => f.name);
}

/** Config that, once changed, has to be pushed to the gateway again. */
function reconciledFields(p: GatewayProxy): string {
  return JSON.stringify([p.targetHost, p.port, p.sni ?? null, p.path, [...p.methods].sort(), p.certProfileId ?? null]);
}

export async function saveGatewayProxy(proxy: GatewayProxy): Promise<GatewayProxy> {
  await sleep(300);
  return mutate((state) => {
    if (!proxy.name.trim()) throw new Error("Name the proxy — flows reference it by name.");
    if (!proxy.targetHost.trim()) throw new Error("Set the target host.");
    const idx = state.gatewayProxies.findIndex((p) => p.id === proxy.id);
    const clash = state.gatewayProxies.find((p) => p.id !== proxy.id && p.name.trim() === proxy.name.trim());
    if (clash) throw new Error(`Another proxy is already called "${proxy.name}".`);

    const next = clone(proxy);
    next.updatedAt = nowIso();
    if (idx === -1) {
      next.id = next.id || uid("gw-proxy");
      next.createdAt = nowIso();
      next.status = "Pending";
      next.statusDetail = "Created — not yet reconciled onto the gateway.";
      state.gatewayProxies.push(next);
      audit(state, "Gateway proxy created", "Gateway", next.name, "Success", `${next.targetHost}:${next.port}${next.path}`);
    } else {
      const before = state.gatewayProxies[idx];
      next.createdAt = before.createdAt;
      if (reconciledFields(before) !== reconciledFields(next)) {
        next.status = "Pending";
        next.statusDetail = "Configuration changed — reconcile to push it to the gateway.";
      } else {
        next.status = before.status;
        next.statusDetail = before.statusDetail;
      }
      state.gatewayProxies[idx] = next;
      audit(state, "Gateway proxy updated", "Gateway", next.name, "Success", next.status === "Pending" ? "Reconciliation required" : undefined);
    }
    return clone(next);
  });
}

export async function deleteGatewayProxy(id: string): Promise<void> {
  await sleep(300);
  mutate((state) => {
    const proxy = state.gatewayProxies.find((p) => p.id === id);
    if (!proxy) return;
    const deps = proxyDependents(id, state);
    if (deps.length > 0)
      throw new Error(
        `Cannot delete: ${deps.length} flow(s) route through "${proxy.name}" (${deps.slice(0, 3).join(", ")}${deps.length > 3 ? "…" : ""}). Repoint them first.`,
      );
    state.gatewayProxies = state.gatewayProxies.filter((p) => p.id !== id);
    audit(state, "Gateway proxy deleted", "Gateway", proxy.name, "Warning");
  });
}

export interface ProxyTestResult {
  ok: boolean;
  detail: string;
  testedAt: string;
}

/** Simulated egress probe — no network, deterministic from the seeded facts. */
export async function testGatewayProxy(id: string): Promise<ProxyTestResult> {
  await sleep(900);
  return mutate((state) => {
    const proxy = state.gatewayProxies.find((p) => p.id === id);
    if (!proxy) throw new Error("Proxy not found.");
    const allowlisted = state.gateway.allowlist.includes(proxy.targetHost);
    const conn = state.connections.find((c) => c.type === "apisix" && c.active);
    let result: ProxyTestResult;
    if (!conn || conn.health !== "Healthy") {
      result = { ok: false, detail: "No healthy active APISIX connection — the gateway itself is unreachable.", testedAt: nowIso() };
    } else if (!allowlisted) {
      result = { ok: false, detail: `Host "${proxy.targetHost}" is not on the gateway allowlist — the probe is refused before it leaves.`, testedAt: nowIso() };
    } else if (proxy.status === "Failed") {
      result = { ok: false, detail: proxy.statusDetail ?? "The proxy failed to reconcile; nothing is listening yet.", testedAt: nowIso() };
    } else {
      result = { ok: true, detail: `Reached ${proxy.targetHost}:${proxy.port}${proxy.path} — TLS handshake ok, HTTP 200.`, testedAt: nowIso() };
    }
    audit(state, result.ok ? "Gateway proxy tested" : "Gateway proxy test failed", "Gateway", proxy.name, result.ok ? "Success" : "Failed", result.detail);
    return { ...result };
  });
}

/**
 * Simulated reconciliation: pushes the proxy definition onto the gateway and
 * flips its status. An un-allowlisted host is the one honest failure — egress
 * hosts are admin-allowlisted, so the gateway refuses the route.
 */
export async function reconcileGatewayProxy(id: string): Promise<GatewayProxy> {
  const exists = getState().gatewayProxies.some((p) => p.id === id);
  if (!exists) throw new Error("Proxy not found.");
  mutate((state) => {
    const proxy = state.gatewayProxies.find((p) => p.id === id)!;
    proxy.status = "Pending";
    proxy.statusDetail = "Reconciling — pushing the route and upstream to the gateway…";
    proxy.updatedAt = nowIso();
  });
  await sleep(1400);
  return mutate((state) => {
    const proxy = state.gatewayProxies.find((p) => p.id === id)!;
    const allowlisted = state.gateway.allowlist.includes(proxy.targetHost);
    const conn = state.connections.find((c) => c.type === "apisix" && c.active);
    if (!conn || conn.health !== "Healthy") {
      proxy.status = "Failed";
      proxy.statusDetail = "No healthy active APISIX connection — the definition could not be pushed.";
    } else if (!allowlisted) {
      proxy.status = "Failed";
      proxy.statusDetail = `Host "${proxy.targetHost}" is not on the gateway allowlist — an administrator has to add it first.`;
    } else {
      proxy.status = "Reconciled";
      proxy.statusDetail = undefined;
    }
    proxy.updatedAt = nowIso();
    audit(
      state,
      proxy.status === "Reconciled" ? "Gateway proxy reconciled" : "Gateway proxy reconciliation failed",
      "Gateway",
      proxy.name,
      proxy.status === "Reconciled" ? "Success" : "Failed",
      proxy.statusDetail ?? `${proxy.targetHost}:${proxy.port}${proxy.path} is live on the gateway`,
    );
    return clone(proxy);
  });
}

// --------------------------------------------------------------- services

export async function listServices(): Promise<AppService[]> {
  await sleep(100);
  return clone(getState().services);
}

export function serviceDependents(serviceId: string, state?: PrototypeState): Flow[] {
  const s = state ?? getState();
  return s.flows.filter((f) => f.blocks.some((b) => b.serviceId === serviceId || b.config.sinkServiceId === serviceId));
}

export async function saveService(svc: AppService): Promise<AppService> {
  await sleep(250);
  return mutate((state) => {
    const idx = state.services.findIndex((x) => x.id === svc.id);
    const next = clone(svc);
    next.updatedAt = nowIso();
    if (idx === -1) {
      next.id = next.id || uid("svc");
      next.revision = 1;
      next.createdAt = nowIso();
      state.services.push(next);
      audit(state, "Service created", "Application Service", next.name);
    } else {
      next.revision = state.services[idx].revision + 1;
      state.services[idx] = next;
      audit(state, "Service revision created", "Application Service", `${next.name} (rev ${next.revision})`, "Success", "Linked flows adopt at next deploy");
    }
    return clone(next);
  });
}

export async function testService(id: string): Promise<AppService> {
  await sleep(900);
  return mutate((state) => {
    const svc = state.services.find((x) => x.id === id);
    if (!svc) throw new Error("Service not found");
    const fails = svc.retired || svc.name.toLowerCase().includes("legacy");
    svc.health = fails ? "Failed" : "Healthy";
    svc.lastTestedAt = nowIso();
    audit(state, fails ? "Service test failed" : "Service tested", "Application Service", svc.name, fails ? "Failed" : "Success");
    return clone(svc);
  });
}

export async function retireService(id: string): Promise<void> {
  await sleep(300);
  mutate((state) => {
    const svc = state.services.find((x) => x.id === id);
    if (!svc) return;
    svc.retired = true;
    svc.updatedAt = nowIso();
    const deps = serviceDependents(id, state);
    audit(state, "Service retired", "Application Service", svc.name, "Warning", deps.length > 0 ? `${deps.length} dependent flow(s) flagged: action required` : undefined);
  });
}

export async function reinstateService(id: string): Promise<void> {
  await sleep(300);
  mutate((state) => {
    const svc = state.services.find((x) => x.id === id);
    if (!svc) return;
    svc.retired = false;
    svc.updatedAt = nowIso();
    audit(state, "Service reinstated", "Application Service", svc.name);
  });
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
// The read-only ops view of a deployed flow. The alpha shipped a user-facing
// controller-services manager and live editing of deployed processors; the
// spec removed both, because editing the runtime out of band is precisely what
// produces the drift this model detects. So this layer READS: a refresh
// touches component/task states, live property values and the read timestamp —
// it never rewrites a flow definition and never clears a drift finding.
// Repair is a separate, explicit, confirmed, audited force action.

const ICEBERG_SINK_CLASS = "org.apache.iceberg.connect.IcebergSinkConnector";
const OPENSEARCH_SINK_CLASS = "io.aiven.kafka.connect.opensearch.OpensearchSinkConnector";

let nifiIdSeq = 0;
/** NiFi-shaped component id. Stable within a session, unique across calls. */
function nifiId(): string {
  nifiIdSeq += 1;
  const tail = (Date.now() % 0xffffff).toString(16).padStart(6, "0");
  return `0193a41c-7f10-1000-9f${(nifiIdSeq % 256).toString(16).padStart(2, "0")}-${tail}${nifiIdSeq.toString(16).padStart(6, "0")}`;
}

function activeNifi(state: PrototypeState): PlatformConnection | null {
  return state.connections.find((c) => c.type === "nifi" && c.active) ?? null;
}

/** Root-group id of a NiFi instance — seeded for the demo ones, derived otherwise. */
function fingerprintOf(conn: PlatformConnection): string {
  const known = NIFI_INSTANCE_FINGERPRINTS[conn.id];
  if (known) return known;
  let h = 0;
  for (const ch of conn.id) h = (h * 31 + ch.charCodeAt(0)) >>> 0;
  const hex = h.toString(16).padStart(8, "0");
  return `0193a41c-7f10-1000-b0${hex.slice(0, 2)}-${hex}${hex.slice(0, 4)}`;
}

/** Live component state implied by the flow's lifecycle state. */
function componentStateFor(flow: Flow): NifiComponentState {
  if (flow.state === "Running" || flow.state === "Degraded") return "RUNNING";
  if (!flow.enabled) return "DISABLED";
  return "STOPPED"; // Paused holds processing: the held processors are stopped
}

function connectStateFor(flow: Flow): ConnectConnectorRuntime["state"] {
  if (flow.state === "Running" || flow.state === "Degraded") return "RUNNING";
  return "PAUSED";
}

const humanize = (key: string) =>
  key.replace(/([A-Z])/g, " $1").replace(/[_.]/g, " ").replace(/^./, (c) => c.toUpperCase()).trim();

/** Non-secret service config renders as live descriptor rows; the secret is masked. */
function serviceProperties(svc: AppService): RuntimeProperty[] {
  const rows: RuntimeProperty[] = Object.entries(svc.config)
    .filter(([, v]) => typeof v === "string" || typeof v === "number" || typeof v === "boolean")
    .map(([k, v]) => ({ name: humanize(k), value: String(v) }));
  if (svc.hasSecret) {
    rows.push({ name: svc.type === "database" ? "Password" : "Client Secret", value: null, sensitive: true });
  }
  return rows;
}

function controllerServiceType(svc: AppService): string {
  if (svc.type === "database") return "DBCPConnectionPool";
  if (svc.type === "external_kafka") return "StandardRestrictedSSLContextService";
  const auth = String(svc.config.authMode ?? "");
  if (auth === "oauth2") return "StandardOauth2AccessTokenProvider";
  if (auth === "session_token") return "DmpSessionTokenProvider";
  return "StandardRestrictedSSLContextService";
}

const KAFKA_CLIENT_PROPS = (): RuntimeProperty[] => [
  { name: "Kafka Brokers", value: "kafka-1.internal.corp:9094,kafka-2.internal.corp:9094" },
  { name: "Security Protocol", value: "SASL_SSL" },
  { name: "SSL Context Service", value: "Platform · Kafka SSL Context" },
  { name: "sasl.jaas.config", value: null, sensitive: true },
];

/** The compiler's runtime-scope map: which generated components a block owns. */
function componentsForBlock(flow: Flow, block: FlowBlock, state: PrototypeState): NifiComponent[] {
  const compState = componentStateFor(flow);
  const out: NifiComponent[] = [];
  const add = (name: string, type: string, properties: RuntimeProperty[]) =>
    out.push({ id: nifiId(), name: `${block.name} · ${name}`, type, blockId: block.id, state: compState, properties });
  const svc = state.services.find((s) => s.id === block.serviceId);

  switch (block.adapter) {
    case "http": {
      const base = String(svc?.config.baseUrl ?? "");
      add("InvokeHTTP", "org.apache.nifi.processors.standard.InvokeHTTP", [
        { name: "HTTP Method", value: String(block.config.method ?? "GET") },
        { name: "Remote URL", value: `${base}${String(block.config.path ?? "")}` },
        { name: "Connection Timeout", value: "5 secs" },
        { name: "Read Timeout", value: "30 secs" },
        {
          name: "Proxy Configuration Service",
          // Egress comes from the service the block is bound to.
          value: (() => {
            const id = blockProxyId(block, state.services);
            return id
              ? `${state.gatewayProxies.find((pr) => pr.id === id)?.name ?? "proxy"} · APISIX Proxy`
              : "No value set";
          })(),
        },
        ...(svc?.hasSecret ? [{ name: "Request Header Authorization", value: null, sensitive: true } as RuntimeProperty] : []),
      ]);
      if (block.config.split) {
        add("SplitJson", "org.apache.nifi.processors.standard.SplitJson", [
          { name: "JsonPath Expression", value: String(block.config.recordPath ?? "$") },
          { name: "Null Value Representation", value: "empty string" },
        ]);
      }
      break;
    }
    case "jdbc": {
      if (block.mode === "write") {
        add("PutDatabaseRecord", "org.apache.nifi.processors.standard.PutDatabaseRecord", [
          { name: "Database Connection Pooling Service", value: `${svc?.name ?? "database"} · Connection Pool` },
          { name: "Table Name", value: String(block.config.table ?? "") },
          { name: "Statement Type", value: "INSERT" },
        ]);
      } else if (block.mode === "lookup") {
        add("LookupRecord", "org.apache.nifi.processors.standard.LookupRecord", [
          { name: "Lookup Service", value: `${svc?.name ?? "database"} · Record Lookup` },
          { name: "Result RecordPath", value: "/" },
        ]);
      } else {
        add("QueryDatabaseTableRecord", "org.apache.nifi.processors.standard.QueryDatabaseTableRecord", [
          { name: "Database Connection Pooling Service", value: `${svc?.name ?? "database"} · Connection Pool` },
          { name: "Table Name", value: String(block.config.table ?? "") },
          { name: "Maximum-value Columns", value: String(block.config.watermarkColumn ?? "") },
          { name: "Fetch Size", value: "1000" },
          { name: "Record Writer", value: "Platform · Avro Record Writer" },
        ]);
      }
      break;
    }
    case "kafka": {
      if (block.mode === "read") {
        add("ConsumeKafkaRecord", "org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_2_6", [
          { name: "Topic Name(s)", value: String(block.config.topicName ?? flow.topics.find((t) => t.id === block.parentId)?.name ?? "") },
          { name: "Group ID", value: `dmp-${flow.id}` },
          { name: "Offset Reset", value: block.config.initialPosition === "latest" ? "latest" : "earliest" },
          ...KAFKA_CLIENT_PROPS(),
        ]);
      } else {
        add("PublishKafkaRecord", "org.apache.nifi.processors.kafka.pubsub.PublishKafkaRecord_2_6", [
          { name: "Topic Name", value: deriveTopicName(flow, block).value },
          { name: "Record Reader", value: "Platform · JSON Tree Reader" },
          { name: "Record Writer", value: "Platform · Avro Record Writer" },
          { name: "Delivery Guarantee", value: "Guarantee Replicated Delivery" },
          { name: "Compression Type", value: "snappy" },
          ...KAFKA_CLIENT_PROPS(),
        ]);
      }
      break;
    }
    case "kafka_kc": {
      add("PublishKafkaRecord", "org.apache.nifi.processors.kafka.pubsub.PublishKafkaRecord_2_6", [
        { name: "Topic Name", value: deriveTopicName(flow, block).value },
        { name: "Record Reader", value: "Platform · JSON Tree Reader" },
        { name: "Record Writer", value: "Platform · Avro Record Writer" },
        { name: "Delivery Guarantee", value: "Guarantee Replicated Delivery" },
        ...KAFKA_CLIENT_PROPS(),
      ]);
      break;
    }
    case "kc":
      // kc subscriptions live entirely on Connect — they generate no NiFi components.
      break;
  }

  // Conditional branches compile to ONE RouteOnAttribute with a property per
  // branch. NiFi's "Route to Property name" strategy sends a FlowFile to every
  // matching relationship, which is exactly the independent evaluation the UI
  // promises — a record satisfying two conditions really does take both.
  // Unconditional branches are plain connections and generate nothing.
  const conditionalBranches = flow.blocks.filter(
    (b) => b.parentId === block.id && (b.branch?.rules?.length ?? 0) > 0,
  );
  if (conditionalBranches.length > 0) {
    add("RouteOnAttribute", "org.apache.nifi.processors.standard.RouteOnAttribute", [
      { name: "Routing Strategy", value: "Route to Property name" },
      ...conditionalBranches.map((b) => {
        const rules = b.branch!.rules ?? [];
        const parts = rules.map((c) =>
          c.op === "is_empty"
            ? `\${${c.field}:isEmpty()}`
            : `\${${c.field}:${c.op === "not_equals" ? "equals" : c.op}('${c.value}')${c.op === "not_equals" ? ":not()" : ""}}`,
        );
        // NiFi EL has no n-ary and/or, so several rules chain through :and()/:or()
        // exactly as the branch's match mode reads.
        const joiner = b.branch!.match === "any" ? ":or" : ":and";
        const value = parts.length === 1 ? parts[0] : parts.reduce((acc, part) => `${acc}${joiner}(${part})`);
        return { name: b.branch!.name, value };
      }),
    ]);
  }

  for (const t of block.transforms) {
    switch (t.kind) {
      case "extract":
        add("EvaluateJsonPath", "org.apache.nifi.processors.standard.EvaluateJsonPath", [
          { name: "Destination", value: "flowfile-attribute" },
          { name: String(t.config.attribute ?? "attribute"), value: String(t.config.path ?? "") },
        ]);
        break;
      case "dedup":
        add("DetectDuplicate", "org.apache.nifi.processors.standard.DetectDuplicate", [
          { name: "Distributed Cache Service", value: "Platform · Redis Connection Pool" },
          { name: "Cache Entry Identifier", value: "${dmp.dedup.fingerprint}" },
          { name: "Age Off Duration", value: `${String(t.config.windowHours ?? 24)} hours` },
        ]);
        break;
      case "remove_field":
        add("JoltTransformJSON", "org.apache.nifi.processors.standard.JoltTransformJSON", [
          { name: "Jolt Transformation DSL", value: "Remove" },
          { name: "Jolt Specification", value: `{ "${String(t.config.field ?? "")}": "" }` },
        ]);
        break;
      default:
        add("UpdateRecord", "org.apache.nifi.processors.standard.UpdateRecord", [
          { name: "Replacement Value Strategy", value: "Literal Value" },
          { name: "Record Reader", value: "Platform · JSON Tree Reader" },
        ]);
    }
  }
  return out;
}

function connectorsForFlow(flow: Flow, state: PrototypeState): ConnectConnectorRuntime[] {
  const runState = connectStateFor(flow);
  return flow.blocks
    .filter((b) => b.adapter === "kafka_kc" || b.adapter === "kc")
    .map((b) => {
      const sinkCfg = (b.config.sinkConfig as Record<string, string> | undefined) ?? {};
      const svc = state.services.find((s) => s.id === ((b.config.sinkServiceId as string) ?? b.serviceId));
      const cls =
        sinkCfg["connector.class"] ??
        (svc?.config.kind === "iceberg_catalog" ? ICEBERG_SINK_CLASS : OPENSEARCH_SINK_CLASS);
      const topic =
        b.adapter === "kafka_kc"
          ? deriveTopicName(flow, b).value
          : flow.topics.find((t) => t.id === (b.config.attachTopicId as string))?.name ?? "";
      const suffix = cls.includes("iceberg") ? "iceberg" : cls.includes("opensearch") ? "opensearch" : "sink";
      const taskCount = Math.max(1, Number(sinkCfg["tasks.max"] ?? 1));
      return {
        name: `dmp.${topic}.${suffix}`,
        blockId: b.id,
        connectorClass: cls,
        state: runState,
        workerId: "connect-1.internal.corp:8083",
        recordsSent: 0,
        recordsFailed: 0,
        tasks: Array.from({ length: taskCount }, (_, i) => ({
          id: i,
          state: runState,
          workerId: i % 2 === 0 ? "connect-1.internal.corp:8083" : "connect-2.internal.corp:8083",
        })),
      };
    });
}

function controllerServicesForFlow(flow: Flow, state: PrototypeState): ControllerServiceRuntime[] {
  const boundIds = Array.from(
    new Set(flow.blocks.map((b) => b.serviceId).filter((id): id is string => !!id)),
  );
  const perService = boundIds
    .map((id) => state.services.find((s) => s.id === id))
    .filter((s): s is AppService => !!s && s.type !== "sink_destination")
    .map<ControllerServiceRuntime>((svc) => ({
      id: `cs-${flow.id}-${svc.id}`,
      name: `${svc.name} · ${svc.type === "database" ? "Connection Pool" : "SSL Context"}`,
      type: controllerServiceType(svc),
      state: "ENABLED",
      appServiceId: svc.id,
      pinnedRevision: flow.servicePins[svc.id] ?? svc.revision,
      scope: "flow",
      properties: serviceProperties(svc),
    }));

  const usesRedis = flow.blocks.some(
    (b) => b.transforms.some((t) => t.kind === "dedup") || (b.adapter === "jdbc" && !!b.config.incremental),
  );
  const others = state.flows.filter((f) => f.id !== flow.id && f.deployedAt).map((f) => f.name);
  return [
    ...perService,
    ...(usesRedis ? [platformRedisService(others)] : []),
    ...platformControllerServices(others),
  ];
}

/** Compile a fresh runtime record for a flow that just deployed. */
function synthesizeRuntime(flow: Flow, state: PrototypeState, previous?: FlowRuntime): FlowRuntime {
  const conn = activeNifi(state);
  const fingerprint = conn ? fingerprintOf(conn) : "unknown";
  return {
    flowId: flow.id,
    nifiConnectionId: conn?.id ?? "conn-nifi-prod",
    processGroupId: nifiId(),
    deployedFingerprint: fingerprint,
    observedFingerprint: fingerprint,
    reachable: true,
    lastReadAt: nowIso(),
    components: flow.blocks.flatMap((b) => componentsForBlock(flow, b, state)),
    controllerServices: controllerServicesForFlow(flow, state),
    connectors: connectorsForFlow(flow, state),
    // A redeploy compiles the block config back onto the runtime, so property
    // drift genuinely goes away. Orphans are a ledger — they never do.
    drift: [],
    orphans: previous?.orphans ?? [],
  };
}

export async function getFlowRuntime(flowId: string): Promise<FlowRuntime | null> {
  await sleep(180);
  const rt = getState().runtimes.find((r) => r.flowId === flowId);
  return rt ? clone(rt) : null;
}

/**
 * Simulated live read of NiFi + Connect — the "load it live" the ops view is
 * for. It refreshes states, live property values and the read timestamp. It
 * does NOT touch the flow definition, and it never clears drift: a stale
 * runtime stays surfaced until the user explicitly repairs it.
 */
export async function refreshFlowRuntime(flowId: string): Promise<FlowRuntime> {
  await sleep(950);
  return mutate((state) => {
    const flow = state.flows.find((f) => f.id === flowId);
    if (!flow) throw new Error("Flow not found");
    const rt = state.runtimes.find((r) => r.flowId === flowId);
    if (!rt) throw new Error("The flow has no runtime — deploy it before reading NiFi.");
    if (!rt.processGroupId) {
      throw new Error("The runtime reference was cleared by a force repair — deploy the flow to compile a new one.");
    }

    const conn = activeNifi(state);
    rt.lastReadAt = nowIso();

    // Unreachable is its own answer: unknown, not "deleted".
    if (!conn || conn.health !== "Healthy" || conn.reachability === "Unreachable") {
      rt.reachable = false;
      rt.observedFingerprint = null;
      rt.unreachableReason = conn
        ? `${conn.name} is ${conn.health === "Failed" ? "failing its health check" : "not reachable"} — component states below are the last known values, not live ones.`
        : "No active NiFi connection — nothing could be read.";
      audit(state, "Runtime read failed", "Flow", flow.name, "Failed", rt.unreachableReason);
      return clone(rt);
    }

    rt.reachable = true;
    delete rt.unreachableReason;
    rt.observedFingerprint = fingerprintOf(conn);

    // Same instance, different instance, or unreachable — the fingerprint is
    // what tells them apart, and a mismatch is reported, never healed.
    if (rt.observedFingerprint !== rt.deployedFingerprint && !rt.drift.some((d) => d.kind === "process_group_missing")) {
      rt.drift.unshift({
        id: uid("drift"),
        kind: "process_group_missing",
        summary: `Process group ${rt.processGroupId ?? "—"} is not on ${conn.name}`,
        where: `${flow.name} (process group)`,
        expected: `root group ${rt.deployedFingerprint}`,
        observed: `root group ${rt.observedFingerprint}`,
        verdict: "deployed_elsewhere",
        verdictDetail: `The active NiFi reports a different root group than the one recorded at deploy, so this is a different instance — the runtime is still standing on the old one. Nothing was moved or re-created by this read.`,
        observedAt: nowIso(),
        repairable: true,
      });
    }

    const compState = componentStateFor(flow);
    for (const c of rt.components) {
      // A live read reports what NiFi says. Diverged property values stay
      // diverged: only a redeploy compiles them back.
      c.state = c.state === "INVALID" ? "INVALID" : compState;
    }
    const connectState = connectStateFor(flow);
    for (const connector of rt.connectors) {
      const failedTasks = connector.tasks.filter((t) => t.state === "FAILED");
      connector.state = failedTasks.length === connector.tasks.length ? "FAILED" : connectState;
      for (const task of connector.tasks) {
        // A read never restarts a failed task — that is a Connect action.
        if (task.state !== "FAILED") task.state = connectState;
      }
      if (connector.state === "RUNNING") {
        const healthy = connector.tasks.length - failedTasks.length;
        connector.recordsSent += healthy * 137;
        if (failedTasks.length > 0) connector.recordsFailed += failedTasks.length * 11;
      }
    }
    audit(
      state,
      "Runtime read",
      "Flow",
      flow.name,
      "Success",
      `Read-only: ${rt.components.length} component(s), ${rt.controllerServices.length} controller service(s), ${rt.connectors.length} connector(s) · drift findings are never cleared by a read`,
    );
    return clone(rt);
  });
}

export interface ForceRepairResult {
  runtime: FlowRuntime;
  orphans: RuntimeOrphan[];
  clearedFindings: number;
}

/**
 * The explicit force path. Clears the dead runtime reference and records what
 * is left behind as orphans — nothing is ever deleted on the runtime. Only
 * reachable as a confirmed user action: never a side effect of opening a tab.
 */
export async function forceRepairRuntime(flowId: string): Promise<ForceRepairResult> {
  await sleep(1200);
  return mutate((state) => {
    const flow = state.flows.find((f) => f.id === flowId);
    if (!flow) throw new Error("Flow not found");
    const rt = state.runtimes.find((r) => r.flowId === flowId);
    if (!rt) throw new Error("The flow has no runtime record.");
    const repairable = rt.drift.filter((d) => d.repairable);
    if (repairable.length === 0) {
      throw new Error("Nothing to repair — no dead runtime reference on this flow. Out-of-band edits are fixed by Redeploy.");
    }
    if (!rt.reachable) {
      throw new Error(
        "The runtime is unreachable, so the platform cannot tell 'deleted' from 'unavailable'. Force repair is refused until a read succeeds.",
      );
    }

    const instance = state.connections.find((c) => c.id === rt.nifiConnectionId)?.name ?? "the recorded NiFi instance";
    const verdict = repairable[0].verdict;
    const recordedAt = nowIso();
    const orphans: RuntimeOrphan[] = [];

    // "Deployed elsewhere" means the process group is still standing on the
    // other instance; "really deleted" means it is gone and only what lives
    // outside the group (Connect connectors, parent-scoped services) survives.
    if (verdict === "deployed_elsewhere" && rt.processGroupId) {
      orphans.push({ id: uid("orphan"), kind: "process_group", ref: rt.processGroupId, instance, recordedAt });
    }
    for (const connector of rt.connectors) {
      orphans.push({ id: uid("orphan"), kind: "connector", ref: connector.name, instance: connector.workerId, recordedAt });
    }
    for (const cs of rt.controllerServices.filter((c) => c.scope === "flow")) {
      orphans.push({ id: uid("orphan"), kind: "controller_service", ref: `${cs.name} (${cs.id})`, instance, recordedAt });
    }

    rt.orphans = [...orphans, ...rt.orphans];
    rt.processGroupId = null;
    rt.components = [];
    rt.connectors = [];
    rt.controllerServices = rt.controllerServices.filter((c) => c.scope === "shared");
    rt.drift = rt.drift.filter((d) => !d.repairable);
    rt.lastReadAt = recordedAt;

    // The platform no longer claims a runtime for this flow.
    flow.deployedAt = null;
    flow.state = "Draft";
    flow.drift = rt.drift.length > 0 ? rt.drift.map((d) => d.summary).join(" · ") : null;
    flow.updatedAt = recordedAt;

    audit(
      state,
      "Runtime reference force-cleared",
      "Flow",
      flow.name,
      "Warning",
      `${repairable.length} drift finding(s) resolved as "${verdict.replace(/_/g, " ")}" · ${orphans.length} orphan(s) recorded on ${instance} · nothing was deleted on the runtime · the flow is back to Draft`,
    );
    return { runtime: clone(rt), orphans: clone(orphans), clearedFindings: repairable.length };
  });
}

// ------------------------------------------------------- everything else

export async function listAudit(search?: string): Promise<AuditEvent[]> {
  await sleep(120);
  const events = getState().audit;
  if (!search?.trim()) return clone(events);
  const q = search.toLowerCase();
  return clone(
    events.filter((e) => [e.action, e.object, e.target, e.user, e.details ?? ""].some((v) => v.toLowerCase().includes(q))),
  );
}

export async function getDlq(flowId: string): Promise<DlqRecord[]> {
  await sleep(150);
  return clone(getState().dlq.filter((d) => d.flowId === flowId));
}

export async function getMetrics(flowId: string): Promise<FlowMetrics | null> {
  await sleep(150);
  const m = getState().metrics.find((x) => x.flowId === flowId);
  return m ? clone(m) : null;
}

export async function getTopicMessages(topic: string): Promise<TopicMessage[]> {
  await sleep(200);
  return clone(getState().topicMessages[topic] ?? []);
}

export async function listConnectors(): Promise<ConnectorExport[]> {
  await sleep(80);
  return clone(getState().connectors);
}

export async function publishConnector(flowId: string, name: string, description?: string): Promise<ConnectorExport> {
  await sleep(700);
  return mutate((state) => {
    const flow = state.flows.find((f) => f.id === flowId);
    if (!flow) throw new Error("Flow not found");
    const existing = state.connectors.filter((c) => c.name === name);
    const version = existing.length > 0 ? Math.max(...existing.map((c) => c.version)) + 1 : 1;
    const connector: ConnectorExport = { id: uid("connector"), name, version, flowId, description, createdAt: nowIso() };
    state.connectors.push(connector);
    audit(state, "Connector published", "Connector", `${name}@${version}`, "Success", "No secrets, no environment details — immutable once published");
    return clone(connector);
  });
}

/**
 * Finalize the (canned) connector import: creates a Draft flow with the
 * bundle's chain bound to the chosen services. Frontend-only, like everything
 * else here.
 */
export async function importConnectorFlow(input: {
  flowName: string;
  httpServiceId: string;
  sinkServiceId: string;
}): Promise<Flow> {
  await sleep(900);
  return mutate((state) => {
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
      createdAt: nowIso(),
      updatedAt: nowIso(),
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
    state.flows.unshift(flow);
    audit(state, "Connector imported", "Connector", "fortisiem-to-opensearch@1", "Success", `Created draft flow "${flow.name}" · services bound · credentials re-entered`);
    return clone(flow);
  });
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
  /** Sink connectors on the Connect cluster whose state is RUNNING. */
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
  await sleep(120);
  const s = getState();
  const active = s.connections.filter((c) => c.active);
  const connectors = s.runtimes.flatMap((r) => r.connectors);
  const deployedFlowIds = new Set(s.runtimes.map((r) => r.flowId));
  const undeployedSinks = s.flows
    .filter((f) => !deployedFlowIds.has(f.id))
    .reduce((n, f) => n + f.blocks.filter((b) => b.adapter === "kc" || b.adapter === "kafka_kc").length, 0);
  return {
    totalFlows: s.flows.length,
    runningFlows: s.flows.filter((f) => f.state === "Running" || f.state === "Degraded").length,
    approvedSchemas: s.schemas.length,
    connectionsHealthy: active.filter((c) => c.health === "Healthy").length,
    connectionsTotal: active.length,
    sinkConnectorsRunning: connectors.filter((c) => c.state === "RUNNING").length,
    sinkConnectorsTotal: connectors.length,
    sinkConnectorsUndeployed: undeployedSinks,
  };
}

export { resetDemoData };
export type { PreflightCheck, ValidationIssue };
