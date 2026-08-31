// Pure validation: block-level and flow-level issues. These drive the error
// badges in the outline/visual, the Validate panel, and the deploy preflight.

import { isValidCron, deriveTopicName, overrideMatchesDerived, topicNameCollision } from "./naming";
import { flowHasTrigger, hostsTransforms, isRawBranch, rootBlock } from "./legality";
import { branchIncomplete } from "./branches";
import { getState } from "./store";
import type { ApprovedSchema, AppService, Flow, FlowBlock, GatewayProxy } from "./types";

export interface ValidationIssue {
  blockId: string | null; // null = flow-level
  where: string; // "Flow settings" or block name
  message: string;
}

/**
 * The gateway facts the http checks need. Passed in so validation stays pure;
 * `readGatewaySnapshot()` is the convenience reader for callers that have no
 * state handle of their own.
 */
export interface GatewaySnapshot {
  proxies: GatewayProxy[];
  allowlist: string[];
}

export function readGatewaySnapshot(): GatewaySnapshot {
  // The store needs localStorage; guard for non-browser contexts.
  try {
    const s = getState();
    return { proxies: s.gatewayProxies ?? [], allowlist: s.gateway?.allowlist ?? [] };
  } catch {
    return { proxies: [], allowlist: [] };
  }
}

/**
 * The proxy an http block routes through, or null.
 *
 * A proxy is how a HOST is reached, and the host belongs to the service — so
 * the reference lives on the service and every block using it inherits the same
 * egress. It used to sit on the block, which meant two blocks calling the same
 * API could disagree about how to get there and nothing could say which was
 * right. A block-level id is still honoured as a fallback so datasets saved
 * before the move keep working; nothing writes one any more.
 */
export function blockProxyId(block: FlowBlock, services: AppService[] = []): string | null {
  if (block.adapter !== "http") return null;
  const service = services.find((s) => s.id === block.serviceId);
  const fromService = service?.config?.proxyId;
  if (typeof fromService === "string" && fromService.trim()) return fromService;
  const legacy = block.config?.proxyId;
  return typeof legacy === "string" && legacy.trim() ? legacy : null;
}

/** Every reason the referenced proxy is not deployable, in user-facing words. */
export function gatewayRefusals(block: FlowBlock, gateway: GatewaySnapshot, services: AppService[] = []): string[] {
  const proxyId = blockProxyId(block, services);
  if (!proxyId) return [];
  const proxy = gateway.proxies.find((p) => p.id === proxyId);
  if (!proxy)
    return [
      `The APISIX proxy this block routes through (${proxyId}) no longer exists — pick one on the Proxies page.`,
    ];
  const refusals: string[] = [];
  if (proxy.status !== "Reconciled")
    refusals.push(
      `APISIX proxy "${proxy.name}" is ${proxy.status} — it must reconcile onto the gateway before this flow can deploy.${proxy.statusDetail ? ` ${proxy.statusDetail}` : ""}`,
    );
  if (!gateway.allowlist.includes(proxy.targetHost))
    refusals.push(
      `Host "${proxy.targetHost}" (proxy "${proxy.name}") is not on the gateway allowlist — egress hosts are admin-allowlisted, so an administrator must add it first.`,
    );
  return refusals;
}

// ------------------------------------------------------------------ dedup
// TTL bounds per the MVP dedup rules: minimum 1 minute, maximum 365 days,
// default 24 hours. windowHours is the canonical unit — fractional so a
// 1-minute window is representable (1/60).
export const DEDUP_WINDOW_MIN_HOURS = 1 / 60;
export const DEDUP_WINDOW_MAX_HOURS = 8760; // 365 days
export const DEDUP_WINDOW_DEFAULT_HOURS = 24;

/** null = fine, string = the message to show (badge + inline). */
export function dedupIdentityFieldsIssue(identityFields: unknown): string | null {
  const fields = Array.isArray(identityFields) ? identityFields.filter((f) => typeof f === "string" && f.trim()) : [];
  return fields.length === 0 ? "At least one identity field is required" : null;
}

/** null = fine, string = the message to show (badge + inline). */
export function dedupWindowIssue(windowHours: unknown): string | null {
  if (typeof windowHours !== "number" || Number.isNaN(windowHours)) return "Window must be between 1 minute and 365 days";
  return windowHours < DEDUP_WINDOW_MIN_HOURS || windowHours > DEDUP_WINDOW_MAX_HOURS
    ? "Window must be between 1 minute and 365 days"
    : null;
}

/**
 * null = fine, string = the message to show (badge + inline). The service
 * supplies the base URL for every http block — a path that starts with a
 * scheme (http:// or https://) means the base URL got typed or pasted into
 * Path, which compiles into a broken concatenated NiFi URL (baseUrl + full
 * URL) at deploy time. Caught here so the builder badge and the backend save
 * both surface it, not just the inline hint in HttpSettings.
 */
export function httpPathIssue(path: string): string | null {
  return /^https?:\/\//i.test(path)
    ? "HTTP path must be a path (the service provides the base URL) — got a full URL."
    : null;
}

function isWrite(block: FlowBlock): boolean {
  return block.mode === "write" || block.adapter === "kafka_kc";
}

function isKafkaFamilyWrite(block: FlowBlock): boolean {
  return (block.adapter === "kafka" && block.mode === "write") || block.adapter === "kafka_kc";
}

function unresolvedPlaceholders(flow: Flow, block: FlowBlock): string[] {
  const text = JSON.stringify(block.config ?? {});
  const found = [...text.matchAll(/\$\{([a-zA-Z0-9_.-]+)\}/g)].map((m) => m[1]);
  if (found.length === 0) return [];
  // Resolved by: this flow's variables, or extraction attributes anywhere
  // upstream. Global variables are gone — per-flow is the only scope.
  const flowVars = new Set(flow.variables.map((v) => v.name));
  const byId = new Map(flow.blocks.map((b) => [b.id, b]));
  const upstreamAttrs = new Set<string>();
  let cur: FlowBlock | undefined = block;
  while (cur) {
    for (const t of cur.transforms) {
      if (t.kind === "extract" && typeof t.config.attribute === "string") upstreamAttrs.add(t.config.attribute as string);
    }
    cur = cur.parentId ? byId.get(cur.parentId) : undefined;
  }
  return [...new Set(found)].filter((name) => !flowVars.has(name) && !upstreamAttrs.has(name));
}

/**
 * Sink-configuration sanity for kc / kafka_kc. Deliberately narrow: an empty
 * sink config is a legitimate "not configured yet" state (the editor's empty
 * state), so only a config that *says* something wrong is an issue.
 */
function sinkConfigRefusals(block: FlowBlock): string[] {
  if (block.adapter !== "kc" && block.adapter !== "kafka_kc") return [];
  const sink = block.config?.sinkConfig;
  if (!sink || typeof sink !== "object") return [];
  const entries = sink as Record<string, unknown>;
  if (Object.keys(entries).length === 0) return [];
  const refusals: string[] = [];
  const connectorClass = entries["connector.class"];
  if (typeof connectorClass !== "string" || !connectorClass.trim()) {
    refusals.push("Set connector.class — the platform has to know which Connect plugin runs this sink.");
  } else if (!/^[\w$]+(\.[\w$]+)+$/.test(connectorClass.trim())) {
    // A class outside the shipped catalog is a CUSTOM sink, not an error: the
    // cluster can carry plugins this UI has never heard of, and refusing deploy
    // on that basis would make custom sinks unusable. Only a value that cannot
    // be a Java class name at all is refused; the editor flags the rest.
    refusals.push(
      `connector.class "${connectorClass}" is not a class name — a custom sink still needs a fully-qualified class, e.g. com.example.kafka.connect.MySinkConnector.`,
    );
  }
  // Platform-owned keys are rendered as disabled rows and computed at render;
  // a persisted copy goes stale the moment a name changes.
  const owned = ["topics", "key.converter", "value.converter"].filter((k) => k in entries);
  if (owned.length > 0)
    refusals.push(`The platform owns ${owned.join(", ")} — remove ${owned.length > 1 ? "them" : "it"}; the value is derived at deploy.`);
  return refusals;
}

const RETENTION_KINDS = new Set(["extract", "add_field", "set_from_attribute", "rename"]);

function retentionTarget(t: FlowBlock["transforms"][number], index: number): { plane: "attribute" | "record"; name: string } | null {
  const cfg = t.config ?? {};
  if (t.kind === "extract") return { plane: "attribute", name: String(cfg.attribute || `extract_${index}`).trim() };
  if (t.kind === "add_field" || t.kind === "set_from_attribute") {
    const name = String(cfg.field || "").trim();
    return name ? { plane: "record", name } : null;
  }
  if (t.kind === "rename") {
    const name = String(cfg.to || "").trim();
    return name ? { plane: "record", name } : null;
  }
  return null;
}

function containsPlaceholder(value: unknown, name: string): boolean {
  if (typeof value === "string") return value.includes(`\${${name}}`);
  if (Array.isArray(value)) return value.some((v) => containsPlaceholder(v, name));
  if (value && typeof value === "object")
    return Object.entries(value as Record<string, unknown>).some(([k, v]) => containsPlaceholder(k, name) || containsPlaceholder(v, name));
  return false;
}

function descendantBlocks(flow: Flow, block: FlowBlock): FlowBlock[] {
  const children = new Map<string | null, FlowBlock[]>();
  for (const candidate of flow.blocks) children.set(candidate.parentId ?? null, [...(children.get(candidate.parentId ?? null) ?? []), candidate]);
  const result: FlowBlock[] = [];
  const queue = [...(children.get(block.id) ?? [])];
  while (queue.length) {
    const current = queue.shift()!;
    result.push(current);
    queue.push(...(children.get(current.id) ?? []));
  }
  return result;
}

function referencesKey(block: FlowBlock, name: string, plane: "attribute" | "record"): boolean {
  if (containsPlaceholder(block.config, name)) return true;
  for (const condition of block.branch?.rules ?? []) if (condition.field === name) return true;
  for (const t of block.transforms) {
    const cfg = t.config ?? {};
    if ((t.kind === "add_field" || t.kind === "set_from_attribute") && containsPlaceholder(cfg.value, name)) return true;
    if (t.kind === "set_from_attribute" && plane === "attribute" && cfg.attribute === name) return true;
    if ((t.kind === "coerce" || t.kind === "remove_field") && plane === "record" && cfg.field === name) return true;
    if (t.kind === "rename" && plane === "record" && cfg.from === name) return true;
    if (t.kind === "dedup" && plane === "record" && ([...(Array.isArray(cfg.identityFields) ? cfg.identityFields : []), ...(Array.isArray(cfg.excludedFields) ? cfg.excludedFields : [])].includes(name))) return true;
  }
  return false;
}

const PAGINATION_TYPES = new Set(["none", "page", "cursor", "offset", "next_url"]);
const PAGINATION_STOPS = new Set(["empty_response", "total_count", "has_more"]);

function positiveWhole(value: unknown, minimum = 1): boolean {
  const text = String(value ?? "").trim();
  if (!text) return false;
  const parsed = Number(text);
  return Number.isInteger(parsed) && parsed >= minimum;
}

function paginationRefusals(block: FlowBlock): string[] {
  const pagination = (block.config.pagination as { type?: unknown; fields?: Record<string, unknown> } | undefined) ?? {};
  const type = String(pagination.type ?? "none").trim().toLowerCase();
  const fields = pagination.fields ?? {};
  if (!PAGINATION_TYPES.has(type)) return [`Unsupported pagination type: ${type || "(blank)"}.`];
  if (type === "none") return [];

  const issues: string[] = [];
  if (block.mode === "write" && (type === "cursor" || type === "next_url"))
    issues.push("HTTP write pagination supports page or offset counters, not cursor or next URL.");
  if (block.mode === "write" && String(block.config.writeForwards ?? "original") !== "response")
    issues.push('HTTP write pagination requires "Continue with" to be the response.');

  const maxPages = fields.maxPages;
  if (String(maxPages ?? "").trim() && !positiveWhole(maxPages))
    issues.push("Pagination maximum pages must be a positive whole number.");

  let stop = "empty_response";
  if (type === "page") {
    if (String(fields.sizeValue ?? "").trim() && !positiveWhole(fields.sizeValue))
      issues.push("Pagination page size must be a positive whole number.");
    if (String(fields.firstPage ?? "").trim() && !positiveWhole(fields.firstPage, 0))
      issues.push("Pagination first page must be a non-negative whole number.");
    stop = String(fields.stop ?? "empty_response");
  } else if (type === "offset") {
    if (String(fields.limitValue ?? "").trim() && !positiveWhole(fields.limitValue))
      issues.push("Pagination limit must be a positive whole number.");
    stop = String(fields.offsetStop ?? "empty_response");
  } else if (type === "cursor") {
    const cursorSize = fields.cursorSizeValue ?? fields.sizeValue;
    if (String(cursorSize ?? "").trim() && !positiveWhole(cursorSize))
      issues.push("Cursor page size must be a positive whole number.");
    if (!["body", "header"].includes(String(fields.cursorSource ?? "body")))
      issues.push("Cursor source must be the response body or a response header.");
    return issues;
  } else {
    const source = String(fields.nextUrlSource ?? (fields.urlPath ? "body" : "link_header"));
    if (!["body", "header", "link_header"].includes(source))
      issues.push("Next URL source must be the body, a response header, or the Link header.");
    return issues;
  }

  if (!PAGINATION_STOPS.has(stop)) issues.push(`Unsupported pagination stop condition: ${stop}.`);
  else if ((stop === "total_count" || stop === "has_more") && !positiveWhole(maxPages))
    issues.push("Total-count and has-more stopping require a positive maximum-pages safety limit.");
  return issues;
}

export function validateBlock(
  flow: Flow,
  block: FlowBlock,
  services: AppService[],
  schemas: ApprovedSchema[],
  gateway: GatewaySnapshot = readGatewaySnapshot(),
): ValidationIssue[] {
  const issues: ValidationIssue[] = [];
  const at = (message: string) => issues.push({ blockId: block.id, where: block.name, message });

  if (!block.name.trim()) at("Block needs a name.");

  const needsService = block.adapter === "http" || block.adapter === "jdbc" || block.adapter === "kafka_kc" || block.adapter === "kc";
  if (needsService && !block.serviceId) at("Select a service — hosts and credentials always come from a saved service.");
  if (block.serviceId) {
    const svc = services.find((s) => s.id === block.serviceId);
    if (!svc) at("The selected service no longer exists.");
    else if (svc.retired) at(`Service "${svc.name}" is retired — action required: select a replacement.`);
  }

  if (isWrite(block) && !block.entity?.trim()) at("No write without an entity, ever — set the entity label.");
  // kc is a write in spec terms but not in `isWrite()` terms: widening that
  // predicate would leak kc into service-type mapping, transform hosting and
  // legality. The entity requirement is therefore its own targeted check.
  if (block.adapter === "kc" && !block.entity?.trim())
    at("No write without an entity, ever — this subscription delivers records, so it needs an entity label.");

  if (block.adapter === "http") {
    const path = (block.config.path as string) ?? "";
    if (!path) at("Set the request path.");
    else {
      const pathIssue = httpPathIssue(path);
      if (pathIssue) at(pathIssue);
    }
    const missing = unresolvedPlaceholders(flow, block);
    if (missing.length > 0)
      at(`Unresolved \${...} values: ${missing.join(", ")} — extract them upstream or define a flow variable.`);
    for (const refusal of gatewayRefusals(block, gateway, services)) at(refusal);
    for (const refusal of paginationRefusals(block)) at(refusal);
  }
  if (block.adapter === "jdbc" && !(block.config.table as string)) at("Pick a table.");
  if (block.adapter === "kafka" && block.mode === "read" && !block.parentId && !(block.config.topicName as string))
    at("Pick a topic to consume.");
  // The override is legal on the whole kafka family (R7), so the collision
  // check has to cover kafka_kc too — its derived name is overridable now.
  if (isKafkaFamilyWrite(block) && block.topicOverride && !overrideMatchesDerived(flow, block)) {
    const collision = topicNameCollision(deriveTopicName(flow, block).value);
    if (collision) at(collision);
  }
  if (block.adapter === "kafka_kc") {
    if (!schemas.some((s) => s.flowId === flow.id && s.blockId === block.id))
      at("Schema ceremony required — the flow cannot deploy until this write's schema is approved.");
    if (!block.config.sinkServiceId && !block.serviceId) at("Select the sink destination service.");
  }
  if (block.adapter === "kc" && !(block.config.attachTopicId as string)) at("Attach the subscription to a topic.");
  for (const refusal of sinkConfigRefusals(block)) at(refusal);

  // A half-written rule matches nothing, so the branch silently receives no
  // records. NO rules is legal and means "everything" — only an unfinished rule
  // is an issue.
  if (branchIncomplete(block.branch))
    at(
      `Branch "${block.branch?.name ?? block.name}" has an unfinished rule — it matches no records until every rule has a field, an operator and a value.`,
    );

  // Transforms sanity
  const dedupIndex = block.transforms.findIndex((t) => t.kind === "dedup");
  if (dedupIndex >= 0 && dedupIndex !== block.transforms.length - 1) at("Dedup must be the last transformation.");
  if (!hostsTransforms(flow, block) && block.transforms.length > 0 && block.adapter !== "kc")
    at("R8 — this branch carries raw bytes; transformations are not available here.");
  for (const t of block.transforms) {
    if (t.kind !== "dedup") continue;
    const identityIssue = dedupIdentityFieldsIssue(t.config.identityFields);
    if (identityIssue) at(identityIssue);
    const windowIssue = dedupWindowIssue(t.config.windowHours);
    if (windowIssue) at(windowIssue);
  }

  const temporaryTargets: string[] = [];
  block.transforms.forEach((t, index) => {
    const retention = String(t.config.retention ?? "flow").trim().toLowerCase();
    if (retention !== "flow" && retention !== "block") at(`Transform ${index + 1} has an invalid retention value; use 'flow' or 'block'.`);
    if (retention !== "block" || !RETENTION_KINDS.has(t.kind)) return;
    const target = retentionTarget(t, index);
    if (!target) {
      at(`Transform ${index + 1} cannot be temporary until its output name is set.`);
      return;
    }
    if (target.plane === "attribute" && target.name === "kafka.key" && block.adapter === "kafka" && block.mode === "write") {
      at("The temporary attribute 'kafka.key' is consumed by this Kafka destination; keep it available through publish.");
      return;
    }
    const targetKey = `${target.plane}:${target.name}`;
    if (temporaryTargets.includes(targetKey)) at(`The temporary ${target.plane} '${target.name}' is produced by more than one transform; use one owner per key.`);
    temporaryTargets.push(targetKey);
    const users = descendantBlocks(flow, block).filter((child) => referencesKey(child, target.name, target.plane));
    if (users.length) at(`Temporary ${target.plane} '${target.name}' is referenced downstream by ${users.map((u) => u.name || u.id).join(", ")}; keep it downstream or recreate it before use.`);
  });

  return issues;
}

export function validateFlow(
  flow: Flow,
  services: AppService[],
  schemas: ApprovedSchema[],
  gateway: GatewaySnapshot = readGatewaySnapshot(),
): ValidationIssue[] {
  const issues: ValidationIssue[] = [];
  const flowLevel = (message: string) => issues.push({ blockId: null, where: "Flow settings", message });

  if (!flow.name.trim()) flowLevel("Name the flow — the name is the first half of every derived name.");
  if (flow.blocks.length === 0 && flow.topics.length === 0) flowLevel("The flow is empty — add a root block.");
  if (flowHasTrigger(flow)) {
    if (!flow.cron) flowLevel("Set the cron schedule on the root block.");
    else if (!isValidCron(flow.cron)) flowLevel("Cron must be a 5-field expression (UTC).");
  }
  const root = rootBlock(flow);
  if (!root && flow.blocks.some((b) => b.adapter !== "kc")) flowLevel("The flow has no legal root (R2).");

  const writes = flow.blocks.filter((b) => isWrite(b) || b.adapter === "kc");
  if (flow.blocks.length > 0 && writes.length === 0)
    flowLevel("Data goes nowhere — add at least one write or sink.");

  for (const block of flow.blocks) issues.push(...validateBlock(flow, block, services, schemas, gateway));
  return issues;
}

/** Issues that specifically block Deploy (beyond plain validation). */
export interface PreflightCheck {
  label: string;
  ok: boolean;
  detail: string;
}

export function deployPreflight(
  flow: Flow,
  services: AppService[],
  schemas: ApprovedSchema[],
  activeConnections: { type: string; name: string; health: string }[],
  gateway: GatewaySnapshot = readGatewaySnapshot(),
): PreflightCheck[] {
  const checks: PreflightCheck[] = [];
  const validation = validateFlow(flow, services, schemas, gateway);
  checks.push({
    label: "Configuration valid",
    ok: validation.length === 0,
    detail: validation.length === 0 ? "All blocks pass validation." : `${validation.length} issue(s) — run Validate for details.`,
  });

  const needed: [string, string][] = [
    ["nifi", "NiFi"],
    ["kafka", "Kafka"],
    ["apicurio", "Schema registry"],
  ];
  const usesConnect = flow.blocks.some((b) => b.adapter === "kafka_kc" || b.adapter === "kc");
  if (usesConnect) needed.push(["kafka_connect", "Kafka Connect"]);
  const usesDedup = flow.blocks.some((b) => b.transforms.some((t) => t.kind === "dedup"));
  const usesBookmarks = flow.blocks.some((b) => b.adapter === "jdbc" && b.config.incremental === true);
  if (usesDedup || usesBookmarks) needed.push(["redis", "Redis"]);
  const proxiedBlocks = flow.blocks.filter((b) => blockProxyId(b, services));
  if (proxiedBlocks.length > 0) needed.push(["apisix", "API gateway"]);

  for (const [type, label] of needed) {
    const conn = activeConnections.find((c) => c.type === type);
    checks.push({
      label: `${label} connection active`,
      ok: !!conn && conn.health === "Healthy",
      detail: conn ? `${conn.name} — ${conn.health}` : `No active ${label} connection.`,
    });
  }

  // One pair of rows per referenced proxy: reconciliation and allowlisting are
  // separate refusals with separate owners (self-serve vs admin).
  const referencedProxyIds = [...new Set(proxiedBlocks.map((b) => blockProxyId(b, services)!))];
  for (const proxyId of referencedProxyIds) {
    const proxy = gateway.proxies.find((p) => p.id === proxyId);
    const users = proxiedBlocks.filter((b) => blockProxyId(b, services) === proxyId).map((b) => b.name);
    if (!proxy) {
      checks.push({
        label: `Gateway proxy resolves — ${proxyId}`,
        ok: false,
        detail: `${users.join(", ")} route through a proxy that no longer exists. Pick one on the Proxies page.`,
      });
      continue;
    }
    checks.push({
      label: `Gateway proxy reconciled — ${proxy.name}`,
      ok: proxy.status === "Reconciled",
      detail:
        proxy.status === "Reconciled"
          ? `${proxy.targetHost}:${proxy.port}${proxy.path} — reconciled onto the gateway.`
          : `${proxy.status}${proxy.statusDetail ? ` — ${proxy.statusDetail}` : ""}`,
    });
    const allowlisted = gateway.allowlist.includes(proxy.targetHost);
    checks.push({
      label: `Gateway host allowlisted — ${proxy.targetHost}`,
      ok: allowlisted,
      detail: allowlisted
        ? "Host is on the admin allowlist."
        : "Egress hosts are admin-allowlisted; this one is not on the list yet.",
    });
  }

  const kafkaKcBlocks = flow.blocks.filter((b) => b.adapter === "kafka_kc");
  for (const b of kafkaKcBlocks) {
    const approved = schemas.some((s) => s.flowId === flow.id && s.blockId === b.id);
    checks.push({
      label: `Schema approved — ${b.name}`,
      ok: approved,
      detail: approved ? "Approved and registered." : "The schema ceremony has not been completed.",
    });
  }

  const usedServices = [...new Set(flow.blocks.map((b) => b.serviceId).filter((id): id is string => !!id))]
    .map((id) => services.find((s) => s.id === id))
    .filter((s): s is AppService => !!s);
  const failing = usedServices.filter((s) => s.health === "Failed");
  checks.push({
    label: "Bound services reachable",
    ok: failing.length === 0,
    detail:
      usedServices.length === 0
        ? "No services bound."
        : failing.length === 0
          ? `${usedServices.length} service(s) — none failing.`
          : `Failing: ${failing.map((s) => s.name).join(", ")}.`,
  });

  const pinnedRetired = Object.keys(flow.servicePins)
    .map((id) => services.find((s) => s.id === id))
    .filter((s): s is AppService => !!s && s.retired);
  checks.push({
    label: "No retired services",
    ok: pinnedRetired.length === 0,
    detail:
      pinnedRetired.length === 0
        ? "All bound services are live."
        : `Action required: ${pinnedRetired.map((s) => s.name).join(", ")} retired.`,
  });

  return checks;
}
