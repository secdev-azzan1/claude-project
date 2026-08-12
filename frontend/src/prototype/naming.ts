// Pure naming logic: the tokenizer, derived names, and a simplified
// version of the spec's deterministic "naming walk".

import type { Flow, FlowBlock } from "./types";

/** One tokenizer everywhere: trim → lowercase → non-alphanumeric → "_". */
export function tokenize(input: string): string {
  return input
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .replace(/_{2,}/g, "_");
}

export function dlqName(flowName: string): string {
  return `dlq.${tokenize(flowName)}`;
}

export function tableName(flowName: string, entity: string): string {
  return `bronze.${tokenize(flowName)}.${tokenize(entity)}__raw`;
}

export function baseTopicName(flowName: string, entity: string): string {
  return `raw.${tokenize(flowName)}.${tokenize(entity)}`;
}

/** Names that already exist on the (mock) cluster and cannot be re-created. */
export const RESERVED_TOPIC_NAMES = [
  "raw.rapid7_assets.asset",
  "raw.fortisiem_events.incident",
  "raw.cmdb_asset_sync.asset",
  "raw.asset_retirement.asset",
  "partner.threatfeed.indicators",
  "audit.raw.mirror",
];

export interface DerivedName {
  value: string;
  overridden: boolean;
  /** Present when the name needed a variant token (naming-walk step 3/4). */
  variant?: string;
  warning?: string;
}

function isKafkaFamilyWrite(b: FlowBlock): boolean {
  return (b.adapter === "kafka" && b.mode === "write") || b.adapter === "kafka_kc";
}

/** Walk up to the root collecting branch labels for variant tokens. */
export function branchPathLabels(flow: Flow, block: FlowBlock): string[] {
  const labels: string[] = [];
  let cur: FlowBlock | undefined = block;
  const byId = new Map(flow.blocks.map((b) => [b.id, b]));
  while (cur) {
    if (cur.branch?.name) labels.unshift(cur.branch.name);
    cur = cur.parentId ? byId.get(cur.parentId) : undefined;
  }
  return labels;
}

/** The one cleanup applied to a typed topic override (R7). */
export function cleanTopicOverride(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "_");
}

/**
 * Simplified naming walk for one flow:
 * - a sole writer for an entity keeps the bare name;
 * - multiple same-entity writers take variant tokens pre-filled from branch
 *   labels (or fork-N defaults);
 * - on a bare-name tie between kafka_kc and a plain kafka write, the
 *   governed (kafka_kc) write holds the bare name.
 *
 * An override wins for the whole kafka family — plain kafka writes *and*
 * kafka_kc governed writes (R7). Gating it on `kafka` alone made the override
 * stored, displayed and silently ignored on kafka_kc.
 */
export function deriveTopicName(flow: Flow, block: FlowBlock): DerivedName {
  if (!isKafkaFamilyWrite(block)) return { value: "", overridden: false };
  if (block.topicOverride) {
    const clean = cleanTopicOverride(block.topicOverride);
    return {
      value: clean,
      overridden: true,
      warning: clean.startsWith("raw.")
        ? "Custom name strays into the derived raw.* namespace — collisions with derived names are likely."
        : undefined,
    };
  }
  const entity = block.entity ? tokenize(block.entity) : "";
  if (!entity) return { value: "raw.<entity missing>", overridden: false, warning: "Set an entity label to derive the topic name." };
  const base = baseTopicName(flow.name, entity);

  const sameEntityWriters = flow.blocks.filter(
    (b) => isKafkaFamilyWrite(b) && !b.topicOverride && b.entity && tokenize(b.entity) === entity,
  );
  if (sameEntityWriters.length <= 1) return { value: base, overridden: false };

  // Governed write wins the bare name when kinds differ.
  const governed = sameEntityWriters.filter((b) => b.adapter === "kafka_kc");
  if (governed.length === 1 && block.id === governed[0].id) {
    return { value: base, overridden: false };
  }
  const labels = branchPathLabels(flow, block);
  const variant = tokenize(labels[labels.length - 1] ?? "") || `variant_${sameEntityWriters.findIndex((b) => b.id === block.id) + 1}`;
  return { value: `${base}.${variant}`, overridden: false, variant };
}

/**
 * What the block's topic would be called with no override at all — the name
 * the override input shows as its placeholder.
 */
export function derivedTopicDefault(flow: Flow, block: FlowBlock): DerivedName {
  if (!block.topicOverride) return deriveTopicName(flow, block);
  const bare = { ...block, topicOverride: null };
  const probe: Flow = { ...flow, blocks: flow.blocks.map((b) => (b.id === block.id ? bare : b)) };
  return deriveTopicName(probe, bare);
}

/**
 * True when a typed override is exactly what the platform would have derived
 * anyway. The form uses this to short-circuit the collision / raw-namespace
 * warnings: re-typing the derived name is not a custom name, so warning about
 * it is noise (and the reserved-name check would fire on the flow's own topic).
 */
export function overrideMatchesDerived(
  flow: Flow,
  block: FlowBlock,
  override: string | null | undefined = block.topicOverride,
): boolean {
  const typed = cleanTopicOverride(override ?? "");
  if (!typed) return false;
  return typed === derivedTopicDefault(flow, block).value;
}

export function topicNameCollision(name: string, existing: string[] = RESERVED_TOPIC_NAMES): string | null {
  if (!name) return null;
  return existing.includes(name) ? `"${name}" is already reserved on the platform cluster.` : null;
}

/** Human preview of the next cron occurrences — canned, not a cron engine. */
export function cronPreview(cron: string | null): string[] {
  if (!cron) return [];
  const known: Record<string, string[]> = {
    "*/5 * * * *": ["today 12:05 UTC", "today 12:10 UTC", "today 12:15 UTC"],
    "*/15 * * * *": ["today 12:15 UTC", "today 12:30 UTC", "today 12:45 UTC"],
    "0 * * * *": ["today 13:00 UTC", "today 14:00 UTC", "today 15:00 UTC"],
    "0 */6 * * *": ["today 18:00 UTC", "tomorrow 00:00 UTC", "tomorrow 06:00 UTC"],
    "0 2 * * *": ["tomorrow 02:00 UTC", "in 2 days 02:00 UTC", "in 3 days 02:00 UTC"],
    "0 6 * * 1": ["Mon 06:00 UTC", "next Mon 06:00 UTC", "in 2 weeks Mon 06:00 UTC"],
  };
  if (known[cron]) return known[cron];
  const fields = cron.trim().split(/\s+/);
  if (fields.length !== 5) return [];
  return ["next occurrence (preview)", "second occurrence (preview)", "third occurrence (preview)"];
}

export function isValidCron(cron: string | null): boolean {
  if (cron === null) return true;
  return cron.trim().split(/\s+/).length === 5;
}

export const CRON_PRESETS: { label: string; value: string }[] = [
  { label: "Every 5 minutes", value: "*/5 * * * *" },
  { label: "Every 15 minutes", value: "*/15 * * * *" },
  { label: "Hourly", value: "0 * * * *" },
  { label: "Every 6 hours", value: "0 */6 * * *" },
  { label: "Daily at 02:00 UTC", value: "0 2 * * *" },
  { label: "Weekly (Mon 06:00 UTC)", value: "0 6 * * 1" },
];
