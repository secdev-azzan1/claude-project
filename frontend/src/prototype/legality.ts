// The placement-rule engine (rules R1–R8): computes what may be added where.
// Every "+ add block" menu in the UI is a rendering of computeAddMenu().

import type { AdapterId, BlockMode, Flow, FlowBlock, FlowTopic } from "./types";

export interface AddMenuEntry {
  key: string;
  adapter?: AdapterId;
  mode?: BlockMode;
  label: string;
  description: string;
  /** Present ⇒ entry is greyed out with this reason (rule refusal or future scope). */
  disabledReason?: string;
  futureScope?: boolean;
}

const FUTURE_SCOPE: AddMenuEntry[] = [
  {
    key: "future-nosql",
    label: "NoSQL family",
    description: "MongoDB, graph, key-value",
    disabledReason: "Coming later — shelved for the MVP, returns as an adapter package.",
    futureScope: true
  },
  {
    key: "future-files",
    label: "File share family",
    description: "File upload, S3, SMB",
    disabledReason: "Coming later — shelved for the MVP, returns as an adapter package.",
    futureScope: true
  },
  {
    key: "future-jdbc",
    label: "More JDBC dialects",
    description: "Oracle, SQL Server…",
    disabledReason: "Coming later — PostgreSQL, Trino and MySQL ship first.",
    futureScope: true
  },
];

export function blockById(flow: Flow, id: string | null | undefined): FlowBlock | undefined {
  return flow.blocks.find((b) => b.id === id);
}

export function topicById(flow: Flow, id: string | null | undefined): FlowTopic | undefined {
  return flow.topics.find((t) => t.id === id);
}

export function childrenOf(flow: Flow, nodeId: string): FlowBlock[] {
  return flow.blocks.filter((b) => b.parentId === nodeId);
}

/** R8 — true when the branch above (and including) this block carries raw bytes. */
export function isRawBranch(flow: Flow, block: FlowBlock | undefined): boolean {
  let cur = block;
  while (cur) {
    if (cur.adapter === "kafka" && cur.mode === "read" && cur.config?.parseFormat === "raw") return true;
    cur = cur.parentId ? blockById(flow, cur.parentId) : undefined;
  }
  return false;
}

/** Terminal blocks (R3/R5): the chain never continues after these. */
export function isTerminal(block: FlowBlock): boolean {
  return block.adapter === "kafka_kc" || block.adapter === "kc";
}

/** Whether a block hosts the Generic-transformations section. */
export function hostsTransforms(flow: Flow, block: FlowBlock): boolean {
  if (block.adapter === "kc") return false;
  return !isRawBranch(flow, block);
}

/** R2 — the menu shown when a flow has no root yet. */
export function computeRootMenu(): AddMenuEntry[] {
  return [
    { key: "http-read", adapter: "http", mode: "read", label: "http · read", description: "Pull records from an API", },
    {
      key: "http-write",
      adapter: "http",
      mode: "write",
      label: "http · write",
      description: "A POST whose response is the data to process"
    },
    { key: "jdbc-read", adapter: "jdbc", mode: "read", label: "jdbc · read", description: "Read tables from a database", },
    {
      key: "kafka-read",
      adapter: "kafka",
      mode: "read",
      label: "kafka · read (adopt a topic)",
      description: "Consume an existing topic — sampled, never renamed"
    },
    {
      key: "kafka_kc-root",
      adapter: "kafka_kc",
      label: "kafka+connect · governed write",
      description: "Avro topic + managed sink",
      disabledReason: "R2 — kafka+connect is always a destination; it can never start a flow."
    },
    ...FUTURE_SCOPE,
  ];
}

/** The + menu at a block's output. */
export function computeAddMenu(flow: Flow, afterBlockId: string): AddMenuEntry[] {
  const parent = blockById(flow, afterBlockId);
  if (!parent) return [];
  if (isTerminal(parent)) return []; // no + on terminal blocks at all

  const raw = isRawBranch(flow, parent);
  const entries: AddMenuEntry[] = [
    { key: "http-read", adapter: "http", mode: "read", label: "http · read", description: "Chained request per record", },
    { key: "http-lookup", adapter: "http", mode: "lookup", label: "http · lookup", description: "Enrich records from an API", },
    { key: "http-write", adapter: "http", mode: "write", label: "http · write", description: "POST records onward", },
    { key: "jdbc-lookup", adapter: "jdbc", mode: "lookup", label: "jdbc · lookup", description: "Enrich records from a table", },
    { key: "jdbc-write", adapter: "jdbc", mode: "write", label: "jdbc · write", description: "Write records to a table", },
    {
      key: "kafka-write",
      adapter: "kafka",
      mode: "write",
      label: "kafka · write",
      description: "Schemaless topic on the platform cluster"
    },
    {
      key: "kafka_kc",
      adapter: "kafka_kc",
      label: "kafka+connect · governed write",
      description: "Avro topic + managed sink (schema ceremony)"
    },
    ...FUTURE_SCOPE,
  ];

  return entries.map((e) => {
    if (e.disabledReason) return e;
    if (raw && e.adapter === "kafka_kc") {
      return { ...e, disabledReason: "R8 — raw bytes are quarantined: a governed Avro write cannot follow a raw kafka read." };
    }
    if (raw && e.adapter !== "kafka") {
      return { ...e, disabledReason: "R8 — raw bytes are quarantined: only byte-preserving delivery (kafka · write) is legal here." };
    }
    return e;
  });
}

/**
 * What placing a block under `parentNodeId` MEANS, so every ＋ menu can say it
 * before the click rather than after.
 *
 * Adding a block IS adding a branch — there is no separate act, and no second
 * kind of branch. Whether that branch carries a condition is decided afterwards,
 * in Routing.
 *
 *  · "root"     — the flow has nothing yet.
 *  · "first"    — this block has no branches yet.
 *  · "parallel" — it already has some, so this is another one alongside them.
 *  · "attach"   — a topic parent: independent subscribers, never branches (R5).
 */
export type AddIntent = "root" | "first" | "parallel" | "attach";

export function addIntent(flow: Flow, parentNodeId: string | null): AddIntent {
  if (parentNodeId === null) return "root";
  if (topicById(flow, parentNodeId)) return "attach";
  return childrenOf(flow, parentNodeId).length > 0 ? "parallel" : "first";
}

/** The one wording for an add menu's heading, shared by every ＋ in the UI. */
export function addMenuLabel(flow: Flow, parentNodeId: string | null): string {
  const intent = addIntent(flow, parentNodeId);
  if (intent === "root") return "Choose the root block";
  if (intent === "attach") return `Attach to ${topicById(flow, parentNodeId)?.name ?? "this topic"}`;
  const name = blockById(flow, parentNodeId)?.name ?? "this block";
  return intent === "parallel"
    ? `Another branch off "${name}" — conditions are set in Routing`
    : `First branch off "${name}" — it receives every record until you add a condition`;
}

/** The menu on a topic node: kafka read continues the chain, kc subscribes. */
export function computeTopicMenu(flow: Flow, topicId: string): AddMenuEntry[] {
  const topic = topicById(flow, topicId);
  if (!topic) return [];
  if (topic.sealed) {
    return [
      {
        key: "sealed",
        label: "This topic is sealed",
        description: "kafka+connect topics are visible but never attachable.",
        disabledReason: "Sealed — the governed topic is managed with its sink as one unit."
      },
    ];
  }
  return [
    {
      key: "kafka-read",
      adapter: "kafka",
      mode: "read",
      label: "kafka · read",
      description: "Continue the chain from this topic (continuous consumer)"
    },
    {
      key: "kc",
      adapter: "kc",
      label: "kc · sink subscription",
      description: "Deliver this topic's bytes untouched into an installed Connect sink"
    },
  ];
}

/**
 * R1 — whether the flow has a cron trigger at all. Only http/jdbc roots are
 * scheduled; kafka reads (rooted or mid-chain) are continuous consumers, and
 * a topic-rooted flow with only kc sinks has no trigger of any kind.
 */
export function flowHasTrigger(flow: Flow): boolean {
  const root = rootBlock(flow);
  return root ? root.adapter === "http" || root.adapter === "jdbc" : false;
}

/**
 * The flow's root block: either a block with no parent, or — for
 * topic-rooted flows — the kafka read attached to the adopted topic
 * (kc subscriptions never count as the root).
 */
export function rootBlock(flow: Flow): FlowBlock | undefined {
  const bare = flow.blocks.find((b) => b.parentId === null);
  if (bare) return bare;
  return flow.blocks.find(
    (b) => b.adapter === "kafka" && b.mode === "read" && topicById(flow, b.parentId)?.kind === "adopted",
  );
}

/** The first runnable block that carries the cron badge. */
export function scheduledBlock(flow: Flow): FlowBlock | undefined {
  return rootBlock(flow);
}

// ------------------------------------------------- subtree walk + re-parent

/**
 * Every node id at or below `blockId` — block ids *and* the ids of the topics
 * those blocks materialize (whose own children then join the closure).
 * Deliberately iterative with a visited set: the flow graph is only a tree by
 * convention, and a single bad parent link would make naive recursion hang the
 * tab rather than raise an error.
 *
 * The starting block's own id is included.
 */
export function subtreeIds(flow: Flow, blockId: string): string[] {
  const seen = new Set<string>();
  const queue: string[] = [blockId];
  while (queue.length > 0) {
    const id = queue.shift()!;
    if (seen.has(id)) continue;
    seen.add(id);
    for (const b of flow.blocks) if (b.parentId === id && !seen.has(b.id)) queue.push(b.id);
    for (const t of flow.topics) if (t.writerBlockId === id && !seen.has(t.id)) queue.push(t.id);
  }
  return [...seen];
}

/** The menu entry shape a block would occupy in an add menu. */
function matchesBlock(entry: AddMenuEntry, block: FlowBlock): boolean {
  if (!entry.adapter) return false;
  return entry.adapter === block.adapter && (entry.mode ?? undefined) === (block.mode ?? undefined);
}

function describeBlock(block: FlowBlock): string {
  return `${block.adapter}${block.mode ? ` · ${block.mode}` : ""}`;
}

/**
 * Whether `blockId` may be moved under `newParentId` (a block id or a topic
 * id). Returns null when the move is legal, otherwise the refusal — in the
 * same voice as the rule refusals, because it is shown to the user verbatim.
 *
 * The subtree check is the cycle guard: nothing else in this codebase has one,
 * and a parent link that points into a block's own descendants makes
 * `buildFlowGraph`, `isRawBranch` and `branchPathLabels` recurse forever.
 */
export function canReparent(flow: Flow, blockId: string, newParentId: string): string | null {
  const block = blockById(flow, blockId);
  if (!block) return "That block is no longer part of this flow.";

  if (block.parentId === null || rootBlock(flow)?.id === blockId)
    return "The root block cannot be re-parented — R2 fixes where a flow starts.";

  if (newParentId === blockId) return "A block cannot be its own parent.";

  const descendants = subtreeIds(flow, blockId);
  if (descendants.includes(newParentId))
    return "That target sits inside this block's own branch — moving it there would close the chain into a loop.";

  if (newParentId === block.parentId) return null; // already there: a legal no-op

  const parentTopic = topicById(flow, newParentId);
  if (parentTopic) {
    if (parentTopic.sealed)
      return "Sealed — the governed topic is managed with its sink as one unit; nothing can attach.";
    const menu = computeTopicMenu(flow, newParentId);
    const entry = menu.find((e) => matchesBlock(e, block));
    if (!entry)
      return `Only a kafka · read or a kc · sink subscription attaches to a topic (R5) — ${describeBlock(block)} cannot.`;
    if (entry.disabledReason) return entry.disabledReason;
    return null;
  }

  const parent = blockById(flow, newParentId);
  if (!parent) return "That target is no longer part of this flow.";

  if (block.adapter === "kc")
    return "kc subscribes to a topic, never to a block (R5) — drop it on a topic node instead.";

  if (isTerminal(parent))
    return `"${parent.name}" is terminal (R3/R5) — the chain never continues after it.`;

  const menu = computeAddMenu(flow, newParentId);
  const entry = menu.find((e) => matchesBlock(e, block));
  if (!entry) return `${describeBlock(block)} is not legal after "${parent.name}".`;
  if (entry.disabledReason) return entry.disabledReason;

  // R8 travels with the branch: a subtree that is legal on its own can still
  // be illegal once it hangs below a raw kafka read.
  if (isRawBranch(flow, parent)) {
    const stranded = descendants
      .map((id) => blockById(flow, id))
      .filter((b): b is FlowBlock => !!b && b.id !== blockId)
      .find((b) => !(b.adapter === "kafka" && b.mode === "write"));
    if (stranded)
      return `R8 — raw bytes are quarantined: "${stranded.name}" (${describeBlock(stranded)}) cannot follow a raw kafka read.`;
  }

  return null;
}
