// The one home for structural flow edits. Every surface — the block form, the
// outline, the graph — calls these, so "a change in the graph shows in the form
// and vice versa" is true by construction rather than by discipline.
//
// Rules of this module:
// - PURE. A mutation takes a Flow and returns a new one. Nothing here touches
//   React state, the store, or selection. Selection comes back as `selectId`
//   and the caller applies it (setting it inside a state updater double-fires
//   under StrictMode).
// - EVERY guard lives here: the edit lock, legality, and the cycle guard.
//   "Don't render the button" stops being a guarantee the moment a second
//   surface can trigger the same edit.
// - Failures come back as data (`{ ok: false, reason }`), never thrown, and the
//   reason is the string the user is shown.

import { getEditLockReason, syncFlowTopics } from "./api";
import {
  blockById,
  canReparent,
  computeAddMenu,
  computeRootMenu,
  computeTopicMenu,
  subtreeIds,
  topicById,
  type AddMenuEntry,
} from "./legality";
import { deriveTopicName } from "./naming";
import { uid } from "./store";
import type { AdapterId, BranchInfo, BranchCondition, BranchMatch, Flow, FlowBlock, FlowTopic } from "./types";

// ------------------------------------------------------------------ results

export interface MutationOk {
  ok: true;
  flow: Flow;
  /**
   * The node the caller should select once it has applied the flow. Omitted by
   * mutations that EDIT what is already selected — moving the selection while
   * someone is typing in a field yanks the form out from under them.
   */
  selectId?: string;
}
export interface MutationRefused {
  ok: false;
  /** Shown to the user verbatim. */
  reason: string;
}
export type MutationResult = MutationOk | MutationRefused;

export type DeleteResult =
  | { ok: true; flow: Flow; removedIds: string[]; removedTopicIds: string[] }
  | MutationRefused;

/** One derived topic name that a re-parent would change. */
export interface TopicRename {
  blockId: string;
  blockName: string;
  from: string;
  to: string;
}

const refuse = (reason: string): MutationRefused => ({ ok: false, reason });

// ------------------------------------------------------------------ helpers

/** Adapter labels, duplicated from ADAPTER_META on purpose: this layer stays free of React. */
const ADAPTER_LABELS: Record<AdapterId, string> = {
  http: "http",
  jdbc: "jdbc",
  kafka: "kafka",
  kafka_kc: "kafka+connect",
  kc: "kc",
};

function describeEntry(entry: AddMenuEntry): string {
  const label = entry.adapter ? ADAPTER_LABELS[entry.adapter] : "That block";
  return `${label}${entry.mode ? ` · ${entry.mode}` : ""}`;
}

/**
 * The edit lock, enforced where it cannot be forgotten. kc is exempt: a sink
 * subscription is an independent, live object — "Save is live".
 */
function lockRefusal(flow: Flow, adapter?: AdapterId): string | null {
  const reason = getEditLockReason(flow);
  if (!reason) return null;
  if (adapter === "kc") return null;
  return reason;
}

/** Per-adapter starting config. `proxyId` (not the old boolean `proxy`) for http. */
export function defaultConfigFor(entry: AddMenuEntry, parentTopicId?: string): Record<string, unknown> {
  switch (entry.adapter) {
    case "http":
      return {
        method: entry.mode === "write" ? "POST" : "GET",
        path: "",
        responseFormat: "json",
        split: true,
        pagination: { type: "none", fields: {} },
        proxyId: null,
      };
    case "jdbc":
      return { columns: [] };
    case "kafka":
      return entry.mode === "read" ? { parseFormat: "json", initialPosition: "beginning" } : {};
    case "kafka_kc":
      return {};
    case "kc":
      return { attachTopicId: parentTopicId ?? "", initialPosition: "beginning" };
    default:
      return {};
  }
}

function defaultBlockName(entry: AddMenuEntry): string {
  const label = entry.adapter ? ADAPTER_LABELS[entry.adapter] : "block";
  return `New ${label}${entry.mode ? ` ${entry.mode}` : ""}`;
}

function entryMatchesEntry(candidate: AddMenuEntry, entry: AddMenuEntry): boolean {
  if (!candidate.adapter) return false;
  return candidate.adapter === entry.adapter && (candidate.mode ?? undefined) === (entry.mode ?? undefined);
}

/** Is this adapter+mode legal at this position? Returns the refusal, or null. */
function placementRefusal(flow: Flow, parentNodeId: string | null, entry: AddMenuEntry): string | null {
  if (parentNodeId === null) {
    const found = computeRootMenu().find((e) => entryMatchesEntry(e, entry));
    if (!found) return `${describeEntry(entry)} cannot start a flow (R2).`;
    return found.disabledReason ?? null;
  }

  const topic = topicById(flow, parentNodeId);
  if (topic) {
    const menu = computeTopicMenu(flow, parentNodeId);
    const found = menu.find((e) => entryMatchesEntry(e, entry));
    if (!found)
      return topic.sealed
        ? "Sealed — the governed topic is managed with its sink as one unit; nothing can attach."
        : `Exactly two things attach to a topic (R5): a kafka · read or a kc · sink subscription — ${describeEntry(entry)} is neither.`;
    return found.disabledReason ?? null;
  }

  const parent = blockById(flow, parentNodeId);
  if (!parent) return "That parent is no longer part of this flow.";
  const menu = computeAddMenu(flow, parentNodeId);
  if (menu.length === 0) return `"${parent.name}" is terminal (R3/R5) — the chain never continues after it.`;
  const found = menu.find((e) => entryMatchesEntry(e, entry));
  if (!found) return `${describeEntry(entry)} is not legal after "${parent.name}".`;
  return found.disabledReason ?? null;
}

/**
 * Name a child joining a parent that already has children, and promote a lone
 * unnamed sibling to branch-1 so no branch sits nameless next to a named one.
 *
 * A lone child stays unnamed on purpose: branch labels feed topic variant
 * tokens, so naming an edge that nothing distinguishes would rename derived
 * topics for no reason. Setting a condition on that lone branch names it, and
 * that rename is the user's own act.
 */
function attachAsBranch(
  blocks: FlowBlock[],
  parentId: string,
  movingId: string | null,
): { blocks: FlowBlock[]; branch?: BranchInfo } {
  const siblings = blocks.filter((b) => b.parentId === parentId && b.id !== movingId && b.adapter !== "kc");
  if (siblings.length === 0) return { blocks };

  const next = blocks.map((b) =>
    b.parentId === parentId && b.id !== movingId && b.adapter !== "kc" && !b.branch
      ? { ...b, branch: { name: "branch-1" } }
      : b,
  );

  const used = new Set(
    next
      .filter((b) => b.parentId === parentId && b.id !== movingId)
      .map((b) => b.branch?.name)
      .filter((n): n is string => !!n),
  );
  let n = siblings.length + 1;
  while (used.has(`branch-${n}`)) n += 1;
  return { blocks: next, branch: { name: `branch-${n}` } };
}

/** The topic a block materializes, if any. */
function materializedTopic(flow: Flow, blockId: string): FlowTopic | undefined {
  return flow.topics.find((t) => t.writerBlockId === blockId);
}

/**
 * Topic objects have to be copied before `syncFlowTopics` runs: it renames them
 * in place, and a shared reference would rename the caller's flow too — a pure
 * function that edits its input is worse than no function at all.
 */
function copyTopics(topics: FlowTopic[]): FlowTopic[] {
  return topics.map((t) => ({ ...t }));
}

// ---------------------------------------------------------------- add block

/**
 * Place a new block under `parentId` (a block id, a topic id, or null for the
 * root). Adopting a topic as the root materializes the topic node itself (R2),
 * exactly as the seeded flows are shaped.
 */
export function addBlock(flow: Flow, parentId: string | null, entry: AddMenuEntry): MutationResult {
  if (!entry.adapter) return refuse("That menu entry cannot be placed.");
  if (entry.disabledReason) return refuse(entry.disabledReason);

  const lock = lockRefusal(flow, entry.adapter);
  if (lock) return refuse(lock);

  const illegal = placementRefusal(flow, parentId, entry);
  if (illegal) return refuse(illegal);

  let parentNodeId = parentId;
  let topics = flow.topics;
  if (parentId === null && entry.adapter === "kafka" && entry.mode === "read") {
    const topicId = uid("t");
    topics = [
      ...topics,
      { id: topicId, kind: "adopted" as const, name: "partner.events.stream", sealed: false, backlogEstimate: 125_000 },
    ];
    parentNodeId = topicId;
  }

  const isTopicParent = topics.some((t) => t.id === parentNodeId);
  const id = uid("b");

  let blocks = flow.blocks;
  let branch: BranchInfo | undefined;
  // Blocks attached to a topic are independent subscribers/readers (R5), not
  // forks of one another — only block parents fork.
  if (!isTopicParent && parentId && entry.adapter !== "kc") {
    const attached = attachAsBranch(blocks, parentId, null);
    blocks = attached.blocks;
    branch = attached.branch;
  }

  const block: FlowBlock = {
    id,
    adapter: entry.adapter,
    mode: entry.mode,
    name: defaultBlockName(entry),
    parentId: parentNodeId,
    branch,
    serviceId: null,
    entity: null,
    config: defaultConfigFor(entry, isTopicParent ? parentNodeId ?? undefined : undefined),
    transforms: [],
    testResult: null,
  };

  const next: Flow = { ...flow, topics: copyTopics(topics), blocks: [...blocks, block] };
  syncFlowTopics(next);
  return { ok: true, flow: next, selectId: id };
}

// ------------------------------------------------------------- re-parent

/** The move itself, with legality already established. */
function applyReparent(flow: Flow, blockId: string, newParentId: string): Flow {
  const block = blockById(flow, blockId)!;
  const toTopic = topicById(flow, newParentId);

  let blocks = flow.blocks;
  let branch: BranchInfo | undefined = block.branch;

  // A branch travels with its block: the condition is the branch's own, not a
  // pointer into the old parent, so moving it keeps meaning exactly what it said.

  if (toTopic) {
    branch = undefined; // topic children are independent, never branches
  } else {
    const attached = attachAsBranch(blocks, newParentId, blockId);
    blocks = attached.blocks;
    if (!branch) branch = attached.branch;
  }

  const config = { ...block.config };
  // kc's association lives in config, not in parentId. syncFlowTopics deletes
  // any kc block whose attach target no longer resolves, so it has to move in
  // the SAME mutation.
  if (block.adapter === "kc") {
    config.attachTopicId = toTopic ? toTopic.id : (materializedTopic(flow, newParentId)?.id ?? "");
  }

  const next: Flow = {
    ...flow,
    topics: copyTopics(flow.topics),
    blocks: blocks.map((b) => (b.id === blockId ? { ...b, parentId: newParentId, branch, config } : b)),
  };
  syncFlowTopics(next);
  return next;
}

/**
 * Move a block (and everything under it) to a new parent. The cycle guard in
 * `canReparent` is load-bearing: a parent link into a block's own subtree is an
 * infinite recursion in graph building, not a validation error.
 */
export function reparentBlock(flow: Flow, blockId: string, newParentId: string): MutationResult {
  const block = blockById(flow, blockId);
  if (!block) return refuse("That block is no longer part of this flow.");

  const lock = lockRefusal(flow, block.adapter);
  if (lock) return refuse(lock);

  const illegal = canReparent(flow, blockId, newParentId);
  if (illegal) return refuse(illegal);

  const next = applyReparent(flow, blockId, newParentId);
  if (!next.blocks.some((b) => b.id === blockId))
    return refuse("The move would leave the block attached to nothing — nothing was changed.");
  return { ok: true, flow: next, selectId: blockId };
}

/**
 * What a re-parent would rename before it happens. Derived topic names take
 * their variant token from the branch labels *above* a block, so moving a
 * subtree quietly renames topics that belong to blocks the user never touched.
 * Returns [] when the move is refused — nothing would be renamed.
 */
export function previewReparentRenames(flow: Flow, blockId: string, newParentId: string): TopicRename[] {
  if (!blockById(flow, blockId)) return [];
  if (canReparent(flow, blockId, newParentId)) return [];
  const next = applyReparent(flow, blockId, newParentId);
  const before = new Map(flow.blocks.map((b) => [b.id, deriveTopicName(flow, b).value]));
  return next.blocks
    .map((b) => ({
      blockId: b.id,
      blockName: b.name,
      from: before.get(b.id) ?? "",
      to: deriveTopicName(next, b).value,
    }))
    .filter((r) => r.from && r.to && r.from !== r.to);
}

// ---------------------------------------------------------------- delete

/**
 * Delete a block, everything below it, the topics it materialized, and any
 * adopted topic left with nothing attached. Reports what actually went — topic
 * sync can take further kc blocks with it when their topic disappears.
 */
export function deleteBlockCascade(flow: Flow, blockId: string): DeleteResult {
  const block = blockById(flow, blockId);
  if (!block) return refuse("That block is no longer part of this flow.");

  const lock = lockRefusal(flow, block.adapter);
  if (lock) return refuse(lock);

  const doomed = new Set(subtreeIds(flow, blockId));
  const next: Flow = {
    ...flow,
    blocks: flow.blocks.filter((b) => !doomed.has(b.id)),
    topics: copyTopics(flow.topics.filter((t) => !doomed.has(t.id))),
  };
  syncFlowTopics(next);
  next.topics = next.topics.filter((t) => t.kind !== "adopted" || next.blocks.some((b) => b.parentId === t.id));

  const survivingBlocks = new Set(next.blocks.map((b) => b.id));
  const survivingTopics = new Set(next.topics.map((t) => t.id));
  return {
    ok: true,
    flow: next,
    removedIds: flow.blocks.filter((b) => !survivingBlocks.has(b.id)).map((b) => b.id),
    removedTopicIds: flow.topics.filter((t) => !survivingTopics.has(t.id)).map((t) => t.id),
  };
}

// ------------------------------------------------------------ branch edits

/**
 * Set (or clear) a branch's condition, and its name.
 *
 * It is a mutation rather than a plain patch because of one asymmetry: a lone
 * child has no branch object at all, and giving it a condition has to mint one
 * — with a name, because names feed topic variant tokens. Doing that at the call
 * site would mean every surface that can edit a condition re-deriving the naming
 * rule, which is how two surfaces start disagreeing.
 *
 * `rules: []` clears them, which returns the branch to "every record".
 */
export function setBranch(
  flow: Flow,
  blockId: string,
  patch: { name?: string; rules?: BranchCondition[]; match?: BranchMatch },
): MutationResult {
  const block = blockById(flow, blockId);
  if (!block) return refuse("That block is no longer part of this flow.");
  if (!block.parentId) return refuse("The root block has nothing above it, so it is not a branch.");
  if (topicById(flow, block.parentId))
    return refuse("Blocks attached to a topic are independent subscribers (R5), not branches — they carry no condition.");

  const lock = lockRefusal(flow, block.adapter);
  if (lock) return refuse(lock);

  const siblings = flow.blocks.filter((b) => b.parentId === block.parentId);
  const existing = block.branch;
  const name =
    patch.name !== undefined
      ? patch.name
      : (existing?.name ?? `branch-${Math.max(1, siblings.indexOf(block) + 1)}`);
  const rules = patch.rules !== undefined ? patch.rules : (existing?.rules ?? []);
  const match = patch.match !== undefined ? patch.match : existing?.match;

  const branch: BranchInfo | undefined =
    rules.length === 0 && !name.trim()
      ? undefined
      : {
          name,
          ...(rules.length > 0 ? { rules } : {}),
          // `match` only means something with more than one rule; storing it
          // otherwise leaves a setting nothing in the UI can explain.
          ...(rules.length > 1 && match ? { match } : {}),
        };

  const next: Flow = {
    ...flow,
    topics: copyTopics(flow.topics),
    blocks: flow.blocks.map((b) => (b.id === blockId ? { ...b, branch } : b)),
  };
  syncFlowTopics(next);
  // No selectId: the branch is edited from its PARENT's form, and jumping to the
  // child on the first keystroke would close the editor being typed into.
  return { ok: true, flow: next };
}
