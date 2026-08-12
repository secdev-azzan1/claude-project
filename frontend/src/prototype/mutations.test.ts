import { describe, expect, it } from "vitest";
import { syncFlowTopics } from "./api";
import { computeAddMenu, computeRootMenu, computeTopicMenu, type AddMenuEntry } from "./legality";
import {
  addBlock,
  deleteBlockCascade,
  previewReparentRenames,
  reparentBlock,
  setBranch,
} from "./mutations";
import type { Flow, FlowBlock, FlowTopic } from "./types";

const block = (over: Partial<FlowBlock>): FlowBlock => ({
  id: "b1",
  adapter: "http",
  mode: "read",
  name: "Read",
  parentId: null,
  serviceId: null,
  entity: null,
  config: {},
  transforms: [],
  ...over,
});

const flow = (blocks: FlowBlock[], topics: FlowTopic[] = [], over: Partial<Flow> = {}): Flow => ({
  id: "f1",
  name: "Test Flow",
  state: "Draft",
  enabled: false,
  cron: null,
  blocks,
  topics,
  variables: [],
  servicePins: {},
  createdAt: "",
  updatedAt: "",
  ...over,
});

/** Unwrap a successful mutation, failing loudly when it refused. */
const ok = <T extends { ok: boolean; reason?: string }>(result: T): Extract<T, { ok: true }> => {
  if (result.ok === false) throw new Error(`expected success, got refusal: ${result.reason}`);
  return result as Extract<T, { ok: true }>;
};

const rootEntry = (key: string): AddMenuEntry => {
  const entry = computeRootMenu().find((e) => e.key === key);
  if (!entry) throw new Error(`no root entry ${key}`);
  return entry;
};

const entryAfter = (f: Flow, blockId: string, key: string): AddMenuEntry => {
  const entry = computeAddMenu(f, blockId).find((e) => e.key === key);
  if (!entry) throw new Error(`no entry ${key} after ${blockId}`);
  return entry;
};

const entryOnTopic = (f: Flow, topicId: string, key: string): AddMenuEntry => {
  const entry = computeTopicMenu(f, topicId).find((e) => e.key === key);
  if (!entry) throw new Error(`no topic entry ${key}`);
  return entry;
};

describe("addBlock", () => {
  it("adopts a topic node when a kafka read is placed at the root (R2)", () => {
    const empty = flow([]);
    const result = ok(addBlock(empty, null, rootEntry("kafka-read")));
    expect(result.flow.topics).toHaveLength(1);
    const added = result.flow.blocks.find((b) => b.id === result.selectId)!;
    expect(added.parentId).toBe(result.flow.topics[0].id);
    expect(result.flow.topics[0].kind).toBe("adopted");
    expect(empty.blocks).toHaveLength(0); // the input flow is untouched
  });

  it("refuses an illegal placement even when the caller hands it a bad entry", () => {
    const root = block({ id: "root" });
    const kkc = block({ id: "kkc", adapter: "kafka_kc", mode: undefined, parentId: "root" });
    const f = flow([root, kkc]);
    const refusal = addBlock(f, "kkc", { key: "http-read", adapter: "http", mode: "read", label: "http · read", description: "" });
    expect(refusal.ok).toBe(false);
    expect(refusal.ok === false && refusal.reason).toMatch(/terminal/);
    expect(addBlock(f, null, rootEntry("kafka_kc-root")).ok).toBe(false);
  });

  it("numbers branches from the siblings that exist", () => {
    const root = block({ id: "root" });
    const high = block({ id: "high", parentId: "root", branch: { name: "high", rules: [{ field: "severity", op: "equals", value: "HIGH" }] } });
    const low = block({ id: "low", parentId: "root", branch: { name: "low", rules: [{ field: "severity", op: "equals", value: "LOW" }] } });
    const f = flow([root, high, low]);

    const first = ok(addBlock(f, "root", entryAfter(f, "root", "http-read")));
    const added = first.flow.blocks.find((b) => b.id === first.selectId)!;
    expect(added.branch).toEqual({ name: "branch-3" });
    // A new branch carries no rules: it receives every record until one is set.
    expect(added.branch?.rules).toBeUndefined();
    expect(first.flow.blocks.find((b) => b.id === "high")!.branch?.name).toBe("high");
  });

  it("promotes a lone unnamed sibling to branch-1 when a second child appears", () => {
    const root = block({ id: "root" });
    const only = block({ id: "only", parentId: "root" });
    const f = flow([root, only]);
    const result = ok(addBlock(f, "root", entryAfter(f, "root", "http-lookup")));
    expect(result.flow.blocks.find((b) => b.id === "only")!.branch).toEqual({ name: "branch-1" });
    expect(result.flow.blocks.find((b) => b.id === result.selectId)!.branch?.name).toBe("branch-2");
  });
});

// ---------------------------------------------------------- route branches

describe("setBranch", () => {
  it("mints a branch — with a name — when a lone child is given a rule", () => {
    const root = block({ id: "root" });
    const only = block({ id: "only", parentId: "root" });
    const f = flow([root, only]);

    const result = ok(setBranch(f, "only", { rules: [{ field: "env", op: "equals", value: "prod" }] }));
    const branch = result.flow.blocks.find((b) => b.id === "only")!.branch!;
    expect(branch.name).toBe("branch-1");
    expect(branch.rules).toEqual([{ field: "env", op: "equals", value: "prod" }]);
  });

  it("clearing the rules returns the branch to every record", () => {
    const root = block({ id: "root" });
    const child = block({ id: "c", parentId: "root", branch: { name: "prod", rules: [{ field: "env", op: "equals", value: "prod" }] } });
    const f = flow([root, child]);

    const result = ok(setBranch(f, "c", { rules: [] }));
    const branch = result.flow.blocks.find((b) => b.id === "c")!.branch!;
    expect(branch.name).toBe("prod"); // the label survives
    expect(branch.rules).toBeUndefined();
  });

  it("keeps several rules and the match mode, and drops match when only one rule is left", () => {
    const root = block({ id: "root" });
    const child = block({ id: "c", parentId: "root", branch: { name: "b1" } });
    const f = flow([root, child]);

    const two = ok(
      setBranch(f, "c", {
        rules: [
          { field: "age", op: "equals", value: "18" },
          { field: "name", op: "equals", value: "azzan" },
        ],
        match: "any",
      }),
    );
    const branch = two.flow.blocks.find((b) => b.id === "c")!.branch!;
    expect(branch.rules).toHaveLength(2);
    expect(branch.match).toBe("any");

    // One rule cannot combine with anything, so the mode is not stored.
    const one = ok(setBranch(two.flow, "c", { rules: [{ field: "age", op: "equals", value: "18" }] }));
    expect(one.flow.blocks.find((b) => b.id === "c")!.branch!.match).toBeUndefined();
  });

  it("refuses on the root and on topic subscribers (R5)", () => {
    const root = block({ id: "root" });
    const f = flow([root]);
    expect(setBranch(f, "root", { name: "x" }).ok).toBe(false);

    const topic: FlowTopic = { id: "t1", kind: "adopted", name: "partner.events", sealed: false };
    const sub = block({ id: "kc1", adapter: "kc", mode: undefined, parentId: "t1", config: { attachTopicId: "t1" } });
    const f2 = flow([sub], [topic]);
    const refusal = setBranch(f2, "kc1", { rules: [{ field: "a", op: "equals", value: "b" }] });
    expect(refusal.ok).toBe(false);
    expect(refusal.ok === false && refusal.reason).toMatch(/independent subscribers/);
  });
});

describe("reparentBlock", () => {
  const chain = () => {
    const root = block({ id: "root" });
    const mid = block({ id: "mid", parentId: "root" });
    const leaf = block({ id: "leaf", parentId: "mid" });
    return flow([root, mid, leaf]);
  };

  it("refuses a move into the block's own subtree — the cycle guard", () => {
    const f = chain();
    const refusal = reparentBlock(f, "mid", "leaf");
    expect(refusal.ok).toBe(false);
    expect(refusal.ok === false && refusal.reason).toMatch(/loop/);
    expect(f.blocks.find((b) => b.id === "mid")!.parentId).toBe("root");
  });

  it("moves a block and leaves the input flow alone", () => {
    const f = chain();
    const result = ok(reparentBlock(f, "leaf", "root"));
    expect(result.flow.blocks.find((b) => b.id === "leaf")!.parentId).toBe("root");
    expect(result.selectId).toBe("leaf");
    expect(f.blocks.find((b) => b.id === "leaf")!.parentId).toBe("mid");
  });

  it("patches a kc block's attachTopicId in the same mutation", () => {
    const root = block({ id: "root" });
    const w1 = block({ id: "w1", adapter: "kafka", mode: "write", entity: "asset", parentId: "root" });
    const w2 = block({ id: "w2", adapter: "kafka", mode: "write", entity: "incident", parentId: "root" });
    const f = flow([root, w1, w2]);
    syncFlowTopics(f);
    const t1 = f.topics.find((t) => t.writerBlockId === "w1")!.id;
    const t2 = f.topics.find((t) => t.writerBlockId === "w2")!.id;
    f.blocks.push(block({ id: "kc1", adapter: "kc", mode: undefined, parentId: t1, config: { attachTopicId: t1 } }));

    const result = ok(reparentBlock(f, "kc1", t2));
    const moved = result.flow.blocks.find((b) => b.id === "kc1");
    expect(moved).toBeDefined(); // not silently swept away by topic sync
    expect(moved!.parentId).toBe(t2);
    expect(moved!.config.attachTopicId).toBe(t2);
  });

  it("previews the topic renames a move causes", () => {
    const root = block({ id: "root" });
    const a = block({ id: "a", parentId: "root", branch: { name: "branch-1" } });
    const b = block({ id: "b", parentId: "root", branch: { name: "branch-2" } });
    const wa = block({ id: "wa", adapter: "kafka", mode: "write", entity: "asset", parentId: "a" });
    const wb = block({ id: "wb", adapter: "kafka", mode: "write", entity: "asset", parentId: "b" });
    const f = flow([root, a, b, wa, wb]);
    syncFlowTopics(f);

    const renames = previewReparentRenames(f, "wa", "b");
    expect(renames.length).toBeGreaterThan(0);
    expect(renames.every((r) => r.from !== r.to)).toBe(true);
    expect(renames.some((r) => r.blockId === "wa")).toBe(true);
    // A refused move renames nothing.
    expect(previewReparentRenames(f, "root", "b")).toEqual([]);
  });
});

// ----------------------------------------------------------------- delete

describe("deleteBlockCascade", () => {
  it("removes the subtree, its topics, and the kc blocks left with nothing to attach to", () => {
    const root = block({ id: "root" });
    const mid = block({ id: "mid", parentId: "root" });
    const leaf = block({ id: "leaf", parentId: "mid" });
    const write = block({ id: "w", adapter: "kafka", mode: "write", entity: "asset", parentId: "mid" });
    const f = flow([root, mid, leaf, write]);
    syncFlowTopics(f);
    const topicId = f.topics.find((t) => t.writerBlockId === "w")!.id;
    f.blocks.push(block({ id: "kc1", adapter: "kc", mode: undefined, parentId: topicId, config: { attachTopicId: topicId } }));

    const result = ok(deleteBlockCascade(f, "mid"));
    expect(result.flow.blocks.map((b) => b.id)).toEqual(["root"]);
    expect(result.removedIds.sort()).toEqual(["kc1", "leaf", "mid", "w"]);
    expect(result.removedTopicIds).toEqual([topicId]);
    expect(f.blocks).toHaveLength(5); // input untouched
  });

  it("drops an adopted topic once nothing is attached to it", () => {
    const empty = flow([]);
    const added = ok(addBlock(empty, null, rootEntry("kafka-read")));
    const result = ok(deleteBlockCascade(added.flow, added.selectId));
    expect(result.flow.blocks).toHaveLength(0);
    expect(result.flow.topics).toHaveLength(0);
  });

  it("refuses a block that is no longer there", () => {
    const refusal = deleteBlockCascade(flow([block({ id: "root" })]), "ghost");
    expect(refusal.ok === false && refusal.reason).toMatch(/no longer part of this flow/);
  });
});

// ------------------------------------------------------------- edit lock

describe("the edit lock", () => {
  const runningFlow = () => {
    const root = block({ id: "root" });
    const write = block({ id: "w", adapter: "kafka", mode: "write", entity: "asset", parentId: "root" });
    const f = flow([root, write], [], { state: "Running", deployedAt: "2026-08-01T00:00:00Z" });
    syncFlowTopics(f);
    return f;
  };

  it("refuses structural edits on a deployed flow, from any surface", () => {
    const f = runningFlow();
    const add = addBlock(f, "root", entryAfter(f, "root", "http-lookup"));
    expect(add.ok).toBe(false);
    expect(add.ok === false && add.reason).toMatch(/refused until it is stopped/);
    expect(reparentBlock(f, "w", "root").ok).toBe(false);
    expect(deleteBlockCascade(f, "w").ok).toBe(false);
    expect(setBranch(f, "w", { rules: [{ field: "a", op: "equals", value: "b" }] }).ok).toBe(false);
  });

  it("keeps kc subscriptions editable — Save is live", () => {
    const f = runningFlow();
    const topicId = f.topics.find((t) => t.writerBlockId === "w")!.id;
    const added = ok(addBlock(f, topicId, entryOnTopic(f, topicId, "kc")));
    expect(added.flow.blocks.find((b) => b.id === added.selectId)!.config.attachTopicId).toBe(topicId);
    expect(deleteBlockCascade(added.flow, added.selectId).ok).toBe(true);
  });
});
