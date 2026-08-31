import { describe, expect, it } from "vitest";
import {
  addMenuLabel,
  canReparent,
  computeAddMenu,
  computeRootMenu,
  computeTopicMenu,
  flowHasTrigger,
  isRawBranch,
  rootBlock,
  subtreeIds,
} from "./legality";
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

const topic = (over: Partial<FlowTopic>): FlowTopic => ({
  id: "t1",
  kind: "materialized",
  name: "raw.x.y",
  sealed: false,
  ...over,
});

const flow = (blocks: FlowBlock[], topics: FlowTopic[] = []): Flow => ({
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
});

describe("root menu (R2)", () => {
  it("offers legal roots and refuses kafka_kc with a reason", () => {
    const menu = computeRootMenu();
    const legal = menu.filter((e) => !e.disabledReason).map((e) => e.key);
    expect(legal).toEqual(["http-read", "http-write", "jdbc-read", "kafka-read"]);
    expect(menu.find((e) => e.key === "kafka_kc-root")?.disabledReason).toMatch(/R2/);
    expect(menu.filter((e) => e.futureScope).length).toBeGreaterThan(0);
  });
});

describe("add menu after a block", () => {
  it("terminal blocks get no + menu at all (R3/R5)", () => {
    const kc = block({ id: "kc1", adapter: "kc", mode: undefined });
    const kkc = block({ id: "kkc1", adapter: "kafka_kc", mode: undefined });
    expect(computeAddMenu(flow([kc]), "kc1")).toHaveLength(0);
    expect(computeAddMenu(flow([kkc]), "kkc1")).toHaveLength(0);
  });

  it("quarantines raw branches (R8): only kafka write stays legal", () => {
    const rawRead = block({ id: "r", adapter: "kafka", mode: "read", config: { parseFormat: "raw" } });
    const menu = computeAddMenu(flow([rawRead]), "r");
    const legal = menu.filter((e) => !e.disabledReason);
    expect(legal.map((e) => e.key).sort()).toEqual(["kafka-write"]);
    expect(menu.find((e) => e.key === "kafka_kc")?.disabledReason).toMatch(/R8/);
  });

  it("offers adapters only — adding a block IS adding a branch", () => {
    const read = block({ id: "r", adapter: "http", mode: "read" });
    const menu = computeAddMenu(flow([read]), "r");
    expect(menu.every((e) => !!e.adapter || !!e.futureScope)).toBe(true);
    expect(menu.find((e) => e.key === "jdbc-read")).toBeUndefined();
    expect(menu.find((e) => e.key === "fork")).toBeUndefined();
  });

  it("the menu heading says whether this is the first branch or another one", () => {
    const read = block({ id: "r", adapter: "http", mode: "read", name: "List Assets" });
    const one = flow([read]);
    expect(addMenuLabel(one, "r")).toMatch(/First branch/);
    expect(addMenuLabel(one, null)).toMatch(/root/);

    const child = block({ id: "c", adapter: "kafka", mode: "write", parentId: "r" });
    expect(addMenuLabel(flow([read, child]), "r")).toMatch(/Another branch/);
  });

  it("raw status propagates down the branch", () => {
    const rawRead = block({ id: "r", adapter: "kafka", mode: "read", config: { parseFormat: "raw" } });
    const child = block({ id: "w", adapter: "kafka", mode: "write", parentId: "r" });
    expect(isRawBranch(flow([rawRead, child]), child)).toBe(true);
  });
});

describe("topic menus (R5)", () => {
  it("sealed topics refuse everything", () => {
    const t = topic({ sealed: true });
    const entries = computeTopicMenu(flow([], [t]), "t1");
    expect(entries.every((e) => e.disabledReason)).toBe(true);
  });

  it("unsealed topics offer kafka read and kc only", () => {
    const t = topic({});
    const keys = computeTopicMenu(flow([], [t]), "t1").map((e) => e.key);
    expect(keys).toEqual(["kafka-read", "kc"]);
  });
});

describe("subtreeIds", () => {
  it("walks blocks and the topics they materialize, including the block itself", () => {
    const root = block({ id: "root" });
    const write = block({ id: "w", adapter: "kafka", mode: "write", parentId: "root" });
    const t = topic({ id: "t-w", writerBlockId: "w" });
    const sink = block({ id: "kc1", adapter: "kc", mode: undefined, parentId: "t-w" });
    const ids = subtreeIds(flow([root, write, sink], [t]), "w");
    expect(ids.sort()).toEqual(["kc1", "t-w", "w"]);
    expect(subtreeIds(flow([root, write, sink], [t]), "kc1")).toEqual(["kc1"]);
  });

  it("terminates on a cyclic parent chain instead of recursing forever", () => {
    const a = block({ id: "a", parentId: "b" });
    const b = block({ id: "b", parentId: "a" });
    expect(subtreeIds(flow([a, b]), "a").sort()).toEqual(["a", "b"]);
  });
});

describe("canReparent", () => {
  // root → mid → leaf, plus a governed write with its sealed topic.
  const build = () => {
    const root = block({ id: "root" });
    const mid = block({ id: "mid", parentId: "root" });
    const leaf = block({ id: "leaf", parentId: "mid" });
    const kkc = block({ id: "kkc", adapter: "kafka_kc", mode: undefined, parentId: "root" });
    const sealed = topic({ id: "t-sealed", sealed: true, writerBlockId: "kkc" });
    const open = topic({ id: "t-open", kind: "adopted", name: "partner.feed" });
    return flow([root, mid, leaf, kkc], [sealed, open]);
  };

  it("refuses the root, self-parenting, and anything inside the block's own subtree", () => {
    const f = build();
    expect(canReparent(f, "root", "mid")).toMatch(/root block cannot be re-parented/);
    expect(canReparent(f, "mid", "mid")).toMatch(/cannot be its own parent/);
    expect(canReparent(f, "mid", "leaf")).toMatch(/loop/);
  });

  it("allows a legal sideways move and treats a no-op as legal", () => {
    const f = build();
    expect(canReparent(f, "leaf", "root")).toBeNull();
    expect(canReparent(f, "leaf", "mid")).toBeNull();
  });

  it("applies terminal, topic and kc rules", () => {
    const f = build();
    expect(canReparent(f, "leaf", "kkc")).toMatch(/terminal/);
    expect(canReparent(f, "leaf", "t-open")).toMatch(/R5/);
    expect(canReparent(f, "leaf", "t-sealed")).toMatch(/Sealed/);
    const withKc = flow(
      [...build().blocks, block({ id: "kc1", adapter: "kc", mode: undefined, parentId: "t-open", config: { attachTopicId: "t-open" } })],
      build().topics,
    );
    expect(canReparent(withKc, "kc1", "mid")).toMatch(/never to a block/);
    expect(canReparent(withKc, "kc1", "t-sealed")).toMatch(/Sealed/);
  });

  it("quarantines raw branches, including the children that would travel along (R8)", () => {
    const rawRead = block({ id: "raw", adapter: "kafka", mode: "read", config: { parseFormat: "raw" } });
    const root = block({ id: "root" });
    const httpChild = block({ id: "h", parentId: "root" });
    const kafkaWrite = block({ id: "kw", adapter: "kafka", mode: "write", parentId: "root" });
    const enricher = block({ id: "e", mode: "lookup", parentId: "kw" });
    const f = flow([root, rawRead, httpChild, kafkaWrite, enricher]);
    expect(canReparent(f, "h", "raw")).toMatch(/R8/);
    // A lone byte-preserving write is fine…
    const f2 = flow([root, rawRead, block({ id: "kw2", adapter: "kafka", mode: "write", parentId: "root" })]);
    expect(canReparent(f2, "kw2", "raw")).toBeNull();
    // …but not once it carries a non-kafka child with it.
    expect(canReparent(f, "kw", "raw")).toMatch(/R8/);
  });
});

describe("triggers (R1)", () => {
  it("http/jdbc roots are scheduled; kafka roots are continuous", () => {
    expect(flowHasTrigger(flow([block({})]))).toBe(true);
    expect(flowHasTrigger(flow([block({ adapter: "jdbc" })]))).toBe(true);
    expect(flowHasTrigger(flow([block({ adapter: "kafka", config: { parseFormat: "json" } })]))).toBe(false);
  });

  it("finds the root through an adopted topic", () => {
    const t = topic({ id: "adopt", kind: "adopted", name: "partner.feed" });
    const read = block({ id: "r", adapter: "kafka", parentId: "adopt" });
    expect(rootBlock(flow([read], [t]))?.id).toBe("r");
  });
});
