import { describe, expect, it } from "vitest";
import { migrateBranches } from "./migrate";
import type { Flow, FlowBlock, PrototypeState } from "./types";

const block = (over: Partial<FlowBlock> & { id: string }): FlowBlock => ({
  adapter: "http",
  mode: "read",
  name: over.id,
  parentId: null,
  serviceId: null,
  entity: null,
  config: {},
  transforms: [],
  ...over,
});

const flow = (blocks: FlowBlock[]): Flow => ({
  id: "f1",
  name: "Legacy Flow",
  state: "Draft",
  enabled: false,
  cron: null,
  blocks,
  topics: [],
  variables: [],
  servicePins: {},
  createdAt: "",
  updatedAt: "",
});

const state = (f: Flow): PrototypeState => ({ flows: [f] } as unknown as PrototypeState);

/** The old shapes, typed loosely because they no longer exist in the model. */
const legacy = (value: unknown) => value as never;

describe("migrateBranches", () => {
  it("the one-condition shape becomes a one-rule list", () => {
    const f = flow([
      block({ id: "root" }),
      block({ id: "a", parentId: "root", branch: legacy({ name: "prod", condition: { field: "env", op: "equals", value: "prod" } }) }),
    ]);
    migrateBranches(state(f));
    expect(f.blocks[1].branch).toEqual({ name: "prod", rules: [{ field: "env", op: "equals", value: "prod" }] });
  });

  it("a fork becomes a branch that takes every record", () => {
    const f = flow([
      block({ id: "root" }),
      block({ id: "a", parentId: "root", branch: legacy({ kind: "fork", name: "mirror" }) }),
    ]);
    migrateBranches(state(f));
    expect(f.blocks[1].branch).toEqual({ name: "mirror" });
  });

  it("a parent's routing rule becomes the branch's own condition", () => {
    const f = flow([
      block({
        id: "root",
        transforms: [
          legacy({
            id: "t1",
            kind: "route",
            config: {
              rules: [{ id: "r1", name: "critical", field: "severity", op: "equals", value: "HIGH", action: "route" }],
              defaultAction: "forward",
            },
          }),
        ],
      }),
      block({ id: "a", parentId: "root", branch: legacy({ kind: "route", name: "critical", ruleId: "r1" }) }),
    ]);
    migrateBranches(state(f));

    expect(f.blocks[1].branch).toEqual({
      name: "critical",
      rules: [{ field: "severity", op: "equals", value: "HIGH" }],
    });
    // The rule set is gone: a condition belongs to the branch now.
    expect(f.blocks[0].transforms).toHaveLength(0);
  });

  it("a whitelist filter on the child becomes the condition of the branch into it", () => {
    const f = flow([
      block({ id: "root" }),
      block({
        id: "a",
        parentId: "root",
        branch: legacy({ kind: "fork", name: "active" }),
        transforms: [
          legacy({
            id: "t1",
            kind: "route",
            config: {
              rules: [{ id: "r1", name: "keep", field: "install_status", op: "equals", value: "in_use", action: "forward" }],
              defaultAction: "drop",
            },
          }),
        ],
      }),
    ]);
    migrateBranches(state(f));

    expect(f.blocks[1].branch).toEqual({
      name: "active",
      rules: [{ field: "install_status", op: "equals", value: "in_use" }],
    });
    expect(f.blocks[1].transforms).toHaveLength(0);
  });

  it("a blacklist filter is inverted rather than dropped", () => {
    const f = flow([
      block({ id: "root" }),
      block({
        id: "a",
        parentId: "root",
        transforms: [
          legacy({
            id: "t1",
            kind: "route",
            config: {
              rules: [{ id: "r1", name: "no-tests", field: "is_test", op: "equals", value: "true", action: "drop" }],
              defaultAction: "forward",
            },
          }),
        ],
      }),
    ]);
    migrateBranches(state(f));

    expect(f.blocks[1].branch?.rules).toEqual([{ field: "is_test", op: "not_equals", value: "true" }]);
  });

  it("reports what could not be carried over instead of losing it silently", () => {
    const f = flow([
      block({ id: "root" }),
      block({
        id: "a",
        parentId: "root",
        transforms: [
          legacy({
            id: "t1",
            kind: "route",
            config: {
              rules: [
                { id: "r1", name: "eu", field: "region", op: "equals", value: "EU", action: "route" },
                { id: "r2", name: "us", field: "region", op: "equals", value: "US", action: "route" },
              ],
              defaultAction: "forward",
            },
          }),
        ],
      }),
    ]);
    const notice = migrateBranches(state(f));
    expect(notice).toMatch(/could not be carried over/);
  });

  it("is a no-op on a flow that is already branch-shaped", () => {
    const f = flow([
      block({ id: "root" }),
      block({ id: "a", parentId: "root", branch: { name: "prod", rules: [{ field: "env", op: "equals", value: "prod" }] } }),
    ]);
    expect(migrateBranches(state(f))).toBeNull();
    expect(f.blocks[1].branch).toEqual({ name: "prod", rules: [{ field: "env", op: "equals", value: "prod" }] });
  });
});
