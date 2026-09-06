import { describe, expect, it } from "vitest";
import { kafkaConnectSyncDeleteImpact, sinkVerbsAvailable } from "./kafkaConnectSyncUi";
import type { Flow } from "./types";

describe("sinkVerbsAvailable", () => {
  it("offers only Start for a connector that does not exist on the cluster yet", () => {
    expect(sinkVerbsAvailable("UNDEPLOYED")).toEqual(["start"]);
  });

  it("offers Pause, Stop and Restart while running", () => {
    expect(sinkVerbsAvailable("RUNNING")).toEqual(["pause", "stop", "restart"]);
  });

  it("offers Resume, Stop and Restart while paused", () => {
    expect(sinkVerbsAvailable("PAUSED")).toEqual(["resume", "stop", "restart"]);
  });

  it("offers Start and Restart once stopped", () => {
    expect(sinkVerbsAvailable("STOPPED")).toEqual(["start", "restart"]);
  });

  it.each(["FAILED", "UNASSIGNED", "RESTARTING"] as const)(
    "offers Stop and Restart in the %s state",
    (state) => {
      expect(sinkVerbsAvailable(state)).toEqual(["stop", "restart"]);
    },
  );

  it("offers nothing when the cluster could not be reached (state is null)", () => {
    // A null state means "unreachable, not UNDEPLOYED" -- offering Start here
    // could create a duplicate of a connector that already exists but just
    // couldn't be reached this poll.
    expect(sinkVerbsAvailable(null)).toEqual([]);
  });
});

describe("kafkaConnectSyncDeleteImpact", () => {
  it("separates deployed and undeployed flow dependents for deletion", () => {
    const impact = kafkaConnectSyncDeleteImpact(
      { id: "sync-1" },
      [
        { id: "flow-draft", name: "Draft flow", deployedAt: null, blocks: [{ config: { syncId: "sync-1" } }] },
        { id: "flow-live", name: "Live flow", deployedAt: "2026-08-30T00:00:00.000Z", blocks: [{ config: { syncId: "sync-1" } }] },
        { id: "flow-other", name: "Other flow", deployedAt: null, blocks: [] },
      ] as unknown as Flow[],
    );
    expect(impact.deployed.map((flow) => flow.id)).toEqual(["flow-live"]);
    expect(impact.undeployed.map((flow) => flow.id)).toEqual(["flow-draft"]);
  });
});
