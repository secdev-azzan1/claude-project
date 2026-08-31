import { describe, expect, it } from "vitest";
import { kafkaConnectSyncDeleteImpact, runtimeControlsAvailable, syncConfigurationLabel, syncPrimaryAction } from "./kafkaConnectSyncUi";
import type { Flow } from "./types";

const sync = (overrides: Partial<Parameters<typeof syncPrimaryAction>[0]> = {}) => ({
  remotePresent: true,
  configurationState: "synced" as const,
  retired: false,
  enabled: true,
  ...overrides,
});

describe("Kafka Connect sync configuration actions", () => {
  it("offers Create connector only for a draft without a remote connector", () => {
    const draft = sync({ remotePresent: false, configurationState: "draft" });
    expect(syncPrimaryAction(draft)).toBe("create");
    expect(syncConfigurationLabel(draft)).toBe("Draft — no connector");
    expect(runtimeControlsAvailable(draft)).toBe(false);
  });

  it("hides the configuration action when the saved definition is synced", () => {
    const synced = sync();
    expect(syncPrimaryAction(synced)).toBeNull();
    expect(syncConfigurationLabel(synced)).toBe("Synced");
    expect(runtimeControlsAvailable(synced)).toBe(true);
  });

  it("offers Apply changes only after a saved edit is pending", () => {
    const pending = sync({ configurationState: "changes_pending" });
    expect(syncPrimaryAction(pending)).toBe("apply");
    expect(syncConfigurationLabel(pending)).toBe("Changes pending");
    expect(runtimeControlsAvailable(pending)).toBe(false);
  });

  it("does not offer actions for retired syncs", () => {
    const retired = sync({ retired: true });
    expect(syncPrimaryAction(retired)).toBeNull();
    expect(runtimeControlsAvailable(retired)).toBe(false);
  });

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
