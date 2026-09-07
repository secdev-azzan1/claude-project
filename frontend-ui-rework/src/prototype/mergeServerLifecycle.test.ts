import { describe, expect, it } from "vitest";

import { getVerbBlockReason, mergeServerLifecycle } from "@/prototype/api";
import type { PrototypeState } from "@/prototype/api";
import type { Flow } from "@/prototype/types";

/** Minimal snapshot: the stop/start reasons below only read flow fields. */
const EMPTY_STATE = {
  services: [],
  schemas: [],
  connections: [],
  flows: [],
  gatewayProxies: [],
} as unknown as PrototypeState;

function makeFlow(over: Partial<Flow> = {}): Flow {
  return {
    id: "f1",
    name: "My Flow",
    description: "",
    state: "Stopped",
    enabled: true,
    cron: "0 * * * *",
    blocks: [],
    topics: [],
    variables: [],
    servicePins: {},
    drift: null,
    deployedAt: "2026-09-01T00:00:00.000Z",
    lastRunAt: null,
    createdAt: "2026-09-01T00:00:00.000Z",
    updatedAt: "2026-09-01T00:00:00.000Z",
    ...over,
  } as Flow;
}

describe("mergeServerLifecycle", () => {
  it("brings the server's lifecycle state onto the draft", () => {
    // The reported bug: Start succeeds, the server says Running, but the
    // builder's buttons keep reading a draft frozen at Stopped.
    const draft = makeFlow({ state: "Stopped" });
    const server = makeFlow({ state: "Running", lastRunAt: "2026-09-07T10:00:00.000Z" });

    const merged = mergeServerLifecycle(draft, server);

    expect(merged.state).toBe("Running");
    expect(merged.lastRunAt).toBe("2026-09-07T10:00:00.000Z");
  });

  it("merges every server-owned field", () => {
    const draft = makeFlow({ state: "Draft", enabled: true, deployedAt: null, drift: null });
    const server = makeFlow({
      state: "Degraded",
      enabled: false,
      deployedAt: "2026-09-07T09:00:00.000Z",
      drift: "Process group missing on Production NiFi",
    });

    const merged = mergeServerLifecycle(draft, server);

    expect(merged.enabled).toBe(false);
    expect(merged.deployedAt).toBe("2026-09-07T09:00:00.000Z");
    expect(merged.drift).toBe("Process group missing on Production NiFi");
  });

  it("never touches fields the builder edits, so unsaved work survives a poll", () => {
    const draft = makeFlow({
      name: "Half-typed rename",
      description: "edited",
      cron: "*/5 * * * *",
      blocks: [{ id: "b1" }] as unknown as Flow["blocks"],
      state: "Stopped",
    });
    const server = makeFlow({
      name: "Old Server Name",
      description: "",
      cron: "0 * * * *",
      blocks: [],
      state: "Running",
    });

    const merged = mergeServerLifecycle(draft, server);

    expect(merged.state).toBe("Running"); // lifecycle still updates
    expect(merged.name).toBe("Half-typed rename");
    expect(merged.description).toBe("edited");
    expect(merged.cron).toBe("*/5 * * * *");
    expect(merged.blocks).toHaveLength(1);
  });

  it("returns the same reference when nothing changed, so idle polls do not re-render", () => {
    const draft = makeFlow();
    const server = makeFlow({ name: "A different name the draft should ignore" });

    expect(mergeServerLifecycle(draft, server)).toBe(draft);
  });

  it("unblocks Stop once the server says Running -- the reported bug", () => {
    // Before the fix the draft stayed at Stopped after a successful Start, so
    // Stop reported "The flow is not running." forever and stayed disabled.
    const draft = makeFlow({ state: "Stopped" });
    const server = makeFlow({ state: "Running" });

    expect(getVerbBlockReason(draft, "stop", EMPTY_STATE)).toBe("The flow is not running.");

    const merged = mergeServerLifecycle(draft, server);

    expect(getVerbBlockReason(merged, "stop", EMPTY_STATE)).toBeNull();
    expect(getVerbBlockReason(merged, "start", EMPTY_STATE)).toBe("Already running.");
  });

  it("does not mutate the draft it was given", () => {
    const draft = makeFlow({ state: "Stopped" });
    const server = makeFlow({ state: "Running" });

    mergeServerLifecycle(draft, server);

    expect(draft.state).toBe("Stopped");
  });
});
