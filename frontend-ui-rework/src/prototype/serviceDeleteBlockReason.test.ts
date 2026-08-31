import { describe, expect, it } from "vitest";
import { serviceDeleteBlockReason, serviceDeleteImpact } from "./api";
import type { AppService, Flow, FlowBlock, FlowState, PrototypeState } from "./types";

const SVC_ID = "svc-1";

const svc = (over: Partial<AppService> = {}): AppService =>
  ({ id: SVC_ID, name: "Partner Kafka", retired: true, revision: 1, ...over }) as unknown as AppService;

const usingBlock = (over: Partial<FlowBlock> = {}): FlowBlock =>
  ({ id: "b1", serviceId: SVC_ID, config: {}, ...over }) as unknown as FlowBlock;

const flow = (
  name: string,
  state: FlowState,
  opts: { deployedAt?: string | null; blocks?: FlowBlock[] } = {},
): Flow =>
  ({
    id: `f-${name}`,
    name,
    state,
    deployedAt: opts.deployedAt ?? null,
    blocks: opts.blocks ?? [usingBlock()],
  }) as unknown as Flow;

const state = (flows: Flow[]): PrototypeState => ({ flows } as unknown as PrototypeState);

describe("serviceDeleteBlockReason", () => {
  // Retirement is now the ONLY gate. Linked flows do not block the delete —
  // they carry the consequence instead (drift warning if deployed, existing
  // "service no longer exists" validation error if not).

  it("refuses an active service and points at retirement first", () => {
    expect(serviceDeleteBlockReason(svc({ retired: false }))).toContain(
      "Retire the service before deleting it",
    );
  });

  it("allows a retired service with no dependent flows", () => {
    expect(serviceDeleteBlockReason(svc())).toBeNull();
  });

  it("allows a retired service even when an undeployed flow still uses it", () => {
    expect(serviceDeleteBlockReason(svc())).toBeNull();
  });

  it("allows a retired service even when a DEPLOYED flow still uses it", () => {
    // This is the behaviour change: previously a 409, now permitted.
    expect(serviceDeleteBlockReason(svc())).toBeNull();
  });

  it("still refuses an active service regardless of how many flows use it", () => {
    expect(serviceDeleteBlockReason(svc({ retired: false }))).not.toBeNull();
  });
});

describe("serviceDeleteImpact", () => {
  it("reports nothing when no flow uses the service", () => {
    expect(serviceDeleteImpact(svc(), state([]))).toEqual({ deployed: [], undeployed: [] });
  });

  it("separates deployed from undeployed dependents", () => {
    const live = flow("Vuln Ingest", "Stopped", { deployedAt: "2026-08-13T11:36:54.142Z" });
    const draft = flow("Draft Ingest", "Draft");
    const impact = serviceDeleteImpact(svc(), state([live, draft]));
    expect(impact.deployed.map((f) => f.name)).toEqual(["Vuln Ingest"]);
    expect(impact.undeployed.map((f) => f.name)).toEqual(["Draft Ingest"]);
  });

  it("classifies by deployedAt, not by run state", () => {
    // A Stopped flow that still has a process group counts as deployed —
    // that is exactly the gw-via-gateway case.
    const stoppedButDeployed = flow("gw via gateway", "Stopped", { deployedAt: "2026-08-13T11:36:54.142Z" });
    const impact = serviceDeleteImpact(svc(), state([stoppedButDeployed]));
    expect(impact.deployed).toHaveLength(1);
    expect(impact.undeployed).toHaveLength(0);
  });

  it("counts a sink-only reference via config.sinkServiceId", () => {
    const sink = flow("Asset Sink", "Stopped", {
      deployedAt: "x",
      blocks: [usingBlock({ serviceId: null, config: { sinkServiceId: SVC_ID } } as Partial<FlowBlock>)],
    });
    expect(serviceDeleteImpact(svc(), state([sink])).deployed.map((f) => f.name)).toEqual(["Asset Sink"]);
  });

  it("ignores flows that reference a different service", () => {
    const other = flow("Unrelated", "Running", {
      deployedAt: "x",
      blocks: [usingBlock({ serviceId: "svc-other" })],
    });
    expect(serviceDeleteImpact(svc(), state([other]))).toEqual({ deployed: [], undeployed: [] });
  });

  it("handles a mixed set of many flows", () => {
    const flows = [
      flow("d1", "Stopped", { deployedAt: "x" }),
      flow("d2", "Running", { deployedAt: "x" }),
      flow("u1", "Draft"),
      flow("u2", "Draft"),
      flow("u3", "Draft"),
    ];
    const impact = serviceDeleteImpact(svc(), state(flows));
    expect(impact.deployed).toHaveLength(2);
    expect(impact.undeployed).toHaveLength(3);
  });
});
