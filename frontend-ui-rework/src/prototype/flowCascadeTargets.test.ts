import { describe, expect, it } from "vitest";
import { bulkJobPercent, flowCascadeTargets, isBulkJobTerminal } from "./api";
import type {
  AppService,
  ApprovedSchema,
  Flow,
  FlowBlock,
  GatewayProxy,
  PrototypeState,
} from "./types";
import type { BulkJob, KafkaConnectSync } from "./api";

const block = (over: Partial<FlowBlock> = {}): FlowBlock =>
  ({ id: "b1", adapter: "http", name: "Read", parentId: null, serviceId: null, config: {}, transforms: [], ...over }) as unknown as FlowBlock;

const flow = (over: Partial<Flow> = {}): Flow =>
  ({ id: "f1", name: "gw via gateway", blocks: [], topics: [], deployedAt: null, ...over }) as unknown as Flow;

const service = (over: Partial<AppService> = {}): AppService =>
  ({ id: "svc-1", name: "gw proxied dummyjson", type: "http", config: {}, ...over }) as unknown as AppService;

const schema = (over: Partial<ApprovedSchema> = {}): ApprovedSchema =>
  ({ id: "sch-1", subject: "raw.gw.user-value", flowId: "f1", blockId: "b1", ...over }) as unknown as ApprovedSchema;

const proxy = (over: Partial<GatewayProxy> = {}): GatewayProxy =>
  ({ id: "px-1", name: "gw-proxy", targetHost: "example.com", port: 443, ...over }) as unknown as GatewayProxy;

const sync = (over: Partial<KafkaConnectSync> = {}): KafkaConnectSync =>
  ({
    id: "sync-1",
    name: "bronze.flow.entity",
    retired: false,
    ...over,
  }) as unknown as KafkaConnectSync;

const state = (over: Partial<PrototypeState> = {}): PrototypeState =>
  ({ flows: [], services: [], schemas: [], gatewayProxies: [], ...over }) as unknown as PrototypeState;

describe("flowCascadeTargets", () => {
  it("returns nothing for a flow with no associations", () => {
    const f = flow();
    expect(flowCascadeTargets(f, state({ flows: [f] }))).toEqual([]);
  });

  it("finds a schema by its reverse flowId pointer", () => {
    const f = flow();
    const targets = flowCascadeTargets(f, state({ flows: [f], schemas: [schema()] }));
    expect(targets).toHaveLength(1);
    expect(targets[0]).toMatchObject({ kind: "schema", id: "sch-1", name: "raw.gw.user-value", sharedWith: [] });
  });

  it("ignores schemas belonging to a different flow", () => {
    const f = flow();
    const other = schema({ id: "sch-2", flowId: "f2" });
    expect(flowCascadeTargets(f, state({ flows: [f], schemas: [other] }))).toEqual([]);
  });

  it("finds a service referenced by block.serviceId", () => {
    const f = flow({ blocks: [block({ serviceId: "svc-1" })] });
    const targets = flowCascadeTargets(f, state({ flows: [f], services: [service()] }));
    expect(targets).toHaveLength(1);
    expect(targets[0]).toMatchObject({ kind: "service", id: "svc-1", sharedWith: [] });
  });

  it("finds a service referenced only via config.sinkServiceId", () => {
    const f = flow({ blocks: [block({ adapter: "kafka_kc", config: { sinkServiceId: "svc-1" } })] });
    const targets = flowCascadeTargets(f, state({ flows: [f], services: [service()] }));
    expect(targets.map((t) => t.id)).toEqual(["svc-1"]);
  });

  it("marks a service shared with another flow, naming that flow", () => {
    const mine = flow({ blocks: [block({ serviceId: "svc-1" })] });
    const other = flow({ id: "f2", name: "s1-agents", blocks: [block({ id: "b9", serviceId: "svc-1" })] });
    const targets = flowCascadeTargets(mine, state({ flows: [mine, other], services: [service()] }));
    expect(targets[0].sharedWith).toEqual(["s1-agents"]);
  });

  it("does not list the flow being deleted as sharing its own service", () => {
    const f = flow({ blocks: [block({ serviceId: "svc-1" })] });
    const targets = flowCascadeTargets(f, state({ flows: [f], services: [service()] }));
    expect(targets[0].sharedWith).toEqual([]);
  });

  it("finds a proxy through the service two-hop link", () => {
    const f = flow({ blocks: [block({ serviceId: "svc-1" })] });
    const svc = service({ config: { proxyId: "px-1" } });
    const targets = flowCascadeTargets(f, state({ flows: [f], services: [svc], gatewayProxies: [proxy()] }));
    expect(targets.map((t) => t.kind)).toEqual(["service", "proxy"]);
    expect(targets.find((t) => t.kind === "proxy")).toMatchObject({ id: "px-1", name: "gw-proxy" });
  });

  it("honours the legacy block.config.proxyId fallback", () => {
    const f = flow({ blocks: [block({ serviceId: "svc-1", config: { proxyId: "px-1" } })] });
    const targets = flowCascadeTargets(f, state({ flows: [f], services: [service()], gatewayProxies: [proxy()] }));
    expect(targets.some((t) => t.kind === "proxy" && t.id === "px-1")).toBe(true);
  });

  it("marks a proxy shared with another flow", () => {
    const svc = service({ config: { proxyId: "px-1" } });
    const mine = flow({ blocks: [block({ serviceId: "svc-1" })] });
    const other = flow({ id: "f2", name: "other flow", blocks: [block({ id: "b9", serviceId: "svc-1" })] });
    const targets = flowCascadeTargets(mine, state({ flows: [mine, other], services: [svc], gatewayProxies: [proxy()] }));
    expect(targets.find((t) => t.kind === "proxy")?.sharedWith).toEqual(["other flow"]);
  });

  it("deduplicates a service referenced by two blocks", () => {
    const f = flow({ blocks: [block({ serviceId: "svc-1" }), block({ id: "b2", serviceId: "svc-1" })] });
    const targets = flowCascadeTargets(f, state({ flows: [f], services: [service()] }));
    expect(targets.filter((t) => t.kind === "service")).toHaveLength(1);
  });

  it("skips references to resources that no longer exist", () => {
    const f = flow({ blocks: [block({ serviceId: "svc-gone" })] });
    expect(flowCascadeTargets(f, state({ flows: [f], services: [] }))).toEqual([]);
  });

  it("flags a still-active service as needing retirement first", () => {
    // The v2 delete endpoint refuses an active service with a 409, so the
    // cascade has to retire it before deleting.
    const f = flow({ blocks: [block({ serviceId: "svc-1" })] });
    const targets = flowCascadeTargets(f, state({ flows: [f], services: [service({ retired: false })] }));
    expect(targets[0].needsRetire).toBe(true);
  });

  it("does not re-retire a service that is already retired", () => {
    const f = flow({ blocks: [block({ serviceId: "svc-1" })] });
    const targets = flowCascadeTargets(f, state({ flows: [f], services: [service({ retired: true })] }));
    expect(targets[0].needsRetire).toBe(false);
  });

  it("finds a Kafka Connect sync referenced by a flow block", () => {
    const f = flow({ blocks: [block({ adapter: "kafka_kc", config: { syncId: "sync-1" } })] });
    const targets = flowCascadeTargets(f, state({ flows: [f] }), [sync()]);
    expect(targets).toEqual([
      expect.objectContaining({
        kind: "kafka_connect_sync",
        id: "sync-1",
        name: "bronze.flow.entity",
        sharedWith: [],
        needsRetire: true,
      }),
    ]);
  });

  it("protects a Kafka Connect sync shared by another flow", () => {
    const mine = flow({ blocks: [block({ adapter: "kafka_kc", config: { syncId: "sync-1" } })] });
    const other = flow({ id: "f2", name: "other flow", blocks: [block({ id: "b9", adapter: "kafka_kc", config: { syncId: "sync-1" } })] });
    const targets = flowCascadeTargets(mine, state({ flows: [mine, other] }), [sync()]);
    expect(targets.find((target) => target.kind === "kafka_connect_sync")?.sharedWith).toEqual(["other flow"]);
  });

  it("never sets needsRetire on schemas or proxies", () => {
    const svc = service({ config: { proxyId: "px-1" } });
    const f = flow({ blocks: [block({ serviceId: "svc-1" })] });
    const targets = flowCascadeTargets(
      f,
      state({ flows: [f], services: [svc], schemas: [schema()], gatewayProxies: [proxy()] }),
    );
    for (const t of targets.filter((x) => x.kind !== "service")) {
      expect(t.needsRetire).toBeUndefined();
    }
  });

  it("covers the real gw-via-gateway shape end to end", () => {
    // The exact deadlock the user hit: flow -> service, service used by
    // nothing else, so the service is offered and tickable.
    const f = flow({ deployedAt: "2026-08-13T11:36:54.142Z", blocks: [block({ serviceId: "svc-u30ous" })] });
    const svc = service({ id: "svc-u30ous", name: "gw proxied dummyjson" });
    const targets = flowCascadeTargets(f, state({ flows: [f], services: [svc] }));
    expect(targets).toHaveLength(1);
    expect(targets[0].sharedWith).toEqual([]);
  });
});

const job = (over: Partial<BulkJob> = {}): BulkJob =>
  ({ id: "bulk-1", verb: "deploy", status: "running", total: 20, completed: 0, succeeded: 0, failed: 0, items: [], ...over }) as unknown as BulkJob;

describe("bulkJobPercent", () => {
  it("is 0 when nothing has finished", () => {
    expect(bulkJobPercent(job())).toBe(0);
  });

  it("rounds to a whole percent", () => {
    expect(bulkJobPercent(job({ total: 20, completed: 7 }))).toBe(35);
    expect(bulkJobPercent(job({ total: 3, completed: 1 }))).toBe(33);
  });

  it("is 100 when every item is done", () => {
    expect(bulkJobPercent(job({ total: 20, completed: 20 }))).toBe(100);
  });

  it("counts failures as progress, not as stalls", () => {
    expect(bulkJobPercent(job({ total: 4, completed: 4, succeeded: 2, failed: 2 }))).toBe(100);
  });

  it("never divides by zero", () => {
    expect(bulkJobPercent(job({ total: 0, completed: 0 }))).toBe(0);
    expect(bulkJobPercent(null)).toBe(0);
  });
});

describe("isBulkJobTerminal", () => {
  it.each(["completed", "failed", "cancelled", "interrupted"] as const)("%s is terminal", (status) => {
    expect(isBulkJobTerminal(job({ status }))).toBe(true);
  });

  it.each(["pending", "running"] as const)("%s is not terminal", (status) => {
    expect(isBulkJobTerminal(job({ status }))).toBe(false);
  });

  it("treats a missing job as not terminal so polling is not stopped early", () => {
    expect(isBulkJobTerminal(null)).toBe(false);
    expect(isBulkJobTerminal(undefined)).toBe(false);
  });
});
