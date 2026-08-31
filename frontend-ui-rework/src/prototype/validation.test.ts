import { describe, expect, it } from "vitest";

import { gatewayRefusals, validateBlock } from "@/prototype/validation";

describe("gatewayRefusals", () => {
  it("uses the Proxies page wording when a referenced proxy is missing", () => {
    const issues = gatewayRefusals(
      { adapter: "http", config: { proxyId: "proxy-1" } } as any,
      { proxies: [], allowlist: [] },
    );

    expect(issues).toEqual([
      "The APISIX proxy this block routes through (proxy-1) no longer exists — pick one on the Proxies page.",
    ]);
  });
});

describe("Trino JDBC table validation", () => {
  const flow = { blocks: [], topics: [] } as any;
  const block = {
    id: "jdbc-read",
    name: "Read assets",
    adapter: "jdbc",
    mode: "read",
    serviceId: "trino",
    config: { table: "asset_groups" },
    transforms: [],
  } as any;
  const services = [{ id: "trino", name: "Trino", retired: false, config: { dialect: "trino" } }] as any;

  it("requires a catalog.schema.table reference", () => {
    expect(validateBlock(flow, block, services, []).map((issue) => issue.message)).toContain(
      "Trino table must be written as catalog.schema.table using simple identifiers.",
    );
  });

  it("accepts a fully-qualified table reference", () => {
    block.config.table = "gold.cmdb.asset_groups";
    expect(validateBlock(flow, block, services, []).map((issue) => issue.message)).not.toContain(
      "Trino table must be written as catalog.schema.table using simple identifiers.",
    );
  });
});

describe("incremental JDBC validation", () => {
  const services = [{ id: "db", name: "Database", retired: false, config: { dialect: "postgresql" } }] as any;

  it("requires a watermark column", () => {
    const block = { id: "jdbc-read", name: "Read", adapter: "jdbc", mode: "read", serviceId: "db", config: { table: "assets", incremental: true }, transforms: [] } as any;
    expect(validateBlock({ blocks: [], topics: [] } as any, block, services, []).map((issue) => issue.message)).toContain(
      "Incremental reads require a watermark column.",
    );
  });

  it("accepts a watermark and tie-breaker", () => {
    const block = {
      id: "jdbc-read", name: "Read", adapter: "jdbc", mode: "read", serviceId: "db",
      config: { table: "assets", incremental: true, watermarkColumn: "updated_at", bookmarkTieBreaker: "id" }, transforms: [],
    } as any;
    const messages = validateBlock({ blocks: [], topics: [] } as any, block, services, []).map((issue) => issue.message);
    expect(messages).not.toContain("Incremental reads require a watermark column.");
  });
});
