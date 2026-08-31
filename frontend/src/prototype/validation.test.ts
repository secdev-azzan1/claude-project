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
