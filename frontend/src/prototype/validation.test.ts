import { describe, expect, it } from "vitest";

import { gatewayRefusals } from "@/prototype/validation";

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
