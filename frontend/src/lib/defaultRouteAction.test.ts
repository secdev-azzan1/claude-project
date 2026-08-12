import { describe, expect, it } from "vitest";

import { normalizeDefaultRouteAction } from "@/lib/defaultRouteAction";

describe("normalizeDefaultRouteAction", () => {
  it("folds legacy KEEP into DROP", () => {
    expect(normalizeDefaultRouteAction("keep", false)).toBe("DROP");
  });

  it("treats empty / unknown / drop as DROP", () => {
    expect(normalizeDefaultRouteAction("", false)).toBe("DROP");
    expect(normalizeDefaultRouteAction("drop", false)).toBe("DROP");
    expect(normalizeDefaultRouteAction("whatever", false)).toBe("DROP");
  });

  it("is ROUTE_TO_STREAM when explicitly routed", () => {
    expect(normalizeDefaultRouteAction("route_to_stream", false)).toBe("ROUTE_TO_STREAM");
  });

  it("infers ROUTE_TO_STREAM when a default child stream is present", () => {
    expect(normalizeDefaultRouteAction("drop", true)).toBe("ROUTE_TO_STREAM");
    expect(normalizeDefaultRouteAction("keep", true)).toBe("ROUTE_TO_STREAM");
  });
});
