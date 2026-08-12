import { describe, expect, it } from "vitest";

import { buildStreamGraph } from "@/lib/streamGraph";

describe("buildStreamGraph", () => {
  it("creates a fanout edge from a stream's parentStreamId", () => {
    const graph = buildStreamGraph([
      { id: "a" },
      { id: "b", parentStreamId: "a" },
    ]);
    expect(graph.edges).toEqual([{ parentId: "a", childId: "b", kind: "fanout" }]);
    expect(graph.childrenByParent.get("a")?.[0].childId).toBe("b");
    expect(graph.incomingByChild.get("b")?.[0].kind).toBe("fanout");
  });

  it("creates a route edge from a ROUTE_TO_STREAM routing rule", () => {
    const graph = buildStreamGraph([
      { id: "a", routingRules: [{ id: "r1", destination: "ROUTE_TO_STREAM", targetStreamId: "b" }] },
      { id: "b" },
    ]);
    expect(graph.edges).toContainEqual({ parentId: "a", childId: "b", kind: "route", routeId: "r1" });
  });

  it("creates a default_route edge and drops edges to missing streams", () => {
    const graph = buildStreamGraph([
      { id: "a", defaultRouteAction: "ROUTE_TO_STREAM", defaultRouteTargetStreamId: "missing" },
    ]);
    // edge is created in edges[] but not indexed because the target stream is absent
    expect(graph.edges).toContainEqual({ parentId: "a", childId: "missing", kind: "default_route", routeId: "default" });
    expect(graph.childrenByParent.get("a")).toBeUndefined();
  });
});
