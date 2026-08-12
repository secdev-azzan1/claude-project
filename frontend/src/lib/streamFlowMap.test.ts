import { describe, expect, it } from "vitest";

import { buildStreamGraph } from "@/lib/streamGraph";
import { streamsToFlowGraph } from "@/lib/streamFlowMap";

type TestStream = {
  id: string;
  name: string;
  endpointPath: string;
  entity?: unknown | null;
  parentStreamId?: string;
  routingRules?: Array<{ id: string; destination: string; targetStreamId?: string }>;
};

const run = (streams: TestStream[]) => streamsToFlowGraph(streams, buildStreamGraph(streams));

describe("streamsToFlowGraph", () => {
  it("returns nothing for an empty stream list", () => {
    expect(run([])).toEqual({ nodes: [], edges: [] });
  });

  it("marks a parentless stream as a root with no edges", () => {
    const { nodes, edges } = run([{ id: "a", name: "list_items", endpointPath: "/api/items" }]);
    expect(nodes).toHaveLength(1);
    expect(edges).toHaveLength(0);
    expect(nodes[0].data.isRoot).toBe(true);
    expect(nodes[0].data.isEntity).toBe(false);
    expect(typeof nodes[0].position.x).toBe("number");
    expect(typeof nodes[0].position.y).toBe("number");
  });

  it("emits a blue fanout edge and a branch child", () => {
    const { nodes, edges } = run([
      { id: "a", name: "list", endpointPath: "/a" },
      { id: "b", name: "child", endpointPath: "/b", parentStreamId: "a" },
    ]);
    expect(edges).toEqual([{ id: "a->b", source: "a", target: "b", kind: "fanout" }]);
    const child = nodes.find((n) => n.id === "b")!;
    expect(child.data.isBranchChild).toBe(true);
    expect(child.data.isRouteChild).toBe(false);
    const parent = nodes.find((n) => n.id === "a")!;
    expect(parent.data.childCount).toBe(1);
    expect(parent.data.hasParallelChildren).toBe(true);
  });

  it("emits a route edge and a route child from a routing rule", () => {
    const { nodes, edges } = run([
      { id: "a", name: "list", endpointPath: "/a", routingRules: [{ id: "r1", destination: "ROUTE_TO_STREAM", targetStreamId: "b" }] },
      { id: "b", name: "put", endpointPath: "/b" },
    ]);
    expect(edges[0].kind).toBe("route");
    const child = nodes.find((n) => n.id === "b")!;
    expect(child.data.isRouteChild).toBe(true);
    expect(nodes.find((n) => n.id === "a")!.data.hasRouteChildren).toBe(true);
  });

  it("marks entity streams", () => {
    const { nodes } = run([{ id: "a", name: "leaf", endpointPath: "/a", entity: { name: "authors" } }]);
    expect(nodes[0].data.isEntity).toBe(true);
  });

  it("lays out a forest of roots at distinct vertical positions", () => {
    const { nodes } = run([
      { id: "a", name: "root_a", endpointPath: "/a" },
      { id: "b", name: "root_b", endpointPath: "/b" },
    ]);
    expect(nodes).toHaveLength(2);
    expect(nodes.every((n) => n.data.isRoot)).toBe(true);
    expect(nodes[0].position.y).not.toBe(nodes[1].position.y);
  });
});
