import { hierarchy, tree } from "d3-hierarchy";

import type { EdgeKind, StreamGraphEdges } from "@/lib/streamGraph";

export const EDGE_COLORS: Record<EdgeKind, string> = {
  fanout: "#3b82f6", // blue-500 (parallel branch)
  route: "#a855f7", // purple-500 (conditional route)
  default_route: "#a855f7", // purple-500
};

/** Minimal stream fields the map needs. The full Stream type satisfies this structurally. */
export interface StreamLike {
  id: string;
  name: string;
  endpointPath: string;
  entity?: unknown | null;
}

export interface FlowMapNodeData {
  name: string;
  endpointPath: string;
  isEntity: boolean;
  isRoot: boolean;
  isRouteChild: boolean;
  isBranchChild: boolean;
  hasRouteChildren: boolean;
  hasParallelChildren: boolean;
  childCount: number;
}

export interface FlowMapNode {
  id: string;
  position: { x: number; y: number };
  data: FlowMapNodeData;
}

export interface FlowMapEdge {
  id: string;
  source: string;
  target: string;
  kind: EdgeKind;
}

const NODE_V_GAP = 96; // vertical spacing between sibling nodes
const NODE_H_GAP = 300; // horizontal spacing between tree depths

type Datum = { streamId: string | null; edgeKind: EdgeKind | null; children: Datum[] };

export function streamsToFlowGraph(
  streams: StreamLike[],
  graph: StreamGraphEdges,
): { nodes: FlowMapNode[]; edges: FlowMapEdge[] } {
  const byId = new Map(streams.map((s) => [s.id, s] as const));
  const hasIncoming = (id: string) => (graph.incomingByChild.get(id) || []).length > 0;
  const visited = new Set<string>();

  const buildDatum = (id: string, edgeKind: EdgeKind | null): Datum => {
    visited.add(id);
    const children = (graph.childrenByParent.get(id) || [])
      .filter((e) => byId.has(e.childId) && !visited.has(e.childId))
      .map((e) => buildDatum(e.childId, e.kind));
    return { streamId: id, edgeKind, children };
  };

  const rootDatums: Datum[] = [];
  for (const s of streams) {
    if (!hasIncoming(s.id) && !visited.has(s.id)) rootDatums.push(buildDatum(s.id, null));
  }
  // Orphan / unreachable streams (had an incoming edge but no laid-out parent) become roots too.
  for (const s of streams) {
    if (!visited.has(s.id)) rootDatums.push(buildDatum(s.id, null));
  }

  if (rootDatums.length === 0) return { nodes: [], edges: [] };

  const virtualRoot: Datum = { streamId: null, edgeKind: null, children: rootDatums };
  const root = hierarchy<Datum>(virtualRoot, (d) => d.children);
  tree<Datum>().nodeSize([NODE_V_GAP, NODE_H_GAP])(root);

  const nodes: FlowMapNode[] = [];
  const edges: FlowMapEdge[] = [];

  for (const point of root.descendants()) {
    const datum = point.data;
    if (datum.streamId === null) continue; // skip the virtual root
    const id = datum.streamId;
    const childEdges = (graph.childrenByParent.get(id) || []).filter((e) => byId.has(e.childId));

    nodes.push({
      id,
      // d3 lays out top-to-bottom (x across, y depth); swap axes for left-to-right
      position: { x: point.y, y: point.x },
      data: {
        name: byId.get(id)?.name ?? id,
        endpointPath: byId.get(id)?.endpointPath ?? "",
        isEntity: Boolean(byId.get(id)?.entity),
        isRoot: datum.edgeKind === null,
        isRouteChild: datum.edgeKind === "route" || datum.edgeKind === "default_route",
        isBranchChild: datum.edgeKind === "fanout",
        hasRouteChildren: childEdges.some((e) => e.kind === "route" || e.kind === "default_route"),
        hasParallelChildren: childEdges.some((e) => e.kind === "fanout"),
        childCount: childEdges.length,
      },
    });

    const parent = point.parent;
    if (parent && parent.data.streamId !== null) {
      edges.push({
        id: `${parent.data.streamId}->${id}`,
        source: parent.data.streamId,
        target: id,
        kind: datum.edgeKind ?? "fanout",
      });
    }
  }

  return { nodes, edges };
}
