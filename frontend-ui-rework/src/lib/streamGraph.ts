export type EdgeKind = "fanout" | "route" | "default_route";

export interface StreamGraphEdge {
  parentId: string;
  childId: string;
  kind: EdgeKind;
  routeId?: string;
}

/** Minimal fields buildStreamGraph reads from a stream. The full Stream type satisfies this. */
export interface StreamGraphInput {
  id: string;
  parentStreamId?: string;
  routingRules?: Array<{ id: string; destination: string; targetStreamId?: string }>;
  defaultRouteAction?: string;
  defaultRouteTargetStreamId?: string;
}

/**
 * The edge-lookup maps produced by buildStreamGraph. Consumers that only need
 * relationships (not full stream records) accept THIS type to avoid Map<K,V>
 * invariance friction when a caller passes a graph whose `byId` is Map<string, Stream>.
 */
export interface StreamGraphEdges {
  childrenByParent: Map<string, StreamGraphEdge[]>;
  incomingByChild: Map<string, StreamGraphEdge[]>;
}

export function buildStreamGraph<T extends StreamGraphInput>(
  streams: T[],
): {
  byId: Map<string, T>;
  edges: StreamGraphEdge[];
  childrenByParent: Map<string, StreamGraphEdge[]>;
  incomingByChild: Map<string, StreamGraphEdge[]>;
} {
  const byId = new Map(streams.map((stream) => [stream.id, stream] as const));
  const edges: StreamGraphEdge[] = [];

  for (const stream of streams) {
    if (stream.parentStreamId) {
      edges.push({ parentId: stream.parentStreamId, childId: stream.id, kind: "fanout" });
    }
    for (const rule of stream.routingRules || []) {
      if (rule.destination !== "ROUTE_TO_STREAM") continue;
      if (!rule.targetStreamId) continue;
      edges.push({ parentId: stream.id, childId: rule.targetStreamId, kind: "route", routeId: rule.id });
    }
    if (stream.defaultRouteAction === "ROUTE_TO_STREAM" && stream.defaultRouteTargetStreamId) {
      edges.push({
        parentId: stream.id,
        childId: stream.defaultRouteTargetStreamId,
        kind: "default_route",
        routeId: "default",
      });
    }
  }

  const childrenByParent = new Map<string, StreamGraphEdge[]>();
  const incomingByChild = new Map<string, StreamGraphEdge[]>();
  for (const edge of edges) {
    if (!byId.has(edge.parentId) || !byId.has(edge.childId)) continue;
    childrenByParent.set(edge.parentId, [...(childrenByParent.get(edge.parentId) || []), edge]);
    incomingByChild.set(edge.childId, [...(incomingByChild.get(edge.childId) || []), edge]);
  }

  return { byId, edges, childrenByParent, incomingByChild };
}
