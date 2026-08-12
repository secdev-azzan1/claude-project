// Pure derivation of the compact flow visual: Flow → nodes with positions +
// styled edges. The layout is a simple left-to-right tree (forks/topics never
// merge, so the graph is a forest).
//
// Every walk in this module carries a visited set. `canReparent` is the real
// cycle guard and it runs before any write, but the graph is also rendered from
// flows that arrive from storage, and an unguarded recursion here is a hung tab
// rather than an error message. Defence in depth is cheap: the walks are O(n).

import { isTerminal } from "@/prototype/legality";
// The condition phrasing comes from the branches module so the map, the outline
// and the form say a branch's condition with ONE function. Duplicating it is how
// two surfaces start disagreeing about what a branch does.
import { describeBranch, isConditional } from "@/prototype/branches";
import type { Flow, FlowBlock, FlowTopic } from "@/prototype/types";

export type MapNodeKind = "block" | "topic";

export interface MapNode {
  id: string;
  kind: MapNodeKind;
  block?: FlowBlock;
  topic?: FlowTopic;
  x: number;
  y: number;
}

export type MapEdgeKind = "flow" | "branch" | "materialize" | "subscription";

export interface MapEdge {
  id: string;
  source: string;
  target: string;
  kind: MapEdgeKind;
  label?: string;
}

export const EDGE_STYLE: Record<MapEdgeKind, { stroke: string; dash?: string; width: number }> = {
  // One line for a chain, one for a named branch. There is no third colour,
  // because there is no third kind of edge: a condition is a property of a
  // branch, not a different species of one.
  flow: { stroke: "hsl(215 16% 57%)", width: 2 },
  branch: { stroke: "hsl(221 83% 53%)", width: 2 },
  materialize: { stroke: "hsl(215 16% 72%)", width: 1.5 },
  subscription: { stroke: "hsl(215 16% 57%)", dash: "6 4", width: 1.5 },
};

/** A materialize edge is derived from `topic.writerBlockId`; it is not a parent link and never re-parents. */
export function isDerivedEdgeKind(kind: MapEdgeKind): boolean {
  return kind === "materialize";
}

/**
 * Whether a block can have branches at all — the same predicate the form uses to
 * decide whether the Routing section exists, so the canvas and the form offer
 * the act on exactly the same blocks.
 */
export function canHostRouting(block: FlowBlock): boolean {
  return !isTerminal(block);
}

const X_GAP = 290;
const Y_GAP = 104;

export function buildFlowGraph(flow: Flow): { nodes: MapNode[]; edges: MapEdge[] } {
  const edges: MapEdge[] = [];
  const childrenMap = new Map<string, string[]>();
  const nodeIndex = new Map<string, MapNode>();

  const addChild = (parent: string, child: string) => {
    childrenMap.set(parent, [...(childrenMap.get(parent) ?? []), child]);
  };

  for (const topic of flow.topics) {
    nodeIndex.set(topic.id, { id: topic.id, kind: "topic", topic, x: 0, y: 0 });
  }
  for (const block of flow.blocks) {
    nodeIndex.set(block.id, { id: block.id, kind: "block", block, x: 0, y: 0 });
  }

  for (const block of flow.blocks) {
    if (block.parentId && nodeIndex.has(block.parentId)) {
      const parentIsTopic = flow.topics.some((t) => t.id === block.parentId);
      const kind: MapEdgeKind = block.adapter === "kc" ? "subscription" : block.branch ? "branch" : "flow";
      // A branch is labelled with its RULES when it has any — they are what
      // decide which records take the edge, and an unfinished rule
      // (`<field> equals "<value>"`) has to be visible on the canvas. With no
      // rules the name is the honest label; the edge carries everything.
      const label = block.branch
        ? isConditional(block.branch)
          ? describeBranch(block.branch)
          : block.branch.name
        : undefined;
      edges.push({
        id: `e-${block.parentId}-${block.id}`,
        source: block.parentId,
        target: block.id,
        kind: parentIsTopic && block.adapter === "kc" ? "subscription" : kind,
        label,
      });
      addChild(block.parentId, block.id);
    }
  }
  for (const topic of flow.topics) {
    if (topic.kind === "materialized" && topic.writerBlockId && nodeIndex.has(topic.writerBlockId)) {
      edges.push({ id: `e-${topic.writerBlockId}-${topic.id}`, source: topic.writerBlockId, target: topic.id, kind: "materialize" });
      addChild(topic.writerBlockId, topic.id);
    }
  }

  // Roots: nodes that are nobody's child.
  const childIds = new Set([...childrenMap.values()].flat());
  const roots = [...nodeIndex.keys()].filter((id) => !childIds.has(id));

  // Tree layout: leaves get consecutive rows; parents center over children.
  let nextRow = 0;
  const rowOf = new Map<string, number>();
  const placed = new Set<string>();
  const visit = (id: string, depth: number): number => {
    if (placed.has(id)) return rowOf.get(id) ?? 0;
    placed.add(id);
    const node = nodeIndex.get(id)!;
    node.x = depth * X_GAP;
    const kids = (childrenMap.get(id) ?? []).filter((k) => !placed.has(k));
    let row: number;
    if (kids.length === 0) {
      row = nextRow++;
    } else {
      const rows = kids.map((k) => visit(k, depth + 1));
      row = (Math.min(...rows) + Math.max(...rows)) / 2;
    }
    rowOf.set(id, row);
    node.y = row * Y_GAP;
    return row;
  };
  for (const rootId of roots) visit(rootId, 0);
  // A cycle has no root at all, so anything still unplaced is laid out as its
  // own root rather than being stacked, invisible, at the origin.
  for (const id of nodeIndex.keys()) if (!placed.has(id)) visit(id, 0);

  return { nodes: [...nodeIndex.values()], edges };
}

export function chainTipIds(flow: Flow): Set<string> {
  const { nodes, edges } = buildFlowGraph(flow);
  const sources = new Set(edges.map((e) => e.source));
  return new Set(nodes.map((n) => n.id).filter((id) => !sources.has(id)));
}
