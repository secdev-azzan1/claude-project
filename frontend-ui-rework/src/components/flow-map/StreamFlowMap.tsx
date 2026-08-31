import { useEffect, useMemo } from "react";
import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
  type Edge,
  type Node,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import { EDGE_COLORS, streamsToFlowGraph, type StreamLike } from "@/lib/streamFlowMap";
import type { StreamGraphEdges } from "@/lib/streamGraph";
import { StreamFlowNode } from "./StreamFlowNode";

const nodeTypes = { streamNode: StreamFlowNode };

export interface StreamFlowMapProps {
  streams: StreamLike[];
  graph: StreamGraphEdges;
  editingStream: string;
  setEditingStream: (id: string) => void;
  onCreateParallelBranch: (parentId: string) => void;
}

function StreamFlowMapInner({
  streams,
  graph,
  editingStream,
  setEditingStream,
  onCreateParallelBranch,
}: StreamFlowMapProps) {
  const { fitView } = useReactFlow();

  const { rfNodes, rfEdges } = useMemo(() => {
    const { nodes, edges } = streamsToFlowGraph(streams, graph);
    const mappedNodes: Node[] = nodes.map((n) => ({
      id: n.id,
      type: "streamNode",
      position: n.position,
      data: {
        ...n.data,
        selected: n.id === editingStream,
        onSelect: setEditingStream,
        onAddBranch: onCreateParallelBranch,
      },
    }));
    const mappedEdges: Edge[] = edges.map((e) => ({
      id: e.id,
      source: e.source,
      target: e.target,
      type: "smoothstep",
      style: { stroke: EDGE_COLORS[e.kind], strokeWidth: 2 },
    }));
    return { rfNodes: mappedNodes, rfEdges: mappedEdges };
  }, [streams, graph, editingStream, setEditingStream, onCreateParallelBranch]);

  // Re-fit whenever the node count changes (e.g. after adding a branch).
  useEffect(() => {
    const timer = window.setTimeout(() => fitView({ padding: 0.2, duration: 300 }), 0);
    return () => window.clearTimeout(timer);
  }, [rfNodes.length, fitView]);

  return (
    <ReactFlow
      nodes={rfNodes}
      edges={rfEdges}
      nodeTypes={nodeTypes}
      onNodeClick={(_, node) => setEditingStream(node.id)}
      nodesDraggable={false}
      nodesConnectable={false}
      fitView
      minZoom={0.1}
      proOptions={{ hideAttribution: true }}
    >
      <Background />
      <Controls showInteractive={false} />
      <MiniMap pannable zoomable />
    </ReactFlow>
  );
}

export function StreamFlowMap(props: StreamFlowMapProps) {
  return (
    <ReactFlowProvider>
      <StreamFlowMapInner {...props} />
    </ReactFlowProvider>
  );
}
