// The interactive flow map: a React Flow canvas over the pure graph module.
//
// The canvas CREATES structure; the form CONFIGURES it. Every gesture here ends
// by handing a block id back to the page so its form opens — configuration
// never moves onto the canvas.
//
// Gestures:
//   · click a node                        → select it (its form opens)
//   · ＋ on a node / the empty canvas      → the legality-filtered add menu
//   · drag a source handle → empty canvas → the same menu, at the drop point
//   · drag a source handle → another node → re-parent (validated, refusals shown)
//   · drag an edge endpoint                → re-parent the edge that moved
//   · node toolbar Delete, or the Delete key → cascade delete with a preview
//
// Nothing here writes to the flow. Every structural change is routed to the page,
// which applies the shared mutations in src/prototype/mutations.ts.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  Background,
  Controls,
  Handle,
  NodeToolbar,
  Panel,
  Position,
  ReactFlow,
  ReactFlowProvider,
  useNodesInitialized,
  useReactFlow,
  type Connection,
  type Edge,
  type FinalConnectionState,
  type Node,
  type NodeProps,
  type OnConnectStartParams,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import { cn } from "@/lib/utils";
import { ADAPTER_META } from "@/components/AdapterChip";
import { AddBlockMenu } from "./AddBlockMenu";
import {
  buildFlowGraph,
  chainTipIds,
  EDGE_STYLE,
  isDerivedEdgeKind,
} from "./graph";
import {
  addMenuLabel,
  canReparent,
  computeAddMenu,
  computeRootMenu,
  computeTopicMenu,
  scheduledBlock,
  subtreeIds,
  type AddMenuEntry,
} from "@/prototype/legality";
import { flowHasTrigger } from "@/prototype/legality";
import { branchAttentionIds, describeBranch, isConditional } from "@/prototype/branches";
import type { Flow, FlowBlock, FlowTopic } from "@/prototype/types";
import { AlertTriangle, CheckCircle2, Clock, Lock, Plus, Radio, Trash2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { toast } from "sonner";

interface BlockNodeData {
  block: FlowBlock;
  selected: boolean;
  issues: number;
  cron: string | null;
  entries: AddMenuEntry[];
  /**
   * The menu heading, from `addMenuLabel` — it states whether this ＋ continues
   * the chain or opens a parallel branch, which is the only difference a fork
   * ever was. Computed centrally so the canvas, the outline and the drop menu
   * cannot word it three ways.
   */
  addLabel: string;
  locked: boolean;
  /** Nothing downstream — the ＋ stays visible on tips so the canvas shows where it grows. */
  isTip: boolean;
  /** A branch here has a half-written condition, so it currently matches nothing. */
  branchAttention: boolean;
  onSelect: (id: string) => void;
  onAdd: (parentNodeId: string | null, entry: AddMenuEntry) => void;
  onRequestDelete: (blockId: string) => void;
  [key: string]: unknown;
}

interface TopicNodeData {
  topic: FlowTopic;
  selected: boolean;
  entries: AddMenuEntry[];
  locked: boolean;
  isTip: boolean;
  onSelect: (id: string) => void;
  onAdd: (parentNodeId: string | null, entry: AddMenuEntry) => void;
  [key: string]: unknown;
}

interface PlaceholderNodeData {
  entries: AddMenuEntry[];
  locked: boolean;
  onAdd: (parentNodeId: string | null, entry: AddMenuEntry) => void;
  [key: string]: unknown;
}

/** The ＋ is pinned open on the selected node and on chain tips; the rest reveal on hover. */
function addButtonClass(pinned: boolean): string {
  return cn(
    "absolute -right-3 top-1/2 -translate-y-1/2 transition-opacity",
    pinned ? "opacity-100" : "opacity-0 focus-within:opacity-100 group-hover:opacity-100",
  );
}

function BlockNode({ data }: NodeProps) {
  const d = data as BlockNodeData;
  const meta = ADAPTER_META[d.block.adapter];
  const Icon = meta.icon;
  return (
    <div
      className={cn(
        "group relative w-[236px] rounded-lg border bg-card px-3 py-2 shadow-sm transition-shadow",
        meta.nodeClass,
        d.selected ? "shadow-lg ring-2 ring-primary" : "hover:shadow",
      )}
      onClick={() => d.onSelect(d.block.id)}
      role="button"
    >
      <NodeToolbar
        // NodeToolbar defaults to xyflow's internal node.selected. Selection here
        // is the app's own selectedId, mirrored onto data.selected — so visibility
        // is passed explicitly or the toolbar simply never appears.
        isVisible={d.selected}
        // Below, not above: the map band is only 320px tall and fitView centres
        // the graph in it, so a toolbar above the node is clipped by the band.
        position={Position.Bottom}
        offset={10}
      >
        <div className="flex items-center gap-1 rounded-md border bg-popover p-1 shadow-xl">
          <Button
            size="sm"
            variant="ghost"
            className="h-7 gap-1.5 px-2 text-xs text-destructive hover:text-destructive"
            onClick={(e) => {
              e.stopPropagation();
              d.onRequestDelete(d.block.id);
            }}
          >
            <Trash2 className="h-3.5 w-3.5" /> Delete
          </Button>
        </div>
      </NodeToolbar>

      <Handle type="target" position={Position.Left} className="!h-2.5 !w-2.5 !bg-muted-foreground/60" />
      <Handle type="source" position={Position.Right} className="!h-2.5 !w-2.5 !bg-muted-foreground/60" />
      <div className="flex items-center gap-1.5">
        <Icon className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
        <span className="text-xs font-medium text-muted-foreground">
          {meta.label}
          {d.block.mode ? ` · ${d.block.mode}` : ""}
        </span>
        <span className="ml-auto flex items-center gap-1">
          {d.branchAttention && (
            <span
              title="A branch here has a half-written condition, so it matches no records — finish its field, operator and value in Routing."
              className="text-warning"
            >
              <AlertTriangle className="h-3.5 w-3.5" />
            </span>
          )}
          {d.cron && (
            <span title={`cron ${d.cron} (UTC)`} className="text-muted-foreground">
              <Clock className="h-3.5 w-3.5" />
            </span>
          )}
          {d.block.testResult?.ok && (
            <span title={`Tested ${d.block.testResult.testedAt}`} className="text-success">
              <CheckCircle2 className="h-3.5 w-3.5" />
            </span>
          )}
          {d.issues > 0 && (
            <span
              title={`${d.issues} validation issue(s)`}
              className="flex h-[18px] min-w-[18px] items-center justify-center rounded-full bg-destructive px-1 text-xs font-semibold text-destructive-foreground"
            >
              {d.issues}
            </span>
          )}
        </span>
      </div>
      <p className="mt-1 truncate text-sm font-medium leading-5">{d.block.name}</p>
      {(d.block.entity || d.block.branch) && (
        <div className="mt-1 flex items-center gap-1.5">
          {d.block.entity && (
            <span className="truncate rounded bg-muted px-1.5 py-0.5 text-xs font-medium text-muted-foreground">
              entity: {d.block.entity}
            </span>
          )}
          {d.block.branch && (
            <span className="truncate rounded bg-accent px-1.5 py-0.5 text-xs font-medium text-accent-foreground">
              {isConditional(d.block.branch) ? describeBranch(d.block.branch, 1) : d.block.branch.name}
            </span>
          )}
        </div>
      )}
      {!d.locked && d.entries.length > 0 && (
        <div className={addButtonClass(d.selected || d.isTip)}>
          <AddBlockMenu
            entries={d.entries}
            onSelect={(e) => d.onAdd(d.block.id, e)}
            label={d.addLabel}
          >
            <Button
              size="icon"
              variant="default"
              className="h-6 w-6 rounded-full shadow"
              title={`Add a block after "${d.block.name}"`}
              onClick={(e) => e.stopPropagation()}
            >
              <Plus className="h-3.5 w-3.5" />
            </Button>
          </AddBlockMenu>
        </div>
      )}
    </div>
  );
}

function TopicNode({ data }: NodeProps) {
  const d = data as TopicNodeData;
  return (
    <div
      className={cn(
        "group relative flex w-[212px] items-center gap-1.5 rounded-full border bg-muted/60 px-3 py-1.5 shadow-sm",
        d.selected ? "ring-2 ring-primary" : "hover:shadow",
        d.topic.sealed && "border-muted-foreground/30",
      )}
      onClick={() => d.onSelect(d.topic.id)}
      role="button"
      title={
        d.topic.sealed
          ? "Sealed — kafka+connect topics are managed with their sink as one unit; nothing can attach."
          : d.topic.kind === "adopted"
            ? "Adopted topic — sampled · never renamed"
            : "Materialized topic"
      }
    >
      <Handle type="target" position={Position.Left} className="!h-2.5 !w-2.5 !bg-muted-foreground/60" />
      <Handle type="source" position={Position.Right} className="!h-2.5 !w-2.5 !bg-muted-foreground/60" />
      {d.topic.sealed ? <Lock className="h-3.5 w-3.5 shrink-0 text-muted-foreground" /> : <Radio className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />}
      <span className="truncate font-mono text-xs">{d.topic.name}</span>
      {d.topic.kind === "adopted" && (
        <span className="shrink-0 rounded bg-info-muted px-1 py-px text-xs font-medium text-info">adopted</span>
      )}
      {!d.locked && !d.topic.sealed && d.entries.length > 0 && (
        <div className={addButtonClass(d.selected || d.isTip)}>
          <AddBlockMenu entries={d.entries} onSelect={(e) => d.onAdd(d.topic.id, e)} label={`Attach to ${d.topic.name}`}>
            <Button
              size="icon"
              variant="default"
              className="h-6 w-6 rounded-full shadow"
              title={`Attach a consumer or sink to ${d.topic.name}`}
              onClick={(e) => e.stopPropagation()}
            >
              <Plus className="h-3.5 w-3.5" />
            </Button>
          </AddBlockMenu>
        </div>
      )}
    </div>
  );
}

/**
 * The empty canvas used to render nothing at all — root placement existed only
 * in the outline, so a brand new flow opened onto a dead grid.
 */
function PlaceholderNode({ data }: NodeProps) {
  const d = data as PlaceholderNodeData;
  return (
    <div className="w-[280px] rounded-xl border-2 border-dashed bg-card/70 px-4 py-5 text-center">
      <p className="text-sm font-medium">No root block yet</p>
      <p className="mt-1 text-xs leading-5 text-muted-foreground">
        A flow starts with the block that pulls the first records. Everything legal after it follows from that choice.
      </p>
      <AddBlockMenu entries={d.entries} onSelect={(e) => d.onAdd(null, e)} label="Choose the root block">
        <Button size="sm" className="mt-3 gap-1.5" disabled={d.locked}>
          <Plus className="h-4 w-4" /> Place the root
        </Button>
      </AddBlockMenu>
    </div>
  );
}

const nodeTypes = { blockNode: BlockNode, topicNode: TopicNode, placeholderNode: PlaceholderNode };

const PLACEHOLDER_ID = "__root-placeholder__";

export interface FlowMapViewProps {
  flow: Flow;
  selectedId: string | null;
  issuesByNode: Map<string, number>;
  locked: boolean;
  /** The verbatim edit-lock refusal, so canvas gestures refuse in the same words the forms use. */
  lockReason?: string | null;
  onSelect: (id: string) => void;
  /** `null` places the root — the map is the second surface that can do it. */
  onAdd: (parentNodeId: string | null, entry: AddMenuEntry) => void;
  onReparent: (blockId: string, newParentId: string) => void;
  onDelete: (blockId: string) => void;
}

/** What a cascade delete would take with it, for the confirm dialog. */
function deletePreview(flow: Flow, blockId: string): { blocks: string[]; topics: string[] } {
  const doomed = new Set(subtreeIds(flow, blockId));
  return {
    blocks: flow.blocks.filter((b) => doomed.has(b.id)).map((b) => b.name),
    topics: flow.topics.filter((t) => doomed.has(t.id)).map((t) => t.name),
  };
}

function pointerOf(event: MouseEvent | TouchEvent): { x: number; y: number } {
  if ("touches" in event) {
    const touch = event.changedTouches[0] ?? event.touches[0];
    return { x: touch?.clientX ?? 0, y: touch?.clientY ?? 0 };
  }
  return { x: event.clientX, y: event.clientY };
}

interface DropMenuState {
  parentId: string;
  label: string;
  entries: AddMenuEntry[];
  /** Blocks that host routing offer the conditional branch here too — parity with the ＋ menu. */
  x: number;
  y: number;
}

function FlowMapViewInner({
  flow,
  selectedId,
  issuesByNode,
  locked,
  lockReason,
  onSelect,
  onAdd,
  onReparent,
  onDelete,
}: FlowMapViewProps) {
  const { fitView } = useReactFlow();
  const wrapperRef = useRef<HTMLDivElement | null>(null);
  const connectFromRef = useRef<string | null>(null);
  const [dropMenu, setDropMenu] = useState<DropMenuState | null>(null);
  const [pendingDelete, setPendingDelete] = useState<string | null>(null);

  const lockText = lockReason ?? "The flow is deployed — stop it before editing its structure.";

  const requestDelete = useCallback(
    (blockId: string) => {
      const block = flow.blocks.find((b) => b.id === blockId);
      if (!block) return;
      if (locked && block.adapter !== "kc") {
        toast.error(lockText);
        return;
      }
      setPendingDelete(blockId);
    },
    [flow, locked, lockText],
  );

  /**
   * Legality as a pure function of ids plus the flow — `isValidConnection` gets
   * `{source, target, sourceHandle, targetHandle}` and nothing else, so it can
   * never consult an event or a node object.
   */
  const reparentRefusal = useCallback(
    (parentNodeId: string, childNodeId: string): string | null => {
      if (parentNodeId === childNodeId) return "A block cannot be its own parent.";
      const child = flow.blocks.find((b) => b.id === childNodeId);
      if (!child)
        return "Only blocks move — a topic belongs to the block that materializes it, and an adopted topic is where the flow starts.";
      if (locked && child.adapter !== "kc") return lockText;
      return canReparent(flow, childNodeId, parentNodeId);
    },
    [flow, locked, lockText],
  );

  const menuFor = useCallback(
    (nodeId: string): { entries: AddMenuEntry[]; label: string } | null => {
      const topic = flow.topics.find((t) => t.id === nodeId);
      // A topic fans out to independent subscribers (R5); routing is a block's
      // own transform, so a topic node never offers a conditional branch.
      if (topic) return { entries: computeTopicMenu(flow, nodeId), label: addMenuLabel(flow, nodeId) };
      const block = flow.blocks.find((b) => b.id === nodeId);
      if (!block) return null;
      return { entries: computeAddMenu(flow, nodeId), label: addMenuLabel(flow, nodeId) };
    },
    [flow],
  );

  const tips = useMemo(() => chainTipIds(flow), [flow]);
  const attention = useMemo(() => branchAttentionIds(flow), [flow]);

  const { rfNodes, rfEdges } = useMemo(() => {
    const { nodes, edges } = buildFlowGraph(flow);

    if (nodes.length === 0) {
      const placeholder: Node = {
        id: PLACEHOLDER_ID,
        type: "placeholderNode",
        position: { x: 0, y: 0 },
        draggable: false,
        selectable: false,
        connectable: false,
        deletable: false,
        // A node that is not selectable, draggable or connectable is given
        // `pointer-events: none` by xyflow — so "Place the root" was painted on
        // a surface the mouse could not touch and every click went to the pane
        // behind it. `node.style` is merged last, so this puts them back.
        style: { pointerEvents: "all" },
        data: { entries: computeRootMenu(), locked, onAdd } satisfies PlaceholderNodeData,
      };
      return { rfNodes: [placeholder], rfEdges: [] as Edge[] };
    }

    const cronOwner = flowHasTrigger(flow) ? scheduledBlock(flow)?.id : undefined;
    const mappedNodes: Node[] = nodes.map((n) =>
      n.kind === "block"
        ? {
            id: n.id,
            type: "blockNode",
            position: { x: n.x, y: n.y },
            // xyflow's own selection drives the Delete key; this codebase's
            // selection is data.selected, so both are kept in step here.
            selected: n.id === selectedId,
            data: {
              block: n.block!,
              selected: n.id === selectedId,
              issues: issuesByNode.get(n.id) ?? 0,
              cron: n.id === cronOwner ? flow.cron : null,
              entries: computeAddMenu(flow, n.id),
              addLabel: addMenuLabel(flow, n.id),
              locked,
              isTip: tips.has(n.id),
              branchAttention: attention.has(n.id),
              onSelect,
              onAdd,
              onRequestDelete: requestDelete,
            } satisfies BlockNodeData,
          }
        : {
            id: n.id,
            type: "topicNode",
            position: { x: n.x, y: n.y + 14 },
            selected: n.id === selectedId,
            deletable: false, // topics follow their writer; they are never deleted directly
            data: {
              topic: n.topic!,
              selected: n.id === selectedId,
              entries: computeTopicMenu(flow, n.id),
              locked,
              isTip: tips.has(n.id),
              onSelect,
              onAdd,
            } satisfies TopicNodeData,
          },
    );
    const mappedEdges: Edge[] = edges.map((e) => {
      const style = EDGE_STYLE[e.kind];
      return {
        id: e.id,
        source: e.source,
        target: e.target,
        type: "smoothstep",
        label: e.label,
        labelStyle: { fontSize: 12, fill: "hsl(215 16% 47%)" },
        labelBgPadding: [4, 2] as [number, number],
        // A materialize edge is derived from writerBlockId, not a parent link —
        // dragging it would claim to re-parent something that has no parent.
        reconnectable: !isDerivedEdgeKind(e.kind),
        style: { stroke: style.stroke, strokeWidth: style.width, strokeDasharray: style.dash },
      };
    });
    return { rfNodes: mappedNodes, rfEdges: mappedEdges };
  }, [flow, selectedId, issuesByNode, locked, tips, attention, onSelect, onAdd, requestDelete]);

  // Camera: fit once per flow, and once more when a blank canvas gains its first
  // real node. Refitting on every node-count change animated the viewport out
  // from under the cursor in the middle of the gesture that caused the change,
  // so user-initiated mutations never move the camera; Controls keeps a manual
  // "fit view" button for when the graph outgrows the frame.
  //
  // The fit has to wait for `nodesInitialized`: node sizes are measured after
  // the first paint, and fitting before that frames a graph of zero-size nodes.
  const nodesInitialized = useNodesInitialized();
  const fitKey = `${flow.id}|${rfNodes[0]?.type === "placeholderNode" ? "empty" : "graph"}`;
  const fittedFor = useRef<string | null>(null);
  useEffect(() => {
    if (!nodesInitialized || fittedFor.current === fitKey) return;
    fittedFor.current = fitKey;
    const timer = window.setTimeout(() => fitView({ padding: 0.2, duration: 300 }), 0);
    return () => window.clearTimeout(timer);
  }, [fitKey, nodesInitialized, fitView]);

  const onConnectStart = useCallback((_event: unknown, params: OnConnectStartParams) => {
    connectFromRef.current = params.nodeId ?? null;
  }, []);

  const onConnectEnd = useCallback(
    (event: MouseEvent | TouchEvent, state: FinalConnectionState) => {
      const fromId = state.fromNode?.id ?? connectFromRef.current;
      connectFromRef.current = null;
      if (!fromId) return;

      // Released over a node: the legal case already went through onConnect, so
      // anything arriving here was refused and owes the user a reason.
      if (state.toNode) {
        const reason = reparentRefusal(fromId, state.toNode.id);
        if (reason) toast.error(reason);
        return;
      }

      // Released on empty canvas: open the same menu the ＋ opens, at the drop
      // point. Never create a bare edge — an edge without a block is not a thing
      // this model has.
      if (locked) {
        toast.error(lockText);
        return;
      }
      const menu = menuFor(fromId);
      if (!menu) return;
      if (menu.entries.length === 0) {
        const block = flow.blocks.find((b) => b.id === fromId);
        toast.error(
          block
            ? `"${block.name}" is terminal (R3/R5) — the chain never continues after it.`
            : "Nothing may be added here.",
        );
        return;
      }
      const rect = wrapperRef.current?.getBoundingClientRect();
      if (!rect) return;
      const point = pointerOf(event);
      setDropMenu({
        parentId: fromId,
        label: menu.label,
        entries: menu.entries,
        x: Math.min(Math.max(point.x - rect.left, 8), Math.max(rect.width - 8, 8)),
        y: Math.min(Math.max(point.y - rect.top, 8), Math.max(rect.height - 8, 8)),
      });
    },
    [flow, locked, lockText, menuFor, reparentRefusal],
  );

  // Handle → handle: "A now feeds B" means B's parent becomes A.
  const onConnect = useCallback(
    (connection: Connection) => {
      if (!connection.source || !connection.target) return;
      onReparent(connection.target, connection.source);
    },
    [onReparent],
  );

  const isValidConnection = useCallback(
    (candidate: Edge | Connection) => {
      const source = candidate.source;
      const target = candidate.target;
      if (!source || !target) return false;
      return reparentRefusal(source, target) === null;
    },
    [reparentRefusal],
  );

  // Dragging an existing edge endpoint is the only gesture that says WHICH edge
  // is moving, so it is the only one that can re-parent an already-connected
  // block without guessing.
  const onReconnect = useCallback(
    (oldEdge: Edge, connection: Connection) => {
      if (!connection.source || !connection.target) return;
      if (connection.target === oldEdge.target && connection.source !== oldEdge.source) {
        onReparent(oldEdge.target, connection.source);
        return;
      }
      if (connection.source === oldEdge.source && connection.target !== oldEdge.target) {
        onReparent(connection.target, oldEdge.source);
      }
    },
    [onReparent],
  );

  const onReconnectEnd = useCallback(
    (_event: MouseEvent | TouchEvent, edge: Edge, _handleType: unknown, state: FinalConnectionState) => {
      const dropped = state.toNode?.id;
      if (!dropped || state.isValid) return;
      // During a reconnect the "from" end is the anchor that is NOT moving, so
      // it identifies which endpoint the user grabbed.
      const anchor = state.fromNode?.id;
      const reason =
        anchor === edge.source
          ? reparentRefusal(edge.source, dropped)
          : anchor === edge.target
            ? reparentRefusal(dropped, edge.target)
            : null;
      if (reason) toast.error(reason);
    },
    [reparentRefusal],
  );

  // The Delete key routes through the same confirm as the toolbar. Returning
  // false always: xyflow never edits its own copy of the graph, the mutation does.
  const onBeforeDelete = useCallback(
    async ({ nodes }: { nodes: Node[]; edges: Edge[] }) => {
      const target = nodes.find((n) => n.type === "blockNode");
      if (target) requestDelete(target.id);
      return false;
    },
    [requestDelete],
  );

  const preview = pendingDelete ? deletePreview(flow, pendingDelete) : null;
  const pendingName = flow.blocks.find((b) => b.id === pendingDelete)?.name ?? "this block";

  return (
    <div ref={wrapperRef} className="relative h-full w-full">
      <ReactFlow
        nodes={rfNodes}
        edges={rfEdges}
        nodeTypes={nodeTypes}
        nodesDraggable={false}
        nodesConnectable
        deleteKeyCode="Delete"
        onBeforeDelete={onBeforeDelete}
        onConnect={onConnect}
        onConnectStart={onConnectStart}
        onConnectEnd={onConnectEnd}
        onReconnect={onReconnect}
        onReconnectEnd={onReconnectEnd}
        isValidConnection={isValidConnection}
        onPaneClick={() => setDropMenu(null)}
        fitView
        minZoom={0.2}
        proOptions={{ hideAttribution: true }}
      >
        <Background />
        <Controls showInteractive={false} />
        {/* Bottom-right: Controls own the bottom-left, and the graph itself grows
            left-to-right from the top-left, so this is the one corner that stays
            clear of nodes at every fit. */}
        {!locked && rfNodes[0]?.type !== "placeholderNode" && (
          <Panel position="bottom-right" className="pointer-events-none max-w-[60%]">
            <p className="rounded-md border bg-card/85 px-2 py-1 text-right text-xs leading-5 text-muted-foreground shadow-sm">
              Drag a block's right dot onto empty space to add the next block · drag an edge end to move a branch ·
              Delete removes one
            </p>
          </Panel>
        )}
      </ReactFlow>

      {/* Anchorless add menu, positioned where the connection was released. */}
      {dropMenu && (
        <div className="absolute z-30" style={{ left: dropMenu.x, top: dropMenu.y }}>
          <AddBlockMenu
            open
            entries={dropMenu.entries}
            label={dropMenu.label}
            onOpenChange={(open) => {
              if (!open) setDropMenu(null);
            }}
            onSelect={(entry) => {
              const parentId = dropMenu.parentId;
              setDropMenu(null);
              onAdd(parentId, entry);
            }}
          />
        </div>
      )}

      <AlertDialog open={!!pendingDelete} onOpenChange={(open) => !open && setPendingDelete(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Remove "{pendingName}"?</AlertDialogTitle>
            <AlertDialogDescription asChild>
              <div className="space-y-2 text-sm">
                <p>Everything below it goes with it — a branch is removed as a whole.</p>
                {preview && (
                  <div className="rounded-md border bg-muted/40 p-2 text-xs">
                    <p className="font-medium text-foreground">
                      {preview.blocks.length} block{preview.blocks.length === 1 ? "" : "s"}
                      {preview.topics.length > 0
                        ? ` · ${preview.topics.length} topic${preview.topics.length === 1 ? "" : "s"}`
                        : ""}
                    </p>
                    <ul className="mt-1 space-y-0.5">
                      {preview.blocks.map((name) => (
                        <li key={`b-${name}`} className="truncate">
                          {name}
                        </li>
                      ))}
                      {preview.topics.map((name) => (
                        <li key={`t-${name}`} className="truncate font-mono">
                          {name}
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Keep it</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              onClick={() => {
                const id = pendingDelete;
                setPendingDelete(null);
                if (id) onDelete(id);
              }}
            >
              Remove
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

export function FlowMapView(props: FlowMapViewProps) {
  return (
    <ReactFlowProvider>
      <FlowMapViewInner {...props} />
    </ReactFlowProvider>
  );
}
