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
    // The node is a card, so it uses the same radius and elevation language as
    // every other card in the app. The adapter's identity moved from a tinted
    // 1px outline onto a tinted icon tile: an outline in five different hues
    // made the canvas read as a colour chart, and it competed with the ring that
    // marks selection.
    <div
      className={cn(
        "group relative w-[240px] rounded-xl bg-card px-3 py-2.5 shadow-sm transition-all duration-150",
        d.selected ? "shadow-lg ring-2 ring-primary" : "ring-1 ring-inset ring-border/70 hover:shadow",
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
        <div className="material-thick flex items-center gap-1 rounded-lg border border-border/60 p-1 shadow-xl">
          <Button
            size="xs"
            variant="ghost"
            className="text-destructive hover:bg-destructive/10 hover:text-destructive"
            onClick={(e) => {
              e.stopPropagation();
              d.onRequestDelete(d.block.id);
            }}
          >
            <Trash2 /> Delete
          </Button>
        </div>
      </NodeToolbar>

      <Handle
        type="target"
        position={Position.Left}
        className="!h-2.5 !w-2.5 !border-2 !border-background !bg-muted-foreground/50"
      />
      <Handle
        type="source"
        position={Position.Right}
        className="!h-2.5 !w-2.5 !border-2 !border-background !bg-muted-foreground/50 transition-colors group-hover:!bg-primary"
      />

      <div className="flex items-center gap-2">
        <span className={cn("flex h-5 w-5 shrink-0 items-center justify-center rounded-md", meta.chipClass)}>
          <Icon className="h-3 w-3" />
        </span>
        <span className="truncate font-mono text-2xs font-medium text-muted-foreground">
          {meta.label}
          {d.block.mode ? `·${d.block.mode}` : ""}
        </span>
        <span className="ml-auto flex shrink-0 items-center gap-1">
          {d.branchAttention && (
            <span
              title="A branch here has a half-written condition, so it matches no records — finish its field, operator and value in Routing."
              className="text-warning"
            >
              <AlertTriangle className="h-3.5 w-3.5" />
            </span>
          )}
          {d.cron && (
            <span title={`cron ${d.cron} (UTC)`} className="text-muted-foreground/70">
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
              className="flex h-4 min-w-4 items-center justify-center rounded-full bg-destructive px-1 text-2xs font-semibold text-destructive-foreground"
            >
              {d.issues}
            </span>
          )}
        </span>
      </div>

      <p className="mt-1.5 truncate text-sm font-medium leading-tight">{d.block.name}</p>

      {(d.block.entity || d.block.branch) && (
        <div className="mt-1.5 flex items-center gap-1">
          {/* "entity: asset" became just "asset" — the node is 240px wide and the
              word "entity" was spending a fifth of it restating the field name. */}
          {d.block.entity && (
            <span className="truncate rounded bg-muted px-1.5 py-0.5 text-2xs font-medium text-muted-foreground">
              {d.block.entity}
            </span>
          )}
          {d.block.branch && (
            <span className="truncate rounded bg-primary-muted px-1.5 py-0.5 text-2xs font-medium text-primary">
              {isConditional(d.block.branch) ? describeBranch(d.block.branch, 1) : d.block.branch.name}
            </span>
          )}
        </div>
      )}

      {!d.locked && d.entries.length > 0 && (
        <div className={addButtonClass(d.selected || d.isTip)}>
          <AddBlockMenu entries={d.entries} onSelect={(e) => d.onAdd(d.block.id, e)} label={d.addLabel}>
            <Button
              size="icon-xs"
              variant="default"
              className="h-6 w-6 rounded-full shadow-md ring-2 ring-background"
              title={`Add a block after "${d.block.name}"`}
              onClick={(e) => e.stopPropagation()}
            >
              <Plus />
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
      // A topic is a pipe, not a step — the pill shape is what distinguishes it
      // from the block cards at a glance, so it stays.
      className={cn(
        "group relative flex w-[212px] items-center gap-1.5 rounded-full bg-muted px-3 py-1.5 shadow-sm transition-all duration-150",
        d.selected ? "ring-2 ring-primary" : "ring-1 ring-inset ring-border/70 hover:shadow",
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
      <Handle
        type="target"
        position={Position.Left}
        className="!h-2.5 !w-2.5 !border-2 !border-background !bg-muted-foreground/50"
      />
      <Handle
        type="source"
        position={Position.Right}
        className="!h-2.5 !w-2.5 !border-2 !border-background !bg-muted-foreground/50 transition-colors group-hover:!bg-primary"
      />
      {d.topic.sealed ? <Lock className="h-3.5 w-3.5 shrink-0 text-muted-foreground" /> : <Radio className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />}
      <span className="truncate font-mono text-xs">{d.topic.name}</span>
      {d.topic.kind === "adopted" && (
        <span className="shrink-0 rounded bg-info-muted px-1 py-px text-2xs font-medium text-info">adopted</span>
      )}
      {!d.locked && !d.topic.sealed && d.entries.length > 0 && (
        <div className={addButtonClass(d.selected || d.isTip)}>
          <AddBlockMenu entries={d.entries} onSelect={(e) => d.onAdd(d.topic.id, e)} label={`Attach to ${d.topic.name}`}>
            <Button
              size="icon-xs"
              variant="default"
              className="h-6 w-6 rounded-full shadow-md ring-2 ring-background"
              title={`Attach a consumer or sink to ${d.topic.name}`}
              onClick={(e) => e.stopPropagation()}
            >
              <Plus />
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
    <div className="w-[300px] rounded-2xl border-2 border-dashed border-border bg-card/80 px-5 py-6 text-center">
      <p className="text-sm font-semibold">No root block yet</p>
      <p className="mx-auto mt-1.5 max-w-[15rem] text-xs leading-relaxed text-muted-foreground">
        A flow starts with the block that pulls the first records. Everything legal after it follows from that choice.
      </p>
      <AddBlockMenu entries={d.entries} onSelect={(e) => d.onAdd(null, e)} label="Choose the root block">
        <Button size="sm" className="mt-4" disabled={d.locked}>
          <Plus /> Place the root
        </Button>
      </AddBlockMenu>
    </div>
  );
}

const nodeTypes = { blockNode: BlockNode, topicNode: TopicNode, placeholderNode: PlaceholderNode };

const PLACEHOLDER_ID = "__root-placeholder__";

/**
 * The legibility floor and ceiling for the whole canvas — NOT just the initial
 * fit. These live on the `<ReactFlow>` component itself (`minZoom`/`maxZoom`
 * props below), because that is the single place xyflow actually enforces a
 * zoom bound: fitView, scroll-zoom and the Controls zoom buttons all clamp
 * against it. An earlier version of this file set a *different*, tighter
 * floor only on `fitViewOptions` while leaving the component prop at 0.2 — the
 * component prop is what genuinely governs zooming, so that mismatch meant a
 * user could still scroll a node down to illegible size the moment they
 * touched the wheel. Keeping exactly one floor/ceiling, set once, is what
 * fixes it: fitView's computed zoom is clamped by the same bound the user's
 * own zooming is, so there is nothing to jump between.
 *
 * 0.5 is xyflow's own documented default minZoom — not an arbitrary choice.
 * A graph too wide to fit even at that floor simply overflows and is panned,
 * which is the behaviour every real canvas app has.
 */
const ZOOM_FLOOR = 0.5;
const ZOOM_CEILING = 1.5;
const FIT_VIEW_OPTIONS = { padding: 0.18, duration: 300 } as const;

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
  /**
   * True while the page has expanded this same mounted canvas to fill the
   * viewport (see FlowBuilder.tsx). The map re-fits when this flips either way
   * — the container just changed size, so the old camera position no longer
   * frames the graph the way it did a moment ago.
   */
  expanded?: boolean;
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
  expanded,
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
        // Was a hardcoded hsl() that stayed dark-on-dark once the theme could
        // actually be switched. The token follows the theme.
        labelStyle: { fontSize: 11, fontWeight: 500, fill: "hsl(var(--muted-foreground))" },
        labelBgStyle: { fill: "hsl(var(--background))", fillOpacity: 0.85 },
        labelBgPadding: [5, 3] as [number, number],
        labelBgBorderRadius: 4,
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
    const timer = window.setTimeout(() => fitView(FIT_VIEW_OPTIONS), 0);
    return () => window.clearTimeout(timer);
  }, [fitKey, nodesInitialized, fitView]);

  // Expand/collapse resizes the SAME mounted canvas (see FlowMapView's
  // `expanded` doc comment) rather than remounting it, so the fit-once guard
  // above does not fire again on its own — the flow/graph identity it keys on
  // hasn't changed, only the viewport has. Re-fit explicitly, once the CSS
  // transition on the wrapper has had a moment to finish, so the graph frames
  // the new size instead of staying at its old-viewport zoom and position.
  const isInitialExpandedRender = useRef(true);
  useEffect(() => {
    if (isInitialExpandedRender.current) {
      isInitialExpandedRender.current = false;
      return;
    }
    if (!nodesInitialized) return;
    const timer = window.setTimeout(() => fitView(FIT_VIEW_OPTIONS), 220);
    return () => window.clearTimeout(timer);
  }, [expanded, nodesInitialized, fitView]);

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
        fitViewOptions={FIT_VIEW_OPTIONS}
        minZoom={ZOOM_FLOOR}
        maxZoom={ZOOM_CEILING}
        proOptions={{ hideAttribution: true }}
      >
        {/* Opacity dropped from the previous pass (25% → 15%) so the grid is
            felt as texture rather than seen as a pattern competing with node
            content — the canvas/whiteboard research converged on "low enough
            to be felt, not seen" as the tell for a background that recedes. */}
        <Background gap={18} size={1} color="hsl(var(--muted-foreground))" className="opacity-[0.15]" />
        {/* xyflow ships its own white-box button styling; these overrides put the
            controls on the app's surface language instead. */}
        <Controls
          showInteractive={false}
          className="!rounded-lg !border !border-border/60 !bg-card !shadow-md [&>button]:!border-0 [&>button]:!bg-transparent [&>button]:!fill-muted-foreground [&>button:hover]:!bg-accent"
        />
        {/* No <MiniMap> here: xyflow 12.11.2's built-in one reads its node list
            from a flat store field that this app's fully-controlled
            nodes/edges usage never populates (confirmed by inspecting the
            compiled output — nodeLookup, which the main canvas and the fit
            math both use, is populated correctly; the separate `.nodes` array
            selectorNodeIds reads is not). It renders as a blank box with zero
            node markers. Not worth carrying a workaround for what was already
            the least essential item here — the Expand toggle covers the same
            "large flow" case with a full-viewport view instead. */}
        {/* The permanent gesture crib sheet that used to live bottom-right is
            gone: FlowBuilder's "Flow map" heading carries the same sentence
            behind its ⓘ, so this was the same text printed onto the canvas
            forever. */}
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
            <AlertDialogDescription>
              Everything below it goes with it — a branch is removed as a whole.
            </AlertDialogDescription>
          </AlertDialogHeader>

          {preview && (
            <div className="rounded-lg bg-muted/60 p-3">
              <p className="text-2xs font-semibold uppercase tracking-wider text-muted-foreground">
                {preview.blocks.length} block{preview.blocks.length === 1 ? "" : "s"}
                {preview.topics.length > 0
                  ? ` · ${preview.topics.length} topic${preview.topics.length === 1 ? "" : "s"}`
                  : ""}
              </p>
              <ul className="mt-2 space-y-1">
                {preview.blocks.map((name) => (
                  <li key={`b-${name}`} className="flex items-center gap-2 truncate text-xs">
                    <span className="h-1 w-1 shrink-0 rounded-full bg-destructive" />
                    {name}
                  </li>
                ))}
                {preview.topics.map((name) => (
                  <li key={`t-${name}`} className="flex items-center gap-2 truncate font-mono text-xs text-muted-foreground">
                    <span className="h-1 w-1 shrink-0 rounded-full bg-muted-foreground/50" />
                    {name}
                  </li>
                ))}
              </ul>
            </div>
          )}
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
