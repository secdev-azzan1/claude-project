import { memo } from "react";
import { Handle, Position, type NodeProps } from "@xyflow/react";
import { Plus, Star } from "lucide-react";

import { cn } from "@/lib/utils";
import type { FlowMapNodeData } from "@/lib/streamFlowMap";

export interface StreamNodeData extends FlowMapNodeData {
  selected: boolean;
  onSelect: (id: string) => void;
  onAddBranch: (id: string) => void;
}

/** Presentational node body — no React Flow context, so it is unit-testable in isolation. */
export function StreamFlowNodeBody({ id, data }: { id: string; data: StreamNodeData }) {
  return (
    <div
      onClick={() => data.onSelect(id)}
      className={cn(
        "relative w-[220px] cursor-pointer rounded-lg border bg-card px-3 py-2 shadow-sm transition-colors",
        data.selected ? "border-primary ring-2 ring-primary/40" : "border-border hover:bg-muted/40",
      )}
    >
      <div className="flex items-center justify-between gap-2">
        <span className="truncate text-sm font-semibold">{data.name}</span>
        {data.isEntity && (
          <span className="inline-flex items-center gap-0.5 rounded bg-warning-muted px-1.5 py-0.5 text-[10px] font-semibold text-warning">
            <Star className="h-2.5 w-2.5 fill-current" /> Entity
          </span>
        )}
      </div>
      <div className="truncate font-mono text-xs text-muted-foreground">{data.endpointPath || "/path"}</div>
      <div className="mt-1 flex flex-wrap items-center gap-1">
        {data.isRouteChild && (
          <span className="rounded border border-purple-500/40 px-1.5 text-[10px] text-purple-500">Route</span>
        )}
        {data.isBranchChild && (
          <span className="rounded border border-blue-500/40 px-1.5 text-[10px] text-blue-500">Branch</span>
        )}
        {data.childCount > 0 && (
          <span className="rounded border px-1.5 text-[10px] text-muted-foreground">
            {data.childCount} {data.childCount === 1 ? "child" : "children"}
          </span>
        )}
      </div>
      <button
        type="button"
        title="Add parallel branch"
        aria-label="Add parallel branch"
        onClick={(event) => {
          event.stopPropagation();
          data.onAddBranch(id);
        }}
        className="absolute -right-2 -top-2 hidden h-5 w-5 items-center justify-center rounded-full border bg-background text-muted-foreground shadow hover:text-primary group-hover:flex"
      >
        <Plus className="h-3 w-3" />
      </button>
    </div>
  );
}

function StreamFlowNodeInner(props: NodeProps) {
  const data = props.data as unknown as StreamNodeData;
  return (
    <div className="group">
      <Handle type="target" position={Position.Left} className="!bg-muted-foreground" />
      <StreamFlowNodeBody id={props.id} data={data} />
      <Handle type="source" position={Position.Right} className="!bg-muted-foreground" />
    </div>
  );
}

export const StreamFlowNode = memo(StreamFlowNodeInner);
