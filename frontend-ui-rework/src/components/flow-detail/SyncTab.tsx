// Per-sink operational control for the Kafka Connect connectors this flow owns.
//
// Setup — connector class, topic wiring, connection properties, credentials —
// lives entirely in the flow builder (block config), reached here via `onEdit`.
// This tab does not configure anything; it only shows live connector/task state
// and lets an operator pause / resume / stop / start / restart / delete ONE
// sink at a time. The flow's own start/stop verbs move every connector the
// flow owns together, in lockstep with the rest of the flow's runtime — this
// tab is the escape hatch for touching a single sink without doing that.
//
// Status is read live from `GET /flows/{flowId}/sink-status` on a 10s poll —
// this is a direct cluster read, not the cached `last_status` field the old
// version of this tab polled, which could say RUNNING forever after a sink
// had actually failed. One entry comes back per kc/kafka_kc block in the
// flow, whether or not it has a sync record, so cards are driven from that
// array rather than from `flow.blocks` joined against a separate sync list.

import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { AdapterChip } from "@/components/AdapterChip";
import { StatusBadge } from "@/components/StatusBadge";
import { Button } from "@/components/ui/button";
import { EmptyState } from "@/components/ui/empty-state";
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
import { cn } from "@/lib/utils";
import {
  deleteKafkaConnectSync,
  flowSinkAction,
  getFlowSinkStatus,
  listFlows,
  retireKafkaConnectSync,
} from "@/prototype/api";
import type { Flow, FlowRuntime } from "@/prototype/types";
import { kafkaConnectSyncDeleteImpact, sinkVerbsAvailable, type SinkVerb } from "@/prototype/kafkaConnectSyncUi";
import { Pause, Play, RefreshCw, RotateCw, Square, Trash2, type LucideIcon } from "lucide-react";

export interface SyncTabProps {
  flow: Flow;
  /** Reason this flow is locked by a queued bulk operation, or null. */
  queueLockReason: string | null;
  /** Opens this flow in the flow builder. */
  onEdit: () => void;
  /** When rendered inside a block form, show only that block's sink. */
  blockId?: string | null;
}

const VERB_ICON: Record<SinkVerb, LucideIcon> = {
  start: Play,
  stop: Square,
  pause: Pause,
  resume: Play,
  restart: RotateCw,
};

const VERB_LABEL: Record<SinkVerb, string> = {
  start: "Start",
  stop: "Stop",
  pause: "Pause",
  resume: "Resume",
  restart: "Restart",
};

/** A disabled action with its reason surfaced as a tooltip — the local stand-in
 * for Flows.tsx's GuardedActionButton, which is not exported from that page. */
function GuardedButton({
  reason,
  className,
  ...props
}: { reason: string | null } & React.ComponentProps<typeof Button>) {
  return (
    <span title={reason ?? undefined} className="inline-flex">
      <Button {...props} className={className} disabled={Boolean(reason) || props.disabled} />
    </span>
  );
}

/** A lightweight stand-in for the sync record — all the delete path needs is
 *  the sync id (to call retire/delete) and a name to show in the confirm
 *  dialog, and the sink-status entry already carries both. */
interface DeleteTarget {
  id: string;
  name: string;
}

export function SyncTab({ flow, queueLockReason, onEdit, blockId = null }: SyncTabProps): JSX.Element {
  const qc = useQueryClient();
  const [deleteTarget, setDeleteTarget] = useState<DeleteTarget | null>(null);

  const hasSinkBlocks = flow.blocks.some((block) => block.adapter === "kc" || block.adapter === "kafka_kc");

  const statusQuery = useQuery({
    queryKey: ["flowSinkStatus", flow.id],
    queryFn: () => getFlowSinkStatus(flow.id),
    refetchInterval: 10000,
    enabled: hasSinkBlocks,
  });
  const flowsQuery = useQuery({ queryKey: ["flows"], queryFn: listFlows });

  const sinks = statusQuery.data?.sinks ?? [];
  const visibleSinks = blockId ? sinks.filter((sink) => sink.blockId === blockId) : sinks;
  const reachable = statusQuery.data?.reachable ?? false;
  const flows = flowsQuery.data ?? [];
  // Read whatever the Runtime tab may already have fetched, but never start
  // that read here: it is a live NiFi call measured at ~3 minutes, and the
  // throughput counters it adds are not worth blocking the tab or hammering
  // NiFi for. Same cache-only pattern the Overview tab uses. The sink-status
  // endpoint above does not carry record counts (Connect-only counters,
  // deliberately separate from NiFi FlowFile counts), so this is the only
  // source for them.
  const runtime = qc.getQueryData<FlowRuntime>(["flow-runtime", flow.id]);

  const afterMutation = () => {
    qc.invalidateQueries({ queryKey: ["flowSinkStatus", flow.id] });
    qc.invalidateQueries({ queryKey: ["flow-runtime", flow.id] });
    qc.invalidateQueries({ queryKey: ["audit"] });
  };

  const runtimeAction = useMutation({
    mutationFn: ({ blockId, verb }: { blockId: string; verb: SinkVerb }) => flowSinkAction(flow.id, blockId, verb),
    onSuccess: (_res, vars) => {
      afterMutation();
      toast.success(`Sent ${VERB_LABEL[vars.verb]}`);
    },
    onError: (e: Error) => toast.error("Runtime action failed", { description: e.message }),
  });

  const refreshStatus = useMutation({
    mutationFn: (blockId: string) => flowSinkAction(flow.id, blockId, "refresh"),
    onSuccess: () => {
      afterMutation();
      toast.success("Status refreshed");
    },
    onError: (e: Error) => toast.error("Could not refresh status", { description: e.message }),
  });

  const deleteSync = useMutation({
    mutationFn: async (id: string) => {
      // The backend refuses to delete a sync that is not retired, so retire it
      // here first, silently, as one step — there is no separate Retire button
      // in this tab for the user to remember to click before deleting.
      await retireKafkaConnectSync(id);
      await deleteKafkaConnectSync(id);
    },
    onSuccess: () => {
      afterMutation();
      setDeleteTarget(null);
      toast.success("Sync deleted");
    },
    onError: (e: Error) => toast.error("Could not delete sync", { description: e.message }),
  });

  if (!hasSinkBlocks) {
    return <EmptyState inline>{blockId ? "This block has no Kafka Connect sink." : "No Kafka Connect sinks in this flow."}</EmptyState>;
  }

  const deleteImpact = deleteTarget ? kafkaConnectSyncDeleteImpact(deleteTarget, flows) : null;
  const otherDeployed = deleteImpact?.deployed.filter((f) => f.id !== flow.id) ?? [];
  const otherUndeployed = deleteImpact?.undeployed.filter((f) => f.id !== flow.id) ?? [];

  // Deletion keeps its own gate exactly as before: a deployed flow's sink is
  // never cut off underneath the running flow.
  const deployedReason = flow.deployedAt
    ? "Undeploy the flow first — the backend refuses to delete a sync while its flow is deployed, so the live sink is never cut off underneath you."
    : null;
  const deleteReason = deployedReason ?? queueLockReason;

  return (
    <div className="space-y-3">
      <div>
        <h3 className="text-sm font-medium">Kafka Connect sinks</h3>
        <p className="text-xs text-muted-foreground">
          Per-sink operational control — pause, resume, restart or delete one connector at a time. Configuration
          lives in the flow builder.
        </p>
      </div>

      {!statusQuery.data && !statusQuery.isError && (
        <p className="text-xs text-muted-foreground">Loading live status…</p>
      )}
      {statusQuery.isError && (
        <p className="text-xs text-destructive">
          Could not load live status: {(statusQuery.error as Error).message}
        </p>
      )}

      <div className="space-y-2">
        {visibleSinks.length === 0 && statusQuery.data && (
          <EmptyState inline>{blockId ? "No runtime sink status is available for this block." : "No sink status is available."}</EmptyState>
        )}
        {visibleSinks.map((sink) => {
          const runtimeBusy = runtimeAction.isPending && runtimeAction.variables?.blockId === sink.blockId;
          const refreshBusy = refreshStatus.isPending && refreshStatus.variables === sink.blockId;
          const deleteBusy = deleteSync.isPending && sink.syncId != null && deleteTarget?.id === sink.syncId;
          const busy = runtimeBusy || refreshBusy || deleteBusy;

          // The endpoint guarantees `state` is null exactly when `reachable`
          // is false — never trust a stale per-sink state once the cluster
          // read itself failed.
          const verbs = reachable ? sinkVerbsAvailable(sink.state) : [];
          const connector = runtime?.connectors.find((c) => c.blockId === sink.blockId);

          return (
            <div key={sink.blockId} className={cn("rounded-md border p-2.5 shadow-sm", !reachable && "opacity-60")}>
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="flex min-w-0 flex-wrap items-center gap-2">
                  <AdapterChip adapter={sink.adapter} />
                  <span className="truncate text-sm font-medium">{sink.blockName}</span>
                </div>
                {!reachable ? (
                  <span className="text-xs text-muted-foreground">Status unavailable</span>
                ) : (
                  <StatusBadge status={sink.state === "UNDEPLOYED" ? "Undeployed" : sink.state ?? "Unknown"} />
                )}
              </div>

              <div className="mt-0.5 break-all font-mono text-xs text-muted-foreground">
                {sink.connectorName || "—"} · {sink.connectorClass || "no connector class"}
              </div>

              {connector && (
                <div className="mt-1 flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
                  <span>
                    records sent <span className="font-mono text-foreground">{connector.recordsSent.toLocaleString()}</span>
                  </span>
                  <span>
                    failed{" "}
                    <span className={cn("font-mono", connector.recordsFailed > 0 ? "text-destructive" : "text-foreground")}>
                      {connector.recordsFailed.toLocaleString()}
                    </span>
                  </span>
                </div>
              )}

              {sink.tasks.length > 0 && (
                <div className="mt-2 space-y-1">
                  {sink.tasks.map((task) => (
                    <div key={task.id} className="flex flex-wrap items-center gap-2 rounded-md border px-2 py-1 text-xs">
                      <span className="font-mono">task {task.id}</span>
                      <StatusBadge status={task.state} />
                      <span className="text-muted-foreground">{task.workerId}</span>
                    </div>
                  ))}
                </div>
              )}

              {sink.lastErrorTrace && (
                <div className="mt-2">
                  <div className="text-xs font-medium">Last error (truncated)</div>
                  <pre className="mt-1 max-h-52 overflow-auto rounded-md bg-muted/60 p-2 font-mono text-xs shadow-inner">
                    {sink.lastErrorTrace}
                  </pre>
                </div>
              )}

              {!reachable && (
                <p className="mt-2 text-xs text-muted-foreground">
                  Status unavailable — could not reach Kafka Connect.
                </p>
              )}

              {reachable && sink.state === "UNDEPLOYED" && (
                <p className="mt-2 text-xs text-muted-foreground">
                  This connector does not exist on the cluster yet — Start will create it from this block's stored
                  configuration.{" "}
                  <Button variant="link" size="sm" className="h-auto p-0 text-xs" onClick={onEdit}>
                    Open flow builder
                  </Button>
                </p>
              )}

              <div className="mt-2 flex flex-wrap gap-2 border-t pt-2">
                {reachable &&
                  verbs.map((verb) => {
                    const Icon = VERB_ICON[verb];
                    return (
                      <GuardedButton
                        key={verb}
                        reason={queueLockReason}
                        size="sm"
                        variant="outline"
                        disabled={busy}
                        onClick={() => runtimeAction.mutate({ blockId: sink.blockId, verb })}
                      >
                        <Icon className="mr-1.5 h-3.5 w-3.5" />
                        {VERB_LABEL[verb]}
                      </GuardedButton>
                    );
                  })}
                {sink.syncId && (
                  <GuardedButton
                    reason={queueLockReason}
                    size="sm"
                    variant="ghost"
                    disabled={busy}
                    onClick={() => refreshStatus.mutate(sink.blockId)}
                  >
                    <RefreshCw className="mr-1.5 h-3.5 w-3.5" />
                    Refresh status
                  </GuardedButton>
                )}
                {sink.syncId && (
                  <GuardedButton
                    reason={deleteReason}
                    size="sm"
                    variant="outline"
                    className="text-destructive hover:text-destructive"
                    disabled={busy}
                    onClick={() => setDeleteTarget({ id: sink.syncId as string, name: sink.connectorName || sink.blockName })}
                  >
                    <Trash2 className="mr-1.5 h-3.5 w-3.5" />
                    Delete
                  </GuardedButton>
                )}
              </div>
            </div>
          );
        })}
      </div>

      <AlertDialog open={Boolean(deleteTarget)} onOpenChange={(open) => !open && setDeleteTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete “{deleteTarget?.name}” permanently?</AlertDialogTitle>
            <AlertDialogDescription>
              This removes the real connector from the Kafka Connect cluster and is not reversible.
            </AlertDialogDescription>
          </AlertDialogHeader>
          {(otherDeployed.length > 0 || otherUndeployed.length > 0) && (
            <div className="rounded-md border border-destructive/30 bg-destructive/5 p-3 text-sm">
              {otherDeployed.length > 0 && (
                <div className="space-y-1">
                  <div className="font-medium text-destructive">Other deployed flows also reference this sync.</div>
                  <ul className="list-disc pl-5 text-xs text-muted-foreground">
                    {otherDeployed.map((f) => <li key={f.id}>{f.name}</li>)}
                  </ul>
                </div>
              )}
              {otherUndeployed.length > 0 && (
                <div className="mt-2 space-y-1">
                  <div className="font-medium text-destructive">Other flows also reference this sync.</div>
                  <ul className="list-disc pl-5 text-xs text-muted-foreground">
                    {otherUndeployed.map((f) => <li key={f.id}>{f.name}</li>)}
                  </ul>
                </div>
              )}
            </div>
          )}
          <AlertDialogFooter>
            <AlertDialogCancel disabled={deleteSync.isPending}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              disabled={deleteSync.isPending}
              onClick={() => deleteTarget && deleteSync.mutate(deleteTarget.id)}
            >
              Delete permanently
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
