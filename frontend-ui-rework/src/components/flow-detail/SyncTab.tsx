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
// Data is read from queries the Flows page already populates (`kafkaConnectSyncs`,
// `flows`, `flow-runtime/:id`), so mounting this tab costs no extra round trip.

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
  kafkaConnectSyncAction,
  listFlows,
  listKafkaConnectSyncs,
  refreshKafkaConnectSyncStatus,
  retireKafkaConnectSync,
  type KafkaConnectSync,
} from "@/prototype/api";
import type { ConnectConnectorRuntime, Flow, FlowRuntime } from "@/prototype/types";
import {
  kafkaConnectSyncDeleteImpact,
  runtimeControlsAvailable,
  syncConfigurationLabel,
} from "@/prototype/kafkaConnectSyncUi";
import { Pause, Play, RefreshCw, RotateCw, Square, Trash2 } from "lucide-react";

export interface SyncTabProps {
  flow: Flow;
  /** Reason this flow is locked by a queued bulk operation, or null. */
  queueLockReason: string | null;
  /** Opens this flow in the flow builder. */
  onEdit: () => void;
}

// Mirrors KafkaConnect.tsx's `runtimeStatus()` — `lastStatus` is typed as an
// opaque `Record<string, unknown>` on the wire, so this is the one place that
// narrows it, rather than trusting the shape everywhere it is read.
type RuntimeStatus = {
  connector?: { state?: string; trace?: string };
  tasks?: Array<{ id?: number; state?: string; worker_id?: string; trace?: string }>;
};

function runtimeStatusOf(sync: KafkaConnectSync | undefined): RuntimeStatus | null {
  return (sync?.lastStatus as RuntimeStatus | null) ?? null;
}

type NormalizedTask = { key: string; id: number | string; state: string; workerId: string };

function normalizeTasks(sync: KafkaConnectSync | undefined, connector: ConnectConnectorRuntime | undefined): NormalizedTask[] {
  const statusTasks = runtimeStatusOf(sync)?.tasks;
  if (statusTasks) {
    return statusTasks.map((task, index) => ({
      key: String(task.id ?? index),
      id: task.id ?? index,
      state: task.state ?? "UNKNOWN",
      workerId: task.worker_id ?? "",
    }));
  }
  return (connector?.tasks ?? []).map((task) => ({
    key: String(task.id),
    id: task.id,
    state: task.state,
    workerId: task.workerId,
  }));
}

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

export function SyncTab({ flow, queueLockReason, onEdit }: SyncTabProps): JSX.Element {
  const qc = useQueryClient();
  const [deleteTarget, setDeleteTarget] = useState<KafkaConnectSync | null>(null);

  const syncsQuery = useQuery({ queryKey: ["kafkaConnectSyncs"], queryFn: listKafkaConnectSyncs, refetchInterval: 10000 });
  const flowsQuery = useQuery({ queryKey: ["flows"], queryFn: listFlows });

  const syncs = syncsQuery.data ?? [];
  const flows = flowsQuery.data ?? [];
  // Read whatever the Runtime tab may already have fetched, but never start
  // that read here: it is a live NiFi call measured at ~3 minutes, and the
  // throughput counters it adds are not worth blocking the tab or hammering
  // NiFi for. Same cache-only pattern the Overview tab uses.
  const runtime = qc.getQueryData<FlowRuntime>(["flow-runtime", flow.id]);

  const sinkBlocks = flow.blocks.filter((block) => block.adapter === "kc" || block.adapter === "kafka_kc");

  const afterMutation = () => {
    qc.invalidateQueries({ queryKey: ["kafkaConnectSyncs"] });
    qc.invalidateQueries({ queryKey: ["flow-runtime", flow.id] });
    qc.invalidateQueries({ queryKey: ["audit"] });
  };

  const runtimeAction = useMutation({
    mutationFn: ({ id, verb }: { id: string; verb: "pause" | "resume" | "restart" | "start" | "stop" }) =>
      kafkaConnectSyncAction(id, verb),
    onSuccess: (_sync, vars) => {
      afterMutation();
      toast.success(`Sent ${vars.verb}`);
    },
    onError: (e: Error) => toast.error("Runtime action failed", { description: e.message }),
  });

  const refreshStatus = useMutation({
    mutationFn: (id: string) => refreshKafkaConnectSyncStatus(id),
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

  if (sinkBlocks.length === 0) {
    return <EmptyState inline>No Kafka Connect sinks in this flow.</EmptyState>;
  }

  const deleteImpact = deleteTarget ? kafkaConnectSyncDeleteImpact(deleteTarget, flows) : null;
  const otherDeployed = deleteImpact?.deployed.filter((f) => f.id !== flow.id) ?? [];
  const otherUndeployed = deleteImpact?.undeployed.filter((f) => f.id !== flow.id) ?? [];

  return (
    <div className="space-y-3">
      <div>
        <h3 className="text-sm font-medium">Kafka Connect sinks</h3>
        <p className="text-xs text-muted-foreground">
          Per-sink operational control — pause, resume, restart or delete one connector at a time. Configuration
          lives in the flow builder.
        </p>
      </div>
      <div className="space-y-2">
        {sinkBlocks.map((block) => {
          const syncId = typeof block.config?.syncId === "string" ? (block.config.syncId as string) : undefined;
          const sync = syncs.find((s) => s.id === syncId);
          const connector = runtime?.connectors.find((c) => c.blockId === block.id);
          const rawState = runtimeStatusOf(sync)?.connector?.state ?? connector?.state ?? null;
          const hasKnownState = Boolean(rawState);
          const state = rawState ? String(rawState).toUpperCase() : null;
          const tasks = normalizeTasks(sync, connector);
          const errorTrace = runtimeStatusOf(sync)?.connector?.trace || connector?.lastErrorTrace;
          const controlsAvailable = sync ? runtimeControlsAvailable(sync) : false;
          const busyId =
            runtimeAction.isPending ? runtimeAction.variables?.id
            : refreshStatus.isPending ? refreshStatus.variables
            : deleteSync.isPending ? deleteSync.variables
            : undefined;
          const busy = Boolean(sync) && busyId === sync!.id;

          const deployedReason = flow.deployedAt
            ? "Undeploy the flow first — the backend refuses to delete a sync while its flow is deployed, so the live sink is never cut off underneath you."
            : null;
          const deleteReason = deployedReason ?? queueLockReason;

          return (
            <div key={block.id} className="rounded-md border p-2.5 shadow-sm">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="flex min-w-0 flex-wrap items-center gap-2">
                  <AdapterChip adapter={block.adapter} />
                  <span className="truncate text-sm font-medium">{block.name}</span>
                </div>
                {hasKnownState ? (
                  <StatusBadge status={state as string} />
                ) : (
                  <span className="text-xs text-muted-foreground">No state reported</span>
                )}
              </div>

              <div className="mt-1 text-xs text-muted-foreground">{sync ? sync.name : "Not managed yet"}</div>
              {sync && (
                <div className="mt-0.5 break-all font-mono text-xs text-muted-foreground">
                  {sync.connectorName || "—"} · {sync.connectorClass || "no connector class"}
                </div>
              )}

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

              {tasks.length > 0 && (
                <div className="mt-2 space-y-1">
                  {tasks.map((task) => (
                    <div key={task.key} className="flex flex-wrap items-center gap-2 rounded-md border px-2 py-1 text-xs">
                      <span className="font-mono">task {task.id}</span>
                      <StatusBadge status={task.state} />
                      <span className="text-muted-foreground">{task.workerId}</span>
                    </div>
                  ))}
                </div>
              )}

              {sync?.lastError && <p className="mt-2 text-xs text-destructive">{sync.lastError}</p>}
              {errorTrace && (
                <div className="mt-2">
                  <div className="text-xs font-medium">Last error (truncated)</div>
                  <pre className="mt-1 max-h-52 overflow-auto rounded-md bg-muted/60 p-2 font-mono text-xs shadow-inner">
                    {errorTrace}
                  </pre>
                </div>
              )}

              {!sync && (
                <p className="mt-2 text-xs text-muted-foreground">
                  This sink is not managed yet — open the flow builder to create a managed sync for it.{" "}
                  <Button variant="link" size="sm" className="h-auto p-0 text-xs" onClick={onEdit}>
                    Open flow builder
                  </Button>
                </p>
              )}

              {sync && !controlsAvailable && (
                sync.configurationState === "changes_pending" ? (
                  <p className="mt-2 text-xs text-muted-foreground">
                    This sink's settings changed since they were last applied — redeploy the flow to push them.{" "}
                    <Button variant="link" size="sm" className="h-auto p-0 text-xs" onClick={onEdit}>
                      Open flow builder
                    </Button>
                  </p>
                ) : (
                  <p className="mt-2 text-xs text-muted-foreground">
                    Runtime controls unavailable — {syncConfigurationLabel(sync)}.
                  </p>
                )
              )}

              {sync && (
                <div className="mt-2 flex flex-wrap gap-2 border-t pt-2">
                  {controlsAvailable && (
                    <>
                      {(!hasKnownState || state === "RUNNING") && (
                        <Button
                          size="sm"
                          variant="outline"
                          disabled={busy}
                          onClick={() => runtimeAction.mutate({ id: sync.id, verb: "pause" })}
                        >
                          <Pause className="mr-1.5 h-3.5 w-3.5" />Pause
                        </Button>
                      )}
                      {state === "PAUSED" && (
                        <Button
                          size="sm"
                          variant="outline"
                          disabled={busy}
                          onClick={() => runtimeAction.mutate({ id: sync.id, verb: "resume" })}
                        >
                          <Play className="mr-1.5 h-3.5 w-3.5" />Resume
                        </Button>
                      )}
                      {state === "STOPPED" && (
                        <Button
                          size="sm"
                          variant="outline"
                          disabled={busy}
                          onClick={() => runtimeAction.mutate({ id: sync.id, verb: "start" })}
                        >
                          <Play className="mr-1.5 h-3.5 w-3.5" />Start
                        </Button>
                      )}
                      {state !== "STOPPED" && (
                        <Button
                          size="sm"
                          variant="outline"
                          disabled={busy}
                          onClick={() => runtimeAction.mutate({ id: sync.id, verb: "stop" })}
                        >
                          <Square className="mr-1.5 h-3.5 w-3.5" />Stop
                        </Button>
                      )}
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={busy}
                        onClick={() => runtimeAction.mutate({ id: sync.id, verb: "restart" })}
                      >
                        <RotateCw className="mr-1.5 h-3.5 w-3.5" />Restart
                      </Button>
                    </>
                  )}
                  <Button
                    size="sm"
                    variant="ghost"
                    disabled={busy}
                    onClick={() => refreshStatus.mutate(sync.id)}
                  >
                    <RefreshCw className="mr-1.5 h-3.5 w-3.5" />Refresh status
                  </Button>
                  <GuardedButton
                    reason={deleteReason}
                    size="sm"
                    variant="outline"
                    className="text-destructive hover:text-destructive"
                    onClick={() => setDeleteTarget(sync)}
                  >
                    <Trash2 className="mr-1.5 h-3.5 w-3.5" />Delete
                  </GuardedButton>
                </div>
              )}
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
