import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { CheckCircle2, ChevronDown, ChevronUp, CircleX, Clock3, Loader2, X } from "lucide-react";
import { toast } from "sonner";

import { getBulkQueue, cancelBulkItem, isBulkJobTerminal, type BulkJob, type BulkJobItem } from "@/prototype/api";
import { Progress } from "@/components/ui/progress";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

const labelFor = (job: BulkJob, item: BulkJobItem) => `${job.verb.replace("_", " ")} — ${item.flowName}`;

type QueueEntry = { job: BulkJob; item: BulkJobItem; index: number };

export function FlowOperationQueue() {
  const qc = useQueryClient();
  const [collapsed, setCollapsed] = useState(false);
  const [dismissedIds, setDismissedIds] = useState<Set<string>>(() => {
    try {
      const saved = window.localStorage.getItem("flowOperationQueue.dismissed");
      return new Set(saved ? (JSON.parse(saved) as string[]) : []);
    } catch {
      return new Set();
    }
  });
  const { data: jobs = [] } = useQuery({
    queryKey: ["flowOperationQueue"],
    queryFn: getBulkQueue,
    refetchInterval: (query) => {
      const list = query.state.data ?? [];
      return list.some((job) => !isBulkJobTerminal(job)) ? 1200 : 5000;
    },
  });
  const cancel = useMutation({
    mutationFn: ({ jobId, itemId }: { jobId: string; itemId: string }) => cancelBulkItem(jobId, itemId),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["flowOperationQueue"] }),
    onError: (error: Error) => toast.error("Could not cancel operation", { description: error.message }),
  });

  const visibleItems = useMemo<QueueEntry[]>(() => {
    const isTerminalItem = (job: BulkJob, item: BulkJobItem) =>
      ["succeeded", "failed", "cancelled", "skipped"].includes(item.status) || isBulkJobTerminal(job);
    const isDismissed = (job: BulkJob, item: BulkJobItem) =>
      isTerminalItem(job, item) && dismissedIds.has(`${job.id}:${item.id}`);
    const active = jobs
      .filter((job) => !isBulkJobTerminal(job))
      .flatMap((job) => job.items.map((item, index) => ({ job, item, index })))
      .filter(({ item }) => ["pending", "running", "cancelled", "failed", "succeeded"].includes(item.status))
      .filter(({ job, item }) => !isDismissed(job, item))
      .sort((a, b) => `${a.job.createdAt}:${a.index}`.localeCompare(`${b.job.createdAt}:${b.index}`));
    const recent = jobs
      .filter(isBulkJobTerminal)
      .slice(0, 3)
      .flatMap((job) => job.items.map((item, index) => ({ job, item, index })))
      .filter(({ job, item }) => !isDismissed(job, item))
      .slice(-6)
      .reverse();
    return [...active, ...recent];
  }, [dismissedIds, jobs]);

  // Depend on the terminal jobs themselves, not just a boolean. The queue can
  // contain old completed history, so a boolean would fail to refresh flows
  // when a newer queued operation finishes.
  const finishedJobSignature = jobs
    .filter((job) => isBulkJobTerminal(job))
    .map((job) => `${job.id}:${job.status}:${job.updatedAt}`)
    .sort()
    .join("|");
  useEffect(() => {
    if (finishedJobSignature) void qc.invalidateQueries({ queryKey: ["flows"] });
  }, [finishedJobSignature, qc]);

  const dismiss = (job: BulkJob, item: BulkJobItem) => {
    const id = `${job.id}:${item.id}`;
    setDismissedIds((previous) => {
      const next = new Set(previous);
      next.add(id);
      try {
        window.localStorage.setItem("flowOperationQueue.dismissed", JSON.stringify([...next]));
      } catch {
        // Dismissal still works for this session if storage is unavailable.
      }
      return next;
    });
  };

  if (visibleItems.length === 0) return null;

  return (
    <div className="fixed bottom-4 right-4 z-50 w-[min(380px,calc(100vw-2rem))]">
      <Card className="border-border/80 bg-card/95 shadow-xl backdrop-blur">
        <CardHeader className="pb-2">
          <CardTitle className="flex items-center gap-2 text-sm">
            <Clock3 className="h-4 w-4" />
            <button
              type="button"
              className="flex min-w-0 flex-1 items-center gap-2 text-left"
              aria-expanded={!collapsed}
              aria-label={collapsed ? "Expand flow operations" : "Collapse flow operations"}
              onClick={() => setCollapsed((value) => !value)}
            >
              <span>Flow operations</span>
            </button>
            <span className="ml-auto text-xs font-normal text-muted-foreground">
              {visibleItems.filter(({ item }) => item.status === "running" || item.status === "pending").length} active
            </span>
            <Button
              variant="ghost"
              size="icon"
              className="h-7 w-7 shrink-0"
              aria-label={collapsed ? "Expand flow operations" : "Collapse flow operations"}
              onClick={() => setCollapsed((value) => !value)}
            >
              {collapsed ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
            </Button>
          </CardTitle>
        </CardHeader>
        {!collapsed && <CardContent className="max-h-[min(60vh,420px)] space-y-2 overflow-y-auto pt-0">
          {visibleItems.map(({ job, item, index }) => {
            const terminal = ["succeeded", "failed", "cancelled", "skipped"].includes(item.status) || isBulkJobTerminal(job);
            const running = item.status === "running";
            const cancelled = item.status === "cancelled";
            const label = labelFor(job, item);
            return (
              <div key={`${job.id}:${item.id || index}`} className="rounded-md border bg-background/70 p-2 text-xs">
                <div className="flex items-center gap-2">
                  {running ? <Loader2 className="h-3.5 w-3.5 animate-spin text-primary" /> : null}
                  {item.status === "pending" ? <Clock3 className="h-3.5 w-3.5 text-muted-foreground" /> : null}
                  {item.status === "succeeded" ? <CheckCircle2 className="h-3.5 w-3.5 text-success" /> : null}
                  {item.status === "failed" ? <CircleX className="h-3.5 w-3.5 text-destructive" /> : null}
                  {cancelled ? <X className="h-3.5 w-3.5 text-muted-foreground" /> : null}
                  <span className="min-w-0 flex-1 truncate font-medium">{label}</span>
                  {!terminal && item.cancellable !== false && item.status === "pending" ? (
                    <Button
                      variant="ghost"
                      size="icon"
                      className="h-6 w-6"
                      aria-label={`Cancel ${label}`}
                      disabled={cancel.isPending}
                      onClick={() => cancel.mutate({ jobId: job.id, itemId: item.id })}
                    >
                      <X className="h-3.5 w-3.5" />
                    </Button>
                  ) : null}
                  {terminal ? (
                    <Button
                      variant="ghost"
                      size="icon"
                      className="h-6 w-6 shrink-0"
                      aria-label={`Dismiss ${label}`}
                      onClick={() => dismiss(job, item)}
                    >
                      <X className="h-3.5 w-3.5" />
                    </Button>
                  ) : null}
                </div>
                <div className="mt-1 flex items-center justify-between text-muted-foreground">
                  <span>{terminal ? item.status : item.status === "running" ? "Running" : "Queued"}</span>
                  {!terminal ? <span>{index + 1}/{job.total}</span> : null}
                </div>
                {!terminal ? <Progress value={item.status === "running" ? 50 : 0} className="mt-1 h-1.5" /> : null}
                {item.error ? <p className="mt-1 line-clamp-2 text-destructive">{item.error}</p> : null}
              </div>
            );
          })}
        </CardContent>}
      </Card>
    </div>
  );
}
