// Dashboard — adapter-model KPIs over the mock prototype layer.
// No polling: a manual Refresh button re-fetches everything.

import { useMemo, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { AppLayout } from "@/components/AppLayout";
import { AdapterChip } from "@/components/AdapterChip";
import { StatusBadge } from "@/components/StatusBadge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";
import { timeAgo } from "@/lib/api";
import {
  getDashboardSummary,
  listAudit,
  listFlows,
  listServices,
  serviceUpdateAvailable,
  validateFlowNow,
} from "@/prototype/api";
import type { AppService, AuditEvent, Flow, FlowBlock } from "@/prototype/types";
import {
  Activity,
  AlertTriangle,
  ArrowUpRight,
  ChevronRight,
  Loader2,
  Plug,
  PlugZap,
  RefreshCw,
  ShieldCheck,
  Workflow,
} from "lucide-react";

/** Root block: parentId === null, else the kafka read attached to an adopted topic. */
function rootBlockOf(flow: Flow): FlowBlock | null {
  return (
    flow.blocks.find((b) => b.parentId === null) ??
    flow.blocks.find((b) => flow.topics.some((t) => t.kind === "adopted" && t.id === b.parentId)) ??
    null
  );
}

const statusDotClass = (status: string) => {
  const s = status.toLowerCase();
  if (s === "failed" || s === "error") return "bg-destructive";
  if (s === "warning") return "bg-warning";
  if (s === "success") return "bg-success";
  return "bg-info";
};

interface AttentionRow {
  flowId: string;
  flowName: string;
  reasons: string[];
}

function computeAttention(flows: Flow[], services: AppService[]): AttentionRow[] {
  const rows: AttentionRow[] = [];
  for (const flow of flows) {
    const reasons: string[] = [];
    if (flow.state === "Draft") {
      const issues = validateFlowNow(flow);
      reasons.push(
        issues.length > 0
          ? `Draft — has validation issues (${issues.length})`
          : "Draft — not yet deployed",
      );
    }
    // Drift is not a property of being Degraded. An out-of-band property edit
    // leaves a flow Stopped or Running and still drifted, so gating this on the
    // state hid exactly the findings the Flows list and the Runtime tab show.
    if (flow.drift) reasons.push(flow.drift);
    const retired = services.filter(
      (svc) =>
        svc.retired &&
        (flow.servicePins[svc.id] !== undefined ||
          flow.blocks.some((b) => b.serviceId === svc.id || b.config.sinkServiceId === svc.id)),
    );
    for (const svc of retired) reasons.push(`Action required — retired service "${svc.name}"`);
    const updates = serviceUpdateAvailable(flow, services);
    if (updates.length > 0)
      reasons.push(
        `Service update available — ${updates.map((s) => `${s.name} rev ${s.revision}`).join(", ")} (adopts at next deploy)`,
      );
    if (reasons.length > 0) rows.push({ flowId: flow.id, flowName: flow.name, reasons });
  }
  return rows;
}

interface ActivityDay {
  key: string;
  label: string;
  success: number;
  warning: number;
  failed: number;
  total: number;
}

function dayKey(date: Date): string {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function buildActivityTrend(audit: AuditEvent[]): ActivityDay[] {
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const days = Array.from({ length: 7 }, (_, index) => {
    const date = new Date(today);
    date.setDate(today.getDate() - (6 - index));
    return {
      key: dayKey(date),
      label: new Intl.DateTimeFormat(undefined, { weekday: "short" }).format(date),
      success: 0,
      warning: 0,
      failed: 0,
      total: 0,
    };
  });
  const byKey = new Map(days.map((day) => [day.key, day]));
  for (const event of audit) {
    const timestamp = new Date(event.ts);
    if (Number.isNaN(timestamp.getTime())) continue;
    const day = byKey.get(dayKey(timestamp));
    if (!day) continue;
    day.total += 1;
    if (event.status === "Failed") day.failed += 1;
    else if (event.status === "Warning") day.warning += 1;
    else day.success += 1;
  }
  return days;
}

function ActivityTrendCard({ days }: { days: ActivityDay[] }) {
  const maxTotal = Math.max(1, ...days.map((day) => day.total));
  const hasActivity = days.some((day) => day.total > 0);
  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle>Operations trend</CardTitle>
        <CardDescription>Audit activity over the last 7 days</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex h-36 items-end gap-2" role="img" aria-label="Audit activity for the last seven days">
          {days.map((day) => {
            const successBottom = ((day.success + day.warning) / maxTotal) * 100;
            const warningBottom = (day.success / maxTotal) * 100;
            return (
              <div key={day.key} className="flex min-w-0 flex-1 flex-col items-center gap-1.5" title={`${day.label}: ${day.total} operation${day.total === 1 ? "" : "s"}`}>
                <span className="text-2xs font-medium tabular-nums text-muted-foreground">{day.total || ""}</span>
                <div className="relative h-24 w-full max-w-9 rounded-md bg-muted/60">
                  {day.success > 0 && <div className="absolute inset-x-0 bottom-0 rounded-b-md bg-success" style={{ height: `${(day.success / maxTotal) * 100}%` }} />}
                  {day.warning > 0 && <div className="absolute inset-x-0 rounded-sm bg-warning" style={{ bottom: `${warningBottom}%`, height: `${(day.warning / maxTotal) * 100}%` }} />}
                  {day.failed > 0 && <div className="absolute inset-x-0 rounded-t-md bg-destructive" style={{ bottom: `${successBottom}%`, height: `${(day.failed / maxTotal) * 100}%` }} />}
                </div>
                <span className="text-[10px] text-muted-foreground">{day.label}</span>
              </div>
            );
          })}
        </div>
        <div className="mt-3 flex flex-wrap gap-x-4 gap-y-1 text-2xs text-muted-foreground">
          <span><i className="mr-1 inline-block h-2 w-2 rounded-sm bg-success" />Success</span>
          <span><i className="mr-1 inline-block h-2 w-2 rounded-sm bg-warning" />Warning</span>
          <span><i className="mr-1 inline-block h-2 w-2 rounded-sm bg-destructive" />Failed</span>
          {!hasActivity && <span className="ml-auto">No activity recorded</span>}
        </div>
      </CardContent>
    </Card>
  );
}

const Dashboard = () => {
  const qc = useQueryClient();
  const [showAllAttention, setShowAllAttention] = useState(false);

  const { data: summary, isLoading: sumLoading, isFetching: sumFetching } = useQuery({
    queryKey: ["dashboard"],
    queryFn: getDashboardSummary,
    refetchInterval: 10000,
  });
  const { data: flows = [], isFetching: flowsFetching } = useQuery({
    queryKey: ["flows"],
    queryFn: listFlows,
  });
  const { data: audit = [], isFetching: auditFetching } = useQuery({
    queryKey: ["audit", ""],
    queryFn: () => listAudit(),
  });
  const { data: services = [], isFetching: servicesFetching } = useQuery({
    queryKey: ["services"],
    queryFn: listServices,
  });

  const refreshing = sumFetching || flowsFetching || auditFetching || servicesFetching;
  const refresh = () => {
    qc.invalidateQueries({ queryKey: ["dashboard"] });
    qc.invalidateQueries({ queryKey: ["flows"] });
    qc.invalidateQueries({ queryKey: ["audit"] });
    qc.invalidateQueries({ queryKey: ["services"] });
  };

  const connectionsDegraded = !!summary && summary.connectionsHealthy < summary.connectionsTotal;
  const sinksDegraded = !!summary && summary.sinkConnectorsRunning < summary.sinkConnectorsTotal;

  const statDefs = [
    { label: "Flows", value: summary?.totalFlows, icon: Workflow, tone: "text-primary bg-primary-muted" },
    { label: "Running", value: summary?.runningFlows, icon: Activity, tone: "text-success bg-success-muted" },
    { label: "Approved schemas", value: summary?.approvedSchemas, icon: ShieldCheck, tone: "text-primary bg-primary-muted" },
    {
      label: "Connections healthy",
      value: summary ? `${summary.connectionsHealthy}/${summary.connectionsTotal}` : undefined,
      icon: Plug,
      tone: connectionsDegraded ? "text-warning bg-warning-muted" : "text-info bg-info-muted",
      warn: connectionsDegraded,
    },
    {
      label: "Sink connectors",
      value: summary ? `${summary.sinkConnectorsRunning}/${summary.sinkConnectorsTotal}` : undefined,
      icon: PlugZap,
      tone: sinksDegraded ? "text-warning bg-warning-muted" : "text-success bg-success-muted",
      warn: sinksDegraded,
      hint:
        summary && summary.sinkConnectorsUndeployed > 0 ? `${summary.sinkConnectorsUndeployed} not deployed` : undefined,
    },
  ];

  const attention = useMemo(() => {
    const rows = computeAttention(flows, services);
    const priority = (row: AttentionRow) => row.reasons.some((reason) =>
      reason.includes("validation issues") || reason.includes("drift") || reason.includes("Action required"),
    ) ? 0 : 1;
    return rows.sort((left, right) => priority(left) - priority(right) || left.flowName.localeCompare(right.flowName));
  }, [flows, services]);
  const recent = audit.slice(0, 8);
  const visibleFlows = useMemo(() => {
    const priority: Record<string, number> = { Degraded: 0, Running: 1, Paused: 2, Stopped: 3, Draft: 4 };
    return [...flows]
      .sort((left, right) => (priority[left.state] ?? 5) - (priority[right.state] ?? 5) || left.name.localeCompare(right.name))
      .slice(0, 10);
  }, [flows]);
  const visibleAttention = showAllAttention ? attention : attention.slice(0, 6);
  const activityTrend = useMemo(() => buildActivityTrend(audit), [audit]);

  return (
    <AppLayout
      title="Dashboard"
      description="Overview of your data mobility platform"
      actions={
        <Button variant="outline" size="sm" onClick={refresh} disabled={refreshing}>
          {refreshing ? <Loader2 className="animate-spin" /> : <RefreshCw />} Refresh
        </Button>
      }
    >
      {/* KPI stats. The number is the point, so it gets the largest type on the
          page and the label recedes beneath it — the old build set the value at
          the same weight as everything else and led with the icon. */}
      <div className="mb-6 grid grid-cols-2 gap-3 sm:gap-4 lg:grid-cols-5">
        {statDefs.map((s) => (
          <Card key={s.label}>
            <CardContent className="p-4">
              <div className="mb-3 flex items-center justify-between">
                <span className={cn("flex h-7 w-7 items-center justify-center rounded-lg", s.tone)}>
                  <s.icon className="h-3.5 w-3.5" />
                </span>
                {sumLoading && <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground" />}
              </div>
              <div
                className={cn(
                  "text-3xl font-semibold tabular-nums tracking-tight",
                  s.warn && "text-warning",
                )}
              >
                {s.value ?? "—"}
              </div>
              <div className="mt-0.5 text-xs text-muted-foreground">{s.label}</div>
              {"hint" in s && s.hint && <div className="mt-1 text-2xs text-warning">{s.hint}</div>}
            </CardContent>
          </Card>
        ))}
      </div>

      <div className="grid grid-cols-1 items-start gap-4 lg:grid-cols-3">
        <div className="flex min-w-0 flex-col gap-4 lg:col-span-2">
          {/* Flow status */}
          <Card>
          <CardHeader className="flex-row items-center justify-between space-y-0 pb-3">
            <div>
              <CardTitle>Flow status</CardTitle>
              <CardDescription>Operational state of all flows</CardDescription>
            </div>
            <Button variant="ghost" size="xs" asChild>
              <Link to="/flows">
                View all <ArrowUpRight />
              </Link>
            </Button>
          </CardHeader>
          <CardContent>
            {flows.length === 0 ? (
              <div className="flex flex-col items-center justify-center py-10 text-center">
                <div className="mb-3 flex h-11 w-11 items-center justify-center rounded-2xl bg-muted">
                  <Workflow className="h-5 w-5 text-muted-foreground/70" />
                </div>
                <p className="text-sm font-medium">No flows yet</p>
                <Button variant="link" asChild className="mt-1 h-auto p-0 text-xs">
                  <Link to="/flow-builder/new">Create your first flow →</Link>
                </Button>
              </div>
            ) : (
              <div className="-mx-2 max-h-[28rem] overflow-y-auto pr-1">
                {visibleFlows.map((f) => {
                  const root = rootBlockOf(f);
                  return (
                    <Link
                      key={f.id}
                      to={`/flow-builder/${f.id}`}
                      className="group flex items-center justify-between gap-3 rounded-lg px-2 py-2.5 transition-colors hover:bg-accent"
                    >
                      <div className="flex min-w-0 items-center gap-2">
                        <span className="truncate text-sm font-medium">{f.name}</span>
                        {root && <AdapterChip adapter={root.adapter} mode={root.mode} />}
                      </div>
                      <div className="flex shrink-0 items-center gap-2">
                        <StatusBadge status={f.state} />
                        <ChevronRight className="h-3.5 w-3.5 text-muted-foreground opacity-0 transition-opacity group-hover:opacity-100" />
                      </div>
                    </Link>
                  );
                })}
                {visibleFlows.length < flows.length && (
                  <Link to="/flows" className="mt-2 block rounded-lg px-2 py-2 text-center text-xs font-medium text-primary hover:bg-accent">
                    View all {flows.length} flows ({flows.length - visibleFlows.length} more)
                  </Link>
                )}
              </div>
            )}
          </CardContent>
          </Card>

          <ActivityTrendCard days={activityTrend} />
        </div>

        <div className="flex min-w-0 flex-col gap-4">
          {/* Recent activity */}
          <Card>
            <CardHeader className="pb-3">
              <CardTitle>Recent activity</CardTitle>
              <CardDescription>Last admin actions</CardDescription>
            </CardHeader>
            <CardContent>
              {recent.length === 0 ? (
                <p className="py-4 text-center text-sm text-muted-foreground">No activity yet</p>
              ) : (
                <div className="space-y-3">
                  {recent.map((a) => (
                    <div key={a.id} className="flex items-start gap-2.5 text-sm">
                      <div className={cn("mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full", statusDotClass(a.status))} />
                      <div className="min-w-0 flex-1">
                        <div className="truncate text-sm">
                          <span className="font-medium">{a.action}</span>
                          {a.target && <span className="text-muted-foreground"> — {a.target}</span>}
                        </div>
                        <div className="text-2xs text-muted-foreground">
                          {timeAgo(a.ts)} · {a.user}
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>

          {/* Needs attention */}
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="flex items-center gap-2">
                <AlertTriangle className="h-4 w-4 text-warning" /> Needs attention
                {attention.length > 0 && <span className="ml-auto text-xs font-normal text-muted-foreground">{attention.length}</span>}
              </CardTitle>
              <CardDescription>Drafts, drift, and service flags</CardDescription>
            </CardHeader>
            <CardContent>
              {attention.length === 0 ? (
                <p className="py-4 text-center text-sm text-muted-foreground">Nothing needs attention</p>
              ) : (
                <div className="max-h-[28rem] space-y-1.5 overflow-y-auto pr-1">
                  {visibleAttention.map((row) => (
                    <Link
                      key={row.flowId}
                      to={`/flow-builder/${row.flowId}`}
                      className="block rounded-lg bg-muted/40 p-2.5 ring-1 ring-inset ring-border/50 transition-colors hover:bg-accent"
                    >
                      <div className="text-sm font-medium">{row.flowName}</div>
                      <ul className="mt-1 space-y-1">
                        {row.reasons.map((reason, i) => (
                          <li key={i} className="flex items-start gap-1.5 text-xs leading-relaxed text-muted-foreground">
                            <span className="mt-1.5 h-1 w-1 shrink-0 rounded-full bg-warning" />
                            {reason}
                          </li>
                        ))}
                      </ul>
                    </Link>
                  ))}
                </div>
              )}
              {attention.length > 6 && (
                <Button variant="ghost" size="xs" className="mt-3 w-full" onClick={() => setShowAllAttention((current) => !current)}>
                  {showAllAttention ? "Show fewer" : `Show all ${attention.length} items`}
                </Button>
              )}
            </CardContent>
          </Card>
        </div>
      </div>
    </AppLayout>
  );
};

export default Dashboard;
