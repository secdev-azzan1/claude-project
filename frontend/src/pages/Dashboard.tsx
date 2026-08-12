// Dashboard — adapter-model KPIs over the mock prototype layer.
// No polling: a manual Refresh button re-fetches everything.

import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { AppLayout } from "@/components/AppLayout";
import { AdapterChip } from "@/components/AdapterChip";
import { StatusBadge } from "@/components/StatusBadge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { timeAgo } from "@/lib/api";
import {
  getDashboardSummary,
  listAudit,
  listFlows,
  listServices,
  serviceUpdateAvailable,
  validateFlowNow,
} from "@/prototype/api";
import type { AppService, Flow, FlowBlock } from "@/prototype/types";
import {
  Activity,
  AlertTriangle,
  ArrowUpRight,
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

const Dashboard = () => {
  const qc = useQueryClient();

  const { data: summary, isLoading: sumLoading, isFetching: sumFetching } = useQuery({
    queryKey: ["dashboard"],
    queryFn: getDashboardSummary,
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

  const connectionsDegraded =
    !!summary && summary.connectionsHealthy < summary.connectionsTotal;
  const sinksDegraded =
    !!summary && summary.sinkConnectorsRunning < summary.sinkConnectorsTotal;

  const statDefs = [
    {
      label: "Flows",
      value: summary?.totalFlows,
      icon: Workflow,
      tone: "text-primary bg-primary-muted",
    },
    {
      label: "Running Flows",
      value: summary?.runningFlows,
      icon: Activity,
      tone: "text-success bg-success-muted",
    },
    {
      label: "Approved Schemas",
      value: summary?.approvedSchemas,
      icon: ShieldCheck,
      tone: "text-primary bg-primary-muted",
    },
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
        summary && summary.sinkConnectorsUndeployed > 0
          ? `${summary.sinkConnectorsUndeployed} not deployed`
          : undefined,
    },
  ];

  const attention = computeAttention(flows, services);
  const recent = audit.slice(0, 8);

  return (
    <AppLayout
      title="Dashboard"
      description="Overview of your data mobility platform"
      actions={
        <Button variant="outline" size="sm" onClick={refresh} disabled={refreshing}>
          {refreshing ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />} Refresh
        </Button>
      }
    >
      {/* KPI Stats */}
      <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-4 mb-6">
        {statDefs.map((s) => (
          <Card key={s.label}>
            <CardContent className="p-4">
              <div className="flex items-center justify-between mb-3">
                <span className={`flex h-8 w-8 items-center justify-center rounded-md ${s.tone}`}>
                  <s.icon className="h-4 w-4" />
                </span>
                {sumLoading && <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground" />}
              </div>
              <div className={`text-2xl font-semibold tracking-tight ${s.warn ? "text-warning" : ""}`}>
                {s.value ?? "—"}
              </div>
              <div className="text-xs text-muted-foreground mt-1">{s.label}</div>
              {"hint" in s && s.hint && <div className="mt-0.5 text-xs leading-4 text-muted-foreground/80">{s.hint}</div>}
            </CardContent>
          </Card>
        ))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        {/* Flow Status Panel */}
        <Card className="lg:col-span-2">
          <CardHeader className="flex-row items-center justify-between space-y-0">
            <div>
              <CardTitle className="text-base">Flow Status</CardTitle>
              <CardDescription>Operational state of all flows</CardDescription>
            </div>
            <Button variant="ghost" size="sm" asChild>
              <Link to="/flows">
                View all <ArrowUpRight className="h-3.5 w-3.5" />
              </Link>
            </Button>
          </CardHeader>
          <CardContent>
            {flows.length === 0 ? (
              <div className="flex flex-col items-center justify-center py-8 text-center">
                <Workflow className="h-8 w-8 text-muted-foreground/40 mb-2" />
                <p className="text-sm text-muted-foreground">No flows yet</p>
                <Button variant="link" asChild className="mt-1 text-xs">
                  <Link to="/flow-builder/new">Create your first flow →</Link>
                </Button>
              </div>
            ) : (
              <div className="divide-y">
                {flows.map((f) => {
                  const root = rootBlockOf(f);
                  return (
                    <div key={f.id} className="flex items-center justify-between gap-3 py-3 first:pt-0 last:pb-0">
                      <div className="flex min-w-0 items-center gap-2">
                        <Link to="/flows" className="text-sm font-medium truncate hover:underline">
                          {f.name}
                        </Link>
                        {root && <AdapterChip adapter={root.adapter} mode={root.mode} />}
                      </div>
                      <StatusBadge status={f.state} />
                    </div>
                  );
                })}
              </div>
            )}
          </CardContent>
        </Card>

        <div className="flex flex-col gap-4">
          {/* Recent Activity */}
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Recent Activity</CardTitle>
              <CardDescription>Last admin actions</CardDescription>
            </CardHeader>
            <CardContent>
              {recent.length === 0 ? (
                <p className="text-sm text-muted-foreground text-center py-4">No activity yet</p>
              ) : (
                <div className="space-y-3">
                  {recent.map((a) => (
                    <div key={a.id} className="flex items-start gap-3 text-sm">
                      <div className={`mt-1.5 h-2 w-2 rounded-full flex-shrink-0 ${statusDotClass(a.status)}`} />
                      <div className="flex-1 min-w-0">
                        <div className="truncate">
                          <span className="font-medium">{a.action}</span>{" "}
                          {a.target && <span className="text-muted-foreground">— {a.target}</span>}
                        </div>
                        <div className="text-xs text-muted-foreground">
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
            <CardHeader>
              <CardTitle className="text-base flex items-center gap-2">
                <AlertTriangle className="h-4 w-4 text-warning" /> Needs attention
              </CardTitle>
              <CardDescription>Drafts, drift, and service flags</CardDescription>
            </CardHeader>
            <CardContent>
              {attention.length === 0 ? (
                <p className="text-sm text-muted-foreground text-center py-4">Nothing needs attention</p>
              ) : (
                <div className="space-y-2">
                  {attention.map((row) => (
                    <Link
                      key={row.flowId}
                      to={`/flow-builder/${row.flowId}`}
                      className="block rounded-md border p-2.5 transition hover:bg-muted/50"
                    >
                      <div className="text-sm font-medium">{row.flowName}</div>
                      <ul className="mt-1 space-y-0.5">
                        {row.reasons.map((reason, i) => (
                          <li key={i} className="text-xs text-muted-foreground">
                            {reason}
                          </li>
                        ))}
                      </ul>
                    </Link>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      </div>
    </AppLayout>
  );
};

export default Dashboard;
