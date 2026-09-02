// Flows — the operational console for the adapter-based model.
// Rebuilt against src/prototype/api.ts: guard-reason-driven actions, bulk
// operations, and a rich right-side detail Sheet. The only polling on the
// page is listFlows every 15s.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import {
  Activity,
  AlertTriangle,
  Cable,
  CheckCircle2,
  ChevronRight,
  Download,
  Eraser,
  Eye,
  FileJson,
  Lock,
  MoreHorizontal,
  Package,
  PackageX,
  Pause,
  Pencil,
  Play,
  Plus,
  RefreshCw,
  Rocket,
  Search,
  Server,
  ShieldAlert,
  Square,
  Trash2,
  Upload,
  Wrench,
  XCircle,
} from "lucide-react";

import { AppLayout } from "@/components/AppLayout";
import { AdapterChip } from "@/components/AdapterChip";
import { SyncTab } from "@/components/flow-detail/SyncTab";
import { StatusBadge } from "@/components/StatusBadge";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
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
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty-state";
import { Checkbox } from "@/components/ui/checkbox";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Switch } from "@/components/ui/switch";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Textarea } from "@/components/ui/textarea";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";

import { timeAgo } from "@/lib/api";
import { cn } from "@/lib/utils";
import { useDebouncedValue } from "@/lib/useDebouncedValue";
import {
  bulkJobPercent,
  cancelBulkJob,
  clearDedupCache,
  clearFlowTopic,
  deleteCascadeTarget,
  flowCascadeTargets,
  forceRepairRuntime,
  getActiveBulkJob,
  getBulkJob,
  getBulkQueue,
  getDlq,
  getFlowRuntime,
  getMetrics,
  getTopicMessages,
  importConnectorFlow,
  isBulkJobTerminal,
  listConnections,
  listConnectors,
  listFlows,
  listKafkaConnectSyncs,
  listSchemas,
  listServices,
  publishConnector,
  refreshFlowRuntime,
  serviceUpdateAvailable,
  startBulkJob,
  waitForBulkJob,
  validateFlowNow,
  type BulkJobItem,
  type CascadeTarget,
  type FlowVerb,
  type FlowDeleteOptions,
} from "@/prototype/api";
import { Progress } from "@/components/ui/progress";
import { rootBlock } from "@/prototype/legality";
import { deriveTopicName, dlqName, tokenize } from "@/prototype/naming";
import type {
  AdapterId,
  AppService,
  ApprovedSchema,
  BlockMode,
  ConnectorExport,
  DriftFinding,
  Flow,
  FlowBlock,
  FlowRuntime,
  NifiComponent,
  PlatformConnection,
  RuntimeOrphan,
  RuntimeProperty,
} from "@/prototype/types";

// ─── Pure helpers ───────────────────────────────────────────────────────────

const RUNTIME_LABELS: Record<string, string> = { nifi: "NiFi", kafka: "Kafka", apicurio: "Apicurio" };

const isWriteBlock = (b: FlowBlock) => b.mode === "write" || b.adapter === "kafka_kc";

function flowEntities(flow: Flow): string[] {
  return Array.from(
    new Set(flow.blocks.filter(isWriteBlock).map((b) => b.entity?.trim() ?? "").filter(Boolean)),
  );
}

/** "a, b +2 more" summarization for narrow table cells. */
function summarize(values: string[], visible = 2): string {
  if (values.length === 0) return "—";
  const shown = values.slice(0, visible).join(", ");
  return values.length > visible ? `${shown} +${values.length - visible} more` : shown;
}

function flowMatchesSearch(flow: Flow, query: string): boolean {
  if (!query) return true;
  const hay = [
    flow.name,
    flow.description ?? "",
    flow.state,
    ...flow.topics.map((t) => t.name),
    ...flow.blocks.flatMap((b) => [b.name, b.entity ?? "", b.adapter]),
  ]
    .join(" ")
    .toLowerCase();
  return hay.includes(query);
}

function schemaStatus(flow: Flow, schemas: ApprovedSchema[]): { required: number; approved: number } {
  const needing = flow.blocks.filter((b) => b.adapter === "kafka_kc");
  const approved = needing.filter((b) =>
    schemas.some((s) => s.flowId === flow.id && s.blockId === b.id),
  ).length;
  return { required: needing.length, approved };
}

/** UI-level guard: disabling is refused while the flow is not stopped. */
function disableBlockReason(flow: Flow): string | null {
  return flow.state === "Running" || flow.state === "Paused" || flow.state === "Degraded" || flow.state === "Deploying"
    ? "Stop the flow first."
    : null;
}

function isAdoptedRooted(flow: Flow): boolean {
  const root = rootBlock(flow);
  return !!root && root.parentId !== null && flow.topics.some((t) => t.id === root.parentId && t.kind === "adopted");
}

/** Chain-order (DFS) block rows with indent depth. kc sinks live in the Topics sub-list instead. */
function chainRows(flow: Flow): { block: FlowBlock; depth: number }[] {
  const rows: { block: FlowBlock; depth: number }[] = [];
  const seen = new Set<string>();
  const nonKc = flow.blocks.filter((b) => b.adapter !== "kc");
  const childrenOf = (b: FlowBlock): FlowBlock[] => [
    ...nonKc.filter((x) => x.parentId === b.id),
    ...flow.topics
      .filter((t) => t.writerBlockId === b.id)
      .flatMap((t) => nonKc.filter((x) => x.parentId === t.id)),
  ];
  const visit = (b: FlowBlock, depth: number) => {
    if (seen.has(b.id)) return;
    seen.add(b.id);
    rows.push({ block: b, depth });
    for (const child of childrenOf(b)) visit(child, depth + 1);
  };
  const roots = nonKc.filter(
    (b) => b.parentId === null || flow.topics.some((t) => t.id === b.parentId && t.kind === "adopted"),
  );
  for (const r of roots) visit(r, 0);
  for (const b of nonKc) visit(b, 0); // safety: orphans still render
  return rows;
}

function sinkServiceId(block: FlowBlock): string | null {
  return (block.config.sinkServiceId as string | undefined) ?? block.serviceId ?? null;
}

function hasDedup(block: FlowBlock): boolean {
  return block.transforms.some((t) => t.kind === "dedup");
}

/** Destination topic for a write/sink block: the topic it materializes, or (for
 * kafka-family writes) the name it would derive/override to. Non-kafka writes
 * (http/jdbc) have no topic destination. */
function outputTopicName(flow: Flow, block: FlowBlock): string | null {
  const owned = flow.topics.find((t) => t.writerBlockId === block.id);
  if (owned) return owned.name;
  if (block.adapter === "kafka" || block.adapter === "kafka_kc") {
    const derived = deriveTopicName(flow, block);
    return derived.value || null;
  }
  return null;
}

function retiredPinnedServices(flow: Flow, services: AppService[]): AppService[] {
  return Object.keys(flow.servicePins)
    .map((id) => services.find((s) => s.id === id))
    .filter((s): s is AppService => !!s && s.retired);
}

function suggestConnectorName(flow: Flow, services: AppService[]): string {
  const base = tokenize(flow.name).replace(/_/g, "-");
  const sinkBlock = flow.blocks.find((b) => b.adapter === "kafka_kc" || b.adapter === "kc");
  const sinkSvc = sinkBlock ? services.find((s) => s.id === sinkServiceId(sinkBlock)) : undefined;
  const sinkToken = sinkSvc ? tokenize(sinkSvc.name.split(/\s+/)[0]).replace(/_/g, "-") : null;
  return sinkToken ? `${base}-to-${sinkToken}` : base;
}

function downloadJson(filename: string, data: unknown) {
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

export function clearTopicActionLabel(flowName: string, clearTopicTarget: string | null): string {
  return clearTopicTarget === dlqName(flowName) ? "Clear DLQ" : "Clear topic";
}

const VERB_META: Record<FlowVerb, { label: string; done: string; detail?: string }> = {
  deploy: { label: "Deploy", done: "Deployed", detail: "Process group created — stopped until Start." },
  start: { label: "Start", done: "Started" },
  pause: { label: "Pause", done: "Paused", detail: "The trigger keeps firing; records queue until Resume." },
  resume: { label: "Resume", done: "Resumed" },
  stop: { label: "Stop", done: "Stopped", detail: "Queues retained." },
  stop_clear: { label: "Stop & Clear", done: "Stopped & cleared", detail: "Queued records discarded — recorded in the audit log." },
  redeploy: { label: "Redeploy", done: "Redeployed", detail: "Service revisions re-pinned at deploy." },
  undeploy: { label: "Undeploy", done: "Undeployed", detail: "Generated topics emptied · dedup caches cleared · positions reset." },
  delete: { label: "Delete", done: "Deleted" },
};

type BulkAction = FlowVerb | "enable" | "disable";

const CASCADE_LABEL: Record<CascadeTarget["kind"], string> = {
  schema: "schema",
  service: "application service",
  proxy: "gateway proxy",
  kafka_connect_sync: "Kafka Connect sync",
};

const BULK_LABEL: Record<BulkAction, string> = {
  pause: "Pause",
  resume: "Resume",
  stop_clear: "Stop & Clear",
  redeploy: "Redeploy",
  undeploy: "Undeploy",
  start: "Start",
  stop: "Stop",
  deploy: "Deploy",
  enable: "Enable",
  disable: "Disable",
  delete: "Delete",
};

// ─── Small guarded controls ─────────────────────────────────────────────────

function GuardedIconButton({
  label,
  reason,
  icon: Icon,
  onClick,
  spinning = false,
  destructive = false,
}: {
  label: string;
  reason: string | null;
  icon: React.ComponentType<{ className?: string }>;
  onClick: () => void;
  spinning?: boolean;
  destructive?: boolean;
}) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="inline-flex">
          <Button
            size="sm"
            variant="ghost"
            className={destructive ? "text-destructive hover:text-destructive" : undefined}
            disabled={Boolean(reason) || spinning}
            onClick={onClick}
            aria-label={label}
          >
            {spinning ? <RefreshCw className="h-3.5 w-3.5 animate-spin" /> : <Icon className="h-3.5 w-3.5" />}
          </Button>
        </span>
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-[280px] text-xs">
        {reason ?? label}
      </TooltipContent>
    </Tooltip>
  );
}

function GuardedActionButton({
  label,
  reason,
  icon: Icon,
  onClick,
  spinning = false,
}: {
  label: string;
  reason: string | null;
  icon: React.ComponentType<{ className?: string }>;
  onClick: () => void;
  spinning?: boolean;
}) {
  return (
    <span title={reason ?? label} className="inline-flex">
      <Button size="sm" variant="outline" disabled={Boolean(reason) || spinning} onClick={onClick}>
        {spinning ? <RefreshCw className="mr-1.5 h-3.5 w-3.5 animate-spin" /> : <Icon className="mr-1.5 h-3.5 w-3.5" />}
        {label}
      </Button>
    </span>
  );
}

function GuardedMenuItem({
  reason,
  onSelect,
  destructive = false,
  children,
}: {
  reason: string | null;
  onSelect: () => void;
  destructive?: boolean;
  children: React.ReactNode;
}) {
  return (
    <div title={reason ?? undefined}>
      <DropdownMenuItem
        disabled={Boolean(reason)}
        onSelect={onSelect}
        className={destructive ? "text-destructive focus:text-destructive" : undefined}
      >
        {children}
      </DropdownMenuItem>
    </div>
  );
}

function ServicePinChips({ flow, service }: { flow: Flow; service: AppService | undefined }) {
  if (!service) return null;
  const pinned = flow.servicePins[service.id];
  if (service.retired) {
    return (
      <span className="inline-flex items-center gap-1 rounded-full border border-warning/20 bg-warning-muted px-2 py-0.5 text-xs font-medium text-warning">
        <AlertTriangle className="h-3 w-3" /> retired — action required
      </span>
    );
  }
  if (pinned && service.revision > pinned) {
    return (
      <span className="inline-flex items-center rounded-full border border-info/20 bg-info-muted px-2 py-0.5 text-xs font-medium text-info">
        rev {pinned} pinned · rev {service.revision} available
      </span>
    );
  }
  if (pinned) {
    return <span className="text-xs text-muted-foreground">rev {pinned} pinned</span>;
  }
  return null;
}

// ─── Runtime tab (read-only ops view) ───────────────────────────────────────
// This is what replaced the alpha's NiFi controller-services manager and its
// live processor editor. The user sees MORE than before — every generated
// component grouped under the block that produced it, every compiled service
// with the Application Service revision it is pinned to — and edits NOTHING,
// because editing the runtime out of band is what produces drift. The copy on
// screen says exactly that, so it reads as a decision, not a missing feature.

const DRIFT_KIND_LABEL: Record<DriftFinding["kind"], string> = {
  process_group_missing: "Process group missing",
  property_edited: "Property edited out of band",
  component_state_changed: "Component state changed",
  connector_missing: "Connector missing",
  consumer_lag: "Consumer lag",
};

const VERDICT_LABEL: Record<DriftFinding["verdict"], string> = {
  really_deleted: "Really deleted (same instance)",
  deployed_elsewhere: "Deployed elsewhere (different instance)",
  unknown: "Unknown (instance unreachable)",
  out_of_band_edit: "Edited out of band",
};

const ORPHAN_KIND_LABEL: Record<RuntimeOrphan["kind"], string> = {
  process_group: "Process group",
  connector: "Connect connector",
  controller_service: "Controller service",
};

/** One live descriptor row. A sensitive value is absent, not blanked out here. */
function PropertyRows({ properties }: { properties: RuntimeProperty[] }) {
  return (
    <div className="mt-2 space-y-1 rounded-md bg-muted/40 p-2 shadow-inner">
      {properties.map((prop) => (
        <div key={prop.name} className="grid grid-cols-[minmax(0,11rem)_minmax(0,1fr)] gap-x-3 text-xs">
          <span className="truncate text-muted-foreground" title={prop.name}>{prop.name}</span>
          {prop.value === null ? (
            <span className="text-muted-foreground">
              <span className="font-mono">••••••••</span>{" "}
              <span className="italic">masked — sensitive descriptor, never sent to the browser</span>
            </span>
          ) : (
            <span className="min-w-0 break-all font-mono">
              {prop.value}
              {prop.divergedFrom !== undefined && (
                <span className="ml-1.5 whitespace-nowrap rounded-full border border-warning/20 bg-warning-muted px-1.5 py-0.5 font-sans text-xs font-medium text-warning">
                  compiled: {prop.divergedFrom}
                </span>
              )}
            </span>
          )}
        </div>
      ))}
    </div>
  );
}

function DisclosureRow({
  title,
  meta,
  badges,
  children,
  defaultOpen = false,
}: {
  title: React.ReactNode;
  meta?: React.ReactNode;
  badges?: React.ReactNode;
  children: React.ReactNode;
  defaultOpen?: boolean;
}) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <Collapsible open={open} onOpenChange={setOpen} className="rounded-md border shadow-sm">
      <CollapsibleTrigger className="flex w-full items-start gap-2 p-2.5 text-left">
        <ChevronRight className={cn("mt-0.5 h-3.5 w-3.5 shrink-0 text-muted-foreground transition-transform", open && "rotate-90")} />
        <span className="min-w-0 flex-1">
          <span className="flex flex-wrap items-center gap-2">{title}</span>
          {meta && <span className="mt-0.5 block text-xs text-muted-foreground">{meta}</span>}
        </span>
        {badges && <span className="flex shrink-0 flex-wrap items-center justify-end gap-1">{badges}</span>}
      </CollapsibleTrigger>
      <CollapsibleContent className="border-t px-2.5 pb-2.5 pt-2">{children}</CollapsibleContent>
    </Collapsible>
  );
}

function RuntimeTab({
  flow,
  services,
  connections,
  onEdit,
}: {
  flow: Flow;
  services: AppService[];
  connections: PlatformConnection[];
  onEdit: () => void;
}) {
  const qc = useQueryClient();
  const [repairOpen, setRepairOpen] = useState(false);

  const runtimeQuery = useQuery({
    queryKey: ["flow-runtime", flow.id],
    queryFn: () => getFlowRuntime(flow.id),
    // Read-only list: loads once when the panel becomes visible, refetches on
    // demand only. Nothing polls NiFi behind the user's back.
    staleTime: 60_000,
    refetchOnWindowFocus: false,
  });
  const runtime = runtimeQuery.data ?? null;

  const refreshMut = useMutation({
    mutationFn: () => refreshFlowRuntime(flow.id),
    onSuccess: (rt) => {
      qc.setQueryData(["flow-runtime", flow.id], rt);
      qc.invalidateQueries({ queryKey: ["audit"] });
      if (rt.reachable) {
        toast.success("Runtime read", {
          description: "States and live property values refreshed. Nothing was written — drift findings stay until you repair them.",
        });
      } else {
        toast.warning("Runtime unreachable", { description: rt.unreachableReason });
      }
    },
    onError: (e: Error) => toast.error("Could not read the runtime", { description: e.message }),
  });

  const repairMut = useMutation({
    mutationFn: () => forceRepairRuntime(flow.id),
    onSuccess: (res) => {
      qc.setQueryData(["flow-runtime", flow.id], res.runtime);
      qc.invalidateQueries({ queryKey: ["flows"] });
      qc.invalidateQueries({ queryKey: ["audit"] });
      toast.success("Runtime reference cleared", {
        description: `${res.clearedFindings} finding(s) resolved · ${res.orphans.length} orphan(s) recorded · nothing was deleted on the runtime.`,
      });
    },
    onError: (e: Error) => toast.error("Force repair refused", { description: e.message }),
  });

  const byBlock = useMemo(() => {
    const groups = new Map<string, NifiComponent[]>();
    for (const c of runtime?.components ?? []) {
      const list = groups.get(c.blockId) ?? [];
      list.push(c);
      groups.set(c.blockId, list);
    }
    return groups;
  }, [runtime]);

  const repairable = (runtime?.drift ?? []).filter((d) => d.repairable);

  const readOnlyNote = (
    <Alert className="border-info/40 bg-info-muted/40">
      <Lock className="h-4 w-4 text-info" />
      <AlertTitle>Read-only, on purpose</AlertTitle>
      <AlertDescription className="text-xs leading-5">
        You can see every component this flow generated and every controller service it compiled — grouped by the block
        that produced them — but there is no editing surface here. The old screens let you change deployed processors and
        services live, and out-of-band edits are exactly what this view now detects as drift. Tune the block in the
        builder and Redeploy;{" "}
        <button type="button" onClick={onEdit} className="font-medium underline underline-offset-2">
          open this flow in the builder
        </button>
        .
      </AlertDescription>
    </Alert>
  );

  if (runtimeQuery.isLoading) {
    return <div className="p-6 text-center text-sm text-muted-foreground">Reading the runtime…</div>;
  }

  if (!flow.deployedAt && !(runtime && runtime.orphans.length > 0)) {
    return (
      <div className="space-y-4">
        {readOnlyNote}
        <div className="rounded-md border p-8 text-center">
          <Server className="mx-auto h-6 w-6 text-muted-foreground" />
          <div className="mt-2 text-sm font-medium">Nothing is deployed</div>
          <div className="mt-1 text-xs text-muted-foreground">
            The flow has no runtime yet. Deploy it to generate its NiFi components, controller services and Connect
            connectors — they appear here read-only.
          </div>
        </div>
      </div>
    );
  }

  if (!runtime) {
    return (
      <div className="space-y-4">
        {readOnlyNote}
        <div className="rounded-md border p-8 text-center text-sm text-muted-foreground">
          No runtime record for this flow. Redeploy to compile one.
        </div>
      </div>
    );
  }

  const nifiConn = connections.find((c) => c.id === runtime.nifiConnectionId);

  return (
    <div className="space-y-4">
      {/* live-read header */}
      <div className="flex flex-wrap items-center justify-between gap-2 rounded-md border p-2.5 shadow-sm">
        <div className="min-w-0 text-xs text-muted-foreground">
          <div>
            Last read {timeAgo(runtime.lastReadAt)} from {nifiConn?.name ?? runtime.nifiConnectionId}
            {runtime.processGroupId ? (
              <> · process group <code className="text-foreground">{runtime.processGroupId}</code></>
            ) : (
              <> · no process group — the reference was cleared</>
            )}
          </div>
          <div className="mt-0.5">
            Instance fingerprint (root group){" "}
            <code className="text-foreground">{runtime.observedFingerprint ?? "unreadable"}</code>
          </div>
        </div>
        <span
          title={
            runtime.processGroupId
              ? "Read NiFi and Connect again — states, live property values, task states. Writes nothing."
              : "The runtime reference was cleared — deploy the flow to compile a new one."
          }
          className="inline-flex"
        >
          <Button
            size="sm"
            variant="outline"
            disabled={refreshMut.isPending || !runtime.processGroupId}
            onClick={() => refreshMut.mutate()}
          >
            <RefreshCw className={cn("mr-1.5 h-3.5 w-3.5", refreshMut.isPending && "animate-spin")} />
            Refresh from NiFi
          </Button>
        </span>
      </div>

      {!runtime.reachable && (
        <Alert className="border-warning/40 bg-warning-muted/40">
          <AlertTriangle className="h-4 w-4 text-warning" />
          <AlertTitle>Runtime unavailable — values below are the last known ones</AlertTitle>
          <AlertDescription className="text-xs leading-5">
            {runtime.unreachableReason} An unreachable instance means <em>unknown</em>, never "deleted": nothing is
            concluded and nothing is repaired from a failed read.
          </AlertDescription>
        </Alert>
      )}

      {readOnlyNote}

      {/* ── Drift ── */}
      {runtime.drift.length > 0 && (
        <div className="rounded-md border border-warning/40 bg-warning-muted/20 p-3 shadow-sm">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div className="flex items-center gap-2 text-sm font-medium">
              <ShieldAlert className="h-4 w-4 text-warning" />
              {runtime.drift.length} drift finding{runtime.drift.length === 1 ? "" : "s"}
            </div>
            {repairable.length > 0 && (
              <Button
                size="sm"
                variant="outline"
                className="text-destructive hover:text-destructive"
                disabled={repairMut.isPending}
                onClick={() => setRepairOpen(true)}
              >
                {repairMut.isPending ? (
                  <RefreshCw className="mr-1.5 h-3.5 w-3.5 animate-spin" />
                ) : (
                  <Wrench className="mr-1.5 h-3.5 w-3.5" />
                )}
                Force repair
              </Button>
            )}
          </div>
          <p className="mt-1.5 text-xs text-muted-foreground">
            Reads never repair. What diverged is surfaced here and stays surfaced — clearing a dead reference is an
            explicit action you take, and it is audited.
          </p>
          <div className="mt-2 space-y-2">
            {runtime.drift.map((finding) => (
              <div key={finding.id} className="rounded-md border bg-card p-2.5">
                <div className="flex flex-wrap items-center gap-2">
                  <Badge variant="outline" className="text-xs">{DRIFT_KIND_LABEL[finding.kind]}</Badge>
                  <span className="text-sm font-medium">{finding.where}</span>
                  <span className="text-xs text-muted-foreground">{timeAgo(finding.observedAt)}</span>
                </div>
                <div className="mt-1 text-xs">{finding.summary}</div>
                {(finding.expected !== undefined || finding.observed !== undefined) && (
                  <div className="mt-1.5 grid gap-1 text-xs sm:grid-cols-2">
                    <div>
                      <span className="text-muted-foreground">Compiled / recorded: </span>
                      <code>{finding.expected ?? "—"}</code>
                    </div>
                    <div>
                      <span className="text-muted-foreground">Observed live: </span>
                      <code>{finding.observed ?? "—"}</code>
                    </div>
                  </div>
                )}
                <div className="mt-2 rounded-md bg-muted/50 p-2 text-xs shadow-inner">
                  <span className="font-medium">Verdict — {VERDICT_LABEL[finding.verdict]}.</span>{" "}
                  {finding.verdictDetail}
                  {!finding.repairable && (
                    <span className="block pt-1 text-muted-foreground">
                      Nothing to force here: the fix is Redeploy, which compiles the block config back over the edit.
                    </span>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── Generated components, grouped by owning block ── */}
      <div className="space-y-2">
        <div className="flex items-center gap-2 text-sm font-medium">
          <Server className="h-4 w-4 text-muted-foreground" />
          Generated NiFi components
          <span className="text-xs font-normal text-muted-foreground">
            {runtime.components.length} component{runtime.components.length === 1 ? "" : "s"} · grouped by the block that generated them
          </span>
        </div>
        {runtime.components.length === 0 ? (
          <EmptyState inline>
            {runtime.processGroupId
              ? "The read returned no components under this flow's process group."
              : "No components — the platform no longer holds a runtime reference for this flow."}
          </EmptyState>
        ) : (
          flow.blocks
            .filter((b) => (byBlock.get(b.id) ?? []).length > 0)
            .map((block) => {
              const comps = byBlock.get(block.id) ?? [];
              const states = Array.from(new Set(comps.map((c) => c.state)));
              return (
                <DisclosureRow
                  key={block.id}
                  title={
                    <>
                      <AdapterChip adapter={block.adapter} mode={block.mode} />
                      <span className="text-sm font-medium">{block.name}</span>
                    </>
                  }
                  meta={`${comps.length} generated component${comps.length === 1 ? "" : "s"}`}
                  badges={states.map((s) => <StatusBadge key={s} status={s} />)}
                >
                  <div className="space-y-2">
                    {comps.map((c) => (
                      <div key={c.id} className="rounded-md border p-2">
                        <div className="flex flex-wrap items-center justify-between gap-2">
                          <span className="text-xs font-medium">{c.name}</span>
                          <StatusBadge status={c.state} />
                        </div>
                        <div className="mt-0.5 break-all font-mono text-xs text-muted-foreground">{c.type}</div>
                        <div className="mt-0.5 font-mono text-xs text-muted-foreground">id {c.id}</div>
                        {c.invalidReasons && c.invalidReasons.length > 0 && (
                          <ul className="mt-1 list-disc space-y-0.5 pl-4 text-xs text-warning">
                            {c.invalidReasons.map((r) => <li key={r}>{r}</li>)}
                          </ul>
                        )}
                        <PropertyRows properties={c.properties} />
                      </div>
                    ))}
                  </div>
                </DisclosureRow>
              );
            })
        )}
        {flow.blocks.some((b) => b.adapter === "kc") && (
          <p className="text-xs text-muted-foreground">
            kc blocks generate no NiFi components — those subscriptions live entirely on Kafka Connect and appear on
            this flow's Sync tab.
          </p>
        )}
      </div>

      {/* ── Compiled controller services ── */}
      <div className="space-y-2">
        <div className="flex items-center gap-2 text-sm font-medium">
          <Cable className="h-4 w-4 text-muted-foreground" />
          Compiled controller services
          <span className="text-xs font-normal text-muted-foreground">
            generated per Application Service revision — never hand-authored
          </span>
        </div>
        {runtime.controllerServices.length === 0 ? (
          <EmptyState inline>No controller services reported.</EmptyState>
        ) : (
          runtime.controllerServices.map((cs) => {
            const svc = services.find((s) => s.id === cs.appServiceId);
            const outdated = !!svc && cs.pinnedRevision !== null && svc.revision > cs.pinnedRevision;
            return (
              <DisclosureRow
                key={cs.id}
                title={
                  <>
                    <span className="text-sm font-medium">{cs.name}</span>
                    <Badge variant="outline" className="text-xs">
                      {cs.scope === "shared" ? "platform-wide" : "this flow"}
                    </Badge>
                  </>
                }
                meta={
                  <>
                    <span className="font-mono">{cs.type}</span>
                    {svc ? (
                      <> · from {svc.name} · pinned rev {cs.pinnedRevision}</>
                    ) : (
                      <> · platform-owned — pinned to no Application Service</>
                    )}
                    {cs.scope === "shared" && cs.sharedWith && cs.sharedWith.length > 0 && (
                      <> · also used by {summarize(cs.sharedWith, 2)}</>
                    )}
                  </>
                }
                badges={
                  <>
                    {outdated && <StatusBadge status="Update available" />}
                    {svc?.retired && <StatusBadge status="Action required" />}
                    <StatusBadge status={cs.state === "ENABLED" ? "Active" : cs.state === "INVALID" ? "Invalid" : "Inactive"} />
                  </>
                }
              >
                {outdated && svc && (
                  <p className="mb-2 text-xs text-info">
                    Pinned to rev {cs.pinnedRevision}; {svc.name} is now at rev {svc.revision}. The flow adopts it at the
                    next Redeploy — the service is never swapped underneath a running flow.
                  </p>
                )}
                {svc?.retired && (
                  <p className="mb-2 text-xs text-warning">
                    {svc.name} has been retired. This service keeps running until the flow is redeployed, which will
                    fail validation while the retirement stands.
                  </p>
                )}
                <PropertyRows properties={cs.properties} />
              </DisclosureRow>
            );
          })
        )}
      </div>

      {/* Per-sink Kafka Connect state moved to the flow's Sync tab, so it isn't
          shown in two places. Runtime is now purely the NiFi view. */}

      {/* ── Orphan ledger ── */}
      {runtime.orphans.length > 0 && (
        <div className="space-y-2">
          <div className="flex items-center gap-2 text-sm font-medium">
            <AlertTriangle className="h-4 w-4 text-muted-foreground" />
            Recorded orphans
            <span className="text-xs font-normal text-muted-foreground">left on the runtime — never deleted for you</span>
          </div>
          <div className="rounded-md border">
            {runtime.orphans.map((orphan) => (
              <div key={orphan.id} className="flex flex-wrap items-center gap-x-3 gap-y-1 border-b p-2 text-xs last:border-b-0">
                <Badge variant="outline" className="text-xs">{ORPHAN_KIND_LABEL[orphan.kind]}</Badge>
                <code className="break-all">{orphan.ref}</code>
                <span className="text-muted-foreground">on {orphan.instance}</span>
                <span className="text-muted-foreground">· recorded {timeAgo(orphan.recordedAt)}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      <p className="text-xs text-muted-foreground">
        Tuning happens in the block form, then Redeploy —{" "}
        <button type="button" onClick={onEdit} className="font-medium underline underline-offset-2">
          open the builder
        </button>
        . Nothing on this tab writes to NiFi or Connect.
      </p>

      {/* ── Force repair confirmation ── */}
      <AlertDialog open={repairOpen} onOpenChange={setRepairOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Force repair "{flow.name}"?</AlertDialogTitle>
            <AlertDialogDescription asChild>
              <div className="space-y-2 text-sm">
                <p>
                  This clears the platform's reference to a runtime it can no longer manage. It is the only thing on this
                  tab that changes anything, it is never run for you, and it is recorded in the audit log.
                </p>
                <ul className="list-disc space-y-0.5 pl-5">
                  {repairable.map((finding) => (
                    <li key={finding.id}>
                      {finding.summary} — <span className="font-medium">{VERDICT_LABEL[finding.verdict]}</span>
                    </li>
                  ))}
                </ul>
                <p>
                  Everything still standing on the runtime — the process group when it lives on another instance, the
                  flow's Connect connectors, its flow-scoped controller services — is <span className="font-medium">recorded as an orphan</span>{" "}
                  and left exactly where it is. Nothing is deleted on NiFi or Connect.
                </p>
                <p className="text-muted-foreground">
                  The flow returns to Draft. Deploy it again to compile a fresh runtime.
                </p>
              </div>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              onClick={() => {
                setRepairOpen(false);
                repairMut.mutate();
              }}
            >
              Clear reference &amp; record orphans
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

// ─── Detail sheet ───────────────────────────────────────────────────────────

export function FlowDetailSheet({
  flow,
  services,
  schemas,
  connections,
  pendingVerb,
  onVerb,
  onToggleEnabled,
  onEdit,
  onSaveConnector,
  enableBusy,
  queueLockReason,
}: {
  flow: Flow;
  services: AppService[];
  schemas: ApprovedSchema[];
  connections: PlatformConnection[];
  pendingVerb: FlowVerb | undefined;
  onVerb: (verb: FlowVerb) => void;
  onToggleEnabled: (enabled: boolean) => void;
  onEdit: () => void;
  onSaveConnector: () => void;
  enableBusy: boolean;
  queueLockReason?: string | null;
}) {
  const qc = useQueryClient();
  const [tab, setTab] = useState("overview");
  const [msgTopic, setMsgTopic] = useState<string | null>(null);
  const [clearDedupTarget, setClearDedupTarget] = useState<FlowBlock | null>(null);
  const [clearTopicTarget, setClearTopicTarget] = useState<string | null>(null);

  const clearDedupMut = useMutation({
    mutationFn: (block: FlowBlock) => clearDedupCache(flow.id, block.id),
    onSuccess: (res, block) => {
      qc.invalidateQueries({ queryKey: ["audit"] });
      if (res.cleared) {
        toast.success("Dedup cache cleared", { description: `${block.name} — previously suppressed records become eligible again before the window expires.` });
      } else {
        toast.info("Nothing to clear", { description: `${block.name} has never been deployed — there is no live cache yet.` });
      }
    },
    onError: (e: Error) => toast.error("Could not clear the dedup cache", { description: e.message }),
  });

  // Alpha parity: the ops-view "Clear Topics" destructive action (MVP §19.7)
  // — shared by the Messages tab's "Clear topic" button (targets the picked
  // topic) and the DLQ tab's "Clear DLQ" button (targets `dlqName(flow.name)`).
  const clearTopicMut = useMutation({
    mutationFn: (topic: string) => clearFlowTopic(flow.id, topic),
    onSuccess: (res) => {
      qc.invalidateQueries({ queryKey: ["flow-metrics", flow.id] });
      qc.invalidateQueries({ queryKey: ["flow-dlq", flow.id] });
      qc.invalidateQueries({ predicate: (q) => q.queryKey[0] === "topic-messages" });
      qc.invalidateQueries({ queryKey: ["audit"] });
      toast.success(`Cleared ${res.before} message(s) from ${res.topic}`);
    },
    onError: (e: Error) => toast.error("Could not clear the topic", { description: e.message }),
  });

  useEffect(() => {
    setTab("overview");
    setMsgTopic(flow.topics[0]?.name ?? null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [flow.id]);

  const clearTopicActionLabelValue = clearTopicActionLabel(flow.name, clearTopicTarget);

  const metricsQuery = useQuery({
    queryKey: ["flow-metrics", flow.id],
    queryFn: () => getMetrics(flow.id),
    enabled: tab === "metrics",
  });
  const dlqQuery = useQuery({
    queryKey: ["flow-dlq", flow.id],
    queryFn: () => getDlq(flow.id),
    enabled: tab === "dlq",
  });
  const messagesQuery = useQuery({
    queryKey: ["topic-messages", msgTopic],
    queryFn: () => getTopicMessages(flow.id, msgTopic!),
    enabled: tab === "messages" && !!msgTopic,
  });

  const issues = useMemo(() => validateFlowNow(flow), [flow]);
  const rows = useMemo(() => chainRows(flow), [flow]);
  const entityBlocks = useMemo(() => flow.blocks.filter(isWriteBlock), [flow]);
  const kcBlocks = flow.blocks.filter((b) => b.adapter === "kc");
  // Reads the runtime cache the Runtime tab populates — never fetched from here,
  // so the process-group reference simply doesn't render until that tab has loaded it.
  const runtime = qc.getQueryData<FlowRuntime>(["flow-runtime", flow.id]);
  const busy = (verb: FlowVerb) => pendingVerb === verb;
  const disableReason = queueLockReason ?? null;

  // Gated on block adapters, not on fetched sync records, so the tab never
  // flickers into existence after a network round trip.
  const hasKafkaConnectSink = flow.blocks.some((b) => b.adapter === "kc" || b.adapter === "kafka_kc");

  // Radix renders nothing when the active tab's trigger disappears. A flow can
  // lose its last Connect sink while the panel is open, so fall back rather
  // than leaving a blank panel.
  useEffect(() => {
    if (tab === "sync" && !hasKafkaConnectSink) setTab("overview");
  }, [tab, hasKafkaConnectSink]);

  const primaryVerb: FlowVerb | null =
    !flow.deployedAt || flow.state === "Deploying"
      ? null
      : flow.state === "Running" || flow.state === "Degraded"
        ? "pause"
        : flow.state === "Paused"
          ? "resume"
          : "start";
  const primaryIcon = primaryVerb === "pause" ? Pause : Play;

  const dlq = dlqQuery.data ?? [];
  const metrics = metricsQuery.data ?? null;
  const messages = useMemo(
    () => [...(messagesQuery.data ?? [])].sort((a, b) => b.offset - a.offset).slice(0, 50),
    [messagesQuery.data],
  );

  return (
    <SheetContent className="w-full overflow-y-auto sm:max-w-2xl">
      <SheetHeader>
        <SheetTitle className="flex flex-wrap items-center gap-2">
          {flow.name}
          <StatusBadge status={flow.state} />
        </SheetTitle>
        <SheetDescription asChild>
          <div>
            <div className="flex items-center gap-2 text-sm">
              <span title={disableReason ?? (flow.enabled ? "Disable flow" : "Enable flow")} className="inline-flex">
                <Switch
                  checked={flow.enabled}
                  disabled={enableBusy || Boolean(disableReason)}
                  onCheckedChange={(checked) => onToggleEnabled(checked)}
                  aria-label="Enabled"
                />
              </span>
              <span className="text-muted-foreground">{flow.enabled ? "Enabled" : "Disabled"}</span>
              <span className="text-muted-foreground">· last run {timeAgo(flow.lastRunAt)}</span>
              <span className="text-muted-foreground">· deployed {flow.deployedAt ? timeAgo(flow.deployedAt) : "never"}</span>
            </div>
            {flow.description && <p className="mt-2 text-sm text-muted-foreground">{flow.description}</p>}
          </div>
        </SheetDescription>
      </SheetHeader>

      {flow.drift && (
        <Alert className="mt-3 border-warning/40 bg-warning-muted/40">
          <AlertTriangle className="h-4 w-4 text-warning" />
          <AlertTitle>Drift detected</AlertTitle>
          <AlertDescription className="text-xs leading-5">
            {flow.drift}{" "}
            <button
              type="button"
              onClick={() => setTab("runtime")}
              className="font-medium underline underline-offset-2"
            >
              Open the Runtime tab
            </button>{" "}
            for what diverged, the fingerprint verdict, and the repair action. Nothing is repaired by looking.
          </AlertDescription>
        </Alert>
      )}

      <div className="mt-4 flex flex-wrap items-center gap-2">
        {primaryVerb && (
          <GuardedActionButton
            label={VERB_META[primaryVerb].label}
            reason={queueLockReason ?? null}
            icon={primaryIcon}
            spinning={busy(primaryVerb)}
            onClick={() => onVerb(primaryVerb)}
          />
        )}
        {(flow.state === "Running" || flow.state === "Paused" || flow.state === "Degraded") && (
          <GuardedActionButton
            label="Stop"
            reason={queueLockReason ?? null}
            icon={Square}
            spinning={busy("stop")}
            onClick={() => onVerb("stop")}
          />
        )}
        {flow.deployedAt ? (
          <GuardedActionButton
            label="Redeploy"
            reason={queueLockReason ?? null}
            icon={RefreshCw}
            spinning={busy("redeploy")}
            onClick={() => onVerb("redeploy")}
          />
        ) : (
          <GuardedActionButton
            label="Deploy"
            reason={queueLockReason ?? null}
            icon={Rocket}
            spinning={busy("deploy")}
            onClick={() => onVerb("deploy")}
          />
        )}
        {(flow.state === "Running" || flow.state === "Paused" || flow.state === "Degraded") && (
          <GuardedActionButton
            label="Stop & Clear"
            reason={queueLockReason ?? null}
            icon={Eraser}
            spinning={busy("stop_clear")}
            onClick={() => onVerb("stop_clear")}
          />
        )}
        {flow.deployedAt && (
          <GuardedActionButton
            label="Undeploy"
            reason={queueLockReason ?? null}
            icon={PackageX}
            spinning={busy("undeploy")}
            onClick={() => onVerb("undeploy")}
          />
        )}
        <span
          title={
            flow.state === "Running" || flow.state === "Paused" || flow.state === "Degraded" || flow.state === "Deploying"
              ? "Opens read-only — stop to edit"
              : "Edit flow"
          }
          className="inline-flex"
        >
          <Button size="sm" variant="outline" disabled={!!queueLockReason} onClick={onEdit}>
            <Pencil className="mr-1.5 h-3.5 w-3.5" /> Edit
          </Button>
        </span>
        <Button size="sm" variant="outline" disabled={!!queueLockReason} onClick={onSaveConnector}>
          <Package className="mr-1.5 h-3.5 w-3.5" /> Save as Connector
        </Button>
      </div>

      <Tabs value={tab} onValueChange={setTab} className="mt-6">
        <TabsList className="w-full">
          <TabsTrigger value="overview" className="flex-1">Overview</TabsTrigger>
          <TabsTrigger value="metrics" className="flex-1">Metrics</TabsTrigger>
          <TabsTrigger value="dlq" className="flex-1">DLQ</TabsTrigger>
          <TabsTrigger value="messages" className="flex-1">Messages</TabsTrigger>
          <TabsTrigger value="runtime" className="flex-1">Runtime</TabsTrigger>
          {hasKafkaConnectSink && <TabsTrigger value="sync" className="flex-1">Sync</TabsTrigger>}
        </TabsList>

        {/* ── Overview ── */}
        <TabsContent value="overview" className="mt-4 space-y-4">
          {issues.length > 0 && (
            <Alert className="border-warning/40 bg-warning-muted/40">
              <AlertTriangle className="h-4 w-4 text-warning" />
              <AlertTitle>{issues.length} validation issue{issues.length === 1 ? "" : "s"}</AlertTitle>
              <AlertDescription className="text-xs">
                <ul className="mt-1 list-disc space-y-0.5 pl-4">
                  {issues.slice(0, 6).map((issue, i) => (
                    <li key={i}>
                      <span className="font-medium">{issue.where}</span> — {issue.message}
                    </li>
                  ))}
                  {issues.length > 6 && <li>… and {issues.length - 6} more</li>}
                </ul>
              </AlertDescription>
            </Alert>
          )}

          {/* Deployment summary */}
          <div className="rounded-md border p-3">
            <div className="mb-2 text-xs font-medium">Deployment</div>
            <div className="grid gap-x-4 gap-y-2.5 sm:grid-cols-2">
              <div>
                <div className="text-xs text-muted-foreground">State</div>
                <div className="mt-0.5"><StatusBadge status={flow.state} /></div>
              </div>
              <div>
                <div className="text-xs text-muted-foreground">Enabled</div>
                <div className="mt-0.5 text-sm">{flow.enabled ? "Enabled" : "Disabled"}</div>
              </div>
              <div>
                <div className="text-xs text-muted-foreground">Deployed</div>
                <div className="mt-0.5 text-sm">{flow.deployedAt ? timeAgo(flow.deployedAt) : "never"}</div>
              </div>
              <div>
                <div className="text-xs text-muted-foreground">Last run</div>
                <div className="mt-0.5 text-sm">{timeAgo(flow.lastRunAt)}</div>
              </div>
              <div>
                <div className="text-xs text-muted-foreground">Schedule</div>
                <div className="mt-0.5 font-mono text-sm">
                  {flow.cron ? `cron ${flow.cron} (UTC)` : "continuous — no trigger"}
                </div>
              </div>
              <div>
                <div className="text-xs text-muted-foreground">DLQ topic</div>
                <div className="mt-0.5 font-mono text-sm">{dlqName(flow.name)}</div>
              </div>
              {flow.deployedAt && runtime?.processGroupId && (
                <div>
                  <div className="text-xs text-muted-foreground">NiFi process group</div>
                  <div className="mt-0.5 flex items-center gap-1.5">
                    <code className="text-sm">{runtime.processGroupId}</code>
                    <span className="text-xs text-muted-foreground">reference</span>
                  </div>
                </div>
              )}
            </div>
          </div>

          {/* Entity outputs */}
          <div className="rounded-md border p-3">
            <div className="mb-2 text-xs font-medium">Entity outputs</div>
            {entityBlocks.length === 0 ? (
              <div className="text-xs text-muted-foreground">No write or sink blocks yet.</div>
            ) : (
              <div className="space-y-1.5">
                {entityBlocks.map((block) => {
                  const topicName = outputTopicName(flow, block);
                  const schemaApproved = schemas.some((s) => s.flowId === flow.id && s.blockId === block.id);
                  return (
                    <div key={block.id} className="flex flex-wrap items-center gap-2 rounded-md border p-2">
                      <AdapterChip adapter={block.adapter} mode={block.mode} />
                      <span className="text-sm font-medium">{block.name}</span>
                      <Badge variant="outline" className="font-mono text-xs">{block.entity || "no entity"}</Badge>
                      {topicName ? (
                        <code className="text-xs text-muted-foreground">{topicName}</code>
                      ) : (
                        <span className="text-xs text-muted-foreground">no destination topic</span>
                      )}
                      {block.adapter === "kafka_kc" && (
                        <StatusBadge status={schemaApproved ? "Schema approved" : "Schema missing"} />
                      )}
                    </div>
                  );
                })}
              </div>
            )}
          </div>

          {/* Blocks — collapsed by default; the builder's map is the primary structure view */}
          <DisclosureRow
            title={<span className="text-sm font-medium">Blocks ({rows.length})</span>}
            meta="Chain order, indented — open to inspect; the map in the builder is the primary structure view."
          >
            <div className="space-y-1.5">
              {rows.length === 0 ? (
                <div className="rounded-md border p-4 text-center text-sm text-muted-foreground">
                  The flow has no blocks yet.
                </div>
              ) : (
                rows.map(({ block, depth }) => {
                  const svc = services.find((s) => s.id === (block.serviceId ?? sinkServiceId(block)));
                  return (
                    <div key={block.id} className="rounded-md border p-2.5" style={{ marginLeft: depth * 16 }}>
                      <div className="flex flex-wrap items-center gap-2">
                        <AdapterChip adapter={block.adapter} mode={block.mode} />
                        <span className="text-sm font-medium">{block.name}</span>
                        {block.branch && (
                          <Badge variant="outline" className="text-xs">
                            branch: {block.branch.name}
                          </Badge>
                        )}
                        {block.entity && (
                          <Badge variant="outline" className="font-mono text-xs">{block.entity}</Badge>
                        )}
                        {hasDedup(block) && (
                          <span className="ml-auto inline-flex">
                            <GuardedIconButton
                              label="Clear dedup cache"
                              reason={null}
                              icon={Eraser}
                              spinning={clearDedupMut.isPending && clearDedupMut.variables?.id === block.id}
                              onClick={() => setClearDedupTarget(block)}
                            />
                          </span>
                        )}
                      </div>
                      <div className="mt-1 flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
                        {svc && <span>{svc.name}</span>}
                        <ServicePinChips flow={flow} service={svc} />
                      </div>
                    </div>
                  );
                })
              )}
            </div>
          </DisclosureRow>

          <div className="rounded-md border p-3">
            <div className="mb-2 text-xs font-medium">Topics</div>
            {flow.topics.length === 0 ? (
              <div className="text-xs text-muted-foreground">No topics — the flow writes to no kafka-family destination yet.</div>
            ) : (
              <div className="space-y-1.5">
                {flow.topics.map((topic) => {
                  const sinks = kcBlocks.filter((b) => (b.config.attachTopicId as string) === topic.id);
                  return (
                    <div key={topic.id} className="flex flex-wrap items-center gap-2">
                      <code className="text-xs">{topic.name}</code>
                      {topic.sealed && <StatusBadge status="Sealed" />}
                      {topic.kind === "adopted" && <StatusBadge status="Adopted" />}
                      {sinks.map((kc) => {
                        const kcSvc = services.find((s) => s.id === sinkServiceId(kc));
                        return (
                          <span
                            key={kc.id}
                            className="inline-flex items-center gap-1 rounded-full border border-dashed border-muted-foreground/40 px-2 py-0.5 text-xs text-muted-foreground"
                          >
                            kc · {kc.name}
                            {kcSvc ? ` → ${kcSvc.name}` : ""}
                          </span>
                        );
                      })}
                    </div>
                  );
                })}
              </div>
            )}
            <div className="mt-3 border-t pt-2 text-xs text-muted-foreground">
              DLQ <code className="text-foreground">{dlqName(flow.name)}</code>
            </div>
          </div>
        </TabsContent>

        {/* ── Metrics ── */}
        <TabsContent value="metrics" className="mt-4 space-y-4">
          {metricsQuery.isLoading ? (
            <div className="p-6 text-center text-sm text-muted-foreground">Loading metrics…</div>
          ) : !metrics ? (
            <div className="rounded-md border p-8 text-center">
              <Activity className="mx-auto h-6 w-6 text-muted-foreground" />
              <div className="mt-2 text-sm font-medium">Metrics unavailable</div>
              <div className="mt-1 text-xs text-muted-foreground">
                This flow has never reported runtime metrics — nothing to show. We never fake zeros.
              </div>
            </div>
          ) : (
            <>
              <div className="grid grid-cols-2 gap-3">
                <div className="rounded-md border p-3">
                  <div className="text-xs text-muted-foreground">Records (24h)</div>
                  <div className="mt-1 text-lg font-semibold">{metrics.records24h.toLocaleString()}</div>
                </div>
                <div className="rounded-md border p-3">
                  <div className="text-xs text-muted-foreground">Errors (24h)</div>
                  <div className={`mt-1 text-lg font-semibold ${metrics.errors24h > 0 ? "text-destructive" : ""}`}>
                    {metrics.errors24h.toLocaleString()}
                  </div>
                </div>
                <div className="rounded-md border p-3">
                  <div className="text-xs text-muted-foreground">Queued</div>
                  <div className="mt-1 text-lg font-semibold">{metrics.queued.toLocaleString()}</div>
                </div>
                <div className="rounded-md border p-3">
                  <div className="text-xs text-muted-foreground">Last run</div>
                  <div className="mt-1">
                    {metrics.lastRunOutcome ? <StatusBadge status={metrics.lastRunOutcome} /> : <span className="text-sm text-muted-foreground">—</span>}
                  </div>
                </div>
              </div>

              <div className="overflow-x-auto rounded-md border">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Block</TableHead>
                      <TableHead className="text-right">In</TableHead>
                      <TableHead className="text-right">Out</TableHead>
                      <TableHead className="text-right">Queued</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {metrics.perBlock.map((b) => (
                      <TableRow key={b.blockId}>
                        <TableCell className="py-2 text-xs">{b.label}</TableCell>
                        <TableCell className="py-2 text-right font-mono text-xs">{b.recordsIn.toLocaleString()}</TableCell>
                        <TableCell className="py-2 text-right font-mono text-xs">{b.recordsOut.toLocaleString()}</TableCell>
                        <TableCell className="py-2 text-right font-mono text-xs">{b.queued.toLocaleString()}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>

              <div className="rounded-md border p-3">
                <div className="mb-2 text-xs font-medium">Topic message counts</div>
                <div className="space-y-1">
                  {metrics.topicCounts.map((t) => (
                    <div key={t.topic} className="flex items-center justify-between gap-2 text-xs">
                      <code>{t.topic}</code>
                      <span className="font-mono text-muted-foreground">{t.messages.toLocaleString()}</span>
                    </div>
                  ))}
                </div>
              </div>
            </>
          )}
        </TabsContent>

        {/* ── DLQ ── */}
        <TabsContent value="dlq" className="mt-4 space-y-3">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <p className="text-xs text-muted-foreground">
              One DLQ per flow: <code className="text-foreground">{dlqName(flow.name)}</code> · 3 retries then here ·
              7-day retention · no automated replay.
            </p>
            <div className="flex items-center gap-2">
              <Button
                size="sm"
                variant="outline"
                disabled={dlq.length === 0}
                onClick={() => downloadJson(`${dlqName(flow.name)}.json`, dlq)}
              >
                <Download className="mr-1.5 h-3.5 w-3.5" /> Download
              </Button>
              <Button
                size="sm"
                variant="outline"
                className="text-destructive hover:text-destructive"
                disabled={clearTopicMut.isPending}
                onClick={() => setClearTopicTarget(dlqName(flow.name))}
              >
                {clearTopicMut.isPending && clearTopicMut.variables === dlqName(flow.name) ? (
                  <RefreshCw className="mr-1.5 h-3.5 w-3.5 animate-spin" />
                ) : (
                  <Eraser className="mr-1.5 h-3.5 w-3.5" />
                )}
                Clear DLQ
              </Button>
            </div>
          </div>
          {dlqQuery.isLoading ? (
            <div className="p-6 text-center text-sm text-muted-foreground">Loading DLQ records…</div>
          ) : dlq.length === 0 ? (
            <div className="rounded-md border p-8 text-center text-sm text-muted-foreground">
              No dead-lettered records.
            </div>
          ) : (
            <div className="overflow-x-auto rounded-md border">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-[80px]">Time</TableHead>
                    <TableHead className="w-[140px]">Block</TableHead>
                    <TableHead className="w-[170px]">Error class</TableHead>
                    <TableHead>Payload preview</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {dlq.map((rec) => (
                    <TableRow key={rec.id}>
                      <TableCell className="py-2 text-xs text-muted-foreground">{timeAgo(rec.ts)}</TableCell>
                      <TableCell className="py-2 text-xs">{rec.blockName}</TableCell>
                      <TableCell className="py-2">
                        <code className="rounded bg-muted px-1.5 py-0.5 text-xs">{rec.errorClass}</code>
                      </TableCell>
                      <TableCell className="py-2">
                        <div className="max-w-[260px] truncate font-mono text-xs text-muted-foreground" title={rec.payloadPreview}>
                          {rec.payloadPreview}
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </TabsContent>

        {/* ── Messages ── */}
        <TabsContent value="messages" className="mt-4 space-y-3">
          {flow.topics.length === 0 ? (
            <div className="rounded-md border p-8 text-center text-sm text-muted-foreground">
              The flow has no topics to inspect.
            </div>
          ) : (
            <>
              <div className="flex flex-wrap items-center gap-2">
                <Select value={msgTopic ?? undefined} onValueChange={setMsgTopic}>
                  <SelectTrigger className="w-[320px] font-mono text-xs">
                    <SelectValue placeholder="Pick a topic" />
                  </SelectTrigger>
                  <SelectContent>
                    {flow.topics.map((t) => (
                      <SelectItem key={t.id} value={t.name} className="font-mono text-xs">
                        {t.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <Button
                  size="sm"
                  variant="outline"
                  className="text-destructive hover:text-destructive"
                  disabled={!msgTopic || clearTopicMut.isPending}
                  onClick={() => msgTopic && setClearTopicTarget(msgTopic)}
                >
                  {clearTopicMut.isPending && clearTopicMut.variables === msgTopic ? (
                    <RefreshCw className="mr-1.5 h-3.5 w-3.5 animate-spin" />
                  ) : (
                    <Eraser className="mr-1.5 h-3.5 w-3.5" />
                  )}
                  Clear topic
                </Button>
              </div>
              <p className="text-xs text-muted-foreground">
                Group-less viewer — nothing is committed. Avro payloads are not decoded here. Newest first, capped at 50.
              </p>
              {messagesQuery.isLoading ? (
                <div className="p-6 text-center text-sm text-muted-foreground">Loading messages…</div>
              ) : messages.length === 0 ? (
                <div className="rounded-md border p-8 text-center text-sm text-muted-foreground">
                  No messages readable on <code>{msgTopic}</code>.
                </div>
              ) : (
                <div className="space-y-1.5">
                  {messages.map((m) => (
                    <div key={m.offset} className="rounded-md border p-2 font-mono text-xs">
                      <div className="flex flex-wrap gap-x-3 text-muted-foreground">
                        <span>offset {m.offset}</span>
                        <span>{timeAgo(m.ts)}</span>
                        <span>key {m.key ?? "—"}</span>
                      </div>
                      <div className="mt-1 break-all">
                        {m.value !== null ? (
                          m.value
                        ) : (
                          <span className="font-sans italic text-muted-foreground">binary payload ({m.bytes} bytes)</span>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </>
          )}
        </TabsContent>

        {/* ── Runtime (read-only: components, services, Connect, drift) ── */}
        <TabsContent value="runtime" className="mt-4">
          <RuntimeTab flow={flow} services={services} connections={connections} onEdit={onEdit} />
        </TabsContent>

        {hasKafkaConnectSink && (
          <TabsContent value="sync" className="mt-4">
            <SyncTab flow={flow} queueLockReason={queueLockReason ?? null} onEdit={onEdit} />
          </TabsContent>
        )}
      </Tabs>

      {/* ── Clear topic confirmation (Messages tab "Clear topic" / DLQ tab "Clear DLQ") ── */}
      <AlertDialog open={!!clearTopicTarget} onOpenChange={(open) => !open && setClearTopicTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Clear all retained messages from "{clearTopicTarget}"?</AlertDialogTitle>
            <AlertDialogDescription>
              This cannot be undone. The action is audited.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              onClick={() => {
                if (clearTopicTarget) clearTopicMut.mutate(clearTopicTarget);
                setClearTopicTarget(null);
              }}
            >
              {clearTopicActionLabelValue}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* ── Clear dedup cache confirmation ── */}
      <AlertDialog open={!!clearDedupTarget} onOpenChange={(open) => !open && setClearDedupTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Clear the dedup cache for "{clearDedupTarget?.name}"?</AlertDialogTitle>
            <AlertDialogDescription>
              Previously suppressed records become eligible again before the window expires. This action is audited.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => {
                if (clearDedupTarget) clearDedupMut.mutate(clearDedupTarget);
                setClearDedupTarget(null);
              }}
            >
              Clear cache
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </SheetContent>
  );
}

// ─── Save as Connector dialog ───────────────────────────────────────────────

function SaveConnectorDialog({
  flow,
  services,
  connectors,
  onClose,
}: {
  flow: Flow | null;
  services: AppService[];
  connectors: ConnectorExport[];
  onClose: () => void;
}) {
  const qc = useQueryClient();
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");

  useEffect(() => {
    if (flow) {
      setName(suggestConnectorName(flow, services));
      setDescription(flow.description ?? "");
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [flow?.id]);

  const nextVersion = useMemo(() => {
    const existing = connectors.filter((c) => c.name === name.trim());
    return existing.length > 0 ? Math.max(...existing.map((c) => c.version)) + 1 : 1;
  }, [connectors, name]);

  const publishMut = useMutation({
    mutationFn: () => publishConnector(flow!.id, name.trim(), description.trim() || undefined),
    onSuccess: (connector) => {
      toast.success(`Published ${connector.name}@${connector.version}`, {
        description: "Connector bundle downloaded — no secrets, no environment details.",
      });
      downloadJson(`${connector.name}@${connector.version}.connector.json`, {
        connector,
        flowName: flow!.name,
        blocks: flow!.blocks.map((b) => ({ adapter: b.adapter, mode: b.mode ?? null, name: b.name })),
      });
      qc.invalidateQueries({ queryKey: ["connectors"] });
      onClose();
    },
    onError: (e: Error) => toast.error("Publish failed", { description: e.message }),
  });

  return (
    <Dialog open={!!flow} onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Save as Connector</DialogTitle>
          <DialogDescription>
            Publish "{flow?.name}" as a reusable connector bundle.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-3">
          <div className="space-y-1.5">
            <Label htmlFor="connector-name">Connector name</Label>
            <Input
              id="connector-name"
              className="font-mono text-sm"
              value={name}
              onChange={(e) => setName(e.target.value)}
            />
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="connector-desc">Description</Label>
            <Textarea
              id="connector-desc"
              rows={2}
              value={description}
              onChange={(e) => setDescription(e.target.value)}
            />
          </div>
          <div className="rounded-md border bg-muted/30 p-2.5 text-xs">
            Will publish <code className="text-foreground">{name.trim() || "…"}@{nextVersion}</code>
          </div>
          <p className="text-xs text-muted-foreground">
            Connectors travel without secrets or environment details. Immutable once published.
          </p>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button disabled={!name.trim() || publishMut.isPending} onClick={() => publishMut.mutate()}>
            {publishMut.isPending ? <RefreshCw className="mr-1.5 h-3.5 w-3.5 animate-spin" /> : <Package className="mr-1.5 h-3.5 w-3.5" />}
            Publish
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

// ─── Import Connector wizard ─────────────────────────────────────────────────

const KNOWN_ADAPTER_IDS: AdapterId[] = ["http", "jdbc", "kafka", "kafka_kc", "kc"];

/** Minimal shape a picked connector bundle JSON must have (mirrors what
 *  SaveConnectorDialog's downloadJson writes: {connector, flowName, blocks}). */
interface PickedBundle {
  fileName: string;
  bundleName: string;
  version: number;
  blocks: { adapter: AdapterId; mode?: BlockMode; name: string }[];
}

function parseBundleFile(fileName: string, text: string): PickedBundle {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    throw new Error(`"${fileName}" is not valid JSON.`);
  }
  if (!parsed || typeof parsed !== "object") throw new Error("The connector bundle must be a JSON object.");
  const obj = parsed as Record<string, unknown>;
  const blocksRaw = obj.blocks;
  if (!Array.isArray(blocksRaw) || blocksRaw.length === 0) {
    throw new Error("The connector bundle has no blocks.");
  }
  const blocks = blocksRaw.map((b, i) => {
    const rec = (b && typeof b === "object" ? b : {}) as Record<string, unknown>;
    const adapter = rec.adapter;
    if (typeof adapter !== "string" || !KNOWN_ADAPTER_IDS.includes(adapter as AdapterId)) {
      throw new Error(`Block ${i + 1} in the bundle has an unrecognized adapter.`);
    }
    return {
      adapter: adapter as AdapterId,
      mode: typeof rec.mode === "string" ? (rec.mode as BlockMode) : undefined,
      name: typeof rec.name === "string" ? rec.name : `Block ${i + 1}`,
    };
  });
  const connector = (obj.connector && typeof obj.connector === "object" ? obj.connector : {}) as Record<string, unknown>;
  const bundleName =
    (typeof connector.name === "string" && connector.name) ||
    (typeof obj.flowName === "string" && obj.flowName) ||
    fileName.replace(/\.connector\.json$|\.json$/i, "");
  const version = typeof connector.version === "number" ? connector.version : 1;
  return { fileName, bundleName, version, blocks };
}

function ImportConnectorDialog({
  open,
  services,
  onClose,
}: {
  open: boolean;
  services: AppService[];
  onClose: () => void;
}) {
  const [step, setStep] = useState(1);
  const [bundle, setBundle] = useState<PickedBundle | null>(null);
  const [httpServiceId, setHttpServiceId] = useState("");
  const [sinkServiceIdSel, setSinkServiceIdSel] = useState("");
  const [flowName, setFlowName] = useState("");
  const [importing, setImporting] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  useEffect(() => {
    if (open) {
      setStep(1);
      setBundle(null);
      setHttpServiceId("");
      setSinkServiceIdSel("");
      setFlowName("");
      if (fileInputRef.current) fileInputRef.current.value = "";
    }
  }, [open]);

  const handleFilePicked = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    e.target.value = ""; // allow re-picking the same file
    if (!file) return;
    try {
      const text = await file.text();
      const picked = parseBundleFile(file.name, text);
      setBundle(picked);
      setFlowName(`${picked.bundleName} (imported)`);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Could not read that file.");
    }
  };

  const httpServices = services.filter((s) => s.type === "http" && !s.retired);
  const sinkServices = services.filter((s) => s.type === "sink_destination" && !s.retired);

  const canNext = step === 1 ? !!bundle : step === 2 ? Boolean(httpServiceId && sinkServiceIdSel) : flowName.trim().length > 0;

  const finish = async () => {
    setImporting(true);
    try {
      const flow = await importConnectorFlow({
        flowName: flowName.trim(),
        httpServiceId,
        sinkServiceId: sinkServiceIdSel,
      });
      queryClient.invalidateQueries({ queryKey: ["flows"] });
      toast.success(`Imported ${bundle?.bundleName ?? "connector"}@${bundle?.version ?? 1} as draft "${flow.name}"`, {
        description: "Review the bound services and deploy when ready.",
      });
      onClose();
      navigate(`/flow-builder/${flow.id}`);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Import failed");
    } finally {
      setImporting(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Import Connector</DialogTitle>
          <DialogDescription>Step {step} of 3 — {step === 1 ? "pick a bundle" : step === 2 ? "bind services" : "name the flow"}</DialogDescription>
        </DialogHeader>

        {step === 1 && (
          <div className="space-y-3">
            <input
              ref={fileInputRef}
              type="file"
              accept=".json"
              className="hidden"
              onChange={handleFilePicked}
            />
            {!bundle ? (
              <Button variant="outline" onClick={() => fileInputRef.current?.click()}>
                <Upload className="mr-1.5 h-3.5 w-3.5" /> Choose file…
              </Button>
            ) : (
              <div className="rounded-md border p-3">
                <div className="flex items-center justify-between gap-2">
                  <div className="flex items-center gap-2 text-sm font-medium">
                    <FileJson className="h-4 w-4 text-muted-foreground" />
                    <code className="text-xs">{bundle.fileName}</code>
                  </div>
                  <Button variant="ghost" size="sm" className="h-6 px-2 text-xs" onClick={() => fileInputRef.current?.click()}>
                    Change…
                  </Button>
                </div>
                <div className="mt-2 text-xs text-muted-foreground">
                  {bundle.bundleName} <code>@{bundle.version}</code>
                </div>
                <div className="mt-2 flex flex-wrap items-center gap-1.5">
                  {bundle.blocks.map((b, i) => (
                    <span key={`${b.name}-${i}`} className="flex items-center gap-1.5">
                      {i > 0 && <span className="text-muted-foreground">→</span>}
                      <AdapterChip adapter={b.adapter} mode={b.mode} />
                    </span>
                  ))}
                </div>
              </div>
            )}
            <p className="text-xs text-muted-foreground">
              Pick a connector bundle exported from "Save as Connector" (JSON). Its identity travels along; the actual
              read/sink services are re-bound to yours on the next step.
            </p>
          </div>
        )}

        {step === 2 && (
          <div className="space-y-3">
            <p className="text-xs text-muted-foreground">
              The bundle travels without environment details. Bind its service placeholders to your Application Services.
            </p>
            <div className="space-y-1.5">
              <Label>HTTP service</Label>
              <Select value={httpServiceId} onValueChange={setHttpServiceId}>
                <SelectTrigger><SelectValue placeholder="Select an HTTP service" /></SelectTrigger>
                <SelectContent>
                  {httpServices.map((s) => (
                    <SelectItem key={s.id} value={s.id}>{s.name}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-1.5">
              <Label>Sink destination</Label>
              <Select value={sinkServiceIdSel} onValueChange={setSinkServiceIdSel}>
                <SelectTrigger><SelectValue placeholder="Select a sink destination" /></SelectTrigger>
                <SelectContent>
                  {sinkServices.map((s) => (
                    <SelectItem key={s.id} value={s.id}>{s.name}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
        )}

        {step === 3 && (
          <div className="space-y-3">
            <div className="space-y-1.5">
              <Label htmlFor="import-flow-name">New flow name</Label>
              <Input id="import-flow-name" value={flowName} onChange={(e) => setFlowName(e.target.value)} />
            </div>
            <p className="text-xs text-muted-foreground">
              The name becomes the first half of every derived topic and DLQ name.
            </p>
          </div>
        )}

        <DialogFooter>
          {step > 1 && (
            <Button variant="ghost" onClick={() => setStep((s) => s - 1)}>Back</Button>
          )}
          {step < 3 ? (
            <Button disabled={!canNext} onClick={() => setStep((s) => s + 1)}>Next</Button>
          ) : (
            <Button disabled={!canNext || importing} onClick={() => void finish()}>
              {importing ? <RefreshCw className="mr-1.5 h-3.5 w-3.5 animate-spin" /> : null}
              Import & open builder
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

// ─── The page ───────────────────────────────────────────────────────────────

const Flows = () => {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const qc = useQueryClient();

  const [search, setSearch] = useState("");
  const debouncedSearch = useDebouncedValue(search, 180);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [openId, setOpenId] = useState<string | null>(null);
  const [pendingVerbs, setPendingVerbs] = useState<Record<string, FlowVerb>>({});
  const [activeJobId, setActiveJobId] = useState<string | null>(null);
  const [lastFinishedJobId, setLastFinishedJobId] = useState<string | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<Flow | null>(null);
  const [cascadeChecked, setCascadeChecked] = useState<Set<string>>(new Set());
  const [deleteTopics, setDeleteTopics] = useState(true);
  const [clearCache, setClearCache] = useState(false);
  const [bulkDeleteOpen, setBulkDeleteOpen] = useState(false);
  const [connectorFlow, setConnectorFlow] = useState<Flow | null>(null);
  const [importOpen, setImportOpen] = useState(false);

  const openDeleteFlow = useCallback((flow: Flow) => {
    setDeleteTarget(flow);
    setCascadeChecked(new Set());
    // Preserve the existing delete behaviour unless the user changes a
    // cleanup choice in this confirmation.
    setDeleteTopics(true);
    setClearCache(false);
  }, []);

  const { data: flows = [], isLoading, error } = useQuery({
    queryKey: ["flows"],
    queryFn: listFlows,
    refetchInterval: 15000, // the only polling on this page
  });
  const { data: schemas = [] } = useQuery({ queryKey: ["schemas"], queryFn: listSchemas });
  const { data: services = [] } = useQuery({ queryKey: ["services"], queryFn: listServices });
  const { data: connections = [] } = useQuery({ queryKey: ["connections"], queryFn: listConnections });
  const { data: connectors = [] } = useQuery({ queryKey: ["connectors"], queryFn: listConnectors });
  const { data: kafkaConnectSyncs = [] } = useQuery({
    queryKey: ["kafkaConnectSyncs"],
    queryFn: listKafkaConnectSyncs,
  });

  // The builder routes its Delete action here so both entry points share this
  // confirmation and its cleanup choices instead of having a second delete
  // implementation that could drift.
  useEffect(() => {
    const requestedId = searchParams.get("delete");
    if (!requestedId || isLoading) return;
    const target = flows.find((flow) => flow.id === requestedId);
    const next = new URLSearchParams(searchParams);
    next.delete("delete");
    setSearchParams(next, { replace: true });
    if (target) openDeleteFlow(target);
  }, [flows, isLoading, openDeleteFlow, searchParams, setSearchParams]);

  // Reattach to a run already in flight. Without this, a refresh mid-deploy
  // would leave the job running server-side with nothing showing it.
  const { data: adoptedJob } = useQuery({
    queryKey: ["bulkJob", "active"],
    queryFn: getActiveBulkJob,
    refetchInterval: activeJobId ? false : 5000,
  });
  const { data: operationQueue = [] } = useQuery({
    queryKey: ["flowOperationQueue"],
    queryFn: getBulkQueue,
    refetchInterval: 1200,
  });
  useEffect(() => {
    if (!activeJobId && adoptedJob && !isBulkJobTerminal(adoptedJob)) setActiveJobId(adoptedJob.id);
  }, [adoptedJob, activeJobId]);

  const { data: bulkJob } = useQuery({
    queryKey: ["bulkJob", activeJobId],
    queryFn: () => getBulkJob(activeJobId as string),
    enabled: !!activeJobId,
    // Poll fast while running, then stop dead once terminal.
    refetchInterval: (query) => (isBulkJobTerminal(query.state.data) ? false : 1200),
  });

  // A run in progress disables the bulk buttons, exactly as bulkPending did.
  const bulkPending: BulkAction | null = null;

  // Per-flow status for the row indicators, keyed by flow id.
  const bulkItemByFlowId = useMemo(() => {
    const map = new Map<string, BulkJobItem>();
    if (bulkJob && !isBulkJobTerminal(bulkJob)) {
      for (const item of bulkJob.items) map.set(item.flowId, item);
    }
    return map;
  }, [bulkJob]);

  // Terminal transition: report once, refresh the flows list once (the old
  // in-tab loop refetched the whole list after every single flow).
  useEffect(() => {
    if (!bulkJob || !isBulkJobTerminal(bulkJob) || lastFinishedJobId === bulkJob.id) return;
    setLastFinishedJobId(bulkJob.id);
    setActiveJobId(null);

    const label = BULK_LABEL[bulkJob.verb as BulkAction] ?? bulkJob.verb;
    if (bulkJob.status === "interrupted") {
      toast.error(`${label}: interrupted`, {
        description: bulkJob.error ?? "The backend restarted mid-run. Re-run for the remaining flows.",
      });
    } else if (bulkJob.status === "cancelled") {
      toast.warning(`${label}: cancelled after ${bulkJob.succeeded} flow(s)`);
    } else if (bulkJob.failed === 0) {
      toast.success(`${label}: ${bulkJob.succeeded} flow${bulkJob.succeeded === 1 ? "" : "s"} done`);
    } else {
      toast.warning(`${label}: ${bulkJob.succeeded} succeeded, ${bulkJob.failed} failed`, {
        description: bulkJob.items
          .filter((i) => i.status === "failed")
          .slice(0, 3)
          .map((i) => `${i.flowName}: ${i.error ?? "failed"}`)
          .join(" · "),
      });
    }
    qc.invalidateQueries({ queryKey: ["flows"] });
    qc.invalidateQueries({ queryKey: ["bulkJob", "active"] });
  }, [bulkJob, lastFinishedJobId, qc]);

  // Prune selection when flows disappear
  useEffect(() => {
    setSelectedIds((prev) => {
      const valid = new Set(flows.map((f) => f.id));
      const next = new Set(Array.from(prev).filter((id) => valid.has(id)));
      return next.size === prev.size ? prev : next;
    });
  }, [flows]);

  // Close the sheet if the open flow is gone
  useEffect(() => {
    if (openId && !isLoading && !flows.some((f) => f.id === openId)) setOpenId(null);
  }, [openId, flows, isLoading]);

  const missingRuntime = useMemo(
    () =>
      (["nifi", "kafka", "apicurio"] as const).filter(
        (t) => !connections.some((c) => c.type === t && c.active && c.health === "Healthy"),
      ),
    [connections],
  );

  const filteredFlows = useMemo(
    () => flows.filter((f) => flowMatchesSearch(f, debouncedSearch.trim().toLowerCase())),
    [flows, debouncedSearch],
  );

  const selectedFlows = useMemo(() => flows.filter((f) => selectedIds.has(f.id)), [flows, selectedIds]);
  const queueLockByFlow = useMemo(() => {
    const result = new Map<string, { verb: string; itemId: string; status: string }>();
    for (const job of operationQueue) {
      if (isBulkJobTerminal(job)) continue;
      for (const item of job.items) {
        if ((item.status === "pending" || item.status === "running") && !result.has(item.flowId)) {
          result.set(item.flowId, { verb: job.verb, itemId: item.id, status: item.status });
        }
      }
    }
    return result;
  }, [operationQueue]);
  const flowQueueLockReason = (flowId: string): string | null => {
    const lock = queueLockByFlow.get(flowId);
    return lock ? `Locked by queued ${lock.verb.replace("_", " ")} operation.` : null;
  };
  const visibleIds = filteredFlows.map((f) => f.id);
  const selectedVisibleCount = visibleIds.filter((id) => selectedIds.has(id)).length;
  const headerChecked: boolean | "indeterminate" =
    visibleIds.length > 0 && selectedVisibleCount === visibleIds.length
      ? true
      : selectedVisibleCount > 0
        ? "indeterminate"
        : false;

  const toggleAllVisible = () => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      const allSelected = visibleIds.length > 0 && visibleIds.every((id) => next.has(id));
      if (allSelected) visibleIds.forEach((id) => next.delete(id));
      else visibleIds.forEach((id) => next.add(id));
      return next;
    });
  };

  const openFlow = openId ? flows.find((f) => f.id === openId) ?? null : null;

  // ── mutations ──
  const verbMut = useMutation({
    mutationFn: ({ flow, verb }: { flow: Flow; verb: FlowVerb }) => startBulkJob(verb, [flow.id]),
    onMutate: ({ flow, verb }) => {
      setPendingVerbs((p) => ({ ...p, [flow.id]: verb }));
      if (verb === "deploy" || verb === "redeploy") {
        // the mock flips the state to "Deploying" synchronously — surface it
        window.setTimeout(() => qc.invalidateQueries({ queryKey: ["flows"] }), 200);
      }
    },
    onSuccess: (_res, { flow, verb }) => {
      toast.info(`Queued ${VERB_META[verb].label} — ${flow.name}`);
    },
    onError: (e: Error, { flow, verb }) =>
      toast.error(`${VERB_META[verb].label} failed — ${flow.name}`, { description: e.message }),
    onSettled: (_r, _e, { flow }) => {
      setPendingVerbs((p) => {
        const { [flow.id]: _gone, ...rest } = p;
        return rest;
      });
      qc.invalidateQueries({ queryKey: ["flows"] });
    },
  });

  const enableMut = useMutation({
    mutationFn: ({ flow, enabled }: { flow: Flow; enabled: boolean }) =>
      startBulkJob(enabled ? "enable" : "disable", [flow.id]),
    onSuccess: (_r, { flow, enabled }) => toast.info(`Queued ${enabled ? "Enable" : "Disable"} — ${flow.name}`),
    onError: (e: Error) => toast.error("Could not change enabled state", { description: e.message }),
    onSettled: () => qc.invalidateQueries({ queryKey: ["flows"] }),
  });

  const runVerb = (flow: Flow, verb: FlowVerb) => {
    if (flowQueueLockReason(flow.id)) {
      toast.warning("Flow is already in the operation queue", { description: "Cancel its queued item or wait for it to finish." });
      return;
    }
    verbMut.mutate({ flow, verb });
  };

  // ── bulk operations ──
  //
  // The run happens on the server as a background job, not in this tab. We
  // POST once, then poll the job for progress. Closing or refreshing the page
  // no longer kills the run — on mount we reattach to whatever is in flight.
  const startBulkMut = useMutation({
    mutationFn: ({ action, flowIds }: { action: BulkAction; flowIds: string[] }) =>
      startBulkJob(action, flowIds),
    onSuccess: (jobId) => setActiveJobId(jobId),
    onError: (e: Error) => toast.error("Could not start the bulk run", { description: e.message }),
  });

  const runBulk = (action: BulkAction) => {
    const targets = selectedFlows.filter((f) => !flowQueueLockReason(f.id));
    if (targets.length === 0) return;
    startBulkMut.mutate({ action, flowIds: targets.map((f) => f.id) });
  };

  const cancelBulkMut = useMutation({
    mutationFn: (jobId: string) => cancelBulkJob(jobId),
    onError: (e: Error) => toast.error("Could not cancel", { description: e.message }),
  });

  // ── cascade delete ──
  //
  // Deleting a flow can optionally take its schemas, application services and
  // gateway proxies with it. Anything shared with another flow is offered but
  // not deletable, so a delete here can never break an unrelated flow.
  const cascadeTargets = useMemo(
    () => (deleteTarget ? flowCascadeTargets(deleteTarget, undefined, kafkaConnectSyncs) : []),
    // `flows`/`services`/`schemas` are listed so the shared-with calculation
    // re-runs when the underlying caches are refreshed.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [deleteTarget, flows, services, schemas, kafkaConnectSyncs],
  );

  const cascadeDeleteMut = useMutation({
    mutationFn: async ({
      flow,
      extras,
      deleteOptions,
    }: {
      flow: Flow;
      extras: CascadeTarget[];
      deleteOptions: FlowDeleteOptions;
    }) => {
      // Order matters. Undeploy first (a deployed flow cannot be dropped),
      // then the flow, and only then its extras — the application-service
      // guard refuses while any flow still references the service, so the
      // flow has to be gone before its service can be.
      const jobId = await startBulkJob("delete", [flow.id], deleteOptions);
      const result = await waitForBulkJob(jobId);
      if (result.status !== "completed" || result.failed > 0) {
        throw new Error(result.items[0]?.error ?? "Flow deletion failed.");
      }

      const failures: string[] = [];
      for (const target of extras) {
        try {
          await deleteCascadeTarget(target);
        } catch (e) {
          failures.push(`${target.name}: ${(e as Error).message}`);
        }
      }
      return { flow, extras, failures };
    },
    onSuccess: ({ flow, extras, failures }) => {
      const extrasNote = extras.length > 0 ? ` and ${extras.length - failures.length} associated resource(s)` : "";
      if (failures.length === 0) {
        toast.success(`Deleted "${flow.name}"${extrasNote}`);
      } else {
        // The flow itself is gone — only some extras survived.
        toast.warning(`Deleted "${flow.name}", but ${failures.length} extra(s) could not be removed`, {
          description: failures.slice(0, 3).join(" · "),
        });
      }
      setOpenId((cur) => (cur === flow.id ? null : cur));
      setSelectedIds((prev) => {
        const next = new Set(prev);
        next.delete(flow.id);
        return next;
      });
      setDeleteTarget(null);
      setCascadeChecked(new Set());
      setDeleteTopics(true);
      setClearCache(false);
      qc.invalidateQueries({ queryKey: ["flows"] });
      qc.invalidateQueries({ queryKey: ["services"] });
      qc.invalidateQueries({ queryKey: ["schemas"] });
      qc.invalidateQueries({ queryKey: ["gatewayProxies"] });
      qc.invalidateQueries({ queryKey: ["kafkaConnectSyncs"] });
      qc.invalidateQueries({ queryKey: ["audit"] });
    },
    onError: (e: Error) => toast.error("Could not delete the flow", { description: e.message }),
  });

  const bulkDeleteTargets = selectedFlows.filter((f) => !flowQueueLockReason(f.id));

  // ─── render ───
  return (
    <AppLayout
      title="Flows"
      description="The operational console — deploy, run, and inspect every adapter flow."
      actions={
        <>
          <Button variant="outline" onClick={() => setImportOpen(true)}>
            <Upload className="mr-2 h-4 w-4" /> Import Connector
          </Button>
          <Button onClick={() => navigate("/flow-builder/new")}>
            <Plus className="mr-2 h-4 w-4" /> New Flow
          </Button>
        </>
      }
    >
      {missingRuntime.length > 0 && (
        <Alert className="mb-4 border-warning/40 bg-warning-muted/40">
          <AlertTriangle className="h-4 w-4 text-warning" />
          <AlertTitle>Runtime connections unavailable: {missingRuntime.map((t) => RUNTIME_LABELS[t]).join(", ")} — flows cannot start</AlertTitle>
          <AlertDescription className="text-xs">
            Start requires an active, healthy NiFi, Kafka, and Apicurio connection.{" "}
            <Link to="/connections" className="font-medium underline underline-offset-2">
              Open Platform Connections
            </Link>
          </AlertDescription>
        </Alert>
      )}

      {selectedFlows.length > 0 && (
        <div className="mb-4 flex flex-col gap-3 rounded-md border bg-card p-3 shadow-sm md:flex-row md:items-center md:justify-between">
          <div className="text-sm font-medium">{selectedFlows.length} selected</div>
          <div className="flex flex-wrap gap-2">
            {(
              [
                ["start", Play],
                ["pause", Pause],
                ["resume", Play],
                ["stop", Square],
                ["stop_clear", Eraser],
                ["deploy", Rocket],
                ["redeploy", Rocket],
                ["undeploy", PackageX],
                ["enable", CheckCircle2],
                ["disable", XCircle],
              ] as const
            ).map(([action, Icon]) => {
              const eligible = selectedFlows.filter((f) => !flowQueueLockReason(f.id)).length;
              return (
                <span key={action} title={`${eligible} of ${selectedFlows.length} eligible`} className="inline-flex">
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={eligible === 0}
                    onClick={() => runBulk(action)}
                  >
                    {bulkPending === action ? (
                      <RefreshCw className="mr-2 h-3.5 w-3.5 animate-spin" />
                    ) : (
                      <Icon className="mr-2 h-3.5 w-3.5" />
                    )}
                    {BULK_LABEL[action]}
                  </Button>
                </span>
              );
            })}
            <span title={`${bulkDeleteTargets.length} of ${selectedFlows.length} eligible`} className="inline-flex">
              <Button
                variant="outline"
                size="sm"
                className="text-destructive hover:text-destructive"
                disabled={bulkDeleteTargets.length === 0}
                onClick={() => setBulkDeleteOpen(true)}
              >
                {bulkPending === "delete" ? (
                  <RefreshCw className="mr-2 h-3.5 w-3.5 animate-spin" />
                ) : (
                  <Trash2 className="mr-2 h-3.5 w-3.5" />
                )}
                Delete
              </Button>
            </span>
            <Button variant="ghost" size="sm" onClick={() => setSelectedIds(new Set())}>
              Clear
            </Button>
          </div>
        </div>
      )}

      {/* ---------------------------------------------- bulk run progress */}
      {bulkJob && !isBulkJobTerminal(bulkJob) && (
        <div className="mb-4 rounded-md border bg-card p-3 shadow-sm">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div className="flex items-center gap-2 text-sm font-medium">
              <RefreshCw className="h-3.5 w-3.5 animate-spin" />
              {BULK_LABEL[bulkJob.verb as BulkAction] ?? bulkJob.verb} in progress
              <span className="font-normal text-muted-foreground">
                {bulkJob.completed} of {bulkJob.total} · {bulkJobPercent(bulkJob)}%
                {bulkJob.failed > 0 ? ` · ${bulkJob.failed} failed` : ""}
              </span>
            </div>
            <Button
              variant="outline"
              size="sm"
              disabled={bulkJob.status !== "queued" || cancelBulkMut.isPending}
              onClick={() => cancelBulkMut.mutate(bulkJob.id)}
            >
              {bulkJob.status === "queued" ? "Cancel" : "Running — cannot cancel"}
            </Button>
          </div>
          <Progress value={bulkJobPercent(bulkJob)} className="mt-2 h-2" />
          <div className="mt-2 text-xs text-muted-foreground">
            {bulkJob.items.find((i) => i.status === "running")
              ? `Running: ${bulkJob.items.find((i) => i.status === "running")?.flowName}`
              : "Runs one flow at a time on the server — safe to close this page."}
            {bulkJob.cancelRequested && " · Cancelling after the current flow finishes."}
          </div>
        </div>
      )}

      <Card>
        <CardContent className="p-0">
          <div className="flex flex-wrap items-center justify-between gap-2 border-b px-3 py-2">
            <div className="relative w-full max-w-sm">
              <Search className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
              <Input
                className="h-8 pl-8 text-sm"
                placeholder="Search flows, entities, topics…"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
              />
            </div>
            <div className="text-xs text-muted-foreground">
              {filteredFlows.length === flows.length
                ? `${flows.length} flow${flows.length === 1 ? "" : "s"}`
                : `${filteredFlows.length} of ${flows.length} flows`}
            </div>
          </div>

          {isLoading ? (
            <div className="p-8 text-center text-sm text-muted-foreground">Loading flows…</div>
          ) : error ? (
            <div className="p-8 text-center text-sm text-destructive">
              Failed to load flows: {(error as Error).message}
            </div>
          ) : flows.length === 0 ? (
            <div className="space-y-3 p-12 text-center">
              <div className="text-sm text-muted-foreground">No flows yet.</div>
              <Button variant="outline" onClick={() => navigate("/flow-builder/new")}>
                <Plus className="mr-2 h-4 w-4" /> Create your first flow
              </Button>
            </div>
          ) : filteredFlows.length === 0 ? (
            <div className="space-y-3 p-12 text-center">
              <div className="text-sm text-muted-foreground">No flows match your search.</div>
              <Button variant="outline" onClick={() => setSearch("")}>Clear search</Button>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <Table className="min-w-[1290px] table-fixed">
                <TableHeader>
                  <TableRow>
                    <TableHead className="w-[44px]">
                      <Checkbox checked={headerChecked} onCheckedChange={toggleAllVisible} aria-label="Select all visible flows" />
                    </TableHead>
                    <TableHead className="w-[72px]">State</TableHead>
                    <TableHead className="w-[260px]">Flow Name</TableHead>
                    <TableHead className="w-[180px]">Entities</TableHead>
                    <TableHead className="w-[230px]">Topics</TableHead>
                    <TableHead className="w-[160px]">Schema</TableHead>
                    <TableHead className="w-[236px] px-1 text-right">Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filteredFlows.map((flow) => {
                    const entities = flowEntities(flow);
                    const topics = flow.topics.map((t) => t.name);
                    const schema = schemaStatus(flow, schemas);
                    const updates = serviceUpdateAvailable(flow, services);
                    const retired = retiredPinnedServices(flow, services);
                    const pending = pendingVerbs[flow.id];
                    const queueLock = queueLockByFlow.get(flow.id);
                    const primary: FlowVerb | null =
                      !flow.deployedAt || flow.state === "Deploying"
                        ? null
                        : flow.state === "Running" || flow.state === "Degraded"
                          ? "pause"
                          : flow.state === "Paused"
                            ? "resume"
                            : "start";
                    const deployVerb: FlowVerb = flow.deployedAt ? "redeploy" : "deploy";
                    const canStop = flow.state === "Running" || flow.state === "Paused" || flow.state === "Degraded";
                    const editLocked =
                      flow.state === "Running" || flow.state === "Paused" || flow.state === "Degraded" || flow.state === "Deploying" || !!flowQueueLockReason(flow.id);

                    return (
                      <TableRow key={flow.id} className={selectedIds.has(flow.id) ? "bg-muted/40" : undefined}>
                        <TableCell className="py-3">
                          <Checkbox
                            checked={selectedIds.has(flow.id)}
                            onCheckedChange={() =>
                              setSelectedIds((prev) => {
                                const next = new Set(prev);
                                if (next.has(flow.id)) next.delete(flow.id);
                                else next.add(flow.id);
                                return next;
                              })
                            }
                            aria-label={`Select ${flow.name}`}
                          />
                        </TableCell>
                        <TableCell className="py-3">
                          <div className="flex items-center gap-1">
                            <StatusBadge status={flow.state} compact />
                            {flow.drift && (
                              <Tooltip>
                                <TooltipTrigger asChild>
                                  <AlertTriangle className="h-3.5 w-3.5 shrink-0 text-warning" />
                                </TooltipTrigger>
                                <TooltipContent side="top" className="max-w-[300px] text-xs">
                                  {flow.drift}
                                </TooltipContent>
                              </Tooltip>
                            )}
                          </div>
                        </TableCell>
                        <TableCell className="py-3">
                          <div className="flex items-center gap-2">
                            <span className="truncate text-sm font-medium" title={flow.name}>{flow.name}</span>
                            {updates.length > 0 && (
                              <span title={`Service update available: ${updates.map((s) => s.name).join(", ")} — adopts at next deploy`}>
                                <StatusBadge status="Update available" className="shrink-0" />
                              </span>
                            )}
                            {retired.length > 0 && (
                              <span title={`Retired service pinned: ${retired.map((s) => s.name).join(", ")}`}>
                                <StatusBadge status="Action required" className="shrink-0" />
                              </span>
                            )}
                            {queueLock && (
                              <Badge variant="warning" className="shrink-0">
                                {queueLock.status === "running" ? "Running" : "Queued"} · {queueLock.verb.replace("_", " ")}
                              </Badge>
                            )}
                          </div>
                          {flow.description && (
                            <div className="mt-0.5 truncate text-xs text-muted-foreground" title={flow.description}>
                              {flow.description}
                            </div>
                          )}
                        </TableCell>
                        <TableCell className="py-3">
                          <span className="text-xs" title={entities.join(", ") || undefined}>
                            {summarize(entities)}
                          </span>
                        </TableCell>
                        <TableCell className="py-3">
                          {topics.length === 0 ? (
                            <span className="text-xs text-muted-foreground">—</span>
                          ) : (
                            <div className="text-xs" title={topics.join("\n")}>
                              <code className="block truncate">{topics[0]}</code>
                              {topics.length > 1 && (
                                <span className="text-muted-foreground">+{topics.length - 1} more</span>
                              )}
                            </div>
                          )}
                        </TableCell>
                        <TableCell className="py-3">
                          {schema.required === 0 ? (
                            <span className="text-xs text-muted-foreground">—</span>
                          ) : schema.approved === schema.required ? (
                            <span className="inline-flex items-center gap-1 text-xs font-medium text-success">
                              <CheckCircle2 className="h-3.5 w-3.5" /> {schema.approved}/{schema.required} approved
                            </span>
                          ) : (
                            <span className="inline-flex items-center gap-1 text-xs font-medium text-warning">
                              <AlertTriangle className="h-3.5 w-3.5" /> {schema.approved}/{schema.required} — ceremony required
                            </span>
                          )}
                        </TableCell>
                        <TableCell className="px-1 py-3 text-right">
                          <div className="flex justify-end gap-0.5">
                            <GuardedIconButton
                              label="Overview"
                              reason={null}
                              icon={Eye}
                              onClick={() => setOpenId(flow.id)}
                            />
                            {primary && (
                              <GuardedIconButton
                                label={VERB_META[primary].label}
                                reason={flowQueueLockReason(flow.id)}
                                icon={primary === "pause" ? Pause : Play}
                                spinning={pending === primary}
                                onClick={() => runVerb(flow, primary)}
                              />
                            )}
                            {canStop && (
                              <GuardedIconButton
                                label="Stop"
                                reason={flowQueueLockReason(flow.id)}
                                icon={Square}
                                spinning={pending === "stop"}
                                onClick={() => runVerb(flow, "stop")}
                              />
                            )}
                            <GuardedIconButton
                              label={VERB_META[deployVerb].label}
                              reason={flowQueueLockReason(flow.id)}
                              icon={Rocket}
                              spinning={pending === deployVerb}
                              onClick={() => runVerb(flow, deployVerb)}
                            />
                            <Tooltip>
                              <TooltipTrigger asChild>
                                <span className="inline-flex">
                                  <Button
                                    size="sm"
                                    variant="ghost"
                                    aria-label="Edit flow"
                                    disabled={editLocked}
                                    onClick={() => navigate(`/flow-builder/${flow.id}`)}
                                  >
                                    <Pencil className="h-3.5 w-3.5" />
                                  </Button>
                                </span>
                              </TooltipTrigger>
                              <TooltipContent side="top" className="text-xs">
                                {editLocked ? "Opens read-only — stop to edit" : "Edit flow"}
                              </TooltipContent>
                            </Tooltip>
                            <DropdownMenu>
                              <DropdownMenuTrigger asChild>
                                <Button size="sm" variant="ghost" aria-label="More actions">
                                  {pending && pending !== primary && pending !== "stop" && pending !== deployVerb ? (
                                    <RefreshCw className="h-3.5 w-3.5 animate-spin" />
                                  ) : (
                                    <MoreHorizontal className="h-3.5 w-3.5" />
                                  )}
                                </Button>
                              </DropdownMenuTrigger>
                              <DropdownMenuContent align="end" className="w-56">
                                {canStop && (
                                  <GuardedMenuItem
                                    reason={flowQueueLockReason(flow.id)}
                                    onSelect={() => runVerb(flow, "stop_clear")}
                                  >
                                    <Eraser className="mr-2 h-3.5 w-3.5" /> Stop &amp; Clear
                                  </GuardedMenuItem>
                                )}
                                {flow.deployedAt && (
                                  <GuardedMenuItem
                                    reason={flowQueueLockReason(flow.id)}
                                    onSelect={() => runVerb(flow, "undeploy")}
                                  >
                                    <PackageX className="mr-2 h-3.5 w-3.5" /> Undeploy
                                  </GuardedMenuItem>
                                )}
                                <DropdownMenuSeparator />
                                {flow.enabled ? (
                                  <GuardedMenuItem
                                    reason={flowQueueLockReason(flow.id)}
                                    onSelect={() => enableMut.mutate({ flow, enabled: false })}
                                  >
                                    <XCircle className="mr-2 h-3.5 w-3.5" /> Disable
                                  </GuardedMenuItem>
                                ) : (
                                  <GuardedMenuItem reason={null} onSelect={() => enableMut.mutate({ flow, enabled: true })}>
                                    <CheckCircle2 className="mr-2 h-3.5 w-3.5" /> Enable
                                  </GuardedMenuItem>
                                )}
                                <GuardedMenuItem reason={null} onSelect={() => setConnectorFlow(flow)}>
                                  <Package className="mr-2 h-3.5 w-3.5" /> Save as Connector
                                </GuardedMenuItem>
                                <DropdownMenuSeparator />
                                {/* No longer gated on deploy state: the delete
                                    dialog undeploys first as a required step,
                                    so a deployed flow is deletable from here.
                                    Bulk delete still skips deployed flows —
                                    it has no dialog to undeploy through. */}
                                <GuardedMenuItem
                                  reason={null}
                                  destructive
                                  onSelect={() => openDeleteFlow(flow)}
                                >
                                  <Trash2 className="mr-2 h-3.5 w-3.5" /> Delete
                                </GuardedMenuItem>
                              </DropdownMenuContent>
                            </DropdownMenu>
                          </div>
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>

      {/* ── Detail sheet ── */}
      <Sheet open={!!openFlow} onOpenChange={(open) => !open && setOpenId(null)}>
        {openFlow && (
          <FlowDetailSheet
            flow={openFlow}
            services={services}
            schemas={schemas}
            connections={connections}
            pendingVerb={pendingVerbs[openFlow.id]}
            queueLockReason={flowQueueLockReason(openFlow.id)}
            onVerb={(verb) => {
              if (verb === "delete") openDeleteFlow(openFlow);
              else runVerb(openFlow, verb);
            }}
            onToggleEnabled={(enabled) => enableMut.mutate({ flow: openFlow, enabled })}
            enableBusy={enableMut.isPending}
            onEdit={() => navigate(`/flow-builder/${openFlow.id}`)}
            onSaveConnector={() => setConnectorFlow(openFlow)}
          />
        )}
      </Sheet>

      {/* ── Delete confirmation (ownership proof) ── */}
      <AlertDialog
        open={!!deleteTarget}
        onOpenChange={(open) => {
          if (open) return;
           setDeleteTarget(null);
           setCascadeChecked(new Set()); // never carry ticks into the next flow
           setDeleteTopics(true);
           setClearCache(false);
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete "{deleteTarget?.name}"?</AlertDialogTitle>
            <AlertDialogDescription asChild>
              <div className="space-y-2 text-sm">
                <p>
                  Choose which flow-owned resources to clean up with the flow. Deployed flows are undeployed first.
                </p>
                {deleteTarget && deleteTarget.topics.filter((t) => t.kind === "materialized").length > 0 ? (
                  <ul className="list-disc space-y-0.5 pl-5">
                    {deleteTarget.topics
                      .filter((t) => t.kind === "materialized")
                      .map((t) => (
                        <li key={t.id}>
                          <code className="text-xs">{t.name}</code>
                        </li>
                      ))}
                    <li>
                      <code className="text-xs">{dlqName(deleteTarget.name)}</code>
                    </li>
                  </ul>
                ) : (
                  <p className="text-muted-foreground">
                    No generated data topics. The DLQ is {deleteTopics ? "also removed" : "retained"}.
                  </p>
                )}
                <p className="text-muted-foreground">
                  Adopted topics are never touched. The listed generated topics are {deleteTopics ? "scheduled for deletion" : "being retained"}.
                </p>

                <div className="space-y-2 rounded-md border p-2">
                  <div className="font-medium">Cleanup options</div>
                  <label className="flex items-start gap-2">
                    <Checkbox
                      className="mt-0.5"
                      checked={deleteTopics}
                      onCheckedChange={(value) => setDeleteTopics(value === true)}
                    />
                    <span>
                      <span className="font-medium">Delete generated topics and DLQ</span>
                      <span className="block text-xs text-muted-foreground">
                        {deleteTopics
                          ? "Removes the flow-owned Kafka topics after teardown."
                          : "Keeps the flow-owned Kafka topics for manual cleanup or later reuse."}
                      </span>
                    </span>
                  </label>
                  <label className="flex items-start gap-2">
                    <Checkbox
                      className="mt-0.5"
                      checked={clearCache}
                      disabled={!deleteTarget?.blocks.some(hasDedup)}
                      onCheckedChange={(value) => setClearCache(value === true)}
                    />
                    <span>
                      <span className="font-medium">Clear all dedup caches for this flow</span>
                      <span className="block text-xs text-muted-foreground">
                        {deleteTarget?.blocks.some(hasDedup)
                          ? "Invalidates every dedup cache namespace. Redis is cluster-internal, so this uses the platform's epoch reset rather than a direct key flush."
                          : "No dedup transforms are configured for this flow."}
                      </span>
                    </span>
                  </label>
                </div>

                {/* Undeploy is a required first step, not a choice: a flow with
                    a live NiFi process group cannot simply be dropped. */}
                {deleteTarget?.deployedAt && (
                  <div className="rounded-md border border-warning/30 bg-warning-muted p-2">
                    <label className="flex items-start gap-2">
                      <Checkbox checked disabled className="mt-0.5" />
                      <span>
                        <span className="font-medium">Undeploy from NiFi first</span>
                        <span className="text-muted-foreground"> — required. Tears down the process group before the flow is removed.</span>
                      </span>
                    </label>
                  </div>
                )}

                {cascadeTargets.length > 0 && (
                  <div className="rounded-md border p-2">
                    <div className="mb-1 font-medium">Also delete:</div>
                    <div className="space-y-1.5">
                      {cascadeTargets.map((target) => {
                        const key = `${target.kind}:${target.id}`;
                        const shared = target.sharedWith.length > 0;
                        return (
                          <label
                            key={key}
                            className={cn("flex items-start gap-2", shared && "opacity-60")}
                            title={shared ? `Also used by ${target.sharedWith.join(", ")}` : undefined}
                          >
                            <Checkbox
                              className="mt-0.5"
                              checked={cascadeChecked.has(key)}
                              disabled={shared}
                              onCheckedChange={(value) =>
                                setCascadeChecked((prev) => {
                                  const next = new Set(prev);
                                  if (value) next.add(key);
                                  else next.delete(key);
                                  return next;
                                })
                              }
                            />
                            <span>
                              <code className="text-xs">{target.name}</code>
                              <span className="text-muted-foreground"> · {CASCADE_LABEL[target.kind]}</span>
                              {shared && (
                                <span className="block text-xs text-muted-foreground">
                                  Kept — also used by {target.sharedWith.join(", ")}
                                </span>
                              )}
                              {!shared && target.needsRetire && (
                                <span className="block text-xs text-muted-foreground">
                                  Still active — will be retired first, then deleted.
                                </span>
                              )}
                            </span>
                          </label>
                        );
                      })}
                    </div>
                    <p className="mt-2 text-xs text-muted-foreground">
                      Nothing here is deleted unless you tick it.
                    </p>
                  </div>
                )}
              </div>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={cascadeDeleteMut.isPending}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              disabled={cascadeDeleteMut.isPending}
              onClick={(event) => {
                // Keep the dialog open while the work runs — it can take a
                // while when an undeploy is involved.
                event.preventDefault();
                if (!deleteTarget) return;
                cascadeDeleteMut.mutate({
                  flow: deleteTarget,
                  extras: cascadeTargets.filter(
                    (t) => t.sharedWith.length === 0 && cascadeChecked.has(`${t.kind}:${t.id}`),
                  ),
                  deleteOptions: { deleteTopics, clearCache },
                });
              }}
            >
              {cascadeDeleteMut.isPending && <RefreshCw className="mr-2 h-3.5 w-3.5 animate-spin" />}
              Delete flow{cascadeChecked.size > 0 ? ` + ${cascadeChecked.size}` : ""}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* ── Bulk delete confirmation ── */}
      <AlertDialog open={bulkDeleteOpen} onOpenChange={setBulkDeleteOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              Delete {bulkDeleteTargets.length} flow{bulkDeleteTargets.length === 1 ? "" : "s"}?
            </AlertDialogTitle>
            <AlertDialogDescription asChild>
              <div className="space-y-2 text-sm">
                <ul className="list-disc space-y-0.5 pl-5">
                  {bulkDeleteTargets.map((f) => (
                    <li key={f.id}>
                      <span className="font-medium">{f.name}</span>
                      {f.topics.filter((t) => t.kind === "materialized").length > 0 && (
                        <span className="text-muted-foreground">
                          {" "}
                          — empties {f.topics.filter((t) => t.kind === "materialized").map((t) => t.name).join(", ")}
                        </span>
                      )}
                    </li>
                  ))}
                </ul>
                <p className="text-muted-foreground">
                  Deployed flows are safely undeployed before deletion. Operations run one at a time in the queue.
                </p>
              </div>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              onClick={() => {
                setBulkDeleteOpen(false);
                void runBulk("delete");
              }}
            >
              Delete flows
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <SaveConnectorDialog
        flow={connectorFlow}
        services={services}
        connectors={connectors}
        onClose={() => setConnectorFlow(null)}
      />
      <ImportConnectorDialog open={importOpen} services={services} onClose={() => setImportOpen(false)} />
    </AppLayout>
  );
};

export default Flows;
