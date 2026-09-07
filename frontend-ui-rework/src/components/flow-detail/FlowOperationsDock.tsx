import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { Activity, Download, Eraser, Fingerprint, RefreshCw } from "lucide-react";

import { AdapterChip } from "@/components/AdapterChip";
import { RuntimeTab } from "@/pages/Flows";
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
import { Button } from "@/components/ui/button";
import { EmptyState } from "@/components/ui/empty-state";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { clearDedupCache, clearFlowTopic, getDlq, getMetrics } from "@/prototype/api";
import { dlqName, deriveTopicName } from "@/prototype/naming";
import { timeAgo } from "@/lib/api";
import type { AppService, ApprovedSchema, Flow, FlowBlock, FlowMetrics, PlatformConnection, DlqRecord } from "@/prototype/types";

export interface FlowOperationsDockProps {
  flow: Flow;
  services: AppService[];
  schemas: ApprovedSchema[];
  connections: PlatformConnection[];
  onEdit: () => void;
  onSelectBlock?: (blockId: string) => void;
}

function downloadJson(filename: string, value: unknown) {
  const blob = new Blob([JSON.stringify(value, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  anchor.click();
  URL.revokeObjectURL(url);
}

function outputTopic(flow: Flow, block: FlowBlock): string | null {
  const owned = flow.topics.find((topic) => topic.writerBlockId === block.id);
  if (owned) return owned.name;
  if (block.adapter === "kafka" || block.adapter === "kafka_kc") return deriveTopicName(flow, block).value || null;
  return null;
}

function OverviewPanel({ flow, schemas, onSelectBlock }: Pick<FlowOperationsDockProps, "flow" | "schemas" | "onSelectBlock">) {
  const queryClient = useQueryClient();
  const [clearTarget, setClearTarget] = useState<FlowBlock | null>(null);
  const clearMutation = useMutation({
    mutationFn: (block: FlowBlock) => clearDedupCache(flow.id, block.id),
    onSuccess: (result, block) => {
      queryClient.invalidateQueries({ queryKey: ["audit"] });
      setClearTarget(null);
      toast[result.cleared ? "success" : "info"](
        result.cleared ? "Dedup cache cleared" : "Nothing to clear",
        { description: result.cleared ? `${block.name} can emit previously suppressed records again.` : `${block.name} has no live cache yet.` },
      );
    },
    onError: (error: Error) => toast.error("Could not clear the dedup cache", { description: error.message }),
  });
  const writeBlocks = flow.blocks.filter((block) => block.mode === "write" || block.adapter === "kafka_kc");
  const dedupBlocks = flow.blocks.filter((block) => block.transforms.some((rule) => rule.kind === "dedup"));

  return (
    <div className="space-y-4">
      <div className="rounded-md border p-3">
        <div className="mb-2 text-xs font-medium">Deployment</div>
        <div className="grid gap-x-4 gap-y-2.5 sm:grid-cols-2">
          <div><div className="text-xs text-muted-foreground">State</div><div className="mt-0.5"><StatusBadge status={flow.state} /></div></div>
          <div><div className="text-xs text-muted-foreground">Enabled</div><div className="mt-0.5 text-sm">{flow.enabled ? "Enabled" : "Disabled"}</div></div>
          <div><div className="text-xs text-muted-foreground">Deployed</div><div className="mt-0.5 text-sm">{flow.deployedAt ? timeAgo(flow.deployedAt) : "never"}</div></div>
          <div><div className="text-xs text-muted-foreground">Last run</div><div className="mt-0.5 text-sm">{timeAgo(flow.lastRunAt)}</div></div>
          <div><div className="text-xs text-muted-foreground">Schedule</div><div className="mt-0.5 font-mono text-sm">{flow.cron ? `cron ${flow.cron} (UTC)` : "continuous — no trigger"}</div></div>
          <div><div className="text-xs text-muted-foreground">DLQ topic</div><div className="mt-0.5 break-all font-mono text-sm">{dlqName(flow.name)}</div></div>
        </div>
      </div>

      <div className="rounded-md border p-3">
        <div className="mb-2 text-xs font-medium">Entity outputs</div>
        {writeBlocks.length === 0 ? <div className="text-xs text-muted-foreground">No write or sink blocks yet.</div> : (
          <div className="space-y-1.5">
            {writeBlocks.map((block) => (
              <button
                type="button"
                key={block.id}
                className="flex w-full flex-wrap items-center gap-2 rounded-md border p-2 text-left transition-colors hover:bg-accent/50"
                onClick={() => onSelectBlock?.(block.id)}
              >
                <AdapterChip adapter={block.adapter} mode={block.mode} />
                <span className="text-sm font-medium">{block.name}</span>
                <span className="font-mono text-xs text-muted-foreground">{outputTopic(flow, block) ?? "no destination topic"}</span>
                {block.adapter === "kafka_kc" && <StatusBadge status={schemas.some((schema) => schema.flowId === flow.id && schema.blockId === block.id) ? "Schema approved" : "Schema missing"} />}
              </button>
            ))}
          </div>
        )}
      </div>

      <div className="rounded-md border p-3">
        <div className="mb-2 text-xs font-medium">Topics</div>
        {flow.topics.length === 0 ? <div className="text-xs text-muted-foreground">No topics — the flow writes to no Kafka-family destination yet.</div> : (
          <div className="space-y-1.5">
            {flow.topics.map((topic) => (
              <div key={topic.id} className="flex flex-wrap items-center gap-2">
                <code className="break-all text-xs">{topic.name}</code>
                {topic.sealed && <StatusBadge status="Sealed" />}
                {topic.kind === "adopted" && <StatusBadge status="Adopted" />}
              </div>
            ))}
          </div>
        )}
      </div>

      {dedupBlocks.length > 0 && (
        <div className="rounded-md border p-3">
          <div className="mb-2 flex items-center gap-2 text-xs font-medium"><Fingerprint className="h-3.5 w-3.5" /> Deduplication</div>
          <div className="space-y-1.5">
            {dedupBlocks.map((block) => (
              <div key={block.id} className="flex flex-wrap items-center justify-between gap-2 rounded-md border p-2">
                <div><div className="text-sm font-medium">{block.name}</div><div className="text-xs text-muted-foreground">{block.transforms.length} transform rule(s)</div></div>
                <Button size="sm" variant="outline" className="text-destructive hover:text-destructive" disabled={clearMutation.isPending} onClick={() => setClearTarget(block)}>
                  <Eraser className="mr-1.5 h-3.5 w-3.5" /> Clear cache
                </Button>
              </div>
            ))}
          </div>
        </div>
      )}

      <AlertDialog open={Boolean(clearTarget)} onOpenChange={(open) => !open && setClearTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader><AlertDialogTitle>Clear the dedup cache for “{clearTarget?.name}”?</AlertDialogTitle><AlertDialogDescription>Previously suppressed records become eligible again before the window expires.</AlertDialogDescription></AlertDialogHeader>
          <AlertDialogFooter><AlertDialogCancel>Cancel</AlertDialogCancel><AlertDialogAction disabled={clearMutation.isPending} onClick={() => clearTarget && clearMutation.mutate(clearTarget)}>Clear cache</AlertDialogAction></AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

function MetricsPanel({ flow }: { flow: Flow }) {
  const metricsQuery = useQuery({ queryKey: ["flow-metrics", flow.id], queryFn: () => getMetrics(flow.id) });
  const metrics = metricsQuery.data as FlowMetrics | null | undefined;
  if (metricsQuery.isLoading) return <div className="p-6 text-center text-sm text-muted-foreground">Loading metrics…</div>;
  if (!metrics) return <div className="rounded-md border p-8 text-center"><Activity className="mx-auto h-6 w-6 text-muted-foreground" /><div className="mt-2 text-sm font-medium">Metrics unavailable</div><div className="mt-1 text-xs text-muted-foreground">This flow has never reported runtime metrics.</div></div>;
  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        {[["Records (24h)", metrics.records24h], ["Errors (24h)", metrics.errors24h], ["Queued", metrics.queued]].map(([label, value]) => <div key={label} className="rounded-md border p-3"><div className="text-xs text-muted-foreground">{label}</div><div className={`mt-1 text-lg font-semibold ${label === "Errors (24h)" && Number(value) > 0 ? "text-destructive" : ""}`}>{Number(value).toLocaleString()}</div></div>)}
        <div className="rounded-md border p-3"><div className="text-xs text-muted-foreground">Last run</div><div className="mt-2">{metrics.lastRunOutcome ? <StatusBadge status={metrics.lastRunOutcome} /> : <span className="text-sm text-muted-foreground">—</span>}</div></div>
      </div>
      <div className="overflow-x-auto rounded-md border"><Table><TableHeader><TableRow><TableHead>Block</TableHead><TableHead className="text-right">In</TableHead><TableHead className="text-right">Out</TableHead><TableHead className="text-right">Queued</TableHead></TableRow></TableHeader><TableBody>{metrics.perBlock.map((block) => <TableRow key={block.blockId}><TableCell className="py-2 text-xs">{block.label}</TableCell><TableCell className="py-2 text-right font-mono text-xs">{block.recordsIn.toLocaleString()}</TableCell><TableCell className="py-2 text-right font-mono text-xs">{block.recordsOut.toLocaleString()}</TableCell><TableCell className="py-2 text-right font-mono text-xs">{block.queued.toLocaleString()}</TableCell></TableRow>)}</TableBody></Table></div>
      <div className="rounded-md border p-3"><div className="mb-2 text-xs font-medium">Topic message counts</div><div className="space-y-1">{metrics.topicCounts.map((topic) => <div key={topic.topic} className="flex items-center justify-between gap-2 text-xs"><code>{topic.topic}</code><span className="font-mono text-muted-foreground">{topic.messages.toLocaleString()}</span></div>)}</div></div>
    </div>
  );
}

function DlqPanel({ flow }: { flow: Flow }) {
  const queryClient = useQueryClient();
  const [clearOpen, setClearOpen] = useState(false);
  const dlqQuery = useQuery({ queryKey: ["flow-dlq", flow.id], queryFn: () => getDlq(flow.id) });
  const clearMutation = useMutation({
    mutationFn: () => clearFlowTopic(flow.id, dlqName(flow.name)),
    onSuccess: (result) => { queryClient.invalidateQueries({ queryKey: ["flow-dlq", flow.id] }); queryClient.invalidateQueries({ queryKey: ["flow-metrics", flow.id] }); setClearOpen(false); toast.success(`Cleared ${result.before} message(s) from ${result.topic}`); },
    onError: (error: Error) => toast.error("Could not clear the DLQ", { description: error.message }),
  });
  const records = (dlqQuery.data ?? []) as DlqRecord[];
  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center justify-between gap-2"><p className="text-xs text-muted-foreground">One DLQ per flow: <code className="text-foreground">{dlqName(flow.name)}</code> · no automated replay.</p><div className="flex items-center gap-2"><Button size="sm" variant="outline" disabled={records.length === 0} onClick={() => downloadJson(`${dlqName(flow.name)}.json`, records)}><Download className="mr-1.5 h-3.5 w-3.5" /> Download</Button><Button size="sm" variant="outline" className="text-destructive hover:text-destructive" disabled={clearMutation.isPending} onClick={() => setClearOpen(true)}><Eraser className="mr-1.5 h-3.5 w-3.5" /> Clear DLQ</Button></div></div>
      {dlqQuery.isLoading ? <div className="p-6 text-center text-sm text-muted-foreground">Loading DLQ records…</div> : records.length === 0 ? <div className="rounded-md border p-8 text-center text-sm text-muted-foreground">No dead-lettered records.</div> : <div className="overflow-x-auto rounded-md border"><Table><TableHeader><TableRow><TableHead>Time</TableHead><TableHead>Block</TableHead><TableHead>Error class</TableHead><TableHead>Payload preview</TableHead></TableRow></TableHeader><TableBody>{records.map((record) => <TableRow key={record.id}><TableCell className="py-2 text-xs text-muted-foreground">{timeAgo(record.ts)}</TableCell><TableCell className="py-2 text-xs">{record.blockName}</TableCell><TableCell className="py-2"><code className="rounded bg-muted px-1.5 py-0.5 text-xs">{record.errorClass}</code></TableCell><TableCell className="py-2"><div className="max-w-[360px] truncate font-mono text-xs text-muted-foreground" title={record.payloadPreview}>{record.payloadPreview}</div></TableCell></TableRow>)}</TableBody></Table></div>}
      <AlertDialog open={clearOpen} onOpenChange={setClearOpen}><AlertDialogContent><AlertDialogHeader><AlertDialogTitle>Clear all retained DLQ messages?</AlertDialogTitle><AlertDialogDescription>This cannot be undone. The action is audited.</AlertDialogDescription></AlertDialogHeader><AlertDialogFooter><AlertDialogCancel>Cancel</AlertDialogCancel><AlertDialogAction className="bg-destructive text-destructive-foreground hover:bg-destructive/90" disabled={clearMutation.isPending} onClick={() => clearMutation.mutate()}>Clear DLQ</AlertDialogAction></AlertDialogFooter></AlertDialogContent></AlertDialog>
    </div>
  );
}

export function FlowOperationsDock({ flow, services, schemas, connections, onEdit, onSelectBlock }: FlowOperationsDockProps): JSX.Element {
  const [tab, setTab] = useState("overview");
  useEffect(() => { setTab("overview"); }, [flow.id]);

  return (
    <div className="flex h-full min-h-0 flex-col overflow-hidden rounded-xl border bg-card/60 shadow-sm">
      <div className="flex shrink-0 flex-wrap items-center justify-between gap-2 border-b bg-muted/20 px-3 py-2">
        <div><div className="text-sm font-semibold">Flow operations</div><div className="text-xs text-muted-foreground">Live operational views for this deployed flow</div></div>
        {flow.deployedAt ? <StatusBadge status={flow.state} /> : <span className="text-xs text-muted-foreground">Not deployed</span>}
      </div>
      <Tabs value={tab} onValueChange={setTab} className="flex min-h-0 flex-1 flex-col">
        <TabsList className="mx-3 mt-2 shrink-0 justify-start overflow-x-auto"><TabsTrigger value="overview">Overview</TabsTrigger><TabsTrigger value="metrics">Metrics</TabsTrigger><TabsTrigger value="dlq">DLQ</TabsTrigger><TabsTrigger value="runtime">Runtime</TabsTrigger></TabsList>
        <div className="min-h-0 flex-1 overflow-y-auto px-3 pb-3 [scrollbar-gutter:stable]">
          <TabsContent value="overview" className="mt-3"><OverviewPanel flow={flow} schemas={schemas} onSelectBlock={onSelectBlock} /></TabsContent>
          <TabsContent value="metrics" className="mt-3"><MetricsPanel flow={flow} /></TabsContent>
          <TabsContent value="dlq" className="mt-3"><DlqPanel flow={flow} /></TabsContent>
          <TabsContent value="runtime" className="mt-3"><RuntimeTab flow={flow} services={services} connections={connections} onEdit={onEdit} /></TabsContent>
        </div>
      </Tabs>
    </div>
  );
}
