import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import { AppLayout } from "@/components/AppLayout";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
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
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import {
  applyKafkaConnectSync,
  deleteKafkaConnectSync,
  kafkaConnectSyncAction,
  listKafkaConnectSyncs,
  listFlows,
  refreshKafkaConnectSyncStatuses,
  refreshKafkaConnectSyncStatus,
  reinstateKafkaConnectSync,
  retireKafkaConnectSync,
  saveKafkaConnectSync,
  unlinkKafkaConnectSync,
  validateKafkaConnectSync,
  type KafkaConnectSync,
  type KafkaConnectSyncInput,
} from "@/prototype/api";
import type { Flow } from "@/prototype/types";
import { kafkaConnectLinkIssueForRows } from "@/prototype/kafkaConnectLink";
import { kafkaConnectSyncDeleteImpact, runtimeControlsAvailable, syncConfigurationLabel, syncPrimaryAction } from "@/prototype/kafkaConnectSyncUi";
import { ArchiveRestore, ArchiveX, ArrowLeftRight, CheckCircle2, Link2, Loader2, Pause, Pencil, Play, Plus, RefreshCw, RotateCw, Square, Trash2, Unlink, XCircle } from "lucide-react";
import { toast } from "sonner";

type ConfigRow = { key: string; value: string };
type FormState = KafkaConnectSyncInput & { configRows: ConfigRow[] };

const emptyForm = (flowId?: string | null, blockId?: string | null): FormState => ({
  name: "",
  description: "",
  direction: "sink",
  connectorClass: "",
  connectorName: "",
  config: {},
  linkedFlowId: flowId ?? null,
  linkedBlockId: blockId ?? null,
  configRows: [{ key: "connector.class", value: "" }, { key: "topics", value: "" }],
});

const formFromSync = (sync: KafkaConnectSync): FormState => ({
  id: sync.id,
  name: sync.name,
  description: sync.description,
  direction: sync.direction,
  connectorClass: sync.connectorClass,
  connectorName: sync.connectorName,
  config: sync.config,
  linkedFlowId: sync.linkedFlowId,
  linkedBlockId: sync.linkedBlockId,
  configRows: Object.entries(sync.config).map(([key, value]) => ({ key, value })),
});

const configFromRows = (rows: ConfigRow[], connectorClass: string): Record<string, string> => {
  const config: Record<string, string> = {};
  for (const row of rows) if (row.key.trim()) config[row.key.trim()] = row.value;
  if (connectorClass.trim()) config["connector.class"] = connectorClass.trim();
  return config;
};

type RuntimeStatus = {
  connector?: { state?: string; trace?: string };
  tasks?: Array<{ id?: number; state?: string; worker_id?: string; trace?: string }>;
};

function runtimeStatus(sync: KafkaConnectSync): RuntimeStatus | null {
  return (sync.lastStatus as RuntimeStatus | null) ?? null;
}

function connectorState(sync: KafkaConnectSync): string {
  return String(runtimeStatus(sync)?.connector?.state ?? "").toUpperCase();
}

function SyncStatus({ sync }: { sync: KafkaConnectSync }) {
  const state = connectorState(sync);
  const configurationLabel = syncConfigurationLabel(sync);
  const configurationVariant = sync.configurationState === "changes_pending" || sync.configurationState === "needs_review"
    ? "warning"
    : sync.configurationState === "synced"
      ? "success"
      : "muted";
  const runtimeBadge = state === "RUNNING"
    ? <Badge variant="success">Running</Badge>
    : state === "PAUSED"
      ? <Badge variant="warning">Paused</Badge>
      : state === "STOPPED"
        ? <Badge variant="outline">Stopped</Badge>
        : state === "FAILED"
          ? <Badge variant="destructive">Failed</Badge>
          : state === "UNASSIGNED"
            ? <Badge variant="warning">Unassigned</Badge>
            : null;
  if (sync.retired) return <Badge variant="muted">Retired</Badge>;
  return (
    <div className="flex shrink-0 flex-wrap justify-end gap-1.5">
      {runtimeBadge}
      <Badge variant={configurationVariant}>{configurationLabel}</Badge>
    </div>
  );
}

function RuntimeDetails({ sync }: { sync: KafkaConnectSync }) {
  if (!runtimeControlsAvailable(sync)) return null;
  const runtime = runtimeStatus(sync);
  const state = connectorState(sync) || "NOT_CHECKED";
  const tasks = Array.isArray(runtime?.tasks) ? runtime.tasks : [];
  return (
    <div className="rounded-md border bg-muted/20 p-2.5 text-xs">
      <div className="flex items-center justify-between gap-2">
        <span className="font-medium">Runtime status</span>
        <span className="font-mono text-muted-foreground">{state === "NOT_CHECKED" ? "Not checked" : state}</span>
      </div>
      <div className="mt-1 text-muted-foreground">
        {tasks.length === 0 ? "No task status returned" : `${tasks.length} task${tasks.length === 1 ? "" : "s"}`}
      </div>
      {tasks.length > 0 && (
        <div className="mt-2 space-y-1">
          {tasks.slice(0, 4).map((task, index) => (
            <div className="flex items-center justify-between gap-2" key={`${task.id ?? index}`}>
              <span>Task {task.id ?? index}</span>
              <span className={task.state === "RUNNING" ? "text-success" : task.state === "FAILED" ? "text-destructive" : "text-muted-foreground"}>
                {task.state ?? "UNKNOWN"}
              </span>
            </div>
          ))}
          {tasks.length > 4 && <div className="text-muted-foreground">+{tasks.length - 4} more task(s)</div>}
        </div>
      )}
      {(runtime?.connector?.trace || tasks.some((task) => task.trace)) && (
        <pre className="mt-2 max-h-24 overflow-auto whitespace-pre-wrap break-words rounded bg-destructive-muted/40 p-2 font-mono text-[11px] text-destructive">
          {runtime?.connector?.trace || tasks.find((task) => task.trace)?.trace}
        </pre>
      )}
    </div>
  );
}

function linkedLabel(sync: KafkaConnectSync, flows: Flow[]): string {
  const flow = flows.find((item) => item.id === sync.linkedFlowId);
  const block = flow?.blocks.find((item) => item.id === sync.linkedBlockId);
  return flow && block ? `${flow.name} · ${block.name}` : "Not linked to a flow";
}

function LinkPicker({ form, setForm, flows }: { form: FormState; setForm: React.Dispatch<React.SetStateAction<FormState>>; flows: Flow[] }) {
  const value = form.linkedFlowId && form.linkedBlockId ? `${form.linkedFlowId}:${form.linkedBlockId}` : "none";
  const optionIssues = new Map(
    flows.flatMap((flow) => flow.blocks
      .filter((block) => block.adapter === "kc" || block.adapter === "kafka_kc")
      .map((block) => [
        `${flow.id}:${block.id}`,
        kafkaConnectLinkIssueForRows(flow, block, form.direction ?? "sink", form.connectorClass, form.configRows),
      ] as const)),
  );
  const selectedIssue = optionIssues.get(value) ?? null;
  const options = flows.flatMap((flow) =>
    flow.blocks
      .filter((block) => block.adapter === "kc" || block.adapter === "kafka_kc")
      .map((block) => ({ value: `${flow.id}:${block.id}`, label: `${flow.name} · ${block.name}` })),
  );
  return (
    <div className="space-y-1.5">
      <Label>Link to a flow Kafka Connect block</Label>
      <Select
        value={value}
        onValueChange={(next) => {
          if (next === "none") setForm((prev) => ({ ...prev, linkedFlowId: null, linkedBlockId: null }));
          else {
            const [linkedFlowId, linkedBlockId] = next.split(":");
            setForm((prev) => ({ ...prev, linkedFlowId, linkedBlockId }));
          }
        }}
      >
        <SelectTrigger><SelectValue placeholder="Choose a flow block" /></SelectTrigger>
        <SelectContent>
          <SelectItem value="none">Not linked</SelectItem>
          {options.map((option) => (
            <SelectItem key={option.value} value={option.value} disabled={Boolean(optionIssues.get(option.value) && option.value !== value)}>
              {option.label}{optionIssues.get(option.value) ? ` - ${optionIssues.get(option.value)}` : ""}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
      {selectedIssue ? (
        <p className="text-xs text-destructive">Cannot link this target: {selectedIssue}</p>
      ) : (
        <p className="text-xs text-muted-foreground">The sync is saved centrally and the selected block keeps a reference to it.</p>
      )}
    </div>
  );
}

function SyncEditor({ open, onOpenChange, editing, flows, initial }: { open: boolean; onOpenChange: (open: boolean) => void; editing: KafkaConnectSync | null; flows: Flow[]; initial?: { flowId?: string | null; blockId?: string | null } }) {
  const qc = useQueryClient();
  const [form, setForm] = useState<FormState>(() => editing ? formFromSync(editing) : emptyForm(initial?.flowId, initial?.blockId));
  useEffect(() => setForm(editing ? formFromSync(editing) : emptyForm(initial?.flowId, initial?.blockId)), [editing, initial?.blockId, initial?.flowId, open]);
  const selectedFlow = form.linkedFlowId ? flows.find((flow) => flow.id === form.linkedFlowId) : undefined;
  const selectedBlock = selectedFlow && form.linkedBlockId
    ? selectedFlow.blocks.find((block) => block.id === form.linkedBlockId)
    : undefined;
  const linkIssue = selectedFlow && selectedBlock
    ? kafkaConnectLinkIssueForRows(selectedFlow, selectedBlock, form.direction ?? "sink", form.connectorClass, form.configRows)
    : null;
  const nextConfig = configFromRows(form.configRows, form.connectorClass);
  const configKey = (config: Record<string, string>) => JSON.stringify(Object.entries(config).sort(([left], [right]) => left.localeCompare(right)));
  const formDirty = !editing
    || form.name !== editing.name
    || form.description !== editing.description
    || form.direction !== editing.direction
    || form.connectorClass !== editing.connectorClass
    || (form.connectorName ?? "") !== editing.connectorName
    || form.linkedFlowId !== editing.linkedFlowId
    || form.linkedBlockId !== editing.linkedBlockId
    || configKey(nextConfig) !== configKey(editing.config);
  const save = useMutation({
    mutationFn: () => saveKafkaConnectSync({ ...form, config: nextConfig }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["kafkaConnectSyncs"] });
      onOpenChange(false);
      toast.success(editing ? "Kafka Connect sync updated" : "Kafka Connect sync created");
    },
    onError: (error: Error) => toast.error("Could not save sync", { description: error.message }),
  });
  const setRow = (index: number, patch: Partial<ConfigRow>) => setForm((prev) => ({ ...prev, configRows: prev.configRows.map((row, rowIndex) => rowIndex === index ? { ...row, ...patch } : row) }));
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[88vh] max-w-2xl overflow-y-auto">
        <DialogHeader>
          <DialogTitle>{editing ? "Configure Kafka Connect sync" : "Create Kafka Connect sync"}</DialogTitle>
          <DialogDescription>Save the connector definition here. Creating or applying it to Kafka Connect is a separate action on the sync card.</DialogDescription>
        </DialogHeader>
        <div className="grid gap-4">
          <div className="grid gap-3 sm:grid-cols-2">
            <div className="space-y-1.5"><Label>Name</Label><Input value={form.name} onChange={(event) => setForm((prev) => ({ ...prev, name: event.target.value }))} placeholder="orders-to-warehouse" /></div>
            <div className="space-y-1.5"><Label>Direction</Label><Select value={form.direction} onValueChange={(direction: "sink" | "source") => setForm((prev) => ({ ...prev, direction }))}><SelectTrigger><SelectValue /></SelectTrigger><SelectContent><SelectItem value="sink">Sink</SelectItem><SelectItem value="source">Source</SelectItem></SelectContent></Select></div>
          </div>
          <div className="space-y-1.5"><Label>Description</Label><Textarea value={form.description} onChange={(event) => setForm((prev) => ({ ...prev, description: event.target.value }))} placeholder="What this sync moves and where it goes" /></div>
          <div className="grid gap-3 sm:grid-cols-2">
            <div className="space-y-1.5"><Label>Connector class</Label><Input value={form.connectorClass} onChange={(event) => setForm((prev) => ({ ...prev, connectorClass: event.target.value }))} placeholder="org.example.MySinkConnector" /></div>
            <div className="space-y-1.5"><Label>Connector name (optional)</Label><Input value={form.connectorName ?? ""} onChange={(event) => setForm((prev) => ({ ...prev, connectorName: event.target.value }))} placeholder="Generated from sync name" /></div>
          </div>
          <LinkPicker form={form} setForm={setForm} flows={flows} />
          <div className="space-y-2"><div className="flex items-center justify-between"><Label>Connector properties</Label><Button type="button" size="sm" variant="outline" onClick={() => setForm((prev) => ({ ...prev, configRows: [...prev.configRows, { key: "", value: "" }] }))}><Plus className="mr-1.5 h-3.5 w-3.5" />Add property</Button></div>
            <div className="space-y-2 rounded-md border p-3">{form.configRows.map((row, index) => <div className="flex gap-2" key={`${index}-${row.key}`}><Input className="font-mono text-xs" value={row.key} onChange={(event) => setRow(index, { key: event.target.value })} placeholder="property.key" /><Input className="font-mono text-xs" type={/pass|secret|token|key/i.test(row.key) ? "password" : "text"} value={row.value} onChange={(event) => setRow(index, { value: event.target.value })} placeholder="value" /><Button type="button" size="icon" variant="ghost" onClick={() => setForm((prev) => ({ ...prev, configRows: prev.configRows.filter((_, rowIndex) => rowIndex !== index) }))}><XCircle className="h-4 w-4" /></Button></div>)}</div>
          </div>
        </div>
        <DialogFooter><Button variant="outline" onClick={() => onOpenChange(false)}>Cancel</Button><Button disabled={save.isPending || (Boolean(editing) && !formDirty) || !form.name.trim() || !form.connectorClass.trim() || Boolean(linkIssue)} onClick={() => save.mutate()}>{save.isPending && <Loader2 className="mr-1.5 h-4 w-4 animate-spin" />}{editing ? "Save changes" : "Save draft"}</Button></DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

export default function KafkaConnect() {
  const qc = useQueryClient();
  const [params] = useSearchParams();
  const [editorOpen, setEditorOpen] = useState(false);
  const [editing, setEditing] = useState<KafkaConnectSync | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<KafkaConnectSync | null>(null);
  const [initialLink, setInitialLink] = useState<{ flowId?: string | null; blockId?: string | null }>({});
  const syncsQuery = useQuery({ queryKey: ["kafkaConnectSyncs"], queryFn: listKafkaConnectSyncs, refetchInterval: 15000 });
  const flowsQuery = useQuery({ queryKey: ["flows"], queryFn: listFlows });
  const syncs = useMemo(() => syncsQuery.data ?? [], [syncsQuery.data]);
  const flows = useMemo(() => flowsQuery.data ?? [], [flowsQuery.data]);
  const activeSyncIds = useMemo(
    () => syncs.filter((sync) => sync.enabled && sync.remotePresent && !sync.retired).map((sync) => sync.id),
    [syncs],
  );
  const statusesQuery = useQuery({
    queryKey: ["kafkaConnectSyncStatuses", activeSyncIds],
    queryFn: refreshKafkaConnectSyncStatuses,
    enabled: activeSyncIds.length > 0,
    refetchInterval: 10000,
    retry: false,
  });
  const displaySyncs = useMemo(() => {
    const liveById = new Map((statusesQuery.data ?? []).map((sync) => [sync.id, sync]));
    return syncs.map((sync) => {
      const live = liveById.get(sync.id);
      return live
        ? { ...sync, lastStatus: live.lastStatus, lastError: live.lastError, updatedAt: live.updatedAt }
        : sync;
    });
  }, [syncs, statusesQuery.data]);
  const openCreate = () => {
    setEditing(null);
    setInitialLink({ flowId: params.get("flow"), blockId: params.get("block") });
    setEditorOpen(true);
  };
  const invalidateSyncs = () => {
    void qc.invalidateQueries({ queryKey: ["kafkaConnectSyncs"] });
    void qc.invalidateQueries({ queryKey: ["kafkaConnectSyncStatuses"] });
  };
  const action = useMutation({
    mutationFn: ({ id, verb }: { id: string; verb: "apply" | "pause" | "resume" | "restart" | "start" | "stop" | "status" | "validate" }) =>
      verb === "apply"
        ? applyKafkaConnectSync(id)
        : verb === "status"
          ? refreshKafkaConnectSyncStatus(id)
          : verb === "validate"
            ? validateKafkaConnectSync(id)
            : kafkaConnectSyncAction(id, verb),
    onSuccess: (_result, variables) => {
      invalidateSyncs();
      toast.success(variables.verb === "validate" ? "Connector configuration is valid" : `Sync ${variables.verb} complete`);
    },
    onError: (error: Error) => toast.error("Kafka Connect action failed", { description: error.message }),
  });
  const retire = useMutation({
    mutationFn: retireKafkaConnectSync,
    onSuccess: () => { invalidateSyncs(); toast.success("Sync retired"); },
    onError: (error: Error) => toast.error("Could not retire sync", { description: error.message }),
  });
  const reinstate = useMutation({
    mutationFn: reinstateKafkaConnectSync,
    onSuccess: () => { invalidateSyncs(); toast.success("Sync reinstated"); },
    onError: (error: Error) => toast.error("Could not reinstate sync", { description: error.message }),
  });
  const unlink = useMutation({
    mutationFn: unlinkKafkaConnectSync,
    onSuccess: () => { invalidateSyncs(); void qc.invalidateQueries({ queryKey: ["flows"] }); toast.success("Sync unlinked"); },
    onError: (error: Error) => toast.error("Could not unlink sync", { description: error.message }),
  });
  const remove = useMutation({
    mutationFn: deleteKafkaConnectSync,
    onSuccess: () => { setDeleteTarget(null); invalidateSyncs(); void qc.invalidateQueries({ queryKey: ["flows"] }); toast.success("Sync permanently deleted"); },
    onError: (error: Error) => toast.error("Could not delete sync", { description: error.message }),
  });
  const deleteImpact = useMemo(
    () => deleteTarget ? kafkaConnectSyncDeleteImpact(deleteTarget, flows) : { deployed: [], undeployed: [] },
    [deleteTarget, flows],
  );
  const empty = useMemo(() => !syncsQuery.isLoading && syncs.length === 0, [syncs.length, syncsQuery.isLoading]);
  return (
    <AppLayout title="Kafka Connect" description="Reusable connector syncs linked to flow destinations" actions={<div className="flex gap-2"><Button variant="outline" onClick={() => syncsQuery.refetch()} disabled={syncsQuery.isFetching}><RefreshCw className={syncsQuery.isFetching ? "mr-1.5 h-4 w-4 animate-spin" : "mr-1.5 h-4 w-4"} />Refresh</Button><Button onClick={openCreate}><Plus className="mr-1.5 h-4 w-4" />New sync</Button></div>}>
      <Alert className="mb-5 border-info/40 bg-info-muted/40"><ArrowLeftRight className="h-4 w-4 text-info" /><AlertTitle>Connector lifecycle and status</AlertTitle><AlertDescription className="text-xs">Draft syncs can be validated and created. Existing connectors show Synced until a saved edit creates pending changes; only then does Apply changes appear. Synced connectors expose live status and runtime controls. Retirement is reversible; permanent deletion is the final step.</AlertDescription></Alert>
      {statusesQuery.isError && <p className="mb-4 text-xs text-muted-foreground">Live status is temporarily unavailable; showing the last saved connector status.</p>}
      {syncsQuery.isLoading && <div className="flex h-40 items-center justify-center"><Loader2 className="h-7 w-7 animate-spin text-muted-foreground" /></div>}
      {empty && <Card><CardContent className="flex flex-col items-center gap-3 py-14 text-center"><ArrowLeftRight className="h-10 w-10 text-muted-foreground" /><div><h3 className="font-medium">No Kafka Connect syncs yet</h3><p className="text-sm text-muted-foreground">Create one to reuse a connector configuration from your flow designs.</p></div><Button onClick={openCreate}><Plus className="mr-1.5 h-4 w-4" />Create sync</Button></CardContent></Card>}
      <div className="grid gap-4 md:grid-cols-2">
        {displaySyncs.map((sync) => {
          const state = connectorState(sync);
          const primaryAction = syncPrimaryAction(sync);
          const runtimeAvailable = runtimeControlsAvailable(sync);
          const deleteImpactForSync = kafkaConnectSyncDeleteImpact(sync, flows);
          const hasDeployedDependents = deleteImpactForSync.deployed.length > 0;
          const lifecyclePending = action.isPending || retire.isPending || reinstate.isPending || remove.isPending;
          const hasKnownState = ["RUNNING", "PAUSED", "STOPPED", "FAILED", "UNASSIGNED"].includes(state);
          return (
            <Card key={sync.id} className={sync.retired ? "overflow-hidden opacity-75" : "overflow-hidden"}>
              <CardHeader className="pb-3"><div className="flex items-start gap-3"><div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-primary-muted text-primary"><ArrowLeftRight className="h-4 w-4" /></div><div className="min-w-0 flex-1"><CardTitle className="truncate text-base">{sync.name}</CardTitle><CardDescription className="truncate">{sync.connectorClass || "No connector class"}</CardDescription></div><SyncStatus sync={sync} /></div></CardHeader>
              <CardContent className="space-y-3">
                <p className="text-sm text-muted-foreground">{sync.description || "No description"}</p>
                <div className="rounded-md border bg-muted/30 p-2.5 text-xs"><div className="flex items-center gap-1.5 font-medium"><Link2 className="h-3.5 w-3.5" />{linkedLabel(sync, flows)}</div><div className="mt-1 text-muted-foreground">{Object.keys(sync.config).length} connector properties · {sync.direction} sync</div></div>
                {hasDeployedDependents && <p className="rounded-md border border-warning/30 bg-warning-muted/30 p-2.5 text-xs text-warning">This sync is used by a deployed flow. Undeploy that flow before retiring or deleting the sync so its live sink connector is not interrupted.</p>}
                <RuntimeDetails sync={sync} />
                {sync.lastError && <p className="text-xs text-destructive">{sync.lastError}</p>}
                <div className="flex flex-wrap gap-2">
                  <Button size="sm" variant="outline" onClick={() => { setEditing(sync); setEditorOpen(true); }} disabled={lifecyclePending}><Pencil className="mr-1.5 h-3.5 w-3.5" />Edit</Button>
                  {primaryAction && <Button size="sm" onClick={() => action.mutate({ id: sync.id, verb: "apply" })} disabled={lifecyclePending} title={primaryAction === "create" ? "Create this connector in Kafka Connect" : "Apply the saved configuration changes to Kafka Connect"}><CheckCircle2 className="mr-1.5 h-3.5 w-3.5" />{primaryAction === "create" ? "Create connector" : "Apply changes"}</Button>}
                  <Button size="sm" variant="outline" onClick={() => action.mutate({ id: sync.id, verb: "validate" })} disabled={lifecyclePending}>Validate</Button>
                  <Button size="sm" variant="ghost" onClick={() => action.mutate({ id: sync.id, verb: "status" })} disabled={lifecyclePending || !runtimeAvailable}><RefreshCw className="mr-1.5 h-3.5 w-3.5" />Status</Button>
                  {sync.linkedFlowId && <Button size="sm" variant="ghost" onClick={() => unlink.mutate(sync.id)} disabled={lifecyclePending || unlink.isPending}><Unlink className="mr-1.5 h-3.5 w-3.5" />Unlink</Button>}
                  {!sync.retired && <Button size="sm" variant="outline" onClick={() => { if (window.confirm(`Retire sync '${sync.name}'? It will no longer accept runtime changes.`)) retire.mutate(sync.id); }} disabled={lifecyclePending || hasDeployedDependents} title={hasDeployedDependents ? "Undeploy linked flows before retiring this sync" : "Retire this sync before permanently deleting it"}><ArchiveX className="mr-1.5 h-3.5 w-3.5" />Retire</Button>}
                  {sync.retired && <Button size="sm" variant="outline" onClick={() => reinstate.mutate(sync.id)} disabled={lifecyclePending}><ArchiveRestore className="mr-1.5 h-3.5 w-3.5" />Reinstate</Button>}
                  <span title={sync.retired ? "Review permanent deletion impact" : "Retire the sync before deleting it"} className="inline-flex"><Button size="sm" variant="ghost" className="text-destructive hover:text-destructive" onClick={() => setDeleteTarget(sync)} disabled={lifecyclePending || !sync.retired}><Trash2 className="mr-1.5 h-3.5 w-3.5" />Delete</Button></span>
                </div>
                {sync.configurationState === "changes_pending" && !sync.retired && <p className="rounded-md border border-warning/30 bg-warning-muted/30 p-2.5 text-xs text-warning">Saved changes are waiting to be applied. Runtime controls stay unchanged until you apply them.</p>}
                {runtimeAvailable && <div className="flex flex-wrap gap-2 border-t pt-3">
                  {(!hasKnownState || state === "RUNNING") && <Button size="sm" variant="outline" onClick={() => action.mutate({ id: sync.id, verb: "pause" })} disabled={lifecyclePending}><Pause className="mr-1.5 h-3.5 w-3.5" />Pause</Button>}
                  {state === "PAUSED" && <Button size="sm" variant="outline" onClick={() => action.mutate({ id: sync.id, verb: "resume" })} disabled={lifecyclePending}><Play className="mr-1.5 h-3.5 w-3.5" />Resume</Button>}
                  {state === "STOPPED" && <Button size="sm" variant="outline" onClick={() => action.mutate({ id: sync.id, verb: "start" })} disabled={lifecyclePending}><Play className="mr-1.5 h-3.5 w-3.5" />Start</Button>}
                  {state !== "STOPPED" && <Button size="sm" variant="outline" onClick={() => action.mutate({ id: sync.id, verb: "stop" })} disabled={lifecyclePending}><Square className="mr-1.5 h-3.5 w-3.5" />Stop</Button>}
                  <Button size="sm" variant="outline" onClick={() => action.mutate({ id: sync.id, verb: "restart" })} disabled={lifecyclePending}><RotateCw className="mr-1.5 h-3.5 w-3.5" />Restart</Button>
                </div>}
              </CardContent>
            </Card>
          );
        })}
      </div>
      <AlertDialog open={Boolean(deleteTarget)} onOpenChange={(open) => !open && setDeleteTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete “{deleteTarget?.name}” permanently?</AlertDialogTitle>
            <AlertDialogDescription>
              This permanently removes the sync definition. If a remote Kafka Connect connector exists, it is deleted first. A remote deletion failure leaves the retired sync available for retry.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <div className="rounded-md border border-destructive/30 bg-destructive/5 p-3 text-sm">
            {deleteImpact.deployed.length > 0 && (
              <div className="space-y-2">
                <div className="font-medium text-destructive">Deletion is blocked while deployed flows use this sync.</div>
                <p className="text-xs text-muted-foreground">Undeploy these flows first; deleting their remote sink connector would interrupt the live deployment.</p>
                <ul className="list-disc pl-5 text-xs text-muted-foreground">
                  {deleteImpact.deployed.map((flow) => <li key={flow.id}>{flow.name}</li>)}
                </ul>
              </div>
            )}
            {deleteImpact.undeployed.length > 0 && (
              <div className={deleteImpact.deployed.length > 0 ? "mt-3 border-t pt-3" : ""}>
                <div className="font-medium">Undeployed flows will keep the reference and report a missing sync:</div>
                <ul className="mt-1 list-disc pl-5 text-xs text-muted-foreground">
                  {deleteImpact.undeployed.map((flow) => <li key={flow.id}>{flow.name}</li>)}
                </ul>
              </div>
            )}
            {deleteImpact.deployed.length === 0 && deleteImpact.undeployed.length === 0 && <span className="text-muted-foreground">No flows use this sync.</span>}
          </div>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              disabled={deleteImpact.deployed.length > 0 || remove.isPending}
              onClick={() => deleteTarget && remove.mutate(deleteTarget.id)}
            >
              {remove.isPending && <Loader2 className="h-4 w-4 animate-spin" />} Delete permanently
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
      <SyncEditor open={editorOpen} onOpenChange={setEditorOpen} editing={editing} flows={flows} initial={initialLink} />
    </AppLayout>
  );
}
