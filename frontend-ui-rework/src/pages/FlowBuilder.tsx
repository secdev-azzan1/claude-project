// The adapter-based Flow Builder: form-centric configuration (outline + a
// per-block form) supported by an interactive map.
//
// Surface hierarchy on this page â€” one raised thing, and it is the form:
//   Â· block form  â†’ the single DOMINANT surface (elevation 3)
//   Â· outline     â†’ a FLAT RAIL, no Card at all
//   Â· flow map    â†’ a RECESSED canvas band (inset), a different kind of surface
//     rather than a third competing panel
//
// Structure is never edited here. Every create / re-parent / delete goes through
// src/prototype/mutations.ts, which owns the edit lock, the legality rules and
// the cycle guard â€” so the map, the outline and the form cannot drift apart, and
// a new surface cannot forget a guard.

import { useCallback, useEffect, useMemo, useState } from "react";
import { useNavigate, useParams, useSearchParams } from "react-router-dom";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { AppLayout } from "@/components/AppLayout";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Separator } from "@/components/ui/separator";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Field, FieldGroup, InfoDot, Mono } from "@/components/form/Field";
import { StatusBadge } from "@/components/StatusBadge";
import { FlowMapView } from "@/components/flow-builder/FlowMapView";
import { BlockForm } from "@/components/flow-builder/BlockForm";
import { FlowSettingsForm } from "@/components/flow-builder/FlowSettingsForm";
import { DestinationsPanel } from "@/components/flow-builder/DestinationsPanel";
import { PreflightDialog } from "@/components/flow-builder/PreflightDialog";
import { CeremonyDialog } from "@/components/flow-builder/CeremonyDialog";
import { cn } from "@/lib/utils";
import {
  consumeCeremonyDraft,
  createFlow,
  getEditLockReason,
  getBulkQueue,
  getFlow,
  getGatewayResources,
  getVerbBlockReason,
  listGatewayProxies,
  listSchemas,
  listServices,
  isBulkJobTerminal,
  saveFlow,
  startBulkJob,
  syncFlowTopics,
  type FlowVerb,
} from "@/prototype/api";
import {
  addBlock,
  setBranch,
  deleteBlockCascade,
  previewReparentRenames,
  reparentBlock,
  type MutationResult,
} from "@/prototype/mutations";
import { validateFlow } from "@/prototype/validation";
import type { AddMenuEntry } from "@/prototype/legality";
import { dlqName, tokenize } from "@/prototype/naming";
import type { BranchCondition, Flow, FlowBlock } from "@/prototype/types";
import {
  ChevronRight,
  Eye,
  EyeOff,
  Loader2,
  Maximize2,
  Minimize2,
  MoreHorizontal,
  Pause,
  Play,
  Rocket,
  Save,
  Settings2,
  ShieldCheck,
  Square,
} from "lucide-react";
import { toast } from "sonner";

export default function FlowBuilder() {
  const { flowId } = useParams();
  const isNew = !flowId;
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [searchParams, setSearchParams] = useSearchParams();

  const [draft, setDraft] = useState<Flow | null>(null);
  const [dirty, setDirty] = useState(false);
  const [selectedId, setSelectedId] = useState<string>("flow");
  const [saving, setSaving] = useState(false);
  const [verbBusy, setVerbBusy] = useState<FlowVerb | null>(null);
  const [enabledBusy, setEnabledBusy] = useState(false);
  const [preflightOpen, setPreflightOpen] = useState(false);
  const [ceremonyBlockId, setCeremonyBlockId] = useState<string | null>(null);
  const [ceremonyPrefill, setCeremonyPrefill] = useState<string | null>(null);
  /** An edited schema handed over from the Schemas page, claimed on arrival. */
  const [ceremonyDraft, setCeremonyDraft] = useState<{ rawAvro: string; label: string } | null>(null);
  const [mapOpen, setMapOpen] = useState(true);
  /** Fills the viewport with the SAME mounted canvas rather than opening a
   *  second one in a dialog â€” the map holds live pan/zoom/selection state in
   *  its ReactFlowProvider, and remounting it elsewhere would flicker and
   *  reset the camera. Expanding just re-parents its visual bounds via CSS. */
  const [mapExpanded, setMapExpanded] = useState(false);

  useEffect(() => {
    if (!mapExpanded) return;
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape") setMapExpanded(false);
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [mapExpanded]);

  const { data: serverFlow, isLoading } = useQuery({
    queryKey: ["flow", flowId],
    queryFn: () => getFlow(flowId!),
    enabled: !isNew,
  });
  const { data: services = [] } = useQuery({ queryKey: ["services"], queryFn: listServices });
  const { data: schemas = [] } = useQuery({ queryKey: ["schemas"], queryFn: listSchemas });
  // `validateFlow`'s gateway param defaults to a localStorage-backed reader
  // when omitted (a leftover of the retired mock store) â€” fetched here and
  // passed explicitly so this page never touches it.
  const { data: gatewayProxies = [] } = useQuery({ queryKey: ["gateway-proxies"], queryFn: listGatewayProxies });
  const { data: gatewayResources } = useQuery({ queryKey: ["gateway"], queryFn: getGatewayResources });
  const { data: operationQueue = [] } = useQuery({
    queryKey: ["flowOperationQueue"],
    queryFn: getBulkQueue,
    enabled: !isNew,
    refetchInterval: 1200,
  });

  useEffect(() => {
    if (serverFlow && (!draft || draft.id !== serverFlow.id)) {
      setDraft(serverFlow);
      setDirty(false);
    }
  }, [serverFlow, draft]);

  // Deep link from the Schemas browser: ?ceremony=<blockId>[&prefill=<templateId>].
  // Both params are consumed here so a refresh does not re-open the ceremony.
  // An edited schema may also be waiting in the store (the "register a new
  // version" path); it is claimed here, once, and handed to the dialog.
  useEffect(() => {
    const target = searchParams.get("ceremony");
    if (!target || !draft?.blocks.some((b) => b.id === target)) return;
    const prefill = searchParams.get("prefill");
    setSelectedId(target);
    if (getEditLockReason(draft)) {
      toast.error("The flow is deployed â€” stop it before re-running the schema ceremony.");
    } else {
      setCeremonyPrefill(prefill);
      setCeremonyBlockId(target);
      void consumeCeremonyDraft(draft.id, target).then((staged) => {
        if (staged) setCeremonyDraft({ rawAvro: staged.rawAvro, label: staged.label });
      });
    }
    const next = new URLSearchParams(searchParams);
    next.delete("ceremony");
    next.delete("prefill");
    setSearchParams(next, { replace: true });
  }, [searchParams, draft, setSearchParams]);

  useEffect(() => {
    const handler = (e: BeforeUnloadEvent) => {
      if (dirty) e.preventDefault();
    };
    window.addEventListener("beforeunload", handler);
    return () => window.removeEventListener("beforeunload", handler);
  }, [dirty]);

  const issues = useMemo(
    () =>
      draft
        ? validateFlow(draft, services, schemas, { proxies: gatewayProxies, allowlist: gatewayResources?.allowlist ?? [] })
        : [],
    [draft, services, schemas, gatewayProxies, gatewayResources],
  );
  const issuesByNode = useMemo(() => {
    const map = new Map<string, number>();
    for (const i of issues) if (i.blockId) map.set(i.blockId, (map.get(i.blockId) ?? 0) + 1);
    return map;
  }, [issues]);
  const flowIssues = issues.filter((i) => i.blockId === null);

  const queueLock = draft
    ? operationQueue
        .filter((job) => !isBulkJobTerminal(job))
        .flatMap((job) => job.items.map((item) => ({ job, item })))
        .find(({ item }) => (item.status === "pending" || item.status === "running") && item.flowId === draft.id)
    : null;
  const lockReason = queueLock
    ? `This flow is locked by queued ${queueLock.job.verb.replace("_", " ")} operation. Cancel it from the Flow operations panel or wait for it to finish.`
    : draft
      ? getEditLockReason(draft)
      : null;
  const queueLocked = !!queueLock;
  const locked = !!lockReason;

  const patchDraft = useCallback((patch: Partial<Flow>) => {
    setDraft((d) => (d ? { ...d, ...patch } : d));
    setDirty(true);
  }, []);

  const patchBlock = useCallback((blockId: string, patch: Partial<FlowBlock>) => {
    setDraft((d) => {
      if (!d) return d;
      const next = { ...d, blocks: d.blocks.map((b) => (b.id === blockId ? { ...b, ...patch } : b)) };
      syncFlowTopics(next);
      return next;
    });
    setDirty(true);
  }, []);

  const patchConfig = useCallback((blockId: string, patch: Record<string, unknown>) => {
    setDraft((d) => {
      if (!d) return d;
      return {
        ...d,
        blocks: d.blocks.map((b) => (b.id === blockId ? { ...b, config: { ...b.config, ...patch } } : b)),
      };
    });
    setDirty(true);
  }, []);

  /**
   * Apply a shared mutation. Selection is set OUTSIDE the state updater â€” the
   * old code called setSelectedId inside setDraft, which double-fires under
   * StrictMode â€” and a refusal is shown verbatim.
   */
  const applyMutation = useCallback((result: MutationResult, successMessage?: string): boolean => {
    // `ok === false` rather than `!ok`: this project compiles with strict off,
    // where truthiness alone does not narrow a boolean-discriminated union.
    if (result.ok === false) {
      toast.error(result.reason);
      return false;
    }
    setDraft(result.flow);
    // Edits to the selected thing carry no selectId â€” leave selection alone.
    if (result.selectId) setSelectedId(result.selectId);
    setDirty(true);
    if (successMessage) toast.success(successMessage);
    return true;
  }, []);

  const handleAdd = useCallback(
    (parentNodeId: string | null, entry: AddMenuEntry) => {
      if (!draft) return;
      // Every menu entry is now a block. A parallel branch is not a separate
      // thing to add â€” `addBlock` names one when the parent already has a child.
      applyMutation(addBlock(draft, parentNodeId, entry));
    },
    [draft, applyMutation],
  );

  /**
   * Write a branch's name or condition. It goes through the shared mutation so
   * the naming rule (a lone child has no branch object until it needs one) lives
   * in exactly one place.
   */
  const handleSetBranch = useCallback(
    (blockId: string, patch: { name?: string; condition?: BranchCondition | null }) => {
      if (!draft) return;
      applyMutation(setBranch(draft, blockId, patch));
    },
    [draft, applyMutation],
  );

  const handleReparent = useCallback(
    (blockId: string, newParentId: string) => {
      if (!draft) return;
      // Derived topic names take their variant token from the branch labels
      // ABOVE a block, so a move quietly renames topics on blocks the user never
      // touched. Compute the renames against the pre-move flow, then say so.
      const renames = previewReparentRenames(draft, blockId, newParentId);
      const result = reparentBlock(draft, blockId, newParentId);
      if (!applyMutation(result)) return;
      if (renames.length > 0) {
        toast.success(`Moved â€” ${renames.length} derived name${renames.length === 1 ? "" : "s"} changed`, {
          description: renames.map((r) => `${r.from} â†’ ${r.to}`).join("\n"),
        });
      } else {
        toast.success("Block moved");
      }
    },
    [draft, applyMutation],
  );

  const handleDeleteBlock = useCallback(
    (blockId: string) => {
      if (!draft) return;
      const result = deleteBlockCascade(draft, blockId);
      if (result.ok === false) {
        toast.error(result.reason);
        return;
      }
      setDraft(result.flow);
      setSelectedId("flow");
      setDirty(true);
      const extra = result.removedIds.length - 1;
      toast.success(extra > 0 ? `Block removed â€” ${extra} downstream block${extra === 1 ? "" : "s"} went with it` : "Block removed");
    },
    [draft],
  );

  /** The one place that actually POSTs the draft â€” Save and the Test panel's
   *  onEnsureSaved both funnel through this rather than each calling
   *  saveFlow separately, so there is exactly one save path to keep honest. */
  const persistDraft = useCallback(
    async (current: Flow): Promise<Flow> => {
      const saved = await saveFlow(current);
      setDraft(saved);
      setDirty(false);
      queryClient.invalidateQueries({ queryKey: ["flows"] });
      queryClient.invalidateQueries({ queryKey: ["flow", current.id] });
      return saved;
    },
    [queryClient],
  );

  const handleSave = async () => {
    if (!draft || queueLocked) return;
    setSaving(true);
    try {
      await persistDraft(draft);
      toast.success("Draft saved");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Save failed");
    } finally {
      setSaving(false);
    }
  };

  /**
   * Test needs the draft to exist server-side â€” a flow fresh out of
   * createFlow() is only staged client-side (api.ts's stagedNewFlows) until
   * its first saveFlow() succeeds. `dirty` is a reliable stand-in for "not
   * yet persisted" here: a staged flow starts with zero blocks, so by the
   * time any block has a Test button at all, placing that block already
   * flipped `dirty`. Already-clean means already saved â€” return the draft
   * as-is rather than round-tripping a no-op save. Throws on save failure so
   * TestPanel can surface it and skip the test call.
   */
  const ensureSaved = useCallback(async (): Promise<Flow> => {
    if (!draft) throw new Error("Nothing to save yet.");
    if (!dirty) return draft;
    setSaving(true);
    try {
      return await persistDraft(draft);
    } finally {
      setSaving(false);
    }
  }, [draft, dirty, persistDraft]);

  const execVerb = async (verb: FlowVerb) => {
    if (!draft || queueLocked) return;
    setVerbBusy(verb);
    try {
      if (verb === "delete") {
        // Deletion needs the same cleanup confirmation as the Flows page
        // (topics, associated resources and dedup caches). Hand the flow id
        // to that page instead of bypassing the confirmation from the builder.
        navigate(`/flows?delete=${encodeURIComponent(draft.id)}`);
        return;
      }
      await startBulkJob(verb, [draft.id]);
      queryClient.invalidateQueries({ queryKey: ["flows"] });
      toast.info(`Queued ${verb.replace("_", " ")} â€” ${draft.name}`);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Action failed");
    } finally {
      setVerbBusy(null);
      setPreflightOpen(false);
    }
  };

  const verbReason = (verb: FlowVerb): string | null => {
    if (!draft) return "Loadingâ€¦";
    if (queueLocked) return lockReason ?? "This flow is already in the operation queue.";
    if (dirty && verb !== "delete") return "Save the draft first.";
    // Queue admission is deliberately permissive. Deploy validation runs
    // when this item's turn arrives so a bad flow cannot block later queued
    // flows. State-machine refusals still remain visible immediately.
    if (verb === "deploy") {
      if (draft.state === "Running" || draft.state === "Paused") return "Stop the flow before deploying.";
      if (draft.state === "Deploying") return "A deploy is already in progress.";
      return null;
    }
    return getVerbBlockReason(draft, verb);
  };

  // ---------------------------------------------------------------- new flow
  if (isNew) {
    return <NewFlowPanel onCreated={(id) => navigate(`/flow-builder/${id}`, { replace: true })} />;
  }

  if (isLoading || !draft) {
    return (
      <AppLayout title="Flow Builder" description="Loadingâ€¦">
        <div className="flex items-center gap-2 py-16 text-sm text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin" /> Loading flowâ€¦
        </div>
      </AppLayout>
    );
  }

  const selectedBlock = draft.blocks.find((b) => b.id === selectedId);
  const ceremonyBlock = draft.blocks.find((b) => b.id === ceremonyBlockId);
  const neverDeployed = !draft.deployedAt;
  const deployReason = verbReason("deploy");

  /**
   * A lifecycle verb button. When the verb is unavailable the reason is a real
   * tooltip rather than a `title` attribute â€” the old build hid every refusal
   * behind native hover text on a disabled button, which most browsers do not
   * even show.
   */
  const verbButton = (verb: FlowVerb, label: string, icon: React.ReactNode, variant: "default" | "outline" = "outline") => {
    const reason = verbReason(verb);
    const button = (
      <Button
        variant={variant}
        size="sm"
        disabled={!!reason || verbBusy !== null}
        onClick={() => (verb === "deploy" ? setPreflightOpen(true) : void execVerb(verb))}
      >
        {verbBusy === verb ? <Loader2 className="animate-spin" /> : icon}
        {label}
      </Button>
    );
    if (!reason) return <span key={verb}>{button}</span>;
    return (
      <Tooltip key={verb}>
        {/* A disabled button swallows pointer events, so the trigger wraps it. */}
        <TooltipTrigger asChild>
          <span className="inline-flex cursor-not-allowed">{button}</span>
        </TooltipTrigger>
        <TooltipContent>{reason}</TooltipContent>
      </Tooltip>
    );
  };

  // A never-deployed draft has one meaningful next move. Everything else stays
  // reachable behind More rather than sitting on screen permanently disabled.
  const moreVerbs: [FlowVerb, string][] = [
    ...(neverDeployed
      ? ([
          ["start", "Start"],
          ["pause", "Pause"],
          ["stop", "Stop"],
        ] as [FlowVerb, string][])
      : []),
    ["stop_clear", "Stop & Clear (discard queues)"],
    ["redeploy", "Redeploy"],
    ["undeploy", "Undeploy"],
  ];

  return (
    <AppLayout
      title={draft.name || "Untitled flow"}
      description={draft.description || "Adapter-based flow"}
      actions={
        <Button variant="outline" size="sm" onClick={() => setSelectedId("flow")} title="Jump to the validation summary">
          <ShieldCheck />
          Validate
          {issues.length > 0 && (
            <span className="ml-0.5 flex h-4 min-w-4 items-center justify-center rounded-full bg-destructive px-1 text-2xs font-semibold text-destructive-foreground">
              {issues.length}
            </span>
          )}
        </Button>
      }
    >
      <div className="space-y-6">
        {(locked || draft.drift) && (
          <div className="space-y-3">
            {locked && (
              <Alert>
                <AlertTitle>Read-only â€” {draft.state}</AlertTitle>
                <AlertDescription>{lockReason}</AlertDescription>
              </Alert>
            )}
            {draft.drift && (
              <Alert variant="destructive">
                <AlertTitle>Drift detected</AlertTitle>
                <AlertDescription>{draft.drift}</AlertDescription>
              </Alert>
            )}
          </div>
        )}

        {/* ------------------------------------------------ lifecycle bar */}
        <Card className="overflow-hidden">
          <div className="flex flex-wrap items-center gap-x-3 gap-y-2 px-4 py-2.5">
            <StatusBadge status={draft.state} />
            <Separator orientation="vertical" className="hidden h-5 sm:block" />

            {/* While the flow is locked, the only editable fields are kc blocks â€” so a
                dirty draft under lock IS the "Save is live" path and must stay savable. */}
            <Button
              size="sm"
              onClick={handleSave}
              disabled={saving || !dirty || queueLocked}
              title={!dirty ? "No unsaved changes" : queueLocked ? "The flow is locked while its queued operation runs" : locked ? "kc changes save live â€” other blocks are read-only until the flow stops" : undefined}
            >
              {saving ? <Loader2 className="animate-spin" /> : <Save />}
              Save
            </Button>
            {verbButton("deploy", "Deploy", <Rocket />, "default")}
            {!neverDeployed && (
              <>
                {verbButton("start", "Start", <Play />)}
                {draft.state === "Paused"
                  ? verbButton("resume", "Resume", <Play />)
                  : verbButton("pause", "Pause", <Pause />)}
                {verbButton("stop", "Stop", <Square />)}
              </>
            )}
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="outline" size="sm">
                  <MoreHorizontal /> More
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="start" className="w-72">
                {moreVerbs.map(([verb, label]) => {
                  const reason = verbReason(verb);
                  return (
                    <DropdownMenuItem
                      key={verb}
                      disabled={!!reason}
                      className="flex-col items-start gap-0.5"
                      onClick={() => void execVerb(verb)}
                    >
                      <span className="text-sm">{label}</span>
                      {reason ? <span className="text-xs text-muted-foreground">{reason}</span> : null}
                    </DropdownMenuItem>
                  );
                })}
                <DropdownMenuSeparator />
                <DropdownMenuItem
                  disabled={queueLocked || enabledBusy}
                  onClick={() => {
                    if (queueLocked) return;
                    const enabled = !draft.enabled;
                    setEnabledBusy(true);
                    void startBulkJob(enabled ? "enable" : "disable", [draft.id])
                      .then(() => {
                        toast.info(`Queued ${enabled ? "Enable" : "Disable"} â€” ${draft.name}`);
                        queryClient.invalidateQueries({ queryKey: ["flows"] });
                      })
                      .catch((err) => toast.error(err instanceof Error ? err.message : "Could not queue enabled state change"))
                      .finally(() => setEnabledBusy(false));
                  }}
                >
                  {enabledBusy ? "Queueingâ€¦" : draft.enabled ? "Disable" : "Enable"}
                </DropdownMenuItem>
                {(() => {
                  const reason = verbReason("delete");
                  return (
                    <DropdownMenuItem
                      className="flex-col items-start gap-0.5 text-destructive"
                      disabled={!!reason}
                      onClick={() => void execVerb("delete")}
                    >
                      <span className="text-sm">Delete flow</span>
                      {reason ? <span className="text-xs text-muted-foreground">{reason}</span> : null}
                    </DropdownMenuItem>
                  );
                })()}
              </DropdownMenuContent>
            </DropdownMenu>

            {/* This strip is the only home of the unsaved-change signal and the
                DLQ name; demoting the verb bar must not take them with it. */}
            <div className="ml-auto flex flex-wrap items-center gap-2">
              {dirty && <Badge variant="warning">Unsaved changes</Badge>}
              <Badge variant="outline">{draft.deployedAt ? "Deployed once" : "Never deployed"}</Badge>
              <span className="flex items-center gap-1.5 text-xs text-muted-foreground">
                DLQ <Mono>{dlqName(draft.name)}</Mono>
                <InfoDot title="Dead-letter queue">
                  Every flow gets one dead-letter topic, named from the flow name. Records that cannot be delivered land
                  there instead of being dropped â€” the name freezes when the flow deploys.
                </InfoDot>
              </span>
            </div>
          </div>

          {/* The reason Deploy is unavailable used to exist only as a `title`
              attribute â€” invisible to anyone not hovering a disabled button. */}
          {deployReason && (
            <div className="border-t border-border/60 bg-muted/40 px-4 py-2">
              <p className="text-xs leading-relaxed text-muted-foreground">
                <span className="font-medium text-foreground">Deploy unavailable</span> â€” {deployReason}
              </p>
            </div>
          )}
        </Card>

        {/* ----------------------------------------- map (left) + form (right)
            Two columns, not three: the outline rail is gone. It navigated and
            added blocks, and the map does both â€” keeping it meant two lists of
            the same nodes competing for the same clicks. What it also carried,
            the way back to Flow settings, moves onto the form pane's header
            where the thing being configured is named. */}
        <div className="grid gap-5 xl:grid-cols-[minmax(0,1.35fr),minmax(420px,0.8fr)] xl:items-start xl:gap-6 2xl:grid-cols-[minmax(0,1.55fr),minmax(520px,0.85fr)]">
          <section className={cn("min-w-0 space-y-3", !mapExpanded && "xl:sticky xl:top-24")}>
            <div className="flex flex-wrap items-center justify-between gap-2">
              <h2 className="flex items-center gap-1.5 text-sm font-semibold">
                Flow map
                <InfoDot title="What the map can do">
                  The map and the forms are two views of one flow. Create here, configure in the form â€” drag a block's
                  right dot onto empty space to add the next block, drag an edge end to move a branch, and press Delete
                  to remove one. Only legal moves are accepted; anything refused says why. Selecting a node opens its
                  form; nothing is configured on the canvas.
                </InfoDot>
              </h2>
              <div className="flex items-center gap-1">
                {mapOpen && (
                  <Button variant="ghost" size="xs" onClick={() => setMapExpanded((o) => !o)}>
                    {mapExpanded ? <Minimize2 /> : <Maximize2 />}
                    {mapExpanded ? "Collapse" : "Expand"}
                  </Button>
                )}
                <Button variant="ghost" size="xs" onClick={() => setMapOpen((o) => !o)}>
                  {mapOpen ? <EyeOff /> : <Eye />}
                  {mapOpen ? "Hide" : "Show"}
                </Button>
              </div>
            </div>
            {/* Kept mounted while hidden: unmounting an xyflow canvas throws away
                its viewport, so every toggle would reset the user's pan and zoom.
                Expanded uses the same trick â€” `fixed inset-0` re-parents the SAME
                instance's visual bounds instead of opening a second canvas, so
                pan/zoom/selection survive the toggle in both directions. */}
            <div
              className={cn(
                "overflow-hidden rounded-xl shadow-inner transition-[height] duration-200",
                !mapOpen && "hidden",
                mapExpanded
                  // bg-muted/50 is right for an inset panel that sits ON the
                  // page background â€” it reads as a recessed canvas. The same
                  // translucency, promoted to `fixed` and covering the
                  // viewport, let the dimmed page underneath bleed through its
                  // own gaps, doubling up with the scrim into a hazy mess. Full
                  // opacity here; the scrim does the dimming job instead.
                  ? "fixed inset-4 z-40 bg-muted shadow-2xl md:inset-6"
                  : "h-[clamp(540px,calc(100svh-13rem),960px)] bg-muted/50",
              )}
            >
              <FlowMapView
                flow={draft}
                selectedId={selectedId}
                issuesByNode={issuesByNode}
                locked={locked}
                lockReason={lockReason}
                onSelect={setSelectedId}
                onAdd={handleAdd}
                onReparent={handleReparent}
                onDelete={handleDeleteBlock}
                expanded={mapExpanded}
              />
              {mapExpanded && (
                <Button
                  variant="outline"
                  size="icon-sm"
                  className="material-thick absolute right-3 top-3 z-10 shadow-md"
                  onClick={() => setMapExpanded(false)}
                  title="Collapse (Esc)"
                >
                  <Minimize2 />
                  <span className="sr-only">Collapse</span>
                </Button>
              )}
            </div>
            {/* A scrim behind the expanded canvas â€” same treatment as a Dialog
                overlay, so the "this is a temporary focused view" read is
                consistent with every other overlay in the app. */}
            {mapExpanded && (
              <div className="fixed inset-0 z-30 bg-foreground/25 backdrop-blur-[2px]" onClick={() => setMapExpanded(false)} />
            )}

            {!mapExpanded && <DestinationsPanel flow={draft} onSelect={setSelectedId} />}
          </section>

          {/* The one dominant surface on the page. The elevation is applied from
              here so the form component itself stays free of page-layout
              concerns; it lands on whatever top-level cards the form renders. */}
          <div className="min-w-0 space-y-3 xl:sticky xl:top-24 xl:max-h-[calc(100svh-7rem)] xl:overflow-y-auto xl:pr-1">
            {/* The form pane says what it is configuring, and holds the only
                route back to flow-level settings now that the rail is gone. */}
            <div className="flex flex-wrap items-center gap-2">
              <Button
                variant={selectedId === "flow" || selectedId === null ? "secondary" : "ghost"}
                size="xs"
                onClick={() => setSelectedId("flow")}
              >
                <Settings2 /> Flow settings
                {flowIssues.length > 0 && (
                  <span className="ml-0.5 flex h-4 min-w-4 items-center justify-center rounded-full bg-destructive px-1 text-2xs font-semibold text-destructive-foreground">
                    {flowIssues.length}
                  </span>
                )}
              </Button>
              {selectedBlock && (
                <span className="flex min-w-0 items-center gap-1 text-xs text-muted-foreground">
                  <ChevronRight className="h-3.5 w-3.5 shrink-0" />
                  <span className="truncate font-medium text-foreground">{selectedBlock.name || "Untitled block"}</span>
                </span>
              )}
            </div>

            <div className="[&>.bg-card]:!shadow-md [&>div>.bg-card]:!shadow-md">
              {selectedBlock ? (
                <BlockForm
                  flow={draft}
                  block={selectedBlock}
                  locked={locked}
                  queueLocked={queueLocked}
                  services={services}
                  schemas={schemas}
                  issues={issues.filter((i) => i.blockId === selectedBlock.id)}
                  onPatchBlock={patchBlock}
                  onPatchConfig={patchConfig}
                  onDeleteBlock={handleDeleteBlock}
                  onSetBranch={handleSetBranch}
                  onSelectBlock={setSelectedId}
                  onOpenCeremony={(id) => {
                    setCeremonyPrefill(null);
                    setCeremonyBlockId(id);
                  }}
                  onEnsureSaved={ensureSaved}
                />
              ) : draft.topics.some((t) => t.id === selectedId) ? (
                <TopicDetails
                  flow={draft}
                  topicId={selectedId}
                  locked={locked}
                  onSelect={setSelectedId}
                  onRename={(topicId, name) => {
                    setDraft((d) =>
                      d ? { ...d, topics: d.topics.map((t) => (t.id === topicId ? { ...t, name } : t)) } : d,
                    );
                    setDirty(true);
                  }}
                />
              ) : (
                <FlowSettingsForm flow={draft} locked={locked} issues={issues} onPatch={patchDraft} onSelectBlock={setSelectedId} />
              )}
            </div>
          </div>
        </div>
      </div>

      <PreflightDialog flow={draft} open={preflightOpen} onOpenChange={setPreflightOpen} onDeploy={() => void execVerb("deploy")} deploying={verbBusy === "deploy"} />

      {ceremonyBlock && (
        <CeremonyDialog
          // ?prefill=<templateId> from the Schemas browser, consumed above and
          // handed to the ceremony as its pre-fill source.
          prefillTemplateId={ceremonyPrefill}
          // An edit made on the Schemas page: it seeds Review directly, because
          // the point of that path is to register exactly what was edited.
          prefillDraft={ceremonyDraft}
          flow={draft}
          block={ceremonyBlock}
          open={!!ceremonyBlockId}
          approvedSchemas={schemas}
          onOpenChange={(open) => {
            if (!open) {
              setCeremonyBlockId(null);
              setCeremonyPrefill(null);
              setCeremonyDraft(null);
            }
          }}
          onApproved={(_schema, entity) => {
            patchBlock(ceremonyBlock.id, { entity });
            setDirty(false);
            queryClient.invalidateQueries({ queryKey: ["schemas"] });
            queryClient.invalidateQueries({ queryKey: ["flow", draft.id] });
          }}
        />
      )}
    </AppLayout>
  );
}

function TopicDetails({
  flow,
  topicId,
  locked,
  onSelect,
  onRename,
}: {
  flow: Flow;
  topicId: string;
  locked: boolean;
  onSelect: (id: string) => void;
  onRename: (topicId: string, name: string) => void;
}) {
  const topic = flow.topics.find((t) => t.id === topicId);
  if (!topic) return null;
  const writer = flow.blocks.find((b) => b.id === topic.writerBlockId);
  const sinks = flow.blocks.filter((b) => b.adapter === "kc" && b.config.attachTopicId === topic.id);
  const readers = flow.blocks.filter((b) => b.adapter === "kafka" && b.mode === "read" && b.parentId === topic.id);
  const adoptionEditable = topic.kind === "adopted" && !locked && !flow.deployedAt;
  return (
    <Card>
      <CardHeader>
        <CardTitle className="font-mono text-base">{topic.name}</CardTitle>
        <CardDescription>
          {topic.kind === "adopted" ? "Adopted topic â€” sampled, never renamed." : `Materialized by ${writer?.name ?? "a write block"}.`}
          {topic.sealed && " Sealed â€” kafka+connect topics are managed with their sink as one unit; nothing can attach."}
        </CardDescription>
      </CardHeader>
      <CardContent>
        <FieldGroup>
          {adoptionEditable && (
            <Field
              label="Topic to adopt"
              className="max-w-sm"
              info="Adoption points at an existing topic â€” the platform samples it and never renames it. The choice freezes at deploy."
            >
              <Input className="font-mono text-xs" value={topic.name} onChange={(e) => onRename(topic.id, e.target.value)} />
            </Field>
          )}

          {typeof topic.backlogEstimate === "number" && (
            <p className="text-xs text-muted-foreground">
              Currently holds ~{topic.backlogEstimate.toLocaleString()} messages.
            </p>
          )}

          <div className="space-y-2">
            <p className="flex items-center gap-1.5 text-2xs font-semibold uppercase tracking-wider text-muted-foreground">
              Attached
              <InfoDot title="What can attach to a topic">
                Exactly two things attach to a topic: a kafka read, which continues the chain, and a kc sink
                subscription, which is an independent subscriber. Nothing else can hang off one.
              </InfoDot>
            </p>
            {readers.length === 0 && sinks.length === 0 && (
              <p className="text-xs text-muted-foreground">Nothing attached yet â€” use ï¼‹ on the topic node.</p>
            )}
            <div className="space-y-1">
              {readers.map((r) => (
                <button
                  key={r.id}
                  type="button"
                  className="flex w-full items-center gap-2 rounded-lg px-2 py-1.5 text-left text-xs transition-colors hover:bg-accent"
                  onClick={() => onSelect(r.id)}
                >
                  <Badge variant="outline">kafka Â· read</Badge>
                  <span className="truncate font-medium text-foreground">{r.name}</span>
                  <span className="ml-auto shrink-0 text-muted-foreground">chain continues</span>
                </button>
              ))}
              {sinks.map((s) => (
                <button
                  key={s.id}
                  type="button"
                  className="flex w-full items-center gap-2 rounded-lg px-2 py-1.5 text-left text-xs transition-colors hover:bg-accent"
                  onClick={() => onSelect(s.id)}
                >
                  <Badge variant="outline">kc Â· sink</Badge>
                  <span className="truncate font-medium text-foreground">{s.name}</span>
                  <span className="ml-auto shrink-0 text-muted-foreground">independent</span>
                </button>
              ))}
            </div>
          </div>
        </FieldGroup>
      </CardContent>
    </Card>
  );
}

function NewFlowPanel({ onCreated }: { onCreated: (id: string) => void }) {
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [creating, setCreating] = useState(false);

  const create = async () => {
    setCreating(true);
    try {
      const flow = await createFlow(name.trim(), description.trim() || undefined);
      toast.success(`Flow "${flow.name}" created â€” now place the root block`);
      onCreated(flow.id);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Create failed");
      setCreating(false);
    }
  };

  return (
    <AppLayout title="New flow" description="Naming comes first â€” the name fixes every derived topic, table and DLQ name.">
      <Card className="max-w-xl shadow-md">
        <CardHeader>
          <CardTitle>Name your flow</CardTitle>
          <CardDescription>
            The flow name is the source name: the first half of every derived name. It freezes at deploy.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <FieldGroup>
            <Field
              label="Name"
              hint={
                name.trim() ? (
                  <div className="space-y-1">
                    <div>
                      topics <Mono>raw.{tokenize(name)}.&lt;entity&gt;</Mono>
                    </div>
                    <div>
                      tables <Mono>bronze.{tokenize(name)}.&lt;entity&gt;__raw</Mono>
                    </div>
                    <div>
                      DLQ <Mono>{dlqName(name)}</Mono>
                    </div>
                  </div>
                ) : undefined
              }
            >
              <Input value={name} onChange={(e) => setName(e.target.value)} placeholder="e.g. CrowdStrike Detections" autoFocus />
            </Field>

            <Field label="Description (optional)">
              <Textarea value={description} onChange={(e) => setDescription(e.target.value)} rows={2} />
            </Field>

            <div className="flex justify-end">
              <Button onClick={create} disabled={!name.trim() || creating}>
                {creating && <Loader2 className="animate-spin" />}
                Create & open builder
              </Button>
            </div>
          </FieldGroup>
        </CardContent>
      </Card>
    </AppLayout>
  );
}


