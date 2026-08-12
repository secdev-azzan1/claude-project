// The adapter-based Flow Builder: form-centric configuration (outline + a
// per-block form) supported by an interactive map.
//
// Surface hierarchy on this page — one raised thing, and it is the form:
//   · block form  → the single DOMINANT surface (elevation 3)
//   · outline     → a FLAT RAIL, no Card at all
//   · flow map    → a RECESSED canvas band (inset), a different kind of surface
//     rather than a third competing panel
//
// Structure is never edited here. Every create / re-parent / delete goes through
// src/prototype/mutations.ts, which owns the edit lock, the legality rules and
// the cycle guard — so the map, the outline and the form cannot drift apart, and
// a new surface cannot forget a guard.

import { useCallback, useEffect, useMemo, useState } from "react";
import { useNavigate, useParams, useSearchParams } from "react-router-dom";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { AppLayout } from "@/components/AppLayout";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { StatusBadge } from "@/components/StatusBadge";
import { FlowMapView } from "@/components/flow-builder/FlowMapView";
import { BlockForm } from "@/components/flow-builder/BlockForm";
import { FlowSettingsForm } from "@/components/flow-builder/FlowSettingsForm";
import { DestinationsPanel } from "@/components/flow-builder/DestinationsPanel";
import { PreflightDialog } from "@/components/flow-builder/PreflightDialog";
import { CeremonyDialog } from "@/components/flow-builder/CeremonyDialog";
import { ADAPTER_META } from "@/components/AdapterChip";
import { cn } from "@/lib/utils";
import {
  consumeCeremonyDraft,
  createFlow,
  getEditLockReason,
  getFlow,
  getVerbBlockReason,
  listSchemas,
  listServices,
  runFlowVerb,
  saveFlow,
  setFlowEnabled,
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
  ChevronDown,
  ChevronRight,
  ChevronsUpDown,
  Info,
  Loader2,
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

/**
 * Rule jargon lives here, not in always-visible helper text. The refusal
 * MESSAGES stay verbatim wherever they are shown — the spec requires every
 * refusal to explain itself in those words — it is the ambient lore that moves
 * behind an info affordance.
 */
function RuleInfo({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <Popover>
      <PopoverTrigger asChild>
        <button
          type="button"
          aria-label={`About: ${title}`}
          className="inline-flex h-5 w-5 shrink-0 items-center justify-center rounded-full text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
        >
          <Info className="h-3.5 w-3.5" />
        </button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-72 shadow-xl">
        <p className="text-xs font-medium">{title}</p>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">{children}</p>
      </PopoverContent>
    </Popover>
  );
}

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
  const [preflightOpen, setPreflightOpen] = useState(false);
  const [ceremonyBlockId, setCeremonyBlockId] = useState<string | null>(null);
  const [ceremonyPrefill, setCeremonyPrefill] = useState<string | null>(null);
  /** An edited schema handed over from the Schemas page, claimed on arrival. */
  const [ceremonyDraft, setCeremonyDraft] = useState<{ rawAvro: string; label: string } | null>(null);
  const [mapOpen, setMapOpen] = useState(true);

  const { data: serverFlow, isLoading } = useQuery({
    queryKey: ["flow", flowId],
    queryFn: () => getFlow(flowId!),
    enabled: !isNew,
  });
  const { data: services = [] } = useQuery({ queryKey: ["services"], queryFn: listServices });
  const { data: schemas = [] } = useQuery({ queryKey: ["schemas"], queryFn: listSchemas });

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
      toast.error("The flow is deployed — stop it before re-running the schema ceremony.");
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

  const issues = useMemo(() => (draft ? validateFlow(draft, services, schemas) : []), [draft, services, schemas]);
  const issuesByNode = useMemo(() => {
    const map = new Map<string, number>();
    for (const i of issues) if (i.blockId) map.set(i.blockId, (map.get(i.blockId) ?? 0) + 1);
    return map;
  }, [issues]);
  const flowIssues = issues.filter((i) => i.blockId === null);

  const lockReason = draft ? getEditLockReason(draft) : null;
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
   * Apply a shared mutation. Selection is set OUTSIDE the state updater — the
   * old code called setSelectedId inside setDraft, which double-fires under
   * StrictMode — and a refusal is shown verbatim.
   */
  const applyMutation = useCallback((result: MutationResult, successMessage?: string): boolean => {
    // `ok === false` rather than `!ok`: this project compiles with strict off,
    // where truthiness alone does not narrow a boolean-discriminated union.
    if (result.ok === false) {
      toast.error(result.reason);
      return false;
    }
    setDraft(result.flow);
    // Edits to the selected thing carry no selectId — leave selection alone.
    if (result.selectId) setSelectedId(result.selectId);
    setDirty(true);
    if (successMessage) toast.success(successMessage);
    return true;
  }, []);

  const handleAdd = useCallback(
    (parentNodeId: string | null, entry: AddMenuEntry) => {
      if (!draft) return;
      // Every menu entry is now a block. A parallel branch is not a separate
      // thing to add — `addBlock` names one when the parent already has a child.
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
        toast.success(`Moved — ${renames.length} derived name${renames.length === 1 ? "" : "s"} changed`, {
          description: renames.map((r) => `${r.from} → ${r.to}`).join("\n"),
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
      toast.success(extra > 0 ? `Block removed — ${extra} downstream block${extra === 1 ? "" : "s"} went with it` : "Block removed");
    },
    [draft],
  );

  const handleSave = async () => {
    if (!draft) return;
    setSaving(true);
    try {
      const saved = await saveFlow(draft);
      setDraft(saved);
      setDirty(false);
      queryClient.invalidateQueries({ queryKey: ["flows"] });
      queryClient.invalidateQueries({ queryKey: ["flow", draft.id] });
      toast.success("Draft saved");
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Save failed");
    } finally {
      setSaving(false);
    }
  };

  const execVerb = async (verb: FlowVerb) => {
    if (!draft) return;
    setVerbBusy(verb);
    try {
      const result = await runFlowVerb(draft.id, verb);
      if (verb === "delete") {
        toast.success(`Flow "${draft.name}" deleted`);
        navigate("/flows");
        return;
      }
      if (result) {
        setDraft(result);
        setDirty(false);
      }
      queryClient.invalidateQueries({ queryKey: ["flows"] });
      toast.success(
        {
          deploy: "Deployed — the flow is built stopped",
          start: "Flow started",
          pause: "Paused — the trigger keeps firing, records queue until Resume",
          resume: "Resumed",
          stop: "Stopped — queues retained",
          stop_clear: "Stopped & cleared — queued records discarded (audited)",
          redeploy: "Redeployed",
          undeploy: "Undeployed — generated topics emptied, dedup caches cleared, positions reset",
          delete: "Deleted",
        }[verb],
      );
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Action failed");
    } finally {
      setVerbBusy(null);
      setPreflightOpen(false);
    }
  };

  const verbReason = (verb: FlowVerb): string | null => {
    if (!draft) return "Loading…";
    if (dirty && verb !== "delete") return "Save the draft first.";
    return getVerbBlockReason(draft, verb);
  };

  // ---------------------------------------------------------------- new flow
  if (isNew) {
    return <NewFlowPanel onCreated={(id) => navigate(`/flow-builder/${id}`, { replace: true })} />;
  }

  if (isLoading || !draft) {
    return (
      <AppLayout title="Flow Builder" description="Loading…">
        <div className="flex items-center gap-2 py-16 text-sm text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin" /> Loading flow…
        </div>
      </AppLayout>
    );
  }

  const selectedBlock = draft.blocks.find((b) => b.id === selectedId);
  const ceremonyBlock = draft.blocks.find((b) => b.id === ceremonyBlockId);
  const neverDeployed = !draft.deployedAt;
  const deployReason = verbReason("deploy");

  const verbButton = (verb: FlowVerb, label: string, icon: React.ReactNode, variant: "default" | "outline" | "secondary" = "outline") => {
    const reason = verbReason(verb);
    return (
      <Button
        key={verb}
        variant={variant}
        size="sm"
        className="gap-1.5"
        disabled={!!reason || verbBusy !== null}
        title={reason ?? undefined}
        onClick={() => (verb === "deploy" ? setPreflightOpen(true) : void execVerb(verb))}
      >
        {verbBusy === verb ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : icon}
        {label}
      </Button>
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
        <Button variant="outline" size="sm" className="gap-1.5" onClick={() => setSelectedId("flow")} title="Jump to the validation summary">
          <ShieldCheck className="h-3.5 w-3.5" />
          Validate {issues.length > 0 ? `(${issues.length})` : ""}
        </Button>
      }
    >
      <div className="space-y-8">
        {(locked || draft.drift) && (
          <div className="space-y-3">
            {locked && (
              <Alert>
                <AlertTitle className="text-sm">Read-only — {draft.state}</AlertTitle>
                <AlertDescription className="text-xs leading-5">{lockReason}</AlertDescription>
              </Alert>
            )}
            {draft.drift && (
              <Alert variant="destructive">
                <AlertTitle className="text-sm">Drift detected</AlertTitle>
                <AlertDescription className="text-xs leading-5">{draft.drift}</AlertDescription>
              </Alert>
            )}
          </div>
        )}

        {/* ------------------------------------------------ lifecycle bar */}
        <Card>
          <CardContent className="flex flex-wrap items-center gap-x-3 gap-y-2 py-3">
            <StatusBadge status={draft.state} />
            <span className="hidden h-5 w-px bg-border sm:block" />
            {/* While the flow is locked, the only editable fields are kc blocks — so a
                dirty draft under lock IS the "Save is live" path and must stay savable. */}
            <Button
              size="sm"
              className="gap-1.5"
              onClick={handleSave}
              disabled={saving || !dirty}
              title={!dirty ? "No unsaved changes" : locked ? "kc changes save live — other blocks are read-only until the flow stops" : undefined}
            >
              {saving ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Save className="h-3.5 w-3.5" />}
              Save
            </Button>
            {verbButton("deploy", "Deploy", <Rocket className="h-3.5 w-3.5" />, "default")}
            {!neverDeployed && (
              <>
                {verbButton("start", "Start", <Play className="h-3.5 w-3.5" />)}
                {draft.state === "Paused"
                  ? verbButton("resume", "Resume", <Play className="h-3.5 w-3.5" />)
                  : verbButton("pause", "Pause", <Pause className="h-3.5 w-3.5" />)}
                {verbButton("stop", "Stop", <Square className="h-3.5 w-3.5" />)}
              </>
            )}
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="outline" size="sm" className="gap-1">
                  <MoreHorizontal className="h-3.5 w-3.5" /> More
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="start" className="w-72 shadow-xl">
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
                  onClick={() => void setFlowEnabled(draft.id, !draft.enabled).then(() => {
                    patchDraft({ enabled: !draft.enabled });
                    setDirty(false);
                  })}
                >
                  {draft.enabled ? "Disable" : "Enable"}
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
            <div className="ml-auto flex flex-wrap items-center gap-2 text-xs">
              {dirty && (
                <span className="rounded-full border border-warning/20 bg-warning-muted px-2 py-0.5 font-medium text-warning">
                  Unsaved changes
                </span>
              )}
              <span className="text-muted-foreground">{draft.deployedAt ? "Deployed once" : "Never deployed"}</span>
              <span className="flex items-center gap-1 text-muted-foreground">
                DLQ <code className="font-mono text-foreground">{dlqName(draft.name)}</code>
                <RuleInfo title="Dead-letter queue">
                  Every flow gets one dead-letter topic, named from the flow name. Records that cannot be delivered land
                  there instead of being dropped — the name freezes when the flow deploys.
                </RuleInfo>
              </span>
            </div>
          </CardContent>

          {/* The reason Deploy is unavailable used to exist only as a `title`
              attribute — invisible to anyone not hovering a disabled button. */}
          {deployReason && (
            <div className="border-t px-6 py-2">
              <p className="text-xs leading-5 text-muted-foreground">
                <span className="font-medium text-foreground">Deploy unavailable</span> — {deployReason}
              </p>
            </div>
          )}
        </Card>

        {/* ----------------------------------------- map (left) + form (right)
            Two columns, not three: the outline rail is gone. It navigated and
            added blocks, and the map does both — keeping it meant two lists of
            the same nodes competing for the same clicks. What it also carried,
            the way back to Flow settings, moves onto the form pane's header
            where the thing being configured is named. */}
        <div className="grid gap-5 xl:grid-cols-[minmax(0,5fr),minmax(0,7fr)] xl:items-start xl:gap-6">
          <section className="min-w-0 space-y-3 xl:sticky xl:top-4">
            <div className="flex flex-wrap items-start justify-between gap-2">
              <div>
                <h2 className="flex items-center gap-1.5 text-sm font-semibold">
                  Flow map
                  <RuleInfo title="What the map can do">
                    The map and the forms are two views of one flow. Create here, configure in the form — drag a block's
                    right dot onto empty space to add the next block, drag an edge end to move a branch, and press
                    Delete to remove one. Only legal moves are accepted; anything refused says why.
                  </RuleInfo>
                </h2>
                <p className="text-xs leading-5 text-muted-foreground">
                  Selecting a node opens its form. Nothing is configured on the canvas.
                </p>
              </div>
              <Button variant="ghost" size="sm" className="gap-1.5" onClick={() => setMapOpen((o) => !o)}>
                <ChevronsUpDown className="h-3.5 w-3.5" />
                {mapOpen ? "Hide" : "Show"}
              </Button>
            </div>
            {/* Kept mounted while hidden: unmounting an xyflow canvas throws away
                its viewport, so every toggle would reset the user's pan and zoom. */}
            <div
              className={cn(
                "h-[420px] overflow-hidden rounded-xl border bg-muted/40 shadow-inner",
                !mapOpen && "hidden",
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
              />
            </div>

            <DestinationsPanel flow={draft} onSelect={setSelectedId} />
          </section>

          {/* The one dominant surface on the page. The elevation is applied from
              here so the form component itself stays free of page-layout
              concerns; it lands on whatever top-level cards the form renders. */}
          <div className="min-w-0 space-y-3">
            {/* The form pane says what it is configuring, and holds the only
                route back to flow-level settings now that the rail is gone. */}
            <div className="flex flex-wrap items-center gap-2">
              <Button
                variant={selectedId === "flow" || selectedId === null ? "secondary" : "ghost"}
                size="sm"
                className="h-8 gap-1.5 text-xs"
                onClick={() => setSelectedId("flow")}
              >
                <Settings2 className="h-3.5 w-3.5" /> Flow settings
                {flowIssues.length > 0 && (
                  <span className="flex h-[18px] min-w-[18px] items-center justify-center rounded-full bg-destructive px-1 text-xs font-semibold text-destructive-foreground">
                    {flowIssues.length}
                  </span>
                )}
              </Button>
              {selectedBlock && (
                <span className="flex min-w-0 items-center gap-1.5 text-xs text-muted-foreground">
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
      <CardHeader className="pb-3">
        <CardTitle className="font-mono text-base">{topic.name}</CardTitle>
        <CardDescription className="leading-5">
          {topic.kind === "adopted" ? "Adopted topic — sampled · never renamed." : `Materialized by ${writer?.name ?? "a write block"}.`}
          {topic.sealed && " Sealed — kafka+connect topics are managed with their sink as one unit; nothing can attach."}
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4 text-sm">
        {adoptionEditable && (
          <div className="grid max-w-sm gap-1.5">
            <Label>Topic to adopt</Label>
            <Input
              className="font-mono text-xs"
              value={topic.name}
              onChange={(e) => onRename(topic.id, e.target.value)}
            />
            <p className="text-xs leading-5 text-muted-foreground">
              Adoption points at an existing topic — the platform samples it and never renames it. The choice freezes at
              deploy.
            </p>
          </div>
        )}
        {typeof topic.backlogEstimate === "number" && (
          <p className="text-xs text-muted-foreground">Currently holds ~{topic.backlogEstimate.toLocaleString()} messages.</p>
        )}
        <div className="space-y-1.5">
          <p className="flex items-center gap-1.5 text-xs font-medium text-muted-foreground">
            Attached
            <RuleInfo title="What can attach to a topic">
              Exactly two things attach to a topic: a kafka read, which continues the chain, and a kc sink
              subscription, which is an independent subscriber. Nothing else can hang off one.
            </RuleInfo>
          </p>
          {readers.length === 0 && sinks.length === 0 && (
            <p className="text-xs text-muted-foreground">Nothing attached yet — use ＋ on the topic node.</p>
          )}
          {readers.map((r) => (
            <button key={r.id} type="button" className="block text-xs hover:underline" onClick={() => onSelect(r.id)}>
              kafka · read — {r.name} (chain continues)
            </button>
          ))}
          {sinks.map((s) => (
            <button key={s.id} type="button" className="block text-xs hover:underline" onClick={() => onSelect(s.id)}>
              kc · sink — {s.name} (independent subscription)
            </button>
          ))}
        </div>
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
      toast.success(`Flow "${flow.name}" created — now place the root block`);
      onCreated(flow.id);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Create failed");
      setCreating(false);
    }
  };

  return (
    <AppLayout title="New flow" description="Naming comes first — the name fixes every derived topic, table and DLQ name.">
      <Card className="max-w-xl shadow-md">
        <CardHeader>
          <CardTitle className="text-base">Name your flow</CardTitle>
          <CardDescription>The flow name is the source name: the first half of every derived name. It freezes at deploy.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-1.5">
            <Label>Name</Label>
            <Input value={name} onChange={(e) => setName(e.target.value)} placeholder="e.g. CrowdStrike Detections" autoFocus />
            {name.trim() && (
              <p className="text-xs leading-5 text-muted-foreground">
                topics <code className="font-mono">raw.{tokenize(name)}.&lt;entity&gt;</code> · tables{" "}
                <code className="font-mono">bronze.{tokenize(name)}.&lt;entity&gt;__raw</code> · DLQ{" "}
                <code className="font-mono">{dlqName(name)}</code>
              </p>
            )}
          </div>
          <div className="grid gap-1.5">
            <Label>Description (optional)</Label>
            <Textarea value={description} onChange={(e) => setDescription(e.target.value)} rows={2} />
          </div>
          <div className="flex justify-end">
            <Button onClick={create} disabled={!name.trim() || creating} className="gap-1.5">
              {creating && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
              Create & open builder
            </Button>
          </div>
        </CardContent>
      </Card>
    </AppLayout>
  );
}
