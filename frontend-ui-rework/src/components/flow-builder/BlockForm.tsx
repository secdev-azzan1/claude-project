// The per-block form — the primary configuration surface of the builder, and
// the single dominant surface on the screen (elevation role: shadow-md).
//
// It is ONE accordion, not a stack of seven always-open cards. The verdict on
// the previous build was "everything seems congested", so the governing rule
// here is that net visible surface goes DOWN even as capability goes up:
// sections carry a one-line summary when closed and only open when they have
// something to say.
//
// The second rule, added in the redesign: rule text does not get a line of its
// own. Every "no write without an entity, ever" / "names freeze at deploy" /
// "hosts and credentials always come from a saved service" moved behind the ⓘ
// beside its field label (see components/form/Field.tsx). The wording is
// unchanged — the spec requires refusals and rules to be stated in those words —
// but a rule the user has already internalised no longer costs a line of the
// form forever. What stays inline is only what is DERIVED and worth seeing: a
// resolved URL, a computed topic name, a warning, a refusal.
//
// Three sections are force-open because collapsing them would hide a deploy
// blocker that appears nowhere else: Identity while the block has validation
// issues (the issue list lives in its header), Entity while a derived-name
// warning or a topic-name collision exists, and Schema always — it is the
// ceremony's only entry point and the target of the `?ceremony=<blockId>` deep
// link, which expects the affordance to be clickable straight away.
//
// Open state is derived from block state and keyed by block id rather than
// persisted per block: the selected block changes on every add, and stale
// per-block open state makes sections appear to jump.

import { useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from "@/components/ui/accordion";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Switch } from "@/components/ui/switch";
import { Textarea } from "@/components/ui/textarea";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
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
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Field, FieldGroup, FieldMessage, FactRow, InfoDot, Mono } from "@/components/form/Field";
import { AdapterChip } from "@/components/AdapterChip";
import { StatusBadge } from "@/components/StatusBadge";
import { TransformsEditor } from "./TransformsEditor";
import { TestPanel } from "./TestPanel";
import { KvRows, type KvRow } from "./KvRows";
import { branchesOf, branchSummary, describeBranch, isConditional } from "@/prototype/branches";
import { PaginationFields } from "./PaginationFields";
import { SinkConfigEditor } from "./SinkConfigEditor";
import { OpenApiPanel } from "./OpenApiPanel";
import { OpenApiPathCombobox } from "./OpenApiPathCombobox";
import {
  ServiceFormFields,
  buildConfig,
  emptyForm,
  formFromService,
  saveBlockReason,
  secretTyped,
  type ServiceForm,
} from "@/components/service-form/ServiceFormFields";
import { BranchesCard } from "./BranchesCard";
import { hostsTransforms, isTerminal } from "@/prototype/legality";
import {
  deriveTopicName,
  derivedTopicDefault,
  overrideMatchesDerived,
  tableName,
  tokenize,
  topicNameCollision,
} from "@/prototype/naming";
import {
  adoptKafkaConnectSync,
  linkKafkaConnectSync,
  listKafkaConnectSyncs,
  saveKafkaConnectSync,
  saveService,
} from "@/prototype/api";
import { kafkaConnectLinkIssue, flowSinkTopic } from "@/prototype/kafkaConnectLink";
import { getOpenApiOperationDetail } from "@/prototype/openapiClient";
import { uid } from "@/prototype/store";
import { cn } from "@/lib/utils";
import type {
  AppService,
  ApprovedSchema,
  BlockMode,
  BranchCondition,
  BlockTestResult,
  Flow,
  FlowBlock,
  TransformRule,
} from "@/prototype/types";
import {
  AlertCircle,
  Fingerprint,
  FlaskConical,
  FlaskConicalOff,
  GitBranch,
  IdCard,
  Info,
  Loader2,
  Lock,
  Plus,
  Pin,
  Settings2,
  ShieldCheck,
  Shuffle,
  Sliders,
  Tags,
  Trash2,
} from "lucide-react";
import { toast } from "sonner";

/**
 * Methods the mode can legally use. A write POSTs, PUTs or PATCHes records
 * onward — offering GET there described the opposite of what the block does, and
 * DELETE is not a shape this platform models (there is nothing to forward).
 */
const METHODS_FOR_MODE = (mode: BlockMode | undefined): string[] =>
  mode === "write" ? ["POST", "PUT", "PATCH"] : ["GET", "POST"];

const SECTION_ID_PREFIX = "block-section-";

function relativeTime(iso: string | undefined | null): string {
  if (!iso) return "";
  const ms = Date.now() - new Date(iso).getTime();
  if (!Number.isFinite(ms) || ms < 0) return "just now";
  const mins = Math.round(ms / 60_000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.round(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.round(hours / 24)}d ago`;
}

/** Compact rendering of a dedup rule's windowHours for the collapsed-section summary. */
function formatDedupWindow(hours: number): string {
  if (hours < 1) return `${Math.round(hours * 60)}m`;
  const rounded = Math.round(hours * 10) / 10;
  return `${rounded}h`;
}

/**
 * Whether a block gets the Test section — the rule is stated once, here.
 *
 * No write is test-run, whatever the adapter. A probe against a write commits
 * real data and nothing here can take it back: jdbc commits rows, http POSTs to
 * the destination, kafka and kafka+connect publish. Reads and lookups keep
 * Test — that one bounded probe is what feeds the response explorer, the field
 * chips, the extraction shortcuts and pagination's Detect. kc has no probe
 * surface at all.
 */
function hostsTest(block: FlowBlock): boolean {
  switch (block.adapter) {
    case "http":
    case "jdbc":
      return block.mode !== "write"; // read · lookup
    case "kafka":
      return block.mode === "read";
    default:
      return false; // kafka_kc (governed terminal write), kc (no test surface)
  }
}

/**
 * The one line that stands where Test would have been, so its absence reads as
 * a decision rather than a missing section. kc never had a Test section to
 * begin with, so it gets no explanation either.
 */
function noTestReason(block: FlowBlock): string | null {
  if (hostsTest(block) || block.adapter === "kc") return null;
  if (block.adapter === "jdbc")
    return "Writes are not test-run — a test would commit rows. Field mapping is validated against the table's metadata instead.";
  if (block.adapter === "http")
    return "Writes are not test-run — a probe would POST real data to the destination. If this block forwards the parsed response, describe it by hand: there is no sampled response to explore, and pagination has to be set manually.";
  return "Nothing to sample: this block publishes, it never returns records.";
}

export interface BlockFormProps {
  flow: Flow;
  block: FlowBlock;
  locked: boolean;
  queueLocked?: boolean;
  services: AppService[];
  schemas: ApprovedSchema[];
  issues: { message: string }[];
  onPatchBlock: (blockId: string, patch: Partial<FlowBlock>) => void;
  onPatchConfig: (blockId: string, patch: Record<string, unknown>) => void;
  onDeleteBlock: (blockId: string) => void;
  /** Write a branch's name and/or condition (routes through the shared mutation). */
  onSetBranch: (blockId: string, patch: { name?: string; condition?: BranchCondition | null }) => void;
  onOpenCeremony: (blockId: string) => void;
  onSelectBlock: (blockId: string) => void;
  /** Test needs the draft persisted first — a flow that only exists in the
   *  builder has nothing server-side to probe. Resolves with the saved flow
   *  (a no-op save if nothing changed); rejects with the save's own error. */
  onEnsureSaved: () => Promise<Flow>;
}

export function BlockForm(props: BlockFormProps) {
  const {
    flow,
    block,
    locked: flowLocked,
    queueLocked = false,
    services,
    schemas,
    issues,
    onPatchBlock,
    onPatchConfig,
    onDeleteBlock,
    onOpenCeremony,
    onSelectBlock,
    onEnsureSaved,
  } = props;
  // kc's "Save is live" exception: kc blocks stay editable while deployed.
  const locked = flowLocked && (queueLocked || block.adapter !== "kc");
  const [deleteOpen, setDeleteOpen] = useState(false);

  const isWrite = block.mode === "write" || block.adapter === "kafka_kc";
  const isKafkaFamilyWrite = (block.adapter === "kafka" && block.mode === "write") || block.adapter === "kafka_kc";
  const hasSinkConfig = block.adapter === "kc" || block.adapter === "kafka_kc";
  const { data: kafkaConnectSyncs = [] } = useQuery({
    queryKey: ["kafkaConnectSyncs"],
    queryFn: listKafkaConnectSyncs,
    enabled: hasSinkConfig,
  });
  const queryClient = useQueryClient();
  const createSyncMutation = useMutation({
    mutationFn: async () => {
      // The link is validated against the flow as SAVED, not the draft in the
      // builder. Persisting first is what makes the entity, sink service and
      // connector class the user just picked actually visible to that check —
      // otherwise the backend rejects the sync citing fields the user can see
      // are filled in. Same reason the Test button does this.
      await onEnsureSaved();
      const sinkConfig = (block.config.sinkConfig ?? {}) as Record<string, string>;
      const topic = flowSinkTopic(flow, block);
      const saved = await saveKafkaConnectSync({
        name: `${flow.name} / ${block.name}`,
        description: `Created from the flow builder for ${block.name}.`,
        direction: "sink",
        connectorClass: String(sinkConfig["connector.class"] ?? ""),
        // Must match the name the flow compiler generates for this block, or the
        // record would point at a second connector instead of this flow's own sink.
        connectorName: `${tokenize(flow.name)}.${block.id}.${block.adapter}`,
        // `topics` is never persisted on the block (the platform derives it at
        // deploy), but the sync record must carry it or the link check rejects it.
        config: { ...sinkConfig, topics: topic ?? "" },
        linkedFlowId: flow.id,
        linkedBlockId: block.id,
      });
      await linkKafkaConnectSync(saved.id, flow.id, block.id); // backend writes syncId onto the block
      let adopted = true;
      try {
        await adoptKafkaConnectSync(saved.id);
      } catch {
        // No connector on the cluster yet — the sync stays a draft and goes
        // live at the flow's next deploy. Not an error.
        adopted = false;
      }
      return { saved, adopted };
    },
    onSuccess: ({ saved, adopted }) => {
      onPatchConfig(block.id, { syncId: saved.id });
      queryClient.invalidateQueries({ queryKey: ["kafkaConnectSyncs"] });
      toast.success(
        adopted
          ? `Managed sync created and linked to the live connector for "${block.name}".`
          : `Managed sync created for "${block.name}" — it will go live at the next deploy.`,
      );
    },
    onError: (error: Error) => toast.error(error.message),
  });
  const showTransforms = hostsTransforms(flow, block);
  const showTest = hostsTest(block);
  const testAbsence = noTestReason(block);
  const showBranches = !isTerminal(block);
  const approved = schemas.find((s) => s.flowId === flow.id && s.blockId === block.id);

  const serviceType =
    block.adapter === "http"
      ? "http"
      : block.adapter === "jdbc"
        ? "database"
        : block.adapter === "kafka_kc" || block.adapter === "kc"
          ? "sink_destination"
          : block.adapter === "kafka" && block.mode === "read"
            ? "external_kafka"
            : null;
  const eligibleServices = services.filter((s) => s.type === serviceType && !s.retired);
  const selectedService = services.find((s) => s.id === block.serviceId);

  const descendants = useMemo(() => {
    const ids = new Set<string>([block.id]);
    let changed = true;
    while (changed) {
      changed = false;
      for (const b of flow.blocks) {
        if (b.parentId && ids.has(b.parentId) && !ids.has(b.id)) {
          ids.add(b.id);
          changed = true;
        }
      }
      for (const t of flow.topics) {
        if (t.writerBlockId && ids.has(t.writerBlockId) && !ids.has(t.id)) {
          ids.add(t.id);
          changed = true;
        }
      }
    }
    ids.delete(block.id);
    return [...ids];
  }, [flow, block.id]);

  // ------------------------------------------------------------- derived names
  const derived = deriveTopicName(flow, block);
  // Re-typing the name the platform would have derived anyway is not a custom
  // name, so both the raw-namespace warning and the reserved-name collision
  // would be pure noise (the collision would fire on the flow's own topic).
  const overrideIsDerived = isKafkaFamilyWrite && overrideMatchesDerived(flow, block);
  const nameWarning = overrideIsDerived ? undefined : derived.warning;
  const collision =
    isKafkaFamilyWrite && block.topicOverride && !overrideIsDerived ? topicNameCollision(derived.value) : null;

  // ---------------------------------------------------------------- branching
  const branchCount = useMemo(() => branchesOf(flow, block.id).length, [flow, block.id]);
  // A terminal block can still hold route rules (drop/forward), but it has no
  // Branches section to jump to — so it gets no badge either.
  const branchBadge = showBranches && branchCount > 0 ? branchSummary(flow, block) : null;

  // Transforms shape records; they no longer name or route anything, so there is
  // nothing to keep in step here.
  const applyTransforms = (transforms: TransformRule[]) => onPatchBlock(block.id, { transforms });

  // ------------------------------------------------------------- open sections
  const forcedSections = useMemo(() => {
    const forced: string[] = [];
    if (issues.length > 0) forced.push("identity");
    if (isWrite && (nameWarning || collision)) forced.push("entity");
    if (block.adapter === "kafka_kc") forced.push("schema");
    return forced;
  }, [issues.length, isWrite, nameWarning, collision, block.adapter]);

  const defaultSections = useMemo(() => {
    const open = ["identity", "adapter"];
    if (showBranches && branchCount > 0) open.push("branches");
    if (isWrite) open.push("entity");
    if (block.adapter === "kafka_kc") open.push("schema");
    if (hasSinkConfig) open.push("sink");
    return open;
  }, [showBranches, branchCount, isWrite, block.adapter, hasSinkConfig]);

  const [openState, setOpenState] = useState<{ blockId: string; values: string[] } | null>(null);
  const base = openState?.blockId === block.id ? openState.values : defaultSections;
  const open = useMemo(() => [...new Set([...base, ...forcedSections])], [base, forcedSections]);
  const setOpen = (values: string[]) =>
    setOpenState({ blockId: block.id, values: [...new Set([...values, ...forcedSections])] });
  const goToSection = (id: string) => {
    setOpen([...open, id]);
    // The accordion animates open; scroll once the panel has height.
    window.setTimeout(() => {
      document.getElementById(`${SECTION_ID_PREFIX}${id}`)?.scrollIntoView({ behavior: "smooth", block: "start" });
    }, 120);
  };

  // ------------------------------------------------------------------ summaries
  const adapterSummary = (() => {
    const cfg = block.config;
    switch (block.adapter) {
      case "http":
        return `${(cfg.method as string) ?? "GET"} ${(cfg.path as string) || "— no path yet"}`;
      case "jdbc":
        return (cfg.table as string) ? `table ${cfg.table as string}` : "no table picked";
      case "kafka":
        if (block.mode === "read") {
          const adopted = flow.topics.find((t) => t.id === block.parentId);
          return adopted ? adopted.name : (cfg.topicName as string) || "no topic picked";
        }
        return "schemaless JSON onto the platform cluster";
      case "kafka_kc":
        return "governed Avro topic + managed sink";
      case "kc": {
        const attached = flow.topics.find((t) => t.id === cfg.attachTopicId);
        return attached ? `subscribes ${attached.name}` : "not attached to a topic";
      }
      default:
        return "";
    }
  })();

  const sinkSummary = (() => {
    const sink = (block.config.sinkConfig as Record<string, string> | undefined) ?? {};
    const cls = sink["connector.class"];
    const keys = Object.keys(sink).filter((k) => k !== "connector.class").length;
    if (!cls) return "not configured";
    return `${cls} · ${keys} key${keys === 1 ? "" : "s"}`;
  })();

  const testSummary = block.testResult
    ? block.testResult.ok
      ? `Tested ✓ ${relativeTime(block.testResult.testedAt)}`
      : `Test failed ${relativeTime(block.testResult.testedAt)}`
    : "Not tested";

  const transformSummary = (() => {
    const dedupRule = block.transforms.find((r) => r.kind === "dedup");
    const ruleCount = block.transforms.length - (dedupRule ? 1 : 0);
    const ruleLabel = ruleCount === 0 ? "none" : `${ruleCount} rule${ruleCount === 1 ? "" : "s"}`;
    if (!dedupRule) return ruleLabel;
    const windowHours = (dedupRule.config.windowHours as number) ?? 24;
    const dedupLabel = `dedup on (${formatDedupWindow(windowHours)})`;
    return ruleCount === 0 ? dedupLabel : `${ruleLabel} · ${dedupLabel}`;
  })();

  const entitySummary = block.entity
    ? isKafkaFamilyWrite
      ? `${block.entity} → ${derived.value}`
      : block.entity
    : "no entity label yet";

  return (
    <div className="space-y-4">
      {/* The block form is the one dominant surface on this screen (elevation 3);
          the outline is a flat rail and the map is a recessed canvas. */}
      <Card className="overflow-hidden shadow-md">
        {/* ------------------------------------------------------ block header */}
        <div className="flex flex-wrap items-center gap-2 border-b border-border/60 bg-muted/40 px-4 py-3">
          <AdapterChip adapter={block.adapter} mode={block.mode} />
          <span className="min-w-0 truncate text-sm font-semibold">{block.name || "Untitled block"}</span>
          {block.branch && (
            <Badge variant="outline">
              <GitBranch />
              {block.branch.name}
            </Badge>
          )}
          {branchBadge && (
            <button
              type="button"
              className="rounded-md border border-border/70 px-1.5 py-0.5 text-2xs font-medium text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
              title="Jump to Routing"
              onClick={() => goToSection("branches")}
            >
              {branchBadge}
            </button>
          )}
          {issues.length > 0 && (
            <Badge variant="destructive">
              <AlertCircle />
              {issues.length} issue{issues.length === 1 ? "" : "s"}
            </Badge>
          )}
          {/* Was a badge reading "Save is live — editable while deployed", which
              is a sentence, not a label. The label is "Save is live"; the
              sentence is what the tooltip is for. */}
          {block.adapter === "kc" && flowLocked && (
            <Tooltip>
              <TooltipTrigger asChild>
                <Badge variant="info" className="ml-auto cursor-help">
                  Save is live
                </Badge>
              </TooltipTrigger>
              <TooltipContent>
                kc blocks stay editable while the flow is deployed — saving updates the sink immediately.
              </TooltipContent>
            </Tooltip>
          )}
        </div>

        <GroupHeading label="Connection" hint="what this block is, and what it talks to" />
        <Accordion type="multiple" value={open} onValueChange={setOpen} className="px-4">
          {/* -------------------------------------------------------- identity */}
          <Section
            value="identity"
            title="Identity"
            icon={<IdCard className="h-4 w-4 text-muted-foreground" />}
            open={open.includes("identity")}
            forced={forcedSections.includes("identity")}
            forcedHint={forcedSections.includes("identity") ? "Kept open while this block has issues" : undefined}
            summary={block.name}
          >
            <FieldGroup>
              {issues.length > 0 && (
                <div className="space-y-1 rounded-lg bg-destructive-muted px-3 py-2.5 ring-1 ring-inset ring-destructive/15">
                  {issues.map((iss, i) => (
                    <FieldMessage key={i}>{iss.message}</FieldMessage>
                  ))}
                </div>
              )}

              <Field label="Block name">
                <Input
                  value={block.name}
                  disabled={locked}
                  className="max-w-sm"
                  onChange={(e) => onPatchBlock(block.id, { name: e.target.value })}
                />
              </Field>

              {block.branch && (
                <Field
                  label="Branch name"
                  info="Branch names pre-fill topic variant tokens in the naming walk."
                  hint={
                    <>
                      This branch receives <span className="font-medium text-foreground">{describeBranch(block.branch)}</span>
                      {isConditional(block.branch) ? " — edit its rules in Routing on the block above." : "."}
                    </>
                  }
                >
                  <Input
                    value={block.branch.name}
                    disabled={locked}
                    className="max-w-xs"
                    onChange={(e) => props.onSetBranch(block.id, { name: e.target.value })}
                  />
                </Field>
              )}

              {/* A sink's destination service is part of its sink configuration —
                  it is what fills the connector's connection properties — so it
                  is rendered there, not here. Every other adapter picks its
                  service as part of its identity. */}
              {serviceType && !hasSinkConfig && (
                <ServiceSelector
                  label={block.adapter === "kafka" && block.mode === "read" ? "Cluster" : "Service"}
                  noneLabel={block.adapter === "kafka" && block.mode === "read" ? "Platform cluster (default)" : undefined}
                  serviceType={serviceType}
                  services={eligibleServices}
                  selected={selectedService}
                  locked={locked}
                  blockId={block.id}
                  blockName={block.name}
                  onSelect={(id) => onPatchBlock(block.id, { serviceId: id })}
                />
              )}
            </FieldGroup>
          </Section>

          {/* ------------------------------------------------- adapter settings */}
          <Section
            value="adapter"
            title="Adapter settings"
            icon={<Sliders className="h-4 w-4 text-muted-foreground" />}
            open={open.includes("adapter")}
            summary={adapterSummary}
          >
            {block.adapter === "http" && (
              <HttpSettings block={block} locked={locked} service={selectedService} onPatchConfig={onPatchConfig} />
            )}
            {block.adapter === "jdbc" && (
              <JdbcSettings block={block} service={selectedService} locked={locked} onPatchConfig={onPatchConfig} />
            )}
            {block.adapter === "kafka" && block.mode === "read" && (
              <KafkaReadSettings flow={flow} block={block} locked={locked} onPatchConfig={onPatchConfig} />
            )}
            {block.adapter === "kafka" && block.mode === "write" && (
              <p className="text-sm leading-relaxed text-muted-foreground">
                Schemaless JSON bytes onto the platform cluster (R6 — write home only). The topic name is derived below.
              </p>
            )}
            {block.adapter === "kafka_kc" && (
              <p className="text-sm leading-relaxed text-muted-foreground">
                The structured write — one unit, always terminal: a governed Avro topic and a managed Connect sink created
                together. The only place Avro and schemas exist.
              </p>
            )}
            {block.adapter === "kc" && (
              <KcSettings flow={flow} block={block} locked={locked} onPatchBlock={onPatchBlock} onPatchConfig={onPatchConfig} />
            )}
          </Section>
        </Accordion>

        <GroupHeading label="Records" hint="what happens to each record on the way through" />
        <Accordion type="multiple" value={open} onValueChange={setOpen} className="px-4">
          {/* ---------------------------------------------------- transforms */}
          {block.adapter !== "kc" && (
            <Section
              value="transforms"
              title="Generic transformations"
              icon={<Fingerprint className="h-4 w-4 text-muted-foreground" />}
              open={open.includes("transforms")}
              summary={transformSummary}
              info="Applied in order, after the adapter's parsing. Dropped records are intentional outcomes — counted, never errors."
            >
              {showTransforms ? (
                <TransformsEditor
                  flow={flow}
                  block={block}
                  locked={locked}
                  onChange={applyTransforms}
                  onGoToBranches={showBranches ? () => goToSection("branches") : undefined}
                />
              ) : (
                <Alert variant="warning">
                  <Lock />
                  <AlertTitle>Quarantined (R8)</AlertTitle>
                  <AlertDescription>
                    This branch carries raw bytes. Transformations are quarantined; only byte-preserving delivery is legal.
                  </AlertDescription>
                </Alert>
              )}
            </Section>
          )}

          {/* ------------------------------------------------------------ test */}
          {showTest && (
            <Section
              value="test"
              title="Test"
              icon={<FlaskConical className="h-4 w-4 text-muted-foreground" />}
              open={open.includes("test")}
              summary={testSummary}
              info="Per block, never per flow — one bounded probe feeds the field pickers downstream."
            >
              <TestPanel
                flow={flow}
                block={block}
                locked={locked}
                service={selectedService}
                onEnsureSaved={onEnsureSaved}
                onTested={(result: BlockTestResult) => onPatchBlock(block.id, { testResult: result })}
                onAddExtraction={(field, path) => {
                  const attribute = field.replace(/^\[\d+\]$/, "item");
                  const rule: TransformRule = {
                    id: uid("t"),
                    kind: "extract",
                    config: { attribute, path: path ?? `$.${field}`, default: "" },
                  };
                  const dedupIdx = block.transforms.findIndex((t) => t.kind === "dedup");
                  const transforms =
                    dedupIdx === -1
                      ? [...block.transforms, rule]
                      : [...block.transforms.slice(0, dedupIdx), rule, ...block.transforms.slice(dedupIdx)];
                  onPatchBlock(block.id, { transforms });
                  toast.success(`Extraction rule added for "${attribute}" (${path ?? `$.${field}`})`);
                }}
                onSetRecordPath={
                  block.adapter === "http"
                    ? (path) => {
                        onPatchConfig(block.id, { recordPath: path });
                        toast.success(`Record path set to ${path}`);
                      }
                    : undefined
                }
              />
            </Section>
          )}

          {/* Test removed, not missing: one line in its place, on the same rhythm
              as a section header so the form does not appear to skip a beat. */}
          {testAbsence && (
            <div className="flex items-start gap-2.5 border-b border-border/60 py-3 text-xs leading-relaxed text-muted-foreground last:border-b-0">
              <FlaskConicalOff className="mt-0.5 h-4 w-4 shrink-0 text-muted-foreground/60" />
              <span>{testAbsence}</span>
            </div>
          )}
        </Accordion>

        <GroupHeading label="Destination" hint="where the records end up, and what follows" />
        <Accordion type="multiple" value={open} onValueChange={setOpen} className="px-4">
          {/* ------------------------------------------------ entity & names */}
          {isWrite && (
            <Section
              value="entity"
              title="Entity & derived names"
              icon={<Tags className="h-4 w-4 text-muted-foreground" />}
              open={open.includes("entity")}
              forced={forcedSections.includes("entity")}
              forcedHint={forcedSections.includes("entity") ? "Kept open while a name is in conflict" : undefined}
              summary={entitySummary}
            >
              <FieldGroup>
                <Field label="Entity label" info="No write without an entity, ever. One word for what the data is.">
                  <Input
                    value={block.entity ?? ""}
                    disabled={locked || (block.adapter === "kafka_kc" && !!approved)}
                    placeholder="asset · incident · order…"
                    className="max-w-xs"
                    title={
                      block.adapter === "kafka_kc" && approved
                        ? "Set in the schema ceremony — re-run the ceremony to change it"
                        : undefined
                    }
                    onChange={(e) => onPatchBlock(block.id, { entity: e.target.value })}
                  />
                </Field>

                {isKafkaFamilyWrite && (
                  <Field
                    label="Topic name"
                    info={
                      <>
                        Names are reserved before creation and freeze at deploy.
                        {block.adapter === "kafka_kc" &&
                          " A custom name here is also what the managed sink subscribes to — its topics= key follows this value."}
                      </>
                    }
                    aside={
                      <>
                        {derived.variant && (
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <Badge variant="outline" className="cursor-help">
                                {derived.variant}
                              </Badge>
                            </TooltipTrigger>
                            <TooltipContent>Variant token taken from the naming walk</TooltipContent>
                          </Tooltip>
                        )}
                        {derived.overridden && (
                          <Badge variant="outline">{overrideIsDerived ? "custom = derived" : "custom"}</Badge>
                        )}
                        {block.adapter === "kafka_kc" && <Badge variant="muted">governed · sealed</Badge>}
                      </>
                    }
                    hint={
                      <>
                        Resolves to <Mono>{derived.value || "raw.<flow>.<entity>"}</Mono>
                      </>
                    }
                    warning={nameWarning}
                    error={collision}
                  >
                    <Input
                      value={block.topicOverride ?? ""}
                      disabled={locked || (block.adapter === "kafka_kc" && !!approved)}
                      placeholder={derivedTopicDefault(flow, block).value}
                      className="max-w-lg font-mono text-xs"
                      title={
                        block.adapter === "kafka_kc" && approved
                          ? "The governed topic name is sealed by the approved schema's subject — re-run the ceremony to change it"
                          : undefined
                      }
                      onChange={(e) => onPatchBlock(block.id, { topicOverride: e.target.value || null })}
                    />
                  </Field>
                )}

                {block.adapter === "kafka_kc" && (
                  <FactRow label="Lakehouse table">
                    <Mono>{block.entity ? tableName(flow.name, block.entity) : "bronze.<flow>.<entity>__raw"}</Mono>
                  </FactRow>
                )}
              </FieldGroup>
            </Section>
          )}

          {/* ---------------------------------------------------------- schema */}
          {block.adapter === "kafka_kc" && (
            <Section
              value="schema"
              title="Schema"
              icon={<ShieldCheck className={cn("h-4 w-4", approved ? "text-success" : "text-warning")} />}
              open={open.includes("schema")}
              forced
              forcedHint="Always reachable — the ceremony's only entry point"
              summary={approved ? `Approved #${approved.registryGlobalId}` : "Ceremony required"}
              info="No evolution — schema changes always re-run the ceremony."
            >
              {approved ? (
                <div className="flex flex-wrap items-center gap-x-3 gap-y-2">
                  <StatusBadge status="Approved" />
                  <Mono>{approved.subject}</Mono>
                  <span className="text-xs text-muted-foreground">
                    #{approved.registryGlobalId} ·{" "}
                    {approved.provenance === "sample_run"
                      ? "live sample run"
                      : approved.provenance === "uploaded"
                        ? "uploaded samples"
                        : "manually authored — not sample-validated"}
                  </span>
                  <Button size="xs" variant="outline" className="ml-auto" disabled={locked} onClick={() => onOpenCeremony(block.id)}>
                    Re-run ceremony
                  </Button>
                </div>
              ) : (
                <div className="flex flex-wrap items-center gap-x-3 gap-y-2">
                  <StatusBadge status="Ceremony required" tone="soft" />
                  <span className="text-xs text-muted-foreground">
                    The flow cannot deploy until this write's schema is approved.
                  </span>
                  <Button size="xs" className="ml-auto" disabled={locked} onClick={() => onOpenCeremony(block.id)}>
                    Start ceremony
                  </Button>
                </div>
              )}
            </Section>
          )}

          {/* --------------------------------------------- sink configuration */}
          {hasSinkConfig && (
            <Section
              value="sink"
              title="Sink configuration"
              icon={<Settings2 className="h-4 w-4 text-muted-foreground" />}
              open={open.includes("sink")}
              summary={sinkSummary}
            >
              <div className="mb-4 rounded-md border border-primary/20 bg-primary-muted/20 p-3">
                <div className="mb-1 text-sm font-medium">Managed Kafka Connect sync</div>
                <p className="mb-2 text-xs text-muted-foreground">Create a reusable sync from this block's own sink settings.</p>
                {(() => {
                  const linkedSyncId = typeof block.config.syncId === "string" ? block.config.syncId : undefined;
                  const linkedSync = linkedSyncId ? kafkaConnectSyncs.find((s) => s.id === linkedSyncId) : undefined;
                  // Deleting a sync leaves its id behind on the block — the backend keeps the
                  // reference on purpose. So a truthy syncId does not mean a live sync exists,
                  // and guarding the create button on the raw id alone hides it forever after
                  // the first delete, stranding the user with no way to make a replacement.
                  const danglingSyncRef = Boolean(linkedSyncId) && !linkedSync;
                  return (
                    !linkedSync && (
                      <>
                        {danglingSyncRef && (
                          <p className="mt-1 flex items-start gap-1 text-xs text-warning">
                            <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                            <span>The sync this block pointed at no longer exists. Create a new one to manage this sink again.</span>
                          </p>
                        )}
                        {(() => {
                          const sinkConfig = (block.config.sinkConfig ?? {}) as Record<string, string>;
                          const connectorClass = String(sinkConfig["connector.class"] ?? "").trim();
                          const linkIssue = kafkaConnectLinkIssue(flow, block, {
                            direction: "sink",
                            connectorClass,
                            config: { ...sinkConfig, topics: flowSinkTopic(flow, block) ?? "" },
                          });
                          // kafkaConnectLinkIssue only catches a class MISMATCH (it compares
                          // the sync's class against itself here, so it never fires on an
                          // empty class) — this guard catches absence, which is what the
                          // backend actually 422s on.
                          const disabledReason = locked
                            ? "The flow is deployed and frozen."
                            : createSyncMutation.isPending
                              ? "Creating the sync…"
                              : !connectorClass
                                ? "Pick a connector class below before creating a sync."
                                : linkIssue;
                          return (
                            <Button
                              type="button"
                              variant="link"
                              size="sm"
                              className="mt-1 h-auto px-0 text-xs"
                              disabled={Boolean(disabledReason)}
                              title={disabledReason ?? undefined}
                              onClick={() => createSyncMutation.mutate()}
                            >
                              <Plus className="mr-1 h-3.5 w-3.5" /> Create managed sync
                            </Button>
                          );
                        })()}
                      </>
                    )
                  );
                })()}
              </div>
              {block.adapter === "kafka_kc" ? (
                // kafka_kc freezes with the rest of the flow at deploy.
                <SinkConfigEditor block={block} locked={locked} onPatchConfig={onPatchConfig} />
              ) : (
                // kc is lock-exempt: Save is live, so the sink stays editable
                // while the flow runs. The lock is decided HERE, per call site.
                <SinkConfigEditor block={block} locked={false} onPatchConfig={onPatchConfig} />
              )}
            </Section>
          )}

          {/* ---------------------------------------------------------- routing */}
          {showBranches && (
            <Section
              value="branches"
              title="Routing"
              icon={<Shuffle className="h-4 w-4 text-muted-foreground" />}
              open={open.includes("branches")}
              summary={branchBadge ?? "nothing follows yet"}
            >
              <BranchesCard
                flow={flow}
                block={block}
                locked={locked}
                onSetBranch={props.onSetBranch}
                onSelectBlock={onSelectBlock}
              />
            </Section>
          )}
        </Accordion>

        {/* Delete is one button on the footer rail, not a section of its own —
            the confirm dialog already lists exactly what goes with it, which is
            the only warning that was ever doing any work. */}
        <div className="flex flex-wrap items-center justify-between gap-2 border-t border-border/60 bg-muted/25 px-4 py-2.5">
          <p className="text-xs text-muted-foreground">
            {descendants.length > 0
              ? `Deleting takes ${descendants.length} downstream node${descendants.length === 1 ? "" : "s"} with it.`
              : "Nothing depends on this block."}
          </p>
          <Button
            variant="ghost"
            size="xs"
            className="text-destructive hover:bg-destructive/10 hover:text-destructive"
            disabled={locked}
            onClick={() => setDeleteOpen(true)}
          >
            <Trash2 /> Delete block
          </Button>
        </div>
      </Card>

      <AlertDialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete "{block.name}"?</AlertDialogTitle>
            <AlertDialogDescription>This removes the block and its entire subtree.</AlertDialogDescription>
          </AlertDialogHeader>
          <div className="rounded-lg bg-muted/60 p-3">
            <ul className="space-y-1">
              <li className="flex items-center gap-2 text-sm font-medium">
                <span className="h-1.5 w-1.5 shrink-0 rounded-full bg-destructive" />
                {block.name}
              </li>
              {descendants.map((id) => {
                const b = flow.blocks.find((x) => x.id === id);
                const t = flow.topics.find((x) => x.id === id);
                return (
                  <li key={id} className="flex items-center gap-2 pl-3.5 text-xs text-muted-foreground">
                    <span className="h-1 w-1 shrink-0 rounded-full bg-muted-foreground/50" />
                    {b?.name ?? t?.name ?? id}
                  </li>
                );
              })}
            </ul>
          </div>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              onClick={() => onDeleteBlock(block.id)}
            >
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

/**
 * A chapter heading inside the form.
 *
 * The sections used to run one after another with nothing to say where one
 * concern ended and the next began — nine collapsed rows read as a list, not as
 * a form. Three headings turn them into: where this block CONNECTS, what it
 * DOES to records, and where those records GO.
 */
function GroupHeading({ label, hint }: { label: string; hint: string }) {
  return (
    <div className="flex items-baseline gap-2 border-y border-border/60 bg-muted/40 px-4 py-1.5">
      <span className="text-2xs font-semibold uppercase tracking-wider text-muted-foreground">{label}</span>
      <span className="truncate text-2xs text-muted-foreground/70">{hint}</span>
    </div>
  );
}

// ------------------------------------------------------------ the disclosure

function Section({
  value,
  title,
  icon,
  summary,
  info,
  open,
  forced = false,
  forcedHint,
  children,
}: {
  value: string;
  title: string;
  icon: React.ReactNode;
  summary?: string;
  /** Rule text for the section as a whole — goes behind a ⓘ, not into the body. */
  info?: React.ReactNode;
  open: boolean;
  /** Force-open: the trigger is inert and the chevron hidden. */
  forced?: boolean;
  forcedHint?: string;
  children: React.ReactNode;
}) {
  return (
    <AccordionItem value={value} id={`${SECTION_ID_PREFIX}${value}`} disabled={forced} className="border-border/60 last:border-b-0">
      <AccordionTrigger className={cn("py-3", forced && "cursor-default [&>svg]:hidden")}>
        <span className="flex min-w-0 flex-1 items-center gap-2 pr-3 text-left">
          {icon}
          <span className="shrink-0 text-sm font-semibold">{title}</span>
          {/* The forced-open hints used to be badges reading "issues must stay
              visible" / "always reachable" — sentences wearing a badge's
              clothes. A pin icon says the same thing in 16px. */}
          {forced && forcedHint && (
            <Tooltip>
              <TooltipTrigger asChild>
                <span className="shrink-0 text-muted-foreground/60">
                  <Pin className="h-3.5 w-3.5" />
                </span>
              </TooltipTrigger>
              <TooltipContent>{forcedHint}</TooltipContent>
            </Tooltip>
          )}
          {!open && summary && (
            <span className="ml-auto truncate text-xs font-normal text-muted-foreground">{summary}</span>
          )}
        </span>
      </AccordionTrigger>
      <AccordionContent className="pb-5 pt-0">
        {/* Section-level rule text stays inline rather than going behind a ⓘ:
            there is at most one per section and it only renders while the
            section is open, so it costs one line a few times — unlike the
            per-field prose, which cost a line on every field permanently. */}
        {info && (
          <p className="mb-3 flex items-start gap-1.5 text-xs leading-relaxed text-muted-foreground">
            <Info className="mt-0.5 h-3.5 w-3.5 shrink-0 text-muted-foreground/60" />
            <span>{info}</span>
          </p>
        )}
        {children}
      </AccordionContent>
    </AccordionItem>
  );
}

// ------------------------------------------------------------ sub-sections

const PLATFORM_CLUSTER = "__platform__";

/**
 * The two explicit ways a block gets a service: pick one already configured
 * for the org, or configure one right here. Both end up writing the exact
 * same thing — `serviceId` on the block — so switching between them is a
 * display choice, never a data migration. "Set up here" still creates (or
 * updates) an ordinary Application Service flagged `private: true`; that is
 * the one storage path for secrets, just presented as first-class manual
 * configuration instead of a dialog bolted onto the Select.
 */
type ServiceMode = "existing" | "manual";

function ServiceSelector({
  label,
  noneLabel,
  serviceType,
  services,
  selected,
  locked,
  blockId,
  blockName,
  onSelect,
}: {
  label: string;
  /** When set, a "no service" choice with this label is offered (maps to null). */
  noneLabel?: string;
  serviceType: string;
  services: AppService[];
  selected: AppService | undefined;
  locked: boolean;
  /** Identifies the owning block — local mode/draft state is keyed by it so
   *  switching the selected block (props change, no remount) cannot leak one
   *  block's in-progress draft into another's picker. */
  blockId: string;
  /** Seeds the generated name for a fresh "Set up here" draft. */
  blockName: string;
  onSelect: (id: string | null) => void;
}) {
  const queryClient = useQueryClient();
  const type = serviceType as AppService["type"];

  // Private services are owned by exactly one block and configured inline —
  // they never appear in the general catalogue picker.
  const catalogServices = services.filter((s) => !s.private);

  // Default: "Existing service" unless the block already points at a private
  // service, in which case that service's own manual configuration is what
  // the user came here to see.
  const defaultMode: ServiceMode = selected?.private ? "manual" : "existing";
  const [modeState, setModeState] = useState<{ key: string; mode: ServiceMode } | null>(null);
  const mode = modeState?.key === blockId ? modeState.mode : defaultMode;
  const setMode = (next: ServiceMode) => setModeState({ key: blockId, mode: next });

  // The same form the Application Services page uses — credentials included.
  // Sending someone to another page to type a password they already have in
  // front of them was the whole complaint; nothing about WHERE the secret ends
  // up changed, only where it can be typed.
  const draftDefault = (): ServiceForm =>
    selected?.private ? formFromService(selected) : { ...emptyForm(), name: `${blockName || "Block"} (manual)` };
  const [draftState, setDraftState] = useState<{ key: string; form: ServiceForm } | null>(null);
  const draft = draftState?.key === blockId ? draftState.form : draftDefault();
  const setDraft = (next: ServiceForm) => setDraftState({ key: blockId, form: next });

  // Editing the block's own private service updates it in place (same id —
  // saveService bumps the revision); anything else, including a fresh draft
  // or a non-private service still bound from "Existing service", creates a
  // brand new private record rather than overwriting someone else's service.
  const editingOwn = selected?.private ? selected : null;

  const saveMutation = useMutation({
    mutationFn: async () =>
      saveService({
        ...(editingOwn ?? {
          id: "",
          type,
          revision: 1,
          retired: false,
          health: "Not Tested" as const,
          lastTestedAt: null,
          createdAt: "",
          updatedAt: "",
          hasSecret: false,
        }),
        type,
        private: true,
        name: draft.name.trim(),
        config: buildConfig(type, draft),
        hasSecret: secretTyped(type, draft) || (editingOwn?.hasSecret ?? false),
      }),
    onSuccess: (svc) => {
      queryClient.invalidateQueries({ queryKey: ["services"] });
      onSelect(svc.id);
      setDraftState({ key: blockId, form: formFromService(svc) });
      toast.success(
        `Private service "${svc.name}" saved${svc.hasSecret ? " with its credentials" : ""} — it lives on the service, never on the block`,
      );
    },
    onError: (error: Error) => toast.error(error.message),
  });

  const saveReason = saveBlockReason(type, draft);

  return (
    <Field
      label={label}
      info="Hosts and credentials always come from a saved service — never typed into a block. Setting one up here stores it as a private service owned by this block; the secret still lives server-side, on the service."
      aside={
        <ToggleGroup
          type="single"
          value={mode}
          onValueChange={(v) => v && setMode(v as ServiceMode)}
          aria-label={`${label} source`}
        >
          <ToggleGroupItem value="existing" size="sm" disabled={locked}>
            Existing
          </ToggleGroupItem>
          <ToggleGroupItem value="manual" size="sm" disabled={locked}>
            Set up here
          </ToggleGroupItem>
        </ToggleGroup>
      }
    >
      {mode === "existing" ? (
        <div className="space-y-1.5">
          <Select
            value={selected?.id ?? (noneLabel ? PLATFORM_CLUSTER : "")}
            disabled={locked}
            onValueChange={(v) => onSelect(v === PLATFORM_CLUSTER ? null : v)}
          >
            <SelectTrigger className="max-w-sm">
              <SelectValue placeholder="Select a service…" />
            </SelectTrigger>
            <SelectContent>
              {noneLabel && <SelectItem value={PLATFORM_CLUSTER}>{noneLabel}</SelectItem>}
              {catalogServices.map((s) => (
                <SelectItem key={s.id} value={s.id}>
                  {s.name} · rev {s.revision}
                </SelectItem>
              ))}
              {selected?.retired && (
                <SelectItem value={selected.id} disabled>
                  {selected.name} · retired — action required
                </SelectItem>
              )}
              {selected?.private && !selected.retired && (
                <SelectItem value={selected.id} disabled>
                  {selected.name} · private — bound via "Set up here"
                </SelectItem>
              )}
            </SelectContent>
          </Select>
          {/* Was one run-on sentence: "rev 3 · Healthy · credentials stored on
              the service · private — switch to Set up here to edit it". Three
              separate facts do better as three chips than as one line. */}
          {selected && (
            <div className="flex flex-wrap items-center gap-1.5">
              <Badge variant="outline">rev {selected.revision}</Badge>
              <StatusBadge status={selected.health} />
              {selected.hasSecret && <Badge variant="outline">credentials stored</Badge>}
              {selected.private && (
                <Tooltip>
                  <TooltipTrigger asChild>
                    <Badge variant="info" className="cursor-help">
                      private
                    </Badge>
                  </TooltipTrigger>
                  <TooltipContent>Owned by this block — switch to "Set up here" to edit it.</TooltipContent>
                </Tooltip>
              )}
            </div>
          )}
        </div>
      ) : (
        <div className="space-y-3 rounded-xl bg-muted/40 p-3.5 ring-1 ring-inset ring-border/60">
          <ServiceFormFields type={type} form={draft} onChange={setDraft} editing={!!selected?.private} />
          {selected?.private && (
            <div className="flex flex-wrap items-center gap-1.5">
              <Badge variant="outline">rev {selected.revision}</Badge>
              <StatusBadge status={selected.health} />
              {selected.hasSecret && <Badge variant="outline">credentials stored</Badge>}
            </div>
          )}
          <div className="flex justify-end">
            <Button
              size="sm"
              disabled={locked || !!saveReason || saveMutation.isPending}
              title={saveReason ?? undefined}
              onClick={() => saveMutation.mutate()}
            >
              {saveMutation.isPending && <Loader2 className="animate-spin" />}
              Save configuration
            </Button>
          </div>
        </div>
      )}
    </Field>
  );
}

function HttpSettings({
  block,
  locked,
  service,
  onPatchConfig,
}: {
  block: FlowBlock;
  locked: boolean;
  service: AppService | undefined;
  onPatchConfig: (blockId: string, patch: Record<string, unknown>) => void;
}) {
  const cfg = block.config;
  const pagination = (cfg.pagination as { type?: string; fields?: Record<string, string> }) ?? { type: "none", fields: {} };

  const openapiSpecId = (cfg.openapiSpecId as string | undefined) || undefined;
  const openapiOperationId = (cfg.openapiOperationId as string | undefined) || undefined;
  // Drives the "N documented parameters" hint below the path field. Reads
  // from the two persisted config keys rather than any transient selection
  // state, so the hint survives a block switch or a page reload.
  const operationDetailQuery = useQuery({
    queryKey: ["openapi-operation-detail", openapiSpecId, openapiOperationId],
    queryFn: () => getOpenApiOperationDetail(openapiSpecId!, openapiOperationId!),
    enabled: !!openapiSpecId && !!openapiOperationId,
    staleTime: 60_000,
  });
  const documentedParamCount = operationDetailQuery.data?.parameters.length ?? 0;

  const headers = (cfg.headers as KvRow[]) ?? [];
  const query = (cfg.query as KvRow[]) ?? [];

  // The base URL always comes from the bound service (existing or a saved
  // "Set up here" private one) — never typed into the block. Path is only
  // ever what's appended to it, and this is what the field's context line
  // and the resolved preview key off.
  const baseUrl = typeof service?.config?.baseUrl === "string" ? service.config.baseUrl : undefined;
  const pathValue = (cfg.path as string) ?? "";
  const pathHasScheme = /^https?:\/\//i.test(pathValue);
  const pathStartsWithBase = !!baseUrl && pathValue.startsWith(baseUrl);
  // A full URL that doesn't match the bound service's base gets a destructive
  // hint instead of a silent auto-strip — stripping a base we can't identify
  // as the right one would guess at the user's intent.
  const showFullUrlHint = pathHasScheme && !pathStartsWithBase;

  /**
   * Shared by both the OpenAPI combobox and the plain path Input: if what
   * came in starts with http(s):// AND matches the bound service's base URL,
   * the base got typed/pasted where only the path belongs (the reported
   * confusion — "aren't we already giving the url in the application
   * services?"). Auto-strip it rather than reject it; anything else is left
   * alone and flagged inline (and by validateBlock's httpPathIssue) instead.
   */
  const resolvePathInput = (raw: string): string => {
    if (baseUrl && /^https?:\/\//i.test(raw) && raw.startsWith(baseUrl)) {
      let stripped = raw.slice(baseUrl.length);
      if (!stripped.startsWith("/")) stripped = `/${stripped}`;
      toast.success("Base URL comes from the service — kept just the path.");
      return stripped;
    }
    return raw;
  };

  const advancedSummary = [
    headers.length > 0 ? `${headers.length} header${headers.length === 1 ? "" : "s"}` : null,
    query.length > 0 ? `${query.length} query param${query.length === 1 ? "" : "s"}` : null,
    pagination.type && pagination.type !== "none" ? `${pagination.type} pagination` : null,
    service?.config?.proxyId ? "via gateway proxy" : null,
    block.mode === "write" && (cfg.bodyTemplate as string) ? "body template" : null,
  ]
    .filter(Boolean)
    .join(" · ");

  return (
    <FieldGroup>
      <OpenApiPanel block={block} locked={locked} onPatchConfig={onPatchConfig} />

      {/* Method and path are one request line, so they sit on one row. */}
      <div className="flex flex-wrap items-start gap-3">
        <Field label="Method" className="w-28 shrink-0">
          <Select
            value={(cfg.method as string) ?? METHODS_FOR_MODE(block.mode)[0]}
            disabled={locked}
            onValueChange={(v) => onPatchConfig(block.id, { method: v })}
          >
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {METHODS_FOR_MODE(block.mode).map((m) => (
                <SelectItem key={m} value={m}>
                  {m}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </Field>

        <Field
          label="Path"
          className="min-w-[16rem] flex-1"
          // Answers the reported confusion directly — "aren't we already giving
          // the url in the application services?" — without spending two
          // permanent lines on it.
          info={
            baseUrl ? (
              <>
                Appended to the base URL from service "{service?.name}". Enter only the path — pasting a full URL that
                matches the base will have the base stripped automatically.
              </>
            ) : (
              <>Select a service (or set one up) in Identity — its base URL prefixes this path.</>
            )
          }
          hint={
            baseUrl ? (
              <span className="font-mono">
                → {baseUrl}
                {pathValue}
              </span>
            ) : undefined
          }
          error={showFullUrlHint ? "Enter only the path — the base URL comes from the selected service." : undefined}
        >
          {openapiSpecId ? (
            <OpenApiPathCombobox
              specId={openapiSpecId}
              locked={locked}
              value={(cfg.path as string) ?? ""}
              placeholder="/users"
              onChange={(path) => {
                // Free typing is always allowed — it clears the operation
                // binding so a hand-edited path is never silently misreported
                // as "from the doc" (flexibility rule).
                const resolved = resolvePathInput(path);
                const patch: Record<string, unknown> = { path: resolved };
                if (cfg.openapiOperationId) patch.openapiOperationId = undefined;
                onPatchConfig(block.id, patch);
              }}
              onSelectOperation={(op) => {
                onPatchConfig(block.id, { path: op.path, method: op.method, openapiOperationId: op.operationId });
                toast.success(`Applied ${op.method} ${op.path}`);
              }}
            />
          ) : (
            <Input
              className="font-mono text-xs"
              value={(cfg.path as string) ?? ""}
              disabled={locked}
              placeholder="/users"
              onChange={(e) => onPatchConfig(block.id, { path: resolvePathInput(e.target.value) })}
            />
          )}
        </Field>
      </div>

      {openapiOperationId && documentedParamCount > 0 && (
        <p className="text-xs text-muted-foreground">
          {documentedParamCount} documented parameter{documentedParamCount === 1 ? "" : "s"} — set values under Advanced.
        </p>
      )}

      {block.mode === "lookup" && (
        <Field label="Join field">
          <Input
            className="max-w-xs font-mono text-xs"
            value={(cfg.lookupJoinField as string) ?? ""}
            disabled={locked}
            placeholder="field joining the lookup result onto each record"
            onChange={(e) => onPatchConfig(block.id, { lookupJoinField: e.target.value })}
          />
        </Field>
      )}

      <Field
        label="Response parsing"
        info={(cfg.responseFormat as string) === "csv" ? "CSV encoding is fixed UTF-8." : undefined}
      >
        <div className="flex flex-wrap items-center gap-2">
          <Select value={(cfg.responseFormat as string) ?? "json"} disabled={locked} onValueChange={(v) => onPatchConfig(block.id, { responseFormat: v })}>
            <SelectTrigger className="w-28">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {["json", "xml", "csv", "text"].map((f) => (
                <SelectItem key={f} value={f}>
                  {f.toUpperCase()}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Input
            className="w-56 font-mono text-xs"
            value={(cfg.recordPath as string) ?? ""}
            disabled={locked}
            placeholder="$.resources[*] (record path)"
            onChange={(e) => onPatchConfig(block.id, { recordPath: e.target.value })}
          />
          <label className="flex items-center gap-2 text-xs text-muted-foreground">
            <Switch checked={cfg.split !== false} disabled={locked} onCheckedChange={(c) => onPatchConfig(block.id, { split: c })} />
            split into records
          </label>
        </div>
      </Field>

      {/* Everything below is optional: it stays folded away until asked for. */}
      <Accordion key={block.id} type="single" collapsible defaultValue={advancedSummary ? "advanced" : undefined} className="rounded-xl bg-muted/35 px-3 ring-1 ring-inset ring-border/60">
        <AccordionItem value="advanced" className="border-b-0">
          <AccordionTrigger className="py-2.5">
            <span className="flex min-w-0 flex-1 items-center gap-2 pr-3 text-left">
              <Settings2 className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
              <span className="shrink-0 text-xs font-semibold">Advanced</span>
              <span className="ml-auto truncate text-xs font-normal text-muted-foreground">
                {advancedSummary || "headers · query · body · pagination · gateway proxy"}
              </span>
            </span>
          </AccordionTrigger>
          <AccordionContent className="space-y-4 pb-4 pt-0">
            <KvRows
              label="Headers"
              rows={headers}
              locked={locked}
              keyPlaceholder="Header-Name"
              valuePlaceholder="value (supports ${…})"
              addLabel="Add header"
              onChange={(rows) => onPatchConfig(block.id, { headers: rows })}
            />
            <KvRows
              label="Query parameters"
              rows={query}
              locked={locked}
              keyPlaceholder="param"
              valuePlaceholder="value (supports ${…})"
              addLabel="Add query param"
              onChange={(rows) => onPatchConfig(block.id, { query: rows })}
            />

            {block.mode === "write" && (
              <>
                <Field label="Body template">
                  <Textarea
                    className="font-mono text-xs"
                    rows={3}
                    value={(cfg.bodyTemplate as string) ?? ""}
                    disabled={locked}
                    placeholder='{"records": ${records}}'
                    onChange={(e) => onPatchConfig(block.id, { bodyTemplate: e.target.value })}
                  />
                </Field>
                <Field label="Chain continues with" info="R3 — what the next block receives from this write.">
                  <Select
                    value={(cfg.writeForwards as string) ?? "original"}
                    disabled={locked}
                    onValueChange={(v) => onPatchConfig(block.id, { writeForwards: v })}
                  >
                    <SelectTrigger className="max-w-xs">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="original">Original records</SelectItem>
                      <SelectItem value="response">Parsed response</SelectItem>
                    </SelectContent>
                  </Select>
                </Field>
              </>
            )}

            <PaginationFields block={block} locked={locked} onPatchConfig={onPatchConfig} />
          </AccordionContent>
        </AccordionItem>
      </Accordion>
    </FieldGroup>
  );
}

function JdbcSettings({
  block,
  service,
  locked,
  onPatchConfig,
}: {
  block: FlowBlock;
  service?: AppService;
  locked: boolean;
  onPatchConfig: (blockId: string, patch: Record<string, unknown>) => void;
}) {
  const cfg = block.config;
  const columns = (cfg.columns as string[]) ?? [];
  const MOCK_TABLES = ["cmdb_assets", "vulnerability_findings", "user_directory", "network_zones"];
  const isTrino = service?.config.dialect === "trino";
  return (
    <FieldGroup>
      <Field label="Table" info={isTrino ? "Use a fully qualified catalog.schema.table name." : "No custom SQL — everything is generated from picked tables and columns."}>
        {isTrino ? (
          <>
            <Input
              className="font-mono text-xs"
              value={(cfg.table as string) ?? ""}
              disabled={locked}
              placeholder="gold.cmdb.asset_groups"
              onChange={(e) => onPatchConfig(block.id, { table: e.target.value })}
            />
            <p className="text-xs text-muted-foreground">
              Trino tables must use <span className="font-mono">catalog.schema.table</span>. The catalog and schema select the lakehouse; each row is emitted as one record.
            </p>
          </>
        ) : (
          <Select value={(cfg.table as string) ?? ""} disabled={locked} onValueChange={(v) => onPatchConfig(block.id, { table: v })}>
            <SelectTrigger className="max-w-xs">
              <SelectValue placeholder="Pick a table from the service's catalog" />
            </SelectTrigger>
            <SelectContent>
              {MOCK_TABLES.map((t) => (
                <SelectItem key={t} value={t}>
                  {t}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        )}
      </Field>

      <Field label="Columns">
        <Input
          className="font-mono text-xs"
          value={columns.join(", ")}
          disabled={locked}
          placeholder="asset_id, hostname, updated_at"
          onChange={(e) => onPatchConfig(block.id, { columns: e.target.value.split(",").map((s) => s.trim()).filter(Boolean) })}
        />
      </Field>

      {block.mode === "lookup" && (
        <Field label="Join field">
          <Input
            className="max-w-xs font-mono text-xs"
            value={(cfg.lookupJoinField as string) ?? ""}
            disabled={locked}
            placeholder="field joining the lookup result onto each record"
            onChange={(e) => onPatchConfig(block.id, { lookupJoinField: e.target.value })}
          />
        </Field>
      )}

      {block.mode === "read" && (
        <div className="space-y-3 rounded-xl bg-muted/35 p-3.5 ring-1 ring-inset ring-border/60">
          <label className="flex items-center gap-2.5 text-sm font-medium">
            <Switch checked={cfg.incremental === true} disabled={locked} onCheckedChange={(c) => onPatchConfig(block.id, { incremental: c })} />
            Incremental reads
            <InfoDot title="Incremental reads">
              Watermark + bookmark. Bookmarks live in Redis — if Redis is down, incremental runs fail rather than lose
              their place.
            </InfoDot>
          </label>
          {cfg.incremental === true && (
            <div className="flex flex-wrap items-center gap-2 pl-[3.25rem]">
              <Input
                className="h-8 w-44 font-mono text-xs"
                value={(cfg.watermarkColumn as string) ?? ""}
                disabled={locked}
                placeholder="watermark column"
                onChange={(e) => onPatchConfig(block.id, { watermarkColumn: e.target.value })}
              />
              <Select
                value={(cfg.initialPosition as string) ?? "oldest"}
                disabled={locked}
                onValueChange={(v) => onPatchConfig(block.id, { initialPosition: v })}
              >
                <SelectTrigger className="h-8 w-56 text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="oldest">First run: from the oldest row</SelectItem>
                  <SelectItem value="new">First run: only new rows</SelectItem>
                </SelectContent>
              </Select>
            </div>
          )}
        </div>
      )}

      {block.mode === "write" && (
        <FactRow label="Write mode">
          History-driven — the record's <Mono>change_type</Mono> maps to INSERT / UPDATE / DELETE.
        </FactRow>
      )}
    </FieldGroup>
  );
}

function KafkaReadSettings({
  flow,
  block,
  locked,
  onPatchConfig,
}: {
  flow: Flow;
  block: FlowBlock;
  locked: boolean;
  onPatchConfig: (blockId: string, patch: Record<string, unknown>) => void;
}) {
  const cfg = block.config;
  const adoptedParent = flow.topics.find((t) => t.id === block.parentId);
  const firstStarted = !!flow.deployedAt;
  return (
    <FieldGroup>
      {adoptedParent ? (
        <FactRow label="Consuming">
          <Mono>{adoptedParent.name}</Mono>
          <span className="ml-2 text-muted-foreground">
            adopted · never renamed
            {typeof adoptedParent.backlogEstimate === "number"
              ? ` · ~${adoptedParent.backlogEstimate.toLocaleString()} messages`
              : ""}
          </span>
        </FactRow>
      ) : (
        <Field label="Topic">
          <Input
            className="max-w-sm font-mono text-xs"
            value={(cfg.topicName as string) ?? ""}
            disabled={locked}
            placeholder="topic to consume"
            onChange={(e) => onPatchConfig(block.id, { topicName: e.target.value })}
          />
        </Field>
      )}

      <Field label="Parse as">
        <Select value={(cfg.parseFormat as string) ?? "json"} disabled={locked} onValueChange={(v) => onPatchConfig(block.id, { parseFormat: v })}>
          <SelectTrigger className="max-w-xs">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="json">JSON</SelectItem>
            <SelectItem value="csv">CSV</SelectItem>
            <SelectItem value="xml">XML</SelectItem>
            <SelectItem value="raw">Raw bytes (quarantined — R8)</SelectItem>
          </SelectContent>
        </Select>
      </Field>

      {(cfg.parseFormat as string) === "raw" && (
        <Alert variant="warning">
          <Lock />
          <AlertTitle>Raw bytes are quarantined (R8)</AlertTitle>
          <AlertDescription>
            No transformations on this branch, and no governed (kafka+connect) write can follow — only byte-preserving
            delivery is legal.
          </AlertDescription>
        </Alert>
      )}

      <Field
        label="Initial position"
        info="Continuous consumer — kafka reads are never scheduled (R1)."
        hint={firstStarted ? "Immutable after the first start." : undefined}
      >
        <Select
          value={(cfg.initialPosition as string) ?? "beginning"}
          disabled={locked || firstStarted}
          onValueChange={(v) => onPatchConfig(block.id, { initialPosition: v })}
        >
          <SelectTrigger
            className="max-w-xs"
            title={firstStarted ? "Immutable after the first start — use the audited offset-skip in ops instead" : undefined}
          >
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="beginning">From the beginning (default)</SelectItem>
            <SelectItem value="new">Only new messages</SelectItem>
          </SelectContent>
        </Select>
      </Field>
    </FieldGroup>
  );
}

function KcSettings({
  flow,
  block,
  locked,
  onPatchBlock,
  onPatchConfig,
}: {
  flow: Flow;
  block: FlowBlock;
  locked: boolean;
  onPatchBlock: (blockId: string, patch: Partial<FlowBlock>) => void;
  onPatchConfig: (blockId: string, patch: Record<string, unknown>) => void;
}) {
  const cfg = block.config;
  const attachable = flow.topics.filter((t) => !t.sealed);
  const attached = flow.topics.find((t) => t.id === cfg.attachTopicId);
  return (
    <FieldGroup>
      <Field
        label="Subscribed topic"
        info="R5 — a sink subscription attaches to topics only."
        hint={
          attached && typeof attached.backlogEstimate === "number"
            ? `Currently holds ~${attached.backlogEstimate.toLocaleString()} messages.`
            : undefined
        }
      >
        <Select value={(cfg.attachTopicId as string) ?? ""} disabled={locked} onValueChange={(v) => onPatchConfig(block.id, { attachTopicId: v })}>
          <SelectTrigger className="max-w-sm">
            <SelectValue placeholder="Attach to a topic" />
          </SelectTrigger>
          <SelectContent>
            {attachable.map((t) => (
              <SelectItem key={t.id} value={t.id}>
                {t.name}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </Field>

      {/*
        kc delivers records, so the spec's "no write without an entity, ever"
        applies to it — but the shared Entity card is gated on isWrite(), which
        correctly excludes kc. Widening that predicate would leak kc into
        service-type mapping, transform hosting and legality, so the field is
        targeted here instead.
      */}
      <Field
        label="Entity label"
        info="No write without an entity, ever — a subscription delivers records, so it needs one too. It names the destination index or table; the topic name it reads is not renamed by it."
      >
        <Input
          value={block.entity ?? ""}
          disabled={locked}
          placeholder="asset · incident · order…"
          className="max-w-xs"
          onChange={(e) => onPatchBlock(block.id, { entity: e.target.value })}
        />
      </Field>

      <Field label="Initial position">
        <Select
          value={(cfg.initialPosition as string) ?? "beginning"}
          disabled={locked}
          onValueChange={(v) => onPatchConfig(block.id, { initialPosition: v })}
        >
          <SelectTrigger className="max-w-xs">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="beginning">From the oldest message (default)</SelectItem>
            <SelectItem value="new">Only new messages</SelectItem>
          </SelectContent>
        </Select>
      </Field>

      <Alert variant="info">
        <AlertTitle>Save is live</AlertTitle>
        <AlertDescription>
          Subscription-only sink over the topic's bytes — untouched, no transforms, no schema surface. Saving creates or
          updates the sink independently of flow deploys, and it never blocks the chain.
        </AlertDescription>
      </Alert>
    </FieldGroup>
  );
}
