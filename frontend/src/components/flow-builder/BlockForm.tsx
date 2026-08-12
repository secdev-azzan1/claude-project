// The per-block form — the primary configuration surface of the builder, and
// the single dominant surface on the screen (elevation role: shadow-md).
//
// It is ONE accordion, not a stack of seven always-open cards. The verdict on
// the previous build was "everything seems congested", so the governing rule
// here is that net visible surface goes DOWN even as capability goes up:
// sections carry a one-line summary when closed and only open when they have
// something to say.
//
// Three sections are force-open because collapsing them would hide a deploy
// blocker that appears nowhere else: Identity while the block has validation
// issues (the issue list lives in its header), Entity while a derived-name
// warning or a topic-name collision exists, and Schema always — it is the
// ceremony's only entry point and the target of the `?ceremony=<blockId>`
// deep link, which expects the affordance to be clickable straight away.
//
// Open state is derived from block state and keyed by block id rather than
// persisted per block: the selected block changes on every add, and stale
// per-block open state makes sections appear to jump.

import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from "@/components/ui/accordion";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
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
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { AdapterChip } from "@/components/AdapterChip";
import { StatusBadge } from "@/components/StatusBadge";
import { TransformsEditor } from "./TransformsEditor";
import { TestPanel } from "./TestPanel";
import { KvRows, type KvRow } from "./KvRows";
import { blockProxyId } from "@/prototype/validation";
import { branchesOf, branchSummary, describeBranch, isConditional } from "@/prototype/branches";
import { PaginationFields } from "./PaginationFields";
import { SinkConfigEditor } from "./SinkConfigEditor";
import {
  ServiceFormFields,
  buildConfig,
  emptyForm,
  saveBlockReason,
  secretTyped,
  type ServiceForm,
} from "@/components/service-form/ServiceFormFields";
import {
  BranchesCard,
} from "./BranchesCard";
import { hostsTransforms, isTerminal } from "@/prototype/legality";
import {
  deriveTopicName,
  derivedTopicDefault,
  overrideMatchesDerived,
  tableName,
  topicNameCollision,
} from "@/prototype/naming";
import { listGatewayProxies, saveService } from "@/prototype/api";
import { CONNECT_PLUGIN_CATALOG } from "@/prototype/seeds";
import { uid } from "@/prototype/store";
import { cn } from "@/lib/utils";
import type { AddMenuEntry } from "@/prototype/legality";
import type {
  AppService,
  ApprovedSchema,
  BlockMode,
  BranchCondition,
  BlockTestResult,
  Flow,
  FlowBlock,
  GatewayProxy,
  TransformRule,
} from "@/prototype/types";
import {
  AlertCircle,
  ExternalLink,
  Fingerprint,
  FlaskConical,
  FlaskConicalOff,
  GitBranch,
  IdCard,
  Loader2,
  Lock,
  Plus,
  Settings2,
  ShieldAlert,
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

/** Canned OpenAPI operations per seeded HTTP service — the picker's data. */
const OPENAPI_OPERATIONS: Record<string, { label: string; method: string; path: string; recordPath?: string }[]> = {
  "svc-rapid7": [
    { label: "GET /api/3/assets — Search assets", method: "GET", path: "/api/3/assets", recordPath: "$.resources[*]" },
    { label: "GET /api/3/scans — List scans", method: "GET", path: "/api/3/scans", recordPath: "$.resources[*]" },
    { label: "GET /api/3/vulnerabilities — List vulnerabilities", method: "GET", path: "/api/3/vulnerabilities", recordPath: "$.resources[*]" },
  ],
  "svc-fortisiem": [
    { label: "GET /phoenix/rest/incident/list — List incidents", method: "GET", path: "/phoenix/rest/incident/list", recordPath: "$.incidents[*]" },
    { label: "GET /phoenix/rest/device/list — List devices", method: "GET", path: "/phoenix/rest/device/list", recordPath: "$.devices[*]" },
  ],
  "svc-servicenow": [
    { label: "GET /api/now/table/cmdb_ci — CMDB items", method: "GET", path: "/api/now/table/cmdb_ci", recordPath: "$.result[*]" },
    { label: "GET /api/now/table/cmdb_ci_retired — Retired items", method: "GET", path: "/api/now/table/cmdb_ci_retired", recordPath: "$.result[*]" },
    { label: "POST /api/now/table/incident — Create incident", method: "POST", path: "/api/now/table/incident" },
  ],
};

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

/** The host a block actually calls, read off its bound http service. */
function hostOf(baseUrl: unknown): string | null {
  if (typeof baseUrl !== "string" || !baseUrl.trim()) return null;
  try {
    return new URL(baseUrl).hostname;
  } catch {
    return null;
  }
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
}

export function BlockForm(props: BlockFormProps) {
  const {
    flow,
    block,
    locked: flowLocked,
    services,
    schemas,
    issues,
    onPatchBlock,
    onPatchConfig,
    onDeleteBlock,
    onOpenCeremony,
    onSelectBlock,
  } = props;
  // kc's "Save is live" exception: kc blocks stay editable while deployed.
  const locked = flowLocked && block.adapter !== "kc";
  const [deleteOpen, setDeleteOpen] = useState(false);

  const isWrite = block.mode === "write" || block.adapter === "kafka_kc";
  const isKafkaFamilyWrite = (block.adapter === "kafka" && block.mode === "write") || block.adapter === "kafka_kc";
  const hasSinkConfig = block.adapter === "kc" || block.adapter === "kafka_kc";
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

  /**
   * The sink's destination service, rendered inside Sink configuration. It is
   * the same control every other adapter gets in Identity — passed down rather
   * than duplicated, so there is still exactly one picker writing `serviceId`.
   */
  const sinkServicePicker = hasSinkConfig && serviceType ? (
    <ServiceSelector
      label="Destination service"
      serviceType={serviceType}
      services={eligibleServices}
      selected={selectedService}
      locked={block.adapter === "kc" ? false : locked}
      onSelect={(id) => {
        onPatchBlock(block.id, { serviceId: id });
        // kafka_kc keeps a copy in config for the connector payload; writing
        // both here is what stops the two from disagreeing.
        if (block.adapter === "kafka_kc") onPatchConfig(block.id, { sinkServiceId: id });
      }}
    />
  ) : null;

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
    const plugin = CONNECT_PLUGIN_CATALOG.find((p) => p.connectorClass === cls);
    return `${plugin?.displayName ?? cls} · ${keys} key${keys === 1 ? "" : "s"}`;
  })();

  const testSummary = block.testResult
    ? block.testResult.ok
      ? `Tested ✓ ${relativeTime(block.testResult.testedAt)}`
      : `Test failed ${relativeTime(block.testResult.testedAt)}`
    : "Not tested";

  const transformSummary =
    block.transforms.length === 0
      ? "none"
      : `${block.transforms.length} rule${block.transforms.length === 1 ? "" : "s"}`;

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
        <div className="flex flex-wrap items-center gap-2 border-b bg-muted/30 px-4 py-3">
          <AdapterChip adapter={block.adapter} mode={block.mode} />
          <span className="truncate text-sm font-semibold">{block.name || "Untitled block"}</span>
          {block.branch && (
            <Badge variant="outline" className="gap-1 text-xs">
              <GitBranch className="h-3 w-3" />
branch: {block.branch.name}
            </Badge>
          )}
          {branchBadge && (
            <button
              type="button"
              className="rounded-full border bg-background px-2 py-0.5 text-xs text-muted-foreground transition-colors hover:bg-muted"
              title="Jump to Routing"
              onClick={() => goToSection("branches")}
            >
              {branchBadge}
            </button>
          )}
          {issues.length > 0 && (
            <Badge variant="outline" className="gap-1 border-destructive/40 text-xs text-destructive">
              <AlertCircle className="h-3 w-3" />
              {issues.length} issue{issues.length === 1 ? "" : "s"}
            </Badge>
          )}
          {block.adapter === "kc" && flowLocked && (
            <Badge variant="outline" className="ml-auto text-xs">
              Save is live — editable while deployed
            </Badge>
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
            forcedHint={forcedSections.includes("identity") ? "issues must stay visible" : undefined}
            summary={block.name}
          >
            <div className="space-y-3">
              {issues.length > 0 && (
                <div className="space-y-0.5 rounded-md border border-destructive/30 bg-destructive-muted/40 px-3 py-2">
                  {issues.map((iss, i) => (
                    <p key={i} className="flex items-start gap-1 text-xs text-destructive">
                      <AlertCircle className="mt-0.5 h-3 w-3 shrink-0" /> {iss.message}
                    </p>
                  ))}
                </div>
              )}

              <div className="grid gap-1.5">
                <Label>Block name</Label>
                <Input
                  value={block.name}
                  disabled={locked}
                  className="max-w-sm"
                  onChange={(e) => onPatchBlock(block.id, { name: e.target.value })}
                />
              </div>

              {block.branch && (
                <div className="grid gap-1.5">
                  <Label>Branch name</Label>
                  <Input
                    value={block.branch.name}
                    disabled={locked}
                    className="max-w-xs"
                    onChange={(e) => props.onSetBranch(block.id, { name: e.target.value })}
                  />
                  <p className="text-xs text-muted-foreground">
                    Branch names pre-fill topic variant tokens in the naming walk. This branch receives{" "}
                    <span className="font-medium text-foreground">{describeBranch(block.branch)}</span>
                    {isConditional(block.branch) ? " — edit its rules in Routing on the block above." : "."}
                  </p>
                </div>
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
                  onSelect={(id) => onPatchBlock(block.id, { serviceId: id })}
                />
              )}
            </div>
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
              <HttpSettings
                block={block}
                locked={locked}
                service={selectedService}
                onPatchConfig={onPatchConfig}
              />
            )}
            {block.adapter === "jdbc" && <JdbcSettings block={block} locked={locked} onPatchConfig={onPatchConfig} />}
            {block.adapter === "kafka" && block.mode === "read" && (
              <KafkaReadSettings flow={flow} block={block} locked={locked} onPatchConfig={onPatchConfig} />
            )}
            {block.adapter === "kafka" && block.mode === "write" && (
              <p className="text-sm text-muted-foreground">
                Schemaless JSON bytes onto the platform cluster (R6 — write home only). The topic name is derived below.
              </p>
            )}
            {block.adapter === "kafka_kc" && (
              <p className="text-sm text-muted-foreground">
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
            >
              {showTransforms ? (
                <div className="space-y-2">
                  <p className="text-xs text-muted-foreground">
                    The shared section every hosting adapter gets from base — identical everywhere.
                  </p>
                  <TransformsEditor
                    flow={flow}
                    block={block}
                    locked={locked}
                    onChange={applyTransforms}
                    onGoToBranches={showBranches ? () => goToSection("branches") : undefined}
                  />
                </div>
              ) : (
                <Alert>
                  <Lock className="h-4 w-4" />
                  <AlertTitle className="text-sm">Quarantined (R8)</AlertTitle>
                  <AlertDescription className="text-xs">
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
            >
              <div className="space-y-2">
                <p className="text-xs text-muted-foreground">
                  Per block, never per flow — one bounded probe feeds the field pickers downstream.
                </p>
                <TestPanel
                  flow={flow}
                  block={block}
                  locked={locked}
                  service={selectedService}
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
              </div>
            </Section>
          )}

          {/* Test removed, not missing: one line in its place, on the same rhythm
              as a section header so the form does not appear to skip a beat. */}
          {testAbsence && (
            <div className="flex items-start gap-2 border-b py-3 text-xs text-muted-foreground last:border-b-0">
              <FlaskConicalOff className="mt-0.5 h-4 w-4 shrink-0" />
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
              forcedHint={forcedSections.includes("entity") ? "name problem must stay visible" : undefined}
              summary={entitySummary}
            >
              <div className="space-y-3">
                <div className="grid gap-1.5">
                  <Label>Entity label</Label>
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
                  <p className="text-xs text-muted-foreground">No write without an entity, ever. One word for what the data is.</p>
                </div>

                {isKafkaFamilyWrite && (
                  <div className="grid gap-1.5">
                    <Label>Topic name</Label>
                    <div className="flex flex-wrap items-center gap-2">
                      <code className="rounded bg-muted px-2 py-1 font-mono text-xs">
                        {derived.value || "raw.<flow>.<entity>"}
                      </code>
                      {derived.variant && (
                        <Badge variant="outline" className="text-xs">
                          variant "{derived.variant}" from the naming walk
                        </Badge>
                      )}
                      {derived.overridden && (
                        <Badge variant="outline" className="text-xs">
                          {overrideIsDerived ? "custom name — same as derived" : "custom name"}
                        </Badge>
                      )}
                      {block.adapter === "kafka_kc" && (
                        <Badge variant="outline" className="text-xs">
                          governed · sealed
                        </Badge>
                      )}
                    </div>
                    <Input
                      value={block.topicOverride ?? ""}
                      disabled={locked || (block.adapter === "kafka_kc" && !!approved)}
                      placeholder={`Custom topic name (optional — R7). Default: ${derivedTopicDefault(flow, block).value}`}
                      className="max-w-lg font-mono text-xs"
                      title={
                        block.adapter === "kafka_kc" && approved
                          ? "The governed topic name is sealed by the approved schema's subject — re-run the ceremony to change it"
                          : undefined
                      }
                      onChange={(e) => onPatchBlock(block.id, { topicOverride: e.target.value || null })}
                    />
                    {nameWarning && (
                      <p className="flex items-center gap-1 text-xs text-warning">
                        <AlertCircle className="h-3.5 w-3.5" /> {nameWarning}
                      </p>
                    )}
                    {collision && (
                      <p className="flex items-center gap-1 text-xs text-destructive">
                        <AlertCircle className="h-3.5 w-3.5" /> {collision}
                      </p>
                    )}
                    <p className="text-xs text-muted-foreground">
                      Names are reserved before creation and freeze at deploy.
                      {block.adapter === "kafka_kc" &&
                        " A custom name here is also what the managed sink subscribes to — its topics= key follows this value."}
                    </p>
                  </div>
                )}

                {block.adapter === "kafka_kc" && (
                  <p className="text-xs text-muted-foreground">
                    Lakehouse table{" "}
                    <code className="font-mono">
                      {block.entity ? tableName(flow.name, block.entity) : "bronze.<flow>.<entity>__raw"}
                    </code>{" "}
                    — table and DLQ names stay derived.
                  </p>
                )}
              </div>
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
              forcedHint="always reachable"
              summary={approved ? `Approved #${approved.registryGlobalId}` : "Ceremony required"}
            >
              <div className="space-y-2">
                {approved ? (
                  <div className="flex flex-wrap items-center gap-2 text-sm">
                    <StatusBadge status="Approved" />
                    <code className="font-mono text-xs">{approved.subject}</code>
                    <span className="text-xs text-muted-foreground">
                      global id #{approved.registryGlobalId} ·{" "}
                      {approved.provenance === "sample_run"
                        ? "live sample run"
                        : approved.provenance === "uploaded"
                          ? "uploaded samples"
                          : "manually authored — not sample-validated"}
                    </span>
                    <Button size="sm" variant="outline" className="ml-auto h-7 text-xs" disabled={locked} onClick={() => onOpenCeremony(block.id)}>
                      Re-run ceremony
                    </Button>
                  </div>
                ) : (
                  <div className="flex flex-wrap items-center gap-2">
                    <StatusBadge status="Ceremony required" />
                    <span className="text-xs text-muted-foreground">
                      The flow cannot deploy until this write's schema is approved.
                    </span>
                    <Button size="sm" className="ml-auto h-7 text-xs" disabled={locked} onClick={() => onOpenCeremony(block.id)}>
                      Start ceremony
                    </Button>
                  </div>
                )}
                <p className="text-xs text-muted-foreground">No evolution — schema changes always re-run the ceremony.</p>
              </div>
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
              {block.adapter === "kafka_kc" ? (
                // kafka_kc freezes with the rest of the flow at deploy.
                <SinkConfigEditor
                  flow={flow}
                  block={block}
                  locked={locked}
                  services={services}
                  onPatchConfig={onPatchConfig}
                  onGoToNames={() => goToSection("entity")}
                  servicePicker={sinkServicePicker}
                />
              ) : (
                // kc is lock-exempt: Save is live, so the sink stays editable
                // while the flow runs. The lock is decided HERE, per call site.
                <SinkConfigEditor
                  flow={flow}
                  block={block}
                  locked={false}
                  services={services}
                  onPatchConfig={onPatchConfig}
                  servicePicker={sinkServicePicker}
                />
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
        <div className="flex flex-wrap items-center justify-between gap-2 border-t px-4 py-2.5">
          <p className="text-xs text-muted-foreground">
            {descendants.length > 0
              ? `Deleting takes ${descendants.length} downstream node(s) with it.`
              : "Nothing depends on this block."}
          </p>
          <Button
            variant="ghost"
            size="sm"
            className="h-8 gap-1.5 text-xs text-destructive hover:bg-destructive/10 hover:text-destructive"
            disabled={locked}
            onClick={() => setDeleteOpen(true)}
          >
            <Trash2 className="h-3.5 w-3.5" /> Delete block
          </Button>
        </div>
      </Card>

      <AlertDialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete "{block.name}"?</AlertDialogTitle>
            <AlertDialogDescription asChild>
              <div className="space-y-1 text-sm">
                <p>This removes the block and its entire subtree:</p>
                <ul className="list-inside list-disc text-xs">
                  <li>{block.name}</li>
                  {descendants.map((id) => {
                    const b = flow.blocks.find((x) => x.id === id);
                    const t = flow.topics.find((x) => x.id === id);
                    return <li key={id}>{b?.name ?? t?.name ?? id}</li>;
                  })}
                </ul>
              </div>
            </AlertDialogDescription>
          </AlertDialogHeader>
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
    <div className="flex items-baseline gap-2 border-b bg-muted/40 px-4 py-1.5">
      <span className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">{label}</span>
      <span className="truncate text-xs text-muted-foreground/80">{hint}</span>
    </div>
  );
}

// ------------------------------------------------------------ the disclosure

function Section({
  value,
  title,
  icon,
  summary,
  open,
  forced = false,
  forcedHint,
  children,
}: {
  value: string;
  title: string;
  icon: React.ReactNode;
  summary?: string;
  open: boolean;
  /** Force-open: the trigger is inert and the chevron hidden. */
  forced?: boolean;
  forcedHint?: string;
  children: React.ReactNode;
}) {
  return (
    <AccordionItem value={value} id={`${SECTION_ID_PREFIX}${value}`} disabled={forced} className="border-b last:border-b-0">
      <AccordionTrigger className={cn("py-3 hover:no-underline", forced && "cursor-default [&>svg]:hidden")}>
        <span className="flex min-w-0 flex-1 items-center gap-2 pr-3 text-left">
          {icon}
          <span className="shrink-0 text-sm font-semibold">{title}</span>
          {forced && forcedHint && (
            <Badge variant="outline" className="shrink-0 text-xs font-normal text-muted-foreground">
              {forcedHint}
            </Badge>
          )}
          {!open && summary && (
            <span className="ml-auto truncate text-xs font-normal text-muted-foreground">{summary}</span>
          )}
        </span>
      </AccordionTrigger>
      <AccordionContent className="pb-4 pt-0">{children}</AccordionContent>
    </AccordionItem>
  );
}

// ------------------------------------------------------------ sub-sections

const PLATFORM_CLUSTER = "__platform__";
const NO_PROXY = "__none__";

function ServiceSelector({
  label,
  noneLabel,
  serviceType,
  services,
  selected,
  locked,
  onSelect,
}: {
  label: string;
  /** When set, a "no service" choice with this label is offered (maps to null). */
  noneLabel?: string;
  serviceType: string;
  services: AppService[];
  selected: AppService | undefined;
  locked: boolean;
  onSelect: (id: string | null) => void;
}) {
  const [createOpen, setCreateOpen] = useState(false);
  // The same form the Application Services page uses — credentials included.
  // Sending someone to another page to type a password they already have in
  // front of them was the whole complaint; nothing about WHERE the secret ends
  // up changed, only where it can be typed.
  const [form, setForm] = useState<ServiceForm>(emptyForm());
  const queryClient = useQueryClient();
  const type = serviceType as AppService["type"];

  const createMutation = useMutation({
    mutationFn: async () =>
      saveService({
        id: "",
        type,
        name: form.name.trim(),
        revision: 1,
        retired: false,
        private: true,
        health: "Not Tested",
        lastTestedAt: null,
        config: buildConfig(type, form),
        hasSecret: secretTyped(type, form),
        createdAt: "",
        updatedAt: "",
      }),
    onSuccess: (svc) => {
      queryClient.invalidateQueries({ queryKey: ["services"] });
      onSelect(svc.id);
      setCreateOpen(false);
      setForm(emptyForm());
      toast.success(
        `Private service "${svc.name}" created${svc.hasSecret ? " with its credentials" : ""} — it lives on the service, never on the block`,
      );
    },
    onError: (error: Error) => toast.error(error.message),
  });

  const createBlockReason = saveBlockReason(type, form);

  return (
    <div className="grid gap-1.5">
      <Label>{label}</Label>
      <div className="flex items-center gap-2">
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
            {services.map((s) => (
              <SelectItem key={s.id} value={s.id}>
                {s.name}
                {s.private ? " (private)" : ""} · rev {s.revision}
              </SelectItem>
            ))}
            {selected?.retired && (
              <SelectItem value={selected.id} disabled>
                {selected.name} · retired — action required
              </SelectItem>
            )}
          </SelectContent>
        </Select>
        <Button variant="outline" size="sm" className="h-8 gap-1 text-xs" disabled={locked} onClick={() => setCreateOpen(true)}>
          <Plus className="h-3 w-3" /> private service
        </Button>
      </div>
      {selected && (
        <p className="text-xs text-muted-foreground">
          rev {selected.revision} · {selected.health}
          {selected.hasSecret ? " · credentials stored on the service" : ""}
        </p>
      )}
      <p className="text-xs text-muted-foreground">Hosts and credentials always come from a saved service — never typed into a block.</p>

      <Dialog open={createOpen} onOpenChange={setCreateOpen}>
        <DialogContent className="max-h-[85vh] max-w-lg overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Create a private service</DialogTitle>
            <DialogDescription>
              A private service is a full Application Service — endpoint, auth and secret — that simply is not offered in
              the shared catalogue. Fill it in here; you can promote or edit it later in Application Services.
            </DialogDescription>
          </DialogHeader>
          <ServiceFormFields type={type} form={form} onChange={setForm} editing={false} />
          <DialogFooter>
            <Button variant="outline" onClick={() => setCreateOpen(false)}>
              Cancel
            </Button>
            <Button
              onClick={() => createMutation.mutate()}
              disabled={!!createBlockReason || createMutation.isPending}
              title={createBlockReason ?? undefined}
            >
              {createMutation.isPending && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
              Create & select
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}

/**
 * The egress this block will actually use — READ-ONLY, because the proxy is a
 * property of the host and the host belongs to the service.
 *
 * It used to be a picker here, which let two blocks calling the same API
 * disagree about how to reach it. The choice moved onto the http service; this
 * states the consequence where the request is configured, and points at the one
 * place that can change it.
 */
function EgressLine({ block, service }: { block: FlowBlock; service: AppService | undefined }) {
  const { data: proxies = [] } = useQuery({ queryKey: ["gatewayProxies"], queryFn: listGatewayProxies });
  const configured = blockProxyId(block, service ? [service] : []);
  const proxy = proxies.find((p) => p.id === configured);
  const endpointHost = hostOf(service?.config?.baseUrl);
  const dangling = !!configured && proxies.length > 0 && !proxy;
  const legacy = !!block.config.proxyId || block.config.proxy === true;

  return (
    <div className="space-y-1.5 rounded-md border px-3 py-2.5">
      <Label className="text-xs">Egress</Label>
      {proxy ? (
        <div className="flex flex-wrap items-center gap-2">
          <StatusBadge status={proxy.status} />
          <span className="text-xs">
            via <span className="font-medium">{proxy.name}</span>
          </span>
          <code className="font-mono text-xs text-muted-foreground">
            {proxy.targetHost}:{proxy.port}
            {proxy.path}
          </code>
          {proxy.certProfileId && (
            <Badge variant="outline" className="text-xs">
              client certificate
            </Badge>
          )}
        </div>
      ) : dangling ? (
        <p className="flex items-start gap-1 text-xs text-destructive">
          <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
          The proxy configured for this service ({configured}) no longer exists — pick another on the service, or
          re-create it on the APISIX Gateway page.
        </p>
      ) : (
        <p className="text-xs text-muted-foreground">
          Direct to {endpointHost ?? "the service host"} — no gateway proxy.
        </p>
      )}
      {proxy && proxy.status !== "Reconciled" && (
        <p className="flex items-start gap-1 text-xs text-warning">
          <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
          {proxy.statusDetail ?? `The proxy is ${proxy.status}.`} It must reconcile onto the gateway before this flow can
          deploy.
        </p>
      )}
      {legacy && !service?.config?.proxyId && (
        <p className="flex items-start gap-1 text-xs text-warning">
          <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
          This block carries a proxy setting from before egress moved onto the service. It is still honoured, but set it
          on the service so every block calling that host agrees.
        </p>
      )}
      <p className="text-xs leading-4 text-muted-foreground">
        Chosen on the service, so every block calling this host routes the same way.{" "}
        {service ? (
          <Link to="/application-services" className="underline underline-offset-2">
            Edit “{service.name}”
          </Link>
        ) : (
          "Select a service first."
        )}
      </p>
    </div>
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

  const openApiOps = block.serviceId ? OPENAPI_OPERATIONS[block.serviceId] ?? [] : [];
  const headers = (cfg.headers as KvRow[]) ?? [];
  const query = (cfg.query as KvRow[]) ?? [];

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
    <div className="space-y-3">
      {openApiOps.length > 0 && (
        <div className="grid gap-1.5">
          <Label>OpenAPI operation</Label>
          <Select
            value=""
            disabled={locked}
            onValueChange={(v) => {
              const op = openApiOps.find((o) => o.label === v);
              if (op) {
                onPatchConfig(block.id, { method: op.method, path: op.path, ...(op.recordPath ? { recordPath: op.recordPath } : {}) });
                toast.success(`Applied ${op.method} ${op.path}${op.recordPath ? ` · record path ${op.recordPath}` : ""}`);
              }
            }}
          >
            <SelectTrigger className="max-w-md">
              <SelectValue placeholder="Pick from the service's imported spec (fills method, path, record path)…" />
            </SelectTrigger>
            <SelectContent>
              {openApiOps.map((op) => (
                <SelectItem key={op.label} value={op.label}>
                  {op.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <p className="text-xs text-muted-foreground">OpenAPI 3.0/3.1 JSON only — imported on the HTTP service.</p>
        </div>
      )}

      <div className="flex flex-wrap items-end gap-2">
        <div className="grid gap-1.5">
          <Label>Method</Label>
          <Select
            value={(cfg.method as string) ?? METHODS_FOR_MODE(block.mode)[0]}
            disabled={locked}
            onValueChange={(v) => onPatchConfig(block.id, { method: v })}
          >
            <SelectTrigger className="w-28">
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
        </div>
        <div className="grid min-w-[16rem] flex-1 gap-1.5">
          <Label>Path</Label>
          <Input
            className="font-mono text-xs"
            value={(cfg.path as string) ?? ""}
            disabled={locked}
            placeholder="/api/v3/assets or /scans/${scan_id}"
            onChange={(e) => onPatchConfig(block.id, { path: e.target.value })}
          />
        </div>
      </div>

      {block.mode === "lookup" && (
        <div className="grid gap-1.5">
          <Label>Join field</Label>
          <Input
            className="max-w-xs font-mono text-xs"
            value={(cfg.lookupJoinField as string) ?? ""}
            disabled={locked}
            placeholder="field joining the lookup result onto each record"
            onChange={(e) => onPatchConfig(block.id, { lookupJoinField: e.target.value })}
          />
        </div>
      )}

      <div className="grid gap-1.5">
        <Label>Response parsing</Label>
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
          <label className="flex items-center gap-1.5 text-xs text-muted-foreground">
            <Switch checked={cfg.split !== false} disabled={locked} onCheckedChange={(c) => onPatchConfig(block.id, { split: c })} />
            split into records
          </label>
        </div>
        {(cfg.responseFormat as string) === "csv" && (
          <p className="text-xs text-muted-foreground">CSV encoding is fixed UTF-8 — and the form says so.</p>
        )}
      </div>

      {/* Everything below is optional: it stays folded away until asked for. */}
      <Accordion key={block.id} type="single" collapsible defaultValue={advancedSummary ? "advanced" : undefined} className="rounded-md border">
        <AccordionItem value="advanced" className="border-b-0">
          <AccordionTrigger className="px-3 py-2 hover:no-underline">
            <span className="flex min-w-0 flex-1 items-center gap-2 pr-3 text-left">
              <Settings2 className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
              <span className="shrink-0 text-xs font-semibold">Advanced</span>
              <span className="ml-auto truncate text-xs font-normal text-muted-foreground">
                {advancedSummary || "headers · query · body · pagination · gateway proxy"}
              </span>
            </span>
          </AccordionTrigger>
          <AccordionContent className="space-y-3 px-3 pb-3 pt-0">
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
                <div className="grid gap-1.5">
                  <Label>Body template</Label>
                  <Textarea
                    className="font-mono text-xs"
                    rows={3}
                    value={(cfg.bodyTemplate as string) ?? ""}
                    disabled={locked}
                    placeholder='{"records": ${records}}'
                    onChange={(e) => onPatchConfig(block.id, { bodyTemplate: e.target.value })}
                  />
                </div>
                <div className="grid gap-1.5">
                  <Label>Chain continues with (R3)</Label>
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
                </div>
              </>
            )}

            <PaginationFields block={block} locked={locked} onPatchConfig={onPatchConfig} />

            <EgressLine block={block} service={service} />
          </AccordionContent>
        </AccordionItem>
      </Accordion>
    </div>
  );
}

function JdbcSettings({
  block,
  locked,
  onPatchConfig,
}: {
  block: FlowBlock;
  locked: boolean;
  onPatchConfig: (blockId: string, patch: Record<string, unknown>) => void;
}) {
  const cfg = block.config;
  const columns = (cfg.columns as string[]) ?? [];
  const MOCK_TABLES = ["cmdb_assets", "vulnerability_findings", "user_directory", "network_zones"];
  return (
    <div className="space-y-3">
      <div className="grid gap-1.5">
        <Label>Table</Label>
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
        <p className="text-xs text-muted-foreground">No custom SQL — everything is generated from picked tables and columns.</p>
      </div>
      <div className="grid gap-1.5">
        <Label>Columns</Label>
        <Input
          className="font-mono text-xs"
          value={columns.join(", ")}
          disabled={locked}
          placeholder="asset_id, hostname, updated_at"
          onChange={(e) => onPatchConfig(block.id, { columns: e.target.value.split(",").map((s) => s.trim()).filter(Boolean) })}
        />
      </div>
      {block.mode === "read" && (
        <div className="space-y-2 rounded-md border px-3 py-2">
          <label className="flex items-center gap-2 text-sm">
            <Switch checked={cfg.incremental === true} disabled={locked} onCheckedChange={(c) => onPatchConfig(block.id, { incremental: c })} />
            Incremental reads (watermark + bookmark)
          </label>
          {cfg.incremental === true && (
            <div className="flex flex-wrap items-center gap-2">
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
              <p className="w-full text-xs text-muted-foreground">
                Bookmarks live in Redis — if Redis is down, incremental runs fail rather than lose their place.
              </p>
            </div>
          )}
        </div>
      )}
      {block.mode === "write" && (
        <p className="rounded-md border px-3 py-2 text-xs text-muted-foreground">
          History-driven writes: the record's <code className="font-mono">change_type</code> maps to INSERT / UPDATE / DELETE.
        </p>
      )}
    </div>
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
    <div className="space-y-3">
      {adoptedParent ? (
        <p className="text-sm text-muted-foreground">
          Consuming the adopted topic <code className="font-mono text-xs">{adoptedParent.name}</code> — sampled · never renamed
          {typeof adoptedParent.backlogEstimate === "number" ? ` · ~${adoptedParent.backlogEstimate.toLocaleString()} messages` : ""}.
        </p>
      ) : (
        <div className="grid gap-1.5">
          <Label>Topic</Label>
          <Input
            className="max-w-sm font-mono text-xs"
            value={(cfg.topicName as string) ?? ""}
            disabled={locked}
            placeholder="topic to consume"
            onChange={(e) => onPatchConfig(block.id, { topicName: e.target.value })}
          />
        </div>
      )}
      <div className="grid gap-1.5">
        <Label>Parse as</Label>
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
        {(cfg.parseFormat as string) === "raw" && (
          <Alert>
            <Lock className="h-4 w-4" />
            <AlertTitle className="text-sm">Raw bytes are quarantined (R8)</AlertTitle>
            <AlertDescription className="text-xs">
              No transformations on this branch, and no governed (kafka+connect) write can follow — only byte-preserving
              delivery is legal.
            </AlertDescription>
          </Alert>
        )}
      </div>
      <div className="grid gap-1.5">
        <Label>Initial position</Label>
        <Select
          value={(cfg.initialPosition as string) ?? "beginning"}
          disabled={locked || firstStarted}
          onValueChange={(v) => onPatchConfig(block.id, { initialPosition: v })}
        >
          <SelectTrigger className="max-w-xs" title={firstStarted ? "Immutable after the first start — use the audited offset-skip in ops instead" : undefined}>
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="beginning">From the beginning (default)</SelectItem>
            <SelectItem value="new">Only new messages</SelectItem>
          </SelectContent>
        </Select>
        {firstStarted && <p className="text-xs text-muted-foreground">Immutable after first start.</p>}
      </div>
      <p className="text-xs text-muted-foreground">Continuous consumer — kafka reads are never scheduled (R1).</p>
    </div>
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
    <div className="space-y-3">
      <div className="grid gap-1.5">
        <Label>Subscribed topic</Label>
        <Select value={(cfg.attachTopicId as string) ?? ""} disabled={locked} onValueChange={(v) => onPatchConfig(block.id, { attachTopicId: v })}>
          <SelectTrigger className="max-w-sm">
            <SelectValue placeholder="Attach to a topic (R5 — topics only)" />
          </SelectTrigger>
          <SelectContent>
            {attachable.map((t) => (
              <SelectItem key={t.id} value={t.id}>
                {t.name}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        {attached && typeof attached.backlogEstimate === "number" && (
          <p className="text-xs text-muted-foreground">This topic currently holds ~{attached.backlogEstimate.toLocaleString()} messages.</p>
        )}
      </div>

      {/*
        kc delivers records, so the spec's "no write without an entity, ever"
        applies to it — but the shared Entity card is gated on isWrite(), which
        correctly excludes kc. Widening that predicate would leak kc into
        service-type mapping, transform hosting and legality, so the field is
        targeted here instead.
      */}
      <div className="grid gap-1.5">
        <Label>Entity label</Label>
        <Input
          value={block.entity ?? ""}
          disabled={locked}
          placeholder="asset · incident · order…"
          className="max-w-xs"
          onChange={(e) => onPatchBlock(block.id, { entity: e.target.value })}
        />
        <p className="text-xs text-muted-foreground">
          No write without an entity, ever — a subscription delivers records, so it needs one too. It names the destination
          index or table; the topic name it reads is not renamed by it.
        </p>
      </div>

      <div className="grid gap-1.5">
        <Label>Initial position</Label>
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
      </div>
      <Alert>
        <AlertTitle className="text-sm">Save is live</AlertTitle>
        <AlertDescription className="text-xs">
          Subscription-only sink over the topic's bytes — untouched, no transforms, no schema surface. Saving creates or
          updates the sink independently of flow deploys, and it never blocks the chain.
        </AlertDescription>
      </Alert>
    </div>
  );
}
