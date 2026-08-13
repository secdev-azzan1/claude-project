// Schemas — the master-detail schema workspace.
//
// TWO record kinds live side by side in the left rail:
//
//   APPROVED SCHEMA  born from a ceremony on a kafka_kc block. Approval IS
//                    registration, so it carries a registry global id and an
//                    approval history. Read-only here — changing one means
//                    re-running its ceremony.
//   LIBRARY TEMPLATE hand-authored, unregistered, bound to nothing. Fully
//                    editable, freely deletable, and usable to pre-fill a
//                    ceremony.
//
// There is deliberately no Draft→Verified lifecycle (the old app's banner and
// artifact/version dialogs). Approval is registration; re-running the ceremony
// is versioning. The editing experience is otherwise identical for both
// kinds — same tabs-plus-Add-Field layout, same one Save button — regardless
// of registration state:
//
//   CHECK    a template (or an edited approved schema) is checked in place —
//            the same structural refusals the ceremony would raise, before
//            walking to it.
//   SAVE     persists the edited buffer in place. For an approved schema this
//            is a `draftAvro` on the record, NOT a new approval — nothing is
//            registered until the ceremony's Approve, which is still the only
//            door. "Register new version…" is what carries the buffer there.

import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useNavigate } from "react-router-dom";
import { AppLayout } from "@/components/AppLayout";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { AvroEditorTabs, SampleInferencePanel, useAvroBuffer } from "@/components/schema-editor";
import { useDebouncedValue } from "@/lib/useDebouncedValue";
import {
  createEmptyAvroTemplate,
  normalizeAvroRecord,
  subjectToRecordName,
  type AvroRecord,
} from "@/lib/schemaEditor";
import { schemaWorkspaceLayout } from "@/lib/schemaLayout";
import { cn } from "@/lib/utils";
import {
  createSchemaTemplate,
  deleteApprovedSchema,
  deleteApprovedSchemaVersion,
  deleteSchemaTemplate,
  getRegistrySubjectVersion,
  listFlows,
  listRegistrySubjectVersions,
  listSchemaTemplates,
  listSchemas,
  registerSchema,
  saveApprovedAsTemplate,
  saveApprovedSchemaDraft,
  saveSchemaTemplate,
  stageCeremonyDraft,
  verifySchema,
  type VerifySchemaResult,
} from "@/prototype/api";
import type { InferenceReport } from "@/prototype/inference";
import type { ApprovedSchema, Flow, SchemaApproval, SchemaProvenance, SchemaTemplate } from "@/prototype/types";
import {
  AlertTriangle,
  BookMarked,
  FileJson,
  History,
  Loader2,
  Plus,
  Save,
  Search,
  ShieldCheck,
  Trash2,
  UploadCloud,
  Wand2,
  X,
} from "lucide-react";
import { toast } from "sonner";

// ------------------------------------------------------------------ helpers

const relativeTime = (iso?: string | null): string => {
  if (!iso) return "—";
  const diff = Date.now() - new Date(iso).getTime();
  if (Number.isNaN(diff)) return "—";
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.floor(hrs / 24)}d ago`;
};

const PROVENANCE_META: Record<
  SchemaProvenance,
  { label: string; short: string; className: string; icon: React.ComponentType<{ className?: string }> }
> = {
  sample_run: {
    label: "Live sample run",
    short: "live run",
    className: "bg-success-muted text-success border-success/20",
    icon: ShieldCheck,
  },
  uploaded: {
    label: "Uploaded samples",
    short: "sample files",
    className: "bg-info-muted text-info border-info/20",
    icon: ShieldCheck,
  },
  manual: {
    label: "Manually authored — not sample-validated",
    short: "manual",
    className: "bg-warning-muted text-warning border-warning/20",
    icon: AlertTriangle,
  },
};

const PROVENANCE_ORDER: SchemaProvenance[] = ["sample_run", "uploaded", "manual"];

/** "Security incident envelope" -> "security-incident-envelope-value" — a starting
 *  point for the Register dialog's subject, never forced on the user. */
const suggestSubject = (name: string): string => {
  const token = name
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return `${token || "schema"}-value`;
};

// ------------------------------------------------------------------- checks

interface CheckLine {
  level: "pass" | "warn" | "fail";
  text: string;
}

/** Avro's own rule for names: no dots, no leading digit. */
const AVRO_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * What the registry would say, said here. Deliberately the same three refusals
 * the ceremony enforces — a check that is kinder than the gate it predicts is
 * worse than no check at all.
 */
function checkAvroRecord(record: AvroRecord | null, rawError: string | null): CheckLine[] {
  if (rawError) return [{ level: "fail", text: `The raw Avro JSON does not parse: ${rawError}` }];
  if (!record) return [{ level: "fail", text: "There is no Avro record to check." }];

  const lines: CheckLine[] = [];
  lines.push({ level: "pass", text: "Valid Avro JSON — it parses and normalises." });

  if (!AVRO_NAME.test(record.name ?? "")) {
    lines.push({
      level: "fail",
      text: `Record name “${record.name ?? ""}” is not a legal Avro name (letters, digits and underscore; never starting with a digit).`,
    });
  }
  if (record.namespace && !record.namespace.split(".").every((part) => AVRO_NAME.test(part))) {
    lines.push({ level: "fail", text: `Namespace “${record.namespace}” has a segment that is not a legal Avro name.` });
  }

  const fields = record.fields ?? [];
  if (fields.length === 0) {
    lines.push({ level: "fail", text: "A schema needs at least one field — an empty record cannot be registered." });
  } else {
    const names = fields.map((f) => f.name);
    const duplicates = [...new Set(names.filter((n, i) => names.indexOf(n) !== i))];
    if (duplicates.length > 0) {
      lines.push({ level: "fail", text: `Duplicate field name(s): ${duplicates.join(", ")}.` });
    }
    const illegal = names.filter((n) => !AVRO_NAME.test(n));
    if (illegal.length > 0) {
      lines.push({ level: "fail", text: `Field name(s) Avro will refuse: ${illegal.join(", ")}.` });
    }
    if (duplicates.length === 0 && illegal.length === 0) {
      lines.push({ level: "pass", text: `${fields.length} field(s), all legally named and distinct.` });
    }
  }
  return lines;
}

function ProvenanceBadge({ provenance }: { provenance: SchemaProvenance }) {
  const meta = PROVENANCE_META[provenance];
  const Icon = meta.icon;
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium",
        meta.className,
      )}
    >
      <Icon className="h-3 w-3" />
      {meta.label}
    </span>
  );
}

function KindBadge({ globalId }: { globalId?: number }) {
  return globalId === undefined ? (
    <span className="inline-flex items-center gap-1 rounded-full border border-border bg-muted px-2 py-0.5 text-xs font-medium text-muted-foreground">
      <BookMarked className="h-3 w-3" /> Template · not registered
    </span>
  ) : (
    <span className="inline-flex items-center gap-1 rounded-full border border-success/20 bg-success-muted px-2 py-0.5 text-xs font-medium text-success">
      <ShieldCheck className="h-3 w-3" /> Approved · <span className="font-mono">#{globalId}</span>
    </span>
  );
}

/**
 * A template's registration pill — the alpha-model counterpart of
 * `KindBadge` for library templates, which register independently (via
 * Register…) rather than through a ceremony. `compact` drops the "Registered"
 * word and version for the tight rail row; the detail header uses the full
 * form. Unregistered templates render nothing in compact mode (nothing to
 * say yet) but the explicit muted pill in the detail header.
 */
function TemplateRegistrationBadge({
  globalId,
  version,
  compact,
}: {
  globalId?: number;
  version?: number;
  compact?: boolean;
}) {
  if (globalId == null) {
    if (compact) return null;
    return (
      <span className="inline-flex items-center gap-1 rounded-full border border-border bg-transparent px-2.5 py-0.5 text-xs font-medium text-muted-foreground">
        Not registered
      </span>
    );
  }
  return (
    <span className="inline-flex items-center gap-1 rounded-full border border-success/20 bg-success-muted px-2 py-0.5 text-xs font-medium text-success">
      <ShieldCheck className="h-3 w-3" />
      {compact ? (
        <span className="font-mono">#{globalId}</span>
      ) : (
        <>
          Registered · <span className="font-mono">#{globalId}</span>
          {version != null && (
            <>
              {" "}
              · <span className="font-mono">v{version}</span>
            </>
          )}
        </>
      )}
    </span>
  );
}

function FilterChip({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn(
        "rounded-full border px-2.5 py-0.5 text-xs font-medium transition-colors",
        active
          ? "border-primary bg-primary-muted text-primary"
          : "border-border bg-transparent text-muted-foreground hover:bg-muted",
      )}
    >
      {children}
    </button>
  );
}

/** Check results, rendered where the thing they judge is. */
function CheckPanel({ lines }: { lines: CheckLine[] }) {
  const failed = lines.filter((l) => l.level === "fail").length;
  return (
    <div
      className={cn(
        "mb-3 space-y-1 rounded-md border p-2.5",
        failed > 0 ? "border-destructive/40 bg-destructive/5" : "border-success/40 bg-success-muted",
      )}
    >
      <p className="text-xs font-medium">
        {failed > 0
          ? `${failed} problem(s) would stop registration`
          : "Registration checks passed"}
      </p>
      {lines.map((line, i) => (
        <p
          key={`${line.level}-${i}`}
          className={cn(
            "flex items-start gap-1.5 text-xs",
            line.level === "fail" ? "text-destructive" : line.level === "warn" ? "text-warning" : "text-muted-foreground",
          )}
        >
          {line.level === "fail" ? (
            <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
          ) : (
            <ShieldCheck className="mt-0.5 h-3.5 w-3.5 shrink-0" />
          )}
          <span>{line.text}</span>
        </p>
      ))}
      {failed === 0 && (
        <p className="text-xs text-muted-foreground">
          Structure only. A schema is registered under a stream's subject, so the ceremony still asks which
          kafka+connect write it governs.
        </p>
      )}
    </div>
  );
}

/**
 * Backend `/api/v2/schemas/verify` results, rendered where the buffer they
 * judge is. Independent of the ceremony's local `checkAvroRecord` — this is
 * the live registry's own read, structural + (when a subject was given)
 * compatibility against the latest registered version.
 */
function VerifyPanel({ result, onDismiss }: { result: VerifySchemaResult; onDismiss: () => void }) {
  const failed = result.issues.length;
  return (
    <div
      className={cn(
        "mb-3 space-y-1 rounded-md border p-2.5",
        failed > 0 ? "border-destructive/40 bg-destructive/5" : "border-success/40 bg-success-muted",
      )}
    >
      <div className="flex items-start justify-between gap-2">
        <p className="text-xs font-medium">
          {failed > 0 ? `${failed} structural issue(s) found` : "Schema is valid"}
        </p>
        <button
          type="button"
          className="shrink-0 rounded p-0.5 text-muted-foreground hover:bg-muted hover:text-foreground"
          title="Dismiss"
          onClick={onDismiss}
        >
          <X className="h-3.5 w-3.5" />
        </button>
      </div>
      {failed > 0 ? (
        <ul className="space-y-1">
          {result.issues.map((issue, i) => (
            <li key={i} className="flex items-start gap-1.5 text-xs text-destructive">
              <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
              <span>{issue}</span>
            </li>
          ))}
        </ul>
      ) : (
        <p className="flex items-start gap-1.5 text-xs text-muted-foreground">
          <ShieldCheck className="mt-0.5 h-3.5 w-3.5 shrink-0" />
          <span>Structurally valid Avro — it parses and normalises.</span>
        </p>
      )}
      {result.compatibility.checked && (
        <p
          className={cn(
            "flex items-start gap-1.5 text-xs",
            result.compatibility.compatible ? "text-muted-foreground" : "text-destructive",
          )}
        >
          {result.compatibility.compatible ? (
            <ShieldCheck className="mt-0.5 h-3.5 w-3.5 shrink-0" />
          ) : (
            <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
          )}
          <span>
            {result.compatibility.compatible
              ? "Compatible with latest registered version."
              : result.compatibility.message}
          </span>
        </p>
      )}
    </div>
  );
}

/**
 * Summary of a "New template → Infer from sample files" run, shown once above
 * the freshly created template's editor. Purely transient client-side state —
 * dismissing it (or navigating to a different artifact) loses nothing that
 * was persisted; the inferred fields themselves are already saved.
 */
function InferredReportPanel({ report, onDismiss }: { report: InferenceReport; onDismiss: () => void }) {
  return (
    <div className="mb-3 space-y-1 rounded-md border border-info/30 bg-info-muted p-2.5">
      <div className="flex items-start justify-between gap-2">
        <p className="text-xs font-medium">
          Inferred {report.fieldCount} field(s) from {report.recordsSampled} sample record(s).
        </p>
        <button
          type="button"
          className="shrink-0 rounded p-0.5 text-muted-foreground hover:bg-muted hover:text-foreground"
          title="Dismiss"
          onClick={onDismiss}
        >
          <X className="h-3.5 w-3.5" />
        </button>
      </div>
      {report.notes.length > 0 ? (
        <ul className="space-y-0.5">
          {report.notes.slice(0, 6).map((note, i) => (
            <li key={`${note.field}-${note.kind}-${i}`} className="text-xs text-muted-foreground">
              {note.field ? (
                <>
                  <code className="font-mono">{note.field}</code> {note.note}
                </>
              ) : (
                note.note
              )}
            </li>
          ))}
          {report.notes.length > 6 && (
            <li className="text-xs text-muted-foreground">…and {report.notes.length - 6} more inference note(s).</li>
          )}
        </ul>
      ) : (
        <p className="text-xs text-muted-foreground">
          Every field was consistent across the samples — nothing had to be widened or made nullable.
        </p>
      )}
    </div>
  );
}

/**
 * The alpha model's registration axis: an approved schema is always
 * registered (approval IS registration); a template is registered only once
 * it has been through the independent Register… action.
 */
type RegistrationFilter = "all" | "registered" | "not_registered";

type Artifact =
  | { kind: "approved"; id: string; label: string; at: string; schema: ApprovedSchema }
  | { kind: "template"; id: string; label: string; at: string; template: SchemaTemplate };

/**
 * One shared delete dialog for both kinds (Change 5). A template deletes
 * outright; an approved schema offers a granular choice between deleting the
 * one approval currently selected and deleting the whole record.
 */
type DeleteTarget =
  | { kind: "template"; template: SchemaTemplate }
  | { kind: "approved"; schema: ApprovedSchema; approval: SchemaApproval };

// ------------------------------------------------------------------ the page

const Schemas = () => {
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const { data: schemas = [], isLoading: schemasLoading } = useQuery({
    queryKey: ["schemas"],
    queryFn: listSchemas,
  });
  const { data: templates = [], isLoading: templatesLoading } = useQuery({
    queryKey: ["schemaTemplates"],
    queryFn: listSchemaTemplates,
  });
  const { data: flows = [] } = useQuery({ queryKey: ["flows"], queryFn: listFlows });

  const isLoading = schemasLoading || templatesLoading;

  // ─── rail state ───────────────────────────────────────────────────────
  const [search, setSearch] = useState("");
  const debouncedSearch = useDebouncedValue(search, 250);
  const [registrationFilter, setRegistrationFilter] = useState<RegistrationFilter>("all");
  const [provenanceFilter, setProvenanceFilter] = useState<SchemaProvenance[]>([]);
  const [selectedId, setSelectedId] = useState("");

  // ─── detail state ─────────────────────────────────────────────────────
  const [approvalVersion, setApprovalVersion] = useState<number | null>(null);
  const [templateName, setTemplateName] = useState("");
  const [templateDescription, setTemplateDescription] = useState("");

  // ─── dialogs ──────────────────────────────────────────────────────────
  const [newTemplateOpen, setNewTemplateOpen] = useState(false);
  const [newTemplateName, setNewTemplateName] = useState("");
  const [newTemplateDescription, setNewTemplateDescription] = useState("");
  /** "Start empty" (current behaviour) vs. "Infer from sample files" (expands SampleInferencePanel inline). */
  const [newTemplateMode, setNewTemplateMode] = useState<"empty" | "infer">("empty");
  /**
   * The inference report for a template just created via the "infer" path —
   * transient, client-side only, shown as a dismissible summary above the
   * editor for that one template until dismissed or the selection moves on.
   */
  const [inferredReport, setInferredReport] = useState<{ templateId: string; report: InferenceReport } | null>(null);
  const [saveAsTemplateFor, setSaveAsTemplateFor] = useState<ApprovedSchema | null>(null);
  const [saveAsTemplateName, setSaveAsTemplateName] = useState("");
  const [deleteTarget, setDeleteTarget] = useState<DeleteTarget | null>(null);
  const [ceremonyPickerFor, setCeremonyPickerFor] = useState<SchemaTemplate | null>(null);
  /** Structural check results, shown against the artifact that produced them. */
  const [checkFor, setCheckFor] = useState<{ id: string; lines: CheckLine[] } | null>(null);
  /** Live backend `/verify` results — independent of the ceremony's local check. */
  const [verifyFor, setVerifyFor] = useState<{ id: string; result: VerifySchemaResult } | null>(null);
  /** Template being registered directly (independently of any flow ceremony). */
  const [registerFor, setRegisterFor] = useState<SchemaTemplate | null>(null);
  const [registerSubject, setRegisterSubject] = useState("");

  // ─── artifact list ────────────────────────────────────────────────────
  const artifacts = useMemo<Artifact[]>(() => {
    const rows: Artifact[] = [
      ...schemas.map<Artifact>((schema) => ({
        kind: "approved",
        id: schema.id,
        label: schema.subject,
        at: schema.approvedAt,
        schema,
      })),
      ...templates.map<Artifact>((template) => ({
        kind: "template",
        id: template.id,
        label: template.name,
        at: template.updatedAt,
        template,
      })),
    ];
    return rows.sort((a, b) => new Date(b.at).getTime() - new Date(a.at).getTime());
  }, [schemas, templates]);

  const flowLabel = useMemo(() => {
    const map = new Map<string, { flow: Flow; blockName?: string }>();
    for (const schema of schemas) {
      const flow = flows.find((f) => f.id === schema.flowId);
      if (flow) {
        map.set(schema.id, { flow, blockName: flow.blocks.find((b) => b.id === schema.blockId)?.name });
      }
    }
    return map;
  }, [schemas, flows]);

  const filtered = useMemo(() => {
    const query = debouncedSearch.trim().toLowerCase();
    return artifacts.filter((artifact) => {
      if (registrationFilter !== "all") {
        const registered = artifact.kind === "approved" || artifact.template.registryGlobalId != null;
        if (registrationFilter === "registered" && !registered) return false;
        if (registrationFilter === "not_registered" && registered) return false;
      }
      // Provenance is an approved-only axis: narrowing it hides templates.
      if (provenanceFilter.length > 0) {
        if (artifact.kind !== "approved") return false;
        if (!provenanceFilter.includes(artifact.schema.provenance)) return false;
      }
      if (!query) return true;
      const owner = flowLabel.get(artifact.id);
      const haystack =
        artifact.kind === "approved"
          ? [
              artifact.schema.subject,
              artifact.schema.entity,
              `#${artifact.schema.registryGlobalId}`,
              owner?.flow.name ?? "",
              owner?.blockName ?? "",
            ]
          : [artifact.template.name, artifact.template.description ?? ""];
      return haystack.some((value) => value.toLowerCase().includes(query));
    });
  }, [artifacts, registrationFilter, provenanceFilter, debouncedSearch, flowLabel]);

  const selected = useMemo(
    () => artifacts.find((a) => a.id === selectedId) ?? filtered[0] ?? artifacts[0] ?? null,
    [artifacts, filtered, selectedId],
  );

  const selectedSchema = selected?.kind === "approved" ? selected.schema : null;
  const selectedTemplate = selected?.kind === "template" ? selected.template : null;

  // Approval history: newest LAST. A stale version number falls back to latest.
  const approvals: SchemaApproval[] = selectedSchema?.approvals ?? [];
  const activeApproval: SchemaApproval | null =
    approvals.find((a) => a.version === approvalVersion) ?? approvals[approvals.length - 1] ?? null;
  const isHistoricalApproval = !!activeApproval && activeApproval.version !== approvals[approvals.length - 1]?.version;

  // ─── registered-template registry version browsing (alpha parity) ─────
  // A registered template's doc only ever tracks the CURRENT registry
  // version. `viewedRegistryVersion` is null while browsing the working
  // (editable) buffer; picking an OLDER version fetches it straight from the
  // registry and shows it read-only, mirroring `approvalVersion` above.
  const [viewedRegistryVersion, setViewedRegistryVersion] = useState<number | null>(null);
  const registeredSubject = selectedTemplate?.registryGlobalId != null ? selectedTemplate.registeredSubject : undefined;
  const templateIsRegistered = !!registeredSubject;

  const { data: templateRegistryVersions = [], isFetching: templateVersionsLoading } = useQuery({
    queryKey: ["schemaRegistryVersions", registeredSubject],
    queryFn: () => listRegistrySubjectVersions(registeredSubject!),
    enabled: templateIsRegistered,
  });

  const isViewingOldTemplateVersion = templateIsRegistered && viewedRegistryVersion != null;

  const { data: viewedTemplateVersionDetail, isFetching: viewedTemplateVersionLoading } = useQuery({
    queryKey: ["schemaRegistryVersionDetail", registeredSubject, viewedRegistryVersion],
    queryFn: () => getRegistrySubjectVersion(registeredSubject!, viewedRegistryVersion!),
    enabled: templateIsRegistered && viewedRegistryVersion != null,
  });

  // ─── the one source of truth for the editor ───────────────────────────
  const detail = useMemo(() => {
    if (!selected) return null;
    if (selected.kind === "approved") {
      // A saved draft only ever applies to the CURRENT approval — history is
      // what was actually registered, and stays exactly that.
      const hasDraft = !isHistoricalApproval && selected.schema.draftAvro !== undefined;
      const raw = hasDraft
        ? JSON.stringify(selected.schema.draftAvro, null, 2)
        : (activeApproval?.rawAvro ?? selected.schema.rawAvro);
      return {
        key: `approved:${selected.id}:${activeApproval?.version ?? "latest"}:${selected.schema.draftUpdatedAt ?? ""}`,
        raw,
        fallbackName: subjectToRecordName(selected.schema.subject),
      };
    }
    if (isViewingOldTemplateVersion) {
      // Never mutates the template — this is a pure read of the registry's
      // own history. Holds the last-shown content until the fetch for the
      // newly picked version lands, rather than flashing the working buffer.
      const raw = viewedTemplateVersionDetail
        ? JSON.stringify(viewedTemplateVersionDetail.avro, null, 2)
        : selected.template.rawAvro;
      return {
        key: `template:${selected.id}:registry-v${viewedRegistryVersion}:${viewedTemplateVersionDetail ? "loaded" : "loading"}`,
        raw,
        fallbackName: subjectToRecordName(selected.template.name),
      };
    }
    return {
      key: `template:${selected.id}:${selected.template.updatedAt}`,
      raw: selected.template.rawAvro,
      fallbackName: subjectToRecordName(selected.template.name),
    };
  }, [
    selected,
    activeApproval,
    isHistoricalApproval,
    isViewingOldTemplateVersion,
    viewedRegistryVersion,
    viewedTemplateVersionDetail,
  ]);

  const parsed = useMemo(() => {
    if (!detail) return { record: null as AvroRecord | null, error: null as string | null };
    try {
      return { record: normalizeAvroRecord(JSON.parse(detail.raw), detail.fallbackName), error: null };
    } catch (error) {
      return {
        record: null as AvroRecord | null,
        error: error instanceof Error ? error.message : "Invalid Avro JSON",
      };
    }
  }, [detail]);

  const buffer = useAvroBuffer(detail?.fallbackName ?? "Record");

  useEffect(() => {
    if (!detail) {
      buffer.reset(null);
      return;
    }
    // A stored schema that does not parse hands the raw text back verbatim so
    // it can be repaired, rather than being blanked.
    buffer.reset(
      parsed.record,
      parsed.error ? { rawText: detail.raw, rawError: parsed.error } : undefined,
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [detail?.key]);

  useEffect(() => {
    setApprovalVersion(null);
    setCheckFor(null);
    setVerifyFor(null);
    setViewedRegistryVersion(null);
  }, [selected?.id]);

  useEffect(() => {
    setTemplateName(selectedTemplate?.name ?? "");
    setTemplateDescription(selectedTemplate?.description ?? "");
  }, [selectedTemplate?.id, selectedTemplate?.updatedAt, selectedTemplate?.name, selectedTemplate?.description]);

  const metaDirty =
    !!selectedTemplate &&
    (templateName !== selectedTemplate.name ||
      templateDescription !== (selectedTemplate.description ?? ""));
  const templateDirty = !!selectedTemplate && (buffer.dirty || metaDirty);
  /**
   * True once a registered template's buffer has moved on from what was last
   * published — either the in-progress edit is unsaved (`templateDirty`), or
   * it was saved after the last registration (`updatedAt` outran
   * `registeredAt`). Meaningless for a template that was never registered.
   */
  const templateEditedSinceRegistration =
    !!selectedTemplate &&
    selectedTemplate.registryGlobalId != null &&
    (templateDirty ||
      (!!selectedTemplate.registeredAt &&
        new Date(selectedTemplate.updatedAt).getTime() > new Date(selectedTemplate.registeredAt).getTime()));

  // ─── mutations ────────────────────────────────────────────────────────
  const invalidateTemplates = () => queryClient.invalidateQueries({ queryKey: ["schemaTemplates"] });

  const createMut = useMutation({
    mutationFn: createSchemaTemplate,
    onSuccess: (tpl) => {
      toast.success(`Template "${tpl.name}" created — not registered, bound to no flow.`);
      setSelectedId(tpl.id);
      setNewTemplateOpen(false);
      setNewTemplateName("");
      setNewTemplateDescription("");
      void invalidateTemplates();
    },
    onError: (error: Error) => toast.error(error.message),
  });

  const saveMut = useMutation({
    mutationFn: saveSchemaTemplate,
    onSuccess: (tpl) => {
      toast.success(`Template "${tpl.name}" saved.`);
      void invalidateTemplates();
    },
    onError: (error: Error) => toast.error(error.message),
  });

  const deleteMut = useMutation({
    mutationFn: deleteSchemaTemplate,
    onSuccess: () => {
      toast.success("Template deleted. Approvals pre-filled from it keep its name in their history.");
      setSelectedId("");
      setDeleteTarget(null);
      void invalidateTemplates();
    },
    onError: (error: Error) => toast.error(error.message),
  });

  const saveAsTemplateMut = useMutation({
    mutationFn: ({ schemaId, name }: { schemaId: string; name: string }) =>
      saveApprovedAsTemplate(schemaId, name),
    onSuccess: (tpl) => {
      toast.success(`Saved to the library as "${tpl.name}".`);
      setSaveAsTemplateFor(null);
      setSelectedId(tpl.id);
      void invalidateTemplates();
    },
    onError: (error: Error) => toast.error(error.message),
  });

  const invalidateSchemas = () => queryClient.invalidateQueries({ queryKey: ["schemas"] });

  const saveApprovedDraftMut = useMutation({
    mutationFn: ({ schemaId, avro }: { schemaId: string; avro: unknown }) => saveApprovedSchemaDraft(schemaId, avro),
    onSuccess: () => {
      toast.success("Draft saved — not registered.");
      void invalidateSchemas();
    },
    onError: (error: Error) => toast.error(error.message),
  });

  const deleteApprovedVersionMut = useMutation({
    mutationFn: ({ schemaId, version }: { schemaId: string; version: number }) =>
      deleteApprovedSchemaVersion(schemaId, version),
    onSuccess: (updated, variables) => {
      toast.success(`Approval v${variables.version} deleted.`);
      setDeleteTarget(null);
      // Fall back the selection to the previous approval — the highest
      // remaining version below the one just deleted, or the new oldest if
      // the oldest one was the one removed.
      const remaining = updated.approvals.map((a) => a.version);
      const prior = remaining.filter((v) => v < variables.version);
      setApprovalVersion(prior.length > 0 ? Math.max(...prior) : Math.min(...remaining));
      void invalidateSchemas();
    },
    onError: (error: Error) => toast.error(error.message),
  });

  const deleteApprovedSchemaMut = useMutation({
    mutationFn: (schemaId: string) => deleteApprovedSchema(schemaId),
    onSuccess: () => {
      toast.success("Schema deleted.");
      setDeleteTarget(null);
      setSelectedId("");
      void invalidateSchemas();
    },
    onError: (error: Error) => toast.error(error.message),
  });

  // Independent of the ceremony: structural validation + (when a subject is
  // known) a registry compatibility check, WITHOUT registering anything.
  const verifyMut = useMutation({
    mutationFn: (vars: { id: string; avro: unknown; subject?: string }) => verifySchema(vars.avro, vars.subject),
    onSuccess: (result, vars) => {
      setVerifyFor({ id: vars.id, result });
      if (!result.ok) {
        toast.error(`${result.issues.length} structural issue(s) found.`);
      } else if (result.compatibility.checked && result.compatibility.compatible === false) {
        toast.error("Valid, but not compatible with the latest registered version.");
      } else {
        toast.success("Schema is valid.");
      }
    },
    onError: (error: Error) => toast.error(error.message),
  });

  // Independent of the ceremony: registers straight to the registry.
  const registerMut = useMutation({
    mutationFn: (vars: { subject: string; avro: unknown; templateId?: string }) =>
      registerSchema(vars.subject, vars.avro, vars.templateId),
    onSuccess: (result) => {
      toast.success(`Registered — ${result.subject} (global id ${result.globalId})`);
      setRegisterFor(null);
      void invalidateSchemas();
      void invalidateTemplates();
    },
    onError: (error: Error) => toast.error(error.message),
  });

  // ─── actions ──────────────────────────────────────────────────────────
  const handleCreateTemplate = () => {
    const name = newTemplateName.trim();
    if (!name) {
      toast.error("A template needs a name.");
      return;
    }
    try {
      // createEmptyAvroTemplate is the ONLY safe way to mint a record — a
      // hand-built object can trip normalizeAvroRecord's throws.
      const record = createEmptyAvroTemplate(name);
      createMut.mutate({
        name,
        description: newTemplateDescription.trim() || undefined,
        rawAvro: JSON.stringify(record, null, 2),
      });
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Could not create the template.");
    }
  };

  /**
   * The "Infer from sample files" path's completion: SampleInferencePanel has
   * already turned the uploaded samples into a normalised Avro record, so this
   * just names it and creates the template with it instead of an empty record.
   */
  const handleInferredTemplate = (avro: unknown, report: InferenceReport) => {
    const name = newTemplateName.trim();
    if (!name) {
      toast.error("A template needs a name.");
      return;
    }
    try {
      const record = normalizeAvroRecord(avro, subjectToRecordName(name));
      createMut.mutate(
        {
          name,
          description: newTemplateDescription.trim() || undefined,
          rawAvro: JSON.stringify(record, null, 2),
        },
        { onSuccess: (tpl) => setInferredReport({ templateId: tpl.id, report }) },
      );
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Could not build a valid Avro record from those samples.");
    }
  };

  const handleSaveTemplate = () => {
    if (!selectedTemplate) return;
    const name = templateName.trim();
    if (!name) {
      toast.error("A template needs a name.");
      return;
    }
    if (buffer.rawError) {
      toast.error(`Cannot save: ${buffer.rawError}`);
      return;
    }
    if (!buffer.record) {
      toast.error("Cannot save: the raw Avro JSON does not describe a record.");
      return;
    }
    try {
      // Re-normalise before persisting: what goes into the store must be
      // something normalizeAvroRecord can read back.
      const record = normalizeAvroRecord(buffer.record, subjectToRecordName(name));
      saveMut.mutate({
        ...selectedTemplate,
        name,
        description: templateDescription.trim() || undefined,
        rawAvro: JSON.stringify(record, null, 2),
      });
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "The schema is not a valid Avro record.");
    }
  };

  const handleSaveApprovedDraft = () => {
    if (!selectedSchema) return;
    if (buffer.rawError) {
      toast.error(`Cannot save: ${buffer.rawError}`);
      return;
    }
    if (!buffer.record) {
      toast.error("Cannot save: the raw Avro JSON does not describe a record.");
      return;
    }
    saveApprovedDraftMut.mutate({ schemaId: selectedSchema.id, avro: buffer.record });
  };

  const owner = selectedSchema ? flowLabel.get(selectedSchema.id) : undefined;

  const ceremonyTargets = useMemo(
    () =>
      flows
        .map((flow) => ({ flow, blocks: flow.blocks.filter((b) => b.adapter === "kafka_kc") }))
        .filter((group) => group.blocks.length > 0),
    [flows],
  );

  const startCeremonyFromTemplate = (flowId: string, blockId: string, templateId: string) => {
    setCeremonyPickerFor(null);
    navigate(`/flow-builder/${flowId}?ceremony=${blockId}&prefill=${templateId}`);
  };

  /**
   * Flows whose kafka_kc block is this schema's own — matched by flowId +
   * blockId, the same identity `schemaStatus` (Flows.tsx) uses to decide a
   * block is "approved". Deleting this schema would leave that block
   * pointing at nothing.
   */
  const flowsReferencingApprovedSchema = (schema: ApprovedSchema): string[] =>
    flows
      .filter(
        (f) => f.id === schema.flowId && f.blocks.some((b) => b.adapter === "kafka_kc" && b.id === schema.blockId),
      )
      .map((f) => f.name);

  /**
   * Ask the live backend to verify the current buffer: structural Avro
   * validation, plus — when `subject` is known — a registry compatibility
   * check against its latest version. Registers nothing.
   */
  const runVerify = (id: string, subject?: string) => {
    if (buffer.rawError) {
      toast.error(`Cannot verify: ${buffer.rawError}`);
      return;
    }
    if (!buffer.record) {
      toast.error("There is no valid Avro record to verify.");
      return;
    }
    verifyMut.mutate({ id, avro: buffer.record, subject });
  };

  /** Open the independent Register dialog, pre-filled from the template's
   *  last direct registration (if any) or a tokenized suggestion. */
  const openRegisterForTemplate = (template: SchemaTemplate) => {
    setRegisterFor(template);
    setRegisterSubject(template.registeredSubject || suggestSubject(template.name));
  };

  /**
   * Carry the edited record to the ceremony that will register it. The schema is
   * staged in the store rather than pushed through the URL: an Avro record does
   * not belong in a query string, and the ceremony has to be able to claim it
   * exactly once.
   */
  const registerNewVersion = async () => {
    if (!selectedSchema || !owner) return;
    if (buffer.rawError) {
      toast.error(`Fix the raw Avro JSON first: ${buffer.rawError}`);
      return;
    }
    if (!buffer.record) {
      toast.error("There is no valid Avro record to register.");
      return;
    }
    const lines = checkAvroRecord(buffer.record, null);
    if (lines.some((l) => l.level === "fail")) {
      setCheckFor({ id: selectedSchema.id, lines });
      toast.error("The edited schema would be refused — see the checks below.");
      return;
    }
    try {
      await stageCeremonyDraft({
        flowId: owner.flow.id,
        blockId: selectedSchema.blockId,
        rawAvro: JSON.stringify(normalizeAvroRecord(buffer.record, subjectToRecordName(selectedSchema.subject)), null, 2),
        label: `${selectedSchema.subject} v${activeApproval?.version ?? approvals.length}`,
      });
      navigate(`/flow-builder/${owner.flow.id}?ceremony=${selectedSchema.blockId}`);
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Could not hand the edit to the ceremony.");
    }
  };

  // ─── render ───────────────────────────────────────────────────────────
  return (
    <AppLayout
      title="Schemas"
      description="Approved schemas come from the flow ceremony. Library templates are hand-authored and can be verified and registered to the registry directly."
    >
      <div className={schemaWorkspaceLayout.grid}>
        {/* ------------------------------------------------ left artifact rail */}
        <Card className={cn(schemaWorkspaceLayout.artifactCard, "shadow-sm")}>
          <CardHeader className="gap-3 space-y-0 pb-3">
            <div className="flex items-start justify-between gap-3">
              <div>
                <CardTitle className="text-base">Schema library</CardTitle>
                <CardDescription>
                  {filtered.length === artifacts.length
                    ? `${artifacts.length} record${artifacts.length === 1 ? "" : "s"}`
                    : `${filtered.length} of ${artifacts.length} records`}
                </CardDescription>
              </div>
              <Button
                size="sm"
                variant="outline"
                onClick={() => {
                  setNewTemplateName("");
                  setNewTemplateDescription("");
                  setNewTemplateMode("empty");
                  setNewTemplateOpen(true);
                }}
              >
                <Plus className="h-4 w-4" /> New template
              </Button>
            </div>

            <div className="relative">
              <Search className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
              <Input
                placeholder="Search subject, entity, flow, template…"
                className="h-9 pl-8"
                value={search}
                onChange={(event) => setSearch(event.target.value)}
              />
            </div>

            <div className="space-y-2">
              <div className="flex flex-wrap gap-1.5">
                {(
                  [
                    ["all", "All"],
                    ["registered", "Registered"],
                    ["not_registered", "Not registered"],
                  ] as [RegistrationFilter, string][]
                ).map(([value, label]) => (
                  <FilterChip
                    key={value}
                    active={registrationFilter === value}
                    onClick={() => setRegistrationFilter(value)}
                  >
                    {label}
                  </FilterChip>
                ))}
              </div>
              <div className="flex flex-wrap items-center gap-1.5">
                <span className="text-xs text-muted-foreground">Evidence</span>
                {PROVENANCE_ORDER.map((provenance) => (
                  <FilterChip
                    key={provenance}
                    active={provenanceFilter.includes(provenance)}
                    onClick={() =>
                      setProvenanceFilter((prev) =>
                        prev.includes(provenance)
                          ? prev.filter((p) => p !== provenance)
                          : [...prev, provenance],
                      )
                    }
                  >
                    {PROVENANCE_META[provenance].short}
                  </FilterChip>
                ))}
                {provenanceFilter.length > 0 && (
                  <button
                    type="button"
                    className="text-xs text-muted-foreground underline-offset-2 hover:underline"
                    onClick={() => setProvenanceFilter([])}
                  >
                    clear
                  </button>
                )}
              </div>
            </div>
          </CardHeader>

          <CardContent className={schemaWorkspaceLayout.artifactContent}>
            <div className={schemaWorkspaceLayout.artifactList}>
              {isLoading ? (
                <div className="flex h-32 items-center justify-center">
                  <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
                </div>
              ) : filtered.length === 0 ? (
                <div className="rounded-md border border-dashed p-6 text-center text-sm text-muted-foreground">
                  {artifacts.length === 0
                    ? "Nothing here yet. Run a schema ceremony from a kafka+connect block, or start a library template."
                    : "No records match these filters."}
                </div>
              ) : (
                filtered.map((artifact) => {
                  const isSelected = selected?.id === artifact.id;
                  const artifactOwner = flowLabel.get(artifact.id);
                  return (
                    <button
                      key={artifact.id}
                      type="button"
                      onClick={() => setSelectedId(artifact.id)}
                      className={cn(
                        "w-full rounded-md border p-2.5 text-left transition-colors",
                        isSelected ? "border-primary bg-primary-muted" : "border-border hover:bg-muted/50",
                      )}
                    >
                      <div className="flex items-start gap-2">
                        {artifact.kind === "approved" ? (
                          <FileJson className="mt-0.5 h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                        ) : (
                          <BookMarked className="mt-0.5 h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                        )}
                        <span className="min-w-0 flex-1 break-all font-mono text-xs font-medium">
                          {artifact.label}
                        </span>
                      </div>
                      {artifact.kind === "approved" ? (
                        <div className="mt-1.5 flex flex-wrap items-center gap-1.5 pl-5">
                          <KindBadge globalId={artifact.schema.registryGlobalId} />
                        </div>
                      ) : (
                        artifact.template.registryGlobalId != null && (
                          <div className="mt-1.5 flex flex-wrap items-center gap-1.5 pl-5">
                            <TemplateRegistrationBadge globalId={artifact.template.registryGlobalId} compact />
                          </div>
                        )
                      )}
                      <div className="mt-1 flex flex-wrap items-center gap-x-2 gap-y-0.5 pl-5 text-xs text-muted-foreground">
                        {artifact.kind === "approved" ? (
                          <>
                            <span>{artifact.schema.entity}</span>
                            <span aria-hidden>·</span>
                            <span>{artifactOwner?.flow.name ?? "flow removed"}</span>
                          </>
                        ) : (
                          <span>library template</span>
                        )}
                        <span aria-hidden>·</span>
                        <span>{relativeTime(artifact.at)}</span>
                      </div>
                    </button>
                  );
                })
              )}
            </div>
          </CardContent>
        </Card>

        {/* ----------------------------------------------------- detail pane */}
        <div className={schemaWorkspaceLayout.detailPane}>
          <Card className={cn(schemaWorkspaceLayout.detailCard, "shadow")}>
            {!selected ? (
              <CardContent className="flex h-full min-h-40 items-center justify-center p-8 text-center text-sm text-muted-foreground">
                {isLoading ? "Loading…" : "Select a schema or template on the left."}
              </CardContent>
            ) : selectedSchema ? (
              <>
                <CardHeader className="gap-3 space-y-0 border-b pb-4">
                  <div className="flex flex-wrap items-start justify-between gap-3">
                    <div className="min-w-0 space-y-1.5">
                      <CardTitle className="break-all font-mono text-base">{selectedSchema.subject}</CardTitle>
                      <div className="flex flex-wrap items-center gap-2">
                        <KindBadge globalId={activeApproval?.registryGlobalId ?? selectedSchema.registryGlobalId} />
                        <ProvenanceBadge provenance={activeApproval?.provenance ?? selectedSchema.provenance} />
                        {!isHistoricalApproval && selectedSchema.draftAvro !== undefined && (
                          <span className="inline-flex items-center gap-1 rounded-full border border-border bg-muted px-2.5 py-0.5 text-xs font-medium text-muted-foreground">
                            Draft saved {relativeTime(selectedSchema.draftUpdatedAt)} — not registered
                          </span>
                        )}
                      </div>
                      <CardDescription className="flex flex-wrap items-center gap-x-2 gap-y-1">
                        <span>
                          Entity <code className="font-mono text-foreground">{selectedSchema.entity}</code>
                        </span>
                        <span aria-hidden>·</span>
                        {owner ? (
                          <Link
                            to={`/flow-builder/${owner.flow.id}`}
                            className="text-foreground underline-offset-2 hover:underline"
                          >
                            {owner.flow.name}
                            {owner.blockName ? ` · ${owner.blockName}` : ""}
                          </Link>
                        ) : (
                          <span>owning flow removed — the schema survives it</span>
                        )}
                        <span aria-hidden>·</span>
                        <span>approved {relativeTime(activeApproval?.approvedAt ?? selectedSchema.approvedAt)}</span>
                      </CardDescription>
                    </div>

                    <div className="flex shrink-0 flex-wrap gap-2">
                      {!isHistoricalApproval && (
                        <>
                          <Button
                            size="sm"
                            disabled={!owner || !!buffer.rawError}
                            title={
                              !owner
                                ? "The owning flow no longer exists"
                                : buffer.rawError
                                  ? `Fix the raw Avro JSON first: ${buffer.rawError}`
                                  : "Carry the current buffer into the ceremony and register it as the next version"
                            }
                            onClick={() => void registerNewVersion()}
                          >
                            <Wand2 className="h-3.5 w-3.5" /> Register new version…
                          </Button>
                          <Button
                            size="sm"
                            variant="outline"
                            disabled={!!buffer.rawError || verifyMut.isPending}
                            title={
                              buffer.rawError
                                ? `Cannot verify: ${buffer.rawError}`
                                : "Structural validation plus a registry compatibility check against the latest registered version — registers nothing"
                            }
                            onClick={() => runVerify(selectedSchema.id, selectedSchema.subject)}
                          >
                            {verifyMut.isPending ? (
                              <Loader2 className="h-3.5 w-3.5 animate-spin" />
                            ) : (
                              <ShieldCheck className="h-3.5 w-3.5" />
                            )}
                            Verify
                          </Button>
                          <Button
                            size="sm"
                            variant="outline"
                            disabled={!buffer.dirty || !!buffer.rawError || saveApprovedDraftMut.isPending}
                            title={buffer.rawError ? `Cannot save: ${buffer.rawError}` : undefined}
                            onClick={handleSaveApprovedDraft}
                          >
                            {saveApprovedDraftMut.isPending ? (
                              <Loader2 className="h-3.5 w-3.5 animate-spin" />
                            ) : (
                              <Save className="h-3.5 w-3.5" />
                            )}
                            Save
                          </Button>
                        </>
                      )}
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={!owner}
                        title={
                          owner
                            ? "Re-run the ceremony from evidence, pre-filled with this schema"
                            : "The owning flow no longer exists"
                        }
                        onClick={() =>
                          owner && navigate(`/flow-builder/${owner.flow.id}?ceremony=${selectedSchema.blockId}`)
                        }
                      >
                        <Wand2 className="h-3.5 w-3.5" /> Re-run ceremony
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() => {
                          setSaveAsTemplateFor(selectedSchema);
                          setSaveAsTemplateName(`${selectedSchema.entity} (from ${selectedSchema.subject})`);
                        }}
                      >
                        <BookMarked className="h-3.5 w-3.5" /> Save as template
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={!activeApproval}
                        onClick={() =>
                          activeApproval &&
                          setDeleteTarget({ kind: "approved", schema: selectedSchema, approval: activeApproval })
                        }
                      >
                        <Trash2 className="h-3.5 w-3.5" /> Delete
                      </Button>
                    </div>
                  </div>

                  <div className="flex flex-wrap items-center gap-2">
                    <History className="h-3.5 w-3.5 text-muted-foreground" />
                    <Label className="text-xs text-muted-foreground">Approval history</Label>
                    <Select
                      value={String(activeApproval?.version ?? "")}
                      onValueChange={(value) => setApprovalVersion(Number(value))}
                    >
                      <SelectTrigger className="h-8 w-[19rem]">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        {[...approvals].reverse().map((approval) => (
                          <SelectItem key={approval.version} value={String(approval.version)}>
                            v{approval.version} · #{approval.registryGlobalId} ·{" "}
                            {PROVENANCE_META[approval.provenance].short} · {relativeTime(approval.approvedAt)}
                            {approval.supersededAt ? " · superseded" : " · current"}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                    {activeApproval?.prefilledFromLabel && (
                      <span className="text-xs text-muted-foreground">
                        pre-filled from template “{activeApproval.prefilledFromLabel}”
                      </span>
                    )}
                  </div>

                  {isHistoricalApproval && (
                    <p className="rounded-md border border-warning/30 bg-warning-muted p-2.5 text-xs text-muted-foreground">
                      Viewing approval v{activeApproval?.version}, superseded{" "}
                      {relativeTime(activeApproval?.supersededAt)}. History is read-only — it records what was
                      registered under global id #{activeApproval?.registryGlobalId}.
                    </p>
                  )}
                </CardHeader>

                <CardContent className={cn(schemaWorkspaceLayout.detailScroll, "pt-4")}>
                  {isHistoricalApproval ? (
                    <p className="mb-3 text-xs text-muted-foreground">
                      Read-only — this is history. Switch to the current approval above to edit it.
                    </p>
                  ) : (
                    <div className="mb-3 space-y-1 rounded-md border border-info/30 bg-info-muted p-2.5">
                      <p className="text-xs font-medium">Editing here is never registered by itself.</p>
                      <p className="text-xs text-muted-foreground">
                        Add or change fields freely — the same editor as a library template. “Save” keeps a draft
                        here, unregistered. “Register new version…” carries exactly this buffer into the ceremony
                        on{" "}
                        <span className="font-medium text-foreground">
                          {owner?.flow.name}
                          {owner?.blockName ? ` · ${owner.blockName}` : ""}
                        </span>
                        , and approval there registers it under a fresh global id — the approval you are viewing now
                        keeps its own id and stays readable in the history.
                      </p>
                    </div>
                  )}

                  {checkFor?.id === selectedSchema.id && <CheckPanel lines={checkFor.lines} />}
                  {verifyFor?.id === selectedSchema.id && (
                    <VerifyPanel result={verifyFor.result} onDismiss={() => setVerifyFor(null)} />
                  )}

                  <AvroEditorTabs
                    buffer={buffer}
                    readOnly={isHistoricalApproval}
                    emptyLabel="This approval's Avro could not be parsed."
                  />
                </CardContent>
              </>
            ) : selectedTemplate ? (
              <>
                <CardHeader className="gap-3 space-y-0 border-b pb-4">
                  <div className="flex flex-wrap items-start justify-between gap-3">
                    <div className="min-w-0 flex-1 space-y-2">
                      <Input
                        value={templateName}
                        onChange={(event) => setTemplateName(event.target.value)}
                        className="h-9 max-w-md text-base font-semibold"
                        aria-label="Template name"
                      />
                      <div className="flex flex-wrap items-center gap-2">
                        <TemplateRegistrationBadge
                          globalId={selectedTemplate.registryGlobalId}
                          version={selectedTemplate.registeredVersion}
                        />
                        <span className="text-xs text-muted-foreground">
                          updated {relativeTime(selectedTemplate.updatedAt)}
                        </span>
                        {templateDirty && (
                          <span className="text-xs font-medium text-warning">unsaved changes</span>
                        )}
                      </div>
                      {selectedTemplate.registeredSubject && (
                        <p className="font-mono text-xs text-muted-foreground">
                          {selectedTemplate.registeredSubject}
                          {selectedTemplate.registeredAt && (
                            <span className="font-sans"> · registered {relativeTime(selectedTemplate.registeredAt)}</span>
                          )}
                        </p>
                      )}
                      {templateEditedSinceRegistration && (
                        <p className="flex items-center gap-1 text-xs font-medium text-warning">
                          <AlertTriangle className="h-3 w-3 shrink-0" />
                          Edited since registration — Register again to publish a new version.
                        </p>
                      )}
                    </div>

                    <div className="flex shrink-0 flex-wrap gap-2">
                      <Button
                        size="sm"
                        disabled={isViewingOldTemplateVersion}
                        title={
                          isViewingOldTemplateVersion
                            ? `Viewing registered v${viewedRegistryVersion} — select v${selectedTemplate.registeredVersion} to edit and register the working copy.`
                            : "Check the shape, then pick the stream it is registered under"
                        }
                        onClick={() => {
                          const lines = checkAvroRecord(buffer.record, buffer.rawError ?? null);
                          setCheckFor({ id: selectedTemplate.id, lines });
                          if (lines.some((l) => l.level === "fail")) {
                            toast.error("This shape would be refused — see the checks below.");
                            return;
                          }
                          setCeremonyPickerFor(selectedTemplate);
                        }}
                      >
                        <Wand2 className="h-3.5 w-3.5" /> Check & register…
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={!!buffer.rawError || verifyMut.isPending}
                        title={
                          buffer.rawError
                            ? `Cannot verify: ${buffer.rawError}`
                            : "Structural validation plus a registry compatibility check against the latest registered version — registers nothing"
                        }
                        onClick={() => runVerify(selectedTemplate.id, selectedTemplate.registeredSubject)}
                      >
                        {verifyMut.isPending ? (
                          <Loader2 className="h-3.5 w-3.5 animate-spin" />
                        ) : (
                          <ShieldCheck className="h-3.5 w-3.5" />
                        )}
                        Verify
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={isViewingOldTemplateVersion}
                        title={
                          isViewingOldTemplateVersion
                            ? `Viewing registered v${viewedRegistryVersion} — select v${selectedTemplate.registeredVersion} to edit and register the working copy.`
                            : "Register the current buffer to the registry immediately — independent of any flow ceremony"
                        }
                        onClick={() => openRegisterForTemplate(selectedTemplate)}
                      >
                        <UploadCloud className="h-3.5 w-3.5" /> Register…
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() => setDeleteTarget({ kind: "template", template: selectedTemplate })}
                      >
                        <Trash2 className="h-3.5 w-3.5" /> Delete
                      </Button>
                    </div>
                  </div>

                  {templateIsRegistered && (
                    <div className="flex flex-wrap items-center gap-2">
                      <History className="h-3.5 w-3.5 text-muted-foreground" />
                      <Label className="text-xs text-muted-foreground">Registered version</Label>
                      <Select
                        value={String(viewedRegistryVersion ?? selectedTemplate.registeredVersion ?? "")}
                        onValueChange={(value) => {
                          const num = Number(value);
                          setViewedRegistryVersion(
                            selectedTemplate.registeredVersion != null && num === selectedTemplate.registeredVersion
                              ? null
                              : num,
                          );
                        }}
                      >
                        <SelectTrigger className="h-8 w-[10rem]">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          {[...templateRegistryVersions]
                            .sort((a, b) => b.version - a.version)
                            .map((rv) => (
                              <SelectItem key={rv.version} value={String(rv.version)}>
                                v{rv.version}
                                {rv.version === selectedTemplate.registeredVersion ? " · current" : ""}
                              </SelectItem>
                            ))}
                        </SelectContent>
                      </Select>
                      {(templateVersionsLoading || viewedTemplateVersionLoading) && (
                        <Loader2 className="h-3.5 w-3.5 animate-spin text-muted-foreground" />
                      )}
                    </div>
                  )}

                  {isViewingOldTemplateVersion && (
                    <p className="rounded-md border border-warning/30 bg-warning-muted p-2.5 text-xs text-muted-foreground">
                      Viewing registered v{viewedRegistryVersion} — read-only. Select v
                      {selectedTemplate.registeredVersion} to edit the working copy.
                    </p>
                  )}

                  <div className="space-y-1.5">
                    <Label className="text-xs text-muted-foreground">Description</Label>
                    <Textarea
                      value={templateDescription}
                      onChange={(event) => setTemplateDescription(event.target.value)}
                      placeholder="What this shape is for, and which sources it fits."
                      className="min-h-[3.5rem] text-sm"
                    />
                  </div>
                </CardHeader>

                <CardContent className={cn(schemaWorkspaceLayout.detailScroll, "pt-4")}>
                  {inferredReport?.templateId === selectedTemplate.id && (
                    <InferredReportPanel
                      report={inferredReport.report}
                      onDismiss={() => setInferredReport(null)}
                    />
                  )}
                  {checkFor?.id === selectedTemplate.id && <CheckPanel lines={checkFor.lines} />}
                  {verifyFor?.id === selectedTemplate.id && (
                    <VerifyPanel result={verifyFor.result} onDismiss={() => setVerifyFor(null)} />
                  )}
                  <AvroEditorTabs
                    buffer={buffer}
                    readOnly={isViewingOldTemplateVersion}
                    emptyLabel="This template's Avro could not be parsed."
                  />
                </CardContent>

                <div className="flex flex-wrap items-center justify-between gap-2 border-t p-4">
                  <span className="text-xs text-muted-foreground">
                    Templates are not registered and are bound to no flow — Save overwrites in place.
                  </span>
                  <div className="flex gap-2">
                    <Button
                      size="sm"
                      disabled={!templateDirty || !!buffer.rawError || saveMut.isPending || isViewingOldTemplateVersion}
                      title={
                        isViewingOldTemplateVersion
                          ? `Viewing registered v${viewedRegistryVersion} — select v${selectedTemplate.registeredVersion} to edit and save the working copy.`
                          : buffer.rawError
                            ? `Cannot save: ${buffer.rawError}`
                            : undefined
                      }
                      onClick={handleSaveTemplate}
                    >
                      {saveMut.isPending ? (
                        <Loader2 className="h-3.5 w-3.5 animate-spin" />
                      ) : (
                        <Save className="h-3.5 w-3.5" />
                      )}
                      Save
                    </Button>
                  </div>
                </div>
              </>
            ) : null}
          </Card>
        </div>
      </div>

      {/* ------------------------------------------------- new template dialog */}
      <Dialog open={newTemplateOpen} onOpenChange={setNewTemplateOpen}>
        <DialogContent className="max-w-lg">
          <DialogHeader>
            <DialogTitle>New library template</DialogTitle>
            <DialogDescription>
              Hand-author an Avro record, or infer one from real sample data. Either way it is not registered and is
              bound to no flow — a ceremony can pre-fill from it later.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-3">
            <div className="space-y-1.5">
              <Label>Name</Label>
              <Input
                value={newTemplateName}
                onChange={(event) => setNewTemplateName(event.target.value)}
                placeholder="Security incident envelope"
              />
              {newTemplateName.trim() && (
                <p className="text-xs text-muted-foreground">
                  Record name <code className="font-mono">{subjectToRecordName(newTemplateName.trim())}</code> ·
                  namespace <code className="font-mono">com.nif</code> (edit both in the template).
                </p>
              )}
            </div>
            <div className="space-y-1.5">
              <Label>Description (optional)</Label>
              <Textarea
                value={newTemplateDescription}
                onChange={(event) => setNewTemplateDescription(event.target.value)}
                className="min-h-[3.5rem]"
              />
            </div>

            <div className="space-y-1.5">
              <Label>Starting point</Label>
              <RadioGroup
                value={newTemplateMode}
                onValueChange={(value) => setNewTemplateMode(value as "empty" | "infer")}
                className="gap-2"
              >
                <label className="flex cursor-pointer items-start gap-2 rounded-md border p-2.5">
                  <RadioGroupItem value="empty" className="mt-0.5" />
                  <span>
                    <span className="flex items-center gap-1.5 text-sm font-medium">
                      <FileJson className="h-3.5 w-3.5" /> Start empty
                    </span>
                    <span className="text-xs text-muted-foreground">One blank record — build it up by hand.</span>
                  </span>
                </label>
                <label
                  className={cn(
                    "flex items-start gap-2 rounded-md border p-2.5",
                    newTemplateName.trim() ? "cursor-pointer" : "cursor-not-allowed opacity-60",
                  )}
                >
                  <RadioGroupItem value="infer" disabled={!newTemplateName.trim()} className="mt-0.5" />
                  <span>
                    <span className="flex items-center gap-1.5 text-sm font-medium">
                      <Wand2 className="h-3.5 w-3.5" /> Infer from sample files
                    </span>
                    <span className="text-xs text-muted-foreground">
                      {newTemplateName.trim()
                        ? "Upload JSON, NDJSON or CSV samples and start from a shape inferred from real data."
                        : "Name the template first — it becomes the inferred record's name."}
                    </span>
                  </span>
                </label>
              </RadioGroup>
            </div>

            {newTemplateMode === "infer" && (
              <div className="rounded-md border bg-muted/30 p-3">
                <SampleInferencePanel
                  compact
                  recordName={subjectToRecordName(newTemplateName.trim() || "record")}
                  namespace="com.nif"
                  onInferred={handleInferredTemplate}
                />
              </div>
            )}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setNewTemplateOpen(false)}>
              Cancel
            </Button>
            {newTemplateMode === "empty" && (
              <Button disabled={createMut.isPending} onClick={handleCreateTemplate}>
                {createMut.isPending && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
                Create template
              </Button>
            )}
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* --------------------------------------------- save approved as template */}
      <Dialog open={!!saveAsTemplateFor} onOpenChange={(open) => !open && setSaveAsTemplateFor(null)}>
        <DialogContent className="max-w-lg">
          <DialogHeader>
            <DialogTitle>Save as library template</DialogTitle>
            <DialogDescription>
              Copies <code className="font-mono">{saveAsTemplateFor?.subject}</code> into the library. The copy is
              not registered and stays independent of the approval it came from.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-1.5">
            <Label>Template name</Label>
            <Input value={saveAsTemplateName} onChange={(event) => setSaveAsTemplateName(event.target.value)} />
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setSaveAsTemplateFor(null)}>
              Cancel
            </Button>
            <Button
              disabled={saveAsTemplateMut.isPending || !saveAsTemplateName.trim()}
              onClick={() =>
                saveAsTemplateFor &&
                saveAsTemplateMut.mutate({
                  schemaId: saveAsTemplateFor.id,
                  name: saveAsTemplateName.trim(),
                })
              }
            >
              {saveAsTemplateMut.isPending && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
              Save to library
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* ------------------------------------------------------- independent register */}
      <Dialog open={!!registerFor} onOpenChange={(open) => !open && setRegisterFor(null)}>
        <DialogContent className="max-w-lg">
          <DialogHeader>
            <DialogTitle>Register “{registerFor?.name}”</DialogTitle>
            <DialogDescription>
              This registers the current buffer to the registry immediately, under the subject below —
              independently of any flow ceremony. It does not create an approved schema or bind this template to a
              flow.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-1.5">
            <Label>Subject</Label>
            <Input
              value={registerSubject}
              onChange={(event) => setRegisterSubject(event.target.value)}
              placeholder="topic-name-value"
              className="font-mono"
            />
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setRegisterFor(null)}>
              Cancel
            </Button>
            <Button
              disabled={
                registerMut.isPending || !registerSubject.trim() || !buffer.record || !!buffer.rawError
              }
              title={buffer.rawError ? `Cannot register: ${buffer.rawError}` : undefined}
              onClick={() =>
                registerFor &&
                buffer.record &&
                registerMut.mutate({
                  subject: registerSubject.trim(),
                  avro: buffer.record,
                  templateId: registerFor.id,
                })
              }
            >
              {registerMut.isPending && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
              Register
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* --------------------------------------------------- delete confirmation */}
      <Dialog open={!!deleteTarget} onOpenChange={(open) => !open && setDeleteTarget(null)}>
        <DialogContent className="max-w-md">
          {deleteTarget?.kind === "template" ? (
            <>
              <DialogHeader>
                <DialogTitle>Delete “{deleteTarget.template.name}”?</DialogTitle>
                <DialogDescription>
                  Templates are bound to nothing, so this is always allowed. Approvals that were pre-filled from it
                  keep the name as a frozen history line.
                </DialogDescription>
              </DialogHeader>
              <DialogFooter>
                <Button variant="outline" onClick={() => setDeleteTarget(null)}>
                  Cancel
                </Button>
                <Button
                  variant="destructive"
                  disabled={deleteMut.isPending}
                  onClick={() => deleteMut.mutate(deleteTarget.template.id)}
                >
                  {deleteMut.isPending && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
                  Delete template
                </Button>
              </DialogFooter>
            </>
          ) : deleteTarget?.kind === "approved" ? (
            (() => {
              const { schema, approval } = deleteTarget;
              const onlyVersionLeft = schema.approvals.length <= 1;
              const referencedBy = flowsReferencingApprovedSchema(schema);
              return (
                <>
                  <DialogHeader>
                    <DialogTitle>Delete from “{schema.subject}”?</DialogTitle>
                    <DialogDescription>
                      Choose exactly what gets removed — one approval from history, or the whole record.
                    </DialogDescription>
                  </DialogHeader>

                  {referencedBy.length > 0 && (
                    <p className="flex items-start gap-1.5 rounded-md border border-warning/30 bg-warning-muted p-2.5 text-xs text-muted-foreground">
                      <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-warning" />
                      <span>
                        Referenced by {referencedBy.join(", ")} — its kafka+connect block points at this schema.
                      </span>
                    </p>
                  )}

                  <div className="space-y-3">
                    <div className="space-y-1.5 rounded-md border p-3">
                      <p className="text-sm font-medium">Delete version v{approval.version} (current selection)</p>
                      <p className="text-xs text-muted-foreground">
                        {onlyVersionLeft
                          ? "This is the only remaining version — use “Delete entire schema” below instead."
                          : `Removes global id #${approval.registryGlobalId} from history and falls the selection back to the previous approval.`}
                      </p>
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={onlyVersionLeft || deleteApprovedVersionMut.isPending}
                        title={onlyVersionLeft ? "The only remaining version cannot be deleted on its own." : undefined}
                        onClick={() =>
                          deleteApprovedVersionMut.mutate({ schemaId: schema.id, version: approval.version })
                        }
                      >
                        {deleteApprovedVersionMut.isPending && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
                        Delete version v{approval.version}
                      </Button>
                    </div>

                    <div className="space-y-1.5 rounded-md border border-destructive/30 p-3">
                      <p className="text-sm font-medium">Delete entire schema</p>
                      <p className="text-xs text-muted-foreground">
                        Removes “{schema.subject}” and all {schema.approvals.length} approval(s), including current
                        global id #{schema.registryGlobalId}.
                      </p>
                      <Button
                        size="sm"
                        variant="destructive"
                        disabled={deleteApprovedSchemaMut.isPending}
                        onClick={() => deleteApprovedSchemaMut.mutate(schema.id)}
                      >
                        {deleteApprovedSchemaMut.isPending && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
                        Delete entire schema
                      </Button>
                    </div>
                  </div>

                  <DialogFooter>
                    <Button variant="outline" onClick={() => setDeleteTarget(null)}>
                      Cancel
                    </Button>
                  </DialogFooter>
                </>
              );
            })()
          ) : null}
        </DialogContent>
      </Dialog>

      {/* ------------------------------------------------ ceremony target picker */}
      <Dialog open={!!ceremonyPickerFor} onOpenChange={(open) => !open && setCeremonyPickerFor(null)}>
        <DialogContent className="max-h-[80vh] max-w-lg overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Register “{ceremonyPickerFor?.name}”</DialogTitle>
            <DialogDescription>
              A schema is registered under a stream's subject, not on its own — so registration needs a target. Pick the
              kafka+connect write this shape governs: the template pre-fills the ceremony's Review step, and Approve
              there is what registers it in the registry.
            </DialogDescription>
          </DialogHeader>

          {ceremonyTargets.length === 0 ? (
            <p className="rounded-md border border-dashed p-6 text-center text-sm text-muted-foreground">
              No kafka+connect blocks exist yet. Add one in a flow first — it is the only block a schema can be
              approved against.
            </p>
          ) : (
            <div className="space-y-4">
              {ceremonyTargets.map(({ flow, blocks }) => (
                <div key={flow.id} className="space-y-1.5">
                  <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">{flow.name}</p>
                  <div className="space-y-1.5">
                    {blocks.map((block) => (
                      <button
                        key={block.id}
                        type="button"
                        className="w-full rounded-md border p-2.5 text-left transition-colors hover:bg-muted/50"
                        onClick={() =>
                          ceremonyPickerFor &&
                          startCeremonyFromTemplate(flow.id, block.id, ceremonyPickerFor.id)
                        }
                      >
                        <div className="text-sm font-medium">{block.name}</div>
                        <div className="text-xs text-muted-foreground">
                          kafka+connect{block.entity ? ` · entity ${block.entity}` : " · no entity yet"} ·{" "}
                          {schemas.some((s) => s.flowId === flow.id && s.blockId === block.id)
                            ? "already approved — this re-runs the ceremony"
                            : "ceremony required"}
                        </div>
                      </button>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          )}
        </DialogContent>
      </Dialog>
    </AppLayout>
  );
};

export default Schemas;
