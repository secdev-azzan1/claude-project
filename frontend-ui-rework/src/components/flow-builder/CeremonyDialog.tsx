// The schema ceremony — Declare → Orchestrate → Review → Approve. The only
// path that creates a schema. Triggered from a kafka+connect block. Approve
// = register: if registration fails, the approval fails with it.
//
// The Review step is the real depth-5 Avro editor (`@/components/schema-editor`),
// not a flat name/type list: an `AvroRecord` is the single source of truth, the
// structured tree is derived from it every render, and a raw-JSON parse failure
// keeps the last valid record instead of destroying the draft.
//
// The "uploaded sample files" evidence path is real too: files are read with a
// `FileReader`, parsed by `@/prototype/inference`, resolved through a record
// path (the same `$.data.items[*]` syntax the http adapter uses), and inferred
// into an `AvroRecord`. The matched records stay in dialog state so the spec's
// rule — "every edit must still fit those samples before Approve unlocks" —
// runs against real data instead of being a no-op.
//
// The "live sample run" path goes through the SAME engine: the upstream probe's
// records are inferred across all of them (not just the first), and they are
// retained as the evidence, so nullability, type widening, the inference notes
// and the re-validation gate behave identically on both paths. The two differ
// in where the records came from, nothing else.

import { useEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Label } from "@/components/ui/label";
import { Progress } from "@/components/ui/progress";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import { Select, SelectContent, SelectGroup, SelectItem, SelectLabel, SelectTrigger, SelectValue } from "@/components/ui/select";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { AvroEditorTabs, recordToDisplayFields, SampleInferencePanel, useAvroBuffer } from "@/components/schema-editor";
import { normalizeAvroRecord, type AvroField as SchemaAvroField, type AvroRecord } from "@/lib/schemaEditor";
import { approveSchema, listSchemaTemplates } from "@/prototype/api";
import {
  inferAvroFromRecords,
  validateRecordsAgainstAvro,
  type InferenceReport,
} from "@/prototype/inference";
import { deriveTopicName } from "@/prototype/naming";
import type { ApprovedSchema, Flow, FlowBlock, SchemaProvenance, SchemaTemplate } from "@/prototype/types";
import { cn } from "@/lib/utils";
import {
  AlertCircle,
  CheckCircle2,
  FlaskConical,
  Loader2,
  PenLine,
  Upload,
} from "lucide-react";
import { toast } from "sonner";

const STEPS = ["Declare", "Orchestrate", "Review", "Approve"] as const;

/** How many matched records are retained for inference and re-validation. */
const MAX_SAMPLE_RECORDS = 500;

const NO_PREFILL = "none";
const schemaOption = (id: string) => `schema:${id}`;
const templateOption = (id: string) => `template:${id}`;

/** What Review starts from when there is no evidence at all to infer from. */
const PLACEHOLDER_FIELDS: Record<SchemaProvenance, SchemaAvroField[]> = {
  sample_run: [{ name: "value", type: "string" }],
  uploaded: [{ name: "value", type: "string" }],
  manual: [{ name: "id", type: "string" }],
};

export interface CeremonyDialogProps {
  flow: Flow;
  block: FlowBlock;
  open: boolean;
  approvedSchemas: ApprovedSchema[];
  /**
   * Library template to pre-fill from, i.e. the `?prefill=<templateId>` deep
   * link out of the Schemas page. When omitted the dialog falls back to reading
   * the query parameter itself, so the deep link works either way.
   */
  prefillTemplateId?: string | null;
  /**
   * An Avro record edited on the Schemas page and handed here to be registered.
   * It seeds Review directly and outranks every other pre-fill: the whole point
   * of that path is that this exact edit becomes the next version.
   */
  prefillDraft?: { rawAvro: string; label: string } | null;
  onOpenChange: (open: boolean) => void;
  onApproved: (schema: ApprovedSchema, entity: string) => void;
}

export function CeremonyDialog({
  flow,
  block,
  open,
  approvedSchemas,
  prefillTemplateId,
  prefillDraft,
  onOpenChange,
  onApproved,
}: CeremonyDialogProps) {
  const [searchParams] = useSearchParams();
  const urlPrefill = prefillTemplateId ?? searchParams.get("prefill");
  // A URL pre-fill is honoured once per id, so re-opening the ceremony for a
  // different block does not silently inherit it from a stale query string.
  const consumedPrefill = useRef<string | null>(null);
  /** `${prefillId}|${provenance}` at the moment the Review record was seeded. */
  const seedSignature = useRef<string | null>(null);

  const { data: templates = [] } = useQuery({
    queryKey: ["schemaTemplates"],
    queryFn: listSchemaTemplates,
    enabled: open,
  });

  const [step, setStep] = useState(0);
  const [entity, setEntity] = useState(block.entity ?? "");
  const [provenance, setProvenance] = useState<SchemaProvenance>("sample_run");
  const [prefillId, setPrefillId] = useState<string>(NO_PREFILL);
  const [progress, setProgress] = useState(0);
  const [collecting, setCollecting] = useState(false);
  const [approving, setApproving] = useState(false);

  // ---- uploaded evidence path (file handling itself lives in SampleInferencePanel)
  /** The records the schema was inferred from — retained as the evidence. */
  const [sampleRecords, setSampleRecords] = useState<unknown[]>([]);
  const [inferReport, setInferReport] = useState<InferenceReport | null>(null);
  /** Record path + file count as of the last successful upload inference, for the read-only Review/Approve summaries. */
  const [uploadRecordPath, setUploadRecordPath] = useState("");
  const [uploadFileCount, setUploadFileCount] = useState(0);

  // The live-sample path infers from the parent's probe. Writes other than http
  // no longer host a Test at all, so for those parents there is genuinely nothing
  // to sample and the step must say so instead of reporting collected evidence.
  const parentRecordCount = useMemo(() => {
    const parent = flow.blocks.find((b) => b.id === block.parentId);
    return parent?.testResult?.records?.length ?? 0;
  }, [flow.blocks, block.parentId]);

  const topicPreview = useMemo(() => {
    const probe = { ...block, entity: entity || block.entity };
    return deriveTopicName({ ...flow, blocks: flow.blocks.map((b) => (b.id === block.id ? probe : b)) }, probe).value;
  }, [flow, block, entity]);

  // The ceremony owns record identity: namespace comes from the governed topic
  // and the record name from the entity. Anything pre-filled adopts them, or
  // new approvals stop matching the `raw.<flow>` convention the seeds use.
  const recordName = entity.trim() || "record";
  const namespace = useMemo(() => topicPreview.split(".").slice(0, -1).join(".") || "raw", [topicPreview]);

  const buffer = useAvroBuffer(recordName);

  useEffect(() => {
    if (!open) return;
    // Re-running (or deep-linking from the Schemas browser) pre-fills from
    // this block's existing approved schema, unless a template was named.
    const existing = approvedSchemas.find((s) => s.flowId === flow.id && s.blockId === block.id);
    const fromUrl = urlPrefill && consumedPrefill.current !== urlPrefill ? urlPrefill : null;
    if (fromUrl) consumedPrefill.current = fromUrl;

    setStep(0);
    setEntity(block.entity ?? existing?.entity ?? "");
    // A handed-over edit is hand-authored by definition, so it declares itself
    // as manual rather than inheriting the old approval's evidence — the
    // records that backed v1 never saw these fields.
    setProvenance(fromUrl || prefillDraft ? "manual" : "sample_run");
    setPrefillId(fromUrl ? templateOption(fromUrl) : existing && !prefillDraft ? schemaOption(existing.id) : NO_PREFILL);
    setProgress(0);
    setCollecting(false);
    setSampleRecords([]);
    setInferReport(null);
    setUploadRecordPath("");
    setUploadFileCount(0);
    seedSignature.current = null;
    buffer.reset(null);

    if (prefillDraft) {
      // `recordName` is derived from the entity state this effect is still
      // setting, so it would be a render behind. Identity is re-stamped at
      // approval anyway; this only has to be a legal fallback name.
      const seedName = (block.entity ?? existing?.entity ?? "").trim() || "record";
      try {
        buffer.reset(normalizeAvroRecord(JSON.parse(prefillDraft.rawAvro), seedName));
        // Claim the seed so Back→Continue cannot overwrite the edit with the
        // stored approval it was derived from.
        seedSignature.current = `${NO_PREFILL}|manual`;
      } catch (error) {
        toast.error(
          `The edited schema could not be read: ${error instanceof Error ? error.message : "invalid Avro JSON"}.`,
        );
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, block.entity, block.id, flow.id, approvedSchemas, urlPrefill, prefillDraft]);

  const selectedTemplate: SchemaTemplate | null = prefillId.startsWith("template:")
    ? (templates.find((t) => t.id === prefillId.slice("template:".length)) ?? null)
    : null;
  const selectedApproved: ApprovedSchema | null = prefillId.startsWith("schema:")
    ? (approvedSchemas.find((s) => s.id === prefillId.slice("schema:".length)) ?? null)
    : null;

  const parsePrefill = (raw: string, label: string): AvroRecord | null => {
    try {
      return normalizeAvroRecord(JSON.parse(raw), recordName);
    } catch (error) {
      toast.error(
        `Could not read ${label}: ${error instanceof Error ? error.message : "invalid Avro JSON"}. Starting from the sample instead.`,
      );
      return null;
    }
  };

  const seedReviewRecord = () => {
    let record: AvroRecord | null = null;
    // Evidence is retained only when the record was actually inferred from it.
    // A pre-filled record was authored elsewhere, so holding it to these samples
    // would block Approve over a disagreement the user never claimed.
    let evidence: unknown[] = [];
    let report: InferenceReport | null = null;

    if (selectedTemplate) record = parsePrefill(selectedTemplate.rawAvro, `template "${selectedTemplate.name}"`);
    else if (selectedApproved) record = parsePrefill(selectedApproved.rawAvro, selectedApproved.subject);

    if (!record && provenance === "sample_run") {
      const parent = flow.blocks.find((b) => b.id === block.parentId);
      const probe = (parent?.testResult?.records ?? []).slice(0, MAX_SAMPLE_RECORDS);
      const inferred = inferAvroFromRecords(probe, recordName, namespace);
      if (inferred.record.fields.length > 0) {
        record = inferred.record;
        evidence = probe;
        report = inferred.report;
      }
    }

    // No pre-fill, no probe (or a hand-authored declaration): one placeholder
    // field to edit rather than an empty tree.
    if (!record) record = { type: "record", name: recordName, namespace, fields: PLACEHOLDER_FIELDS[provenance] };

    setSampleRecords(evidence);
    setInferReport(report);

    // Identity always comes from the ceremony, never from the pre-fill source.
    const stamped: AvroRecord = { ...record, name: recordName, namespace };
    try {
      buffer.reset(normalizeAvroRecord(stamped, recordName));
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Could not build a valid Avro record.");
      buffer.reset({ type: "record", name: recordName, namespace, fields: [] });
    }
  };

  // ------------------------------------------------ uploaded sample files
  // File reading, the record-path input, and the live match count all live in
  // SampleInferencePanel now — this just reacts to a successful inference the
  // same way `runInference` used to react to itself.
  const handleUploadInferred = (
    avro: unknown,
    report: InferenceReport,
    context: { records: unknown[]; recordPath: string; fileCount: number },
  ) => {
    try {
      buffer.reset(normalizeAvroRecord(avro, recordName));
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Could not build a valid Avro record from those samples.");
      return;
    }
    setSampleRecords(context.records);
    setInferReport(report);
    setUploadRecordPath(context.recordPath);
    setUploadFileCount(context.fileCount);
    // The inferred record is the seed for this declaration — do not let
    // Back→Continue overwrite it with the pre-fill or the live-sample guess.
    seedSignature.current = `${prefillId}|${provenance}`;
    setStep(2);
    toast.success(
      `Inferred ${report.fieldCount} field(s) from ${report.recordsSampled} sample record(s)${
        report.notes.length ? ` · ${report.notes.length} note(s)` : ""
      }`,
    );
  };

  /**
   * The spec's rule: every edit must still fit the retained samples. Records are
   * only ever retained by an inference path (uploaded files, or the live sample
   * run), so this covers exactly the declarations that claimed sample evidence.
   */
  const sampleCheck = useMemo(() => {
    if (sampleRecords.length === 0 || !buffer.record) return null;
    return validateRecordsAgainstAvro(sampleRecords, buffer.record);
  }, [sampleRecords, buffer.record]);

  /** What the retained evidence is called, so the copy never lies about it. */
  const evidenceLabel = provenance === "uploaded" ? "uploaded samples" : "sampled records";

  const startOrchestrate = () => {
    setStep(1);
    if (provenance === "sample_run") {
      setCollecting(true);
      setProgress(0);
      const timer = window.setInterval(() => {
        setProgress((p) => {
          if (p >= 100) {
            window.clearInterval(timer);
            setCollecting(false);
            return 100;
          }
          return p + 12;
        });
      }, 260);
    }
  };

  // Re-seeding on every Back→Continue would throw away hand-edits. Seed only
  // when there is nothing yet, or when the declarations that produced the last
  // seed (evidence path / pre-fill source) actually changed.
  const goReview = () => {
    const signature = `${prefillId}|${provenance}`;
    if (!buffer.record || seedSignature.current !== signature) {
      seedReviewRecord();
      seedSignature.current = signature;
    }
    setStep(2);
  };

  const approve = async () => {
    if (!buffer.record) {
      toast.error("There is no valid Avro record to approve.");
      return;
    }
    if (buffer.rawError) {
      toast.error(`Fix the raw Avro JSON first: ${buffer.rawError}`);
      return;
    }
    // The uploaded path's contract: the approved schema must still fit the
    // very samples it was inferred from.
    if (sampleCheck && !sampleCheck.ok) {
      toast.error(sampleCheck.summary);
      return;
    }
    setApproving(true);
    try {
      const record: AvroRecord = { ...buffer.record, name: recordName, namespace };
      const schema = await approveSchema(flow.id, block.id, {
        entity,
        provenance,
        // `fields` is write-only derived display output — the tree of record is
        // never reconstructed from it.
        fields: recordToDisplayFields(record),
        rawAvro: JSON.stringify(record, null, 2),
        ...(selectedTemplate
          ? { prefilledFromLabel: selectedTemplate.name }
          : prefillDraft
            ? { prefilledFromLabel: `edit of ${prefillDraft.label}` }
            : {}),
      });
      toast.success(`Approved & registered — ${schema.subject} (global id ${schema.registryGlobalId})`);
      onApproved(schema, entity);
      onOpenChange(false);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Approval failed");
    } finally {
      setApproving(false);
    }
  };

  const canDeclare = entity.trim().length > 0;
  // The uploaded path's "Continue" lives inside SampleInferencePanel itself
  // (its own "Infer schema" button gates on having matched records), so this
  // only needs to gate the sample_run path's footer button.
  const canOrchestrateNext = provenance === "sample_run" ? progress >= 100 : true;
  const fieldCount = buffer.record?.fields.length ?? 0;
  const samplesFail = !!sampleCheck && !sampleCheck.ok;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[90vh] max-w-4xl overflow-y-auto">
        <DialogHeader>
          <DialogTitle>Schema ceremony — {block.name}</DialogTitle>
          <DialogDescription>
            The only path that creates a schema. Approve = register; there is no evolution — re-run the ceremony deliberately.
          </DialogDescription>
        </DialogHeader>

        <div className="flex items-center gap-1">
          {STEPS.map((s, i) => (
            <div key={s} className="flex items-center gap-1">
              <span
                className={cn(
                  "rounded-full px-2.5 py-0.5 text-xs font-medium",
                  i === step ? "bg-primary text-primary-foreground" : i < step ? "bg-success-muted text-success" : "bg-muted text-muted-foreground",
                )}
              >
                {i + 1}. {s}
              </span>
              {i < STEPS.length - 1 && <span className="text-muted-foreground">→</span>}
            </div>
          ))}
        </div>

        {/*
          Always mounted — just hidden off-step — so the uploaded files and
          record path survive Back→Forward navigation and toggling the
          evidence-path radio, exactly like the ceremony-level state this
          replaced did. Only step 1 with "uploaded" evidence ever shows it.
        */}
        <div className={cn("space-y-3", !(step === 1 && provenance === "uploaded") && "hidden")}>
          <p className="text-sm">
            Upload sample files — they are read and parsed here in the browser, and never sent anywhere.
          </p>
          <SampleInferencePanel recordName={recordName} namespace={namespace} onInferred={handleUploadInferred} />
        </div>

        {step === 0 && (
          <div className="space-y-4 py-2">
            <div className="space-y-1.5">
              <Label>Entity — one word for what the data is</Label>
              <Input value={entity} onChange={(e) => setEntity(e.target.value)} placeholder="asset · incident · order…" className="max-w-xs" />
              <p className="text-xs text-muted-foreground">
                Governed topic: <code className="font-mono">{topicPreview || "raw.<flow>.<entity>"}</code> · subject{" "}
                <code className="font-mono">{topicPreview ? `${topicPreview}-value` : ""}</code>
              </p>
            </div>
            <div className="space-y-1.5">
              <Label>Evidence path</Label>
              <RadioGroup value={provenance} onValueChange={(v) => setProvenance(v as SchemaProvenance)} className="gap-2">
                <label className="flex cursor-pointer items-start gap-2 rounded-md border p-2.5">
                  <RadioGroupItem value="sample_run" className="mt-0.5" />
                  <span>
                    <span className="flex items-center gap-1.5 text-sm font-medium">
                      <FlaskConical className="h-3.5 w-3.5" /> Live sample run
                    </span>
                    <span className="text-xs text-muted-foreground">
                      Runs the real upstream chain into a throwaway <code>-schema-inference</code> topic (~10 messages).
                    </span>
                  </span>
                </label>
                <label className="flex cursor-pointer items-start gap-2 rounded-md border p-2.5">
                  <RadioGroupItem value="uploaded" className="mt-0.5" />
                  <span>
                    <span className="flex items-center gap-1.5 text-sm font-medium">
                      <Upload className="h-3.5 w-3.5" /> Uploaded sample files
                    </span>
                    <span className="text-xs text-muted-foreground">
                      JSON / NDJSON / CSV files you provide, with a record path into the structure — the schema is
                      inferred from the records that path lands on.
                    </span>
                  </span>
                </label>
                <label className="flex cursor-pointer items-start gap-2 rounded-md border p-2.5">
                  <RadioGroupItem value="manual" className="mt-0.5" />
                  <span>
                    <span className="flex items-center gap-1.5 text-sm font-medium">
                      <PenLine className="h-3.5 w-3.5" /> Author by hand
                    </span>
                    <span className="text-xs text-muted-foreground">Flagged forever as "manually authored — not sample-validated".</span>
                  </span>
                </label>
              </RadioGroup>
            </div>
            <div className="space-y-1.5">
              <Label>Pre-fill the Review step (optional)</Label>
              <Select value={prefillId} onValueChange={setPrefillId}>
                <SelectTrigger className="max-w-sm">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value={NO_PREFILL}>No pre-fill</SelectItem>
                  {approvedSchemas.length > 0 && (
                    <SelectGroup>
                      <SelectLabel>Approved schemas</SelectLabel>
                      {approvedSchemas.map((s) => (
                        <SelectItem key={s.id} value={schemaOption(s.id)}>
                          {s.subject}
                        </SelectItem>
                      ))}
                    </SelectGroup>
                  )}
                  {templates.length > 0 && (
                    <SelectGroup>
                      <SelectLabel>Library templates (unregistered)</SelectLabel>
                      {templates.map((t) => (
                        <SelectItem key={t.id} value={templateOption(t.id)}>
                          {t.name}
                        </SelectItem>
                      ))}
                    </SelectGroup>
                  )}
                </SelectContent>
              </Select>
              <p className="text-xs text-muted-foreground">
                {selectedTemplate
                  ? `Pre-filling from the library template "${selectedTemplate.name}" — the template stays unregistered; approval happens here.`
                  : "A pre-fill only seeds the Review step. Approval still registers a fresh schema for this stream."}
              </p>
            </div>
            <div className="flex justify-end">
              <Button onClick={startOrchestrate} disabled={!canDeclare} title={canDeclare ? undefined : "Set the entity label first"}>
                Continue
              </Button>
            </div>
          </div>
        )}

        {step === 1 && (
          <div className="space-y-4 py-2">
            {provenance === "sample_run" && (
              <div className="space-y-2">
                <p className="text-sm">
                  Collecting ~10 messages through the upstream chain into{" "}
                  <code className="font-mono text-xs">{topicPreview}-schema-inference</code> (throwaway, deleted afterwards).
                </p>
                <Progress value={progress} />
                <p className="text-xs text-muted-foreground">
                  {collecting
                    ? "Sampling… nothing is committed."
                    : parentRecordCount > 0
                      ? `Sample collected — the schema is inferred from all ${parentRecordCount} record(s) the upstream probe returned, and they are kept as the evidence every later edit is checked against.`
                      : "Sample collected, but the upstream block has no probe to read: Review starts from a placeholder field. Upload samples instead if you want the schema inferred from real data."}
                </p>
              </div>
            )}
            {provenance === "manual" && (
              <p className="text-sm text-muted-foreground">
                No evidence is collected. The schema you author will carry the provenance flag{" "}
                <Badge variant="outline" className="text-xs">manually authored — not sample-validated</Badge>.
              </p>
            )}
            <div className="flex justify-between">
              <Button variant="outline" onClick={() => setStep(0)}>
                Back
              </Button>
              {provenance !== "uploaded" && (
                <Button onClick={goReview} disabled={!canOrchestrateNext}>
                  Continue to Review
                </Button>
              )}
            </div>
          </div>
        )}

        {step === 2 && (
          <div className="min-w-0 space-y-3 py-2">
            {prefillDraft && (
              <Alert>
                <PenLine className="h-4 w-4" />
                <AlertTitle>Editing {prefillDraft.label}</AlertTitle>
                <AlertDescription className="text-xs">
                  These are your edits, carried over from the Schemas page. Approving registers them as a new version —
                  the previous approval stays in the history under its own global id, unchanged.
                </AlertDescription>
              </Alert>
            )}

            {inferReport && (
              <div className="space-y-1 rounded-md border bg-muted/40 p-2.5 shadow-inner">
                <p className="text-xs">
                  Inferred {inferReport.fieldCount} top-level field(s) from {inferReport.recordsSampled} record(s){" "}
                  {provenance === "uploaded" ? (
                    <>
                      at <code className="font-mono">{uploadRecordPath.trim() || "$"}</code> across {uploadFileCount}{" "}
                      uploaded file(s).
                    </>
                  ) : (
                    <>collected by the live sample run — every record was inspected, not just the first.</>
                  )}
                </p>
                {inferReport.notes.length > 0 ? (
                  <ul className="space-y-0.5">
                    {inferReport.notes.slice(0, 6).map((note, i) => (
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
                    {inferReport.notes.length > 6 && (
                      <li className="text-xs text-muted-foreground">
                        …and {inferReport.notes.length - 6} more inference note(s).
                      </li>
                    )}
                  </ul>
                ) : (
                  <p className="text-xs text-muted-foreground">
                    Every field was consistent across the samples — nothing had to be widened or made nullable.
                  </p>
                )}
              </div>
            )}

            {sampleCheck && (
              <Alert variant={sampleCheck.ok ? "default" : "destructive"}>
                {sampleCheck.ok ? <CheckCircle2 className="h-4 w-4 text-success" /> : <AlertCircle className="h-4 w-4" />}
                <AlertTitle>
                  {sampleCheck.ok ? `Edits still fit the ${evidenceLabel}` : `Edits no longer fit the ${evidenceLabel}`}
                </AlertTitle>
                <AlertDescription className="space-y-0.5 text-xs">
                  <p>{sampleCheck.summary}</p>
                  {!sampleCheck.ok && (
                    <>
                      {sampleCheck.failures.slice(1, 4).map((f) => (
                        <p key={`${f.index}-${f.field}`}>
                          record #{f.index + 1}: {f.reason}
                        </p>
                      ))}
                      <p>Approve stays locked until every retained sample record fits the schema again.</p>
                    </>
                  )}
                </AlertDescription>
              </Alert>
            )}

            <AvroEditorTabs
              buffer={buffer}
              showIdentity={false}
              bodyClassName="max-h-[55vh] overflow-y-auto pr-1"
              rawClassName="h-[55vh]"
              emptyLabel="No record was seeded. Go back and pick an evidence path."
            />
            <p className="text-xs text-muted-foreground">
              The record is registered as <code className="font-mono">{recordName}</code> in namespace{" "}
              <code className="font-mono">{namespace}</code> — both derived from the entity and the governed topic, and
              re-applied at approval.
            </p>
            <div className="flex justify-between">
              <Button variant="outline" onClick={() => setStep(1)}>
                Back
              </Button>
              <Button
                onClick={() => setStep(3)}
                disabled={fieldCount === 0 || !!buffer.rawError || samplesFail}
                title={
                  buffer.rawError
                    ? "The raw Avro JSON does not parse."
                    : fieldCount === 0
                      ? "A schema needs at least one field."
                      : samplesFail
                        ? sampleCheck?.summary
                        : undefined
                }
              >
                Continue to Approve
              </Button>
            </div>
          </div>
        )}

        {step === 3 && (
          <div className="space-y-4 py-2">
            <div className="rounded-md border p-3 text-sm">
              <p className="flex items-center gap-2 font-medium">
                <CheckCircle2 className="h-4 w-4 text-success" /> Ready to approve
              </p>
              <dl className="mt-2 grid grid-cols-[140px,1fr] gap-y-1 text-xs">
                <dt className="text-muted-foreground">Subject</dt>
                <dd className="font-mono">{topicPreview}-value</dd>
                <dt className="text-muted-foreground">Record</dt>
                <dd className="font-mono">
                  {namespace}.{recordName}
                </dd>
                <dt className="text-muted-foreground">Entity</dt>
                <dd>{entity}</dd>
                <dt className="text-muted-foreground">Top-level fields</dt>
                <dd>{fieldCount}</dd>
                <dt className="text-muted-foreground">Evidence</dt>
                <dd>
                  {provenance === "sample_run"
                    ? sampleRecords.length > 0
                      ? `live sample run — inferred from ${sampleRecords.length} record(s) off the upstream probe`
                      : parentRecordCount > 0
                        ? `live sample run — ${parentRecordCount} record(s) collected, but the record was pre-filled rather than inferred from them`
                        : "live sample run — the upstream block has no probe, so no records backed this"
                    : provenance === "uploaded"
                      ? `uploaded samples — ${uploadFileCount} file(s), ${sampleRecords.length} record(s) at ${uploadRecordPath.trim() || "$"}`
                      : "manually authored — not sample-validated"}
                </dd>
                {sampleCheck && (
                  <>
                    <dt className="text-muted-foreground">Sample re-validation</dt>
                    <dd>{sampleCheck.summary}</dd>
                  </>
                )}
                {selectedTemplate && (
                  <>
                    <dt className="text-muted-foreground">Pre-filled from</dt>
                    <dd>library template “{selectedTemplate.name}”</dd>
                  </>
                )}
                {prefillDraft && (
                  <>
                    <dt className="text-muted-foreground">Edited from</dt>
                    <dd>{prefillDraft.label} — this approval becomes its next version</dd>
                  </>
                )}
              </dl>
            </div>
            <p className="text-xs text-muted-foreground">
              Approve registers the schema in the active registry. If registration fails, the approval fails with it. The flow
              stays undeployable until this succeeds.
            </p>
            <div className="flex justify-between">
              <Button variant="outline" onClick={() => setStep(2)}>
                Back
              </Button>
              <Button onClick={approve} disabled={approving} className="gap-1.5">
                {approving && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
                Approve & register
              </Button>
            </div>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}
