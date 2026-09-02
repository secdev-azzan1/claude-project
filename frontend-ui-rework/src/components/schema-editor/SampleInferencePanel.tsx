// The reusable half of the "uploaded sample files" evidence path: file
// upload + record path + live match count + one "Infer schema" button. Pure
// UI plus the file-reading glue around `@/prototype/inference` — it owns no
// opinion about what happens after inference succeeds, that is entirely the
// caller's job via `onInferred`.
//
// Two callers today:
//   - CeremonyDialog's "uploaded sample files" evidence path (Declare →
//     Orchestrate), which advances its own step machinery from the callback.
//   - The Schemas page's "New template" dialog, which creates the template
//     from the inferred Avro instead of starting empty.

import { useEffect, useMemo, useRef, useState } from "react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { normalizeAvroRecord } from "@/lib/schemaEditor";
import { cn } from "@/lib/utils";
import {
  inferAvroFromRecords,
  mergeCsvHints,
  parseSampleFile,
  resolveRecordPath,
  suggestRecordPaths,
  type InferenceReport,
  type ParseSampleResult,
} from "@/prototype/inference";
import { AlertCircle, FileJson, FileUp, Loader2, Wand2, X } from "lucide-react";
import { toast } from "sonner";

/** Sample files are read in the browser — big ones are refused, not truncated. */
const MAX_SAMPLE_BYTES = 2_000_000;
/** How many matched records are kept for inference (and any later re-validation). */
const MAX_SAMPLE_RECORDS = 500;

interface SampleFile {
  id: string;
  name: string;
  status: "reading" | "ok" | "error";
  parsed?: ParseSampleResult;
  error?: string;
}

export interface SampleInferencePanelProps {
  /** Record identity the inferred Avro is stamped with. */
  recordName?: string;
  namespace?: string;
  /**
   * Fired once "Infer schema" succeeds. `avro` is already a normalised
   * `AvroRecord` (typed `unknown` here so this component stays decoupled from
   * callers that don't otherwise import `@/lib/schemaEditor`). `context`
   * carries the matched records — retain them if the caller needs to
   * re-validate later edits against this evidence — plus the record path and
   * file count actually used, for callers that display them after this panel
   * is gone from view.
   */
  onInferred: (
    avro: unknown,
    report: InferenceReport,
    context: { records: unknown[]; recordPath: string; fileCount: number },
  ) => void;
  /** Tighter spacing for inline use, e.g. inside another dialog. */
  compact?: boolean;
}

export function SampleInferencePanel({
  recordName = "Record",
  namespace = "com.nif",
  onInferred,
  compact = false,
}: SampleInferencePanelProps) {
  const [sampleFiles, setSampleFiles] = useState<SampleFile[]>([]);
  const [recordPath, setRecordPath] = useState("");
  const fileInputRef = useRef<HTMLInputElement>(null);
  /** A suggested path is offered once; after that the field is the user's. */
  const suggestedPathApplied = useRef(false);

  const patchFile = (id: string, patch: Partial<SampleFile>) =>
    setSampleFiles((prev) => prev.map((f) => (f.id === id ? { ...f, ...patch } : f)));

  const readFiles = (files: FileList) => {
    for (const file of Array.from(files)) {
      const id = `${file.name}-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
      setSampleFiles((prev) => [...prev, { id, name: file.name, status: "reading" }]);

      if (file.size > MAX_SAMPLE_BYTES) {
        patchFile(id, {
          status: "error",
          error: `"${file.name}" is ${(file.size / 1_000_000).toFixed(1)} MB — samples are read in the browser, so the limit is 2 MB. Upload a slice of it.`,
        });
        continue;
      }

      const reader = new FileReader();
      reader.onerror = () => patchFile(id, { status: "error", error: `Could not read "${file.name}".` });
      reader.onload = () => {
        const parsed = parseSampleFile(file.name, String(reader.result ?? ""));
        if (parsed.ok) patchFile(id, { status: "ok", parsed });
        else {
          patchFile(id, { status: "error", error: parsed.error });
          toast.error(parsed.error);
        }
      };
      reader.readAsText(file);
    }
  };

  const readyFiles = useMemo(
    () => sampleFiles.filter((f) => f.status === "ok" && f.parsed?.ok) as (SampleFile & { parsed: ParseSampleResult })[],
    [sampleFiles],
  );

  /** Candidate record paths across every parsed file, deepest first. */
  const pathSuggestions = useMemo(() => {
    const out: string[] = [];
    for (const file of readyFiles) {
      if (!file.parsed.ok) continue;
      for (const path of suggestRecordPaths(file.parsed.value)) if (!out.includes(path)) out.push(path);
    }
    return out.slice(0, 8);
  }, [readyFiles]);

  // Offer the shallowest suggestion once, when the file's top level is a
  // wrapper object rather than records — the common "it's nested deep" case.
  useEffect(() => {
    if (suggestedPathApplied.current || recordPath !== "" || pathSuggestions.length === 0) return;
    const topLevelIsRecords = readyFiles.some((f) => f.parsed.ok && Array.isArray(f.parsed.value));
    if (topLevelIsRecords) return;
    setRecordPath(pathSuggestions[pathSuggestions.length - 1]);
    suggestedPathApplied.current = true;
  }, [pathSuggestions, readyFiles, recordPath]);

  /** Live resolution of the record path across every parsed file. */
  const resolution = useMemo(() => {
    if (readyFiles.length === 0) return null;
    const records: unknown[] = [];
    const errors: { name: string; error: string }[] = [];
    for (const file of readyFiles) {
      if (!file.parsed.ok) continue;
      const result = resolveRecordPath(file.parsed.value, recordPath);
      if (result.ok) records.push(...result.records);
      else errors.push({ name: file.name, error: result.error ?? "Could not resolve the record path." });
    }
    return { records, kept: records.slice(0, MAX_SAMPLE_RECORDS), errors };
  }, [readyFiles, recordPath]);

  const columnHints = useMemo(() => mergeCsvHints(readyFiles.map((f) => f.parsed)), [readyFiles]);

  const matchedCount = resolution?.kept.length ?? 0;

  const runInference = () => {
    const records = resolution?.kept ?? [];
    if (records.length === 0) {
      toast.error("No records matched the record path — nothing to infer from.");
      return;
    }
    const { record, report } = inferAvroFromRecords(records, recordName, namespace, {
      stringColumnHints: columnHints,
    });
    try {
      const normalized = normalizeAvroRecord({ ...record, name: recordName, namespace }, recordName);
      onInferred(normalized, report, { records, recordPath, fileCount: readyFiles.length });
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Could not build a valid Avro record from those samples.");
    }
  };

  return (
    <div className={cn("space-y-3", compact && "space-y-2")}>
      <div className="space-y-1.5">
        <input
          ref={fileInputRef}
          type="file"
          multiple
          accept=".json,.ndjson,.jsonl,.csv,.txt,application/json,text/csv,text/plain"
          className="hidden"
          onChange={(e) => {
            if (e.target.files?.length) readFiles(e.target.files);
            // Allow re-selecting the same file after a failed parse.
            e.target.value = "";
          }}
        />
        <Button type="button" variant="outline" size="sm" onClick={() => fileInputRef.current?.click()}>
          <FileUp className="mr-1.5 h-3.5 w-3.5" /> Add sample files
        </Button>
        <p className="text-xs text-muted-foreground">
          JSON, NDJSON (one object per line) and CSV, up to 2 MB each. XML and XLSX are refused with a note rather
          than half-parsed — export those as CSV or JSON first.
        </p>
      </div>

      {sampleFiles.length > 0 && (
        <ul className="space-y-1.5">
          {sampleFiles.map((file) => {
            const parsed = file.parsed?.ok ? file.parsed : null;
            const topLevel = parsed
              ? Array.isArray(parsed.value)
                ? `${(parsed.value as unknown[]).length} top-level item(s)`
                : "1 top-level object"
              : null;
            return (
              <li
                key={file.id}
                className={cn(
                  "flex items-start gap-2 rounded-md border p-2 shadow-sm",
                  file.status === "error" && "border-destructive/40 bg-destructive/5",
                )}
              >
                <FileJson className="mt-0.5 h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                <div className="min-w-0 flex-1 space-y-0.5">
                  <span className="flex flex-wrap items-center gap-1.5 text-sm">
                    <span className="truncate font-mono text-xs">{file.name}</span>
                    {file.status === "reading" && (
                      <span className="flex items-center gap-1 text-xs text-muted-foreground">
                        <Loader2 className="h-3 w-3 animate-spin" /> reading…
                      </span>
                    )}
                    {parsed && (
                      <>
                        <Badge variant="outline" className="text-xs uppercase">
                          {parsed.format}
                        </Badge>
                        <span className="text-xs text-muted-foreground">{topLevel}</span>
                      </>
                    )}
                  </span>
                  {file.status === "error" && <p className="text-xs text-destructive">{file.error}</p>}
                  {parsed?.notes?.map((note) => (
                    <p key={note} className="text-xs text-muted-foreground">
                      {note}
                    </p>
                  ))}
                </div>
                <button
                  type="button"
                  className="rounded p-0.5 text-muted-foreground hover:bg-muted hover:text-foreground"
                  title={`Remove ${file.name}`}
                  onClick={() => setSampleFiles((prev) => prev.filter((f) => f.id !== file.id))}
                >
                  <X className="h-3.5 w-3.5" />
                </button>
              </li>
            );
          })}
        </ul>
      )}

      {readyFiles.length > 0 && (
        <div className="space-y-1.5">
          <Label>Record path — where the records live inside the file</Label>
          <Input
            value={recordPath}
            onChange={(e) => {
              suggestedPathApplied.current = true;
              setRecordPath(e.target.value);
            }}
            placeholder="$.result.records[*] — blank means the file's top level"
            className="max-w-md font-mono text-xs"
          />
          {pathSuggestions.length > 0 && (
            <div className="flex flex-wrap items-center gap-1.5">
              <span className="text-xs text-muted-foreground">Found in these files:</span>
              {pathSuggestions.map((path) => (
                <button
                  key={path}
                  type="button"
                  onClick={() => {
                    suggestedPathApplied.current = true;
                    setRecordPath(path);
                  }}
                >
                  <Badge
                    variant={recordPath === path ? "default" : "outline"}
                    className={cn("cursor-pointer font-mono text-xs", recordPath !== path && "hover:bg-accent")}
                  >
                    {path}
                  </Badge>
                </button>
              ))}
              {recordPath !== "" && (
                <button
                  type="button"
                  onClick={() => {
                    suggestedPathApplied.current = true;
                    setRecordPath("");
                  }}
                >
                  <Badge variant="outline" className="cursor-pointer text-xs hover:bg-accent">
                    use the top level
                  </Badge>
                </button>
              )}
            </div>
          )}
          <p className="text-xs text-muted-foreground">
            Same syntax as the http adapter's response record path — <code className="font-mono">$.data.items[*]</code>{" "}
            or <code className="font-mono">$.result.records[*].payload</code>.
          </p>

          {resolution && resolution.errors.length > 0 && (
            <Alert variant="destructive">
              <AlertCircle className="h-4 w-4" />
              <AlertTitle>The record path does not fit every file</AlertTitle>
              <AlertDescription className="space-y-0.5 text-xs">
                {resolution.errors.map((e) => (
                  <p key={e.name}>
                    <span className="font-mono">{e.name}</span> — {e.error}
                  </p>
                ))}
              </AlertDescription>
            </Alert>
          )}

          <p className="text-xs text-muted-foreground">
            {matchedCount > 0 ? (
              <>
                <span className="font-medium text-foreground">{resolution?.records.length} record(s) matched</span>{" "}
                across {readyFiles.length} file(s)
                {(resolution?.records.length ?? 0) > MAX_SAMPLE_RECORDS
                  ? ` — the first ${MAX_SAMPLE_RECORDS} are used for inference and kept as the evidence.`
                  : " — all of them are inferred from and kept as the evidence."}
              </>
            ) : (
              "No records matched yet. Pick a suggested path, or leave the field blank to use the file's top level."
            )}
          </p>

          <div className="flex justify-end">
            <Button
              type="button"
              onClick={runInference}
              disabled={matchedCount === 0}
              size="sm"
              className="gap-1.5"
              title={matchedCount === 0 ? "No records match the record path yet." : undefined}
            >
              <Wand2 className="h-3.5 w-3.5" />
              Infer schema
            </Button>
          </div>
        </div>
      )}
    </div>
  );
}
