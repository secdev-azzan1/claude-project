// The shared Kafka Connect sink-configuration editor, mounted on both `kc` and
// `kafka_kc`.
//
// `locked` is passed PER CALL SITE and never computed here: kafka_kc freezes at
// deploy like the rest of the flow, while kc stays editable because Save is
// live. A lock computed inside the component would have to re-derive that
// exception and would get it wrong the moment either rule moves.
//
// Every kc/kafka_kc block's `block.config.sinkConfig` is now a COMPLETE Kafka
// Connect connector config — connector.class, topics, both converters, every
// iceberg.catalog.* key, credentials, all of it — either migrated in wholesale
// or typed by hand. This editor is a plain key/value property list over the
// WHOLE thing: no locked rows, no owned-key filtering, no derived values, no
// destination-service picker, no connector.class dropdown. What is stored is
// exactly what gets sent to Kafka Connect, byte for byte.
//
// Secrets: the backend returns a credential value as the literal string
// "[secret]" and restores the real value on save when it sees that placeholder
// unchanged. So this editor must round-trip "[secret]" untouched — never
// filtered, trimmed, or special-cased — and only overwrite it when the user
// types something else in its place.

import { useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { KvRows, type KvRow } from "./KvRows";
import type { FlowBlock } from "@/prototype/types";
import { AlertCircle, FileUp, ShieldAlert } from "lucide-react";

export interface SinkConfigEditorProps {
  block: FlowBlock;
  /** Pass the CALL SITE's lock — kafka_kc freezes at deploy, kc does not. */
  locked: boolean;
  onPatchConfig: (blockId: string, patch: Record<string, unknown>) => void;
}

interface ImportSummary {
  fileName: string;
  applied: string[];
  droppedNonScalar: string[];
  envelope: boolean;
}

export function SinkConfigEditor({ block, locked, onPatchConfig }: SinkConfigEditorProps) {
  const fileRef = useRef<HTMLInputElement>(null);
  const [importError, setImportError] = useState<string | null>(null);
  const [imported, setImported] = useState<ImportSummary | null>(null);
  // The persisted shape is a property MAP, but the editor is a row LIST: a
  // freshly added (still blank) row has no key to be stored under and would
  // vanish on the next render. So the row list is held here and projected down
  // to the map on every edit. Keyed by block id — the selected block changes on
  // every add, and rows from the previous block must not leak into the next.
  const [draftRows, setDraftRows] = useState<{ blockId: string; rows: KvRow[] } | null>(null);

  const sink = (block.config.sinkConfig as Record<string, string> | undefined) ?? {};

  const write = (next: Record<string, string>) => onPatchConfig(block.id, { sinkConfig: next });

  const persistedRows: KvRow[] = Object.entries(sink).map(([k, v]) => ({ k, v: String(v) }));
  const rows = draftRows?.blockId === block.id ? draftRows.rows : persistedRows;
  const hasSecretRow = rows.some((r) => r.v === "[secret]");

  const setRows = (next: KvRow[]) => {
    setDraftRows({ blockId: block.id, rows: next });
    const merged: Record<string, string> = {};
    for (const row of next) {
      const key = row.k.trim();
      if (!key) continue;
      merged[key] = row.v;
    }
    write(merged);
  };

  // ----------------------------------------------------------- json upload
  const handleFile = (file: File) => {
    setImportError(null);
    setImported(null);
    const reader = new FileReader();
    reader.onerror = () => setImportError(`Could not read "${file.name}".`);
    reader.onload = () => {
      let parsed: unknown;
      try {
        parsed = JSON.parse(String(reader.result ?? ""));
      } catch (err) {
        setImportError(`"${file.name}" is not valid JSON — ${err instanceof Error ? err.message : "parse failed"}.`);
        return;
      }
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        setImportError(`"${file.name}" must contain a JSON object of connector properties.`);
        return;
      }
      // Accept both a bare property map and the Connect REST envelope
      // ({ "name": ..., "config": { ... } }), which is what people actually have.
      const record = parsed as Record<string, unknown>;
      const envelope = typeof record.config === "object" && record.config !== null && !Array.isArray(record.config);
      const source = (envelope ? record.config : record) as Record<string, unknown>;

      const next: Record<string, string> = {};
      const applied: string[] = [];
      const droppedNonScalar: string[] = [];

      for (const [key, value] of Object.entries(source)) {
        const name = key.trim();
        if (!name) continue;
        if (typeof value === "object" && value !== null) {
          droppedNonScalar.push(name);
          continue;
        }
        const text = value === null || value === undefined ? "" : String(value);
        next[name] = text;
        applied.push(name);
      }

      if (applied.length === 0 && droppedNonScalar.length === 0) {
        setImportError(`"${file.name}" contained no connector properties.`);
        return;
      }

      write(next);
      setDraftRows(null); // the file replaces the row list wholesale
      setImported({ fileName: file.name, applied, droppedNonScalar, envelope });
    };
    reader.readAsText(file);
  };

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center gap-2">
        <input
          ref={fileRef}
          type="file"
          accept="application/json,.json"
          className="hidden"
          onChange={(e) => {
            const file = e.target.files?.[0];
            if (file) handleFile(file);
            // Allow re-selecting the same file after a failed parse.
            e.target.value = "";
          }}
        />
        <Button
          variant="outline"
          size="sm"
          className="h-9 gap-1.5 text-xs"
          disabled={locked}
          onClick={() => fileRef.current?.click()}
        >
          <FileUp className="h-3.5 w-3.5" /> Upload .json
        </Button>
      </div>

      {importError && (
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertTitle className="text-sm">Import failed</AlertTitle>
          <AlertDescription className="text-xs">{importError}</AlertDescription>
        </Alert>
      )}

      {imported && (
        <Alert>
          <FileUp className="h-4 w-4" />
          <AlertTitle className="text-sm">Imported {imported.fileName}</AlertTitle>
          <AlertDescription className="space-y-1 text-xs">
            <p>
              {imported.applied.length} propert{imported.applied.length === 1 ? "y" : "ies"} applied
              {imported.envelope ? ' (read from the file\'s "config" object — the Connect REST envelope)' : ""}, replacing
              the config wholesale. The editor below is the source of truth from here on; the file is not kept.
            </p>
            {imported.droppedNonScalar.length > 0 && (
              <p>
                Ignored because a connector property must be a single value:{" "}
                <code className="font-mono">{imported.droppedNonScalar.join(", ")}</code>.
              </p>
            )}
          </AlertDescription>
        </Alert>
      )}

      <KvRows
        label="Connector properties"
        rows={rows}
        onChange={setRows}
        locked={locked}
        keyPlaceholder="property name"
        valuePlaceholder="value"
        addLabel="Add property"
        keyClassName="w-64"
        emptyHint="No connector properties yet — upload a .json config or add properties by hand (connector.class, topics, converters, iceberg.catalog.*, credentials — everything Kafka Connect needs)."
      />

      {hasSecretRow && (
        <p className="flex items-start gap-1.5 text-xs text-muted-foreground">
          <ShieldAlert className="mt-0.5 h-3.5 w-3.5 shrink-0" />
          <span>
            A value shown as <code className="font-mono">[secret]</code> is already stored — it is kept unless you type a
            new value in its place.
          </span>
        </p>
      )}
    </div>
  );
}
