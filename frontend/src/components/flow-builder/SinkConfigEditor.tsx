// The shared Kafka Connect sink-configuration editor, mounted on both `kc` and
// `kafka_kc`.
//
// `locked` is passed PER CALL SITE and never computed here: kafka_kc freezes at
// deploy like the rest of the flow, while kc stays editable because Save is
// live. A lock computed inside the component would have to re-derive that
// exception and would get it wrong the moment either rule moves.
//
// Two invariants worth stating out loud:
//  · Platform-owned keys (`topics`, the converters, and — on lakehouse sinks —
//    the table name and auto-create/evolution) are COMPUTED AT RENDER and never
//    persisted. `topics` in particular is read from `deriveTopicName`, so it
//    cannot go stale when an entity, a branch label or an override changes.
//  · `connector.class` is checked against the installed-plugin catalog, but the
//    catalog is a convenience, not the boundary: any Connect plugin installed on
//    the cluster is a legal sink, and the UI cannot know every one of them. So
//    the picker has a "Custom connector class…" option and the class can be
//    typed in full. A class outside the catalog is flagged — clearly, once —
//    rather than refused, because "this UI has not heard of it" is not the same
//    claim as "the cluster does not have it".

import { useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { KvRows, type KvRow, type LockedKvRow } from "./KvRows";
import { CONNECT_PLUGIN_CATALOG } from "@/prototype/seeds";
import { deriveTopicName, tableName } from "@/prototype/naming";
import type { AppService, ConnectPlugin, Flow, FlowBlock } from "@/prototype/types";
import { cn } from "@/lib/utils";
import { AlertCircle, FileUp, Package, Plug, Trash2 } from "lucide-react";
import { toast } from "sonner";

/** Keys the platform owns for every sink, whatever the plugin. */
const ALWAYS_OWNED = ["topics", "key.converter", "value.converter", "value.converter.schema.registry.url"];

/** Extra keys the platform owns once the plugin is a recognized lakehouse sink. */
const LAKEHOUSE_OWNED = ["iceberg.tables", "iceberg.tables.auto-create-enabled", "iceberg.tables.evolve-schema-enabled"];

/**
 * The connection properties the DESTINATION SERVICE answers, per sink kind.
 *
 * This is the seam that made the section feel like two disagreeing forms: the
 * service knew the URL and the property list asked for it again, with nothing
 * saying which one wins. They are the same fact, so the service states it and
 * the property becomes a derived row like `topics` — visible, locked, never
 * stored, and impossible to contradict.
 */
function serviceDerived(service: AppService | undefined): { k: string; v: string; reason: string }[] {
  if (!service) return [];
  const cfg = service.config ?? {};
  const kind = typeof cfg.kind === "string" ? cfg.kind : "";
  const from = `Answered by the destination service "${service.name}" (rev ${service.revision}) — change it there and every sink using it follows.`;
  if (kind === "iceberg_catalog") {
    return [
      { k: "iceberg.catalog.uri", v: String(cfg.catalogUrl ?? ""), reason: from },
      { k: "iceberg.catalog.warehouse", v: String(cfg.warehouse ?? ""), reason: from },
    ];
  }
  if (kind === "opensearch") {
    const rows = [{ k: "connection.url", v: String(cfg.url ?? ""), reason: from }];
    if (cfg.indexPrefix) rows.push({ k: "index.prefix", v: String(cfg.indexPrefix), reason: from });
    return rows;
  }
  return [];
}

function ownedKeys(plugin: ConnectPlugin | undefined, service: AppService | undefined): string[] {
  const derived = serviceDerived(service).map((r) => r.k);
  return plugin?.lakehouseSink ? [...ALWAYS_OWNED, ...LAKEHOUSE_OWNED, ...derived] : [...ALWAYS_OWNED, ...derived];
}

/**
 * The topic this sink reads — computed, never stored. kc subscribes to a topic
 * node by id; kafka_kc owns the topic it writes, so its name comes out of the
 * naming walk (which already honours an R7 override).
 */
function topicsValue(flow: Flow, block: FlowBlock): string {
  if (block.adapter === "kc") {
    const topic = flow.topics.find((t) => t.id === block.config.attachTopicId);
    return topic?.name ?? "";
  }
  return deriveTopicName(flow, block).value;
}

export interface SinkConfigEditorProps {
  flow: Flow;
  block: FlowBlock;
  /** Pass the CALL SITE's lock — kafka_kc freezes at deploy, kc does not. */
  locked: boolean;
  services: AppService[];
  onPatchConfig: (blockId: string, patch: Record<string, unknown>) => void;
  /** Opens the Entity & derived names section — the topic override lives there. */
  onGoToNames?: () => void;
  /**
   * The destination-service picker, rendered by the form and mounted here. The
   * service is not separate from the sink configuration: it IS the connection
   * half of it, and the properties it fills are shown below as derived rows.
   */
  servicePicker?: React.ReactNode;
}

interface ImportSummary {
  fileName: string;
  applied: string[];
  lockedIgnored: string[];
  droppedNonScalar: string[];
  connectorClass: string | null;
  classInstalled: boolean;
  envelope: boolean;
}

/** The picker's escape hatch — never a real connector class. */
const CUSTOM_CLASS = "__custom__";

export function SinkConfigEditor({
  flow,
  block,
  locked,
  services,
  onPatchConfig,
  onGoToNames,
  servicePicker,
}: SinkConfigEditorProps) {
  const fileRef = useRef<HTMLInputElement>(null);
  const [importError, setImportError] = useState<string | null>(null);
  const [imported, setImported] = useState<ImportSummary | null>(null);
  /**
   * "Custom" is a mode, not a value: an empty custom class and no class at all
   * are the same persisted state, so the intent has to be held here or the
   * text field would close itself on the first keystroke that clears it.
   */
  const [customMode, setCustomMode] = useState<{ blockId: string; on: boolean } | null>(null);
  // The persisted shape is a property MAP, but the editor is a row LIST: a
  // freshly added (still blank) row has no key to be stored under and would
  // vanish on the next render. So the row list is held here and projected down
  // to the map on every edit. Keyed by block id — the selected block changes on
  // every add, and rows from the previous block must not leak into the next.
  const [draftRows, setDraftRows] = useState<{ blockId: string; rows: KvRow[] } | null>(null);

  const sink = (block.config.sinkConfig as Record<string, string> | undefined) ?? {};
  const connectorClass = typeof sink["connector.class"] === "string" ? sink["connector.class"] : "";
  const plugin = CONNECT_PLUGIN_CATALOG.find((p) => p.connectorClass === connectorClass);
  // A class outside the catalog puts the editor in custom mode on its own, so a
  // config that arrived by upload or from an older build opens in the shape that
  // can actually edit it.
  const custom = customMode?.blockId === block.id ? customMode.on : !!connectorClass && !plugin;
  const outsideCatalog = !!connectorClass && !plugin;
  // kafka_kc keeps a copy of the id in config; either is the same selection.
  const serviceId = block.serviceId ?? (block.config.sinkServiceId as string | undefined) ?? null;
  const service = services.find((s) => s.id === serviceId);
  const derivedRows = serviceDerived(service);
  const owned = ownedKeys(plugin, service);

  const write = (next: Record<string, string>) => onPatchConfig(block.id, { sinkConfig: next });

  // A persisted platform-owned key can only exist if it was hand-crafted or
  // survived an older build. Surface it — validation refuses to deploy with it.
  const strayOwned = Object.keys(sink).filter((k) => owned.includes(k));

  // Everything the user genuinely owns: not connector.class (its own control),
  // not a platform-owned key (rendered locked below).
  const persistedRows: KvRow[] = Object.entries(sink)
    .filter(([k]) => k !== "connector.class" && !owned.includes(k))
    .map(([k, v]) => ({ k, v: String(v) }));
  const rows = draftRows?.blockId === block.id ? draftRows.rows : persistedRows;
  // A typed row whose key the platform owns is dropped at write time; say so
  // rather than letting it look saved.
  const typedOwned = [...new Set(rows.map((r) => r.k.trim()).filter((k) => k && owned.includes(k)))];

  const setRows = (next: KvRow[]) => {
    setDraftRows({ blockId: block.id, rows: next });
    const merged: Record<string, string> = {};
    if (connectorClass) merged["connector.class"] = connectorClass;
    // Stray owned keys are preserved rather than silently swept up by an
    // unrelated edit — removing them is an explicit, explained action.
    for (const key of strayOwned) merged[key] = sink[key];
    for (const row of next) {
      const key = row.k.trim();
      if (!key || key === "connector.class" || owned.includes(key)) continue;
      merged[key] = row.v;
    }
    write(merged);
  };

  const setConnectorClass = (value: string) => {
    write({ ...sink, "connector.class": value });
  };

  /** The picker writes a class; "Custom…" only switches the control. */
  const handlePickClass = (value: string) => {
    if (value === CUSTOM_CLASS) {
      setCustomMode({ blockId: block.id, on: true });
      return;
    }
    setCustomMode({ blockId: block.id, on: false });
    setConnectorClass(value);
  };

  const clearStrayOwned = () => {
    const next = { ...sink };
    for (const key of strayOwned) delete next[key];
    write(next);
    toast.success(`Removed ${strayOwned.join(", ")} — the platform derives ${strayOwned.length > 1 ? "those" : "that"} at deploy.`);
  };

  // ------------------------------------------------------------- locked rows
  const lockedRows: LockedKvRow[] = [];
  const topics = topicsValue(flow, block);
  const overridden = block.adapter === "kafka_kc" && !!block.topicOverride?.trim();
  lockedRows.push({
    k: "topics",
    v: topics || "(no topic yet)",
    reason: overridden
      ? "Your custom topic name drives this value (R7). It is still derived at deploy rather than stored here, so it can never go stale."
      : block.adapter === "kc"
        ? "Bound to the topic this subscription is attached to — change the subscription above and this follows."
        : "Bound to the governed topic this block writes. It is derived at deploy, so renaming the entity or the flow never leaves a stale copy behind.",
    action: onGoToNames && block.adapter === "kafka_kc" && (
      <Button variant="ghost" size="sm" className="h-6 shrink-0 px-2 text-xs" onClick={onGoToNames}>
        {overridden ? "Edit name" : "Override name"}
      </Button>
    ),
  });

  if (block.adapter === "kafka_kc") {
    lockedRows.push({
      k: "key.converter",
      v: "org.apache.kafka.connect.storage.StringConverter",
      reason: "The governed write's key encoding is fixed by the platform.",
    });
    lockedRows.push({
      k: "value.converter",
      v: "io.confluent.connect.avro.AvroConverter",
      reason: "kafka+connect is the only place Avro exists — the converter follows the approved schema and is not configurable.",
    });
  } else {
    lockedRows.push({
      k: "value.converter",
      v: "org.apache.kafka.connect.converters.ByteArrayConverter",
      reason: "A kc subscription delivers the topic's bytes untouched — no transforms, no schema surface, so no decoding converter.",
    });
  }

  for (const row of derivedRows) lockedRows.push(row);

  if (plugin?.lakehouseSink) {
    lockedRows.push({
      k: "iceberg.tables",
      v: block.entity ? tableName(flow.name, block.entity) : "bronze.<flow>.<entity>__raw",
      reason: "Lakehouse table names stay derived — flow name and entity label decide them, and they freeze at deploy.",
    });
    lockedRows.push({
      k: "iceberg.tables.auto-create-enabled",
      v: "true",
      reason: "The platform creates the bronze table with the sink, as one unit.",
    });
    lockedRows.push({
      k: "iceberg.tables.evolve-schema-enabled",
      v: "false",
      reason: "No evolution — a schema change always re-runs the ceremony rather than mutating a live table.",
    });
  }

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
      const lockedIgnored: string[] = [];
      const droppedNonScalar: string[] = [];
      let importedClass: string | null = null;

      for (const [key, value] of Object.entries(source)) {
        const name = key.trim();
        if (!name) continue;
        if (typeof value === "object" && value !== null) {
          droppedNonScalar.push(name);
          continue;
        }
        const text = value === null || value === undefined ? "" : String(value);
        // Owned keys are recomputed from the flow at render — importing one
        // would persist a value that goes stale on the next rename.
        if (owned.includes(name)) {
          lockedIgnored.push(name);
          continue;
        }
        if (name === "connector.class") importedClass = text;
        next[name] = text;
        applied.push(name);
      }

      if (applied.length === 0 && lockedIgnored.length === 0 && droppedNonScalar.length === 0) {
        setImportError(`"${file.name}" contained no connector properties.`);
        return;
      }

      write(next);
      setDraftRows(null); // the file replaces the row list wholesale
      const classInstalled = !!importedClass && CONNECT_PLUGIN_CATALOG.some((p) => p.connectorClass === importedClass);
      setImported({
        fileName: file.name,
        applied,
        lockedIgnored,
        droppedNonScalar,
        connectorClass: importedClass,
        classInstalled,
        envelope,
      });
      toast.success(`Imported ${applied.length} propert${applied.length === 1 ? "y" : "ies"} from ${file.name}`);
    };
    reader.readAsText(file);
  };

  return (
    <div className="space-y-3">
      {/* Where it writes, then what runs there, then the properties. The service
          is the first question because the rest is meaningless without it. */}
      {servicePicker}

      {!service && (
        <p className="flex items-start gap-1.5 rounded-md border border-warning/40 bg-warning-muted p-2.5 text-xs">
          <Plug className="mt-0.5 h-3.5 w-3.5 shrink-0 text-warning" />
          <span>
            Without a destination service this sink has no endpoint and no credentials, so it cannot be created.
          </span>
        </p>
      )}

      <div className="flex flex-wrap items-end gap-2">
        <div className="grid gap-1.5">
          <Label>connector.class</Label>
          {custom ? (
            <Input
              className="w-[26rem] max-w-full font-mono text-xs"
              value={connectorClass}
              disabled={locked}
              autoFocus
              placeholder="com.example.kafka.connect.MySinkConnector"
              onChange={(e) => setConnectorClass(e.target.value)}
            />
          ) : (
            <Select value={connectorClass} disabled={locked} onValueChange={handlePickClass}>
              <SelectTrigger className="w-[26rem] max-w-full">
                <SelectValue placeholder="Pick an installed Connect plugin…" />
              </SelectTrigger>
              <SelectContent>
                {CONNECT_PLUGIN_CATALOG.map((p) => (
                  <SelectItem key={p.connectorClass} value={p.connectorClass}>
                    <span className="flex flex-col">
                      <span className="text-sm leading-4">{p.displayName}</span>
                      <span className="font-mono text-xs text-muted-foreground">{p.connectorClass}</span>
                    </span>
                  </SelectItem>
                ))}
                <SelectItem value={CUSTOM_CLASS}>
                  <span className="flex flex-col">
                    <span className="text-sm leading-4">Custom connector class…</span>
                    <span className="text-xs text-muted-foreground">
                      Any sink plugin installed on the cluster — type its class name
                    </span>
                  </span>
                </SelectItem>
              </SelectContent>
            </Select>
          )}
        </div>
        {custom && (
          <Button
            variant="ghost"
            size="sm"
            className="mb-0.5 h-9 px-2 text-xs"
            disabled={locked}
            onClick={() => {
              setCustomMode({ blockId: block.id, on: false });
              if (outsideCatalog) setConnectorClass("");
            }}
          >
            Back to the catalog
          </Button>
        )}
        {plugin && (
          <Badge variant="outline" className="mb-2 gap-1 text-xs">
            <Package className="h-3 w-3" />
            {plugin.displayName}
            {plugin.lakehouseSink ? " · lakehouse" : ""}
          </Badge>
        )}
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
          className="mb-0.5 h-9 gap-1.5 text-xs"
          disabled={locked}
          onClick={() => fileRef.current?.click()}
        >
          <FileUp className="h-3.5 w-3.5" /> Upload .json
        </Button>
      </div>

      {outsideCatalog && (
        <Alert className="border-warning/40 bg-warning-muted">
          <AlertCircle className="h-4 w-4 text-warning" />
          <AlertTitle className="text-sm">Custom connector class — not in the known catalog</AlertTitle>
          <AlertDescription className="space-y-1 text-xs">
            <p>
              <code className="font-mono">{connectorClass}</code> is not one of the plugins this platform ships with, so
              nothing here can check its property names or defaults for you. The sink is created exactly as configured
              and will fail at the worker if the plugin is not installed on the Connect cluster.
            </p>
            <p>Known plugins: {CONNECT_PLUGIN_CATALOG.map((p) => p.connectorClass).join(", ")}.</p>
          </AlertDescription>
        </Alert>
      )}

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
              {imported.envelope ? ' (read from the file\'s "config" object — the Connect REST envelope)' : ""}. The editor
              below is the source of truth from here on; the file is not kept.
            </p>
            {imported.connectorClass && (
              <p>
                connector.class <code className="font-mono">{imported.connectorClass}</code> —{" "}
                {imported.classInstalled
                  ? "a known plugin."
                  : "outside the known catalog; it is kept as a custom class and flagged above."}
              </p>
            )}
            {imported.lockedIgnored.length > 0 && (
              <p>
                Ignored because the platform owns {imported.lockedIgnored.length > 1 ? "them" : "it"}:{" "}
                <code className="font-mono">{imported.lockedIgnored.join(", ")}</code>. Those values are derived at deploy
                and shown as locked rows below.
              </p>
            )}
            {imported.droppedNonScalar.length > 0 && (
              <p>
                Ignored because a connector property must be a single value:{" "}
                <code className="font-mono">{imported.droppedNonScalar.join(", ")}</code>.
              </p>
            )}
          </AlertDescription>
        </Alert>
      )}

      {strayOwned.length > 0 && (
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertTitle className="text-sm">These properties are answered somewhere else</AlertTitle>
          <AlertDescription className="space-y-1.5 text-xs">
            <p>
              <code className="font-mono">{strayOwned.join(", ")}</code> {strayOwned.length > 1 ? "are" : "is"} stored on
              this block, but {strayOwned.length > 1 ? "they are" : "it is"} derived —{" "}
              {strayOwned.some((k) => derivedRows.some((r) => r.k === k))
                ? "from the destination service and the flow's names"
                : "from the flow's names"}
              . A stored copy goes stale the moment the source changes, and the flow will not pass preflight while{" "}
              {strayOwned.length > 1 ? "they are" : "it is"} here.
            </p>
            <Button variant="outline" size="sm" className="h-7 gap-1 text-xs" disabled={locked} onClick={clearStrayOwned}>
              <Trash2 className="h-3 w-3" /> Remove {strayOwned.length > 1 ? "them" : "it"}
            </Button>
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
        lockedRows={lockedRows}
        emptyHint="No connector properties yet — pick a plugin, upload a .json config, or add properties by hand."
      />

      {typedOwned.length > 0 && (
        <p className="flex items-start gap-1 text-xs text-warning">
          <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
          <span>
            <code className="font-mono">{typedOwned.join(", ")}</code> {typedOwned.length > 1 ? "are" : "is"} owned by the
            platform — {typedOwned.length > 1 ? "those rows are" : "that row is"} not saved. The derived value is shown as a
            locked row above.
          </span>
        </p>
      )}

      <p className="text-xs leading-4 text-muted-foreground">
        Locked rows are computed when the sink is created and are deliberately absent from what gets saved.
        {service
          ? service.hasSecret
            ? ` Credentials come from "${service.name}" — its stored secret — never from a property here.`
            : ` "${service.name}" has no stored secret yet; add one on the service rather than as a property here.`
          : ""}
      </p>
    </div>
  );
}
