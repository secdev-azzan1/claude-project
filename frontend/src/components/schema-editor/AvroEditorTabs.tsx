// `Structured Editor | Raw Avro JSON` — the two faces of one `AvroRecord`.
// Both read from the same `useAvroBuffer`, so they cannot drift.

import { useState, type ReactNode } from "react";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Textarea } from "@/components/ui/textarea";
import { MAX_STRUCTURED_SCHEMA_DEPTH } from "@/lib/schemaEditor";
import { cn } from "@/lib/utils";
import { AlertCircle } from "lucide-react";
import { SchemaFieldList } from "./SchemaFieldList";
import type { AvroBuffer } from "./useAvroBuffer";

export function AvroEditorTabs({
  buffer,
  readOnly = false,
  showIdentity = true,
  bodyClassName,
  rawClassName,
  className,
  emptyLabel = "Nothing to edit yet.",
  footer,
}: {
  buffer: AvroBuffer;
  readOnly?: boolean;
  /** Record name + namespace row. Off in the ceremony, where both are derived. */
  showIdentity?: boolean;
  /** Scroll container for the structured body, e.g. `max-h-[55vh] overflow-y-auto`. */
  bodyClassName?: string;
  /** Sizing for the raw pane; falls back to `bodyClassName`, then `h-72`. */
  rawClassName?: string;
  className?: string;
  emptyLabel?: string;
  footer?: ReactNode;
}) {
  const [tab, setTab] = useState("structured");
  const record = buffer.record;

  return (
    <Tabs value={tab} onValueChange={setTab} className={cn("min-w-0", className)}>
      <TabsList>
        <TabsTrigger value="structured">Structured Editor</TabsTrigger>
        <TabsTrigger value="raw">Raw Avro JSON</TabsTrigger>
      </TabsList>

      {buffer.rawError && tab === "structured" && (
        <p className="mt-3 flex items-start gap-1.5 rounded-md border border-warning/30 bg-warning-muted p-2.5 text-xs text-muted-foreground">
          <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-warning" />
          <span>
            The raw JSON does not parse right now ({buffer.rawError}). This tree is the last valid version — fix the
            raw tab or keep editing here to overwrite it.
          </span>
        </p>
      )}

      <TabsContent value="structured" className="mt-3 min-w-0 space-y-3">
        {!record ? (
          <div className="rounded-md border border-dashed p-6 text-center text-sm text-muted-foreground">
            {emptyLabel}
          </div>
        ) : (
          <>
            {showIdentity && (
              <div className="grid gap-3 sm:grid-cols-2">
                <div className="space-y-1.5">
                  <Label className="text-xs text-muted-foreground">Record name</Label>
                  <Input
                    value={record.name}
                    readOnly={readOnly}
                    className="h-8 font-mono text-sm"
                    onChange={(event) => buffer.setRecord({ ...record, name: event.target.value })}
                  />
                </div>
                <div className="space-y-1.5">
                  <Label className="text-xs text-muted-foreground">Namespace</Label>
                  <Input
                    value={record.namespace ?? ""}
                    readOnly={readOnly}
                    placeholder="com.example"
                    className="h-8 font-mono text-sm"
                    onChange={(event) =>
                      buffer.setRecord({ ...record, namespace: event.target.value || undefined })
                    }
                  />
                </div>
              </div>
            )}

            <div className={cn("min-w-0", bodyClassName)}>
              <SchemaFieldList fields={buffer.fields} depth={1} readOnly={readOnly} onChange={buffer.setFields} />
            </div>

            <p className="text-xs text-muted-foreground">
              Structured editing is capped at depth {MAX_STRUCTURED_SCHEMA_DEPTH}. Anything deeper — plus enums,
              fixed, unions and named references — is preserved exactly and edited on the Raw Avro JSON tab.
            </p>
          </>
        )}
        {footer}
      </TabsContent>

      <TabsContent value="raw" className="mt-3 min-w-0 space-y-2">
        {readOnly ? (
          <pre
            className={cn(
              "overflow-auto rounded-md border bg-muted/40 p-4 font-mono text-xs leading-5",
              rawClassName ?? bodyClassName,
            )}
          >
            {buffer.rawText || "—"}
          </pre>
        ) : (
          <Textarea
            value={buffer.rawText}
            spellCheck={false}
            className={cn("font-mono text-xs", rawClassName ?? bodyClassName ?? "h-72")}
            onChange={(event) => buffer.setRawText(event.target.value)}
            aria-label="Raw Avro JSON"
          />
        )}

        {buffer.rawError && (
          <p className="flex items-start gap-1.5 text-xs text-destructive">
            <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0" /> {buffer.rawError} — the structured view keeps the
            last valid schema.
          </p>
        )}

        {!readOnly && (
          <p className="text-xs text-muted-foreground">
            This tab is the escape hatch for shapes the structured editor protects. Note that a round trip through the
            structured editor rewrites non-null field <code className="font-mono">default</code> values to
            nullable-only defaults — author them here last if you need them.
          </p>
        )}
        {footer}
      </TabsContent>
    </Tabs>
  );
}
