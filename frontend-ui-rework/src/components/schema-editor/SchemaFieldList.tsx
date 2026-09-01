// The recursive Avro editor — ported from the original app's Schemas page
// (module scope, lines 65–407) with a `readOnly` prop threaded through every
// level so the approved-schema detail pane can render the exact same tree
// without any of it being editable.
//
// `SchemaNodeEditor`, `SchemaFieldList` and `SchemaFieldRow` are mutually
// recursive, so they deliberately share one module.

import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { Textarea } from "@/components/ui/textarea";
import { cn } from "@/lib/utils";
import {
  describeStructuredType,
  MAX_STRUCTURED_SCHEMA_DEPTH,
  type StructuredField,
  type StructuredTypeNode,
} from "@/lib/schemaEditor";
import { schemaFieldRowKey } from "@/lib/schemaLayout";
import { ChevronDown, ChevronRight, ChevronUp, FileText, Plus, Trash2 } from "lucide-react";
import { SchemaTypeSelect } from "./SchemaTypeSelect";
import { applyFieldType, createDefaultField, createDefaultNode, isBranchType, schemaLevelTone } from "./schemaTypes";
import { EmptyState } from "@/components/ui/empty-state";

/** Keeps a disabled control legible instead of the default 50% wash-out. */
const READ_ONLY_CONTROL = "disabled:opacity-100 disabled:cursor-default";

export function SchemaNodeEditor({
  label,
  node,
  depth,
  readOnly = false,
  onChange,
}: {
  label: string;
  node: StructuredTypeNode;
  depth: number;
  readOnly?: boolean;
  onChange: (node: StructuredTypeNode) => void;
}) {
  const nestedDepth = depth + 1;

  return (
    <div className={cn("mt-3 min-w-0 rounded-md border-l-4 p-3", schemaLevelTone(nestedDepth))}>
      <div className="grid min-w-0 gap-2 md:grid-cols-[minmax(0,1fr)_minmax(130px,200px)_52px] md:items-center">
        <div className="min-w-0">
          <div className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">{label}</div>
        </div>
        <SchemaTypeSelect
          value={node.type}
          depth={nestedDepth}
          readOnly={readOnly}
          onChange={(type) => onChange(createDefaultNode(type, label.replace(/\s+/g, "_")))}
        />
        <label className="flex min-w-0 items-center justify-center rounded-md border px-1 py-2" aria-label="Nullable">
          <Switch
            checked={Boolean(node.nullable)}
            disabled={readOnly}
            className={READ_ONLY_CONTROL}
            onCheckedChange={(checked) => onChange({ ...node, nullable: checked })}
          />
        </label>
      </div>

      {node.type === "object" && (
        <SchemaFieldList
          fields={node.fields ?? []}
          depth={nestedDepth}
          readOnly={readOnly}
          onChange={(fields) => onChange({ ...node, fields })}
        />
      )}

      {node.type === "array" && (
        <SchemaNodeEditor
          label="Array item"
          node={node.item ?? { type: "string" }}
          depth={nestedDepth}
          readOnly={readOnly}
          onChange={(item) => onChange({ ...node, item })}
        />
      )}

      {node.type === "map" && (
        <SchemaNodeEditor
          label="Map value"
          node={node.value ?? { type: "string" }}
          depth={nestedDepth}
          readOnly={readOnly}
          onChange={(value) => onChange({ ...node, value })}
        />
      )}

      {node.type === "advanced" && (
        <div className="mt-3 rounded-md border border-warning/30 bg-warning-muted p-3 text-xs text-muted-foreground">
          This shape is preserved exactly. Use Raw Avro JSON to edit it.
        </div>
      )}
    </div>
  );
}

export function SchemaFieldList({
  fields,
  depth,
  readOnly = false,
  onChange,
}: {
  fields: StructuredField[];
  depth: number;
  readOnly?: boolean;
  onChange: (fields: StructuredField[]) => void;
}) {
  const canAddNestedField = depth <= MAX_STRUCTURED_SCHEMA_DEPTH;
  const isNested = depth > 1;
  const listLayout = isNested
    ? "relative mt-3 ml-2 min-w-0 space-y-3 border-l-2 border-dashed border-primary/25 pl-4"
    : "mt-3 grid min-w-0 gap-3 xl:grid-cols-2";

  return (
    <div className={listLayout}>
      {fields.length === 0 ? (
        <div className={isNested ? undefined : "xl:col-span-2"}>
          <EmptyState inline>No fields at this level.</EmptyState>
        </div>
      ) : (
        fields.map((field, index) => (
          <SchemaFieldRow
            key={schemaFieldRowKey(index)}
            field={field}
            fieldIndex={index}
            depth={depth}
            readOnly={readOnly}
            onChange={(nextField) => {
              const next = [...fields];
              next[index] = nextField;
              onChange(next);
            }}
            onRemove={() => onChange(fields.filter((_, fieldIndex) => fieldIndex !== index))}
          />
        ))
      )}

      {/*
       * The root-level (depth 1) "Add Field" lives ONLY in AvroEditorTabs'
       * top-right action row now — rendering it here too would duplicate it.
       * Nested levels still need this inline affordance: there is no other
       * place in the layout to add a field to a nested record.
       */}
      {!readOnly && isNested && (
        <Button
          variant="outline"
          size="sm"
          className="bg-background"
          disabled={!canAddNestedField}
          title={canAddNestedField ? undefined : `Structured editing stops at depth ${MAX_STRUCTURED_SCHEMA_DEPTH}.`}
          onClick={() => onChange([...fields, createDefaultField(depth)])}
        >
          <Plus className="h-4 w-4" /> Add Field
        </Button>
      )}
    </div>
  );
}

export function SchemaFieldRow({
  field,
  fieldIndex = 0,
  depth,
  readOnly = false,
  onChange,
  onRemove,
}: {
  field: StructuredField;
  fieldIndex?: number;
  depth: number;
  readOnly?: boolean;
  onChange: (field: StructuredField) => void;
  onRemove: () => void;
}) {
  const isBranch = isBranchType(field.type);
  const [docsOpen, setDocsOpen] = useState(Boolean(field.doc));
  const [childrenOpen, setChildrenOpen] = useState(false);
  const rowTone = isBranch
    ? schemaLevelTone(depth)
    : depth > 1
      ? "border-border/80 bg-muted/25"
      : "border-border bg-card";

  useEffect(() => {
    setDocsOpen(Boolean(field.doc));
  }, [field.doc]);

  const docsId = `schema-docs-${depth}-${fieldIndex}`;
  const childrenId = `schema-children-${depth}-${fieldIndex}`;
  const nestedSummary =
    field.type === "object"
      ? `${field.fields?.length ?? 0} nested ${(field.fields?.length ?? 0) === 1 ? "key" : "keys"}`
      : field.type === "array"
        ? `Array item · ${field.item?.type ?? "string"}`
        : `Map value · ${field.value?.type ?? "string"}`;

  return (
    <div className={cn("min-w-0 overflow-hidden rounded-lg border p-2.5 transition-colors hover:border-foreground/20", isBranch && "border-l-4", rowTone)}>
      <div className="flex items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-2">
          <span className="flex h-6 w-6 shrink-0 items-center justify-center rounded-full bg-background text-2xs font-semibold tabular-nums text-muted-foreground shadow-sm">
            {fieldIndex + 1}
          </span>
          <p className="text-2xs font-semibold uppercase tracking-[0.14em] text-muted-foreground">
            Key {fieldIndex + 1}
            {isBranch && <span className="ml-2 font-normal normal-case tracking-normal">· nested {field.type}</span>}
          </p>
        </div>
        {!readOnly && (
          <Button
            variant="ghost"
            size="icon"
            className="h-7 w-7 shrink-0 text-muted-foreground hover:bg-destructive/10 hover:text-destructive"
            onClick={onRemove}
            aria-label="Remove field"
            title="Remove key"
          >
            <Trash2 className="h-3.5 w-3.5" />
          </Button>
        )}
      </div>

      <div className="mt-2 flex min-w-0 flex-wrap items-end gap-2.5">
        <div className="min-w-[12rem] max-w-[26rem] flex-1 space-y-1">
          <Label className="text-2xs text-muted-foreground">Key name</Label>
          <Input
            value={field.name}
            readOnly={readOnly}
            className="h-8 min-w-0 bg-background/85 font-mono text-sm"
            onChange={(event) => onChange({ ...field, name: event.target.value })}
            aria-label="Field name"
          />
        </div>
        <div className="w-[9.5rem] space-y-1">
          <Label className="text-2xs text-muted-foreground">Type</Label>
          <SchemaTypeSelect
            value={field.type}
            depth={depth}
            readOnly={readOnly}
            onChange={(type) => onChange(applyFieldType(field, type))}
          />
        </div>
        <div className="w-[8.5rem] space-y-1">
          <Label className="text-2xs text-muted-foreground">Nullable</Label>
          <label
            className="flex h-8 items-center justify-between gap-2 rounded-md border bg-background/80 px-2.5"
            aria-label="Nullable"
          >
            <span className="text-2xs text-muted-foreground">Allow null</span>
            <Switch
              checked={field.nullable}
              disabled={readOnly}
              className={cn("scale-[0.82]", READ_ONLY_CONTROL)}
              onCheckedChange={(checked) => onChange({ ...field, nullable: checked })}
            />
          </label>
        </div>
      </div>

      <div className="mt-2 border-t border-border/70 pt-2">
        <button
          type="button"
          className="flex min-h-7 w-full max-w-[26rem] items-center gap-2 rounded-md px-2 py-1 text-left transition-colors hover:bg-muted/60"
          onClick={() => setDocsOpen((open) => !open)}
          aria-expanded={docsOpen}
          aria-controls={docsId}
        >
          <FileText className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
          <span className="text-xs font-medium">Docs</span>
          <span className="text-2xs text-muted-foreground">
            {field.doc ? "Description added" : "Add a description"}
          </span>
          {docsOpen ? (
            <ChevronUp className="ml-auto h-3.5 w-3.5 text-muted-foreground" />
          ) : (
            <ChevronDown className="ml-auto h-3.5 w-3.5 text-muted-foreground" />
          )}
        </button>
        {docsOpen && (
          <div id={docsId} className="mt-2 max-w-2xl pl-2">
            {readOnly ? (
              <p className="whitespace-pre-wrap text-sm leading-5 text-muted-foreground">
                {field.doc || "No documentation provided."}
              </p>
            ) : (
              <Textarea
                value={field.doc}
                rows={2}
                className="min-h-[3.25rem] resize-y bg-background/80 text-sm"
                placeholder="Describe what this key contains for schema consumers."
                onChange={(event) => onChange({ ...field, doc: event.target.value })}
                aria-label="Field documentation"
              />
            )}
          </div>
        )}
      </div>

      {isBranch && (
        <>
          <button
            type="button"
            className="mt-2 flex min-h-7 w-full items-center gap-2 rounded-md border border-dashed bg-background/35 px-2 py-1 text-left transition-colors hover:bg-muted/60"
            onClick={() => setChildrenOpen((open) => !open)}
            aria-expanded={childrenOpen}
            aria-controls={childrenId}
          >
            {childrenOpen ? (
              <ChevronDown className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
            ) : (
              <ChevronRight className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
            )}
            <span className="text-xs font-medium">{childrenOpen ? "Hide nested fields" : "Show nested fields"}</span>
            <span className="text-2xs text-muted-foreground">{nestedSummary}</span>
          </button>

          {childrenOpen && (
            <div id={childrenId}>
              {field.type === "object" && (
                <SchemaFieldList
                  fields={field.fields ?? []}
                  depth={depth + 1}
                  readOnly={readOnly}
                  onChange={(fields) => onChange({ ...field, fields })}
                />
              )}

              {field.type === "array" && (
                <SchemaNodeEditor
                  label="Array item"
                  node={field.item ?? { type: "string" }}
                  depth={depth}
                  readOnly={readOnly}
                  onChange={(item) => onChange({ ...field, item })}
                />
              )}

              {field.type === "map" && (
                <SchemaNodeEditor
                  label="Map value"
                  node={field.value ?? { type: "string" }}
                  depth={depth}
                  readOnly={readOnly}
                  onChange={(value) => onChange({ ...field, value })}
                />
              )}
            </div>
          )}
        </>
      )}

      {field.type === "advanced" && (
        <div className="mt-3 rounded-md border border-warning/30 bg-warning-muted p-3 text-xs text-muted-foreground">
          This field is preserved exactly. Use Raw Avro JSON to edit unsupported Avro shapes.
        </div>
      )}

      {(field.type === "enum" || field.type === "fixed" || field.type === "union" || field.type === "reference") && (
        <div className="mt-3 rounded-md border bg-muted/20 p-3 text-xs text-muted-foreground">
          {describeStructuredType(field)} is preserved exactly. Use Raw Avro JSON to edit its detailed definition.
        </div>
      )}
    </div>
  );
}
