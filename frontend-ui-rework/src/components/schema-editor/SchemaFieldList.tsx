// The recursive Avro editor — ported from the original app's Schemas page
// (module scope, lines 65–407) with a `readOnly` prop threaded through every
// level so the approved-schema detail pane can render the exact same tree
// without any of it being editable.
//
// `SchemaNodeEditor`, `SchemaFieldList` and `SchemaFieldRow` are mutually
// recursive, so they deliberately share one module.

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import { cn } from "@/lib/utils";
import {
  describeStructuredType,
  MAX_STRUCTURED_SCHEMA_DEPTH,
  type StructuredField,
  type StructuredTypeNode,
} from "@/lib/schemaEditor";
import { schemaFieldRowKey } from "@/lib/schemaLayout";
import { Plus, Trash2 } from "lucide-react";
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

  return (
    <div
      className={
        isNested
          ? "relative mt-3 ml-2 min-w-0 space-y-3 border-l-2 border-dashed border-primary/25 pl-4"
          : "mt-3 min-w-0 space-y-3"
      }
    >
      {fields.length === 0 ? (
        <EmptyState inline>No fields at this level.</EmptyState>
      ) : (
        fields.map((field, index) => (
          <SchemaFieldRow
            key={schemaFieldRowKey(index)}
            field={field}
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
  depth,
  readOnly = false,
  onChange,
  onRemove,
}: {
  field: StructuredField;
  depth: number;
  readOnly?: boolean;
  onChange: (field: StructuredField) => void;
  onRemove: () => void;
}) {
  const isBranch = isBranchType(field.type);
  const rowTone = isBranch
    ? schemaLevelTone(depth)
    : depth > 1
      ? "border-border/80 bg-muted/25"
      : "border-border bg-card";

  return (
    <div className={cn("min-w-0 overflow-hidden rounded-md border p-3", isBranch && "border-l-4", rowTone)}>
      <div
        className={cn(
          "grid min-w-0 gap-2 md:items-center",
          readOnly
            ? "md:grid-cols-[minmax(0,1fr)_minmax(130px,200px)_52px]"
            : "md:grid-cols-[minmax(0,1fr)_minmax(130px,200px)_52px_32px]",
        )}
      >
        <Input
          value={field.name}
          readOnly={readOnly}
          className="h-8 min-w-0 bg-background/85 font-medium"
          onChange={(event) => onChange({ ...field, name: event.target.value })}
          aria-label="Field name"
        />
        <div className="min-w-0 space-y-1">
          <SchemaTypeSelect
            value={field.type}
            depth={depth}
            readOnly={readOnly}
            onChange={(type) => onChange(applyFieldType(field, type))}
          />
        </div>
        <label
          className="flex min-w-0 items-center justify-center rounded-md border bg-background/80 px-1 py-1.5"
          aria-label="Nullable"
        >
          <Switch
            checked={field.nullable}
            disabled={readOnly}
            className={READ_ONLY_CONTROL}
            onCheckedChange={(checked) => onChange({ ...field, nullable: checked })}
          />
        </label>
        {!readOnly && (
          <Button variant="ghost" size="icon" className="h-8 w-8" onClick={onRemove} aria-label="Remove field">
            <Trash2 className="h-4 w-4" />
          </Button>
        )}
      </div>

      {(!readOnly || field.doc) && (
        <Input
          value={field.doc}
          readOnly={readOnly}
          className="mt-3 h-8 min-w-0 border-dashed bg-background/55 text-sm"
          placeholder="Doc"
          onChange={(event) => onChange({ ...field, doc: event.target.value })}
          aria-label="Field documentation"
        />
      )}

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
