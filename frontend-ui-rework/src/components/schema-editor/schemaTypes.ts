// Type tables and node factories for the deep Avro editor.
//
// Ported verbatim (behaviour-for-behaviour) from the original app's
// `pages/Schemas.tsx` module scope, so the editor components below stay a
// thin rendering layer over `@/lib/schemaEditor`.

import {
  MAX_STRUCTURED_SCHEMA_DEPTH,
  type StructuredField,
  type StructuredType,
  type StructuredTypeNode,
} from "@/lib/schemaEditor";

export const SCALAR_TYPE_OPTIONS: StructuredType[] = [
  "null",
  "boolean",
  "int",
  "long",
  "float",
  "double",
  "bytes",
  "string",
];

export const LOGICAL_TYPE_OPTIONS: StructuredType[] = [
  "date",
  "time-millis",
  "time-micros",
  "timestamp-millis",
  "timestamp-micros",
  "local-timestamp-millis",
  "local-timestamp-micros",
  "uuid",
  "decimal",
  "duration",
  "logical",
];

export const NESTED_TYPE_OPTIONS: StructuredType[] = ["object", "array", "map"];
export const NAMED_TYPE_OPTIONS: StructuredType[] = ["enum", "fixed", "union", "reference"];
export const ADVANCED_TYPE_OPTION: StructuredType = "advanced";

/**
 * Nested container types drop off the menu past the structured depth cap so a
 * user cannot author a level the editor is unable to render — unless the node
 * already *is* one, in which case it stays selectable so its own value round
 * trips.
 */
export const typeOptionsForDepth = (depth: number, currentType: StructuredType): StructuredType[] => {
  const options = [...SCALAR_TYPE_OPTIONS, ...LOGICAL_TYPE_OPTIONS];
  if (depth < MAX_STRUCTURED_SCHEMA_DEPTH || NESTED_TYPE_OPTIONS.includes(currentType)) {
    options.push(...NESTED_TYPE_OPTIONS);
  }
  options.push(...NAMED_TYPE_OPTIONS);
  options.push(ADVANCED_TYPE_OPTION);
  return options;
};

export const typeLabel = (type: StructuredType): string => {
  if (type === "timestamp-millis") return "timestamp";
  if (type === "timestamp-micros") return "timestamp micros";
  if (type === "local-timestamp-millis") return "local timestamp";
  if (type === "local-timestamp-micros") return "local timestamp micros";
  if (type === "advanced") return "advanced";
  return type;
};

const titleCaseName = (value: string, fallback = "NestedRecord") => {
  const parts = value.replace(/[^A-Za-z0-9_]/g, "_").split("_").filter(Boolean);
  const name = parts.length > 0 ? parts.map((part) => part.charAt(0).toUpperCase() + part.slice(1)).join("") : fallback;
  return /^[A-Za-z_]/.test(name) ? name : `S_${name}`;
};

export const createDefaultNode = (type: StructuredType, fallbackName: string): StructuredTypeNode => {
  const recordName = titleCaseName(fallbackName);
  if (type === "object") {
    return { type, recordName, fields: [] };
  }
  if (type === "array") {
    return { type, item: { type: "string" } };
  }
  if (type === "map") {
    return { type, value: { type: "string" } };
  }
  if (type === "date") return { type, rawType: { type: "int", logicalType: "date" } };
  if (type === "time-millis") return { type, rawType: { type: "int", logicalType: "time-millis" } };
  if (type === "time-micros") return { type, rawType: { type: "long", logicalType: "time-micros" } };
  if (type === "timestamp-millis") return { type, rawType: { type: "long", logicalType: "timestamp-millis" } };
  if (type === "timestamp-micros") return { type, rawType: { type: "long", logicalType: "timestamp-micros" } };
  if (type === "local-timestamp-millis") return { type, rawType: { type: "long", logicalType: "local-timestamp-millis" } };
  if (type === "local-timestamp-micros") return { type, rawType: { type: "long", logicalType: "local-timestamp-micros" } };
  if (type === "uuid") return { type, rawType: { type: "string", logicalType: "uuid" } };
  if (type === "decimal") return { type, rawType: { type: "bytes", logicalType: "decimal", precision: 10, scale: 0 } };
  if (type === "duration") return { type, rawType: { type: "fixed", logicalType: "duration", name: recordName, size: 12 } };
  if (type === "logical") return { type, rawType: { type: "string", logicalType: "logical" } };
  if (type === "enum") return { type, rawType: { type: "enum", name: recordName, symbols: ["UNKNOWN"] } };
  if (type === "fixed") return { type, rawType: { type: "fixed", name: recordName, size: 16 } };
  if (type === "union") return { type, rawType: ["string", "int"] };
  if (type === "reference") return { type, rawType: recordName };
  if (type === "advanced") {
    return { type, rawType: "string" };
  }
  return { type };
};

export const createDefaultField = (depth: number): StructuredField => ({
  name: `new_field_${depth}`,
  type: "string",
  nullable: false,
  doc: "",
});

export const applyFieldType = (field: StructuredField, type: StructuredType): StructuredField => {
  const next = createDefaultNode(type, field.type === "array" ? `${field.name}_item` : field.name);
  return {
    name: field.name,
    type: next.type,
    nullable: field.nullable,
    doc: field.doc,
    fields: next.fields,
    item: next.item,
    value: next.value,
    recordName: next.recordName,
    rawType: next.rawType,
  };
};

export const isBranchType = (type: StructuredType): boolean =>
  type === "object" || type === "array" || type === "map";

/** One tone per nesting level so a deep tree stays readable at a glance. */
export const schemaLevelTone = (depth: number): string => {
  const tones = [
    "border-primary/35 bg-primary/5",
    "border-sky-300/60 bg-sky-50/60 dark:border-sky-500/40 dark:bg-sky-950/25",
    "border-emerald-300/60 bg-emerald-50/60 dark:border-emerald-500/40 dark:bg-emerald-950/25",
    "border-amber-300/70 bg-amber-50/60 dark:border-amber-500/40 dark:bg-amber-950/25",
    "border-violet-300/60 bg-violet-50/60 dark:border-violet-500/40 dark:bg-violet-950/25",
  ];
  return tones[Math.min(Math.max(depth - 1, 0), tones.length - 1)];
};
