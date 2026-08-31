import type { ApiSchemaVersion, SchemaVersionStatus } from "@/lib/schemaApi";

export type StructuredPrimitiveType = "null" | "boolean" | "int" | "long" | "float" | "double" | "bytes" | "string";
export type StructuredLogicalType =
  | "date"
  | "time-millis"
  | "time-micros"
  | "timestamp-millis"
  | "timestamp-micros"
  | "local-timestamp-millis"
  | "local-timestamp-micros"
  | "uuid"
  | "decimal"
  | "duration"
  | "logical";
export type StructuredType =
  | StructuredPrimitiveType
  | StructuredLogicalType
  | "object"
  | "array"
  | "map"
  | "enum"
  | "fixed"
  | "union"
  | "reference"
  | "advanced";

export type StructuredField = {
  name: string;
  type: StructuredType;
  nullable: boolean;
  doc: string;
  fields?: StructuredField[];
  item?: StructuredTypeNode;
  value?: StructuredTypeNode;
  recordName?: string;
  rawType?: unknown;
};

export type StructuredTypeNode = {
  type: StructuredType;
  nullable?: boolean;
  fields?: StructuredField[];
  item?: StructuredTypeNode;
  value?: StructuredTypeNode;
  recordName?: string;
  rawType?: unknown;
};

export type AvroField = {
  name: string;
  type: unknown;
  doc?: string;
  default?: null;
};

export type AvroRecord = {
  type: "record";
  name: string;
  namespace?: string;
  fields: AvroField[];
};

const clone = <T,>(value: T): T => JSON.parse(JSON.stringify(value));
export const MAX_STRUCTURED_SCHEMA_DEPTH = 5;

const AVRO_PRIMITIVE_TYPES: StructuredPrimitiveType[] = [
  "null",
  "boolean",
  "int",
  "long",
  "float",
  "double",
  "bytes",
  "string",
];

const AVRO_LOGICAL_TYPES: StructuredLogicalType[] = [
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
];

export const subjectToRecordName = (subject: string) => {
  const base = subject.replace(/-value$/i, "").replace(/[^A-Za-z0-9_]/g, "_");
  const safe = /^[A-Za-z_]/.test(base) ? base : `S_${base}`;
  const parts = safe.split("_").filter(Boolean);
  if (parts.length === 0) return "Record";
  return parts.map((part) => part.charAt(0).toUpperCase() + part.slice(1)).join("");
};

export const createEmptyAvroTemplate = (artifactId: string, namespace = "com.nif"): AvroRecord => ({
  type: "record",
  name: subjectToRecordName(artifactId),
  namespace,
  fields: [],
});

const isPrimitiveAvroType = (value: unknown): value is StructuredPrimitiveType => {
  return AVRO_PRIMITIVE_TYPES.includes(value as StructuredPrimitiveType);
};

const isObject = (value: unknown): value is Record<string, unknown> => !!value && typeof value === "object" && !Array.isArray(value);

const logicalTypeFor = (value: unknown): StructuredLogicalType | null => {
  if (!isObject(value) || typeof value.logicalType !== "string") {
    return null;
  }
  return AVRO_LOGICAL_TYPES.includes(value.logicalType as StructuredLogicalType)
    ? (value.logicalType as StructuredLogicalType)
    : "logical";
};

const rawTypeName = (value: unknown): string | null => {
  if (typeof value === "string") {
    return value;
  }
  if (isObject(value) && typeof value.type === "string") {
    return value.type;
  }
  return null;
};

const namedTypeName = (value: unknown): string | null => {
  if (isObject(value) && typeof value.name === "string") {
    return value.name;
  }
  return rawTypeName(value);
};

const isTimestampMillis = (value: unknown): boolean => {
  return (
    !!value &&
    typeof value === "object" &&
    (value as Record<string, unknown>).type === "long" &&
    (value as Record<string, unknown>).logicalType === "timestamp-millis"
  );
};

export const normalizeBaseType = (value: unknown): unknown => {
  if (isPrimitiveAvroType(value)) {
    return value;
  }

  if (typeof value === "string") {
    return value;
  }

  if (Array.isArray(value)) {
    const normalized = value.map((item) => (item === "null" ? "null" : normalizeBaseType(item)));
    const nonNull = normalized.filter((item) => item !== "null");
    if (normalized.includes("null") && nonNull.length === 1) {
      const inner = nonNull[0];
      if (Array.isArray(inner)) {
        return ["null", ...inner.filter((item) => item !== "null")];
      }
      return ["null", inner];
    }
    return normalized;
  }

  if (isObject(value)) {
    const obj = clone(value as Record<string, unknown>);
    if (obj.type === "record" && Array.isArray(obj.fields)) {
      obj.fields = obj.fields.map((rawField) => {
        if (!rawField || typeof rawField !== "object") return rawField;
        const field = rawField as Record<string, unknown>;
        if (!("type" in field)) return field;
        return { ...field, type: normalizeBaseType(field.type) };
      });
      return obj;
    }
    if (obj.type === "array" && "items" in obj) {
      obj.items = normalizeBaseType(obj.items);
      return obj;
    }
    if (obj.type === "map" && "values" in obj) {
      obj.values = normalizeBaseType(obj.values);
      return obj;
    }
    if (Array.isArray(obj.type)) {
      obj.type = normalizeBaseType(obj.type);
      return obj;
    }
    if (typeof obj.type === "string") {
      return obj;
    }
  }

  return ["null", "string"];
};

export const normalizeAvroRecord = (value: unknown, fallbackName = "Record"): AvroRecord => {
  if (!value || typeof value !== "object") {
    throw new Error("Schema must be a JSON object");
  }

  const obj = value as Record<string, unknown>;
  let recordObj: Record<string, unknown> = obj;
  if (obj.type !== "record") {
    if (obj.type === "array") {
      const items = obj.items;
      const candidates = Array.isArray(items) ? items : [items];
      const nestedRecord = candidates.find(
        (item) => item && typeof item === "object" && (item as Record<string, unknown>).type === "record",
      ) as Record<string, unknown> | undefined;
      if (nestedRecord) {
        recordObj = nestedRecord;
      } else {
        throw new Error("Array-root schema does not contain a record item");
      }
    } else {
      throw new Error("Schema type must be 'record'");
    }
  }

  if (!Array.isArray(recordObj.fields)) {
    throw new Error("Schema must include a fields array");
  }

  const fields = recordObj.fields.map((rawField, index) => {
    if (!rawField || typeof rawField !== "object") {
      throw new Error(`Field at index ${index} must be an object`);
    }

    const fieldObj = rawField as Record<string, unknown>;
    if (typeof fieldObj.name !== "string" || fieldObj.name.trim() === "") {
      throw new Error(`Field at index ${index} must include a valid name`);
    }

    if (!("type" in fieldObj)) {
      throw new Error(`Field '${fieldObj.name}' must include a type`);
    }

    const normalizedType = normalizeBaseType(fieldObj.type);
    const field: AvroField = {
      name: fieldObj.name,
      type: normalizedType,
    };

    if (typeof fieldObj.doc === "string") {
      field.doc = fieldObj.doc;
    }

    if (Array.isArray(normalizedType) && fieldObj.default === null) {
      field.default = null;
    }

    return field;
  });

  return {
    type: "record",
    name: typeof recordObj.name === "string" && recordObj.name.trim() !== "" ? recordObj.name : fallbackName,
    namespace: typeof recordObj.namespace === "string" ? recordObj.namespace : undefined,
    fields,
  };
};

const unwrapNullableType = (value: unknown): { nullable: boolean; type: unknown } => {
  if (!Array.isArray(value)) {
    return { nullable: false, type: value };
  }

  const nonNull = value.filter((item) => item !== "null");
  return {
    nullable: value.includes("null"),
    type: nonNull.length === 1 ? nonNull[0] : nonNull,
  };
};

const pascalCaseName = (value: string, fallback = "NestedRecord") => {
  const safe = value.replace(/[^A-Za-z0-9_]/g, "_");
  const parts = safe.split("_").filter(Boolean);
  const name = parts.length > 0 ? parts.map((part) => part.charAt(0).toUpperCase() + part.slice(1)).join("") : fallback;
  return /^[A-Za-z_]/.test(name) ? name : `S_${name}`;
};

const avroTypeToStructuredNode = (
  value: unknown,
  depth: number,
  fallbackRecordName: string,
): StructuredTypeNode => {
  const { nullable, type } = unwrapNullableType(value);
  const withNullable = (node: StructuredTypeNode): StructuredTypeNode => (nullable ? { ...node, nullable } : node);

  if (depth > MAX_STRUCTURED_SCHEMA_DEPTH) {
    return withNullable({ type: "advanced", rawType: type });
  }

  if (Array.isArray(type)) {
    return withNullable({ type: "union", rawType: type });
  }

  const logicalType = logicalTypeFor(type);
  if (logicalType) {
    return withNullable({ type: logicalType, rawType: type });
  }

  if (isPrimitiveAvroType(type)) {
    return withNullable({ type });
  }

  if (typeof type === "string") {
    return withNullable({ type: "reference", rawType: type });
  }

  if (isObject(type)) {
    const obj = type as Record<string, unknown>;

    if (obj.type === "record" && Array.isArray(obj.fields)) {
      const record: AvroRecord = {
        type: "record",
        name: typeof obj.name === "string" ? obj.name : fallbackRecordName,
        namespace: typeof obj.namespace === "string" ? obj.namespace : undefined,
        fields: obj.fields as AvroField[],
      };
      return withNullable({
        type: "object",
        recordName: record.name,
        fields: avroToStructuredFields(record, depth + 1),
      });
    }

    if (obj.type === "array" && "items" in obj) {
      return withNullable({
        type: "array",
        item: avroTypeToStructuredNode(obj.items, depth + 1, `${fallbackRecordName}Item`),
      });
    }

    if (obj.type === "map" && "values" in obj) {
      return withNullable({
        type: "map",
        value: avroTypeToStructuredNode(obj.values, depth + 1, `${fallbackRecordName}Value`),
      });
    }

    if (obj.type === "enum") {
      return withNullable({ type: "enum", rawType: type });
    }

    if (obj.type === "fixed") {
      return withNullable({ type: "fixed", rawType: type });
    }

    if (typeof obj.type === "string") {
      return withNullable({ type: "reference", rawType: type });
    }
  }

  return withNullable({ type: "advanced", rawType: type });
};

export const avroToStructuredFields = (record: AvroRecord, depth = 1): StructuredField[] => {
  return record.fields.map((field) => {
    const node = avroTypeToStructuredNode(field.type, depth, pascalCaseName(field.name));

    return {
      name: field.name,
      type: node.type,
      nullable: Boolean(node.nullable),
      doc: field.doc ?? "",
      fields: node.fields,
      item: node.item,
      value: node.value,
      recordName: node.recordName,
      rawType: node.rawType,
    };
  });
};

const applyNullable = (type: unknown, nullable?: boolean): unknown => {
  if (!nullable) {
    return type;
  }

  if (type === "null") {
    return "null";
  }

  if (Array.isArray(type)) {
    const nonNull = type.filter((item) => item !== "null");
    return ["null", ...nonNull];
  }

  return ["null", type];
};

const isStructuredLogicalType = (type: StructuredType): type is StructuredLogicalType => {
  return type === "logical" || AVRO_LOGICAL_TYPES.includes(type as StructuredLogicalType);
};

const defaultRawTypeFor = (type: StructuredType, fallbackRecordName: string): unknown => {
  if (type === "date") return { type: "int", logicalType: "date" };
  if (type === "time-millis") return { type: "int", logicalType: "time-millis" };
  if (type === "time-micros") return { type: "long", logicalType: "time-micros" };
  if (type === "timestamp-millis") return { type: "long", logicalType: "timestamp-millis" };
  if (type === "timestamp-micros") return { type: "long", logicalType: "timestamp-micros" };
  if (type === "local-timestamp-millis") return { type: "long", logicalType: "local-timestamp-millis" };
  if (type === "local-timestamp-micros") return { type: "long", logicalType: "local-timestamp-micros" };
  if (type === "uuid") return { type: "string", logicalType: "uuid" };
  if (type === "decimal") return { type: "bytes", logicalType: "decimal", precision: 10, scale: 0 };
  if (type === "duration") return { type: "fixed", logicalType: "duration", name: fallbackRecordName, size: 12 };
  if (type === "logical") return { type: "string", logicalType: "logical" };
  if (type === "enum") return { type: "enum", name: fallbackRecordName, symbols: ["UNKNOWN"] };
  if (type === "fixed") return { type: "fixed", name: fallbackRecordName, size: 16 };
  if (type === "union") return ["string", "int"];
  if (type === "reference") return fallbackRecordName;
  return "string";
};

const structuredNodeToAvroType = (node: StructuredTypeNode, fallbackRecordName: string): unknown => {
  let baseType: unknown;

  if (isStructuredLogicalType(node.type)) {
    baseType = normalizeBaseType(node.rawType ?? defaultRawTypeFor(node.type, fallbackRecordName));
  } else if (isPrimitiveAvroType(node.type)) {
    baseType = node.type;
  } else if (node.type === "object") {
    baseType = {
      type: "record",
      name: node.recordName || fallbackRecordName,
      fields: structuredToAvroFields(node.fields ?? []),
    };
  } else if (node.type === "array") {
    baseType = {
      type: "array",
      items: structuredNodeToAvroType(node.item ?? { type: "string" }, `${fallbackRecordName}Item`),
    };
  } else if (node.type === "map") {
    baseType = {
      type: "map",
      values: structuredNodeToAvroType(node.value ?? { type: "string" }, `${fallbackRecordName}Value`),
    };
  } else if (node.type === "enum" || node.type === "fixed" || node.type === "union" || node.type === "reference") {
    baseType = normalizeBaseType(node.rawType ?? defaultRawTypeFor(node.type, fallbackRecordName));
  } else {
    baseType = normalizeBaseType(node.rawType ?? "string");
  }

  return applyNullable(baseType, node.nullable);
};

const structuredTypeToAvroType = (field: StructuredField): unknown => {
  const baseType = structuredNodeToAvroType(
    {
      type: field.type,
      fields: field.fields,
      item: field.item,
      value: field.value,
      recordName: field.recordName || pascalCaseName(field.name),
      rawType: field.rawType,
    },
    field.recordName || pascalCaseName(field.name),
  );

  return applyNullable(baseType, field.nullable);
};

export const structuredToAvroFields = (fields: StructuredField[]): AvroField[] => {
  return fields.map((field) => {
    const type = structuredTypeToAvroType(field);
    return {
      name: field.name,
      type,
      ...(field.doc ? { doc: field.doc } : {}),
      ...(Array.isArray(type) ? { default: null } : {}),
    };
  });
};

export const describeStructuredType = (node: StructuredTypeNode): string => {
  if (node.type === "array") {
    return `array<${describeStructuredType(node.item ?? { type: "string" })}>`;
  }

  if (node.type === "map") {
    return `map<${describeStructuredType(node.value ?? { type: "string" })}>`;
  }

  if (node.type === "enum") {
    return `enum<${namedTypeName(node.rawType) ?? "unnamed"}>`;
  }

  if (node.type === "fixed") {
    const name = namedTypeName(node.rawType) ?? "unnamed";
    const size = isObject(node.rawType) && typeof node.rawType.size === "number" ? node.rawType.size : "?";
    return `fixed<${name}:${size}>`;
  }

  if (node.type === "union") {
    const raw = Array.isArray(node.rawType) ? node.rawType : [];
    return `union<${raw.map((item) => describeAvroType(item)).join(" | ")}>`;
  }

  if (node.type === "reference") {
    return `reference<${namedTypeName(node.rawType) ?? "unknown"}>`;
  }

  if (node.type === "decimal") {
    const precision = isObject(node.rawType) && typeof node.rawType.precision === "number" ? node.rawType.precision : "?";
    const scale = isObject(node.rawType) && typeof node.rawType.scale === "number" ? node.rawType.scale : 0;
    return `decimal(${precision},${scale})`;
  }

  if (node.type === "logical") {
    return isObject(node.rawType) && typeof node.rawType.logicalType === "string"
      ? `logical<${node.rawType.logicalType}>`
      : "logical";
  }

  return node.type;
};

const describeAvroType = (value: unknown): string => {
  const { nullable, type } = unwrapNullableType(value);
  const node = avroTypeToStructuredNode(type, MAX_STRUCTURED_SCHEMA_DEPTH, "Nested");
  const label = describeStructuredType(node);
  return nullable ? `${label}?` : label;
};

export const avroFromVersion = (version: ApiSchemaVersion, artifactId: string): AvroRecord => {
  if (version.avro_schema) {
    try {
      return normalizeAvroRecord(version.avro_schema, subjectToRecordName(artifactId));
    } catch {
      // fall through to empty template
    }
  }
  return createEmptyAvroTemplate(artifactId);
};

export const shouldPersistBeforeVerify = ({
  dirty,
  status,
}: {
  dirty: boolean;
  status?: SchemaVersionStatus | null;
}): boolean => dirty && status !== "Verified";
