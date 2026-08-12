// `ApprovedSchema.fields` is WRITE-ONLY derived display output.
//
// The prototype's `AvroField` (`@/prototype/types`) is a flat, lossy shape:
// `type` is a free string and `children` cannot distinguish a record's fields
// from an array's element fields. Going the other way — flat list back to an
// `AvroRecord` — is therefore ambiguous and is never done anywhere. Whenever a
// real Avro tree is needed, parse `rawAvro` with `normalizeAvroRecord`.
//
// This module only ever runs in the safe direction: real record → display list.

import {
  avroToStructuredFields,
  describeStructuredType,
  type AvroRecord,
  type StructuredField,
  type StructuredTypeNode,
} from "@/lib/schemaEditor";
import type { AvroField as DisplayAvroField } from "@/prototype/types";

const nodeOf = (field: StructuredField): StructuredTypeNode => ({
  type: field.type,
  fields: field.fields,
  item: field.item,
  value: field.value,
  recordName: field.recordName,
  rawType: field.rawType,
});

/** Nested rows a reader would expect to see indented under this one. */
const childrenOf = (field: StructuredField): StructuredField[] | undefined => {
  if (field.type === "object") return field.fields;
  if (field.type === "array" && field.item?.type === "object") return field.item.fields;
  if (field.type === "map" && field.value?.type === "object") return field.value.fields;
  return undefined;
};

const displayTypeOf = (field: StructuredField): string => {
  if (field.type === "object") return field.recordName ? `record<${field.recordName}>` : "record";
  return describeStructuredType(nodeOf(field));
};

export function structuredToDisplayFields(fields: StructuredField[]): DisplayAvroField[] {
  return fields.map((field) => {
    const children = childrenOf(field);
    return {
      name: field.name,
      type: displayTypeOf(field),
      ...(field.doc ? { doc: field.doc } : {}),
      ...(field.nullable ? { nullable: true } : {}),
      ...(children && children.length > 0 ? { children: structuredToDisplayFields(children) } : {}),
    };
  });
}

/** The only supported way to produce `ApprovedSchema.fields`. */
export function recordToDisplayFields(record: AvroRecord): DisplayAvroField[] {
  return structuredToDisplayFields(avroToStructuredFields(record));
}
