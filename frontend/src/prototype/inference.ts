// Schema inference from uploaded sample files.
//
// The ceremony's "uploaded sample files" evidence path used to be theatre: it
// appended invented filenames to a list and inferred nothing. This module is
// the real thing — pure, synchronous, React-free, and testable:
//
//   parseSampleFile()        one file's text  -> parsed value (JSON/NDJSON/CSV)
//   suggestRecordPaths()     parsed value     -> candidate paths at arrays of objects
//   resolveRecordPath()      parsed + path    -> the records to infer from
//   inferAvroFromRecords()   records          -> AvroRecord + an honest report
//   validateRecordsAgainstAvro()  edited Avro -> does it still fit those samples?
//
// The path syntax is deliberately the SAME one the http adapter already uses
// for its response record path (`$.resources[*]`, `$.result[*]`, `$.scan`) —
// see `BlockForm`'s recordPath input and `TestPanel`'s response explorer, which
// emits `$.a.b[*]` when you click an array node. Users learn one syntax.

import type { AvroField as SchemaAvroField, AvroRecord } from "@/lib/schemaEditor";

// ---------------------------------------------------------------- parse files

export type SampleFormat = "json" | "ndjson" | "csv";

/** A CSV column keeps string values, plus what those strings *look* like. */
export interface CsvColumn {
  name: string;
  /** What the column's non-blank values consistently parse as. */
  hint: "long" | "double" | "boolean" | "string";
}

/**
 * One parsed sample file. Kept as a flat shape rather than a discriminated
 * union because this project compiles with `strict: false`, where narrowing on
 * a boolean discriminant does not hold — `ok` is still the field to check.
 */
export interface ParseSampleResult {
  ok: boolean;
  format?: SampleFormat;
  /** The file's top level — what a record path is resolved against. */
  value?: unknown;
  /** Row objects for ndjson/csv (for a JSON scalar top level this is absent). */
  rows?: Record<string, unknown>[];
  /** CSV only: per-column type hints, because CSV values are all strings. */
  columns?: CsvColumn[];
  /** Non-fatal remarks worth showing next to the file. */
  notes?: string[];
  error?: string;
}

const SUPPORTED_BLURB = "This prototype parses JSON, NDJSON (one object per line), and CSV.";

const extensionOf = (name: string) => {
  const dot = name.lastIndexOf(".");
  return dot >= 0 ? name.slice(dot + 1).toLowerCase() : "";
};

/**
 * Parse one uploaded sample file. XML and XLSX are refused honestly rather
 * than half-parsed: the spec lists them as evidence formats, the prototype
 * does not implement them, and pretending otherwise is exactly the kind of
 * theatre this module exists to remove.
 */
export function parseSampleFile(name: string, text: string): ParseSampleResult {
  const ext = extensionOf(name);

  if (ext === "xml") {
    return {
      ok: false,
      error: `"${name}" is an XML file. ${SUPPORTED_BLURB} Convert it to JSON or CSV and upload that.`,
    };
  }
  if (ext === "xlsx" || ext === "xls") {
    return {
      ok: false,
      error: `"${name}" is an ${ext.toUpperCase()} workbook. ${SUPPORTED_BLURB} Export the sheet as CSV and upload that.`,
    };
  }

  const body = text.charCodeAt(0) === 0xfeff ? text.slice(1) : text; // strip a UTF-8 BOM
  if (body.trim() === "") {
    return { ok: false, error: `"${name}" is empty — there is nothing to infer from.` };
  }

  if (ext === "csv") return parseCsvFile(name, body);
  if (ext === "ndjson" || ext === "jsonl") return parseNdjsonFile(name, body);
  if (ext === "json") return parseJsonFile(name, body);

  // .txt / no extension: sniff, in the order that produces the least surprise.
  const asJson = parseJsonFile(name, body);
  if (asJson.ok) return asJson;
  const asNdjson = parseNdjsonFile(name, body);
  if (asNdjson.ok) return asNdjson;
  if (body.includes(",")) {
    const asCsv = parseCsvFile(name, body);
    if (asCsv.ok) return asCsv;
  }
  return {
    ok: false,
    error: `Could not tell what "${name}" is. ${SUPPORTED_BLURB} Give the file a .json, .ndjson or .csv extension so it is read the right way.`,
  };
}

function parseJsonFile(name: string, body: string): ParseSampleResult {
  try {
    const value = JSON.parse(body) as unknown;
    const rows = Array.isArray(value)
      ? (value.filter((v) => isPlainObject(v)) as Record<string, unknown>[])
      : isPlainObject(value)
        ? [value]
        : undefined;
    return { ok: true, format: "json", value, ...(rows ? { rows } : {}) };
  } catch (error) {
    return {
      ok: false,
      format: "json",
      error: `"${name}" is not valid JSON — ${error instanceof Error ? error.message : "parse failed"}.`,
    };
  }
}

function parseNdjsonFile(name: string, body: string): ParseSampleResult {
  const lines = body.split(/\r?\n/);
  const values: unknown[] = [];
  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i].trim();
    if (line === "") continue;
    try {
      values.push(JSON.parse(line));
    } catch (error) {
      return {
        ok: false,
        format: "ndjson",
        error: `Line ${i + 1} of "${name}" is not valid JSON — ${error instanceof Error ? error.message : "parse failed"}.`,
      };
    }
  }
  if (values.length === 0) {
    return { ok: false, format: "ndjson", error: `"${name}" contained no JSON lines.` };
  }
  const rows = values.filter((v) => isPlainObject(v)) as Record<string, unknown>[];
  return { ok: true, format: "ndjson", value: values, rows };
}

// ------------------------------------------------------------------- CSV

/** RFC4180-ish: quoted fields, "" escapes, CRLF or LF, quotes may hold commas. */
export function splitCsvRows(body: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let cell = "";
  let quoted = false;
  let touched = false;

  const endCell = () => {
    row.push(cell);
    cell = "";
  };
  const endRow = () => {
    endCell();
    rows.push(row);
    row = [];
    touched = false;
  };

  for (let i = 0; i < body.length; i += 1) {
    const ch = body[i];
    if (quoted) {
      if (ch === '"') {
        if (body[i + 1] === '"') {
          cell += '"';
          i += 1;
        } else {
          quoted = false;
        }
      } else {
        cell += ch;
      }
      touched = true;
      continue;
    }
    if (ch === '"') {
      quoted = true;
      touched = true;
      continue;
    }
    if (ch === ",") {
      endCell();
      touched = true;
      continue;
    }
    if (ch === "\r") continue;
    if (ch === "\n") {
      if (touched || row.length > 0 || cell !== "") endRow();
      continue;
    }
    cell += ch;
    touched = true;
  }
  if (touched || cell !== "" || row.length > 0) endRow();

  return rows.filter((r) => !(r.length === 1 && r[0].trim() === ""));
}

const INT_RE = /^-?\d+$/;
const FLOAT_RE = /^-?(?:\d+\.\d*|\.\d+|\d+)(?:[eE][+-]?\d+)?$/;
const BOOL_RE = /^(?:true|false)$/i;

function parseCsvFile(name: string, body: string): ParseSampleResult {
  const rows = splitCsvRows(body);
  if (rows.length === 0) return { ok: false, format: "csv", error: `"${name}" has no rows.` };

  const header = rows[0].map((h) => h.trim());
  if (header.some((h) => h === "")) {
    return { ok: false, format: "csv", error: `"${name}" has a blank column header — every column needs a name.` };
  }
  const dupes = header.filter((h, i) => header.indexOf(h) !== i);
  if (dupes.length > 0) {
    return {
      ok: false,
      format: "csv",
      error: `"${name}" has duplicate column headers (${[...new Set(dupes)].join(", ")}) — rename them and re-upload.`,
    };
  }
  if (rows.length === 1) {
    return { ok: false, format: "csv", error: `"${name}" has a header row but no data rows.` };
  }

  const objects: Record<string, unknown>[] = [];
  const notes: string[] = [];
  let blanks = 0;

  for (let r = 1; r < rows.length; r += 1) {
    const cells = rows[r];
    if (cells.length > header.length) {
      return {
        ok: false,
        format: "csv",
        error: `Row ${r + 1} of "${name}" has ${cells.length} values but the header declares ${header.length} columns.`,
      };
    }
    const obj: Record<string, unknown> = {};
    header.forEach((key, c) => {
      const raw = cells[c];
      if (raw === undefined) return; // short row: the field is absent, not empty
      const trimmed = raw.trim();
      if (trimmed === "") {
        blanks += 1;
        obj[key] = null; // a blank CSV cell is read as null
        return;
      }
      obj[key] = raw; // values stay strings — `columns` says what they look like
    });
    objects.push(obj);
  }

  const columns: CsvColumn[] = header.map((columnName) => {
    const values = objects
      .map((o) => o[columnName])
      .filter((v): v is string => typeof v === "string" && v.trim() !== "")
      .map((v) => v.trim());
    if (values.length === 0) return { name: columnName, hint: "string" };
    if (values.every((v) => INT_RE.test(v))) return { name: columnName, hint: "long" };
    if (values.every((v) => FLOAT_RE.test(v))) return { name: columnName, hint: "double" };
    if (values.every((v) => BOOL_RE.test(v))) return { name: columnName, hint: "boolean" };
    return { name: columnName, hint: "string" };
  });

  if (blanks > 0) notes.push(`${blanks} blank cell(s) read as null — those columns infer as nullable.`);

  return { ok: true, format: "csv", value: objects, rows: objects, columns, ...(notes.length ? { notes } : {}) };
}

/** CSV column hints from every parsed file, keyed by field name, for inference. */
export function mergeCsvHints(results: ParseSampleResult[]): Record<string, CsvColumn["hint"]> {
  const out: Record<string, CsvColumn["hint"]> = {};
  for (const result of results) {
    if (!result.ok || !result.columns) continue;
    for (const column of result.columns) {
      const existing = out[column.name];
      if (!existing) out[column.name] = column.hint;
      else if (existing !== column.hint) out[column.name] = "string"; // disagreement widens
    }
  }
  return out;
}

// ------------------------------------------------------------- record paths

type PathSegment = { kind: "key"; key: string } | { kind: "index"; index: number } | { kind: "wildcard" };

export interface ParsedRecordPath {
  ok: boolean;
  segments: PathSegment[];
  error?: string;
}

/**
 * `$.data.items[*]`, `$.result.records[*].payload`, `$.scan`, `$[0].rows[*]`,
 * `$['odd key']`. A leading `$` is optional. Empty means "the file's top level".
 */
export function parseRecordPath(path: string): ParsedRecordPath {
  const text = (path ?? "").trim();
  const segments: PathSegment[] = [];
  if (text === "" || text === "$") return { ok: true, segments };

  let i = 0;
  if (text[i] === "$") i += 1;

  const fail = (message: string): ParsedRecordPath => ({ ok: false, segments: [], error: message });

  while (i < text.length) {
    const ch = text[i];
    if (ch === ".") {
      i += 1;
      if (text[i] === ".") return fail("Recursive descent (`..`) is not supported — spell the path out, e.g. `$.data.items[*]`.");
      let key = "";
      while (i < text.length && !".[".includes(text[i])) {
        key += text[i];
        i += 1;
      }
      if (key === "") return fail("A `.` must be followed by a field name, e.g. `$.data.items[*]`.");
      segments.push({ kind: "key", key });
      continue;
    }
    if (ch === "[") {
      const close = text.indexOf("]", i);
      if (close < 0) return fail("Unclosed `[` in the record path.");
      const inner = text.slice(i + 1, close).trim();
      i = close + 1;
      if (inner === "*") segments.push({ kind: "wildcard" });
      else if (/^\d+$/.test(inner)) segments.push({ kind: "index", index: Number(inner) });
      else if (/^'.*'$/.test(inner) || /^".*"$/.test(inner)) segments.push({ kind: "key", key: inner.slice(1, -1) });
      else return fail(`\`[${inner}]\` is not a valid step — use \`[*]\`, \`[0]\`, or \`['field name']\`.`);
      continue;
    }
    if (segments.length === 0) {
      // Tolerate a path written without the leading `$`, e.g. `data.items[*]`.
      let key = "";
      while (i < text.length && !".[".includes(text[i])) {
        key += text[i];
        i += 1;
      }
      segments.push({ kind: "key", key });
      continue;
    }
    return fail(`Unexpected "${ch}" in the record path — expected \`.field\` or \`[…]\`.`);
  }

  return { ok: true, segments };
}

export interface ResolveRecordPathResult {
  ok: boolean;
  records: unknown[];
  matched: number;
  error?: string;
}

const isPlainObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const describeValue = (value: unknown): string => {
  if (value === null) return "null";
  if (Array.isArray(value)) return "array";
  return typeof value;
};

/**
 * Resolve the record path against one file's parsed top level.
 *
 * Empty path = the file's top level: an array becomes N records, an object
 * becomes 1. A path that lands on a single array is expanded to its elements,
 * so `$.data.items` and `$.data.items[*]` mean the same thing.
 */
export function resolveRecordPath(value: unknown, path: string): ResolveRecordPathResult {
  const parsed = parseRecordPath(path);
  if (!parsed.ok) return { ok: false, records: [], matched: 0, error: parsed.error };

  const label = (path ?? "").trim() || "$";

  let current: unknown[] = [value];
  // `$.rows[*]` over `{ "rows": [] }` found the array and it was empty — a very
  // different thing from a path that points nowhere, so the two are told apart.
  let expandedEmptyArray = false;
  for (const segment of parsed.segments) {
    const next: unknown[] = [];
    for (const node of current) {
      if (segment.kind === "key") {
        if (isPlainObject(node) && segment.key in node) next.push(node[segment.key]);
      } else if (segment.kind === "index") {
        if (Array.isArray(node) && segment.index < node.length) next.push(node[segment.index]);
      } else if (Array.isArray(node)) {
        if (node.length === 0) expandedEmptyArray = true;
        next.push(...node);
      }
    }
    current = next;
    if (current.length === 0) break;
  }

  if (parsed.segments.length === 0) {
    if (Array.isArray(value)) {
      if (value.length === 0) return { ok: false, records: [], matched: 0, error: "The file's top level is an empty array — no records to infer from." };
      return finishRecords(value, "the file's top level");
    }
    if (isPlainObject(value)) return finishRecords([value], "the file's top level");
    return {
      ok: false,
      records: [],
      matched: 0,
      error: `The file's top level is a ${describeValue(value)}, not records. Point the record path at an array of objects instead.`,
    };
  }

  if (current.length === 0) {
    return {
      ok: false,
      records: [],
      matched: 0,
      error: expandedEmptyArray
        ? `\`${label}\` matched an empty array — no records to infer from.`
        : `Nothing matched \`${label}\` in this file — check the path, or pick one of the suggestions.`,
    };
  }

  if (current.length === 1 && Array.isArray(current[0])) {
    const arr = current[0] as unknown[];
    if (arr.length === 0) return { ok: false, records: [], matched: 0, error: `\`${label}\` matched an empty array — no records to infer from.` };
    return finishRecords(arr, label);
  }

  return finishRecords(current, label);
}

function finishRecords(candidates: unknown[], label: string): ResolveRecordPathResult {
  const bad = candidates.filter((c) => !isPlainObject(c));
  if (bad.length === candidates.length) {
    const kinds = [...new Set(bad.map(describeValue))].join(", ");
    return {
      ok: false,
      records: [],
      matched: 0,
      error: `\`${label}\` matched ${candidates.length} value(s), but they are ${kinds}, not objects. A record path must land on objects.`,
    };
  }
  if (bad.length > 0) {
    const kinds = [...new Set(bad.map(describeValue))].join(", ");
    return {
      ok: false,
      records: [],
      matched: 0,
      error: `\`${label}\` matched ${candidates.length} value(s), but ${bad.length} of them are ${kinds}, not objects. A record path must land on objects only.`,
    };
  }
  return { ok: true, records: candidates, matched: candidates.length };
}

/**
 * Candidate record paths — every array of objects reachable in the parsed
 * value, deepest first, so a schema buried inside `$.result.records[*].payload`
 * can be picked instead of typed.
 */
export function suggestRecordPaths(
  value: unknown,
  options?: { maxDepth?: number; maxResults?: number },
): string[] {
  const maxDepth = options?.maxDepth ?? 8;
  const maxResults = options?.maxResults ?? 12;
  const found: { path: string; depth: number }[] = [];
  const seen = new Set<string>();

  const push = (path: string, depth: number) => {
    if (seen.has(path)) return;
    seen.add(path);
    found.push({ path, depth });
  };

  const walk = (node: unknown, path: string, depth: number) => {
    if (depth > maxDepth || found.length > maxResults * 4) return;

    if (Array.isArray(node)) {
      const sampled = node.slice(0, 5).filter((v) => v !== null && v !== undefined);
      const objects = sampled.filter(isPlainObject);
      if (sampled.length > 0 && objects.length === sampled.length) {
        push(`${path}[*]`, depth);
        walk(objects[0], `${path}[*]`, depth + 1);
      }
      return;
    }

    if (isPlainObject(node)) {
      for (const [key, child] of Object.entries(node)) {
        walk(child, `${path}.${key}`, depth + 1);
      }
    }
  };

  walk(value, "$", 0);

  return found
    .sort((a, b) => b.depth - a.depth)
    .slice(0, maxResults)
    .map((f) => f.path);
}

// ---------------------------------------------------------------- inference

export type InferenceNoteKind = "nullable" | "conflict" | "timestamp" | "empty" | "csv-hint" | "sample";

export interface InferenceNote {
  /** Dotted field path inside the record, e.g. `payload.severity`. */
  field: string;
  kind: InferenceNoteKind;
  note: string;
}

export interface InferenceReport {
  recordsSampled: number;
  fieldCount: number;
  notes: InferenceNote[];
}

export interface InferenceResult {
  record: AvroRecord;
  report: InferenceReport;
}

export interface InferenceOptions {
  /**
   * CSV columns whose values are strings in the file but consistently look
   * like something narrower. Applied only when every observed value is a
   * string, and always recorded as a note.
   */
  stringColumnHints?: Record<string, "long" | "double" | "boolean" | "string">;
}

type Scalar = "boolean" | "long" | "double" | "string" | "bytes";

interface Observation {
  scalars: Set<Scalar>;
  /** Nested-object observations, merged across records. */
  object?: Map<string, Observation>;
  objectCount: number;
  /** Element observation for arrays, merged across every element seen. */
  array?: Observation;
  arrayCount: number;
  arrayEmptyOnly: boolean;
  present: number;
  nulls: number;
  isoStrings: number;
  strings: number;
}

const newObservation = (): Observation => ({
  scalars: new Set<Scalar>(),
  objectCount: 0,
  arrayCount: 0,
  arrayEmptyOnly: true,
  present: 0,
  nulls: 0,
  isoStrings: 0,
  strings: 0,
});

const ISO_RE = /^\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?)?$/;

const isBytes = (value: unknown): boolean =>
  (typeof Uint8Array !== "undefined" && value instanceof Uint8Array) ||
  (typeof ArrayBuffer !== "undefined" && value instanceof ArrayBuffer);

function observe(target: Observation, value: unknown) {
  target.present += 1;
  if (value === null || value === undefined) {
    target.nulls += 1;
    return;
  }
  if (isBytes(value)) {
    target.scalars.add("bytes");
    return;
  }
  if (Array.isArray(value)) {
    target.arrayCount += 1;
    if (value.length > 0) target.arrayEmptyOnly = false;
    if (!target.array) target.array = newObservation();
    for (const element of value.slice(0, 50)) observe(target.array, element);
    return;
  }
  if (isPlainObject(value)) {
    target.objectCount += 1;
    if (!target.object) target.object = new Map();
    for (const [key, child] of Object.entries(value)) {
      let slot = target.object.get(key);
      if (!slot) {
        slot = newObservation();
        target.object.set(key, slot);
      }
      observe(slot, child);
    }
    // A key absent from THIS object still has to be nullable later, which the
    // per-field `present` count against `objectCount` catches below.
    return;
  }
  if (typeof value === "boolean") {
    target.scalars.add("boolean");
    return;
  }
  if (typeof value === "number") {
    target.scalars.add(Number.isInteger(value) ? "long" : "double");
    return;
  }
  target.strings += 1;
  if (typeof value === "string" && ISO_RE.test(value)) target.isoStrings += 1;
  target.scalars.add("string");
}

const pascal = (value: string, fallback = "Nested") => {
  const parts = value.replace(/[^A-Za-z0-9_]/g, "_").split("_").filter(Boolean);
  const name = parts.length ? parts.map((p) => p.charAt(0).toUpperCase() + p.slice(1)).join("") : fallback;
  return /^[A-Za-z_]/.test(name) ? name : `S_${name}`;
};

interface BuildContext {
  notes: InferenceNote[];
  hints: Record<string, "long" | "double" | "boolean" | "string">;
}

/** Turn one merged observation into an Avro type (without the null union). */
function observationToType(
  observation: Observation,
  path: string,
  recordName: string,
  ctx: BuildContext,
): unknown {
  const shapes: string[] = [];
  if (observation.scalars.size > 0) shapes.push("scalar");
  if (observation.objectCount > 0) shapes.push("object");
  if (observation.arrayCount > 0) shapes.push("array");

  if (shapes.length > 1) {
    ctx.notes.push({
      field: path,
      kind: "conflict",
      note: `mixes ${shapes.join(" and ")} values across the samples — widened to string, the widest safe type.`,
    });
    return "string";
  }

  if (observation.objectCount > 0 && observation.object) {
    return {
      type: "record",
      name: recordName,
      fields: buildFields(observation.object, observation.objectCount, path, ctx),
    };
  }

  if (observation.arrayCount > 0) {
    if (!observation.array || observation.arrayEmptyOnly) {
      ctx.notes.push({
        field: path,
        kind: "empty",
        note: "is always an empty array in the samples — its element type defaults to string.",
      });
      return { type: "array", items: "string" };
    }
    const itemType = observationToType(observation.array, `${path}[]`, `${recordName}Item`, ctx);
    const itemNullable = observation.array.nulls > 0;
    if (itemNullable) {
      ctx.notes.push({ field: `${path}[]`, kind: "nullable", note: "contains null elements — the element type is a nullable union." });
    }
    return { type: "array", items: itemNullable ? ["null", itemType] : itemType };
  }

  // Scalars only.
  const scalars = observation.scalars;
  if (scalars.size === 0) {
    ctx.notes.push({ field: path, kind: "nullable", note: "is null in every sample — typed as a nullable string." });
    return "string";
  }
  if (scalars.size === 1) {
    const only = [...scalars][0];
    if (only === "string") {
      const hint = ctx.hints[path];
      if (hint && hint !== "string") {
        ctx.notes.push({
          field: path,
          kind: "csv-hint",
          note: `is a CSV column whose values all look like ${hint} — typed as ${hint} rather than string.`,
        });
        return hint;
      }
      if (observation.isoStrings > 0 && observation.isoStrings === observation.strings) {
        ctx.notes.push({
          field: path,
          kind: "timestamp",
          note: "looks ISO-8601 in every sample — kept as string; pick a logical type by hand if that is what you mean.",
        });
      }
    }
    return only;
  }
  if (scalars.size === 2 && scalars.has("long") && scalars.has("double")) {
    ctx.notes.push({ field: path, kind: "conflict", note: "has both whole and fractional numbers — widened to double." });
    return "double";
  }
  ctx.notes.push({
    field: path,
    kind: "conflict",
    note: `has conflicting types across the samples (${[...scalars].sort().join(", ")}) — widened to string, the widest safe type.`,
  });
  return "string";
}

function buildFields(
  observations: Map<string, Observation>,
  total: number,
  prefix: string,
  ctx: BuildContext,
): SchemaAvroField[] {
  const fields: SchemaAvroField[] = [];
  for (const [name, observation] of observations) {
    const path = prefix ? `${prefix}.${name}` : name;
    const missing = total - observation.present;
    const nullable = missing > 0 || observation.nulls > 0;
    const baseType = observationToType(observation, path, pascal(name), ctx);

    if (nullable) {
      const reason =
        missing > 0 && observation.nulls > 0
          ? `is missing from ${missing} and null in ${observation.nulls} of ${total} sample record(s)`
          : missing > 0
            ? `is missing from ${missing} of ${total} sample record(s)`
            : `is null in ${observation.nulls} of ${total} sample record(s)`;
      ctx.notes.push({ field: path, kind: "nullable", note: `${reason} — typed as a nullable union.` });
      fields.push({ name, type: ["null", baseType], default: null });
    } else {
      fields.push({ name, type: baseType });
    }
  }
  return fields;
}

/**
 * Infer an Avro record from sample records. Every record is inspected, not
 * just the first: a field missing or null anywhere becomes a nullable union,
 * and conflicting types widen (long+double -> double, anything else -> string)
 * with the reason recorded in the report so the UI can be honest about it.
 */
export function inferAvroFromRecords(
  records: unknown[],
  recordName: string,
  namespace: string,
  options?: InferenceOptions,
): InferenceResult {
  const objects = records.filter(isPlainObject);
  const ctx: BuildContext = { notes: [], hints: options?.stringColumnHints ?? {} };

  if (objects.length === 0) {
    return {
      record: { type: "record", name: recordName, namespace, fields: [] },
      report: {
        recordsSampled: 0,
        fieldCount: 0,
        notes: [{ field: "", kind: "sample", note: "No object records were sampled, so nothing could be inferred." }],
      },
    };
  }

  if (objects.length < records.length) {
    ctx.notes.push({
      field: "",
      kind: "sample",
      note: `${records.length - objects.length} of ${records.length} matched value(s) were not objects and were skipped.`,
    });
  }

  const observations = new Map<string, Observation>();
  for (const object of objects) {
    for (const [key, value] of Object.entries(object)) {
      let slot = observations.get(key);
      if (!slot) {
        slot = newObservation();
        observations.set(key, slot);
      }
      observe(slot, value);
    }
  }

  const fields = buildFields(observations, objects.length, "", ctx);

  return {
    record: { type: "record", name: recordName, namespace, fields },
    report: { recordsSampled: objects.length, fieldCount: fields.length, notes: ctx.notes },
  };
}

// -------------------------------------------------------- re-validation

export interface SampleValidationFailure {
  /** 0-based index into the retained sample records. */
  index: number;
  /** Dotted field path, e.g. `payload.severity`. */
  field: string;
  reason: string;
}

export interface SampleValidationResult {
  ok: boolean;
  checked: number;
  failed: number;
  failures: SampleValidationFailure[];
  /** Spec voice, e.g. "3 of 10 records fail; first failure: …". */
  summary: string;
}

const unwrapUnion = (type: unknown): { nullable: boolean; branches: unknown[] } => {
  if (!Array.isArray(type)) return { nullable: false, branches: [type] };
  return { nullable: type.includes("null"), branches: type.filter((t) => t !== "null") };
};

const describeType = (type: unknown): string => {
  if (typeof type === "string") return type;
  if (Array.isArray(type)) return type.map(describeType).join(" | ");
  if (isPlainObject(type) && typeof type.type === "string") return String(type.type);
  return "unknown";
};

/**
 * Does a sample value fit an Avro type?
 *
 * Deliberately tolerant about strings that hold numbers or booleans: CSV
 * samples are strings on disk, and a `long` inferred from a CSV column hint
 * must not then be reported as failing against the very rows it came from.
 */
function fits(value: unknown, type: unknown): boolean {
  if (typeof type === "string") {
    switch (type) {
      case "null":
        return value === null || value === undefined;
      case "boolean":
        return typeof value === "boolean" || (typeof value === "string" && BOOL_RE.test(value.trim()));
      case "int":
      case "long":
        return (
          (typeof value === "number" && Number.isInteger(value)) ||
          (typeof value === "string" && INT_RE.test(value.trim()))
        );
      case "float":
      case "double":
        return typeof value === "number" || (typeof value === "string" && FLOAT_RE.test(value.trim()));
      case "bytes":
        return typeof value === "string" || isBytes(value);
      case "string":
        return typeof value === "string" || typeof value === "number" || typeof value === "boolean";
      default:
        return true; // named reference — the prototype cannot resolve it, so it passes
    }
  }

  if (Array.isArray(type)) {
    const { nullable, branches } = unwrapUnion(type);
    if (value === null || value === undefined) return nullable;
    return branches.some((branch) => fits(value, branch));
  }

  if (isPlainObject(type)) {
    if (type.logicalType) return fits(value, type.type);
    if (type.type === "record") return isPlainObject(value);
    if (type.type === "array") return Array.isArray(value);
    if (type.type === "map") return isPlainObject(value);
    if (type.type === "enum") {
      const symbols = Array.isArray(type.symbols) ? type.symbols : [];
      return typeof value === "string" && (symbols.length === 0 || symbols.includes(value));
    }
    if (type.type === "fixed") return typeof value === "string" || isBytes(value);
    if (typeof type.type === "string") return fits(value, type.type);
  }

  return true;
}

function checkValue(
  value: unknown,
  type: unknown,
  path: string,
  out: { field: string; reason: string } | null,
): { field: string; reason: string } | null {
  if (out) return out;

  const { nullable, branches } = unwrapUnion(type);

  if (value === null || value === undefined) {
    if (nullable || branches.includes("null")) return null;
    return {
      field: path,
      reason: value === undefined ? `field \`${path}\` is missing but not nullable` : `field \`${path}\` is null but not nullable`,
    };
  }

  const single = branches.length === 1 ? branches[0] : null;

  if (single && isPlainObject(single) && single.type === "record" && Array.isArray(single.fields)) {
    if (!isPlainObject(value)) {
      return { field: path, reason: `field \`${path}\` is a ${describeValue(value)} but the schema says record` };
    }
    let failure: { field: string; reason: string } | null = null;
    for (const rawField of single.fields as SchemaAvroField[]) {
      failure = checkValue(value[rawField.name], rawField.type, path ? `${path}.${rawField.name}` : rawField.name, failure);
      if (failure) break;
    }
    return failure;
  }

  if (single && isPlainObject(single) && single.type === "array" && "items" in single) {
    if (!Array.isArray(value)) {
      return { field: path, reason: `field \`${path}\` is a ${describeValue(value)} but the schema says array` };
    }
    let failure: { field: string; reason: string } | null = null;
    for (let i = 0; i < value.length && !failure; i += 1) {
      failure = checkValue(value[i], single.items, `${path}[${i}]`, failure);
    }
    return failure;
  }

  if (single && isPlainObject(single) && single.type === "map" && "values" in single) {
    if (!isPlainObject(value)) {
      return { field: path, reason: `field \`${path}\` is a ${describeValue(value)} but the schema says map` };
    }
    let failure: { field: string; reason: string } | null = null;
    for (const [key, child] of Object.entries(value)) {
      failure = checkValue(child, single.values, `${path}.${key}`, failure);
      if (failure) break;
    }
    return failure;
  }

  if (fits(value, branches.length === 1 ? branches[0] : branches)) return null;

  return {
    field: path,
    reason: `field \`${path}\` is a ${describeValue(value)} but the schema says ${describeType(branches.length === 1 ? branches[0] : type)}`,
  };
}

/**
 * The spec's rule for the uploaded evidence path: "every edit must still fit
 * those samples before Approve unlocks". This is that check — the edited Avro
 * record against the retained sample records.
 */
export function validateRecordsAgainstAvro(records: unknown[], record: AvroRecord): SampleValidationResult {
  const objects = records.filter(isPlainObject);
  const failures: SampleValidationFailure[] = [];

  objects.forEach((object, index) => {
    let failure: { field: string; reason: string } | null = null;
    for (const field of record.fields) {
      failure = checkValue(object[field.name], field.type, field.name, failure);
      if (failure) break;
    }
    if (failure) failures.push({ index, field: failure.field, reason: failure.reason });
  });

  const checked = objects.length;
  const failed = failures.length;
  const summary =
    checked === 0
      ? "No sample records are retained for this schema."
      : failed === 0
        ? `All ${checked} sample record(s) still fit this schema.`
        : `${failed} of ${checked} records fail; first failure: ${failures[0].reason}`;

  return { ok: failed === 0 && checked > 0, checked, failed, failures: failures.slice(0, 20), summary };
}
