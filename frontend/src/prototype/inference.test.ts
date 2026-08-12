import { describe, expect, it } from "vitest";
import {
  inferAvroFromRecords,
  mergeCsvHints,
  parseRecordPath,
  parseSampleFile,
  resolveRecordPath,
  suggestRecordPaths,
  validateRecordsAgainstAvro,
} from "./inference";
import type { AvroRecord } from "@/lib/schemaEditor";

const NESTED = {
  meta: { page: 1 },
  result: {
    records: [
      { id: "a", payload: { severity: 5, tags: ["x"] } },
      { id: "b", payload: { severity: 7, tags: [] } },
    ],
  },
};

// ------------------------------------------------------------- file parsing

describe("parseSampleFile", () => {
  it("parses a JSON array and keeps the top level", () => {
    const result = parseSampleFile("sample.json", JSON.stringify([{ id: 1 }, { id: 2 }]));
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.format).toBe("json");
    expect(Array.isArray(result.value)).toBe(true);
    expect(result.rows).toHaveLength(2);
  });

  it("parses NDJSON one object per line and names the bad line", () => {
    const ok = parseSampleFile("s.ndjson", '{"id":1}\n\n{"id":2}\n');
    expect(ok.ok).toBe(true);
    if (ok.ok) expect(ok.rows).toHaveLength(2);

    const bad = parseSampleFile("s.ndjson", '{"id":1}\n{"id":');
    expect(bad.ok).toBe(false);
    if (!bad.ok) expect(bad.error).toContain("Line 2");
  });

  it("parses CSV into string values with per-column type hints", () => {
    const csv = ['id,score,ratio,active,label', '1,10,1.5,true,"a, b"', '2,20,2.25,false,plain'].join("\n");
    const result = parseSampleFile("rows.csv", csv);
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.format).toBe("csv");
    expect(result.rows).toEqual([
      { id: "1", score: "10", ratio: "1.5", active: "true", label: "a, b" },
      { id: "2", score: "20", ratio: "2.25", active: "false", label: "plain" },
    ]);
    expect(result.columns).toEqual([
      { name: "id", hint: "long" },
      { name: "score", hint: "long" },
      { name: "ratio", hint: "double" },
      { name: "active", hint: "boolean" },
      { name: "label", hint: "string" },
    ]);
  });

  it("reads a blank CSV cell as null and says so", () => {
    const result = parseSampleFile("rows.csv", "id,note\n1,hello\n2,\n");
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.rows?.[1]).toEqual({ id: "2", note: null });
    expect(result.notes?.join(" ")).toContain("blank cell");
  });

  it("rejects a CSV row with more values than the header", () => {
    const result = parseSampleFile("rows.csv", "id,note\n1,hello,extra\n");
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.error).toContain("Row 2");
  });

  it("refuses .xlsx honestly instead of half-parsing it", () => {
    const result = parseSampleFile("book.xlsx", "PKbinary");
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error).toContain("XLSX");
      expect(result.error).toContain("JSON, NDJSON (one object per line), and CSV");
      expect(result.error).toContain("Export the sheet as CSV");
    }
  });

  it("refuses .xml and names the formats it does parse", () => {
    const result = parseSampleFile("feed.xml", "<rows><row/></rows>");
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.error).toContain("JSON, NDJSON (one object per line), and CSV");
  });

  it("merges CSV hints across files and widens on disagreement", () => {
    const a = parseSampleFile("a.csv", "id,score\n1,10\n");
    const b = parseSampleFile("b.csv", "id,score\nx,20\n");
    expect(mergeCsvHints([a, b])).toEqual({ id: "string", score: "long" });
  });
});

// -------------------------------------------------------------- record paths

describe("resolveRecordPath", () => {
  it("resolves a nested array of objects", () => {
    const result = resolveRecordPath(NESTED, "$.result.records[*]");
    expect(result.ok).toBe(true);
    expect(result.matched).toBe(2);
    expect((result.records[0] as { id: string }).id).toBe("a");
  });

  it("resolves through a wildcard into a nested object field", () => {
    const result = resolveRecordPath(NESTED, "$.result.records[*].payload");
    expect(result.ok).toBe(true);
    expect(result.matched).toBe(2);
    expect((result.records[1] as { severity: number }).severity).toBe(7);
  });

  it("treats a path landing on an array the same as an explicit [*]", () => {
    expect(resolveRecordPath(NESTED, "$.result.records").matched).toBe(2);
  });

  it("treats an empty path as the file's top level", () => {
    expect(resolveRecordPath([{ a: 1 }, { a: 2 }], "").matched).toBe(2);
    expect(resolveRecordPath({ a: 1 }, "").matched).toBe(1);
    expect(resolveRecordPath({ a: 1 }, "$").matched).toBe(1);
  });

  it("supports an index step and a quoted key", () => {
    expect(resolveRecordPath(NESTED, "$.result.records[0]").matched).toBe(1);
    expect(resolveRecordPath({ "odd key": [{ a: 1 }] }, "$['odd key'][*]").matched).toBe(1);
  });

  it("errors clearly when nothing matches", () => {
    const result = resolveRecordPath(NESTED, "$.result.missing[*]");
    expect(result.ok).toBe(false);
    expect(result.error).toContain("Nothing matched");
    expect(result.error).toContain("$.result.missing[*]");
  });

  it("errors when the path matches non-objects", () => {
    const result = resolveRecordPath({ ids: [1, 2, 3] }, "$.ids[*]");
    expect(result.ok).toBe(false);
    expect(result.error).toContain("not objects");
  });

  it("errors on an empty match set and on unparseable syntax", () => {
    expect(resolveRecordPath({ rows: [] }, "$.rows[*]").error).toContain("empty array");
    expect(parseRecordPath("$..rows").ok).toBe(false);
    expect(resolveRecordPath(NESTED, "$.result[oops]").error).toContain("not a valid step");
  });
});

describe("suggestRecordPaths", () => {
  it("finds a deep array of objects and returns the deepest first", () => {
    const value = {
      result: {
        records: [{ id: 1, payload: { items: [{ sku: "a" }, { sku: "b" }] } }],
      },
    };
    const suggestions = suggestRecordPaths(value);
    expect(suggestions).toContain("$.result.records[*]");
    expect(suggestions).toContain("$.result.records[*].payload.items[*]");
    expect(suggestions.indexOf("$.result.records[*].payload.items[*]")).toBeLessThan(
      suggestions.indexOf("$.result.records[*]"),
    );
  });

  it("suggests the root when the file is an array of objects, and every suggestion resolves", () => {
    const value = [{ a: 1 }, { a: 2 }];
    const suggestions = suggestRecordPaths(value);
    expect(suggestions).toEqual(["$[*]"]);
    expect(resolveRecordPath(value, suggestions[0]).matched).toBe(2);
  });

  it("ignores arrays of scalars", () => {
    expect(suggestRecordPaths({ ids: [1, 2], rows: [{ a: 1 }] })).toEqual(["$.rows[*]"]);
  });
});

// ---------------------------------------------------------------- inference

describe("inferAvroFromRecords", () => {
  it("infers scalars, nested records and arrays", () => {
    const { record } = inferAvroFromRecords(
      [
        { id: "a", count: 3, ratio: 1.5, ok: true, nested: { deep: "x" }, tags: ["t"] },
        { id: "b", count: 4, ratio: 2.5, ok: false, nested: { deep: "y" }, tags: ["u", "v"] },
      ],
      "asset",
      "raw.flow",
    );
    const byName = Object.fromEntries(record.fields.map((f) => [f.name, f.type]));
    expect(record.name).toBe("asset");
    expect(record.namespace).toBe("raw.flow");
    expect(byName.id).toBe("string");
    expect(byName.count).toBe("long");
    expect(byName.ratio).toBe("double");
    expect(byName.ok).toBe("boolean");
    expect(byName.tags).toEqual({ type: "array", items: "string" });
    expect(byName.nested).toEqual({ type: "record", name: "Nested", fields: [{ name: "deep", type: "string" }] });
  });

  it("makes a field nullable when it is absent in any record, and reports why", () => {
    const { record, report } = inferAvroFromRecords(
      [{ id: "a", severity: "high" }, { id: "b" }, { id: "c", severity: null }],
      "incident",
      "raw.flow",
    );
    const severity = record.fields.find((f) => f.name === "severity");
    expect(severity?.type).toEqual(["null", "string"]);
    expect(severity?.default).toBeNull();
    expect(record.fields.find((f) => f.name === "id")?.type).toBe("string");
    const note = report.notes.find((n) => n.field === "severity" && n.kind === "nullable");
    expect(note?.note).toContain("missing from 1");
    expect(note?.note).toContain("null in 1");
    expect(report.recordsSampled).toBe(3);
  });

  it("widens conflicting types to string and records the conflict", () => {
    const { record, report } = inferAvroFromRecords(
      [{ v: 1 }, { v: "one" }, { v: true }],
      "r",
      "ns",
    );
    expect(record.fields[0].type).toBe("string");
    const note = report.notes.find((n) => n.field === "v" && n.kind === "conflict");
    expect(note?.note).toContain("widened to string");
  });

  it("widens whole + fractional numbers to double, and object-vs-scalar to string", () => {
    const numbers = inferAvroFromRecords([{ v: 1 }, { v: 1.5 }], "r", "ns");
    expect(numbers.record.fields[0].type).toBe("double");
    expect(numbers.report.notes.some((n) => n.note.includes("widened to double"))).toBe(true);

    const mixed = inferAvroFromRecords([{ v: { a: 1 } }, { v: "flat" }], "r", "ns");
    expect(mixed.record.fields[0].type).toBe("string");
    expect(mixed.report.notes.some((n) => n.note.includes("mixes"))).toBe(true);
  });

  it("keeps ISO-8601 strings as string but says it noticed", () => {
    const { record, report } = inferAvroFromRecords(
      [{ at: "2026-08-11T10:00:00Z" }, { at: "2026-08-12T11:30:00Z" }],
      "r",
      "ns",
    );
    expect(record.fields[0].type).toBe("string");
    expect(report.notes.some((n) => n.kind === "timestamp")).toBe(true);
  });

  it("types CSV string columns from their hints, and says it did", () => {
    const parsed = parseSampleFile("rows.csv", "id,score\n1,10\n2,20\n");
    expect(parsed.ok).toBe(true);
    if (!parsed.ok) return;
    const { record, report } = inferAvroFromRecords(parsed.rows ?? [], "row", "ns", {
      stringColumnHints: mergeCsvHints([parsed]),
    });
    expect(record.fields.map((f) => f.type)).toEqual(["long", "long"]);
    expect(report.notes.filter((n) => n.kind === "csv-hint")).toHaveLength(2);
  });

  it("nests inference into records reached through a record path", () => {
    const resolved = resolveRecordPath(NESTED, "$.result.records[*].payload");
    const { record } = inferAvroFromRecords(resolved.records, "payload", "ns");
    expect(record.fields.map((f) => f.name)).toEqual(["severity", "tags"]);
    expect(record.fields[0].type).toBe("long");
  });

  it("returns an empty record plus a note when nothing usable was sampled", () => {
    const { record, report } = inferAvroFromRecords([], "r", "ns");
    expect(record.fields).toEqual([]);
    expect(report.notes[0].note).toContain("nothing could be inferred");
  });
});

// ------------------------------------------------------------ re-validation

describe("validateRecordsAgainstAvro", () => {
  const samples = [
    { id: "a", severity: "high" },
    { id: "b", severity: null },
    { id: "c", severity: "low" },
  ];

  it("passes when the edited schema still fits every sample", () => {
    const record: AvroRecord = {
      type: "record",
      name: "incident",
      fields: [
        { name: "id", type: "string" },
        { name: "severity", type: ["null", "string"], default: null },
      ],
    };
    const result = validateRecordsAgainstAvro(samples, record);
    expect(result.ok).toBe(true);
    expect(result.summary).toContain("All 3 sample record(s) still fit");
  });

  it("fails in the spec's voice when nullability is edited away", () => {
    const record: AvroRecord = {
      type: "record",
      name: "incident",
      fields: [
        { name: "id", type: "string" },
        { name: "severity", type: "string" },
      ],
    };
    const result = validateRecordsAgainstAvro(samples, record);
    expect(result.ok).toBe(false);
    expect(result.failed).toBe(1);
    expect(result.summary).toBe("1 of 3 records fail; first failure: field `severity` is null but not nullable");
  });

  it("reports a missing field and a wrong type inside a nested record", () => {
    const missing = validateRecordsAgainstAvro([{ id: "a" }], {
      type: "record",
      name: "r",
      fields: [{ name: "extra", type: "string" }],
    });
    expect(missing.summary).toContain("field `extra` is missing but not nullable");

    const nested = validateRecordsAgainstAvro([{ payload: { severity: "high" } }], {
      type: "record",
      name: "r",
      fields: [
        {
          name: "payload",
          type: { type: "record", name: "Payload", fields: [{ name: "severity", type: "long" }] },
        },
      ],
    });
    expect(nested.ok).toBe(false);
    expect(nested.failures[0].field).toBe("payload.severity");
    expect(nested.summary).toContain("the schema says long");
  });

  it("accepts CSV string values against the numeric types inferred from them", () => {
    const parsed = parseSampleFile("rows.csv", "id,score\n1,10\n2,20\n");
    expect(parsed.ok).toBe(true);
    if (!parsed.ok) return;
    const rows = parsed.rows ?? [];
    const { record } = inferAvroFromRecords(rows, "row", "ns", { stringColumnHints: mergeCsvHints([parsed]) });
    expect(validateRecordsAgainstAvro(rows, record).ok).toBe(true);
  });
});
