import { describe, expect, it } from "vitest";
import {
  avroToStructuredFields,
  describeStructuredType,
  normalizeAvroRecord,
  shouldPersistBeforeVerify,
  structuredToAvroFields,
  type StructuredField,
} from "@/lib/schemaEditor";

const linksType = [
  "null",
  {
    type: "array",
    items: [
      "null",
      {
        type: "record",
        name: "links__",
        fields: [
          { name: "href", type: ["null", "string"], default: null },
          { name: "rel", type: ["null", "string"], default: null },
        ],
      },
    ],
  },
];

describe("schema editor helpers", () => {
  it("classifies every Avro primitive type with a precise structured type", () => {
    const record = normalizeAvroRecord(
      {
        type: "record",
        name: "PrimitiveCoverage",
        fields: [
          { name: "nothing", type: "null" },
          { name: "enabled", type: "boolean" },
          { name: "count", type: "int" },
          { name: "total", type: "long" },
          { name: "ratio", type: "float" },
          { name: "score", type: "double" },
          { name: "payload", type: "bytes" },
          { name: "label", type: "string" },
        ],
      },
      "PrimitiveCoverage",
    );

    const structured = avroToStructuredFields(record);

    expect(structured.map((field) => [field.name, field.type, describeStructuredType(field)])).toEqual([
      ["nothing", "null", "null"],
      ["enabled", "boolean", "boolean"],
      ["count", "int", "int"],
      ["total", "long", "long"],
      ["ratio", "float", "float"],
      ["score", "double", "double"],
      ["payload", "bytes", "bytes"],
      ["label", "string", "string"],
    ]);
    expect(structuredToAvroFields(structured)).toEqual(record.fields);
  });

  it("classifies enum fixed union and named references without hiding them as advanced", () => {
    const record = normalizeAvroRecord(
      {
        type: "record",
        name: "ComplexCoverage",
        fields: [
          { name: "state", type: { type: "enum", name: "State", symbols: ["OPEN", "CLOSED"] } },
          { name: "fingerprint", type: { type: "fixed", name: "Fingerprint", size: 16 } },
          { name: "value", type: ["null", "string", "int"], default: null },
          { name: "related", type: "OtherRecord" },
        ],
      },
      "ComplexCoverage",
    );

    const structured = avroToStructuredFields(record);

    expect(structured.map((field) => [field.name, field.type, describeStructuredType(field), field.nullable])).toEqual([
      ["state", "enum", "enum<State>", false],
      ["fingerprint", "fixed", "fixed<Fingerprint:16>", false],
      ["value", "union", "union<string | int>", true],
      ["related", "reference", "reference<OtherRecord>", false],
    ]);
    expect(structuredToAvroFields(structured)).toEqual(record.fields);
  });

  it("classifies Avro logical types with precise labels and preserves their raw definitions", () => {
    const logicalFields = [
      { name: "event_date", type: { type: "int", logicalType: "date" } },
      { name: "time_ms", type: { type: "int", logicalType: "time-millis" } },
      { name: "time_us", type: { type: "long", logicalType: "time-micros" } },
      { name: "timestamp_ms", type: { type: "long", logicalType: "timestamp-millis" } },
      { name: "timestamp_us", type: { type: "long", logicalType: "timestamp-micros" } },
      { name: "local_timestamp_ms", type: { type: "long", logicalType: "local-timestamp-millis" } },
      { name: "local_timestamp_us", type: { type: "long", logicalType: "local-timestamp-micros" } },
      { name: "id", type: { type: "string", logicalType: "uuid" } },
      { name: "amount", type: { type: "bytes", logicalType: "decimal", precision: 12, scale: 2 } },
      { name: "duration", type: { type: "fixed", logicalType: "duration", name: "Duration", size: 12 } },
    ];
    const record = normalizeAvroRecord(
      {
        type: "record",
        name: "LogicalCoverage",
        fields: logicalFields,
      },
      "LogicalCoverage",
    );

    const structured = avroToStructuredFields(record);

    expect(structured.map((field) => describeStructuredType(field))).toEqual([
      "date",
      "time-millis",
      "time-micros",
      "timestamp-millis",
      "timestamp-micros",
      "local-timestamp-millis",
      "local-timestamp-micros",
      "uuid",
      "decimal(12,2)",
      "duration",
    ]);
    expect(structuredToAvroFields(structured)).toEqual(record.fields);
  });

  it("preserves nullable array record field types when normalizing Avro records", () => {
    const normalized = normalizeAvroRecord(
      {
        type: "record",
        name: "AssetDetail",
        namespace: "com.nif",
        fields: [{ name: "links", type: linksType, default: null }],
      },
      "AssetDetail",
    );

    expect(normalized.fields[0]).toEqual({
      name: "links",
      type: linksType,
      default: null,
    });
  });

  it("round trips array object fields through the structured editor without coercing them to string", () => {
    const record = normalizeAvroRecord(
      {
        type: "record",
        name: "AssetDetail",
        fields: [{ name: "links", type: linksType, default: null }],
      },
      "AssetDetail",
    );

    const structured = avroToStructuredFields(record);
    const roundTripped = structuredToAvroFields(structured);

    expect(structured[0]).toMatchObject({
      name: "links",
      type: "array",
      nullable: true,
    });
    expect(describeStructuredType(structured[0])).toBe("array<object>");
    expect(roundTripped[0]).toEqual({
      name: "links",
      type: linksType,
      default: null,
    });
  });

  it("converts manually built nested object fields into Avro records", () => {
    const fields: StructuredField[] = [
      {
        name: "addresses",
        type: "array",
        nullable: true,
        doc: "",
        item: {
          type: "object",
          fields: [
            { name: "city", type: "string", nullable: false, doc: "" },
            {
              name: "geo",
              type: "object",
              nullable: true,
              doc: "",
              fields: [
                { name: "lat", type: "double", nullable: false, doc: "" },
                { name: "lon", type: "double", nullable: false, doc: "" },
              ],
            },
          ],
        },
      },
    ];

    expect(structuredToAvroFields(fields)).toEqual([
      {
        name: "addresses",
        type: [
          "null",
          {
            type: "array",
            items: {
              type: "record",
              name: "AddressesItem",
              fields: [
                { name: "city", type: "string" },
                {
                  name: "geo",
                  type: [
                    "null",
                    {
                      type: "record",
                      name: "Geo",
                      fields: [
                        { name: "lat", type: "double" },
                        { name: "lon", type: "double" },
                      ],
                    },
                  ],
                  default: null,
                },
              ],
            },
          },
        ],
        default: null,
      },
    ]);
  });

  it("represents structures deeper than the visual editor limit as advanced while preserving raw Avro", () => {
    const deepType = {
      type: "record",
      name: "Level1",
      fields: [
        {
          name: "level2",
          type: {
            type: "record",
            name: "Level2",
            fields: [
              {
                name: "level3",
                type: {
                  type: "record",
                  name: "Level3",
                  fields: [
                    {
                      name: "level4",
                      type: {
                        type: "record",
                        name: "Level4",
                        fields: [
                          {
                            name: "level5",
                            type: {
                              type: "record",
                              name: "Level5",
                              fields: [{ name: "level6", type: "string" }],
                            },
                          },
                        ],
                      },
                    },
                  ],
                },
              },
            ],
          },
        },
      ],
    };

    const record = normalizeAvroRecord(
      {
        type: "record",
        name: "DeepRecord",
        fields: [{ name: "payload", type: deepType }],
      },
      "DeepRecord",
    );

    const structured = avroToStructuredFields(record);
    const level6 = structured[0].fields?.[0].fields?.[0].fields?.[0].fields?.[0].fields?.[0];

    expect(level6).toMatchObject({ name: "level6", type: "advanced", rawType: "string" });
    expect(structuredToAvroFields(structured)).toEqual(record.fields);
  });

  it("only persists before verify when the schema editor is dirty and the version is not already verified", () => {
    expect(shouldPersistBeforeVerify({ dirty: false, status: "Needs Verification" })).toBe(false);
    expect(shouldPersistBeforeVerify({ dirty: true, status: "Needs Verification" })).toBe(true);
    expect(shouldPersistBeforeVerify({ dirty: true, status: "Verified" })).toBe(false);
  });
});
