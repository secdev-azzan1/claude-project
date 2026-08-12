import { describe, expect, it } from "vitest";

import { filterFlowRows, filterSchemaArtifacts } from "@/lib/pageSearch";

describe("filterSchemaArtifacts", () => {
  const artifacts = [
    {
      artifact_id: "bronze.fakebook.authors__history-value",
      stream: "authors",
      namespace: "com.nif",
      versions: [{ version: 1, status: "Verified" }],
    },
    {
      artifact_id: "bronze.rapid7.asset_detail__history-value",
      stream: "asset_detail",
      namespace: "com.security",
      versions: [{ version: 1, status: "Draft" }],
    },
    {
      artifact_id: "bronze.usgs.explosions__history-value",
      stream: "explosions",
      namespace: "com.nif",
      versions: [{ version: 1, status: "Needs Verification" }],
    },
  ];

  it("matches artifact id, stream, namespace, and version status", () => {
    expect(filterSchemaArtifacts(artifacts, "fakebook")).toHaveLength(1);
    expect(filterSchemaArtifacts(artifacts, "asset_detail")).toHaveLength(1);
    expect(filterSchemaArtifacts(artifacts, "security")).toHaveLength(1);
    expect(filterSchemaArtifacts(artifacts, "verified")).toHaveLength(1);
  });

  it("returns all artifacts when the query is blank", () => {
    expect(filterSchemaArtifacts(artifacts, "   ")).toHaveLength(3);
  });

  it("filters artifacts by latest verified status", () => {
    expect(filterSchemaArtifacts(artifacts, "", { verified: true, unverified: false })).toEqual([
      artifacts[0],
    ]);
  });

  it("filters artifacts by latest unverified status", () => {
    expect(filterSchemaArtifacts(artifacts, "", { verified: false, unverified: true })).toEqual([
      artifacts[1],
      artifacts[2],
    ]);
  });

  it("returns all status groups when both status filters are selected", () => {
    expect(filterSchemaArtifacts(artifacts, "", { verified: true, unverified: true })).toHaveLength(3);
  });

  it("combines artifact search with status filters", () => {
    expect(filterSchemaArtifacts(artifacts, "usgs", { verified: true, unverified: false })).toEqual([]);
    expect(filterSchemaArtifacts(artifacts, "usgs", { verified: false, unverified: true })).toEqual([
      artifacts[2],
    ]);
  });
});

describe("filterFlowRows", () => {
  const flows = [
    {
      name: "rapid7_asset_flow",
      source_type: "REST API",
      state: "Running",
      primary_stream_id: "stream-asset-detail",
      primary_stream_name: "asset_detail",
      kafka_topic: "bronze.rapid7.asset_detail__history",
      schema_artifact_id: "bronze.rapid7.asset_detail__history-value",
      entity_destinations: [],
    },
    {
      name: "mongo_assets_flow",
      source_type: "MongoDB",
      state: "Draft",
      primary_stream_id: "stream-assets",
      primary_stream_name: "assets",
      kafka_topic: "bronze.mongo.assets__history",
      schema_artifact_id: "bronze.mongo.assets__history-value",
      entity_destinations: [],
    },
  ];

  it("matches flow name, source type, stream, topic, schema, and state", () => {
    expect(filterFlowRows(flows, "rapid7")).toHaveLength(1);
    expect(filterFlowRows(flows, "mongodb")).toHaveLength(1);
    expect(filterFlowRows(flows, "asset_detail")).toHaveLength(1);
    expect(filterFlowRows(flows, "bronze.mongo")).toHaveLength(1);
    expect(filterFlowRows(flows, "draft")).toHaveLength(1);
  });

  it("returns all flows when the query is blank", () => {
    expect(filterFlowRows(flows, "")).toHaveLength(2);
  });
});
