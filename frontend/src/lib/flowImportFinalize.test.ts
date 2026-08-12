import { describe, expect, it } from "vitest";

import {
  createImportSchemaResolutions,
  getSuggestedImportFlowName,
  getNifiImportCredentialFields,
  isFlowImportReady,
  type FlowImportCredentialValues,
  type FlowImportPreview,
} from "./flowApi";
import { refreshImportedFlowQueries } from "./flowImportCache";

const preview = (overrides: Partial<FlowImportPreview> = {}): FlowImportPreview => ({
  ok: true,
  summary: {
    flow_name: "Imported Rapid7",
    source_name: "Imported Rapid7",
    source_type: "REST API",
    streams: 1,
    schemas: 2,
    schema_versions: 2,
    application_services: 1,
    nifi_services: 0,
    openapi_specs: 1,
    credentials_required: 1,
  },
  credentials_required: [{ path: "source/source.json", field: "rest_password" }],
  conflicts: {
    flow_name_exists: true,
    schema_artifacts: [
      {
        artifact_id: "bronze.imported_rapid7.sites-value",
        exists: true,
        compatible: true,
        reason: "Schema body matches existing version 4",
      },
      {
        artifact_id: "bronze.imported_rapid7.assets-value",
        exists: true,
        compatible: false,
        reason: "Existing schema artifact has no matching exported schema version",
      },
    ],
  },
  errors: [],
  warnings: [],
  ...overrides,
});

describe("flow import finalize helpers", () => {
  it("refreshes flow, schema, and dashboard data after import", async () => {
    const invalidated: string[][] = [];
    const queryClient = {
      invalidateQueries: async ({ queryKey }: { queryKey: string[] }) => {
        invalidated.push(queryKey);
      },
    };

    await refreshImportedFlowQueries(queryClient);

    expect(invalidated).toEqual([["flows"], ["schemas"], ["dashboard"]]);
  });

  it("suggests a copy name when the imported flow name already exists", () => {
    expect(getSuggestedImportFlowName(preview())).toBe("Imported Rapid7 copy");
    expect(getSuggestedImportFlowName(preview({ conflicts: { flow_name_exists: false, schema_artifacts: [] } }))).toBe(
      "Imported Rapid7",
    );
  });

  it("defaults copied flow schema conflicts to rename", () => {
    expect(createImportSchemaResolutions(preview(), "Imported Rapid7 copy")).toEqual({
      "bronze.imported_rapid7.sites-value": { action: "rename" },
      "bronze.imported_rapid7.assets-value": { action: "rename" },
    });
  });

  it("defaults compatible schema conflicts to link existing when the flow is not being copied", () => {
    expect(createImportSchemaResolutions(preview({ conflicts: { ...preview().conflicts, flow_name_exists: false } }), "Imported Rapid7")).toEqual({
      "bronze.imported_rapid7.sites-value": { action: "link_existing" },
      "bronze.imported_rapid7.assets-value": { action: "rename" },
    });
  });

  it("requires a flow name, resolved schemas, and validated credentials before import", () => {
    const credentials: FlowImportCredentialValues = {
      "source/source.json": { rest_password: "secret" },
    };
    const resolutions = createImportSchemaResolutions(preview(), "Imported Rapid7 copy");

    expect(isFlowImportReady(preview(), "", credentials, null, resolutions)).toBe(false);
    expect(isFlowImportReady(preview(), "Imported Rapid7 copy", credentials, null, resolutions)).toBe(false);
    expect(
      isFlowImportReady(
        preview(),
        "Imported Rapid7 copy",
        credentials,
        { ok: true, credentials_valid: true, missing: [], unexpected: [], provided: [], ready_for_import: true },
        resolutions,
      ),
    ).toBe(true);
  });

  it("returns imported NiFi service credential fields by service kind", () => {
    expect(getNifiImportCredentialFields("kafka")).toEqual([
      { key: "bootstrapServers", label: "Bootstrap servers", type: "text" },
      { key: "username", label: "Username", type: "text" },
      { key: "password", label: "Password", type: "password" },
    ]);
    expect(getNifiImportCredentialFields("schema_registry")).toEqual([
      { key: "registryUrl", label: "Registry URL", type: "text" },
      { key: "username", label: "Username", type: "text" },
      { key: "password", label: "Password", type: "password" },
    ]);
    expect(getNifiImportCredentialFields("ssl-context")).toEqual([]);
  });
});
