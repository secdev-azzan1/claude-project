import { describe, expect, it } from "vitest";

import { summarizeImportPreviewIssues, type FlowImportPreview } from "./flowApi";

describe("flow import preview helpers", () => {
  it("summarizes blocking errors before warnings", () => {
    const preview: FlowImportPreview = {
      ok: true,
      summary: {
        flow_name: "Imported Flow",
        source_name: "Imported Source",
        source_type: "REST API",
        streams: 2,
        schemas: 1,
        schema_versions: 1,
        application_services: 1,
        nifi_services: 0,
        openapi_specs: 1,
        credentials_required: 1,
      },
      credentials_required: [],
      conflicts: { flow_name_exists: true, schema_artifacts: [] },
      errors: ["Checksum mismatch"],
      warnings: ["Schema exists"],
    };

    expect(summarizeImportPreviewIssues(preview)).toEqual([
      "Checksum mismatch",
      "Flow name already exists",
      "Schema exists",
    ]);
  });
});
