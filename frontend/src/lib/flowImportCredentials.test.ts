import { describe, expect, it } from "vitest";

import {
  createImportCredentialValues,
  isImportCredentialValuesComplete,
  summarizeImportCredentialValidation,
  type FlowImportCredentialValidation,
} from "./flowApi";

describe("flow import credential helpers", () => {
  const requirements = [
    { path: "source/source.json", field: "rest_password" },
    { path: "services/application.json", field: "services.0.config.rest_password" },
  ];

  it("initializes nested credential values from preview requirements", () => {
    expect(createImportCredentialValues(requirements)).toEqual({
      "source/source.json": { rest_password: "" },
      "services/application.json": { "services.0.config.rest_password": "" },
    });
  });

  it("requires every credential value to be non-empty", () => {
    const values = createImportCredentialValues(requirements);
    values["source/source.json"].rest_password = "source-secret";

    expect(isImportCredentialValuesComplete(requirements, values)).toBe(false);

    values["services/application.json"]["services.0.config.rest_password"] = "service-secret";
    expect(isImportCredentialValuesComplete(requirements, values)).toBe(true);
  });

  it("summarizes validation issues without exposing submitted values", () => {
    const validation: FlowImportCredentialValidation = {
      ok: false,
      credentials_valid: false,
      missing: [{ path: "source/source.json", field: "rest_password" }],
      unexpected: [{ path: "source/source.json", field: "api_key" }],
      provided: [{ path: "services/application.json", field: "services.0.config.rest_password" }],
      ready_for_import: false,
    };

    expect(summarizeImportCredentialValidation(validation)).toEqual([
      "Missing credential: source/source.json :: rest_password",
      "Unexpected credential: source/source.json :: api_key",
    ]);
  });
});
