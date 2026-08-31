import {
  getEntitySchemaStatusLabel,
  requiresSchemaWorkflow,
  shouldBlockSaveForUnverifiedSchemas,
} from "./flowDesignerSchemaRequirement";

describe("flow designer schema requirements", () => {
  it("requires the schema workflow for existing source types", () => {
    expect(requiresSchemaWorkflow("rest")).toBe(true);
    expect(requiresSchemaWorkflow("mongo")).toBe(true);
    expect(requiresSchemaWorkflow("smb")).toBe(true);
    expect(requiresSchemaWorkflow("webhook")).toBe(true);
    expect(requiresSchemaWorkflow("postgres")).toBe(true);
  });

  it("does not require the schema workflow for Trino sources", () => {
    expect(requiresSchemaWorkflow("trino")).toBe(false);
    expect(requiresSchemaWorkflow("Trino")).toBe(false);
  });

  it("blocks non-Trino saves when an entity schema is not verified", () => {
    expect(shouldBlockSaveForUnverifiedSchemas("rest", ["Verified", "Needs Verification"])).toBe(true);
    expect(shouldBlockSaveForUnverifiedSchemas("rest", ["Verified"])).toBe(false);
  });

  it("does not block Trino saves when no entity schema is available", () => {
    expect(shouldBlockSaveForUnverifiedSchemas("trino", ["Needs Verification"])).toBe(false);
    expect(shouldBlockSaveForUnverifiedSchemas("trino", [])).toBe(false);
  });

  it("labels Trino entity schema as optional", () => {
    expect(getEntitySchemaStatusLabel("trino", "Needs Verification")).toBe("Schema Optional");
    expect(getEntitySchemaStatusLabel("rest", "Needs Verification")).toBe("Needs Verification");
  });
});
