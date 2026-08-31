import { describe, expect, test } from "vitest";
import { schemaFieldRowKey, schemaWorkspaceLayout } from "./schemaLayout";

describe("schemaWorkspaceLayout", () => {
  test("uses independent desktop scroll regions while preserving mobile document flow", () => {
    expect(schemaWorkspaceLayout.grid).toContain("grid-cols-1");
    expect(schemaWorkspaceLayout.grid).toContain("lg:h-[calc(100vh-9.5rem)]");
    expect(schemaWorkspaceLayout.grid).toContain("lg:overflow-hidden");

    expect(schemaWorkspaceLayout.artifactCard).toContain("lg:flex");
    expect(schemaWorkspaceLayout.artifactList).toContain("lg:overflow-y-auto");

    expect(schemaWorkspaceLayout.detailPane).toContain("lg:min-h-0");
    expect(schemaWorkspaceLayout.detailCard).toContain("lg:overflow-hidden");
    expect(schemaWorkspaceLayout.detailScroll).toContain("lg:overflow-y-auto");
  });

  test("keeps field row keys stable when a field name changes", () => {
    expect(schemaFieldRowKey(0)).toBe(schemaFieldRowKey(0));
    expect(schemaFieldRowKey(0)).not.toContain("customer_name");
    expect(schemaFieldRowKey(1)).not.toBe(schemaFieldRowKey(0));
  });
});
