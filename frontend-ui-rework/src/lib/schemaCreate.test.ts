import { describe, expect, it } from "vitest";
import {
  createInitialSchemaDraft,
  createSchemaPayload,
  validateCreateSchemaDraft,
  type CreateSchemaDraft,
} from "@/lib/schemaCreate";
import type { ApiSchemaArtifact } from "@/lib/schemaApi";

const existingArtifact = (artifactId: string): ApiSchemaArtifact => ({
  artifact_id: artifactId,
  group_id: "default",
  stream: null,
  versions: [],
});

describe("schema create helpers", () => {
  it("builds a trimmed create payload with the default namespace", () => {
    const draft: CreateSchemaDraft = {
      name: " demo.products-value ",
      stream: " products ",
      namespace: " ",
    };

    expect(createSchemaPayload(draft)).toEqual({
      artifact_id: "demo.products-value",
      stream: "products",
      namespace: "com.nif",
    });
  });

  it("validates required artifact id, duplicates, stream format, and namespace format", () => {
    expect(validateCreateSchemaDraft(createInitialSchemaDraft(), [])).toMatchObject({
      name: "Schema artifact id is required.",
    });

    expect(
      validateCreateSchemaDraft(
        { name: " Existing.Value ", stream: "bad stream", namespace: "bad namespace" },
        [existingArtifact("existing.value")],
      ),
    ).toEqual({
      name: "Schema artifact already exists.",
      stream: "Stream name may only contain letters, numbers, dots, underscores, or hyphens.",
      namespace: "Namespace must look like a Java package, for example com.nif.",
    });
  });
});
