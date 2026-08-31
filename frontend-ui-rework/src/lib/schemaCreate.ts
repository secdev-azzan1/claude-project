import type { ApiSchemaArtifact } from "@/lib/schemaApi";

export type CreateSchemaDraft = {
  name: string;
  stream: string;
  namespace: string;
};

export type CreateSchemaErrors = Record<keyof CreateSchemaDraft, string>;

const ARTIFACT_ID_PATTERN = /^[A-Za-z0-9._-]+$/;
const NAMESPACE_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$/;

export const createInitialSchemaDraft = (): CreateSchemaDraft => ({
  name: "",
  stream: "",
  namespace: "com.nif",
});

export const validateCreateSchemaDraft = (
  draft: CreateSchemaDraft,
  existingArtifacts: ApiSchemaArtifact[],
): Partial<CreateSchemaErrors> => {
  const errors: Partial<CreateSchemaErrors> = {};
  const artifactId = draft.name.trim();
  if (!artifactId) {
    errors.name = "Schema artifact id is required.";
  } else if (!ARTIFACT_ID_PATTERN.test(artifactId)) {
    errors.name = "Artifact id may only contain letters, numbers, dots, underscores, or hyphens.";
  } else if (
    existingArtifacts.some((artifact) => artifact.artifact_id.toLowerCase() === artifactId.toLowerCase())
  ) {
    errors.name = "Schema artifact already exists.";
  }

  const stream = draft.stream.trim();
  if (stream && !ARTIFACT_ID_PATTERN.test(stream)) {
    errors.stream = "Stream name may only contain letters, numbers, dots, underscores, or hyphens.";
  }

  const namespace = draft.namespace.trim();
  if (namespace && !NAMESPACE_PATTERN.test(namespace)) {
    errors.namespace = "Namespace must look like a Java package, for example com.nif.";
  }

  return errors;
};

export const createSchemaPayload = (draft: CreateSchemaDraft) => ({
  artifact_id: draft.name.trim(),
  stream: draft.stream.trim() || undefined,
  namespace: draft.namespace.trim() || "com.nif",
});
