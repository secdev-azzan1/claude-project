import { api } from "@/lib/api";

// ─── Backend response types ──────────────────────────────────────────────────

export type SchemaVersionStatus = "Draft" | "Needs Verification" | "Verified";

export type ApiSchemaVersion = {
  version: number;
  status: SchemaVersionStatus;
  avro_schema?: Record<string, unknown> | null;
  raw_avro?: string | null;
  created_at?: string;
  updated_at?: string;
  verified_at?: string | null;
  verified_by?: string | null;
  apicurio_global_id?: number | null;
};

export type ApiSchemaArtifact = {
  id: string;
  artifact_id: string;
  stream?: string | null;
  namespace?: string | null;
  versions: ApiSchemaVersion[];
  created_at?: string;
  updated_at?: string;
};

export type SchemaArtifactCreatePayload = {
  artifact_id: string;
  stream?: string;
  namespace?: string;
};

export type SchemaInferPayload = {
  samples: unknown[];
  name?: string;
  namespace?: string;
};

export type SchemaInferResponse = {
  schema: Record<string, unknown>;
  sample_count: number;
};

export type VerifyResponse = {
  status: "Verified";
  version: number;
  verified_at: string;
  apicurio: {
    ok: boolean;
    api_version?: string;
    global_id?: number;
    version?: string;
    error?: string;
  } | null;
};

export type GenerateResponse = {
  status: "Needs Verification";
  version: number;
};

export type UpdateVersionResponse = {
  forked: boolean;
  version?: number;
  new_version?: number;
};

// ─── API helper ──────────────────────────────────────────────────────────────

export const schemasApi = {
  list: () => api.get<ApiSchemaArtifact[]>("/api/schemas/"),
  get: (artifactId: string) =>
    api.get<ApiSchemaArtifact>(`/api/schemas/${encodeURIComponent(artifactId)}`),
  create: (data: SchemaArtifactCreatePayload) =>
    api.post<ApiSchemaArtifact>("/api/schemas/", data),
  remove: (artifactId: string) =>
    api.delete<void>(`/api/schemas/${encodeURIComponent(artifactId)}`),
  removeVersion: (artifactId: string, version: number) =>
    api.delete<void>(`/api/schemas/${encodeURIComponent(artifactId)}/versions/${version}`),
  updateVersion: (artifactId: string, version: number, avroSchema: Record<string, unknown>) =>
    api.put<UpdateVersionResponse>(
      `/api/schemas/${encodeURIComponent(artifactId)}/versions/${version}`,
      avroSchema,
    ),
  generate: (artifactId: string, version: number) =>
    api.post<GenerateResponse>(
      `/api/schemas/${encodeURIComponent(artifactId)}/versions/${version}/generate`,
    ),
  verify: (artifactId: string, version: number) =>
    api.post<VerifyResponse>(
      `/api/schemas/${encodeURIComponent(artifactId)}/versions/${version}/verify`,
    ),
};
