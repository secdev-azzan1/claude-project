import { api } from "@/lib/api";

export type OpenApiServer = {
  url: string;
  description?: string;
};

export type OpenApiSpecSummary = {
  spec_id: string;
  title: string;
  version: string;
  format: string;
  openapi_version: string;
  servers: OpenApiServer[];
  warnings: string[];
  operations_count: number;
  created_at?: string;
  updated_at?: string;
};

export type OpenApiOperationListItem = {
  operation_id: string;
  method: "GET" | "POST" | "PUT" | "PATCH" | "DELETE" | "HEAD" | "OPTIONS" | "TRACE";
  path_original: string;
  path_runtime: string;
  summary: string;
  tags: string[];
  deprecated: boolean;
  required_path_params: string[];
  required_query_params: string[];
};

export type OpenApiOperationDetail = {
  operation_id: string;
  method: string;
  path_original: string;
  path_runtime: string;
  summary: string;
  description: string;
  tags: string[];
  deprecated: boolean;
  security: unknown[];
  request_content_types: string[];
  parameters: Array<{
    name: string;
    in: "path" | "query" | "header" | "cookie" | "body";
    required: boolean;
    schema_type?: string | null;
    schema_format?: string | null;
    default?: unknown;
    enum?: unknown;
    description?: string;
  }>;
};

export const openApiApi = {
  parse: async (file: File) => {
    const form = new FormData();
    form.append("file", file);
    return api.postForm<OpenApiSpecSummary>("/api/openapi/parse", form);
  },
  getSpec: (specId: string) => api.get<OpenApiSpecSummary>(`/api/openapi/${specId}`),
  listOperations: (
    specId: string,
    opts?: { search?: string; method?: string; tag?: string; page?: number; pageSize?: number },
  ) => {
    const q = new URLSearchParams();
    if (opts?.search) q.set("search", opts.search);
    if (opts?.method) q.set("method", opts.method);
    if (opts?.tag) q.set("tag", opts.tag);
    q.set("page", String(opts?.page ?? 1));
    q.set("page_size", String(opts?.pageSize ?? 25));
    return api.get<{ items: OpenApiOperationListItem[]; total: number; page: number; page_size: number }>(
      `/api/openapi/${specId}/operations?${q.toString()}`,
    );
  },
  getOperation: (specId: string, operationId: string) =>
    api.get<OpenApiOperationDetail>(`/api/openapi/${specId}/operations/${encodeURIComponent(operationId)}`),
  attachToSource: (sourceId: string, payload: { spec_id: string; selected_server_url?: string | null }) =>
    api.post<{ ok: boolean; source_id: string; spec_id: string; selected_server_url?: string | null }>(
      `/api/openapi/sources/${sourceId}/attach`,
      payload,
    ),
  detachFromSource: (sourceId: string) =>
    api.delete<{ ok: boolean; source_id: string }>(`/api/openapi/sources/${sourceId}/attach`),
};
