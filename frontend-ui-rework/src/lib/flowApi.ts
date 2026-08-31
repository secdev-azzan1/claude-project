import { ApiError, api, getApiBase } from "@/lib/api";

// ─── Backend response types ──────────────────────────────────────────────────

export type ApiSource = {
  id: string;
  name: string;
  type: string;
  connection_setup_mode?: "manual" | "application_service" | string | null;
  application_service_id?: string | null;
  base_url?: string | null;
  auth_type?: string | null;
  api_key_name?: string | null;
  api_key_value?: string | null;
  api_key_location?: string | null;
  bearer_token?: string | null;
  rest_username?: string | null;
  rest_password?: string | null;
  connection_timeout?: number | null;
  read_timeout?: number | null;
  write_timeout?: number | null;
  idle_timeout?: number | null;
  rate_limit?: number | null;
  openapi_spec_id?: string | null;
  openapi_selected_server_url?: string | null;
  db_host?: string | null;
  db_port?: number | null;
  db_database?: string | null;
  db_username?: string | null;
  db_password?: string | null;
  db_ssl_mode?: string | null;
  db_connection_timeout?: number | null;
  db_connection_uri?: string | null;
  mongo_connection_mode?: string | null;
  nifi_service_refs?: Record<string, string> | null;
  db_query?: string | null;
  db_projection?: string | null;
  db_sort?: string | null;
  db_limit?: number | null;
  smb_hostname?: string | null;
  smb_username?: string | null;
  smb_domain?: string | null;
  smb_password?: string | null;
  smb_share?: string | null;
  smb_base_directory?: string | null;
  smb_file_filter?: string | null;
  smb_file_format?: string | null;
  smb_csv_delimiter?: string | null;
  smb_has_header_row?: boolean | null;
  webhook_endpoint_id?: string | null;
  webhook_secret?: string | null;
  webhook_signature_header?: string | null;
  webhook_signature_algo?: string | null;
  webhook_enabled?: boolean | null;
  webhook_content_type?: string | null;
  trino_endpoint?: string | null;
  trino_user?: string | null;
  trino_catalog?: string | null;
  trino_schema?: string | null;
  trino_table?: string | null;
  trino_checkpoint_table?: string | null;
  trino_checkpoint_catalog?: string | null;
  trino_checkpoint_schema?: string | null;
  trino_checkpoint_table_name?: string | null;
  trino_auto_create_checkpoint_table?: boolean | null;
  trino_column_mode?: "manual" | "all" | string | null;
  trino_columns?: string[] | null;
  trino_bootstrap_mode?: string | null;
  trino_output_mode?: string | null;
  trino_include_snapshot_metadata?: boolean | null;
  schedule?: Record<string, unknown> | null;
  streams?: unknown[];
  kafka_output?: Record<string, unknown> | null;
  designer_payload?: Record<string, unknown> | null;
  state?: string;
  created_at?: string;
  updated_at?: string;
};

export type ApiFlow = {
  id: string;
  name: string;
  source_id: string;
  source_type: string;
  primary_stream_id: string;
  primary_stream_name?: string | null;
  kafka_topic: string;
  state: "Draft" | "Deploying" | "Running" | "Stopped" | "Degraded" | "Error";
  enabled: boolean;
  schema_artifact_id?: string | null;
  schema_version?: number | null;
  entity_destinations?: FlowEntityDestination[];
  nifi_process_group_id?: string | null;
  last_run?: string | null;
  total_records?: number;
  total_errors?: number;
  created_at?: string;
  updated_at?: string;
};

export type FlowEntityDestination = {
  stream_id?: string | null;
  stream_name?: string | null;
  topic?: string | null;
  schema_artifact_id?: string | null;
  schema_version?: number | null;
};

export type SourceCreatePayload = Partial<ApiSource> & {
  name: string;
  type: string;
};

export type FlowCreatePayload = {
  name: string;
  source_id: string;
  source_type: string;
  primary_stream_id: string;
  kafka_topic?: string;
  enabled?: boolean;
  schema_artifact_id?: string | null;
  schema_version?: number | null;
};

// ─── Sources ─────────────────────────────────────────────────────────────────

export const sourcesApi = {
  list: () => api.get<ApiSource[]>("/api/sources/"),
  get: (id: string) => api.get<ApiSource>(`/api/sources/${id}`),
  create: (data: SourceCreatePayload) => api.post<ApiSource>("/api/sources/", data),
  update: (id: string, data: SourceCreatePayload) => api.put<ApiSource>(`/api/sources/${id}`, data),
  remove: (id: string) => api.delete<void>(`/api/sources/${id}`),
};

// ─── Flow Metrics type ────────────────────────────────────────────────────────

export type FlowMetrics = {
  throughput: number;
  queued: number;
  records_processed: number;
  kafka_messages: number;
  kafka_topic_total_messages?: number;
  kafka_topic?: string | null;
  kafka_topics?: string[];
  kafka_topic_counts?: Array<{ topic: string; total_messages?: number; error?: string }>;
  kafka_message_error?: string;
  bytes_in: number;
  bytes_out: number;
  active_threads: number;
  processors: {
    id: string;
    name: string;
    type: string;
    state: string;
    active_threads: number;
    files_in: number;
    files_out: number;
  }[];
  error?: string;
};

export type KafkaTopicMessage = {
  topic?: string;
  partition: number;
  offset: number;
  timestamp_ms?: number | null;
  timestamp?: string | null;
  key?: string | null;
  value?: string | null;
};

export type FlowKafkaMessages = {
  topic: string | null;
  topics?: Array<{
    topic: string;
    messages: KafkaTopicMessage[];
    source?: "kafka" | "kafbat";
    direct_error?: string | null;
    error?: string;
  }>;
  messages: KafkaTopicMessage[];
  source?: "kafka" | "kafbat";
  direct_error?: string | null;
  fetched_at?: string;
  error?: string;
};

export type FlowExportDownload = {
  blob: Blob;
  filename: string;
};

export type FlowImportPreviewSummary = {
  flow_name: string;
  source_name: string;
  source_type: string;
  streams: number;
  schemas: number;
  schema_versions: number;
  application_services: number;
  nifi_services: number;
  openapi_specs: number;
  credentials_required: number;
};

export type FlowImportCredentialRequirement = {
  path: string;
  field: string;
};

export type FlowImportCredentialValues = Record<string, Record<string, string>>;

export type FlowImportPreview = {
  ok: boolean;
  summary: FlowImportPreviewSummary;
  credentials_required: FlowImportCredentialRequirement[];
  nifi_service_resolutions?: Array<{
    service_kind: "kafka" | "schema_registry" | string;
    name: string;
    resolution: "default" | string;
    import_enabled: boolean;
    import_reason?: string;
  }>;
  conflicts: {
    flow_name_exists: boolean;
    schema_artifacts: Array<{ artifact_id: string; exists: boolean; compatible?: boolean; reason?: string }>;
  };
  errors: string[];
  warnings: string[];
};

export type FlowImportCredentialValidation = {
  ok: boolean;
  credentials_valid: boolean;
  missing: FlowImportCredentialRequirement[];
  unexpected: FlowImportCredentialRequirement[];
  provided: FlowImportCredentialRequirement[];
  ready_for_import: boolean;
};

export type FlowImportSchemaResolution =
  | { action: "link_existing" }
  | { action: "rename" };

export type FlowImportSchemaResolutions = Record<string, FlowImportSchemaResolution>;

export type NifiImportCredentialField = {
  key: string;
  label: string;
  type: "text" | "password";
};

export type FlowImportFinalizeOptions = {
  flow_name: string;
  schema_resolutions: FlowImportSchemaResolutions;
};

export type FlowImportFinalizeResult = {
  ok: boolean;
  flow: ApiFlow;
  created: {
    flow: boolean;
    source: boolean;
    streams: number;
    schemas: number;
    linked_schemas: number;
    application_services: number;
    nifi_services: number;
    openapi_specs: number;
  };
};

export function getFlowExportFilename(contentDisposition: string | null): string {
  if (!contentDisposition) return "flow-export.flowpack.json";
  const quoted = contentDisposition.match(/filename="([^"]+)"/i);
  if (quoted?.[1]) return quoted[1];
  const bare = contentDisposition.match(/filename=([^;]+)/i);
  return bare?.[1]?.trim() || "flow-export.flowpack.json";
}

export function summarizeImportPreviewIssues(preview: FlowImportPreview): string[] {
  const issues = [...(preview.errors || [])];
  if (preview.conflicts?.flow_name_exists) issues.push("Flow name already exists");
  for (const item of preview.conflicts?.schema_artifacts || []) {
    if (item.exists) issues.push(`Schema artifact already exists: ${item.artifact_id}`);
  }
  issues.push(...(preview.warnings || []));
  return issues;
}

export function createImportCredentialValues(
  requirements: FlowImportCredentialRequirement[],
): FlowImportCredentialValues {
  return requirements.reduce<FlowImportCredentialValues>((acc, item) => {
    if (!acc[item.path]) acc[item.path] = {};
    acc[item.path][item.field] = "";
    return acc;
  }, {});
}

export function isImportCredentialValuesComplete(
  requirements: FlowImportCredentialRequirement[],
  values: FlowImportCredentialValues,
): boolean {
  return requirements.every((item) => Boolean(values[item.path]?.[item.field]?.trim()));
}

export function summarizeImportCredentialValidation(
  validation: FlowImportCredentialValidation | null,
): string[] {
  if (!validation) return [];
  return [
    ...(validation.missing || []).map((item) => `Missing credential: ${item.path} :: ${item.field}`),
    ...(validation.unexpected || []).map((item) => `Unexpected credential: ${item.path} :: ${item.field}`),
  ];
}

export function getSuggestedImportFlowName(preview: FlowImportPreview): string {
  const name = (preview.summary.flow_name || "Imported Flow").trim();
  return preview.conflicts?.flow_name_exists ? `${name} copy` : name;
}

function schemaNamespaceFromFlowName(flowName: string): string {
  return flowName
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "") || "imported_flow";
}

export function suggestedRenamedSchemaArtifactId(artifactId: string, flowName: string): string {
  const namespace = schemaNamespaceFromFlowName(flowName);
  const match = artifactId.match(/^([^.]+)\.[^.]+\.([^.]+)$/);
  if (!match) return `bronze.${namespace}.${schemaNamespaceFromFlowName(artifactId)}__history-value`;
  const stream = match[2]
    .replace(/-value$/i, "")
    .replace(/__history$/i, "");
  return `${match[1]}.${namespace}.${schemaNamespaceFromFlowName(stream)}__history-value`;
}

export function createImportSchemaResolutions(
  preview: FlowImportPreview,
  flowName: string,
): FlowImportSchemaResolutions {
  const renamedCopy = Boolean(preview.conflicts?.flow_name_exists)
    || schemaNamespaceFromFlowName(flowName) !== schemaNamespaceFromFlowName(preview.summary.flow_name);
  return (preview.conflicts?.schema_artifacts || []).reduce<FlowImportSchemaResolutions>((acc, item) => {
    if (!item.exists) return acc;
    acc[item.artifact_id] = item.compatible && !renamedCopy
      ? { action: "link_existing" }
      : { action: "rename" };
    return acc;
  }, {});
}

export function getNifiImportCredentialFields(serviceKind: string): NifiImportCredentialField[] {
  if (serviceKind === "kafka") {
    return [
      { key: "bootstrapServers", label: "Bootstrap servers", type: "text" },
      { key: "username", label: "Username", type: "text" },
      { key: "password", label: "Password", type: "password" },
    ];
  }
  if (serviceKind === "schema_registry") {
    return [
      { key: "registryUrl", label: "Registry URL", type: "text" },
      { key: "username", label: "Username", type: "text" },
      { key: "password", label: "Password", type: "password" },
    ];
  }
  return [];
}

export function isFlowImportReady(
  preview: FlowImportPreview | null,
  flowName: string,
  credentialValues: FlowImportCredentialValues,
  credentialValidation: FlowImportCredentialValidation | null,
  schemaResolutions: FlowImportSchemaResolutions,
): boolean {
  void credentialValues;
  if (!preview || !flowName.trim()) return false;
  const credentialsReady = preview.credentials_required.length === 0
    || Boolean(credentialValidation?.ready_for_import);
  if (!credentialsReady) return false;
  return (preview.conflicts?.schema_artifacts || []).every((item) => {
    if (!item.exists) return true;
    const resolution = schemaResolutions[item.artifact_id];
    if (!resolution) return false;
    if (resolution.action === "link_existing") return Boolean(item.compatible);
    return true;
  });
}

export type FlowKafkaClearResult = {
  ok: boolean;
  topic: string | null;
  topics?: Array<{
    topic: string;
    before_total_messages?: number | null;
    after_total_messages?: number | null;
    cleared_messages?: number | null;
    source?: "kafka" | "kafbat";
    direct_error?: string | null;
  }>;
  before_total_messages?: number | null;
  after_total_messages?: number | null;
  cleared_messages?: number | null;
  source?: "kafka" | "kafbat";
  direct_error?: string | null;
  cleared_at?: string;
  error?: string;
};

export type FlowRuntimeError = {
  event_type: "rest_flow_failure";
  flow_id: string;
  source_id?: string;
  stream_id: string;
  stream_name: string;
  stage: string;
  processor_name: string;
  http_status?: string | null;
  message: string;
  error_category: string;
  pagination_page_count?: string | null;
  request_method?: string | null;
  request_url?: string | null;
  event_time?: string | null;
  topic?: string | null;
  partition?: number | null;
  offset?: number | null;
  attributes?: Record<string, string>;
  raw_event?: unknown;
};

export type FlowRuntimeErrorsResponse = {
  ok: boolean;
  source: "kafka" | "kafbat" | "unverified" | string;
  complete_history: boolean;
  topic: string;
  items: FlowRuntimeError[];
  message?: string | null;
  direct_error?: string | null;
  fetched_at?: string;
};

export type ControllerService = {
  id: string;
  name: string;
  type: string;
  state: string;
  validation_errors: string[];
  bulletin_level?: string;
  revision: number;
};

export type ControllerServiceDescriptor = {
  name: string;
  displayName: string;
  description?: string;
  defaultValue?: string | null;
  allowableValues?: Array<{ allowableValue: { value: string; displayName: string } }> | null;
  required: boolean;
  sensitive: boolean;
  dynamic?: boolean;
};

export type ControllerServiceConfig = {
  id: string;
  name: string;
  type: string;
  state: string;
  properties: Record<string, string | null>;
  descriptors: Record<string, ControllerServiceDescriptor>;
  validation_errors: string[];
  revision: number;
};

export type ControllerServicesListResponse = {
  ok: boolean;
  services: ControllerService[];
};

export type ControllerServiceUpdateResult = {
  ok: boolean;
  id: string;
  state: string;
  was_enabled: boolean;
  message: string;
};

export type FlowProcessor = {
  id: string;
  name: string;
  type: string;
  state: string;
  validation_errors: string[];
  bulletin_level?: string;
  revision: number;
  process_group_id?: string;
  process_group_name?: string;
};

export type ProcessorConfig = {
  id: string;
  name: string;
  type: string;
  state: string;
  properties: Record<string, string | null>;
  descriptors: Record<string, ControllerServiceDescriptor>;
  validation_errors: string[];
  revision: number;
};

export type ProcessorsListResponse = {
  ok: boolean;
  processors: FlowProcessor[];
};

export type ProcessorUpdateResult = {
  ok: boolean;
  id: string;
  state: string;
  was_running: boolean;
  message: string;
};

// ─── Flows ───────────────────────────────────────────────────────────────────

export const flowsApi = {
  list: () => api.get<ApiFlow[]>("/api/flows/"),
  get: (id: string) => api.get<ApiFlow>(`/api/flows/${id}`),
  create: (data: FlowCreatePayload) => api.post<ApiFlow>("/api/flows/", data),
  update: (id: string, data: FlowCreatePayload) => api.put<ApiFlow>(`/api/flows/${id}`, data),
  patch: (id: string, updates: Partial<ApiFlow>) =>
    api.patch<ApiFlow>(`/api/flows/${id}`, updates),
  remove: (id: string) => api.delete<void>(`/api/flows/${id}`),
  start: (id: string) => api.post<{ ok: boolean; reason?: string; state?: string }>(`/api/flows/${id}/start`),
  stop: (id: string) => api.post<{ ok: boolean; state?: string }>(`/api/flows/${id}/stop`),
  deploy: (id: string) => api.post<{ ok: boolean; message?: string; process_group_id?: string }>(`/api/flows/${id}/deploy`),
  undeploy: (id: string) => api.post<{ ok: boolean; message?: string; removed_process_group_id?: string }>(`/api/flows/${id}/undeploy`),
  export: async (id: string): Promise<FlowExportDownload> => {
    const res = await fetch(`${getApiBase()}/api/flows/${encodeURIComponent(id)}/export`, {
      method: "GET",
    });
    if (!res.ok) {
      let message = await res.text();
      try {
        const parsed = JSON.parse(message) as { detail?: unknown; message?: unknown };
        message = String(parsed.detail ?? parsed.message ?? message);
      } catch {
        // Keep raw response text.
      }
      throw new ApiError(res.status, message || "Failed to export flow");
    }
    return {
      blob: await res.blob(),
      filename: getFlowExportFilename(res.headers.get("content-disposition")),
    };
  },
  previewImport: (file: File) => {
    const form = new FormData();
    form.append("file", file);
    return api.postForm<FlowImportPreview>("/api/flows/import/preview", form);
  },
  validateImportCredentials: (file: File, credentials: FlowImportCredentialValues) => {
    const form = new FormData();
    form.append("file", file);
    form.append("credentials_json", JSON.stringify(credentials));
    return api.postForm<FlowImportCredentialValidation>("/api/flows/import/credentials/validate", form);
  },
  finalizeImport: (file: File, credentials: FlowImportCredentialValues, options: FlowImportFinalizeOptions) => {
    const form = new FormData();
    form.append("file", file);
    form.append("credentials_json", JSON.stringify(credentials));
    form.append("options_json", JSON.stringify(options));
    return api.postForm<FlowImportFinalizeResult>("/api/flows/import/finalize", form);
  },
  getMetrics: (id: string) => api.get<FlowMetrics>(`/api/flows/${id}/metrics`),
  getKafkaMessages: (id: string, limit = 50) =>
    api.get<FlowKafkaMessages>(`/api/flows/${id}/kafka-messages?limit=${limit}`),
  clearKafkaMessages: (id: string) =>
    api.post<FlowKafkaClearResult>(`/api/flows/${id}/kafka-messages/clear`),
  getRuntimeErrors: (id: string, limit = 50) =>
    api.get<FlowRuntimeErrorsResponse>(`/api/flows/${id}/runtime-errors?limit=${limit}`),
  listControllerServices: (flowId: string) =>
    api.get<ControllerServicesListResponse>(`/api/flows/${flowId}/controller-services`),
  getControllerService: (flowId: string, serviceId: string) =>
    api.get<ControllerServiceConfig>(`/api/flows/${flowId}/controller-services/${serviceId}`),
  updateControllerService: (
    flowId: string,
    serviceId: string,
    properties: Record<string, string | null>,
  ) =>
    api.put<ControllerServiceUpdateResult>(
      `/api/flows/${flowId}/controller-services/${serviceId}`,
      { properties },
    ),
  listProcessors: (flowId: string) =>
    api.get<ProcessorsListResponse>(`/api/flows/${flowId}/processors`),
  getProcessor: (flowId: string, processorId: string) =>
    api.get<ProcessorConfig>(`/api/flows/${flowId}/processors/${processorId}`),
  updateProcessor: (
    flowId: string,
    processorId: string,
    properties: Record<string, string | null>,
  ) =>
    api.put<ProcessorUpdateResult>(
      `/api/flows/${flowId}/processors/${processorId}`,
      { properties },
    ),
};

// ─── Source-type label mapping ───────────────────────────────────────────────

export const apiTypeToLabel = (apiType: string): string => {
  switch (apiType) {
    case "REST API":
      return "REST API";
    case "PostgreSQL":
      return "PostgreSQL";
    case "MongoDB":
      return "MongoDB";
    case "SMB":
      return "SMB";
    case "Webhook":
      return "Webhook";
    default:
      return apiType;
  }
};
