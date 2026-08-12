import { api } from "@/lib/api";

/** Connector states as reported by the Kafka Connect status API, plus states we derive locally. */
export type IcebergSinkState =
  | "RUNNING"
  | "PAUSED"
  | "FAILED"
  | "UNASSIGNED"
  | "RESTARTING"
  | "Disabled"
  | "Not Deployed"
  | "Unknown";

export type IcebergSinkTask = {
  id: number;
  state: string;
  worker_id?: string;
  trace?: string;
};

export type IcebergSinkStatus = {
  state: IcebergSinkState;
  tasks: IcebergSinkTask[];
  trace?: string;
  checked_at?: string;
  /** "live" when freshly polled from Connect, "cache" when Connect was unreachable. */
  source?: "live" | "cache";
};

export type IcebergSink = {
  id: string;
  flow_id: string;
  stream_id: string;
  topic: string;
  connector_name: string;
  table_name: string;
  /** Desired state. Seeded from the designer toggle, then owned by enable/disable actions. */
  enabled: boolean;
  overrides?: {
    enabled?: boolean;
  };
  connect_connection_id?: string | null;
  connect_cluster_fingerprint?: string | null;
  iceberg_connection_id?: string | null;
  kafka_connection_id?: string | null;
  last_status?: IcebergSinkStatus | null;
  last_error?: string | null;
  lifecycle_lock_job_id?: string | null;
  created_at?: string;
  updated_at?: string;
};

export type IcebergSinkListResponse = {
  items: IcebergSink[];
  /** False when the Connect cluster could not be reached; statuses then come from cache. */
  connect_reachable: boolean;
  /** Sinks in other flows writing the same table — advisory only, never blocking. */
  shared_table_warnings?: Array<{ table_name: string; flow_id: string; connector_name: string }>;
};

export type PreflightCheck = {
  name: string;
  ok: boolean;
  detail?: string;
};

export type PreflightResult = {
  ok: boolean;
  checks: PreflightCheck[];
};

export type KafkaConnectCluster = {
  ok: boolean;
  reachable: boolean;
  version?: string;
  commit?: string;
  kafka_cluster_id?: string;
  /** Whether the Iceberg sink plugin is installed on the worker. */
  iceberg_plugin_installed?: boolean;
  /** True/false when both cluster ids are known, null when the Kafka side has no proven id. */
  cluster_match?: boolean | null;
  cluster_match_detail?: string;
  error?: string;
};

export type SinkActionResult = {
  ok: boolean;
  sink?: IcebergSink;
  message?: string;
  error?: string;
  /** Present when an action was refused by preflight. */
  checks?: PreflightCheck[];
};

export const icebergSinksApi = {
  list: () => api.get<IcebergSinkListResponse>("/api/iceberg-sinks/"),

  listForFlow: (flowId: string, live = true) =>
    api.get<IcebergSinkListResponse>(
      `/api/flows/${encodeURIComponent(flowId)}/iceberg-sinks?live=${live ? "true" : "false"}`,
    ),

  preflight: (id: string) => api.post<PreflightResult>(`/api/iceberg-sinks/${encodeURIComponent(id)}/preflight`),

  enable: (id: string) => api.post<SinkActionResult>(`/api/iceberg-sinks/${encodeURIComponent(id)}/enable`),
  disable: (id: string) => api.post<SinkActionResult>(`/api/iceberg-sinks/${encodeURIComponent(id)}/disable`),
  pause: (id: string) => api.post<SinkActionResult>(`/api/iceberg-sinks/${encodeURIComponent(id)}/pause`),
  resume: (id: string) => api.post<SinkActionResult>(`/api/iceberg-sinks/${encodeURIComponent(id)}/resume`),
  restart: (id: string) => api.post<SinkActionResult>(`/api/iceberg-sinks/${encodeURIComponent(id)}/restart`),

  /** Config is always returned with secret values masked by the backend. */
  getConfig: (id: string) =>
    api.get<{ ok: boolean; connector_name: string; config: Record<string, string> }>(
      `/api/iceberg-sinks/${encodeURIComponent(id)}/config`,
    ),

  cluster: () => api.get<KafkaConnectCluster>("/api/kafka-connect/cluster"),
};
