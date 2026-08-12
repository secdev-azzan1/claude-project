/**
 * Schema Inference API client.
 */
import { api } from "@/lib/api";

export type InferenceStatus =
  | "idle"
  | "deploying_nifi"
  | "nifi_running"
  | "collecting"
  | "inferring"
  | "complete"
  | "failed"
  | "stopped";

export interface InferenceJob {
  id: string;
  flow_id: string;
  flow_name: string;
  kafka_topic: string;
  inference_topic: string;
  entity_stream_id: string | null;
  nifi_pg_id: string | null;
  status: InferenceStatus;
  messages_collected: number;
  target_messages: number;
  error: string | null;
  generated_schema: Record<string, unknown> | null;
  schema_artifact_id: string | null;
  schema_version: number | null;
  created_at: string;
  updated_at: string;
}

export const inferenceApi = {
  start: (
    flowId: string,
    targetMessages = 10,
    runtimeValues: Record<string, string> = {},
    entityStreamId?: string,
  ): Promise<InferenceJob> =>
    api.post<InferenceJob>("/api/schema-inference/start", {
      flow_id: flowId,
      target_messages: targetMessages,
      runtime_values: runtimeValues,
      entity_stream_id: entityStreamId || null,
    }),

  getStatus: (jobId: string): Promise<InferenceJob> =>
    api.get<InferenceJob>(`/api/schema-inference/${jobId}`),

  getJobForFlow: (flowId: string, entityStreamId?: string): Promise<InferenceJob | null> => {
    const query = entityStreamId ? `?entity_stream_id=${encodeURIComponent(entityStreamId)}` : "";
    return api.get<InferenceJob | null>(`/api/schema-inference/flow/${flowId}${query}`);
  },

  stop: (jobId: string): Promise<{ ok: boolean; status: string }> =>
    api.post<{ ok: boolean; status: string }>(`/api/schema-inference/${jobId}/stop`),

  accept: (
    jobId: string,
    opts: { namespace?: string } = {},
  ): Promise<{ ok: boolean; schema_artifact_id: string; schema_version: number; messages_sampled: number }> =>
    api.post<{ ok: boolean; schema_artifact_id: string; schema_version: number; messages_sampled: number }>(
      `/api/schema-inference/${jobId}/accept`,
      { namespace: opts.namespace ?? "com.nif" },
    ),
};
