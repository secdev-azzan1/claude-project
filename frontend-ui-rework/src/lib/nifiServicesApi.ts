import { api } from "@/lib/api";

export type NifiServiceKind = "kafka" | "schema_registry" | "mongodb" | "custom";

export type NifiControllerServiceType = {
  type: string;
  bundle?: Record<string, unknown>;
  tags?: string[];
  description?: string;
  restricted?: boolean;
};

export type NifiGlobalService = {
  id: string;
  name: string;
  service_kind: NifiServiceKind;
  nifi_controller_service_id: string;
  nifi_process_group_id: string;
  nifi_type: string;
  is_default: boolean;
  created_at?: string;
  updated_at?: string;
  live?: {
    ok: boolean;
    state?: string;
    validation_errors?: string[];
    revision?: number;
    name?: string;
    type?: string;
    error?: string;
  };
};

export type NifiServiceConfig = {
  ok: boolean;
  id: string;
  name: string;
  type: string;
  state: string;
  properties: Record<string, string | null>;
  descriptors: Record<string, {
    name?: string;
    displayName?: string;
    description?: string;
    required?: boolean;
    sensitive?: boolean;
    allowableValues?: Array<{ value?: string; displayName?: string; description?: string }>;
    defaultValue?: string;
  }>;
  validation_errors: string[];
  revision: number;
  global_service: NifiGlobalService;
};

export const nifiServicesApi = {
  list: () => api.get<{ items: NifiGlobalService[] }>("/api/nifi-services/"),
  listTypes: (serviceKind?: NifiServiceKind) => {
    const q = serviceKind ? `?service_kind=${encodeURIComponent(serviceKind)}` : "";
    return api.get<{ items: NifiControllerServiceType[] }>(`/api/nifi-services/types${q}`);
  },
  create: (payload: {
    name: string;
    service_kind?: NifiServiceKind;
    nifi_type: string;
    properties?: Record<string, string | null>;
    is_default?: boolean;
  }) => api.post<NifiGlobalService>("/api/nifi-services/", payload),
  get: (id: string) => api.get<NifiServiceConfig>(`/api/nifi-services/${id}`),
  update: (id: string, payload: { name?: string; properties?: Record<string, string | null>; is_default?: boolean }) =>
    api.put<NifiGlobalService>(`/api/nifi-services/${id}`, payload),
  enable: (id: string) => api.post<NifiServiceConfig>(`/api/nifi-services/${id}/enable`),
  disable: (id: string) => api.post<NifiServiceConfig>(`/api/nifi-services/${id}/disable`),
  setDefault: (id: string) => api.post<NifiGlobalService>(`/api/nifi-services/${id}/set-default`),
  remove: (id: string) => api.delete<void>(`/api/nifi-services/${id}`),
};
