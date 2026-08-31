import { api } from "@/lib/api";

export type ApplicationServiceType = "REST API" | "SMB" | "Webhook";

export type ApplicationService = {
  id: string;
  name: string;
  service_type: ApplicationServiceType;
  description?: string | null;
  config: Record<string, unknown>;
  has_secrets?: Record<string, boolean>;
  health: "Healthy" | "Failed" | "Not Tested";
  last_tested?: string | null;
  created_at?: string;
  updated_at?: string;
};

export type ApplicationServicePayload = {
  name: string;
  service_type: ApplicationServiceType;
  description?: string | null;
  config: Record<string, unknown>;
};

export const applicationServicesApi = {
  list: () => api.get<ApplicationService[]>("/api/application-services/"),
  get: (id: string) => api.get<ApplicationService>(`/api/application-services/${id}`),
  create: (payload: ApplicationServicePayload) =>
    api.post<ApplicationService>("/api/application-services/", payload),
  update: (id: string, payload: Partial<ApplicationServicePayload>) =>
    api.put<ApplicationService>(`/api/application-services/${id}`, payload),
  test: (id: string) =>
    api.post<{ ok: boolean; health: string; message: string; last_tested?: string }>(
      `/api/application-services/${id}/test`,
    ),
  remove: (id: string) => api.delete<void>(`/api/application-services/${id}`),
};
