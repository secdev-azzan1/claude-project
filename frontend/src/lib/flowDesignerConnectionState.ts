export type SourceConnectionMode = "manual" | "application_service";
export type FlowDesignerSourceType = "rest" | "postgres" | "mongo" | "smb" | "webhook" | "trino";

export type FlowDesignerConnectionState = {
  restConnectionMode: SourceConnectionMode;
  restApplicationServiceId: string;
  smbConnectionMode: SourceConnectionMode;
  smbApplicationServiceId: string;
  webhookConnectionMode: SourceConnectionMode;
  webhookApplicationServiceId: string;
};

export type FlowDesignerConnectionPayload = Partial<FlowDesignerConnectionState> & {
  sourceType?: FlowDesignerSourceType | string;
};

export type SourceConnectionRecord = {
  sourceType?: FlowDesignerSourceType | string | null;
  connection_setup_mode?: SourceConnectionMode | string | null;
  application_service_id?: string | null;
};

const normalizeMode = (value: unknown): SourceConnectionMode =>
  value === "application_service" ? "application_service" : "manual";

const asString = (value: unknown): string => (typeof value === "string" ? value : "");

export function mergeSourceConnectionState(
  defaults: FlowDesignerConnectionState,
  source: SourceConnectionRecord | null | undefined,
  payload: FlowDesignerConnectionPayload | null | undefined,
): FlowDesignerConnectionState {
  const state: FlowDesignerConnectionState = {
    ...defaults,
    restConnectionMode: normalizeMode(payload?.restConnectionMode ?? defaults.restConnectionMode),
    restApplicationServiceId: asString(payload?.restApplicationServiceId) || defaults.restApplicationServiceId,
    smbConnectionMode: normalizeMode(payload?.smbConnectionMode ?? defaults.smbConnectionMode),
    smbApplicationServiceId: asString(payload?.smbApplicationServiceId) || defaults.smbApplicationServiceId,
    webhookConnectionMode: normalizeMode(payload?.webhookConnectionMode ?? defaults.webhookConnectionMode),
    webhookApplicationServiceId: asString(payload?.webhookApplicationServiceId) || defaults.webhookApplicationServiceId,
  };

  const sourceType = asString(source?.sourceType ?? payload?.sourceType);
  const sourceMode = normalizeMode(source?.connection_setup_mode);
  const sourceServiceId = asString(source?.application_service_id);

  if (sourceType === "rest") {
    state.restConnectionMode = sourceMode;
    state.restApplicationServiceId = sourceMode === "application_service" ? sourceServiceId : "";
  } else if (sourceType === "smb") {
    state.smbConnectionMode = sourceMode;
    state.smbApplicationServiceId = sourceMode === "application_service" ? sourceServiceId : "";
  } else if (sourceType === "webhook") {
    state.webhookConnectionMode = sourceMode;
    state.webhookApplicationServiceId = sourceMode === "application_service" ? sourceServiceId : "";
  }

  return state;
}
