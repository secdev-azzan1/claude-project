export type FlowDesignerSchemaSourceType = "rest" | "postgres" | "mongo" | "smb" | "webhook" | "trino" | string;

export type FlowDesignerSchemaStatus = "Draft" | "Needs Verification" | "Verified" | string;

const normalizeSourceType = (sourceType: FlowDesignerSchemaSourceType): string =>
  String(sourceType || "").trim().toLowerCase().replace("_", " ");

export const requiresSchemaWorkflow = (sourceType: FlowDesignerSchemaSourceType): boolean =>
  normalizeSourceType(sourceType) !== "trino";

export const shouldBlockSaveForUnverifiedSchemas = (
  sourceType: FlowDesignerSchemaSourceType,
  statuses: FlowDesignerSchemaStatus[],
): boolean => requiresSchemaWorkflow(sourceType) && statuses.some((status) => status !== "Verified");

export const getEntitySchemaStatusLabel = (
  sourceType: FlowDesignerSchemaSourceType,
  status: FlowDesignerSchemaStatus,
): string => (requiresSchemaWorkflow(sourceType) ? status : "Schema Optional");
