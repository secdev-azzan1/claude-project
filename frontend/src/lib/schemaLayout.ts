export const schemaWorkspaceLayout = {
  grid: "grid grid-cols-1 gap-4 lg:grid-cols-3 lg:h-[calc(100vh-9.5rem)] lg:min-h-0 lg:overflow-hidden",
  artifactCard: "lg:col-span-1 lg:flex lg:h-full lg:min-h-0 lg:flex-col lg:overflow-hidden",
  artifactContent: "flex min-h-0 flex-1 flex-col space-y-2",
  artifactList: "min-h-0 space-y-2 lg:flex-1 lg:overflow-y-auto lg:pr-1",
  detailPane: "lg:col-span-2 lg:min-h-0",
  detailCard: "lg:flex lg:h-full lg:min-h-0 lg:flex-col lg:overflow-hidden",
  detailScroll: "min-h-0 lg:flex-1 lg:overflow-y-auto",
} as const;

export const schemaFieldRowKey = (index: number): string => `schema-field-row-${index}`;
