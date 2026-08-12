type ImportQueryClient = {
  invalidateQueries: (options: { queryKey: string[] }) => Promise<unknown>;
};

export async function refreshImportedFlowQueries(queryClient: ImportQueryClient): Promise<void> {
  await queryClient.invalidateQueries({ queryKey: ["flows"] });
  await queryClient.invalidateQueries({ queryKey: ["schemas"] });
  await queryClient.invalidateQueries({ queryKey: ["dashboard"] });
}
