export type ListSummary = {
  visible: string[];
  hiddenCount: number;
  overflowLabel: string | null;
};

export function summarizeList(values: string[], visibleLimit = 2): ListSummary {
  const normalizedLimit = Math.max(0, Math.floor(visibleLimit));
  const visible = values.slice(0, normalizedLimit);

  return {
    visible,
    hiddenCount: Math.max(0, values.length - visible.length),
    overflowLabel: values.length > visible.length ? `See ${values.length - visible.length} more` : null,
  };
}
