export type VisibleSelectionState = boolean | "indeterminate";

export function getVisibleSelectionState(selectedIds: string[], visibleIds: string[]): VisibleSelectionState {
  if (visibleIds.length === 0) return false;
  const selected = new Set(selectedIds);
  const selectedVisibleCount = visibleIds.filter((id) => selected.has(id)).length;

  if (selectedVisibleCount === 0) return false;
  if (selectedVisibleCount === visibleIds.length) return true;
  return "indeterminate";
}

export function toggleVisibleSelection(selectedIds: string[], visibleIds: string[]): string[] {
  const selected = new Set(selectedIds);
  const allVisibleSelected = visibleIds.length > 0 && visibleIds.every((id) => selected.has(id));

  if (allVisibleSelected) {
    visibleIds.forEach((id) => selected.delete(id));
  } else {
    visibleIds.forEach((id) => selected.add(id));
  }

  return Array.from(selected);
}
