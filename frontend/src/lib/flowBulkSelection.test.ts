import { describe, expect, it } from "vitest";

import { getVisibleSelectionState, toggleVisibleSelection } from "@/lib/flowBulkSelection";

describe("flow bulk selection", () => {
  it("reports indeterminate when some visible rows are selected", () => {
    expect(getVisibleSelectionState(["a"], ["a", "b"])).toBe("indeterminate");
  });

  it("selects all visible rows when not all visible rows are selected", () => {
    expect(toggleVisibleSelection(["hidden"], ["a", "b"])).toEqual(["hidden", "a", "b"]);
  });

  it("clears only visible rows when all visible rows are selected", () => {
    expect(toggleVisibleSelection(["hidden", "a", "b"], ["a", "b"])).toEqual(["hidden"]);
  });
});
