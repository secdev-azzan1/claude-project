import { describe, expect, it } from "vitest";

import { getFlowExportFilename } from "./flowApi";

describe("flow export API helpers", () => {
  it("extracts the attachment filename from content-disposition", () => {
    expect(getFlowExportFilename('attachment; filename="Orders.flowpack.json"')).toBe("Orders.flowpack.json");
  });

  it("falls back to flow export filename when header is missing", () => {
    expect(getFlowExportFilename(null)).toBe("flow-export.flowpack.json");
  });
});
