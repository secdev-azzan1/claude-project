import { describe, expect, it } from "vitest";

import { summarizeList } from "@/lib/flowTableSummary";

describe("summarizeList", () => {
  it("shows the first two values and counts the hidden values by default", () => {
    expect(summarizeList(["asset_detail", "asset_services", "asset_software", "asset_tags"])).toEqual({
      visible: ["asset_detail", "asset_services"],
      hiddenCount: 2,
      overflowLabel: "See 2 more",
    });
  });

  it("does not report hidden values when the list fits inside the limit", () => {
    expect(summarizeList(["sites", "site_assets"])).toEqual({
      visible: ["sites", "site_assets"],
      hiddenCount: 0,
      overflowLabel: null,
    });
  });
});
