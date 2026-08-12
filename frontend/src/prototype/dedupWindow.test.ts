// Dedup TTL bounds per the MVP dedup rules: minimum 1 minute, maximum 365
// days, default 24 hours. windowHours is the canonical unit, so 1 minute is
// 1/60 and 365 days is 8760. These exported validators back both the
// TransformsEditor's inline messages and validateBlock's badges — one
// source of truth, tested here directly.

import { describe, expect, it } from "vitest";
import {
  DEDUP_WINDOW_DEFAULT_HOURS,
  DEDUP_WINDOW_MAX_HOURS,
  DEDUP_WINDOW_MIN_HOURS,
  dedupIdentityFieldsIssue,
  dedupWindowIssue,
} from "./validation";

describe("dedup window bounds", () => {
  it("exposes the documented bounds and default", () => {
    expect(DEDUP_WINDOW_MIN_HOURS).toBeCloseTo(1 / 60);
    expect(DEDUP_WINDOW_MAX_HOURS).toBe(8760);
    expect(DEDUP_WINDOW_DEFAULT_HOURS).toBe(24);
  });

  it("accepts the minimum — 1 minute", () => {
    expect(dedupWindowIssue(1 / 60)).toBeNull();
  });

  it("rejects anything below the minimum", () => {
    expect(dedupWindowIssue(1 / 60 - 0.0001)).toBe("Window must be between 1 minute and 365 days");
    expect(dedupWindowIssue(0)).toBe("Window must be between 1 minute and 365 days");
  });

  it("accepts the maximum — 365 days", () => {
    expect(dedupWindowIssue(8760)).toBeNull();
  });

  it("rejects anything above the maximum", () => {
    expect(dedupWindowIssue(8761)).toBe("Window must be between 1 minute and 365 days");
  });

  it("accepts the default — 24 hours", () => {
    expect(dedupWindowIssue(DEDUP_WINDOW_DEFAULT_HOURS)).toBeNull();
  });

  it("rejects missing or non-numeric values", () => {
    expect(dedupWindowIssue(undefined)).toBe("Window must be between 1 minute and 365 days");
    expect(dedupWindowIssue("24")).toBe("Window must be between 1 minute and 365 days");
    expect(dedupWindowIssue(Number.NaN)).toBe("Window must be between 1 minute and 365 days");
  });
});

describe("dedup identity fields", () => {
  it("requires at least one identity field", () => {
    expect(dedupIdentityFieldsIssue(undefined)).toBe("At least one identity field is required");
    expect(dedupIdentityFieldsIssue([])).toBe("At least one identity field is required");
    expect(dedupIdentityFieldsIssue(["", "  "])).toBe("At least one identity field is required");
  });

  it("passes once at least one non-blank field is present", () => {
    expect(dedupIdentityFieldsIssue(["assetId"])).toBeNull();
    expect(dedupIdentityFieldsIssue(["", "hostName"])).toBeNull();
  });
});
