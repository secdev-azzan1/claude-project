// http adapter Path field: the service supplies the base URL, so a path that
// starts with a scheme is a full URL typed (or pasted) where only the path
// belongs — it would compile into a broken concatenated NiFi URL. This
// exported validator backs both validateBlock's badge and the inline hint in
// HttpSettings — one source of truth, tested here directly (see
// dedupWindow.test.ts for the same pattern applied to dedup).

import { describe, expect, it } from "vitest";
import { httpPathIssue } from "./validation";

describe("http path issue", () => {
  it("accepts a plain path", () => {
    expect(httpPathIssue("/users")).toBeNull();
    expect(httpPathIssue("/scans/${scan_id}")).toBeNull();
  });

  it("accepts an empty path (that's the separate 'set the request path' issue)", () => {
    expect(httpPathIssue("")).toBeNull();
  });

  it("flags a full URL — http or https, any case", () => {
    expect(httpPathIssue("http://api.example.com/users")).toBe(
      "HTTP path must be a path (the service provides the base URL) — got a full URL.",
    );
    expect(httpPathIssue("https://api.example.com/users")).toBe(
      "HTTP path must be a path (the service provides the base URL) — got a full URL.",
    );
    expect(httpPathIssue("HTTPS://api.example.com/users")).toBe(
      "HTTP path must be a path (the service provides the base URL) — got a full URL.",
    );
  });

  it("does not flag a path that merely mentions a scheme mid-string", () => {
    expect(httpPathIssue("/redirect?url=http://example.com")).toBeNull();
  });
});
