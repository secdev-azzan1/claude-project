import { describe, expect, it } from "vitest";
import { normalizeSmbClickedPath } from "@/lib/smbPathNormalization";

describe("normalizeSmbClickedPath", () => {
  it("converts bracket-quoted clicked header path to sanitized dot path", () => {
    expect(normalizeSmbClickedPath('$["Sl. No"]', "Sl__No")).toBe("$.Sl__No");
  });

  it("keeps non-bracket JSONPath unchanged", () => {
    expect(normalizeSmbClickedPath("$.id", "id")).toBe("$.id");
  });
});

