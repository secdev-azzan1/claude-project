import { describe, expect, it } from "vitest";

import { mergeSourceConnectionState } from "@/lib/flowDesignerConnectionState";

describe("mergeSourceConnectionState", () => {
  it("uses persisted REST source connection mode when old designer payload lacks it", () => {
    const state = mergeSourceConnectionState(
      {
        restConnectionMode: "manual",
        restApplicationServiceId: "",
        smbConnectionMode: "manual",
        smbApplicationServiceId: "",
        webhookConnectionMode: "manual",
        webhookApplicationServiceId: "",
      },
      {
        sourceType: "rest",
        connection_setup_mode: "application_service",
        application_service_id: "service-123",
      },
      {},
    );

    expect(state.restConnectionMode).toBe("application_service");
    expect(state.restApplicationServiceId).toBe("service-123");
  });
});
