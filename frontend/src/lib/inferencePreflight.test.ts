import { describe, expect, it } from "vitest";
import { collectMissingPathParamResolutionsForInference } from "./inferencePreflight";

describe("collectMissingPathParamResolutionsForInference", () => {
  const organization = {
    id: "forti_organizations",
    name: "organizations",
    endpointPath: "/config/Domain",
    attributeExtractions: [{ attributeName: "org_name" }],
  };

  const deviceList = {
    id: "forti_device_list",
    name: "device_list",
    endpointPath: "/cmdbDeviceInfo/devices?organization=${org_name}",
    parentStreamId: "forti_organizations",
    attributeExtractions: [{ attributeName: "org_name" }, { attributeName: "access_ip" }],
  };

  const deviceDetail = {
    id: "forti_device_detail",
    name: "device_detail",
    endpointPath: "/cmdbDeviceInfo/device?organization=${org_name}&ip=${access_ip}",
    routeSource: { parentStreamId: "forti_device_list" },
    attributeExtractions: [],
  };

  it("validates only the selected entity stream branch for inference", () => {
    const issues = collectMissingPathParamResolutionsForInference({
      sourceType: "rest",
      streams: [organization, deviceList, deviceDetail],
      selectedEntityStreamId: "forti_organizations",
    });

    expect(issues).toEqual([]);
  });

  it("resolves route child path params from the route parent extractions", () => {
    const issues = collectMissingPathParamResolutionsForInference({
      sourceType: "rest",
      streams: [organization, deviceList, deviceDetail],
      selectedEntityStreamId: "forti_device_detail",
    });

    expect(issues).toEqual([]);
  });
});
