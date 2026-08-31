import { describe, expect, it } from "vitest";

import { buildServiceManagerCreateOptions, HTTP_SERVICE_OPTION_ID } from "@/lib/serviceManagerOptions";
import type { NifiControllerServiceType } from "@/lib/nifiServicesApi";

describe("buildServiceManagerCreateOptions", () => {
  it("adds HTTP Service as an application-backed option after live NiFi service types", () => {
    const types: NifiControllerServiceType[] = [
      {
        type: "org.apache.nifi.mongodb.MongoDBControllerService",
        description: "MongoDB service from NiFi",
        tags: ["mongodb"],
      },
      {
        type: "org.apache.nifi.kafka.KafkaConnectionService",
        description: "Kafka service from NiFi",
        tags: ["kafka"],
      },
    ];

    const options = buildServiceManagerCreateOptions(types);

    expect(options).toHaveLength(3);
    expect(options[0]).toMatchObject({
      id: "nifi:org.apache.nifi.mongodb.MongoDBControllerService",
      source: "nifi",
      label: "MongoDBControllerService",
      nifiType: "org.apache.nifi.mongodb.MongoDBControllerService",
    });
    expect(options[2]).toMatchObject({
      id: HTTP_SERVICE_OPTION_ID,
      source: "application",
      label: "HTTP Service",
      applicationType: "REST API",
    });
  });
});
