import { describe, expect, it } from "vitest";
import { flowSinkTopic, kafkaConnectLinkIssue, syncTopicIssue } from "./kafkaConnectLink";
import type { Flow, FlowBlock } from "./types";

const block = (over: Partial<FlowBlock> = {}): FlowBlock => ({
  id: "sink-1",
  adapter: "kc",
  name: "Orders sink",
  parentId: "topic-1",
  serviceId: "service-1",
  entity: "orders",
  config: { attachTopicId: "topic-1" },
  transforms: [],
  ...over,
});

const flow = (over: Partial<Flow> = {}): Flow => ({
  id: "flow-1",
  name: "Orders",
  state: "Draft",
  enabled: false,
  cron: null,
  blocks: [block()],
  topics: [{ id: "topic-1", name: "orders-topic", kind: "materialized", sealed: false }],
  variables: [],
  servicePins: {},
  ...over,
});

const sync = (config: Record<string, string> = {}) => ({
  direction: "sink" as const,
  connectorClass: "com.example.Sink",
  config,
});

describe("Kafka Connect flow linking", () => {
  it("requires one exact topic", () => {
    expect(syncTopicIssue([{ key: "topics.regex", value: "orders-.*" }])).toMatch(/exact topic/);
    expect(syncTopicIssue([{ key: "topics", value: "a,b" }])).toMatch(/exactly one/);
    expect(syncTopicIssue([{ key: "topics", value: "orders-topic" }])).toBeNull();
  });

  it("accepts a matching kc subscription", () => {
    expect(kafkaConnectLinkIssue(flow(), block(), sync({ topics: "orders-topic" }))).toBeNull();
    expect(flowSinkTopic(flow(), block())).toBe("orders-topic");
  });

  it("rejects source syncs and mismatched topics", () => {
    expect(kafkaConnectLinkIssue(flow(), block(), { ...sync({ topics: "other-topic" }), direction: "source" })).toMatch(/sink-direction/);
    expect(kafkaConnectLinkIssue(flow(), block(), sync({ topics: "other-topic" }))).toMatch(/Topic mismatch/);
  });

  it("uses the flow-owned topic for kafka_kc", () => {
    const governed = block({ adapter: "kafka_kc", parentId: "upstream", config: {} });
    const governedFlow = flow({
      blocks: [governed],
      topics: [{ id: "governed-topic", name: "orders-topic", kind: "materialized", sealed: true, writerBlockId: governed.id }],
    });
    expect(kafkaConnectLinkIssue(governedFlow, governed, sync({ topics: "orders-topic" }))).toBeNull();
  });

  it("accepts a block without a serviceId — the sink config carries its own endpoint now", () => {
    const noService = block({ serviceId: undefined });
    expect(kafkaConnectLinkIssue(flow({ blocks: [noService] }), noService, sync({ topics: "orders-topic" }))).toBeNull();
  });
});
