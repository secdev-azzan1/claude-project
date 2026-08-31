import { deriveTopicName } from "./naming";
import type { KafkaConnectSync } from "./api";
import type { Flow, FlowBlock } from "./types";

type ConfigRow = { key: string; value: string };

function topicValues(rows: ConfigRow[]): string[] {
  const values = rows
    .filter((row) => row.key.trim() === "topics" || row.key.trim() === "topic")
    .flatMap((row) => row.value.split(",").map((value) => value.trim()).filter(Boolean));
  return [...new Set(values)];
}

export function syncTopicIssue(rows: ConfigRow[]): string | null {
  const regex = rows.find((row) => row.key.trim() === "topics.regex" && row.value.trim());
  if (regex) return "A flow-linked sync must use one exact topic; topics.regex is not supported.";
  const values = topicValues(rows);
  if (values.length === 0) return "Set exactly one topic (topics or topic) before linking this sync to a flow.";
  if (values.length !== 1) return "A flow-linked sync must target exactly one topic.";
  return null;
}

export function flowSinkTopic(flow: Flow, block: FlowBlock): string | null {
  if (block.adapter === "kc") {
    const topic = flow.topics.find((item) => item.id === block.config.attachTopicId);
    return topic && !topic.sealed ? topic.name.trim() || null : null;
  }
  return flow.topics.find((item) => item.writerBlockId === block.id)?.name.trim() || deriveTopicName(flow, block).value || null;
}

export function kafkaConnectLinkIssue(
  flow: Flow,
  block: FlowBlock,
  sync: Pick<KafkaConnectSync, "direction" | "connectorClass" | "config">,
): string | null {
  if (block.adapter !== "kc" && block.adapter !== "kafka_kc") return "This is not a Kafka Connect sink block.";
  if (sync.direction !== "sink") return "Only sink-direction syncs can be linked to flow sink blocks.";

  const serviceId = String(block.serviceId ?? block.config.sinkServiceId ?? "").trim();
  if (!serviceId) return "Select a sink destination service on the flow block first.";
  if (!block.entity?.trim()) return "Set the sink entity on the flow block first.";

  const flowTopic = flowSinkTopic(flow, block);
  if (!flowTopic) {
    return block.adapter === "kc"
      ? "Attach this subscription to a valid, unsealed flow topic first."
      : "The flow sink does not have a resolvable topic yet.";
  }

  const rows = Object.entries(sync.config ?? {}).map(([key, value]) => ({ key, value: String(value) }));
  const topicIssue = syncTopicIssue(rows);
  if (topicIssue) return topicIssue;
  const configured = topicValues(rows)[0];
  if (configured !== flowTopic) return `Topic mismatch: the flow resolves to '${flowTopic}', but the sync targets '${configured}'.`;

  const blockClass = typeof block.config.sinkConfig === "object" && block.config.sinkConfig
    ? String((block.config.sinkConfig as Record<string, unknown>)["connector.class"] ?? "").trim()
    : "";
  if (blockClass && sync.connectorClass && blockClass !== sync.connectorClass) {
    return `Connector class mismatch: the flow block uses '${blockClass}' but the sync uses '${sync.connectorClass}'.`;
  }
  return null;
}

export function kafkaConnectLinkIssueForRows(
  flow: Flow,
  block: FlowBlock,
  direction: "sink" | "source",
  connectorClass: string,
  rows: ConfigRow[],
): string | null {
  return kafkaConnectLinkIssue(flow, block, {
    direction,
    connectorClass,
    config: Object.fromEntries(rows.filter((row) => row.key.trim()).map((row) => [row.key.trim(), row.value])),
  });
}
