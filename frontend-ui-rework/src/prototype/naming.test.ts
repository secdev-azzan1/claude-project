import { describe, expect, it } from "vitest";
import {
  baseTopicName,
  cleanTopicOverride,
  cronPreview,
  derivedTopicDefault,
  deriveTopicName,
  dlqName,
  isValidCron,
  overrideMatchesDerived,
  tableName,
  tokenize,
  topicNameCollision,
} from "./naming";
import type { Flow, FlowBlock } from "./types";

const block = (over: Partial<FlowBlock>): FlowBlock => ({
  id: "b1",
  adapter: "kafka",
  mode: "write",
  name: "Write",
  parentId: null,
  serviceId: null,
  entity: "asset",
  config: {},
  transforms: [],
  ...over,
});

const flow = (blocks: FlowBlock[], name = "Asset Retirement"): Flow => ({
  id: "f1",
  name,
  state: "Draft",
  enabled: false,
  cron: null,
  blocks,
  topics: [],
  variables: [],
  servicePins: {},
  createdAt: "",
  updatedAt: "",
});

describe("tokenize", () => {
  it("lowercases and collapses non-alphanumerics", () => {
    expect(tokenize("Rapid7  Assets!")).toBe("rapid7_assets");
    expect(tokenize("  FortiSIEM--Events ")).toBe("fortisiem_events");
  });
});

describe("derived names", () => {
  it("derives topic, table and dlq names from flow + entity", () => {
    expect(baseTopicName("Rapid7 Assets", "asset")).toBe("raw.rapid7_assets.asset");
    expect(tableName("Rapid7 Assets", "asset")).toBe("bronze.rapid7_assets.asset__raw");
    expect(dlqName("Rapid7 Assets")).toBe("dlq.rapid7_assets");
  });

  it("gives the sole writer the bare name", () => {
    const b = block({});
    expect(deriveTopicName(flow([b]), b).value).toBe("raw.asset_retirement.asset");
  });

  it("governed write wins the bare name; schemaless copy takes a variant from its branch label", () => {
    const governed = block({ id: "g", adapter: "kafka_kc", mode: undefined, branch: { name: "all" } });
    const copy = block({ id: "c", branch: { name: "active" } });
    const f = flow([governed, copy]);
    expect(deriveTopicName(f, governed).value).toBe("raw.asset_retirement.asset");
    expect(deriveTopicName(f, copy).value).toBe("raw.asset_retirement.asset.active");
  });

  it("honors a custom override and warns on raw.* names", () => {
    const b = block({ topicOverride: "asset_retired" });
    const d = deriveTopicName(flow([b]), b);
    expect(d.value).toBe("asset_retired");
    expect(d.overridden).toBe(true);
    expect(d.warning).toBeUndefined();
    const rawB = block({ topicOverride: "raw.sneaky.name" });
    expect(deriveTopicName(flow([rawB]), rawB).warning).toMatch(/raw\.\*/);
  });

  it("honors the override on kafka_kc too — not just plain kafka writes (R7)", () => {
    const governed = block({ id: "g", adapter: "kafka_kc", mode: undefined, topicOverride: "Governed Feed!" });
    const d = deriveTopicName(flow([governed]), governed);
    expect(d.value).toBe("governed_feed_");
    expect(d.overridden).toBe(true);
    expect(cleanTopicOverride("Governed Feed!")).toBe("governed_feed_");
  });

  it("still ignores an override on blocks that own no topic", () => {
    const read = block({ id: "r", adapter: "http", mode: "read", topicOverride: "nope" });
    expect(deriveTopicName(flow([read]), read).value).toBe("");
  });

  it("reports the derived default behind an override, and when the two agree", () => {
    const b = block({ topicOverride: "raw.asset_retirement.asset" });
    expect(derivedTopicDefault(flow([b]), b).value).toBe("raw.asset_retirement.asset");
    expect(overrideMatchesDerived(flow([b]), b)).toBe(true);
    // A genuinely custom name is not the derived one, so the warnings stand.
    const custom = block({ topicOverride: "asset_retired" });
    expect(overrideMatchesDerived(flow([custom]), custom)).toBe(false);
    // And a candidate the user is still typing can be checked before it is saved.
    const bare = block({});
    expect(overrideMatchesDerived(flow([bare]), bare, "RAW.Asset_Retirement.asset")).toBe(true);
    expect(overrideMatchesDerived(flow([bare]), bare, "")).toBe(false);
  });

  it("flags reserved-name collisions", () => {
    expect(topicNameCollision("raw.rapid7_assets.asset")).toMatch(/already reserved/);
    expect(topicNameCollision("raw.brand_new.topic")).toBeNull();
  });
});

describe("cron", () => {
  it("validates 5-field expressions and previews presets", () => {
    expect(isValidCron("*/15 * * * *")).toBe(true);
    expect(isValidCron("15 * * *")).toBe(false);
    expect(isValidCron(null)).toBe(true);
    expect(cronPreview("*/15 * * * *")).toHaveLength(3);
    expect(cronPreview(null)).toHaveLength(0);
  });
});
