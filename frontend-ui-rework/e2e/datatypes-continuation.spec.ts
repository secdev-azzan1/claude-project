// Datatypes continuation pass for the live 8011 backend.
//
// Scope:
// - create or reuse unique JSON / CSV / XML ingestion flows with the
//   codex15aug26- prefix;
// - prove parsed records reach the Kafka topics through the real UI;
// - prove JSON dedup suppression with topic counts plus NiFi processor stats;
// - verify Flows row-click does not open Overview, the eye button does;
// - verify Messages shows Clear topic and DLQ shows Clear DLQ, then cancel the
//   confirmation dialogs so evidence stays visible;
// - leave every created flow deployed/stopped and never clear/delete anything.

import { test, expect, type BrowserContext, type Page } from "playwright/test";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";

const FRONTEND = "http://localhost:3001";
const BACKEND = "http://localhost:8011";
const ART = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "artifacts", "datatypes-continuation");
const CRON = "*/3 * * * *";

const FLOW_JSON_BASE = "codex15aug26-json-posts";
const FLOW_CSV_BASE = "codex15aug26-csv-addresses";
const FLOW_XML_BASE = "codex15aug26-xml-feed";
const SERVICE_JSON = "codex15aug26-json-service";
const SERVICE_CSV = "codex15aug26-csv-service";
const SERVICE_XML = "codex15aug26-xml-service";

process.env.NODE_TLS_REJECT_UNAUTHORIZED = process.env.NODE_TLS_REJECT_UNAUTHORIZED ?? "0";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const rx = (s: string) => new RegExp(s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
const tokenize = (s: string) =>
  s.trim().toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");

test.describe.configure({ mode: "serial" });

let context: BrowserContext;
let page: Page;
const consoleErrors: string[] = [];

type PhaseResult = { ok: boolean; error?: string; note?: string; at: string };
type TopicMessage = { offset: number; ts: string; key: string | null; value: string | null; bytes: number };
type NifiProcStat = { name: string; type: string; flowFilesIn: number; flowFilesOut: number; taskCount: number };
type FlowResult = {
  flowName: string;
  flowId: string;
  serviceName: string;
  topic: string;
  phases: Record<string, PhaseResult>;
  screenshots: string[];
  data: Record<string, unknown>;
  deployed?: boolean;
  started?: boolean;
};

const RESULTS: Record<string, FlowResult> = {};

function saveResults() {
  fs.writeFileSync(path.join(ART, "results.json"), JSON.stringify(RESULTS, null, 2), "utf-8");
}

function result(key: string): FlowResult {
  if (!RESULTS[key]) {
    RESULTS[key] = {
      flowName: "",
      flowId: "",
      serviceName: "",
      topic: "",
      phases: {},
      screenshots: [],
      data: {},
    };
  }
  return RESULTS[key];
}

async function shot(key: string, name: string) {
  await page.screenshot({ path: path.join(ART, `${name}.png`), fullPage: true });
  result(key).screenshots.push(`${name}.png`);
}

async function runPhase(key: string, phase: string, fn: () => Promise<void>): Promise<boolean> {
  const res = result(key);
  try {
    await fn();
    res.phases[phase] = { ok: true, at: new Date().toISOString() };
    saveResults();
    return true;
  } catch (error) {
    res.phases[phase] = { ok: false, error: String(error).slice(0, 1000), at: new Date().toISOString() };
    saveResults();
    throw error;
  }
}

async function backendReady(): Promise<void> {
  const deadline = Date.now() + 180_000;
  while (Date.now() < deadline) {
    try {
      const r = await fetch(`${BACKEND}/api`);
      if (r.ok) return;
    } catch {
      // wait
    }
    await sleep(3000);
  }
  throw new Error(`Backend at ${BACKEND}/api did not return 200 within 180s`);
}

async function apiFlows(): Promise<{ id: string; name?: string; state?: string; nifiProcessGroupId?: string | null }[]> {
  const r = await fetch(`${BACKEND}/api/v2/flows/`);
  if (!r.ok) throw new Error(`GET /api/v2/flows/ -> ${r.status}`);
  return (await r.json()) as never;
}

async function uniqueFlowName(base: string): Promise<string> {
  const names = new Set((await apiFlows()).map((f) => f.name));
  if (!names.has(base)) return base;
  for (let i = 2; ; i += 1) {
    const candidate = `${base}-${i}`;
    if (!names.has(candidate)) return candidate;
  }
}

async function uniqueServiceName(base: string): Promise<string> {
  const services = (await (await fetch(`${BACKEND}/api/v2/services/`)).json()) as { name?: string; retired?: boolean }[];
  const names = new Set(services.filter((s) => !s.retired).map((s) => s.name));
  if (!names.has(base)) return base;
  for (let i = 2; ; i += 1) {
    const candidate = `${base}-${i}`;
    if (!names.has(candidate)) return candidate;
  }
}

async function apiMessages(flowId: string, topic: string): Promise<TopicMessage[]> {
  try {
    const r = await fetch(`${BACKEND}/api/v2/flows/${flowId}/messages?topic=${encodeURIComponent(topic)}`);
    if (!r.ok) return [];
    const j = (await r.json()) as { messages?: TopicMessage[] };
    return j.messages ?? [];
  } catch {
    return [];
  }
}

async function apiMetrics(flowId: string): Promise<Record<string, unknown> | null> {
  try {
    const r = await fetch(`${BACKEND}/api/v2/flows/${flowId}/metrics`);
    if (!r.ok) return null;
    return (await r.json()) as Record<string, unknown>;
  } catch {
    return null;
  }
}

function metricTopicCount(metrics: Record<string, unknown> | null, topic: string): number | null {
  if (!metrics || metrics["available"] !== true) return null;
  const rows = (metrics["topicCounts"] as { topic: string; messages: number }[]) ?? [];
  const row = rows.find((t) => t.topic === topic);
  return row ? row.messages : null;
}

function fieldInput(scope: ReturnType<Page["locator"]>, label: string) {
  return scope.locator(`div:has(> label:text-is("${label}")) input`).first();
}

async function nifiEnv(): Promise<{ url: string; username: string; password: string } | null> {
  try {
    const env = fs.readFileSync(path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../backend/.env"), "utf-8");
    const pick = (k: string) => new RegExp(`^${k}=(.+)$`, "m").exec(env)?.[1].trim();
    const url = pick("NIFI_URL");
    const username = pick("NIFI_USERNAME");
    const password = pick("NIFI_PASSWORD");
    if (!url || !username || !password) return null;
    return { url: url.replace(/\/$/, ""), username, password };
  } catch {
    return null;
  }
}

async function nifiToken(): Promise<string | null> {
  const env = await nifiEnv();
  if (!env) return null;
  try {
    const r = await fetch(`${env.url}/nifi-api/access/token`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: `username=${encodeURIComponent(env.username)}&password=${encodeURIComponent(env.password)}`,
      signal: AbortSignal.timeout(20_000),
    });
    if (!r.ok) return null;
    return (await r.text()).trim();
  } catch {
    return null;
  }
}

async function nifiPgProcessorStats(pgId: string): Promise<NifiProcStat[] | null> {
  const env = await nifiEnv();
  const token = await nifiToken();
  if (!env || !token) return null;
  try {
    const r = await fetch(`${env.url}/nifi-api/flow/process-groups/${pgId}/status?recursive=true`, {
      headers: { Authorization: `Bearer ${token}` },
      signal: AbortSignal.timeout(20_000),
    });
    if (!r.ok) return null;
    const d = (await r.json()) as Record<string, unknown>;
    const out: NifiProcStat[] = [];
    const walk = (pg: Record<string, unknown> | undefined) => {
      const snap = (pg?.["aggregateSnapshot"] ?? {}) as Record<string, unknown>;
      for (const p of (snap["processorStatusSnapshots"] as { processorStatusSnapshot?: Record<string, unknown> }[]) ?? []) {
        const s = p.processorStatusSnapshot ?? {};
        out.push({
          name: String(s["name"] ?? ""),
          type: String(s["type"] ?? ""),
          flowFilesIn: Number(s["flowFilesIn"] ?? 0),
          flowFilesOut: Number(s["flowFilesOut"] ?? 0),
          taskCount: Number(s["taskCount"] ?? 0),
        });
      }
      for (const c of (snap["processGroupStatusSnapshots"] as { processGroupStatusSnapshot?: Record<string, unknown> }[]) ?? []) {
        walk(c.processGroupStatusSnapshot);
      }
    };
    walk((d["processGroupStatus"] ?? {}) as Record<string, unknown>);
    return out;
  } catch {
    return null;
  }
}

function processorKey(stat: NifiProcStat) {
  return `${stat.name}::${stat.type}`;
}

async function waitForProcessorDelta(
  pgId: string,
  baseline: NifiProcStat[],
  timeoutMs: number,
): Promise<{ before: NifiProcStat[]; after: NifiProcStat[]; delta: NifiProcStat[] }> {
  const beforeMap = new Map(baseline.map((s) => [processorKey(s), s]));
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const after = await nifiPgProcessorStats(pgId);
    if (after) {
      const delta = after.filter((s) => {
        const prev = beforeMap.get(processorKey(s));
        return !prev || s.flowFilesIn > prev.flowFilesIn || s.flowFilesOut > prev.flowFilesOut || s.taskCount > prev.taskCount;
      });
      if (delta.some((s) => /dedupe|fetch|split|publish/i.test(s.name))) {
        return { before: baseline, after, delta };
      }
    }
    await sleep(15_000);
  }
  throw new Error("NiFi processor stats never advanced for a second firing within the allotted window");
}

async function pollStableMessages(flowId: string, topic: string, budgetMs: number): Promise<TopicMessage[]> {
  const deadline = Date.now() + budgetMs;
  let lastCount = -1;
  let stable = 0;
  let msgs: TopicMessage[] = [];
  while (Date.now() < deadline) {
    msgs = await apiMessages(flowId, topic);
    if (msgs.length > 0) {
      if (msgs.length === lastCount) {
        stable += 1;
        if (stable >= 2) return msgs;
      } else {
        lastCount = msgs.length;
        stable = 0;
      }
    }
    await sleep(10_000);
  }
  return msgs;
}

function parseNewestMessage(msgs: TopicMessage[]): Record<string, unknown> {
  const withValue = msgs.find((m) => m.value);
  if (!withValue?.value) throw new Error("no message with a non-binary value on the topic");
  const parsed = JSON.parse(withValue.value) as unknown;
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error(`message value is not a JSON object: ${withValue.value.slice(0, 200)}`);
  }
  return parsed as Record<string, unknown>;
}

async function ensureHttpService(key: string, name: string, baseUrl: string, shotName: string) {
  const services = (await (await fetch(`${BACKEND}/api/v2/services/`)).json()) as {
    id: string;
    name?: string;
    retired?: boolean;
  }[];
  if (services.find((s) => s.name === name && !s.retired)) {
    result(key).phases["service_reused"] = { ok: true, note: `service "${name}" already existed`, at: new Date().toISOString() };
    saveResults();
    return;
  }
  await page.goto("/application-services");
  await page.getByRole("button", { name: "Add Service" }).first().click();
  const dialog = page.getByRole("dialog");
  await expect(dialog.getByText("Add Application Service")).toBeVisible();
  await dialog.getByRole("button", { name: /HTTP service/ }).click();
  await fieldInput(dialog, "Name").fill(name);
  await fieldInput(dialog, "Base URL").fill(baseUrl);
  await dialog.getByRole("button", { name: "Create Service" }).click();
  await expect(page.getByText(`Service "${name}" created`)).toBeVisible();
  await shot(key, shotName);
}

async function createFlow(name: string): Promise<string> {
  await page.goto("/flows");
  await page.getByRole("button", { name: "New Flow" }).click();
  await expect(page).toHaveURL(/\/flow-builder\/new/);
  await page.getByPlaceholder("e.g. CrowdStrike Detections").fill(name);
  await page.getByRole("button", { name: "Create & open builder" }).click();
  await expect(page).toHaveURL(/\/flow-builder\/(?!new)[\w-]+/);
  const flowId = new URL(page.url()).pathname.split("/").pop()!;
  await expect(page.getByText("Never deployed")).toBeVisible();
  return flowId;
}

async function configureHttpReadRoot(opts: { serviceName: string; pathValue: string; format?: "csv" | "xml"; recordPath?: string }) {
  await page.getByRole("button", { name: "Place the root" }).click();
  await page.getByRole("menuitem", { name: /http · read/ }).click();
  await expect(page.getByText("Existing service")).toBeVisible();
  await page.getByRole("combobox").filter({ hasText: "Select a service" }).click();
  await page.getByRole("option", { name: rx(opts.serviceName) }).click();
  await expect(page.getByText(/Base URL —/)).toBeVisible();
  await page.getByPlaceholder("/users").fill(opts.pathValue);

  if (opts.format) {
    await page.getByRole("combobox").filter({ hasText: "JSON" }).first().click();
    await page.getByRole("option", { name: opts.format.toUpperCase(), exact: true }).click();
  }
  if (opts.recordPath !== undefined) {
    await page.getByPlaceholder("$.resources[*] (record path)").fill(opts.recordPath);
  }

  const splitSwitch = page.locator('label:has-text("split into records")').getByRole("switch");
  await expect(splitSwitch).toHaveAttribute("aria-checked", "true");
  await page.getByRole("button", { name: /Flow settings/ }).click();
  await page.getByRole("combobox").first().click();
  await page.getByRole("option", { name: /Custom/ }).click();
  await page.getByPlaceholder("*/15 * * * *").fill(CRON);
  await expect(page.getByText("5 fields required")).toHaveCount(0);
  await expect(page.getByText(/^Next:/)).toBeVisible();
}

async function selectHttpReadNode() {
  await page.locator(".react-flow__node", { hasText: "New http read" }).first().click();
}

async function enableDedup(identityField: string, key: string, shotName: string) {
  await selectHttpReadNode();
  await page.locator("#block-section-transforms button").first().click();
  await page.getByRole("button", { name: /Enable deduplication/ }).click();
  await page.getByPlaceholder("identity fields (comma-separated)").fill(identityField);
  await expect(page.getByText(/Platform metadata \(ingest_id, ingest_ts, op\) is always excluded/)).toBeVisible();
  await shot(key, shotName);
}

async function addKafkaWriteChild(entity: string, expectedTopic: string, key: string, shotName: string) {
  await selectHttpReadNode();
  await page.locator('button[title^="Add a block after"]').click();
  await page.getByRole("menuitem", { name: /kafka · write/ }).click();
  await page.locator(".react-flow__node").filter({ hasText: "kafka · write" }).first().click();
  await page.getByPlaceholder("asset · incident · order…").fill(entity);
  await expect(page.getByText(expectedTopic).first()).toBeVisible();
  await shot(key, shotName);
}

async function saveFlow() {
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await expect(page.getByText("Draft saved").first()).toBeVisible();
}

async function deployFlow(key: string, shotName: string) {
  await page.getByRole("button", { name: "Deploy", exact: true }).click();
  const dlg = page.getByRole("dialog").filter({ hasText: "Deploy preflight" });
  await expect(dlg).toBeVisible();
  await expect(dlg.locator("li").first()).toBeVisible({ timeout: 30_000 });
  await shot(key, `${shotName}-preflight`);
  const deployBtn = dlg.getByRole("button", { name: "Deploy" });
  await expect(deployBtn).toBeEnabled();
  await deployBtn.click();
  await expect(page.getByText("Deployed — the flow is built stopped").first()).toBeVisible({ timeout: 240_000 });
  await expect(page.locator('span[aria-label="Stopped"]').first()).toBeVisible({ timeout: 30_000 });
  result(key).deployed = true;
  saveResults();
  await shot(key, `${shotName}-deployed`);
}

async function enableAndStart(key: string, shotName: string) {
  await page.getByRole("button", { name: "More" }).click();
  await page.getByRole("menuitem", { name: "Enable" }).click();
  await page.keyboard.press("Escape");
  const startBtn = page.getByRole("button", { name: "Start", exact: true });
  await expect(startBtn).toBeEnabled({ timeout: 20_000 });
  await startBtn.click();
  await expect(page.getByText("Flow started").first()).toBeVisible({ timeout: 120_000 });
  await expect(page.locator('span[aria-label="Running"]').first()).toBeVisible({ timeout: 30_000 });
  result(key).started = true;
  saveResults();
  await shot(key, `${shotName}-started`);
}

async function stopFlow(key: string, flowId: string, shotName: string) {
  await page.goto(`/flow-builder/${flowId}`);
  await page.getByRole("button", { name: "Stop", exact: true }).click();
  await expect(page.getByText("Stopped — queues retained").first()).toBeVisible({ timeout: 120_000 });
  await shot(key, shotName);
}

async function openFlowSheet(flowName: string) {
  await page.goto("/flows");
  const row = page.getByRole("row").filter({ hasText: flowName }).first();
  await expect(row).toBeVisible();
  await row.getByText(flowName, { exact: true }).click();
  await expect(page.getByRole("tab", { name: "Overview" })).toHaveCount(0);
  await row.getByRole("button", { name: "Overview" }).click();
  await expect(page.getByRole("tab", { name: "Overview" })).toBeVisible();
}

async function verifyMessagesUi(key: string, flowName: string, topic: string, shotName: string) {
  await openFlowSheet(flowName);
  await page.getByRole("tab", { name: "Messages" }).click();
  const topicSelect = page.getByRole("combobox").filter({ hasText: /raw\.|Pick a topic/ }).first();
  await topicSelect.click();
  await page.getByRole("option", { name: topic }).click();
  await expect(page.getByText(/offset \d+/).first()).toBeVisible({ timeout: 30_000 });
  await expect(page.getByRole("button", { name: "Clear topic" })).toBeVisible();
  await shot(key, shotName);

  await page.getByRole("button", { name: "Clear topic" }).click();
  const confirm = page.getByRole("alertdialog");
  await expect(confirm.getByText(`Clear all retained messages from "${topic}"?`)).toBeVisible();
  await confirm.getByRole("button", { name: "Cancel" }).click();

  await page.getByRole("tab", { name: "DLQ" }).click();
  await expect(page.getByRole("button", { name: "Clear DLQ" })).toBeVisible();
  await page.getByRole("button", { name: "Clear DLQ" }).click();
  await expect(confirm.getByText(`Clear all retained messages from "dlq.${tokenize(flowName)}"?`)).toBeVisible();
  await confirm.getByRole("button", { name: "Cancel" }).click();
  await page.keyboard.press("Escape");
}

async function apiPreflightSummary(flowId: string, topic: string) {
  const msgs = await apiMessages(flowId, topic);
  const metrics = await apiMetrics(flowId);
  return {
    messageCount: msgs.length,
    metricTopicCount: metricTopicCount(metrics, topic),
    metrics,
  };
}

test.beforeAll(async ({ browser }) => {
  fs.mkdirSync(ART, { recursive: true });
  await backendReady();
  context = await browser.newContext({ baseURL: FRONTEND, viewport: { width: 1680, height: 1000 } });
  page = await context.newPage();
  page.on("console", (m) => {
    if (m.type() === "error") consoleErrors.push(m.text());
  });
  page.on("pageerror", (e) => consoleErrors.push(`pageerror: ${String(e)}`));
});

test.afterAll(async () => {
  if (consoleErrors.length > 0) {
    fs.writeFileSync(path.join(ART, "console-errors.txt"), consoleErrors.join("\n---\n"), "utf-8");
  }
  saveResults();
  await context?.close().catch(() => undefined);
});

test("JSON continuation — build flow, prove dedup suppression, preserve Messages/DLQ evidence", async () => {
  test.setTimeout(1_500_000);
  const key = "json";
  const flowName = await uniqueFlowName(FLOW_JSON_BASE);
  const serviceName = await uniqueServiceName(SERVICE_JSON);
  const topic = `raw.${tokenize(flowName)}.${tokenize("codex15aug26_post")}`;
  const res = result(key);
  res.flowName = flowName;
  res.serviceName = serviceName;
  res.topic = topic;
  saveResults();

  let flowId = "";

  await runPhase(key, "service", async () => {
    await ensureHttpService(key, serviceName, "https://jsonplaceholder.typicode.com", "01-json-service");
  });

  await runPhase(key, "create_flow", async () => {
    flowId = await createFlow(flowName);
    res.flowId = flowId;
    saveResults();
    await shot(key, "02-json-flow-created");
  });

  await runPhase(key, "configure_root", async () => {
    await configureHttpReadRoot({ serviceName, pathValue: "/posts", recordPath: "$[*]" });
    await shot(key, "03-json-root-configured");
  });

  await runPhase(key, "dedup_panel", async () => {
    await enableDedup("id", key, "04-json-dedup-panel");
  });

  await runPhase(key, "kafka_child", async () => {
    await addKafkaWriteChild("codex15aug26_post", topic, key, "05-json-kafka-child");
  });

  await runPhase(key, "block_test", async () => {
    await selectHttpReadNode();
    await page.locator("#block-section-test button").first().click();
    await page.getByRole("button", { name: "Test block" }).click();
    await expect(page.getByText(/Test succeeded — \d+ sample record\(s\), nothing committed/)).toBeVisible({
      timeout: 90_000,
    });
    await shot(key, "06-json-block-test");
  });

  await runPhase(key, "save", async () => {
    await saveFlow();
  });

  await runPhase(key, "deploy", async () => {
    await deployFlow(key, "07-json");
  });

  await runPhase(key, "start", async () => {
    await enableAndStart(key, "08-json");
  });

  const flow = await apiFlows().then((list) => {
    const found = list.find((f) => f.id === flowId);
    if (!found?.nifiProcessGroupId) throw new Error(`flow ${flowName} did not deploy`);
    return found;
  });

  await runPhase(key, "records", async () => {
    const msgs = await pollStableMessages(flowId, topic, 300_000);
    if (msgs.length === 0) throw new Error(`no messages on ${topic} within 300s of Start`);
    const sample = parseNewestMessage(msgs);
    res.data["firstBatchCount"] = msgs.length;
    res.data["sampleFields"] = Object.keys(sample);
    res.data["sample"] = sample;
    fs.writeFileSync(path.join(ART, "json-sample-messages.json"), JSON.stringify(msgs.slice(0, 5), null, 2), "utf-8");
    saveResults();
    if (!("id" in sample) || !("title" in sample) || !("body" in sample)) {
      throw new Error(`parsed JSON sample lacks post fields: ${JSON.stringify(sample).slice(0, 300)}`);
    }
    await shot(key, "09-json-records-visible");
  });

  await runPhase(key, "dedup_suppression", async () => {
    const pgId = flow.nifiProcessGroupId as string;
    const baseline = (await nifiPgProcessorStats(pgId)) ?? [];
    const before = await apiPreflightSummary(flowId, topic);
    const delta = await waitForProcessorDelta(pgId, baseline, 420_000);
    await sleep(45_000);
    const after = await apiPreflightSummary(flowId, topic);

    res.data["dedup"] = {
      baseline,
      delta,
      before,
      after,
    };
    fs.writeFileSync(
      path.join(ART, "json-dedup-evidence.json"),
      JSON.stringify({ baseline, delta, before, after }, null, 2),
      "utf-8",
    );
    saveResults();

    expect(after.messageCount).toBe(before.messageCount);
    if (before.metricTopicCount !== null && after.metricTopicCount !== null) {
      expect(after.metricTopicCount).toBe(before.metricTopicCount);
    }
  });

  await runPhase(key, "messages_ui", async () => {
    await verifyMessagesUi(key, flowName, topic, "10-json-messages-ui");
  });

  await runPhase(key, "stop", async () => {
    await stopFlow(key, flowId, "11-json-stopped");
  });
});

test("CSV continuation — build flow, prove parsed rows, preserve Messages/DLQ evidence", async () => {
  test.setTimeout(1_200_000);
  const key = "csv";
  const flowName = await uniqueFlowName(FLOW_CSV_BASE);
  const serviceName = await uniqueServiceName(SERVICE_CSV);
  const topic = `raw.${tokenize(flowName)}.${tokenize("codex15aug26_row")}`;
  const res = result(key);
  res.flowName = flowName;
  res.serviceName = serviceName;
  res.topic = topic;
  saveResults();

  let flowId = "";
  let headerCells: string[] = [];

  await runPhase(key, "probe", async () => {
    const candidates = [
      "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/tips.csv",
      "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv",
    ];
    for (const url of candidates) {
      const r = await fetch(url, { headers: { "User-Agent": "Mozilla/5.0" }, signal: AbortSignal.timeout(20_000) });
      const ct = r.headers.get("content-type") ?? "";
      const body = r.ok ? await r.text() : "";
      const firstLine = body.split(/\r?\n/, 1)[0] ?? "";
      const cells = firstLine.split(",").map((c) => c.trim().replace(/^"|"$/g, ""));
      const headerLike = r.ok && (ct.includes("csv") || ct.includes("text/plain")) && firstLine.includes(",") && cells.every((c) => /^[A-Za-z_][A-Za-z0-9_ .\-\/]*$/.test(c));
      if (headerLike) {
        const u = new URL(url);
        res.data["probe"] = { chosen: url, contentType: ct, firstLine, headerCells: cells };
        headerCells = cells;
        res.data["baseUrl"] = `${u.protocol}//${u.host}`;
        res.data["pathValue"] = u.pathname;
        saveResults();
        await shot(key, "20-csv-probe");
        return;
      }
    }
    throw new Error("no CSV candidate passed the probe");
  });

  const baseUrl = String(res.data["baseUrl"] ?? "");
  const pathValue = String(res.data["pathValue"] ?? "");
  if (!baseUrl || !pathValue) throw new Error("CSV probe did not record a usable baseUrl/pathValue");

  await runPhase(key, "service", async () => {
    await ensureHttpService(key, serviceName, baseUrl, "21-csv-service");
  });

  await runPhase(key, "create_flow", async () => {
    flowId = await createFlow(flowName);
    res.flowId = flowId;
    saveResults();
  });

  await runPhase(key, "configure_root", async () => {
    await configureHttpReadRoot({ serviceName, pathValue, format: "csv" });
    await shot(key, "22-csv-root-configured");
  });

  await runPhase(key, "kafka_child", async () => {
    await addKafkaWriteChild("codex15aug26_row", topic, key, "23-csv-kafka-child");
  });

  await runPhase(key, "save", async () => {
    await saveFlow();
  });

  await runPhase(key, "deploy", async () => {
    await deployFlow(key, "24-csv");
  });

  await runPhase(key, "start", async () => {
    await enableAndStart(key, "25-csv");
  });

  await runPhase(key, "records", async () => {
    const msgs = await pollStableMessages(flowId, topic, 300_000);
    if (msgs.length === 0) throw new Error(`no messages on ${topic} within 300s of Start`);
    const sample = parseNewestMessage(msgs);
    const present = headerCells.filter((h) => h in sample);
    res.data["firstBatchCount"] = msgs.length;
    res.data["sampleFields"] = Object.keys(sample);
    res.data["sample"] = sample;
    fs.writeFileSync(path.join(ART, "csv-sample-messages.json"), JSON.stringify(msgs.slice(0, 5), null, 2), "utf-8");
    saveResults();
    if (present.length < Math.min(3, headerCells.length)) {
      throw new Error(`CSV message is not keyed by the CSV header columns. header=${JSON.stringify(headerCells)} sampleKeys=${JSON.stringify(Object.keys(sample))}`);
    }
    await shot(key, "26-csv-records-visible");
  });

  await runPhase(key, "messages_ui", async () => {
    await verifyMessagesUi(key, flowName, topic, "27-csv-messages-ui");
  });

  await runPhase(key, "stop", async () => {
    await stopFlow(key, flowId, "28-csv-stopped");
  });
});

test("XML continuation — build flow, prove parsed elements, preserve Messages/DLQ evidence", async () => {
  test.setTimeout(1_200_000);
  const key = "xml";
  const flowName = await uniqueFlowName(FLOW_XML_BASE);
  const serviceName = await uniqueServiceName(SERVICE_XML);
  const topic = `raw.${tokenize(flowName)}.${tokenize("codex15aug26_item")}`;
  const res = result(key);
  res.flowName = flowName;
  res.serviceName = serviceName;
  res.topic = topic;
  saveResults();

  let flowId = "";
  let expectFields: string[] = [];

  await runPhase(key, "probe", async () => {
    const candidates = [
      {
        url: "https://feeds.bbci.co.uk/news/rss.xml",
        recordPath: "$[0].channel.item[*]",
        expectFields: ["title", "pubDate"],
      },
      {
        url: "https://www.w3schools.com/xml/cd_catalog.xml",
        recordPath: "$[0].CD[*]",
        expectFields: ["TITLE", "ARTIST"],
      },
    ];
    for (const cand of candidates) {
      const r = await fetch(cand.url, { headers: { "User-Agent": "Mozilla/5.0" }, signal: AbortSignal.timeout(20_000) });
      const body = r.ok ? await r.text() : "";
      const xmlish = r.ok && /^\s*<\?xml|<rss[\s>]|<CATALOG[\s>]/i.test(body.slice(0, 300));
      if (xmlish) {
        const u = new URL(cand.url);
        res.data["probe"] = { chosen: cand.url, recordPath: cand.recordPath, head: body.slice(0, 120) };
        res.data["baseUrl"] = `${u.protocol}//${u.host}`;
        res.data["pathValue"] = u.pathname;
        res.data["recordPath"] = cand.recordPath;
        expectFields = cand.expectFields;
        saveResults();
        await shot(key, "30-xml-probe");
        return;
      }
    }
    throw new Error("no XML candidate passed the probe");
  });

  const baseUrl = String(res.data["baseUrl"] ?? "");
  const pathValue = String(res.data["pathValue"] ?? "");
  const recordPath = String(res.data["recordPath"] ?? "");
  if (!baseUrl || !pathValue || !recordPath) throw new Error("XML probe did not record a usable baseUrl/pathValue/recordPath");

  await runPhase(key, "service", async () => {
    await ensureHttpService(key, serviceName, baseUrl, "31-xml-service");
  });

  await runPhase(key, "create_flow", async () => {
    flowId = await createFlow(flowName);
    res.flowId = flowId;
    saveResults();
  });

  await runPhase(key, "configure_root", async () => {
    await configureHttpReadRoot({ serviceName, pathValue, format: "xml", recordPath });
    await shot(key, "32-xml-root-configured");
  });

  await runPhase(key, "kafka_child", async () => {
    await addKafkaWriteChild("codex15aug26_item", topic, key, "33-xml-kafka-child");
  });

  await runPhase(key, "save", async () => {
    await saveFlow();
  });

  await runPhase(key, "deploy", async () => {
    await deployFlow(key, "34-xml");
  });

  await runPhase(key, "start", async () => {
    await enableAndStart(key, "35-xml");
  });

  await runPhase(key, "records", async () => {
    const msgs = await pollStableMessages(flowId, topic, 300_000);
    if (msgs.length === 0) throw new Error(`no messages on ${topic} within 300s of Start`);
    const sample = parseNewestMessage(msgs);
    const present = expectFields.filter((f) => f in sample);
    res.data["firstBatchCount"] = msgs.length;
    res.data["sampleFields"] = Object.keys(sample);
    res.data["sample"] = sample;
    fs.writeFileSync(path.join(ART, "xml-sample-messages.json"), JSON.stringify(msgs.slice(0, 5), null, 2), "utf-8");
    saveResults();
    if (present.length === 0) {
      throw new Error(`XML-derived sample lacks the expected element fields ${JSON.stringify(expectFields)}: keys=${JSON.stringify(Object.keys(sample))}`);
    }
    await shot(key, "36-xml-records-visible");
  });

  await runPhase(key, "messages_ui", async () => {
    await verifyMessagesUi(key, flowName, topic, "37-xml-messages-ui");
  });

  await runPhase(key, "stop", async () => {
    await stopFlow(key, flowId, "38-xml-stopped");
  });
});

test("Final continuation evidence — flows exist, row click stays inert, Overview is explicit, evidence preserved", async () => {
  test.setTimeout(300_000);

  const flows = await apiFlows();
  fs.writeFileSync(path.join(ART, "flows-final.json"), JSON.stringify(flows, null, 2), "utf-8");
  for (const base of [FLOW_JSON_BASE, FLOW_CSV_BASE, FLOW_XML_BASE]) {
    const matches = flows.filter((f) => (f.name ?? "").startsWith(base));
    expect.soft(matches.length, `${base} should exist in GET /api/v2/flows/`).toBeGreaterThan(0);
    for (const f of matches) {
      expect.soft(f.nifiProcessGroupId, `${f.name} should remain deployed`).toBeTruthy();
      expect.soft(f.state, `${f.name} should be left Stopped`).toBe("Stopped");
    }
  }

  const jsonFlow = flows.find((f) => (f.name ?? "").startsWith(FLOW_JSON_BASE));
  if (!jsonFlow) throw new Error(`could not locate the JSON flow for UI verification`);

  await page.goto("/flows");
  const row = page.getByRole("row").filter({ hasText: jsonFlow.name ?? FLOW_JSON_BASE }).first();
  await expect(row).toBeVisible();
  await row.getByText(jsonFlow.name ?? FLOW_JSON_BASE, { exact: true }).click();
  await expect(page.getByRole("tab", { name: "Overview" })).toHaveCount(0);
  await row.getByRole("button", { name: "Overview" }).click();
  await expect(page.getByRole("tab", { name: "Overview" })).toBeVisible();
  await page.getByRole("tab", { name: "Messages" }).click();
  await expect(page.getByRole("button", { name: "Clear topic" })).toBeVisible();
  await page.getByRole("tab", { name: "DLQ" }).click();
  await expect(page.getByRole("button", { name: "Clear DLQ" })).toBeVisible();
  await page.screenshot({ path: path.join(ART, "40-final-flows-page.png"), fullPage: true });
  saveResults();
});
