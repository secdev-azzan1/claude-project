// UI E2E DATATYPE verification — proves the app processes JSON, CSV and XML
// end to end through the real UI (frontend :3001, backend :8010, live
// NiFi/Kafka). Three flows, prefix "dt":
//   1. JSON  "dt json products"  — dummyjson /products, recordPath
//      $.products[*], NEW dedup panel (identity "id"), kafka write dt_product.
//      Verifies ~30 parsed records AND dedup suppression across a 2nd cron
//      firing (topic count must NOT grow).
//   2. CSV   "dt csv addresses"  — probed CSV url, responseFormat csv,
//      recordPath blank (compiler default "$" splits the ConvertRecord JSON
//      array), kafka write dt_row. Verifies messages are JSON OBJECTS carrying
//      the CSV's header column fields.
//   3. XML   "dt xml feed"       — probed XML url, responseFormat xml,
//      recordPath into the ConvertRecord JSON array (XMLReader infer-schema,
//      Expect Records as Array=false → the whole doc is ONE record, wrapped in
//      a JSON array by the writer → $[0].<...>), kafka write dt_item. Verifies
//      messages are JSON objects derived from XML elements; on failure captures
//      DLQ/runtime/metrics defect evidence — never silently passes.
//
// HARD RULE (user): NOTHING is deleted or cleaned up. No pre-clean, no
// undeploy, no retire. Every flow is left DEPLOYED and STOPPED as evidence.
// Reruns therefore use a name suffix (" 2", " 3"…) if a previous partial run
// left a same-named flow behind; services are reused idempotently by name.
//
// Evidence-first architecture: each datatype is ONE serial test that runs its
// phases guarded (a phase failure aborts later phases of the SAME datatype but
// never throws, so the next datatype still runs and the flow still gets
// stopped). The FINAL test hard-asserts every datatype passed — an honest
// overall FAIL without sacrificing evidence collection. All evidence lands in
// e2e/artifacts/datatypes/ (screenshots + results.json + samples + defects).
import { test, expect, type BrowserContext, type Page } from "playwright/test";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";

const FRONTEND = "http://localhost:3001";
const BACKEND = "http://localhost:8010";
const ART = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "artifacts", "datatypes");
const CRON = "*/3 * * * *";

// Port of backend services/adapter/naming.tokenize (topic derivation).
const tokenize = (s: string) =>
  s.trim().toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
const rx = (s: string) => new RegExp(s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

test.describe.configure({ mode: "serial" });

let context: BrowserContext;
let page: Page;
const consoleErrors: string[] = [];

// ------------------------------------------------------------ result ledger
interface PhaseResult { ok: boolean; error?: string; note?: string; at: string }
interface DtResult {
  datatype: "json" | "csv" | "xml";
  flowName: string;
  flowId: string;
  serviceName: string;
  baseUrl: string;
  pathValue: string;
  topic: string;
  recordPath: string;
  probe?: unknown;
  phases: Record<string, PhaseResult>;
  aborted?: boolean;
  deployed?: boolean;
  started?: boolean;
  firstBatchCount?: number;
  sample?: unknown;
  sampleFields?: string[];
  dedup?: Record<string, unknown>;
  defect?: unknown;
  screenshots: string[];
}
const RESULTS: Record<string, DtResult> = {};
function saveResults() {
  fs.writeFileSync(path.join(ART, "results.json"), JSON.stringify(RESULTS, null, 2), "utf-8");
}

async function shot(res: DtResult | null, name: string) {
  await page.screenshot({ path: path.join(ART, `${name}.png`), fullPage: true });
  if (res) res.screenshots.push(`${name}.png`);
}

async function runPhase(res: DtResult, phase: string, fn: () => Promise<void>): Promise<boolean> {
  if (res.aborted) {
    res.phases[phase] = { ok: false, error: "skipped — an earlier phase failed", at: new Date().toISOString() };
    saveResults();
    return false;
  }
  try {
    await fn();
    res.phases[phase] = { ok: true, at: new Date().toISOString() };
    saveResults();
    return true;
  } catch (e) {
    res.phases[phase] = { ok: false, error: String(e).slice(0, 800), at: new Date().toISOString() };
    res.aborted = true;
    await page
      .screenshot({ path: path.join(ART, `FAIL-${res.datatype}-${phase}.png`), fullPage: true })
      .then(() => res.screenshots.push(`FAIL-${res.datatype}-${phase}.png`))
      .catch(() => undefined);
    saveResults();
    return false;
  }
}

// --------------------------------------------------------------- api helpers
async function backendReady(): Promise<void> {
  const deadline = Date.now() + 180_000;
  while (Date.now() < deadline) {
    try {
      const r = await fetch(`${BACKEND}/api`);
      if (r.ok) return;
    } catch { /* not up yet */ }
    await sleep(3_000);
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
  for (let i = 2; ; i++) if (!names.has(`${base} ${i}`)) return `${base} ${i}`;
}

interface TopicMessage { offset: number; ts: string; key: string | null; value: string | null; bytes: number }
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

/** Poll until the topic holds a STABLE non-zero message count (3 consecutive
 * identical reads, 10s apart) — guards against sampling mid-publish, which
 * would understate the pre-dedup baseline. */
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
        if (stable >= 2) return msgs; // 3 identical reads total
      } else {
        lastCount = msgs.length;
        stable = 0;
      }
    }
    await sleep(10_000);
  }
  return msgs;
}

// ---------------------------------------------------------------- ui helpers
/** A labelled input in the shadcn service form (Label is not htmlFor-linked). */
function fieldInput(scope: ReturnType<Page["locator"]>, label: string) {
  return scope.locator(`div:has(> label:text-is("${label}")) input`).first();
}

/** Create the HTTP service through the UI unless an active same-named service
 * already exists (reruns reuse it — nothing is ever retired/deleted). */
async function ensureHttpService(name: string, baseUrl: string, shotName: string, res: DtResult) {
  const services = (await (await fetch(`${BACKEND}/api/v2/services/`)).json()) as {
    id: string; name?: string; retired?: boolean;
  }[];
  const existing = services.find((s) => s.name === name && !s.retired);
  if (existing) {
    res.phases[`service_note`] = { ok: true, note: `service "${name}" already existed — reused (no-delete rule)`, at: new Date().toISOString() };
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
  const activeCard = page.locator("div.bg-card").filter({ hasText: name }).filter({ hasNotText: "Retired" }).first();
  await expect(activeCard).toBeVisible();
  await shot(res, shotName);
}

/** New Flow -> name -> Create & open builder. Returns the flow id. The flow is
 * staged CLIENT-side until first save — no page reloads before saving. */
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

/** Place http·read root, bind service, set path (+format/recordPath), assert
 * split ON, then set the cron via Flow settings. */
async function configureHttpReadRoot(opts: {
  serviceName: string;
  pathValue: string;
  format?: "csv" | "xml";
  recordPath?: string;
}) {
  await page.getByRole("button", { name: "Place the root" }).click();
  await page.getByRole("menuitem", { name: /http · read/ }).click();

  await expect(page.getByText("Existing service")).toBeVisible();
  await page.getByRole("combobox").filter({ hasText: "Select a service" }).click();
  await page.getByRole("option", { name: rx(opts.serviceName) }).click();
  await expect(page.getByText(/Base URL —/)).toBeVisible();

  await page.getByPlaceholder("/users").fill(opts.pathValue);

  if (opts.format) {
    // Response parsing row: the format Select shows "JSON" until changed.
    await page.getByRole("combobox").filter({ hasText: "JSON" }).first().click();
    await page.getByRole("option", { name: opts.format.toUpperCase(), exact: true }).click();
    if (opts.format === "csv") {
      // Must be asserted HERE — opening Flow settings below replaces the
      // block form, hiding this hint (first-run lesson).
      await expect(page.getByText("CSV encoding is fixed UTF-8 — and the form says so.")).toBeVisible();
    }
  }
  if (opts.recordPath !== undefined && opts.recordPath !== "") {
    await page.getByPlaceholder("$.resources[*] (record path)").fill(opts.recordPath);
  }
  // recordPath deliberately left untouched otherwise: the key stays ABSENT in
  // the block config, so the compiler applies its own "$" default (SplitJson
  // over the ConvertRecord output array). Typing would store "" instead.

  const splitSwitch = page.locator('label:has-text("split into records")').getByRole("switch");
  await expect(splitSwitch).toHaveAttribute("aria-checked", "true"); // default ON

  // Cron via Flow settings -> Custom.
  await page.getByRole("button", { name: /Flow settings/ }).click();
  await page.getByRole("combobox").first().click();
  await page.getByRole("option", { name: /Custom/ }).click();
  await page.getByPlaceholder("*/15 * * * *").fill(CRON);
  await expect(page.getByText("5 fields required")).toHaveCount(0);
  await expect(page.getByText(/^Next:/)).toBeVisible();
}

/** Select the http read node again (Flow settings replaced the block form). */
async function selectHttpReadNode() {
  await page.locator(".react-flow__node", { hasText: "New http read" }).first().click();
}

/** THE NEW DEDUPLICATION PANEL (Generic transformations -> Enable
 * deduplication -> identity fields). Screenshotted as user-requested
 * evidence. */
async function enableDedup(identityField: string, res: DtResult, shotName: string) {
  await selectHttpReadNode();
  await page.locator("#block-section-transforms button").first().click();
  await expect(page.getByRole("button", { name: /Enable deduplication/ })).toBeVisible();
  await page.getByRole("button", { name: /Enable deduplication/ }).click();
  await page.getByPlaceholder("identity fields (comma-separated)").fill(identityField);
  // Panel contract text proves the real DedupPanel rendered, configured state.
  await expect(page.getByText(/Platform metadata \(ingest_id, ingest_ts, op\) is always excluded/)).toBeVisible();
  await expect(page.getByText(/at least one identity field/i)).toHaveCount(0);
  await shot(res, shotName);
}

async function addKafkaWriteChild(entity: string, expectedTopic: string, res: DtResult, shotName: string) {
  await selectHttpReadNode();
  await page.locator('button[title^="Add a block after"]').click();
  await page.getByRole("menuitem", { name: /kafka · write/ }).click();
  // Adding a block does not always move selection — click the new kafka node
  // so its form opens. "Entity & derived names" is FORCED OPEN for a write
  // block with no entity (disabled trigger — never click it).
  await page.locator(".react-flow__node").filter({ hasText: "kafka · write" }).first().click();
  await page.getByPlaceholder("asset · incident · order…").fill(entity);
  await expect(page.getByText(expectedTopic).first()).toBeVisible();
  await shot(res, shotName);
}

async function saveFlow() {
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await expect(page.getByText("Draft saved").first()).toBeVisible();
  await expect(page.getByText(/Deploy unavailable/)).toHaveCount(0);
}

async function deployFlow(res: DtResult, shotName: string) {
  await page.getByRole("button", { name: "Deploy", exact: true }).click();
  const dlg = page.getByRole("dialog").filter({ hasText: "Deploy preflight" });
  await expect(dlg).toBeVisible();
  await expect(dlg.locator("li").first()).toBeVisible({ timeout: 30_000 });
  expect(await dlg.locator("li").count()).toBeGreaterThanOrEqual(4);
  await expect(dlg.locator("svg.text-destructive")).toHaveCount(0); // every row ok
  await shot(res, `${shotName}-preflight`);
  const deployBtn = dlg.getByRole("button", { name: "Deploy" });
  await expect(deployBtn).toBeEnabled();
  await deployBtn.click();
  await expect(page.getByText("Deployed — the flow is built stopped").first()).toBeVisible({ timeout: 240_000 });
  await expect(page.locator('span[aria-label="Stopped"]').first()).toBeVisible({ timeout: 30_000 });
  res.deployed = true;
  await shot(res, `${shotName}-deployed`);
}

async function enableAndStart(res: DtResult, shotName: string) {
  await page.getByRole("button", { name: "More" }).click();
  await page.getByRole("menuitem", { name: "Enable" }).click();
  await page.keyboard.press("Escape");
  const startBtn = page.getByRole("button", { name: "Start", exact: true });
  await expect(startBtn).toBeEnabled({ timeout: 20_000 });
  await startBtn.click();
  await expect(page.getByText("Flow started").first()).toBeVisible({ timeout: 120_000 });
  await expect(page.locator('span[aria-label="Running"]').first()).toBeVisible({ timeout: 30_000 });
  res.started = true;
  await shot(res, `${shotName}-started`);
}

/** Stop through the UI (evidence screenshot); API-verb fallback so the cron
 * NEVER keeps firing even if the UI path hiccups. Flows stay DEPLOYED. */
async function stopFlowAlways(res: DtResult, shotName: string) {
  if (!res.started) {
    res.phases["stop"] = { ok: true, note: "flow was never started — nothing to stop", at: new Date().toISOString() };
    saveResults();
    return;
  }
  try {
    await page.goto(`/flow-builder/${res.flowId}`);
    await page.getByRole("button", { name: "Stop", exact: true }).click();
    await expect(page.getByText("Stopped — queues retained").first()).toBeVisible({ timeout: 120_000 });
    await shot(res, shotName);
    res.phases["stop"] = { ok: true, at: new Date().toISOString() };
  } catch (e) {
    try {
      const r = await fetch(`${BACKEND}/api/v2/flows/${res.flowId}/verbs/stop`, { method: "POST" });
      res.phases["stop"] = {
        ok: r.ok,
        note: `UI stop failed (${String(e).slice(0, 200)}) — API fallback -> ${r.status}`,
        at: new Date().toISOString(),
      };
    } catch (e2) {
      res.phases["stop"] = { ok: false, error: `UI and API stop both failed: ${String(e2).slice(0, 200)}`, at: new Date().toISOString() };
    }
  }
  saveResults();
}

/** Open a flow's details sheet from /flows — the sheet opens via the row's
 * "Overview" (eye) icon button (`GuardedIconButton` aria-label), NOT by
 * clicking the flow name (a plain, non-clickable span — first-run lesson). */
async function openFlowSheet(flowName: string) {
  await page.goto("/flows");
  const row = page.getByRole("row").filter({ hasText: flowName }).first();
  await expect(row).toBeVisible();
  await row.getByRole("button", { name: "Overview" }).click();
}

/** Prove the records through the UI: Flows -> flow -> Messages tab -> topic. */
async function verifyMessagesTabUI(res: DtResult, shotName: string) {
  await openFlowSheet(res.flowName);
  await expect(page.getByRole("tab", { name: "Messages" })).toBeVisible();
  await page.getByRole("tab", { name: "Messages" }).click();
  const topicSelect = page.getByRole("combobox").filter({ hasText: /raw\.|Pick a topic/ }).first();
  await topicSelect.click();
  await page.getByRole("option", { name: res.topic }).click();
  await expect(page.getByText(/offset \d+/).first()).toBeVisible({ timeout: 30_000 });
  await shot(res, shotName);
  await page.keyboard.press("Escape"); // close the sheet
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

/** Defect evidence when records never land: DLQ (parse failures are routed to
 * the DLQ by the compiler), live metrics, and the flow runtime (per-processor
 * state + invalidReasons read from NiFi via the backend). */
async function captureDefectEvidence(res: DtResult) {
  const out: Record<string, unknown> = {};
  for (const ep of ["dlq", "metrics", "runtime"]) {
    try {
      const r = await fetch(`${BACKEND}/api/v2/flows/${res.flowId}/${ep}`);
      out[ep] = r.ok ? await r.json() : { status: r.status, body: (await r.text()).slice(0, 2000) };
    } catch (e) {
      out[ep] = { error: String(e) };
    }
  }
  fs.writeFileSync(path.join(ART, `defect-${res.datatype}.json`), JSON.stringify(out, null, 2), "utf-8");
  // Condensed pointers for the report.
  const runtime = out["runtime"] as { components?: { name?: string; state?: string; invalidReasons?: string[] | null }[] } | undefined;
  res.defect = {
    dlqSummary: (() => {
      const dlq = out["dlq"] as { messages?: unknown[] } | undefined;
      return Array.isArray(dlq?.messages) ? `${dlq.messages.length} DLQ message(s) — see defect-${res.datatype}.json` : out["dlq"];
    })(),
    invalidComponents: runtime?.components
      ?.filter((c) => (c.invalidReasons ?? []).length > 0 || c.state === "INVALID")
      .map((c) => ({ name: c.name, state: c.state, invalidReasons: c.invalidReasons })),
    evidenceFile: `defect-${res.datatype}.json`,
  };
  saveResults();
}

// ------------------------------------------------------------------ fixtures
test.beforeAll(async ({ browser }) => {
  fs.mkdirSync(ART, { recursive: true });
  await backendReady();
  // NO pre-clean of any kind — the user's hard rule: nothing gets deleted.
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
  await context?.close();
});

// =====================================================================
// 1. JSON — dt json products (dummyjson /products + NEW dedup panel)
// =====================================================================
test("json datatype — dt json products with dedup panel, deploy, records, dedup suppression, stop", async () => {
  test.setTimeout(1_500_000); // build + deploy + two cron windows + metrics UI

  const res: DtResult = {
    datatype: "json",
    flowName: await uniqueFlowName("dt json products"),
    flowId: "",
    serviceName: "dt dummyjson",
    baseUrl: "https://dummyjson.com",
    pathValue: "/products",
    recordPath: "$.products[*]",
    topic: "",
    phases: {},
    screenshots: [],
  };
  res.topic = `raw.${tokenize(res.flowName)}.${tokenize("dt_product")}`;
  RESULTS["json"] = res;
  saveResults();

  await runPhase(res, "service", async () => {
    await ensureHttpService(res.serviceName, res.baseUrl, "01-json-service", res);
  });

  await runPhase(res, "create_flow", async () => {
    res.flowId = await createFlow(res.flowName);
    saveResults();
    await shot(res, "02-json-flow-created");
  });

  await runPhase(res, "configure_root", async () => {
    await configureHttpReadRoot({ serviceName: res.serviceName, pathValue: res.pathValue, recordPath: res.recordPath });
    await shot(res, "03-json-root-configured");
  });

  await runPhase(res, "dedup_panel", async () => {
    await enableDedup("id", res, "04-json-dedup-panel"); // USER-REQUESTED EVIDENCE
  });

  await runPhase(res, "kafka_child", async () => {
    await addKafkaWriteChild("dt_product", res.topic, res, "05-json-kafka-child");
  });

  await runPhase(res, "block_test", async () => {
    // Live block Test (json-only harness) proves the recordPath before deploy.
    await selectHttpReadNode();
    await page.locator("#block-section-test button").first().click();
    await page.getByRole("button", { name: "Test block" }).click();
    await expect(page.getByText(/Test succeeded — \d+ sample record\(s\), nothing committed/)).toBeVisible({ timeout: 90_000 });
    await shot(res, "06-json-block-test");
  });

  await runPhase(res, "save", async () => {
    await saveFlow();
  });

  await runPhase(res, "deploy", async () => {
    await deployFlow(res, "07-json");
  });

  await runPhase(res, "enable_start", async () => {
    await enableAndStart(res, "08-json");
  });

  await runPhase(res, "records", async () => {
    const msgs = await pollStableMessages(res.flowId, res.topic, 300_000);
    if (msgs.length === 0) {
      await captureDefectEvidence(res);
      throw new Error(`no messages on ${res.topic} within 300s of Start (cron ${CRON})`);
    }
    res.firstBatchCount = msgs.length;
    const sample = parseNewestMessage(msgs);
    res.sample = sample;
    res.sampleFields = Object.keys(sample);
    fs.writeFileSync(path.join(ART, "json-sample-messages.json"), JSON.stringify(msgs.slice(0, 5), null, 2), "utf-8");
    saveResults();
    // Product fields prove PARSED json records, not raw payload blobs.
    if (!("title" in sample) || !("price" in sample) || !("id" in sample)) {
      throw new Error(`parsed message lacks product fields (title/price/id): ${JSON.stringify(sample).slice(0, 300)}`);
    }
    if (msgs.length < 20 || msgs.length > 40) {
      throw new Error(`expected ~30 product records, got ${msgs.length}`);
    }
  });

  await runPhase(res, "messages_ui", async () => {
    await verifyMessagesTabUI(res, "09-json-messages-ui");
  });

  await runPhase(res, "dedup_suppression", async () => {
    // Baseline after the FIRST firing (stable count), then wait long enough to
    // guarantee a SECOND cron firing (*/3 fires every 180s; +90s processing
    // margin), then assert the topic did NOT grow.
    const c1 = (await apiMessages(res.flowId, res.topic)).length;
    const m1 = await apiMetrics(res.flowId);
    const mc1 = metricTopicCount(m1, res.topic);
    await sleep(270_000);
    const c2 = (await apiMessages(res.flowId, res.topic)).length;
    const m2 = await apiMetrics(res.flowId);
    const mc2 = metricTopicCount(m2, res.topic);
    res.dedup = {
      messagesApiCountAfterFirstFiring: c1,
      messagesApiCountAfterSecondFiring: c2,
      metricsTopicCountAfterFirstFiring: mc1,
      metricsTopicCountAfterSecondFiring: mc2,
      waitedSecondsBetweenReads: 270,
    };
    fs.writeFileSync(
      path.join(ART, "json-dedup-evidence.json"),
      JSON.stringify({ before: { messages: c1, metricsTopicCount: mc1, metricsRaw: m1 }, after: { messages: c2, metricsTopicCount: mc2, metricsRaw: m2 } }, null, 2),
      "utf-8",
    );
    saveResults();
    if (c2 !== c1) throw new Error(`dedup FAILED to suppress: messages count grew ${c1} -> ${c2} after the second cron firing`);
    if (mc1 !== null && mc2 !== null && mc2 !== mc1) {
      throw new Error(`dedup FAILED to suppress: live metrics topic count grew ${mc1} -> ${mc2}`);
    }
    // Metrics tab screenshot — the user-requested dedup evidence in the UI.
    await openFlowSheet(res.flowName);
    await page.getByRole("tab", { name: "Metrics" }).click();
    await expect(page.getByText(res.topic).first()).toBeVisible({ timeout: 30_000 });
    await shot(res, "10-json-metrics-after-dedup");
    await page.keyboard.press("Escape");
  });

  await stopFlowAlways(res, "11-json-stopped");
});

// =====================================================================
// 2. CSV — dt csv addresses (probed url, responseFormat csv)
// =====================================================================
test("csv datatype — probe url, build dt csv addresses, deploy, csv->json records, stop", async () => {
  test.setTimeout(1_200_000);

  const res: DtResult = {
    datatype: "csv",
    flowName: await uniqueFlowName("dt csv addresses"),
    flowId: "",
    serviceName: "dt csvhost",
    baseUrl: "",
    pathValue: "",
    recordPath: "", // left blank — compiler default "$" splits the converted JSON array
    topic: "",
    phases: {},
    screenshots: [],
  };
  res.topic = `raw.${tokenize(res.flowName)}.${tokenize("dt_row")}`;
  RESULTS["csv"] = res;
  saveResults();

  let headerCells: string[] = [];

  await runPhase(res, "probe", async () => {
    // The compiler pins CSVReader "Treat First Line as Header: true", so a
    // candidate must have a real header row for "JSON objects with the CSV's
    // column fields" to be provable — a headerless file (addresses.csv is one:
    // its first line is data, "John,Doe,120 jefferson st.,…") would turn row 1
    // values into field names. The probe therefore requires 200 + csv-ish
    // content + a header-looking first line.
    const candidates = [
      "https://people.sc.fsu.edu/~jburkardt/data/csv/addresses.csv",
      "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv",
    ];
    const probeLog: Record<string, unknown>[] = [];
    const looksLikeHeader = (line: string) => {
      const cells = line.split(",").map((c) => c.trim().replace(/^"|"$/g, ""));
      return cells.length >= 2 && cells.every((c) => /^[A-Za-z_][A-Za-z0-9_ .\-/]*$/.test(c) && !/^\d+$/.test(c));
    };
    for (const url of candidates) {
      try {
        const r = await fetch(url, { headers: { "User-Agent": "Mozilla/5.0" }, signal: AbortSignal.timeout(20_000) });
        const ct = r.headers.get("content-type") ?? "";
        const body = r.ok ? await r.text() : "";
        const firstLine = body.split(/\r?\n/, 1)[0] ?? "";
        const csvish = r.ok && (ct.includes("csv") || ct.includes("text/plain")) && firstLine.includes(",");
        const headerOk = csvish && looksLikeHeader(firstLine);
        probeLog.push({ url, status: r.status, contentType: ct, firstLine: firstLine.slice(0, 160), csvish, headerOk });
        if (csvish && headerOk) {
          const u = new URL(url);
          res.baseUrl = `${u.protocol}//${u.host}`;
          res.pathValue = u.pathname;
          headerCells = firstLine.split(",").map((c) => c.trim().replace(/^"|"$/g, ""));
          break;
        }
      } catch (e) {
        probeLog.push({ url, error: String(e).slice(0, 200) });
      }
    }
    res.probe = { candidates: probeLog, chosenBaseUrl: res.baseUrl, chosenPath: res.pathValue, headerCells };
    fs.writeFileSync(path.join(ART, "csv-probe.json"), JSON.stringify(res.probe, null, 2), "utf-8");
    saveResults();
    if (!res.baseUrl) throw new Error(`no CSV candidate passed the probe: ${JSON.stringify(probeLog)}`);
  });

  await runPhase(res, "service", async () => {
    await ensureHttpService(res.serviceName, res.baseUrl, "20-csv-service", res);
  });

  await runPhase(res, "create_flow", async () => {
    res.flowId = await createFlow(res.flowName);
    saveResults();
  });

  await runPhase(res, "configure_root", async () => {
    await configureHttpReadRoot({ serviceName: res.serviceName, pathValue: res.pathValue, format: "csv" });
    await expect(page.getByText("CSV encoding is fixed UTF-8 — and the form says so.")).toBeVisible();
    await shot(res, "21-csv-root-configured");
  });

  await runPhase(res, "kafka_child", async () => {
    await addKafkaWriteChild("dt_row", res.topic, res, "22-csv-kafka-child");
  });

  // No block Test here: the Test harness is JSON-only by design
  // (backend/services/adapter/runtime.py `_run_http_read_probe` rejects
  // non-JSON bodies) — deploy + live records are the CSV verification.

  await runPhase(res, "save", async () => {
    await saveFlow();
  });

  await runPhase(res, "deploy", async () => {
    await deployFlow(res, "23-csv");
  });

  await runPhase(res, "enable_start", async () => {
    await enableAndStart(res, "24-csv");
  });

  await runPhase(res, "records", async () => {
    const msgs = await pollStableMessages(res.flowId, res.topic, 300_000);
    if (msgs.length === 0) {
      await captureDefectEvidence(res);
      throw new Error(`no messages on ${res.topic} within 300s of Start (cron ${CRON})`);
    }
    res.firstBatchCount = msgs.length;
    const sample = parseNewestMessage(msgs);
    res.sample = sample;
    res.sampleFields = Object.keys(sample);
    fs.writeFileSync(path.join(ART, "csv-sample-messages.json"), JSON.stringify(msgs.slice(0, 5), null, 2), "utf-8");
    saveResults();
    // csv -> record conversion proof: the JSON object carries the CSV's own
    // header column names as fields.
    const present = headerCells.filter((h) => h in sample);
    if (present.length < Math.min(3, headerCells.length)) {
      throw new Error(
        `message is not a JSON object keyed by the CSV header columns. header=${JSON.stringify(headerCells)} sampleKeys=${JSON.stringify(Object.keys(sample))}`,
      );
    }
  });

  await runPhase(res, "messages_ui", async () => {
    await verifyMessagesTabUI(res, "25-csv-messages-ui");
  });

  await stopFlowAlways(res, "26-csv-stopped");
});

// =====================================================================
// 3. XML — dt xml feed (probed url, responseFormat xml)
// =====================================================================
test("xml datatype — probe url, build dt xml feed, deploy, xml->json records (or precise defect), stop", async () => {
  test.setTimeout(1_200_000);

  const res: DtResult = {
    datatype: "xml",
    flowName: await uniqueFlowName("dt xml feed"),
    flowId: "",
    serviceName: "dt xmlhost",
    baseUrl: "",
    pathValue: "",
    recordPath: "",
    topic: "",
    phases: {},
    screenshots: [],
  };
  res.topic = `raw.${tokenize(res.flowName)}.${tokenize("dt_item")}`;
  RESULTS["xml"] = res;
  saveResults();

  let expectFields: string[] = [];

  await runPhase(res, "probe", async () => {
    // recordPath semantics for xml (backend compiler blocks_http.py):
    // XMLReader (infer-schema, Expect Records as Array=false) reads the WHOLE
    // document as ONE record; JsonRecordSetWriter (Output Grouping:
    // output-array) wraps it -> `[{...doc...}]`; SplitJson then applies
    // recordPath over THAT JSON. So the path starts `$[0].` and walks the
    // element tree to the repeating element.
    const candidates: { url: string; recordPath: string; expectFields: string[]; shape: string }[] = [
      {
        url: "https://feeds.bbci.co.uk/news/rss.xml",
        recordPath: "$[0].channel.item[*]",
        expectFields: ["title", "pubDate"],
        shape: "rss -> channel -> item[]",
      },
      {
        url: "https://www.w3schools.com/xml/cd_catalog.xml",
        recordPath: "$[0].CD[*]",
        expectFields: ["TITLE", "ARTIST"],
        shape: "CATALOG -> CD[]",
      },
    ];
    const probeLog: Record<string, unknown>[] = [];
    for (const cand of candidates) {
      try {
        const r = await fetch(cand.url, { headers: { "User-Agent": "Mozilla/5.0" }, signal: AbortSignal.timeout(20_000) });
        const ct = r.headers.get("content-type") ?? "";
        const body = r.ok ? await r.text() : "";
        const xmlish = r.ok && /^\s*<\?xml|<rss[\s>]|<CATALOG[\s>]/i.test(body.slice(0, 300));
        probeLog.push({ url: cand.url, status: r.status, contentType: ct, xmlish, head: body.slice(0, 120) });
        if (xmlish) {
          const u = new URL(cand.url);
          res.baseUrl = `${u.protocol}//${u.host}`;
          res.pathValue = u.pathname;
          res.recordPath = cand.recordPath;
          expectFields = cand.expectFields;
          break;
        }
      } catch (e) {
        probeLog.push({ url: cand.url, error: String(e).slice(0, 200) });
      }
    }
    res.probe = { candidates: probeLog, chosenBaseUrl: res.baseUrl, chosenPath: res.pathValue, recordPath: res.recordPath };
    fs.writeFileSync(path.join(ART, "xml-probe.json"), JSON.stringify(res.probe, null, 2), "utf-8");
    saveResults();
    if (!res.baseUrl) throw new Error(`no XML candidate passed the probe: ${JSON.stringify(probeLog)}`);
  });

  await runPhase(res, "service", async () => {
    await ensureHttpService(res.serviceName, res.baseUrl, "30-xml-service", res);
  });

  await runPhase(res, "create_flow", async () => {
    res.flowId = await createFlow(res.flowName);
    saveResults();
  });

  await runPhase(res, "configure_root", async () => {
    await configureHttpReadRoot({
      serviceName: res.serviceName,
      pathValue: res.pathValue,
      format: "xml",
      recordPath: res.recordPath,
    });
    await shot(res, "31-xml-root-configured");
  });

  await runPhase(res, "kafka_child", async () => {
    await addKafkaWriteChild("dt_item", res.topic, res, "32-xml-kafka-child");
  });

  await runPhase(res, "save", async () => {
    await saveFlow();
  });

  await runPhase(res, "deploy", async () => {
    await deployFlow(res, "33-xml");
  });

  await runPhase(res, "enable_start", async () => {
    await enableAndStart(res, "34-xml");
  });

  await runPhase(res, "records", async () => {
    const msgs = await pollStableMessages(res.flowId, res.topic, 300_000);
    if (msgs.length === 0) {
      // Defect path — precise engine evidence, never a silent pass.
      await captureDefectEvidence(res);
      throw new Error(
        `XML DEFECT: no messages on ${res.topic} within 300s of Start — DLQ/runtime/metrics captured in defect-xml.json`,
      );
    }
    res.firstBatchCount = msgs.length;
    const sample = parseNewestMessage(msgs);
    res.sample = sample;
    res.sampleFields = Object.keys(sample);
    fs.writeFileSync(path.join(ART, "xml-sample-messages.json"), JSON.stringify(msgs.slice(0, 5), null, 2), "utf-8");
    saveResults();
    const present = expectFields.filter((f) => f in sample);
    if (present.length === 0) {
      throw new Error(
        `xml-derived message lacks the expected element fields ${JSON.stringify(expectFields)}: keys=${JSON.stringify(Object.keys(sample))}`,
      );
    }
  });

  await runPhase(res, "messages_ui", async () => {
    await verifyMessagesTabUI(res, "35-xml-messages-ui");
  });

  await stopFlowAlways(res, "36-xml-stopped");
});

// =====================================================================
// 4. Final evidence + the honest overall verdict
// =====================================================================
test("final — all three dt flows present (API + Flows page), per-datatype verdict", async () => {
  test.setTimeout(300_000);

  // API evidence: GET /api/v2/flows/ shows the three dt flows, deployed.
  const flows = await apiFlows();
  fs.writeFileSync(path.join(ART, "flows-final.json"), JSON.stringify(flows, null, 2), "utf-8");
  for (const key of ["json", "csv", "xml"] as const) {
    const res = RESULTS[key];
    expect.soft(res, `${key} journey never ran`).toBeTruthy();
    if (!res?.flowId) continue;
    const f = flows.find((x) => x.id === res.flowId);
    expect.soft(f, `${key} flow ${res.flowName} missing from GET /api/v2/flows/`).toBeTruthy();
    if (res.deployed) {
      expect.soft(f?.nifiProcessGroupId, `${key} flow should still be DEPLOYED (left in place)`).toBeTruthy();
      expect.soft(f?.state, `${key} flow should be left Stopped`).toBe("Stopped");
    }
  }

  // UI evidence: Flows page filtered to the dt flows.
  await page.goto("/flows");
  await page.getByPlaceholder("Search flows, entities, topics…").fill("dt");
  for (const key of ["json", "csv", "xml"] as const) {
    const res = RESULTS[key];
    if (res?.flowName) await expect(page.getByText(res.flowName, { exact: true }).first()).toBeVisible();
  }
  await page.screenshot({ path: path.join(ART, "40-final-flows-page.png"), fullPage: true });
  saveResults();

  // The honest verdict: every phase of every datatype must have passed.
  const failures: string[] = [];
  for (const key of ["json", "csv", "xml"] as const) {
    const res = RESULTS[key];
    if (!res) {
      failures.push(`${key}: journey never ran`);
      continue;
    }
    for (const [phase, p] of Object.entries(res.phases)) {
      if (!p.ok) failures.push(`${key}/${phase}: ${p.error ?? "failed"}`);
    }
  }
  expect(failures, `datatype journey failures:\n${failures.join("\n")}`).toEqual([]);
});
