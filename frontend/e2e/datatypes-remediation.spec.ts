// Remediation pass for datatypes.spec.ts run #1 (2026-08-13). That run proved
// the JSON and XML journeys end-to-end through records-on-topic, but two SPEC
// bugs (not app bugs) left gaps, both fixed in datatypes.spec.ts afterwards:
//   1. the flow details sheet opens via the row's "Overview" (eye) button —
//      clicking the flow-name span does nothing -> messages/metrics UI
//      evidence was missed and the JSON dedup-suppression phase was skipped;
//   2. the CSV "fixed UTF-8" hint was asserted AFTER opening Flow settings
//      (which replaces the block form) -> the CSV build aborted before saving
//      (so no backend flow exists for csv — full journey redone here).
// This spec completes ONLY the missing evidence against the LIVE state left
// in place (nothing was deleted — the user's hard rule):
//   R1 json  — restart "dt json products" (deployed+stopped, dedup cache
//              warm), prove a second firing publishes NOTHING new (dedup
//              suppression), Messages tab UI, Metrics tab UI, stop again.
//   R2 csv   — the full "dt csv addresses" journey with the fixed helpers.
//   R3 xml   — Messages tab UI evidence for the stopped "dt xml feed".
//   R4 final — flows present via API + Flows page, honest overall verdict.
// Artifacts land in the SAME e2e/artifacts/datatypes/ set (canonical shot
// names), ledger: results-remediation.json.
import { test, expect, type BrowserContext, type Page } from "playwright/test";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";

const FRONTEND = "http://localhost:3001";
const BACKEND = "http://localhost:8010";
const ART = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "artifacts", "datatypes");
const CRON = "*/3 * * * *";

const JSON_FLOW = "dt json products";
const XML_FLOW = "dt xml feed";
const CSV_FLOW = "dt csv addresses";

const tokenize = (s: string) =>
  s.trim().toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
const rx = (s: string) => new RegExp(s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

test.describe.configure({ mode: "serial" });

let context: BrowserContext;
let page: Page;
const consoleErrors: string[] = [];

interface PhaseResult { ok: boolean; error?: string; note?: string; at: string }
interface Ledger {
  [k: string]: {
    phases: Record<string, PhaseResult>;
    aborted?: boolean;
    flowId?: string;
    flowName?: string;
    topic?: string;
    data?: Record<string, unknown>;
    screenshots: string[];
    started?: boolean;
    deployed?: boolean;
  };
}
const LEDGER: Ledger = {};
function saveLedger() {
  fs.writeFileSync(path.join(ART, "results-remediation.json"), JSON.stringify(LEDGER, null, 2), "utf-8");
}
function entry(k: string) {
  if (!LEDGER[k]) LEDGER[k] = { phases: {}, screenshots: [], data: {} };
  return LEDGER[k];
}
async function shot(k: string, name: string) {
  await page.screenshot({ path: path.join(ART, `${name}.png`), fullPage: true });
  entry(k).screenshots.push(`${name}.png`);
}
async function runPhase(k: string, phase: string, fn: () => Promise<void>): Promise<boolean> {
  const e = entry(k);
  if (e.aborted) {
    e.phases[phase] = { ok: false, error: "skipped — an earlier phase failed", at: new Date().toISOString() };
    saveLedger();
    return false;
  }
  try {
    await fn();
    e.phases[phase] = { ok: true, at: new Date().toISOString() };
    saveLedger();
    return true;
  } catch (err) {
    e.phases[phase] = { ok: false, error: String(err).slice(0, 800), at: new Date().toISOString() };
    e.aborted = true;
    await page
      .screenshot({ path: path.join(ART, `FAIL-rem-${k}-${phase}.png`), fullPage: true })
      .then(() => e.screenshots.push(`FAIL-rem-${k}-${phase}.png`))
      .catch(() => undefined);
    saveLedger();
    return false;
  }
}

// --------------------------------------------------------------- api helpers
async function apiFlows(): Promise<{ id: string; name?: string; state?: string; nifiProcessGroupId?: string | null }[]> {
  const r = await fetch(`${BACKEND}/api/v2/flows/`);
  if (!r.ok) throw new Error(`GET /api/v2/flows/ -> ${r.status}`);
  return (await r.json()) as never;
}
async function flowByName(name: string) {
  const f = (await apiFlows()).find((x) => x.name === name);
  if (!f) throw new Error(`flow "${name}" not found on the backend`);
  return f;
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
async function captureDefectEvidence(k: string, flowId: string) {
  const out: Record<string, unknown> = {};
  for (const ep of ["dlq", "metrics", "runtime"]) {
    try {
      const r = await fetch(`${BACKEND}/api/v2/flows/${flowId}/${ep}`);
      out[ep] = r.ok ? await r.json() : { status: r.status, body: (await r.text()).slice(0, 2000) };
    } catch (e) {
      out[ep] = { error: String(e) };
    }
  }
  fs.writeFileSync(path.join(ART, `defect-${k}.json`), JSON.stringify(out, null, 2), "utf-8");
}

// ---------------------------------------------------------------- ui helpers
function fieldInput(scope: ReturnType<Page["locator"]>, label: string) {
  return scope.locator(`div:has(> label:text-is("${label}")) input`).first();
}
async function ensureHttpService(k: string, name: string, baseUrl: string, shotName: string) {
  const services = (await (await fetch(`${BACKEND}/api/v2/services/`)).json()) as {
    id: string; name?: string; retired?: boolean;
  }[];
  if (services.find((s) => s.name === name && !s.retired)) {
    entry(k).phases["service_note"] = { ok: true, note: `service "${name}" already existed — reused`, at: new Date().toISOString() };
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
  await shot(k, shotName);
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
    if (opts.format === "csv") {
      // Asserted BEFORE opening Flow settings (which replaces the block form).
      await expect(page.getByText("CSV encoding is fixed UTF-8 — and the form says so.")).toBeVisible();
    }
  }
  if (opts.recordPath) {
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
async function addKafkaWriteChild(k: string, entity: string, expectedTopic: string, shotName: string) {
  await selectHttpReadNode();
  await page.locator('button[title^="Add a block after"]').click();
  await page.getByRole("menuitem", { name: /kafka · write/ }).click();
  await page.locator(".react-flow__node").filter({ hasText: "kafka · write" }).first().click();
  await page.getByPlaceholder("asset · incident · order…").fill(entity);
  await expect(page.getByText(expectedTopic).first()).toBeVisible();
  await shot(k, shotName);
}
async function saveFlow() {
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await expect(page.getByText("Draft saved").first()).toBeVisible();
  await expect(page.getByText(/Deploy unavailable/)).toHaveCount(0);
}
async function deployFlow(k: string, shotName: string) {
  await page.getByRole("button", { name: "Deploy", exact: true }).click();
  const dlg = page.getByRole("dialog").filter({ hasText: "Deploy preflight" });
  await expect(dlg).toBeVisible();
  await expect(dlg.locator("li").first()).toBeVisible({ timeout: 30_000 });
  await expect(dlg.locator("svg.text-destructive")).toHaveCount(0);
  await shot(k, `${shotName}-preflight`);
  const deployBtn = dlg.getByRole("button", { name: "Deploy" });
  await expect(deployBtn).toBeEnabled();
  await deployBtn.click();
  await expect(page.getByText("Deployed — the flow is built stopped").first()).toBeVisible({ timeout: 240_000 });
  await expect(page.locator('span[aria-label="Stopped"]').first()).toBeVisible({ timeout: 30_000 });
  entry(k).deployed = true;
  await shot(k, `${shotName}-deployed`);
}
async function enableAndStart(k: string, shotName: string) {
  await page.getByRole("button", { name: "More" }).click();
  await page.getByRole("menuitem", { name: "Enable" }).click();
  await page.keyboard.press("Escape");
  const startBtn = page.getByRole("button", { name: "Start", exact: true });
  await expect(startBtn).toBeEnabled({ timeout: 20_000 });
  await startBtn.click();
  await expect(page.getByText("Flow started").first()).toBeVisible({ timeout: 120_000 });
  await expect(page.locator('span[aria-label="Running"]').first()).toBeVisible({ timeout: 30_000 });
  entry(k).started = true;
  await shot(k, `${shotName}-started`);
}
async function stopFlowUI(k: string, flowId: string, shotName: string) {
  try {
    await page.goto(`/flow-builder/${flowId}`);
    await page.getByRole("button", { name: "Stop", exact: true }).click();
    await expect(page.getByText("Stopped — queues retained").first()).toBeVisible({ timeout: 120_000 });
    await shot(k, shotName);
    entry(k).phases["stop"] = { ok: true, at: new Date().toISOString() };
  } catch (e) {
    try {
      const r = await fetch(`${BACKEND}/api/v2/flows/${flowId}/verbs/stop`, { method: "POST" });
      entry(k).phases["stop"] = { ok: r.ok, note: `UI stop failed — API fallback -> ${r.status}`, at: new Date().toISOString() };
    } catch (e2) {
      entry(k).phases["stop"] = { ok: false, error: `UI and API stop both failed: ${String(e2).slice(0, 200)}`, at: new Date().toISOString() };
    }
  }
  saveLedger();
}
/** The sheet opens via the row's "Overview" (eye) button — run-1 lesson. */
async function openFlowSheet(flowName: string) {
  await page.goto("/flows");
  const row = page.getByRole("row").filter({ hasText: flowName }).first();
  await expect(row).toBeVisible();
  await row.getByRole("button", { name: "Overview" }).click();
}
async function verifyMessagesTabUI(k: string, flowName: string, topic: string, shotName: string) {
  await openFlowSheet(flowName);
  await expect(page.getByRole("tab", { name: "Messages" })).toBeVisible();
  await page.getByRole("tab", { name: "Messages" }).click();
  const topicSelect = page.getByRole("combobox").filter({ hasText: /raw\.|Pick a topic/ }).first();
  await topicSelect.click();
  await page.getByRole("option", { name: topic }).click();
  await expect(page.getByText(/offset \d+/).first()).toBeVisible({ timeout: 30_000 });
  await shot(k, shotName);
  await page.keyboard.press("Escape");
}

// ------------------------------------------------------------------ fixtures
test.beforeAll(async ({ browser }) => {
  fs.mkdirSync(ART, { recursive: true });
  context = await browser.newContext({ baseURL: FRONTEND, viewport: { width: 1680, height: 1000 } });
  page = await context.newPage();
  page.on("console", (m) => {
    if (m.type() === "error") consoleErrors.push(m.text());
  });
  page.on("pageerror", (e) => consoleErrors.push(`pageerror: ${String(e)}`));
});

test.afterAll(async () => {
  if (consoleErrors.length > 0) {
    fs.writeFileSync(path.join(ART, "console-errors-remediation.txt"), consoleErrors.join("\n---\n"), "utf-8");
  }
  saveLedger();
  await context?.close();
});

// =====================================================================
// R1 — JSON dedup suppression + missing UI evidence on the existing flow
// =====================================================================
test("R1 json — restart, second firing suppressed by dedup, messages+metrics UI, stop", async () => {
  test.setTimeout(1_100_000);
  const k = "json";
  const e = entry(k);
  const topic = `raw.${tokenize(JSON_FLOW)}.dt_product`;
  e.flowName = JSON_FLOW;
  e.topic = topic;
  saveLedger();

  let flowId = "";
  await runPhase(k, "locate_flow", async () => {
    const f = await flowByName(JSON_FLOW);
    flowId = f.id;
    e.flowId = f.id;
    if (!f.nifiProcessGroupId) throw new Error(`${JSON_FLOW} is not deployed — cannot verify dedup`);
    saveLedger();
  });

  let c0 = 0;
  let m0: Record<string, unknown> | null = null;
  await runPhase(k, "baseline", async () => {
    c0 = (await apiMessages(flowId, topic)).length;
    m0 = await apiMetrics(flowId);
    e.data!["baselineMessages"] = c0;
    e.data!["baselineMetricsTopicCount"] = metricTopicCount(m0, topic);
    saveLedger();
    if (c0 === 0) throw new Error(`expected the run-1 records (~30) on ${topic}, found 0`);
  });

  await runPhase(k, "restart", async () => {
    // Start from the FLOWS PAGE row button. The flow-builder toolbar on a
    // fresh page load keeps Start disabled ("Runtime connections
    // unavailable") because api.ts's module-level connections cache fills
    // asynchronously and nothing re-renders the toolbar afterwards — the
    // Flows page recomputes the guard after its own queries land, so its row
    // Start button reflects the true (healthy) state. Debugged live with
    // e2e/debug-check-start.mjs; noted as a UI quirk in the journey doc.
    await page.goto("/flows");
    const row = page.getByRole("row").filter({ hasText: JSON_FLOW }).first();
    await expect(row).toBeVisible();
    const rowStart = row.getByRole("button", { name: "Start", exact: true });
    await expect(rowStart).toBeEnabled({ timeout: 30_000 });
    await rowStart.click();
    await expect(page.getByText(`Started — ${JSON_FLOW}`).first()).toBeVisible({ timeout: 120_000 });
    e.started = true;
    await shot(k, "08b-json-restarted-for-dedup");
  });

  await runPhase(k, "dedup_suppression", async () => {
    // Wait for PROOF the cron fired and processed records after restart:
    // records24h is the flow PG's flowFilesOut in NiFi's live status window —
    // ~0 right after restart (the flow sat stopped for >5 min), >0 once the
    // firing ran the fetch/split/dedup chain. Then allow publish time and
    // assert the topic did NOT grow (all 30 re-fetched products are dedup
    // cache hits from run 1 — the 24h Redis window is still warm).
    const deadline = Date.now() + 330_000;
    let fired = false;
    let records24h: unknown = null;
    while (Date.now() < deadline) {
      const m = await apiMetrics(flowId);
      if (m && m["available"] === true) {
        records24h = m["records24h"];
        if (typeof records24h === "number" && records24h > 0) {
          fired = true;
          break;
        }
      }
      await sleep(20_000);
    }
    e.data!["cronFiredEvidence"] = { fired, records24hInStatusWindow: records24h };
    await sleep(60_000); // publish margin after the observed activity
    const c1 = (await apiMessages(flowId, topic)).length;
    const m1 = await apiMetrics(flowId);
    e.data!["afterSecondFiring"] = {
      messages: c1,
      metricsTopicCount: metricTopicCount(m1, topic),
    };
    fs.writeFileSync(
      path.join(ART, "json-dedup-evidence.json"),
      JSON.stringify(
        {
          note:
            "run 1 published 30 products at ~11:39Z then the flow was stopped; this restart re-fetches the same 30 " +
            "products — every one a dedup cache hit (identity field: id, 24h window, Redis). Suppression is proven " +
            "by the topic count NOT growing across the restart firing.",
          baseline: { messages: c0, metrics: m0 },
          cronFiredEvidence: e.data!["cronFiredEvidence"],
          after: { messages: c1, metrics: m1 },
        },
        null,
        2,
      ),
      "utf-8",
    );
    saveLedger();
    if (!fired) throw new Error("no NiFi activity observed within 330s of restart — cannot attribute a suppressed firing");
    if (c1 !== c0) throw new Error(`dedup FAILED to suppress: messages count changed ${c0} -> ${c1}`);
  });

  await runPhase(k, "messages_ui", async () => {
    await verifyMessagesTabUI(k, JSON_FLOW, topic, "09-json-messages-ui");
  });

  await runPhase(k, "metrics_ui", async () => {
    await openFlowSheet(JSON_FLOW);
    await page.getByRole("tab", { name: "Metrics" }).click();
    await expect(page.getByText(topic).first()).toBeVisible({ timeout: 30_000 });
    await shot(k, "10-json-metrics-after-dedup");
    await page.keyboard.press("Escape");
  });

  if (e.started) await stopFlowUI(k, flowId, "11-json-stopped");
});

// =====================================================================
// R2 — the full CSV journey (run 1 aborted before saving; no leftovers)
// =====================================================================
test("R2 csv — probe, build dt csv addresses, deploy, csv->json records, UI, stop", async () => {
  test.setTimeout(1_200_000);
  const k = "csv";
  const e = entry(k);
  const topic = `raw.${tokenize(CSV_FLOW)}.dt_row`;
  e.flowName = CSV_FLOW;
  e.topic = topic;
  saveLedger();

  let baseUrl = "";
  let pathValue = "";
  let headerCells: string[] = [];
  let flowId = "";
  let alreadyVerified = false;

  await runPhase(k, "probe", async () => {
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
          baseUrl = `${u.protocol}//${u.host}`;
          pathValue = u.pathname;
          headerCells = firstLine.split(",").map((c) => c.trim().replace(/^"|"$/g, ""));
          break;
        }
      } catch (err) {
        probeLog.push({ url, error: String(err).slice(0, 200) });
      }
    }
    e.data!["probe"] = { candidates: probeLog, chosenBaseUrl: baseUrl, chosenPath: pathValue, headerCells };
    fs.writeFileSync(path.join(ART, "csv-probe.json"), JSON.stringify(e.data!["probe"], null, 2), "utf-8");
    saveLedger();
    if (!baseUrl) throw new Error(`no CSV candidate passed the probe: ${JSON.stringify(probeLog)}`);
  });

  await runPhase(k, "resume_check", async () => {
    // Rerun safety: if a previous (possibly partial) remediation run already
    // built, deployed and landed records for this flow, verify against the
    // LIVE state instead of duplicating the flow (nothing is ever deleted).
    const existing = (await apiFlows()).find((f) => f.name === CSV_FLOW);
    if (!existing) return;
    if (!existing.nifiProcessGroupId) throw new Error(`flow "${CSV_FLOW}" exists undeployed (${existing.id}) — manual look needed`);
    const msgs = await apiMessages(existing.id, topic);
    if (msgs.length === 0) throw new Error(`flow "${CSV_FLOW}" exists (${existing.id}) but has no records on ${topic}`);
    alreadyVerified = true;
    flowId = existing.id;
    e.flowId = flowId;
    const sample = parseNewestMessage(msgs);
    e.data!["firstBatchCount"] = msgs.length;
    e.data!["sample"] = sample;
    e.data!["sampleFields"] = Object.keys(sample);
    e.data!["resumedFromPriorRun"] = true;
    fs.writeFileSync(path.join(ART, "csv-sample-messages.json"), JSON.stringify(msgs.slice(0, 5), null, 2), "utf-8");
    saveLedger();
    const present = headerCells.filter((h) => h in sample);
    if (present.length < Math.min(3, headerCells.length)) {
      throw new Error(
        `message is not a JSON object keyed by the CSV header columns. header=${JSON.stringify(headerCells)} sampleKeys=${JSON.stringify(Object.keys(sample))}`,
      );
    }
  });

  if (!alreadyVerified) {
    await runPhase(k, "service", async () => {
      await ensureHttpService(k, "dt csvhost", baseUrl, "20-csv-service");
    });

    await runPhase(k, "create_flow", async () => {
      flowId = await createFlow(CSV_FLOW);
      e.flowId = flowId;
      saveLedger();
    });

    await runPhase(k, "configure_root", async () => {
      await configureHttpReadRoot({ serviceName: "dt csvhost", pathValue, format: "csv" });
      await shot(k, "21-csv-root-configured");
    });

    await runPhase(k, "kafka_child", async () => {
      await addKafkaWriteChild(k, "dt_row", topic, "22-csv-kafka-child");
    });

    await runPhase(k, "save", async () => {
      await saveFlow();
    });

    await runPhase(k, "deploy", async () => {
      await deployFlow(k, "23-csv");
    });

    await runPhase(k, "enable_start", async () => {
      await enableAndStart(k, "24-csv");
    });

    await runPhase(k, "records", async () => {
      const msgs = await pollStableMessages(flowId, topic, 300_000);
      if (msgs.length === 0) {
        await captureDefectEvidence(k, flowId);
        throw new Error(`no messages on ${topic} within 300s of Start (cron ${CRON}) — defect-csv.json captured`);
      }
      const sample = parseNewestMessage(msgs);
      e.data!["firstBatchCount"] = msgs.length;
      e.data!["sample"] = sample;
      e.data!["sampleFields"] = Object.keys(sample);
      fs.writeFileSync(path.join(ART, "csv-sample-messages.json"), JSON.stringify(msgs.slice(0, 5), null, 2), "utf-8");
      saveLedger();
      const present = headerCells.filter((h) => h in sample);
      if (present.length < Math.min(3, headerCells.length)) {
        throw new Error(
          `message is not a JSON object keyed by the CSV header columns. header=${JSON.stringify(headerCells)} sampleKeys=${JSON.stringify(Object.keys(sample))}`,
        );
      }
    });
  }

  await runPhase(k, "messages_ui", async () => {
    await verifyMessagesTabUI(k, CSV_FLOW, topic, "25-csv-messages-ui");
  });

  if (e.started) {
    await stopFlowUI(k, flowId, "26-csv-stopped");
  } else if (alreadyVerified && flowId) {
    // Resumed path: make sure the prior run's flow is not left running.
    const f = (await apiFlows()).find((x) => x.id === flowId);
    if (f?.state === "Running" || f?.state === "Paused") {
      await stopFlowUI(k, flowId, "26-csv-stopped");
    } else {
      entry(k).phases["stop"] = { ok: true, note: `flow already ${f?.state ?? "unknown"} from the prior run`, at: new Date().toISOString() };
      saveLedger();
    }
  }
});

// =====================================================================
// R3 — XML Messages-tab UI evidence (flow already verified + stopped)
// =====================================================================
test("R3 xml — Messages tab UI evidence on the stopped dt xml feed", async () => {
  test.setTimeout(300_000);
  const k = "xml";
  const e = entry(k);
  const topic = `raw.${tokenize(XML_FLOW)}.dt_item`;
  e.flowName = XML_FLOW;
  e.topic = topic;
  saveLedger();

  await runPhase(k, "locate_flow", async () => {
    const f = await flowByName(XML_FLOW);
    e.flowId = f.id;
    if (!f.nifiProcessGroupId) throw new Error(`${XML_FLOW} is not deployed`);
    const msgs = await apiMessages(f.id, topic);
    e.data!["messagesOnTopic"] = msgs.length;
    saveLedger();
    if (msgs.length === 0) throw new Error(`expected run-1 records on ${topic}, found 0`);
  });

  await runPhase(k, "messages_ui", async () => {
    await verifyMessagesTabUI(k, XML_FLOW, topic, "35-xml-messages-ui");
  });
});

// =====================================================================
// R4 — final evidence + verdict
// =====================================================================
test("R4 final — all three dt flows present (API + Flows page), overall verdict", async () => {
  test.setTimeout(300_000);

  const flows = await apiFlows();
  fs.writeFileSync(path.join(ART, "flows-final.json"), JSON.stringify(flows, null, 2), "utf-8");
  for (const name of [JSON_FLOW, CSV_FLOW, XML_FLOW]) {
    const f = flows.find((x) => x.name === name);
    expect.soft(f, `${name} missing from GET /api/v2/flows/`).toBeTruthy();
    expect.soft(f?.nifiProcessGroupId, `${name} should be left DEPLOYED`).toBeTruthy();
    expect.soft(f?.state, `${name} should be left Stopped`).toBe("Stopped");
  }

  await page.goto("/flows");
  await page.getByPlaceholder("Search flows, entities, topics…").fill("dt");
  for (const name of [JSON_FLOW, CSV_FLOW, XML_FLOW]) {
    await expect(page.getByText(name, { exact: true }).first()).toBeVisible();
  }
  await page.screenshot({ path: path.join(ART, "40-final-flows-page.png"), fullPage: true });
  saveLedger();

  const failures: string[] = [];
  for (const [k, e] of Object.entries(LEDGER)) {
    for (const [phase, p] of Object.entries(e.phases)) {
      if (!p.ok) failures.push(`${k}/${phase}: ${p.error ?? "failed"}`);
    }
  }
  expect(failures, `remediation failures:\n${failures.join("\n")}`).toEqual([]);
});
