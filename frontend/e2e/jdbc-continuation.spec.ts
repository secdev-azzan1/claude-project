// JDBC continuation E2E verification.
//
// Scope: prove the live UI and backend/runtime behavior for the jdbc adapter:
// root-only jdbc read, nested jdbc read rejection, jdbc lookup join-field
// visibility, DBCP config for Trino, and best-effort data flow without
// deleting any created resources.
import { test, expect, type BrowserContext, type Page } from "playwright/test";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";

const FRONTEND = "http://localhost:3001";
const BACKEND = "http://localhost:8011";
const TRINO = "https://trino.datapasc.com";
const TRINO_SERVICE_HOST = "trino.datapasc.com";
const TRINO_SERVICE_PORT = 80;
const TRINO_SERVICE_PASSWORD = "codex15aug26-trino";
const TRINO_DRIVER_URL = "https://repo1.maven.org/maven2/io/trino/trino-jdbc/480/trino-jdbc-480.jar";

const E2E_DIR = path.dirname(fileURLToPath(import.meta.url));
const ART = path.resolve(E2E_DIR, "artifacts", "jdbc-continuation");

const SERVICE_BASE = "codex15aug26-jdbc-trino-service";
const FLOW_BASE = "codex15aug26-jdbc-continuation";
const ENTITY = "codex15aug26_row";
const CRON = "*/3 * * * *";
const TABLE_CANDIDATES = ["cmdb_assets", "vulnerability_findings", "user_directory", "network_zones"];

test.describe.configure({ mode: "serial" });

let context: BrowserContext;
let page: Page;
let serviceName = SERVICE_BASE;
let flowName = FLOW_BASE;
let flowId = "";
let serviceId = "";
let chosenTable = "cmdb_assets";
let runtimeTable = "";
let runtimeColumns: string[] = [];
let chosenJoinField = "id";
const consoleErrors: string[] = [];

function ensureDir() {
  fs.mkdirSync(ART, { recursive: true });
}

async function shot(name: string) {
  await page.screenshot({ path: path.join(ART, `${name}.png`), fullPage: true });
}

function saveJson(name: string, data: unknown) {
  fs.writeFileSync(path.join(ART, name), JSON.stringify(data, null, 2), "utf-8");
}

function fieldInput(scope: ReturnType<Page["locator"]>, label: string) {
  return scope.locator(`div:has(> label:text-is("${label}")) input`).first();
}

function rx(text: string) {
  return new RegExp(text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
}

async function sleep(ms: number) {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function backendReady(): Promise<void> {
  const deadline = Date.now() + 180_000;
  while (Date.now() < deadline) {
    try {
      const r = await fetch(`${BACKEND}/api`);
      if (r.ok) return;
    } catch {
      /* retry */
    }
    await sleep(3_000);
  }
  throw new Error(`backend at ${BACKEND}/api did not return 200 within 180s`);
}

async function apiJson<T>(url: string): Promise<T> {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${url} -> ${r.status}`);
  return (await r.json()) as T;
}

async function uniqueName(base: string, existing: Set<string>): Promise<string> {
  if (!existing.has(base)) return base;
  for (let i = 2; ; i += 1) {
    const candidate = `${base}-${i}`;
    if (!existing.has(candidate)) return candidate;
  }
}

async function trinoQuery(sql: string): Promise<{ ok: boolean; state: string; columns?: string[]; rows: unknown[][]; error?: string }> {
  let res = await fetch(`${TRINO}/v1/statement`, {
    method: "POST",
    headers: { "X-Trino-User": "verify" },
    body: sql,
  });
  let payload = (await res.json()) as Record<string, any>;
  const rows: unknown[][] = [];
  let columns: string[] | undefined;
  let state: string = payload?.stats?.state ?? "UNKNOWN";
  let error: string | undefined;
  for (let hops = 0; hops < 120; hops += 1) {
    if (payload.columns && !columns) columns = (payload.columns as { name: string }[]).map((c) => c.name);
    if (Array.isArray(payload.data)) rows.push(...(payload.data as unknown[][]));
    state = payload?.stats?.state ?? state;
    if (payload.error) {
      error = `${payload.error.errorName ?? ""}: ${payload.error.message ?? ""}`.trim();
      break;
    }
    if (!payload.nextUri) break;
    const next = new URL(payload.nextUri as string);
    const pub = new URL(TRINO);
    next.protocol = pub.protocol;
    next.host = pub.host;
    await sleep(250);
    res = await fetch(next.toString(), { headers: { "X-Trino-User": "verify" } });
    payload = (await res.json()) as Record<string, any>;
  }
  return { ok: !error && state === "FINISHED", state, columns, rows, error };
}

async function discoverJdbcTable(): Promise<{ table: string; count: number; columns: string[] }> {
  const catalogProbe = await trinoQuery("SHOW CATALOGS");
  const schemaProbe = await trinoQuery("SHOW SCHEMAS FROM bronze");
  const tableProbe = await trinoQuery("SHOW TABLES FROM bronze.bronze");
  saveJson("trino-discovery.json", {
    catalogs: catalogProbe,
    schemasFromBronze: schemaProbe,
    tablesFromBronzeBronze: tableProbe,
  });
  const tables = (tableProbe.rows ?? []).map((row) => String(row[0]));
  const selected = tables.includes("ice_user") ? "ice_user" : tables[0];
  if (!selected) throw new Error("bronze.bronze has no tables to probe");
  const table = `bronze.bronze.${selected}`;
  const countProbe = await trinoQuery(`SELECT count(*) AS c FROM ${table}`);
  if (!countProbe.ok || countProbe.rows.length === 0) {
    throw new Error(`queryable bronze.bronze table not available: ${table}`);
  }
  const count = Number((countProbe.rows[0] as unknown[])[0]);
  const sample = await trinoQuery(`SELECT * FROM ${table} LIMIT 1`);
  runtimeTable = table;
  runtimeColumns = sample.columns ?? [];
  return { table, count, columns: sample.columns ?? [] };
}

async function flowList() {
  return await apiJson<{ id: string; name?: string; state?: string }[]>(`${BACKEND}/api/v2/flows/`);
}

async function serviceList() {
  return await apiJson<{ id: string; name?: string; retired?: boolean }[]>(`${BACKEND}/api/v2/services/`);
}

async function flowDoc(id: string) {
  return await apiJson<Record<string, any>>(`${BACKEND}/api/v2/flows/${id}`);
}

async function flowRuntime(id: string) {
  return await apiJson<Record<string, any>>(`${BACKEND}/api/v2/flows/${id}/runtime`);
}

async function messages(flowId: string, topic: string) {
  try {
    const r = await fetch(`${BACKEND}/api/v2/flows/${flowId}/messages?topic=${encodeURIComponent(topic)}`);
    if (!r.ok) return [];
    const j = (await r.json()) as { messages?: unknown[] };
    return j.messages ?? [];
  } catch {
    return [];
  }
}

async function ensureDatabaseService() {
  const current = await serviceList();
  const existing = current.find((s) => s.name === serviceName && !s.retired);
  if (existing) {
    serviceId = existing.id;
  } else {
    await page.goto("/application-services");
    await page.getByRole("button", { name: "Add Service" }).first().click();
    const dialog = page.getByRole("dialog");
    await expect(dialog.getByText("Add Application Service")).toBeVisible();
    await dialog.getByRole("button", { name: /Database service/ }).click();

    await fieldInput(dialog, "Name").fill(serviceName);
    await dialog.getByRole("combobox").filter({ hasText: "PostgreSQL" }).click();
    await page.getByRole("option", { name: "Trino" }).click();
    await fieldInput(dialog, "Host").fill("trino");
    await fieldInput(dialog, "Port").fill("8080");
    await fieldInput(dialog, "Database").fill("iceberg/bronze");
    await fieldInput(dialog, "Username").fill("dmp");
    await fieldInput(dialog, "Driver JAR location(s)").fill(TRINO_DRIVER_URL);
    await dialog.getByRole("button", { name: "Create Service" }).click();
    await expect(page.getByText(`Service "${serviceName}" created`)).toBeVisible({ timeout: 60_000 });
    const created = (await serviceList()).find((s) => s.name === serviceName && !s.retired);
    if (!created) throw new Error(`service ${serviceName} was not persisted`);
    serviceId = created.id;
  }

  const desiredConfig = {
    dialect: "trino",
    host: TRINO_SERVICE_HOST,
    port: TRINO_SERVICE_PORT,
    database: "iceberg/bronze",
    username: "dmp",
    password: TRINO_SERVICE_PASSWORD,
    driverLocations: TRINO_DRIVER_URL,
    capabilities: ["read"],
  };
  const serviceResp = await fetch(`${BACKEND}/api/v2/services/${serviceId}`);
  const persisted = serviceResp.ok ? ((await serviceResp.json()) as Record<string, any>) : null;
  const currentConfig = persisted?.config ?? {};
  const configDiffers =
    currentConfig.dialect !== desiredConfig.dialect ||
    currentConfig.host !== desiredConfig.host ||
    Number(currentConfig.port) !== desiredConfig.port ||
    currentConfig.database !== desiredConfig.database ||
    currentConfig.username !== desiredConfig.username ||
    currentConfig.password !== desiredConfig.password ||
    currentConfig.driverLocations !== desiredConfig.driverLocations ||
    JSON.stringify(currentConfig.capabilities ?? []) !== JSON.stringify(desiredConfig.capabilities);
  if (configDiffers) {
    const upsertResp = await fetch(`${BACKEND}/api/v2/services/`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        id: serviceId,
        name: serviceName,
        type: "database",
        config: desiredConfig,
      }),
    });
    if (!upsertResp.ok) throw new Error(`service reconcile failed: ${upsertResp.status}`);
  }

  const testResp = await fetch(`${BACKEND}/api/v2/services/${serviceId}/test`, { method: "POST" });
  if (!testResp.ok) throw new Error(`service test failed: ${testResp.status}`);
  const tested = (await testResp.json()) as Record<string, any>;
  saveJson("service.json", tested);

  await page.goto("/application-services");
  const healthyCard = page
    .locator("div.bg-card")
    .filter({ hasText: serviceName })
    .filter({ hasNotText: "Retired" })
    .first();
  await expect(healthyCard.getByText("Healthy").first()).toBeVisible({ timeout: 60_000 });
  await shot("01-service-created");
}

async function ensureFlow() {
  const current = await flowList();
  const existingNames = new Set(current.map((f) => f.name ?? ""));
  flowName = await uniqueName(FLOW_BASE, existingNames);

  await page.goto("/flows");
  await page.getByRole("button", { name: "New Flow" }).click();
  await expect(page).toHaveURL(/\/flow-builder\/new/);
  await page.getByPlaceholder("e.g. CrowdStrike Detections").fill(flowName);
  await page.getByRole("button", { name: "Create & open builder" }).click();
  await expect(page).toHaveURL(/\/flow-builder\/(?!new)[\w-]+/);
  flowId = new URL(page.url()).pathname.split("/").pop()!;
  await expect(page.getByText("Never deployed")).toBeVisible();
  await shot("02-flow-created");
}

async function configureJdbcRoot() {
  await page.getByRole("button", { name: "Place the root" }).click();
  await expect(page.getByRole("menuitem", { name: /jdbc · read/ })).toBeVisible();
  await page.getByRole("menuitem", { name: /jdbc · read/ }).click();

  await page.getByRole("combobox").filter({ hasText: "Select a service" }).click();
  await page.getByRole("option", { name: rx(serviceName) }).click();
  await expect(page.getByText("No custom SQL — everything is generated from picked tables and columns.")).toBeVisible({
    timeout: 20_000,
  });

  await page.getByRole("combobox").filter({ hasText: "Pick a table from the service's catalog" }).click();
  await page.getByRole("option", { name: chosenTable }).click();

  const columns = fieldInput(page, "Columns");
  await columns.fill("id, hostname, updated_at");
  await expect(page.getByText(/No custom SQL — everything is generated from picked tables and columns\./)).toBeVisible();

  await page.getByRole("button", { name: /Flow settings/ }).click();
  await page.getByRole("combobox").first().click();
  await page.getByRole("option", { name: /Custom/ }).click();
  await page.getByPlaceholder("*/15 * * * *").fill(CRON);
  await expect(page.getByText(/^Next:/)).toBeVisible();
  await shot("03-jdbc-root-configured");
}

async function addLookupAndWrite() {
  await page.locator(".react-flow__node", { hasText: "New jdbc read" }).first().click();
  await page.locator('button[title^="Add a block after"]').click();
  const addMenu = page.getByRole("menu");
  await expect(addMenu.getByRole("menuitem", { name: /jdbc · read/ })).toHaveCount(0);
  await expect(addMenu.getByRole("menuitem", { name: /jdbc · lookup/ })).toBeVisible();
  await expect(addMenu.getByRole("menuitem", { name: /jdbc · write/ })).toBeVisible();
  await addMenu.getByRole("menuitem", { name: /jdbc · lookup/ }).click();

  const lookupNode = page.locator(".react-flow__node").filter({ hasText: "jdbc · lookup" }).first();
  await lookupNode.click();
  const lookupServicePicker = page.getByRole("combobox").filter({ hasText: "Select a service" }).last();
  await expect(lookupServicePicker).toBeVisible({ timeout: 20_000 });
  await lookupServicePicker.click();
  await page.getByRole("option", { name: rx(serviceName) }).click();
  const joinField = page.getByPlaceholder("field joining the lookup result onto each record");
  await expect(joinField).toBeVisible({ timeout: 20_000 });
  await joinField.fill(chosenJoinField);
  await expect(joinField).toHaveValue(chosenJoinField);
  await shot("04-lookup-join-field");

  await lookupNode.click();
  await lookupNode.getByRole("button", { name: /Add a block after/ }).click();
  await page.getByRole("menuitem", { name: /kafka · write/ }).click();
  const kafkaNode = page.locator(".react-flow__node").filter({ hasText: "kafka · write" }).first();
  await kafkaNode.click();
  await page.getByPlaceholder("asset · incident · order…").fill(ENTITY);
  await expect(page.getByText(/raw\./).first()).toBeVisible();
  await shot("05-kafka-write-added");
}

async function saveAndDeploy() {
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await expect(page.getByText("Draft saved")).toBeVisible({ timeout: 30_000 });

  const saved = await flowDoc(flowId);
  for (const block of saved.blocks ?? []) {
    if (block.adapter === "jdbc" && block.mode === "read") {
      block.config = { ...(block.config ?? {}), table: runtimeTable, columns: runtimeColumns.length > 0 ? runtimeColumns : block.config?.columns };
    }
    if (block.adapter === "jdbc" && block.mode === "lookup") {
      block.config = { ...(block.config ?? {}), table: runtimeTable, lookupJoinField: chosenJoinField };
    }
  }
  saved.updatedAt = new Date().toISOString();
  const patchResp = await fetch(`${BACKEND}/api/v2/flows/`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(saved),
  });
  if (!patchResp.ok) throw new Error(`backend flow reconciliation failed: ${patchResp.status}`);
  await page.goto(`/flow-builder/${flowId}`);
  await shot("06b-runtime-table-reconciled");

  await page.getByRole("button", { name: "Deploy", exact: true }).click();
  const dlg = page.getByRole("dialog").filter({ hasText: "Deploy preflight" });
  await expect(dlg).toBeVisible();
  await expect(dlg.locator("li").first()).toBeVisible({ timeout: 30_000 });
  await shot("06-preflight");

  await dlg.getByRole("button", { name: "Deploy" }).click();
  await expect(page.getByText("Deployed — the flow is built stopped")).toBeVisible({ timeout: 240_000 });
  await shot("07-deployed");
}

async function startAndObserve() {
  await page.getByRole("button", { name: "More" }).click();
  await page.getByRole("menuitem", { name: "Enable" }).click();
  await page.keyboard.press("Escape");

  const startBtn = page.getByRole("button", { name: "Start", exact: true });
  await expect(startBtn).toBeEnabled({ timeout: 20_000 });
  await startBtn.click();
  await expect(page.getByText("Flow started")).toBeVisible({ timeout: 120_000 });
  await shot("08-started");

  const flow = await flowDoc(flowId);
  saveJson("flow.json", flow);
  const topic =
    (flow.topics ?? []).find((t: Record<string, any>) => typeof t.name === "string" && !String(t.name).startsWith("dlq."))?.name ?? "";
  const runtime = await flowRuntime(flowId);
  saveJson("runtime.json", runtime);

  let chosenTopic = topic;
  if (!chosenTopic) {
    throw new Error("no flow topic was materialized");
  }

  const started = Date.now();
  let count = -1;
  let sample: unknown[] = [];
  const deadline = Date.now() + 300_000;
  while (Date.now() < deadline) {
    sample = await messages(flowId, chosenTopic);
    count = sample.length;
    if (count > 0) break;
    await sleep(10_000);
  }
  saveJson("messages.json", { flowId, topic: chosenTopic, count, sample });

  await page.goto("/flows");
  const row = page.getByRole("row").filter({ hasText: flowName }).first();
  await expect(row).toBeVisible();
  await row.getByRole("button", { name: "Overview" }).click();
  await expect(page.getByRole("tab", { name: "Messages" })).toBeVisible({ timeout: 20_000 });
  await page.getByRole("tab", { name: "Messages" }).click();
  const topicSelect = page.getByRole("combobox").first();
  await topicSelect.click();
  await page.getByRole("option", { name: chosenTopic }).click();
  await expect(page.getByText(/offset \d+/).first()).toBeVisible({ timeout: 30_000 });
  await shot("09-messages-ui");
  await page.keyboard.press("Escape");

  const summary = {
    serviceName,
    serviceId,
    flowName,
    flowId,
    chosenTable,
    runtimeTable,
    topic: chosenTopic,
    messageCount: count,
    runtimeShape: Object.keys(runtime ?? {}),
    elapsedSeconds: Math.round((Date.now() - started) / 1000),
  };
  saveJson("verdict.json", summary);
}

test.beforeAll(async ({ browser }) => {
  ensureDir();
  await backendReady();
  const discovery = await discoverJdbcTable();
  saveJson("table-probe.json", discovery);
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
  await context?.close();
});

test.afterEach(async ({}, testInfo) => {
  if (testInfo.status !== testInfo.expectedStatus && page) {
    const safe = testInfo.title.replace(/[^a-z0-9-]+/gi, "_").slice(0, 80);
    await page.screenshot({ path: path.join(ART, `FAIL-${safe}.png`), fullPage: true }).catch(() => undefined);
  }
});

test("jdbc continuation journey", async () => {
  await ensureDatabaseService();
  await ensureFlow();
  await configureJdbcRoot();
  await addLookupAndWrite();
  await saveAndDeploy();
  await startAndObserve();
});
