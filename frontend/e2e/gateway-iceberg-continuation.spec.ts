// UI+API E2E verification - Gateway routing + Iceberg sink continuation.
//
// Fresh run requested on 2026-08-15. Uses unique `codex15aug26-*` resource
// names and leaves every created resource in place.

import { expect, test, type BrowserContext, type Page } from "playwright/test";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";

const FRONTEND = "http://localhost:3001";
const BACKEND = "http://localhost:8011";
const APISIX_ADMIN = "https://apisix-admin.datapasc.com";
const TRINO = "https://trino.datapasc.com";
const KAFKA_CONNECT = "https://kafkaconnect.datapasc.com";

const E2E_DIR = path.dirname(fileURLToPath(import.meta.url));
const ART = path.resolve(E2E_DIR, "artifacts", "gateway-iceberg-continuation");
const SAMPLE_PATH = path.join(ART, "codex15aug26-posts-sample.json");

const PREFIX = "codex15aug26";
const PROXY_NAME = `${PREFIX} dummyjson`;
const GATEWAY_SERVICE = `${PREFIX} proxied dummyjson`;
const GATEWAY_FLOW = `${PREFIX} gateway posts`;
const GATEWAY_ENTITY = `${PREFIX}_post`;
const ICEBERG_SERVICE = `${PREFIX} iceberg catalog`;
const ICEBERG_FLOW = `${PREFIX} iceberg posts`;
const ICEBERG_ENTITY = `${PREFIX}_ice_post`;
const CRON = "*/1 * * * *";

test.describe.configure({ mode: "serial" });

let context: BrowserContext;
let page: Page;
let consoleErrors: string[] = [];
let gatewayFlowId = "";
let icebergFlowId = "";
let gatewayTopic = "";
let icebergTopic = "";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const tokenize = (input: string) =>
  input
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .replace(/_{2,}/g, "_");

async function shot(name: string) {
  await page.screenshot({ path: path.join(ART, `${name}.png`), fullPage: true });
}

function saveJson(name: string, data: unknown) {
  fs.writeFileSync(path.join(ART, name), JSON.stringify(data, null, 2), "utf-8");
}

function fieldInput(scope: ReturnType<Page["locator"]>, label: string) {
  return scope.locator(`div:has(> label:text-is("${label}")) input`).first();
}

function backendEnv(): string {
  return fs.readFileSync(path.resolve(E2E_DIR, "..", "..", "backend", ".env"), "utf-8");
}

function apisixAdminKey(): string {
  const env = backendEnv();
  const m = env.match(/^APISIX_ADMIN_KEY=(.+)$/m);
  if (!m) throw new Error("APISIX_ADMIN_KEY not found in backend/.env");
  return m[1].trim();
}

async function backendReady(): Promise<void> {
  const deadline = Date.now() + 180_000;
  while (Date.now() < deadline) {
    try {
      const r = await fetch(`${BACKEND}/api`);
      if (r.ok) return;
    } catch {
      // keep polling
    }
    await sleep(3000);
  }
  throw new Error(`Backend at ${BACKEND}/api did not return 200 within 180s`);
}

async function listGatewayProxies(): Promise<{ id: string; name: string }[]> {
  const r = await fetch(`${BACKEND}/api/v2/gateway/`);
  if (!r.ok) throw new Error(`gateway list failed: ${r.status}`);
  const j = (await r.json()) as { proxies?: { id: string; name: string }[] };
  return j.proxies ?? [];
}

async function listServices(): Promise<{ id: string; name: string }[]> {
  const r = await fetch(`${BACKEND}/api/v2/services/`);
  if (!r.ok) throw new Error(`service list failed: ${r.status}`);
  const j = (await r.json()) as { services?: { id: string; name: string }[] };
  return j.services ?? [];
}

async function flowDoc(flowId: string): Promise<Record<string, any>> {
  const r = await fetch(`${BACKEND}/api/v2/flows/${flowId}`);
  if (!r.ok) throw new Error(`flow ${flowId} fetch failed: ${r.status}`);
  return (await r.json()) as Record<string, any>;
}

async function flowRuntime(flowId: string): Promise<Record<string, any>> {
  const r = await fetch(`${BACKEND}/api/v2/flows/${flowId}/runtime`);
  if (!r.ok) throw new Error(`runtime ${flowId} fetch failed: ${r.status}`);
  return (await r.json()) as Record<string, any>;
}

async function messagesCount(flowId: string, topic: string): Promise<number> {
  const r = await fetch(`${BACKEND}/api/v2/flows/${flowId}/messages?topic=${encodeURIComponent(topic)}`);
  if (!r.ok) return -1;
  const j = (await r.json()) as { messages?: unknown[] };
  return (j.messages ?? []).length;
}

async function trinoQuery(
  sql: string,
): Promise<{ ok: boolean; state: string; columns?: string[]; rows: unknown[][]; error?: string }> {
  let res = await fetch(`${TRINO}/v1/statement`, {
    method: "POST",
    headers: { "X-Trino-User": "verify" },
    body: sql,
  });
  let j = (await res.json()) as Record<string, any>;
  const rows: unknown[][] = [];
  let columns: string[] | undefined;
  let state = j?.stats?.state ?? "UNKNOWN";
  let error: string | undefined;
  for (let hops = 0; hops < 120; hops++) {
    if (j.columns && !columns) columns = (j.columns as { name: string }[]).map((c) => c.name);
    if (Array.isArray(j.data)) rows.push(...(j.data as unknown[][]));
    state = j?.stats?.state ?? state;
    if (j.error) {
      error = `${j.error.errorName ?? ""}: ${j.error.message ?? ""}`.trim();
      break;
    }
    if (!j.nextUri) break;
    const next = new URL(j.nextUri as string);
    const pub = new URL(TRINO);
    next.protocol = pub.protocol;
    next.host = pub.host;
    await sleep(250);
    res = await fetch(next.toString(), { headers: { "X-Trino-User": "verify" } });
    j = (await res.json()) as Record<string, any>;
  }
  return { ok: !error && state === "FINISHED", state, columns, rows, error };
}

async function findIcebergTable(term: string): Promise<{ catalog: string; schema: string; table: string }> {
  for (const catalog of ["bronze", "iceberg"]) {
    const res = await trinoQuery(
      `SELECT table_schema, table_name FROM ${catalog}.information_schema.tables WHERE table_schema LIKE '%${term}%' OR table_name LIKE '%${term}%'`,
    );
    if (res.ok && res.rows.length > 0) {
      const row = res.rows[0] as [string, string];
      return { catalog, schema: String(row[0]), table: String(row[1]) };
    }
  }
  throw new Error(`Could not find an Iceberg table containing "${term}"`);
}

test.beforeAll(async ({ browser }) => {
  fs.mkdirSync(ART, { recursive: true });
  await backendReady();

  const sample = await fetch("https://dummyjson.com/posts?limit=5");
  if (!sample.ok) throw new Error(`sample fetch failed: ${sample.status}`);
  fs.writeFileSync(SAMPLE_PATH, JSON.stringify(await sample.json(), null, 2), "utf-8");

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
    const safe = testInfo.title.replace(/[^a-z0-9-]+/gi, "_").slice(0, 60);
    await page.screenshot({ path: path.join(ART, `FAIL-${safe}.png`), fullPage: true }).catch(() => undefined);
  }
});

test("A1 proxies navigation uses Proxies and the gateway proxy can be allowlisted", async () => {
  await page.goto("/");
  const link = page.getByRole("link", { name: "Proxies", exact: true });
  await expect(link).toBeVisible();
  await shot("A1-sidebar-proxies");

  await link.click();
  await expect(page).toHaveURL(/\/apisix/);
  await expect(page.locator("h1", { hasText: "Proxies" })).toBeVisible();
  await shot("A1b-proxies-page");

  await page.getByLabel("Host to allowlist").fill("dummyjson.com");
  await page.getByRole("button", { name: /Add host \(admin\)/ }).click();
  const adminDlg = page.getByRole("alertdialog");
  await expect(adminDlg.getByText("Administrator action")).toBeVisible();
  await expect(adminDlg.getByText("dummyjson.com").first()).toBeVisible();
  await shot("A2-admin-confirm-dialog");
  await adminDlg.getByRole("button", { name: /Confirm as admin/ }).click();
  await expect(page.getByText(/Admin action recorded/)).toBeVisible();
  await expect(page.getByText("dummyjson.com").first()).toBeVisible();
  await shot("A2b-host-allowlisted");

  await page.getByRole("button", { name: "Add Proxy" }).first().click();
  const dlg = page.getByRole("dialog");
  await expect(dlg.getByText("Add APISIX Proxy")).toBeVisible();
  await fieldInput(dlg, "Name").fill(PROXY_NAME);
  await fieldInput(dlg, "Target host").fill("dummyjson.com");
  await expect(fieldInput(dlg, "Port")).toHaveValue("443");
  await fieldInput(dlg, "SNI").fill("dummyjson.com");
  await dlg.getByRole("button", { name: "Create Proxy" }).click();
  await expect(page.getByText(`Proxy "${PROXY_NAME}" created`)).toBeVisible();

  const card = page.locator("div.bg-card").filter({ hasText: PROXY_NAME }).first();
  await expect(card).toBeVisible();
  await card.getByRole("button", { name: "Reconcile" }).click();
  await expect(page.getByText(`"${PROXY_NAME}" is live on the gateway`)).toBeVisible({ timeout: 60_000 });
  await shot("A3-proxy-reconciled-card");

  const proxies = await listGatewayProxies();
  const proxy = proxies.find((p) => p.name === PROXY_NAME);
  expect(proxy, "proxy present in gateway catalog").toBeTruthy();
  const key = apisixAdminKey();
  const admin = async (p: string) => {
    const r = await fetch(`${APISIX_ADMIN}${p}`, { headers: { "X-API-KEY": key } });
    return { status: r.status, body: (await r.json()) as unknown };
  };
  const upstream = await admin(`/apisix/admin/upstreams/dmp_${proxy!.id}`);
  const routeRoot = await admin(`/apisix/admin/routes/dmp_${proxy!.id}_root`);
  const routeWild = await admin(`/apisix/admin/routes/dmp_${proxy!.id}_wild`);
  saveJson("A3-apisix-objects.json", { proxyId: proxy!.id, upstream, routeRoot, routeWild });
  expect(upstream.status).toBe(200);
  expect(routeRoot.status).toBe(200);
  expect(routeWild.status).toBe(200);

  await card.getByRole("button", { name: "Test", exact: true }).click();
  await expect(card.getByText(/Reached codex15aug26_dummyjson/).first()).toBeVisible({ timeout: 60_000 });
  await shot("A3b-proxy-test-ok");

  await page.goto("/application-services");
  await page.getByRole("button", { name: "Add Service" }).first().click();
  const svcDlg = page.getByRole("dialog");
  await expect(svcDlg.getByText("Add Application Service")).toBeVisible();
  await svcDlg.getByRole("button", { name: /HTTP service/ }).click();
  await fieldInput(svcDlg, "Name").fill(GATEWAY_SERVICE);
  await fieldInput(svcDlg, "Base URL").fill("https://dummyjson.com");
  await svcDlg.getByRole("combobox").filter({ hasText: "No proxy" }).click();
  await page.getByRole("option", { name: new RegExp(PROXY_NAME) }).click();
  await expect(svcDlg.getByText(/Every block using this service calls dummyjson\.com through/)).toBeVisible();
  await shot("A4-service-egress-select");
  await svcDlg.getByRole("button", { name: "Create Service" }).click();
  await expect(page.getByText(`Service "${GATEWAY_SERVICE}" created`)).toBeVisible();
  const svcCard = page.locator("div.bg-card").filter({ hasText: GATEWAY_SERVICE }).filter({ hasNotText: "Retired" }).first();
  await expect(svcCard).toBeVisible();
  await shot("A4b-service-created");

  const services = await listServices();
  const service = services.find((s) => s.name === GATEWAY_SERVICE);
  expect(service, "gateway service present").toBeTruthy();
});

test("A2 gateway flow routes dummyjson posts through APISIX and leaves messages on-topic", async () => {
  await page.goto("/flows");
  await page.getByRole("button", { name: "New Flow" }).click();
  await expect(page).toHaveURL(/\/flow-builder\/new/);
  await page.getByPlaceholder("e.g. CrowdStrike Detections").fill(GATEWAY_FLOW);
  await page.getByRole("button", { name: "Create & open builder" }).click();
  await expect(page).toHaveURL(/\/flow-builder\/(?!new)[\w-]+/);
  gatewayFlowId = new URL(page.url()).pathname.split("/").pop()!;
  gatewayTopic = `raw.${tokenize(GATEWAY_FLOW)}.${tokenize(GATEWAY_ENTITY)}`;

  await page.getByRole("button", { name: "Place the root" }).click();
  await page.getByRole("menuitem", { name: /http · read/ }).click();
  await page.getByRole("combobox").filter({ hasText: "Select a service" }).click();
  await page.getByRole("option", { name: new RegExp(GATEWAY_SERVICE) }).click();
  await page.getByPlaceholder("/users").fill("/posts");
  await page.getByPlaceholder("$.resources[*] (record path)").fill("$.posts[*]");

  await page.getByRole("button", { name: /Flow settings/ }).click();
  await page.getByRole("combobox").first().click();
  await page.getByRole("option", { name: /Custom/ }).click();
  await page.getByPlaceholder("*/15 * * * *").fill(CRON);

  await page.locator(".react-flow__node", { hasText: "New http read" }).first().click();
  await page.locator('button[title^="Add a block after"]').click();
  await page.getByRole("menuitem", { name: /kafka · write/ }).click();
  await page.locator(".react-flow__node").filter({ hasText: "kafka · write" }).first().click();
  await page.getByPlaceholder("asset · incident · order…").fill(GATEWAY_ENTITY);
  await shot("A5-flow-built");

  await page.getByRole("button", { name: "Save", exact: true }).click();
  await expect(page.getByText("Draft saved")).toBeVisible();

  await page.getByRole("button", { name: "Deploy", exact: true }).click();
  const dlg = page.getByRole("dialog").filter({ hasText: "Deploy preflight" });
  await expect(dlg).toBeVisible();
  await expect(dlg.locator("li").first()).toBeVisible({ timeout: 30_000 });
  await shot("A6-preflight");
  await dlg.getByRole("button", { name: "Deploy" }).click();
  await expect(page.getByText("Deployed — the flow is built stopped")).toBeVisible({ timeout: 240_000 });
  await shot("A6b-deployed");

  const rt = await flowRuntime(gatewayFlowId);
  const withProxy = JSON.stringify(rt).includes("codex15aug26_dummyjson");
  saveJson("A6-runtime-fetch-property.json", { gatewayFlowId, runtime: rt, withProxy });
  expect(withProxy, "runtime should reference the gateway proxy").toBeTruthy();

  await page.goto("/flows");
  await page.getByText(GATEWAY_FLOW).first().click();
  await page.getByRole("tab", { name: "Runtime" }).click();
  await expect(page.getByText("Generated NiFi components")).toBeVisible({ timeout: 60_000 });
  await page.locator("button").filter({ hasText: "New http read" }).first().click();
  await expect(page.getByText(/codex15aug26_dummyjson/).first()).toBeVisible({ timeout: 30_000 });
  await shot("A6c-runtime-tab-gateway-url");

  await page.goto(`/flow-builder/${gatewayFlowId}`);
  await page.getByRole("button", { name: "More" }).click();
  await page.getByRole("menuitem", { name: "Enable" }).click();
  await page.keyboard.press("Escape");
  await page.getByRole("button", { name: "Start", exact: true }).click();
  await expect(page.getByText("Flow started")).toBeVisible({ timeout: 120_000 });
  await shot("A7-started");

  const topicDeadline = Date.now() + 180_000;
  let count = -1;
  while (Date.now() < topicDeadline) {
    count = await messagesCount(gatewayFlowId, gatewayTopic);
    if (count >= 5) break;
    await sleep(10_000);
  }
  fs.writeFileSync(
    path.join(ART, "A7-topic-poll.txt"),
    `topic=${gatewayTopic}\nflowId=${gatewayFlowId}\nmessages=${count}\nexpected>=5\n`,
    "utf-8",
  );
  expect(count, `messages on ${gatewayTopic}`).toBeGreaterThanOrEqual(5);

  await page.goto("/flows");
  await page.getByText(GATEWAY_FLOW).first().click();
  await page.getByRole("tab", { name: "Messages" }).click();
  const topicSelect = page.getByRole("combobox").filter({ hasText: /raw\.|Pick a topic/ }).first();
  await topicSelect.click();
  await page.getByRole("option", { name: gatewayTopic }).click();
  await expect(page.getByText(/offset \d+/).first()).toBeVisible({ timeout: 30_000 });
  await shot("A7b-messages-tab");

  await page.goto(`/flow-builder/${gatewayFlowId}`);
  await page.getByRole("button", { name: "Stop", exact: true }).click();
  await expect(page.getByText("Stopped — queues retained")).toBeVisible({ timeout: 120_000 });
  await shot("A7c-stopped-still-deployed");
});

test("B1 iceberg sink service uses the full visible form and remains healthy", async () => {
  await page.goto("/application-services");
  await page.getByRole("button", { name: "Add Service" }).first().click();
  const dlg = page.getByRole("dialog");
  await expect(dlg.getByText("Add Application Service")).toBeVisible();
  await dlg.getByRole("button", { name: /Sink destination/ }).click();
  await fieldInput(dlg, "Name").fill(ICEBERG_SERVICE);
  await dlg.getByRole("combobox").filter({ hasText: "OpenSearch" }).click();
  await page.getByRole("option", { name: "Iceberg catalog" }).click();
  await fieldInput(dlg, "Catalog URL").fill("https://polaris.datapasc.com/api/catalog");
  await fieldInput(dlg, "Warehouse").fill("bronze");
  await fieldInput(dlg, "OAuth client id").fill("root");
  await fieldInput(dlg, "OAuth client secret").fill("s3cr3t");
  await fieldInput(dlg, "S3 endpoint").fill("https://ozones3g.datapasc.com");
  await fieldInput(dlg, "S3 access key").fill("eltadmin");
  await fieldInput(dlg, "S3 secret key").fill("OzoneS3Key123");
  await fieldInput(dlg, "S3 region").fill("us-east-1");
  await expect(dlg.getByLabel("Path-style access")).toBeChecked();
  await shot("B1-sink-service-form");
  await dlg.getByRole("button", { name: "Create Service" }).click();
  await expect(page.getByText(`Service "${ICEBERG_SERVICE}" created`)).toBeVisible();
  const card = page.locator("div.bg-card").filter({ hasText: ICEBERG_SERVICE }).filter({ hasNotText: "Retired" }).first();
  await expect(card).toBeVisible();
  await card.getByRole("button", { name: "Test", exact: true }).click();
  await expect(card.getByText("Healthy").first()).toBeVisible({ timeout: 60_000 });
  await shot("B1c-sink-service-healthy");

  const services = await listServices();
  const service = services.find((s) => s.name === ICEBERG_SERVICE);
  expect(service, "iceberg sink service present").toBeTruthy();
});

test("B2 iceberg flow registers schema, deploys connector, and lands rows in Trino", async () => {
  await page.goto("/flows");
  await page.getByRole("button", { name: "New Flow" }).click();
  await page.getByPlaceholder("e.g. CrowdStrike Detections").fill(ICEBERG_FLOW);
  await page.getByRole("button", { name: "Create & open builder" }).click();
  await expect(page).toHaveURL(/\/flow-builder\/(?!new)[\w-]+/);
  icebergFlowId = new URL(page.url()).pathname.split("/").pop()!;
  icebergTopic = `raw.${tokenize(ICEBERG_FLOW)}.${tokenize(ICEBERG_ENTITY)}`;

  await page.getByRole("button", { name: "Place the root" }).click();
  await page.getByRole("menuitem", { name: /http · read/ }).click();
  await page.getByRole("combobox").filter({ hasText: "Select a service" }).click();
  await page.getByRole("option", { name: new RegExp(GATEWAY_SERVICE) }).click();
  await page.getByPlaceholder("/users").fill("/posts");
  await page.getByPlaceholder("$.resources[*] (record path)").fill("$.posts[*]");
  await page.getByRole("button", { name: /Flow settings/ }).click();
  await page.getByRole("combobox").first().click();
  await page.getByRole("option", { name: /Custom/ }).click();
  await page.getByPlaceholder("*/15 * * * *").fill(CRON);

  await page.locator(".react-flow__node", { hasText: "New http read" }).first().click();
  await page.locator('button[title^="Add a block after"]').click();
  await page.getByRole("menuitem", { name: "kafka+connect · governed write" }).click();
  await page.locator(".react-flow__node").filter({ hasText: "kafka+connect" }).first().click();
  await page.getByPlaceholder("asset · incident · order…").fill(ICEBERG_ENTITY);
  await page
    .locator("#block-section-sink")
    .getByRole("combobox")
    .filter({ hasText: "Select a service" })
    .click();
  await page.getByRole("option", { name: new RegExp(ICEBERG_SERVICE) }).click();
  await expect(page.locator("#block-section-sink").getByText(/polaris\.datapasc\.com/).first()).toBeVisible();
  await shot("B2-flow-built");

  await page.getByRole("button", { name: "Start ceremony" }).click();
  const cdlg = page.getByRole("dialog").filter({ hasText: "Schema ceremony" });
  await expect(cdlg).toBeVisible();
  await cdlg.getByText("Uploaded sample files").click();
  await shot("B3-ceremony-declare");
  await cdlg.getByRole("button", { name: "Continue", exact: true }).click();

  await cdlg.locator('input[type="file"]').setInputFiles(SAMPLE_PATH);
  await expect(cdlg.getByText("codex15aug26-posts-sample.json")).toBeVisible();
  await cdlg.getByPlaceholder(/blank means the file/).fill("$.posts[*]");
  await expect(cdlg.getByText(/5 record\(s\) matched/)).toBeVisible();
  await shot("B3b-ceremony-upload");
  await cdlg.getByRole("button", { name: "Infer schema" }).click();
  await expect(cdlg.getByText(/Inferred \d+ top-level field\(s\) from 5 record\(s\)/)).toBeVisible({
    timeout: 30_000,
  });
  await shot("B3c-ceremony-review");
  await cdlg.getByRole("button", { name: "Continue to Approve" }).click();
  await expect(cdlg.getByText("Ready to approve")).toBeVisible();
  await cdlg.getByRole("button", { name: /Approve & register/ }).click();
  await expect(page.getByText(/Approved & registered/)).toBeVisible({ timeout: 90_000 });
  await shot("B3d-ceremony-approve");

  await page.getByRole("button", { name: "Deploy", exact: true }).click();
  const preflight = page.getByRole("dialog").filter({ hasText: "Deploy preflight" });
  await expect(preflight).toBeVisible();
  await expect(preflight.locator("li").first()).toBeVisible({ timeout: 30_000 });
  await shot("B4-preflight");
  await preflight.getByRole("button", { name: "Deploy" }).click();
  await expect(page.getByText("Deployed — the flow is built stopped")).toBeVisible({ timeout: 240_000 });
  await shot("B4b-deployed");

  await page.getByRole("button", { name: "More" }).click();
  await page.getByRole("menuitem", { name: "Enable" }).click();
  await page.keyboard.press("Escape");
  await page.getByRole("button", { name: "Start", exact: true }).click();
  await expect(page.getByText("Flow started")).toBeVisible({ timeout: 120_000 });
  await shot("B4c-started");

  const doc = await flowDoc(icebergFlowId);
  const kcBlock = (doc.blocks as { id: string; adapter: string }[]).find((b) => b.adapter === "kafka_kc");
  expect(kcBlock, "kafka_kc block present").toBeTruthy();
  const connectorName = `${icebergTopic}__iceberg`;

  let status: Record<string, any> | null = null;
  const deadline = Date.now() + 180_000;
  while (Date.now() < deadline) {
    const r = await fetch(`${KAFKA_CONNECT}/connectors/${encodeURIComponent(connectorName)}/status`);
    const text = await r.text();
    if (r.ok) {
      status = JSON.parse(text) as Record<string, any>;
      const connState = status?.connector?.state;
      const taskStates = ((status?.tasks ?? []) as { state: string }[]).map((t) => t.state);
      if (connState === "RUNNING" && taskStates.length > 0 && taskStates.every((s) => s === "RUNNING")) break;
    }
    await sleep(10_000);
  }
  saveJson("B5-connector-status.json", { connectorName, status });
  expect(status, `connector ${connectorName} status readable`).toBeTruthy();
  expect(status!.connector?.state).toBe("RUNNING");
  expect(((status!.tasks ?? []) as { state: string }[]).every((t) => t.state === "RUNNING")).toBe(true);

  let count = -1;
  const trinoDeadline = Date.now() + 420_000;
  let countResult: Awaited<ReturnType<typeof trinoQuery>> | null = null;
  const icebergTable = await findIcebergTable(tokenize(ICEBERG_FLOW));
  while (Date.now() < trinoDeadline) {
    const r = await trinoQuery(`SELECT count(*) FROM ${icebergTable.catalog}.${icebergTable.schema}.${icebergTable.table}`);
    countResult = r;
    if (r.ok && r.rows.length > 0) {
      count = Number((r.rows[0] as unknown[])[0]);
      if (count >= 5) break;
    }
    await sleep(15_000);
  }
  const sampleRows = await trinoQuery(
    `SELECT * FROM ${icebergTable.catalog}.${icebergTable.schema}.${icebergTable.table} LIMIT 5`,
  );
  saveJson("B5-trino-results.json", {
    icebergFlowId,
    icebergTopic,
    connectorName,
    icebergTable,
    finalCount: { state: countResult?.state, rows: countResult?.rows, error: countResult?.error, count },
    sample: { state: sampleRows.state, columns: sampleRows.columns, rows: sampleRows.rows, error: sampleRows.error },
  });
  expect(count, "rows landed in Trino").toBeGreaterThanOrEqual(5);

  await page.goto(`/flow-builder/${icebergFlowId}`);
  await page.getByRole("button", { name: "Stop", exact: true }).click();
  await expect(page.getByText("Stopped — queues retained")).toBeVisible({ timeout: 120_000 });
  await shot("B5b-stopped-still-deployed");
});
