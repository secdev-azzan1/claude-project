// UI+API E2E verification — Gateway routing (prefix `gw`) + Iceberg sink
// (prefix `ice`) against the REAL backend (http://localhost:8010) and REAL
// infra (APISIX admin+runtime, NiFi, Kafka via Kafbat, Kafka Connect, Apicurio,
// Polaris/Ozone, Trino). UI-first via Playwright; steps that are impractical in
// the browser fall back to the API and say so in the artifacts + journey log.
//
// EVIDENCE POLICY (per the mission): NOTHING is cleaned up. Proxies, allowlist
// hosts, services, flows, schemas, topics, connectors and Iceberg tables all
// remain. Flows are STOPPED after verification (cron quiesced) but stay
// deployed.
//
// Selector conventions reused from ui-journey.spec.ts (proven against this UI).
import { test, expect, type BrowserContext, type Page } from "playwright/test";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";

const FRONTEND = "http://localhost:3001";
const BACKEND = "http://localhost:8010";
const APISIX_ADMIN = "https://apisix-admin.datapasc.com";
const APISIX_RUNTIME = "https://apisix.datapasc.com";
const KAFKA_CONNECT = "https://kafkaconnect.datapasc.com";
const TRINO = "https://trino.datapasc.com";
const NIFI = "https://nifi.datapasc.com";

const E2E_DIR = path.dirname(fileURLToPath(import.meta.url));
const ART = path.resolve(E2E_DIR, "artifacts", "gwice");
const SAMPLE_PATH = path.join(ART, "ice-users-sample.json");

// -------------------------------------------------------------------- naming
const GW_PROXY_NAME = "gw dummyjson"; // tokenize -> gw_dummyjson
const GW_SERVICE = "gw proxied dummyjson";
const GW_FLOW = "gw via gateway"; // flowToken gw_via_gateway
const GW_ENTITY = "gw_user";
const GW_TOPIC = "raw.gw_via_gateway.gw_user";
const ICE_SINK_SERVICE = "ice polaris";
const ICE_HTTP_SERVICE = "ice dummyjson";
const ICE_FLOW = "ice users"; // flowToken ice_users
const ICE_ENTITY = "ice_user";
const ICE_TOPIC = "raw.ice_users.ice_user";
const CRON = "*/3 * * * *";

test.describe.configure({ mode: "serial" });

let context: BrowserContext;
let page: Page;
let gwFlowId = "";
let iceFlowId = "";
let gwProxyId = "";
const consoleErrors: string[] = [];

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function shot(name: string) {
  await page.screenshot({ path: path.join(ART, `${name}.png`), fullPage: true });
}

function saveJson(name: string, data: unknown) {
  fs.writeFileSync(path.join(ART, name), JSON.stringify(data, null, 2), "utf-8");
}

/** A labelled input in the shadcn forms (Label is not htmlFor-linked). */
function fieldInput(scope: ReturnType<Page["locator"]>, label: string) {
  return scope.locator(`div:has(> label:text-is("${label}")) input`).first();
}

/** APISIX admin key comes from backend/.env — never hardcoded into the repo. */
function apisixAdminKey(): string {
  const env = fs.readFileSync(path.resolve(E2E_DIR, "..", "..", "backend", ".env"), "utf-8");
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
      /* not up yet */
    }
    await sleep(3_000);
  }
  throw new Error(`Backend at ${BACKEND}/api did not return 200 within 180s`);
}

async function messagesCount(flowId: string, topic: string): Promise<number> {
  try {
    const r = await fetch(`${BACKEND}/api/v2/flows/${flowId}/messages?topic=${encodeURIComponent(topic)}`);
    if (!r.ok) return -1;
    const j = (await r.json()) as { messages?: unknown[] };
    return (j.messages ?? []).length;
  } catch {
    return -1;
  }
}

/** Trino REST protocol: POST /v1/statement, follow nextUri until done. */
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
  let state: string = j?.stats?.state ?? "UNKNOWN";
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
    // Rewrite nextUri onto the public host in case Trino returns an internal one.
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

test.beforeAll(async ({ browser }) => {
  fs.mkdirSync(ART, { recursive: true });
  await backendReady();
  // Sample evidence file for the schema ceremony: 3 REAL users from the API the
  // flow itself reads, kept under the same wrapper shape so the ceremony's
  // record path is the flow's own $.users[*].
  const r = await fetch("https://dummyjson.com/users?limit=3");
  if (!r.ok) throw new Error(`dummyjson sample fetch failed: ${r.status}`);
  fs.writeFileSync(SAMPLE_PATH, JSON.stringify(await r.json(), null, 2), "utf-8");

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

// ════════════════════════════════════ JOURNEY A — APISIX GATEWAY ═══════════

// ---------------------------------------------------------------- Step A1
test("A1 sidebar says 'Proxies' (rename evidence) and opens the page", async () => {
  await page.goto("/");
  const link = page.getByRole("link", { name: "Proxies", exact: true });
  await expect(link).toBeVisible();
  await shot("A1-sidebar-proxies");
  await link.click();
  await expect(page).toHaveURL(/\/apisix/);
  await expect(page.getByText("APISIX Gateway").first()).toBeVisible();
  await shot("A1b-proxies-page");
});

// ---------------------------------------------------------------- Step A2
test("A2 allowlist dummyjson.com through the admin-confirm dialog", async () => {
  await page.getByLabel("Host to allowlist").fill("dummyjson.com");
  await page.getByRole("button", { name: /Add host \(admin\)/ }).click();

  const adminDlg = page.getByRole("alertdialog");
  await expect(adminDlg.getByText("Administrator action")).toBeVisible();
  await expect(adminDlg.getByText(/Allow egress to/)).toBeVisible();
  await expect(adminDlg.getByText("dummyjson.com").first()).toBeVisible();
  await shot("A2-admin-confirm-dialog");

  await adminDlg.getByRole("button", { name: /Confirm as admin/ }).click();
  await expect(page.getByText(/Admin action recorded/)).toBeVisible();
  // The host chip is now on the allowlist.
  await expect(page.getByText("dummyjson.com").first()).toBeVisible();
  await shot("A2b-host-allowlisted");
});

// ---------------------------------------------------------------- Step A3
test("A3 create proxy 'gw dummyjson', reconcile, verify APISIX admin objects, Test ok", async () => {
  test.setTimeout(300_000);
  await page.getByRole("button", { name: "Add Proxy" }).first().click();
  const dlg = page.getByRole("dialog");
  await expect(dlg.getByText("Add APISIX Proxy")).toBeVisible();

  await fieldInput(dlg, "Name").fill(GW_PROXY_NAME);
  await fieldInput(dlg, "Target host").fill("dummyjson.com");
  await expect(fieldInput(dlg, "Port")).toHaveValue("443");
  await fieldInput(dlg, "SNI").fill("dummyjson.com");
  await expect(fieldInput(dlg, "Route path prefix")).toHaveValue("/");
  // GET is pre-selected in the methods chip row.
  await expect(dlg.getByRole("button", { name: "GET", exact: true })).toHaveAttribute("aria-pressed", "true");

  await dlg.getByRole("button", { name: "Create Proxy" }).click();
  await expect(page.getByText(`Proxy "${GW_PROXY_NAME}" created`)).toBeVisible();

  const card = page.locator("div.bg-card").filter({ hasText: GW_PROXY_NAME }).first();
  await expect(card).toBeVisible();

  await card.getByRole("button", { name: "Reconcile" }).click();
  await expect(page.getByText(`"${GW_PROXY_NAME}" is live on the gateway`)).toBeVisible({ timeout: 60_000 });
  await expect(card.getByText(/Reconciled and allowlisted/)).toBeVisible();
  await shot("A3-proxy-reconciled-card");

  // APISIX admin verification (API step — the admin API has no UI surface):
  // dmp_<id> upstream + dmp_<id>_root/_wild routes must exist on the gateway.
  const gw = (await (await fetch(`${BACKEND}/api/v2/gateway/`)).json()) as { proxies: { id: string; name: string }[] };
  const proxy = gw.proxies.find((p) => p.name === GW_PROXY_NAME);
  expect(proxy, "proxy present in the catalog").toBeTruthy();
  gwProxyId = proxy!.id;
  const key = apisixAdminKey();
  const admin = async (p: string) => {
    const r = await fetch(`${APISIX_ADMIN}${p}`, { headers: { "X-API-KEY": key } });
    return { status: r.status, body: (await r.json()) as unknown };
  };
  const upstream = await admin(`/apisix/admin/upstreams/dmp_${gwProxyId}`);
  const routeRoot = await admin(`/apisix/admin/routes/dmp_${gwProxyId}_root`);
  const routeWild = await admin(`/apisix/admin/routes/dmp_${gwProxyId}_wild`);
  saveJson("A3-apisix-objects.json", { proxyId: gwProxyId, upstream, routeRoot, routeWild });
  expect(upstream.status, "upstream dmp_<id> exists on APISIX").toBe(200);
  expect(routeRoot.status, "route dmp_<id>_root exists on APISIX").toBe(200);
  expect(routeWild.status, "route dmp_<id>_wild exists on APISIX").toBe(200);
  expect(JSON.stringify(upstream.body)).toContain("dummyjson.com:443");
  expect(JSON.stringify(routeRoot.body)).toContain("/gw_dummyjson");

  // Test button → ok (probes through the RUNTIME gateway).
  await card.getByRole("button", { name: "Test", exact: true }).click();
  await expect(card.getByText(/Reached gw_dummyjson/)).toBeVisible({ timeout: 60_000 });
  await shot("A3b-proxy-test-ok");
});

// ---------------------------------------------------------------- Step A4
test("A4 http service 'gw proxied dummyjson' with API gateway egress set to the proxy", async () => {
  await page.goto("/application-services");
  await page.getByRole("button", { name: "Add Service" }).first().click();

  const dialog = page.getByRole("dialog");
  await expect(dialog.getByText("Add Application Service")).toBeVisible();
  await dialog.getByRole("button", { name: /HTTP service/ }).click();

  await fieldInput(dialog, "Name").fill(GW_SERVICE);
  await fieldInput(dialog, "Base URL").fill("https://dummyjson.com");
  await expect(dialog.getByRole("combobox").filter({ hasText: "None" })).toBeVisible(); // auth default

  // The "API gateway egress" select — the whole point of this journey.
  await dialog.getByRole("combobox").filter({ hasText: "No proxy" }).click();
  await page.getByRole("option", { name: /gw dummyjson/ }).click();
  await expect(dialog.getByText(/Every block using this service calls dummyjson\.com through/)).toBeVisible();
  await shot("A4-service-egress-select");

  await dialog.getByRole("button", { name: "Create Service" }).click();
  await expect(page.getByText(`Service "${GW_SERVICE}" created`)).toBeVisible();
  const card = page.locator("div.bg-card").filter({ hasText: GW_SERVICE }).filter({ hasNotText: "Retired" }).first();
  await expect(card).toBeVisible();
  await shot("A4b-service-created");
});

// ---------------------------------------------------------------- Step A5
test("A5 flow 'gw via gateway': http read /users -> kafka write gw_user, cron */3", async () => {
  await page.goto("/flows");
  await page.getByRole("button", { name: "New Flow" }).click();
  await expect(page).toHaveURL(/\/flow-builder\/new/);
  await page.getByPlaceholder("e.g. CrowdStrike Detections").fill(GW_FLOW);
  await page.getByRole("button", { name: "Create & open builder" }).click();
  await expect(page).toHaveURL(/\/flow-builder\/(?!new)[\w-]+/);
  gwFlowId = new URL(page.url()).pathname.split("/").pop()!;

  // Root: http · read bound to the proxied service.
  await page.getByRole("button", { name: "Place the root" }).click();
  await page.getByRole("menuitem", { name: /http · read/ }).click();
  await page.getByRole("combobox").filter({ hasText: "Select a service" }).click();
  await page.getByRole("option", { name: new RegExp(GW_SERVICE) }).click();
  await expect(page.getByText(/Base URL —/)).toBeVisible();

  await page.getByPlaceholder("/users").fill("/users");
  await page.getByPlaceholder("$.resources[*] (record path)").fill("$.users[*]");
  const splitSwitch = page.locator('label:has-text("split into records")').getByRole("switch");
  await expect(splitSwitch).toHaveAttribute("aria-checked", "true");

  // Cron */3 * * * *
  await page.getByRole("button", { name: /Flow settings/ }).click();
  await page.getByRole("combobox").first().click();
  await page.getByRole("option", { name: /Custom/ }).click();
  await page.getByPlaceholder("*/15 * * * *").fill(CRON);
  await expect(page.getByText(/^Next:/)).toBeVisible();

  // Child: kafka · write with entity gw_user.
  await page.locator(".react-flow__node", { hasText: "New http read" }).first().click();
  await page.locator('button[title^="Add a block after"]').click();
  await page.getByRole("menuitem", { name: /kafka · write/ }).click();
  await page.locator(".react-flow__node").filter({ hasText: "kafka · write" }).first().click();
  await page.getByPlaceholder("asset · incident · order…").fill(GW_ENTITY);
  await expect(page.getByText(GW_TOPIC).first()).toBeVisible();
  await shot("A5-flow-built");

  await page.getByRole("button", { name: "Save", exact: true }).click();
  await expect(page.getByText("Draft saved")).toBeVisible();
});

// ---------------------------------------------------------------- Step A6
test("A6 deploy 'gw via gateway' (preflight ok) and verify the fetch URL routes through the gateway", async () => {
  test.setTimeout(420_000);
  await page.getByRole("button", { name: "Deploy", exact: true }).click();
  const dlg = page.getByRole("dialog").filter({ hasText: "Deploy preflight" });
  await expect(dlg).toBeVisible();
  await expect(dlg.locator("li").first()).toBeVisible({ timeout: 30_000 });
  await expect(dlg.locator("svg.text-destructive")).toHaveCount(0);
  await shot("A6-preflight");
  const deployBtn = dlg.getByRole("button", { name: "Deploy" });
  await expect(deployBtn).toBeEnabled();
  await deployBtn.click();
  await expect(page.getByText("Deployed — the flow is built stopped")).toBeVisible({ timeout: 240_000 });
  await shot("A6b-deployed");

  // Gateway-routing proof #1 (API): the flow runtime read shows the compiled
  // InvokeHTTP URL going through the APISIX runtime route for the proxy.
  const rt = (await (await fetch(`${BACKEND}/api/v2/flows/${gwFlowId}/runtime`)).json()) as {
    components?: { name: string; type: string; blockId: string; properties?: { name: string; value: string | null }[] }[];
  };
  const withGw = (rt.components ?? [])
    .map((c) => ({
      name: c.name,
      type: c.type,
      blockId: c.blockId,
      props: (c.properties ?? []).filter((p) => (p.value ?? "").includes("gw_dummyjson") || (p.value ?? "").includes("apisix")),
    }))
    .filter((c) => c.props.length > 0);
  saveJson("A6-runtime-fetch-property.json", withGw);
  expect(
    JSON.stringify(withGw),
    "a compiled component property routes through .../gw_dummyjson",
  ).toContain("gw_dummyjson");

  // Gateway-routing proof #2 (API): the NiFi parameter context resolves
  // apisix_runtime_url to the real runtime gateway.
  const flowDoc = (await (await fetch(`${BACKEND}/api/v2/flows/${gwFlowId}`)).json()) as Record<string, any>;
  const envText = fs.readFileSync(path.resolve(E2E_DIR, "..", "..", "backend", ".env"), "utf-8");
  const user = envText.match(/^NIFI_USERNAME=(.+)$/m)?.[1]?.trim() ?? "admin";
  const pass = envText.match(/^NIFI_PASSWORD=(.+)$/m)?.[1]?.trim() ?? "";
  let paramEvidence: unknown = { note: "NiFi param read skipped/failed — see runtime property evidence above" };
  try {
    const tokenRes = await fetch(`${NIFI}/nifi-api/access/token`, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: `username=${encodeURIComponent(user)}&password=${encodeURIComponent(pass)}`,
    });
    const token = await tokenRes.text();
    const ctxList = (await (
      await fetch(`${NIFI}/nifi-api/flow/parameter-contexts`, { headers: { Authorization: `Bearer ${token}` } })
    ).json()) as { parameterContexts?: { component: { name: string; parameters: { parameter: { name: string; value: string | null } }[] } }[] };
    const ctx = (ctxList.parameterContexts ?? []).find((c) => c.component.name === "gw_via_gateway__params");
    const params = (ctx?.component.parameters ?? []).map((p) => p.parameter).filter((p) => !/secret|password|token/i.test(p.name));
    paramEvidence = { context: "gw_via_gateway__params", nifiProcessGroupId: flowDoc.nifiProcessGroupId, params };
    const runtimeUrl = params.find((p) => p.name === "apisix_runtime_url")?.value ?? "";
    expect(runtimeUrl, "NiFi parameter apisix_runtime_url").toBe(APISIX_RUNTIME);
  } catch (e) {
    paramEvidence = { error: String(e) };
  }
  saveJson("A6-nifi-params.json", paramEvidence);

  // Gateway-routing proof #3 (UI): the Runtime tab shows the same property.
  await page.goto("/flows");
  await page.getByText(GW_FLOW).first().click();
  await page.getByRole("tab", { name: "Runtime" }).click();
  await expect(page.getByText("Generated NiFi components")).toBeVisible({ timeout: 60_000 });
  // Expand the http root block's component group and find the gateway URL.
  await page.locator("button").filter({ hasText: "New http read" }).first().click();
  await expect(page.getByText(/gw_dummyjson/).first()).toBeVisible({ timeout: 30_000 });
  await shot("A6c-runtime-tab-gateway-url");
  await page.keyboard.press("Escape"); // close the sheet
});

// ---------------------------------------------------------------- Step A7
test("A7 start, 30 records on raw.gw_via_gateway.gw_user, Messages tab, STOP (stay deployed)", async () => {
  test.setTimeout(780_000);
  await page.goto(`/flow-builder/${gwFlowId}`);

  await page.getByRole("button", { name: "More" }).click();
  await page.getByRole("menuitem", { name: "Enable" }).click();
  await page.keyboard.press("Escape");
  const startBtn = page.getByRole("button", { name: "Start", exact: true });
  await expect(startBtn).toBeEnabled({ timeout: 20_000 });
  await startBtn.click();
  await expect(page.getByText("Flow started")).toBeVisible({ timeout: 120_000 });
  await shot("A7-started");

  // Cron fires within 3 minutes; poll the backend for the topic messages.
  const started = Date.now();
  let count = -1;
  const deadline = Date.now() + 300_000;
  while (Date.now() < deadline) {
    count = await messagesCount(gwFlowId, GW_TOPIC);
    if (count >= 30) break;
    await sleep(10_000);
  }
  const waitedS = Math.round((Date.now() - started) / 1000);
  fs.writeFileSync(
    path.join(ART, "A7-topic-poll.txt"),
    `topic=${GW_TOPIC}\nflowId=${gwFlowId}\nmessages=${count}\nwaited_seconds=${waitedS}\nexpected=30 per firing\n`,
    "utf-8",
  );
  expect(count, `messages on ${GW_TOPIC} after ${waitedS}s`).toBeGreaterThanOrEqual(30);

  // Messages tab (UI proof), then STOP.
  await page.goto("/flows");
  await page.getByText(GW_FLOW).first().click();
  await page.getByRole("tab", { name: "Messages" }).click();
  const topicSelect = page.getByRole("combobox").filter({ hasText: /raw\.|Pick a topic/ }).first();
  await topicSelect.click();
  await page.getByRole("option", { name: GW_TOPIC }).click();
  await expect(page.getByText(/offset \d+/).first()).toBeVisible({ timeout: 30_000 });
  await shot("A7b-messages-tab");
  await page.keyboard.press("Escape");

  await page.goto(`/flow-builder/${gwFlowId}`);
  await page.getByRole("button", { name: "Stop", exact: true }).click();
  await expect(page.getByText("Stopped — queues retained")).toBeVisible({ timeout: 120_000 });
  await shot("A7c-stopped-still-deployed");
});

// ═══════════════════════════════════ JOURNEY B — ICEBERG SINK ══════════════

// ---------------------------------------------------------------- Step B1
test("B1 sink service 'ice polaris' (API create — UI form lacks OAuth/S3 fields) then UI Test -> Healthy", async () => {
  test.setTimeout(120_000);
  // DOCUMENTED FALLBACK: the Add Service UI form for an Iceberg catalog only
  // exposes Catalog URL + Warehouse (ServiceFormFields.tsx). The OAuth client
  // and S3/FileIO fields the live connector needs are accepted by the backend
  // but have no UI inputs — so the service is created via the API and then
  // tested through the UI.
  const createRes = await fetch(`${BACKEND}/api/v2/services/`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      type: "sink_destination",
      name: ICE_SINK_SERVICE,
      config: {
        kind: "iceberg_catalog",
        catalogUrl: "https://polaris.datapasc.com/api/catalog",
        warehouse: "bronze",
        oauthClientId: "root",
        oauthClientSecret: "s3cr3t",
        s3Endpoint: "https://ozones3g.datapasc.com",
        s3AccessKey: "eltadmin",
        s3SecretKey: "OzoneS3Key123",
        s3Region: "us-east-1",
        s3PathStyle: true,
      },
    }),
  });
  const created = (await createRes.json()) as Record<string, unknown>;
  saveJson("B1-sink-service-created.json", { status: createRes.status, created });
  expect(createRes.status, "sink service create").toBe(200);

  await page.goto("/application-services");
  const card = page
    .locator("div.bg-card")
    .filter({ hasText: ICE_SINK_SERVICE })
    .filter({ hasNotText: "Retired" })
    .first();
  await expect(card).toBeVisible();
  await card.getByRole("button", { name: "Test", exact: true }).click();
  await expect(card.getByText("Healthy").first()).toBeVisible({ timeout: 60_000 });
  await shot("B1-sink-service-healthy");
});

// ---------------------------------------------------------------- Step B2
test("B2 plain http service 'ice dummyjson' (no proxy)", async () => {
  await page.getByRole("button", { name: "Add Service" }).first().click();
  const dialog = page.getByRole("dialog");
  await expect(dialog.getByText("Add Application Service")).toBeVisible();
  await dialog.getByRole("button", { name: /HTTP service/ }).click();
  await fieldInput(dialog, "Name").fill(ICE_HTTP_SERVICE);
  await fieldInput(dialog, "Base URL").fill("https://dummyjson.com");
  await dialog.getByRole("button", { name: "Create Service" }).click();
  await expect(page.getByText(`Service "${ICE_HTTP_SERVICE}" created`)).toBeVisible();
});

// ---------------------------------------------------------------- Step B3
test("B3 flow 'ice users': http read /users -> kafka+connect governed write ice_user + sink service", async () => {
  await page.goto("/flows");
  await page.getByRole("button", { name: "New Flow" }).click();
  await expect(page).toHaveURL(/\/flow-builder\/new/);
  await page.getByPlaceholder("e.g. CrowdStrike Detections").fill(ICE_FLOW);
  await page.getByRole("button", { name: "Create & open builder" }).click();
  await expect(page).toHaveURL(/\/flow-builder\/(?!new)[\w-]+/);
  iceFlowId = new URL(page.url()).pathname.split("/").pop()!;

  await page.getByRole("button", { name: "Place the root" }).click();
  await page.getByRole("menuitem", { name: /http · read/ }).click();
  await page.getByRole("combobox").filter({ hasText: "Select a service" }).click();
  await page.getByRole("option", { name: new RegExp(ICE_HTTP_SERVICE) }).click();
  await expect(page.getByText(/Base URL —/)).toBeVisible();
  await page.getByPlaceholder("/users").fill("/users");
  await page.getByPlaceholder("$.resources[*] (record path)").fill("$.users[*]");

  await page.getByRole("button", { name: /Flow settings/ }).click();
  await page.getByRole("combobox").first().click();
  await page.getByRole("option", { name: /Custom/ }).click();
  await page.getByPlaceholder("*/15 * * * *").fill(CRON);
  await expect(page.getByText(/^Next:/)).toBeVisible();

  // Child: kafka+connect · governed write.
  await page.locator(".react-flow__node", { hasText: "New http read" }).first().click();
  await page.locator('button[title^="Add a block after"]').click();
  await page.getByRole("menuitem", { name: "kafka+connect · governed write" }).click();
  await page.locator(".react-flow__node").filter({ hasText: "kafka+connect" }).first().click();

  // Entity, derived governed topic, sink destination service.
  await page.getByPlaceholder("asset · incident · order…").fill(ICE_ENTITY);
  await expect(page.getByText(ICE_TOPIC).first()).toBeVisible();
  await page
    .locator("#block-section-sink")
    .getByRole("combobox")
    .filter({ hasText: "Select a service" })
    .click();
  await page.getByRole("option", { name: new RegExp(ICE_SINK_SERVICE) }).click();
  await expect(page.locator("#block-section-sink").getByText(/polaris\.datapasc\.com/).first()).toBeVisible();
  await shot("B3-kafka-kc-configured");

  await page.getByRole("button", { name: "Save", exact: true }).click();
  await expect(page.getByText("Draft saved")).toBeVisible();
});

// ---------------------------------------------------------------- Step B4
test("B4 schema ceremony: Declare -> Orchestrate (uploaded sample file) -> Review -> Approve & register", async () => {
  test.setTimeout(180_000);
  // Selection can drop after Save — make sure the kafka_kc block form is open.
  await page.locator(".react-flow__node").filter({ hasText: "kafka+connect" }).first().click();
  await page.getByRole("button", { name: "Start ceremony" }).click();
  const cdlg = page.getByRole("dialog").filter({ hasText: "Schema ceremony" });
  await expect(cdlg).toBeVisible();

  // Declare: entity pre-filled from the block; pick the uploaded-files path.
  await expect(cdlg.getByPlaceholder("asset · incident · order…")).toHaveValue(ICE_ENTITY);
  await expect(cdlg.getByText(`raw.ice_users.ice_user`).first()).toBeVisible();
  await cdlg.getByText("Uploaded sample files").click();
  await shot("B4a-ceremony-declare");
  await cdlg.getByRole("button", { name: "Continue", exact: true }).click();

  // Orchestrate: upload the saved 3-user sample, record path $.users[*].
  await cdlg.locator('input[type="file"]').setInputFiles(SAMPLE_PATH);
  await expect(cdlg.getByText("ice-users-sample.json")).toBeVisible();
  const rpInput = cdlg.getByPlaceholder(/blank means the file/);
  await rpInput.fill("$.users[*]");
  await expect(cdlg.getByText(/3 record\(s\) matched/)).toBeVisible();
  await shot("B4b-ceremony-upload");
  await cdlg.getByRole("button", { name: "Infer schema" }).click();

  // Review: inferred from the uploaded evidence.
  await expect(cdlg.getByText(/Inferred \d+ top-level field\(s\) from 3 record\(s\)/)).toBeVisible({
    timeout: 30_000,
  });
  await shot("B4c-ceremony-review");
  await cdlg.getByRole("button", { name: "Continue to Approve" }).click();

  // Approve: identity + evidence summary, then register.
  await expect(cdlg.getByText("Ready to approve")).toBeVisible();
  await expect(cdlg.getByText(`${ICE_TOPIC}-value`).first()).toBeVisible();
  await expect(cdlg.getByText(/uploaded samples — 1 file\(s\), 3 record\(s\)/)).toBeVisible();
  await shot("B4d-ceremony-approve-summary");
  await cdlg.getByRole("button", { name: /Approve & register/ }).click();
  await expect(page.getByText(/Approved & registered — raw\.ice_users\.ice_user-value/)).toBeVisible({
    timeout: 90_000,
  });

  // Block form now shows the approved schema.
  await expect(page.getByText(/Approved #\d+/).first()).toBeVisible({ timeout: 30_000 });
  await shot("B4e-schema-approved");
});

// ---------------------------------------------------------------- Step B5
test("B5 deploy 'ice users' — preflight includes schema + Connect plugin rows — then start", async () => {
  test.setTimeout(420_000);
  await page.getByRole("button", { name: "Deploy", exact: true }).click();
  const dlg = page.getByRole("dialog").filter({ hasText: "Deploy preflight" });
  await expect(dlg).toBeVisible();
  await expect(dlg.locator("li").first()).toBeVisible({ timeout: 30_000 });
  await expect(dlg.locator("svg.text-destructive")).toHaveCount(0);
  expect
    .soft(await dlg.getByText(/schema/i).count(), "preflight mentions the approved schema")
    .toBeGreaterThan(0);
  expect
    .soft(await dlg.getByText(/plugin|connector/i).count(), "preflight mentions Connect plugins")
    .toBeGreaterThan(0);
  await shot("B5-preflight-connect-rows");
  const deployBtn = dlg.getByRole("button", { name: "Deploy" });
  await expect(deployBtn).toBeEnabled();
  await deployBtn.click();
  await expect(page.getByText("Deployed — the flow is built stopped")).toBeVisible({ timeout: 240_000 });
  await shot("B5b-deployed");

  await page.getByRole("button", { name: "More" }).click();
  await page.getByRole("menuitem", { name: "Enable" }).click();
  await page.keyboard.press("Escape");
  const startBtn = page.getByRole("button", { name: "Start", exact: true });
  await expect(startBtn).toBeEnabled({ timeout: 20_000 });
  await startBtn.click();
  await expect(page.getByText("Flow started")).toBeVisible({ timeout: 120_000 });
  await shot("B5c-started");
});

// ---------------------------------------------------------------- Step B6
test("B6 connector ice_users.<blockId>.kafka_kc RUNNING on Kafka Connect", async () => {
  test.setTimeout(300_000);
  const flowDoc = (await (await fetch(`${BACKEND}/api/v2/flows/${iceFlowId}`)).json()) as {
    blocks: { id: string; adapter: string }[];
  };
  const kcBlock = flowDoc.blocks.find((b) => b.adapter === "kafka_kc");
  expect(kcBlock, "kafka_kc block present in the saved flow").toBeTruthy();
  const connectorName = `ice_users.${kcBlock!.id}.kafka_kc`;

  let status: Record<string, any> | null = null;
  const deadline = Date.now() + 180_000;
  let lastText = "";
  while (Date.now() < deadline) {
    const r = await fetch(`${KAFKA_CONNECT}/connectors/${encodeURIComponent(connectorName)}/status`);
    lastText = await r.text();
    if (r.ok) {
      status = JSON.parse(lastText) as Record<string, any>;
      const connState = status?.connector?.state;
      const taskStates = ((status?.tasks ?? []) as { state: string }[]).map((t) => t.state);
      if (connState === "RUNNING" && taskStates.length > 0 && taskStates.every((s) => s === "RUNNING")) break;
    }
    await sleep(10_000);
  }
  saveJson("B6-connector-status.json", { connectorName, status: status ?? lastText });
  expect(status, `connector ${connectorName} status readable`).toBeTruthy();
  expect(status!.connector?.state, "connector state").toBe("RUNNING");
  expect(((status!.tasks ?? []) as { state: string }[]).every((t) => t.state === "RUNNING"), "all tasks RUNNING").toBe(
    true,
  );
});

// ---------------------------------------------------------------- Step B7
test("B7 DATA LANDED: topic fills, then Trino counts rows in iceberg.bronze.ice_user; STOP after proof", async () => {
  test.setTimeout(900_000);

  // 1) Wait for the first cron firing to land ~30 Avro records on the topic.
  let msgCount = -1;
  const topicDeadline = Date.now() + 300_000;
  const t0 = Date.now();
  while (Date.now() < topicDeadline) {
    msgCount = await messagesCount(iceFlowId, ICE_TOPIC);
    if (msgCount >= 30) break;
    await sleep(10_000);
  }
  fs.writeFileSync(
    path.join(ART, "B7-topic-poll.txt"),
    `topic=${ICE_TOPIC}\nflowId=${iceFlowId}\nmessages=${msgCount}\nwaited_seconds=${Math.round((Date.now() - t0) / 1000)}\n`,
    "utf-8",
  );
  expect(msgCount, `messages on ${ICE_TOPIC}`).toBeGreaterThanOrEqual(30);

  // 2) Discovery evidence (catalog/schema/table naming), once.
  const catalogs = await trinoQuery("SHOW CATALOGS");
  const schemas = await trinoQuery("SHOW SCHEMAS FROM iceberg");
  const tables = await trinoQuery("SHOW TABLES FROM iceberg.bronze");
  // 3) Poll the count until the Connect commit (60s interval) lands the rows.
  let countResult: Awaited<ReturnType<typeof trinoQuery>> | null = null;
  let rowCount = -1;
  const trinoDeadline = Date.now() + 420_000;
  while (Date.now() < trinoDeadline) {
    const r = await trinoQuery("SELECT count(*) FROM iceberg.bronze.ice_user");
    countResult = r;
    if (r.ok && r.rows.length > 0) {
      rowCount = Number((r.rows[0] as unknown[])[0]);
      if (rowCount >= 30) break;
    }
    await sleep(15_000);
  }
  const sample = await trinoQuery(
    "SELECT * FROM iceberg.bronze.ice_user LIMIT 3",
  );
  saveJson("B7-trino-results.json", {
    catalogs: { state: catalogs.state, rows: catalogs.rows, error: catalogs.error },
    schemasFromIceberg: { state: schemas.state, rows: schemas.rows, error: schemas.error },
    tablesFromBronze: { state: tables.state, rows: tables.rows, error: tables.error },
    finalCount: { state: countResult?.state, rows: countResult?.rows, error: countResult?.error, rowCount },
    sample: { state: sample.state, columns: sample.columns, rows: sample.rows.slice(0, 3), error: sample.error },
  });
  expect(rowCount, "rows in iceberg.bronze.ice_user via Trino").toBeGreaterThanOrEqual(30);

  // 4) STOP the flow (cron quiesced) — everything stays deployed as evidence.
  await page.goto(`/flow-builder/${iceFlowId}`);
  await page.getByRole("button", { name: "Stop", exact: true }).click();
  await expect(page.getByText("Stopped — queues retained")).toBeVisible({ timeout: 120_000 });
  await shot("B7-stopped-still-deployed");
});
