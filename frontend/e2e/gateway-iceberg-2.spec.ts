// Continuation of gateway-iceberg.spec.ts (phases are run selectively with
// --grep). Run 1 completed A1–A5 plus A6's deploy and both API-level gateway
// proofs; it stopped at the flow-detail sheet (the sheet opens via the
// "Overview" eye button, not the row text — fixed here). This file finishes
// Journey A and runs all of Journey B. State is re-discovered via the API by
// resource NAME so each phase can run in its own browser session.
//
// EVIDENCE POLICY: nothing is deleted. Flows are stopped after verification
// but stay deployed; services/proxies/schemas/topics/connectors/tables remain.
import { test, expect, type BrowserContext, type Page } from "playwright/test";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";

const FRONTEND = "http://localhost:3001";
const BACKEND = "http://localhost:8010";
const KAFKA_CONNECT = "https://kafkaconnect.datapasc.com";
const TRINO = "https://trino.datapasc.com";

const E2E_DIR = path.dirname(fileURLToPath(import.meta.url));
const ART = path.resolve(E2E_DIR, "artifacts", "gwice");
const SAMPLE_PATH = path.join(ART, "ice-users-sample.json");

const GW_FLOW = "gw via gateway";
const GW_TOPIC = "raw.gw_via_gateway.gw_user";
const ICE_SINK_SERVICE = "ice polaris";
const ICE_HTTP_SERVICE = "ice dummyjson";
const ICE_FLOW = "ice users";
const ICE_ENTITY = "ice_user";
const ICE_TOPIC = "raw.ice_users.ice_user";
const CRON = "*/3 * * * *";

test.describe.configure({ mode: "serial" });

let context: BrowserContext;
let page: Page;
const consoleErrors: string[] = [];

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function shot(name: string) {
  await page.screenshot({ path: path.join(ART, `${name}.png`), fullPage: true });
}

function saveJson(name: string, data: unknown) {
  fs.writeFileSync(path.join(ART, name), JSON.stringify(data, null, 2), "utf-8");
}

function fieldInput(scope: ReturnType<Page["locator"]>, label: string) {
  return scope.locator(`div:has(> label:text-is("${label}")) input`).first();
}

async function flowIdByName(name: string): Promise<string> {
  const flows = (await (await fetch(`${BACKEND}/api/v2/flows/`)).json()) as { id: string; name: string }[];
  return flows.find((f) => f.name === name)?.id ?? "";
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

/** Open the flow detail sheet from /flows via the Overview (eye) button. */
async function openFlowSheet(flowName: string) {
  await page.goto("/flows");
  const row = page.getByRole("row").filter({ hasText: flowName }).first();
  await expect(row).toBeVisible();
  await row.getByRole("button", { name: "Overview" }).click();
  await expect(page.getByRole("tab", { name: "Runtime" })).toBeVisible({ timeout: 15_000 });
}

test.beforeAll(async ({ browser }) => {
  fs.mkdirSync(ART, { recursive: true });
  if (!fs.existsSync(SAMPLE_PATH)) {
    const r = await fetch("https://dummyjson.com/users?limit=3");
    if (!r.ok) throw new Error(`dummyjson sample fetch failed: ${r.status}`);
    fs.writeFileSync(SAMPLE_PATH, JSON.stringify(await r.json(), null, 2), "utf-8");
  }
  context = await browser.newContext({ baseURL: FRONTEND, viewport: { width: 1680, height: 1000 } });
  page = await context.newPage();
  page.on("console", (m) => {
    if (m.type() === "error") consoleErrors.push(m.text());
  });
  page.on("pageerror", (e) => consoleErrors.push(`pageerror: ${String(e)}`));
});

test.afterAll(async () => {
  if (consoleErrors.length > 0) {
    fs.appendFileSync(path.join(ART, "console-errors.txt"), consoleErrors.join("\n---\n") + "\n", "utf-8");
  }
  await context?.close();
});

test.afterEach(async ({}, testInfo) => {
  if (testInfo.status !== testInfo.expectedStatus && page) {
    const safe = testInfo.title.replace(/[^a-z0-9-]+/gi, "_").slice(0, 60);
    await page.screenshot({ path: path.join(ART, `FAIL-${safe}.png`), fullPage: true }).catch(() => undefined);
  }
});

// ---------------------------------------------------------------- Phase C
test("C1 gw flow Runtime tab shows the fetch URL routed through the gateway", async () => {
  await openFlowSheet(GW_FLOW);
  await page.getByRole("tab", { name: "Runtime" }).click();
  await expect(page.getByText("Generated NiFi components")).toBeVisible({ timeout: 60_000 });
  await page.locator("button").filter({ hasText: "New http read" }).first().click();
  await expect(page.getByText(/gw_dummyjson/).first()).toBeVisible({ timeout: 30_000 });
  await shot("A6c-runtime-tab-gateway-url");
  await page.keyboard.press("Escape");
});

test("C2 start gw flow, 30 records on the topic, Messages tab, STOP (stay deployed)", async () => {
  test.setTimeout(540_000);
  const gwFlowId = await flowIdByName(GW_FLOW);
  expect(gwFlowId, "gw flow exists").not.toBe("");
  await page.goto(`/flow-builder/${gwFlowId}`);

  await page.getByRole("button", { name: "More" }).click();
  await page.getByRole("menuitem", { name: "Enable" }).click();
  await page.keyboard.press("Escape");
  const startBtn = page.getByRole("button", { name: "Start", exact: true });
  await expect(startBtn).toBeEnabled({ timeout: 20_000 });
  await startBtn.click();
  await expect(page.getByText("Flow started")).toBeVisible({ timeout: 120_000 });
  await shot("A7-started");

  const started = Date.now();
  let count = -1;
  const deadline = Date.now() + 260_000;
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

  await openFlowSheet(GW_FLOW);
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

// ---------------------------------------------------------------- Phase D
test("D1 sink service 'ice polaris' (API create — UI form lacks OAuth/S3 fields) then UI Test -> Healthy", async () => {
  test.setTimeout(120_000);
  // DOCUMENTED FALLBACK: the Add Service UI form for an Iceberg catalog only
  // exposes Catalog URL + Warehouse. The OAuth client + S3/FileIO fields the
  // live connector needs are backend-accepted but have no UI inputs — created
  // via the API, then health-tested through the UI Test button.
  const existing = (await (await fetch(`${BACKEND}/api/v2/services/`)).json()) as {
    id: string;
    name: string;
    retired: boolean;
  }[];
  if (!existing.some((s) => s.name === ICE_SINK_SERVICE && !s.retired)) {
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
  }

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

test("D2 plain http service 'ice dummyjson' (no proxy)", async () => {
  await page.goto("/application-services");
  const already = await page
    .locator("div.bg-card")
    .filter({ hasText: ICE_HTTP_SERVICE })
    .filter({ hasNotText: "Retired" })
    .count();
  if (already > 0) return;
  await page.getByRole("button", { name: "Add Service" }).first().click();
  const dialog = page.getByRole("dialog");
  await expect(dialog.getByText("Add Application Service")).toBeVisible();
  await dialog.getByRole("button", { name: /HTTP service/ }).click();
  await fieldInput(dialog, "Name").fill(ICE_HTTP_SERVICE);
  await fieldInput(dialog, "Base URL").fill("https://dummyjson.com");
  await dialog.getByRole("button", { name: "Create Service" }).click();
  await expect(page.getByText(`Service "${ICE_HTTP_SERVICE}" created`)).toBeVisible();
});

test("D3 flow 'ice users': http read /users -> kafka+connect governed write ice_user + sink service", async () => {
  await page.goto("/flows");
  await page.getByRole("button", { name: "New Flow" }).click();
  await expect(page).toHaveURL(/\/flow-builder\/new/);
  await page.getByPlaceholder("e.g. CrowdStrike Detections").fill(ICE_FLOW);
  await page.getByRole("button", { name: "Create & open builder" }).click();
  await expect(page).toHaveURL(/\/flow-builder\/(?!new)[\w-]+/);

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

  await page.locator(".react-flow__node", { hasText: "New http read" }).first().click();
  await page.locator('button[title^="Add a block after"]').click();
  await page.getByRole("menuitem", { name: "kafka+connect · governed write" }).click();
  await page.locator(".react-flow__node").filter({ hasText: "kafka+connect" }).first().click();

  await page.getByPlaceholder("asset · incident · order…").fill(ICE_ENTITY);
  await expect(page.getByText(ICE_TOPIC).first()).toBeVisible();
  await page
    .locator("#block-section-sink")
    .getByRole("combobox")
    .filter({ hasText: "Select a service" })
    .click();
  await page.getByRole("option", { name: new RegExp(ICE_SINK_SERVICE) }).click();
  // The bound service shows in the picker; the service-derived catalog rows
  // (iceberg.catalog.uri/warehouse) render as locked disabled inputs, so they
  // are asserted via input values rather than text.
  await expect(
    page.locator("#block-section-sink").getByRole("combobox").filter({ hasText: ICE_SINK_SERVICE }),
  ).toBeVisible();
  await shot("B3-kafka-kc-configured");

  await page.getByRole("button", { name: "Save", exact: true }).click();
  await expect(page.getByText("Draft saved")).toBeVisible();
});

test("D4 schema ceremony: Declare -> Orchestrate (uploaded sample file) -> Review -> Approve & register", async () => {
  test.setTimeout(180_000);
  const iceFlowId = await flowIdByName(ICE_FLOW);
  expect(iceFlowId, "ice flow exists server-side").not.toBe("");
  await page.goto(`/flow-builder/${iceFlowId}`);
  await page.locator(".react-flow__node").filter({ hasText: "kafka+connect" }).first().click();
  await page.getByRole("button", { name: "Start ceremony" }).click();
  const cdlg = page.getByRole("dialog").filter({ hasText: "Schema ceremony" });
  await expect(cdlg).toBeVisible();

  await expect(cdlg.getByPlaceholder("asset · incident · order…")).toHaveValue(ICE_ENTITY);
  await expect(cdlg.getByText(`raw.ice_users.ice_user`).first()).toBeVisible();
  await cdlg.getByText("Uploaded sample files").click();
  await shot("B4a-ceremony-declare");
  await cdlg.getByRole("button", { name: "Continue", exact: true }).click();

  await cdlg.locator('input[type="file"]').setInputFiles(SAMPLE_PATH);
  await expect(cdlg.getByText("ice-users-sample.json")).toBeVisible();
  const rpInput = cdlg.getByPlaceholder(/blank means the file/);
  await rpInput.fill("$.users[*]");
  await expect(cdlg.getByText(/3 record\(s\) matched/)).toBeVisible();
  await shot("B4b-ceremony-upload");
  await cdlg.getByRole("button", { name: "Infer schema" }).click();

  await expect(cdlg.getByText(/Inferred \d+ top-level field\(s\) from 3 record\(s\)/)).toBeVisible({
    timeout: 30_000,
  });
  await shot("B4c-ceremony-review");

  // DEFECT WORKAROUND (documented in the journey log): the sample-inference
  // engine emits a FULL record definition at every nested-object site, naming
  // it after the field — dummyjson users carry `address` and `company.address`
  // (and `coordinates` under both), so the generated Avro redefines
  // raw.ice_users.Address / raw.ice_users.Coordinates and the backend 422s
  // with "redefined named type". Avro forbids redefinition, so the duplicate
  // definitions are renamed uniquely here, inside the ceremony's OWN
  // Raw Avro JSON editor (the sanctioned surface for exactly this class of
  // edit), and the samples are re-validated by the dialog before Approve.
  await cdlg.getByRole("tab", { name: "Raw Avro JSON" }).click();
  const rawArea = cdlg.getByRole("textbox", { name: "Raw Avro JSON" });
  const schema = JSON.parse(await rawArea.inputValue()) as Record<string, any>;
  const seen = new Set<string>();
  const renames: string[] = [];
  const uniquify = (node: any): void => {
    if (Array.isArray(node)) {
      node.forEach(uniquify);
      return;
    }
    if (!node || typeof node !== "object") return;
    if (node.type === "record" || node.type === "enum" || node.type === "fixed") {
      const base = String(node.name);
      let name = base;
      let i = 2;
      while (seen.has(`${node.namespace ?? ""}|${name}`)) name = `${base}${i++}`;
      if (name !== base) renames.push(`${base} -> ${name}`);
      node.name = name;
      seen.add(`${node.namespace ?? ""}|${name}`);
    }
    if (Array.isArray(node.fields)) for (const f of node.fields) uniquify(f.type);
    if (node.items) uniquify(node.items);
    if (node.values) uniquify(node.values);
    if (typeof node.type === "object" || Array.isArray(node.type)) uniquify(node.type);
  };
  uniquify(schema);
  const fixedRaw = JSON.stringify(schema, null, 2);
  fs.writeFileSync(path.join(ART, "B4-fixed-avro.json"), fixedRaw, "utf-8");
  saveJson("B4-raw-avro-renames.json", { renames });
  await rawArea.fill(fixedRaw);
  await expect(cdlg.getByText(/Edits still fit the uploaded samples/)).toBeVisible({ timeout: 20_000 });
  await shot("B4c2-ceremony-raw-avro-fixed");

  await cdlg.getByRole("button", { name: "Continue to Approve" }).click();

  await expect(cdlg.getByText("Ready to approve")).toBeVisible();
  await expect(cdlg.getByText(`${ICE_TOPIC}-value`).first()).toBeVisible();
  await expect(cdlg.getByText(/uploaded samples — 1 file\(s\), 3 record\(s\)/)).toBeVisible();
  await shot("B4d-ceremony-approve-summary");
  const [approveResp] = await Promise.all([
    page
      .waitForResponse((r) => r.url().includes("/api/v2/schemas/approve"), { timeout: 60_000 })
      .catch(() => null),
    cdlg.getByRole("button", { name: /Approve & register/ }).click(),
  ]);
  if (approveResp) {
    const body = await approveResp.text().catch(() => "");
    saveJson("B4-approve-response.json", { status: approveResp.status(), body: body.slice(0, 4000) });
  } else {
    saveJson("B4-approve-response.json", { status: null, body: "no /schemas/approve request observed" });
  }
  await expect(page.getByText(/Approved & registered — raw\.ice_users\.ice_user-value/)).toBeVisible({
    timeout: 90_000,
  });
  await expect(page.getByText(/Approved #\d+/).first()).toBeVisible({ timeout: 30_000 });
  await shot("B4e-schema-approved");
});

// ---------------------------------------------------------------- Phase E
test("E1 deploy 'ice users' — preflight includes schema + Connect plugin rows — then start", async () => {
  test.setTimeout(480_000);
  const iceFlowId = await flowIdByName(ICE_FLOW);
  await page.goto(`/flow-builder/${iceFlowId}`);
  // Evidence deferred from D4 (selection drops when the ceremony closes):
  // the block form's Schema section now shows the approved registration.
  await page.locator(".react-flow__node").filter({ hasText: "kafka+connect" }).first().click();
  await expect(page.getByText(/global id #\d+/).first()).toBeVisible({ timeout: 30_000 });
  await expect(page.getByText("Approved", { exact: true }).first()).toBeVisible();
  await shot("B4e-schema-approved");
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

// ---------------------------------------------------------------- Phase F
test("F1 connector ice_users.<blockId>.kafka_kc RUNNING on Kafka Connect", async () => {
  test.setTimeout(300_000);
  const iceFlowId = await flowIdByName(ICE_FLOW);
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

test("F2 first firing lands ~30 Avro records on raw.ice_users.ice_user", async () => {
  test.setTimeout(420_000);
  const iceFlowId = await flowIdByName(ICE_FLOW);
  let msgCount = -1;
  const t0 = Date.now();
  const topicDeadline = Date.now() + 300_000;
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
});

// ---------------------------------------------------------------- Phase G
test("G1 DATA LANDED: Trino discovery + count on bronze.bronze.ice_user", async () => {
  test.setTimeout(500_000);
  // Discovery (per the mission: "discover with SHOW CATALOGS / SHOW SCHEMAS /
  // SHOW TABLES"): the alpha convention's guess `iceberg.bronze.ice_user` is
  // wrong in the CATALOG position only — this Trino maps the Polaris warehouse
  // `bronze` to the catalog named `bronze`, so the table is
  // catalog=bronze, schema(namespace)=bronze, table=ice_user.
  const catalogs = await trinoQuery("SHOW CATALOGS");
  const icebergSchemas = await trinoQuery("SHOW SCHEMAS FROM iceberg");
  const icebergTables = await trinoQuery("SHOW TABLES FROM iceberg.bronze");
  const bronzeSchemas = await trinoQuery("SHOW SCHEMAS FROM bronze");
  const bronzeTables = await trinoQuery("SHOW TABLES FROM bronze.bronze");
  let countResult: Awaited<ReturnType<typeof trinoQuery>> | null = null;
  let rowCount = -1;
  const trinoDeadline = Date.now() + 420_000;
  while (Date.now() < trinoDeadline) {
    const r = await trinoQuery("SELECT count(*) FROM bronze.bronze.ice_user");
    countResult = r;
    if (r.ok && r.rows.length > 0) {
      rowCount = Number((r.rows[0] as unknown[])[0]);
      if (rowCount >= 30) break;
    }
    await sleep(15_000);
  }
  const sample = await trinoQuery(
    "SELECT id, firstName, lastName, age, gender, email FROM bronze.bronze.ice_user ORDER BY id LIMIT 5",
  );
  saveJson("B7-trino-results.json", {
    catalogs: { state: catalogs.state, rows: catalogs.rows, error: catalogs.error },
    schemasFromIceberg: { state: icebergSchemas.state, rows: icebergSchemas.rows, error: icebergSchemas.error },
    tablesFromIcebergBronze: { state: icebergTables.state, rows: icebergTables.rows, error: icebergTables.error },
    schemasFromBronzeCatalog: { state: bronzeSchemas.state, rows: bronzeSchemas.rows, error: bronzeSchemas.error },
    tablesFromBronzeBronze: { state: bronzeTables.state, rows: bronzeTables.rows, error: bronzeTables.error },
    finalCount: { state: countResult?.state, rows: countResult?.rows, error: countResult?.error, rowCount },
    sample: { state: sample.state, columns: sample.columns, rows: sample.rows.slice(0, 5), error: sample.error },
  });
  expect(rowCount, "rows in bronze.bronze.ice_user via Trino").toBeGreaterThanOrEqual(30);
});

test("G2 STOP the ice flow after the landing proof (stays deployed)", async () => {
  test.setTimeout(180_000);
  const iceFlowId = await flowIdByName(ICE_FLOW);
  await page.goto(`/flow-builder/${iceFlowId}`);
  await page.getByRole("button", { name: "Stop", exact: true }).click();
  await expect(page.getByText("Stopped — queues retained")).toBeVisible({ timeout: 120_000 });
  await shot("B7-stopped-still-deployed");
});
