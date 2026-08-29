// Part 1 Step B (majestic-discovering-leaf.md): prove the write-mode HTTP
// pagination compiler feature (backend/services/adapter/compiler/blocks_http.py)
// against the REAL live FortiSIEM API, by building a throwaway flow through
// the ACTUAL application UI (never a direct backend/Mongo/NiFi write).
//
// Scope of THIS spec: build + validate + deploy ONLY (Save -> Deploy ->
// "Deployed — the flow is built stopped"). Runtime lifecycle (enable, start,
// pause, cron-triggered run, NiFi/Kafka verification) is deliberately a
// SEPARATE step done via a direct backend-API script afterward — building
// through the UI and driving the live run are kept apart on purpose.
//
// FortiSIEM password: read ONLY from process.env.FORTISIEM_PASSWORD at
// invocation time. Never hardcoded here, never printed, never written to a
// screenshot-adjacent file.
//
//   FORTISIEM_PASSWORD='...' npx playwright test fortisiem-pagination-build.spec.ts
import { test, expect, type BrowserContext, type Page } from "playwright/test";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";

const FRONTEND = "http://localhost:3001";
const BACKEND = "http://localhost:8010";
const ART = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "artifacts");

const FORTISIEM_PASSWORD = process.env.FORTISIEM_PASSWORD;

const SERVICE_NAME = "FortiSIEM Test";
const BASE_URL_VALUE = "https://172.16.30.6:443";
const FORTISIEM_USERNAME = "super/CMDBAPI";

const FLOW_NAME = "FortiSIEM Pagination Test";
const ENTITY = "event_pulling";
const TOPIC = "raw.fortisiem_pagination_test.event_pulling";
const CRON = "*/2 * * * *";

// event_pulling: verified_totalCount=684 (.tmp_work/fs_build_spec.json), the
// smallest FortiSIEM CMDB entity found during recon — chosen specifically so
// Limit=250 with the total_count stop condition needs EXACTLY 3 live
// requests (250 + 250 + 184 = 684), no wasted empty-page probe.
const BODY_TEMPLATE = JSON.stringify({
  target: "EVENT_PULLING",
  selectFields: [
    "Discover_Status",
    "Agent_Status",
    "Customer_ID",
    "Device_Type_Model",
    "Event_Pulling_Reporter",
    "Agent_Policy",
    "Event_Pulling_Access_Protocol",
    "Latest_Event_Pulling_Time",
    "Event_Pulling_Status",
    "Agent_Type",
    "Device_IP",
    "Device_Type_Vendor",
    "Device_Name",
    "Agent_Version",
    "Customer_Name",
    "Agent_Upgrade_Status",
    "Event_Pulling_Status_Description",
  ],
});

test.describe.configure({ mode: "serial" });

let context: BrowserContext;
let page: Page;
let flowId = "";
const consoleErrors: string[] = [];

async function shot(name: string) {
  await page.screenshot({ path: path.join(ART, `${name}.png`), fullPage: true });
}

/** A labelled input in the shadcn forms (Label is not htmlFor-linked). */
function fieldInput(scope: ReturnType<Page["locator"]>, label: string) {
  return scope.locator(`div:has(> label:text-is("${label}")) input`).first();
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
    await new Promise((r) => setTimeout(r, 3_000));
  }
  throw new Error(`Backend at ${BACKEND}/api did not return 200 within 180s`);
}

/** Best-effort API cleanup of leftovers from previous attempts (prefix fortisiem). */
async function apiPreClean(): Promise<void> {
  try {
    const flows = (await (await fetch(`${BACKEND}/api/v2/flows/`)).json()) as { id: string; name?: string }[];
    for (const f of flows) {
      if (!String(f.name ?? "").toLowerCase().startsWith("fortisiem")) continue;
      for (const verb of ["pause", "stop", "undeploy"]) {
        await fetch(`${BACKEND}/api/v2/flows/${f.id}/verbs/${verb}`, { method: "POST" }).catch(() => undefined);
      }
      await fetch(`${BACKEND}/api/v2/flows/${f.id}`, { method: "DELETE" }).catch(() => undefined);
    }
  } catch {
    /* best effort */
  }
  try {
    const services = (await (await fetch(`${BACKEND}/api/v2/services/`)).json()) as {
      id: string;
      name?: string;
      retired?: boolean;
    }[];
    for (const s of services) {
      if (!String(s.name ?? "").toLowerCase().startsWith("fortisiem") || s.retired) continue;
      await fetch(`${BACKEND}/api/v2/services/${s.id}/retire`, { method: "POST" }).catch(() => undefined);
    }
  } catch {
    /* best effort */
  }
}

test.beforeAll(async ({ browser }) => {
  if (!FORTISIEM_PASSWORD) {
    throw new Error(
      "FORTISIEM_PASSWORD env var is required (never hardcode it) — " +
        "e.g. FORTISIEM_PASSWORD='...' npx playwright test fortisiem-pagination-build.spec.ts",
    );
  }
  fs.mkdirSync(ART, { recursive: true });
  await backendReady();
  await apiPreClean();
  context = await browser.newContext({ baseURL: FRONTEND, viewport: { width: 1680, height: 1000 } });
  page = await context.newPage();
  page.on("console", (m) => {
    if (m.type() === "error") consoleErrors.push(m.text());
  });
  page.on("pageerror", (e) => consoleErrors.push(`pageerror: ${String(e)}`));
});

test.afterAll(async () => {
  if (consoleErrors.length > 0) {
    fs.writeFileSync(path.join(ART, "fs-console-errors.txt"), consoleErrors.join("\n---\n"), "utf-8");
  }
  await context?.close();
});

test.afterEach(async ({}, testInfo) => {
  if (testInfo.status !== testInfo.expectedStatus && page) {
    const safe = testInfo.title.replace(/[^a-z0-9-]+/gi, "_").slice(0, 60);
    await page.screenshot({ path: path.join(ART, `FS-FAIL-${safe}.png`), fullPage: true }).catch(() => undefined);
  }
});

// ---------------------------------------------------------------- Step 1
test("fs-step01 create FortiSIEM Test HTTP service (basic auth) on Application Services", async () => {
  await page.goto("/application-services");
  await page.getByRole("button", { name: "Add Service" }).first().click();

  const dialog = page.getByRole("dialog");
  await expect(dialog.getByText("Add Application Service")).toBeVisible();
  await dialog.getByRole("button", { name: /HTTP service/ }).click();

  await fieldInput(dialog, "Name").fill(SERVICE_NAME);
  await fieldInput(dialog, "Base URL").fill(BASE_URL_VALUE);

  await dialog.getByRole("combobox").filter({ hasText: "None" }).click();
  await page.getByRole("option", { name: "Basic" }).click();

  await fieldInput(dialog, "Username").fill(FORTISIEM_USERNAME);
  await dialog.locator('input[type="password"]').fill(FORTISIEM_PASSWORD as string);

  await dialog.getByRole("button", { name: "Create Service" }).click();
  await expect(page.getByText(`Service "${SERVICE_NAME}" created`)).toBeVisible();

  const activeCard = page
    .locator("div.bg-card")
    .filter({ hasText: SERVICE_NAME })
    .filter({ hasNotText: "Retired" })
    .first();
  await expect(activeCard).toBeVisible();
  await expect(activeCard.getByText(BASE_URL_VALUE)).toBeVisible();
  await shot("fs-01-service-created");
});

// ---------------------------------------------------------------- Step 2
test("fs-step02 new flow named 'FortiSIEM Pagination Test'", async () => {
  await page.goto("/flows");
  await page.getByRole("button", { name: "New Flow" }).click();
  await expect(page).toHaveURL(/\/flow-builder\/new/);

  await page.getByPlaceholder("e.g. CrowdStrike Detections").fill(FLOW_NAME);
  await page.getByRole("button", { name: "Create & open builder" }).click();

  await expect(page).toHaveURL(/\/flow-builder\/(?!new)[\w-]+/);
  flowId = new URL(page.url()).pathname.split("/").pop()!;
  expect(flowId.length).toBeGreaterThan(0);

  await expect(page.getByText("Never deployed")).toBeVisible();
  await shot("fs-02-flow-created");
});

// ---------------------------------------------------------------- Step 3
test("fs-step03 place http·write root, bind service, POST /query/cmdb, response parsing, cron", async () => {
  await page.getByRole("button", { name: "Place the root" }).click();
  await page.getByRole("menuitem", { name: /http · write/ }).click();

  // Identity — Existing service mode (default) + pick the service.
  await expect(page.getByText("Existing service")).toBeVisible();
  await page.getByRole("combobox").filter({ hasText: "Select a service" }).click();
  await page.getByRole("option", { name: new RegExp(SERVICE_NAME) }).click();

  await expect(page.getByText(/Base URL —/)).toBeVisible();
  await expect(page.getByText(BASE_URL_VALUE).first()).toBeVisible();

  // Method defaults to POST for write mode (first of METHODS_FOR_MODE).
  await expect(page.getByRole("combobox").filter({ hasText: "POST" }).first()).toBeVisible();

  const pathInput = page.getByPlaceholder("/users");
  await pathInput.fill("/query/cmdb");
  await expect(pathInput).toHaveValue("/query/cmdb");
  await expect(page.getByText(/→\s*https:\/\/172\.16\.30\.6:443\/query\/cmdb/)).toBeVisible();

  // Response parsing: JSON (default), record path = FortiSIEM's column-
  // oriented `data` array, split ON (default).
  const splitSwitch = page.locator('label:has-text("split into records")').getByRole("switch");
  await expect(splitSwitch).toHaveAttribute("aria-checked", "true");
  await page.getByPlaceholder("$.resources[*] (record path)").fill("$.data[*]");
  await shot("fs-03a-root-basics");

  // Advanced: body template, chain-continues, pagination.
  await page.locator('button:has-text("Advanced")').first().click();

  await page.getByPlaceholder('{"records": ${records}}').fill(BODY_TEMPLATE);

  await page.getByRole("combobox").filter({ hasText: "Original records" }).click();
  await page.getByRole("option", { name: "Parsed response" }).click();

  // Pagination: offset/limit, start/size, Limit=250, stop on total_count,
  // total read from the response body at $.totalCount.
  await page.getByRole("combobox").filter({ hasText: "No pagination" }).click();
  await page.getByRole("option", { name: "Offset / limit" }).click();

  await fieldInput(page, "Offset parameter").fill("start");
  await fieldInput(page, "Limit parameter").fill("size");
  await fieldInput(page, "Limit").fill("250");

  await page.getByRole("combobox").filter({ hasText: "Empty page" }).click();
  await page.getByRole("option", { name: "Total count field" }).click();

  await expect(page.getByText("Where is the total count?")).toBeVisible();
  await fieldInput(page, "JSONPath").fill("$.totalCount");

  await expect(
    page.getByText("Pagination fields (start, size) are added to this body automatically"),
  ).toBeVisible();
  await shot("fs-03b-advanced-pagination");

  // Cron: Flow settings -> Custom -> */2 * * * * (same interval proven
  // elsewhere in this repo for CRON_DRIVEN NiFi scheduling).
  await page.getByRole("button", { name: /Flow settings/ }).click();
  await page.getByRole("combobox").first().click();
  await page.getByRole("option", { name: /Custom/ }).click();
  await page.getByPlaceholder("*/15 * * * *").fill(CRON);
  await expect(page.getByText("5 fields required")).toHaveCount(0);
  await expect(page.getByText(/^Next:/)).toBeVisible();

  // Clicking "Flow settings" swapped the right-hand panel away from the
  // block form entirely (it now shows Flow identity/Trigger/Validation) —
  // the entity field lives on the block form, so we must reselect the
  // http·write node on the canvas to bring that form back before filling it.
  // Every write block needs its own entity label (validation.ts: "No write
  // without an entity, ever") — http-write is no exception, even though it
  // derives no topic/table name of its own (that's the kafka child below).
  await page.locator(".react-flow__node", { hasText: "New http write" }).first().click();
  await page.getByPlaceholder("asset · incident · order…").fill(ENTITY);
  await shot("fs-03c-root-configured");
});

// ---------------------------------------------------------------- Step 4
test("fs-step04 add kafka·write child with entity event_pulling -> raw.fortisiem_pagination_test.event_pulling", async () => {
  await page.locator(".react-flow__node", { hasText: "New http write" }).first().click();
  await page.locator('button[title^="Add a block after"]').click();
  await page.getByRole("menuitem", { name: /kafka · write/ }).click();

  await page.locator(".react-flow__node").filter({ hasText: "kafka · write" }).first().click();
  await page.getByPlaceholder("asset · incident · order…").fill(ENTITY);
  await expect(page.getByText(TOPIC).first()).toBeVisible();
  await shot("fs-04-kafka-child");
});

// ---------------------------------------------------------------- Step 5
test("fs-step05 save then deploy via preflight — all rows ok", async () => {
  test.setTimeout(300_000);
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await expect(page.getByText("Draft saved")).toBeVisible();
  await expect(page.getByText(/Deploy unavailable/)).toHaveCount(0);
  await shot("fs-05a-saved");

  await page.getByRole("button", { name: "Deploy", exact: true }).click();
  const dlg = page.getByRole("dialog").filter({ hasText: "Deploy preflight" });
  await expect(dlg).toBeVisible();
  await expect(dlg.locator("li").first()).toBeVisible({ timeout: 30_000 });
  expect(await dlg.locator("li").count()).toBeGreaterThanOrEqual(3);
  await expect(dlg.locator("svg.text-destructive")).toHaveCount(0); // every row ok
  await shot("fs-05b-preflight");

  const deployBtn = dlg.getByRole("button", { name: "Deploy" });
  await expect(deployBtn).toBeEnabled();
  await deployBtn.click();

  await expect(page.getByText("Deployed — the flow is built stopped")).toBeVisible({ timeout: 240_000 });
  await expect(page.locator('span[aria-label="Stopped"]').first()).toBeVisible({ timeout: 30_000 });
  await shot("fs-05c-deployed");

  fs.writeFileSync(
    path.join(ART, "fs-flow-id.txt"),
    `flowId=${flowId}\nflowName=${FLOW_NAME}\nserviceName=${SERVICE_NAME}\ntopic=${TOPIC}\ncron=${CRON}\n`,
    "utf-8",
  );
});
