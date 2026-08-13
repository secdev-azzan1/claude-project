// UI E2E verification journey — drives the EXACT user scenario through the real
// browser UI against the real backend (http://localhost:8010):
//   HTTP service -> flow -> http·read root (full-URL auto-strip) -> kafka·write
//   child -> block Test (save-before-test) -> Deploy (preflight) -> Start ->
//   messages on the topic -> Stop/Undeploy/Redeploy (regression: own-topic
//   reservation) -> UI cleanup -> API sweep.
//
// The flow created by the "New flow" page is staged CLIENT-side until its first
// save (frontend/src/prototype/api.ts stagedNewFlows), so steps share one page
// in serial mode — a fresh page before the first save would 404 the flow.
import { test, expect, type BrowserContext, type Page } from "playwright/test";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";

const FRONTEND = "http://localhost:3001";
const BACKEND = "http://localhost:8010";
const ART = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "artifacts");

const SERVICE_NAME = "e2eui dummyjson";
const BASE_URL_VALUE = "https://dummyjson.com";
const FLOW_NAME = "e2eui users";
const ENTITY = "e2eui_user";
const TOPIC = "raw.e2eui_users.e2eui_user";
const CRON = "*/3 * * * *";

test.describe.configure({ mode: "serial" });

let context: BrowserContext;
let page: Page;
let flowId = "";
const consoleErrors: string[] = [];

async function shot(name: string) {
  await page.screenshot({ path: path.join(ART, `${name}.png`), fullPage: true });
}

/** A labelled input in the shadcn service form (Label is not htmlFor-linked). */
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

/** Best-effort API cleanup of leftovers from previous runs (prefix e2eui). */
async function apiPreClean(): Promise<void> {
  try {
    const flows = (await (await fetch(`${BACKEND}/api/v2/flows/`)).json()) as { id: string; name?: string }[];
    for (const f of flows) {
      if (!String(f.name ?? "").toLowerCase().startsWith("e2eui")) continue;
      for (const verb of ["stop", "undeploy"]) {
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
      if (!String(s.name ?? "").toLowerCase().startsWith("e2eui") || s.retired) continue;
      await fetch(`${BACKEND}/api/v2/services/${s.id}/retire`, { method: "POST" }).catch(() => undefined);
    }
  } catch {
    /* best effort */
  }
}

async function messagesCount(): Promise<number> {
  try {
    const r = await fetch(`${BACKEND}/api/v2/flows/${flowId}/messages?topic=${encodeURIComponent(TOPIC)}`);
    if (!r.ok) return -1;
    const j = (await r.json()) as { messages?: unknown[] };
    return (j.messages ?? []).length;
  } catch {
    return -1;
  }
}

test.beforeAll(async ({ browser }) => {
  fs.mkdirSync(ART, { recursive: true });
  await backendReady();
  await apiPreClean();
  context = await browser.newContext({ baseURL: FRONTEND, viewport: { width: 1680, height: 1000 } });
  // tracing handled by playwright.config.ts (trace: retain-on-failure)
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

// ---------------------------------------------------------------- Step 1
test("step01 create HTTP service on Application Services", async () => {
  await page.goto("/application-services");
  await page.getByRole("button", { name: "Add Service" }).first().click();

  const dialog = page.getByRole("dialog");
  await expect(dialog.getByText("Add Application Service")).toBeVisible();
  await dialog.getByRole("button", { name: /HTTP service/ }).click();

  await fieldInput(dialog, "Name").fill(SERVICE_NAME);
  await fieldInput(dialog, "Base URL").fill(BASE_URL_VALUE);
  // Auth mode defaults to "None" — leave it.
  await expect(dialog.getByRole("combobox").filter({ hasText: "None" })).toBeVisible();

  await dialog.getByRole("button", { name: "Create Service" }).click();
  await expect(page.getByText(`Service "${SERVICE_NAME}" created`)).toBeVisible();

  const activeCard = page
    .locator("div.bg-card")
    .filter({ hasText: SERVICE_NAME })
    .filter({ hasNotText: "Retired" })
    .first();
  await expect(activeCard).toBeVisible();
  await expect(activeCard.getByText(BASE_URL_VALUE)).toBeVisible();
  await shot("01-service-created");
});

// ---------------------------------------------------------------- Step 2
test("step02 new flow named 'e2eui users'", async () => {
  await page.goto("/flows");
  await page.getByRole("button", { name: "New Flow" }).click();
  await expect(page).toHaveURL(/\/flow-builder\/new/);

  await page.getByPlaceholder("e.g. CrowdStrike Detections").fill(FLOW_NAME);
  await page.getByRole("button", { name: "Create & open builder" }).click();

  await expect(page).toHaveURL(/\/flow-builder\/(?!new)[\w-]+/);
  flowId = new URL(page.url()).pathname.split("/").pop()!;
  expect(flowId.length).toBeGreaterThan(0);

  await expect(page.getByText("Never deployed")).toBeVisible();
  // Trigger note: the cron control only appears once an http/jdbc root exists
  // (FlowSettingsForm: "Add a root block first…") — cron is set in step 3.
  await shot("02-flow-created");
});

// ---------------------------------------------------------------- Step 3
test("step03 place http·read root, service bind, full-URL auto-strip, record path, cron", async () => {
  await page.getByRole("button", { name: "Place the root" }).click();
  await page.getByRole("menuitem", { name: /http · read/ }).click();

  // Identity — Existing service mode (default) + pick the service.
  await expect(page.getByText("Existing service")).toBeVisible();
  await page.getByRole("combobox").filter({ hasText: "Select a service" }).click();
  await page.getByRole("option", { name: /e2eui dummyjson/ }).click();

  // Adapter settings: base-URL context line appears once the service is bound.
  await expect(page.getByText(/Base URL —/)).toBeVisible();
  await expect(page.getByText(BASE_URL_VALUE).first()).toBeVisible();

  // Method GET (default for read mode) — assert it.
  await expect(page.getByRole("combobox").filter({ hasText: "GET" }).first()).toBeVisible();

  // THE FIX UNDER TEST: type the FULL URL into the Path field.
  const pathInput = page.getByPlaceholder("/users");
  await pathInput.fill(`${BASE_URL_VALUE}/users`);
  await expect(page.getByText("Base URL comes from the service — kept just the path.")).toBeVisible();
  await expect(pathInput).toHaveValue("/users");
  await expect(page.getByText(/→\s*https:\/\/dummyjson\.com\/users/)).toBeVisible();
  await shot("03a-autostrip");

  // Response parsing: record path + split default ON.
  const splitSwitch = page.locator('label:has-text("split into records")').getByRole("switch");
  await expect(splitSwitch).toHaveAttribute("aria-checked", "true"); // default ON
  await page.getByPlaceholder("$.resources[*] (record path)").fill("$.users[*]");
  await expect(splitSwitch).toHaveAttribute("aria-checked", "true");

  // Cron: Flow settings -> Custom -> */3 * * * *
  await page.getByRole("button", { name: /Flow settings/ }).click();
  await page.getByRole("combobox").first().click();
  await page.getByRole("option", { name: /Custom/ }).click();
  await page.getByPlaceholder("*/15 * * * *").fill(CRON);
  await expect(page.getByText("5 fields required")).toHaveCount(0);
  await expect(page.getByText(/^Next:/)).toBeVisible();
  await shot("03-root-configured");
});

// ---------------------------------------------------------------- Step 4
test("step04 add kafka·write child with entity e2eui_user", async () => {
  await page.locator(".react-flow__node", { hasText: "New http read" }).first().click();
  await page.locator('button[title^="Add a block after"]').click();
  await page.getByRole("menuitem", { name: /kafka · write/ }).click();

  // Adding a block does not always move selection — click the new kafka node
  // on the canvas so its form opens, then expand the collapsed Entity section.
  await page.locator(".react-flow__node").filter({ hasText: "kafka · write" }).first().click();
  // "Entity & derived names" is FORCED OPEN for a write block with no entity
  // (the trigger renders disabled) — the input is already visible, no click.
  await page.getByPlaceholder("asset · incident · order…").fill(ENTITY);
  await expect(page.getByText(TOPIC).first()).toBeVisible();
  await shot("04-kafka-child");
});

// ---------------------------------------------------------------- Step 5
test("step05 block Test on the read block works on the unsaved flow", async () => {
  test.setTimeout(300_000);
  await page.locator(".react-flow__node", { hasText: "New http read" }).first().click();

  // Open the Test accordion section (closed by default).
  await page.locator("#block-section-test button").first().click();
  await expect(page.getByText("Testing saves the flow first.")).toBeVisible();

  await page.getByRole("button", { name: "Test block" }).click();

  // Save-before-test fix: real results, never "Deployment engine pending".
  await expect(page.getByText(/Test succeeded — \d+ sample record\(s\), nothing committed/)).toBeVisible({
    timeout: 90_000,
  });
  await expect(page.getByText("Deployment engine pending")).toHaveCount(0);
  expect.soft(await page.getByText(/Test succeeded — 10 sample record\(s\)/).count(), "expected 10 records").toBe(1);
  await expect(page.getByText("Detected fields — click to add an extraction rule:")).toBeVisible();
  await expect(page.getByText(/Response explorer — first sample record/)).toBeVisible();
  await shot("05-test-results");
});

// ---------------------------------------------------------------- Step 6
test("step06 save then deploy via preflight — all rows ok", async () => {
  test.setTimeout(300_000);
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await expect(page.getByText("Draft saved")).toBeVisible();
  await expect(page.getByText(/Deploy unavailable/)).toHaveCount(0);

  await page.getByRole("button", { name: "Deploy", exact: true }).click();
  const dlg = page.getByRole("dialog").filter({ hasText: "Deploy preflight" });
  await expect(dlg).toBeVisible();
  await expect(dlg.locator("li").first()).toBeVisible({ timeout: 30_000 });
  expect(await dlg.locator("li").count()).toBeGreaterThanOrEqual(4);
  await expect(dlg.locator("svg.text-destructive")).toHaveCount(0); // every row ok
  await shot("06-preflight");

  const deployBtn = dlg.getByRole("button", { name: "Deploy" });
  await expect(deployBtn).toBeEnabled();
  await deployBtn.click();

  await expect(page.getByText("Deployed — the flow is built stopped")).toBeVisible({ timeout: 240_000 });
  await expect(page.locator('span[aria-label="Stopped"]').first()).toBeVisible({ timeout: 30_000 });
  await shot("06b-deployed");
});

// ---------------------------------------------------------------- Step 7
test("step07 enable, start, wait for topic messages (API), verify Messages tab (UI)", async () => {
  test.setTimeout(780_000);

  // New flows are created disabled; Start is guarded by "The flow is disabled."
  await page.getByRole("button", { name: "More" }).click();
  await page.getByRole("menuitem", { name: "Enable" }).click();
  await page.keyboard.press("Escape");

  const startBtn = page.getByRole("button", { name: "Start", exact: true });
  await expect(startBtn).toBeEnabled({ timeout: 20_000 });
  await startBtn.click();
  await expect(page.getByText("Flow started")).toBeVisible({ timeout: 120_000 });
  await expect(page.locator('span[aria-label="Running"]').first()).toBeVisible({ timeout: 30_000 });
  await shot("07-started");

  // Cron wait happens OUTSIDE the browser: poll the backend for topic messages.
  const started = Date.now();
  let count = -1;
  let windowsUsed = 1;
  const pollWindow = async (ms: number) => {
    const deadline = Date.now() + ms;
    while (Date.now() < deadline) {
      count = await messagesCount();
      if (count > 0) return;
      await new Promise((r) => setTimeout(r, 10_000));
    }
  };
  await pollWindow(240_000); // ≤4 min budget
  if (count <= 0) {
    windowsUsed = 2; // noted retry per instructions
    await pollWindow(210_000);
  }
  const waitedS = Math.round((Date.now() - started) / 1000);
  fs.writeFileSync(
    path.join(ART, "07-poll-log.txt"),
    `topic=${TOPIC}\nflowId=${flowId}\nmessages=${count}\nwaited_seconds=${waitedS}\npoll_windows=${windowsUsed}\n`,
    "utf-8",
  );
  expect(count, `messages on ${TOPIC} after ${waitedS}s (${windowsUsed} poll window(s))`).toBeGreaterThan(0);

  // Now prove the same through the UI: Flows -> flow -> Messages tab -> topic.
  await page.goto("/flows");
  await page.getByText(FLOW_NAME).first().click();
  await expect(page.getByRole("tab", { name: "Messages" })).toBeVisible();
  await page.getByRole("tab", { name: "Messages" }).click();
  const topicSelect = page.getByRole("combobox").filter({ hasText: /raw\.|Pick a topic/ }).first();
  await topicSelect.click();
  await page.getByRole("option", { name: TOPIC }).click();
  await expect(page.getByText(/offset \d+/).first()).toBeVisible({ timeout: 30_000 });
  await shot("07b-messages-ui");
  await page.keyboard.press("Escape"); // close the sheet
});

// ---------------------------------------------------------------- Step 8
test("step08 REGRESSION stop -> undeploy -> redeploy passes preflight (own topic not a collision)", async () => {
  test.setTimeout(600_000);
  await page.goto(`/flow-builder/${flowId}`);

  await page.getByRole("button", { name: "Stop", exact: true }).click();
  await expect(page.getByText("Stopped — queues retained")).toBeVisible({ timeout: 120_000 });
  await shot("08-stopped");

  await page.getByRole("button", { name: "More" }).click();
  await page.getByRole("menuitem", { name: /Undeploy/ }).click();
  await expect(page.getByText(/Undeployed — generated topics emptied/)).toBeVisible({ timeout: 180_000 });
  await shot("08b-undeployed");

  // Deploy again — the user's bug: blocked by the flow's OWN leftover topic.
  await page.getByRole("button", { name: "Deploy", exact: true }).click();
  const dlg = page.getByRole("dialog").filter({ hasText: "Deploy preflight" });
  await expect(dlg).toBeVisible();
  await expect(dlg.locator("li").first()).toBeVisible({ timeout: 30_000 });
  await expect(dlg.locator("svg.text-destructive")).toHaveCount(0);
  await expect(dlg.getByText(/not owned by this flow/)).toHaveCount(0);
  await shot("08c-preflight-2nd");

  const deployBtn = dlg.getByRole("button", { name: "Deploy" });
  await expect(deployBtn).toBeEnabled();
  await deployBtn.click();

  await expect(page.getByText("Deployed — the flow is built stopped")).toBeVisible({ timeout: 240_000 });
  await expect(page.getByText(/not owned by this flow/)).toHaveCount(0);
  await expect(page.getByText(/Deploy failed/)).toHaveCount(0);
  await expect(page.locator('span[aria-label="Stopped"]').first()).toBeVisible({ timeout: 30_000 });
  await shot("08d-redeployed");
});

// ---------------------------------------------------------------- Step 9
test("step09 UI cleanup — delete flow, retire service", async () => {
  test.setTimeout(600_000);

  // Delete requires undeployed: undeploy the just-redeployed flow first (UI).
  await page.getByRole("button", { name: "More" }).click();
  await page.getByRole("menuitem", { name: /Undeploy/ }).click();
  await expect(page.getByText(/Undeployed — generated topics emptied/)).toBeVisible({ timeout: 180_000 });

  await page.goto("/flows");
  await page.getByPlaceholder("Search flows, entities, topics…").fill("e2eui");
  const row = page.getByRole("row").filter({ hasText: FLOW_NAME });
  await expect(row).toBeVisible();
  await row.getByRole("button", { name: "More actions" }).click();
  await page.getByRole("menuitem", { name: "Delete" }).click();

  const delDlg = page.getByRole("alertdialog");
  await expect(delDlg.getByText(`Delete "${FLOW_NAME}"?`)).toBeVisible();
  await delDlg.getByRole("button", { name: "Delete flow" }).click();
  await expect(page.getByText(`Deleted — ${FLOW_NAME}`)).toBeVisible({ timeout: 120_000 });
  await expect(page.getByRole("row").filter({ hasText: FLOW_NAME })).toHaveCount(0, { timeout: 30_000 });
  await shot("09-flow-deleted");

  // Retire the service. NOTE: retirement is logical by design ("no hard
  // delete") — the card stays listed, marked Retired, with Reinstate.
  await page.goto("/application-services");
  const activeCard = page
    .locator("div.bg-card")
    .filter({ hasText: SERVICE_NAME })
    .filter({ hasNotText: "Retired" })
    .first();
  await expect(activeCard).toBeVisible();
  await activeCard.getByRole("button", { name: "Retire" }).click();

  const retDlg = page.getByRole("alertdialog");
  await expect(retDlg.getByText(`Retire "${SERVICE_NAME}"?`)).toBeVisible();
  await retDlg.getByRole("button", { name: "Retire Service" }).click();
  await expect(page.getByText(`"${SERVICE_NAME}" retired`)).toBeVisible({ timeout: 60_000 });

  // No ACTIVE card with that name remains; the retired card shows Reinstate.
  await expect(
    page.locator("div.bg-card").filter({ hasText: SERVICE_NAME }).filter({ hasNotText: "Retired" }),
  ).toHaveCount(0, { timeout: 30_000 });
  const retiredCard = page.locator("div.bg-card").filter({ hasText: SERVICE_NAME }).first();
  await expect(retiredCard.getByRole("button", { name: "Reinstate" })).toBeVisible();
  await shot("09b-service-retired");
});

// ---------------------------------------------------------------- Step 10
test("step10 post-cleanup API sweep — no e2eui flow remains", async () => {
  const r = await fetch(`${BACKEND}/api/v2/flows/`);
  expect(r.ok).toBe(true);
  const flows = (await r.json()) as { id: string; name?: string }[];
  const leftovers = flows.filter((f) => String(f.name ?? "").toLowerCase().startsWith("e2eui"));
  expect(leftovers, `e2eui flows left on the backend: ${JSON.stringify(leftovers.map((f) => f.name))}`).toEqual([]);
  // NiFi PG removal is the backend delete's duty; the empty flows list is the
  // observable contract asserted here (best-effort per instructions).
  await shot("10-final-flows-list");
});
