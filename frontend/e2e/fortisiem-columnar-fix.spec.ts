// Part 1 Step B closeout: the FortiSIEM Pagination Test flow (flow-9d7ask)
// already proved offset/limit pagination works live against FortiSIEM's
// /phoenix/rest/query/cmdb, but delivered 0 usable Kafka messages because
// that endpoint is COLUMN-ORIENTED ({"data": [[v0,v1,...], ...]}) rather than
// an array of named objects — SplitJson was splitting bare row-arrays, so
// every downstream record was empty.
//
// backend/services/adapter/compiler/blocks_http.py now supports a per-block
// `columnar` config (JoltTransformJSON inserted before SplitJson to turn
// positional rows into named objects using a `columns` list). This spec
// applies that fix to block b-utrhfu through the real UI ONLY: lowers the
// offset Limit to 50 (small, forces a genuine multi-page loop against
// FortiSIEM's ~150ish EVENT_PULLING rows) and turns on Column-oriented
// response with rowsField="data" and the block's own 17 selectFields names,
// in order, as the columns list — then saves, deploys (real preflight +
// compile against the live block), enables, starts for one light-touch run,
// and stops. Live NiFi/Kafka verification of the actual run happens
// SEPARATELY via direct REST calls (not part of this spec) per the plan's
// "not through the app's own metrics endpoint as sole evidence" requirement.
import { test, expect, type BrowserContext, type Page } from "playwright/test";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";

const FRONTEND = "http://localhost:3001";
const BACKEND = "http://localhost:8010";
const ART = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "artifacts", "fortisiem-columnar-fix");

const FLOW_ID = "flow-9d7ask";
const TOPIC = "raw.fortisiem_pagination_test.event_pulling";
const NEW_LIMIT = "50";
const COLUMNS = [
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
];

test.describe.configure({ mode: "serial" });

let context: BrowserContext;
let page: Page;
const consoleErrors: string[] = [];

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
      /* not up yet */
    }
    await sleep(3_000);
  }
  throw new Error(`Backend at ${BACKEND}/api did not return 200 within 180s`);
}

async function flowDoc(): Promise<Record<string, any>> {
  const r = await fetch(`${BACKEND}/api/v2/flows/${FLOW_ID}`);
  if (!r.ok) throw new Error(`flow fetch failed: ${r.status}`);
  return r.json();
}

async function messages(): Promise<unknown[]> {
  try {
    const r = await fetch(`${BACKEND}/api/v2/flows/${FLOW_ID}/messages?topic=${encodeURIComponent(TOPIC)}`);
    if (!r.ok) return [];
    const j = (await r.json()) as { messages?: unknown[] };
    return j.messages ?? [];
  } catch {
    return [];
  }
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
  await context?.close();
});

test.afterEach(async ({}, testInfo) => {
  if (testInfo.status !== testInfo.expectedStatus && page) {
    const safe = testInfo.title.replace(/[^a-z0-9-]+/gi, "_").slice(0, 70);
    await page.screenshot({ path: path.join(ART, `FAIL-${safe}.png`), fullPage: true }).catch(() => undefined);
  }
});

// ---------------------------------------------------------------- Step 1
test("cfx-step01 open flow-9d7ask, edit b-utrhfu: Limit=50 + columnar transform", async () => {
  test.setTimeout(120_000);
  const before = await flowDoc();
  saveJson("00-before.json", before);
  expect(before.state).toBe("Stopped");

  await page.goto(`/flow-builder/${FLOW_ID}`);
  await expect(page.locator('span[aria-label="Stopped"]').first()).toBeVisible({ timeout: 30_000 });

  await page.locator(".react-flow__node", { hasText: "New http write" }).first().click();
  await expect(page.getByText(/→\s*https:\/\/172\.16\.30\.6:443/)).toBeVisible();

  // Advanced defaults open (pagination + body template already set -> non-empty
  // advancedSummary -> Accordion defaultValue="advanced") but click it defensively
  // if the Limit field isn't already visible.
  const limitInput = fieldInput(page, "Limit");
  if (!(await limitInput.isVisible().catch(() => false))) {
    await page.locator('button:has-text("Advanced")').first().click();
  }
  await expect(limitInput).toBeVisible({ timeout: 10_000 });
  // A prior partial run of this spec already saved limitValue=50 (Save
  // succeeded before the columnar-persistence bug was hit further down), so
  // the starting value here may legitimately be "250" (untouched) or "50"
  // (left over from that earlier partial success) — either is fine, only
  // the end state after this fill matters.
  await expect(limitInput).toHaveValue(/^(250|50)$/);
  await limitInput.fill(NEW_LIMIT);
  await expect(limitInput).toHaveValue(NEW_LIMIT);
  await shot("01a-limit-updated");

  // Each of these onChange handlers spreads the CURRENT `cfg.columnar` prop
  // captured in that render's closure, then patches the parent's draft state
  // wholesale-replacing the "columnar" key (patchConfig does a shallow merge,
  // not a deep one). A prior run of this spec proved the race is real: with
  // rowsField filled BEFORE columns, the saved doc came back with `columns`
  // correct but `rowsField` silently dropped — the columns field's onChange
  // closed over a stale `cfg.columnar` snapshot from before the rowsField
  // patch committed, so its own patch (spread of the stale snapshot + the
  // new columns) overwrote rowsField clean out of existence. Fix: settle
  // after every field edit so each next handler closes over freshly patched
  // config, AND do rowsField LAST (nothing after it can clobber it before
  // Save). Also use a sentinel value first to positively prove persistence,
  // since "data" happens to equal the input's own fallback default and so
  // can't distinguish "genuinely set" from "key missing, showing fallback".
  // A prior partial run may have already left this switch on (and columnar
  // present but incomplete) — only click if it isn't already checked, then
  // proceed to (re)apply columns/rowsField regardless so the final state is
  // deterministic either way.
  const columnarSwitch = page.locator('label:has-text("Column-oriented response")').getByRole("switch");
  await expect(columnarSwitch).toHaveAttribute("aria-checked", /^(true|false)$/);
  if ((await columnarSwitch.getAttribute("aria-checked")) !== "true") {
    await columnarSwitch.click();
  }
  await expect(columnarSwitch).toHaveAttribute("aria-checked", "true");
  await sleep(300);

  const columnsInput = page.getByPlaceholder("name, ipAddress, status (columns, in order)");
  await columnsInput.fill(COLUMNS.join(", "));
  await expect(columnsInput).toHaveValue(COLUMNS.join(", "));
  await sleep(300);

  const rowsFieldInput = page.getByPlaceholder("data (rows field)");
  await expect(rowsFieldInput).toBeVisible();
  await rowsFieldInput.fill("data_sentinel_check");
  await expect(rowsFieldInput).toHaveValue("data_sentinel_check");
  await sleep(300);
  // Reselect (remount) now, mid-sentinel, to positively prove the sentinel
  // value is genuinely in draft state (no default collision possible here).
  await page.locator(".react-flow__node", { hasText: "New kafka write" }).first().click();
  await sleep(200);
  await page.locator(".react-flow__node", { hasText: "New http write" }).first().click();
  await expect(page.getByText(/→\s*https:\/\/172\.16\.30\.6:443/)).toBeVisible();
  const rowsFieldSentinelCheck = page.getByPlaceholder("data (rows field)");
  await expect(rowsFieldSentinelCheck).toHaveValue("data_sentinel_check", { timeout: 10_000 });
  const columnsInputSentinelCheck = page.getByPlaceholder("name, ipAddress, status (columns, in order)");
  await expect(columnsInputSentinelCheck).toHaveValue(COLUMNS.join(", "), { timeout: 10_000 });

  // Now set the real desired value, last, with nothing after it but the
  // remount re-check (read-only) and Save.
  await rowsFieldSentinelCheck.fill("data");
  await expect(rowsFieldSentinelCheck).toHaveValue("data");
  await sleep(300);
  await shot("01b-columnar-configured");

  await page.locator(".react-flow__node", { hasText: "New kafka write" }).first().click();
  await sleep(200);
  await page.locator(".react-flow__node", { hasText: "New http write" }).first().click();
  await expect(page.getByText(/→\s*https:\/\/172\.16\.30\.6:443/)).toBeVisible();

  const limitInputRemount = fieldInput(page, "Limit");
  if (!(await limitInputRemount.isVisible().catch(() => false))) {
    await page.locator('button:has-text("Advanced")').first().click();
  }
  await expect(limitInputRemount).toHaveValue(NEW_LIMIT, { timeout: 10_000 });
  const columnarSwitchRemount = page.locator('label:has-text("Column-oriented response")').getByRole("switch");
  await expect(columnarSwitchRemount).toHaveAttribute("aria-checked", "true");
  const rowsFieldRemount = page.getByPlaceholder("data (rows field)");
  await expect(rowsFieldRemount).toHaveValue("data");
  const columnsInputRemount = page.getByPlaceholder("name, ipAddress, status (columns, in order)");
  await expect(columnsInputRemount).toHaveValue(COLUMNS.join(", "), { timeout: 10_000 });
  await shot("01b2-columnar-confirmed-after-remount");

  await page.getByRole("button", { name: "Save", exact: true }).click();
  await expect(page.getByText("Draft saved")).toBeVisible({ timeout: 30_000 });
  await expect(page.getByText(/Deploy unavailable/)).toHaveCount(0);
  await shot("01c-saved");

  const after = await flowDoc();
  saveJson("01d-after-save.json", after);
  const block = (after.blocks ?? []).find((b: any) => b.id === "b-utrhfu");
  expect(block.config.pagination.fields.limitValue).toBe(NEW_LIMIT);
  expect(block.config.columnar).toMatchObject({ enabled: true, rowsField: "data" });
  expect(block.config.columnar.columns).toEqual(COLUMNS);
});

// ---------------------------------------------------------------- Step 2
test("cfx-step02 validate (preflight) then deploy through the UI", async () => {
  test.setTimeout(300_000);

  await page.getByRole("button", { name: "Deploy", exact: true }).click();
  const dlg = page.getByRole("dialog").filter({ hasText: "Deploy preflight" });
  await expect(dlg).toBeVisible();
  await expect(dlg.locator("li").first()).toBeVisible({ timeout: 30_000 });
  const rowCount = await dlg.locator("li").count();
  expect(rowCount).toBeGreaterThanOrEqual(1);
  const failingCount = await dlg.locator("svg.text-destructive").count();
  await shot("02a-preflight");
  if (failingCount > 0) {
    const rowTexts = await dlg.locator("li").allTextContents();
    saveJson("02b-preflight-failures.json", rowTexts);
    throw new Error(`Preflight dialog shows ${failingCount} failing row(s): ${rowTexts.join(" | ")}`);
  }

  const deployBtn = dlg.getByRole("button", { name: "Deploy" });
  await expect(deployBtn).toBeEnabled();
  await deployBtn.click();

  // The dialog's own checks are a client-side approximation; the REAL
  // compiler compile (including the new columnar_transform processor) only
  // runs server-side on this click. Race success vs. a compile-error toast
  // so a genuine failure is reported precisely rather than timing out blind.
  const deployed = page.getByText("Deployed — the flow is built stopped");
  const errorToast = page.locator('[data-sonner-toast][data-type="error"]');
  await Promise.race([
    deployed.waitFor({ state: "visible", timeout: 240_000 }),
    errorToast.waitFor({ state: "visible", timeout: 240_000 }),
  ]);
  if (await errorToast.isVisible().catch(() => false)) {
    const text = await errorToast.allTextContents();
    await shot("02c-deploy-error");
    throw new Error(`Deploy failed: ${text.join(" | ")}`);
  }
  await expect(deployed).toBeVisible();
  await expect(page.locator('span[aria-label="Stopped"]').first()).toBeVisible({ timeout: 30_000 });
  await shot("02d-deployed");

  const after = await flowDoc();
  saveJson("02e-after-deploy.json", after);
  expect(after.runtimeScopeMap?.["b-utrhfu"]?.components?.columnar_transform).toBeTruthy();
});

// ---------------------------------------------------------------- Step 3
test("cfx-step03 enable, start (one light-touch live run), poll briefly, stop", async () => {
  test.setTimeout(420_000);

  await page.getByRole("button", { name: "More" }).click();
  await expect(page.getByRole("menuitem", { name: "Enable" })).toBeVisible();
  await page.getByRole("menuitem", { name: "Enable" }).click();
  await page.keyboard.press("Escape");

  const startBtn = page.getByRole("button", { name: "Start", exact: true });
  await expect(startBtn).toBeEnabled({ timeout: 20_000 });
  await startBtn.click();
  await expect(page.getByText("Flow started")).toBeVisible({ timeout: 120_000 });
  await shot("03a-started");

  // Cron is */2 * * * * — poll briefly (do NOT wait for the queue to fully
  // drain) until a few records land, then stop immediately. This is the
  // single live hit against FortiSIEM for this verification pass.
  const deadline = Date.now() + 330_000;
  let sample: unknown[] = [];
  while (Date.now() < deadline) {
    sample = await messages();
    if (sample.length > 0) break;
    await sleep(10_000);
  }
  saveJson("03b-messages-sample.json", { topic: TOPIC, count: sample.length, sample: sample.slice(0, 3) });

  const stopBtn = page.getByRole("button", { name: "Stop", exact: true });
  await expect(stopBtn).toBeEnabled({ timeout: 20_000 });
  await stopBtn.click();
  await expect(page.getByText(/^Stopped/)).toBeVisible({ timeout: 60_000 });
  await shot("03c-stopped");

  const final = await flowDoc();
  saveJson("03d-final-flow.json", final);
  saveJson("03e-verdict.json", {
    flowId: FLOW_ID,
    topic: TOPIC,
    lightPollMessageCount: sample.length,
    finalState: final.state,
    note: "Authoritative NiFi/Kafka verification is performed separately via direct REST calls, not via this app-side poll.",
  });
});
