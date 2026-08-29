// Part 1 Step B continuation: every live cron-triggered run of the
// FortiSIEM Pagination Test flow (flow-9d7ask) got HTTP 404 from
// https://apisix.datapasc.com/fortisiem_egress/query/cmdb. Diagnosed via
// (a) NiFi provenance events showing 404 + Apache-httpd-style error body on
// every InvokeHTTP attempt, (b) a bare unauthenticated GET to
// /fortisiem_egress/ returning genuine FortiSIEM HTML (proving APISIX
// routing/TLS/upstream all work), (c) a pre-existing unrelated legacy APISIX
// route set for prefix /fortisiem (ids 1001-1004) that rewrites to
// /phoenix/rest/, and (d) tools/build_nifi_external_connectivity_diag.py
// (reference-only, read not run) explicitly testing
// https://172.16.30.6/phoenix/rest/ as FortiSIEM's real REST base — followed
// by a final confirming diagnostic: an unauthenticated GET to
// /fortisiem_egress/phoenix/rest/ returns HTTP 401 (auth required — the path
// EXISTS), whereas /fortisiem_egress/query/cmdb (no prefix) returns 404 (the
// path does NOT exist at web root). Conclusion: the flow's own `path` field
// is missing FortiSIEM's required /phoenix/rest prefix — a flow-configuration
// mistake made during the original Part 1 Step B UI build, not a compiler or
// pagination bug. Fix: edit path from /query/cmdb to
// /phoenix/rest/query/cmdb through the UI (block config is a "build" action,
// UI-only per plan constraints), save, redeploy.
import { test, expect, type BrowserContext, type Page } from "playwright/test";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";

const FRONTEND = "http://localhost:3001";
const ART = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "artifacts");

const FLOW_NAME = "FortiSIEM Pagination Test";
const OLD_PATH = "/query/cmdb";
const NEW_PATH = "/phoenix/rest/query/cmdb";

test.describe.configure({ mode: "serial" });

let context: BrowserContext;
let page: Page;

async function shot(name: string) {
  await page.screenshot({ path: path.join(ART, `${name}.png`), fullPage: true });
}

test.beforeAll(async ({ browser }) => {
  fs.mkdirSync(ART, { recursive: true });
  context = await browser.newContext({ baseURL: FRONTEND, viewport: { width: 1680, height: 1000 } });
  page = await context.newPage();
});

test.afterAll(async () => {
  await context?.close();
});

test.afterEach(async ({}, testInfo) => {
  if (testInfo.status !== testInfo.expectedStatus && page) {
    const safe = testInfo.title.replace(/[^a-z0-9-]+/gi, "_").slice(0, 60);
    await page.screenshot({ path: path.join(ART, `PATHFIX-FAIL-${safe}.png`), fullPage: true }).catch(() => undefined);
  }
});

test("pathfix-step01 open the flow and fix the http write block's path to include /phoenix/rest", async () => {
  test.setTimeout(180_000);
  await page.goto("/flows");
  await page.getByPlaceholder("Search flows, entities, metrics…").fill(FLOW_NAME).catch(async () => {
    await page.getByPlaceholder("Search flows, entities, topics…").fill(FLOW_NAME);
  });
  const row = page.getByRole("row", { name: new RegExp(FLOW_NAME) });
  await expect(row).toBeVisible();
  await row.getByRole("button", { name: "Edit flow" }).click();
  await expect(page).toHaveURL(/\/flow-builder\/[\w-]+/);

  await expect(page.locator('span[aria-label="Stopped"]').first()).toBeVisible({ timeout: 30_000 });

  await page.locator(".react-flow__node", { hasText: "New http write" }).first().click();
  const pathInput = page.getByPlaceholder("/users");
  await expect(pathInput).toHaveValue(OLD_PATH);
  await pathInput.fill(NEW_PATH);
  await expect(pathInput).toHaveValue(NEW_PATH);
  await expect(page.getByText(new RegExp(`→\\s*https://172\\.16\\.30\\.6:443${NEW_PATH.replace(/\//g, "\\/")}`))).toBeVisible();
  await shot("pathfix-01-path-edited");
});

test("pathfix-step02 save then redeploy", async () => {
  test.setTimeout(300_000);
  await page.getByRole("button", { name: "Save", exact: true }).click();
  await expect(page.getByText("Draft saved")).toBeVisible();
  await expect(page.getByText(/Deploy unavailable/)).toHaveCount(0);
  await shot("pathfix-02a-saved");

  await page.getByRole("button", { name: /More/ }).click();
  await page.getByRole("menuitem", { name: "Redeploy" }).click();

  await expect(page.getByText("Redeployed")).toBeVisible({ timeout: 240_000 });
  await shot("pathfix-02b-redeployed");
});
