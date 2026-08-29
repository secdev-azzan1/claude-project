// Part 1 Step B continuation: the throwaway FortiSIEM Pagination Test flow
// (flow-9d7ask, built via fortisiem-pagination-build.spec.ts) deployed clean
// but every live InvokeHTTP request failed with
// javax.net.ssl.SSLHandshakeException (PKIX path building failed) — FortiSIEM
// at https://172.16.30.6:443 presents a self-signed certificate that NiFi's
// default JVM trust store cannot validate. Confirmed via the NiFi bulletin
// board (read-only REST call) — not a pagination bug.
//
// The app has a purpose-built, self-serve UI feature for exactly this case:
// an APISIX "gateway egress" proxy. frontend/src/pages/Apisix.tsx's own
// client-cert-profile hint says it outright: "Gateway mode does not verify
// the upstream server certificate. Egress only." And
// ServiceFormFields.tsx's ProxyField describes the no-proxy default as "For
// endpoints NiFi refuses (broken or nonstandard TLS)". Binding the FortiSIEM
// Test service to a reconciled, allowlisted proxy routes InvokeHTTP's target
// through APISIX (#{apisix_runtime_url}/<proxy_token>/query/cmdb) instead of
// straight to 172.16.30.6 — APISIX does the TLS handshake to FortiSIEM on
// NiFi's behalf, sidestepping the untrusted cert entirely. This is a real,
// first-class app feature — not a workaround.
//
// This spec: create+reconcile the proxy, allowlist the host (admin action),
// bind the existing "FortiSIEM Test" service to it (new revision), all
// through the UI. The flow itself needs no changes — block_proxy_id() reads
// the proxy from the SERVICE's own config, so a plain Redeploy (already
// stopped via backend verb) picks up the new routing at compile time.
import { test, expect, type BrowserContext, type Page } from "playwright/test";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";

const FRONTEND = "http://localhost:3001";
const ART = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "artifacts");

const PROXY_NAME = "FortiSIEM Egress";
const TARGET_HOST = "172.16.30.6";
const SERVICE_NAME = "FortiSIEM Test";
const FLOW_NAME = "FortiSIEM Pagination Test";

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
    await page.screenshot({ path: path.join(ART, `PFX-FAIL-${safe}.png`), fullPage: true }).catch(() => undefined);
  }
});

// ---------------------------------------------------------------- Step 1
test("pfx-step01 create APISIX egress proxy targeting FortiSIEM (172.16.30.6:443)", async () => {
  await page.goto("/apisix");
  await page.getByRole("button", { name: "Add Proxy" }).click();

  const dialog = page.getByRole("dialog");
  await expect(dialog.getByText("Add APISIX Proxy")).toBeVisible();

  await dialog.locator('div:has(> label:text-is("Name")) input').fill(PROXY_NAME);
  await dialog.locator('div:has(> label:text-is("Target host")) input').fill(TARGET_HOST);
  // Port stays at its "443" default; Route path prefix stays at its "/"
  // default (matches everything under the proxy's own route token, which is
  // enough for our single POST /query/cmdb call).

  // Allow POST — the form defaults to GET only, and FortiSIEM's /query/cmdb
  // is POST-only.
  await dialog.getByRole("button", { name: "POST", exact: true }).click();

  await shot("pfx-01a-proxy-form");
  await dialog.getByRole("button", { name: "Create Proxy" }).click();
  await expect(page.getByText(`Proxy "${PROXY_NAME}" created`)).toBeVisible();
  await shot("pfx-01b-proxy-created");
});

/** The proxy's own card — scoped, since an unrelated pre-existing proxy
 * ("gw dummyjson") also renders a "Reconcile" button on the same page. */
function proxyCard(pageArg: Page) {
  return pageArg.locator("div.bg-card").filter({ hasText: PROXY_NAME }).first();
}

// ---------------------------------------------------------------- Step 2
test("pfx-step02 allowlist 172.16.30.6 (admin action)", async () => {
  const card = proxyCard(page);
  await expect(card.getByText(`${TARGET_HOST} is `)).toBeVisible();
  await card.getByRole("button", { name: "Add to allowlist (admin)" }).click();

  const confirmDlg = page.getByRole("alertdialog").filter({ hasText: "Administrator action" });
  await expect(confirmDlg).toBeVisible();
  await expect(confirmDlg.getByText(TARGET_HOST)).toBeVisible();
  await confirmDlg.getByRole("button", { name: /Confirm as admin — add host/ }).click();

  await expect(page.getByText(`"${TARGET_HOST}" added to the gateway allowlist`)).toBeVisible();
  await shot("pfx-02-allowlisted");
});

// ---------------------------------------------------------------- Step 3
test("pfx-step03 reconcile the proxy onto the live APISIX gateway", async () => {
  await page.goto("/apisix");
  const card = proxyCard(page);
  await card.getByRole("button", { name: "Reconcile" }).click();
  await expect(page.getByText(`"${PROXY_NAME}" is live on the gateway`)).toBeVisible({ timeout: 30_000 });
  await expect(card.getByText("Reconciled and allowlisted")).toBeVisible();
  await shot("pfx-03-reconciled");
});

// ---------------------------------------------------------------- Step 4
test("pfx-step04 bind FortiSIEM Test service to the reconciled proxy", async () => {
  await page.goto("/application-services");
  const card = page.locator("div.bg-card").filter({ hasText: SERVICE_NAME }).filter({ hasNotText: "Retired" }).first();
  await card.getByRole("button", { name: "Edit" }).click();

  const dialog = page.getByRole("dialog");
  await expect(dialog.getByText(`Edit ${SERVICE_NAME}`)).toBeVisible();

  await dialog.getByRole("combobox").filter({ hasText: /No proxy|call the host directly/ }).click();
  await page.getByRole("option", { name: new RegExp(PROXY_NAME) }).click();
  await shot("pfx-04a-proxy-bound");

  await dialog.getByRole("button", { name: /Save as revision/ }).click();
  await expect(page.getByText(/Revision \d+ created/)).toBeVisible();
  await shot("pfx-04b-revision-saved");
});

// ---------------------------------------------------------------- Step 5
test("pfx-step05 redeploy the FortiSIEM flow to pick up the new proxy routing", async () => {
  test.setTimeout(300_000);
  await page.goto("/flows");
  // Flow name text in the table is plain (non-clickable) text — the actual
  // navigation trigger is the row's "Edit flow" icon button (Flows.tsx line
  // ~2421, aria-label="Edit flow", onClick navigates to /flow-builder/:id).
  // Filter via the search box first to keep the row locator unambiguous
  // among all 36 flows.
  await page.getByPlaceholder("Search flows, entities, topics…").fill(FLOW_NAME);
  const row = page.getByRole("row", { name: new RegExp(FLOW_NAME) });
  await expect(row).toBeVisible();
  await row.getByRole("button", { name: "Edit flow" }).click();
  await expect(page).toHaveURL(/\/flow-builder\/[\w-]+/);

  // Redeploy requires the flow Stopped (verbReason in prototype/api.ts) —
  // already stopped via a direct backend verb call ahead of this UI step.
  await expect(page.locator('span[aria-label="Stopped"]').first()).toBeVisible({ timeout: 30_000 });

  await page.getByRole("button", { name: /More/ }).click();
  await page.getByRole("menuitem", { name: "Redeploy" }).click();

  await expect(page.getByText("Redeployed")).toBeVisible({ timeout: 240_000 });
  await shot("pfx-05-redeployed");
});
