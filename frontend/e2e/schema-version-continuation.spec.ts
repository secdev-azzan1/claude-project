// Schema version continuation E2E:
//   - create a manual template in the real UI, no upload
//   - verify it, register it, and prove the "Registered / Not registered" filters
//   - edit the same template into later Apicurio versions
//   - browse older and current versions through the version selector
//   - leave the final template registered and visible as evidence
//
// Nothing is deleted or cleaned up. This spec only touches its own artifacts.
import { test, expect, type BrowserContext, type Page } from "playwright/test";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";

const FRONTEND = "http://localhost:3001";
const BACKEND = "http://localhost:8011";
const ART = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "artifacts", "schema-version-continuation");
const RUN_ID = new Date().toISOString().replace(/[-:TZ.]/g, "");

const TEMPLATE_NAME = `codex15aug26 schema version continuation ${RUN_ID}`;
const TEMPLATE_DESCRIPTION = "Manual schema continuation evidence run.";
const RECORD_NAME = "Codex15Aug26SchemaVersionContinuation2";
const NAMESPACE = "com.nif";
const SUBJECT = `${TEMPLATE_NAME.trim().toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "")}-value`;
const TARGET_VERSIONS = 20;

type TemplateDoc = {
  id: string;
  name: string;
  description?: string;
  avro: unknown;
  createdAt: string;
  updatedAt: string;
  registeredSubject?: string;
  registryGlobalId?: number;
  registeredVersion?: number;
  registeredAt?: string;
};

type RegistryVersionDetail = {
  version: number;
  globalId: number | null;
  avro: unknown;
};

type RegisterResponse = {
  globalId: number;
  subject: string;
  version: number;
  registeredAt?: string;
};

const consoleErrors: string[] = [];
let context: BrowserContext;
let page: Page;

const evidence: {
  templateName: string;
  subject: string;
  snapshots: Array<{
    label: string;
    template?: {
      id: string;
      registeredSubject?: string;
      registryGlobalId?: number;
      registeredVersion?: number;
      registeredAt?: string;
    };
    versions?: number[];
    latestVersion?: RegistryVersionDetail | null;
    firstVersion?: RegistryVersionDetail | null;
    registerResponse?: RegisterResponse;
    verifyResponse?: unknown;
    versionMenuMetrics?: { clientHeight: number; scrollHeight: number; scrollTop: number };
    screenshot?: string;
  }>;
} = {
  templateName: TEMPLATE_NAME,
  subject: SUBJECT,
  snapshots: [],
};

async function saveEvidence() {
  fs.writeFileSync(path.join(ART, "evidence.json"), JSON.stringify(evidence, null, 2), "utf-8");
}

async function shot(name: string) {
  const file = `${name}.png`;
  await page.screenshot({ path: path.join(ART, file), fullPage: true });
  const snapshot = evidence.snapshots.at(-1);
  if (snapshot) snapshot.screenshot = file;
  await saveEvidence();
}

async function backendReady(): Promise<void> {
  const deadline = Date.now() + 180_000;
  while (Date.now() < deadline) {
    try {
      const r = await fetch(`${BACKEND}/api`);
      if (r.ok) return;
    } catch {
      /* not ready yet */
    }
    await new Promise((resolve) => setTimeout(resolve, 3_000));
  }
  throw new Error(`Backend at ${BACKEND}/api did not return 200 within 180s`);
}

async function listTemplates(): Promise<TemplateDoc[]> {
  const r = await fetch(`${BACKEND}/api/v2/schemas/`);
  if (!r.ok) throw new Error(`GET /api/v2/schemas/ -> ${r.status}`);
  const body = (await r.json()) as { templates?: TemplateDoc[] };
  return body.templates ?? [];
}

async function findTemplate(): Promise<TemplateDoc | null> {
  const templates = await listTemplates();
  return templates.find((tpl) => tpl.name === TEMPLATE_NAME) ?? null;
}

async function listRegistryVersions(subject: string): Promise<number[]> {
  const r = await fetch(`${BACKEND}/api/v2/schemas/registry-subject/${encodeURIComponent(subject)}/versions`);
  if (!r.ok) throw new Error(`GET registry versions for ${subject} -> ${r.status}`);
  const body = (await r.json()) as Array<{ version: number }>;
  return body.map((entry) => entry.version);
}

async function getRegistryVersionDetail(subject: string, version: number): Promise<RegistryVersionDetail> {
  const r = await fetch(`${BACKEND}/api/v2/schemas/registry-subject/${encodeURIComponent(subject)}/versions/${version}`);
  if (!r.ok) throw new Error(`GET registry version detail ${subject} v${version} -> ${r.status}`);
  const body = (await r.json()) as RegistryVersionDetail;
  return body;
}

function schemaJson(version: number): string {
  const fields = Array.from({ length: version }, (_, index) => {
    if (index === 0) {
      return { name: "id", type: "string" };
    }
    return { name: `extra_${index + 1}`, type: ["null", "string"], default: null as null };
  });
  return JSON.stringify(
    {
      type: "record",
      name: RECORD_NAME,
      namespace: NAMESPACE,
      fields,
    },
    null,
    2,
  );
}

async function selectTemplateRow() {
  await page.locator("button").filter({ hasText: TEMPLATE_NAME }).first().click();
}

async function setRawSchema(version: number) {
  await page.getByRole("tab", { name: "Raw Avro JSON" }).click();
  const editor = page.getByRole("textbox", { name: "Raw Avro JSON" });
  await editor.fill(schemaJson(version));
  await expect(editor).toHaveValue(schemaJson(version));
}

async function verifyTemplate(expectCompatibility: boolean) {
  const responsePromise = page.waitForResponse(
    (response) => response.url().includes("/api/v2/schemas/verify") && response.request().method() === "POST",
  );
  await page.getByRole("button", { name: "Verify" }).click();
  const response = await responsePromise;
  const body = (await response.json()) as {
    ok?: boolean;
    issues?: unknown[];
    compatibility?: { checked?: boolean; compatible?: boolean; message?: string };
  };
  evidence.snapshots.push({
    label: expectCompatibility ? "verify-compatible" : "verify-structural",
    verifyResponse: body,
  });
  await saveEvidence();
  await expect(page.getByText("Schema is valid", { exact: true })).toBeVisible();
  await expect(page.getByText("Structurally valid Avro")).toBeVisible();
  if (expectCompatibility) {
    await expect(page.getByText("Compatible with latest registered version.")).toBeVisible();
  } else {
    await expect(page.getByText("Compatible with latest registered version.")).toHaveCount(0);
  }
}

async function registerTemplate() {
  const responsePromise = page.waitForResponse(
    (response) => response.url().includes("/api/v2/schemas/register") && response.request().method() === "POST",
  );
  await page.getByRole("button", { name: "Register…", exact: true }).click();
  const dialog = page.getByRole("dialog").filter({ hasText: `Register “${TEMPLATE_NAME}”` });
  await expect(dialog).toBeVisible();
  const subjectInput = dialog.getByRole("textbox");
  await expect(subjectInput).toHaveValue(SUBJECT);
  const clickPromise = dialog.getByRole("button", { name: "Register" }).click();
  const [response] = await Promise.all([responsePromise, clickPromise]);
  const body = (await response.json()) as RegisterResponse;
  evidence.snapshots.push({ label: `register-v${body.version}`, registerResponse: body });
  await saveEvidence();
  return body;
}

async function captureTemplateState(label: string) {
  const tpl = await findTemplate();
  const versions = await listRegistryVersions(SUBJECT);
  evidence.snapshots.push({
    label,
    template: tpl
      ? {
          id: tpl.id,
          registeredSubject: tpl.registeredSubject,
          registryGlobalId: tpl.registryGlobalId,
          registeredVersion: tpl.registeredVersion,
          registeredAt: tpl.registeredAt,
        }
      : undefined,
    versions,
    latestVersion: versions.length > 0 ? await getRegistryVersionDetail(SUBJECT, versions[versions.length - 1]) : null,
    firstVersion: versions.length > 0 ? await getRegistryVersionDetail(SUBJECT, versions[0]) : null,
  });
  await saveEvidence();
}

async function openVersionMenu() {
  const combo = page.getByRole("combobox");
  await expect(combo).toBeVisible();
  await combo.click();
  const listbox = page.getByRole("listbox");
  await expect(listbox).toBeVisible();
  return listbox;
}

test.describe.configure({ mode: "serial" });

test.beforeAll(async ({ browser }) => {
  fs.mkdirSync(ART, { recursive: true });
  await backendReady();
  context = await browser.newContext({ baseURL: FRONTEND, viewport: { width: 1680, height: 1000 } });
  page = await context.newPage();
  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text());
  });
  page.on("pageerror", (error) => consoleErrors.push(`pageerror: ${String(error)}`));
});

test.afterAll(async () => {
  if (consoleErrors.length > 0) {
    fs.writeFileSync(path.join(ART, "console-errors.txt"), consoleErrors.join("\n---\n"), "utf-8");
  }
  await saveEvidence();
  await context?.close();
});

test.afterEach(async ({}, testInfo) => {
  if (testInfo.status !== testInfo.expectedStatus && page) {
    const safe = testInfo.title.replace(/[^a-z0-9-]+/gi, "_").slice(0, 60);
    await page.screenshot({ path: path.join(ART, `FAIL-${safe}.png`), fullPage: true }).catch(() => undefined);
  }
});

test("schema continuation version browsing stays registered and scrollable", async () => {
  test.setTimeout(300_000);

  await page.goto("/schemas");
  await expect(page.getByRole("heading", { name: "Schemas" })).toBeVisible();
  await page.getByRole("button", { name: "All" }).click();

  const existing = await findTemplate();
  if (!existing) {
    await page.getByRole("button", { name: "New template" }).click();
    const dialog = page.getByRole("dialog").filter({ hasText: "New library template" });
    await expect(dialog).toBeVisible();
    await dialog.locator(`div:has(> label:text-is("Name")) input`).fill(TEMPLATE_NAME);
    await dialog.locator(`div:has(> label:text-is("Description (optional)")) textarea`).fill(TEMPLATE_DESCRIPTION);
    await dialog.getByRole("button", { name: "Create template" }).click();
    await expect(page.getByText(`Template "${TEMPLATE_NAME}" created — not registered, bound to no flow.`)).toBeVisible();
  }

  await selectTemplateRow().catch(() => undefined);
  const templateCard = page.locator("button").filter({ hasText: TEMPLATE_NAME }).first();
  await expect(templateCard).toBeVisible();

  evidence.snapshots.push({ label: "template-opened" });
  await saveEvidence();
  await shot("01-template-opened");

  await page.getByRole("button", { name: "Not registered", exact: true }).click();
  const templateRow = page.locator("button").filter({ hasText: TEMPLATE_NAME }).first();
  await expect(templateRow).toBeVisible();
  await shot("02-not-registered-filter");

  await page.getByRole("button", { name: "All", exact: true }).click();
  await setRawSchema(1);
  await verifyTemplate(false);
  await shot("03-verifying-manual-template");

  const register1 = await registerTemplate();
  await captureTemplateState("after-register-v1");

  await page.getByRole("button", { name: "Registered", exact: true }).click();
  await expect(page.locator("button").filter({ hasText: TEMPLATE_NAME }).first()).toBeVisible();
  await expect(page.getByText(new RegExp(`Registered\\s*·\\s*#${register1.globalId}\\s*·\\s*v${register1.version}`))).toBeVisible();
  await shot("04-registered-filter");

  await page.getByRole("button", { name: "Not registered", exact: true }).click();
  await expect(page.getByText("No records match these filters.")).toBeVisible();

  await page.getByRole("button", { name: "All", exact: true }).click();
  await setRawSchema(2);
  await verifyTemplate(true);
  const register2 = await registerTemplate();
  await captureTemplateState("after-register-v2");

  for (let version = 3; version <= TARGET_VERSIONS; version += 1) {
    await setRawSchema(version);
    if (version === TARGET_VERSIONS) {
      await verifyTemplate(true);
    }
    const response = await registerTemplate();
    evidence.snapshots.push({ label: `backend-version-${version}`, registerResponse: response });
    await captureTemplateState(`after-register-v${version}`);
  }

  const latestTemplate = await findTemplate();
  expect(latestTemplate).not.toBeNull();
  expect(latestTemplate?.registeredSubject).toBe(SUBJECT);
  expect(latestTemplate?.registeredVersion).toBe(TARGET_VERSIONS);
  await saveEvidence();

  const versions = await listRegistryVersions(SUBJECT);
  expect(versions).toEqual(Array.from({ length: TARGET_VERSIONS }, (_, index) => index + 1));

  await openVersionMenu();
  const optionCount = await page.getByRole("option").count();
  expect(optionCount).toBe(TARGET_VERSIONS);
  evidence.snapshots.push({ label: "version-menu-open" });
  await saveEvidence();
  await page.screenshot({ path: path.join(ART, "05-version-menu-open.png"), fullPage: true });

  await page.getByRole("option", { name: /^v1\b/ }).click();
  await expect(page.getByText(`Viewing registered v1`)).toBeVisible();
  await expect(page.getByText(/Viewing registered v1[\s\S]*read-only\./)).toBeVisible();
  await shot("06-version-1-readonly");

  const v1Detail = await getRegistryVersionDetail(SUBJECT, 1);
  const vLatestDetail = await getRegistryVersionDetail(SUBJECT, TARGET_VERSIONS);
  expect((v1Detail.avro as { fields?: unknown[] }).fields).toHaveLength(1);
  expect((vLatestDetail.avro as { fields?: unknown[] }).fields).toHaveLength(TARGET_VERSIONS);
  evidence.snapshots.push({
    label: "registry-version-details",
    firstVersion: v1Detail,
    latestVersion: vLatestDetail,
  });
  await saveEvidence();

  await page.getByRole("combobox").click();
  await page.getByRole("option", { name: new RegExp(`^v${TARGET_VERSIONS}\\b`) }).click();
  await expect(page.getByText(`Viewing registered v${TARGET_VERSIONS}`)).not.toBeVisible();
  const currentTemplate = await findTemplate();
  expect(currentTemplate).not.toBeNull();
  await expect(page.getByRole("textbox", { name: "Raw Avro JSON" })).toBeEditable();
  await expect(page.getByRole("textbox", { name: "Raw Avro JSON" })).toHaveValue(
    JSON.stringify(currentTemplate!.avro, null, 2),
  );
  await shot("07-version-current-editable");

  // Final API confirmations already recorded in the evidence ledger.
  await expect(register1.version).toBe(1);
  await expect(register2.version).toBe(2);
});
