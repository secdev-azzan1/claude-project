// Playwright config for the UI E2E verification journey.
// Run from frontend/:  npx playwright test -c e2e/playwright.config.ts
import { defineConfig } from "playwright/test";

export default defineConfig({
  testDir: ".",
  outputDir: "./artifacts/test-output",
  // Single long journey with real deploys + a cron wait — generous budgets.
  timeout: 300_000,
  expect: { timeout: 20_000 },
  fullyParallel: false,
  workers: 1,
  retries: 0,
  reporter: [["list"]],
  use: {
    baseURL: "http://localhost:3001",
    headless: true,
    screenshot: "on",
    trace: "retain-on-failure",
    viewport: { width: 1680, height: 1000 },
    actionTimeout: 30_000,
    navigationTimeout: 60_000,
  },
});
