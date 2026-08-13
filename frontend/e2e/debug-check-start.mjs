import { chromium } from "playwright";

const run = async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ baseURL: "http://localhost:3001", viewport: { width: 1680, height: 1000 } });
  await page.goto("/flow-builder/flow-loypce");
  await page.waitForTimeout(5000);
  const startBtn = page.locator('button:text-is("Start")').first();
  const box = await startBtn.boundingBox();
  console.log("box:", JSON.stringify(box));
  if (box) {
    await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
    await page.waitForTimeout(1500);
    const tips = await page.locator('[role="tooltip"]').allTextContents();
    console.log("tooltips:", JSON.stringify(tips));
  }
  // Also: what does the flows page row say?
  await page.goto("/flows");
  await page.waitForTimeout(3000);
  const row = page.getByRole("row").filter({ hasText: "dt json products" }).first();
  const rowButtons = await row.locator("button").evaluateAll((els) =>
    els.map((e) => ({ aria: e.getAttribute("aria-label"), disabled: e.disabled })),
  );
  console.log("row buttons:", JSON.stringify(rowButtons));
  await browser.close();
};
run();
