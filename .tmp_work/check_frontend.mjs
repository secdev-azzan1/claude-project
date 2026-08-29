// Confirm the new flow renders in the running frontend (vite dev on :3001).
import { chromium } from 'playwright';

const OUT = '.tmp_work/frontend-pgdemo';

const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1500, height: 950 } });
const errors = [];
page.on('console', (m) => { if (m.type() === 'error') errors.push(m.text()); });

await page.goto('http://localhost:3001/flows', { waitUntil: 'networkidle', timeout: 45000 });
await page.waitForTimeout(2500);
await page.screenshot({ path: `${OUT}-list.png`, fullPage: false });

const bodyText = await page.locator('body').innerText();
console.log('FLOW NAME VISIBLE IN LIST:', bodyText.includes('FortiSIEM POST Pagination Demo'));

// Open the flow itself.
const link = page.getByText('FortiSIEM POST Pagination Demo', { exact: false }).first();
if (await link.count()) {
  await link.click();
  await page.waitForTimeout(3500);
  await page.screenshot({ path: `${OUT}-detail.png`, fullPage: false });
  const detail = await page.locator('body').innerText();
  console.log('DETAIL shows block name :', detail.includes('Query CMDB'));
  console.log('DETAIL shows pagination :', /pagination/i.test(detail));
  console.log('DETAIL shows topic      :', detail.includes('raw.fortisiem_post_pagination_demo.cmdb_user'));
  console.log('--- detail text sample ---');
  console.log(detail.slice(0, 1200));
} else {
  console.log('FLOW LINK NOT FOUND on /flows');
  console.log('--- list text sample ---');
  console.log(bodyText.slice(0, 1200));
}

console.log('CONSOLE ERRORS:', errors.length ? errors.slice(0, 5) : 'none');
await browser.close();
