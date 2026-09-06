const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage();
  await page.goto('file:///C:/Users/kaifm/Desktop/Data Mobility - Overview copy/index.html', { waitUntil: 'networkidle' });
  await page.pdf({
    path: 'C:/Users/kaifm/Desktop/Data Mobility - Overview copy/Data Mobility - Platform Overview copy.pdf',
    width: '1600px',
    height: '900px',
    printBackground: true,
    margin: { top: '0', bottom: '0', left: '0', right: '0' }
  });
  await browser.close();
  console.log('PDF written');
})();
