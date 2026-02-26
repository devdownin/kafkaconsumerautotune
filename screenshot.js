const { chromium } = require('playwright');

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage();
  await page.goto('http://localhost:8080/');
  await page.waitForTimeout(5000); // Wait for stats to update
  await page.screenshot({ path: '/home/jules/verification/dashboard_traffic.png', fullPage: true });
  await browser.close();
})();
