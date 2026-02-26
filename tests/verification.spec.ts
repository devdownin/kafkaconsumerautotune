import { test, expect } from '@playwright/test';

test('capture dashboard', async ({ page }) => {
  await page.goto('http://localhost:8080/');
  await page.waitForTimeout(2000); // Wait for animations
  await page.screenshot({ path: '/home/jules/verification/dashboard_with_traffic.png', fullPage: true });
});
