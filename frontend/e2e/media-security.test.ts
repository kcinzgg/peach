import assert from 'node:assert/strict';
import { after, before, describe, it } from 'node:test';
import type { Browser } from 'playwright-core';
import { launch, requiredEnv, VIEWPORTS } from './harness.ts';

describe('媒体文档隔离', () => {
  let browser: Browser;
  before(async () => { browser = await launch() });
  after(async () => { await browser?.close() });
  for (const viewport of VIEWPORTS) {
    it(`${viewport.name}：图片响应策略禁止 SVG 文档脚本执行`, async () => {
      const origin = requiredEnv('PEACH_E2E_ORIGIN');
      const context = await browser.newContext({ viewport: { width: viewport.width, height: viewport.height } });
      try {
        const actual = await context.request.get(`${origin}/site-icon/github.png`);
        assert.equal(actual.status(), 200);
        const policy = actual.headers()['content-security-policy'];
        assert.ok(policy?.includes('sandbox'));
        const page = await context.newPage();
        const document = '<svg xmlns="http://www.w3.org/2000/svg"><script>window.mediaAuditExecuted = true</script><text y="20">媒体夹具</text></svg>';
        await page.route('**/security-fixture.svg*', route => route.fulfill({
          contentType: 'image/svg+xml', body: document,
          headers: route.request().url().endsWith('?control') ? {} : {
            'Content-Security-Policy': policy, 'X-Content-Type-Options': 'nosniff',
          },
        }));
        await page.goto(`${origin}/security-fixture.svg?control`);
        assert.equal(await page.evaluate(() => window['mediaAuditExecuted']), true);
        await page.goto(`${origin}/security-fixture.svg`);
        assert.equal(await page.evaluate(() => window['mediaAuditExecuted']), undefined);
      } finally { await context.close() }
    });
  }
});
