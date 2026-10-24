'use strict';

/**
 * AO CRM Brief Me — mobile viewport regression.
 * Ensures one scroll container owns the full brief and the first/last
 * brief blocks are geometrically visible at scroll extremes.
 */

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');

const ROOT = path.join(__dirname, '..');
const MOBILE_VIEWPORTS = [
  { name: 'iphone-narrow', width: 360, height: 740 },
  { name: 'iphone-standard', width: 390, height: 844 },
  { name: 'iphone-max', width: 430, height: 932 },
];

function contentType(filePath) {
  if (filePath.endsWith('.css')) return 'text/css; charset=utf-8';
  if (filePath.endsWith('.html')) return 'text/html; charset=utf-8';
  return 'application/octet-stream';
}

function startStaticServer() {
  const server = http.createServer((req, res) => {
    const urlPath = decodeURIComponent((req.url || '/').split('?')[0]);
    let abs;
    if (urlPath === '/') {
      abs = path.join(ROOT, 'test/fixtures/ao-crm-brief-viewport.html');
    } else if (urlPath.startsWith('/shared/')) {
      abs = path.join(ROOT, 'public', urlPath.slice(1));
    } else if (urlPath.startsWith('/fixtures/')) {
      abs = path.join(ROOT, 'test', urlPath.slice(1));
    } else {
      res.writeHead(404);
      res.end('not found');
      return;
    }
    if (!abs.startsWith(ROOT) || !fs.existsSync(abs)) {
      res.writeHead(404);
      res.end('not found');
      return;
    }
    res.writeHead(200, { 'Content-Type': contentType(abs) });
    fs.createReadStream(abs).pipe(res);
  });
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      resolve({
        base: `http://127.0.0.1:${port}`,
        async close() {
          await new Promise((r) => server.close(r));
        },
      });
    });
  });
}

describe('AO CRM Brief Me mobile layout', () => {
  let server;
  let browser;
  let puppeteer;

  before(async () => {
    try {
      puppeteer = require('puppeteer');
    } catch (_err) {
      throw Object.assign(new Error('puppeteer not installed'), { code: 'ERR_TEST_SKIP' });
    }
    server = await startStaticServer();
    try {
      browser = await puppeteer.launch({
        headless: true,
        args: ['--no-sandbox', '--disable-setuid-sandbox'],
      });
    } catch (err) {
      if (/Could not find Chrome/i.test(String(err && err.message))) {
        await server.close();
        server = null;
        throw Object.assign(new Error('Chrome not available — skipped'), { code: 'ERR_TEST_SKIP' });
      }
      throw err;
    }
  });

  after(async () => {
    if (browser) await browser.close();
    if (server) await server.close();
  });

  it('ao-crm.html uses a single brief-panel scroll surface', () => {
    const crm = fs.readFileSync(path.join(ROOT, 'public', 'ao-crm.html'), 'utf8');
    assert.match(crm, /\.brief-modal/);
    assert.match(crm, /\.brief-panel/);
    assert.match(crm, /brief-section-body/);
    assert.match(crm, /resetBriefScrollPanel/);
    assert.doesNotMatch(crm, /modal-brief-scroll/);
  });

  for (const vp of MOBILE_VIEWPORTS) {
    it(`Curtin-style long brief: top and bottom visible at ${vp.name} (${vp.width}x${vp.height})`, async () => {
      const page = await browser.newPage();
      await page.setViewport({ width: vp.width, height: vp.height, deviceScaleFactor: 1 });
      await page.goto(`${server.base}/`, { waitUntil: 'domcontentloaded' });
      await page.waitForSelector('#brief-panel');

      const top = await page.evaluate(() => window.__measureAoBriefLayout());
      assert.equal(top.scrollTop, 0, 'brief panel should open at scrollTop 0');
      assert.equal(top.nestedScrollerCount, 0, 'brief must not use nested scroll regions');
      assert.ok(Number(top.backdropZ) > Number(top.navZ), 'brief modal must stack above AO shell nav');
      assert.equal(top.firstClippedAbove, false, 'first brief text must not sit above scroll container top');
      assert.match(top.firstTextPrefix, /^No prior conversation or decision-maker is logged yet/);

      await page.evaluate(() => window.__scrollBriefToBottom());
      const bottom = await page.evaluate(() => window.__measureBriefBottom());
      assert.equal(bottom.lastClippedBelow, false, 'last brief text must be visible at bottom scroll');

      await page.close();
    });
  }
});
