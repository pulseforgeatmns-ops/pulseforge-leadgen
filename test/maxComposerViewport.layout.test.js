'use strict';

/**
 * SPEC-099 — live layout regression for Max Workspace composer clipping.
 * Uses Puppeteer to assert Ask Max input + Send stay fully inside the
 * modal viewport with long conversation, evidence, and many suggestion chips.
 */

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { pathToFileURL } = require('node:url');

const ROOT = path.join(__dirname, '..');
const VIEWPORTS = [
  { name: 'macbook-13', width: 1280, height: 800 },
  { name: 'short-laptop', width: 1280, height: 700 },
  { name: 'short-wide', width: 1440, height: 720 },
];

function contentType(filePath) {
  if (filePath.endsWith('.css')) return 'text/css; charset=utf-8';
  if (filePath.endsWith('.js')) return 'application/javascript; charset=utf-8';
  if (filePath.endsWith('.html')) return 'text/html; charset=utf-8';
  return 'application/octet-stream';
}

function startStaticServer() {
  const server = http.createServer((req, res) => {
    const urlPath = decodeURIComponent((req.url || '/').split('?')[0]);
    let rel = urlPath === '/' ? '/fixtures/max-composer-viewport.html' : urlPath;
    if (rel.startsWith('/shared/')) {
      rel = path.join('public', rel);
    } else if (rel.startsWith('/command-deck/')) {
      rel = path.join('public', rel);
    } else if (rel.startsWith('/fixtures/')) {
      rel = path.join('test', rel.slice(1));
    } else {
      res.writeHead(404);
      res.end('not found');
      return;
    }
    const abs = path.join(ROOT, rel);
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

describe('SPEC-099 Max composer live viewport layout', () => {
  let server;
  let browser;
  let puppeteer;

  before(async () => {
    try {
      puppeteer = require('puppeteer');
    } catch (err) {
      throw Object.assign(new Error('puppeteer not installed'), { code: 'ERR_TEST_SKIP' });
    }
    server = await startStaticServer();
    try {
      browser = await puppeteer.launch({
        headless: true,
        args: ['--no-sandbox', '--disable-setuid-sandbox'],
      });
    } catch (err) {
      // CI images may lack the downloaded Chrome binary — CSS static tests still guard the contract.
      if (/Could not find Chrome/i.test(String(err && err.message))) {
        await server.close();
        server = null;
        throw Object.assign(
          new Error('Chrome not available for Puppeteer layout harness — skipped'),
          { code: 'ERR_TEST_SKIP' }
        );
      }
      throw err;
    }
  });

  after(async () => {
    if (browser) await browser.close();
    if (server) await server.close();
  });

  for (const vp of VIEWPORTS) {
    it(`keeps composer fully visible at ${vp.name} (${vp.width}x${vp.height})`, async () => {
      const page = await browser.newPage();
      await page.setViewport({ width: vp.width, height: vp.height, deviceScaleFactor: 1 });
      await page.goto(`${server.base}/fixtures/max-composer-viewport.html`, {
        waitUntil: 'networkidle0',
      });
      await page.waitForSelector('#maxWorkspace:not([hidden])');
      await page.waitForSelector('#mxAskInput');
      await page.waitForSelector('#mxAskSend');

      // Stress: long reply + evidence + many chips
      await page.evaluate(() => {
        window.__populateMaxStressFixture();
      });

      const metrics = await page.evaluate(() => {
        const workspace = document.getElementById('maxWorkspace');
        const panel = document.querySelector('.mx-panel');
        const thread = document.getElementById('mxThread');
        const dock = document.querySelector('.mx-composer-dock');
        const suggestions = document.getElementById('mxSuggestions');
        const input = document.getElementById('mxAskInput');
        const send = document.getElementById('mxAskSend');
        const vh = window.innerHeight;
        const vw = window.innerWidth;

        function box(el) {
          const r = el.getBoundingClientRect();
          const cs = window.getComputedStyle(el);
          return {
            top: r.top,
            left: r.left,
            right: r.right,
            bottom: r.bottom,
            width: r.width,
            height: r.height,
            overflowY: cs.overflowY,
            overflow: cs.overflow,
          };
        }

        return {
          vh,
          vw,
          workspace: box(workspace),
          panel: box(panel),
          thread: box(thread),
          dock: box(dock),
          suggestions: box(suggestions),
          input: box(input),
          send: box(send),
          panelBoxSizing: window.getComputedStyle(panel).boxSizing,
          threadScrollable: thread.scrollHeight > thread.clientHeight + 4,
        };
      });

      // Modal panel fits inside viewport
      assert.ok(metrics.panel.top >= -0.5, 'panel top in viewport');
      assert.ok(
        metrics.panel.bottom <= metrics.vh + 0.5,
        `panel bottom ${metrics.panel.bottom} must be <= vh ${metrics.vh}`
      );
      assert.equal(metrics.panelBoxSizing, 'border-box');

      // Composer input fully visible (all four sides inside viewport)
      assert.ok(metrics.input.top >= 0, 'input top visible');
      assert.ok(metrics.input.left >= 0, 'input left visible');
      assert.ok(metrics.input.right <= metrics.vw + 0.5, 'input right visible');
      assert.ok(
        metrics.input.bottom <= metrics.vh + 0.5,
        `input bottom ${metrics.input.bottom} clipped below vh ${metrics.vh}`
      );
      assert.ok(metrics.input.height > 20, 'input has height');

      // Send fully visible
      assert.ok(metrics.send.top >= 0, 'send top visible');
      assert.ok(
        metrics.send.bottom <= metrics.vh + 0.5,
        `send bottom ${metrics.send.bottom} clipped below vh ${metrics.vh}`
      );
      assert.ok(metrics.send.right <= metrics.vw + 0.5, 'send right visible');

      // Dock is not a second vertical scroll region
      assert.notEqual(metrics.dock.overflowY, 'auto');
      assert.notEqual(metrics.dock.overflowY, 'scroll');
      assert.notEqual(metrics.suggestions.overflowY, 'auto');
      assert.notEqual(metrics.suggestions.overflowY, 'scroll');

      // Conversation remains independently scrollable under stress
      assert.equal(metrics.thread.overflowY, 'auto');
      assert.ok(metrics.threadScrollable, 'thread should scroll with long content');

      // No need to scroll page/workspace to reach composer
      assert.ok(metrics.dock.bottom <= metrics.vh + 0.5, 'dock fully in viewport');

      await page.close();
    });
  }
});
