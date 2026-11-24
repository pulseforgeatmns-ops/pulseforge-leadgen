'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const puppeteer = require('puppeteer');
const { createServer } = require('./server.cjs');
async function setup(t, touch = false) {
  const server = createServer();
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const browser = await puppeteer.launch({ executablePath: process.env.CHROMIUM_PATH || undefined, headless: true, args: ['--no-sandbox', '--disable-dev-shm-usage'] });
  t.after(() => browser.close());
  const page = await browser.newPage();
  await page.setViewport({ width: touch ? 390 : 1440, height: 1000, isMobile: touch, hasTouch: touch });
  const errors = [], requests = [];
  page.on('pageerror', e => errors.push(e.message)); page.on('request', r => requests.push(r.url()));
  const url = `http://127.0.0.1:${server.address().port}`;
  await page.goto(url);
  await page.waitForFunction(() => window.livingWorkspace);
  return { page, url, errors, requests };
}
const state = page => page.evaluate(() => window.livingWorkspace.getState());
test('desktop field, mouse move/resize, docking, focus, Max commands and tenant reset', async t => {
  const { page, url, errors, requests } = await setup(t);
  assert.equal(await page.$$eval('#cdSpatialOrbit button[data-domain-id]', nodes => nodes.length), 4);
  assert.equal(await page.$$eval('#cdIntelConnections .domain-link', nodes => nodes.length), 4);
  await page.evaluate(() => { const w = window.livingWorkspace; for (const p of w.getState().panels) w.dispatch({ ...w.scope(), type: 'close_panel', source: 'human', panelId: p.id }); });
  await page.click('#cdSpatialOrbit button[data-domain-id="acquisition"]');
  await page.waitForSelector('.panel');
  const panel = '.panel';
  const grip = await page.$(`${panel} .grip`), box = await grip.boundingBox();
  await page.mouse.move(box.x + 50, box.y + 15); await page.mouse.down(); await page.mouse.move(box.x + 150, box.y + 75, { steps: 5 }); await page.mouse.up();
  assert.ok((await state(page)).panels[0].x > 50);
  const resize = await page.$(`${panel} .resize`), rb = await resize.boundingBox();
  await page.mouse.move(rb.x + 15, rb.y + 15); await page.mouse.down(); await page.mouse.move(rb.x + 95, rb.y + 65, { steps: 5 }); await page.mouse.up();
  assert.ok((await state(page)).panels[0].width > 400);
  await page.click('[aria-label="Dock secondary slot"]'); assert.equal((await state(page)).panels[0].dock, 'right');
  await page.click('.maximize'); assert.equal((await state(page)).panels[0].maximized, true);
  await page.click('.maximize'); assert.equal((await state(page)).panels[0].maximized, false);
  await page.click('#max-orb'); await page.type('#question', 'investigate acquisition'); await page.click('#ask button');
  assert.equal((await state(page)).panels.length, 2);
  assert.deepEqual((await state(page)).panels.map(p => p.dock), ['left', 'right']);
  await page.keyboard.press('Escape'); assert.equal(await page.$eval('#max-orb', el => el === document.activeElement), true);
  await page.select('#tenant', 'cedar');
  const reset = await state(page);
  assert.equal(reset.selected, null); assert.equal(reset.panels.length, 0); assert.equal(reset.messages.length, 0); assert.equal(reset.trail.length, 0);
  assert.equal(await page.$eval('body', el => el.textContent.includes('Harbor prospect research')), false);
  await page.select('#tenant', 'harbor'); assert.equal((await state(page)).history.length, 0);
  assert.ok(requests.every(r => r.startsWith(url + '/') || r.startsWith('data:')));
  assert.ok(requests.every(r => !r.includes('/api/')));
  assert.deepEqual(errors, []);
});
test('touch list selection, keyboard resize, panel switching and reduced motion', async t => {
  const { page, errors } = await setup(t, true);
  await page.emulateMediaFeatures([{ name: 'prefers-reduced-motion', value: 'reduce' }]);
  await page.evaluate(() => { const w = window.livingWorkspace; for (const p of w.getState().panels) w.dispatch({ ...w.scope(), type: 'close_panel', source: 'human', panelId: p.id }); });
  await page.tap('#cdViewToggle');
  await page.tap('#cdSpatialList [data-domain-id="content"]');
  assert.equal((await state(page)).selected, 'harbor-content');
  await page.tap('.panel-body button'); assert.equal((await state(page)).panels.length, 2);
  await page.click('.panel[data-focused="true"] .resize');
  const prior = (await state(page)).panels[1].height;
  await page.keyboard.press('ArrowDown'); assert.ok((await state(page)).panels[1].height > prior);
  const handle = await page.$('.panel[data-focused="true"] .resize');
  await handle.scrollIntoView();
  const rect = await handle.boundingBox();
  const beforeTouch = (await state(page)).panels[1].height;
  const touch = await page.touchscreen.touchStart(rect.x + 20, rect.y + 20);
  await touch.move(rect.x + 20, rect.y + 60);
  await touch.end();
  assert.ok((await state(page)).panels[1].height > beforeTouch);
  await page.tap('.panel-tabs button');
  await page.waitForFunction(() => window.livingWorkspace.getState().focused === 'domain:harbor-content');
  assert.equal(await page.$eval('#max-orb .filaments', n => getComputedStyle(n).animationName), 'none');
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true);
  assert.deepEqual(errors, []);
});
test('server rejects APIs, non-GET methods, repository paths, and blocks connections', async t => {
  const { url } = await setup(t);
  for (const path of ['/api/v1/command-deck', '/server.js', '/store.test.mjs', '/.env', '/%2e%2e/server.js']) assert.equal((await fetch(url + path)).status, 404);
  assert.equal((await fetch(url + '/', { method: 'POST' })).status, 405);
  assert.match((await fetch(url)).headers.get('content-security-policy'), /connect-src 'none'/);
});

test('desktop and tablet: field and two panels fit viewport, layout transitions preserve context', async t => {
  const { page, errors } = await setup(t);
  for (const [width, height] of [[1440, 900], [1180, 820], [1024, 768], [1280, 800]]) {
    await page.setViewport({ width, height });
    await page.reload();
    await page.waitForFunction(() => window.livingWorkspace?.getState().panels.length === 2);
    const before = await state(page);
    const layout = await page.evaluate(() => {
      const boxes = [document.getElementById('field'), ...document.querySelectorAll('.panel')].map(n => {
        const r = n.getBoundingClientRect(); return { x: r.x, y: r.y, right: r.right, bottom: r.bottom, width: r.width, height: r.height };
      });
      return { boxes, width: innerWidth, height: innerHeight, scrollHeight: document.documentElement.scrollHeight, scrollWidth: document.documentElement.scrollWidth };
    });
    assert.equal(layout.scrollHeight, height); assert.equal(layout.scrollWidth, width);
    for (const box of layout.boxes) { assert.ok(box.x >= 0 && box.y >= 0 && box.right <= width + 1 && box.bottom <= height + 1); assert.ok(box.width > 250 && box.height > 200); }
    assert.ok(layout.boxes[0].right <= layout.boxes[1].x);
    assert.ok(layout.boxes[1].bottom <= layout.boxes[2].y);
    for (const mode of ['expanded', 'minimized', 'docked']) {
      await page.click(`[data-field-mode="${mode}"]`);
      const after = await state(page);
      assert.equal(after.fieldMode, mode); assert.equal(after.selected, before.selected);
      assert.deepEqual(after.trail, before.trail); assert.deepEqual(after.panels, before.panels);
    }
    await page.click('#zoom-in'); assert.ok((await state(page)).fieldView.zoom > 1);
    await page.click('#reset-field'); assert.equal((await state(page)).fieldView.zoom, 1);
    await page.focus('#cdSpatialCanvas'); await page.keyboard.press('ArrowRight');
    assert.equal((await state(page)).fieldView.x, 25);
    await page.click('#reset-field');
    const fieldBox = await (await page.$('#cdSpatialCanvas')).boundingBox();
    await page.mouse.move(fieldBox.x + fieldBox.width / 2, fieldBox.y + 20);
    await page.mouse.down(); await page.mouse.move(fieldBox.x + fieldBox.width / 2 + 35, fieldBox.y + 45); await page.mouse.up();
    assert.equal((await state(page)).fieldView.x, 35);
    await page.click('#reset-field');
    assert.equal(await page.$eval('#max-orb', n => n.textContent.trim()), '');
    if (process.env.WORKSPACE_SCREENSHOTS) {
      const fs = require('node:fs'); fs.mkdirSync(process.env.WORKSPACE_SCREENSHOTS, { recursive: true });
      await page.screenshot({ path: `${process.env.WORKSPACE_SCREENSHOTS}/workspace-${width}x${height}.png` });
      await page.click('[data-field-mode="expanded"]');
      await page.screenshot({ path: `${process.env.WORKSPACE_SCREENSHOTS}/expanded-${width}x${height}.png` });
    }
  }
  assert.deepEqual(errors, []);
});
