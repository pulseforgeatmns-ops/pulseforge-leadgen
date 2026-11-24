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
// Panel placement goes through the compact title-bar menu.
async function panelAction(page, action, panel = '.panel') {
  await page.click(`${panel} .panel-menu-button`);
  await page.waitForSelector('#panel-menu');
  await page.click(`#panel-menu [data-action="${action}"]`);
}
test('desktop field, mouse move/resize, docking, focus, Max commands and tenant reset', async t => {
  const { page, url, errors, requests } = await setup(t);
  assert.equal(await page.$$eval('#cdSpatialOrbit button[data-domain-id]', nodes => nodes.length), 4);
  assert.equal(await page.$$eval('#cdIntelConnections .orbit-path', nodes => nodes.length), 3);
  assert.equal(await page.$$eval('#cdSpatialOrbit .sun', nodes => nodes.length), 1);
  await page.evaluate(() => { const w = window.livingWorkspace; for (const p of w.getState().panels) w.dispatch({ ...w.scope(), type: 'close_panel', source: 'human', panelId: p.id }); });
  await page.click('#cdSpatialOrbit button[data-domain-id="acquisition"]');
  await page.waitForSelector('.panel');
  await panelAction(page, 'dock-floating');
  const panel = '.panel';
  const grip = await page.$(`${panel} .grip`), box = await grip.boundingBox();
  await page.mouse.move(box.x + 50, box.y + 15); await page.mouse.down(); await page.mouse.move(box.x + 150, box.y + 75, { steps: 5 }); await page.mouse.up();
  assert.ok((await state(page)).panels[0].x > 50);
  const resize = await page.$(`${panel} .resize`), rb = await resize.boundingBox();
  await page.mouse.move(rb.x + 15, rb.y + 15); await page.mouse.down(); await page.mouse.move(rb.x + 95, rb.y + 65, { steps: 5 }); await page.mouse.up();
  assert.ok((await state(page)).panels[0].width > 400);
  await panelAction(page, 'dock-right'); assert.equal((await state(page)).panels[0].dock, 'right');
  await panelAction(page, 'maximize'); assert.equal((await state(page)).panels[0].maximized, true);
  await panelAction(page, 'maximize'); assert.equal((await state(page)).panels[0].maximized, false);
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
    assert.ok(layout.boxes[0].bottom <= layout.boxes[1].y);
    assert.ok(layout.boxes[1].right <= layout.boxes[2].x);
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

test('operator and Max layouts preserve context; named layouts persist and isolate tenants', async t => {
  const { page, errors } = await setup(t);
  await page.setViewport({ width: 1180, height: 820 });
  const initial = await state(page);
  const assertContext = async () => {
    const now = await state(page); assert.equal(now.selected, initial.selected); assert.deepEqual(now.trail, initial.trail);
    assert.deepEqual(now.panels.map(p => [p.id, p.recordId]), initial.panels.map(p => [p.id, p.recordId]));
  };
  const capture = async name => {
    if (process.env.WORKSPACE_SCREENSHOTS) await page.screenshot({ path: `${process.env.WORKSPACE_SCREENSHOTS}/${name}-1180x820.png` });
  };
  await capture('default');
  await page.click('#layout-menu summary');
  await page.select('#field-dock', 'right');
  await page.$eval('#field-ratio', el => { el.value = 43; el.dispatchEvent(new Event('change', { bubbles: true })); });
  await page.click('#layout-menu summary');
  await panelAction(page, 'dock-top');
  await assertContext();
  await page.click('#layout-menu summary'); await page.type('#layout-name', 'My research'); await page.click('#save-layout-form button');
  await page.click('#layout-menu summary');
  const custom = await state(page); await capture('customized');
  await page.click('#max-orb'); await page.type('#question', 'arrange research'); await page.click('#ask button'); await page.click('#close-max');
  await assertContext(); assert.equal((await state(page)).fieldDock, 'left'); await capture('max-rearranged');
  await page.click('#undo-layout'); await assertContext();
  assert.deepEqual((await state(page)).panels, custom.panels); assert.equal((await state(page)).fieldDock, 'right');
  await page.click('#redo-layout'); assert.equal((await state(page)).fieldDock, 'left');
  await page.click('#layout-menu summary'); await page.select('#saved-layouts', 'My research'); await page.click('#load-layout');
  assert.equal((await state(page)).fieldDock, 'right'); await assertContext();
  await page.click('.panel [aria-label="Minimize panel"]'); assert.equal((await state(page)).panels[0].minimized, true);
  await page.click('.panel-tabs button'); assert.equal((await state(page)).panels[0].minimized, false); await assertContext();
  await page.reload(); await page.waitForFunction(() => window.livingWorkspace);
  assert.ok((await state(page)).savedLayouts['My research']);
  await page.select('#tenant', 'cedar');
  assert.deepEqual((await state(page)).savedLayouts, {}); assert.equal((await state(page)).canUndo, false);
  assert.equal(await page.$eval('#saved-layouts', el => el.options.length), 0);
  await page.select('#tenant', 'harbor'); assert.ok((await state(page)).savedLayouts['My research']);
  assert.equal((await state(page)).selected, null); assert.equal((await state(page)).panels.length, 0);
  assert.deepEqual(errors, []);
});

test('orbital navigation: overview, domain focus, object selection, breadcrumbs, zoom and minimum panel sizes', async t => {
  const { page, errors, requests, url } = await setup(t);
  const shots = process.env.WORKSPACE_SCREENSHOTS;
  const capture = async name => { if (shots) { require('node:fs').mkdirSync(shots, { recursive: true }); await page.screenshot({ path: `${shots}/${name}.png` }); } };
  await page.setViewport({ width: 1440, height: 900, deviceScaleFactor: shots ? 2 : 1 });
  await page.reload(); await page.waitForFunction(() => window.livingWorkspace?.getState().panels.length === 2);
  const settle = () => new Promise(r => setTimeout(r, 900));
  await settle();
  // Overview: sun centred, four planets, no interactive moons, two panels underneath.
  let s = await state(page);
  assert.equal(s.fieldFocus, null);
  assert.equal(await page.$eval('#cdSpatialCanvas', n => n.dataset.level), 'overview');
  assert.equal(await page.$$eval('.moon:not([inert])', n => n.length), 0);
  assert.deepEqual(await page.$$eval('#field-crumbs li', n => n.map(x => x.textContent)), ['Harbor Works']);
  const inField = await page.evaluate(() => {
    const f = document.getElementById('cdSpatialCanvas').getBoundingClientRect();
    return [...document.querySelectorAll('.planet, .sun')].every(n => { const r = n.getBoundingClientRect(); return r.left >= f.left && r.right <= f.right && r.top >= f.top && r.bottom <= f.bottom; });
  });
  assert.ok(inField, 'sun and planets fit inside the field');
  await capture('orbital-overview-1440x900');
  // Acquisition exploration.
  await page.click('.planet[data-domain-id="acquisition"]'); await settle();
  s = await state(page);
  assert.equal(s.fieldFocus, 'acquisition'); assert.equal(s.selected, 'harbor-acquisition');
  assert.equal(await page.$eval('#cdSpatialCanvas', n => n.dataset.level), 'domain');
  assert.equal(await page.$$eval('.moon:not([inert])', n => n.length), 3);
  assert.equal(s.panels.length, 2, 'focusing a domain reuses the existing panels');
  assert.deepEqual(await page.$$eval('#field-crumbs li', n => n.map(x => x.textContent)), ['Harbor Works', 'Acquisition']);
  assert.equal(await page.$eval('#return-overview', n => n.hidden), false);
  await capture('orbital-acquisition-1440x900');
  // Selecting a prospect updates shared context and both investigation panels in place.
  await page.click('.moon[data-object-id="harbor-acquisition-1"]'); await settle();
  s = await state(page);
  assert.equal(s.selected, 'harbor-acquisition-1');
  assert.deepEqual(s.panels.map(p => [p.id, p.dock]), [['domain:harbor-acquisition-1', 'left'], ['investigation:harbor-acquisition-1', 'right']]);
  assert.deepEqual(await page.$$eval('#field-crumbs li', n => n.map(x => x.textContent)), ['Harbor Works', 'Acquisition', 'Northgate Bakery']);
  assert.ok(await page.$eval('.panel[data-panel-id="investigation:harbor-acquisition-1"]', n => n.textContent.includes('Northgate Bakery')));
  assert.equal(await page.$eval('#selection', n => n.textContent), 'Shared selection · Northgate Bakery');
  await capture('orbital-selected-prospect-1440x900');
  // Sibling switching from the details panel; undo reverts the panel retarget only.
  await page.click('.panel[data-panel-id="domain:harbor-acquisition-1"] .orbit-list button:nth-of-type(2)');
  assert.equal((await state(page)).selected, 'harbor-acquisition-2');
  // Zoom: buttons and wheel, then breadcrumbs return to the domain and the overview.
  await page.click('#zoom-in'); assert.ok((await state(page)).fieldView.zoom > 1);
  const box = await (await page.$('#cdSpatialCanvas')).boundingBox();
  await page.mouse.move(box.x + 30, box.y + 30); await page.mouse.wheel({ deltaY: -200 });
  await page.waitForFunction(z => window.livingWorkspace.getState().fieldView.zoom > z, {}, (await state(page)).fieldView.zoom);
  await page.click('#field-crumbs li:nth-child(2) button');
  s = await state(page); assert.equal(s.selected, 'harbor-acquisition'); assert.equal(s.fieldFocus, 'acquisition');
  await page.click('#field-crumbs li:first-child button');
  s = await state(page); assert.equal(s.fieldFocus, null); assert.deepEqual(s.fieldView, { x: 0, y: 0, zoom: 1 });
  assert.equal(s.selected, 'harbor-acquisition', 'returning to overview keeps the shared selection');
  // Keyboard: Escape from inside a focused domain returns to overview and restores focus to its planet.
  await page.focus('.planet[data-domain-id="clients"]'); await page.keyboard.press('Enter');
  assert.equal((await state(page)).fieldFocus, 'clients');
  await page.focus('.moon[data-object-id="harbor-clients-1"]'); await page.keyboard.press('Escape');
  assert.equal((await state(page)).fieldFocus, null);
  assert.equal(await page.$eval('.planet[data-domain-id="clients"]', n => n === document.activeElement), true);
  // Max navigates through the same dispatcher.
  await page.click('#max-orb'); await page.type('#question', 'investigate Tidewater'); await page.click('#ask button');
  s = await state(page); assert.equal(s.selected, 'harbor-acquisition-2'); assert.equal(s.fieldFocus, 'acquisition');
  await page.click('#close-max');
  // Additional panels never shrink below the usable minimum; overflow is stacked into the dock.
  await page.evaluate(() => { const w = window.livingWorkspace; for (const id of ['harbor-content', 'harbor-clients', 'harbor-campaigns']) { w.dispatch({ ...w.scope(), type: 'open_tool', source: 'human', toolId: 'investigation', recordId: id }); w.dispatch({ ...w.scope(), type: 'dock_panel', source: 'human', panelId: `investigation:${id}`, dock: 'right' }); } });
  const sizes = await page.$$eval('.panel:not([hidden])', n => n.map(x => [x.offsetWidth, x.offsetHeight]));
  assert.ok(sizes.length >= 2 && sizes.length < 5);
  for (const [w, h] of sizes) assert.ok(w >= 300 && h >= 220, `panel ${w}x${h} stays usable`);
  assert.match(await page.$eval('.panel-tabs', n => n.dataset.stackedNote), /stacked/);
  const stackedTab = await page.$('.panel-tabs button[title]');
  await stackedTab.click();
  const focused = (await state(page)).focused;
  assert.equal(await page.$eval(`.panel[data-panel-id="${focused}"]`, n => n.hidden), false, 'a stacked panel comes forward when selected');
  // Reduced motion keeps navigation but removes ambient animation and camera transitions.
  await page.emulateMediaFeatures([{ name: 'prefers-reduced-motion', value: 'reduce' }]);
  assert.equal(await page.$eval('.planet-body', n => getComputedStyle(n).animationName), 'none');
  assert.equal(await page.$eval('.planet', n => getComputedStyle(n).transitionDuration), '0s');
  // Tenant switch resets orbital focus.
  await page.select('#tenant', 'cedar');
  assert.equal((await state(page)).fieldFocus, null);
  assert.equal(await page.$eval('.sun', n => n.textContent.includes('Cedar Studio')), true);
  assert.ok(requests.every(r => r.startsWith(url + '/') || r.startsWith('data:')));
  assert.deepEqual(errors, []);
});

test('touch: tapping planets and moons navigates without panning', async t => {
  const { page, errors } = await setup(t, true);
  await page.setViewport({ width: 1024, height: 768, isMobile: true, hasTouch: true });
  await page.reload(); await page.waitForFunction(() => window.livingWorkspace?.getState().panels.length === 2);
  await page.tap('.planet[data-domain-id="campaigns"]');
  assert.equal((await state(page)).fieldFocus, 'campaigns');
  await new Promise(r => setTimeout(r, 900));
  await page.tap('.moon[data-object-id="harbor-campaigns-1"]');
  const s = await state(page);
  assert.equal(s.selected, 'harbor-campaigns-1'); assert.deepEqual(s.fieldView, { x: 0, y: 0, zoom: 1 });
  const targets = await page.$$eval('.planet, .moon:not([inert]), .sun', n => n.map(x => Math.min(x.offsetWidth, x.offsetHeight)));
  assert.ok(targets.every(size => size >= 44), 'touch targets are at least 44px');
  assert.deepEqual(errors, []);
});
