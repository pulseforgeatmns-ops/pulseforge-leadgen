'use strict';
// V0.5 acceptance: a complete operator + simulated-Max collaboration, on desktop
// (mouse + keyboard) and tablet landscape (touch). Fixture data only.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const puppeteer = require('puppeteer');
const { createServer } = require('./server.cjs');
const shots = process.env.WORKSPACE_SCREENSHOTS;
const pause = ms => new Promise(r => setTimeout(r, ms));
async function setup(t, viewport) {
  const server = createServer();
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const browser = await puppeteer.launch({ executablePath: process.env.CHROMIUM_PATH || undefined, headless: true, args: ['--no-sandbox', '--disable-dev-shm-usage'] });
  t.after(() => browser.close());
  const page = await browser.newPage();
  await page.setViewport({ deviceScaleFactor: shots ? 2 : 1, ...viewport });
  const errors = [], requests = [];
  page.on('pageerror', e => errors.push(e.message)); page.on('request', r => requests.push(r.url()));
  const url = `http://127.0.0.1:${server.address().port}`;
  await page.goto(url);
  await page.waitForFunction(() => window.livingWorkspace?.getState().panels.length === 2);
  await pause(300);
  return { page, errors, requests, url };
}
const state = page => page.evaluate(() => window.livingWorkspace.getState());
const lastMax = page => page.evaluate(() => window.livingWorkspace.getState().messages.filter(m => m.source === 'max').at(-1));
const crumbs = page => page.$$eval('#field-crumbs li', n => n.map(x => x.textContent));
const box = (page, sel) => page.$eval(sel, n => { const r = n.getBoundingClientRect(); return { x: r.x, y: r.y, right: r.right, bottom: r.bottom, width: r.width, height: r.height, hidden: n.hidden }; });

async function scenario(t, { name, viewport, touch }) {
  const { page, errors, requests, url } = await setup(t, viewport);
  const capture = async step => { if (shots) { fs.mkdirSync(shots, { recursive: true }); await page.screenshot({ path: `${shots}/v05-${name}-${step}.png` }); } };
  const press = async sel => (touch ? page.tap(sel) : page.click(sel));
  const ask = async text => {
    const before = (await state(page)).messages.length;
    await page.click('#question', { clickCount: 3 }); await page.type('#question', text);
    if (touch) await page.tap('#ask button'); else await page.keyboard.press('Enter');
    await page.waitForFunction(n => window.livingWorkspace.getState().messages.length >= n + 2, {}, before);
    return lastMax(page);
  };

  // 1. Orbital overview.
  let s = await state(page);
  assert.equal(s.fieldFocus, null); assert.equal(await page.$eval('#cdSpatialCanvas', n => n.dataset.level), 'overview');
  await capture('1-overview');

  // 2. Open Max (touch: tap the sphere; desktop: keyboard focus + Enter).
  if (touch) await page.tap('#max-orb'); else { await page.focus('#max-orb'); await page.keyboard.press('Enter'); }
  assert.equal((await state(page)).maxOpen, true);
  assert.equal(await page.$eval('#question', n => n === document.activeElement), true, 'focus moves to the composer');
  const context = await page.$eval('#max-context', n => n.textContent);
  assert.match(context, /Harbor Works/); assert.match(context, /Harbor prospect research/); assert.match(context, /Details \(left\)/);

  // 3. Ask Max to show Northgate Bakery.
  let reply = await ask('Max, show me Northgate Bakery.');
  await pause(900);
  // 4. Orbit, breadcrumbs, Details and Investigation all reflect Max's selection; Max reports verified facts.
  s = await state(page);
  assert.equal(s.selected, 'harbor-acquisition-1'); assert.equal(s.fieldFocus, 'acquisition');
  assert.equal(await page.$eval('.moon[data-object-id="harbor-acquisition-1"]', n => n.classList.contains('selected') && n.getAttribute('aria-pressed')), 'true');
  assert.deepEqual(await crumbs(page), ['Harbor Works', 'Acquisition', 'Northgate Bakery']);
  for (const id of ['domain:harbor-acquisition-1', 'investigation:harbor-acquisition-1']) {
    const panel = `.panel[data-panel-id="${id}"]`;
    assert.equal((await box(page, panel)).hidden, false);
    assert.match(await page.$eval(panel, n => n.textContent), /Northgate Bakery/);
  }
  assert.match(reply.text, /Northgate Bakery/); assert.match(reply.text, /Details is on the left and the evidence is on the right/);
  assert.ok(reply.receipt.some(r => r.startsWith('select_entity → Northgate Bakery')));
  assert.equal(s.history.filter(h => h.source === 'max' && h.type === 'select_entity').length, 1, 'Max used the dispatcher');
  assert.match(await page.$eval('#max-context', n => n.textContent), /Northgate Bakery · Prospect/);
  await capture('2-max-selected-northgate');

  // Operator moves the evidence under the field so Max has something real to rearrange.
  await press('.panel[data-panel-id="investigation:harbor-acquisition-1"] .panel-menu-button');
  await press('#panel-menu [data-action="dock-top"]');
  assert.equal((await state(page)).panels.find(p => p.toolId === 'investigation').dock, 'top');
  const operatorLayout = await state(page);

  // 5. Ask Max to put the evidence beside the details — visible immediately, described from the result.
  reply = await ask('Put the evidence beside the prospect details.');
  s = await state(page);
  assert.deepEqual(s.panels.map(p => [p.toolId, p.dock]), [['domain', 'left'], ['investigation', 'right']]);
  const details = await box(page, '.panel[data-panel-id="domain:harbor-acquisition-1"]');
  const evidence = await box(page, '.panel[data-panel-id="investigation:harbor-acquisition-1"]');
  assert.ok(details.right <= evidence.x + 1 && Math.abs(details.y - evidence.y) < 2, 'evidence sits beside the details');
  assert.equal(await page.$eval('.panel[data-panel-id="investigation:harbor-acquisition-1"]', n => n.dataset.maxTouched), 'true');
  assert.match(reply.text, /Details is on the left and the evidence is on the right, both for Northgate Bakery/);
  assert.ok(s.layoutCheckpoint, 'Max remembered the operator layout first');
  await capture('3-max-arranged');

  // 6. Operator resizes the spatial field: drag (mouse or touch), then keyboard on the separator.
  await page.click('#close-max');
  const split = await box(page, '#field-splitter');
  const startRatio = (await state(page)).fieldRatio;
  const sx = split.x + split.width / 2, sy = split.y + split.height / 2;
  if (touch) { const tp = await page.touchscreen.touchStart(sx, sy); await tp.move(sx, sy + 40); await tp.move(sx, sy + 70); await tp.end(); }
  else { await page.mouse.move(sx, sy); await page.mouse.down(); await page.mouse.move(sx, sy + 70, { steps: 6 }); await page.mouse.up(); }
  let ratio = (await state(page)).fieldRatio;
  assert.ok(ratio > startRatio + 0.03, `field grew from ${startRatio} to ${ratio}`);
  await page.focus('#field-splitter'); await page.keyboard.press('ArrowUp');
  assert.ok(Math.abs((await state(page)).fieldRatio - (ratio - 0.02)) < 0.002, 'arrow keys resize the field');
  assert.equal(await page.$eval('#field-splitter', n => n.getAttribute('aria-valuenow')), String(Math.round((ratio - 0.02) * 100)));
  for (const sel of ['.panel[data-panel-id="domain:harbor-acquisition-1"]', '.panel[data-panel-id="investigation:harbor-acquisition-1"]']) {
    const b = await box(page, sel); assert.ok(b.width >= 300 && b.height >= 220, `panel stays usable (${b.width}x${b.height})`);
  }
  await page.keyboard.press('End');
  const capped = await state(page);
  const panelBox = await box(page, '.panel[data-panel-id="domain:harbor-acquisition-1"]');
  assert.ok(panelBox.height >= 219.5, `End caps the field so panels keep 220px (${panelBox.height})`);
  assert.ok(capped.fieldRatio <= 0.65);
  await capture('4-field-resized');

  // 7. Ask Max to restore the previous layout (the one Max replaced).
  if (touch) await page.tap('#max-orb'); else await page.click('#max-orb');
  reply = await ask('Restore my previous layout.');
  s = await state(page);
  assert.equal(s.fieldRatio, operatorLayout.fieldRatio);
  assert.deepEqual(s.panels.map(p => [p.id, p.dock]), operatorLayout.panels.map(p => [p.id, p.dock]));
  assert.match(reply.text, /^Restored the layout you had before my last change/);
  assert.match(reply.text, new RegExp(`was docked top at ${Math.round(capped.fieldRatio * 100)}%`));
  assert.match(reply.text, /Investigation is across the top/);
  // 8. Selection is still Northgate Bakery everywhere.
  assert.equal(s.selected, 'harbor-acquisition-1');
  assert.deepEqual(await crumbs(page), ['Harbor Works', 'Acquisition', 'Northgate Bakery']);
  assert.equal(await page.$eval('.moon[data-object-id="harbor-acquisition-1"]', n => n.classList.contains('selected')), true);
  assert.equal(await page.$eval('#selection', n => n.textContent), 'Shared selection · Northgate Bakery');
  await capture('5-restored');
  // Undo is still a separate, working path for the operator.
  await page.click('#close-max');
  await press('#undo-layout');
  assert.equal((await state(page)).fieldRatio, capped.fieldRatio);
  await press('#redo-layout');
  assert.equal((await state(page)).fieldRatio, operatorLayout.fieldRatio);

  // 9. Tenant switch isolates context: nothing of Harbor survives, and Max cannot see it.
  await page.select('#tenant', 'cedar');
  s = await state(page);
  assert.deepEqual([s.selected, s.fieldFocus, s.layoutCheckpoint, s.messages.length, s.panels.length, s.maxOpen], [null, null, null, 0, 0, false]);
  assert.equal(await page.$eval('body', n => /Northgate|Harbor prospect/.test(n.innerText)), false);
  if (touch) await page.tap('#max-orb'); else await page.click('#max-orb');
  assert.match(await page.$eval('#max-context', n => n.textContent), /Cedar Studio/);
  reply = await ask('Max, show me Northgate Bakery.');
  assert.match(reply.text, /I didn’t find that in Cedar Studio/);
  assert.equal((await state(page)).selected, null);
  reply = await ask('Show me Willowmere Florist');
  assert.equal((await state(page)).selected, 'cedar-acquisition-1');
  await pause(900);
  await capture('6-cedar-isolated');
  assert.ok(requests.every(r => r.startsWith(url + '/') || r.startsWith('data:')));
  assert.deepEqual(errors, []);
}

test('acceptance · desktop 1440×900, mouse and keyboard', t => scenario(t, { name: 'desktop', viewport: { width: 1440, height: 900 } }));
test('acceptance · tablet landscape 1180×820, touch', t => scenario(t, { name: 'tablet', viewport: { width: 1180, height: 820, isMobile: true, hasTouch: true }, touch: true }));
test('acceptance · tablet landscape 1024×768, touch', t => scenario(t, { name: 'tablet-small', viewport: { width: 1024, height: 768, isMobile: true, hasTouch: true }, touch: true }));

test('Max commands: investigation, expand field, back to Acquisition, and honest replies', async t => {
  const { page, errors } = await setup(t, { width: 1280, height: 800 });
  await page.click('#max-orb');
  const ask = async text => { const n = (await state(page)).messages.length; await page.type('#question', text); await page.keyboard.press('Enter'); await page.waitForFunction(k => window.livingWorkspace.getState().messages.length >= k + 2, {}, n); return lastMax(page); };
  let r = await ask('Open the investigation.');
  assert.match(r.text, /already open/);
  await page.evaluate(() => { const w = window.livingWorkspace; w.dispatch({ ...w.scope(), type: 'close_panel', source: 'human', panelId: 'investigation:harbor-acquisition' }); });
  await ask('show me Tidewater');
  await page.evaluate(() => { const w = window.livingWorkspace; w.dispatch({ ...w.scope(), type: 'close_panel', source: 'human', panelId: 'investigation:harbor-acquisition-2' }); });
  r = await ask('Open the investigation.');
  assert.match(r.text, /^Opened the investigation\. It shows Tidewater Dental Studio’s evidence, on the right\./);
  r = await ask('Expand the spatial field.');
  assert.equal((await state(page)).fieldMode, 'expanded'); assert.match(r.text, /now expanded/);
  assert.equal(await page.$eval('.panel', n => n.hidden), true, 'expanded field hides panels immediately');
  r = await ask('Take me back to Acquisition.');
  let s = await state(page);
  assert.equal(s.fieldFocus, 'acquisition'); assert.equal(s.selected, 'harbor-acquisition');
  assert.deepEqual(await crumbs(page), ['Harbor Works', 'Acquisition']);
  assert.match(r.text, /^Back in Acquisition: 3 prospects in orbit\./);
  r = await ask('Restore my previous layout.');
  assert.equal((await state(page)).fieldMode, 'docked');
  r = await ask('Restore my previous layout.');
  assert.match(r.text, /no earlier layout of yours/); // nothing claimed, nothing changed
  r = await ask('Put the evidence beside the prospect details.');
  assert.match(r.text, /both for Harbor prospect research/);
  r = await ask('What am I looking at?');
  assert.match(r.text, /Harbor Works › Acquisition/); assert.match(r.text, /Selected: Harbor prospect research/);
  // Suggestion chips submit through the same path and are context aware.
  await page.evaluate(() => { const w = window.livingWorkspace; w.dispatch({ ...w.scope(), type: 'select_entity', source: 'human', recordId: 'harbor-acquisition-3' }); });
  await page.click('#close-max'); await page.click('#max-orb');
  const chips = await page.$$eval('#max-suggestions button', n => n.map(b => b.textContent));
  assert.ok(chips.includes('Open the investigation') && chips.includes('Restore my previous layout'), chips.join(' | '));
  // The workspace reflows beside the conversation: no panel sits underneath Max.
  const covered = await page.evaluate(() => { const c = document.getElementById('conversation').getBoundingClientRect(); return [...document.querySelectorAll('.panel:not([hidden]), #field')].some(n => n.getBoundingClientRect().right > c.left + 1); });
  assert.equal(covered, false);
  // Escape closes Max and returns focus to the sphere.
  await page.focus('#question'); await page.keyboard.press('Escape');
  assert.equal(await page.$eval('#max-orb', n => n === document.activeElement), true);
  assert.deepEqual(errors, []);
});

test('title-bar menu is keyboard operable and returns focus', async t => {
  const { page, errors } = await setup(t, { width: 1280, height: 800 });
  const btn = '.panel[data-panel-id="investigation:harbor-acquisition"] .panel-menu-button';
  await page.focus(btn); await page.keyboard.press('Enter');
  assert.equal(await page.$eval(btn, n => n.getAttribute('aria-expanded')), 'true');
  assert.equal(await page.$eval('#panel-menu', n => n.getAttribute('role')), 'menu');
  assert.equal(await page.evaluate(() => document.activeElement.dataset.action), 'dock-right', 'opens on the checked item');
  await page.keyboard.press('ArrowDown'); await page.keyboard.press('ArrowDown');
  assert.equal(await page.evaluate(() => document.activeElement.dataset.action), 'dock-bottom');
  await page.keyboard.press('Escape');
  assert.equal(await page.$('#panel-menu'), null); assert.equal(await page.$eval(btn, n => n === document.activeElement), true);
  await page.keyboard.press('ArrowDown'); await page.keyboard.press('ArrowDown'); await page.keyboard.press('Enter');
  assert.equal((await state(page)).panels.find(p => p.toolId === 'investigation').dock, 'top');
  assert.equal(await page.$eval(btn, n => n === document.activeElement), true, 'focus returns to the menu button');
  await page.focus(btn); await page.keyboard.press('ArrowUp');
  assert.equal(await page.evaluate(() => document.activeElement.dataset.action), 'close');
  await page.keyboard.press('Escape');
  // Menu fits inside the viewport even for the right-most panel.
  await page.click(btn);
  const m = await box(page, '#panel-menu');
  assert.ok(m.x >= 0 && m.right <= 1280 && m.y >= 0 && m.bottom <= 800);
  await page.mouse.click(10, 790); assert.equal(await page.$('#panel-menu'), null, 'outside click closes');
  // Sphere has no resting text and only the slow halo is animated.
  assert.equal(await page.$eval('#max-orb', n => n.textContent.trim()), '');
  assert.deepEqual(await page.$$eval('#max-orb *', ns => [...new Set(ns.map(n => getComputedStyle(n).animationName).filter(a => a !== 'none'))]), ['orb-breathe']);
  await page.emulateMediaFeatures([{ name: 'prefers-reduced-motion', value: 'reduce' }]);
  assert.equal(await page.$eval('#max-orb .orb-halo', n => getComputedStyle(n).animationName), 'none');
  assert.deepEqual(errors, []);
});
