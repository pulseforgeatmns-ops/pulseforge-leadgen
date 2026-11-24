'use strict';
// V0.6 acceptance: Living Business Field + workspace refinement. Fixture data only.
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
  await pause(250);
  return { page, errors, requests, url };
}
const state = page => page.evaluate(() => window.livingWorkspace.getState());
const crumbs = page => page.$$eval('#field-crumbs li', n => n.map(x => x.textContent));
const rect = (page, sel) => page.$eval(sel, n => { const r = n.getBoundingClientRect(); return { x: r.x, y: r.y, right: r.right, bottom: r.bottom, width: r.width, height: r.height }; });
const consistent = s => s.panels.every(p => p.follow === false || p.recordId === s.selected);
const settle = () => pause(750);

async function scenario(t, { name, viewport, touch }) {
  const { page, errors, requests, url } = await setup(t, viewport);
  const capture = async step => { if (shots) { fs.mkdirSync(shots, { recursive: true }); await page.screenshot({ path: `${shots}/v06-${name}-${step}.png` }); } };
  const press = sel => (touch ? page.tap(sel) : page.click(sel));
  const ask = async text => {
    const n = (await state(page)).messages.length;
    await page.click('#question', { clickCount: 3 }); await page.type('#question', text);
    if (touch) await page.tap('#ask button'); else await page.keyboard.press('Enter');
    await page.waitForFunction(k => window.livingWorkspace.getState().messages.length >= k + 2, {}, n);
    return page.evaluate(() => window.livingWorkspace.getState().messages.filter(m => m.source === 'max').at(-1));
  };

  // 1. Living Business Field overview — no celestial vocabulary, no idle animation.
  assert.equal(await page.$eval('#cdSpatialCanvas', n => n.dataset.level), 'overview');
  assert.equal(await page.$$eval('.domain-node', n => n.length), 4);
  assert.equal(await page.$$eval('.context-core', n => n.length), 1);
  assert.equal(await page.$$eval('.core-link', n => n.length), 4);
  assert.equal(await page.$$eval('.domain-link', n => n.length), 3, 'declared domain relationships are drawn');
  assert.equal(await page.$$eval('.sun, .planet, .moon, .orbit-path', n => n.length), 0);
  assert.deepEqual(await page.$$eval('#field *', ns => ns.map(n => getComputedStyle(n).animationName).filter(a => a !== 'none')), [], 'nothing in the field animates while idle');
  assert.equal(await page.$eval('.domain-node[data-domain-id="acquisition"]', n => n.classList.contains('priority-elevated')), true);
  await capture('1-overview');

  // 2. Focus Acquisition; prospects come forward, other domains recede but remain.
  await press('.domain-node[data-domain-id="acquisition"]'); await settle();
  let s = await state(page);
  assert.equal(s.fieldFocus, 'acquisition'); assert.equal(await page.$eval('#cdSpatialCanvas', n => n.dataset.level), 'domain');
  assert.equal(await page.$$eval('.object-node:not([inert])', n => n.length), 3);
  for (const id of ['content', 'clients', 'campaigns']) {
    const st = await page.$eval(`.domain-node[data-domain-id="${id}"]`, n => [n.dataset.state, Number(getComputedStyle(n).opacity), n.getBoundingClientRect().width]);
    assert.equal(st[0], 'recede'); assert.ok(st[1] > 0.3 && st[1] < 1 && st[2] > 40, `${id} recedes but stays visible`);
  }
  const before = await rect(page, '.object-node[data-object-id="harbor-acquisition-1"]'); await pause(600);
  assert.deepEqual(await rect(page, '.object-node[data-object-id="harbor-acquisition-1"]'), before, 'targets are stable once settled');
  await capture('2-acquisition');

  // 3–4. Select Northgate Bakery; Details, Investigation, breadcrumbs and Max context agree.
  assert.equal(await page.$eval('#max-orb', n => n.dataset.attention), 'false');
  await press('.object-node[data-object-id="harbor-acquisition-1"]'); await settle();
  s = await state(page);
  assert.equal(s.selected, 'harbor-acquisition-1'); assert.ok(consistent(s));
  assert.deepEqual(s.panels.map(p => p.id), ['domain:harbor-acquisition-1', 'investigation:harbor-acquisition-1']);
  assert.deepEqual(await crumbs(page), ['Harbor Works', 'Acquisition', 'Northgate Bakery']);
  const details = await page.$eval('.panel[data-panel-id="domain:harbor-acquisition-1"]', n => n.textContent);
  for (const part of ['Northgate Bakery', 'Qualification signals', 'Relationships', 'Suggested next actions', 'Autumn reactivation', 'Proposal · not executed']) assert.ok(details.includes(part), part);
  assert.equal(await page.$eval('.panel[data-panel-id="domain:harbor-acquisition-1"] .stage-track [aria-current]', n => n.textContent), 'Research');
  const inv = await page.$eval('.panel[data-panel-id="investigation:harbor-acquisition-1"]', n => n.textContent);
  for (const part of ['Evidence', 'Website (fixture)', 'Research findings', 'Outstanding questions', 'Is the second storefront open yet?', 'Activity']) assert.ok(inv.includes(part), part);
  assert.equal(await page.$$eval('.relation-link', n => n.length), 2, 'selection relationships are drawn');
  assert.equal(await page.$eval('.domain-node[data-domain-id="campaigns"]', n => n.dataset.related), 'true');
  assert.equal(await page.$eval('#max-orb', n => n.dataset.attention), 'true', 'Max signals open questions without opening');
  await capture('3-selected-prospect');
  if (touch) await page.tap('#max-orb'); else { await page.focus('#max-orb'); await page.keyboard.press('Enter'); }
  assert.equal(await page.$eval('#max-orb', n => n.dataset.attention), 'false');
  assert.equal(await page.$eval('#max-context-details', n => n.open), false, 'technical context starts collapsed');
  assert.match(await page.$eval('#max-context-line', n => n.textContent), /Harbor Works › Acquisition › Northgate Bakery · 2 panels/);
  await press('#max-context-details summary');
  assert.match(await page.$eval('#max-context', n => n.textContent), /Northgate Bakery · Prospect/);

  // 5. Max rearranges; the change is visible at once and described from the result.
  let reply = await ask('Put the evidence below the prospect details.');
  s = await state(page);
  assert.deepEqual(s.panels.map(p => p.dock), ['top', 'bottom']);
  const d = await rect(page, '.panel[data-panel-id="domain:harbor-acquisition-1"]'), e = await rect(page, '.panel[data-panel-id="investigation:harbor-acquisition-1"]');
  // Max's reply must match what is actually on screen (minimum sizes may prevent stacking).
  if (d.bottom <= e.y + 1) {
    assert.match(reply.text, /Details is across the top and the evidence is across the bottom, both for Northgate Bakery/);
    assert.ok(d.height >= 219.5 && e.height >= 219.5, 'stacked panels stay usable');
  } else {
    assert.ok(e.x >= d.right - 1, 'reflowed side by side');
    assert.match(reply.text, /there isn’t room to show both below at a usable size here, so they’re side by side/);
  }
  assert.ok(reply.receipt.length >= 3);
  reply = await ask('What is still outstanding?');
  assert.match(reply.text, /second storefront open yet/);
  await capture('4-max-collaborating');
  await page.click('#close-max');

  // 6. Resize the field manually.
  const split = await rect(page, '#field-splitter');
  const r0 = (await state(page)).fieldRatio, sx = split.x + split.width / 2, sy = split.y + split.height / 2;
  if (touch) { const tp = await page.touchscreen.touchStart(sx, sy); await tp.move(sx, sy - 30); await tp.move(sx, sy - 50); await tp.end(); await pause(400); /* a person's pause between gestures */ }
  else { await page.mouse.move(sx, sy); await page.mouse.down(); await page.mouse.move(sx, sy - 50, { steps: 5 }); await page.mouse.up(); }
  const r1 = (await state(page)).fieldRatio;
  assert.ok(r1 < r0 - 0.03, `field shrank ${r0} → ${r1}`);
  for (const p of await page.$$eval('.panel:not([hidden])', ns => ns.map(n => [n.offsetWidth, n.offsetHeight]))) assert.ok(p[0] >= 300 && p[1] >= 220);

  // 7. Save and restore a composition without changing the selected business object.
  await press('#layout-menu summary');
  await page.type('#layout-name', 'Morning review'); await press('#save-layout-form button');
  await press('[data-preset="research"]');
  assert.equal((await state(page)).fieldDock, 'left');
  await press('#field-crumbs li:nth-child(2) button'); await settle();
  await press('.object-node[data-object-id="harbor-acquisition-2"]'); await settle();
  assert.equal((await state(page)).selected, 'harbor-acquisition-2');
  await press('#layout-menu summary'); await page.select('#saved-layouts', 'Morning review'); await press('#load-layout');
  s = await state(page);
  assert.equal(s.fieldDock, 'top'); assert.equal(s.fieldRatio, r1); assert.deepEqual(s.panels.map(p => p.dock), ['top', 'bottom']);
  assert.equal(s.selected, 'harbor-acquisition-2', 'restoring a composition never changes the selection'); assert.ok(consistent(s));

  // 8. Undo a layout change: geometry reverts, panels still agree with the selection.
  await press('#undo-layout');
  s = await state(page);
  assert.equal(s.fieldDock, 'left'); assert.equal(s.selected, 'harbor-acquisition-2'); assert.ok(consistent(s));
  assert.ok(await page.$eval('.panel[data-panel-id="investigation:harbor-acquisition-2"]', n => n.textContent.includes('Tidewater Dental Studio')));
  while ((await state(page)).canUndo) await press('#undo-layout');
  s = await state(page); assert.ok(consistent(s) && s.selected === 'harbor-acquisition-2');
  await press('#redo-layout'); assert.ok(consistent(await state(page)));
  // Navigation history is separate: Back returns to Northgate, layout untouched.
  const layoutBefore = JSON.stringify((await state(page)).panels.map(p => [p.dock, p.width]));
  if (touch) await press('#nav-back'); else { await page.focus('#cdSpatialCanvas'); await page.keyboard.down('Alt'); await page.keyboard.press('ArrowLeft'); await page.keyboard.up('Alt'); }
  s = await state(page);
  assert.ok(['harbor-acquisition', 'harbor-acquisition-1'].includes(s.selected)); assert.ok(consistent(s));
  assert.equal(JSON.stringify(s.panels.map(p => [p.dock, p.width])), layoutBefore);
  await press('#nav-forward'); assert.equal((await state(page)).selected, 'harbor-acquisition-2');

  // Expanded field as a first-class, savable composition.
  await press('[data-field-mode="expanded"]'); await settle();
  assert.equal(await page.$eval('#field-selection', n => !n.hidden && n.textContent.includes('Tidewater Dental Studio')), true);
  await press('.object-node[data-object-id="harbor-acquisition-1"]'); await settle();
  s = await state(page); assert.equal(s.fieldMode, 'expanded', 'selecting in the expanded field keeps it expanded'); assert.equal(s.selected, 'harbor-acquisition-1');
  assert.match(await page.$eval('#field-selection', n => n.textContent), /Northgate Bakery/);
  await press('#layout-menu summary'); await page.type('#layout-name', 'Field study'); await press('#save-layout-form button'); await press('#layout-menu summary');
  assert.equal((await state(page)).savedLayouts['Field study'].fieldMode, 'expanded');
  await capture('5-expanded-field');
  // The current composition is remembered per tenant across reloads.
  await page.reload(); await page.waitForFunction(() => window.livingWorkspace?.getState().panels.length === 2);
  s = await state(page); assert.equal(s.fieldMode, 'expanded'); assert.equal(s.hasPreference, true); assert.equal(s.canUndo, false);
  await press('#layout-menu summary'); await press('#reset-composition'); await press('#layout-menu summary');
  s = await state(page); assert.equal(s.fieldMode, 'docked'); assert.equal(s.hasPreference, true);

  // 9. Tenant switch: complete isolation.
  await page.select('#tenant', 'cedar');
  s = await state(page);
  assert.deepEqual([s.selected, s.fieldFocus, s.panels.length, s.messages.length, s.canBack, s.canUndo, Object.keys(s.savedLayouts).length], [null, null, 0, 0, false, false, 0]);
  assert.equal(await page.$eval('body', n => /Northgate|Tidewater|Harbor prospect|Morning review/.test(n.innerText + n.querySelector('#max-context').textContent)), false);
  assert.match(await page.$eval('.context-core', n => n.textContent), /Cedar Studio/);
  await page.select('#tenant', 'harbor');
  assert.ok((await state(page)).savedLayouts['Field study'], 'Harbor compositions return with Harbor');
  assert.ok(requests.every(r => r.startsWith(url + '/') || r.startsWith('data:')));
  assert.deepEqual(errors, []);
}
test('V0.6 acceptance · desktop 1440×900, mouse and keyboard', t => scenario(t, { name: 'desktop', viewport: { width: 1440, height: 900 } }));
test('V0.6 acceptance · tablet landscape 1180×820, touch', t => scenario(t, { name: 'tablet', viewport: { width: 1180, height: 820, isMobile: true, hasTouch: true }, touch: true }));
test('V0.6 acceptance · tablet landscape 1024×768, touch', t => scenario(t, { name: 'tablet-small', viewport: { width: 1024, height: 768, isMobile: true, hasTouch: true }, touch: true }));

test('keyboard exploration and reduced motion', async t => {
  const { page, errors } = await setup(t, { width: 1280, height: 800 });
  await page.emulateMediaFeatures([{ name: 'prefers-reduced-motion', value: 'reduce' }]);
  await page.focus('.domain-node[data-domain-id="clients"]'); await page.keyboard.press('Enter');
  assert.equal((await state(page)).fieldFocus, 'clients');
  for (const sel of ['.domain-node', '.object-node', '.context-core', '.link']) assert.equal(await page.$eval(sel, n => getComputedStyle(n).transitionDuration), '0s', sel);
  await page.keyboard.press('Tab');
  const focused = await page.evaluate(() => document.activeElement.className);
  assert.ok(/object-node|domain-node/.test(focused), `tab moves through field nodes (${focused})`);
  await page.focus('.object-node[data-object-id="harbor-clients-1"]'); await page.keyboard.press('Enter');
  assert.equal((await state(page)).selected, 'harbor-clients-1');
  await page.keyboard.press('Escape');
  assert.equal((await state(page)).fieldFocus, null);
  assert.equal(await page.$eval('.domain-node[data-domain-id="clients"]', n => n === document.activeElement), true);
  // Pin from the title-bar menu; the pinned panel keeps its record, explicitly labelled.
  const btn = '.panel[data-panel-id="investigation:harbor-clients-1"] .panel-menu-button';
  await page.focus(btn); await page.keyboard.press('Enter');
  await page.click('#panel-menu [data-action="pin"]');
  await page.focus('.domain-node[data-domain-id="content"]'); await page.keyboard.press('Enter');
  const s = await state(page);
  assert.equal(s.panels.find(p => p.toolId === 'investigation').recordId, 'harbor-clients-1');
  assert.equal(await page.$eval('.panel[data-panel-id="investigation:harbor-clients-1"]', n => n.dataset.pinned), 'true');
  assert.equal(s.panels.find(p => p.toolId === 'domain').recordId, 'harbor-content');
  // List fallback mirrors the field.
  await page.click('#cdViewToggle');
  assert.ok((await page.$$eval('#cdSpatialList button', n => n.map(b => b.textContent))).some(tx => tx.includes('Storm-season checklist')));
  assert.deepEqual(errors, []);
});
