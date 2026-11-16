'use strict';
/*
 * Real Chromium runs of the EXACT built "javascript:" strings. The bookmarklet
 * URL is placed verbatim in an <a href> on a fake page served for the
 * production origin and clicked, so Chromium itself percent-decodes and runs
 * it (the same javascript: URL path a bookmark uses). Every request is
 * intercepted by page.route: API calls get local mock responses and anything
 * else is aborted, so nothing ever reaches production.
 *   node --test scripts/s2-rest-acceptance/bookmarklets.browser.test.js
 */
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');

const ORIGIN = 'https://pulseforge-leadgen-production.up.railway.app';
const MISSION = 'mission_bdf1e016-ae3b-4d97-be54-3f1dc366d169';
const STATUS = 'Plan approved (review only)';
const FIVE = ['provider_research', 'outreach', 'outreach_authorization', 'paid_media', 'content'];
const INTENTS = ['EXECUTE_OUTBOUND', 'APPROVE_EXECUTION', 'GENERATE_VARIANTS', 'ASSESS_PAID_ACQUISITION', 'START_DISCOVERY', 'APPROVE_DISCOVERY'];
const read = (f) => fs.readFileSync(path.join(__dirname, f), 'utf8').trim();

let browser;
test.before(async () => { browser = await chromium.launch(); });
test.after(async () => { if (browser) await browser.close(); });

async function open(href, { csp, api, holdFirstApi } = {}) {
  const page = await browser.newPage();
  const requests = [];
  const errors = [];
  page.on('pageerror', (e) => errors.push(String(e && e.message)));
  page.on('console', (m) => { if (m.type() === 'error') errors.push(m.text()); });
  let release;
  const held = new Promise((r) => { release = r; });
  await page.route('**/*', async (route) => {
    const req = route.request();
    const url = new URL(req.url());
    if (url.origin === ORIGIN && url.pathname === '/dashboard') {
      const headers = { 'content-type': 'text/html' };
      if (csp) headers['content-security-policy'] = csp;
      const html = '<!doctype html><title>Pulseforge (fake)</title><body><p>fake dashboard</p>'
        + '<a id="bm" href="' + href.replace(/"/g, '&quot;') + '">s2run</a></body>';
      return route.fulfill({ status: 200, headers, body: html });
    }
    requests.push({ method: req.method(), path: url.origin === ORIGIN ? url.pathname : url.href, body: req.postData() });
    if (url.origin === ORIGIN && api) {
      if (holdFirstApi && requests.length === 1) await held;
      const [status, body] = api(req.method(), url.pathname, req.postData());
      return route.fulfill({ status, headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });
    }
    return route.abort();
  });
  await page.goto(ORIGIN + '/dashboard');
  return { page, requests, errors, release };
}

const click = (page) => page.evaluate(() => document.getElementById('bm').click());

test('minimal diagnostic: visible message, zero requests, page not replaced', async () => {
  const { page, requests, errors } = await open(read('diagnostic.bookmarklet.txt'));
  await click(page);
  const panel = page.locator('#s2-diagnostic-panel');
  await panel.waitFor({ state: 'visible', timeout: 3000 });
  const text = await panel.textContent();
  assert.match(text, /^S2 DIAGNOSTIC: bookmarklet RAN \(build [0-9a-f]{12}\)/);
  assert.match(text, /page: https:\/\/pulseforge-leadgen-production\.up\.railway\.app\/dashboard/);
  const box = await panel.boundingBox();
  assert.ok(box.width > 300 && box.height > 100, 'panel is large: ' + JSON.stringify(box));
  assert.equal(await page.locator('#bm').count(), 1, 'page was not replaced');
  await panel.click();
  assert.equal(await panel.count(), 0, 'tap closes it');
  assert.deepEqual(requests, []);
  assert.deepEqual(errors, []);
  await page.close();
});

test('size probe (same length as acceptance): runs, zero requests', async () => {
  const { page, requests } = await open(read('sizeprobe.bookmarklet.txt'));
  await click(page);
  await page.locator('#s2-diagnostic-panel').waitFor({ state: 'visible', timeout: 3000 });
  assert.match(await page.locator('#s2-diagnostic-panel').textContent(), /kind: size-probe/);
  assert.deepEqual(requests, []);
  await page.close();
});

test('FINDING: a truncated bookmarklet fails with no visible sign (SyntaxError only in console)', async () => {
  const url = read('bookmarklet.txt');
  const { page, requests, errors } = await open(url.slice(0, Math.floor(url.length * 0.8)));
  await click(page);
  await page.waitForTimeout(500);
  assert.equal(await page.locator('#s2-acceptance-panel').count(), 0);
  assert.ok(errors.some((e) => /SyntaxError|Unexpected|missing|Invalid/i.test(e)), JSON.stringify(errors));
  assert.deepEqual(requests, []);
  await page.close();
});

test('FINDING: a page CSP without unsafe-inline blocks even the diagnostic, silently', async () => {
  const { page, requests, errors } = await open(read('diagnostic.bookmarklet.txt'), { csp: "script-src 'self'" });
  await click(page);
  await page.waitForTimeout(500);
  assert.equal(await page.locator('#s2-diagnostic-panel').count(), 0);
  assert.ok(errors.some((e) => /Content Security Policy/i.test(e)), JSON.stringify(errors));
  assert.deepEqual(requests, []);
  await page.close();
});

function mockApi({ posts }) {
  const m = { id: MISSION, tenantId: '25', stage: 'discover', status: STATUS, version: 3, pendingOperatorDecision: null };
  return (method, p, body) => {
    if (method === 'POST') { posts.push(JSON.parse(body)); return [403, { error: 'authorization_constraint_blocked' }]; }
    if (p === '/api/v1/tenant/context') return [200, { tenantId: '25', active_client_id: 25 }];
    if (p === '/api/v1/amo/missions') return [200, { tenantId: '25', missions: [m] }];
    if (p.endsWith('/authorization')) return [200, { missionId: MISSION, tenantId: '25', constraintsHash: 'h1', constraints: { reviewOnly: true, prohibited: FIVE, revision: 0 } }];
    return [200, { tenantId: '25', mission: m }];
  };
}

test('acceptance: startup message is visible before the first GET answers', async () => {
  const posts = [];
  const { page, requests, release } = await open(read('bookmarklet.txt'), { api: mockApi({ posts }), holdFirstApi: true });
  page.on('dialog', (d) => d.dismiss());
  await click(page);
  const panel = page.locator('#s2-acceptance-panel');
  await panel.waitFor({ state: 'visible', timeout: 3000 });
  assert.match(await panel.textContent(), /^S2 acceptance bookmarklet STARTED \(build [0-9a-f]{12}\)[\s\S]*No request sent yet/);
  const box = await panel.boundingBox();
  assert.ok(box.width > 300 && box.height > 300, JSON.stringify(box));
  assert.equal(requests.length, 1);
  assert.equal(requests[0].path, '/api/v1/tenant/context');
  release();
  await page.waitForFunction(() => /"verdict"/.test(document.getElementById('s2-acceptance-panel').textContent));
  assert.equal(JSON.parse(await panel.textContent()).haltReason, 'operator_cancelled_before_send');
  assert.equal(posts.length, 0);
  assert.ok(requests.every((r) => r.method === 'GET'));
  await page.close();
});

test('acceptance: wrong origin halts visibly with zero requests', async () => {
  const page = await browser.newPage();
  const requests = [];
  await page.route('**/*', (route) => {
    const u = route.request().url();
    if (u === 'https://example.test/') return route.fulfill({ status: 200, headers: { 'content-type': 'text/html' }, body: '<a id="bm" href="' + read('bookmarklet.txt') + '">x</a>' });
    requests.push(u); return route.abort();
  });
  const alerts = [];
  page.on('dialog', (d) => { alerts.push(d.message()); d.dismiss(); });
  await page.goto('https://example.test/');
  await click(page);
  await page.waitForFunction(() => { const p = document.getElementById('s2-acceptance-panel'); return p && /"verdict"/.test(p.textContent); });
  assert.equal(JSON.parse(await page.locator('#s2-acceptance-panel').textContent()).haltReason, 'wrong_origin');
  assert.deepEqual(requests, []);
  assert.deepEqual(alerts, ['S2 HALT: wrong_origin']);
  await page.close();
});

test('acceptance (mocked locally): confirm, six 403s in order, verdict ALL_SIX_BLOCKED_403', async () => {
  const posts = [];
  const { page, requests } = await open(read('bookmarklet.txt'), { api: mockApi({ posts }) });
  const dialogs = [];
  page.on('dialog', (d) => { dialogs.push(d.type()); d.accept(); });
  await click(page);
  await page.waitForFunction(() => /"verdict": "(ALL|HALT)/.test(document.getElementById('s2-acceptance-panel').textContent), null, { timeout: 5000 });
  const log = JSON.parse(await page.locator('#s2-acceptance-panel').textContent());
  assert.equal(log.verdict, 'ALL_SIX_BLOCKED_403');
  assert.deepEqual(posts.map((b) => b.intent), INTENTS);
  assert.ok(posts.every((b) => b.allowFixtureFallback === false && Object.keys(b).length === 2));
  assert.deepEqual(dialogs, ['confirm']);
  assert.ok(requests.every((r) => r.path.startsWith('/api/v1/')));
  await page.close();
});
