'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const puppeteer = require('puppeteer');
const root = path.join(__dirname, '../..');

function model(clientId = 10) {
  return {
    meta: { tenantId: String(clientId), generatedAt: '2026-10-19T12:00:00Z' },
    spatialOverview: {
      generatedAt: '2026-10-19T12:00:00Z', maxAnchor: {},
      domains: ['content', 'acquisition', 'clients', 'campaigns'].map((id, i) => ({
        id, label: id[0].toUpperCase() + id.slice(1), priority: 'normal',
        position: { slot: ['top', 'left', 'right', 'bottom'][i] },
        summary: { lines: ['Monitoring activity'], compressed: 'Monitoring activity' },
        intelligence: { active: false }, actions: [], drawer: { sections: [] },
      })),
    },
    livingBrief: {
      clientId, generatedAt: '2026-10-19T12:00:00Z', pipeline: { total: 17, new24h: 2, warm: 3, booked: 1 }, approvals: { pending: 4 },
      googleAds: { available: true, currency: 'USD', window: { start: '2026-10-13', end: '2026-10-19' }, campaigns: [
        { name: 'Search <img src=x onerror="window.injected=true">', status: 'ENABLED', spend: 20.42, clicks: 3, conversions: null },
        { name: 'PMax', status: 'PAUSED', spend: 300, clicks: 40, conversions: 10 },
      ] },
    },
  };
}

async function setup(t, { role = 'admin', width = 1440, motion = 'no-preference' } = {}) {
  const browser = await puppeteer.launch({ executablePath: process.env.CHROMIUM_PATH || undefined, headless: true, args: ['--no-sandbox', '--disable-dev-shm-usage'] });
  t.after(() => browser.close());
  const page = await browser.newPage();
  await page.setViewport({ width, height: 1000 });
  await page.emulateMediaFeatures([{ name: 'prefers-reduced-motion', value: motion }]);
  const errors = [], writes = [];
  page.on('pageerror', error => errors.push(error.message));
  await page.setRequestInterception(true);
  page.on('request', async request => {
    const url = new URL(request.url());
    if (url.origin !== 'http://localhost') return request.abort();
    if (url.pathname.startsWith('/api/')) {
      let data = {};
      if (url.pathname === '/api/me') data = { user: { name: 'Jacob', role }, active_client_id: 10, client: { display_name: 'Test workspace' } };
      else if (url.pathname === '/api/clients') data = { active_client_id: 10, clients: [{ id: 10, name: 'Test workspace' }, { id: 11, name: 'Other workspace' }] };
      else if (url.pathname === '/api/v1/command-deck') data = model();
      else if (url.pathname === '/api/v1/max/workspace/open') data = { sessionId: 'fixture-session', opening: { fullText: 'Test opening' }, suggestions: [] };
      else if (url.pathname === '/api/v1/max/workspace/ask') data = { sessionId: 'fixture-session', prose: 'Test answer', suggestions: [] };
      if (request.method() !== 'GET') writes.push({ path: url.pathname, body: JSON.parse(request.postData() || '{}') });
      return request.respond({ status: 200, contentType: 'application/json', body: JSON.stringify(data) });
    }
    const file = path.join(root, 'public', url.pathname === '/command-deck' ? 'command-deck.html' : url.pathname);
    if (!fs.existsSync(file) || !fs.statSync(file).isFile()) return request.respond({ status: 404, body: '' });
    const extension = path.extname(file);
    return request.respond({ status: 200, contentType: extension === '.css' ? 'text/css' : extension === '.js' ? 'application/javascript' : 'text/html', body: fs.readFileSync(file) });
  });
  await page.goto('http://localhost/command-deck');
  await page.waitForSelector('.lc-brief-line');
  await page.waitForSelector('.pf-living-shell');
  return { page, errors, writes };
}

test('desktop opening brief, scoped Ads facts and real Max conversation path', async t => {
  const { page, errors, writes } = await setup(t);
  assert.deepEqual(await page.$$eval('.pf-nav-links a', links => links.map(link => link.textContent)), ['Command', 'Pipeline', 'Clients']);
  const ads = await page.$eval('.lc-brief-line:nth-child(2)', element => element.textContent);
  assert.match(ads, /20.42/);
  assert.match(ads, /conversions unavailable/);
  assert.doesNotMatch(ads, /300|40 clicks|10 platform/);
  assert.equal(await page.evaluate(() => window.injected), undefined);
  assert.match(await page.$eval('#lcBriefTitle', element => element.textContent), /Jacob/);
  assert.equal(await page.$eval('#pfNeedsCount', element => element.textContent), '4');
  const before = await page.$eval('.lc-ring-one', element => getComputedStyle(element).transform);
  await new Promise(resolve => setTimeout(resolve, 140));
  assert.notEqual(await page.$eval('.lc-ring-one', element => getComputedStyle(element).transform), before);
  if (process.env.LIVING_COMMAND_QA_DIR) await page.screenshot({ path: path.join(process.env.LIVING_COMMAND_QA_DIR, 'living-command-desktop.png'), fullPage: true });
  const answerResponse = page.waitForResponse(response => response.url().endsWith('/api/v1/max/workspace/ask'));
  await page.click('.lc-brief-line:nth-child(2)');
  await answerResponse;
  await page.waitForFunction(() => !document.getElementById('maxWorkspace').hidden);
  await page.waitForFunction(() => !document.body.classList.contains('lc-thinking'));
  assert.ok(writes.find(write => write.path === '/api/v1/max/workspace/open'));
  const question = writes.find(write => write.path.endsWith('/ask'));
  assert.match(question.body.question, /Penny/);
  assert.equal(question.body.context.tenantId, '10');
  assert.deepEqual(errors, []);
});

test('mobile and reduced motion remain usable; tenant transition clears prior facts', async t => {
  const { page, errors } = await setup(t, { role: 'client', width: 390, motion: 'reduce' });
  assert.deepEqual(await page.$$eval('.pf-nav-links a', links => links.map(link => link.textContent)), ['Command', 'Pipeline', 'My Business']);
  assert.equal(await page.$('#pfTenantSelect'), null);
  assert.equal(await page.$eval('.lc-ring-one', element => getComputedStyle(element).animationName), 'none');
  assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
  if (process.env.LIVING_COMMAND_QA_DIR) await page.screenshot({ path: path.join(process.env.LIVING_COMMAND_QA_DIR, 'living-command-mobile.png'), fullPage: true });
  await page.evaluate(() => window.LivingCommand.clear());
  assert.equal(await page.$eval('#lcBriefLines', element => element.children.length), 0);
  assert.equal(await page.$eval('[data-lc-ask]', element => element.disabled), true);
  assert.deepEqual(errors, []);
});
