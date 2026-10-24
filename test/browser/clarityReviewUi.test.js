'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const puppeteer = require('puppeteer');
const root = path.join(__dirname, '../..');
const A = '00000000-0000-4000-8000-000000000001';
const B = '00000000-0000-4000-8000-000000000002';
const C = '00000000-0000-4000-8000-000000000003';
const payload = '<img src=x onerror="window.injected=true">';
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
const reviewUrl = id => `http://localhost/clarity-review?id=${id}&client_id=10`;

function fixture(id = A) {
  return {
    recommendation: {
      id, client_id: 10, digest: `digest-${id}`, title: id === A ? `Investigate dead clicks ${payload}` : `Recommendation ${id}`,
      summary: `A hypothesis, not a website change. ${payload}`, confidence: 'moderate', status: 'pending',
      evidence: [{ label: `Dead clicks ${payload}`, value: '8.5% of sessions' }],
      validation_steps: [`Inspect the affected control ${payload}`, 'Compare a reviewed test against the same reporting period.'],
      expected_outcome: `Fewer unresponsive interactions ${payload}`,
    },
    report: {
      id: 'report-one', window_start: '2026-10-12', window_end: '2026-10-18', synthetic: true,
      metrics: { sessions: 350, pages_per_session: 1.8, scroll_depth: 38, session_duration: 65, dead_clicks: 8.5, rage_clicks: null, quick_backs: 4, js_errors: 2 },
      deltas_pct: { sessions: 12, dead_clicks: -2 },
      provenance: { source_type: `gmail ${payload}`, gmail_message_id: 'synthetic-message', gmail_thread_id: 'synthetic-thread', content_sha256: 'synthetic-hash', parser_version: 'v1', received_at: '2026-10-19T10:00:00Z' },
    },
    permissions: { can_decide: true }, csrf_token: 'synthetic-csrf-token',
  };
}

async function setup(t, options = {}) {
  const browser = await puppeteer.launch({ executablePath: process.env.CHROMIUM_PATH || '/usr/bin/chromium', headless: true, args: ['--no-sandbox', '--disable-dev-shm-usage'] });
  t.after(() => browser.close());
  const page = await browser.newPage();
  await page.setViewport({ width: options.width || 1280, height: 950 });
  const records = new Map([A, B, C].map(id => [id, fixture(id)]));
  if (options.modify) options.modify(records);
  const reads = [], writes = [], errors = [], outside = [];
  const outcomes = [...(options.outcomes || [])];
  const brief = { clientId: 10, clarity: { available: true, report: records.get(A).report, recommendations: [A, B, C].map(id => records.get(id).recommendation) } };
  if (options.modifyBrief) options.modifyBrief(brief);
  page.on('pageerror', error => errors.push(error.message));
  await page.setRequestInterception(true);
  page.on('request', async request => {
    const url = new URL(request.url());
    if (url.origin !== 'http://localhost') { outside.push(url.href); return request.abort(); }
    if (url.pathname.startsWith('/api/v1/clarity/recommendations/')) {
      const id = url.pathname.split('/')[5];
      if (request.method() === 'GET') {
        reads.push({ url: url.href, id });
        if (options.detailDelay?.[id]) await pause(options.detailDelay[id]);
        const data = records.get(id);
        const code = options.getStatus || (data ? 200 : 404);
        return request.respond({ status: code, contentType: 'application/json', body: JSON.stringify(code === 200 ? data : { error: 'Do not show raw provider or database error' }) }).catch(() => {});
      }
      const body = JSON.parse(request.postData());
      writes.push({ path: url.pathname, body, headers: request.headers() });
      if (options.decisionDelay) await pause(options.decisionDelay);
      const outcome = outcomes.shift();
      if (outcome === 'network') return request.abort('failed').catch(() => {});
      if (typeof outcome === 'number') return request.respond({ status: outcome, contentType: 'application/json', body: JSON.stringify({ error: 'Do not display internal failure' }) });
      const data = records.get(id);
      data.recommendation.status = body.decision === 'approve' ? 'approved' : 'rejected';
      return request.respond({ status: 200, contentType: 'application/json', body: JSON.stringify({ recommendation: data.recommendation, decision: { status: data.recommendation.status, website_changes_authorized: false }, routed: true }) }).catch(() => {});
    }
    if (url.pathname === '/command-deck') return request.respond({ status: 200, contentType: 'text/html', body: `<!doctype html><html><head><link rel="stylesheet" href="/shared/tokens.css"><link rel="stylesheet" href="/command-deck/living-command.css"></head><body><div id="lcWorkspaceLabel"></div><h2 id="lcBriefTitle"></h2><p id="lcBriefIntro"></p><div id="pfNeedsCount"></div><div id="lcBriefLines"></div><p id="lcBriefSource"></p><button id="lcRefresh">Refresh</button><button id="cdSpatialMax">Max</button><script src="/command-deck/living-command.js"></script><script>window.PulseforgeShell = { activeClientId: ${JSON.stringify(options.activeTenant === undefined ? 10 : options.activeTenant)} }; LivingCommand.render(${JSON.stringify({ livingBrief: brief }).replace(/</g, '\\u003c')});</script></body></html>` });
    const file = url.pathname.startsWith('/public/') ? path.join(root,url.pathname) : path.join(root, 'public', url.pathname === '/clarity-review' ? 'clarity-review.html' : url.pathname);
    if (!fs.existsSync(file) || !fs.statSync(file).isFile()) return request.respond({ status: 404, body: '' });
    const ext = path.extname(file);
    return request.respond({ status: 200, contentType: ext === '.css' ? 'text/css' : ext === '.js' ? 'application/javascript' : 'text/html', body: fs.readFileSync(file) });
  });
  return { page, reads, writes, errors, outside, records };
}

async function ready(page, id = A) {
  await page.goto(reviewUrl(id));
  await page.waitForFunction(() => !document.getElementById('clarityContent').hidden);
}

async function choose(page, decision) {
  await page.click(decision === 'approve' ? '#clarityApprove' : '#clarityReject');
  await page.waitForSelector('dialog[open]');
}

test('daily brief links three server-ordered recommendations; escaped visual review, cancel, approval and history', async t => {
  const { page, writes, errors, outside } = await setup(t);
  await page.goto('http://localhost/command-deck');
  assert.deepEqual(await page.$$eval('.lc-clarity-card', nodes => nodes.map(n => new URL(n.href).searchParams.get('id'))), [A, B, C]);
  assert.match(await page.$eval('.lc-clarity', n => n.textContent), /moderate confidence/);
  assert.equal(await page.$('.lc-clarity img'), null);
  await page.click('.lc-clarity-card');
  await page.waitForSelector('#clarityApprove:not([disabled])');
  assert.match(await page.$eval('#clarityContent', n => n.textContent), /Synthetic test data/);
  assert.match(await page.$eval('#clarityContent', n => n.textContent), /Unavailable/);
  assert.equal(await page.$('#clarityContent img'), null);
  assert.equal(await page.evaluate(() => window.injected), undefined);
  await choose(page, 'approve');
  assert.match(await page.$eval('#clarityConfirmCopy', n => n.textContent), /does not authorize implementation/);
  await page.click('#clarityCancel');
  await choose(page, 'reject');
  await page.keyboard.press('Escape');
  assert.equal(writes.length, 0);
  await choose(page, 'approve');
  await page.$eval('#clarityConfirmSubmit', n => { n.click(); n.click(); });
  await page.waitForFunction(() => document.getElementById('clarityStatus').textContent.startsWith('Recommendation approved.'));
  assert.equal(writes.length, 1);
  assert.equal(writes[0].headers['x-csrf-token'], 'synthetic-csrf-token');
  assert.deepEqual(Object.keys(writes[0].body).sort(), ['client_id', 'decision', 'expected_digest', 'idempotency_key']);
  assert.equal(writes[0].body.client_id, 10);
  assert.equal(writes[0].body.expected_digest, `digest-${A}`);
  assert.match(writes[0].body.idempotency_key, /^[a-f0-9-]{36}$/);
  assert.equal(await page.$eval('#clarityActions', n => n.hidden), true);
  await page.goBack();
  await page.waitForSelector('.lc-clarity-card');
  await page.goForward();
  await page.waitForFunction(() => document.getElementById('clarityStatus').textContent.startsWith('Recommendation approved.'));
  assert.equal(writes.length, 1);
  assert.deepEqual(outside, []);
  assert.deepEqual(errors, []);
});

test('mobile rejection is review-only, has no overflow, and remains persisted on reload', async t => {
  const { page, writes, errors } = await setup(t, { width: 390, decisionDelay: 100 });
  await ready(page);
  // The title includes a long UUID and hostile source text. It must wrap,
  // not be clipped or expand the viewport, at each supported narrow width.
  for (const width of [320, 390, 450]) {
    await page.setViewport({ width, height: 950 });
    const layout = await page.evaluate(() => ({
      viewport: innerWidth, scroll: document.documentElement.scrollWidth,
      overflow: [...document.querySelectorAll('main *')].filter(node => node.getBoundingClientRect().right > innerWidth).map(node => node.id || node.className),
    }));
    assert.ok(layout.scroll <= layout.viewport, JSON.stringify(layout));
  }
  await page.setViewport({ width: 390, height: 950 });
  await choose(page, 'reject');
  await page.$eval('#clarityConfirmSubmit', n => { n.click(); n.click(); });
  await page.keyboard.press('Escape');
  await page.waitForFunction(() => document.getElementById('clarityStatus').textContent.startsWith('Recommendation rejected.'));
  await page.reload();
  await page.waitForFunction(() => document.getElementById('clarityStatus').textContent.startsWith('Recommendation rejected.'));
  assert.equal(writes.length, 1);
  assert.equal(writes[0].body.decision, 'reject');
  assert.match(await page.$eval('#clarityStatus', n => n.textContent), /No website changes/);
  assert.deepEqual(errors, []);
});

test('stale digest conflict disables decisions until latest evidence is explicitly reloaded', async t => {
  const { page, writes, records, errors } = await setup(t, { outcomes: [409] });
  await ready(page);
  await choose(page, 'approve');
  await page.click('#clarityConfirmSubmit');
  await page.waitForFunction(() => document.getElementById('clarityStatus').textContent.includes('changed or was already reviewed'));
  assert.equal(await page.$eval('#clarityApprove', n => n.disabled), true);
  records.get(A).recommendation.digest = 'new-exact-evidence';
  records.get(A).recommendation.summary = 'New evidence must be reviewed.';
  await page.click('#clarityReload');
  await page.waitForFunction(() => document.getElementById('clarityContent').textContent.includes('New evidence must be reviewed.'));
  await choose(page, 'approve');
  await page.click('#clarityConfirmSubmit');
  await page.waitForFunction(() => document.getElementById('clarityStatus').textContent.startsWith('Recommendation approved.'));
  assert.equal(writes.length, 2);
  assert.equal(writes[1].body.expected_digest, 'new-exact-evidence');
  assert.notEqual(writes[0].body.idempotency_key, writes[1].body.idempotency_key);
  assert.deepEqual(errors, []);
});

test('uncertain network outcome must be reconciled; retry preserves exact idempotency key', async t => {
  const { page, writes, errors } = await setup(t, { outcomes: ['network'] });
  await ready(page);
  await choose(page, 'approve');
  await page.click('#clarityConfirmSubmit');
  await page.waitForFunction(() => document.getElementById('clarityStatus').textContent.includes('could not be confirmed'));
  assert.equal(await page.$eval('#clarityReject', n => n.disabled), true);
  await page.click('#clarityReload');
  await page.waitForSelector('#clarityApprove:not([disabled])');
  assert.equal(await page.$eval('#clarityReject', n => n.disabled), true);
  await choose(page, 'approve');
  await page.click('#clarityConfirmSubmit');
  await page.waitForFunction(() => document.getElementById('clarityStatus').textContent.startsWith('Recommendation approved.'));
  assert.equal(writes.length, 2);
  assert.deepEqual(writes[1].body, writes[0].body);
  assert.deepEqual(errors, []);
});

test('newer navigation wins over delayed loads and decision responses; tenant changes clear evidence', async t => {
  const { page, writes, errors } = await setup(t, { detailDelay: { [A]: 200 }, decisionDelay: 200 });
  await page.goto(reviewUrl(A));
  await page.evaluate(url => { history.pushState({}, '', url); dispatchEvent(new PopStateEvent('popstate')); }, reviewUrl(B));
  await page.waitForFunction(id => document.getElementById('clarityTitle').textContent.includes(id), {}, B);
  await pause(260);
  assert.equal(await page.$eval('#clarityTitle', n => n.textContent), `Recommendation ${B}`);
  await choose(page, 'approve');
  await page.click('#clarityConfirmSubmit');
  await page.waitForFunction(() => document.getElementById('clarityConfirmSubmit').disabled);
  await page.evaluate(url => { history.pushState({}, '', url); dispatchEvent(new PopStateEvent('popstate')); }, reviewUrl(C));
  await page.waitForFunction(id => document.getElementById('clarityTitle').textContent.includes(id), {}, C);
  await pause(260);
  assert.equal(await page.$eval('#clarityTitle', n => n.textContent), `Recommendation ${C}`);
  assert.equal(await page.$('dialog[open]'), null);
  assert.equal(writes[0].path, `/api/v1/clarity/recommendations/${B}/decision`);
  await page.evaluate(() => document.dispatchEvent(new CustomEvent('pulseforge:tenant-changed')));
  assert.equal(await page.$eval('#clarityContent', n => n.textContent), '');
  assert.equal(await page.$eval('#clarityActions', n => n.hidden), true);
  assert.deepEqual(errors, []);
});

test('readonly permission and missing CSRF token never expose an enabled decision', async t => {
  for (const mode of ['readonly', 'csrf']) {
    const { page, writes, errors } = await setup(t, { modify: records => {
      if (mode === 'readonly') records.get(A).permissions.can_decide = false;
      else delete records.get(A).csrf_token;
    } });
    await ready(page);
    assert.equal(await page.$eval('#clarityActions', n => n.hidden), true);
    await page.$eval('#clarityApprove', n => n.click());
    assert.equal(await page.$('dialog[open]'), null);
    assert.equal(writes.length, 0);
    assert.deepEqual(errors, []);
  }
});

test('invalid scope, cross-tenant responses and backend denial fail closed without source leakage', async t => {
  const { page, reads, writes } = await setup(t, { modify: records => { records.get(A).recommendation.client_id = 11; } });
  await page.goto('http://localhost/clarity-review?id=' + A);
  await page.waitForFunction(() => document.getElementById('clarityTitle').textContent === 'Review link unavailable');
  assert.equal(reads.length, 0);
  await page.goto(reviewUrl(A));
  await page.waitForFunction(() => document.getElementById('clarityTitle').textContent === 'Recommendation unavailable');
  assert.equal(await page.$eval('#clarityContent', n => n.textContent), '');
  assert.equal(writes.length, 0);
  for (const getStatus of [401, 403, 404]) {
    const { page: denied } = await setup(t, { getStatus });
    await denied.goto(reviewUrl(A));
    await denied.waitForFunction(() => document.getElementById('clarityTitle').textContent === 'Recommendation unavailable');
    assert.doesNotMatch(await denied.$eval('#clarityMain', n => n.textContent), /raw provider or database/);
    assert.equal(await denied.$eval('#clarityActions', n => n.hidden), true);
  }
});

test('daily brief rejects oversize or mismatched tenant model instead of choosing recommendations in UI', async t => {
  for (const mode of ['oversize', 'tenant']) {
    const { page } = await setup(t, { modifyBrief: brief => {
      if (mode === 'oversize') brief.clarity.recommendations.push({ ...fixture(A).recommendation });
      else brief.clarity.recommendations[0].client_id = 11;
    } });
    await page.goto('http://localhost/command-deck');
    assert.equal(await page.$('.lc-clarity-card'), null);
    assert.match(await page.$eval('.lc-clarity', n => n.textContent), /No current Clarity report/);
  }
});

test('dark feature and missing consent omit Clarity; current shell tenant overrides a late old model', async t => {
  for (const reason of ['disabled', 'not_authorized']) {
    const { page } = await setup(t, { modifyBrief: brief => { brief.clarity = { available: false, reason }; } });
    await page.goto('http://localhost/command-deck');
    assert.equal(await page.$('.lc-clarity'), null);
  }
  const { page } = await setup(t);
  await page.goto('http://localhost/command-deck');
  assert.ok(await page.$('.lc-clarity'));
  await page.evaluate(() => {
    window.PulseforgeShell.activeClientId = 11;
    document.dispatchEvent(new CustomEvent('pulseforge:tenant-changed'));
  });
  assert.equal(await page.$('.lc-clarity'), null);
  await page.evaluate(data => LivingCommand.render({ livingBrief: { clientId: 10, clarity: { available: true, report: data.report, recommendations: [data.recommendation] } } }), fixture());
  assert.equal(await page.$('.lc-clarity'), null);
});
