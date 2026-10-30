'use strict';
// Chrome (headless Chromium) check of the Scout research approval page's candidate
// and spend reporting. Serves the real page assets on loopback under the page's
// CSP, with a synthetic receipt shaped like the Oct 29 Tenant 10 grant (eight
// cycles, 79 tickets, 19 accepted, 18 retained, 34 awaiting evidence, 1,845,000
// reserved micros). Business names are synthetic. No app server, DB or network.
// Set SCREENSHOT_DIR to keep desktop and phone captures.
const test = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { once } = require('node:events');
const puppeteer = require('puppeteer');

const PAGE = '/admin/scout-research-approval';
const CSP = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'";
const ASSETS = { [`${PAGE}/assets/style.css`]: ['scout-research-approval.css', 'text/css'],
  [`${PAGE}/assets/controller.js`]: ['scout-research-approval.js', 'text/javascript'],
  [`${PAGE}/assets/tokens.css`]: ['shared/tokens.css', 'text/css'] };

const CYCLE_MICROS = [40000, 40000, 515000, 215000, 290000, 440000, 215000, 90000];
const RETAINED_BY_CYCLE = [0, 0, 7, 5, 4, 0, 2, 0];
function receipt() {
  const retainedRows = RETAINED_BY_CYCLE.flatMap((n, i) => Array.from({ length: n }, (_, j) => ({ cycleNumber: i + 1,
    company: `Synthetic Facility ${i + 1}-${j + 1}`, domain: `synthetic-${i + 1}-${j + 1}.example`,
    vertical: ['school', 'campus', 'commercial_office', 'restaurant', 'warehouse'][j % 5], location: 'Manchester, NH', reconciled: false })));
  const types = [['car_dealer'], ['school', 'point_of_interest'], ['cafe', 'food'], ['point_of_interest', 'establishment'], ['storage']];
  const awaitingRows = Array.from({ length: 34 }, (_, i) => ({ state: 'deferred', reason: 'evidence_budget_deferred',
    name: `Synthetic Unclassified ${i + 1}`, location: '1 Main St, Manchester, NH 03101', website: `synthetic-u${i + 1}.example`,
    discoveryQuery: 'auto dealership Manchester NH', placeTypes: types[i % types.length] }));
  return { status: 'exhausted', grantId: 'a'.repeat(64), approvalRef: 'synthetic-research-only-2026-10-29', tenantId: '10',
    maxCycles: 8, cyclesConsumed: 8, cyclesRemaining: 0, dailyGoogleUsdMicros: 5000000,
    notBefore: '2026-10-29T15:00:00Z', expiresAt: '2026-11-05T15:00:00Z', installedAt: '2026-10-29T15:30:00Z',
    observedAt: new Date().toISOString(), expectedRevision: 'b'.repeat(40), researchMissionId: 'mission_anchor_facility_research_00000000000000000000',
    spend: [{ day: '2026-10-29', state: 'ok', operation: 'text_search', attempts: 8, micros: 320000 },
      { day: '2026-10-29', state: 'ok', operation: 'place_details', attempts: 61, micros: 1525000 }],
    reservedUsdMicros: CYCLE_MICROS.reduce((a, b) => a + b, 0), spendBasis: 'reserved_upper_bound',
    cycles: CYCLE_MICROS.map((micros, i) => ({ cycle_number: i + 1, state: 'completed', result: { googleReservedUsdMicros: micros } })),
    candidates: {
      retained: { total: 18, reconciled: 0, listed: 18, rows: retainedRows },
      // Cycles that ran before this change recorded only the retained count.
      perCycle: RETAINED_BY_CYCLE.map((n, i) => ({ cycleNumber: i + 1, state: 'completed', acceptedCandidates: null,
        retainedCandidates: n, evaluatedCandidates: null })),
      ledger: { scope: 'search_fingerprint', fingerprint: 'c'.repeat(64), total: 79,
        byState: { pending: 0, deferred: 51, blocked: 8, handled: 20 },
        byReason: { queued: 19, evidence_budget_deferred: 34, ownership_policy_observed: 17, geography_unresolved: 5,
          identity_incomplete: 3, policy_excluded: 1 },
        acceptedTickets: 19, acceptedNotRetained: 1, awaitingBusinessEvidence: { total: 34, listed: 34, rows: awaitingRows } } } };
}

test('Chromium: research approval page reports candidates and exact reserved spend', { timeout: 60000 }, async t => {
  const external = [];
  const requests = [];
  const cohort = { tenantId: '10', grantId: 'a'.repeat(64), records: Array.from({ length: 79 }, (_, i) => ({
    candidate: { name: `Synthetic Raw Facility ${i}` }, previous: { reason: i < 34 ? 'evidence_budget_deferred' : 'ownership_policy_observed' } })) };
  const server = http.createServer((req, res) => {
    requests.push({ method: req.method, path: req.url });
    res.setHeader('Content-Security-Policy', CSP); res.setHeader('Cache-Control', 'no-store');
    const asset = ASSETS[req.url];
    if (req.url === PAGE || asset) {
      const [file, type] = asset || ['scout-research-approval.html', 'text/html'];
      res.setHeader('content-type', `${type}; charset=utf-8`);
      res.end(fs.readFileSync(path.join(__dirname, '../../public', file))); return;
    }
    res.setHeader('content-type', 'application/json');
    if (req.url === '/api/v1/admin/scout-research-approval/context') {
      res.end(JSON.stringify({ name: 'Synthetic admin', role: 'admin', email: 'admin@example.test', approvalReady: true,
        activeTenant: { id: 10, name: 'Anchor Cleaning' } })); return;
    }
    if (req.method === 'POST' && req.url === '/api/v1/tenant-outreach/anchor-program/research-only-grant/inspect') {
      res.end(JSON.stringify(receipt())); return;
    }
    if (req.method === 'POST' && req.url === '/api/v1/tenant-outreach/anchor-program/research-only-grant/export') {
      let body = ''; req.on('data', chunk => { body += chunk; });
      req.on('end', () => { assert.deepEqual(JSON.parse(body), { grantId: 'a'.repeat(64) }); res.end(JSON.stringify(cohort)); });
      return;
    }
    res.statusCode = 404; res.end('{}');
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  t.after(() => new Promise(resolve => { server.closeAllConnections(); server.close(resolve); }));
  const origin = `http://127.0.0.1:${server.address().port}`;
  // Same launch as the other approval-page browser suite; loopback only.
  const browser = await puppeteer.launch({ executablePath: process.env.CHROMIUM_PATH || '/opt/pw-browsers/chromium', headless: true, pipe: true,
    args: ['--no-sandbox', '--disable-dev-shm-usage', '--disable-background-networking',
      '--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE 127.0.0.1'] });
  t.after(() => browser.close());
  const shots = process.env.SCREENSHOT_DIR;
  const downloads = fs.mkdtempSync(path.join(os.tmpdir(), 'scout-cohort-download-'));
  t.after(() => fs.rmSync(downloads, { recursive: true, force: true }));

  for (const [label, viewport] of [['desktop', { width: 1280, height: 900 }], ['phone', { width: 390, height: 844 }]]) {
    const page = await browser.newPage();
    await page.setViewport(viewport);
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    page.on('console', message => { if (message.type() === 'error') errors.push(message.text()); });
    page.on('request', request => { const url = request.url(); if (/^(https?|wss?):/.test(url) && !url.startsWith(origin)) external.push(url); });
    await page.goto(origin + PAGE, { waitUntil: 'domcontentloaded' });
    await page.waitForSelector('#grantStatus .tally');
    await page.waitForFunction(() => !document.querySelector('#exportCohort').disabled);
    const downloadDirectory = path.join(downloads, label); fs.mkdirSync(downloadDirectory);
    await (await page.createCDPSession()).send('Page.setDownloadBehavior', { behavior: 'allow', downloadPath: downloadDirectory });
    await page.click('#exportCohort');
    await page.waitForFunction(() => document.querySelector('#status').textContent.includes('Downloaded 79 existing research records'));
    const filename = path.join(downloadDirectory, `scout-research-cohort-${'a'.repeat(12)}.json`);
    const deadline = Date.now() + 5000;
    while (!fs.existsSync(filename) && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 20));
    assert.deepEqual(JSON.parse(fs.readFileSync(filename, 'utf8')), cohort, 'the complete JSON actually downloaded');
    const text = await page.$eval('#grantStatus', node => node.innerText);
    assert.match(text, /\$1\.845 total/, 'exact reserved total, not the float-rounded $1.84');
    assert.doesNotMatch(text, /\$1\.84 /);
    assert.match(text, /upper-bound reservations, not Google’s invoice/);
    assert.match(text, /\$5\.00 per UTC day/);
    const tallies = await page.$$eval('.tally > div', nodes => nodes.map(node => node.innerText));
    assert.deepEqual(tallies.map(value => value.split('\n')[0]), ['18', '19', '1', '34']);
    assert.match(tallies[0], /Retained in the research ledger/); assert.match(tallies[1], /Accepted by admission/);
    assert.match(tallies[2], /Accepted, not retained/); assert.match(tallies[3], /Awaiting website evidence/);
    assert.match(text, /79 unique businesses/);
    assert.match(text, /Already owned, contacted or assigned in Anchor\s+17/);
    assert.match(text, /ran before accepted and evaluated counts were recorded/);
    assert.match(text, /Nothing on this list has been enriched, prepared or contacted/);
    const lists = await page.$$('#grantStatus details');
    assert.equal(lists.length, 2);
    await page.$$eval('#grantStatus details', nodes => { nodes[1].open = true; });
    assert.match(await page.$$eval('#grantStatus details', nodes => nodes[1].innerText), /car_dealer/);
    // Wide tables scroll inside their own container instead of crushing columns.
    assert.ok(await page.$$eval('.table-wrap', nodes => nodes.every(node => getComputedStyle(node).overflowX === 'auto')));
    // Tablet/phone: the page itself never scrolls sideways; wide tables scroll in place.
    const overflow = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
    assert.ok(overflow <= 0, `${label}: no horizontal page scroll (${overflow}px)`);
    assert.deepEqual(errors, []);
    if (shots) {
      fs.mkdirSync(shots, { recursive: true });
      // Phone capture shows the summary; desktop also shows both lists.
      await page.$$eval('#grantStatus details', (nodes, open) => { nodes.forEach(node => { node.open = open; }); }, label === 'desktop');
      const area = await page.$('#grantStatus');
      await area.screenshot({ path: path.join(shots, `scout-research-approval-candidates-${label}.png`) });
    }
    await page.close();
  }
  assert.deepEqual(external, [], 'no external requests');
  assert.ok(requests.filter(r => r.method === 'POST').every(r => /research-only-grant\/(inspect|export)$/.test(r.path)),
    'the download never requests approval, grant installation, revocation or discovery');
});
