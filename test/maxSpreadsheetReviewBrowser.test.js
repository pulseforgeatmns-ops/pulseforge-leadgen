'use strict';
// Optional real Chromium UI exercise against a loopback-only authenticated fixture API.
// No application server/database/env file or external service is imported.
const test = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const { readFileSync, existsSync } = require('node:fs');
const { once } = require('node:events');
const path = require('node:path');
const puppeteer = require('puppeteer');
const { launchTestBrowser } = require('./helpers/puppeteerTestBrowser');
const run = process.env.MAX_SPREADSHEET_BROWSER_TEST === '1';

test('Chromium: authenticated local review, exact selection, negative save, held rows and no external requests', { skip: !run, timeout: 30000 }, async t => {
  assert.ok(existsSync(puppeteer.executablePath()), 'Install the pinned Puppeteer browser to run this gate');
  const received = [];
  const proposal = { id: 'review-browser-1', digest: 'immutable', sourceHash: 'actual-source-sha', tenantId: 10, actorId: 7, aoId: 7, conversationId: 'local-browser', can_approve: true, plan: {
    rows: [{ sheet: 'Sheet1', rowNumber: 7, company: 'University identity', outcome: 'HELD', evidence: 'SNHU name conflicts with UNH email/site', accountResolution: { status: 'ambiguous', candidates: ['university-1'] }, candidateSummaries: [{ id: 'university-1', name: 'Verified University', address: '1 Campus Road', email: 'facilities@example.test' }], contacts: [{ name: 'Robert', status: 'unresolved', candidates: ['contact-1'], candidateSummaries: [{ id: 'contact-1', name: 'Robert Example', title: 'Facilities manager' }] }], provider: { raw: 'Campus Facilities', status: 'unresolved', candidateSummaries: [{ id: 'provider-1', name: 'Campus Facilities LLC', address: '2 Campus Road' }] }, conflicts: [{ code: 'IDENTITY_CONFLICT', message: 'Verify organization identity' }] }],
    operations: [
      { id: 'note-1', type: 'ADD_NOTE', target: { accountId: 8 }, before: null, after: 'Mark is now decision maker', evidence: { cell: 'L8' } },
      { id: 'call-1', type: 'SUPPRESS_CALL', target: { accountId: 11 }, before: false, after: true, evidence: { cell: 'K11', raw: 'No - Remove from call list' } },
      { id: 'held-1', type: 'CREATE_ACCOUNT', after: { name: 'Unresolved identity', outreachReviewRequired: true }, blocked: true, evidence: { cell: 'A7' } },
    ],
  } };
  let liveProposal = proposal;
  const resolutionRequests = [];
  const script = readFileSync(path.join(__dirname, '../public/shared/spreadsheetReview.js'));
  const server = http.createServer(async (req, res) => {
    if (req.url === '/') { res.setHeader('content-type', 'text/html; charset=utf-8'); res.setHeader('set-cookie', 'test_session=jake; HttpOnly; SameSite=Strict'); res.end('<meta charset="utf-8"><div id="scope"></div><div id="proposal"></div><div id="messages"></div><script src="/review.js"></script>'); return; }
    if (req.url === '/review.js') { res.setHeader('content-type', 'text/javascript; charset=utf-8'); res.end(script); return; }
    res.setHeader('content-type', 'application/json');
    if (req.headers.cookie !== 'test_session=jake') { res.statusCode = 401; res.end('{}'); return; }
    if (req.url === '/api/v1/max/spreadsheet/scope') { res.end(JSON.stringify({ tenant_id: 10, actor_id: 1, ao_id: null, can_approve: true, aos: [{ id: 7, name: 'Tony' }] })); return; }
    if (req.url === '/api/v1/max/spreadsheet/proposals?ao_id=7') { res.end(JSON.stringify({ proposals: [{ id: liveProposal.id, conversationId: liveProposal.conversationId, actorId: 7, status: liveProposal.status || 'pending' }] })); return; }
    if (req.method === 'GET' && req.url.startsWith('/api/v1/max/spreadsheet/proposals/')) { res.end(JSON.stringify({ spreadsheet_proposal: liveProposal, can_approve: true })); return; }
    if (req.method === 'POST' && req.url === '/api/v1/max/spreadsheet/proposals/review-browser-1/resolve') {
      let body = ''; for await (const chunk of req) body += chunk;
      const parsed = JSON.parse(body); resolutionRequests.push(parsed);
      if (!parsed.resolutions[0].acknowledgedIdentityConflict) { res.statusCode = 422; res.end(JSON.stringify({ error: 'identity_still_unresolved' })); return; }
      liveProposal = { ...proposal, id: 'review-browser-2', digest: 'fresh-immutable' };
      res.end(JSON.stringify({ spreadsheet_proposal: liveProposal, preview_only: true })); return;
    }
    if (req.method === 'POST' && req.url === '/api/v1/max/spreadsheet/proposals/review-browser-2/commit') {
      let body = ''; for await (const chunk of req) body += chunk;
      received.push(JSON.parse(body)); liveProposal = { ...liveProposal, status: 'committed', receipt: { committed: true, selectedOperationIds: received[0].operation_ids } }; res.end(JSON.stringify({ ok: true, committed: true, spreadsheet_commit: liveProposal.receipt, operational_response: 'Selected note verified; call suppression unselected; university held.' })); return;
    }
    res.statusCode = 404; res.end('{}');
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  t.after(() => new Promise(resolve => { server.closeAllConnections(); server.close(resolve); }));
  const browser = await launchTestBrowser(puppeteer);
  t.after(async () => {
    await browser.close();
  });
  const page = await browser.newPage();
  const origin = `http://127.0.0.1:${server.address().port}`;
  await page.setRequestInterception(true);
  const unexpected = [];
  page.on('request', request => { if (request.url().startsWith(origin + '/')) request.continue(); else { unexpected.push(request.url()); request.abort(); } });
  await page.goto(origin);
  await page.evaluate(async () => {
    window.review = PulseforgeSpreadsheetReview.create({ fetch: (...args) => fetch(...args), host: document.getElementById('proposal'), scopeHost: document.getElementById('scope'), onMessage: message => { document.getElementById('messages').textContent = message; } });
    await review.loadScope();
  });
  await page.select('[data-spreadsheet-ao]', '7');
  await page.click('[data-spreadsheet-refresh]');
  await page.waitForSelector('[data-resume-choice]');
  await page.select('[data-resume-choice]', '0');
  await page.click('[data-resume-load]');
  await page.waitForSelector('[data-resolution-submit]');
  assert.match(await page.$eval('[data-resolution-account]', node => node.textContent), /Verified University · facilities@example.test · 1 Campus Road/);
  assert.match(await page.$eval('[data-resolution-contact]', node => node.textContent), /Robert Example · Facilities manager/);
  assert.match(await page.$eval('#proposal', node => node.textContent), /Outreach held for separate authorization/);
  await page.select('[data-resolution-account]', 'university-1');
  await page.select('[data-resolution-contact]', 'contact-1');
  await page.select('[data-resolution-provider]', 'provider-1');
  await page.type('[data-resolution-evidence]', 'Confirmed legal identity against supplied source records.');
  await page.click('[data-resolution-submit]');
  await page.waitForFunction(() => document.getElementById('messages').textContent === 'identity_still_unresolved');
  assert.equal(received.length, 0, 'failed resolution is not approval or commit');
  assert.match(await page.$eval('[data-resolution-account]', node => node.textContent), /Verified University · facilities@example.test · 1 Campus Road/);
  assert.match(await page.$eval('[data-resolution-contact]', node => node.textContent), /Robert Example · Facilities manager/);
  assert.match(await page.$eval('#proposal', node => node.textContent), /Outreach held for separate authorization/);
  await page.select('[data-resolution-account]', 'university-1');
  await page.select('[data-resolution-contact]', 'contact-1');
  await page.select('[data-resolution-provider]', 'provider-1');
  await page.type('[data-resolution-evidence]', 'Confirmed legal identity against supplied source records.');
  await page.click('[data-resolution-identity]');
  await page.click('[data-resolution-submit]');
  await page.waitForFunction(() => document.getElementById('proposal').textContent.includes('review-browser-2'));
  assert.equal(resolutionRequests.length, 2);
  assert.equal(resolutionRequests[1].resolutions[0].providerId, 'provider-1');
  assert.deepEqual(resolutionRequests[1].resolutions[0].contacts, [{ sourceName: 'Robert', contactId: 'contact-1' }]);
  assert.equal(received.length, 0, 'fresh proposal has not been saved');
  assert.equal(await page.$$eval('[data-spreadsheet-operation]:checked', inputs => inputs.length), 2);
  assert.equal(await page.$eval('[data-spreadsheet-operation="held-1"]', input => input.disabled), true);
  await page.evaluate(() => review.handleText('Do not save those updates yet'));
  assert.equal(received.length, 0);
  await page.click('[data-spreadsheet-operation="call-1"]');
  await page.click('[data-spreadsheet-save]');
  await page.waitForFunction(() => document.getElementById('messages').textContent.includes('Selected note verified'));
  assert.equal(received.length, 1);
  assert.deepEqual(received[0].operation_ids, ['note-1']);
  assert.equal(received[0].proposal_digest, 'fresh-immutable');
  assert.equal(received[0].ao_id, 7);
  assert.equal(received[0].plan, undefined);
  assert.match(await page.$eval('#proposal', node => node.textContent), /SNHU name conflicts with UNH/);
  assert.equal(await page.$('[data-spreadsheet-save]'), null);
  await page.reload();
  await page.evaluate(async () => {
    window.review = PulseforgeSpreadsheetReview.create({ fetch: (...args) => fetch(...args), host: document.getElementById('proposal'), scopeHost: document.getElementById('scope') });
    await review.loadScope();
  });
  await page.select('[data-spreadsheet-ao]', '7');
  await page.click('[data-spreadsheet-refresh]'); await page.waitForSelector('[data-resume-choice]');
  await page.select('[data-resume-choice]', '0'); await page.click('[data-resume-load]');
  await page.waitForFunction(() => document.getElementById('proposal').textContent.includes('Saved result recovered'));
  assert.equal(received.length, 1, 'reload recovers receipt without another commit');
  assert.equal(await page.$('[data-spreadsheet-save]'), null);
  assert.deepEqual(unexpected, []);
});
