'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const puppeteer = require('puppeteer');
const { startHarness, browserLogin, launchOptions, pagePath, apiBase, preparePath } = require('../helpers/anchorPreparationApprovalBrowserHarness');

const noEffects = { consumed: 0, children: 0, reservations: 0, execution_approvals: 0 };
const readyEffects = { ...noEffects, consumed: 1, children: 1 };
const claimedEffects = { ...noEffects, consumed: 1 };
const preparationStorageKey = 'pulseforge.anchorPreparation.attempt.v1';
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(check, description) {
  const end = Date.now() + 10000;
  while (Date.now() < end) { if (await check()) return; await sleep(20); }
  assert.fail(`Timed out: ${description}`);
}
function barrier() {
  let release;
  const promise = new Promise(resolve => { release = resolve; });
  return { promise, release };
}
function loseCommittedResponse(res) {
  // Send headers and an incomplete body after the real commit. This is an
  // interrupted response, not a pre-header connection failure Chromium may
  // transparently retry at its transport layer.
  res.status(200).set('Content-Type', 'application/json');
  res.write('{"interrupted":', () => res.destroy());
  return res;
}
const operationRequests = (h, operation) => h.requests.filter(r => r.method === 'POST' && r.path === apiBase + operation);
const preparationRequests = h => h.requests.filter(r => r.method === 'POST' && r.path === preparePath);
const mutationRequests = h => h.requests.filter(r => r.method === 'POST' && /\/(?:install|revoke)$/.test(r.path));
async function field(page, selector, value) {
  await page.$eval(selector, (input, text) => { input.value = text; input.dispatchEvent(new Event('input', { bubbles: true }));
    input.dispatchEvent(new Event('change', { bubbles: true })); }, String(value));
}
async function check(page, selector, desired = true) {
  if (await page.$eval(selector, el => el.checked) !== desired) await page.click(selector);
}
async function enabled(page, selector) { return page.$eval(selector, el => !el.disabled); }
async function open(page, h) {
  const response = await page.goto(h.origin + pagePath);
  assert.equal(response.status(), 200);
  await page.waitForFunction(() => !document.querySelector('#receiptButton').disabled);
}
async function fillIntent(page, h, { review = true, overrides = {} } = {}) {
  const i = { ...h.intent(), ...overrides };
  for (const name of ['manifestId','programId','sourceMissionId','approvalReference','preparationApprovalReference','dailyOperationCap','dailyItemCap']) {
    await field(page, '#' + name, i[name === 'manifestId' ? 'id' : name]);
  }
  for (const name of ['validFrom','expiresAt']) await field(page, '#' + name, i[name].replace(/\.\d{3}Z$/, '').replace(/Z$/, ''));
  await field(page, '[name=prospectId]', i.admissions[0].prospectId);
  await field(page, '[name=admissionReference]', i.admissions[0].approvalReference);
  await check(page, '[name=reviewSelected]', review);
  if (review) await field(page, '#reviewApprovalReference', 'synthetic-browser-exact-review');
  await check(page, '#newApprovalAcknowledgment');
  return i;
}
async function preview(page, h, options) {
  await fillIntent(page, h, options);
  await page.waitForFunction(() => !document.querySelector('#previewButton').disabled);
  const previous = h.responses.length;
  await page.click('#previewButton');
  await until(() => h.responses.slice(previous).some(r => r.path === apiBase + 'preview'), 'actual preview response');
  const response = h.responses.slice(previous).find(r => r.path === apiBase + 'preview');
  assert.equal(response.status, 200);
  await page.waitForSelector('#previewSection:not([hidden])');
  return response.body;
}
async function confirmReady(page, { review = true } = {}) {
  await check(page, '#recoverySaved'); await check(page, '#grantConfirm');
  if (review) await check(page, '#reviewConfirm');
  await page.waitForFunction(() => !document.querySelector('#installButton').disabled);
  await page.click('#installButton');
  await page.waitForSelector('#confirmDialog[open]');
}
async function install(page, h) {
  await confirmReady(page);
  await page.click('#confirmSubmit');
  await until(async () => await h.grantCount() === 1, 'actual grant commit');
  await page.waitForFunction(() => !document.querySelector('#confirmDialog').open);
}
async function receipt(page, h, p) {
  await field(page, '#receiptManifestId', p.proposal.id);
  await field(page, '#receiptManifestHash', p.manifestHash);
  const n = h.responses.length;
  await page.click('#receiptButton');
  await until(() => h.responses.slice(n).some(r => r.path === apiBase + 'preview'), 'actual receipt response');
  await page.waitForFunction(() => !document.querySelector('#receiptButton').disabled);
  return h.responses.slice(n).find(r => r.path === apiBase + 'preview').body;
}
async function preparationSetup(page, h, options) {
  await open(page, h); const p = await preview(page, h, options); await install(page, h);
  const r = await receipt(page, h, p);
  assert.equal(r.preparationIntent.originalSessionMatches, true);
  assert.equal(r.preparationOutcome.status, 'not_recorded');
  assert.equal(await enabled(page, '#prepareButton'), false, 'grant installation is not preparation consent');
  return p;
}
async function confirmPreparation(page) {
  await check(page, '#prepareConfirm');
  await page.waitForFunction(() => !document.querySelector('#prepareButton').disabled);
  await page.click('#prepareButton'); await page.waitForSelector('#confirmDialog[open]');
}
async function prepare(page, h) {
  await confirmPreparation(page); const n = h.responses.length; await page.click('#confirmSubmit');
  await until(() => h.responses.slice(n).some(r => r.path === preparePath), 'actual guarded preparation response');
  await page.waitForFunction(() => !document.querySelector('#receiptButton').disabled);
  return h.responses.slice(n).find(r => r.path === preparePath);
}
async function assertEffects(h, expected = noEffects, preparation = false) {
  assert.deepEqual(await h.effects(), expected);
  assert.equal(h.deny.attempts(), 0);
  if (!preparation) assert.equal(preparationRequests(h).length, 0);
  assert.equal(h.requests.some(r => /\/cron\/|\/api\/run\/|\/execute$|\/authorize$|\/mode$/.test(r.path)), false);
}

test('tablet administrator journeys use real persisted sessions and guarded preparation transactions', { timeout: 240000 }, async t => {
  const h = await startHarness();
  let browser, blockedNativeFonts = 0;
  t.after(async () => { if (browser) await browser.close(); await h.close();
    t.diagnostic(`Browser transport: ${blockedNativeFonts} existing native-login Google Fonts attempts aborted; zero external requests allowed; new UI resources restricted to fixture origin.`); });
  browser = await puppeteer.launch(launchOptions());
  async function journey(name, work, { login = true, selectTenant = true, effects = noEffects, preparation = false } = {}) {
    await t.test(name, async () => {
      await h.reset(); const page = await browser.newPage(), errors = [], external = [];
      page.setDefaultTimeout(10000);
      await page.setViewport({ width: 820, height: 1180, isMobile: true, hasTouch: true, deviceScaleFactor: 1 });
      await page.setRequestInterception(true);
      page.on('request', request => {
        if (new URL(request.url()).protocol === 'data:') return request.continue();
        if (new URL(request.url()).origin === h.origin) return request.continue();
        // The pre-existing native login template references Google Fonts. Deny
        // that asset without altering its markup; new approval-page resources
        // must still be exclusively same-origin and every other attempt fails.
        if (new URL(request.url()).hostname === 'fonts.googleapis.com'
          && request.frame()?.url().startsWith(h.origin + '/login')) { blockedNativeFonts++; return request.abort(); }
        external.push(request.url().split('?')[0]); return request.abort();
      });
      page.on('pageerror', error => errors.push(error.message));
      page.on('dialog', dialog => dialog.accept());
      try {
        if (login) await browserLogin(page, h, { selectTenant });
        await work(page);
        await assertEffects(h, effects, preparation);
        assert.deepEqual(errors, [], 'browser must not have uncaught JavaScript errors');
        assert.deepEqual(external, [], 'no browser request may reach an external provider');
      } finally { for (const key of Object.keys(h.faults)) delete h.faults[key]; await page.close(); }
    });
  }

  await journey('unauthenticated, interrupted and incorrect native login cannot expose approval controls', async page => {
    const denied = await page.goto(h.origin + pagePath); assert.equal(denied.status(), 401);
    assert.equal(await page.$('#intentForm'), null);
    await page.click('a[href="/login?workflow=identity-check"]');
    await page.waitForSelector('input[name=password]');
    await field(page, 'input[name=email]', 'identity@example.test');
    await field(page, 'input[name=password]', 'incorrect-synthetic-password');
    await Promise.all([page.waitForNavigation(), page.click('button[type=submit]')]);
    assert.match(page.url(), /\/login\?error=/);
    await page.goto(h.origin + pagePath); assert.equal(await page.$('#intentForm'), null);
    assert.equal(await h.grantCount(), 0); assert.equal(await h.reviewed(), null);
    assert.equal(mutationRequests(h).length, 0);
  }, { login: false });

  await journey('wrong selected tenant is denied until explicit native tenant selection', async page => {
    const denied = await page.goto(h.origin + pagePath); assert.equal(denied.status(), 403);
    assert.equal(await page.$('#intentForm'), null);
    await page.click('a[href="/identity-check/tenant"]');
    await page.waitForSelector('button[name=confirm]');
    await Promise.all([page.waitForNavigation(), page.click('button[name=confirm]')]);
    await open(page, h);
    assert.match(await page.$eval('#approvalContext', el => el.textContent), /10/);
    assert.equal(await page.$eval('[name=reviewSelected]', el => el.checked), false);
    assert.equal(await page.$eval('#manifestId', el => el.value), '');
    assert.equal(mutationRequests(h).length, 0);
  }, { selectTenant: false });

  await journey('a failed native login session save cannot create a falsely verified approval page', async page => {
    const original = h.store.set;
    h.store.set = (_sid, _session, callback) => callback(new Error('synthetic login-store failure'));
    try {
      await browserLogin(page, h, { selectTenant: false });
      assert.equal((await h.currentSessions()).rows.length, 0, 'failed login must not leave an authenticated session row');
      const denied = await page.goto(h.origin + pagePath); assert.equal(denied.status(), 401);
      assert.equal(await page.$('#intentForm'), null);
      assert.equal(mutationRequests(h).length, 0); assert.equal(await h.grantCount(), 0);
    } finally { h.store.set = original; }
  }, { login: false });

  await journey('page, context and all fetched assets do not touch or rewrite the authenticated session', async page => {
    const before = (await h.pool.query('SELECT sid,sess,expire FROM session ORDER BY sid')).rows;
    await open(page, h);
    const after = (await h.pool.query('SELECT sid,sess,expire FROM session ORDER BY sid')).rows;
    assert.deepEqual(after, before, 'read-only page/assets/context must not extend or overwrite session authority');
    for (const alias of ['/public/anchor-preparation-approval.html', '/anchor-preparation-approval.html']) {
      const response = await page.goto(h.origin + alias); assert.equal(response.status(), 200);
      await page.waitForFunction(() => !document.querySelector('#receiptButton').disabled);
    }
    assert.deepEqual((await h.pool.query('SELECT sid,sess,expire FROM session ORDER BY sid')).rows, before);
    await h.pool.query("UPDATE users SET active=false WHERE id='3'");
    for (const alias of [pagePath, '/public/anchor-preparation-approval.html', '/anchor-preparation-approval.html', apiBase + 'context']) {
      const response = await page.goto(h.origin + alias); assert.equal(response.status(), 403, alias);
      assert.equal(await page.$('#intentForm'), null);
    }
  });

  await journey('canonical page/context reject a foreign Origin without falsely authenticating', async page => {
    await page.setExtraHTTPHeaders({ Origin: 'https://synthetic-foreign-origin.invalid' });
    for (const target of [pagePath, apiBase + 'context']) {
      const response = await page.goto(h.origin + target); assert.equal(response.status(), 403, target);
      assert.equal(await page.$('#intentForm'), null);
    }
    assert.equal(h.requests.filter(r => r.origin === 'https://synthetic-foreign-origin.invalid').length >= 2, true);
    assert.equal(await h.grantCount(), 0); assert.equal(mutationRequests(h).length, 0);
  });

  await journey('real preview, separate confirmations, install, exact receipt and revocation preserve no-send boundaries', async page => {
    await open(page, h); const p = await preview(page, h);
    assert.equal(p.installable, true); assert.equal(await h.reviewed(), null); assert.equal(await h.grantCount(), 0);
    assert.equal(await enabled(page, '#installButton'), false);
    await check(page, '#grantConfirm'); assert.equal(await enabled(page, '#installButton'), false);
    await check(page, '#recoverySaved'); assert.equal(await enabled(page, '#installButton'), false);
    await check(page, '#reviewConfirm');
    await page.click('#installButton'); await page.waitForSelector('#confirmDialog[open]');
    assert.equal(mutationRequests(h).length, 0, 'opening confirmation is not approval');
    await page.click('#confirmCancel'); assert.equal(await h.reviewed(), null);
    await install(page, h);
    assert.equal(await h.reviewed(), false); assert.equal(operationRequests(h, 'install').length, 1);
    const installed = await receipt(page, h, p); assert.equal(installed.status, 'installed');
    assert.equal(installed.reviewInitializationCount, 1); assert.equal(installed.currentReviewState, 'not_checked');
    await h.pool.query('UPDATE prospects SET ao_outreach_review_required=true WHERE id::text=$1', [h.f.manifest.admissions[0].prospectId]);
    const historical = await receipt(page, h, p); assert.equal(historical.currentReviewState, 'not_checked');
    await field(page, '#revokeReference', 'synthetic-browser-explicit-revocation');
    await page.click('#revokeButton'); await page.waitForSelector('#confirmDialog[open]');
    assert.equal(operationRequests(h, 'revoke').length, 0);
    await page.click('#confirmSubmit');
    await until(() => operationRequests(h, 'revoke').length === 1, 'explicit revoke request');
    await page.waitForFunction(() => !document.querySelector('#confirmDialog').open);
    assert.equal((await receipt(page, h, p)).status, 'revoked');
    assert.equal(await h.reviewed(), true, 'revocation cannot release a later true review hold');
    assert.equal(await h.grantCount(), 1, 'history is preserved');
  });

  await journey('ordinary unreviewed and settings-blocked previews never offer installation', async page => {
    await open(page, h); let p = await preview(page, h, { review: false });
    assert.equal(p.installable, false); assert.ok(p.blockers.includes('outreach_review_required'));
    assert.equal(await enabled(page, '#installButton'), false);
    process.env.ANCHOR_EMAIL_SCOPED_RESUME_ID = 'synthetic-not-activated';
    p = await preview(page, h); assert.equal(p.installable, false);
    assert.ok(p.blockers.includes('preparation_settings_not_activated'));
    assert.equal(await enabled(page, '#installButton'), false);
    assert.equal(await h.reviewed(), null); assert.equal(mutationRequests(h).length, 0);
  });

  await journey('unknown prospects and existing true holds cannot yield an eligible batch or reuse earlier preview', async page => {
    await open(page, h); await preview(page, h);
    await field(page, '[name=prospectId]', 'synthetic-nonexistent-prospect');
    let n = h.responses.length; await page.click('#previewButton');
    await until(() => h.responses.slice(n).some(r => r.path === apiBase + 'preview'), 'unknown prospect denial');
    await page.waitForFunction(() => document.querySelector('#approvalStatus').dataset.tone === 'error'
      && !document.querySelector('#previewButton').disabled);
    assert.equal(await enabled(page, '#installButton'), false);
    await h.pool.query('UPDATE prospects SET ao_outreach_review_required=true WHERE id::text=$1', [h.f.manifest.admissions[0].prospectId]);
    await field(page, '[name=prospectId]', h.f.manifest.admissions[0].prospectId);
    n = h.responses.length; await page.click('#previewButton');
    await until(() => h.responses.slice(n).some(r => r.path === apiBase + 'preview'), 'existing true review hold denial');
    await page.waitForFunction(() => document.querySelector('#approvalStatus').dataset.tone === 'error'
      && !document.querySelector('#previewButton').disabled);
    assert.equal(await enabled(page, '#installButton'), false);
    assert.equal(h.responses.slice(n).find(r => r.path === apiBase + 'preview').body.installable === true, false);
    assert.equal(await h.reviewed(), true); assert.equal(mutationRequests(h).length, 0); assert.equal(await h.grantCount(), 0);
  });

  await journey('edited inputs invalidate every pin and confirmation before any install', async page => {
    await open(page, h); await preview(page, h); await confirmReady(page);
    await page.click('#confirmCancel');
    await field(page, '#approvalReference', 'synthetic-changed-reference');
    assert.equal(await enabled(page, '#installButton'), false);
    assert.equal(await page.$eval('#grantConfirm', el => el.checked), false);
    assert.equal(await page.$eval('#reviewConfirm', el => el.checked), false);
    assert.equal(mutationRequests(h).length, 0); assert.equal(await h.reviewed(), null);
  });

  await journey('real database pin drift rejects install and requires readback rather than retry', async page => {
    await open(page, h); const p = await preview(page, h); await confirmReady(page);
    await h.pool.query("UPDATE acquisition_outbound_programs SET last_error='synthetic-pin-drift' WHERE id=$1", [h.f.program.id]);
    await page.click('#confirmSubmit');
    await until(() => h.responses.some(r => r.path.endsWith('/install')), 'rejected stale install');
    await page.waitForFunction(() => !document.querySelector('#confirmDialog').open);
    assert.equal(await h.grantCount(), 0); assert.equal(await h.reviewed(), null);
    assert.equal(await enabled(page, '#installButton'), false);
    assert.equal((await receipt(page, h, p)).status, 'not_installed');
    assert.equal(await enabled(page, '#previewButton'), false, 'absent readback cannot unlock uncertain mutation');
  });

  for (const kind of ['admin-role-revoked', 'session-expired']) await journey(`${kind} after preview is rejected by current persisted authorization`, async page => {
    await open(page, h); await preview(page, h); await confirmReady(page);
    if (kind === 'admin-role-revoked') await h.pool.query("UPDATE users SET role='manager' WHERE id='3'");
    else await h.pool.query('UPDATE session SET expire=NOW()-interval \'1 minute\' WHERE sid<>$1', [h.f.sessionId]);
    await page.click('#confirmSubmit');
    await page.waitForFunction(() => /rejected|changed|verified/i.test(document.querySelector('#approvalStatus').textContent)
      && document.querySelector('#approvalStatus').dataset.tone === 'error');
    assert.equal(await h.grantCount(), 0); assert.equal(await h.reviewed(), null);
    assert.equal(await enabled(page, '#installButton'), false);
    const response = await page.reload(); assert.equal(response.status(), 403 === response.status() ? 403 : 401);
    assert.equal(await page.$('#intentForm'), null);
  });

  await journey('UTC labeled fields do not shift the approved window in a distant tablet timezone', async page => {
    await page.emulateTimezone('Pacific/Honolulu'); await open(page, h);
    const p = await preview(page, h), expected = h.intent();
    assert.equal(p.proposal.validFrom, expected.validFrom.replace(/\.\d{3}Z$/, '.000Z'));
    assert.equal(p.proposal.expiresAt, expected.expiresAt.replace(/\.\d{3}Z$/, '.000Z'));
    assert.match(await page.$eval('#intentForm', el => el.textContent), /UTC/);
  });

  await journey('hostile approval references are literal text, not executable preview or receipt markup', async page => {
    await open(page, h); const hostile = '<img src=x onerror="window.syntheticXss=true">';
    const p = await preview(page, h, { overrides: { approvalReference: hostile } });
    assert.match(await page.$eval('#previewResult', el => el.textContent), /<img src=x/);
    assert.equal(await page.evaluate(() => window.syntheticXss), undefined);
    assert.equal(await page.$('#previewResult img'), null);
    assert.equal(p.proposal.approvalReference, hostile);
  });

  await journey('Back, navigation and reload clear an unsubmitted preview without approving anything', async page => {
    await open(page, h); await preview(page, h); await confirmReady(page); await page.click('#confirmCancel');
    await page.goto(h.origin + '/fixture-away'); await page.goBack();
    await page.waitForSelector('#intentForm');
    assert.equal(await enabled(page, '#installButton'), false);
    assert.equal(await page.$eval('#grantConfirm', el => el.checked), false);
    await page.reload(); await page.waitForSelector('#intentForm');
    assert.equal(await enabled(page, '#installButton'), false);
    assert.equal(await h.grantCount(), 0); assert.equal(mutationRequests(h).length, 0);
  });

  await journey('late obsolete preview cannot restore pins after a newer edit and response', async page => {
    await open(page, h); await fillIntent(page, h); const pending = barrier(); let held = false;
    h.faults.response = (req, res, body, send) => {
      if (req.path.endsWith('/preview') && req.body?.intent && !held) { held = true; pending.promise.then(send); return res; }
      return send();
    };
    try {
      await page.click('#previewButton'); await until(() => held, 'first actual preview held');
      await field(page, '#approvalReference', 'synthetic-newer-approved-reference');
      pending.release();
      await page.waitForFunction(() => !document.querySelector('#previewButton').disabled);
      assert.equal(await enabled(page, '#installButton'), false);
      const p = await preview(page, h, { overrides: { approvalReference: 'synthetic-newer-approved-reference' } });
      assert.equal(p.proposal.approvalReference, 'synthetic-newer-approved-reference');
      assert.equal(await page.$eval('#grantConfirm', el => el.checked), false);
    } finally { pending.release(); }
  });

  await journey('rapid repeated confirmation dispatches once and stores recovery identity before dispatch', async page => {
    await open(page, h); const p = await preview(page, h); await confirmReady(page);
    let atDispatch;
    h.faults.before = (req, res, next) => {
      if (req.path.endsWith('/install')) {
        page.evaluate(() => Object.fromEntries(Object.entries(sessionStorage))).then(value => { atDispatch = value; next(); });
      } else next();
    };
    await page.$eval('#confirmSubmit', button => { button.click(); button.click(); button.click(); });
    await until(async () => await h.grantCount() === 1, 'one committed repeated-click grant');
    await page.waitForFunction(() => !document.querySelector('#confirmDialog').open);
    assert.equal(operationRequests(h, 'install').length, 1);
    const marker = JSON.stringify(atDispatch);
    assert.ok(marker.includes(p.manifestHash)); assert.ok(marker.includes(p.proposal.id));
    assert.ok(!marker.includes('approverSessionHash')); assert.ok(!marker.includes('preparationAuthority'));
    assert.ok(marker.length < 2000, 'marker must remain a minimal recovery reference, not a saved proposal');
  });

  await journey('lost committed install response survives reload and exact receipt never enables reinstall', async page => {
    await open(page, h); const p = await preview(page, h); await confirmReady(page);
    h.faults.response = (req, res, body, send) => req.path.endsWith('/install') ? loseCommittedResponse(res) : send();
    await page.click('#confirmSubmit'); await until(async () => await h.grantCount() === 1, 'commit before response loss');
    await page.waitForFunction(() => !document.querySelector('#confirmDialog').open);
    assert.equal(await enabled(page, '#installButton'), false);
    const before = operationRequests(h, 'preview').length;
    await page.reload(); await page.waitForFunction(() => !document.querySelector('#receiptButton').disabled);
    assert.equal(operationRequests(h, 'preview').length, before, 'reload must not automatically read or repeat a transaction');
    await check(page, '#newApprovalAcknowledgment');
    assert.equal(await enabled(page, '#previewButton'), false, 'reload cannot bypass unresolved mutation');
    assert.ok((await page.evaluate(() => JSON.stringify(Object.fromEntries(Object.entries(sessionStorage))))).includes(p.manifestHash));
    assert.equal((await receipt(page, h, p)).status, 'installed');
    assert.equal(await h.reviewed(), false); assert.equal(operationRequests(h, 'install').length, 1);
    assert.equal(await enabled(page, '#previewButton'), false, 'historical installed receipt is not a fresh approval');
    await field(page, '#revokeReference', 'synthetic-close-after-lost-response');
    assert.equal(await enabled(page, '#revokeButton'), true, 'matching receipt permits only explicit safe closing');
  });

  await journey('absent receipt while an interrupted install is still running cannot authorize a second mutation', async page => {
    await open(page, h); const p = await preview(page, h); await confirmReady(page);
    const gate = barrier(); let started = false;
    h.faults.before = (req, res, next) => {
      if (req.path.endsWith('/install') && !started) { started = true; gate.promise.then(next); return; }
      next();
    };
    try {
      await page.click('#confirmSubmit'); await until(() => started, 'original install reached server');
      await page.reload(); await page.waitForFunction(() => !document.querySelector('#receiptButton').disabled);
      assert.equal((await receipt(page, h, p)).status, 'not_installed');
      await check(page, '#newApprovalAcknowledgment');
      assert.equal(await enabled(page, '#previewButton'), false);
      assert.equal(await enabled(page, '#installButton'), false);
      assert.ok((await page.evaluate(() => JSON.stringify(Object.fromEntries(Object.entries(sessionStorage))))).includes(p.manifestHash));
      gate.release(); await until(async () => await h.grantCount() === 1, 'original in-flight install later commits');
      assert.equal((await receipt(page, h, p)).status, 'installed');
      assert.equal(operationRequests(h, 'install').length, 1);
    } finally { gate.release(); }
  });

  await journey('storage-write failure prevents approval dispatch rather than losing durable uncertainty state', async page => {
    await open(page, h); await preview(page, h); await confirmReady(page);
    await page.evaluate(() => { Storage.prototype.setItem = () => { throw new DOMException('Synthetic storage denied', 'SecurityError'); }; });
    await page.click('#confirmSubmit');
    await page.waitForFunction(() => /storage|locator/i.test(document.querySelector('#approvalStatus').textContent)
      && document.querySelector('#approvalStatus').dataset.tone === 'error');
    assert.equal(mutationRequests(h).length, 0); assert.equal(await h.grantCount(), 0); assert.equal(await h.reviewed(), null);
    assert.equal(await enabled(page, '#installButton'), false);
  });

  await journey('login interruption after uncertain commit never replaces the recovery reference or auto-replays approval', async page => {
    await open(page, h); const p = await preview(page, h); await confirmReady(page);
    h.faults.response = (req, res, body, send) => req.path.endsWith('/install') ? loseCommittedResponse(res) : send();
    await page.click('#confirmSubmit'); await until(async () => await h.grantCount() === 1, 'first commit');
    await page.waitForFunction(() => !document.querySelector('#confirmDialog').open);
    await h.pool.query('DELETE FROM session WHERE sid<>$1', [h.f.sessionId]);
    const denied = await page.reload(); assert.equal(denied.status(), 401); assert.equal(await page.$('#intentForm'), null);
    await browserLogin(page, h); await open(page, h);
    assert.equal((await receipt(page, h, p)).status, 'installed');
    assert.equal(operationRequests(h, 'install').length, 1, 'fresh login reads history but cannot replay old installation');
  });

  await journey('final authority check freezes native Cancel/Escape and rechecks consent before dispatch', async page => {
    await open(page, h); await preview(page, h); await confirmReady(page);
    const gate = barrier(); let waiting = false;
    h.faults.response = (req, res, body, send) => {
      if (req.path.endsWith('/context') && !waiting) { waiting = true; gate.promise.then(send); return res; }
      return send();
    };
    try {
      await page.click('#confirmSubmit'); await until(() => waiting, 'final current authority response pending');
      assert.equal(await enabled(page, '#confirmCancel'), false);
      assert.equal(await enabled(page, '#grantConfirm'), false);
      await page.keyboard.press('Escape');
      assert.equal(await page.$eval('#confirmDialog', el => el.open), true, 'explicit submission cannot be silently dismissed while checking');
      // Even a forced DOM change must be rechecked after the await boundary.
      await page.$eval('#grantConfirm', el => { el.checked = false; el.dispatchEvent(new Event('input', { bubbles: true })); });
      gate.release();
      await page.waitForFunction(() => /changed|acknowledg/i.test(document.querySelector('#approvalStatus').textContent)
        && document.querySelector('#approvalStatus').dataset.tone === 'error');
      assert.equal(mutationRequests(h).length, 0); assert.equal(await h.grantCount(), 0);
    } finally { gate.release(); }
  });

  await journey('closed confirmation cannot dispatch after a delayed current-authority response', async page => {
    await open(page, h); await preview(page, h); await confirmReady(page);
    const gate = barrier(); let waiting = false;
    h.faults.response = (req, res, body, send) => {
      if (req.path.endsWith('/context') && !waiting) { waiting = true; gate.promise.then(send); return res; }
      return send();
    };
    try {
      await page.click('#confirmSubmit'); await until(() => waiting, 'delayed final context');
      await page.$eval('#confirmDialog', el => el.close()); gate.release();
      await page.waitForFunction(() => /changed/i.test(document.querySelector('#approvalStatus').textContent)
        && document.querySelector('#approvalStatus').dataset.tone === 'error');
      assert.equal(mutationRequests(h).length, 0); assert.equal(await h.reviewed(), null);
    } finally { gate.release(); }
  });

  for (const [mode, raw] of [['malformed', '{broken'], ['json-null', 'null'], ['json-false', 'false'],
    ['json-zero', '0'], ['json-empty-string', '""'], ['read-denied', null]]) await journey(`${mode} recovery storage is fail-closed on reload`, async page => {
    await open(page, h);
    if (mode !== 'read-denied') await page.evaluate(value => sessionStorage.setItem('pulseforge.anchorApproval.pending.v1', value), raw);
    else await page.evaluateOnNewDocument(() => { Storage.prototype.getItem = () => { throw new DOMException('Synthetic read denied', 'SecurityError'); }; });
    await page.reload(); await page.waitForFunction(() => !document.querySelector('#receiptButton').disabled);
    if (mode !== 'read-denied') {
      assert.equal(await enabled(page, '#newApprovalAcknowledgment'), false);
      assert.equal(await enabled(page, '#previewButton'), false);
    }
    assert.equal(await enabled(page, '#installButton'), false);
    assert.match(await page.$eval('#approvalStatus', el => el.textContent), /storage|invalid|unavailable/i);
    if (mode === 'read-denied') {
      await preview(page, h); await check(page, '#grantConfirm'); await check(page, '#reviewConfirm'); await check(page, '#recoverySaved');
      assert.equal(await enabled(page, '#installButton'), false, 'read-only preview cannot turn denied recovery storage into mutation authority');
    }
    assert.equal(mutationRequests(h).length, 0);
  });

  await journey('mismatched review-audit receipt cannot resolve a committed lost response or enable revocation', async page => {
    await open(page, h); const p = await preview(page, h); await confirmReady(page);
    h.faults.response = (req, res, body, send) => {
      if (req.path.endsWith('/install')) return loseCommittedResponse(res);
      if (req.body?.receipt) { body.reviewInitializationHash = '0'.repeat(64); return send(); }
      return send();
    };
    await page.click('#confirmSubmit'); await until(async () => await h.grantCount() === 1, 'committed installation');
    await page.waitForFunction(() => !document.querySelector('#receiptButton').disabled);
    await receipt(page, h, p);
    await field(page, '#revokeReference', 'synthetic-close-needs-matching-audit');
    assert.equal(await enabled(page, '#revokeButton'), false);
    assert.equal(await enabled(page, '#previewButton'), false);
    assert.match(await page.$eval('#approvalStatus', el => el.textContent), /audit|match/i);
    assert.ok((await page.evaluate(() => sessionStorage.getItem('pulseforge.anchorApproval.pending.v1'))).includes(p.manifestHash));
    assert.equal(await h.reviewed(), false);
  });

  await journey('interrupted in-flight revocation remains recovery-only until an exact revoked receipt', async page => {
    await open(page, h); const p = await preview(page, h); await install(page, h); await receipt(page, h, p);
    await field(page, '#revokeReference', 'synthetic-pending-revocation');
    await page.click('#revokeButton'); await page.waitForSelector('#confirmDialog[open]');
    const gate = barrier(); let waiting = false;
    h.faults.before = (req, res, next) => {
      if (req.path.endsWith('/revoke') && !waiting) { waiting = true; gate.promise.then(next); return; }
      next();
    };
    try {
      await page.click('#confirmSubmit'); await until(() => waiting, 'revoke reached server');
      await page.reload(); await page.waitForFunction(() => !document.querySelector('#receiptButton').disabled);
      assert.equal((await receipt(page, h, p)).status, 'installed');
      await field(page, '#revokeReference', 'must-not-submit-second-revocation');
      assert.equal(await enabled(page, '#revokeButton'), false);
      assert.equal(await enabled(page, '#previewButton'), false);
      const marker = JSON.parse(await page.evaluate(() => sessionStorage.getItem('pulseforge.anchorApproval.pending.v1')));
      assert.equal(marker.operation, 'revoke');
      gate.release();
      await until(async () => (await h.pool.query('SELECT revoked_at FROM anchor_scoped_resume_grants')).rows[0]?.revoked_at != null, 'actual revoke commit');
      assert.equal((await receipt(page, h, p)).status, 'revoked');
      assert.equal(await page.evaluate(() => sessionStorage.getItem('pulseforge.anchorApproval.pending.v1')), null);
      assert.equal(await page.$eval('#newApprovalAcknowledgment', el => el.checked), false);
      assert.equal(operationRequests(h, 'revoke').length, 1); assert.equal(await h.reviewed(), false);
    } finally { gate.release(); }
  });

  await journey('noncurrent historical grant is held for operator review under the existing revocation guard', async page => {
    await open(page, h); const p = await preview(page, h); await install(page, h);
    await h.pool.query('UPDATE anchor_email_recovery_control SET manifest=NULL');
    const r = await receipt(page, h, p); assert.equal(r.currentControlMatches, false); assert.equal(r.status, 'installed');
    await field(page, '#revokeReference', 'synthetic-close-noncurrent-grant');
    assert.equal(await enabled(page, '#revokeButton'), false, 'the existing guarded revocation transaction requires matching current control');
    assert.equal(operationRequests(h, 'revoke').length, 0);
    assert.equal((await h.pool.query('SELECT revoked_at FROM anchor_scoped_resume_grants')).rows[0].revoked_at, null);
    assert.equal((await h.pool.query('SELECT manifest FROM anchor_email_recovery_control')).rows[0].manifest, null);
  });

  await journey('separate exact preparation confirmation creates real READY drafts without send authority or refunds', async page => {
    const p = await preparationSetup(page, h), before = await h.protectedState();
    await confirmPreparation(page);
    assert.equal(preparationRequests(h).length, 0, 'opening the separate confirmation is not preparation');
    await page.click('#confirmCancel'); assert.deepEqual(await h.effects(), noEffects);
    const result = await prepare(page, h);
    assert.equal(result.status, 200); assert.equal(result.body.status, 'ready_for_review');
    assert.equal(result.body.retryAllowed, false); assert.equal(result.body.providerCalls, 0); assert.equal(result.body.sent, 0);
    assert.equal(result.body.executionApproved, false); assert.equal(result.body.envelopeAuthorized, false);
    assert.deepEqual(preparationRequests(h)[0].body, { revision: p.revision, manifestHash: p.manifestHash, sourceMissionHash: p.sourceMissionHash });
    const saved = await h.savedMission(result.body.missionId);
    assert.equal(saved.mission.stage, 'ready'); assert.equal(saved.mission.pendingOperatorDecision.kind, 'execution_approval');
    assert.equal(saved.mission.orchestrationMissionId, h.f.source.id);
    assert.equal(saved.mission.crmPreparation.providerReadiness, 'not_checked');
    assert.equal(saved.contributions.some(c => c.specialist === 'paige' && c.kind === 'variants'), true);
    const r = await receipt(page, h, p), outcome = r.preparationOutcome;
    assert.equal(outcome.status, 'ready_for_review'); assert.equal(outcome.evidenceScope, 'current_persisted_snapshot');
    assert.equal(outcome.missionId, result.body.missionId); assert.equal(outcome.operationId, result.body.operationId);
    assert.equal(outcome.preparedArtifactRevision, result.body.preparedArtifactRevision);
    assert.match(outcome.draftContentHash, /^[a-f0-9]{64}$/); assert.equal(outcome.drafts.length, 1);
    const draft = outcome.drafts[0]; assert.equal(draft.prospectId, h.f.manifest.admissions[0].prospectId);
    assert.equal(draft.recipientEmail, h.f.manifest.admissions[0].recipientEmail);
    assert.ok(draft.subject.length && draft.body.length);
    const displayed = await page.$eval('#preparationResult', el => el.textContent);
    assert.ok(displayed.includes(draft.subject)); assert.ok(displayed.includes(draft.body));
    assert.equal(await enabled(page, '#prepareButton'), false);
    assert.deepEqual(await h.protectedState(), before, 'expired outbound authority, settings, quarantine and source stay unchanged');
    await field(page, '#revokeReference', 'synthetic-close-after-preparation');
    await page.click('#revokeButton'); await page.waitForSelector('#confirmDialog[open]'); await page.click('#confirmSubmit');
    await page.waitForFunction(() => !document.querySelector('#receiptButton').disabled);
    assert.equal((await receipt(page, h, p)).status, 'revoked');
    assert.ok(await page.evaluate(key => sessionStorage.getItem(key), preparationStorageKey), 'revocation must not clear the no-retry preparation marker');
    assert.equal(await enabled(page, '#previewButton'), false); assert.equal(await enabled(page, '#prepareButton'), false);
    assert.equal(await h.grantCount(), 1); assert.equal(preparationRequests(h).length, 1);
  }, { preparation: true, effects: readyEffects });

  await journey('an installed grant reloaded before any preparation never runs automatically', async page => {
    const p = await preparationSetup(page, h); await check(page, '#prepareConfirm');
    await page.goto(h.origin + '/fixture-away'); await page.goBack();
    await page.waitForFunction(() => !document.querySelector('#receiptButton').disabled);
    assert.equal(await page.$eval('#prepareConfirm', el => el.checked), false);
    assert.equal(preparationRequests(h).length, 0);
    await receipt(page, h, p); assert.equal(await enabled(page, '#prepareButton'), false);
    assert.equal(preparationRequests(h).length, 0);
  });

  await journey('fresh native login reads old preparation authority but cannot consume the original-session grant', async page => {
    const p = await preparationSetup(page, h);
    await h.pool.query('DELETE FROM session WHERE sid<>$1', [h.f.sessionId]);
    await browserLogin(page, h); await open(page, h);
    const r = await receipt(page, h, p); assert.equal(r.preparationIntent.originalSessionMatches, false);
    assert.equal(await enabled(page, '#prepareButton'), false);
    assert.equal(preparationRequests(h).length, 0); assert.equal(await h.grantCount(), 1);
  });

  for (const mode of ['maintenance', 'source', 'program', 'true-review-hold']) await journey(`preparation rechecks ${mode} at the real guarded action without consuming`, async page => {
    const p = await preparationSetup(page, h); await confirmPreparation(page);
    if (mode === 'maintenance') process.env.ANCHOR_EMAIL_RECOVERY_MAINTENANCE = 'false';
    if (mode === 'source') await h.pool.query("UPDATE acquisition_missions SET payload=jsonb_set(payload,'{status}','\"synthetic-stale-source\"') WHERE id=$1", [h.f.source.id]);
    if (mode === 'program') await h.pool.query("UPDATE acquisition_outbound_programs SET last_error='synthetic-stale-program'");
    if (mode === 'true-review-hold') await h.pool.query('UPDATE prospects SET ao_outreach_review_required=true');
    const before = await h.protectedState(), n = h.responses.length;
    await page.click('#confirmSubmit');
    await until(() => h.responses.slice(n).some(r => r.path === preparePath), 'rejected real preparation action');
    const result = h.responses.slice(n).find(r => r.path === preparePath);
    assert.equal(result.status, 409); assert.equal(result.body.retryAllowed, false);
    await page.waitForFunction(() => !document.querySelector('#receiptButton').disabled);
    assert.equal(await enabled(page, '#prepareButton'), false);
    assert.deepEqual(await h.protectedState(), before);
    assert.ok(await page.evaluate(key => sessionStorage.getItem(key), preparationStorageKey));
    await receipt(page, h, p); assert.equal(await enabled(page, '#prepareButton'), false, 'a rejected attempt does not authorize retry');
  }, { preparation: true });

  for (const pin of ['manifestHash', 'sourceMissionHash']) await journey(`stale ${pin} cannot cross the native preparation transaction`, async page => {
    await preparationSetup(page, h);
    h.faults.before = (req, res, next) => { if (req.path === preparePath) req.body[pin] = '0'.repeat(64); next(); };
    const result = await prepare(page, h);
    assert.equal(result.status, 409); assert.equal(result.body.status, 'rejected');
    assert.equal(result.body.writesMayHaveOccurred, false); assert.equal(result.body.retryAllowed, false);
    assert.equal(await enabled(page, '#prepareButton'), false);
  }, { preparation: true });

  for (const mode of ['revision', 'admin-revoked', 'session-expired', 'wrong-tenant']) await journey(`preparation final identity check rejects ${mode} before dispatch`, async page => {
    await preparationSetup(page, h); await confirmPreparation(page);
    if (mode === 'revision') process.env.RAILWAY_GIT_COMMIT_SHA = 'b'.repeat(40);
    if (mode === 'admin-revoked') await h.pool.query("UPDATE users SET role='ao' WHERE id='3'");
    if (mode === 'session-expired') await h.pool.query("UPDATE session SET expire='2001-01-01' WHERE sid<>$1", [h.f.sessionId]);
    if (mode === 'wrong-tenant') await h.pool.query("UPDATE session SET sess=jsonb_set(sess::jsonb,'{active_client_id}','11')::json WHERE sid<>$1", [h.f.sessionId]);
    await page.click('#confirmSubmit');
    await page.waitForFunction(() => !document.querySelector('#reloadContextButton').disabled
      && document.querySelector('#approvalStatus').dataset.tone === 'error');
    assert.equal(preparationRequests(h).length, 0);
    assert.equal(await enabled(page, '#prepareButton'), false);
    assert.doesNotMatch(await page.$eval('#approvalContext', el => el.textContent), /^Verified current admin/);
  });

  await journey('expired fixed UTC window cannot dispatch after delayed final preparation confirmation', async page => {
    const p = await preparationSetup(page, h, { overrides: { expiresAt: new Date(Date.now() + 6000).toISOString() } });
    await confirmPreparation(page); const gate = barrier(); let waiting = false;
    h.faults.response = (req, res, body, send) => { if (req.path.endsWith('/context') && !waiting) {
      waiting = true; gate.promise.then(send); return res; } return send(); };
    try {
      await page.click('#confirmSubmit'); await until(() => waiting, 'delayed final preparation context');
      await sleep(Math.max(0, Date.parse(p.proposal.expiresAt) - Date.now()) + 100); gate.release();
      await page.waitForFunction(() => !document.querySelector('#reloadContextButton').disabled);
      assert.equal(preparationRequests(h).length, 0); assert.equal(await enabled(page, '#prepareButton'), false);
    } finally { gate.release(); }
  });

  await journey('rapid repeated preparation clicks persist the minimal exact locator before one real dispatch', async page => {
    const p = await preparationSetup(page, h), expected = h.responses.at(-1).body.preparationIntent; await confirmPreparation(page);
    const gate = barrier(); let waiting = false;
    h.faults.before = (req, res, next) => { if (req.path === preparePath && !waiting) { waiting = true; gate.promise.then(next); return; } next(); };
    try {
      await page.$eval('#confirmSubmit', button => { button.click(); button.click(); button.click(); });
      await until(() => waiting, 'preparation request gated before action');
      const marker = JSON.parse(await page.evaluate(key => sessionStorage.getItem(key), preparationStorageKey));
      assert.deepEqual(Object.keys(marker).sort(), ['expectedReviewInitializationHash','manifestHash','manifestId','missionId','operationId','revision','sourceMissionHash','version']);
      assert.equal(marker.manifestId, p.proposal.id); assert.equal(marker.manifestHash, p.manifestHash);
      assert.equal(marker.sourceMissionHash, p.sourceMissionHash); assert.equal(marker.revision, p.revision);
      assert.equal(marker.expectedReviewInitializationHash, p.reviewInitializationHash);
      assert.equal(marker.version, 1); assert.equal(marker.operationId, expected.expectedOperationId); assert.equal(marker.missionId, expected.expectedMissionId);
      assert.match(marker.operationId, /^[a-f0-9]{64}$/); assert.match(marker.missionId, /^mission_crm_[a-f0-9]{32}$/);
      assert.deepEqual(await h.effects(), noEffects); assert.equal(preparationRequests(h).length, 1);
      gate.release(); await until(async () => (await h.effects()).children === 1, 'single real READY child');
      await page.waitForFunction(() => !document.querySelector('#receiptButton').disabled);
      assert.equal(preparationRequests(h).length, 1);
    } finally { gate.release(); }
  }, { preparation: true, effects: readyEffects });

  await journey('lost READY response reloads recovery-only and reads exact durable draft text without replay', async page => {
    const p = await preparationSetup(page, h);
    h.faults.response = (req, res, body, send) => req.path === preparePath ? loseCommittedResponse(res) : send();
    const result = await prepare(page, h); assert.equal(result.body.status, 'ready_for_review');
    await page.reload(); await page.waitForFunction(() => !document.querySelector('#receiptButton').disabled);
    assert.equal(preparationRequests(h).length, 1); assert.equal(await enabled(page, '#prepareButton'), false);
    assert.equal(await enabled(page, '#previewButton'), false);
    const before = await h.protectedState(), r = await receipt(page, h, p);
    assert.equal(r.preparationOutcome.status, 'ready_for_review');
    assert.ok((await page.$eval('#preparationResult', el => el.textContent)).includes(r.preparationOutcome.drafts[0].body));
    assert.deepEqual(await h.protectedState(), before); assert.equal(preparationRequests(h).length, 1);
    assert.ok(await page.evaluate(key => sessionStorage.getItem(key), preparationStorageKey));
    await page.goto(h.origin + '/fixture-away'); await page.goBack();
    await page.waitForFunction(() => !document.querySelector('#receiptButton').disabled);
    assert.equal(preparationRequests(h).length, 1); assert.equal(await enabled(page, '#prepareButton'), false);
  }, { preparation: true, effects: readyEffects });

  await journey('in-flight preparation with unconsumed receipt cannot authorize retry or replacement after reload', async page => {
    const p = await preparationSetup(page, h); await confirmPreparation(page);
    const gate = barrier(); let waiting = false;
    h.faults.before = (req, res, next) => { if (req.path === preparePath && !waiting) { waiting = true; gate.promise.then(next); return; } next(); };
    try {
      await page.click('#confirmSubmit'); await until(() => waiting, 'original preparation reached server');
      await page.reload(); await page.waitForFunction(() => !document.querySelector('#receiptButton').disabled);
      const pending = await receipt(page, h, p); assert.equal(pending.status, 'installed');
      assert.equal(pending.preparationOutcome.status, 'not_recorded');
      assert.equal(await enabled(page, '#prepareButton'), false); assert.equal(await enabled(page, '#previewButton'), false);
      assert.deepEqual(await h.effects(), noEffects);
      gate.release(); await until(() => h.responses.some(r => r.path === preparePath), 'original preparation terminal response after snapshot');
      assert.equal(h.responses.find(r => r.path === preparePath).body.status, 'ready_for_review');
      assert.equal((await receipt(page, h, p)).preparationOutcome.status, 'ready_for_review');
      assert.equal(preparationRequests(h).length, 1); assert.equal(await h.grantCount(), 1);
    } finally { gate.release(); }
  }, { preparation: true, effects: readyEffects });

  await journey('failure after durable consumption retains UNKNOWN outcome and never refunds or permits replacement', async page => {
    const p = await preparationSetup(page, h); let injected = false;
    h.faults.afterConsumption = async () => { injected = true; throw Error('synthetic interruption after durable preparation claim'); };
    const result = await prepare(page, h); assert.equal(injected, true);
    assert.equal(result.status, 409); assert.equal(result.body.status, 'blocked_review_required');
    assert.equal(result.body.writesMayHaveOccurred, true); assert.equal(result.body.retryAllowed, false);
    await page.reload(); await page.waitForFunction(() => !document.querySelector('#receiptButton').disabled);
    const r = await receipt(page, h, p); assert.equal(r.status, 'consumed');
    assert.equal(r.preparationOutcome.status, 'consumed_outcome_unknown'); assert.deepEqual(r.preparationOutcome.drafts, []);
    assert.equal(await enabled(page, '#prepareButton'), false); assert.equal(await enabled(page, '#previewButton'), false);
    assert.match(await page.$eval('#preparationResult', el => el.textContent), /unknown|not.*ready|not.*proof/i);
    await field(page, '#revokeReference', 'synthetic-close-incomplete-consumed');
    await page.click('#revokeButton'); await page.waitForSelector('#confirmDialog[open]'); await page.click('#confirmSubmit');
    await page.waitForFunction(() => !document.querySelector('#receiptButton').disabled);
    const closed = await receipt(page, h, p); assert.equal(closed.status, 'revoked');
    assert.equal(closed.preparationOutcome.status, 'consumed_outcome_unknown');
    assert.ok(await page.evaluate(key => sessionStorage.getItem(key), preparationStorageKey));
    assert.equal(await enabled(page, '#previewButton'), false); assert.equal(await h.grantCount(), 1);
    assert.equal(preparationRequests(h).length, 1);
  }, { preparation: true, effects: claimedEffects });

  await journey('preparation recovery storage write failure prevents dispatch without losing existing grant history', async page => {
    await preparationSetup(page, h); await confirmPreparation(page);
    await page.evaluate(key => { const set = Storage.prototype.setItem; Storage.prototype.setItem = function(k, v) {
      if (k === key) throw new DOMException('Synthetic preparation write failure', 'QuotaExceededError'); return set.call(this, k, v); }; }, preparationStorageKey);
    await page.click('#confirmSubmit'); await page.waitForFunction(() => !document.querySelector('#reloadContextButton').disabled);
    assert.equal(preparationRequests(h).length, 0); assert.equal(await enabled(page, '#prepareButton'), false);
    assert.equal(await h.grantCount(), 1);
  });

  for (const raw of ['{broken', 'null', 'false', '0', '""']) await journey(`invalid preparation locator ${raw} cannot unlock new work on reload`, async page => {
    const p = await preparationSetup(page, h);
    await page.evaluate(({key, raw}) => sessionStorage.setItem(key, raw), { key: preparationStorageKey, raw });
    await page.reload(); await page.waitForFunction(() => !document.querySelector('#receiptButton').disabled);
    await receipt(page, h, p);
    assert.equal(await enabled(page, '#prepareButton'), false); assert.equal(await enabled(page, '#previewButton'), false);
    assert.equal(preparationRequests(h).length, 0);
  });

  await journey('cancelled final preparation confirmation and changed consent cannot dispatch after delayed context', async page => {
    await preparationSetup(page, h); await confirmPreparation(page);
    const gate = barrier(); let waiting = false;
    h.faults.response = (req, res, body, send) => { if (req.path.endsWith('/context') && !waiting) {
      waiting = true; gate.promise.then(send); return res; } return send(); };
    try {
      await page.click('#confirmSubmit'); await until(() => waiting, 'delayed preparation final authority');
      assert.equal(await enabled(page, '#confirmCancel'), false);
      await page.keyboard.press('Escape'); assert.equal(await page.$eval('#confirmDialog', el => el.open), true);
      await page.$eval('#prepareConfirm', el => { el.checked = false; el.dispatchEvent(new Event('input', { bubbles: true })); });
      await page.$eval('#confirmDialog', el => el.close()); gate.release();
      await page.waitForFunction(() => !document.querySelector('#reloadContextButton').disabled);
      assert.equal(preparationRequests(h).length, 0);
    } finally { gate.release(); }
  });

  await journey('preparation locator read denial permits no new mutation even with an otherwise valid installed receipt', async page => {
    const p = await preparationSetup(page, h);
    await page.evaluateOnNewDocument(key => { const get = Storage.prototype.getItem; Storage.prototype.getItem = function(k) {
      if (k === key) throw new DOMException('Synthetic preparation read denied', 'SecurityError'); return get.call(this, k); }; }, preparationStorageKey);
    await page.reload(); await page.waitForFunction(() => !document.querySelector('#receiptButton').disabled);
    await receipt(page, h, p); assert.equal(await enabled(page, '#prepareButton'), false);
    assert.equal(await enabled(page, '#installButton'), false); assert.equal(preparationRequests(h).length, 0);
  });

  await journey('a mismatched READY receipt operation cannot prove an outstanding preparation outcome', async page => {
    const p = await preparationSetup(page, h); await prepare(page, h);
    h.faults.response = (req, res, body, send) => { if (req.body?.receipt) body.preparationOutcome.operationId = '0'.repeat(64); return send(); };
    await receipt(page, h, p);
    assert.equal(await enabled(page, '#prepareButton'), false); assert.equal(await enabled(page, '#previewButton'), false);
    assert.equal(await page.$eval('#approvalStatus', el => el.dataset.tone), 'error');
    assert.ok(await page.evaluate(key => sessionStorage.getItem(key), preparationStorageKey));
    assert.equal(preparationRequests(h).length, 1);
  }, { preparation: true, effects: readyEffects });

  await journey('durable claim still in flight is UNKNOWN until the exact READY transaction commits', async page => {
    const p = await preparationSetup(page, h); await confirmPreparation(page);
    const gate = barrier(); let waiting = false;
    h.faults.afterConsumption = async () => { waiting = true; await gate.promise; };
    try {
      await page.click('#confirmSubmit'); await until(() => waiting, 'durable claim before canonical preparation');
      assert.deepEqual(await h.effects(), claimedEffects);
      await page.reload(); await page.waitForFunction(() => !document.querySelector('#receiptButton').disabled);
      const pending = await receipt(page, h, p); assert.equal(pending.status, 'consumed');
      assert.equal(pending.preparationOutcome.status, 'consumed_outcome_unknown'); assert.deepEqual(pending.preparationOutcome.drafts, []);
      assert.equal(await enabled(page, '#prepareButton'), false); assert.equal(await enabled(page, '#previewButton'), false);
      gate.release(); await until(() => h.responses.some(r => r.path === preparePath), 'real preparation terminal response after unknown snapshot');
      assert.equal(h.responses.find(r => r.path === preparePath).body.status, 'ready_for_review');
      assert.equal((await receipt(page, h, p)).preparationOutcome.status, 'ready_for_review');
      assert.equal(preparationRequests(h).length, 1);
    } finally { gate.release(); }
  }, { preparation: true, effects: readyEffects });

  await journey('draft subject and body from a synthetic hostile receipt render only as literal text', async page => {
    const p = await preparationSetup(page, h); await prepare(page, h);
    const hostile = '<img src=x onerror="window.syntheticDraftInjection=1"><script>window.syntheticDraftInjection=1</script>';
    h.faults.response = (req, res, body, send) => { if (req.body?.receipt) {
      body.preparationOutcome.drafts[0].subject = hostile;
      body.preparationOutcome.drafts[0].body = hostile + '\nSynthetic literal draft text';
    } return send(); };
    await receipt(page, h, p);
    const displayed = await page.$eval('#preparationResult', el => el.textContent);
    assert.ok(displayed.includes(hostile)); assert.ok(displayed.includes('Synthetic literal draft text'));
    assert.equal(await page.$('#preparationResult img'), null); assert.equal(await page.$('#preparationResult script'), null);
    assert.equal(await page.evaluate(() => window.syntheticDraftInjection), undefined);
    assert.equal(preparationRequests(h).length, 1);
  }, { preparation: true, effects: readyEffects });
});
