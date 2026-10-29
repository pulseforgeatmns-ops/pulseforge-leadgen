'use strict';
// Chrome (headless Chromium) on Android-tablet and phone viewports against the
// synthetic Tenant 10 fixture: the Scout research approval page end to end, and
// Remember me across a real browser restart. Set SCREENSHOT_DIR to keep PNGs.
const { startApprovalHarness, ADMIN, AO } = require('../helpers/scoutResearchApprovalHarness');
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const puppeteer = require('puppeteer');

const PAGE = '/admin/scout-research-approval';
const DAY = 86400000;
const TABLET = { width: 800, height: 1280, deviceScaleFactor: 1, isMobile: true, hasTouch: true };
const PHONE = { width: 390, height: 844, deviceScaleFactor: 1, isMobile: true, hasTouch: true };
const ANDROID_CHROME = 'Mozilla/5.0 (Linux; Android 14; SM-X710) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36';
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

const open = new Set();
async function launch(userDataDir) {
  const browser = await puppeteer.launch({ executablePath: process.env.CHROMIUM_PATH || '/opt/pw-browsers/chromium', headless: true, pipe: true,
    userDataDir, args: ['--no-sandbox', '--disable-dev-shm-usage', '--disable-background-networking',
      '--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE 127.0.0.1'] });
  open.add(browser); browser.once('disconnected', () => open.delete(browser));
  return browser;
}
async function shot(page, name) {
  if (!process.env.SCREENSHOT_DIR) return;
  fs.mkdirSync(process.env.SCREENSHOT_DIR, { recursive: true });
  await page.screenshot({ path: path.join(process.env.SCREENSHOT_DIR, `${name}.png`), fullPage: true });
}
async function newPage(browser, viewport = TABLET) {
  const page = await browser.newPage();
  await page.setUserAgent(ANDROID_CHROME);
  await page.setViewport(viewport);
  return page;
}
async function signIn(page, h, user, { remember }) {
  await page.goto(h.origin + '/login');
  await page.type('#login-email', user.email);
  await page.type('#login-password', h.fixturePassword);
  if (remember) await page.click('#login-remember');
  await Promise.all([page.waitForNavigation(), page.click('button[type=submit]')]);
}
const text = (page, selector) => page.$eval(selector, el => el.textContent);
const disabled = (page, selector) => page.$eval(selector, el => el.disabled);
async function noHorizontalScroll(page) {
  const { scroll, width } = await page.evaluate(() => ({ scroll: document.documentElement.scrollWidth, width: window.innerWidth }));
  assert.ok(scroll <= width, `page scrolls horizontally (${scroll} > ${width})`);
}
async function waitText(page, selector, pattern) {
  await page.waitForFunction((s, p) => new RegExp(p).test(document.querySelector(s)?.textContent || ''), { timeout: 10000 }, selector, pattern.source);
}
async function confirmWith(page, password) {
  await page.waitForSelector('#confirmDialog[open]');
  if (password !== null) await page.type('#confirmPassword', password);
  await page.click('#confirmSubmit');
}

test('Scout research approval page and Remember me in Chrome', { timeout: 600000 }, async t => {
  const h = await startApprovalHarness(); t.after(() => h.close());
  // A failed assertion must never leave a Chromium process behind.
  t.afterEach(async () => { await Promise.all([...open].map(browser => browser.close().catch(() => {}))); });
  const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'pf-remember-profile-'));
  t.after(() => fs.rmSync(profile, { recursive: true, force: true }));

  await t.test('AO is turned away from the admin page', async () => {
    const browser = await launch();
    const page = await newPage(browser);
    await signIn(page, h, AO, { remember: false });
    await page.goto(h.origin + PAGE);
    assert.equal(new URL(page.url()).pathname, '/ao');
  });

  await t.test('Remember me survives a browser restart; logout ends it', async () => {
    let browser = await launch(profile);
    let page = await newPage(browser);
    await page.goto(h.origin + '/login');
    assert.equal(await page.$eval('#login-email', el => el.autocomplete), 'username');
    assert.equal(await page.$eval('#login-password', el => el.autocomplete), 'current-password');
    assert.equal(await page.$eval('#login-remember', el => el.checked), false);
    await shot(page, '01-login-remember-me');
    await signIn(page, h, ADMIN, { remember: true });
    const cookie = (await page.cookies()).find(c => c.name === 'connect.sid');
    assert.equal(cookie.httpOnly, true); assert.equal(cookie.sameSite, 'Lax');
    assert.ok(Math.abs(cookie.expires * 1000 - (Date.now() + 30 * DAY)) < 60000, 'persistent 30-day cookie');
    assert.equal(await page.evaluate(() => document.cookie), '', 'not readable by page scripts');
    await browser.close();

    browser = await launch(profile);
    page = await newPage(browser);
    const response = await page.goto(h.origin + PAGE);
    assert.equal(response.status(), 200, 'still signed in after restart');
    await waitText(page, '#authority', /Ready to approve/);
    assert.match(await text(page, '#authority'), /remembered on this device/);
    await page.goto(h.origin + '/logout');
    await browser.close();

    browser = await launch(profile);
    page = await newPage(browser);
    await page.goto(h.origin + PAGE);
    assert.equal(new URL(page.url()).pathname, '/login', 'logout invalidated the persistent session');
    await signIn(page, h, ADMIN, { remember: false });
    const plain = (await page.cookies()).find(c => c.name === 'connect.sid');
    assert.ok(Math.abs(plain.expires * 1000 - (Date.now() + DAY)) < 60000, 'unremembered keeps the 24h policy');
    // Signing in again in the same browser issues a new session ID and ends the old one.
    await signIn(page, h, ADMIN, { remember: true });
    const rotated = (await page.cookies()).find(c => c.name === 'connect.sid');
    assert.notEqual(rotated.value, plain.value, 'session ID rotated on sign-in');
    assert.equal((await h.http('/api/me', { cookie: `connect.sid=${plain.value}` })).status, 401, 'previous ID no longer signed in');
    await browser.close();

    // The rotated remembered session survives a restart, and ends when the account is deactivated.
    browser = await launch(profile);
    page = await newPage(browser);
    assert.equal((await page.goto(h.origin + PAGE)).status(), 200, 'rotated remembered session survives restart');
    await h.pool.query('UPDATE users SET active=false WHERE id=$1', [ADMIN.id]);
    require('../../utils/sessionLifecycle').forgetUser(ADMIN.id);
    await page.goto(h.origin + PAGE);
    assert.equal(new URL(page.url()).pathname, '/login', 'deactivation signs the browser out');
    await h.pool.query('UPDATE users SET active=true WHERE id=$1', [ADMIN.id]);
    await browser.close();
  });

  await t.test('tablet: preview and approve mission, preview blocked and clean grants, install, revoke', async () => {
    const browser = await launch();
    const page = await newPage(browser, TABLET);
    await page.setCookie({ name: 'connect.sid', value: decodeURIComponent(h.adminCookie.split('=')[1]), url: h.origin, httpOnly: true });
    await page.goto(h.origin + PAGE);
    await waitText(page, '#authority', /Ready to approve/);
    await waitText(page, '#grantStatus', /No open grant/);
    await noHorizontalScroll(page);
    assert.equal(await disabled(page, '#previewMission'), true, 'needs a reference first');
    await shot(page, '02-tablet-initial');

    await page.type('#missionRef', 'synthetic-browser-mission-1');
    assert.equal(await disabled(page, '#previewMission'), false);
    await page.click('#previewMission');
    await waitText(page, '#missionResult', /Ready to approve/);
    const missionText = await text(page, '#missionResult');
    assert.match(missionText, /Managed facilities/); assert.match(missionText, /Restaurant front of house/);
    assert.match(missionText, /Cities \(8\)/); assert.match(missionText, /Mission hash[a-f0-9]{64}/);
    assert.equal(await disabled(page, '#approveMission'), true, 'approval needs explicit confirmation');
    await page.click('#missionConfirm');
    assert.equal(await disabled(page, '#approveMission'), false);
    await shot(page, '03-tablet-mission-preview');

    // Wrong password: a clear error, nothing recorded.
    await page.click('#approveMission');
    await confirmWith(page, 'wrong-password');
    await waitText(page, '#missionResult', /That password was not accepted/);
    assert.equal((await h.pool.query('SELECT count(*)::int n FROM scout_research_missions')).rows[0].n, 0);
    await shot(page, '04-tablet-wrong-password');

    await page.click('#approveMission');
    await confirmWith(page, h.fixturePassword);
    await waitText(page, '#status', /Research mission approved/);
    await waitText(page, '#missionResult', /Already approved/);
    assert.match(await page.$eval('#grantMissionId', el => el.value), /^mission_anchor_facility_research_[a-f0-9]{20}$/);

    // Blocked: the outbound restriction is not in place, so the grant cannot be approved.
    await page.type('#grantRef', 'synthetic-browser-grant-blocked');
    await page.type('#grantStatement', 'Synthetic browser approval for research-only Scout discovery.');
    process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED = 'true';
    await page.click('#previewGrant');
    await waitText(page, '#grantResult', /Blocked — approval is disabled/);
    process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED = 'false';
    assert.match(await text(page, '#grantResult'), /research_only_closed_environment_required/);
    await page.click('#grantConfirm');
    assert.equal(await disabled(page, '#installGrant'), true, 'blockers disable installation');
    await shot(page, '05-tablet-grant-blocked');

    await page.$eval('#grantRef', el => { el.value = ''; });
    await page.type('#grantRef', 'synthetic-browser-grant-1');
    await page.click('#previewGrant');
    await waitText(page, '#grantResult', /Ready to install/);
    const grantText = await text(page, '#grantResult');
    assert.match(grantText, /8 discovery cycles/); assert.match(grantText, /\$5\.00 per UTC day/);
    assert.match(grantText, /Revision pin/); assert.match(grantText, /b{40}/);
    assert.match(grantText, /Email recovery stays in maintenance/);
    assert.equal(await disabled(page, '#installGrant'), true);
    await page.click('#grantConfirm');
    assert.equal(await disabled(page, '#installGrant'), false);
    await shot(page, '06-tablet-grant-preview');

    // A dropped connection on install is reported as uncertain, not as success.
    await page.setRequestInterception(true);
    const abort = request => request.url().endsWith('/research-only-grant/install') ? request.abort() : request.continue();
    page.on('request', abort);
    await page.click('#installGrant');
    await confirmWith(page, h.fixturePassword);
    await waitText(page, '#grantResult', /may or may not have been saved/);
    assert.match(await text(page, '#grantResult'), /Outcome uncertain/);
    page.off('request', abort);
    await page.setRequestInterception(false);
    await shot(page, '07-tablet-install-uncertain');

    await page.click('#installGrant');
    await page.waitForSelector('#confirmDialog[open]');
    assert.match(await text(page, '#confirmFacts'), /\$5\.00 per UTC day/);
    await shot(page, '08-tablet-install-confirm');
    await page.type('#confirmPassword', h.fixturePassword);
    await page.click('#confirmSubmit');
    await waitText(page, '#status', /Grant installed/);
    await waitText(page, '#grantStatus', /Active/);
    const status = await text(page, '#grantStatus');
    assert.match(status, /Cycles used: 0 of 8/); assert.match(status, /\$0\.00 total/); assert.match(status, /\$5\.00 per UTC day/);
    await shot(page, '09-tablet-grant-active');

    assert.equal(await disabled(page, '#revokeButton'), true, 'needs a revocation reference');
    await page.type('#revokeReference', 'synthetic-browser-revoke-1');
    await page.click('#revokeButton');
    await page.waitForSelector('#confirmDialog[open]');
    assert.equal(await page.$eval('#confirmPasswordArea', el => el.hidden), true, 'revocation never asks for a password');
    await confirmWith(page, null);
    await waitText(page, '#grantStatus', /Revoked/);
    await shot(page, '10-tablet-grant-revoked');
    await noHorizontalScroll(page);
  });

  await t.test('phone: layout fits and an expired grant is explained', async () => {
    const grants = require('../../services/scoutResearchOnlyGrant');
    const actor = { id: ADMIN.id, role: 'admin', sessionId: 'synthetic-research-admin-session' };
    const researchMissionId = (await h.pool.query('SELECT id FROM scout_research_missions LIMIT 1')).rows[0].id;
    const expiresAt = new Date(Math.floor((Date.now() + 3000) / 1000) * 1000).toISOString();
    const preview = await grants.previewResearchOnlyGrant(h.pool, { approvalRef: 'synthetic-browser-short', expiresAt,
      statement: 'Synthetic short grant.', researchMissionId }, actor);
    await grants.installResearchOnlyGrant(h.pool, { manifest: preview.manifest, manifestHash: preview.manifestHash }, actor);
    await sleep(Math.max(0, Date.parse(expiresAt) - Date.now()) + 300);

    const browser = await launch();
    const page = await newPage(browser, PHONE);
    await page.setCookie({ name: 'connect.sid', value: decodeURIComponent(h.adminCookie.split('=')[1]), url: h.origin, httpOnly: true });
    await page.goto(h.origin + PAGE);
    await waitText(page, '#grantStatus', /Expired/);
    assert.match(await text(page, '#grantStatus'), /No further cycles can be claimed/);
    await noHorizontalScroll(page);
    const tap = await page.$eval('#refreshGrant', el => el.getBoundingClientRect().height);
    assert.ok(tap >= 44, 'touch targets are at least 44px');
    await shot(page, '11-phone-expired-grant');
    await page.type('#revokeReference', 'synthetic-browser-revoke-expired');
    await page.click('#revokeButton');
    await confirmWith(page, null);
    await waitText(page, '#grantStatus', /Revoked/);
  });
});
