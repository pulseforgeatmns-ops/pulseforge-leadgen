'use strict';
const { startApprovalHarness, ADMIN } = require('../helpers/scoutResearchApprovalHarness');
const deny = require('../helpers/denyExternalProviders.cjs');
const test = require('node:test'), assert = require('node:assert/strict');
const puppeteer = require('puppeteer');

test('Anchor admin reviews and releases exactly one next cycle on tablet and phone', { timeout: 600000 }, async t => {
  const h = await startApprovalHarness(); t.after(() => h.close());
  const { pool } = h;
  const grants = require('../../services/scoutResearchOnlyGrant'), missions = require('../../services/scoutResearchMission');
  const mp = await missions.previewResearchMission(pool, { approvalRef: 'synthetic-browser-release-mission' }, h.actor);
  const mission = await missions.installResearchMission(pool, { approvalRef: 'synthetic-browser-release-mission', mission: mp.mission, missionHash: mp.missionHash }, h.actor);
  const gp = await grants.previewResearchOnlyGrant(pool, { approvalRef: 'synthetic-browser-release-grant', researchMissionId: mission.missionId,
    expiresAt: new Date(Date.now() + 86400000).toISOString(), statement: 'Synthetic first-cycle-only fixture.' }, h.actor);
  const grant = await grants.installResearchOnlyGrant(pool, { manifest: gp.manifest, manifestHash: gp.manifestHash }, h.actor);
  const control = require('../../services/maxOutboundControlLoop');
  const program = (await pool.query("SELECT * FROM acquisition_outbound_programs WHERE tenant_id='10' ORDER BY authorized_at DESC LIMIT 1")).rows[0];
  const first = await grants.claimResearchCycle(pool, { program, source: await control._test.loadSource(pool, program, '10'), cycleId: 'synthetic-browser-first' });
  await grants.finishResearchCycle(pool, first, { completed: false, result: { reason: 'synthetic_review_fixture' } });
  const browser = await puppeteer.launch({ executablePath: process.env.CHROMIUM_PATH || '/opt/pw-browsers/chromium', headless: true, pipe: true,
    args: ['--no-sandbox','--disable-dev-shm-usage','--disable-background-networking','--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE 127.0.0.1'] });
  t.after(() => browser.close());
  const page = await browser.newPage();
  await page.setViewport({ width: 800, height: 1280, isMobile: true, hasTouch: true });
  const errors = []; page.on('pageerror', e => errors.push(e.message));
  await page.setRequestInterception(true);
  page.on('request', req => req.url().startsWith(h.origin + '/') ? req.continue() : req.abort());
  await page.goto(h.origin + '/login');
  await page.type('#login-email', ADMIN.email); await page.type('#login-password', h.fixturePassword);
  await Promise.all([page.waitForNavigation(),page.click('button[type=submit]')]);
  await page.goto(h.origin + '/admin/scout-research-approval');
  await page.waitForFunction(() => document.querySelector('#grantStatus')?.textContent.includes('Awaiting cycle release'));
  assert.equal(await page.$eval('#releaseArea', e => e.hidden), false);
  await page.type('#releaseReference', 'synthetic-browser-release-two');
  await page.type('#releaseStatement', 'Synthetic review of cycle one; approve only cycle two.');
  await page.click('#previewRelease');
  await page.waitForSelector('#confirmDialog[open]');
  const facts = await page.$eval('#confirmFacts', e => e.textContent);
  assert.match(facts, /1 → 2/); assert.match(facts, /\$5\.00/); assert.match(facts, new RegExp(gp.expectedRevision));
  await page.click('#confirmCancel');
  assert.equal((await grants.inspectResearchOnlyGrant(pool, grant.grantId)).releasedCycles, 1);
  await page.click('#previewRelease'); await page.waitForSelector('#confirmDialog[open]');
  await page.type('#confirmPassword', h.fixturePassword); await page.click('#confirmSubmit');
  await page.waitForFunction(() => document.querySelector('#status')?.textContent.includes('Cycle 2 released.'));
  const receipt = await grants.inspectResearchOnlyGrant(pool, grant.grantId);
  assert.equal(receipt.releasedCycles, 2); assert.equal(receipt.cyclesConsumed, 1);
  assert.equal(receipt.releases.length, 2); assert.equal(receipt.releases[1].administrator_id, ADMIN.id);
  assert.equal(await page.$eval('#releaseArea', e => e.hidden), true, 'no release can be banked before cycle two is consumed');
  for (const width of [800,390]) {
    await page.setViewport({ width, height: 1280, isMobile: true, hasTouch: true });
    assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth));
  }
  assert.deepEqual(errors, []);
  assert.equal((await pool.query('SELECT count(*)::int n FROM scout_research_google_spend')).rows[0].n, 0);
  assert.equal(deny.attempts(), 0);
});
