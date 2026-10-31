'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const grants = require('../services/scoutResearchOnlyGrant');
const H = 'a'.repeat(64);
const release = () => ({ version: 1, kind: 'scout_research_cycle_release', tenantId: '10', grantId: H,
  previousAllowance: 1, newAllowance: 2, expectedRevision: 'b'.repeat(40),
  operator: { id: '71', role: 'admin', accountClientId: 10, sessionHash: H },
  approvalReference: 'synthetic-release-two', statement: 'Synthetic explicit review of cycle one.' });
test('a release is exactly one cumulative cycle within the immutable ceiling', () => {
  assert.equal(grants.INITIAL_RELEASED_CYCLES, 1);
  for (let n = 1; n < 8; n++) assert.equal(grants.validateCycleRelease({ ...release(), previousAllowance: n, newAllowance: n + 1 }).newAllowance, n + 1);
  for (const mutate of [r => { r.previousAllowance = 0; }, r => { r.newAllowance = 1; }, r => { r.newAllowance = 3; },
    r => { r.previousAllowance = 8; r.newAllowance = 9; }, r => { r.previousAllowance = 1.5; r.newAllowance = 2.5; },
    r => { r.tenantId = '13'; }, r => { r.grantId = 'x'; }, r => { r.operator.role = 'manager'; },
    r => { r.operator.accountClientId = 13; }, r => { r.operator.sessionHash = ''; }, r => { r.expectedRevision = 'x'; },
    r => { r.statement = ''; }, r => { r.approvalReference = 'bad reference'; }, r => { r.dailyGoogleUsdMicros = 6000000; },
    r => { r.expiresAt = '2099-01-01'; }, r => { r.permissions = { outbound: true }; }]) {
    const r = release(); mutate(r);
    assert.throws(() => grants.validateCycleRelease(r), { code: 'research_only_release_invalid' });
  }
});
test('invalid release never touches a database or a provider', async () => {
  const noDb = { connect() { assert.fail('invalid authority must fail before connection'); } };
  await assert.rejects(grants.releaseResearchCycle(noDb, {}, {}), { code: 'research_only_release_invalid' });
  await assert.rejects(grants.releaseResearchCycle(noDb, { release: release(), releaseHash: H }, { id: '71', role: 'admin' }),
    { code: 'research_only_release_exact_confirmation_required' });
});
