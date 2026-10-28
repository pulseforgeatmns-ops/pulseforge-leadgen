'use strict';
// SPEC-SCOUT-P0-003 unit contract: manifest shape, CLI invocation and pause wiring.
const test = require('node:test'), assert = require('node:assert/strict');
const grants = require('../services/scoutResearchOnlyGrant');
const { invocation } = require('../scripts/scout-research-only/run.cjs');

const H = 'a'.repeat(64);
function manifest() {
  return { version: 1, kind: 'scout_research_only', approvalRef: 'synthetic-ref', tenantId: '10', programId: 'p', missionId: 'm',
    programHash: H, sourceHash: H, fingerprint: H, expectedRevision: 'b'.repeat(40), quarantineDecisionHash: H, recoveryControlHash: H,
    operator: { id: '71', role: 'admin', accountClientId: 10, sessionHash: H },
    authorizationEvidence: { kind: 'admin_session_approval', statement: 'Approved.' }, maxCycles: 8, dailyGoogleUsdMicros: 5000000,
    notBefore: '2026-10-28T00:00:00.000Z', expiresAt: '2026-10-29T00:00:00.000Z',
    permissions: { discovery: true, enrichment: false, verification: false, preparation: false, outbound: false }, providers: ['google_places'] };
}

test('only the exact research-only manifest validates', () => {
  assert.equal(grants.validateManifest(manifest()).maxCycles, 8);
  for (const mutate of [m => { m.maxCycles = 9; }, m => { m.maxCycles = 7; }, m => { m.dailyGoogleUsdMicros = 5000001; },
    m => { m.permissions.preparation = true; }, m => { m.permissions.outbound = true; }, m => { m.permissions.verification = true; },
    m => { m.permissions.discovery = false; }, m => { m.providers.push('prospeo'); }, m => { m.tenantId = '13'; },
    m => { m.operator.role = 'manager'; }, m => { m.authorizationEvidence.kind = 'cli'; }, m => { m.extra = true; },
    m => { m.expiresAt = '2026-11-05T00:00:01.000Z'; }, m => { m.expiresAt = m.notBefore; }, m => { m.expectedRevision = 'x'; }])
  { const m = manifest(); mutate(m); assert.throws(() => grants.validateManifest(m), { code: 'research_only_manifest_invalid' }); }
});

test('scheduler branch is default-off and needs the closed environment', () => {
  assert.equal(grants.schedulerEnabled({}), false);
  const env = { SCOUT_RESEARCH_ONLY_GRANTS_ENABLED: 'true', ANCHOR_EMAIL_RECOVERY_MAINTENANCE: 'true', ANCHOR_RESEARCH_PROVIDERS_PAUSED: 'true',
    ANCHOR_GOVERNED_OUTBOUND_ENABLED: 'false', SCOUT_ANCHOR_DISCOVERY_MEMORY_ENABLED: 'true', RAILWAY_GIT_COMMIT_SHA: 'b'.repeat(40) };
  assert.doesNotThrow(() => grants.assertEnvironment(grants.validateManifest(manifest()), env));
  for (const [key, value] of [['ANCHOR_GOVERNED_OUTBOUND_ENABLED', 'true'], ['ANCHOR_RESEARCH_PROVIDERS_PAUSED', 'false'],
    ['ANCHOR_EMAIL_RECOVERY_MANIFEST_ID', 'x'], ['SCOUT_RESEARCH_ONLY_GRANTS_ENABLED', 'false'], ['RAILWAY_GIT_COMMIT_SHA', 'c'.repeat(40)]])
    assert.throws(() => grants.assertEnvironment(grants.validateManifest(manifest()), { ...env, [key]: value }));
});

test('outside a claimed cycle nothing is allowed and no meter exists', () => {
  assert.equal(grants.allowsResearchOnlyProvider('research', '10'), false);
  assert.equal(grants.allowsResearchOnlyProvider('google_places', '10'), false);
  assert.equal(grants.googleMeter(), null);
  assert.throws(() => grants.assertResearchOnlyScope('10'), { code: 'research_only_scope_required' });
  const pause = require('../utils/anchorResearchProviderPause');
  assert.equal(pause.isResearchProviderControlError({ code: grants.SPEND_CODE }), true, 'meter refusals stop the whole ramp');
});

test('CLI only inspects or revokes, and revoke needs an exact confirmation', () => {
  assert.deepEqual(invocation(['--inspect']), { mode: 'inspect', grantId: undefined });
  assert.equal(invocation(['--revoke', H, '--approval-ref', 'close-1', '--confirm', H]).mode, 'revoke');
  for (const args of [['--install'], ['--revoke', H, '--approval-ref', 'close-1', '--confirm', 'b'.repeat(64)], ['--revoke', H],
    ['--inspect', 'not-a-hash'], ['--execute']]) assert.throws(() => invocation(args), { code: 'research_only_explicit_invocation_required' });
});
