'use strict';
require('./helpers/denyWebsiteEvidenceNetwork.cjs');
const test = require('node:test');
const assert = require('node:assert/strict');
const { defaultScoutRamp } = require('../services/maxOutboundControlLoop');
const fingerprint = 'a'.repeat(64);
const manifest = () => ({ version: 1, epoch: 1, approvalRef: 'local-synthetic-review', tenantId: '13', fingerprint,
  rootQueryKeys: ['b'.repeat(64)], notBefore: '2026-10-19T00:00:00.000Z', expiresAt: '2026-10-20T00:00:00.000Z' });
const names = ['SCOUT_DURABLE_CANDIDATES_ENABLED', 'SCOUT_LEGACY_COVERAGE_EPOCH_ENABLED', 'SCOUT_LEGACY_COVERAGE_EPOCH_JSON'];
function environment(t, values) {
  const previous = Object.fromEntries(names.map(name => [name, process.env[name]]));
  for (const name of names) values[name] == null ? delete process.env[name] : process.env[name] = values[name];
  t.after(() => { for (const name of names) previous[name] == null ? delete process.env[name] : process.env[name] = previous[name]; });
}
async function run({ tenantId = '13', durable = true } = {}) {
  const calls = { discovery: 0, claims: 0, pageInputs: [], events: [] };
  const tickets = { tenantId, assertReady: async () => {}, claimDue: async () => { calls.claims++; return []; },
    snapshot: async () => ({ status: 'observed_candidate_set_handled', total: 0, due: 0, claimed: 0, byState: {}, byReason: {} }) };
  const result = await defaultScoutRamp({
    pool: { query: async () => { throw Error('Unexpected database or provider call'); } },
    store: { tenantId, clientId: Number(tenantId), event: async (...args) => calls.events.push(args) },
    program: { id: 'fixture', tenant_id: tenantId, policy: {} },
    source: { payload: { structuredMission: { market: { segment: 'small_business_owners' },
      geography: { region: 'Manchester', cities: ['Manchester'], scope: 'local' } } } },
    plan: { deficit: 1 }, searchFingerprint: fingerprint, skipVerificationRetry: true,
    enrichment: { run: async () => ({ considered: 0 }) }, recoverExistingInventory: async () => ({}),
    loadDiscoveryBackoff: async () => null,
    ...(durable ? { createCandidateTicketStore: () => tickets } : {}),
    createPageCheckpointStore: input => { calls.pageInputs.push(input); return { assertReady: async () => {} }; },
    createQueryCheckpointSession: () => ({ acknowledge: async () => {}, snapshot: () => ({ status: 'not_observed' }) }),
    runDiscovery: async (_input, options) => { calls.discovery++; await options.persistCompanies({ companies: [] }); return { kind: 'investigated' }; },
    logger: { log() {}, warn() {} },
  });
  return { calls, result };
}

test('epoch permission is resolved from exact tenant and fingerprint inside the durable gate', async t => {
  environment(t, { SCOUT_LEGACY_COVERAGE_EPOCH_ENABLED: 'true', SCOUT_LEGACY_COVERAGE_EPOCH_JSON: JSON.stringify(manifest()) });
  const { calls } = await run();
  assert.equal(calls.pageInputs.length, 1);
  assert.equal(calls.pageInputs[0].tenantId, '13');
  assert.equal(calls.pageInputs[0].fingerprint, fingerprint);
  assert.equal(calls.pageInputs[0].coverageEpoch.epoch, 1);
  assert.deepEqual(calls.pageInputs[0].coverageEpoch.rootQueryKeys, manifest().rootQueryKeys);
  assert.equal(calls.discovery, 1); assert.equal(calls.claims, 1);
});

test('flag-off and nonallowlisted tenants never activate or parse an epoch manifest', async t => {
  environment(t, { SCOUT_LEGACY_COVERAGE_EPOCH_ENABLED: 'true', SCOUT_LEGACY_COVERAGE_EPOCH_JSON: '{malformed' });
  assert.equal((await run({ durable: false })).calls.pageInputs.length, 0);
  assert.equal((await run({ tenantId: '17' })).calls.pageInputs.length, 0);
});

test('a valid manifest for another tenant grants no epoch permission to this run', async t => {
  environment(t, { SCOUT_LEGACY_COVERAGE_EPOCH_ENABLED: 'true', SCOUT_LEGACY_COVERAGE_EPOCH_JSON: JSON.stringify(manifest()) });
  const { calls } = await run({ tenantId: '10' });
  assert.equal(calls.pageInputs[0].coverageEpoch, null);
  assert.equal(calls.discovery, 1); assert.equal(calls.claims, 1);
});

test('malformed enabled permission blocks acquisition while existing candidate obligations still drain', async t => {
  environment(t, { SCOUT_LEGACY_COVERAGE_EPOCH_ENABLED: 'true', SCOUT_LEGACY_COVERAGE_EPOCH_JSON: '{private-token:bad' });
  const { calls, result } = await run();
  assert.equal(calls.discovery, 0); assert.equal(calls.claims, 1);
  assert.equal(calls.pageInputs[0].coverageEpoch, null);
  assert.equal(result.discovery.reason, 'coverage_epoch_config_invalid');
  assert.equal(result.queryProgression.coverageEpochStatus, 'configuration_invalid');
  assert.ok(calls.events.some(event => event[0] === 'tick_blocked'));
  assert.ok(!JSON.stringify({ calls, result }).includes('private-token'));
});

test('approval diagnostics re-project only bounded observed hashes and derived ceilings', () => {
  const { queryProgressionDiagnostic } = require('../services/scoutCandidateTicketDispatch');
  const result = queryProgressionDiagnostic({ status: 'pending_query_work', coverageApprovalManifest: {
    ...manifest(), rootQueryKeys: ['b'.repeat(64)], truncated: true, maxTotalHttpAttempts: 1000000,
    providerToken: 'private-provider-token', rawQuery: 'private query', approvalRef: 'private-person' },
    continuation: { statuses: { coverage_epoch_permission_required: 1, 'private-token': 1 } } });
  assert.equal(result.coverageApprovalManifest.truncated, true);
  assert.equal(result.coverageApprovalManifest.observedOnly, true);
  assert.equal(result.coverageApprovalManifest.maxTotalHttpAttempts, 69);
  assert.equal(result.continuation.statuses.coverage_epoch_permission_required, 1);
  assert.ok(!JSON.stringify(result).includes('private'));
  assert.equal(queryProgressionDiagnostic({ coverageApprovalManifest: { ...manifest(), fingerprint: 'not-a-hash' } }).coverageApprovalManifest, undefined);
});
