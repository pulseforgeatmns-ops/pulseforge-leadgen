'use strict';

// Exercise the actual durable Places/Max callers with synthetic storage and
// transport. Even a swallowed accidental network attempt fails this process.
require('./helpers/denyWebsiteEvidenceNetwork.cjs');
const { test, after, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const spend = require('../utils/apiSpendControl');
const attribution = require('../utils/placesCostAttribution');
const original = { reserve: spend.reserveApiCall, settle: spend.settleApiCall,
  record: attribution.recordPlacesRequest, flag: process.env.ANCHOR_RESEARCH_PROVIDERS_PAUSED };
let reservations = 0;
spend.reserveApiCall = async () => { reservations++; return 'synthetic-reservation'; };
spend.settleApiCall = async () => {};
attribution.recordPlacesRequest = async () => {};
const guard = require('../utils/anchorResearchProviderPause');
const { createPlacesProvider } = require('../packages/capabilities/discovery/providers/PlacesProvider');
const { createCheckpointSession } = require('../services/scoutQueryCheckpoints');
const { defaultScoutRamp } = require('../services/maxOutboundControlLoop');
const flag = 'ANCHOR_RESEARCH_PROVIDERS_PAUSED';
const codes = ['anchor_research_provider_paused', 'research_provider_tenant_required', 'research_provider_tenant_mismatch'];
beforeEach(() => { delete process.env[flag]; reservations = 0; });
afterEach(() => {
  if (original.flag === undefined) delete process.env[flag];
  else process.env[flag] = original.flag;
  assert.equal(guard.hasResearchProviderScope(), false, 'research authority must not leak');
});
after(() => {
  spend.reserveApiCall = original.reserve;
  spend.settleApiCall = original.settle;
  attribution.recordPlacesRequest = original.record;
});

function interrupt(code) {
  process.env[flag] = 'true';
  guard.assertResearchProviderAllowed('synthetic-research', code === 'research_provider_tenant_required'
    ? 'invalid' : code === 'research_provider_tenant_mismatch' ? '13' : undefined);
  assert.fail('Expected a real research control error');
}

function placesFixture(label, { tenantId = '10', afterSearch, detailFailure } = {}) {
  const effects = { search: 0, details: 0, staged: 0, failures: [] };
  const checkpoint = createCheckpointSession({
    store: { tenantId, load: async () => null, acknowledge: async () => assert.fail('Unexpected acknowledgement') },
    pageStore: { tenantId,
      plan: async rootKey => ({ rootKey, key: rootKey, index: 0, pageToken: null }),
      reserve: async () => 'synthetic-page-reservation',
      stage: async (_plan, candidates) => { effects.staged++; return { state: 'fetched', candidates }; },
      fail: async (_plan, failure) => effects.failures.push(failure),
    },
  });
  const provider = createPlacesProvider({ tenantId, apiKey: `synthetic-${label}`,
    queryCheckpointSession: checkpoint, fetchImpl: async url => {
      if (String(url).includes('/textsearch/')) {
        effects.search++;
        await afterSearch?.();
        return { ok: true, status: 200, json: async () => ({ status: 'OK',
          results: [{ place_id: `synthetic-${label}`, name: 'Synthetic Law Firm' }] }) };
      }
      assert.match(String(url), /\/details\//);
      effects.details++;
      detailFailure?.();
      return { ok: true, status: 200, json: async () => ({ status: 'OK',
        result: { place_id: `synthetic-${label}`, name: 'Synthetic Law Firm', website: 'https://fixture.example' } }) };
    },
  });
  return { effects, checkpoint, provider, run: () => provider.collectEvidence({ tenantId,
    segment: 'law_firm', evidenceType: 'identity', discoveryConcept: 'Law Firm',
    geography: { cities: ['Manchester'], state: 'NH' } }) };
}

test('durable single-page Places propagates a real search-to-details pause without false checkpoint failure', async () => {
  const f = placesFixture('midflight', { afterSearch: () => { process.env[flag] = 'true'; } });
  await assert.rejects(f.run, { code: codes[0] });
  assert.deepEqual(f.effects, { search: 1, details: 0, staged: 0, failures: [] });
  assert.equal(reservations, 1, 'only the completed synthetic search was reserved');
  assert.equal(f.provider.lastExecution, null, 'interruption is not an ordinary empty provider outcome');
  const snapshot = f.checkpoint.snapshot();
  assert.equal(snapshot.unresolvedQueries, 1);
  assert.equal(snapshot.fetchedQueries, 0);
  assert.equal(snapshot.continuation.newPagesReserved, 1, 'existing page reservation accounting is retained');
});

for (const code of codes) test(`durable single-page details rethrows the original ${code}`, async () => {
  let failure;
  const f = placesFixture(code, { detailFailure: () => {
    try { interrupt(code); } catch (error) { failure = error; throw error; }
  } });
  await assert.rejects(f.run, error => error === failure && error.code === code);
  assert.deepEqual(f.effects, { search: 1, details: 1, staged: 0, failures: [] });
  assert.equal(reservations, 2);
});

test('durable single-page ordinary details failure retains sanitized failure and page accounting', async () => {
  const f = placesFixture('ordinary', { detailFailure: () => { throw Error('synthetic private provider detail'); } });
  assert.deepEqual(await f.run(), []);
  assert.equal(f.effects.search, 1);
  assert.equal(f.effects.details, 1);
  assert.equal(f.effects.staged, 0);
  assert.equal(f.effects.failures.length, 1);
  assert.equal(f.effects.failures[0].code, 'google_places_details_failed');
  assert.equal(f.effects.failures[0].noNetwork, false);
  assert.equal(f.provider.lastExecution.errors[0].code, 'google_places_details_failed');
  assert.equal(JSON.stringify(f.effects.failures).includes('synthetic private provider detail'), false);
  assert.equal(f.checkpoint.snapshot().continuation.newPagesReserved, 1);
});

for (const tenantId of ['10', '13']) test(`durable single-page normal tenant ${tenantId} behavior is unchanged`, async () => {
  if (tenantId === '13') process.env[flag] = 'true';
  const f = placesFixture(`normal-${tenantId}`, { tenantId });
  assert.equal((await f.run()).length, 1);
  assert.deepEqual(f.effects, { search: 1, details: 1, staged: 1, failures: [] });
  assert.equal(reservations, 2);
});

test('already-armed Anchor durable Places remains blocked before reservations or transport', async () => {
  process.env[flag] = 'true';
  const f = placesFixture('already-paused');
  await assert.rejects(f.run, { code: codes[0] });
  assert.deepEqual(f.effects, { search: 0, details: 0, staged: 0, failures: [] });
  assert.equal(reservations, 0);
  assert.equal(f.checkpoint.snapshot().continuation.newPagesReserved, 0);
});

function rampFixture(mode, { tenantId = '10', fail } = {}) {
  const effects = { claims: 0, discovery: 0, candidateSnapshots: 0, querySnapshots: 0, events: [], logs: [] };
  const tickets = { tenantId, assertReady: async () => {},
    claimDue: async () => { effects.claims++; if (mode === 'drain') fail?.(); return []; },
    snapshot: async () => { effects.candidateSnapshots++; return {
      status: 'observed_candidate_set_handled', total: 0, due: 0, claimed: 0, byState: {}, byReason: {},
    }; },
  };
  return { effects, run: () => defaultScoutRamp({
    pool: { query: async sql => {
      assert.equal(sql, "SELECT to_regclass('public.anchor_email_recovery_control') AS control_table");
      return { rows: [{ control_table: null }] };
    } },
    store: { tenantId, clientId: Number(tenantId), event: async name => effects.events.push(name) },
    program: { id: 'synthetic', tenant_id: tenantId, policy: {} },
    source: { payload: { structuredMission: { market: { segment: 'small_business_owners' },
      geography: { region: 'Manchester', cities: ['Manchester'], scope: 'local' } } } },
    plan: { deficit: 1 }, searchFingerprint: 'a'.repeat(64), skipVerificationRetry: true,
    enrichment: { run: async () => ({ considered: 0 }) }, recoverExistingInventory: async () => ({}),
    loadDiscoveryBackoff: async () => mode === 'drain' ? { reason: 'synthetic-backoff' } : null,
    createCandidateTicketStore: () => tickets, createPageCheckpointStore: () => ({ assertReady: async () => {} }),
    createTenantDiscoveryMemory: input => ({tenantId:input.tenantId,fingerprint:input.fingerprint,assertReady:async()=>{},snapshot:()=>({})}),
    createQueryCheckpointSession: () => ({ acknowledge: async () => assert.fail('Unexpected acknowledgement'),
      snapshot: () => { effects.querySnapshots++; return { status: 'not_observed' }; } }),
    runDiscovery: async () => { effects.discovery++; fail?.(); return { kind: 'investigated' }; },
    logger: { log: name => effects.logs.push(name), warn: name => effects.logs.push(name) },
  }) };
}

for (const mode of ['discovery', 'drain']) {
  for (const code of codes) test(`Max durable ${mode} rethrows ${code} before later diagnostics or ticket work`, async () => {
    let failure;
    const f = rampFixture(mode, { fail: () => {
      try { interrupt(code); } catch (error) { failure = error; throw error; }
    } });
    await assert.rejects(f.run, error => error === failure && error.code === code);
    assert.deepEqual(f.effects, { claims: mode === 'drain' ? 1 : 0,
      discovery: mode === 'discovery' ? 1 : 0, candidateSnapshots: 1, querySnapshots: 0,
      events: mode === 'drain' ? ['scout_replenishment_backoff'] : [], logs: [] });
    assert.equal(reservations, 0);
  });

  test(`Max durable ${mode} preserves ordinary blocked-result behavior`, async () => {
    const f = rampFixture(mode, { fail: () => { throw Error('synthetic ordinary failure'); } });
    const result = await f.run();
    assert.deepEqual(result.discovery, { kind: 'blocked', reason: mode === 'discovery'
      ? 'discovery_execution_failed' : 'candidate_processing_failed' });
    assert.equal(f.effects.claims, 1, 'ordinary failure retains one durable drain attempt');
    assert.equal(f.effects.candidateSnapshots, 2);
    assert.ok(f.effects.events.includes('scout_candidate_work_progression'));
    assert.equal(result.discoveredQueued, 0);
  });

  test(`Max durable ${mode} preserves default-off Anchor and armed non-Anchor research`, async () => {
    for (const tenantId of ['10', '13']) {
      if (tenantId === '13') process.env[flag] = 'true';
      const f = rampFixture(mode, { tenantId, fail: () => guard.assertResearchProviderAllowed('synthetic-research') });
      const result = await f.run();
      assert.equal(f.effects.claims, 1);
      assert.equal(f.effects.candidateSnapshots, 2);
      assert.equal(result.discoveredQueued, 0);
      assert.equal(result.discovery.kind, mode === 'discovery' ? 'investigated' : 'backoff');
    }
  });
}

test('already-armed Anchor Max remains blocked before durable work or transport', async () => {
  process.env[flag] = 'true';
  const f = rampFixture('discovery');
  await assert.rejects(f.run, { code: codes[0] });
  assert.deepEqual(f.effects, { claims: 0, discovery: 0, candidateSnapshots: 0, querySnapshots: 0, events: [], logs: [] });
  assert.equal(reservations, 0);
});
