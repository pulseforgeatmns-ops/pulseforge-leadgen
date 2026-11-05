'use strict';
// SPEC-SCOUT-P1-001 Rev. 2 (#1048): crash recovery and allocation accounting.
// Drives the REAL Places provider, checkpoint session and page selection
// (selectPage) over an in-memory model of the P0-005 tables. The tenant query
// owner model mirrors scoutTenantDiscoveryMemory.queryStatus; its SQL is
// covered by scoutTenantDiscoveryMemoryPostgres. No network, no Postgres.
const test = require('node:test'), assert = require('node:assert/strict');
const { createCheckpointSession, queryIdentity } = require('../services/scoutQueryCheckpoints');
const { selectPage } = require('../services/scoutQueryContinuation');
const { createPlacesProvider } = require('../packages/capabilities/discovery/providers/PlacesProvider');
const { createPlacesDiscoveryAdapter } = require('../packages/max/scoutAcquisition/DiscoveryAdapters');
const coverage = require('../services/scoutFacilityResearchCoverage');
const { deriveLedger } = require('../services/scoutFacilityTierAllocation');
const { ANCHOR_FACILITY_TIERS: CONFIG } = require('../config/anchorFacilityTiers');

const scope = { eligibleSubsegments: ['managed_facilities', 'restaurant_foh'],
  cities: ['Manchester', 'Bedford', 'Londonderry', 'Hooksett', 'Auburn', 'Goffstown', 'Nashua', 'Merrimack'] };
const ACTIVATED = '2026-11-10T00:00:00.000Z';
const request = { tenantId: '10', scope: 'market', evidenceType: 'identity', segment: 'managed_facilities',
  geography: { cities: scope.cities, state: 'NH' } };

function world() {
  return { pages: new Map(), checkpoints: new Map(), owners: new Map(), charges: [], spend: [],
    clock: Date.parse('2026-11-10T12:00:00Z') };
}
const snapshotOf = w => JSON.stringify({ pages: [...w.pages], checkpoints: [...w.checkpoints], owners: [...w.owners] });

function lifecycle(w, fp, faults = {}) {
  const ck = key => `${fp}|${key}`;
  const store = { tenantId: '10', fingerprint: fp,
    load: async key => w.checkpoints.get(ck(key)) || null,
    stage: async () => assert.fail('page store path only'),
    acknowledge: async keys => keys.forEach(key => Object.assign(w.checkpoints.get(ck(key)), { state: 'handled', candidates: [] })) };
  const pageStore = { tenantId: '10',
    plan: async rootKey => {
      const rows = [...w.pages.values()].filter(r => r.search_fingerprint === fp && r.root_query_key === rootKey)
        .map(r => ({ ...r, checkpoint_state: w.checkpoints.get(ck(r.query_key))?.state,
          candidates: w.checkpoints.get(ck(r.query_key))?.candidates }));
      return selectPage(rootKey, rows, rows.length ? null : w.checkpoints.get(ck(rootKey)) || null, w.clock);
    },
    reserve: async plan => {
      if (w.pages.has(ck(plan.key))) return null;
      w.pages.set(ck(plan.key), { search_fingerprint: fp, query_key: plan.key, root_query_key: plan.rootKey,
        page_index: plan.index, state: 'reserved', request_token: plan.pageToken, next_page_token: null,
        created_at: new Date(w.clock).toISOString(), available_at: null, blocked_reason: null });
      return `reservation-${w.pages.size}`;
    },
    stage: async (plan, candidates, meta) => {
      const row = w.pages.get(ck(plan.key));
      Object.assign(row, { state: 'fetched', next_page_token: meta?.nextPageToken || null,
        available_at: new Date(w.clock).toISOString() });
      w.checkpoints.set(ck(plan.key), { state: 'fetched', candidates });
      // Crash after the staging transaction commits, before ingestion acknowledges.
      if (faults.crashAfterStageCommit) throw Object.assign(Error('synthetic_process_crash'), { code: 'synthetic_process_crash' });
      return { state: 'fetched', candidates };
    },
    fail: async (plan, failure = {}) => {
      if (failure.noNetwork === true) { w.pages.delete(ck(plan.key)); return; }
      Object.assign(w.pages.get(ck(plan.key)), { state: 'blocked', blocked_reason: 'page_provider_failure' });
    } };
  const status = (key, query) => {
    const owner = w.owners.get(queryIdentity(query, false));
    if (owner?.fingerprint === fp && owner.rootKey === key) return null;
    const others = [...w.pages.values()].filter(r => r.root_query_key === key && r.search_fingerprint !== fp);
    if (others.some(r => ['fetched', 'handled'].includes(w.checkpoints.get(`${r.search_fingerprint}|${r.query_key}`)?.state)))
      return 'tenant_query_previously_searched';
    return owner || others.length ? 'tenant_query_outcome_unconfirmed' : null;
  };
  const tenantMemory = { tenantId: '10', fingerprint: fp,
    checkQuery: async (key, query) => status(key, query),
    reserveQuery: async (key, query) => {
      const found = status(key, query); if (found) return found;
      const id = queryIdentity(query, false);
      if (!w.owners.has(id)) w.owners.set(id, { fingerprint: fp, rootKey: key, claimedAt: new Date(w.clock).toISOString() });
      return status(key, query);
    },
    continuationStatus: async () => null, snapshot: () => ({}) };
  return { store, pageStore, tenantMemory };
}

// Read-only loaders derived from the same tables the production SQL reads.
function loaders(w) {
  return {
    loadHistory: async () => [...w.owners.values()].map(o => ({ root_query_key: o.rootKey, attempts: 1 })),
    loadAllocation: async keys => ({ activations: [{ allocation_version: CONFIG.version, activated_at: ACTIVATED }],
      pages: [...w.pages.values()].filter(p => keys.includes(p.root_query_key) && p.created_at >= ACTIVATED),
      claimedRoots: [...new Set([...[...w.owners.values()].map(o => o.rootKey), ...[...w.pages.values()].map(p => p.root_query_key)])]
        .filter(k => keys.includes(k)) }),
  };
}

async function cycle(w, fp, faults = {}) {
  const { store, pageStore, tenantMemory } = lifecycle(w, fp, faults);
  const session = createCheckpointSession({ store, pageStore, tenantMemory });
  const planner = coverage.createFacilityResearchCoverage({ tenantId: '10', scope, tierAllocation: CONFIG, ...loaders(w) });
  const provider = createPlacesProvider({ apiKey: 'synthetic-key', queryCheckpointSession: session,
    tenantDiscoveryMemory: tenantMemory, facilityResearchCoverage: planner, useCache: false,
    placesApiDeps: { db: { query: async () => ({ rows: [] }) },
      reserveApiCall: async input => {
        if (faults.budgetDenied) throw Object.assign(Error('synthetic_budget_denied'), { code: 'budget_denied' });
        w.spend.push(input); return `usage-${w.spend.length}`;
      }, settleApiCall: async () => {} },
    fetchImpl: async url => {
      const query = new URL(url).searchParams.get('query'); w.charges.push(query);
      if (faults.providerFailure) return { status: 500, ok: false, json: async () => ({ status: 'UNKNOWN_ERROR', results: [] }) };
      return { status: 200, ok: true, json: async () => ({ status: 'ZERO_RESULTS', results: [] }) };
    } });
  let error = null;
  try { await createPlacesDiscoveryAdapter({ placesProvider: provider }).discover({ tenantId: '10', evidenceRequest: request }); }
  catch (e) { error = e; }
  if (!error && !faults.skipAcknowledge) await session.acknowledge();
  return { error, planner: planner.snapshot(), session: session.snapshot(), tenantMemory };
}

const universe = coverage.versionUniverse(CONFIG, require('../packages/capabilities/discovery/providers/PlacesProvider').buildQueriesForEvidence);
const ledgerOf = w => deriveLedger({ pages: [...w.pages.values()], version: CONFIG.version,
  activations: [{ allocation_version: CONFIG.version, activated_at: ACTIVATED }], universeOf: v => (v === CONFIG.version ? universe : null) });

test('crash after the paid call, before durable completion: never selected or charged again; counted once', async () => {
  const w = world();
  // Simulate a crash between the provider response and staging by killing the
  // process inside fetch's caller: the response is received, stage never runs.
  const { store, pageStore, tenantMemory } = lifecycle(w, 'fp-a');
  const session = createCheckpointSession({ store, pageStore, tenantMemory });
  const planner = coverage.createFacilityResearchCoverage({ tenantId: '10', scope, tierAllocation: CONFIG, ...loaders(w) });
  const queries = await planner.plan(request, require('../packages/capabilities/discovery/providers/PlacesProvider').buildQueriesForEvidence);
  assert.equal(planner.snapshot().tierAllocation.intendedTier, 1);
  const first = queries[0];
  const prior = await session.lookup(first, false);
  assert.equal(prior.handled, false); assert.equal(prior.candidates, null);
  w.charges.push(first); // Provider call issued and charged; process dies here.
  assert.equal([...w.pages.values()][0].state, 'reserved');

  const before = ledgerOf(w);
  assert.deepEqual(before.counts, { 1: 1, 2: 0, 3: 0 }); assert.equal(before.uncertain, 1);
  assert.deepEqual(before.countsByStatus[1], { completed: 0, uncertain: 1 }, 'possibly spent, reported apart from completed');

  // Restart, same mission: the reserved page is non-terminal and never refetched.
  w.clock += 5 * 60 * 1000;
  const retry = await createCheckpointSession(lifecycle(w, 'fp-a')).lookup(first, false);
  assert.deepEqual([retry.handled, retry.continuationStatus], [true, 'page_fetch_in_progress']);
  w.clock += 2 * 60 * 60 * 1000;
  const late = await createCheckpointSession(lifecycle(w, 'fp-a')).lookup(first, false);
  assert.deepEqual([late.handled, late.continuationStatus], [true, 'page_fetch_outcome_unknown']);
  // Another mission fingerprint sees the durable tenant claim.
  const other = await createCheckpointSession(lifecycle(w, 'fp-b')).lookup(first, false);
  assert.deepEqual([other.handled, other.continuationStatus], [true, 'tenant_query_outcome_unconfirmed']);
  assert.equal(w.charges.filter(q => q === first).length, 1);

  // Counters are identical across restarts, and the next cycle buys a different query.
  assert.equal(JSON.stringify(ledgerOf(w)), JSON.stringify(before));
  const next = await cycle(w, 'fp-a');
  assert.equal(next.error, null);
  assert.equal(next.planner.tierAllocation.position, 2);
  assert.deepEqual(next.planner.tierAllocation.counts, { 1: 1, 2: 0, 3: 0 });
  assert.equal(next.planner.tierAllocation.uncertainSelections, 1);
  assert.deepEqual(next.planner.tierAllocation.countsByStatus[1], { completed: 0, uncertain: 1 });
  assert.equal(w.charges.length, 2); assert.notEqual(w.charges[1], first);
  assert.equal(w.charges.filter(q => q === first).length, 1);
  assert.deepEqual(ledgerOf(w).counts, { 1: 1, 2: 1, 3: 0 });
});

test('crash after staging commits, before acknowledgement: replayed from the durable payload, not repurchased', async () => {
  const w = world();
  const crashed = await cycle(w, 'fp-a', { crashAfterStageCommit: true });
  assert.equal(w.charges.length, 1);
  const query = w.charges[0];
  assert.equal(crashed.planner.tierAllocation.outcome, 'as_intended');
  assert.equal([...w.pages.values()][0].state, 'fetched');
  assert.equal([...w.checkpoints.values()][0].state, 'fetched', 'committed payload awaits acknowledgement');
  assert.deepEqual(ledgerOf(w).counts, { 1: 1, 2: 0, 3: 0 }); assert.equal(ledgerOf(w).uncertain, 0);
  const replay = await createCheckpointSession(lifecycle(w, 'fp-a')).lookup(query, false);
  assert.equal(replay.handled, false); assert.deepEqual(replay.candidates, []);
  assert.equal(w.charges.length, 1, 'replay never reaches the provider');
  const owners = JSON.stringify([...w.owners]);
  const again = await cycle(w, 'fp-a');
  assert.equal(again.planner.tierAllocation.position, 2);
  assert.equal(w.charges.filter(q => q === query).length, 1);
  assert.ok(JSON.stringify([...w.owners]).startsWith(owners.slice(0, -1)), 'existing ownership records unchanged');
});

test('pre-network budget denial releases only the reservation: no charge, no allocation slot, ownership kept', async () => {
  const w = world();
  const denied = await cycle(w, 'fp-a', { budgetDenied: true });
  assert.equal(denied.error, null);
  assert.equal(w.charges.length, 0); assert.equal(w.pages.size, 0);
  assert.equal(w.owners.size, 1, 'the tenant query claim is permanent');
  assert.deepEqual(ledgerOf(w).counts, { 1: 0, 2: 0, 3: 0 });
  const retried = await cycle(w, 'fp-a');
  assert.equal(retried.planner.tierAllocation.position, 1, 'the aborted attempt consumed no position');
  assert.equal(w.charges.length, 1);
  assert.deepEqual(ledgerOf(w).counts, { 1: 1, 2: 0, 3: 0 });
});

test('provider failure is terminal: counted once, never retried', async () => {
  const w = world();
  await cycle(w, 'fp-a', { providerFailure: true });
  const failed = w.charges.slice();
  assert.ok(failed.length >= 1);
  assert.equal([...w.pages.values()][0].state, 'blocked');
  assert.deepEqual(ledgerOf(w).counts, { 1: 1, 2: 0, 3: 0 });
  const retry = await createCheckpointSession(lifecycle(w, 'fp-a')).lookup(failed[0], false);
  assert.deepEqual([retry.handled, retry.continuationStatus], [true, 'page_provider_failure']);
  const charged = w.charges.length;
  await cycle(w, 'fp-a');
  assert.equal(w.charges.filter(q => q === failed[0]).length, failed.filter(q => q === failed[0]).length);
  assert.equal(w.charges.length, charged + 1);
});

test('one page per cycle holds and repeated restarts reproduce identical decisions', async () => {
  const runs = [];
  for (let attempt = 0; attempt < 2; attempt++) {
    const w = world(), receipts = [];
    for (let n = 0; n < 12; n++) {
      const r = await cycle(w, 'fp-a');
      assert.equal(r.session.continuation.newPagesReserved, 1);
      receipts.push(r.planner.tierAllocation);
      w.clock += 60 * 60 * 1000;
    }
    runs.push({ receipts, charges: w.charges.slice(), ledger: ledgerOf(w) });
  }
  assert.equal(JSON.stringify(runs[0]), JSON.stringify(runs[1]));
  const tiers = runs[0].ledger.history.map(r => r.tier);
  assert.deepEqual(tiers.slice(0, 10), [1, 2, 1, 3, 2, 1, 1, 2, 1, 2]);
  assert.equal(new Set(runs[0].charges).size, 12, 'no query purchased twice');
  for (const r of runs[0].receipts) assert.equal(r.outcome, 'as_intended');
});

test('allocation state is read-only and inert until activated', async () => {
  const statements = [];
  const pool = { query: async (sql, params) => {
    statements.push(sql);
    if (/scout_query_allocation_activations/.test(sql)) throw Object.assign(Error('relation does not exist'), { code: '42P01' });
    return { rows: [] };
  } };
  assert.deepEqual(await coverage.loadAllocationState(pool, CONFIG, []), { activations: [] });
  const planner = coverage.createFacilityResearchCoverage({ tenantId: '10', scope, pool, tierAllocation: CONFIG });
  const planned = await planner.plan(request, require('../packages/capabilities/discovery/providers/PlacesProvider').buildQueriesForEvidence);
  const v1 = coverage.createFacilityResearchCoverage({ tenantId: '10', scope, loadHistory: async () => [] });
  assert.deepEqual(planned, await v1.plan(request, require('../packages/capabilities/discovery/providers/PlacesProvider').buildQueriesForEvidence));
  assert.deepEqual(planner.snapshot().tierAllocation, { version: CONFIG.version, status: 'not_activated' });
  for (const sql of statements) assert.match(sql.trim(), /^(SELECT|WITH)\b/i, 'no write statement');

  const active = { query: async (sql) => { statements.push(sql);
    return /activations/.test(sql) ? { rows: [{ allocation_version: CONFIG.version, activated_at: ACTIVATED }] } : { rows: [] }; } };
  const state = await coverage.loadAllocationState(active, CONFIG, ['a'.repeat(64)]);
  assert.deepEqual(state.activations, [{ allocation_version: CONFIG.version, activated_at: ACTIVATED }]);
  for (const sql of statements) assert.doesNotMatch(sql, /\b(INSERT|UPDATE|DELETE|TRUNCATE|ALTER|DROP)\b/i);
});
