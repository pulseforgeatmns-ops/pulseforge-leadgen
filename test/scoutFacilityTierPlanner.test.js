'use strict';
// SPEC-SCOUT-P1-001 Rev. 2 (#1048): planner wiring, taxonomy, fixture, reporting.
const test = require('node:test'), assert = require('node:assert/strict');
const coverage = require('../services/scoutFacilityResearchCoverage');
const { buildQueriesForEvidence } = require('../packages/capabilities/discovery/providers/PlacesProvider');
const { ANCHOR_FACILITY_TIERS: CONFIG } = require('../config/anchorFacilityTiers');
const { deriveLedger, attributePage, activationWindows } = require('../services/scoutFacilityTierAllocation');
const { fixture } = require('../scripts/scoutTierAllocationFixture');

const scope = { eligibleSubsegments: ['managed_facilities', 'restaurant_foh'],
  cities: ['Manchester', 'Bedford', 'Londonderry', 'Hooksett', 'Auburn', 'Goffstown', 'Nashua', 'Merrimack'] };
const request = { tenantId: '10', segment: 'managed_facilities', evidenceType: 'identity', geography: { cities: scope.cities, state: 'NH' } };
const ACTIVATED = '2026-11-10T00:00:00.000Z';
const activation = [{ allocation_version: CONFIG.version, activated_at: ACTIVATED }];
const active = (pages = [], claimedRoots = []) => async () => ({ activations: activation, pages, claimedRoots });

test('Anchor tier config names the institutional, medical and larger-office concepts explicitly', () => {
  const plan = coverage.approvedQueryPlan(scope, 0, buildQueriesForEvidence, CONFIG);
  const concepts = new Map(plan.map(r => [r.concept, r.category]));
  for (const [concept, category] of [['daycare', 'early_learning'], ['preschool', 'early_learning'],
    ['early learning center', 'early_learning'], ['commercial property management', 'property_manager'],
    ['medical office', 'medical'], ['dental office', 'medical'], ['multi-tenant office building', 'commercial_office']])
    assert.equal(concepts.get(concept), category, concept);
  assert.equal(CONFIG.tierByCategory.early_learning, 1); assert.equal(CONFIG.tierByCategory.property_manager, 1);
  assert.equal(CONFIG.tierByCategory.medical, 2); assert.equal(CONFIG.tierByCategory.commercial_office, 2);
  assert.equal(CONFIG.tierByCategory.restaurant, 3);
  for (const row of plan) assert.ok([1, 2, 3].includes(CONFIG.tierByCategory[row.category]), row.concept);
  // Preschool is early learning, not K-12; dental office is medical, not generic office.
  assert.equal(coverage.category('preschool'), 'early_learning');
  assert.equal(coverage.category('dental office'), 'medical');
});

test('the v1 plan and order are unchanged without tier allocation', () => {
  const v1 = coverage.approvedQueryPlan(scope, 0, buildQueriesForEvidence);
  assert.equal(v1.length, 88);
  assert.ok(!v1.some(r => ['early_learning', 'medical'].includes(r.category)));
});

test('an eligible institutional query is not displaced because a cafe query has lower coverage', async () => {
  const plan = coverage.approvedQueryPlan(scope, 0, buildQueriesForEvidence, CONFIG);
  // Every non-restaurant category has prior attempts; cafes have none.
  const history = plan.filter(r => r.category !== 'restaurant').map(r => ({ query_text: r.query, attempts: 2 }));
  const v1 = coverage.createFacilityResearchCoverage({ tenantId: '10', scope, loadHistory: async () => history });
  const v1First = (await v1.plan(request, buildQueriesForEvidence))[0];
  assert.equal(coverage.category(v1First), 'restaurant', 'v1 least-coverage picks the cafe/restaurant query');
  const tiered = coverage.createFacilityResearchCoverage({ tenantId: '10', scope, loadHistory: async () => history,
    tierAllocation: CONFIG, loadAllocation: active() });
  const first = (await tiered.plan(request, buildQueriesForEvidence))[0];
  assert.equal(CONFIG.tierByCategory[plan.find(r => r.query === first).category], 1);
  assert.equal(tiered.snapshot().tierAllocation.reason, 'deficit');
});

test('identical inputs produce identical plans and decision records; planned set is only approved queries', async () => {
  const make = () => coverage.createFacilityResearchCoverage({ tenantId: '10', scope, loadHistory: async () => [],
    tierAllocation: CONFIG, loadAllocation: active() });
  const a = make(), b = make();
  const pa = await a.plan(request, buildQueriesForEvidence), pb = await b.plan(request, buildQueriesForEvidence);
  assert.deepEqual(pa, pb);
  for (const s of [a, b]) { s.note(pa[0], { handled: true, continuationStatus: 'tenant_query_low_novelty' }); s.note(pa[1], { singlePage: true, pageIndex: 0 }); }
  assert.equal(JSON.stringify(a.snapshot()), JSON.stringify(b.snapshot()));
  const approved = new Set(coverage.approvedQueryPlan(scope, 0, buildQueriesForEvidence, CONFIG).map(r => r.query));
  assert.equal(pa.length, approved.size); assert.ok(pa.every(q => approved.has(q)));
  assert.deepEqual(await a.plan(request, buildQueriesForEvidence), [], 'still one allocation per cycle');
});

test('a lookup-level skip in the intended tier is recorded as redistribution, not compliance', async () => {
  const s = coverage.createFacilityResearchCoverage({ tenantId: '10', scope, loadHistory: async () => [],
    tierAllocation: CONFIG, loadAllocation: active() });
  const planned = await s.plan(request, buildQueriesForEvidence);
  const plan = coverage.approvedQueryPlan(scope, 0, buildQueriesForEvidence, CONFIG);
  const tierOf = q => CONFIG.tierByCategory[plan.find(r => r.query === q).category];
  const t1 = planned.filter(q => tierOf(q) === 1), firstT2 = planned.find(q => tierOf(q) === 2);
  for (const q of t1) s.note(q, { handled: true, continuationStatus: 'tenant_query_low_novelty' });
  s.note(firstT2, { singlePage: true, pageIndex: 0 });
  const record = s.snapshot().tierAllocation;
  assert.equal(record.intendedTier, 1); assert.equal(record.selectedTier, 2); assert.equal(record.outcome, 'redistributed');
  assert.deepEqual(record.redistribution, { fromTier: 1, skippedQueries: t1.length, reasons: ['tenant_query_low_novelty'] });
});

test('a tier with no unclaimed query is ineligible, recorded as a shortfall and its share redistributed', async () => {
  const universe = coverage.versionUniverse(CONFIG, buildQueriesForEvidence);
  const plan0 = coverage.approvedQueryPlan(scope, 0, buildQueriesForEvidence, CONFIG);
  const gen0Restaurants = plan0.filter(r => r.category === 'restaurant').map(r => r.rootKey);
  const allRestaurants = [...universe].filter(([, t]) => t === 3).map(([k]) => k);
  const t12 = plan0.filter(r => CONFIG.tierByCategory[r.category] !== 3);
  const pages = Array.from({ length: 9 }, (_, i) => ({ search_fingerprint: 'f', query_key: t12[i].rootKey,
    root_query_key: t12[i].rootKey, page_index: 0, state: 'fetched', created_at: new Date(Date.parse(ACTIVATED) + i * 1000).toISOString() }));
  for (const [claimed, reason] of [[gen0Restaurants, 'no_unclaimed_query_in_current_generation'], [allRestaurants, 'tier_exhausted_in_version']]) {
    const s = coverage.createFacilityResearchCoverage({ tenantId: '10', scope, loadHistory: async () => [],
      tierAllocation: CONFIG, loadAllocation: active(pages, [...claimed, ...pages.map(p => p.root_query_key)]) });
    const planned = await s.plan(request, buildQueriesForEvidence);
    const record = s.snapshot().tierAllocation;
    assert.deepEqual(record.eligibleTiers, [1, 2]);
    assert.deepEqual(record.eligibilityShortfalls, [{ tier: 3, reason }]);
    assert.notEqual(record.intendedTier, 3, 'tier 3 is due but has nothing new to search');
    assert.deepEqual(record.targetQuotas, { 1: 6, 2: 4, 3: 0 });
    // Claimed roots (continuation only) come after every unclaimed query; nothing is repeated to fill tier 3.
    const firstClaimed = planned.findIndex(q => claimed.includes(plan0.find(r => r.query === q).rootKey)
      || pages.some(p => p.root_query_key === plan0.find(r => r.query === q).rootKey));
    assert.ok(planned.slice(firstClaimed).every(q => { const k = plan0.find(r => r.query === q).rootKey;
      return claimed.includes(k) || pages.some(p => p.root_query_key === k); }));
  }
});

test('restaurant rotation: the window holds whenever an unclaimed query exists; otherwise a shortfall is recorded', async () => {
  const searched = [], log = [];
  let generation = 0;
  for (let cycle = 0; cycle < 150; cycle++) {
    if (cycle && cycle % 7 === 0) generation++; // adversarial: rotate far more often than production
    const s = coverage.createFacilityResearchCoverage({ tenantId: '10', scope, generation, loadHistory: async () => [],
      tierAllocation: CONFIG, loadAllocation: async () => ({ activations: activation,
        pages: searched.map((x, i) => ({ search_fingerprint: 'f', query_key: x.rootKey, root_query_key: x.rootKey, page_index: 0,
          state: 'fetched', created_at: new Date(Date.parse(ACTIVATED) + (i + 1) * 1000).toISOString() })),
        claimedRoots: searched.map(x => x.rootKey) }) });
    const planned = await s.plan(request, buildQueriesForEvidence);
    const plan = coverage.approvedQueryPlan(scope, generation, buildQueriesForEvidence, CONFIG);
    const record = s.snapshot().tierAllocation;
    const query = planned.find(q => !searched.some(x => x.query === q));
    if (!query) { log.push({ eligible: record.eligibleTiers, tier: null, record }); continue; }
    const row = plan.find(r => r.query === query);
    const tier = CONFIG.tierByCategory[row.category];
    assert.equal(tier, record.intendedTier, 'first unclaimed query is in the intended tier');
    searched.push({ query, rootKey: row.rootKey });
    log.push({ eligible: record.eligibleTiers, tier, record });
  }
  assert.equal(new Set(searched.map(x => x.query)).size, searched.length, 'no query repeated');
  for (const t of [1, 2, 3]) {
    let run = 0;
    for (const step of log) {
      if (!step.eligible.includes(t)) { run = 0; assert.ok(step.record.eligibilityShortfalls.some(x => x.tier === t)); continue; }
      run = step.tier === t ? 0 : run + 1;
      assert.ok(run < CONFIG.window, `tier ${t} unselected for ${run} eligible selections`);
    }
  }
  assert.ok(log.some(step => step.record.eligibilityShortfalls.length), 'the run exercises an ineligible tier');
  for (const step of log) assert.deepEqual(step.record.windowAtRisk, [], 'no position had two tiers due at once');
  for (const step of log.filter(s => s.record.overdueOnReturn.length))
    assert.equal(step.tier, step.record.overdueOnReturn[0]);
});

test('historical attribution uses the version effective at selection, never today\'s map', () => {
  const universe = coverage.versionUniverse(CONFIG, buildQueriesForEvidence);
  const [root, tier] = [...universe][0];
  const page = at => ({ search_fingerprint: 'f', query_key: root, root_query_key: root, page_index: 0, state: 'fetched', created_at: at });
  const windows = activationWindows([...activation, { allocation_version: 'anchor-p1-001-v2-hypothetical', activated_at: '2026-12-01T00:00:00Z' }]);
  const universeOf = v => (v === CONFIG.version ? universe : null);
  assert.deepEqual(attributePage(page('2026-11-01T00:00:00Z'), windows, universeOf).attribution, 'pre_ledger');
  assert.equal(attributePage(page('2026-11-15T00:00:00Z'), windows, universeOf).tier, tier);
  assert.equal(attributePage(page('2026-12-15T00:00:00Z'), windows, universeOf).attribution, 'unknown_version',
    'a later version is not reclassified with v1');
  const ledger = deriveLedger({ version: CONFIG.version, universeOf,
    activations: [...activation, { allocation_version: 'anchor-p1-001-v2-hypothetical', activated_at: '2026-12-01T00:00:00Z' }],
    pages: [page('2026-11-01T00:00:00Z'), { ...page('2026-11-15T00:00:00Z'), search_fingerprint: 'g' }, { ...page('2026-12-15T00:00:00Z'), search_fingerprint: 'h' }] });
  assert.equal(ledger.history.length, 1); assert.equal(ledger.superseded, true);
  assert.deepEqual(ledger.attribution, { pre_ledger: 1, other_version: 1, unknown_version: 0, unattributed: 0 });
});

test('a version universe is pinned: classifier, concept or query-builder drift fails here', () => {
  const universe = coverage.versionUniverse(CONFIG, buildQueriesForEvidence);
  assert.equal(universe.size, 208);
  assert.equal(coverage.universeSha256(universe), CONFIG.universeSha256,
    'changing attribution requires a NEW allocation version, not an edit to this one');
});

test('inert states fall back to the unchanged v1 plan and are recorded', async () => {
  const v1 = await coverage.createFacilityResearchCoverage({ tenantId: '10', scope, loadHistory: async () => [] }).plan(request, buildQueriesForEvidence);
  for (const [state, status, sc] of [
    [{ activations: [] }, 'not_activated', scope],
    [{ activations: [...activation, { allocation_version: 'anchor-p1-001-v2-hypothetical', activated_at: '2026-12-01T00:00:00Z' }], pages: [], claimedRoots: [] }, 'superseded', scope],
    [{ activations: activation, pages: [], claimedRoots: [] }, 'scope_version_mismatch', { ...scope, cities: scope.cities.slice(0, 7) }]]) {
    const s = coverage.createFacilityResearchCoverage({ tenantId: '10', scope: sc, loadHistory: async () => [],
      tierAllocation: CONFIG, loadAllocation: async () => state });
    const r = { ...request, geography: { cities: sc.cities, state: 'NH' } };
    const planned = await s.plan(r, buildQueriesForEvidence);
    assert.deepEqual(s.snapshot().tierAllocation, { version: CONFIG.version, status });
    if (sc === scope) assert.deepEqual(planned, v1);
  }
});

test('before/after allocation fixture is reproducible and meets 44/31/13', async () => {
  const committed = require('./fixtures/scoutTierAllocation.before-after.json');
  assert.equal(JSON.stringify(await fixture()), JSON.stringify(committed));
  assert.deepEqual(committed.after.byTier, { 1: 44, 2: 31, 3: 13 });
  assert.deepEqual(committed.targetQuotas, { 1: 44, 2: 31, 3: 13 });
  assert.equal(new Set(committed.after.selections.map(s => s.query)).size, 88);
  assert.ok(committed.before.byTier[3] > committed.after.byTier[3]);
});

test('allocation receipt reports completed and uncertain selections separately', async () => {
  const plan0 = coverage.approvedQueryPlan(scope, 0, buildQueriesForEvidence, CONFIG);
  const t1 = plan0.filter(r => CONFIG.tierByCategory[r.category] === 1);
  const pages = [
    { search_fingerprint: 'f', query_key: t1[0].rootKey, root_query_key: t1[0].rootKey, page_index: 0, state: 'fetched', created_at: '2026-11-11T00:00:00Z' },
    { search_fingerprint: 'f', query_key: t1[1].rootKey, root_query_key: t1[1].rootKey, page_index: 0, state: 'reserved', created_at: '2026-11-11T00:00:01Z' },
    { search_fingerprint: 'f', query_key: t1[2].rootKey, root_query_key: t1[2].rootKey, page_index: 0, state: 'blocked', created_at: '2026-11-11T00:00:02Z' }];
  const s = coverage.createFacilityResearchCoverage({ tenantId: '10', scope, loadHistory: async () => [],
    tierAllocation: CONFIG, loadAllocation: active(pages, pages.map(p => p.root_query_key)) });
  await s.plan(request, buildQueriesForEvidence);
  const r = s.snapshot().tierAllocation;
  assert.deepEqual(r.counts, { 1: 3, 2: 0, 3: 0 }, 'uncertain counts toward allocation (conservative)');
  assert.deepEqual(r.countsByStatus[1], { completed: 2, uncertain: 1 });
  assert.equal(r.uncertainSelections, 1); assert.equal(r.position, 4);
});

test('no qualification writer or automatic qualification path exists', () => {
  const fs = require('node:fs');
  for (const file of ['services/scoutFacilityResearchCoverage.js', 'services/scoutFacilityTierAllocation.js',
    'scripts/scoutTierAllocationFixture.js', 'config/anchorFacilityTiers.js'])
    assert.doesNotMatch(fs.readFileSync(require.resolve(`../${file}`), 'utf8'), /scout_candidate_commercial_qualifications/);
});
