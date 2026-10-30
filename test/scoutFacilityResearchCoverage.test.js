'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const { approvedQueryPlan, rankQueries, createFacilityResearchCoverage } = require('../services/scoutFacilityResearchCoverage');
const { buildQueriesForEvidence } = require('../packages/capabilities/discovery/providers/PlacesProvider');
const scope = { eligibleSubsegments: ['managed_facilities', 'restaurant_foh'],
  cities: ['Manchester', 'Bedford', 'Londonderry', 'Hooksett', 'Auburn', 'Goffstown', 'Nashua', 'Merrimack'] };
const plan = () => approvedQueryPlan(scope, 0, buildQueriesForEvidence);

test('eight planned cycles span all available approved categories and towns across restarts', () => {
  const history = [], selected = [];
  for (let n = 0; n < 8; n++) {
    const next = rankQueries(plan(), history).ordered[0]; selected.push(next);
    history.push({ query_text: next.query, attempts: 1 });
  }
  assert.equal(new Set(selected.map(x => x.category)).size, 8);
  assert.equal(new Set(selected.map(x => x.city)).size, 8);
  assert.equal(new Set(selected.map(x => x.query)).size, 8);
  assert.ok(selected.some(x => x.category === 'restaurant'));
});

test('saturated school synonyms follow under-covered categories and previous pages stay counted', () => {
  const queries = plan(), privateSchool = queries.find(x => x.concept === 'private school');
  const ranked = rankQueries(queries, [{ query_text: privateSchool.query, attempts: 3 }]);
  assert.notEqual(ranked.ordered[0].category, 'school');
  assert.equal(ranked.counts.categories.school, 3);
  assert.equal(ranked.counts.towns.manchester, 3);
  assert.equal(ranked.counts.roots[privateSchool.rootKey], 3);
  assert.ok(ranked.ordered.findIndex(x => x.concept === 'academy') > 0);
});

test('planning only reorders queries from the approved segments/cities and records skipped work', async () => {
  let loads = 0;
  const scheduler = createFacilityResearchCoverage({ tenantId: '10', scope, loadHistory: async () => { loads++; return []; } });
  const request = { tenantId: '10', segment: 'managed_facilities', evidenceType: 'identity', geography: { cities: scope.cities, state: 'NH' } };
  const planned = await scheduler.plan(request, buildQueriesForEvidence);
  const approved = new Set(plan().map(x => x.query));
  assert.equal(planned.length, approved.size); assert.ok(planned.every(q => approved.has(q)));
  scheduler.note(planned[0], { handled: true, continuationStatus: 'tenant_query_previously_searched' });
  scheduler.note(planned[1], { singlePage: true, pageIndex: 0 });
  scheduler.note(planned[2], { handled: true, continuationStatus: 'cycle_page_budget_reached' });
  assert.deepEqual(await scheduler.plan(request, buildQueriesForEvidence), []);
  assert.equal(loads, 1);
  assert.deepEqual(scheduler.snapshot().decisions.map(x => x.status), ['skipped', 'selected', 'skipped']);
  assert.equal(scheduler.snapshot().maxNewPages, 1);
});

test('unsupported tenants, categories, geography and entity work fail closed', async () => {
  assert.throws(() => createFacilityResearchCoverage({ tenantId: '13', scope, loadHistory: async () => [] }));
  assert.throws(() => approvedQueryPlan({ ...scope, eligibleSubsegments: ['realtor'] }, 0, buildQueriesForEvidence));
  for (const change of [{ tenantId: '13' }, { segment: 'realtor' }, { scope: 'entity' }, { evidenceType: 'owner' },
    { geography: { cities: ['Boston'], state: 'MA' } }]) {
    const s = createFacilityResearchCoverage({ tenantId: '10', scope, loadHistory: async () => [] });
    await assert.rejects(s.plan({ tenantId: '10', segment: 'managed_facilities', evidenceType: 'identity', geography: { cities: scope.cities, state: 'NH' }, ...change }, buildQueriesForEvidence));
  }
});
