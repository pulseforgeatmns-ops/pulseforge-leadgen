'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { toDiscoveredCompany } = require('../packages/max/scoutAcquisition/DiscoveryAdapters');
const { resolveCandidateUniverse } = require('../packages/max/scoutAcquisition/EntityResolution');
const { evaluateReplenishmentAdmission } = require('../utils/replenishmentVertical');
const { _test: { persistDiscoveredCompanies } } = require('../services/maxOutboundControlLoop');

const context = { missionSegments: ['restaurant_foh'], allowedCities: ['manchester'] };
function discovered(overrides = {}) {
  return toDiscoveredCompany({
    name: 'Elm Street', website: 'https://elm.example', location: 'Manchester, NH',
    industry: 'Restaurant Manchester NH', placeTypes: ['restaurant', 'food', 'establishment'],
    discoveryQuery: 'Restaurant Manchester NH', discoveryConcept: 'Restaurant',
    ...overrides,
  }, { tenantId: '10' }, 'google_places');
}
function resolve(candidate) {
  return resolveCandidateUniverse([], [candidate]).companies[0];
}

test('new-company resolution preserves observed Places type through restaurant admission', () => {
  const candidate = discovered();
  assert.equal(evaluateReplenishmentAdmission(candidate, context).admitted, true);
  const resolved = resolve(candidate);
  assert.deepEqual(resolved.placeTypes, candidate.placeTypes);
  assert.notEqual(resolved.placeTypes, candidate.placeTypes);
  assert.equal(evaluateReplenishmentAdmission(resolved, context).vertical, 'restaurant');
});

test('new-company resolution preserves business descriptions and contradictory evidence', () => {
  const restaurant = discovered({ placeTypes: [], description: 'Independent restaurant' });
  assert.equal(evaluateReplenishmentAdmission(resolve(restaurant), context).admitted, true);
  const industrial = discovered({ description: 'Food manufacturing plant' });
  assert.equal(evaluateReplenishmentAdmission(resolve(industrial), context).reason, 'contradictory_business_type');
});

test('preserved query provenance never becomes evidence of business type', () => {
  const candidate = discovered({ industry: 'Restaurant', placeTypes: [], discoveryQuery: 'Restaurant' });
  assert.equal(evaluateReplenishmentAdmission(candidate, context).reason, 'unclassifiable_vertical');
  const resolved = resolve(candidate);
  assert.equal(resolved.discoveryConcept, 'Restaurant');
  assert.equal(evaluateReplenishmentAdmission(resolved, context).reason, 'unclassifiable_vertical');
});

test('preserved observations do not widen geography or mission scope', () => {
  assert.equal(evaluateReplenishmentAdmission(resolve(discovered({ location: 'Boston, MA' })), context).reason, 'outside_geography');
  assert.equal(evaluateReplenishmentAdmission(resolve(discovered()), {
    ...context, missionSegments: ['property_manager'],
  }).reason, 'segment_mismatch');
});

test('resolved observed restaurant reaches tenant-scoped admission with no provider calls', async () => {
  const inserts = [];
  const pool = { query: async (sql, params) => {
    if (/INSERT INTO scout_unenriched/.test(sql)) {
      inserts.push(params);
      return { rows: [{ id: 1 }], rowCount: 1 };
    }
    assert.match(sql, /FROM prospects p/);
    assert.equal(params[3], 10);
    return { rows: [], rowCount: 0 };
  } };
  const store = { tenantId: '10', clientId: 10, candidateOwnership: async () => null };
  const result = await persistDiscoveredCompanies(pool, store, {
    companies: [resolve(discovered())], tenantId: '10',
    scoutContext: { scope: { eligibleSubsegments: ['restaurant_foh'] }, allowedCities: ['manchester'] },
  });
  assert.equal(result.admission.fit, 1);
  assert.equal(result.admission.admittedToEnrichment, 1);
  assert.equal(inserts[0][3], 'restaurant');
  assert.equal(inserts[0][7], 10);
  assert.match(inserts[0][5], /Restaurant Manchester NH/);
});

test('resolved evidence does not bypass existing ownership rejection', async () => {
  const pool = { query: async sql => {
    assert.doesNotMatch(sql, /INSERT INTO/);
    return { rows: [], rowCount: 0 };
  } };
  const result = await persistDiscoveredCompanies(pool, {
    tenantId: '10', clientId: 10, candidateOwnership: async () => 'already_owned',
  }, {
    companies: [resolve(discovered())], tenantId: '10',
    scoutContext: { scope: { eligibleSubsegments: ['restaurant_foh'] }, allowedCities: ['manchester'] },
  });
  assert.equal(result.admission.fit, 0);
  assert.equal(result.admission.rejected.owned_elsewhere, 1);
});


test('deduplication keeps observed types when an earlier hit lacks them', () => {
  const result = resolveCandidateUniverse([], [discovered({ placeTypes: [] }), discovered()]);
  assert.equal(result.companies.length, 1);
  assert.deepEqual(result.companies[0].placeTypes, ['restaurant', 'food', 'establishment']);
  assert.equal(evaluateReplenishmentAdmission(result.companies[0], context).admitted, true);
});

for (const field of ['description', 'snippet', 'businessType']) {
  test(`duplicate ${field} observations retain contrary evidence in both orders`, () => {
    const restaurant = discovered({ [field]: 'Independent restaurant' });
    const industrial = discovered({ [field]: 'Food manufacturing plant' });
    for (const candidates of [[restaurant, industrial], [industrial, restaurant]]) {
      const resolved = resolveCandidateUniverse([], candidates).companies[0];
      assert.equal(evaluateReplenishmentAdmission(resolved, context).reason, 'contradictory_business_type');
    }
  });
}

test('duplicate query-only hits cannot cross-wire industry and query provenance in either order', () => {
  const first = discovered({ industry: null, placeTypes: [], discoveryQuery: 'Catering', discoveryConcept: 'Catering' });
  const second = discovered({ industry: 'Restaurant', placeTypes: [], discoveryQuery: 'Restaurant', discoveryConcept: 'Restaurant' });
  for (const candidate of [first, second]) {
    assert.equal(evaluateReplenishmentAdmission(candidate, context).reason, 'unclassifiable_vertical');
  }
  for (const candidates of [[first, second], [second, first]]) {
    const resolved = resolveCandidateUniverse([], candidates).companies[0];
    assert.equal(evaluateReplenishmentAdmission(resolved, context).reason, 'unclassifiable_vertical');
  }
});

test('classification observations retain per-hit provenance without mutating inputs or growing on replay', () => {
  const first = discovered({ description: 'Independent restaurant', discoveryConcept: 'Cafe', discoveryQuery: 'Cafe Manchester NH' });
  const second = discovered({ description: 'Food manufacturing plant', discoveryConcept: 'Restaurant', discoveryQuery: 'Restaurant Manchester NH' });
  const original = JSON.stringify([first, second]);
  const resolved = resolveCandidateUniverse([], [first, second]).companies[0];
  assert.equal(JSON.stringify([first, second]), original);
  assert.deepEqual(resolved.classificationObservations.map(row => [row.description, row.discoveryConcept, row.discoveryQuery]), [
    ['Independent restaurant', 'Cafe', 'Cafe Manchester NH'],
    ['Food manufacturing plant', 'Restaurant', 'Restaurant Manchester NH'],
  ]);
  const replayed = resolveCandidateUniverse([resolved], [second, first]).companies[0];
  assert.equal(replayed.classificationObservations.length, 2);
  assert.equal(evaluateReplenishmentAdmission(replayed, context).reason, 'contradictory_business_type');
});

test('a later first-party business description remains usable after identity resolution', () => {
  const resolved = resolve(discovered({ placeTypes: [] }));
  assert.equal(evaluateReplenishmentAdmission(resolved, context).reason, 'unclassifiable_vertical');
  assert.equal(evaluateReplenishmentAdmission({ ...resolved, description: 'Independent restaurant' }, context).admitted, true);
  assert.equal(evaluateReplenishmentAdmission({ ...resolved, description: 'Food manufacturing plant' }, context).reason, 'contradictory_business_type');
});


test('missing per-hit provenance retains legitimate caller search context', () => {
  const candidate = {
    name: 'Elm Street', website: 'https://elm.example', location: 'Manchester, NH', industry: 'Restaurant',
  };
  const searchContext = { ...context, discoveryConcept: 'Restaurant', discoveryQuery: 'Restaurant' };
  assert.equal(evaluateReplenishmentAdmission(candidate, searchContext).reason, 'unclassifiable_vertical');
  assert.equal(evaluateReplenishmentAdmission(resolve(candidate), searchContext).reason, 'unclassifiable_vertical');
});
