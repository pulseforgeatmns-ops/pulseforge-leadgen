'use strict';

require('./helpers/denyExternalProviders.cjs');
const test = require('node:test');
const assert = require('node:assert/strict');
const { buildBoundedScoutContext, criteriaFingerprint } = require('../packages/max/scoutAcquisition/BoundedContext');
const { buildAcquisitionSearchDefinition } = require('../packages/max/scoutAcquisition/SearchDefinition');
const { buildDiscoveryPlan, executeCoveragePlan } = require('../packages/scout/coverage/DiscoveryCoverageEngine');
const { buildQueriesForEvidence } = require('../packages/capabilities/discovery/providers/PlacesProvider');
const { resolveMarketHypothesisBySegmentKey, SEARCH_SOURCES } = require('../packages/scout/hypothesis/MarketHypothesisRegistry');

function boundedTarget(segments = ['property_manager'], cities = ['Hooksett', 'Auburn']) {
  return {
    authorizedTenantId: '10',
    businessContext: { commercialCapability: 'commercial_cleaning', serviceGeography: 'Greater Manchester',
      preferredSegments: segments, acquisitionDirection: 'Use the approved mission only.' },
    targetContext: { geography: 'Greater Manchester', geographyScope: 'local', cities, missionBound: true, segments },
    operatorDirection: 'Maintain the approved inventory. Do not contact prospects.',
  };
}

test('the normal bounded Scout delegation preserves the explicit mission city scope and order', () => {
  const input = boundedTarget();
  const bounded = buildBoundedScoutContext(input);
  const definition = buildAcquisitionSearchDefinition({ ...bounded, authorizedTenantId: '10' });
  assert.equal(bounded.targetContext.missionBound, true);
  assert.equal(bounded.targetContext.geographyScope, 'local');
  assert.deepEqual(bounded.targetContext.cities, ['Hooksett', 'Auburn']);
  assert.equal(definition.missionBound, true);
  assert.equal(definition.geography.explicitCities, true);
  const plan = buildDiscoveryPlan(definition);
  assert.deepEqual(plan.cities, ['Hooksett NH', 'Auburn NH']);
  input.targetContext.cities.push('Boston');
  assert.deepEqual(bounded.targetContext.cities, ['Hooksett', 'Auburn']);
});

test('different explicit mission city scopes cannot reuse the same acquisition criteria identity', () => {
  const a = buildBoundedScoutContext(boundedTarget(['property_manager'], ['Hooksett']));
  const b = buildBoundedScoutContext(boundedTarget(['property_manager'], ['Bedford']));
  assert.notEqual(criteriaFingerprint(a.targetContext, a.businessContext), criteriaFingerprint(b.targetContext, b.businessContext));
  const reordered = buildBoundedScoutContext(boundedTarget(['property_manager'], ['Auburn', 'Hooksett']));
  const original = buildBoundedScoutContext(boundedTarget());
  assert.equal(criteriaFingerprint(reordered.targetContext), criteriaFingerprint(original.targetContext));
  assert.equal(criteriaFingerprint({ geography: 'Manchester, NH', segments: ['property_manager'] }), 'manchester nh|property_manager|');
});

test('an explicitly authorized managed-facilities scope reaches its existing concrete provider categories', async () => {
  const bounded = buildBoundedScoutContext(boundedTarget(['managed_facilities'], ['Hooksett']));
  const definition = buildAcquisitionSearchDefinition({ ...bounded, authorizedTenantId: '10' });
  const plan = buildDiscoveryPlan(definition, { enabledSources: ['public_business_data'] });
  const queries = [];
  await executeCoveragePlan(plan, definition, [{ id: 'public_business_places', sourceType: 'public_business_data',
    available: () => true, discover: async scoped => {
      const request = scoped.evidenceRequest;
      queries.push(...buildQueriesForEvidence({ ...request, cities: request.geography.cities, state: request.geography.state }));
      return { candidates: [], errors: [], available: true };
    } }]);
  const existing = resolveMarketHypothesisBySegmentKey('managed_facilities').searchStrategies
    .filter(strategy => strategy.source === SEARCH_SOURCES.GOOGLE_PLACES)
    .flatMap(strategy => strategy.queryTemplates.map(template => template.replace('{city}', 'Hooksett').replace('{state}', 'NH')));
  assert.deepEqual(queries, existing);
  assert.ok(queries.some(query => /private school/i.test(query)));
  assert.ok(queries.some(query => /manufacturing facility/i.test(query)));
  assert.ok(queries.some(query => /commercial property management/i.test(query)));
  assert.ok(queries.every(query => !/real estate|realtor|managed facilities/i.test(query)));
});

test('the old approved audience does not silently acquire an institutional or industrial scope', () => {
  const bounded = buildBoundedScoutContext(boundedTarget(['property_manager', 'str_manager', 'realtor', 'commercial_office']));
  const plan = buildDiscoveryPlan(buildAcquisitionSearchDefinition({ ...bounded, authorizedTenantId: '10' }));
  assert.ok(plan.concepts.some(concept => /real estate/i.test(concept)));
  assert.ok(plan.concepts.every(concept => !/school|academy|college|manufactur|warehouse/i.test(concept)));
});
