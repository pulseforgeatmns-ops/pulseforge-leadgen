'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const offer = require('../utils/pulseforgeCashflowOffer');
const { pulseforgeStructuredMission } = require('../utils/pulseforgeCashflowTenant');
const { freezeStructuredMission } = require('../packages/acquisition-mission/StructuredMission');
const { scoutDelegationFromMission } = require('../packages/acquisition-mission/SpecialistInputs');
const taxonomy = require('../utils/canonicalBusinessTaxonomy');
const { missionCompatibleVerticals, resolveReplenishmentVertical, evaluateReplenishmentAdmission, ENRICHABLE_SCOUT_VERTICALS, CANONICAL_ENRICHABLE_SCOUT_VERTICALS, MISSION_SCOPED_FACILITIES_VERTICALS } = require('../utils/replenishmentVertical');
const { sourceScope, missionCandidateReason, _test: { scoutInput, persistDiscoveredCompanies, runEnrichmentBatches } } = require('../services/maxOutboundControlLoop');
const { buildAcquisitionSearchDefinition } = require('../packages/max/scoutAcquisition/SearchDefinition');
const { buildMarketDefinition } = require('../packages/scout/intelligence/MarketUnderstanding');
const { reviseMarketDefinition } = require('../packages/scout/intelligence/MarketDefinition');
const { expandConcepts } = require('../packages/scout/coverage/ConceptLibrary');
const { expandCitiesFromSearchDefinition, buildDiscoveryPlan, executeCoveragePlan } = require('../packages/scout/coverage/DiscoveryCoverageEngine');
const { generateInitialSearchHypotheses, generateFollowUpHypotheses } = require('../packages/scout/investigation/SearchHypothesisEngine');
const { matchesSegment } = require('../packages/max/scoutAcquisition/ExistingIntelligence');
const { buildPerProspectVariants } = require('../packages/max/workspace/PaigeVariantsExecutor');
const TRADES = offer.PULSEFORGE_TARGET_INDUSTRIES;
const CITIES = offer.PULSEFORGE_TARGET_CITIES;
function mission() {
  const plan = pulseforgeStructuredMission();
  return { id: 'cashflow-source', tenantId: '1', clientId: 1, objective: plan.objective,
    structuredMission: freezeStructuredMission(plan, { approvedBy: 'test', approvedAt: '2026-10-13T16:00:00Z' }) };
}
function business(extra = {}) {
  return { name: 'Acme', domain: 'acme.example', website: 'https://acme.example', location: 'Manchester, NH', ...extra };
}
const context = { missionSegment: 'small_business_owner', missionSegments: TRADES, missionCities: CITIES };
const aliasCases = [
  ['plumbing', ['plumbing']], ['Plumber', ['plumbing']], ['plumbing contractor', ['plumbing']],
  ['heating', ['heating']], ['Heating Company', ['heating']], ['furnace repair', ['heating']],
  ['HVAC', ['hvac']], ['air-conditioning', ['hvac']], ['heating & cooling', ['hvac']],
  ['plumbing/heating', ['plumbing', 'heating']], ['Plumbing, Heating and HVAC', TRADES],
  ['plumbing_and_heating', ['plumbing', 'heating']],
];

test('explicit trade aliases share a strict scalar/compound mission contract', () => {
  for (const [alias, expected] of aliasCases) {
    assert.deepEqual(taxonomy.serviceTradeVerticals(alias), expected, alias);
    assert.deepEqual(missionCompatibleVerticals({ missionSegment: alias }), expected, alias);
    assert.deepEqual(missionCompatibleVerticals({ missionSegments: [alias] }), expected, alias);
  }
  assert.deepEqual(missionCompatibleVerticals(context), TRADES);
  for (const unknown of ['unsupported', 'hvac software', 'plumbing/roofing', 'plumbing_contractor_manchester_nh']) {
    assert.deepEqual(missionCompatibleVerticals({ missionSegment: unknown }), [], unknown);
    assert.deepEqual(missionCompatibleVerticals({ missionSegments: ['hvac', unknown] }), [], unknown);
  }
});

test('each approved trade admits from actual business evidence, including Places categories', () => {
  for (const [alias, trades] of aliasCases) {
    const row = business({ industry: alias });
    const admission = evaluateReplenishmentAdmission(row, context);
    assert.equal(admission.admitted, true, alias);
    assert.ok(trades.includes(admission.vertical), alias);
    assert.equal(matchesSegment(row, TRADES), true, alias);
    assert.equal(offer.pulseforgeTargetReason(row), null, alias);
    assert.equal(missionCandidateReason(row, sourceScope({ mission: mission() }), '1'), null, alias);
  }
  for (const [row, expected] of [
    [business({ name: 'Acme Plumbing' }), 'plumbing'],
    [business({ name: 'Acme Heating' }), 'heating'],
    [business({ description: 'Local HVAC contractor' }), 'hvac'],
    [business({ placeTypes: ['plumber'] }), 'plumbing'],
  ]) assert.equal(evaluateReplenishmentAdmission(row, context).vertical, expected);
  // A compound approval never manufactures evidence of a second trade.
  assert.equal(resolveReplenishmentVertical(business({ industry: 'plumbing' }), context), 'plumbing');
  assert.equal(evaluateReplenishmentAdmission(business({ industry: 'plumbing' }), { missionSegment: 'heating' }).admitted, false);
  assert.equal(evaluateReplenishmentAdmission(business({ industry: 'plumbing/heating' }), { missionSegment: 'heating' }).vertical, 'heating');
});

test('generic contractors, adjacent businesses and query-only classifications remain held', () => {
  for (const industry of ['contractor', 'home_services', 'roofing', 'restaurant', 'electrician', 'painting']) {
    assert.equal(evaluateReplenishmentAdmission(business({ industry }), context).admitted, false, industry);
    assert.equal(matchesSegment(business({ industry }), TRADES), false, industry);
    assert.equal(offer.pulseforgeTargetReason(business({ industry })), 'mission_segment_mismatch', industry);
  }
  for (const row of [
    business({ industry: 'plumbing_contractor_manchester_nh', discoveryConcept: 'Plumbing Contractor Manchester NH' }),
    business({ industry: 'HVAC Contractor', discoveryConcept: 'HVAC Contractor' }),
    business({ name: 'Acme Plumbing Supplies' }),
    business({ industry: 'hvac', description: 'Software for HVAC companies' }),
    business({ industry: 'hvac', description: 'Roofing contractor' }),
  ]) assert.equal(evaluateReplenishmentAdmission(row, context).admitted, false, JSON.stringify(row));
  assert.equal(evaluateReplenishmentAdmission(business({ industry: 'hvac', website: '', domain: '' }), context).reason, 'insufficient_business_fit');
});

test('Scout direct and semantic paths retain all trades and exactly the reviewed cities', () => {
  const m = mission();
  const delegation = scoutDelegationFromMission(m);
  const direct = scoutInput({ tenant_id: '1' }, { mission: m }, { deficit: 3 }, { tenantId: '1' });
  const directSearch = buildAcquisitionSearchDefinition(direct);
  const learned = { unrelated: { geography: 'Southern New Hampshire', terminology: 'General Contractor', performance: 99 } };
  const semantic = buildMarketDefinition({ mission: m, delegation, terminologyLearning: learned });
  const implicit = buildMarketDefinition({ mission: m });
  for (const result of [semantic, implicit]) {
    assert.deepEqual(result.segments, TRADES);
    assert.deepEqual(result.searchDefinition.segments, TRADES);
    assert.deepEqual(result.searchDefinition.geography.cities, CITIES);
    assert.deepEqual(result.adjacentMarkets, []);
    assert.deepEqual(result.buyingSignals, []);
    assert.deepEqual(result.terminology, taxonomy.serviceTradeSearchTerms(TRADES));
  }
  assert.deepEqual(directSearch.segments, TRADES);
  assert.deepEqual(directSearch.geography.cities, CITIES);
  assert.deepEqual(expandCitiesFromSearchDefinition(directSearch), CITIES.map(city => `${city} NH`));
  assert.deepEqual(expandConcepts(directSearch), taxonomy.serviceTradeSearchTerms(TRADES));
  const hypotheses = generateInitialSearchHypotheses(semantic, { terminologyLearning: [{ terminology: 'Roofing', performance: 100 }] });
  assert.deepEqual(hypotheses.flatMap(row => row.searchTerms), taxonomy.serviceTradeSearchTerms(TRADES));
  assert.deepEqual(generateFollowUpHypotheses(semantic, hypotheses), []);
  const revised = reviseMarketDefinition(semantic, { dominantTerminology: 'General Contractor', addedTerminology: ['Roofing'], promoteToCustomerType: 'Home Services' });
  assert.deepEqual(revised.terminology, semantic.terminology);
});

test('coverage workloads preserve the actual trade instead of reusing the first segment', async () => {
  const search = buildAcquisitionSearchDefinition(scoutDelegationFromMission(mission()));
  const seen = [];
  const adapter = { id: 'test', sourceType: 'public_business_data', available: () => true,
    discover: async scoped => { seen.push(scoped); return { candidates: [] }; } };
  const plan = buildDiscoveryPlan(search, { adapters: [adapter] });
  assert.equal(plan.cities.length, CITIES.length);
  assert.deepEqual(plan.concepts, taxonomy.serviceTradeSearchTerms(TRADES));
  await executeCoveragePlan(plan, search, [adapter]);
  assert.equal(seen.length, CITIES.length * plan.concepts.length);
  for (const scoped of seen) {
    const expected = taxonomy.serviceTradeVerticals(scoped._coverageWorkload.concept);
    assert.deepEqual(scoped.segments, expected);
    assert.equal(scoped.evidenceRequest.segment, expected[0]);
    assert.match(scoped.geography.label, / NH$/);
  }
});

async function persist(tenantId, scope, companies, existing = []) {
  const writes = [];
  const pool = { query: async (sql, params = []) => {
    if (/SELECT p.id, p.company_id/.test(sql)) return { rows: existing };
    if (/INSERT INTO scout_unenriched/.test(sql)) {
      writes.push({ clientId: params[7], vertical: params[3], location: params[4], notes: params[5] });
      return { rowCount: 1, rows: [{ id: `new-${writes.length}` }] };
    }
    assert.ok(!/INSERT|UPDATE|DELETE/.test(sql), sql);
    return { rows: [] };
  } };
  const store = { tenantId, clientId: Number(tenantId), candidateOwnership: async () => null,
    one: async () => { throw Error('unexpected alternate-contact recovery'); } };
  const result = await persistDiscoveredCompanies(pool, store, { companies,
    scoutContext: { scope, authorizedTenantId: tenantId } });
  return { result, writes };
}

test('tenant 1 persistence, enrichment and preparation agree on each admitted trade', async () => {
  const m = mission();
  const rows = TRADES.map((industry, i) => business({ name: `Acme ${i}`, industry, domain: `acme${i}.example`, website: `https://acme${i}.example` }));
  const { result, writes } = await persist('1', sourceScope({ mission: m }), rows);
  assert.equal(result.inserted, 3);
  assert.deepEqual(writes.map(row => row.vertical), TRADES);
  assert.ok(writes.every(row => row.clientId === 1 && CANONICAL_ENRICHABLE_SCOUT_VERTICALS.includes(row.vertical)));
  let seen;
  await runEnrichmentBatches({ run: async input => { seen = input; return { considered: 3, promoted: 3 }; } }, {}, 3, { tenantId: '1', source: { mission: m } });
  assert.equal(seen.client_id, 1);
  assert.ok(TRADES.every(vertical => seen.verticals.includes(vertical)));
  for (const row of rows) {
    const crm = { ...row, id: 'p1', client_id: 1, company_name: row.name, company_location: row.location, company_website: row.website,
      vertical: row.industry, first_name: 'Alex' };
    const variants = buildPerProspectVariants({ clientId: 1, mission: m,
      max: { rankedTargets: [{ id: crm.id, prospectId: crm.id, name: row.name }] }, crmByProspectId: new Map([[crm.id, crm]]) });
    assert.equal(variants.length, 1);
    assert.equal(offer.validatePulseforgeCashflowCopy(variants[0]).ok, true);
    assert.doesNotMatch(variants[0].body, /I found|losing customers|guarantee/i);
  }
});

test('tenant 1 holds out-of-scope geography and manual first touches before recovery or writes', async () => {
  const scope = sourceScope({ mission: mission() });
  for (const location of ['Manchester, CT', 'Berlin, NH', 'Hooksett, NH', 'Southern New Hampshire', '']) {
    const { result, writes } = await persist('1', scope, [business({ industry: 'plumbing', location })]);
    assert.equal(result.inserted, 0, location);
    assert.equal(writes.length, 0, location);
  }
  for (const manual of offer.PULSEFORGE_MANUAL_FIRST_TOUCHES) {
    for (const existing of [[], [{ id: 'old', company_id: 'old-company', email: 'unverified@example.test', email_verified: false }]]) {
      const { result, writes } = await persist('1', scope, [business({ industry: 'plumbing', domain: manual.domain, website: `https://${manual.domain}` })], existing);
      assert.equal(result.admission.rejected[offer.MANUAL_FIRST_TOUCH_BLOCKER], 1);
      assert.equal(writes.length, 0);
    }
  }
});

test('all-four-tenant matrix preserves reviewed audiences and tenant ownership', async () => {
  const fixtures = [
    ['1', sourceScope({ mission: mission() }), business({ industry: 'plumbing' }), business({ industry: 'painting' })],
    ['10', { segment: 'short_term_rental', cities: ['Manchester'] }, business({ industry: 'property_manager' }), business({ industry: 'plumbing' })],
    ['13', { segment: 'small_business_owner', region: 'United States', cities: [] }, business({ industry: 'painting', location: 'Austin, TX' }), business({ industry: 'heating', location: 'Austin, TX' })],
    ['17', { segment: 'web_design', cities: ['Manchester'] }, null, business({ industry: 'plumbing' })],
  ];
  for (const [tenantId, scope, allowed, denied] of fixtures) {
    const { result, writes } = await persist(tenantId, scope, [allowed, denied].filter(Boolean));
    assert.equal(result.inserted, allowed ? 1 : 0, tenantId);
    assert.ok(writes.every(row => row.clientId === Number(tenantId)), tenantId);
  }
  assert.deepEqual(taxonomy.FOUNDER_LED_SMALL_BUSINESS_VERTICALS,
    ['cleaning', 'landscaping', 'painting', 'hvac', 'electrician', 'home_services', 'restaurant', 'salon', 'fitness', 'auto']);
  assert.deepEqual(missionCompatibleVerticals({ missionSegment: 'small_business_owner' }), taxonomy.FOUNDER_LED_SMALL_BUSINESS_VERTICALS);
  assert.ok(!missionCompatibleVerticals({}).includes('plumbing'));
  assert.ok(!missionCompatibleVerticals({}).includes('heating'));
});

test('unsupported mixed trade scope fails closed in semantic projection', () => {
  const m = structuredClone(mission());
  m.structuredMission.market.eligibleSubsegments = ['plumbing', 'unsupported'];
  const result = buildMarketDefinition({ mission: m });
  assert.equal(result.valid, false);
  assert.deepEqual(result.terminology, []);
  assert.deepEqual(result.segments, []);
});


test('new canonical trades never widen legacy tenant enrichment defaults', async () => {
  const expected = ['property_manager', 'str_manager', 'commercial_office', 'realtor',
    'cleaning', 'landscaping', 'painting', 'hvac', 'electrician', 'home_services', 'restaurant', 'salon', 'fitness', 'auto'];
  assert.deepEqual(ENRICHABLE_SCOUT_VERTICALS, expected);
  assert.deepEqual(require('../scoutUnenrichedEnrichmentAgent')._test.ANCHOR_PRIORITY_VERTICALS, expected);
  for (const tenantId of ['10', '13', '17']) {
    let seen;
    await runEnrichmentBatches({ run: async input => { seen = input; return { considered: 0 }; } }, {}, 1, { tenantId });
    assert.equal(seen.client_id, Number(tenantId));
    assert.deepEqual(seen.verticals, expected, tenantId);
  }
  let calls = 0;
  await assert.rejects(runEnrichmentBatches({ run: async () => { calls++; } }, {}, 1, { tenantId: '1' }),
    { code: offer.SOURCE_REVIEW_BLOCKER });
  assert.equal(calls, 0);
});

test('reviewed mixed eligible lists keep their legacy tenant semantic model', () => {
  for (const tenantId of ['10', '13', '17']) {
    const eligibleSubsegments = ['cleaning', 'painting', 'hvac'];
    const m = { tenantId, structuredMission: { market: { segment: 'small_business_owner', eligibleSubsegments },
      geography: { region: 'United States', cities: [] } } };
    const model = buildMarketDefinition({ mission: m });
    assert.equal(model.valid, true, tenantId);
    assert.equal(model.explicitTradeScope, false, tenantId);
    assert.equal(model.segmentKey, 'small_business_owners', tenantId);
    assert.deepEqual(missionCompatibleVerticals({ missionSegments: eligibleSubsegments }), eligibleSubsegments);
  }
});


test('canonical facilities remain outside legacy missions and unscoped admission', () => {
  assert.deepEqual(MISSION_SCOPED_FACILITIES_VERTICALS,
    ['dealership', 'school', 'campus', 'manufacturer', 'warehouse']);
  for (const vertical of MISSION_SCOPED_FACILITIES_VERTICALS) {
    assert.ok(CANONICAL_ENRICHABLE_SCOUT_VERTICALS.includes(vertical), vertical);
    assert.equal(require('../scripts/reconcileLegacyMaxReplenishment').isNonCanonicalVertical(vertical), false, vertical);
    assert.ok(!ENRICHABLE_SCOUT_VERTICALS.includes(vertical), vertical);
    assert.ok(!missionCompatibleVerticals({}).includes(vertical), vertical);
    assert.ok(!missionCompatibleVerticals({ missionSegment: 'small_business_owner' }).includes(vertical), vertical);
    const row = business({ vertical });
    assert.equal(resolveReplenishmentVertical(row), vertical);
    assert.equal(evaluateReplenishmentAdmission(row).reason, 'segment_mismatch', vertical);
    assert.equal(evaluateReplenishmentAdmission(row, { missionSegment: 'managed_facilities' }).admitted, true, vertical);
  }
});

function facilitiesSource(market) {
  return { payload: { tenantId: '10', structuredMission: {
    market,
    geography: { cities: ['Manchester'] },
  } } };
}

async function selectedEnrichmentVerticals(tenantId, source, scope) {
  let seen;
  await runEnrichmentBatches({ run: async input => { seen = input; return { considered: 0 }; } }, {}, 1,
    { tenantId, source, scope });
  assert.equal(seen.client_id, Number(tenantId));
  assert.equal(seen.tenantId, tenantId);
  return seen.verticals;
}

test('explicit Anchor facilities mission carries admitted categories into enrichment without a catch-all', async () => {
  const source = facilitiesSource({ segment: 'commercial_cleaning',
    eligibleSubsegments: ['property_manager', 'restaurant_foh', 'managed_facilities'] });
  const scope = sourceScope(source);
  const expected = ['property_manager', 'str_manager', 'restaurant', 'commercial_office',
    ...MISSION_SCOPED_FACILITIES_VERTICALS];
  const { result, writes } = await persist('10', scope,
    MISSION_SCOPED_FACILITIES_VERTICALS.map((vertical, i) => business({
      name: `Facility ${i}`, vertical, domain: `facility${i}.example`, website: `https://facility${i}.example`,
    })));
  assert.equal(result.inserted, MISSION_SCOPED_FACILITIES_VERTICALS.length);
  assert.deepEqual(writes.map(row => row.vertical), MISSION_SCOPED_FACILITIES_VERTICALS);
  assert.ok(writes.every(row => row.clientId === 10));
  assert.deepEqual(await selectedEnrichmentVerticals('10', source), expected);
  // Queue duplicate accounting receives the projected scope, while the first
  // enrichment pass receives the source. Both must use the same eligibility.
  assert.deepEqual(await selectedEnrichmentVerticals('10', undefined, scope), expected);
  assert.ok(!expected.includes('cleaning'));
  assert.ok(!expected.includes('plumbing'));
});

test('Anchor facilities selection honors scalar, narrow and unsupported mixed mission scopes', async () => {
  assert.deepEqual(await selectedEnrichmentVerticals('10', facilitiesSource({ segment: 'managed_facilities' })),
    ['property_manager', 'commercial_office', ...MISSION_SCOPED_FACILITIES_VERTICALS]);
  for (const vertical of MISSION_SCOPED_FACILITIES_VERTICALS) {
    assert.deepEqual(await selectedEnrichmentVerticals('10', facilitiesSource({ segment: vertical })), [vertical]);
    assert.deepEqual(await selectedEnrichmentVerticals('10', facilitiesSource({
      segment: 'commercial_cleaning', eligibleSubsegments: [vertical],
    })), [vertical]);
  }
  assert.deepEqual(await selectedEnrichmentVerticals('10', facilitiesSource({
    segment: 'managed_facilities', eligibleSubsegments: ['warehouse', 'unsupported'],
  })), []);
  // Explicit subsegments take precedence over the broader parent segment.
  assert.deepEqual(await selectedEnrichmentVerticals('10', facilitiesSource({
    segment: 'managed_facilities', eligibleSubsegments: ['restaurant_foh'],
  })), ENRICHABLE_SCOUT_VERTICALS);
});

test('facilities opt-in cannot widen other tenants or existing Anchor missions', async () => {
  const managed = facilitiesSource({ segment: 'managed_facilities' });
  for (const tenantId of ['13', '17']) {
    assert.deepEqual(await selectedEnrichmentVerticals(tenantId, managed), ENRICHABLE_SCOUT_VERTICALS);
  }
  for (const source of [undefined, facilitiesSource({}),
    facilitiesSource({ segment: 'short_term_rental' }),
    facilitiesSource({ segment: 'commercial_cleaning', eligibleSubsegments: ['property_manager', 'restaurant_foh'] })]) {
    assert.deepEqual(await selectedEnrichmentVerticals('10', source), ENRICHABLE_SCOUT_VERTICALS);
  }
});

test('Anchor duplicate queue accounting uses the same facilities selection as enrichment', async () => {
  const source = facilitiesSource({ eligibleSubsegments: ['restaurant_foh', 'managed_facilities'] });
  const expected = await selectedEnrichmentVerticals('10', source);
  let observed = false;
  const pool = { query: async (sql, params) => {
    if (/FROM prospects p/.test(sql)) return { rows: [] };
    if (/INSERT INTO scout_unenriched/.test(sql)) return { rowCount: 0, rows: [] };
    assert.match(sql, /WHERE u.client_id=\$1/);
    assert.equal(params[0], 10);
    assert.deepEqual(params[4], expected);
    observed = true;
    return { rows: [{ id: 'existing-facility', client_id: 10, eligibility_bucket: 'eligible_now' }] };
  } };
  const result = await persistDiscoveredCompanies(pool, { tenantId: '10', clientId: 10,
    candidateOwnership: async () => null }, { tenantId: '10', companies: [business({ vertical: 'warehouse' })],
    scoutContext: { source, scope: sourceScope(source) } });
  assert.equal(observed, true);
  assert.equal(result.admission.alreadyQueued, 1);
  assert.equal(result.admission.queueMatches.firstFailureCounts.eligible_now, 1);
});
