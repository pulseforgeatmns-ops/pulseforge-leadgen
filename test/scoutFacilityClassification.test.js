'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const facility = require('../utils/anchorFacilityResearch');
const { evaluateReplenishmentAdmission } = require('../utils/replenishmentVertical');
const { resolveCandidateUniverse } = require('../packages/max/scoutAcquisition/EntityResolution');
const { mergeIdentities } = require('../packages/scout/coverage/HypothesisDrivenDiscoveryEngine');
const context = { tenantId: '10', facilityResearchRecovery: true,
  missionSegments: ['managed_facilities', 'restaurant_foh'], missionCities: ['Manchester', 'Bedford'] };
const candidate = extra => ({ tenantId: '10', name: 'Observed Business', website: 'https://example.test',
  address: '101 Fixture Way, Manchester, NH 03101, USA', ...extra });
const decision = extra => facility.evaluateFacilityResearch(candidate(extra), context);

test('structured categories recognize suitable facilities without website investigation', () => {
  for (const [type, vertical] of [['school', 'school'], ['primary_school', 'school'], ['secondary_school', 'school'],
    ['university', 'campus'], ['college', 'campus'], ['car_dealer', 'dealership'], ['restaurant', 'restaurant'], ['cafe', 'restaurant']]) {
    const result = decision({ placeTypes: ['point_of_interest', type] });
    assert.equal(result.vertical, vertical); assert.equal(result.admitted, true, type);
    assert.equal(result.facilityDecision.procurement.independentPurchasingAuthority, false);
    assert.equal(result.facilityDecision.admission.downstreamAuthorized, false);
  }
  assert.equal(decision({ name: 'Granite Manufacturing' }).vertical, 'manufacturer');
  assert.equal(decision({ name: 'Granite Logistics Facility' }).vertical, 'warehouse');
  assert.equal(decision({ name: 'Granite Cafe' }).admitted, true);
});

test('category recognition never overrides suitability exclusions', () => {
  for (const name of ['Granite Driving School', 'Academy of Health and Beauty', 'Granite BJJ and MMA Academy',
    'Synthetic Music School', "Synthetic Gymnastics Academy", 'Karate Academy']) {
    const result = decision({ name, placeTypes: ['school'] });
    assert.equal(result.facilityDecision.recognition.status, 'recognized');
    assert.equal(result.admitted, false, name); assert.equal(result.facilityDecision.suitability.status, 'unsuitable');
  }
  for (const extra of [{ name: 'Granite Self Storage', placeTypes: ['self_storage_facility'] },
    { name: 'Manufacturing Supplies Retail Store' }, { name: 'Manchester Food Truck', placeTypes: ['restaurant'] }])
    assert.equal(decision(extra).admitted, false);
  assert.equal(decision({ name: 'Central High School', placeTypes: ['school'], description: 'A high school with a music program.' }).admitted, true);
});

test('ordinal street names establish premises without weakening geography or other decisions', () => {
  for (const ordinal of ['1st', '2nd', '3rd', '4th', '22ND']) {
    const result = decision({ name: 'Synthetic Numbered Street School', placeTypes: ['primary_school'],
      address: `101 ${ordinal} Fixture St, Manchester, NH 03101, USA` });
    assert.equal(result.admitted, true, ordinal);
    assert.equal(result.facilityDecision.procurement.independentPurchasingAuthority, false);
    assert.equal(result.facilityDecision.admission.downstreamAuthorized, false);
  }
  for (const address of ['Manchester, NH 03101, USA', '101 234, Manchester, NH 03101, USA', 'PO Box 101, Manchester, NH 03101, USA']) {
    const result = decision({ placeTypes: ['primary_school'], address });
    assert.equal(result.admitted, false, address);
    assert.ok(result.facilityDecision.suitability.missingEvidence.includes('physical_service_premises'));
  }
  assert.equal(decision({ placeTypes: ['primary_school'], address: '101 3rd Fixture St, Manchester, England, UK' }).admitted, false);
  assert.equal(decision({ name: 'Synthetic Beauty School', placeTypes: ['school'],
    address: '101 3rd Fixture St, Manchester, NH 03101, USA' }).admitted, false);
});

test('ambiguous categories, operation and premises evidence remain explicit', () => {
  for (const extra of [{ name: 'Granite Works' }, { name: 'Granite Academy' },
    { name: 'University Administration', placeTypes: ['university'] },
    { name: 'Synthetic University Graduate School', placeTypes: ['university'] },
    { name: 'Online Autos', placeTypes: ['car_dealer'], address: 'Manchester, NH' }]) {
    const result = decision(extra); assert.equal(result.admitted, false); assert.equal(result.deferred, true);
  }
  const verified = { verified: true, source: 'synthetic-observed-lease', distinctServicePremises: true };
  assert.equal(decision({ name: 'University Administration', placeTypes: ['university'], verifiedBusinessAttributes: verified }).admitted, true);
  assert.equal(decision({ name: 'ABC', placeTypes: ['school'], businessStatus: 'CLOSED_PERMANENTLY' }).admitted, false);
  assert.equal(decision({ name: 'ABC', discoveryQuery: 'manufacturing facility Manchester NH', industry: 'manufacturer' }).vertical, null);
});

test('Manchester England and other states cannot match NH; ambiguous/conflicting geography defers', () => {
  const foreign = decision({ name: 'Synthetic Academy', placeTypes: ['school'],
    address: '1 Synthetic Road, Manchester, England, UK', city: 'Manchester' });
  assert.equal(foreign.reason, 'outside_geography');
  assert.equal(foreign.facilityDecision.geography.status, 'ineligible');
  for (const address of ['101 Fixture Way, Manchester, CT 06040, USA', '101 Fixture Way, Nashua, NH 03060, USA'])
    assert.equal(decision({ placeTypes: ['school'], address }).reason, 'outside_geography');
  for (const address of ['Manchester', 'Manchester, NH', '101 Fixture Way, Manchester'])
    assert.equal(decision({ placeTypes: ['school'], address }).facilityDecision.geography.status, 'deferred');
  const conflict = decision({ placeTypes: ['school'], addressComponents: [
    { short_name: 'GB', types: ['country'] }, { short_name: 'England', types: ['administrative_area_level_1'] }] });
  assert.equal(conflict.admitted, false);
  const structured = decision({ placeTypes: ['school'], address: '101 Fixture Way', addressComponents: [
    { short_name: 'US', types: ['country'] }, { short_name: 'NH', types: ['administrative_area_level_1'] },
    { short_name: 'Manchester', types: ['locality'] }] });
  assert.equal(structured.admitted, true);
});

test('mission membership and identity are independently required', () => {
  assert.equal(facility.evaluateFacilityResearch(candidate({ placeTypes: ['school'] }), { ...context, missionSegments: ['restaurant_foh'] }).reason, 'segment_mismatch');
  assert.equal(decision({ placeTypes: ['school'], website: '' }).admitted, false);
  assert.equal(facility.evaluateFacilityResearch(candidate({ placeTypes: ['school'] }), { ...context, missionCities: [] }).admitted, false);
});

test('default-off and non-Anchor/ordinary admission behavior are preserved', () => {
  assert.equal(facility.enabled('10', true, {}), false);
  assert.equal(facility.enabled('10', false, { [facility.FLAG]: 'true' }), false);
  assert.equal(facility.enabled('13', true, { [facility.FLAG]: 'true' }), false);
  const publicSchool = candidate({ name: 'Synthetic Public Elementary School', placeTypes: ['primary_school', 'school'], location: 'Manchester, NH' });
  assert.equal(evaluateReplenishmentAdmission(publicSchool, { ...context, facilityResearchRecovery: false }).reason, 'unclassifiable_vertical');
  assert.equal(evaluateReplenishmentAdmission(publicSchool, context).admitted, true);
  assert.equal(evaluateReplenishmentAdmission(publicSchool, { ...context, tenantId: '13' }).reason, 'unclassifiable_vertical');
});

test('district/campus relationships stay unverified and co-located organizations remain distinct', () => {
  const rows = [candidate({ name: 'Synthetic Manchester School District', placeId: 'district', website: 'syntheticdistrict.example' }),
    candidate({ name: 'Public Elementary School', placeId: 'elementary', website: 'syntheticdistrict.example', address: '102 Fixture Way, Manchester, NH 03101, USA' }),
    candidate({ name: 'Synthetic University Campus', placeId: 'unh1', website: 'campus.syntheticuniversity.example', address: '200 Fixture Way, Manchester, NH 03101, USA' }),
    candidate({ name: 'Synthetic University Graduate School', placeId: 'unh2', website: 'graduate.syntheticuniversity.example', address: '200 Fixture Way, Manchester, NH 03101, USA' }),
    candidate({ name: 'Synthetic Community College', placeId: 'mcc', website: 'syntheticcollege.example', address: '300 Fixture Way, Manchester, NH 03102, USA' }),
    candidate({ name: 'Synthetic Technology Academy', placeId: 'spark', website: 'syntheticacademy.example', address: '300 Fixture Way, Manchester, NH 03102, USA' })]
    .map(row => ({ ...row, facilityResearchRecovery: true }));
  assert.equal(resolveCandidateUniverse([], rows).companies.length, rows.length);
  assert.equal(mergeIdentities(rows).length, rows.length);
  for (const group of facility.potentialProcurementGroups(rows)) {
    assert.equal(group.verified, false); assert.equal(group.mergeFacilities, false);
    assert.equal(group.salesOpportunityGroupingAuthorized, false);
  }
  assert.equal(facility.procurementDecision(rows[1]).independentPurchasingAuthority, false);
});

test('previously deferred research tickets need separate review even when now recognizable', () => {
  const { claimedCompanies } = require('../services/scoutCandidateTicketDispatch');
  const claim = { reason: 'business_evidence_not_authorized', members: [candidate({
    name: 'Synthetic Primary School', placeId: 'synthetic-held', source: 'google_places', placeTypes: ['primary_school'] })] };
  const held = claimedCompanies([claim], '10', [], { holdExistingResearchDeferrals: true });
  assert.equal(held[0].researchDeferredReviewRequired, true);
  assert.equal(claimedCompanies([claim], '10')[0].researchDeferredReviewRequired, undefined);
  assert.equal(claimedCompanies([claim], '13', [], { holdExistingResearchDeferrals: true })[0].researchDeferredReviewRequired, undefined);
});
