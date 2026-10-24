'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  CONTROL_ARM,
  PORTAL_EXPERIMENT_OPT_IN,
  PORTAL_ARM,
  managedFacilitiesScore,
  isManagedFacilitiesCandidate,
  experimentArm,
} = require('../utils/anchorManagedFacilities');
const { buildAnchorLifecycleVariant } = require('../utils/anchorLifecycleEmail');
const { freezeStructuredMission } = require('../packages/acquisition-mission/StructuredMission');
const { evaluateReplenishmentAdmission } = require('../utils/replenishmentVertical');
const { resolveMarketHypothesisBySegmentKey, expandSearchStrategies } =
  require('../packages/scout/hypothesis/MarketHypothesisRegistry');
const { missionCandidateReason } = require('../services/maxOutboundControlLoop');
const { successorMission } = require('../scripts/activateAnchorInventoryCapacityMission');

function managedProspect(id) {
  return {
    id,
    prospect_id: id,
    client_id: 10,
    company_id: 'co-' + id,
    company_name: 'Granite Auto Group',
    vertical: 'dealership',
    title: 'Facilities Manager',
    location_count: 3,
    first_name: 'Sam',
    company_fields: {
      name: 'Granite Auto Group',
      location: 'Manchester, NH',
    },
  };
}

function optedInMission(overrides = {}) {
  return { id: 'anchor-facilities-copy-experiment', tenantId: '10', clientId: 10,
    structuredMission: freezeStructuredMission({
      missionType: 'acquisition', objective: 'Find recurring managed-facilities cleaning buyers',
      successMetric: { type: 'customers', target: 1 }, priority: 3,
      market: { segment: 'managed_facilities', eligibleSubsegments: ['managed_facilities'] },
      geography: { region: 'Manchester', cities: ['Manchester'] },
      constraints: [PORTAL_EXPERIMENT_OPT_IN], ...overrides,
    }, { approvedBy: 'test-operator', approvedAt: '2026-10-20T19:00:00Z' }) };
}

test('managed facilities scoring promotes operationally complex accounts, not tiny vertical-only matches', () => {
  const managed = managedProspect('managed-1');
  assert.equal(managedFacilitiesScore(managed, managed.company_fields), 8);
  assert.equal(isManagedFacilitiesCandidate(managed, managed.company_fields), true);

  const tiny = {
    vertical: 'dealership',
    tiny_facility: true,
    title: 'Owner',
  };
  assert.ok(managedFacilitiesScore(tiny) < 7);
  assert.equal(isManagedFacilitiesCandidate(tiny), false);
});

test('managed facilities experiment arm is deterministic and exercises both control and portal arms', () => {
  const ids = Array.from({ length: 64 }, (_, i) => 'prospect-' + i);
  const arms = new Set(ids.map(experimentArm));
  assert.deepEqual([...arms].sort(), [CONTROL_ARM, PORTAL_ARM].sort());
  for (const id of ids) assert.equal(experimentArm(id), experimentArm(id));
});

test('portal-led first touch keeps CTA/cadence copy shape and adds no image markup', () => {
  const ids = Array.from({ length: 128 }, (_, i) => 'managed-copy-' + i);
  const portalId = ids.find(id => experimentArm(id) === PORTAL_ARM);
  const controlId = ids.find(id => experimentArm(id) === CONTROL_ARM);
  assert.ok(portalId);
  assert.ok(controlId);

  const portal = buildAnchorLifecycleVariant({
    candidate: { candidateId: portalId, name: 'Granite Auto Group' },
    crmRecord: managedProspect(portalId),
    mission: optedInMission(),
  });
  const control = buildAnchorLifecycleVariant({
    candidate: { candidateId: controlId, name: 'Granite Auto Group' },
    crmRecord: managedProspect(controlId),
    mission: optedInMission(),
  });

  assert.equal(portal.cohort, 'managed_facilities');
  assert.equal(portal.experimentArm, PORTAL_ARM);
  assert.equal(control.cohort, 'managed_facilities');
  assert.equal(control.experimentArm, CONTROL_ARM);
  assert.match(portal.body, /simple portal to see service history, upcoming visits, reports, photos/i);
  assert.doesNotMatch(control.body, /simple portal to see service history/i);
  assert.equal(portal.cta, control.cta);
  assert.doesNotMatch(portal.body, /<img|data:image|\.png|\.jpg|\.jpeg/i);
  assert.doesNotMatch(portal.body, /[—–]/);
});

test('Scout recognizes managed facilities mission verticals and keeps geography binding', () => {
  const context = {
    missionSegments: ['managed_facilities'],
    allowedCities: ['manchester'],
  };
  const dealership = evaluateReplenishmentAdmission({
    name: 'Granite Auto Dealership',
    domain: 'granite-auto.example',
    location: 'Manchester, NH',
    vertical: 'dealership',
  }, context);
  assert.equal(dealership.admitted, true);
  assert.equal(dealership.vertical, 'dealership');

  const school = evaluateReplenishmentAdmission({
    name: 'Queen City Academy',
    domain: 'qca.example',
    location: 'Manchester, NH',
    description: 'Independent private school and academy',
  }, context);
  assert.equal(school.admitted, true);
  assert.equal(school.vertical, 'school');

  assert.equal(evaluateReplenishmentAdmission({
    name: 'Boston Academy',
    domain: 'boston-academy.example',
    location: 'Boston, MA',
    vertical: 'school',
  }, context).reason, 'outside_geography');

  const scope = { segment: 'managed_facilities', cities: ['manchester'] };
  assert.equal(missionCandidateReason({
    client_id: 10,
    service_area_match: 'Manchester',
    company_location: 'Manchester, NH',
    vertical: 'warehouse',
  }, scope, '10'), null);
});

test('managed facilities search hypothesis deliberately hunts target account classes', () => {
  const hypothesis = resolveMarketHypothesisBySegmentKey('managed_facilities');
  assert.ok(hypothesis);
  const rows = expandSearchStrategies(hypothesis, { city: 'Manchester', state: 'NH' });
  const queries = rows.map(row => row.query).join('\n');
  assert.match(queries, /auto dealership Manchester NH/i);
  assert.match(queries, /private school Manchester NH/i);
  assert.match(queries, /manufacturing facility Manchester NH/i);
  assert.match(queries, /warehouse distribution center Manchester NH/i);
  assert.match(queries, /commercial property management Manchester NH/i);
});

test('Anchor successor mission adds managed facilities without removing restaurant FOH or mutating predecessor', () => {
  const source = {
    id: 'source',
    client_id: 10,
    stage: 'ready',
    status: 'active',
    objective: 'Find recurring commercial cleaning buyers',
    target_segment: 'commercial_cleaning',
    campaign: null,
    title: 'Anchor',
    priority: 'high',
    confidence: 0.9,
    owner: 'max',
    payload: {
      tenantId: '10',
      structuredMission: {
        immutable: true,
        market: { eligibleSubsegments: ['property_manager'] },
      },
      constraints: ['Restaurants excluded'],
    },
  };
  const before = JSON.stringify(source);
  const next = successorMission(source);
  assert.ok(next.structuredMission.market.eligibleSubsegments.includes('property_manager'));
  assert.ok(next.structuredMission.market.eligibleSubsegments.includes('restaurant_foh'));
  assert.ok(next.structuredMission.market.eligibleSubsegments.includes('managed_facilities'));
  assert.ok(next.constraints.some(value => /Client portal is a managed-service differentiator/i.test(value)));
  assert.equal(JSON.stringify(source), before);
});
