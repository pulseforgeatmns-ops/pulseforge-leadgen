'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { PORTAL_ARM, CONTROL_ARM, PORTAL_EXPERIMENT_OPT_IN, managedFacilitiesExperiment,
  managedFacilitiesScore, experimentArm } = require('../utils/anchorManagedFacilities');
const { buildAnchorLifecycleVariant } = require('../utils/anchorLifecycleEmail');
const { freezeStructuredMission } = require('../packages/acquisition-mission/StructuredMission');
const { buildAnchorCopy, resolveSegment, LIFECYCLE_STAGES } = require('../utils/anchorCopyDoctrine');
const { PERSONALIZATION_STATUS } = require('../utils/scoutPersonalizationEvidence');
const { buildPerProspectVariants } = require('../packages/max/workspace/PaigeVariantsExecutor');
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

test('high-scoring existing property-manager copy is byte-equivalent to legacy copy without explicit opt-in', () => {
  const id = Array.from({ length: 128 }, (_, i) => `legacy-pm-${i}`).find(value => experimentArm(value) === PORTAL_ARM);
  const prospect = { ...managedProspect(id), vertical: 'property_manager', company_name: 'Mill City Property Management',
    company_fields: { name: 'Mill City Property Management', location: 'Manchester, NH' } };
  assert.ok(managedFacilitiesScore(prospect, prospect.company_fields) >= 7);
  const plan = { market: { segment: 'property_management', label: 'property management' }, geography: { label: 'Manchester' } };
  const legacy = buildAnchorCopy({ lifecycleStage: LIFECYCLE_STAGES.COLD, firstName: 'Sam',
    companyName: prospect.company_name, senderName: 'Jacob Maynard', serviceArea: 'Manchester',
    segment: resolveSegment(plan), evidence: { personalization_status: PERSONALIZATION_STATUS.NO_USEFUL_FACT },
    company: prospect.company_fields });
  for (const mission of [{}, optedInMission({ constraints: [] })]) {
    const result = buildAnchorLifecycleVariant({ candidate: { candidateId: id, name: prospect.company_name },
      crmRecord: prospect, mission, plan });
    for (const field of ['subject', 'body', 'cta']) assert.equal(result[field], legacy[field], field);
    assert.equal(result.usedPersonalization, legacy.usedPersonalization === true);
    assert.equal(result.cohort, null);
    assert.equal(result.experimentArm, null);
    assert.equal(result.managedFacilitiesScore, 0);
    const variants = buildPerProspectVariants({ clientId: 10, mission, plan,
      max: { rankedTargets: [{ candidateId: id, name: prospect.company_name }] },
      crmByProspectId: new Map([[id, prospect]]) });
    assert.equal(variants.length, 1);
    assert.equal(variants[0].body, legacy.body);
    assert.equal(variants[0].attributableIntelligence.cohort, null);
    assert.equal(variants[0].attributableIntelligence.experimentArm, null);
  }
});

test('experiment opt-in fails closed on missing, stale, unapproved, ambiguous and cross-tenant mission context', () => {
  const change = mutate => { const mission = structuredClone(optedInMission()); mutate(mission); return mission; };
  const missingOptIn = optedInMission({ constraints: [] });
  const invalidMissions = [
    undefined, null, {},
    missingOptIn,
    { ...missingOptIn, managedFacilitiesExperiment: true },
    change(m => { delete m.id; }),
    change(m => { delete m.tenantId; delete m.clientId; }),
    change(m => { m.tenantId = '13'; m.clientId = 13; }),
    change(m => { m.client_id = 13; }),
    change(m => { m.structuredMission.immutable = false; }),
    change(m => { m.structuredMission.immutable = 'true'; }),
    change(m => { m.structuredMission.execution.state = 'drafting'; }),
    change(m => { delete m.structuredMission.contractHash; }),
    change(m => { m.structuredMission.constraints.push('unreviewed_change'); }),
    change(m => { m.structuredMission.constraints = PORTAL_EXPERIMENT_OPT_IN; }),
    optedInMission({ market: { segment: 'property_manager', eligibleSubsegments: ['property_manager'] } }),
    optedInMission({ market: { segment: 'managed_facilities', eligibleSubsegments: ['restaurant_foh'] } }),
    optedInMission({ market: { segment: 'managed_facilities', eligibleSubsegments: ['managed_facilities', 'unsupported'] } }),
    optedInMission({ constraints: ['enable ' + PORTAL_EXPERIMENT_OPT_IN] }),
  ];
  const prospect = managedProspect('opt-in-fail-closed');
  for (const [index, mission] of invalidMissions.entries()) {
    assert.deepEqual(managedFacilitiesExperiment({ prospect, company: prospect.company_fields, mission }),
      { cohort: null, score: 0, arm: null }, `mission ${index}`);
  }
  const { client_id, ...withoutTenant } = prospect;
  for (const crm of [withoutTenant, { ...prospect, client_id: 13 }, { ...prospect, tenantId: '13' }]) {
    assert.deepEqual(managedFacilitiesExperiment({ prospect: crm, mission: optedInMission() }),
      { cohort: null, score: 0, arm: null });
  }
  assert.deepEqual(managedFacilitiesExperiment({ prospect, mission: optedInMission(), company: { client_id: 13 } }),
    { cohort: null, score: 0, arm: null });
});

test('explicit mission does not enroll follow-up stages, non-facilities or below-threshold records', () => {
  const mission = optedInMission();
  for (const lifecycleStage of Object.values(LIFECYCLE_STAGES).filter(stage => stage !== LIFECYCLE_STAGES.COLD)) {
    const result = buildAnchorLifecycleVariant({ candidate: { candidateId: 'follow-up' },
      crmRecord: managedProspect('follow-up'), mission, lifecycleStage });
    assert.equal(result.cohort, null, lifecycleStage);
    assert.equal(result.experimentArm, null, lifecycleStage);
    assert.doesNotMatch(result.body, /simple portal to see service history/);
  }
  for (const prospect of [
    { ...managedProspect('unrelated'), vertical: 'plumbing', recurring_janitorial_need: true },
    { ...managedProspect('tiny'), tiny_facility: true, location_count: 1, title: 'Owner' },
  ]) {
    const result = managedFacilitiesExperiment({ prospect, mission });
    assert.equal(result.cohort, null);
    assert.equal(result.arm, null);
  }
});

test('successor mission never opts into copy experimentation implicitly', () => {
  const source = { id: 'legacy-source', client_id: 10, objective: 'Find commercial cleaning clients',
    payload: { structuredMission: { immutable: true, market: { eligibleSubsegments: ['property_manager'] } } } };
  const next = successorMission(source);
  assert.ok(next.structuredMission.market.eligibleSubsegments.includes('managed_facilities'));
  assert.ok(!next.structuredMission.constraints?.includes(PORTAL_EXPERIMENT_OPT_IN));
  assert.equal(managedFacilitiesExperiment({ mission: next, prospect: managedProspect('successor') }).cohort, null);
});


test('approved hash-bound mission opt-in enrolls both copy arms through Paige without changing CTA', () => {
  const mission = optedInMission();
  let cta;
  for (const arm of [PORTAL_ARM, CONTROL_ARM]) {
    const id = Array.from({ length: 128 }, (_, i) => `opted-in-${i}`).find(value => experimentArm(value) === arm);
    const prospect = managedProspect(id);
    const variants = buildPerProspectVariants({ clientId: 10, mission,
      max: { rankedTargets: [{ candidateId: id, name: prospect.company_name }] },
      crmByProspectId: new Map([[id, prospect]]) });
    assert.equal(variants.length, 1);
    assert.equal(variants[0].attributableIntelligence.cohort, 'managed_facilities');
    assert.equal(variants[0].attributableIntelligence.experimentArm, arm);
    assert.equal(/simple portal to see service history/.test(variants[0].body), arm === PORTAL_ARM);
    if (cta != null) assert.equal(variants[0].cta, cta);
    cta = variants[0].cta;
  }
});


test('plan, prospect and company opt-in-looking flags cannot enable the copy experiment', () => {
  const id = Array.from({ length: 128 }, (_, i) => `flag-only-${i}`).find(value => experimentArm(value) === PORTAL_ARM);
  const flags = { managedFacilitiesExperiment: true, managedFacilitiesPortalCopyExperiment: true,
    [PORTAL_EXPERIMENT_OPT_IN]: true, constraints: [PORTAL_EXPERIMENT_OPT_IN],
    structuredMission: optedInMission().structuredMission };
  const prospect = { ...managedProspect(id), ...flags,
    company_fields: { ...managedProspect(id).company_fields, ...flags } };
  for (const mission of [{}, optedInMission({ constraints: [] })]) {
    const result = buildAnchorLifecycleVariant({ candidate: { candidateId: id, ...flags },
      crmRecord: prospect, mission, plan: flags });
    assert.equal(result.cohort, null);
    assert.equal(result.experimentArm, null);
    assert.doesNotMatch(result.body, /simple portal to see service history/);
  }
});
