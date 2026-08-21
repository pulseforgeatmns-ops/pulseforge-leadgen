'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../index');
const { createMission } = require('../Mission');
const { planFromObjective } = require('../MissionPlanner');
const {
  freezeStructuredMission,
  formatMissionUnderstanding,
} = require('../StructuredMission');
const {
  scoutInput,
  paigeInput,
  veraInput,
  rexInput,
  scoutDelegationFromMission,
} = require('../SpecialistInputs');
const {
  buildDelegationFromAmoMission,
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  hasPendingPlanApproval,
  hasPendingDiscoveryApproval,
} = require('../../max/workspace/AmoOperatorApproval');

const STR_OBJECTIVE =
  'Acquire one recurring commercial cleaning client from a short-term rental operator in Greater Manchester.';

describe('SPEC-130 — Structured Mission Planning', () => {
  it('Mission Planner converts operator objective into structured contract', () => {
    const { draft, understanding } = planFromObjective(STR_OBJECTIVE);
    assert.equal(draft.missionType, 'acquisition');
    assert.match(draft.objective, /Acquire one recurring commercial cleaning client/i);
    assert.equal(draft.successMetric.type, 'customers');
    assert.equal(draft.successMetric.target, 1);
    assert.equal(draft.market.segment, 'short_term_rental');
    assert.equal(draft.market.industry, 'hospitality');
    assert.equal(draft.market.buyer, 'property_operator');
    assert.equal(draft.geography.region, 'Greater Manchester');
    assert.ok(draft.geography.cities.includes('Manchester'));
    assert.ok(draft.geography.cities.includes('Hooksett'));
    assert.deepEqual(draft.constraints, ['recurring', 'commercial_only']);
    assert.equal(draft.priority, 1);
    assert.equal(understanding.market, 'Short-term rental operators');
  });

  it('seeds plan approval before discovery on new missions', () => {
    const mission = createMission({
      tenantId: '10',
      objective: STR_OBJECTIVE,
    });
    assert.ok(mission.missionPlanDraft);
    assert.equal(mission.structuredMission, null);
    assert.ok(mission.pendingOperatorDecision);
    assert.equal(mission.pendingOperatorDecision.prompt, 'Approve mission plan?');
    assert.match(mission.pendingOperatorDecision.missionUnderstanding, /Mission Understanding/i);
  });

  it('freezes structured mission after operator plan approval', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: STR_OBJECTIVE,
    });
    const snapshotBefore = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(hasPendingPlanApproval(snapshotBefore), true);
    assert.equal(hasPendingDiscoveryApproval(snapshotBefore), false);

    const result = await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved.',
    });

    assert.equal(result.alreadyExecuted, false);
    assert.ok(result.structuredMission.immutable);
    assert.ok(result.structuredMission.contractHash);

    const snapshotAfter = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(hasPendingPlanApproval(snapshotAfter), false);
    assert.equal(hasPendingDiscoveryApproval(snapshotAfter), true);
    assert.equal(snapshotAfter.mission.pendingOperatorDecision.prompt, 'Approve discovery?');
    assert.equal(snapshotAfter.mission.missionPlanDraft, null);
  });

  it('Scout delegation uses structured mission — no English parsing', () => {
    const draft = planFromObjective(STR_OBJECTIVE).draft;
    const frozen = freezeStructuredMission(draft, { approvedBy: 'operator' });
    const mission = {
      id: 'mission_str',
      tenantId: '10',
      objective: STR_OBJECTIVE,
      targetSegment: 'Property Managers, Facility Managers',
      structuredMission: frozen,
      structuredMissionApproved: true,
    };

    const delegation = buildDelegationFromAmoMission(mission);
    assert.equal(delegation.targetContext.segments[0], 'short_term_rental');
    assert.equal(delegation.targetContext.industry, 'hospitality');
    assert.equal(delegation.targetContext.buyer, 'property_operator');
    assert.equal(delegation.businessContext.structuredOnly, true);
    assert.equal(delegation.businessContext.missionObjectiveImmutable, true);
    assert.deepEqual(delegation.businessContext.exclusions, ['recurring', 'commercial_only']);
    assert.notEqual(delegation.targetContext.segments[0], 'property_managers,_facility_managers');
  });

  it('specialists receive structured input contracts', () => {
    const mission = {
      id: 'm1',
      title: 'STR — Manchester',
      stage: 'discover',
      status: 'Discovering',
      progressPercent: 8,
      structuredMission: freezeStructuredMission(planFromObjective(STR_OBJECTIVE).draft),
    };

    const scout = scoutInput(mission);
    assert.equal(scout.segment, 'short_term_rental');
    assert.equal(scout.structuredOnly, true);
    assert.ok(scout.geography.cities.length);

    const paige = paigeInput(mission);
    assert.equal(paige.market.segment, 'short_term_rental');
    assert.match(paige.objective, /recurring commercial cleaning client/i);

    const vera = veraInput(mission, [{ name: 'Summit STR' }]);
    assert.equal(vera.region, 'Greater Manchester');
    assert.equal(vera.companies.length, 1);

    const rex = rexInput(mission);
    assert.equal(rex.successMetric.target, 1);
    assert.equal(rex.mission.id, 'm1');
  });

  it('discovery cannot run until plan is approved', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: STR_OBJECTIVE,
    });

    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(hasPendingDiscoveryApproval(snapshot), false);

    await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved.',
    });

    const result = await advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: true,
    });

    assert.equal(result.executionOutcome, 'completed');
    assert.ok(result.discovery);
  });

  it('formatMissionUnderstanding matches operator-facing summary', () => {
    const understanding = formatMissionUnderstanding(planFromObjective(STR_OBJECTIVE).draft);
    assert.match(understanding.objective, /recurring commercial cleaning client/i);
    assert.equal(understanding.market, 'Short-term rental operators');
    assert.equal(understanding.region, 'Greater Manchester');
    assert.equal(understanding.buyer, 'property_operator');
    assert.deepEqual(understanding.constraints, ['recurring', 'commercial_only']);
  });
});
