'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../index');
const { createMission } = require('../Mission');
const {
  planFromObjective,
  applyClarification,
  applyEdits,
} = require('../MissionPlanner');
const {
  freezeStructuredMission,
  formatMissionUnderstanding,
  formatOperatorConfirmation,
  CONTEXT_PRECEDENCE,
  isReadyForLock,
} = require('../StructuredMission');
const {
  scoutInput,
  paigeInput,
  veraInput,
  rexInput,
  scoutDelegationFromMission,
  requireLockedMissionPlan,
} = require('../SpecialistInputs');
const {
  buildDelegationFromAmoMission,
  advancePlanAfterApproval,
  advancePlanClarification,
  cancelMissionPlan,
  advanceDiscoveryAfterApproval,
  hasPendingPlanApproval,
  hasPendingPlanClarification,
  hasPendingDiscoveryApproval,
} = require('../../max/workspace/AmoOperatorApproval');

const STR_OBJECTIVE =
  'Acquire one recurring commercial cleaning client from a short-term rental operator in Greater Manchester.';

describe('SPEC-130 — Mission Planning Engine', () => {
  it('converts a fully specified objective into a structured contract with provenance', () => {
    const { draft, understanding, confirmation, executed, pipeline } = planFromObjective(STR_OBJECTIVE);
    assert.equal(draft.missionType, 'acquisition');
    assert.equal(draft.type, 'acquisition');
    assert.match(draft.objective, /Acquire one recurring commercial cleaning client/i);
    assert.equal(draft.successMetric.target, 1);
    assert.ok(['customers', 'recurring_clients'].includes(draft.successMetric.type));
    assert.equal(draft.market.segment, 'short_term_rental');
    assert.equal(draft.market.industry, 'hospitality');
    assert.equal(draft.market.buyer, 'property_operator');
    assert.equal(draft.geography.region, 'Greater Manchester');
    assert.ok(draft.geography.cities.includes('Manchester'));
    assert.ok(draft.geography.cities.includes('Hooksett'));
    assert.deepEqual(draft.constraints, ['recurring', 'commercial_only']);
    assert.equal(draft.priority, 1);
    assert.equal(draft.evidence.minimumConfidence, 0.7);
    assert.equal(draft.evidence.minimumBuyingSignals, 2);
    assert.equal(draft.evidence.thresholdLabel, 'medium');
    assert.equal(draft.execution.state, 'planned');
    assert.equal(executed, false);
    assert.ok(pipeline.includes('intent_analysis'));
    assert.equal(understanding.market, 'Short-term rental operators');
    assert.match(confirmation, /Proceed\?/);
    assert.match(confirmation, /Approve/);
    assert.match(confirmation, /Edit/);
    assert.match(confirmation, /Cancel/);
    const segmentProvenance = draft.provenance.find((row) => row.field === 'market.segment');
    assert.equal(segmentProvenance.value, 'short_term_rental');
    assert.ok(segmentProvenance.confidence >= 0.96);
    assert.match(segmentProvenance.reason, /STR operator taxonomy/i);
  });

  it('asks instead of guessing Manchester NH vs Manchester UK', () => {
    const planned = planFromObjective('Find STR operators around Manchester.');
    assert.equal(planned.readyForConfirmation, false);
    assert.equal(planned.executed, false);
    assert.ok(planned.ambiguities.length);
    assert.match(planned.clarificationPrompt, /Manchester NH or Manchester UK/i);
    assert.equal(planned.draft.geography.region, null);
    assert.throws(
      () => freezeStructuredMission(planned.draft),
      /ambiguities|invalid|geography/i
    );
  });

  it('updates the mission after the operator chooses Manchester NH', () => {
    const first = planFromObjective('Find STR operators around Manchester.');
    const next = applyClarification('Find STR operators around Manchester.', 'Manchester NH', {
      prior: first,
    });
    assert.equal(next.readyForConfirmation, true);
    assert.equal(next.draft.geography.region, 'Greater Manchester');
    assert.ok(next.draft.geography.cities.includes('Hooksett'));
    assert.equal(next.draft.market.segment, 'short_term_rental');
    assert.match(next.confirmation, /Evidence Threshold/i);
  });

  it('asks instead of guessing property-manager slice', () => {
    const planned = planFromObjective('Find property managers.');
    assert.equal(planned.readyForConfirmation, false);
    assert.match(planned.clarificationPrompt, /Residential\?/i);
    assert.match(planned.clarificationPrompt, /Short-term rental\?/i);
    const commercial = applyClarification('Find property managers.', 'Commercial', { prior: planned });
    assert.ok(commercial.draft.constraints.includes('commercial_only'));
    assert.equal(commercial.draft.market.segment, 'property_management');
  });

  it('never lets Blueprint override operator geography', () => {
    const planned = planFromObjective(
      'Acquire one recurring STR client in Charleston WV.',
      {
        context: {
          blueprint: { geography: 'Greater Manchester NH' },
        },
      }
    );
    assert.equal(planned.draft.geography.region, 'Charleston WV');
    const geo = planned.draft.provenance.find((row) => row.field === 'geography.region');
    assert.notEqual(geo.source, 'blueprint');
  });

  it('uses Blueprint only as reference for missing geography', () => {
    const planned = planFromObjective(
      'Acquire one recurring commercial cleaning client from a short-term rental operator.',
      {
        context: {
          blueprint: { geography: 'Greater Manchester NH' },
        },
      }
    );
    assert.equal(planned.draft.geography.region, 'Greater Manchester');
    const geo = planned.draft.provenance.find((row) => row.field === 'geography.region');
    assert.equal(geo.source, 'blueprint');
  });

  it('exposes operator-approval → mission-plan → blueprint precedence', () => {
    assert.deepEqual(CONTEXT_PRECEDENCE.slice(0, 5), [
      'operator_approval',
      'operator',
      'mission_plan',
      'workspace',
      'blueprint',
    ]);
  });

  it('seeds clarification or plan approval on new missions — never execution', () => {
    const mission = createMission({
      tenantId: '10',
      objective: STR_OBJECTIVE,
    });
    assert.ok(mission.missionPlanDraft);
    assert.equal(mission.structuredMission, null);
    assert.ok(mission.pendingOperatorDecision);
    assert.equal(mission.pendingOperatorDecision.prompt, 'Approve mission plan?');
    assert.match(mission.pendingOperatorDecision.missionUnderstanding, /Mission Understanding/i);
    assert.deepEqual(mission.pendingOperatorDecision.actions, ['Approve', 'Edit', 'Cancel']);
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
    assert.equal(result.structuredMission.execution.state, 'approved');

    const snapshotAfter = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(hasPendingPlanApproval(snapshotAfter), false);
    assert.equal(hasPendingDiscoveryApproval(snapshotAfter), true);
    assert.equal(snapshotAfter.mission.pendingOperatorDecision.prompt, 'Approve discovery?');
    assert.equal(snapshotAfter.mission.missionPlanDraft, null);
  });

  it('Scout delegation uses the locked plan — no English parsing', () => {
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
    assert.equal(delegation.businessContext.evidencePolicy.minimumConfidence, 0.7);
    assert.deepEqual(delegation.businessContext.exclusions, ['recurring', 'commercial_only']);
    assert.notEqual(delegation.targetContext.segments[0], 'property_managers,_facility_managers');
  });

  it('refuses specialist execution before the plan is locked', () => {
    const mission = createMission({
      tenantId: '10',
      objective: STR_OBJECTIVE,
    });
    assert.throws(() => requireLockedMissionPlan(mission), /approved and locked/i);
    assert.throws(() => scoutDelegationFromMission(mission), /approved and locked/i);
  });

  it('specialists receive structured input contracts only', () => {
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
    assert.equal(scout.evidencePolicy.minimumBuyingSignals, 2);
    assert.ok(scout.geography.cities.length);

    const paige = paigeInput(mission);
    assert.equal(paige.market.segment, 'short_term_rental');
    assert.equal(paige.audience, 'Short-term rental operators');
    assert.equal(paige.tone, 'operator_voice');
    assert.match(paige.objective, /recurring commercial cleaning client/i);

    const vera = veraInput(mission, [{ name: 'Summit STR' }]);
    assert.equal(vera.region, 'Greater Manchester');
    assert.equal(vera.companies.length, 1);
    assert.ok(vera.reviewPolicy);

    const rex = rexInput(mission);
    assert.equal(rex.successMetric.target, 1);
    assert.equal(rex.mission.id, 'm1');
    assert.equal(rex.kpis[0].target, 1);
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

  it('clarification then lock then Scout — planner is the only interpreter', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: 'Find STR operators around Manchester.',
    });
    assert.equal(hasPendingPlanClarification(engine.inspect(mission.id, { tenantId: '10' })), true);

    const clarified = advancePlanClarification({
      engine,
      mission,
      tenantId: '10',
      question: 'Manchester NH',
    });
    assert.equal(clarified.matched, true);
    assert.equal(clarified.readyForConfirmation, true);
    assert.equal(hasPendingPlanApproval(clarified.snapshot), true);

    const locked = await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approve',
    });
    assert.ok(locked.structuredMission.immutable);
    const scout = scoutInput(locked.snapshot.mission);
    assert.equal(scout.segment, 'short_term_rental');
    assert.equal(scout.geography.region, 'Greater Manchester');
  });

  it('cancel leaves the plan unlocked and specialists idle', () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: STR_OBJECTIVE,
    });
    const result = cancelMissionPlan({
      engine,
      mission,
      tenantId: '10',
      question: 'Cancel',
    });
    assert.equal(result.cancelled, true);
    assert.equal(result.snapshot.mission.planCancelled, true);
    assert.equal(result.snapshot.mission.pendingOperatorDecision, null);
    assert.equal(result.snapshot.mission.structuredMission, null);
    assert.throws(() => scoutDelegationFromMission(result.snapshot.mission));
  });

  it('edits replan without executing', () => {
    const edited = applyEdits(STR_OBJECTIVE, { region: 'Charleston WV' });
    assert.equal(edited.draft.geography.region, 'Charleston WV');
    assert.equal(edited.executed, false);
    assert.equal(isReadyForLock(edited.draft), true);
  });

  it('formatMissionUnderstanding matches operator-facing summary', () => {
    const understanding = formatMissionUnderstanding(planFromObjective(STR_OBJECTIVE).draft);
    assert.match(understanding.objective, /recurring commercial cleaning client/i);
    assert.equal(understanding.market, 'Short-term rental operators');
    assert.equal(understanding.region, 'Greater Manchester');
    assert.equal(understanding.buyer, 'property_operator');
    assert.deepEqual(understanding.constraints, ['recurring', 'commercial_only']);
    assert.equal(understanding.evidenceThreshold, 'medium');
    const confirmation = formatOperatorConfirmation(planFromObjective(STR_OBJECTIVE).draft);
    assert.match(confirmation, /One recurring client/i);
  });
});
