'use strict';

/**
 * Regression — Studio Substral region clarification + compound approval dispatches Scout.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { createTestAmoRuntime } = require('./amoTestRuntime');
const { analyzeOperatorIntent } = require('../OperatorIntent');
const { maybeHandleAcquisitionMissionExecution } = require('../AcquisitionMissionExecution');
const {
  advancePlanClarification,
  hasPendingPlanClarification,
  hasPendingPlanApproval,
} = require('../AmoOperatorApproval');
const { hasPendingPlanClarification: hasPlanClarificationPredicate } = require('../../../acquisition-mission/PendingOperatorDecision');

const SUBSTRAL_OBJECTIVE =
  'Acquire one paid Studio Substral website assessment from an established business with a live website and a real decision about what to fix, rebuild, or leave alone.';

const COMPOUND_OPERATOR_MESSAGE =
  'Decision: region = Greater Manchester, NH and southern New Hampshire. Approval: approved. Proceed with Scout discovery for the first controlled Studio Substral batch. Outreach remains disabled.';

describe('Studio Substral — region clarification and Scout dispatch', () => {
  let engine;
  let mission;

  beforeEach(() => {
    engine = amo.createAcquisitionMissionEngine();
    mission = engine.create({
      tenantId: '17',
      objective: SUBSTRAL_OBJECTIVE,
      targetSegment: 'Established businesses with live websites — United States',
    });
  });

  it('starts with region clarification for nationwide Studio Substral objective', () => {
    const snapshot = engine.inspect(mission.id, { tenantId: '17' });
    assert.equal(hasPlanClarificationPredicate(snapshot), true);
    assert.match(
      snapshot.mission.pendingOperatorDecision.prompt,
      /Which region should this mission cover/i
    );
  });

  it('accepts free-text region answers without multiple-choice options', () => {
    const clarified = advancePlanClarification({
      engine,
      mission,
      tenantId: '17',
      question: COMPOUND_OPERATOR_MESSAGE,
    });
    assert.equal(clarified.matched, true);
    assert.equal(hasPendingPlanApproval(clarified.snapshot), true);
    assert.equal(clarified.snapshot.mission.missionPlanDraft.geography.region, 'Greater Manchester');
    assert.equal(hasPendingPlanClarification(clarified.snapshot), false);
  });

  it('compound turn clears clarification, locks plan, and creates Scout execution request', async () => {
    const snapshot = engine.inspect(mission.id, { tenantId: '17' });
    const operatorIntent = await analyzeOperatorIntent({
      question: COMPOUND_OPERATOR_MESSAGE,
      mission,
      snapshot,
      resolveMission: false,
      missionsEnabled: true,
    });

    assert.equal(operatorIntent.pendingDecisionResolution.action, 'clarify_plan');

    const turn = await maybeHandleAcquisitionMissionExecution({
      question: COMPOUND_OPERATOR_MESSAGE,
      operatorIntent,
      context: { tenantId: '17', missionId: mission.id },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine }),
      allowFixtureFallback: true,
    });

    assert.ok(turn);
    assert.equal(turn.action, 'discovery_approved');
    assert.ok(turn.executionRequest);

    const after = engine.inspect(mission.id, { tenantId: '17' });
    assert.equal(after.mission.structuredMissionApproved, true);
    assert.equal(after.mission.structuredMission.geography.region, 'Greater Manchester');
    assert.equal(hasPlanClarificationPredicate(after), false);
    assert.ok(
      (after.contributions || []).some(
        (row) => row.specialist === 'scout' && row.kind === 'discovery'
      )
    );
    assert.notEqual(after.mission.pendingOperatorDecision?.kind, 'plan_clarification');
  });
});
