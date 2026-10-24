'use strict';

/**
 * Regression — Studio Substral mission start: region clarification must not use yes/no capture.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { OPERATOR_DECISION_RESPONSE_TYPES } = amo;
const SUBSTRAL_MISSION_OBJECTIVE =
  'Acquire one paid Studio Substral website assessment from an established business with a live website and a real decision about what to fix, rebuild, or leave alone.';
const {
  resolvePendingOperatorDecision,
  pendingDecisionOwnsTurn,
} = require('../PendingDecisionResolver');
const { analyzeOperatorIntent } = require('../OperatorIntent');
const { maybeHandlePendingDecisionTurn, buildClarifyProse } = require('../PendingDecisionTurn');
const { RESOLUTION_OUTCOMES } = require('../PendingDecisionResolver');
const {
  advancePlanClarification,
} = require('../AmoOperatorApproval');
const { createTestAmoRuntime } = require('./amoTestRuntime');
const { detectExecutionAction, maybeHandleAcquisitionMissionExecution } = require('../AcquisitionMissionExecution');

const REGION_ANSWER = 'Greater Manchester, NH and southern New Hampshire';
const REGION_ANSWER_WITH_YES = `yes, ${REGION_ANSWER}`;

describe('Studio Substral — region pending decision (text response)', () => {
  function createSubstralPlanningMission() {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '17',
      objective: SUBSTRAL_MISSION_OBJECTIVE,
      targetSegment: 'Established businesses with live websites — United States',
    });
    return { engine, mission };
  }

  it('seeds plan_clarification with text responseType for missing geography', () => {
    const { mission } = createSubstralPlanningMission();
    assert.equal(mission.pendingOperatorDecision.kind, 'plan_clarification');
    assert.equal(
      mission.pendingOperatorDecision.prompt,
      'Which region should this mission cover?'
    );
    assert.equal(
      mission.pendingOperatorDecision.responseType,
      OPERATOR_DECISION_RESPONSE_TYPES.TEXT
    );
  });

  it('does not treat region answers as ambiguous yes/no pending ownership', async () => {
    const { mission } = createSubstralPlanningMission();
    for (const utterance of [REGION_ANSWER, REGION_ANSWER_WITH_YES]) {
      const resolution = resolvePendingOperatorDecision(utterance, mission);
      assert.equal(resolution.resolved, true);
      assert.equal(resolution.action, 'clarify_plan');
      assert.equal(resolution.executionAction, 'plan_clarified');
      assert.equal(resolution.decisionKind, 'plan_clarification');
      assert.equal(pendingDecisionOwnsTurn(resolution), false);
    }
  });

  for (const answer of [REGION_ANSWER, REGION_ANSWER_WITH_YES]) {
  it(`routes a clear region answer to clarification without yes/no capture: ${answer}`, async () => {
    const { engine, mission } = createSubstralPlanningMission();
    const runtime = createTestAmoRuntime({ engine });
    const intent = await analyzeOperatorIntent({
      question: answer,
      mission,
      resolveMission: false,
      acquisitionMissionRuntime: runtime,
    });

    assert.equal(intent.pendingDecisionResolution.action, 'clarify_plan');
    assert.equal(intent.pendingDecisionResolution.executionAction, 'plan_clarified');
    assert.equal(pendingDecisionOwnsTurn(intent.pendingDecisionResolution), false);

    const pendingTurn = await maybeHandlePendingDecisionTurn({
      question: answer,
      operatorIntent: intent,
      context: { tenantId: '17' },
      acquisitionMissionRuntime: runtime,
    });
    assert.equal(pendingTurn, null);
    const result = await maybeHandleAcquisitionMissionExecution({
      question: answer,
      operatorIntent: intent,
      context: { tenantId: '17', missionId: mission.id },
      acquisitionMissionRuntime: runtime,
      allowFixtureFallback: true,
    });
    assert.equal(result.action, 'plan_clarified');
    assert.doesNotMatch(result.prose, /yes or no/i);
    const after = engine.inspect(mission.id, { tenantId: '17' });
    assert.equal(after.mission.missionPlanDraft.geography.region, 'Greater Manchester');
    assert.equal(after.mission.pendingOperatorDecision.kind, 'plan_approval');
    assert.equal(after.mission.structuredMissionApproved, false);
    assert.equal(after.contributions.some((row) => row.specialist === 'scout'), false);
  });
  }

  it('never prepends yes/no clarification copy for text pending decisions', () => {
    const { mission } = createSubstralPlanningMission();
    const prose = buildClarifyProse(REGION_ANSWER, {
      outcome: RESOLUTION_OUTCOMES.AMBIGUOUS,
      decisionKind: 'plan_clarification',
      prompt: mission.pendingOperatorDecision.prompt,
    }, { mission });
    assert.doesNotMatch(prose, /yes or no/i);
  });

  it('stores operator region on the mission plan after clarification', () => {
    const { engine, mission } = createSubstralPlanningMission();
    const snapshot = engine.inspect(mission.id, { tenantId: '17' });
    const action = detectExecutionAction(REGION_ANSWER_WITH_YES, snapshot, {
      planningRequested: true,
    });
    assert.equal(action, 'plan_clarified');

    const result = advancePlanClarification({
      engine,
      mission,
      tenantId: '17',
      question: REGION_ANSWER_WITH_YES,
    });
    assert.equal(result.matched, true);
    assert.ok(result.planned.draft.geography.region);
    assert.match(String(result.planned.draft.geography.region), /Greater Manchester/i);
  });
});
