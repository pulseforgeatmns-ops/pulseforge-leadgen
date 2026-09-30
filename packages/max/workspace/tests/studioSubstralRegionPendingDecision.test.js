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
const { detectExecutionAction } = require('../AcquisitionMissionExecution');

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
      assert.equal(resolution.resolved, false);
      assert.equal(resolution.pending, undefined);
      assert.equal(pendingDecisionOwnsTurn(resolution), false);
    }
  });

  it('routes region answers through mission planning instead of pending yes/no turn', async () => {
    const { engine, mission } = createSubstralPlanningMission();
    const runtime = createTestAmoRuntime({ engine });
    const intent = await analyzeOperatorIntent({
      question: REGION_ANSWER_WITH_YES,
      mission,
      resolveMission: false,
      acquisitionMissionRuntime: runtime,
    });

    assert.equal(intent.planningRequested, true);
    assert.equal(intent.pendingDecisionResolution, null);
    assert.equal(intent.conversationIntent.via, 'mission_planning_turn');

    const pendingTurn = await maybeHandlePendingDecisionTurn({
      question: REGION_ANSWER_WITH_YES,
      operatorIntent: intent,
      context: { tenantId: '17' },
      acquisitionMissionRuntime: runtime,
    });
    assert.equal(pendingTurn, null);
  });

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
