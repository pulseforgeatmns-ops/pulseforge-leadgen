'use strict';

/**
 * Regression: operational status questions must not mutate acquisition mission state.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const {
  classifyOperatorMissionTurnIntent,
  OPERATOR_TURN_INTENT_TYPES,
  STATUS_QUERY_DOMAINS,
  STATUS_QUERY_ACTIONS,
} = require('../OperatorMissionTurnIntent');
const { isMissionExecutionCommand } = require('../ExecutionLanguageDetection');
const { classifyOperatorCognition } = require('../../operatorCognition');
const { analyzeOperatorIntent } = require('../OperatorIntent');
const {
  maybeHandleAcquisitionMissionExecution,
  detectExecutionAction,
  buildExecutionMissionResponse,
} = require('../AcquisitionMissionExecution');
const { createTestAmoRuntime } = require('./amoTestRuntime');

const PAIGE_QUESTION = 'does Paige have pending posts for me to approve?';

describe('Max status query vs mission approval guard', () => {
  describe('Test 1 — Paige pending posts question', () => {
    it('classifies as read-only Paige social status query', () => {
      const intent = classifyOperatorMissionTurnIntent(PAIGE_QUESTION);
      assert.equal(intent.type, OPERATOR_TURN_INTENT_TYPES.STATUS_QUERY);
      assert.equal(intent.domain, STATUS_QUERY_DOMAINS.PAIGE_SOCIAL);
      assert.equal(intent.action, STATUS_QUERY_ACTIONS.READ_PENDING_APPROVAL_QUEUE);
      assert.equal(intent.mutatesMissionState, false);
    });

    it('does not treat the question as a mission execution command', () => {
      assert.equal(isMissionExecutionCommand(PAIGE_QUESTION), false);
      const cognition = classifyOperatorCognition(PAIGE_QUESTION, {});
      assert.equal(cognition.intent, 'inspect');
      assert.equal(cognition.via, 'status_query');
    });

    it('does not execute or emit Operator approved — Executing', async () => {
      const engine = amo.createAcquisitionMissionEngine();
      const runtime = createTestAmoRuntime({ engine });
      const mission = engine.create({
        tenantId: '10',
        title: 'Anchor Cleaning pilot',
        objective: 'Acquire commercial cleaning customers in Manchester NH.',
      });
      const snapshotBefore = engine.inspect(mission.id, { tenantId: '10' });

      const operatorIntent = await analyzeOperatorIntent({
        question: PAIGE_QUESTION,
        session: { id: 's-status', context: { tenantId: '10', missionId: mission.id } },
        mission,
        resolveMission: false,
        acquisitionMissionRuntime: runtime,
      });
      assert.equal(operatorIntent.executionRequested, false);
      assert.equal(operatorIntent.statusQuery.action, STATUS_QUERY_ACTIONS.READ_PENDING_APPROVAL_QUEUE);

      const action = detectExecutionAction(PAIGE_QUESTION, snapshotBefore, operatorIntent);
      assert.equal(action, null);

      const turn = await maybeHandleAcquisitionMissionExecution({
        question: PAIGE_QUESTION,
        session: { id: 's-status', context: { tenantId: '10', missionId: mission.id } },
        operatorIntent,
        conversationIntent: operatorIntent.conversationIntent,
        acquisitionMissionRuntime: runtime,
        persist: false,
      });
      assert.equal(turn, null);

      const snapshotAfter = engine.inspect(mission.id, { tenantId: '10' });
      assert.equal(snapshotAfter.mission.stage, snapshotBefore.mission.stage);
      assert.deepEqual(
        snapshotAfter.mission.pendingOperatorDecision,
        snapshotBefore.mission.pendingOperatorDecision
      );

      const hypothetical = buildExecutionMissionResponse({
        mission: snapshotAfter.mission,
        snapshot: snapshotAfter,
        action: 'operator_approved',
        question: PAIGE_QUESTION,
        executionResult: null,
      });
      assert.doesNotMatch(hypothetical.prose, /Operator approved — Executing/i);
    });
  });

  describe('Test 2 — general pending approval question', () => {
    it('classifies as operator approval status query', () => {
      const q = 'do we have anything pending for approval?';
      const intent = classifyOperatorMissionTurnIntent(q);
      assert.equal(intent.type, OPERATOR_TURN_INTENT_TYPES.STATUS_QUERY);
      assert.equal(
        intent.action,
        STATUS_QUERY_ACTIONS.READ_PENDING_OPERATOR_APPROVAL_ITEMS
      );
      assert.equal(intent.mutatesMissionState, false);
    });
  });

  describe('Test 3 — explicit approval still works', () => {
    it('classifies explicit approval as mission approval', () => {
      const q = 'approved, proceed with the first controlled batch';
      const intent = classifyOperatorMissionTurnIntent(q);
      assert.equal(intent.type, OPERATOR_TURN_INTENT_TYPES.MISSION_APPROVAL);
      assert.equal(isMissionExecutionCommand(q), true);
    });
  });

  describe('Test 4 — ambiguous go ahead', () => {
    it('does not blindly approve without a single pending decision', () => {
      const intent = classifyOperatorMissionTurnIntent('go ahead', {
        hasSinglePendingOperatorApproval: false,
      });
      assert.equal(intent.type, OPERATOR_TURN_INTENT_TYPES.STATUS_QUERY);
      assert.equal(intent.ambiguousGoAhead, true);
      assert.equal(isMissionExecutionCommand('go ahead'), false);
    });

    it('allows go ahead when exactly one pending operator decision is active', () => {
      const intent = classifyOperatorMissionTurnIntent('go ahead', {
        hasSinglePendingOperatorApproval: true,
      });
      assert.equal(intent.type, OPERATOR_TURN_INTENT_TYPES.MISSION_APPROVAL);
    });
  });
});
