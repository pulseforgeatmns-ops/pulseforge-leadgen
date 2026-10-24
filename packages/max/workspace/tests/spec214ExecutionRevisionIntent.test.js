'use strict';

/**
 * SPEC-214 — Route Canonical Execution Revision Intent.
 * REQUEST_REVISION must preserve structured intent through routing,
 * NOT fall through to generic cognition, and NOT approve/send/execute.
 */

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const { STAGES, OPERATOR_DECISION_KINDS } = amo;
const {
  analyzeOperatorIntent,
} = require('../OperatorIntent');
const {
  resetOperatorIntentAudit,
  getOperatorIntentAuditViolations,
} = require('../audit/OperatorIntentAudit');
const { THINKING_MODES } = require('../../operatorCognition');
const {
  resolvePendingOperatorDecision,
} = require('../PendingDecisionResolver');
const { createTestAmoRuntime } = require('./amoTestRuntime');

const OBJECTIVE = 'Acquire commercial cleaning customers in Manchester NH for law firms.';

describe('SPEC-214 — Route Canonical Execution Revision Intent', () => {
  afterEach(() => {
    resetOperatorIntentAudit();
  });

  function executionApprovalMission(extra = {}) {
    return {
      id: 'm-execution-approval',
      stage: STAGES.READY,
      objective: OBJECTIVE,
      pendingOperatorDecision: {
        stage: STAGES.READY,
        kind: OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL,
        prompt: 'Approve execution of the prepared outbound plan?',
      },
      ...extra,
    };
  }

  describe('buildPendingDecisionConversationIntent — REQUEST_REVISION', () => {
    it('recognizes REQUEST_REVISION outcome and returns canonical revision intent', async () => {
      const mission = executionApprovalMission();

      const question =
        'Do not authorize this outreach. The prepared messaging needs to be regenerated before execution. Regenerate the outreach messaging and outbound plan for these prospects, then return the updated package for my approval. Do not send anything.';

      const pendingDecisionResolution = resolvePendingOperatorDecision(question, mission);
      assert.equal(pendingDecisionResolution.resolved, true);
      assert.equal(pendingDecisionResolution.outcome, 'request_revision');

      const intent = await analyzeOperatorIntent({
        question,
        mission,
        resolveMission: false,
        session: { id: 's-spec214', context: { tenantId: '10', missionId: mission.id } },
      });

      assert.ok(intent.conversationIntent);
      assert.equal(intent.conversationIntent.intent, THINKING_MODES.EDIT);
      assert.equal(intent.conversationIntent.via, 'pending_decision_revision_request');
      assert.equal(intent.conversationIntent.thinkingMode, 'execution_revision');
      assert.equal(intent.conversationIntent.mutatesMission, true);
      assert.equal(intent.conversationIntent.pendingDecisionOutcome, 'request_revision');
    });

    it('REQUEST_REVISION requests execution for SPEC-217 canonical routing', async () => {
      const mission = executionApprovalMission();

      const question = 'Regenerate the message.';

      const intent = await analyzeOperatorIntent({
        question,
        mission,
        resolveMission: false,
        session: { id: 's-spec214-exec', context: { tenantId: '10', missionId: mission.id } },
      });

      // SPEC-217: REQUEST_REVISION with REVISE_PREPARED_OUTREACH must be execution-capable
      // so it can reach the canonical execution router and dispatch to advancePreparedOutreachRevision()
      assert.equal(intent.executionRequested, true, 'executionRequested must be true for revision routing');
      assert.equal(intent.planningRequested, false);
      assert.equal(intent.mutatesMission, true);
    });

    it('REQUEST_REVISION does NOT approve execution', async () => {
      const mission = executionApprovalMission();

      const question = 'Regenerate the message.';
      const pendingDecisionResolution = resolvePendingOperatorDecision(question, mission);

      assert.equal(pendingDecisionResolution.resolved, true);
      assert.equal(pendingDecisionResolution.outcome, 'request_revision');
      assert.equal(pendingDecisionResolution.action, 'request_revision');
      assert.equal(pendingDecisionResolution.executionIntent, amo.EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH);
    });

    it('REQUEST_REVISION preserves structured pending decision through intent', async () => {
      const mission = executionApprovalMission();

      const question = 'Regenerate the message.';

      const intent = await analyzeOperatorIntent({
        question,
        mission,
        resolveMission: false,
        session: { id: 's-spec214-preserve', context: { tenantId: '10', missionId: mission.id } },
      });

      assert.ok(intent.pendingDecisionResolution);
      assert.equal(intent.pendingDecisionResolution.outcome, 'request_revision');
      assert.equal(intent.pendingDecisionResolution.action, 'request_revision');
      assert.equal(
        intent.pendingDecisionResolution.decisionKind,
        OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL
      );
    });

    it('REQUEST_REVISION routes to canonical mission execution, NOT generic cognition', async () => {
      const mission = executionApprovalMission();
      const engine = amo.createAcquisitionMissionEngine();
      const runtime = createTestAmoRuntime({ engine });

      const question = 'Regenerate the message.';

      // First, verify generic cognition would classify differently
      const { classifyOperatorCognition } = require('../../operatorCognition');
      const genericCognition = classifyOperatorCognition(question, { mission });
      assert.notEqual(genericCognition.via, 'pending_decision_revision_request');

      // Now verify that with the mission, REQUEST_REVISION takes precedence
      const intent = await analyzeOperatorIntent({
        question,
        mission,
        resolveMission: false,
        session: { id: 's-spec214-routing', context: { tenantId: '10', missionId: mission.id } },
        acquisitionMissionRuntime: runtime,
      });

      assert.equal(intent.conversationIntent.via, 'pending_decision_revision_request');
      assert.equal(intent.conversationIntent.intent, THINKING_MODES.EDIT);
      assert.equal(getOperatorIntentAuditViolations().length, 0);
    });
  });

  describe('REGRESSION — existing pending decision intents still work', () => {
    it('affirm "Yes, authorize it" still approves execution', async () => {
      const mission = executionApprovalMission();

      const question = 'Yes, authorize it.';

      const intent = await analyzeOperatorIntent({
        question,
        mission,
        resolveMission: false,
        session: { id: 's-spec214-affirm', context: { tenantId: '10', missionId: mission.id } },
      });

      assert.equal(intent.executionRequested, true);
      assert.equal(intent.conversationIntent.intent, THINKING_MODES.EXECUTE);
      assert.equal(intent.conversationIntent.via, 'pending_decision_resolved');
    });

    it('reject "No, don\'t send this" still cancels execution', async () => {
      const mission = executionApprovalMission();

      const question = "No, don't send this.";
      const pendingDecisionResolution = resolvePendingOperatorDecision(question, mission);

      assert.equal(pendingDecisionResolution.resolved, true);
      assert.equal(pendingDecisionResolution.outcome, 'reject');
      assert.equal(pendingDecisionResolution.action, 'cancel');
    });

    it('modify "Change the objective to restaurants" still requests planning', async () => {
      const mission = executionApprovalMission();

      const question = 'Change the objective to restaurants.';

      const intent = await analyzeOperatorIntent({
        question,
        mission,
        resolveMission: false,
        session: { id: 's-spec214-modify', context: { tenantId: '10', missionId: mission.id } },
      });

      assert.equal(intent.planningRequested, true);
      assert.equal(intent.conversationIntent.intent, THINKING_MODES.EDIT);
      assert.equal(intent.conversationIntent.via, 'pending_decision_modify');
    });
  });

  describe('NON-REGRESSION — ambiguous and unrelated still fall through', () => {
    it('ambiguous "Continuee" does not request execution', async () => {
      const mission = executionApprovalMission();

      const question = 'Continuee';
      const pendingDecisionResolution = resolvePendingOperatorDecision(question, mission);

      assert.equal(pendingDecisionResolution.resolved, false);
      assert.equal(pendingDecisionResolution.outcome, 'ambiguous');
    });

    it('unrelated "Today\'s briefing" does not resolve pending decision', async () => {
      const mission = executionApprovalMission();

      const question = "Tell me today's briefing";
      const pendingDecisionResolution = resolvePendingOperatorDecision(question, mission);

      assert.equal(pendingDecisionResolution.resolved, false);
      assert.equal(pendingDecisionResolution.outcome, 'unrelated');
    });
  });

  describe('SPEC-214 Section 7 — Regression Test (exact production scenario)', () => {
    it('REQUEST_REVISION reaches canonical router (SPEC-217: executionRequested = true)', async () => {
      const mission = executionApprovalMission();

      const operatorUtterance =
        'Do not authorize this outreach. The prepared messaging needs to be regenerated before execution. ' +
        'Regenerate the outreach messaging and outbound plan for these prospects, then return the updated package for my approval. ' +
        'Do not send anything.';

      const resolution = resolvePendingOperatorDecision(operatorUtterance, mission);

      // PendingDecisionResolver must produce REQUEST_REVISION
      assert.equal(resolution.resolved, true);
      assert.equal(resolution.outcome, 'request_revision');

      // OperatorIntent must produce canonical revision intent (not null, not generic cognition)
      const operatorIntent = await analyzeOperatorIntent({
        question: operatorUtterance,
        mission,
        resolveMission: false,
        session: { id: 's-spec214-regression', context: { tenantId: '10', missionId: mission.id } },
      });

      // SPEC-217: Must set executionRequested = true so it reaches the canonical router
      // (The router will dispatch to advancePreparedOutreachRevision, which regenerates but does NOT approve/send/execute)
      assert.equal(operatorIntent.executionRequested, true, 'SPEC-217: executionRequested must be true for canonical routing');
      assert.equal(operatorIntent.planningRequested, false);
      assert.equal(operatorIntent.mutatesMission, true);

      // Must produce canonical revision intent (not fall through to generic cognition)
      assert.equal(operatorIntent.conversationIntent.via, 'pending_decision_revision_request');
      assert.notEqual(operatorIntent.conversationIntent.via, 'execution_command');
      assert.notEqual(operatorIntent.conversationIntent.via, 'conversational_continue');

      // Must NOT invoke generic cognition audit violations
      assert.equal(getOperatorIntentAuditViolations().length, 0);

      // Must have the canonical execution intent preserved
      assert.ok(operatorIntent.pendingDecisionResolution);
      assert.equal(operatorIntent.pendingDecisionResolution.outcome, 'request_revision');
      assert.equal(operatorIntent.pendingDecisionResolution.executionIntent, amo.EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH);
    });
  });

  describe('SPEC-214 Section 8 — Non-Regression: Existing approval paths', () => {
    it('affirm "Yes, authorize it" → existing approval path', async () => {
      const mission = executionApprovalMission();

      const operatorUtterance = 'Yes, authorize it.';
      const resolution = resolvePendingOperatorDecision(operatorUtterance, mission);

      assert.equal(resolution.resolved, true);
      assert.equal(resolution.outcome, 'affirm');
      assert.equal(resolution.action, 'approve_execution');

      const operatorIntent = await analyzeOperatorIntent({
        question: operatorUtterance,
        mission,
        resolveMission: false,
        session: { id: 's-spec214-nr-affirm', context: { tenantId: '10', missionId: mission.id } },
      });

      assert.equal(operatorIntent.executionRequested, true);
      assert.equal(operatorIntent.conversationIntent.intent, THINKING_MODES.EXECUTE);
    });

    it('reject "No, don\'t send this" → existing reject path', async () => {
      const mission = executionApprovalMission();

      const operatorUtterance = "No, don't send this.";
      const resolution = resolvePendingOperatorDecision(operatorUtterance, mission);

      assert.equal(resolution.resolved, true);
      assert.equal(resolution.outcome, 'reject');
      assert.equal(resolution.action, 'cancel');
    });

    it('modify "Change the mission objective" → existing modify path', async () => {
      const mission = executionApprovalMission();

      const operatorUtterance = 'Change the mission objective instead.';
      const resolution = resolvePendingOperatorDecision(operatorUtterance, mission);

      assert.equal(resolution.resolved, true);
      assert.equal(resolution.outcome, 'modify');

      const operatorIntent = await analyzeOperatorIntent({
        question: operatorUtterance,
        mission,
        resolveMission: false,
        session: { id: 's-spec214-nr-modify', context: { tenantId: '10', missionId: mission.id } },
      });

      assert.equal(operatorIntent.planningRequested, true);
      assert.equal(operatorIntent.conversationIntent.intent, THINKING_MODES.EDIT);
    });
  });
});
