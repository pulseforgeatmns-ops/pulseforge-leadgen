'use strict';

/**
 * SPEC-156 — Reasoning Operator Engine acceptance tests.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const {
  OPERATOR_IDS,
  dispatchReasoningOperator,
  executeReasoning,
  executeReasoningOperator,
  verbalizeReasoningResult,
  computeReasoningDepth,
  clearReasoningHistory,
  getReasoningHistory,
  detectExplicitOperator,
} = require('../ReasoningOperatorEngine');
const {
  createInitialArc,
  setActiveReasoningContext,
  FOLLOW_UP_TYPES,
} = require('../ActiveReasoningContext');
const { REASONING_GOALS } = require('../../reasoning/ConceptGraph/ConceptPlanner');
const { CONVERSATION_SUBJECTS } = require('../ConversationSubject');
const { composeIdentityReasoning } = require('../../identity/IdentityReasoning');
const { createWorkspaceEngine } = require('../WorkspaceEngine');

function seedSession() {
  const session = { context: {} };
  const arc = createInitialArc({
    goal: REASONING_GOALS.EXPLAIN_IDENTITY,
    subject: CONVERSATION_SUBJECTS.IDENTITY,
  });
  setActiveReasoningContext(session, arc);
  clearReasoningHistory(session);
  return { session, arc };
}

function runTurn(session, arc, question, arcFollowUp = null) {
  return executeReasoning({
    session,
    arc,
    question,
    arcFollowUp,
  });
}

describe('SPEC-156 — Reasoning Operator Engine', () => {
  describe('operator dispatch', () => {
    it('never falls back to Explain for explicit assumption questions', () => {
      const dispatch = dispatchReasoningOperator({
        question: 'What assumption is that based on?',
        arcFollowUp: { type: FOLLOW_UP_TYPES.WHY },
      });
      assert.equal(dispatch.operatorId, OPERATOR_IDS.SURFACE_ASSUMPTIONS);
      assert.equal(dispatch.source, 'explicit_operator');
    });

    it('maps Why? to Justify via ARC follow-up', () => {
      const dispatch = dispatchReasoningOperator({
        question: 'Why?',
        arcFollowUp: { type: FOLLOW_UP_TYPES.WHY },
      });
      assert.equal(dispatch.operatorId, OPERATOR_IDS.JUSTIFY);
    });

    it('maps compare questions to Compare operator', () => {
      const dispatch = dispatchReasoningOperator({
        question: 'Compared to Scout?',
        arcFollowUp: { type: FOLLOW_UP_TYPES.COMPARE },
      });
      assert.equal(dispatch.operatorId, OPERATOR_IDS.COMPARE);
    });

    it('detects explicit operators from question text', () => {
      assert.equal(detectExplicitOperator('Summarize how your reasoning evolved.'), OPERATOR_IDS.SUMMARIZE);
      assert.equal(detectExplicitOperator('Earlier you said Max coordinates specialists.'), OPERATOR_IDS.REFLECT);
      assert.equal(detectExplicitOperator('Could that assumption fail?'), OPERATOR_IDS.COUNTERFACTUAL);
    });
  });

  describe('reasoning depth', () => {
    it('increments depth across recursive Justify operations', () => {
      const { session, arc } = seedSession();

      const turn1 = runTurn(session, arc, 'What is your role?');
      assert.equal(turn1.depth, 0);

      const turn2 = runTurn(session, arc, 'Why?', { type: FOLLOW_UP_TYPES.WHY });
      assert.equal(turn2.depth, 1);

      const turn3 = runTurn(session, arc, 'Why is that necessary?', { type: FOLLOW_UP_TYPES.WHY });
      assert.equal(turn3.depth, 2);
    });

    it('assigns assumption depth at layer 3+', () => {
      const { session, arc } = seedSession();
      runTurn(session, arc, 'Why?', { type: FOLLOW_UP_TYPES.WHY });
      runTurn(session, arc, 'Why?', { type: FOLLOW_UP_TYPES.WHY });

      const assumptionTurn = executeReasoningOperator({
        session,
        arc,
        question: 'What assumption is that based on?',
        operatorId: OPERATOR_IDS.SURFACE_ASSUMPTIONS,
      });
      assert.ok(assumptionTurn.depth >= 3);
    });
  });

  describe('ReasoningResult structure', () => {
    it('every operator produces transformedClaim, evidenceUsed, assumptions, confidence', () => {
      const { session, arc } = seedSession();
      const result = runTurn(session, arc, 'Why?', { type: FOLLOW_UP_TYPES.WHY });

      assert.ok(result.operator);
      assert.ok(result.operator.id);
      assert.ok(result.transformedClaim);
      assert.ok(Array.isArray(result.evidenceUsed));
      assert.ok(Array.isArray(result.assumptions));
      assert.ok(typeof result.confidence === 'number');
      assert.ok(result.arcDelta);
    });

    it('verbalizeReasoningResult returns prose without re-reasoning', () => {
      const { session, arc } = seedSession();
      const result = runTurn(session, arc, 'Why?', { type: FOLLOW_UP_TYPES.WHY });
      const prose = verbalizeReasoningResult(result);
      assert.equal(prose, result.transformedClaim);
    });
  });

  describe('acceptance test 1 — deepening Why chain', () => {
    it('Role → Why → Why → Why — each answer deepens, no repetition', () => {
      const { session, arc } = seedSession();

      const role = runTurn(session, arc, 'What is your role?');
      const why1 = runTurn(session, arc, 'Why?', { type: FOLLOW_UP_TYPES.WHY });
      const why2 = runTurn(session, arc, 'Why is that necessary?', { type: FOLLOW_UP_TYPES.WHY });
      const why3 = runTurn(session, arc, 'Why?', { type: FOLLOW_UP_TYPES.WHY });

      assert.ok(role.transformedClaim);
      assert.ok(why1.transformedClaim);
      assert.ok(why2.transformedClaim);
      assert.ok(why3.transformedClaim);

      assert.notEqual(why1.transformedClaim, why2.transformedClaim);
      assert.notEqual(why2.transformedClaim, why3.transformedClaim);

      assert.equal(why1.operator.id, OPERATOR_IDS.JUSTIFY);
      assert.ok(why2.depth > why1.depth);
      assert.ok(why3.depth > why2.depth);

      assert.match(why1.transformedClaim, /because|holds|role|operating/i);
      assert.match(why2.transformedClaim, /depth|necessary|layer/i);
    });
  });

  describe('acceptance test 2 — assumption → counterfactual → revise', () => {
    it('What assumption? → Could it fail? → Would that change your conclusion?', () => {
      const { session, arc } = seedSession();
      runTurn(session, arc, 'Why?', { type: FOLLOW_UP_TYPES.WHY });
      runTurn(session, arc, 'Why?', { type: FOLLOW_UP_TYPES.WHY });

      const assumptions = runTurn(session, arc, 'What assumption is that based on?');
      const counter = runTurn(session, arc, 'Could that assumption fail?');
      const revise = runTurn(session, arc, 'Would that change your conclusion?');

      assert.equal(assumptions.operator.id, OPERATOR_IDS.SURFACE_ASSUMPTIONS);
      assert.equal(counter.operator.id, OPERATOR_IDS.COUNTERFACTUAL);
      assert.equal(revise.operator.id, OPERATOR_IDS.REVISE);

      assert.match(assumptions.transformedClaim, /assumption/i);
      assert.match(counter.transformedClaim, /Suppose|If that assumption/i);
      assert.match(revise.transformedClaim, /Revised|conclusion/i);
    });
  });

  describe('acceptance test 3 — compare → generalize → summarize', () => {
    it('Scout → Compare Paige → Generalize → Summarize — correct operator chain', () => {
      const { session, arc } = seedSession();

      runTurn(session, arc, 'Tell me about Scout.');
      const compare = runTurn(session, arc, 'Compare to Paige.', { type: FOLLOW_UP_TYPES.COMPARE });
      const generalize = runTurn(session, arc, 'Generalize that pattern.');
      const summarize = runTurn(session, arc, 'Summarize how your reasoning evolved.');

      assert.equal(compare.operator.id, OPERATOR_IDS.COMPARE);
      assert.equal(generalize.operator.id, OPERATOR_IDS.GENERALIZE);
      assert.equal(summarize.operator.id, OPERATOR_IDS.SUMMARIZE);

      assert.match(compare.transformedClaim, /scout|paige|owns|optimizes/i);
      assert.match(generalize.transformedClaim, /abstraction|higher level|pattern/i);
      assert.match(summarize.transformedClaim, /Summary|reasoning chain/i);
    });
  });

  describe('acceptance test 4 — reflect → revise', () => {
    it('Earlier you said... Do you still believe that? — Reflect then Revise', () => {
      const { session, arc } = seedSession();
      runTurn(session, arc, 'What is your role?');
      runTurn(session, arc, 'Why?', { type: FOLLOW_UP_TYPES.WHY });

      const reflect = runTurn(session, arc, 'Earlier you said Max coordinates specialists.');
      const revise = runTurn(session, arc, 'Do you still believe that?');

      assert.equal(reflect.operator.id, OPERATOR_IDS.REFLECT);
      assert.equal(revise.operator.id, OPERATOR_IDS.REVISE);

      assert.match(reflect.transformedClaim, /Evolution|Step|reasoning/i);
      assert.match(revise.transformedClaim, /Revised|conclusion|believe/i);
    });
  });

  describe('acceptance test 5 — evolving reasoning chain', () => {
    it('Explain → Challenge → Counterfactual → Revise → Summarize', () => {
      const { session, arc } = seedSession();

      const explain = executeReasoningOperator({
        session, arc, question: 'Explain your role.', operatorId: OPERATOR_IDS.EXPLAIN,
      });
      const challenge = executeReasoningOperator({
        session, arc, question: 'What is wrong with that?', operatorId: OPERATOR_IDS.CHALLENGE,
      });
      const counter = executeReasoningOperator({
        session, arc, question: 'Suppose that assumption failed.', operatorId: OPERATOR_IDS.COUNTERFACTUAL,
      });
      const revise = executeReasoningOperator({
        session, arc, question: 'Does that change your conclusion?', operatorId: OPERATOR_IDS.REVISE,
      });
      const summarize = executeReasoningOperator({
        session, arc, question: 'Summarize the chain.', operatorId: OPERATOR_IDS.SUMMARIZE,
      });

      const claims = [
        explain.transformedClaim,
        challenge.transformedClaim,
        counter.transformedClaim,
        revise.transformedClaim,
        summarize.transformedClaim,
      ];

      const unique = new Set(claims);
      assert.equal(unique.size, 5, 'each operator produces distinct transformed output');

      const history = getReasoningHistory(session);
      assert.equal(history.length, 5);
      assert.deepEqual(
        history.map((h) => h.operator),
        [
          OPERATOR_IDS.EXPLAIN,
          OPERATOR_IDS.CHALLENGE,
          OPERATOR_IDS.COUNTERFACTUAL,
          OPERATOR_IDS.REVISE,
          OPERATOR_IDS.SUMMARIZE,
        ]
      );
    });
  });

  describe('IdentityReasoning integration', () => {
    it('composeIdentityReasoning routes ARC-bound follow-ups through ROE', () => {
      const { session, arc } = seedSession();

      const prose = composeIdentityReasoning({
        question: 'Why?',
        resolvedQuestion: 'claim_why(identity)',
        conversationIntent: { continuity: true, intent: 'operating_model' },
        activeReasoningContext: arc,
        arcFollowUp: { type: FOLLOW_UP_TYPES.WHY },
        session,
      });

      assert.ok(prose);
      assert.match(prose, /because|holds|operating|specialist/i);
    });
  });

  describe('E2E workspace — success criteria conversation', () => {
    let workspace;

    beforeEach(() => {
      workspace = createWorkspaceEngine({
        missionsEnabled: false,
        resolverEnabled: false,
        disableLlm: true,
      });
    });

    it('full reasoning evolution chain produces deepening responses', async () => {
      const opened = await workspace.open({ tenantId: '10' });
      const sessionId = opened.sessionId;

      const questions = [
        'What is your role?',
        'Why?',
        'Why is that necessary?',
        'What assumption is that based on?',
        'Could that assumption fail?',
        'If it failed, what would change?',
        'Does that change your conclusion?',
        'Summarize how your reasoning evolved.',
      ];

      const responses = [];
      for (const question of questions) {
        const turn = await workspace.ask({
          sessionId,
          question,
          context: { tenantId: '10' },
        });
        assert.ok(turn.prose, `expected prose for: ${question}`);
        responses.push(turn.prose);
      }

      const unique = new Set(responses);
      assert.ok(unique.size >= 6, 'responses should not collapse to repetition');

      assert.match(responses[1], /because|holds|specialist|operating/i);
      assert.match(responses[3], /assumption/i);
      assert.match(responses[7], /summary|reasoning|evolved/i);
    });
  });
});
