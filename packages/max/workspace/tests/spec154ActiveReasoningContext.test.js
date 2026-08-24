'use strict';

/**
 * SPEC-154 — Active Reasoning Context.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const {
  createInitialArc,
  applyActiveReasoningContinuity,
  advanceActiveReasoningContext,
  computeArcDelta,
  applyArcDelta,
  getActiveReasoningContext,
  setActiveReasoningContext,
  parseArcResolvedQuestion,
  classifyArcFollowUp,
  detectConversationGoalChange,
  synthesizeFromArc,
  FOLLOW_UP_TYPES,
  REASONING_CHAIN_NODES,
} = require('../ActiveReasoningContext');
const { REASONING_GOALS } = require('../../reasoning/ConceptGraph/ConceptPlanner');
const {
  composeIdentityReasoning,
  shouldUseOperatingModelReasoning,
} = require('../../identity/IdentityReasoning');
const { analyzeOperatorIntent } = require('../OperatorIntent');
const {
  setConversationalState,
  CONVERSATIONAL_MODES,
} = require('../ConversationalStateMachine');
const {
  CONVERSATION_SUBJECTS,
} = require('../ConversationSubject');
const {
  WORKSPACE_OWNERS,
} = require('../WorkspaceOwnershipResolver');
const { THINKING_MODES } = require('../../operatorCognition');
const { createWorkspaceEngine } = require('../WorkspaceEngine');

function seedGovernanceArc(session) {
  const arc = createInitialArc({
    goal: REASONING_GOALS.RESOLVE_CONFLICT,
    subject: CONVERSATION_SUBJECTS.IDENTITY,
    specialists: ['scout', 'paige'],
  });
  setActiveReasoningContext(session, arc);
  setConversationalState(session, {
    subject: CONVERSATION_SUBJECTS.IDENTITY,
    owner: WORKSPACE_OWNERS.CONVERSATION_IDENTITY,
    activeObject: 'max',
    mode: CONVERSATIONAL_MODES.CONCEPT_GRAPH_REASONING,
    depth: 2,
    lastQuestion: 'If Scout and Paige disagreed, what would you do?',
    lastIntent: THINKING_MODES.OPERATING_MODEL,
    confidence: 0.95,
  });
  return arc;
}

function seedIdentityArc(session) {
  const arc = createInitialArc({
    goal: REASONING_GOALS.EXPLAIN_IDENTITY,
    subject: CONVERSATION_SUBJECTS.IDENTITY,
  });
  setActiveReasoningContext(session, arc);
  setConversationalState(session, {
    subject: CONVERSATION_SUBJECTS.IDENTITY,
    owner: WORKSPACE_OWNERS.CONVERSATION_IDENTITY,
    activeObject: 'max',
    mode: CONVERSATIONAL_MODES.EXPLANATION,
    depth: 1,
    lastQuestion: 'What is your role?',
    lastIntent: THINKING_MODES.EXPLAIN,
    confidence: 0.97,
  });
  return arc;
}

describe('SPEC-154 — Active Reasoning Context', () => {
  describe('ARC structure', () => {
    it('creates primaryClaim, supportingClaims, assumptions, openQuestions, reasoningChain', () => {
      const arc = createInitialArc({ goal: REASONING_GOALS.RESOLVE_CONFLICT, specialists: ['scout', 'paige'] });
      assert.ok(arc.primaryClaim);
      assert.ok(Array.isArray(arc.supportingClaims));
      assert.ok(arc.supportingClaims.length >= 2);
      assert.ok(Array.isArray(arc.assumptions));
      assert.ok(Array.isArray(arc.openQuestions));
      assert.ok(arc.conversationGoal);
      assert.deepEqual(arc.reasoningChain, [REASONING_CHAIN_NODES.GOVERNANCE]);
      assert.ok(arc.createdAt);
      assert.ok(arc.updatedAt);
    });
  });

  describe('follow-up binding', () => {
    it('binds Why? to active primaryClaim instead of why(identity)', () => {
      const session = { context: {} };
      seedGovernanceArc(session);

      const result = applyActiveReasoningContinuity({
        question: 'Why?',
        session,
        priorState: session.conversationalState,
        continuityApplied: true,
      });

      assert.equal(result.applied, true);
      assert.match(result.resolvedQuestion, /^claim_why\(/);
      assert.match(result.bindToClaim, /operator retains final authority/i);
    });

    it('classifies governance follow-up types', () => {
      assert.equal(classifyArcFollowUp('Why?').type, FOLLOW_UP_TYPES.WHY);
      assert.equal(classifyArcFollowUp('How?').type, FOLLOW_UP_TYPES.HOW);
      assert.equal(classifyArcFollowUp('What if Scout disagreed?').type, FOLLOW_UP_TYPES.CHALLENGE);
      assert.equal(classifyArcFollowUp('Why not?').type, FOLLOW_UP_TYPES.WHY_NOT);
      assert.equal(classifyArcFollowUp('Compared to Scout?').type, FOLLOW_UP_TYPES.COMPARE);
    });

    it('parseArcResolvedQuestion decodes claim bindings', () => {
      assert.deepEqual(parseArcResolvedQuestion('claim_why(governance)'), {
        kind: 'claim_why',
        node: 'governance',
      });
    });
  });

  describe('synthesis from ARC', () => {
    it('explains governance claim on Why? — not identity restart', () => {
      const arc = createInitialArc({
        goal: REASONING_GOALS.RESOLVE_CONFLICT,
        specialists: ['scout', 'paige'],
      });
      const prose = synthesizeFromArc(arc, FOLLOW_UP_TYPES.WHY);
      assert.match(prose, /Neither Scout nor Paige|each optimizes one domain/i);
      assert.match(prose, /final authority remains yours|operator retains final authority/i);
      assert.match(prose, /surface those tradeoffs|surface the tradeoff/i);
    });

    it('composeIdentityReasoning uses ARC for bound follow-ups', () => {
      const session = { context: {} };
      const arc = seedGovernanceArc(session);

      const prose = composeIdentityReasoning({
        question: 'Why?',
        resolvedQuestion: 'claim_why(governance)',
        conversationIntent: { continuity: true, intent: THINKING_MODES.OPERATING_MODEL },
        activeReasoningContext: arc,
        arcFollowUp: { type: FOLLOW_UP_TYPES.WHY },
        session,
      });

      assert.ok(prose);
      assert.match(prose, /Neither Scout nor Paige|final authority/i);
      assert.doesNotMatch(prose, /Blueprint|ICP/i);
    });
  });

  describe('ARC delta', () => {
    it('extends reasoningChain without replacing prior nodes', () => {
      const prior = createInitialArc({ goal: REASONING_GOALS.EXPLAIN_IDENTITY });
      const delta = computeArcDelta({
        priorArc: prior,
        goal: REASONING_GOALS.COMPARE_ROLES,
        specialists: ['max', 'scout'],
      });
      const next = applyArcDelta(prior, delta);
      assert.ok(next.reasoningChain.includes(REASONING_CHAIN_NODES.IDENTITY));
      assert.ok(next.reasoningChain.includes(REASONING_CHAIN_NODES.COMPARISON));
      assert.ok(next.supportingClaims.some((c) => c.includes('coordinates specialists')));
    });
  });

  describe('goal change detection', () => {
    it('resets ARC on explicit topic change', () => {
      const session = { context: {} };
      const arc = seedIdentityArc(session);
      const change = detectConversationGoalChange("Enough about Max. Let's talk about Anchor.", arc, {
        subject: CONVERSATION_SUBJECTS.IDENTITY,
      });
      assert.equal(change.changed, true);
      assert.equal(change.reset, true);
    });
  });

  describe('OperatorIntent integration', () => {
    it('analyzeOperatorIntent binds Why? to ARC claim', async () => {
      const session = { context: {} };
      seedGovernanceArc(session);

      const intent = await analyzeOperatorIntent({
        question: 'Why?',
        session,
        resolveMission: false,
      });

      assert.match(intent.resolvedQuestion, /^claim_why\(/);
      assert.ok(intent.activeReasoningContext);
      assert.equal(intent.arcFollowUp.type, FOLLOW_UP_TYPES.WHY);
    });
  });

  describe('acceptance — identity chain', () => {
    let workspace;

    beforeEach(() => {
      workspace = createWorkspaceEngine({
        missionsEnabled: false,
        resolverEnabled: false,
        disableLlm: true,
      });
    });

    it('What is your role? → Why? → compare Scout → Why? — no identity restart', async () => {
      const opened = await workspace.open({ tenantId: '10' });
      const sessionId = opened.sessionId;

      const turn1 = await workspace.ask({
        sessionId,
        question: 'What is your role?',
        context: { tenantId: '10' },
      });
      assert.equal(turn1.conversationSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);
      assert.ok(turn1.activeReasoningContext);
      assert.ok(turn1.activeReasoningContext.primaryClaim);

      const turn2 = await workspace.ask({ sessionId, question: 'Why?', context: { tenantId: '10' } });
      assert.match(turn2.resolvedQuestion, /^claim_why\(/);
      assert.equal(turn2.conversationSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);
      assert.match(turn2.prose, /specialist sees the entire business|integrate competing evidence|purpose is to/i);
      assert.doesNotMatch(turn2.prose, /Blueprint|ICP/i);

      const turn3 = await workspace.ask({
        sessionId,
        question: 'How is that different from Scout?',
        context: { tenantId: '10' },
      });
      assert.equal(turn3.conversationSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);
      assert.match(turn3.prose, /Scout/i);
      assert.match(turn3.prose, /specializ|integrat/i);

      const turn4 = await workspace.ask({ sessionId, question: 'Why?', context: { tenantId: '10' } });
      assert.match(turn4.resolvedQuestion, /^claim_why\(/);
      assert.match(turn4.prose, /Scout|specializ|integrat|Max/i);
      assert.doesNotMatch(turn4.prose, /Blueprint|ICP/i);
    });
  });

  describe('acceptance — governance chain', () => {
    let workspace;

    beforeEach(() => {
      workspace = createWorkspaceEngine({
        missionsEnabled: false,
        resolverEnabled: false,
        disableLlm: true,
      });
    });

    it('If Scout and Paige disagree → Why? explains governance claim', async () => {
      const opened = await workspace.open({ tenantId: '10' });
      const sessionId = opened.sessionId;

      const turn1 = await workspace.ask({
        sessionId,
        question: 'If Scout and Paige disagreed, what would you do?',
        context: { tenantId: '10' },
      });
      assert.equal(turn1.conversationSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);
      assert.match(turn1.prose, /disagree|operator|authority|tradeoff/i);

      const turn2 = await workspace.ask({ sessionId, question: 'Why?', context: { tenantId: '10' } });
      assert.match(turn2.resolvedQuestion, /^claim_why\(/);
      assert.match(turn2.prose, /Neither Scout nor Paige|each optimizes one domain/i);
      assert.match(turn2.prose, /final authority remains yours|operator retains final authority/i);
      assert.doesNotMatch(turn2.prose, /Blueprint|ICP/i);
    });

    it('Why not let Scout decide? reasons from governance claim', async () => {
      const opened = await workspace.open({ tenantId: '10' });
      const sessionId = opened.sessionId;

      await workspace.ask({
        sessionId,
        question: 'If Scout and Paige disagreed, what would you do?',
        context: { tenantId: '10' },
      });

      const turn = await workspace.ask({
        sessionId,
        question: 'Why not let Scout decide?',
        context: { tenantId: '10' },
      });
      assert.match(turn.prose, /operator|authority|domain|whole business/i);
      assert.doesNotMatch(turn.prose, /Blueprint|ICP/i);
    });
  });

  describe('acceptance — full success criteria chain', () => {
    let workspace;

    beforeEach(() => {
      workspace = createWorkspaceEngine({
        missionsEnabled: false,
        resolverEnabled: false,
        disableLlm: true,
      });
    });

    it('ten-step Why? chain binds to preceding proposition throughout', async () => {
      const opened = await workspace.open({ tenantId: '10' });
      const sessionId = opened.sessionId;

      const questions = [
        'What is your role?',
        'Why?',
        'How is that different from Scout?',
        'Why?',
        'What if Scout disagreed?',
        'Why?',
        'Who decides?',
        'Why?',
        'When would you disagree with me?',
        'Why?',
      ];

      let priorClaim = null;
      for (const question of questions) {
        const turn = await workspace.ask({ sessionId, question, context: { tenantId: '10' } });
        assert.equal(turn.conversationSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);
        assert.doesNotMatch(turn.prose, /Blueprint|ICP|ideal customer/i);

        if (/^why\b/i.test(question) && priorClaim) {
          assert.match(turn.resolvedQuestion, /^claim_why\(/);
          assert.ok(turn.activeReasoningContext);
        }

        if (turn.activeReasoningContext && turn.activeReasoningContext.primaryClaim) {
          priorClaim = turn.activeReasoningContext.primaryClaim;
        }
      }

      assert.ok(priorClaim);
      assert.equal(
        shouldUseOperatingModelReasoning({
          question: 'Why?',
          resolvedQuestion: 'claim_why(authority)',
          activeReasoningContext: { primaryClaim: priorClaim },
        }),
        true
      );
    });
  });

  describe('acceptance — topic change resets ARC', () => {
    it('advances and clears ARC on goal change', () => {
      const session = { context: {} };
      const arc = createInitialArc({ goal: REASONING_GOALS.EXPLAIN_IDENTITY });
      setActiveReasoningContext(session, arc);

      advanceActiveReasoningContext(session, {
        question: "Enough about Max. Let's talk about Anchor.",
        conversationSubject: { subject: CONVERSATION_SUBJECTS.BUSINESS },
        structured: { metadata: { goal: REASONING_GOALS.EXPLAIN_IDENTITY } },
      });

      const goalChange = applyActiveReasoningContinuity({
        question: "Enough about Max. Let's talk about Anchor.",
        session,
        priorState: { subject: CONVERSATION_SUBJECTS.BUSINESS },
        continuityApplied: false,
      });

      assert.equal(goalChange.goalChanged, true);
    });
  });
});
