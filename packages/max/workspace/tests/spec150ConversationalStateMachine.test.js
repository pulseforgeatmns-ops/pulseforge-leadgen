'use strict';

/**
 * SPEC-150 — Conversational State Machine.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const {
  CONVERSATIONAL_MODES,
  getConversationalState,
  setConversationalState,
  isContinuityFollowUp,
  isExplicitSubjectChange,
  resolveContinuityIntent,
  extractCompareObjects,
  buildResolvedQuestion,
  applyConversationalContinuity,
  advanceConversationalState,
  modeFromIntent,
} = require('../ConversationalStateMachine');
const {
  CONVERSATION_SUBJECTS,
  detectConversationSubject,
} = require('../ConversationSubject');
const {
  classifyOperatorCognition,
  THINKING_MODES,
} = require('../../operatorCognition');
const {
  resolveWorkspaceOwner,
  WORKSPACE_OWNERS,
} = require('../WorkspaceOwnershipResolver');
const { createWorkspaceEngine } = require('../WorkspaceEngine');

function seedIdentityState(session, depth = 1) {
  setConversationalState(session, {
    subject: CONVERSATION_SUBJECTS.IDENTITY,
    owner: WORKSPACE_OWNERS.CONVERSATION_IDENTITY,
    activeObject: 'max',
    mode: CONVERSATIONAL_MODES.EXPLANATION,
    depth,
    objects: ['max'],
    lastQuestion: 'What is your role?',
    lastIntent: THINKING_MODES.EXPLAIN,
    lastResolvedQuestion: 'What is your role?',
    confidence: 0.97,
  });
}

describe('SPEC-150 — Conversational State Machine', () => {
  describe('continuity detection', () => {
    it('detects bare Why? as a follow-up when prior state exists', () => {
      const prior = { subject: 'identity', depth: 1 };
      assert.equal(isContinuityFollowUp('Why?', prior), true);
      assert.equal(isContinuityFollowUp('Why', prior), true);
    });

    it('detects compare follow-ups with pronoun reference', () => {
      const prior = { subject: 'identity', activeObject: 'max', depth: 2 };
      assert.equal(
        isContinuityFollowUp('How is that different from Scout?', prior),
        true
      );
    });

    it('does not treat explicit subject changes as follow-ups', () => {
      const prior = { subject: 'identity', depth: 2 };
      assert.equal(isContinuityFollowUp('What is our ICP?', prior), false);
      assert.equal(isContinuityFollowUp('Who are you?', prior), false);
      assert.equal(isExplicitSubjectChange('What is our ICP?'), true);
    });
  });

  describe('resolveContinuityIntent', () => {
    it('maps Why? to explain, not re-classified from scratch', () => {
      const prior = {
        subject: 'identity',
        activeObject: 'max',
        lastIntent: THINKING_MODES.EXPLAIN,
        depth: 2,
      };
      const resolved = resolveContinuityIntent('Why?', prior);
      assert.equal(resolved.intent, THINKING_MODES.EXPLAIN);
      assert.equal(resolved.via, 'conversation_continuity_why');
    });

    it('maps compare follow-up to compare, NOT explain', () => {
      const prior = {
        subject: 'identity',
        activeObject: 'max',
        lastIntent: THINKING_MODES.EXPLAIN,
        depth: 2,
      };
      const resolved = resolveContinuityIntent(
        'How is that different from Scout?',
        prior
      );
      assert.equal(resolved.intent, THINKING_MODES.COMPARE);
      assert.notEqual(resolved.intent, THINKING_MODES.EXPLAIN);
      assert.deepEqual(resolved.objects, ['max', 'scout']);
    });
  });

  describe('buildResolvedQuestion', () => {
    it('builds why(identity) for bare Why? follow-up', () => {
      const prior = { subject: 'identity', activeObject: 'max', depth: 2 };
      const resolved = buildResolvedQuestion('Why?', prior, {
        subject: 'identity',
        intent: THINKING_MODES.EXPLAIN,
      });
      assert.equal(resolved, 'why(identity)');
    });

    it('builds compare(max,scout) for specialist comparison', () => {
      const prior = { subject: 'identity', activeObject: 'max', depth: 2 };
      const resolved = buildResolvedQuestion(
        'How is that different from Scout?',
        prior,
        {
          subject: 'identity',
          intent: THINKING_MODES.COMPARE,
          objects: ['max', 'scout'],
        }
      );
      assert.equal(resolved, 'compare(max,scout)');
    });
  });

  describe('applyConversationalContinuity', () => {
    it('inherits identity subject for Why? instead of default business', () => {
      const session = { context: {} };
      seedIdentityState(session, 2);

      const rawSubject = detectConversationSubject('Why?', null, session);
      const rawIntent = classifyOperatorCognition('Why?');
      assert.notEqual(rawSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);
      assert.notEqual(rawIntent.via, 'conversation_continuity_why');

      const continuity = applyConversationalContinuity({
        question: 'Why?',
        session,
        conversationSubject: rawSubject,
        conversationIntent: rawIntent,
      });

      assert.equal(continuity.applied, true);
      assert.equal(continuity.conversationSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);
      assert.equal(continuity.conversationIntent.intent, THINKING_MODES.OPERATING_MODEL);
      assert.equal(continuity.conversationIntent.thinkingMode, 'operating_model_reflection');
      assert.equal(continuity.conversationIntent.underlyingIntent, THINKING_MODES.EXPLAIN);
      assert.equal(continuity.resolvedQuestion, 'why(identity)');
      assert.equal(continuity.conversationIntent.continuity, true);
    });

    it('inherits identity subject and compare intent for Scout comparison', () => {
      const session = { context: {} };
      seedIdentityState(session, 2);

      const rawSubject = detectConversationSubject(
        'How is that different from Scout?',
        null,
        session
      );
      const rawIntent = classifyOperatorCognition('How is that different from Scout?');

      const continuity = applyConversationalContinuity({
        question: 'How is that different from Scout?',
        session,
        conversationSubject: rawSubject,
        conversationIntent: rawIntent,
      });

      assert.equal(continuity.applied, true);
      assert.equal(continuity.conversationSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);
      assert.equal(continuity.conversationIntent.intent, THINKING_MODES.OPERATING_MODEL);
      assert.equal(continuity.conversationIntent.thinkingMode, 'operating_model_reflection');
      assert.equal(continuity.conversationIntent.underlyingIntent, THINKING_MODES.COMPARE);
      assert.deepEqual(continuity.compareObjects, ['max', 'scout']);
      assert.equal(continuity.resolvedQuestion, 'compare(max,scout)');
    });
  });

  describe('advanceConversationalState', () => {
    it('maintains state shape across turns', () => {
      const session = { context: {} };

      const afterFirst = advanceConversationalState(session, {
        question: 'What is your role?',
        conversationSubject: detectConversationSubject('What is your role?'),
        conversationIntent: classifyOperatorCognition('What is your role?'),
        workspaceOwnership: {
          owner: WORKSPACE_OWNERS.CONVERSATION_IDENTITY,
          reason: 'role_question',
        },
        resolvedQuestion: 'What is your role?',
      });

      assert.equal(afterFirst.subject, CONVERSATION_SUBJECTS.IDENTITY);
      assert.equal(afterFirst.owner, WORKSPACE_OWNERS.CONVERSATION_IDENTITY);
      assert.equal(afterFirst.activeObject, 'max');
      assert.equal(afterFirst.mode, CONVERSATIONAL_MODES.EXPLANATION);
      assert.equal(afterFirst.depth, 1);

      const continuity = applyConversationalContinuity({
        question: 'Why?',
        session,
        conversationSubject: detectConversationSubject('Why?'),
        conversationIntent: classifyOperatorCognition('Why?'),
      });

      const afterSecond = advanceConversationalState(session, {
        question: 'Why?',
        conversationSubject: continuity.conversationSubject,
        conversationIntent: continuity.conversationIntent,
        workspaceOwnership: {
          owner: WORKSPACE_OWNERS.CONVERSATION_IDENTITY,
          reason: 'conversation_continuity',
        },
        resolvedQuestion: continuity.resolvedQuestion,
        continuityApplied: true,
      });

      assert.equal(afterSecond.subject, CONVERSATION_SUBJECTS.IDENTITY);
      assert.equal(afterSecond.activeObject, 'max');
      assert.equal(afterSecond.mode, CONVERSATIONAL_MODES.OPERATING_MODEL_REFLECTION);
      assert.equal(afterSecond.depth, 2);
      assert.equal(getConversationalState(session).depth, 2);
    });
  });

  describe('WorkspaceEngine multi-turn integration', () => {
    let workspace;

    beforeEach(() => {
      workspace = createWorkspaceEngine({
        missionsEnabled: false,
        resolverEnabled: false,
        disableLlm: true,
      });
    });

    it('identity → Why? → compare Scout maintains conversational continuity', async () => {
      const opened = await workspace.open({ tenantId: '10' });
      const sessionId = opened.sessionId;

      const turn1 = await workspace.ask({
        sessionId,
        question: 'What is your role?',
        context: { tenantId: '10' },
      });

      assert.equal(turn1.conversationalState.subject, CONVERSATION_SUBJECTS.IDENTITY);
      assert.equal(turn1.conversationalState.activeObject, 'max');
      assert.equal(turn1.conversationalState.depth, 1);

      const turn2 = await workspace.ask({
        sessionId,
        question: 'Why?',
        context: { tenantId: '10' },
      });

      assert.equal(turn2.conversationSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);
      assert.equal(turn2.conversationIntent.intent, THINKING_MODES.OPERATING_MODEL);
      assert.equal(turn2.conversationIntent.thinkingMode, 'operating_model_reflection');
      assert.equal(turn2.conversationIntent.underlyingIntent, THINKING_MODES.EXPLAIN);
      assert.equal(turn2.conversationIntent.continuity, true);
      assert.match(turn2.resolvedQuestion, /^claim_why\(|^why\(identity\)$/);
      assert.equal(turn2.conversationalState.depth, 2);
      assert.equal(turn2.routingTrace.continuity, true);

      const turn3 = await workspace.ask({
        sessionId,
        question: 'How is that different from Scout?',
        context: { tenantId: '10' },
      });

      assert.equal(turn3.conversationSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);
      assert.equal(turn3.conversationIntent.intent, THINKING_MODES.OPERATING_MODEL);
      assert.equal(turn3.conversationIntent.underlyingIntent, THINKING_MODES.COMPARE);
      assert.equal(turn3.resolvedQuestion, 'compare(max,scout)');
      assert.equal(turn3.conversationalState.depth, 3);
      assert.equal(turn3.conversationalState.mode, CONVERSATIONAL_MODES.OPERATING_MODEL_REFLECTION);
    });

    it('explicit topic change resets depth on new subject', async () => {
      const opened = await workspace.open({ tenantId: '10' });
      const sessionId = opened.sessionId;

      await workspace.ask({
        sessionId,
        question: 'What is your role?',
        context: { tenantId: '10' },
      });

      const icpTurn = await workspace.ask({
        sessionId,
        question: 'What is our ICP?',
        context: { tenantId: '10' },
      });

      assert.equal(icpTurn.conversationSubject.subject, CONVERSATION_SUBJECTS.BUSINESS);
      assert.equal(icpTurn.conversationalState.subject, CONVERSATION_SUBJECTS.BUSINESS);
      assert.equal(icpTurn.conversationalState.depth, 1);
    });

    it('continuity follow-up routes to identity owner, not business pipeline', async () => {
      const opened = await workspace.open({ tenantId: '10' });
      const sessionId = opened.sessionId;

      await workspace.ask({
        sessionId,
        question: 'What is your role?',
        context: { tenantId: '10' },
      });

      const whyTurn = await workspace.ask({
        sessionId,
        question: 'Why?',
        context: { tenantId: '10' },
      });

      assert.equal(whyTurn.workspaceOwnership.owner, WORKSPACE_OWNERS.CONVERSATION_IDENTITY);
      assert.equal(whyTurn.routingTrace.owner, WORKSPACE_OWNERS.CONVERSATION_IDENTITY);
      assert.match(whyTurn.prose, /operating system|Max/i);
      assert.doesNotMatch(whyTurn.prose, /ideal customer|ICP|Blueprint/i);
    });
  });

  describe('mode mapping', () => {
    it('maps compare intent to comparison mode', () => {
      assert.equal(modeFromIntent(THINKING_MODES.COMPARE), CONVERSATIONAL_MODES.COMPARISON);
    });

    it('maps reflect intent to reflection mode', () => {
      assert.equal(modeFromIntent(THINKING_MODES.REFLECT), CONVERSATIONAL_MODES.REFLECTION);
    });
  });
});
