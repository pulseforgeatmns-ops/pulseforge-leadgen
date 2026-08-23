'use strict';

/**
 * SPEC-151 — Max Operating Model Reasoning.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const { OPERATING_MODEL } = require('../../identity/OperatingModel');
const {
  planOperatingModelQuery,
  parseResolvedQuestion,
  classifyDirectQuestion,
  shouldUseOperatingModelReasoning,
  composeIdentityReasoning,
  REASONING_TARGETS,
} = require('../../identity/IdentityReasoning');
const { maybeHandleIdentityTurn } = require('../IdentityConversationContext');
const {
  detectConversationSubject,
} = require('../ConversationSubject');
const {
  applyConversationalContinuity,
  setConversationalState,
  CONVERSATIONAL_MODES,
} = require('../ConversationalStateMachine');
const {
  classifyOperatorCognition,
  THINKING_MODES,
} = require('../../operatorCognition');
const {
  resolveWorkspaceOwner,
  WORKSPACE_OWNERS,
} = require('../WorkspaceOwnershipResolver');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const { CONVERSATION_SUBJECTS } = require('../ConversationSubject');

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

describe('SPEC-151 — Max Operating Model Reasoning', () => {
  describe('OperatingModel knowledge structure', () => {
    it('defines structured role, purpose, principles, why, boundaries, authority, relationships, failureModes', () => {
      assert.equal(OPERATING_MODEL.role.title, 'Business Operating System');
      assert.ok(OPERATING_MODEL.purpose.length >= 5);
      assert.ok(OPERATING_MODEL.principles.length >= 8);
      assert.ok(OPERATING_MODEL.why.length >= 5);
      assert.ok(OPERATING_MODEL.boundaries.length >= 5);
      assert.ok(OPERATING_MODEL.authority.operator.length >= 3);
      assert.ok(OPERATING_MODEL.authority.max.length >= 3);
      assert.ok(OPERATING_MODEL.relationships.scout);
      assert.ok(OPERATING_MODEL.relationships.paige);
      assert.ok(OPERATING_MODEL.relationships.rex);
      assert.ok(OPERATING_MODEL.failureModes.length >= 4);
    });

    it('stores architectural reasoning in why — not generated prose', () => {
      const whyText = OPERATING_MODEL.why.join(' ');
      assert.match(whyText, /No single specialist sees the entire business/i);
      assert.match(whyText, /Scout understands markets/i);
      assert.match(whyText, /integrate competing evidence/i);
    });
  });

  describe('IdentityReasoning query planning', () => {
    it('maps why(identity) to why target', () => {
      const parsed = parseResolvedQuestion('why(identity)');
      assert.equal(parsed.target, REASONING_TARGETS.WHY);
      assert.equal(parsed.subject, 'identity');
    });

    it('maps compare(max,scout) to compare target', () => {
      const parsed = parseResolvedQuestion('compare(max,scout)');
      assert.equal(parsed.target, REASONING_TARGETS.COMPARE);
      assert.deepEqual(parsed.objects, ['max', 'scout']);
    });

    it('classifies "When should I ignore your advice?" to failure modes', () => {
      const direct = classifyDirectQuestion('When should I ignore your advice?');
      assert.equal(direct.target, REASONING_TARGETS.FAILURE_MODES);
    });

    it('classifies "Why shouldn\'t Scout do your job?" to compare max vs scout', () => {
      const direct = classifyDirectQuestion("Why shouldn't Scout do your job?");
      assert.equal(direct.target, REASONING_TARGETS.COMPARE);
      assert.deepEqual(direct.objects, ['max', 'scout']);
    });

    it('classifies philosophical separation questions', () => {
      const direct = classifyDirectQuestion('Why does PulseForge separate specialists?');
      assert.equal(direct.target, REASONING_TARGETS.SPECIALIST_SEPARATION);
    });
  });

  describe('Operating model prose synthesis', () => {
    it('synthesizes why(identity) from operating model concepts', () => {
      const prose = composeIdentityReasoning({
        question: 'Why?',
        resolvedQuestion: 'why(identity)',
        conversationIntent: { continuity: true, intent: THINKING_MODES.OPERATING_MODEL },
        session: { context: {} },
      });
      assert.ok(prose);
      assert.match(prose, /No single specialist sees the entire business/i);
      assert.match(prose, /Scout understands markets/i);
      assert.match(prose, /integrate competing evidence/i);
      assert.doesNotMatch(prose, /Blueprint|ICP|ideal customer/i);
    });

    it('synthesizes compare(max,scout) from relationship knowledge', () => {
      const prose = composeIdentityReasoning({
        question: 'How is that different from Scout?',
        resolvedQuestion: 'compare(max,scout)',
        conversationIntent: {
          continuity: true,
          intent: THINKING_MODES.OPERATING_MODEL,
          compareObjects: ['max', 'scout'],
        },
        session: { context: {} },
      });
      assert.ok(prose);
      assert.match(prose, /Scout/i);
      assert.match(prose, /Max/i);
      assert.match(prose, /specializ/i);
      assert.match(prose, /synthesiz/i);
      assert.doesNotMatch(prose, /Blueprint|ICP/i);
    });

    it('synthesizes boundary reasoning for "What should never belong to you?"', () => {
      const prose = composeIdentityReasoning({
        question: 'What should never belong to you?',
        conversationIntent: { intent: THINKING_MODES.OPERATING_MODEL },
        session: { context: {} },
      });
      assert.ok(prose);
      assert.match(prose, /never signs contracts|never invents evidence|never impersonates/i);
      assert.match(prose, /Operator retains authority/i);
    });

    it('synthesizes failure mode reasoning for ignore-advice question', () => {
      const prose = composeIdentityReasoning({
        question: 'When should I ignore your advice?',
        conversationIntent: { intent: THINKING_MODES.OPERATING_MODEL },
        session: { context: {} },
      });
      assert.ok(prose);
      assert.match(prose, /your judgment overrides/i);
      assert.match(prose, /evidence is thin|disagree with a recommendation/i);
    });

    it('synthesizes Max vs Paige comparison', () => {
      const prose = composeIdentityReasoning({
        question: 'How is Max different from Paige?',
        conversationIntent: { intent: THINKING_MODES.COMPARE },
        session: { context: {} },
      });
      assert.ok(prose);
      assert.match(prose, /Paige/i);
      assert.match(prose, /communication/i);
      assert.match(prose, /Max/i);
    });

    it('synthesizes Scout vs Rex comparison', () => {
      const prose = composeIdentityReasoning({
        question: 'Scout vs Rex — what is the difference?',
        conversationIntent: { intent: THINKING_MODES.COMPARE },
        session: { context: {} },
      });
      assert.ok(prose);
      assert.match(prose, /Scout/i);
      assert.match(prose, /Rex/i);
      assert.match(prose, /discovery|market/i);
      assert.match(prose, /reporting|performance/i);
    });
  });

  describe('Identity conversation integration', () => {
    it('maybeHandleIdentityTurn uses operating model reflection for continuity follow-ups', async () => {
      const turn = await maybeHandleIdentityTurn({
        question: 'Why?',
        resolvedQuestion: 'why(identity)',
        conversationSubject: detectConversationSubject('What is your role?'),
        conversationIntent: {
          intent: THINKING_MODES.OPERATING_MODEL,
          thinkingMode: 'operating_model_reflection',
          continuity: true,
        },
        session: { context: { tenantId: '10' } },
      });
      assert.ok(turn);
      assert.equal(turn.structured.metadata.operatingModelReflection, true);
      assert.equal(turn.structured.metadata.identityConversation, true);
      assert.equal(turn.structured.metadata.businessIntelligenceUsed, false);
      assert.equal(turn.reason, 'operating_model_reflection');
      assert.match(turn.prose, /specialist sees the entire business/i);
      assert.doesNotMatch(turn.prose, /Blueprint|ICP/i);
    });

    it('initial role question still uses canonical identity prose', async () => {
      const turn = await maybeHandleIdentityTurn({
        question: 'What is your role?',
        conversationSubject: detectConversationSubject('What is your role?'),
        session: { context: {} },
      });
      assert.ok(turn);
      assert.equal(turn.structured.metadata.operatingModelReflection, false);
      assert.match(turn.prose, /operating system/i);
    });
  });

  describe('acceptance — identity continuity chain', () => {
    let workspace;

    beforeEach(() => {
      workspace = createWorkspaceEngine({
        missionsEnabled: false,
        resolverEnabled: false,
        disableLlm: true,
      });
    });

    it('What is your role? → Why? → compare Scout → ignore advice — all stay in identity', async () => {
      const opened = await workspace.open({ tenantId: '10' });
      const sessionId = opened.sessionId;

      const turn1 = await workspace.ask({
        sessionId,
        question: 'What is your role?',
        context: { tenantId: '10' },
      });
      assert.equal(turn1.conversationSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);
      assert.equal(turn1.workspaceOwnership.owner, WORKSPACE_OWNERS.CONVERSATION_IDENTITY);
      assert.match(turn1.prose, /operating system/i);

      const turn2 = await workspace.ask({
        sessionId,
        question: 'Why?',
        context: { tenantId: '10' },
      });
      assert.equal(turn2.conversationSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);
      assert.equal(turn2.workspaceOwnership.owner, WORKSPACE_OWNERS.CONVERSATION_IDENTITY);
      assert.equal(turn2.conversationIntent.intent, THINKING_MODES.OPERATING_MODEL);
      assert.equal(turn2.conversationIntent.thinkingMode, 'operating_model_reflection');
      assert.equal(turn2.resolvedQuestion, 'why(identity)');
      assert.match(turn2.prose, /specialist sees the entire business|integrate competing evidence/i);
      assert.doesNotMatch(turn2.prose, /Blueprint|ICP|ideal customer/i);

      const turn3 = await workspace.ask({
        sessionId,
        question: 'How is that different from Scout?',
        context: { tenantId: '10' },
      });
      assert.equal(turn3.conversationSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);
      assert.equal(turn3.workspaceOwnership.owner, WORKSPACE_OWNERS.CONVERSATION_IDENTITY);
      assert.equal(turn3.conversationIntent.intent, THINKING_MODES.OPERATING_MODEL);
      assert.equal(turn3.resolvedQuestion, 'compare(max,scout)');
      assert.match(turn3.prose, /Scout/i);
      assert.match(turn3.prose, /specializ|synthesiz/i);
      assert.doesNotMatch(turn3.prose, /Blueprint|ICP/i);

      const turn4 = await workspace.ask({
        sessionId,
        question: "Why shouldn't Scout do your job?",
        context: { tenantId: '10' },
      });
      assert.equal(turn4.conversationSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);
      assert.equal(turn4.workspaceOwnership.owner, WORKSPACE_OWNERS.CONVERSATION_IDENTITY);
      assert.match(turn4.prose, /Scout/i);
      assert.match(turn4.prose, /integrat|synthesiz|specialist/i);
      assert.doesNotMatch(turn4.prose, /Blueprint|ICP/i);

      const turn5 = await workspace.ask({
        sessionId,
        question: 'When should I ignore your advice?',
        context: { tenantId: '10' },
      });
      assert.equal(turn5.conversationSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);
      assert.equal(turn5.workspaceOwnership.owner, WORKSPACE_OWNERS.CONVERSATION_IDENTITY);
      assert.match(turn5.prose, /judgment overrides|evidence is thin|disagree/i);
      assert.doesNotMatch(turn5.prose, /Blueprint|ICP/i);
    });

    it('identity continuity uses operating_model_reflection mode, not Blueprint', async () => {
      const session = { context: {} };
      seedIdentityState(session, 2);

      const continuity = applyConversationalContinuity({
        question: 'Why?',
        session,
        conversationSubject: detectConversationSubject('Why?'),
        conversationIntent: classifyOperatorCognition('Why?'),
      });

      assert.equal(continuity.conversationIntent.intent, THINKING_MODES.OPERATING_MODEL);
      assert.equal(continuity.conversationIntent.thinkingMode, 'operating_model_reflection');
      assert.equal(continuity.conversationIntent.underlyingIntent, THINKING_MODES.EXPLAIN);

      const ownership = await resolveWorkspaceOwner({
        question: 'Why?',
        conversationSubject: continuity.conversationSubject,
        conversationIntent: continuity.conversationIntent,
      });
      assert.equal(ownership.owner, WORKSPACE_OWNERS.CONVERSATION_IDENTITY);
    });
  });

  describe('shouldUseOperatingModelReasoning', () => {
    it('returns true for resolved questions and continuity', () => {
      assert.equal(
        shouldUseOperatingModelReasoning({ resolvedQuestion: 'why(identity)' }),
        true
      );
      assert.equal(
        shouldUseOperatingModelReasoning({
          question: 'Why?',
          conversationIntent: { continuity: true },
        }),
        true
      );
      assert.equal(
        shouldUseOperatingModelReasoning({ question: 'What is your role?' }),
        false
      );
    });
  });
});
