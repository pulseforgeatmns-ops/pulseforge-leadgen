'use strict';

/**
 * SPEC-149 — Conversation Subject Routing.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const {
  CONVERSATION_SUBJECTS,
  detectConversationSubject,
  blocksBusinessSubsystemClaim,
} = require('../ConversationSubject');
const {
  classifyOperatorCognition,
  THINKING_MODES,
} = require('../../operatorCognition');
const {
  resolveWorkspaceOwner,
  WORKSPACE_OWNERS,
  claimsBlueprintOwnership,
} = require('../WorkspaceOwnershipResolver');
const { shouldClaimClientIntelligenceTurn } = require('../ClientIntelligenceContext');
const { maybeHandleIdentityTurn, composeIdentityProse } = require('../IdentityConversationContext');
const { maybeHandleReflectionTurn } = require('../ReflectionRouting');
const { buildRoutingTrace } = require('../SubjectRoutingTrace');
const { createWorkspaceEngine } = require('../WorkspaceEngine');

describe('SPEC-149 — Conversation Subject Routing', () => {
  describe('detectConversationSubject — subject before intent', () => {
    it('detects identity for role questions independent of intent', () => {
      const cases = [
        'What is your role?',
        'Who are you?',
        'What do you believe your role is?',
      ];
      for (const q of cases) {
        const subject = detectConversationSubject(q);
        assert.equal(subject.subject, CONVERSATION_SUBJECTS.IDENTITY, q);
        assert.equal(subject.locked, true, q);
        assert.ok(subject.confidence >= 0.9, q);
      }
    });

    it('distinguishes identity from business ICP questions', () => {
      const identity = detectConversationSubject('What is your role?');
      const business = detectConversationSubject('What is our ICP?');
      assert.equal(identity.subject, CONVERSATION_SUBJECTS.IDENTITY);
      assert.equal(business.subject, CONVERSATION_SUBJECTS.BUSINESS);
      assert.notEqual(identity.subject, business.subject);
    });

    it('detects reflection subject for meta-cognitive prompts', () => {
      const cases = [
        'Why did you answer that?',
        'Did you misunderstand me?',
        'What assumptions are you making?',
        'Why are you waiting?',
        'What pipeline answered me?',
      ];
      for (const q of cases) {
        const subject = detectConversationSubject(q);
        assert.equal(subject.subject, CONVERSATION_SUBJECTS.REFLECTION, q);
        assert.equal(subject.locked, true, q);
      }
    });

    it('detects specialist subject for operational specialist questions', () => {
      const subject = detectConversationSubject('Why did Scout stop?');
      assert.equal(subject.subject, CONVERSATION_SUBJECTS.SPECIALIST);
      assert.equal(subject.locked, false);
    });

    it('detects knowledge subject for educational prompts', () => {
      const subject = detectConversationSubject('Teach me embeddings.');
      assert.equal(subject.subject, CONVERSATION_SUBJECTS.KNOWLEDGE);
      assert.equal(subject.locked, true);
    });

    it('detects conversation subject for repeat/recall prompts', () => {
      const subject = detectConversationSubject('Repeat that.');
      assert.equal(subject.subject, CONVERSATION_SUBJECTS.CONVERSATION);
      assert.equal(subject.locked, true);
    });
  });

  describe('same intent, different subjects', () => {
    it('both classify as EXPLAIN intent but route to different subjects', () => {
      const icpIntent = classifyOperatorCognition('What is our ICP?');
      const roleIntent = classifyOperatorCognition('What is your role?');
      assert.equal(icpIntent.intent, THINKING_MODES.EXPLAIN);
      assert.equal(roleIntent.intent, THINKING_MODES.EXPLAIN);

      const icpSubject = detectConversationSubject('What is our ICP?');
      const roleSubject = detectConversationSubject('What is your role?');
      assert.equal(icpSubject.subject, CONVERSATION_SUBJECTS.BUSINESS);
      assert.equal(roleSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);
    });
  });

  describe('owner lock — business subsystems blocked', () => {
    it('identity subject blocks Blueprint ownership', () => {
      const subject = detectConversationSubject('What is your role?');
      assert.equal(
        claimsBlueprintOwnership('What is your role?', { conversationSubject: subject }),
        false
      );
    });

    it('identity subject blocks Client Intelligence claim', () => {
      const subject = detectConversationSubject('What is your role?');
      assert.equal(blocksBusinessSubsystemClaim(subject), true);
      assert.equal(
        shouldClaimClientIntelligenceTurn('What is your role?', null, {
          approvedBlueprint: true,
          conversationSubject: subject,
        }),
        false
      );
    });

    it('reflection subject blocks Client Intelligence claim', () => {
      const subject = detectConversationSubject('Why did you answer that?');
      assert.equal(
        shouldClaimClientIntelligenceTurn('Why did you answer that?', null, {
          approvedBlueprint: true,
          conversationSubject: subject,
        }),
        false
      );
    });
  });

  describe('resolveWorkspaceOwner — subject-first routing', () => {
    it('routes identity subject to conversation_identity owner', async () => {
      const subject = detectConversationSubject('Who are you?');
      const owner = await resolveWorkspaceOwner({
        question: 'Who are you?',
        session: { id: 's1', context: { tenantId: '10' } },
        conversationSubject: subject,
      });
      assert.equal(owner.owner, WORKSPACE_OWNERS.CONVERSATION_IDENTITY);
      assert.equal(owner.subjectLock, true);
    });

    it('routes reflection subject to reflection owner', async () => {
      const subject = detectConversationSubject('Did you misunderstand me?');
      const owner = await resolveWorkspaceOwner({
        question: 'Did you misunderstand me?',
        session: { id: 's1', context: { tenantId: '10' } },
        conversationSubject: subject,
      });
      assert.equal(owner.owner, WORKSPACE_OWNERS.REFLECTION);
    });

    it('routes knowledge subject to knowledge_retrieval owner', async () => {
      const subject = detectConversationSubject('Teach me embeddings.');
      const owner = await resolveWorkspaceOwner({
        question: 'Teach me embeddings.',
        session: { id: 's1', context: { tenantId: '10' } },
        conversationSubject: subject,
      });
      assert.equal(owner.owner, WORKSPACE_OWNERS.KNOWLEDGE_RETRIEVAL);
    });
  });

  describe('Identity Conversation — no Blueprint advisory', () => {
    it('identity response addresses Max, not acquisition strategy', async () => {
      const turn = await maybeHandleIdentityTurn({
        question: 'What is your role?',
        conversationSubject: detectConversationSubject('What is your role?'),
        conversationIntent: classifyOperatorCognition('What is your role?'),
        session: { context: { tenantId: '10' } },
      });
      assert.ok(turn);
      assert.equal(turn.handled, true);
      assert.match(turn.prose, /Max|operating system|coordinate/i);
      assert.doesNotMatch(turn.prose, /repeatable commercial acquisition motion/i);
      assert.doesNotMatch(turn.prose, /ideal customer|ICP|Blueprint/i);
      assert.equal(turn.structured.metadata.businessIntelligenceUsed, false);
      assert.equal(turn.structured.metadata.identityConversation, true);
    });

    it('composeIdentityProse never mentions Blueprint strategy', () => {
      const prose = composeIdentityProse('Who are you?', { context: {} }, {
        listCallable: () => [{ specialist: 'scout', capability: 'acquisition_intelligence' }],
      });
      assert.match(prose, /operating system/i);
      assert.doesNotMatch(prose, /Blueprint|ICP|commercial acquisition motion/i);
    });
  });

  describe('acceptance — prompts must not invoke Blueprint Advisory', () => {
    const ACCEPTANCE_PROMPTS = [
      'What is your role?',
      'Who are you?',
      'Why did you answer that?',
      'Did you misunderstand me?',
      'What assumptions are you making?',
      'Why are you waiting?',
      'What pipeline answered me?',
    ];

    for (const question of ACCEPTANCE_PROMPTS) {
      it(`blocks CIE for: ${question}`, () => {
        const subject = detectConversationSubject(question);
        assert.notEqual(subject.subject, CONVERSATION_SUBJECTS.BUSINESS);
        assert.equal(
          shouldClaimClientIntelligenceTurn(question, null, {
            approvedBlueprint: true,
            conversationSubject: subject,
          }),
          false
        );
        assert.equal(
          claimsBlueprintOwnership(question, { conversationSubject: subject }),
          false
        );
      });
    }

    for (const question of ACCEPTANCE_PROMPTS) {
      it(`identity/reflection handler avoids business intelligence: ${question}`, async () => {
        const subject = detectConversationSubject(question);
        const conversationIntent = classifyOperatorCognition(question);
        const session = {
          messages: [
            { role: 'operator', text: 'What should we focus on?' },
            { role: 'max', text: 'I recommend targeting property managers first.' },
            { role: 'operator', text: question },
          ],
          context: { tenantId: '10' },
        };

        if (subject.subject === CONVERSATION_SUBJECTS.IDENTITY) {
          const turn = await maybeHandleIdentityTurn({
            question,
            conversationSubject: subject,
            conversationIntent,
            session,
          });
          assert.ok(turn);
          assert.equal(turn.structured.metadata.businessIntelligenceUsed, false);
          assert.doesNotMatch(turn.prose, /property managers|Blueprint|ICP/i);
          return;
        }

        if (subject.subject === CONVERSATION_SUBJECTS.REFLECTION) {
          const turn = await maybeHandleReflectionTurn({
            question,
            conversationSubject: subject,
            conversationIntent,
            session,
            previousTurnContext: {
              conversationIntent: { intent: THINKING_MODES.STRATEGY },
              workspaceOwner: WORKSPACE_OWNERS.REASONING,
            },
          });
          assert.ok(turn);
          assert.equal(turn.structured.metadata.businessIntelligenceUsed, false);
          assert.doesNotMatch(turn.prose, /Blueprint|ideal customer/i);
        }
      });
    }
  });

  describe('routing trace', () => {
    it('builds independent subject, intent, owner, pipeline fields', () => {
      const subject = detectConversationSubject('What is your role?');
      const intent = classifyOperatorCognition('What is your role?');
      const trace = buildRoutingTrace({
        conversationSubject: subject,
        conversationIntent: intent,
        workspaceOwnership: {
          owner: WORKSPACE_OWNERS.CONVERSATION_IDENTITY,
          reason: 'role_question',
        },
        pipeline: 'IdentityConversation',
        claimedBy: 'identity_subject_router',
      });
      assert.equal(trace.subject, 'identity');
      assert.equal(trace.intent, 'explain');
      assert.equal(trace.thinkingMode, intent.thinkingMode);
      assert.equal(trace.owner, 'conversation_identity');
      assert.equal(trace.pipeline, 'IdentityConversation');
      assert.equal(trace.claimedBy, 'identity_subject_router');
    });
  });

  describe('WorkspaceEngine integration', () => {
    let workspace;

    beforeEach(() => {
      workspace = createWorkspaceEngine({
        missionsEnabled: false,
        resolverEnabled: false,
        disableLlm: true,
      });
    });

    it('identity ask returns routingTrace and identity owner', async () => {
      const opened = await workspace.open({ tenantId: '10' });
      const result = await workspace.ask({
        sessionId: opened.sessionId,
        question: 'What is your role?',
        context: { tenantId: '10' },
      });

      assert.equal(result.conversationSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);
      assert.equal(result.workspaceOwnership.owner, WORKSPACE_OWNERS.CONVERSATION_IDENTITY);
      assert.ok(result.routingTrace);
      assert.equal(result.routingTrace.subject, 'identity');
      assert.equal(result.routingTrace.pipeline, 'IdentityConversation');
      assert.match(result.prose, /Max|operating system/i);
      assert.doesNotMatch(result.prose, /repeatable commercial acquisition motion|ideal customer profile/i);
    });

    it('ICP ask stays on business subject', async () => {
      const opened = await workspace.open({ tenantId: '10' });
      const result = await workspace.ask({
        sessionId: opened.sessionId,
        question: 'What is our ICP?',
        context: { tenantId: '10' },
      });

      assert.equal(result.conversationSubject.subject, CONVERSATION_SUBJECTS.BUSINESS);
      assert.notEqual(result.workspaceOwnership.owner, WORKSPACE_OWNERS.CONVERSATION_IDENTITY);
    });
  });
});
