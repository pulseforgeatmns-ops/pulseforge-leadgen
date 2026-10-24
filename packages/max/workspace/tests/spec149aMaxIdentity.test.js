'use strict';

/**
 * SPEC-149A — Max Identity & Operating Model.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  MAX_ROLE,
  MAX_OWNS,
  OPERATOR_OWNS,
  MAX_DOES_NOT,
  FORBIDDEN_IDENTITY_TERMS,
  composeWorkspaceIntroduction,
  containsForbiddenIdentityTerm,
  assertIdentityCompliance,
  LEGACY_CHAT_SYSTEM,
  PRESENTATION_IDENTITY,
} = require('../../identity/MaxIdentity');
const {
  composeIdentityProse,
  maybeHandleIdentityTurn,
  classifyIdentityQuestion,
} = require('../IdentityConversationContext');
const { detectConversationSubject } = require('../ConversationSubject');
const { PresentationEngine } = require('../PresentationEngine');

describe('SPEC-149A — Max Identity & Operating Model', () => {
  describe('canonical identity module', () => {
    it('MAX_ROLE describes organizational responsibility', () => {
      assert.match(MAX_ROLE, /operating system/i);
      assert.match(MAX_ROLE, /measurable business outcomes/i);
      assert.match(MAX_ROLE, /you retain final authority/i);
      assert.doesNotMatch(MAX_ROLE, /mission manager|manager agent|ai assistant|chatbot/i);
    });

    it('forbidden terms are not in identity response strings', () => {
      const corpus = [MAX_ROLE, LEGACY_CHAT_SYSTEM, ...MAX_OWNS, ...OPERATOR_OWNS, ...MAX_DOES_NOT].join(' ');
      assert.equal(containsForbiddenIdentityTerm(corpus), false);
    });

    it('workspace introduction adapts to business name', () => {
      const named = composeWorkspaceIntroduction({
        context: { businessName: 'Anchor Cleaning' },
      });
      assert.match(named, /Anchor Cleaning/i);
      assert.match(named, /operating system/i);
      assertIdentityCompliance(named);

      const generic = composeWorkspaceIntroduction({ context: {} });
      assert.match(generic, /operating system/i);
      assertIdentityCompliance(generic);
    });
  });

  describe('identity conversation responses', () => {
    const registry = {
      listCallable: () => [{ specialist: 'scout', capability: 'acquisition_intelligence' }],
    };

    it('role question uses operating-system identity', () => {
      const prose = composeIdentityProse('What is your role?', { context: {} }, registry);
      assert.match(prose, /operating system/i);
      assert.match(prose, /specialists perform domain-specific work/i);
      assert.doesNotMatch(prose, /mission manager|manager agent|intelligence advisor/i);
      assertIdentityCompliance(prose);
    });

    it('who are you uses tenant-aware introduction when business name present', () => {
      const prose = composeIdentityProse(
        'Who are you?',
        { context: { businessName: 'Anchor Cleaning' } },
        registry
      );
      assert.match(prose, /Anchor Cleaning/i);
      assert.match(prose, /operating system/i);
      assertIdentityCompliance(prose);
    });

    it('roster describes specialists without claiming their domain work', () => {
      const prose = composeIdentityProse('Who are your specialists?', { context: {} }, registry);
      assert.match(prose, /Scout/i);
      assert.match(prose, /Paige/i);
      assert.match(prose, /Emmett/i);
      assert.match(prose, /Vera/i);
      assert.match(prose, /Rex/i);
      assert.match(prose, /Sam/i);
      assert.match(prose, /coordinate/i);
      assert.doesNotMatch(prose, /Blueprint|ICP/i);
      assertIdentityCompliance(prose);
    });

    it('operator authority is explicit', () => {
      const kind = classifyIdentityQuestion('Who has final authority?');
      assert.equal(kind, 'operator_authority');
      const prose = composeIdentityProse('Who has final authority?', { context: {} }, registry);
      assert.match(prose, /Only the operator owns/i);
      assert.match(prose, /final decisions/i);
      assert.match(prose, /never replaces operator judgment/i);
      assertIdentityCompliance(prose);
    });

    it('maybeHandleIdentityTurn marks identityConversation metadata', async () => {
      const turn = await maybeHandleIdentityTurn({
        question: 'What is your role?',
        conversationSubject: detectConversationSubject('What is your role?'),
        session: { context: { tenantId: '10' } },
      });
      assert.ok(turn);
      assert.equal(turn.structured.metadata.identityConversation, true);
      assert.equal(turn.structured.metadata.businessIntelligenceUsed, false);
      assertIdentityCompliance(turn.prose);
    });
  });

  describe('PresentationEngine — identity bypass', () => {
    it('passes identity prose through without LLM rewrite', async () => {
      const engine = new PresentationEngine({ disableLlm: true });
      const structured = {
        answer: MAX_ROLE,
        reasoning: ['SPEC-149A identity'],
        metadata: { identityConversation: true },
      };
      const result = await engine.present(structured);
      assert.equal(result.presentation, 'identity_conversation');
      assert.equal(result.prose, MAX_ROLE);
    });
  });

  describe('acceptance — consistent identity across prompts', () => {
    const PROMPTS = [
      'What is your role?',
      'Who are you?',
      'What do you believe your role is?',
    ];

    for (const question of PROMPTS) {
      it(`consistent operating-system identity for: ${question}`, async () => {
        const turn = await maybeHandleIdentityTurn({
          question,
          conversationSubject: detectConversationSubject(question),
          session: { context: { businessName: 'Test Restaurant' } },
        });
        assert.ok(turn);
        assert.match(turn.prose, /operating system/i);
        assert.match(turn.prose, /Test Restaurant|measurable business outcomes/i);
        assert.doesNotMatch(
          turn.prose,
          /mission manager|manager agent|ai assistant|chatbot|intelligence advisor/i
        );
        assertIdentityCompliance(turn.prose);
      });
    }
  });
});
