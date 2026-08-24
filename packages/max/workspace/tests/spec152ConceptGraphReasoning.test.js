'use strict';

/**
 * SPEC-152 — Concept Graph Reasoning.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const {
  ConceptGraph,
  getOperatingConceptGraph,
  planConceptQuery,
  shouldUseConceptGraphReasoning,
  reasonFromPlan,
  REASONING_GOALS,
  composeConceptGraphAnswer,
} = require('../../reasoning/ConceptGraph');
const {
  planOperatingModelQuery,
  composeIdentityReasoning,
  shouldUseOperatingModelReasoning,
  REASONING_TARGETS,
} = require('../../identity/IdentityReasoning');
const { maybeHandleIdentityTurn } = require('../IdentityConversationContext');
const {
  detectConversationSubject,
  CONVERSATION_SUBJECTS,
} = require('../ConversationSubject');
const {
  applyConversationalContinuity,
  advanceConversationalState,
  setConversationalState,
  getConversationalState,
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

function seedIdentityState(session, depth = 1, activeConcepts = null) {
  setConversationalState(session, {
    subject: CONVERSATION_SUBJECTS.IDENTITY,
    owner: WORKSPACE_OWNERS.CONVERSATION_IDENTITY,
    activeObject: 'max',
    mode: CONVERSATIONAL_MODES.EXPLANATION,
    depth,
    objects: ['max'],
    activeConcepts: activeConcepts || ['identity', 'max'],
    lastQuestion: 'What is your role?',
    lastIntent: THINKING_MODES.EXPLAIN,
    lastResolvedQuestion: 'What is your role?',
    confidence: 0.97,
  });
}

describe('SPEC-152 — Concept Graph Reasoning', () => {
  describe('ConceptGraph structure', () => {
    it('seeds concepts and relationships from operating model', () => {
      const graph = getOperatingConceptGraph();
      assert.ok(graph.getConcept('operator'));
      assert.ok(graph.getConcept('max'));
      assert.ok(graph.getConcept('scout'));
      assert.ok(graph.getConcept('authority'));
      assert.ok(graph.getConcept('business_decisions'));
      assert.ok(graph.getRelationships({ from: 'operator', to: 'max', relation: 'delegates_to' }).length);
      assert.ok(graph.getRelationships({ from: 'max', to: 'operator', relation: 'cannot_override' }).length);
      assert.ok(graph.getRelationships({ from: 'scout', to: 'market_discovery', relation: 'specializes_in' }).length);
    });

    it('traverses multi-hop paths', () => {
      const graph = getOperatingConceptGraph();
      const traversal = graph.traverse(['operator', 'scout'], {
        targetConcepts: ['business_decisions'],
        maxHops: 4,
      });
      assert.ok(traversal.path.length >= 2);
      assert.ok(traversal.hops >= 1);
    });
  });

  describe('ConceptPlanner', () => {
    it('plans authority questions', () => {
      const plan = planConceptQuery({ question: 'Who ultimately decides?' });
      assert.ok(plan.concepts.includes('authority'));
      assert.ok(plan.concepts.includes('operator'));
      assert.equal(plan.goal, REASONING_GOALS.EXPLAIN_AUTHORITY);
    });

    it('plans compare_roles for Scout job question', () => {
      const plan = planConceptQuery({ question: "Why shouldn't Scout do your job?" });
      assert.equal(plan.goal, REASONING_GOALS.COMPARE_ROLES);
      assert.ok(plan.concepts.includes('scout'));
      assert.ok(plan.concepts.includes('authority'));
    });

    it('plans resolve_conflict for specialist disagreement', () => {
      const plan = planConceptQuery({ question: 'If Scout and Paige disagreed, who wins?' });
      assert.equal(plan.goal, REASONING_GOALS.RESOLVE_CONFLICT);
      assert.ok(plan.concepts.includes('conflict'));
      assert.ok(plan.concepts.includes('governance'));
      assert.ok(plan.concepts.includes('operator'));
    });

    it('merges activeConcepts from conversation state', () => {
      const plan = planConceptQuery({
        question: 'Why?',
        resolvedQuestion: 'why(identity)',
        activeConcepts: ['identity', 'authority'],
        conversationIntent: { continuity: true },
      });
      assert.ok(plan.concepts.includes('identity'));
      assert.ok(plan.concepts.includes('authority'));
      assert.ok(plan.concepts.includes('purpose'));
    });
  });

  describe('ConceptReasoner synthesis', () => {
    it('synthesizes authority reasoning without Blueprint leakage', () => {
      const graph = getOperatingConceptGraph();
      const plan = planConceptQuery({ question: 'Can Scout approve outreach?' });
      const result = reasonFromPlan(plan, graph);
      assert.ok(result.prose);
      assert.match(result.prose, /operator|You retain final authority/i);
      assert.match(result.prose, /Specialists execute within their domain/i);
      assert.doesNotMatch(result.prose, /Blueprint|ICP|ideal customer/i);
      assert.ok(result.hops >= 0);
      assert.ok(result.conceptsUsed.length >= 3);
    });

    it('synthesizes conflict resolution through governance', () => {
      const graph = getOperatingConceptGraph();
      const plan = planConceptQuery({ question: 'Scout disagrees with Paige. Explain.' });
      const result = reasonFromPlan(plan, graph);
      assert.match(result.prose, /disagree|Neither specialist wins/i);
      assert.match(result.prose, /You retain final authority/i);
      assert.doesNotMatch(result.prose, /Blueprint|ICP/i);
    });

    it('synthesizes dependency relationships', () => {
      const graph = getOperatingConceptGraph();
      const plan = planConceptQuery({ question: 'How do Scout and Paige depend on each other?' });
      const result = reasonFromPlan(plan, graph);
      assert.match(result.prose, /Scout/i);
      assert.match(result.prose, /Paige/i);
      assert.match(result.prose, /Max coordinates both/i);
    });

    it('answers multi-hop business decision question', () => {
      const composed = composeConceptGraphAnswer({
        question: "Why shouldn't Scout make business decisions?",
      });
      assert.ok(composed.result.prose);
      assert.match(composed.result.prose, /Scout|specializ|business decisions|operator|authority/i);
      assert.ok(composed.result.hops >= 0);
      assert.equal(composed.metadata.conceptGraphReasoning, true);
    });
  });

  describe('IdentityReasoning integration', () => {
    it('routes through concept graph for operating model questions', () => {
      const query = planOperatingModelQuery({ question: 'Who ultimately decides?' });
      assert.equal(query.target, REASONING_TARGETS.AUTHORITY);
      assert.ok(query.concepts);
      assert.ok(query.goal);
    });

    it('composeIdentityReasoning uses graph traversal', () => {
      const prose = composeIdentityReasoning({
        question: 'Who ultimately decides?',
        conversationIntent: { intent: THINKING_MODES.OPERATING_MODEL },
        session: { context: {} },
      });
      assert.ok(prose);
      assert.match(prose, /authority|operator|decide/i);
      assert.doesNotMatch(prose, /Blueprint|ICP/i);
    });
  });

  describe('Identity conversation handler', () => {
    it('returns concept graph metadata for authority questions', async () => {
      const turn = await maybeHandleIdentityTurn({
        question: 'Can Scout approve outreach?',
        conversationSubject: detectConversationSubject('Can Scout approve outreach?'),
        conversationIntent: { intent: THINKING_MODES.OPERATING_MODEL },
        session: { context: { tenantId: '1' } },
      });
      assert.ok(turn);
      assert.equal(turn.structured.metadata.conceptGraphReasoning, true);
      assert.ok(turn.structured.metadata.activeConcepts);
      assert.equal(turn.reason, 'concept_graph_reasoning');
      assert.doesNotMatch(turn.prose, /Blueprint|ICP/i);
    });
  });

  describe('Conversation state — activeConcepts', () => {
    it('advances activeConcepts across turns', () => {
      const session = { context: { tenantId: '1' } };
      seedIdentityState(session, 1, ['identity']);

      advanceConversationalState(session, {
        question: 'Why?',
        resolvedQuestion: 'why(identity)',
        conversationSubject: { subject: CONVERSATION_SUBJECTS.IDENTITY },
        conversationIntent: { intent: THINKING_MODES.OPERATING_MODEL },
        workspaceOwnership: { owner: WORKSPACE_OWNERS.CONVERSATION_IDENTITY },
        structured: {
          metadata: {
            operatingModelReasoning: {
              activeConcepts: ['identity', 'purpose', 'max'],
              concepts: ['identity', 'purpose', 'max'],
            },
          },
        },
      });

      const state = getConversationalState(session);
      assert.ok(state.activeConcepts.includes('identity'));
      assert.ok(state.activeConcepts.includes('purpose'));
    });
  });

  describe('acceptance — full identity concept graph chain', () => {
    let workspace;

    beforeEach(() => {
      workspace = createWorkspaceEngine({
        missionsEnabled: false,
        resolverEnabled: false,
        disableLlm: true,
      });
    });

    it('identity chain never exits concept graph reasoning', async () => {
      const opened = await workspace.open({ tenantId: '10' });
      const sessionId = opened.sessionId;

      const turn1 = await workspace.ask({
        sessionId,
        question: 'Who are you?',
        context: { tenantId: '10' },
      });
      assert.equal(turn1.conversationSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);

      const turn2 = await workspace.ask({ sessionId, question: 'Why?', context: { tenantId: '10' } });
      assert.equal(turn2.conversationSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);
      assert.equal(turn2.workspaceOwnership.owner, WORKSPACE_OWNERS.CONVERSATION_IDENTITY);
      assert.match(turn2.prose, /specialist sees the entire business|integrate competing evidence/i);

      const turn3 = await workspace.ask({
        sessionId,
        question: 'How are you different from Scout?',
        context: { tenantId: '10' },
      });
      assert.equal(turn3.conversationSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);
      assert.match(turn3.prose, /Scout/i);
      assert.match(turn3.prose, /specializ|synthesiz/i);

      const turn4 = await workspace.ask({
        sessionId,
        question: "Why shouldn't Scout replace you?",
        context: { tenantId: '10' },
      });
      assert.equal(turn4.conversationSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);
      assert.match(turn4.prose, /Scout|integrat|specialist/i);

      const turn5 = await workspace.ask({
        sessionId,
        question: 'Who decides?',
        context: { tenantId: '10' },
      });
      assert.equal(turn5.conversationSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);
      assert.match(turn5.prose, /authority|operator|decide/i);

      const turn6 = await workspace.ask({
        sessionId,
        question: 'When should I ignore your advice?',
        context: { tenantId: '10' },
      });
      assert.equal(turn6.conversationSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);
      assert.match(turn6.prose, /judgment overrides|evidence is thin|disagree/i);

      for (const turn of [turn2, turn3, turn4, turn5, turn6]) {
        assert.doesNotMatch(turn.prose, /Blueprint|ICP|ideal customer/i);
      }
    });

    it('conflict chain uses graph traversal', async () => {
      const opened = await workspace.open({ tenantId: '10' });
      const sessionId = opened.sessionId;

      const turn1 = await workspace.ask({
        sessionId,
        question: 'Scout disagrees with Paige.',
        context: { tenantId: '10' },
      });
      assert.equal(turn1.conversationSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);

      const turn2 = await workspace.ask({ sessionId, question: 'Explain.', context: { tenantId: '10' } });
      assert.equal(turn2.conversationSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);

      const turn3 = await workspace.ask({ sessionId, question: 'Who wins?', context: { tenantId: '10' } });
      assert.match(turn3.prose, /operator|authority|decide/i);
      assert.doesNotMatch(turn3.prose, /Blueprint|ICP/i);

      const turn4 = await workspace.ask({ sessionId, question: 'Why?', context: { tenantId: '10' } });
      assert.equal(turn4.conversationSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);
      assert.doesNotMatch(turn4.prose, /Blueprint|ICP/i);
    });

    it('authority chain avoids Blueprint fallback', async () => {
      const opened = await workspace.open({ tenantId: '10' });
      const sessionId = opened.sessionId;

      const q1 = await workspace.ask({
        sessionId,
        question: 'Can Scout approve outreach?',
        context: { tenantId: '10' },
      });
      assert.match(q1.prose, /operator|authority|approve/i);

      const q2 = await workspace.ask({ sessionId, question: 'Why not?', context: { tenantId: '10' } });
      assert.equal(q2.conversationSubject.subject, CONVERSATION_SUBJECTS.IDENTITY);

      const q3 = await workspace.ask({ sessionId, question: 'Who can?', context: { tenantId: '10' } });
      assert.match(q3.prose, /operator|You retain/i);

      for (const turn of [q1, q2, q3]) {
        assert.doesNotMatch(turn.prose, /Blueprint|ICP/i);
      }
    });
  });

  describe('shouldUseConceptGraphReasoning', () => {
    it('returns true for graph-eligible questions', () => {
      assert.equal(shouldUseConceptGraphReasoning({ question: 'Who ultimately decides?' }), true);
      assert.equal(shouldUseConceptGraphReasoning({ resolvedQuestion: 'why(identity)' }), true);
      assert.equal(shouldUseConceptGraphReasoning({ question: 'What is your role?' }), false);
    });

    it('shouldUseOperatingModelReasoning delegates to concept graph', () => {
      assert.equal(shouldUseOperatingModelReasoning({ question: 'Who decides?' }), true);
    });
  });
});
