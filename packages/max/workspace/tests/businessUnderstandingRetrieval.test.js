'use strict';

/**
 * SPEC-103 — Durable Business Understanding Retrieval acceptance tests.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const {
  loadDurableBusinessUnderstanding,
  composeDurableRetrievalAnswer,
  buildBusinessUnderstandingContract,
  KNOWLEDGE_STATES,
  formatNeverLearnedAnswer,
  formatRetrievalFailureAnswer,
} = require('../BusinessUnderstandingRetrieval');
const {
  maybeHandleRetrievalBeforeDelegationTurn,
} = require('../RetrievalBeforeDelegationContext');
const {
  shouldHandleScoutAcquisition,
} = require('../ScoutAcquisitionContext');
const {
  createMemoryStore,
  startClientInterview,
  postInterviewMessage,
  approveBlueprint,
} = require('../../../../services/clientIntelligenceInterview');
const { classifyCognitiveMode } = require('../../specialistDelegation/CognitiveMode');

const ANCHOR_ID = 10;

const ANCHOR_ANSWERS = [
  'Anchor Cleaning — commercial cleaning for professional offices.',
  'Recurring commercial cleaning and weekly office cleans.',
  'Property managers, facility managers, and professional offices.',
  'Lowest-price bargain hunters.',
  'Greater Manchester including Bedford and Hooksett.',
  'Reliable crews that do the work right without chasing.',
  'Calm professional reliable voice.',
  'Grow commercial cleaning in Greater Manchester.',
  'Clearer path to commercial opportunities in 90 days.',
];

async function approveAnchor(store) {
  const opts = { store };
  const started = await startClientInterview({ clientId: ANCHOR_ID, forceNew: true }, opts);
  let turn = started;
  for (const answer of ANCHOR_ANSWERS) {
    turn = await postInterviewMessage(started.interviewId, answer, opts);
  }
  assert.ok(turn.blueprint);
  await approveBlueprint(turn.blueprint.id, opts);
  return opts;
}

describe('SPEC-103 Business Understanding Contract', () => {
  it('exposes minimum operating context fields from approved Blueprint', async () => {
    const store = createMemoryStore();
    const cieOpts = await approveAnchor(store);
    const loaded = await loadDurableBusinessUnderstanding({
      tenantId: String(ANCHOR_ID),
      cieOpts,
    });
    assert.equal(loaded.knowledgeState, KNOWLEDGE_STATES.AVAILABLE);
    const contract = buildBusinessUnderstandingContract(
      loaded.summary,
      loaded.playbook,
      loaded.activeObjectives
    );
    assert.ok(contract);
    assert.match(contract.companyName, /Anchor/i);
    assert.match(contract.serviceArea, /Manchester/i);
    assert.ok(contract.services);
    assert.match(contract.targetCustomers, /property managers|professional offices/i);
    assert.ok(contract.businessGoals);
    assert.ok(Array.isArray(contract.unknowns));
  });
});

describe('SPEC-103 durable load path', () => {
  it('loads approved Blueprint from store when session has no clientIntelligence', async () => {
    const store = createMemoryStore();
    const cieOpts = await approveAnchor(store);
    const session = { id: 'fresh', context: { tenantId: String(ANCHOR_ID) } };

    const loaded = await loadDurableBusinessUnderstanding({
      session,
      context: session.context,
      cieOpts,
    });

    assert.equal(loaded.knowledgeState, KNOWLEDGE_STATES.AVAILABLE);
    assert.match(loaded.summary.identity || '', /Anchor/i);
    assert.equal(session.context.clientIntelligence.approved, true);
    assert.equal(loaded.blueprintSource, 'blueprint');
  });

  it('distinguishes never learned from retrieval failure', async () => {
    const neverLearned = await loadDurableBusinessUnderstanding({
      context: { tenantId: '999' },
      cieOpts: { store: createMemoryStore() },
    });
    assert.equal(neverLearned.knowledgeState, KNOWLEDGE_STATES.NEVER_LEARNED);
    assert.match(formatNeverLearnedAnswer(), /don't currently know enough/i);

    const failingService = {
      async getApprovedClientBlueprint() {
        throw new Error('db unavailable');
      },
    };
    const failed = await loadDurableBusinessUnderstanding({
      context: { tenantId: String(ANCHOR_ID) },
      cieService: failingService,
    });
    assert.equal(failed.knowledgeState, KNOWLEDGE_STATES.RETRIEVAL_FAILURE);
    assert.match(formatRetrievalFailureAnswer(), /learned about your business previously/i);
  });
});

describe('SPEC-103 acceptance tests', () => {
  let store;
  let cieOpts;
  let session;

  beforeEach(async () => {
    store = createMemoryStore();
    cieOpts = await approveAnchor(store);
    session = { id: 'anchor-sess', context: { tenantId: String(ANCHOR_ID) } };
  });

  it('Test 1 — rich business summary for Anchor Cleaning', async () => {
    const turn = await maybeHandleRetrievalBeforeDelegationTurn({
      question: 'What do you currently understand about Anchor Cleaning?',
      session,
      context: session.context,
      cieOpts,
    });
    assert.ok(turn);
    assert.equal(turn.delegated, false);
    assert.match(turn.prose, /Anchor Cleaning/i);
    assert.match(turn.prose, /commercial|Manchester|property managers/i);
    assert.equal(turn.knowledgeState, KNOWLEDGE_STATES.AVAILABLE);
  });

  it('Test 2 — service area from Blueprint', async () => {
    const turn = await maybeHandleRetrievalBeforeDelegationTurn({
      question: 'What is our service area?',
      session,
      context: session.context,
      cieOpts,
    });
    assert.ok(turn);
    assert.match(turn.prose, /Manchester/i);
    assert.equal(turn.delegated, false);
  });

  it('Test 3 — ideal customers from approved understanding', async () => {
    const turn = await maybeHandleRetrievalBeforeDelegationTurn({
      question: 'Who are our ideal customers?',
      session,
      context: session.context,
      cieOpts,
    });
    assert.ok(turn);
    assert.match(turn.prose, /property managers|professional offices/i);
    assert.equal(turn.delegated, false);
  });

  it('Test 4 — current business priorities from durable objectives or goals', async () => {
    const turn = await maybeHandleRetrievalBeforeDelegationTurn({
      question: 'What are our current business priorities?',
      session,
      context: session.context,
      cieOpts,
    });
    assert.ok(turn);
    assert.match(turn.prose, /commercial|Manchester|goal|focus|priorit/i);
    assert.equal(turn.delegated, false);
  });

  it('Test 5 — retrieval questions never invoke Scout', async () => {
    const questions = [
      'What do you currently understand about Anchor Cleaning?',
      'What is our service area?',
      'Who are our ideal customers?',
      'What are our current business priorities?',
    ];
    for (const question of questions) {
      const mode = classifyCognitiveMode(question, { session, context: session.context });
      assert.equal(
        shouldHandleScoutAcquisition({ question, session, context: session.context }),
        false,
        `Scout should not handle: ${question}`
      );
      const turn = await maybeHandleRetrievalBeforeDelegationTurn({
        question,
        session,
        context: session.context,
        cieOpts,
      });
      assert.ok(turn, question);
      assert.equal(turn.delegated, false, question);
      assert.doesNotMatch(turn.prose, /\bScout\b/i, question);
    }
  });

  it('grounds answers naturally without citing Blueprint version', async () => {
    const turn = await maybeHandleRetrievalBeforeDelegationTurn({
      question: 'What do you currently understand about Anchor Cleaning?',
      session,
      context: session.context,
      cieOpts,
    });
    assert.match(turn.prose, /Based on my current understanding/i);
    assert.doesNotMatch(turn.prose, /Blueprint Version|blueprint version/i);
  });

  it('recommends learning when no approved understanding exists', async () => {
    const bare = { id: 'empty', context: { tenantId: '999' } };
    const turn = await maybeHandleRetrievalBeforeDelegationTurn({
      question: 'What do you currently understand about my business?',
      session: bare,
      context: bare.context,
      cieOpts: { store: createMemoryStore() },
    });
    assert.ok(turn);
    assert.match(turn.prose, /don't currently know enough|Client Intelligence/i);
    assert.equal(turn.knowledgeState, KNOWLEDGE_STATES.NEVER_LEARNED);
    assert.equal(turn.delegated, false);
  });
});

describe('SPEC-103 composeDurableRetrievalAnswer', () => {
  it('returns rich summary via formatUnderstandingAnswer', async () => {
    const store = createMemoryStore();
    const cieOpts = await approveAnchor(store);
    const bundle = await loadDurableBusinessUnderstanding({
      tenantId: String(ANCHOR_ID),
      cieOpts,
    });
    const mode = { kind: 'retrieval' };
    const answer = composeDurableRetrievalAnswer(
      'What do you currently understand about Anchor Cleaning?',
      mode,
      bundle
    );
    assert.match(answer.prose, /Anchor Cleaning/i);
    assert.match(answer.prose, /Manchester/i);
    assert.ok(answer.used.includes('blueprint'));
  });
});
