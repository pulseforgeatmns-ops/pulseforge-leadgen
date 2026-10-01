'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  MESSAGE_TYPES,
  createMemoryStore,
  startClientInterview,
  postInterviewMessage,
  classifyInterviewMessage,
  QUESTION_BANK,
} = require('../services/clientIntelligenceInterview');

const {
  detectInterviewEscapeIntent,
  looksLikeInterviewWritingGuidance,
} = require('../services/clientIntelligenceReasoning');

function withStore() {
  const store = createMemoryStore();
  return { store, opts: { store, useMemoryPlaybookStore: true } };
}

const STRUCTURED_IDENTITY_ANSWER =
  'Business name: Studio Substral. What we do today: Studio Substral sells website design and redesign services for local businesses who need a credible web presence.';

const PARAGRAPH_IDENTITY_ANSWER =
  'Studio Substral is a website design and redesign service for local businesses who need a credible web presence.';

describe('Client Intelligence interview answer routing (P0)', () => {
  it('1. direct structured answer persists as evidence and advances once', async () => {
    const { opts, store } = withStore();
    const started = await startClientInterview({ clientId: 501 }, opts);
    assert.equal(started.question.id, 'identity');
    const opened = await store.getSession(started.interviewId);
    assert.equal(opened.interview_state.awaitingQuestionId, 'identity');

    const turn = await postInterviewMessage(started.interviewId, STRUCTURED_IDENTITY_ANSWER, opts);
    assert.equal(turn.messageType, MESSAGE_TYPES.DIRECT_ANSWER);
    assert.equal(turn.question.id, 'services');
    assert.match(turn.message, /services your business provides/i);

    const session = await store.getSession(started.interviewId);
    assert.match(session.interview_state.answers.identity || '', /Studio Substral/i);
    assert.equal(session.interview_state.stepIndex, 1);
    assert.equal(session.interview_state.awaitingQuestionId, 'services');
    const evidence = await store.listEvidence(started.interviewId);
    assert.ok(evidence.some((row) => row.category === 'identity' && /Studio Substral/i.test(row.statement)));
    const facts = session.interview_state.normalizedFacts || {};
    if (facts.business_name) {
      assert.match(facts.business_name, /Studio Substral/i);
    }
  });

  it('2. natural paragraph answer is not classified as writing guidance', async () => {
    const { opts } = withStore();
    const started = await startClientInterview({ clientId: 502 }, opts);
    const identityQ = QUESTION_BANK.find((q) => q.id === 'identity');
    assert.equal(
      classifyInterviewMessage(PARAGRAPH_IDENTITY_ANSWER, { activeQuestion: identityQ }),
      MESSAGE_TYPES.DIRECT_ANSWER
    );
    const turn = await postInterviewMessage(started.interviewId, PARAGRAPH_IDENTITY_ANSWER, opts);
    assert.equal(turn.messageType, MESSAGE_TYPES.DIRECT_ANSWER);
    assert.equal(turn.question.id, 'services');
    assert.doesNotMatch(turn.message || '', /guidance for how I write/i);
  });

  it('3. repeated answer does not loop on the same question', async () => {
    const { opts, store } = withStore();
    const started = await startClientInterview({ clientId: 503 }, opts);
    const first = await postInterviewMessage(started.interviewId, PARAGRAPH_IDENTITY_ANSWER, opts);
    assert.equal(first.question.id, 'services');
    const second = await postInterviewMessage(
      started.interviewId,
      'Office cleaning and recurring commercial cleans.',
      opts
    );
    assert.equal(second.question.id, 'ideal_customers');
    const session = await store.getSession(started.interviewId);
    assert.equal(session.interview_state.stepIndex, 2);
  });

  it('4. explicit writing guidance stays refinement and does not mark identity answered', async () => {
    const { opts, store } = withStore();
    const started = await startClientInterview({ clientId: 504 }, opts);
    const guidance = 'Make the website copy sound more premium and less generic';
    assert.ok(looksLikeInterviewWritingGuidance(guidance));
    const turn = await postInterviewMessage(started.interviewId, guidance, opts);
    assert.equal(turn.messageType, MESSAGE_TYPES.REFINEMENT_FEEDBACK);
    assert.equal(turn.question.id, 'identity');
    assert.match(turn.message, /guidance for how I write/i);
    assert.match(turn.message, /tell me about the business/i);

    const session = await store.getSession(started.interviewId);
    assert.equal(session.interview_state.stepIndex, 0);
    assert.equal(session.interview_state.awaitingQuestionId, 'identity');
    assert.equal(session.interview_state.answers.identity, undefined);
  });

  it('5. skip advances with deferred state without faking evidence', async () => {
    const { opts, store } = withStore();
    const started = await startClientInterview({ clientId: 505 }, opts);
    assert.equal(detectInterviewEscapeIntent('skip this for now'), 'skip');
    const turn = await postInterviewMessage(started.interviewId, 'skip this for now', opts);
    assert.equal(turn.messageType, MESSAGE_TYPES.SKIP);
    assert.equal(turn.question.id, 'services');
    const session = await store.getSession(started.interviewId);
    assert.equal(session.interview_state.stepIndex, 1);
    assert.ok(!session.interview_state.answers.identity);
  });

  it('6. explicit skip after probe uses defer language — not a fake identity answer', async () => {
    const { opts, store } = withStore();
    const started = await startClientInterview({ clientId: 506 }, opts);
    let turn = await postInterviewMessage(started.interviewId, 'various things', opts);
    assert.equal(turn.messageType, MESSAGE_TYPES.INSUFFICIENT_ANSWER);
    assert.equal(turn.question.id, 'identity');
    turn = await postInterviewMessage(started.interviewId, 'skip this for now', opts);
    assert.equal(turn.messageType, MESSAGE_TYPES.SKIP);
    assert.match(turn.message, /leave that open|unresolved|skip/i);
    const session = await store.getSession(started.interviewId);
    assert.equal(session.interview_state.stepIndex, 1);
    assert.ok(!session.interview_state.answers.identity);
  });
});
