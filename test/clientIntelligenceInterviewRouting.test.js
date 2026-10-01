'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  MESSAGE_TYPES,
  createMemoryStore,
  startClientInterview,
  postInterviewMessage,
  classifyInterviewMessage,
  looksLikeSupplementalContext,
  looksLikeRefinementFeedback,
  containsMetaInstructionLanguage,
  QUESTION_BANK,
  buildExecutiveSummary,
  sectionsFromNormalizedFacts,
} = require('../services/clientIntelligenceInterview');

const {
  detectInterviewEscapeIntent,
  looksLikeInterviewWritingGuidance,
} = require('../services/clientIntelligenceReasoning');

const METRICS_ANSWER =
  'We watch qualified prospects entering the pipeline, positive replies, discovery calls booked, proposals sent, and revenue closed.';

const SIGNAL_METRICS_ANSWER =
  'A weak signal is when reply quality drops; a strong signal is when discovery calls convert to proposals. We also track qualified prospects and revenue closed.';

const REGENERATE_DEMAND_ANSWER =
  'We regenerate interest through outreach and measure positive replies, discovery calls booked, proposals sent, and revenue closed.';

async function advanceToSuccessMetricsQuestion(interviewId, opts) {
  const steps = [
    'Studio Substral — website design and redesign for local businesses who need a credible web presence.',
    'Website design, redesign, and landing pages.',
    'Professional service firms in Greater Manchester — law firms and accountants.',
    'Price-driven clients looking for the cheapest possible website.',
    'Greater Manchester and southern New Hampshire first.',
    'They choose us for clarity, speed, and a credible design process.',
    'Clear, confident, and practical — never hypey or jargon-heavy.',
    'More qualified discovery calls and signed web projects in the next 90 days.',
  ];
  for (const step of steps) {
    await postInterviewMessage(interviewId, step, opts);
  }
}

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

  it('7. exclusion answer with price keywords stays on Customers to Avoid', async () => {
    const { opts, store } = withStore();
    const started = await startClientInterview({ clientId: 507 }, opts);
    await postInterviewMessage(
      started.interviewId,
      'Studio Substral — website design and redesign for local businesses.',
      opts
    );
    await postInterviewMessage(
      started.interviewId,
      'Website design, redesign, and landing pages.',
      opts
    );
    await postInterviewMessage(
      started.interviewId,
      'Local service businesses that value credibility over the cheapest quote.',
      opts
    );

    const exclusion =
      "We'd also rather not take on price-driven clients looking for the cheapest possible website, one-off budget shoppers, or anyone who treats the site as a commodity.";
    const avoidQ = QUESTION_BANK.find((q) => q.id === 'avoid_customers');
    assert.equal(
      classifyInterviewMessage(exclusion, {
        activeQuestion: avoidQ,
        awaitingQuestionId: 'avoid_customers',
      }),
      MESSAGE_TYPES.DIRECT_ANSWER
    );
    assert.equal(looksLikeSupplementalContext(exclusion, { activeQuestion: avoidQ }), false);

    const turn = await postInterviewMessage(started.interviewId, exclusion, opts);
    assert.equal(turn.messageType, MESSAGE_TYPES.DIRECT_ANSWER);
    assert.equal(turn.question.id, 'target_markets');

    const session = await store.getSession(started.interviewId);
    assert.match(session.interview_state.answers.avoid_customers || '', /cheapest possible website/i);
    assert.match(session.interview_state.answers.avoid_customers || '', /price-driven/i);
    assert.match(
      session.interview_state.sectionState.avoidCustomers.summary || '',
      /cheapest possible website|price-driven/i
    );
    assert.doesNotMatch(turn.message || '', /\bunder pricing\b/i);

    const evidence = await store.listEvidence(started.interviewId);
    assert.ok(
      evidence.some(
        (row) =>
          row.category === 'avoidCustomers' &&
          /cheapest possible website|price-driven/i.test(row.statement)
      )
    );
    const supporting = session.interview_state.intakeSupportingEvidence || [];
    assert.ok(
      supporting.some(
        (row) =>
          row.questionId === 'avoid_customers' &&
          row.domains.includes('pricing')
      )
    );
  });

  it('8. ICP answer mentioning geography stays on Ideal Customers', async () => {
    const { opts, store } = withStore();
    const started = await startClientInterview({ clientId: 508 }, opts);
    await postInterviewMessage(
      started.interviewId,
      'Studio Substral — credible websites for local businesses.',
      opts
    );
    await postInterviewMessage(
      started.interviewId,
      'Website design and redesign.',
      opts
    );

    const icp =
      'We most want to work with professional service firms in Greater Manchester and southern New Hampshire — law firms, accountants, and medical practices that need a credible web presence and steady lead flow from owner-operators who delegate marketing.';
    const icpQ = QUESTION_BANK.find((q) => q.id === 'ideal_customers');
    assert.equal(
      classifyInterviewMessage(icp, {
        activeQuestion: icpQ,
        awaitingQuestionId: 'ideal_customers',
      }),
      MESSAGE_TYPES.DIRECT_ANSWER
    );

    const turn = await postInterviewMessage(started.interviewId, icp, opts);
    assert.equal(turn.question.id, 'avoid_customers');
    const session = await store.getSession(started.interviewId);
    assert.match(session.interview_state.answers.ideal_customers || '', /Greater Manchester/i);
    assert.match(session.interview_state.answers.ideal_customers || '', /owner-operators/i);
    assert.match(session.interview_state.sectionState.idealCustomers.summary || '', /Greater Manchester/i);
    assert.match(session.interview_state.sectionState.idealCustomers.summary || '', /law firms/i);
    assert.equal(
      /^Ideal customers are lead flow, operator/i.test(
        session.interview_state.sectionState.idealCustomers.summary || ''
      ),
      false
    );

    const supporting = session.interview_state.intakeSupportingEvidence || [];
    assert.ok(
      supporting.some(
        (row) => row.questionId === 'ideal_customers' && row.domains.includes('geography')
      )
    );

    const brief = buildExecutiveSummary(session.interview_state.sectionState, {
      normalizedFacts: session.interview_state.normalizedFacts,
    });
    const whoYouServe = brief.sections.find((s) => s.id === 'whoYouServe');
    assert.match(whoYouServe.body, /Greater Manchester|professional service/i);
  });

  it('9. answered guided fields are skipped on resume — avoid is not re-asked', async () => {
    const { opts, store } = withStore();
    const started = await startClientInterview({ clientId: 509 }, opts);
    await postInterviewMessage(
      started.interviewId,
      'Studio Substral — website design for local businesses.',
      opts
    );
    await postInterviewMessage(
      started.interviewId,
      'Website design and redesign.',
      opts
    );
    await postInterviewMessage(
      started.interviewId,
      'Local businesses that care about credibility.',
      opts
    );
    await postInterviewMessage(
      started.interviewId,
      'Price-driven clients looking for the cheapest possible website.',
      opts
    );

    let session = await store.getSession(started.interviewId);
    assert.match(session.interview_state.answers.avoid_customers || '', /cheapest possible website/i);
    assert.equal(QUESTION_BANK[session.interview_state.stepIndex].id, 'target_markets');

    session.interview_state.stepIndex = 3;
    session.interview_state.awaitingQuestionId = 'avoid_customers';
    await store.updateSession(started.interviewId, { interview_state: session.interview_state });

    const resumed = await startClientInterview({ clientId: 509 }, opts);
    assert.equal(resumed.question.id, 'target_markets');
    session = await store.getSession(started.interviewId);
    assert.equal(QUESTION_BANK[session.interview_state.stepIndex].id, 'target_markets');
    assert.match(session.interview_state.answers.avoid_customers || '', /cheapest possible website/i);
  });

  it('10. success-metrics answer persists as business evidence and advances', async () => {
    const { opts, store } = withStore();
    const started = await startClientInterview({ clientId: 510 }, opts);
    const prior = [
      'Studio Substral — website design for local businesses.',
      'Website design and redesign.',
      'Local businesses that care about credibility.',
      'Price-driven clients looking for the cheapest possible website.',
      'Greater Manchester NH and southern New Hampshire.',
      'Trust and responsiveness tip the decision.',
      'Professional, direct, no hype.',
      'Book more qualified discovery calls in the next 90 days.',
    ];
    for (const answer of prior) {
      await postInterviewMessage(started.interviewId, answer, opts);
    }
    const sessionBefore = await store.getSession(started.interviewId);
    assert.equal(sessionBefore.interview_state.awaitingQuestionId, 'success_metrics');

    const metricsAnswer =
      'Qualified prospects identified, prospects contacted, positive replies, discovery calls booked, proposals sent, proposals accepted, and revenue closed. We also watch reply quality, call quality, urgency, budget fit, and strong vs. weak demand signals.';
    const successQ = QUESTION_BANK.find((q) => q.id === 'success_metrics');
    assert.equal(
      classifyInterviewMessage(metricsAnswer, {
        activeQuestion: successQ,
        awaitingQuestionId: 'success_metrics',
        looksLikeAddOn: looksLikeSupplementalContext,
      }),
      MESSAGE_TYPES.DIRECT_ANSWER
    );
    assert.equal(
      detectInterviewEscapeIntent(metricsAnswer, {
        activeQuestion: successQ,
        awaitingQuestionId: 'success_metrics',
      }),
      null
    );
    assert.equal(
      looksLikeInterviewWritingGuidance(metricsAnswer, {
        activeQuestion: successQ,
        awaitingQuestionId: 'success_metrics',
      }),
      false
    );

    const turn = await postInterviewMessage(started.interviewId, metricsAnswer, opts);
    assert.equal(turn.nextAction, 'GENERATE_BLUEPRINT');
    assert.doesNotMatch(turn.message || '', /guidance for how I write/i);
    assert.equal(turn.question, null);

    const session = await store.getSession(started.interviewId);
    assert.match(session.interview_state.answers.success_metrics || '', /qualified prospects/i);
    assert.match(session.interview_state.answers.success_metrics || '', /budget fit/i);
    const metrics = session.interview_state.normalizedFacts.success_metrics || [];
    assert.ok(metrics.some((item) => /qualified prospects/i.test(item)));
    assert.ok(metrics.some((item) => /positive replies|discovery calls/i.test(item)));
    assert.ok(metrics.some((item) => /reply quality|budget fit|demand signals/i.test(item)));
    assert.equal(session.interview_state.stepIndex >= QUESTION_BANK.length, true);

    const summary = session.interview_state.sectionState.successMetrics.summary || '';
    assert.match(summary, /Success will be judged by/i);
    assert.match(summary, /qualified prospects/i);
    assert.doesNotMatch(summary, /^Success will be judged by reply quality\b/i);
  });

  it('11. success-metrics answer with signal-quality vocabulary is not re-asked', async () => {
    const { opts, store } = withStore();
    const started = await startClientInterview({ clientId: 511 }, opts);
    for (const answer of [
      'Studio Substral — credible websites for local businesses.',
      'Website design and redesign.',
      'Local service businesses in southern NH.',
      'Commodity price shoppers.',
      'Greater Manchester NH.',
      'Responsiveness tips the decision.',
      'Grounded and direct.',
      'More booked discovery calls in 90 days.',
    ]) {
      await postInterviewMessage(started.interviewId, answer, opts);
    }

    const metricsAnswer =
      'Strong signal vs weak signal on reply quality, call quality, urgency, and budget fit alongside qualified prospects, positive replies, discovery calls, proposals, and revenue closed.';
    const turn = await postInterviewMessage(started.interviewId, metricsAnswer, opts);
    assert.doesNotMatch(turn.message || '', /guidance for how I write/i);
    assert.notEqual(turn.question?.id, 'success_metrics');

    const session = await store.getSession(started.interviewId);
    assert.match(session.interview_state.answers.success_metrics || '', /strong signal/i);
    assert.match(session.interview_state.answers.success_metrics || '', /weak signal/i);
  });

  it('12. persisted success_metrics evidence recovers stuck step without re-ask loop', async () => {
    const { opts, store } = withStore();
    const started = await startClientInterview({ clientId: 512 }, opts);
    for (const answer of [
      'Studio Substral — credible websites for local businesses.',
      'Website design and redesign.',
      'Local service businesses in southern NH.',
      'Commodity price shoppers.',
      'Greater Manchester NH.',
      'Responsiveness tips the decision.',
      'Grounded and direct.',
      'More booked discovery calls in 90 days.',
    ]) {
      await postInterviewMessage(started.interviewId, answer, opts);
    }

    let session = await store.getSession(started.interviewId);
    assert.equal(session.interview_state.awaitingQuestionId, 'success_metrics');
    session.interview_state.normalizedFacts = session.interview_state.normalizedFacts || {};
    session.interview_state.normalizedFacts.success_metrics = [
      'qualified prospects identified',
      'discovery calls booked',
      'revenue closed',
    ];
    session.interview_state.sectionState = session.interview_state.sectionState || {};
    session.interview_state.sectionState.successMetrics = {
      summary: 'Success will be judged by qualified prospects identified, discovery calls booked, revenue closed',
      confidence: 0.82,
      evidenceIds: [],
      unknowns: [],
    };
    delete session.interview_state.answers.success_metrics;
    await store.updateSession(started.interviewId, { interview_state: session.interview_state });

    const turn = await postInterviewMessage(
      started.interviewId,
      'Qualified prospects identified, discovery calls booked, and revenue closed.',
      opts
    );
    assert.doesNotMatch(turn.message || '', /guidance for how I write/i);
    assert.notEqual(turn.question?.id, 'success_metrics');
    session = await store.getSession(started.interviewId);
    assert.equal(session.interview_state.stepIndex >= QUESTION_BANK.length, true);
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
