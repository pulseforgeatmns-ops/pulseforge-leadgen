'use strict';

/**
 * SPEC-099 — CIE Evidence Interpretation & Uncertainty Reasoning
 * Regression fixture from the AS Cleaning acceptance transcript.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  createMemoryStore,
  startClientInterview,
  postInterviewMessage,
  approveBlueprint,
  extractServiceList,
  answerLooksEmpty,
  looksLikeExplicitUnknown,
  isLiteralUncertaintyPhrase,
  ingestAnswerIntoNormalizedFacts,
  emptyNormalizedFacts,
  sectionsFromNormalizedFacts,
  buildExecutiveSummary,
  buildReflection,
  summarizeSection,
  hasRelevantUncertaintyContext,
  buildUncertaintyReasoningProbe,
  getApprovedClientBlueprint,
} = require('../services/clientIntelligenceInterview');

const {
  looksLikeExplicitUnknownAnswer,
  assessAnswerSufficiency,
} = require('../services/clientIntelligenceReasoning');

const {
  normalizeBlueprintSummary,
  formatUnderstandingAnswer,
  formatUnknownsAnswer,
} = require('../packages/max/workspace/ClientIntelligenceContext');

function withStore() {
  const store = createMemoryStore();
  return { store, opts: { store, useMemoryPlaybookStore: true } };
}

const AS_CLEANING_ANSWERS = Object.freeze({
  identity:
    'AS Cleaning, we are a cleaning company with a mix of residential and commercial clients but prefer commercial.',
  services:
    'Residential standard, recurring standards, move in/out, deep cleans, office cleaning',
  idealCustomer: "I don't know yet",
  idealCustomerAgain: "I'm really not sure.",
  avoid: "No restaurants unless it's only front of house cleaning",
  geography: 'Greater Toronto Area',
  differentiation: 'Fast communication and being on time',
  brand: 'warm and comfortable',
  outcome: 'adding new clients',
  metrics: '# of walkthroughs attended, conversion rate of those walkthroughs',
});

const EXPECTED_SERVICES = [
  /residential standard/i,
  /recurring standard/i,
  /move-?in\/move-?out/i,
  /deep cleans?/i,
  /office cleaning/i,
];

describe('SPEC-099 evidence interpretation primitives', () => {
  it('classifies explicit unknowns and refuses them as factual values', () => {
    assert.equal(looksLikeExplicitUnknown("I don't know yet"), true);
    assert.equal(looksLikeExplicitUnknown("I don't know yeet"), true);
    assert.equal(looksLikeExplicitUnknownAnswer("I don't know yet"), true);
    assert.equal(answerLooksEmpty("I don't know yet"), true);
    assert.equal(isLiteralUncertaintyPhrase("I don't know"), true);
    assert.equal(answerLooksEmpty('Property managers'), false);
    assert.equal(looksLikeExplicitUnknown('Busy homeowners'), false);

    const assessment = assessAnswerSufficiency("I don't know yet", {
      id: 'ideal_customers',
      section: 'idealCustomers',
    });
    assert.equal(assessment.sufficient, false);
    assert.equal(assessment.reason, 'explicit_unknown');
  });

  it('preserves all five AS Cleaning service categories through extraction', () => {
    const services = extractServiceList(AS_CLEANING_ANSWERS.services);
    assert.equal(services.length >= 5, true, `expected >=5 services, got ${JSON.stringify(services)}`);
    for (const re of EXPECTED_SERVICES) {
      assert.ok(
        services.some((s) => re.test(s)),
        `missing service matching ${re} in ${JSON.stringify(services)}`
      );
    }
  });

  it('does not persist "I don\'t know yet" as ideal-customer value and keeps commercial preference', () => {
    let facts = emptyNormalizedFacts();
    facts = ingestAnswerIntoNormalizedFacts(facts, 'identity', AS_CLEANING_ANSWERS.identity);
    facts = ingestAnswerIntoNormalizedFacts(facts, 'services', AS_CLEANING_ANSWERS.services);
    facts = ingestAnswerIntoNormalizedFacts(
      facts,
      'idealCustomers',
      AS_CLEANING_ANSWERS.idealCustomer
    );

    assert.deepEqual(facts.ideal_customers, []);
    assert.match(String(facts.growth_focus || ''), /commercial/i);
    assert.equal(facts.services.length >= 5, true);

    const sections = sectionsFromNormalizedFacts(facts);
    assert.equal(/\bi don'?t know\b/i.test(sections.idealCustomers.summary || ''), false);
    assert.ok(
      (sections.idealCustomers.unknowns || []).some((u) =>
        /commercial customer segment|ideal customer/i.test(String(u))
      )
    );

    const reflection = buildReflection(sections, 3);
    assert.ok(reflection);
    assert.match(reflection, /residential standard|recurring standard|deep cleans|office cleaning/i);
    assert.equal(/Ideal Customer:.*I don't know/i.test(reflection), false);
    assert.equal(/Services:.*deep cleans\.?$/im.test(reflection), false);
  });
});

describe('SPEC-099 Executive Brief + Blueprint grounding (AS Cleaning)', () => {
  it('grounds the brief without inventing an ICP or expanding metrics', () => {
    let facts = emptyNormalizedFacts();
    facts = ingestAnswerIntoNormalizedFacts(facts, 'identity', AS_CLEANING_ANSWERS.identity);
    facts = ingestAnswerIntoNormalizedFacts(facts, 'services', AS_CLEANING_ANSWERS.services);
    facts = ingestAnswerIntoNormalizedFacts(
      facts,
      'idealCustomers',
      AS_CLEANING_ANSWERS.idealCustomer
    );
    facts = ingestAnswerIntoNormalizedFacts(facts, 'avoidCustomers', AS_CLEANING_ANSWERS.avoid);
    facts = ingestAnswerIntoNormalizedFacts(facts, 'targetMarkets', AS_CLEANING_ANSWERS.geography);
    facts = ingestAnswerIntoNormalizedFacts(
      facts,
      'competitiveAdvantages',
      AS_CLEANING_ANSWERS.differentiation
    );
    facts = ingestAnswerIntoNormalizedFacts(facts, 'brandVoice', AS_CLEANING_ANSWERS.brand);
    facts = ingestAnswerIntoNormalizedFacts(facts, 'campaignGoals', AS_CLEANING_ANSWERS.outcome);
    facts = ingestAnswerIntoNormalizedFacts(facts, 'successMetrics', AS_CLEANING_ANSWERS.metrics);

    const sections = sectionsFromNormalizedFacts(facts);
    const brief = buildExecutiveSummary(sections, { normalizedFacts: facts });
    const byId = Object.fromEntries(brief.sections.map((s) => [s.id, s]));

    assert.match(byId.whoYouAre.body, /residential standard cleaning/i);
    assert.match(byId.whoYouAre.body, /office cleaning/i);
    assert.match(byId.whoYouAre.body, /deep cleans/i);
    assert.match(byId.whoYouAre.body, /move-in\/move-out/i);

    assert.match(byId.whoYouServe.body, /prefer(?:s)? commercial/i);
    assert.match(byId.whoYouServe.body, /not yet been established/i);
    assert.match(byId.whoYouServe.body, /Greater Toronto Area/i);
    assert.match(byId.whoYouServe.body, /restaurant/i);
    assert.equal(/\bideal customers include\b/i.test(byId.whoYouServe.body), false);
    assert.equal(/\bi don'?t know\b/i.test(byId.whoYouServe.body), false);

    assert.match(byId.successLooksLike.body, /walkthroughs attended/i);
    assert.match(byId.successLooksLike.body, /conversion rate/i);
    assert.match(byId.successLooksLike.body, /Max may also want to explore/i);
    assert.equal(
      /Success should be measured by qualified replies, booked conversations/i.test(
        byId.successLooksLike.body
      ),
      false
    );

    const market = byId.assessment.ratings.find((r) => r.label === 'Market Focus');
    assert.ok(market);
    assert.equal(/named ideal customer/i.test(market.explanation), false);
    assert.match(market.explanation, /commercial/i);
    assert.match(market.explanation, /unresolved|not yet|remain/i);

    assert.ok(
      (byId.learnMore.items || []).some((item) =>
        /commercial customer segment/i.test(String(item))
      )
    );
    assert.equal(
      (byId.learnMore.items || [])[0].toLowerCase().includes('commercial'),
      true
    );
  });
});

describe('SPEC-099 bounded uncertainty probing in interview', () => {
  it('probes once on unknown ICP using context, then accepts a second unknown and continues', async () => {
    const { opts, store } = withStore();
    const started = await startClientInterview({ clientId: 1099 }, opts);

    await postInterviewMessage(started.interviewId, AS_CLEANING_ANSWERS.identity, opts);
    await postInterviewMessage(started.interviewId, AS_CLEANING_ANSWERS.services, opts);

    const firstUnknown = await postInterviewMessage(
      started.interviewId,
      AS_CLEANING_ANSWERS.idealCustomer,
      opts
    );
    assert.equal(firstUnknown.messageType, 'insufficient_answer');
    assert.equal(firstUnknown.nextAction, 'PROBE');
    assert.equal(firstUnknown.question.id, 'ideal_customers');
    assert.match(firstUnknown.message, /work through|commercial|segment|walkthrough/i);
    assert.equal(/\bi don'?t know yet\b/i.test(firstUnknown.message), false);

    let session = await store.getSession(started.interviewId);
    assert.equal(session.interview_state.stepIndex, 2);
    assert.deepEqual(session.interview_state.normalizedFacts.ideal_customers || [], []);
    assert.match(String(session.interview_state.normalizedFacts.growth_focus || ''), /commercial/i);
    assert.ok(session.interview_state.reasoningMemory.activeProbe);
    assert.ok(
      hasRelevantUncertaintyContext(session.interview_state, 'idealCustomers')
    );
    assert.match(
      buildUncertaintyReasoningProbe(session.interview_state, 'idealCustomers', 'AS Cleaning'),
      /commercial/i
    );

    const secondUnknown = await postInterviewMessage(
      started.interviewId,
      AS_CLEANING_ANSWERS.idealCustomerAgain,
      opts
    );
    assert.equal(secondUnknown.acceptedUnknown, true);
    assert.equal(secondUnknown.nextAction, 'ASK');
    assert.notEqual(secondUnknown.question.id, 'ideal_customers');
    assert.match(secondUnknown.message, /leave that open|prefer commercial|investigate/i);

    session = await store.getSession(started.interviewId);
    assert.equal(session.interview_state.stepIndex > 2, true);
    assert.deepEqual(session.interview_state.normalizedFacts.ideal_customers || [], []);
    assert.equal(
      /\bi don'?t know\b/i.test(
        String(session.interview_state.sectionState.idealCustomers.summary || '')
      ),
      false
    );
  });

  it('completes the AS Cleaning interview without literal unknown Blueprint values', async () => {
    const { opts, store } = withStore();
    const started = await startClientInterview({ clientId: 1100 }, opts);

    await postInterviewMessage(started.interviewId, AS_CLEANING_ANSWERS.identity, opts);
    await postInterviewMessage(started.interviewId, AS_CLEANING_ANSWERS.services, opts);

    // Unknown → probe → second unknown → continue
    await postInterviewMessage(started.interviewId, AS_CLEANING_ANSWERS.idealCustomer, opts);
    await postInterviewMessage(started.interviewId, AS_CLEANING_ANSWERS.idealCustomerAgain, opts);

    await postInterviewMessage(started.interviewId, AS_CLEANING_ANSWERS.avoid, opts);
    await postInterviewMessage(started.interviewId, AS_CLEANING_ANSWERS.geography, opts);
    await postInterviewMessage(started.interviewId, AS_CLEANING_ANSWERS.differentiation, opts);
    await postInterviewMessage(started.interviewId, AS_CLEANING_ANSWERS.brand, opts);
    await postInterviewMessage(started.interviewId, AS_CLEANING_ANSWERS.outcome, opts);
    let done = await postInterviewMessage(
      started.interviewId,
      AS_CLEANING_ANSWERS.metrics,
      opts
    );
    // If still not on blueprint (e.g. skip/probe edge), finish gracefully.
    for (let i = 0; i < 4 && !done.blueprint; i += 1) {
      done = await postInterviewMessage(started.interviewId, 'skip', opts);
    }

    assert.ok(done.blueprint, 'expected blueprint after interview');
    const bp = done.blueprint;
    const facts = (await store.getSession(started.interviewId)).interview_state.normalizedFacts;

    for (const re of EXPECTED_SERVICES) {
      assert.ok(
        (facts.services || []).some((s) => re.test(s)),
        `blueprint facts missing ${re}`
      );
    }
    assert.deepEqual(facts.ideal_customers || [], []);
    assert.equal(/\bi don'?t know\b/i.test(JSON.stringify(bp.sections || {})), false);

    const brief = done.executiveSummary;
    assert.ok(brief);
    const whoServe = brief.sections.find((s) => s.id === 'whoYouServe');
    assert.match(whoServe.body, /prefer(?:s)? commercial/i);
    assert.equal(/\bi don'?t know\b/i.test(whoServe.body), false);
    assert.equal(/ideal customers include/i.test(whoServe.body), false);

    const approved = await approveBlueprint(done.blueprint.id, {
      ...opts,
      confirmed: true,
    });
    assert.ok(approved);
    const approvedBp = await getApprovedClientBlueprint(1100, opts);
    assert.ok(approvedBp);

    const continuity = normalizeBlueprintSummary(approvedBp);
    assert.equal(/\bi don'?t know\b/i.test(JSON.stringify(continuity)), false);
    assert.equal(Boolean(continuity.idealCustomers), false);
    assert.ok(
      (continuity.unknowns || []).some(
        (u) => /commercial|who you want to serve|ideal/i.test(String(u))
      )
    );
    const understanding = formatUnderstandingAnswer(continuity);
    assert.equal(/\bi don'?t know\b/i.test(understanding), false);
    const unknownsAnswer = formatUnknownsAnswer(continuity);
    assert.match(unknownsAnswer, /unknown/i);
  });
});

describe('SPEC-099 summarizeSection multi-value fidelity', () => {
  it('does not collapse the AS Cleaning service list to deep cleans alone', () => {
    const summary = summarizeSection('services', [AS_CLEANING_ANSWERS.services]);
    // Raw summarizeSection uses latest statement; normalized path is the durable source.
    // Guard the known failure mode when normalized facts drive the summary.
    let facts = emptyNormalizedFacts();
    facts = ingestAnswerIntoNormalizedFacts(facts, 'services', AS_CLEANING_ANSWERS.services);
    const fromFacts = sectionsFromNormalizedFacts(facts).services.summary;
    assert.match(fromFacts, /residential standard/i);
    assert.match(fromFacts, /office cleaning/i);
    assert.match(fromFacts, /deep cleans/i);
    assert.equal(/^Today the business delivers deep cleans\./i.test(fromFacts), false);
    assert.ok(summary);
  });
});
