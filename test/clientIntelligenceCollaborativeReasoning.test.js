'use strict';

/**
 * SPEC-100 — CIE Collaborative Reasoning & Uncertainty Resolution
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  createMemoryStore,
  startClientInterview,
  postInterviewMessage,
  getInterview,
  approveBlueprint,
  extractServiceList,
  looksLikeExplicitUnknown,
  ingestAnswerIntoNormalizedFacts,
  emptyNormalizedFacts,
  sectionsFromNormalizedFacts,
  buildExecutiveSummary,
  buildUncertaintyReasoningProbe,
  classifyAnswerDisposition,
  ANSWER_DISPOSITIONS,
  MAX_PROBE_ATTEMPTS,
} = require('../services/clientIntelligenceInterview');

const {
  assessAnswerSufficiency,
  looksLikeVagueAnswer,
  looksLikeGenericCategoryAnswer,
  looksLikeExplicitDeferral,
} = require('../services/clientIntelligenceReasoning');

function withStore() {
  const store = createMemoryStore();
  return { store, opts: { store, useMemoryPlaybookStore: true } };
}

const IDENTITY =
  'AS Cleaning. We do residential and commercial cleaning but prefer commercial.';
const SERVICES =
  'Residential standard, recurring standards, move in/out, deep cleans, office cleaning';

async function advanceToIdealCustomer(opts) {
  const started = await startClientInterview({ clientId: 11100 }, opts);
  await postInterviewMessage(started.interviewId, IDENTITY, opts);
  await postInterviewMessage(started.interviewId, SERVICES, opts);
  return started;
}

describe('SPEC-100 answer disposition model', () => {
  it('distinguishes uncertain, vague, deferred, and accepted answers', () => {
    assert.equal(MAX_PROBE_ATTEMPTS, 2);
    assert.equal(
      classifyAnswerDisposition("I don't know yet.", { section: 'idealCustomers' })
        .disposition,
      ANSWER_DISPOSITIONS.UNCERTAIN
    );
    assert.equal(
      classifyAnswerDisposition('Businesses.', { section: 'idealCustomers' }).disposition,
      ANSWER_DISPOSITIONS.NEEDS_SPECIFICITY
    );
    assert.equal(
      classifyAnswerDisposition('Skip this for now.', { section: 'idealCustomers' })
        .disposition,
      ANSWER_DISPOSITIONS.DEFERRED
    );
    assert.equal(
      classifyAnswerDisposition("Let's leave it open.", { section: 'idealCustomers' })
        .disposition,
      ANSWER_DISPOSITIONS.DEFERRED
    );
    assert.equal(
      classifyAnswerDisposition(
        'Small professional offices in the GTA with recurring evening cleaning.',
        { section: 'idealCustomers', hasSpecificity: true }
      ).disposition,
      ANSWER_DISPOSITIONS.ACCEPTED
    );
    assert.equal(looksLikeGenericCategoryAnswer('Businesses'), true);
    assert.equal(looksLikeVagueAnswer('Businesses'), true);
    assert.equal(looksLikeExplicitDeferral('Skip this for now.'), true);
    assert.equal(looksLikeExplicitUnknown("We're still deciding."), true);
    assert.equal(
      assessAnswerSufficiency('Businesses.', { section: 'idealCustomers' }).reason,
      'needs_specificity'
    );
  });
});

describe('SPEC-100 TEST A — explicit uncertainty', () => {
  it('does not store uncertainty as ideal customer and stays for collaborative reasoning', async () => {
    const { opts, store } = withStore();
    const started = await advanceToIdealCustomer(opts);

    const res = await postInterviewMessage(started.interviewId, "I don't know yet.", opts);
    assert.equal(res.answerDisposition, ANSWER_DISPOSITIONS.UNCERTAIN);
    assert.equal(res.nextAction, 'PROBE');
    assert.equal(res.question.id, 'ideal_customers');
    assert.match(res.message, /commercial|prefer|prioritize|property managers|offices/i);
    assert.equal(/\bi don'?t know yet\b/i.test(res.message), false);

    const session = await store.getSession(started.interviewId);
    assert.equal(session.interview_state.stepIndex, 2);
    assert.deepEqual(session.interview_state.normalizedFacts.ideal_customers || [], []);
    assert.match(String(session.interview_state.normalizedFacts.growth_focus || ''), /commercial/i);
    assert.ok(session.interview_state.reasoningMemory.activeProbe);
    assert.equal(session.interview_state.reasoningMemory.activeProbe.attemptCount, 1);
  });
});

describe('SPEC-100 TEST B — vague answer', () => {
  it('probes specificity and does not treat Businesses as high-confidence ICP', async () => {
    const { opts, store } = withStore();
    const started = await advanceToIdealCustomer(opts);

    const res = await postInterviewMessage(started.interviewId, 'Businesses.', opts);
    assert.equal(res.answerDisposition, ANSWER_DISPOSITIONS.NEEDS_SPECIFICITY);
    assert.equal(res.nextAction, 'PROBE');
    assert.equal(res.question.id, 'ideal_customers');
    assert.match(res.message, /kinds of businesses|more specific|segment|role/i);

    const session = await store.getSession(started.interviewId);
    assert.equal(session.interview_state.stepIndex, 2);
    assert.deepEqual(session.interview_state.normalizedFacts.ideal_customers || [], []);
    assert.equal(
      /\bbusinesses\b/i.test(
        String(session.interview_state.sectionState.idealCustomers.summary || '')
      ),
      false
    );

    let facts = emptyNormalizedFacts();
    facts = ingestAnswerIntoNormalizedFacts(facts, 'idealCustomers', 'Businesses');
    assert.deepEqual(facts.ideal_customers, []);
  });
});

describe('SPEC-100 TEST C — resolution after reasoning', () => {
  it('commits operator confirmation after a probe and advances', async () => {
    const { opts, store } = withStore();
    const started = await advanceToIdealCustomer(opts);

    await postInterviewMessage(started.interviewId, "I don't know.", opts);
    const resolved = await postInterviewMessage(
      started.interviewId,
      'Property managers sound most attractive because one relationship could lead to multiple buildings.',
      opts
    );
    assert.equal(resolved.answerDisposition, ANSWER_DISPOSITIONS.ACCEPTED);
    assert.equal(resolved.nextAction, 'ASK');
    assert.notEqual(resolved.question.id, 'ideal_customers');

    const session = await store.getSession(started.interviewId);
    assert.ok(
      (session.interview_state.normalizedFacts.ideal_customers || []).some((s) =>
        /property managers?/i.test(s)
      )
    );
    assert.equal(session.interview_state.stepIndex > 2, true);
    assert.equal(session.interview_state.reasoningMemory.activeProbe, null);
    assert.ok(
      Number(session.interview_state.sectionState.idealCustomers.confidence || 0) >= 0.5
    );
  });
});

describe('SPEC-100 TEST D — persistent uncertainty with bounded deferral', () => {
  it('preserves unknown after probe bound and explicit leave-it-open', async () => {
    const { opts, store } = withStore();
    const started = await advanceToIdealCustomer(opts);

    await postInterviewMessage(started.interviewId, "I don't know.", opts);
    const probe2 = await postInterviewMessage(started.interviewId, 'Still not sure.', opts);
    assert.equal(probe2.nextAction, 'PROBE');
    assert.equal(probe2.question.id, 'ideal_customers');

    const deferred = await postInterviewMessage(
      started.interviewId,
      "Let's leave it open.",
      opts
    );
    assert.equal(deferred.acceptedUnknown, true);
    assert.ok(
      deferred.answerDisposition === ANSWER_DISPOSITIONS.DEFERRED ||
        deferred.messageType === 'skip' ||
        deferred.acceptedUnknown === true
    );
    assert.notEqual(deferred.question.id, 'ideal_customers');

    const session = await store.getSession(started.interviewId);
    assert.deepEqual(session.interview_state.normalizedFacts.ideal_customers || [], []);
    assert.equal(
      /\bi don'?t know\b|still not sure|leave it open/i.test(
        String(session.interview_state.sectionState.idealCustomers.summary || '')
      ),
      false
    );
    assert.equal(session.interview_state.stepIndex > 2, true);
  });
});

describe('SPEC-100 TEST E — Executive Brief unknowns', () => {
  it('describes unresolved ideal customer without printing uncertainty phrases as facts', () => {
    let facts = emptyNormalizedFacts();
    facts = ingestAnswerIntoNormalizedFacts(facts, 'identity', IDENTITY);
    facts = ingestAnswerIntoNormalizedFacts(facts, 'services', SERVICES);
    facts = ingestAnswerIntoNormalizedFacts(facts, 'idealCustomers', "I don't know yet");
    facts = ingestAnswerIntoNormalizedFacts(
      facts,
      'avoidCustomers',
      "No restaurants unless it's only front of house cleaning"
    );
    facts = ingestAnswerIntoNormalizedFacts(facts, 'targetMarkets', 'Greater Toronto Area');

    const sections = sectionsFromNormalizedFacts(facts);
    const brief = buildExecutiveSummary(sections, { normalizedFacts: facts });
    const byId = Object.fromEntries(brief.sections.map((s) => [s.id, s]));

    assert.equal(/\bi don'?t know\b/i.test(byId.whoYouServe.body), false);
    assert.equal(/ideal customers (?:are|include)\s+i don'?t know/i.test(byId.whoYouServe.body), false);
    assert.match(
      byId.whoYouServe.body,
      /has not chosen|open decision|not yet|unresolved|prefer/i
    );

    const market = byId.assessment.ratings.find((r) => r.label === 'Market Focus');
    assert.ok(market);
    assert.equal(/named ideal customer/i.test(market.explanation), false);
    assert.match(market.explanation, /unresolved|not yet|remain/i);
    assert.ok(market.stars <= 2);
  });
});

describe('SPEC-100 TEST F — service list extraction', () => {
  it('preserves the full supported service set', () => {
    const services = extractServiceList(SERVICES);
    assert.ok(services.length >= 5);
    for (const re of [
      /residential standard/i,
      /recurring standard/i,
      /move-?in\/move-?out/i,
      /deep cleans?/i,
      /office cleaning/i,
    ]) {
      assert.ok(services.some((s) => re.test(s)), `missing ${re}`);
    }

    let facts = emptyNormalizedFacts();
    facts = ingestAnswerIntoNormalizedFacts(facts, 'services', SERVICES);
    const summary = sectionsFromNormalizedFacts(facts).services.summary;
    assert.match(summary, /residential standard/i);
    assert.match(summary, /office cleaning/i);
    assert.equal(/^Today the business delivers deep cleans\./i.test(summary), false);
  });
});

describe('SPEC-100 TEST G — refresh recovers probe state', () => {
  it('keeps the unresolved question and probe after recovery', async () => {
    const { opts, store } = withStore();
    const started = await advanceToIdealCustomer(opts);
    const probed = await postInterviewMessage(started.interviewId, "I don't know yet.", opts);
    assert.equal(probed.nextAction, 'PROBE');

    const recovered = await startClientInterview({ clientId: 11100 }, opts);
    assert.equal(recovered.resumedExisting, true);
    assert.equal(recovered.interviewId, started.interviewId);
    assert.equal(recovered.question.id, 'ideal_customers');
    assert.equal(recovered.nextAction, 'PROBE');
    assert.match(recovered.message, /commercial|prioritize|property managers|offices/i);

    const detail = await getInterview(started.interviewId, opts);
    assert.equal(detail.question.id, 'ideal_customers');
    assert.ok(detail.probe || detail.nextAction === 'PROBE');

    const session = await store.getSession(started.interviewId);
    assert.equal(session.interview_state.stepIndex, 2);
    assert.match(String(session.interview_state.normalizedFacts.growth_focus || ''), /commercial/i);
    assert.ok((session.interview_state.normalizedFacts.services || []).length >= 5);
  });
});

describe('SPEC-100 TEST H — explicit deferral', () => {
  it('preserves unknown and advances intentionally', async () => {
    const { opts, store } = withStore();
    const started = await advanceToIdealCustomer(opts);

    const res = await postInterviewMessage(started.interviewId, 'Skip this for now.', opts);
    assert.equal(res.acceptedUnknown, true);
    assert.notEqual(res.question && res.question.id, 'ideal_customers');
    assert.match(res.message, /leave|unresolved|open|come back/i);

    const session = await store.getSession(started.interviewId);
    assert.deepEqual(session.interview_state.normalizedFacts.ideal_customers || [], []);
    assert.equal(session.interview_state.stepIndex > 2, true);
    assert.equal(
      /\bskip this for now\b/i.test(
        String(session.interview_state.sectionState.idealCustomers.summary || '')
      ),
      false
    );
  });
});

describe('SPEC-100 TEST I — tenant isolation', () => {
  it('does not leak Anchor/client_id=10 into AS Cleaning uncertainty reasoning', async () => {
    const { opts, store } = withStore();

    const anchor = await startClientInterview({ clientId: 10 }, opts);
    await postInterviewMessage(
      anchor.interviewId,
      'Anchor Cleaning — commercial cleaning for Manchester NH law firms.',
      opts
    );
    await postInterviewMessage(
      anchor.interviewId,
      'Office cleaning for professional services firms',
      opts
    );

    const as = await startClientInterview({ clientId: 11 }, opts);
    await postInterviewMessage(as.interviewId, IDENTITY, opts);
    await postInterviewMessage(as.interviewId, SERVICES, opts);
    const probe = await postInterviewMessage(as.interviewId, "I don't know yet.", opts);

    assert.match(probe.message, /commercial|AS|prefer|prioritize/i);
    assert.equal(/\banchor\b/i.test(probe.message), false);
    assert.equal(/\bmanchester\b/i.test(probe.message), false);
    assert.equal(/\blaw firms?\b/i.test(probe.message), false);

    const asSession = await store.getSession(as.interviewId);
    const anchorSession = await store.getSession(anchor.interviewId);
    assert.equal(asSession.client_id, 11);
    assert.equal(anchorSession.client_id, 10);
    assert.deepEqual(asSession.interview_state.normalizedFacts.ideal_customers || [], []);
    assert.equal(
      /\banchor\b/i.test(JSON.stringify(asSession.interview_state.normalizedFacts || {})),
      false
    );
  });
});

describe('SPEC-100 TEST J — restart clears probe state', () => {
  it('keeps old probe with superseded Interview A and starts Interview B cleanly', async () => {
    const { opts, store } = withStore();
    const started = await startClientInterview({ clientId: 11120 }, opts);
    await postInterviewMessage(started.interviewId, IDENTITY, opts);
    await postInterviewMessage(started.interviewId, SERVICES, opts);
    await postInterviewMessage(started.interviewId, "I don't know yet.", opts);

    const interviewA = started.interviewId;
    let sessionA = await store.getSession(interviewA);
    assert.ok(sessionA.interview_state.reasoningMemory.activeProbe);

    const restarted = await startClientInterview(
      { clientId: 11120, restart: true },
      opts
    );
    assert.notEqual(restarted.interviewId, interviewA);
    assert.equal(restarted.resumedExisting, false);
    assert.equal(restarted.restarted, true);
    assert.ok(restarted.question && restarted.question.id);
    assert.equal(restarted.probe || null, null);
    assert.notEqual(restarted.question.id, 'ideal_customers');

    sessionA = await store.getSession(interviewA);
    assert.ok(sessionA.interview_state.supersededAt || sessionA.interview_state.lifecycle === 'superseded');
    assert.ok(sessionA.interview_state.reasoningMemory.activeProbe);

    const recovered = await startClientInterview({ clientId: 11120 }, opts);
    assert.equal(recovered.interviewId, restarted.interviewId);
    assert.equal(recovered.resumedExisting, true);
  });
});

describe('SPEC-100 collaborative reasoning uses known context without fabricating evidence', () => {
  it('frames hypotheses without writing them into ideal_customers', () => {
    let facts = emptyNormalizedFacts();
    facts = ingestAnswerIntoNormalizedFacts(facts, 'identity', IDENTITY);
    facts = ingestAnswerIntoNormalizedFacts(facts, 'services', SERVICES);
    facts = ingestAnswerIntoNormalizedFacts(facts, 'targetMarkets', 'Greater Toronto Area');
    const state = {
      normalizedFacts: facts,
      sectionState: sectionsFromNormalizedFacts(facts),
    };
    const probe = buildUncertaintyReasoningProbe(state, 'idealCustomers', 'AS Cleaning', {
      attemptCount: 1,
    });
    assert.match(probe, /prefer commercial|commercial work/i);
    assert.match(probe, /property managers|smaller offices|recurring facilities/i);
    assert.match(probe, /office cleaning|residential standard|deep cleans/i);
    // Probe text is Max reasoning — ingesting it must not invent operator evidence.
    const after = ingestAnswerIntoNormalizedFacts(
      facts,
      'idealCustomers',
      "I don't know yet"
    );
    assert.deepEqual(after.ideal_customers, []);
  });
});
