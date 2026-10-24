'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  BLUEPRINT_SECTIONS,
  EPISTEMIC_STATES,
  MESSAGE_TYPES,
  QUESTION_BANK,
  createMemoryStore,
  postInterviewMessage,
  resumeInterview,
  reviewCorrectionOperations,
  startClientInterview,
} = require('../services/clientIntelligenceInterview');

const TURN_1 =
  "Our current differentiation is still a hypothesis, not an established buying reason. We think Babrun's practical, transformation-focused 12-week approach may be more compelling than generic business education or open-ended consulting because the goal is to actually change how the owner operates the business.";

const TURN_2 =
  'Babrun would rather not work with people who are still at the idea stage, people looking only for a quick lead-generation fix, or owners expecting someone else to run the business for them. The right customer needs to be willing to change how they manage, delegate, and operate.';

const TURN_3 =
  'The brand voice should be practical, direct, experienced, and grounded—owner/operator to owner/operator. Encouraging without being fluffy, confident without pretending we have every answer, and willing to challenge the owner when necessary. Avoid jargon, hype, exaggerated promises, corporate polish, and anything that makes the transformation sound easy or passive.';

function emptySections() {
  const out = {};
  for (const key of BLUEPRINT_SECTIONS) out[key] = { summary: '', confidence: 0, unknowns: [], evidenceIds: [] };
  return out;
}

function baseFacts(overrides = {}) {
  const facts = {
    business_name: 'Babrun',
    business_description: 'coaching and transformation programs for owners of small, founder-led businesses',
    services: ['12-week transformation program'],
    growth_focus: null,
    ideal_customers: ['owners of small, founder-led businesses'],
    ideal_customer_traits: [],
    disqualified_customers: [],
    geography: [],
    vertical_focus: null,
    differentiation: null,
    brand_voice: null,
    ninety_day_outcomes: null,
    success_metrics: [],
    excluded_metrics: [],
    learning_signals: [],
    pains: [],
    transformation_areas: [],
    epistemic_states: {
      business_name: EPISTEMIC_STATES.KNOWN,
      business_description: EPISTEMIC_STATES.KNOWN,
      services: EPISTEMIC_STATES.KNOWN,
      growth_focus: EPISTEMIC_STATES.UNRESOLVED,
      ideal_customers: EPISTEMIC_STATES.KNOWN,
      ideal_customer_traits: EPISTEMIC_STATES.UNKNOWN,
      disqualified_customers: EPISTEMIC_STATES.UNKNOWN,
      geography: EPISTEMIC_STATES.NOT_APPLICABLE,
      differentiation: EPISTEMIC_STATES.UNKNOWN,
      brand_voice: EPISTEMIC_STATES.UNKNOWN,
      ninety_day_outcomes: EPISTEMIC_STATES.UNKNOWN,
      success_metrics: EPISTEMIC_STATES.UNKNOWN,
    },
    hypotheses: {},
    evidence_statements: {},
    superseded_slots: [],
    business_facts: {
      business_description: [],
      services: [],
      ideal_customers: [],
      ideal_customer_traits: [],
      disqualified_customers: [],
      differentiation: [],
      brand_voice: [],
    },
  };
  return {
    ...facts,
    ...overrides,
    epistemic_states: {
      ...facts.epistemic_states,
      ...(overrides.epistemic_states || {}),
    },
  };
}

async function refinementHarness(facts = baseFacts()) {
  const store = createMemoryStore();
  await store.insertSession({
    id: 'spec-244-session',
    client_id: 1,
    status: 'DISCOVERY',
    current_stage: 'Refinement',
    started_at: new Date(),
    completed_at: null,
    summary: null,
    confidence_score: null,
    interview_state: {
      mode: 'interactive',
      stepIndex: QUESTION_BANK.length,
      answers: {},
      sectionState: emptySections(),
      normalizedFacts: facts,
      refinementPass: true,
      done: false,
      revisionGuidance: [],
      workingSemanticCorrections: [],
    },
  });
  return { store, opts: { store } };
}

async function runRefinement(message, facts) {
  const { store, opts } = await refinementHarness(facts);
  const result = await postInterviewMessage('spec-244-session', message, opts);
  const session = await store.getSession('spec-244-session');
  const evidence = await store.listEvidence('spec-244-session');
  return { result, session, evidence };
}

function evidenceCategories(evidence) {
  return evidence.map((row) => row.category);
}

describe('SPEC-244: contextual avoidance-object attribution', () => {
  it('routes exact AUDIT-126 Turn 3 entirely to Brand Voice', async () => {
    const before = baseFacts({
      ideal_customer_traits: ['The right customer needs to be willing to change how they manage, delegate, and operate.'],
      disqualified_customers: ['people who are still at the idea stage'],
      epistemic_states: {
        ideal_customer_traits: EPISTEMIC_STATES.KNOWN,
        disqualified_customers: EPISTEMIC_STATES.KNOWN,
      },
    });
    const { result, session, evidence } = await runRefinement(TURN_3, before);
    const facts = session.interview_state.normalizedFacts;

    assert.deepEqual(reviewCorrectionOperations(TURN_3, { normalizedFacts: before }, 'turn3'), []);
    assert.deepEqual(evidenceCategories(evidence), ['brandVoice']);
    assert.match(evidence[0].statement, /practical, direct, experienced, and grounded/i);
    assert.match(evidence[0].statement, /Encouraging without being fluffy/i);
    assert.match(evidence[0].statement, /Avoid jargon, hype, exaggerated promises/i);
    assert.match(facts.brand_voice, /owner\/operator to owner\/operator/i);
    assert.deepEqual(facts.disqualified_customers, before.disqualified_customers);
    assert.equal(facts.business_description, before.business_description);
    assert.deepEqual(facts.ideal_customers, before.ideal_customers);
    assert.deepEqual(facts.ideal_customer_traits, before.ideal_customer_traits);
    assert.doesNotMatch(JSON.stringify(facts.business_facts.business_description || []), /owner\/operator/i);
    assert.match(result.message, /Brand Voice/);
    assert.doesNotMatch(result.message, /Customers to Avoid|Identity|Ideal Customers|Ideal Customer Traits/);
  });

  it('routes standalone communication avoidance to Brand Voice', async () => {
    for (const message of [
      'Avoid jargon.',
      'Avoid hype and exaggerated promises.',
      "Don't sound corporate.",
      'Never use fluffy language.',
    ]) {
      const { result, session, evidence } = await runRefinement(message);
      assert.deepEqual(evidenceCategories(evidence), ['brandVoice'], message);
      assert.match(session.interview_state.normalizedFacts.brand_voice || '', /avoid|hype|corporate|fluffy|jargon|promises/i);
      assert.deepEqual(session.interview_state.normalizedFacts.disqualified_customers, []);
      assert.match(result.message, /Brand Voice/);
      assert.doesNotMatch(result.message, /Customers to Avoid/);
    }
  });

  it('keeps customer avoidance routed to Customers to Avoid', async () => {
    for (const message of [
      'Avoid customers who only want the cheapest option.',
      'Avoid restaurants.',
      "We don't work with idea-stage founders.",
    ]) {
      const { session, evidence } = await runRefinement(message);
      assert.deepEqual(evidenceCategories(evidence), ['avoidCustomers'], message);
      assert.ok(session.interview_state.normalizedFacts.disqualified_customers.length, message);
      assert.equal(session.interview_state.normalizedFacts.brand_voice, null);
    }
  });

  it('keeps explicit mixed-domain avoidance in separate slots', async () => {
    const brandThenCustomer = await runRefinement(
      'The brand voice should be direct. Avoid restaurants as customers.'
    );
    assert.deepEqual(evidenceCategories(brandThenCustomer.evidence), ['brandVoice', 'avoidCustomers']);
    assert.match(brandThenCustomer.session.interview_state.normalizedFacts.brand_voice || '', /direct/i);
    assert.ok(
      brandThenCustomer.session.interview_state.normalizedFacts.disqualified_customers.some((item) =>
        /restaurants/i.test(item)
      )
    );

    const customerThenBrand = await runRefinement(
      "We don't work with idea-stage founders. Avoid jargon in our messaging."
    );
    assert.deepEqual(evidenceCategories(customerThenBrand.evidence), ['avoidCustomers', 'brandVoice']);
    assert.ok(
      customerThenBrand.session.interview_state.normalizedFacts.disqualified_customers.some((item) =>
        /idea-stage founders/i.test(item)
      )
    );
    assert.match(customerThenBrand.session.interview_state.normalizedFacts.brand_voice || '', /jargon/i);
  });

  it('fails closed for unclassifiable avoidance instead of defaulting to identity or customers to avoid', async () => {
    for (const message of ['Avoid ambiguity.', 'Avoid unnecessary complexity.']) {
      const { session, evidence, result } = await runRefinement(message);
      assert.deepEqual(evidence, [], message);
      assert.equal(session.interview_state.normalizedFacts.business_description, baseFacts().business_description);
      assert.deepEqual(session.interview_state.normalizedFacts.disqualified_customers, []);
      assert.doesNotMatch(result.message, /Identity|Customers to Avoid/);
    }
  });

  it('preserves SPEC-243 customer roles', async () => {
    const { session, evidence } = await runRefinement(TURN_2);
    const facts = session.interview_state.normalizedFacts;
    assert.ok(facts.disqualified_customers.some((item) => /idea stage/i.test(item)));
    assert.ok(facts.disqualified_customers.some((item) => /quick lead-generation fix/i.test(item)));
    assert.ok(facts.ideal_customer_traits.some((item) => /willing to change how they manage, delegate, and operate/i.test(item)));
    assert.equal(facts.business_description, baseFacts().business_description);
    assert.deepEqual(facts.ideal_customers, baseFacts().ideal_customers);
    assert.deepEqual(evidenceCategories(evidence), ['avoidCustomers', 'idealCustomerTraits']);
  });

  it('preserves positive fit traits, customer categories, identity, services, and geography fallback', async () => {
    const trait = await runRefinement('The right customer is willing to delegate.');
    assert.deepEqual(evidenceCategories(trait.evidence), ['idealCustomerTraits']);
    assert.ok(trait.session.interview_state.normalizedFacts.ideal_customer_traits.some((item) => /willing to delegate/i.test(item)));

    const category = await runRefinement('We work with property managers and facility managers.');
    assert.deepEqual(evidenceCategories(category.evidence), ['idealCustomers']);
    assert.ok(category.session.interview_state.normalizedFacts.ideal_customers.some((item) => /property managers/i.test(item)));

    const identity = await runRefinement('Babrun is a 12-week transformation program for small business owners.');
    assert.deepEqual(evidenceCategories(identity.evidence), ['identity']);
    assert.match(identity.session.interview_state.normalizedFacts.business_description, /12-week transformation program/i);

    const services = await runRefinement('Services include workshop facilitation and implementation coaching.');
    assert.deepEqual(evidenceCategories(services.evidence), ['services']);
    assert.ok(services.session.interview_state.normalizedFacts.services.some((item) => /workshop facilitation/i.test(item)));

    const geography = await runRefinement('Priority markets center on Manchester and Bedford.');
    assert.deepEqual(evidenceCategories(geography.evidence), ['targetMarkets']);
    assert.ok(geography.session.interview_state.normalizedFacts.geography.some((item) => /Manchester/i.test(item)));
  });

  it('preserves primary semantic operation precedence and SPEC-242 Turn 1 behavior', async () => {
    const { session, evidence } = await runRefinement(TURN_1);
    const facts = session.interview_state.normalizedFacts;
    assert.match(facts.differentiation, /practical, transformation-focused 12-week approach/i);
    assert.equal(facts.epistemic_states.differentiation, EPISTEMIC_STATES.HYPOTHESIS);
    assert.deepEqual(evidenceCategories(evidence), ['refinement']);

    const op = reviewCorrectionOperations(TURN_1, { normalizedFacts: baseFacts() }, 'turn1');
    assert.equal(op.length, 1);
    assert.equal(op[0].slot, 'differentiation');
    assert.equal(op[0].epistemic_state, EPISTEMIC_STATES.HYPOTHESIS);
    assert.match(op[0].value, /practical, transformation-focused 12-week approach/i);
    assert.doesNotMatch(op[0].value, /^not an established buying reason\.?$/i);
  });

  it('preserves SPEC-241 conversational ownership on CLIENT_REVIEW refinement', async () => {
    const store = createMemoryStore();
    const opts = { store };
    const started = await startClientInterview({ clientId: 1 }, opts);
    let turn = started;
    for (const q of QUESTION_BANK) {
      turn = await postInterviewMessage(started.interviewId, `${q.prompt} Answer for ${q.id}.`, opts);
    }
    assert.equal(turn.status, 'CLIENT_REVIEW');

    await assert.rejects(
      () => postInterviewMessage(started.interviewId, 'More detail', opts),
      (err) => err.code === 'awaiting_review'
    );
    const reopened = await resumeInterview(started.interviewId, opts);
    assert.equal(reopened.status, 'DISCOVERY');
    assert.equal(reopened.resumed, true);

    const resumed = await postInterviewMessage(
      started.interviewId,
      'Please make the brief more conversational without changing the business facts.',
      opts
    );
    assert.equal(resumed.messageType, MESSAGE_TYPES.REFINEMENT_FEEDBACK);
    assert.equal(resumed.status, 'DISCOVERY');
    assert.equal(resumed.question, null);
  });
});
