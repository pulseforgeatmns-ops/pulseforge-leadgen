'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  BLUEPRINT_SECTIONS,
  EPISTEMIC_STATES,
  QUESTION_BANK,
  createMemoryStore,
  postInterviewMessage,
  reviewCorrectionOperations,
} = require('../services/clientIntelligenceInterview');

const TURN_1 =
  "Our current differentiation is still a hypothesis, not an established buying reason. We think Babrun's practical, transformation-focused 12-week approach may be more compelling than generic business education or open-ended consulting because the goal is to actually change how the owner operates the business.";

const TURN_2 =
  'Babrun would rather not work with people who are still at the idea stage, people looking only for a quick lead-generation fix, or owners expecting someone else to run the business for them. The right customer needs to be willing to change how they manage, delegate, and operate.';

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
    id: 'spec-243-session',
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
  const beforeEvidence = await store.listEvidence('spec-243-session');
  const result = await postInterviewMessage('spec-243-session', message, opts);
  const session = await store.getSession('spec-243-session');
  const evidence = await store.listEvidence('spec-243-session');
  return {
    result,
    session,
    evidence: evidence.slice(beforeEvidence.length),
  };
}

describe('SPEC-243: refinement fallback semantic slot fidelity', () => {
  it('routes exact AUDIT-126 Turn 2 exclusion and fit requirement to distinct slots', async () => {
    const { result, session, evidence } = await runRefinement(TURN_2);
    const facts = session.interview_state.normalizedFacts;

    assert.ok(facts.disqualified_customers.some((item) => /idea stage/i.test(item)));
    assert.ok(facts.disqualified_customers.some((item) => /quick lead-generation fix/i.test(item)));
    assert.ok(facts.ideal_customer_traits.some((item) => /willing to change how they manage, delegate, and operate/i.test(item)));
    assert.equal(facts.business_description, baseFacts().business_description);
    assert.deepEqual(facts.ideal_customers, baseFacts().ideal_customers);
    assert.equal(evidence.some((row) => row.category === 'identity'), false);
    assert.equal(evidence.some((row) => row.category === 'idealCustomers'), false);
    assert.ok(evidence.some((row) => row.category === 'avoidCustomers'));
    assert.ok(evidence.some((row) => row.category === 'idealCustomerTraits'));
    assert.match(result.message, /Customers to Avoid/);
    assert.match(result.message, /Ideal Customer Traits/);
    assert.doesNotMatch(result.message, /Identity/);
  });

  it('routes customer categories to idealCustomers', async () => {
    const { session } = await runRefinement('We work with property managers and facility managers.');
    assert.ok(session.interview_state.normalizedFacts.ideal_customers.some((item) => /property managers/i.test(item)));
  });

  it('routes positive fit conditions to idealCustomerTraits', async () => {
    const { session } = await runRefinement('The right customer is willing to delegate.');
    assert.ok(session.interview_state.normalizedFacts.ideal_customer_traits.some((item) => /willing to delegate/i.test(item)));
    assert.deepEqual(session.interview_state.normalizedFacts.ideal_customers, baseFacts().ideal_customers);
  });

  it('routes exclusions to avoidCustomers', async () => {
    const { session } = await runRefinement('We do not serve restaurants.');
    assert.ok(session.interview_state.normalizedFacts.disqualified_customers.some((item) => /restaurants/i.test(item)));
  });

  it('routes true business identity to identity', async () => {
    const { session } = await runRefinement('Babrun is a 12-week transformation program for small business owners.');
    assert.match(session.interview_state.normalizedFacts.business_description, /12-week transformation program/i);
  });

  it('does not use identity as a generic customer fallback', async () => {
    const { session, evidence } = await runRefinement('Customer readiness matters here.');
    assert.equal(session.interview_state.normalizedFacts.business_description, baseFacts().business_description);
    assert.equal(evidence.some((row) => row.category === 'identity'), false);
  });

  it('keeps category and trait statements separate in one turn', async () => {
    const { session } = await runRefinement(
      'We work with owner-led service businesses. The right customer must be willing to delegate.'
    );
    const facts = session.interview_state.normalizedFacts;
    assert.ok(facts.ideal_customers.some((item) => /owner-led service businesses/i.test(item)));
    assert.ok(facts.ideal_customer_traits.some((item) => /willing to delegate/i.test(item)));
  });

  it('keeps trait and exclusion statements separate in one turn', async () => {
    const { session } = await runRefinement(
      'The owner needs to be willing to change how they manage. We do not serve restaurants.'
    );
    const facts = session.interview_state.normalizedFacts;
    assert.ok(facts.ideal_customer_traits.some((item) => /willing to change/i.test(item)));
    assert.ok(facts.disqualified_customers.some((item) => /restaurants/i.test(item)));
  });

  it('keeps identity and exclusion statements separate in one turn', async () => {
    const { session } = await runRefinement(
      'Babrun is a 12-week transformation program for small business owners. We do not serve restaurants.'
    );
    const facts = session.interview_state.normalizedFacts;
    assert.match(facts.business_description, /12-week transformation program/i);
    assert.ok(facts.disqualified_customers.some((item) => /restaurants/i.test(item)));
  });

  it('primary semantic operations retain precedence and fallback does not duplicate them', async () => {
    const { session, evidence } = await runRefinement(TURN_1);
    const facts = session.interview_state.normalizedFacts;
    assert.match(facts.differentiation, /practical, transformation-focused 12-week approach/i);
    assert.equal(facts.epistemic_states.differentiation, EPISTEMIC_STATES.HYPOTHESIS);
    assert.deepEqual(evidence.map((row) => row.category), ['refinement']);
  });

  it('keeps services, geography, and brand voice fallback routing intact', async () => {
    const services = await runRefinement('Services include workshop facilitation and implementation coaching.');
    assert.ok(services.session.interview_state.normalizedFacts.services.some((item) => /workshop facilitation/i.test(item)));

    const geography = await runRefinement('Priority markets center on Manchester and Bedford.');
    assert.ok(geography.session.interview_state.normalizedFacts.geography.some((item) => /Manchester/i.test(item)));

    const brand = await runRefinement('Brand voice should be practical, direct, and grounded.');
    assert.match(brand.session.interview_state.normalizedFacts.brand_voice, /practical, direct, and grounded/i);
  });

  it('leaves SPEC-242 exact Turn 1 behavior intact', () => {
    const op = reviewCorrectionOperations(TURN_1, { normalizedFacts: baseFacts() }, 'turn-spec-243');
    assert.equal(op.length, 1);
    assert.equal(op[0].slot, 'differentiation');
    assert.equal(op[0].epistemic_state, EPISTEMIC_STATES.HYPOTHESIS);
    assert.match(op[0].value, /practical, transformation-focused 12-week approach/i);
    assert.doesNotMatch(op[0].value, /^not an established buying reason\.?$/i);
  });
});
