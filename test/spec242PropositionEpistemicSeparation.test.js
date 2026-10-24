'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  BLUEPRINT_SECTIONS,
  EPISTEMIC_STATES,
  QUESTION_BANK,
  createMemoryStore,
  ingestAnswerIntoNormalizedFacts,
  postInterviewMessage,
  projectWorkingSemanticOperations,
  reviewCorrectionOperations,
} = require('../services/clientIntelligenceInterview');

const AUDIT_126_TURN_1 =
  "Our current differentiation is still a hypothesis, not an established buying reason. We think Babrun's practical, transformation-focused 12-week approach may be more compelling than generic business education or open-ended consulting because the goal is to actually change how the owner operates the business.";

function baseFacts(overrides = {}) {
  return {
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
    ...overrides,
  };
}

function emptySections() {
  const sections = {};
  for (const key of BLUEPRINT_SECTIONS) sections[key] = { summary: '', confidence: 0, unknowns: [] };
  return sections;
}

function differentiationOp(message, facts = baseFacts()) {
  return reviewCorrectionOperations(message, { normalizedFacts: facts }, 'turn-spec-242')
    .find((op) => op.slot === 'differentiation' && op.operation === 'CORRECT');
}

function assertSubstantiveHypothesis(op) {
  assert.ok(op, 'expected differentiation correction');
  assert.equal(op.epistemic_state, EPISTEMIC_STATES.HYPOTHESIS);
  assert.match(op.value, /Babrun's practical, transformation-focused 12-week approach/i);
  assert.match(op.value, /more compelling than generic business education or open-ended consulting/i);
  assert.match(op.value, /change how the owner operates the business/i);
  assert.doesNotMatch(op.value, /^not an established buying reason\.?$/i);
}

describe('SPEC-242: proposition and epistemic qualifier separation', () => {
  it('preserves the exact AUDIT-126 Turn 1 substantive proposition as a hypothesis', () => {
    const op = differentiationOp(AUDIT_126_TURN_1);
    assertSubstantiveHypothesis(op);

    const active = projectWorkingSemanticOperations(baseFacts(), [op]);
    assert.equal(active.epistemic_states.differentiation, EPISTEMIC_STATES.HYPOTHESIS);
    assert.equal(active.differentiation, op.value);
    assert.equal(active.hypotheses.differentiation, op.value);
    assert.equal(active.evidence_statements.differentiation, op.value);
    assert.doesNotMatch(active.differentiation, /^not an established buying reason\.?$/i);
  });

  it('supports qualifier first and proposition second', () => {
    const op = differentiationOp(
      "Our differentiation is still only a hypothesis. We think Babrun's practical 12-week model may outperform open-ended consulting because owners need operating change."
    );
    assert.ok(op);
    assert.match(op.value, /practical 12-week model/i);
    assert.match(op.value, /outperform open-ended consulting/i);
    assert.equal(op.epistemic_state, EPISTEMIC_STATES.HYPOTHESIS);
  });

  it('supports proposition first and qualifier second', () => {
    const op = differentiationOp(
      "We think Babrun's practical 12-week model may be more compelling because it changes owner behavior. The differentiation is still a hypothesis, not yet proven."
    );
    assert.ok(op);
    assert.match(op.value, /practical 12-week model/i);
    assert.match(op.value, /changes owner behavior/i);
    assert.equal(op.epistemic_state, EPISTEMIC_STATES.HYPOTHESIS);
  });

  it('supports same-sentence proposition plus qualifier', () => {
    const op = differentiationOp(
      "The differentiation is a hypothesis: Babrun's practical 12-week model may be more compelling than open-ended consulting."
    );
    assert.ok(op);
    assert.match(op.value, /Babrun's practical 12-week model/i);
    assert.doesNotMatch(op.value, /differentiation is a hypothesis/i);
  });

  it('stores the body of "our differentiation hypothesis is X"', () => {
    const op = differentiationOp(
      "Our differentiation hypothesis is Babrun's operating transformation program may be more useful than generic business education."
    );
    assert.ok(op);
    assert.match(op.value, /operating transformation program/i);
    assert.doesNotMatch(op.value, /^our differentiation hypothesis/i);
  });

  it('stores the body of "we think X may" with HYPOTHESIS state', () => {
    const op = differentiationOp(
      "The differentiation remains a working theory. We think Babrun may win because the work is practical, direct, and operating-focused."
    );
    assert.ok(op);
    assert.match(op.value, /Babrun may win/i);
    assert.equal(op.epistemic_state, EPISTEMIC_STATES.HYPOTHESIS);
  });

  it('supports not-yet-proven colon framing', () => {
    const op = differentiationOp(
      "This differentiation is not yet proven: Babrun's transformation program may be more compelling than self-paced education."
    );
    assert.ok(op);
    assert.match(op.value, /transformation program/i);
    assert.doesNotMatch(op.value, /not yet proven/i);
  });

  it('preserves an existing proposition for a pure epistemic update', () => {
    const facts = baseFacts({
      differentiation: "Babrun's practical model changes how owners operate.",
      epistemic_states: {
        ...baseFacts().epistemic_states,
        differentiation: EPISTEMIC_STATES.KNOWN,
      },
    });
    const op = differentiationOp('The differentiation is still only a hypothesis.', facts);
    assert.ok(op);
    assert.equal(op.value, "Babrun's practical model changes how owners operate.");
    assert.equal(op.epistemic_state, EPISTEMIC_STATES.HYPOTHESIS);
  });

  it('does not fabricate a proposition for a pure epistemic update without one', () => {
    const ops = reviewCorrectionOperations(
      'The differentiation is still only a hypothesis.',
      { normalizedFacts: baseFacts() },
      'turn-spec-242-empty'
    );
    assert.equal(ops.some((op) => op.slot === 'differentiation' && op.value), false);
  });

  it('does not treat legitimate negative business propositions as epistemic qualifiers', () => {
    const services = ingestAnswerIntoNormalizedFacts(baseFacts(), 'services', 'The service is not self-paced.');
    const identity = ingestAnswerIntoNormalizedFacts(baseFacts(), 'identity', 'Babrun is not a lead-generation agency.');
    const avoid = ingestAnswerIntoNormalizedFacts(baseFacts(), 'avoidCustomers', 'Customers should not expect us to run the business for them.');

    assert.ok(services.services.some((item) => /not self-paced/i.test(item)));
    assert.match(identity.business_description, /not a lead-generation agency/i);
    assert.ok(avoid.disqualified_customers.some((item) => /not expect us to run the business/i.test(item)));
  });

  it('keeps explicit correction and retraction behavior working', () => {
    const correction = differentiationOp(
      "Change the differentiation hypothesis to: Babrun's structured operating model may beat generic consulting."
    );
    assert.ok(correction);
    assert.match(correction.value, /structured operating model/i);

    const retraction = reviewCorrectionOperations(
      'We did not establish premium positioning.',
      { normalizedFacts: baseFacts({ differentiation: 'premium positioning' }) },
      'turn-spec-242-retract'
    ).find((op) => op.operation === 'RETRACT' && op.slot === 'differentiation');
    assert.ok(retraction);
  });

  it('replays AUDIT-126 Turn 1 through the refinement branch', async () => {
    const store = createMemoryStore();
    await store.insertSession({
      id: 'spec-242-session',
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
        normalizedFacts: baseFacts(),
        refinementPass: true,
        done: false,
        revisionGuidance: [],
        workingSemanticCorrections: [],
      },
    });

    await postInterviewMessage('spec-242-session', AUDIT_126_TURN_1, { store });
    const session = await store.getSession('spec-242-session');
    const facts = session.interview_state.normalizedFacts;
    assert.match(facts.differentiation, /practical, transformation-focused 12-week approach/i);
    assert.equal(facts.epistemic_states.differentiation, EPISTEMIC_STATES.HYPOTHESIS);
    assert.equal(facts.hypotheses.differentiation, facts.differentiation);
    assert.equal(facts.evidence_statements.differentiation, facts.differentiation);
  });
});
