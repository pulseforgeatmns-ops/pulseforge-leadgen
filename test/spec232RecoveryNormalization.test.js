'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  normalizeRecoveredInterviewState,
  projectWorkingSemanticOperations,
  reviewCorrectionOperations,
} = require('../services/clientIntelligenceInterview');

const STALE = 'I did not establish premium positioning for Babrun, so remove that assumption.';
const ACTIVE = 'practical, direct, founder-to-founder, outcome-focused';
const DIFFERENTIATION_HYPOTHESIS = 'the practical, transformation-focused 12-week approach may be more compelling than generic business education or open-ended advice because it changes how the owner actually operates';

function recoveredFacts(overrides = {}) {
  return {
    business_name: 'Babrun',
    business_description: 'Babrun is a coaching programs for founders',
    services: ['delegation', 'premium positioning', '12-week coaching'],
    growth_focus: null,
    ideal_customers: ['founders'],
    ideal_customer_traits: [],
    disqualified_customers: [],
    geography: ['United States'],
    vertical_focus: null,
    differentiation: 'premium positioning',
    brand_voice: null,
    ninety_day_outcomes: null,
    success_metrics: ['raw lead volume'],
    epistemic_states: { brand_voice: 'HYPOTHESIS', differentiation: 'KNOWN', geography: 'KNOWN' },
    hypotheses: {},
    evidence_statements: {},
    business_facts: {},
    transformation_areas: [],
    pains: [],
    learning_signals: [],
    excluded_metrics: [],
    superseded_slots: [],
    ...overrides,
  };
}

function recoveredState(facts) {
  return {
    normalizedFacts: facts,
    sectionState: {
      brandVoice: { summary: `Current hypothesis: brand voice tone may align with ${STALE}` },
    },
  };
}

function normalizeFacts(facts) {
  return normalizeRecoveredInterviewState(recoveredState(facts)).normalizedFacts;
}

describe('SPEC-232 orphaned HYPOTHESIS recovery normalization', () => {
  it('demotes null HYPOTHESIS with absent hypothesis to UNKNOWN', () => {
    const facts = normalizeFacts(recoveredFacts());

    assert.equal(facts.brand_voice, null);
    assert.equal(facts.epistemic_states.brand_voice, 'UNKNOWN');
    assert.equal(facts.hypotheses.brand_voice, undefined);
    assert.equal(facts.evidence_statements.brand_voice, undefined);
    assert.ok(facts.superseded_slots.includes('brand_voice'));
  });

  it('demotes null HYPOTHESIS with null hypothesis to UNKNOWN', () => {
    const facts = normalizeFacts(recoveredFacts({
      hypotheses: { brand_voice: null },
    }));

    assert.equal(facts.brand_voice, null);
    assert.equal(facts.epistemic_states.brand_voice, 'UNKNOWN');
    assert.equal(facts.hypotheses.brand_voice, undefined);
    assert.equal(facts.evidence_statements.brand_voice, undefined);
    assert.ok(facts.superseded_slots.includes('brand_voice'));
  });

  it('demotes null HYPOTHESIS with stale non-null hypothesis to UNKNOWN', () => {
    const facts = normalizeFacts(recoveredFacts({
      hypotheses: { brand_voice: STALE },
      evidence_statements: { brand_voice: STALE },
    }));

    assert.equal(facts.brand_voice, null);
    assert.equal(facts.epistemic_states.brand_voice, 'UNKNOWN');
    assert.equal(facts.hypotheses.brand_voice, undefined);
    assert.equal(facts.evidence_statements.brand_voice, undefined);
    assert.ok(facts.superseded_slots.includes('brand_voice'));
  });

  it('preserves active value with matching HYPOTHESIS metadata', () => {
    const facts = normalizeFacts(recoveredFacts({
      brand_voice: ACTIVE,
      hypotheses: { brand_voice: ACTIVE },
      evidence_statements: { brand_voice: ACTIVE },
    }));

    assert.equal(facts.brand_voice, ACTIVE);
    assert.equal(facts.epistemic_states.brand_voice, 'HYPOTHESIS');
    assert.equal(facts.hypotheses.brand_voice, ACTIVE);
    assert.equal(facts.evidence_statements.brand_voice, ACTIVE);
  });

  it('repairs active HYPOTHESIS with missing metadata to the same active value', () => {
    const facts = normalizeFacts(recoveredFacts({
      brand_voice: ACTIVE,
      hypotheses: {},
    }));

    assert.equal(facts.brand_voice, ACTIVE);
    assert.equal(facts.epistemic_states.brand_voice, 'HYPOTHESIS');
    assert.equal(facts.hypotheses.brand_voice, ACTIVE);
  });

  it('repairs active HYPOTHESIS with mismatched stale metadata to the active value', () => {
    const facts = normalizeFacts(recoveredFacts({
      brand_voice: ACTIVE,
      hypotheses: { brand_voice: STALE },
      evidence_statements: { brand_voice: STALE },
    }));

    assert.equal(facts.brand_voice, ACTIVE);
    assert.equal(facts.epistemic_states.brand_voice, 'HYPOTHESIS');
    assert.equal(facts.hypotheses.brand_voice, ACTIVE);
    assert.equal(facts.evidence_statements.brand_voice, ACTIVE);
  });

  it('leaves unrelated KNOWN, UNKNOWN, and NOT_APPLICABLE slots unchanged', () => {
    const facts = normalizeFacts(recoveredFacts({
      brand_voice: null,
      differentiation: 'operator-stated differentiation',
      growth_focus: null,
      vertical_focus: null,
      epistemic_states: {
        brand_voice: 'UNKNOWN',
        differentiation: 'KNOWN',
        growth_focus: 'UNKNOWN',
        vertical_focus: 'NOT_APPLICABLE',
      },
      hypotheses: {},
      evidence_statements: { differentiation: 'operator-stated evidence' },
    }));

    assert.equal(facts.differentiation, 'operator-stated differentiation');
    assert.equal(facts.epistemic_states.differentiation, 'KNOWN');
    assert.equal(facts.evidence_statements.differentiation, 'operator-stated evidence');
    assert.equal(facts.growth_focus, null);
    assert.equal(facts.epistemic_states.growth_focus, 'UNKNOWN');
    assert.equal(facts.vertical_focus, null);
    assert.equal(facts.epistemic_states.vertical_focus, 'NOT_APPLICABLE');
  });

  it('is idempotent after orphaned HYPOTHESIS recovery', () => {
    const first = normalizeRecoveredInterviewState(recoveredState(recoveredFacts({
      hypotheses: { brand_voice: STALE },
    })));
    const second = normalizeRecoveredInterviewState(first);

    assert.deepEqual(second, first);
  });

  it('lets exact recovered Babrun state pass SPEC-230 coherence during production refinement', () => {
    const normalized = normalizeRecoveredInterviewState(recoveredState(recoveredFacts()));
    const correction = 'Our differentiation is still a hypothesis: ' +
      DIFFERENTIATION_HYPOTHESIS +
      '. We have not validated that as a consistent buying reason yet.\n' +
      'We have not established premium positioning for Babrun.';
    const operations = reviewCorrectionOperations(correction, normalized, 'turn-spec-232');

    assert.doesNotThrow(() => {
      projectWorkingSemanticOperations(normalized.normalizedFacts, operations);
    });
  });
});