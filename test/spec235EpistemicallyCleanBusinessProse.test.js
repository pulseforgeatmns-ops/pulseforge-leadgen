'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  buildExecutiveSummary,
  sectionsFromNormalizedFacts,
  EPISTEMIC_STATES,
} = require('../services/clientIntelligenceInterview');

const proposition =
  'the practical, transformation-focused 12-week approach may be more compelling than generic business education or open-ended advice because it changes how the owner actually operates';

function facts(overrides = {}) {
  return {
    business_name: 'Babrun',
    differentiation: proposition,
    brand_voice: null,
    hypotheses: {},
    evidence_statements: {},
    epistemic_states: {
      differentiation: EPISTEMIC_STATES.KNOWN,
      brand_voice: EPISTEMIC_STATES.UNKNOWN,
    },
    ...overrides,
  };
}

function why(factOverrides) {
  const normalizedFacts = facts(factOverrides);
  const brief = buildExecutiveSummary({}, { normalizedFacts });
  return brief.sections.find((section) => section.id === 'whyChooseYou').body;
}

describe('SPEC-235 epistemically clean Executive Brief prose', () => {
  it('renders known differentiation naturally', () => {
    const body = why();
    assert.match(body, /Customers choose Babrun because/i);
    assert.doesNotMatch(body, /current hypothesis: competitive advantage around/i);
  });

  it('qualifies hypothesis differentiation without changing the proposition', () => {
    const body = why({
      epistemic_states: {
        differentiation: EPISTEMIC_STATES.HYPOTHESIS,
        brand_voice: EPISTEMIC_STATES.UNKNOWN,
      },
      hypotheses: { differentiation: proposition },
    });
    assert.match(body, /current differentiation hypothesis/i);
    assert.match(body, /practical, transformation-focused 12-week approach/i);
    assert.doesNotMatch(body, /current hypothesis: competitive advantage around/i);
  });

  it('suppresses unknown and not-applicable differentiation', () => {
    for (const state of [EPISTEMIC_STATES.UNKNOWN, EPISTEMIC_STATES.NOT_APPLICABLE]) {
      const body = why({
        differentiation: null,
        epistemic_states: { differentiation: state, brand_voice: EPISTEMIC_STATES.UNKNOWN },
      });
      assert.doesNotMatch(body, /Customers choose Babrun because/i);
      assert.doesNotMatch(body, /differentiation hypothesis/i);
    }
  });

  it('preserves known and qualifies hypothesis brand voice', () => {
    assert.match(
      why({
        brand_voice: 'direct and practical',
        epistemic_states: { differentiation: EPISTEMIC_STATES.KNOWN, brand_voice: EPISTEMIC_STATES.KNOWN },
      }),
      /brand voice should feel direct and practical/i
    );
    assert.match(
      why({
        brand_voice: 'direct and practical',
        epistemic_states: { differentiation: EPISTEMIC_STATES.KNOWN, brand_voice: EPISTEMIC_STATES.HYPOTHESIS },
        hypotheses: { brand_voice: 'direct and practical' },
      }),
      /current brand voice hypothesis/i
    );
  });

  it('suppresses unknown or not-applicable brand voice from business prose', () => {
    for (const state of [EPISTEMIC_STATES.UNKNOWN, EPISTEMIC_STATES.NOT_APPLICABLE]) {
      const body = why({
        epistemic_states: { differentiation: EPISTEMIC_STATES.KNOWN, brand_voice: state },
      });
      assert.doesNotMatch(body, /brand voice should feel brand voice: not yet defined/i);
    }
  });

  it('keeps the Babrun-equivalent hypothesis and omits fake voice content', () => {
    const normalizedFacts = facts({
      epistemic_states: {
        differentiation: EPISTEMIC_STATES.HYPOTHESIS,
        brand_voice: EPISTEMIC_STATES.UNKNOWN,
      },
      hypotheses: { differentiation: proposition },
    });
    const sections = sectionsFromNormalizedFacts(normalizedFacts);
    assert.match(sections.competitiveAdvantages.summary, /current differentiation hypothesis/i);
    const body = why(normalizedFacts);
    assert.match(body, /current differentiation hypothesis/i);
    assert.doesNotMatch(body, /brand voice should feel/i);
  });
});
