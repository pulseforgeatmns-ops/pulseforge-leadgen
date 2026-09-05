'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  normalizeRecoveredInterviewState,
  sectionsFromNormalizedFacts,
  buildExecutiveSummary,
  EPISTEMIC_STATES,
} = require('../services/clientIntelligenceInterview');

const CONTAMINATED_DIFFERENTIATION = 'as a rather than an established buying reason, do not invent a brand voice if it is currently unknown, preserve the operator-defined success metrics separately from Max\'s recommended scorecard metrics, and present customer exclusions naturally without duplicating or reframing their meaning.';

function contaminatedRecoveredState(overrides = {}) {
  return {
    normalizedFacts: {
      business_name: 'Babrun',
      business_description: null,
      differentiation: CONTAMINATED_DIFFERENTIATION,
      brand_voice: null,
      disqualified_customers: ['customers'],
      ideal_customers: ['ideally a small team'],
      ideal_customer_traits: ['is willing to change how they manage', 'delegate', 'operate'],
      epistemic_states: {
        differentiation: EPISTEMIC_STATES.HYPOTHESIS,
        brand_voice: EPISTEMIC_STATES.UNKNOWN,
        disqualified_customers: EPISTEMIC_STATES.KNOWN,
        ideal_customers: EPISTEMIC_STATES.KNOWN,
        ideal_customer_traits: EPISTEMIC_STATES.KNOWN,
      },
      hypotheses: { differentiation: CONTAMINATED_DIFFERENTIATION },
      evidence_statements: { differentiation: CONTAMINATED_DIFFERENTIATION },
      superseded_slots: [],
      ...overrides.normalizedFacts,
    },
    sectionState: {
      competitiveAdvantages: { summary: `The current differentiation hypothesis is that ${CONTAMINATED_DIFFERENTIATION}.` },
      avoidCustomers: { summary: 'Babrun prefers to avoid customers' },
      ...overrides.sectionState,
    },
    ...overrides,
  };
}

describe('SPEC-239: Historical Semantic Corruption Recovery Integrity', () => {
  it('invalidates contaminated differentiation and removes stale hypothesis/evidence', () => {
    const state = contaminatedRecoveredState();
    const normalized = normalizeRecoveredInterviewState(state);
    const facts = normalized.normalizedFacts;

    assert.equal(facts.differentiation, null);
    assert.equal(facts.epistemic_states.differentiation, EPISTEMIC_STATES.UNKNOWN);
    assert.equal(facts.hypotheses.differentiation, undefined);
    assert.equal(facts.evidence_statements.differentiation, undefined);
    assert.ok(facts.superseded_slots.includes('differentiation'));
  });

  it('suppresses stale sectionState differentiation authority', () => {
    const state = contaminatedRecoveredState();
    const normalized = normalizeRecoveredInterviewState(state);
    const sections = sectionsFromNormalizedFacts(normalized.normalizedFacts, normalized.sectionState);

    assert.equal(sections.competitiveAdvantages.summary, 'Differentiation: Not yet defined.');
    assert.equal(normalized.sectionState.competitiveAdvantages.summary, '');
  });

  it('preserves valid differentiation hypothesis and known values', () => {
    const validHypothesis = contaminatedRecoveredState({
      normalizedFacts: {
        differentiation: 'structured 12-week transformation support',
        epistemic_states: { differentiation: EPISTEMIC_STATES.HYPOTHESIS },
        hypotheses: { differentiation: 'structured 12-week transformation support' },
      },
    });
    const normalized = normalizeRecoveredInterviewState(validHypothesis);
    assert.equal(normalized.normalizedFacts.differentiation, 'structured 12-week transformation support');
    assert.equal(normalized.normalizedFacts.epistemic_states.differentiation, EPISTEMIC_STATES.HYPOTHESIS);

    const known = contaminatedRecoveredState({
      normalizedFacts: {
        differentiation: 'direct, practical transformation support',
        epistemic_states: { differentiation: EPISTEMIC_STATES.KNOWN },
        hypotheses: { differentiation: 'structured 12-week transformation support' },
      },
    });
    const normalizedKnown = normalizeRecoveredInterviewState(known);
    assert.equal(normalizedKnown.normalizedFacts.differentiation, 'direct, practical transformation support');
    assert.equal(normalizedKnown.normalizedFacts.epistemic_states.differentiation, EPISTEMIC_STATES.KNOWN);
  });

  it('invalidates malformed customer fragments without inventing replacements', () => {
    const state = contaminatedRecoveredState({
      normalizedFacts: {
        disqualified_customers: ['customers', 'idea-stage businesses'],
        ideal_customers: ['ideally a small team', 'existing operating small businesses'],
        ideal_customer_traits: ['is willing to change how they manage', 'delegate', 'operate', 'has a small team'],
      },
    });
    const normalized = normalizeRecoveredInterviewState(state);
    assert.deepEqual(normalized.normalizedFacts.disqualified_customers, ['idea-stage businesses']);
    assert.deepEqual(normalized.normalizedFacts.ideal_customers, ['existing operating small businesses']);
    assert.deepEqual(normalized.normalizedFacts.ideal_customer_traits, ['has a small team']);
  });

  it('does not synthesize replacement customer facts', () => {
    const state = contaminatedRecoveredState({
      normalizedFacts: {
        disqualified_customers: ['customers'],
        ideal_customers: ['ideally a small team'],
        ideal_customer_traits: ['delegate'],
      },
    });
    const normalized = normalizeRecoveredInterviewState(state);
    assert.deepEqual(normalized.normalizedFacts.disqualified_customers, []);
    assert.deepEqual(normalized.normalizedFacts.ideal_customers, []);
    assert.deepEqual(normalized.normalizedFacts.ideal_customer_traits, []);
  });

  it('is idempotent across repeated recovery normalization', () => {
    const state = contaminatedRecoveredState();
    const once = normalizeRecoveredInterviewState(state);
    const twice = normalizeRecoveredInterviewState(once);
    assert.deepEqual(twice, once);
  });

  it('cleans the exact AUDIT-121 recovered fixture before sections and brief synthesis', () => {
    const state = contaminatedRecoveredState();
    const recovered = normalizeRecoveredInterviewState(state);
    const sections = sectionsFromNormalizedFacts(recovered.normalizedFacts, recovered.sectionState);
    const brief = buildExecutiveSummary(sections, { normalizedFacts: recovered.normalizedFacts });
    const rendered = JSON.stringify({ sections, brief });

    assert.doesNotMatch(rendered, /as a rather than an established buying reason|do not invent a brand voice|preserve the operator-defined success metrics separately|Babrun prefers to avoid customers|ideally a small team|is willing to change how they manage|delegate|operate/i);
  });
});
