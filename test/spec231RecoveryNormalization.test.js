'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  buildExecutiveSummary,
  normalizeRecoveredInterviewState,
  sectionsFromNormalizedFacts,
} = require('../services/clientIntelligenceInterview');

const STALE = 'I did not establish premium positioning for Babrun, so remove that assumption.';

function recoveredState(overrides = {}) {
  return {
    normalizedFacts: {
      business_name: 'Babrun',
      brand_voice: null,
      differentiation: 'practical transformation-focused implementation',
      epistemic_states: { brand_voice: 'UNKNOWN', differentiation: 'KNOWN' },
      hypotheses: { brand_voice: STALE },
      evidence_statements: { brand_voice: STALE, differentiation: 'operator-stated evidence' },
      superseded_slots: [],
    },
    sectionState: {
      brandVoice: { summary: `Current hypothesis: brand voice tone may align with ${STALE}` },
    },
    ...overrides,
  };
}

describe('SPEC-231 recovered semantic-state normalization', () => {
  it('removes stale hypothesis metadata for null UNKNOWN and NOT_APPLICABLE slots', () => {
    for (const epistemicState of ['UNKNOWN', 'NOT_APPLICABLE']) {
      const state = recoveredState({
        normalizedFacts: {
          ...recoveredState().normalizedFacts,
          epistemic_states: { brand_voice: epistemicState, differentiation: 'KNOWN' },
        },
      });
      const normalized = normalizeRecoveredInterviewState(state);
      assert.equal(normalized.normalizedFacts.hypotheses.brand_voice, undefined);
      assert.equal(normalized.normalizedFacts.evidence_statements.brand_voice, undefined);
      assert.ok(normalized.normalizedFacts.superseded_slots.includes('brand_voice'));
    }
  });

  it('preserves a matching active hypothesis and repairs a mismatched one to the active proposition', () => {
    const matching = normalizeRecoveredInterviewState(recoveredState({
      normalizedFacts: {
        business_name: 'Example',
        brand_voice: 'direct and practical',
        epistemic_states: { brand_voice: 'HYPOTHESIS' },
        hypotheses: { brand_voice: 'direct and practical' },
        evidence_statements: {},
        superseded_slots: [],
      },
    }));
    assert.equal(matching.normalizedFacts.hypotheses.brand_voice, 'direct and practical');

    const repaired = normalizeRecoveredInterviewState(recoveredState({
      normalizedFacts: {
        ...matching.normalizedFacts,
        hypotheses: { brand_voice: STALE },
      },
    }));
    assert.equal(repaired.normalizedFacts.hypotheses.brand_voice, 'direct and practical');
  });

  it('preserves legitimate unrelated active semantic state and is idempotent', () => {
    const first = normalizeRecoveredInterviewState(recoveredState());
    const second = normalizeRecoveredInterviewState(first);
    assert.equal(first.normalizedFacts.differentiation, 'practical transformation-focused implementation');
    assert.equal(first.normalizedFacts.evidence_statements.differentiation, 'operator-stated evidence');
    assert.deepEqual(second, first);
  });

  it('cleans recovered Babrun correction prose and suppresses the historical section fallback downstream', () => {
    const state = recoveredState({
      normalizedFacts: {
        ...recoveredState().normalizedFacts,
        brand_voice: STALE,
        epistemic_states: { brand_voice: 'KNOWN', differentiation: 'KNOWN' },
      },
    });
    const normalized = normalizeRecoveredInterviewState(state);
    const facts = normalized.normalizedFacts;
    const sections = sectionsFromNormalizedFacts(facts, normalized.sectionState);
    const brief = buildExecutiveSummary(sections, { normalizedFacts: facts });
    const rendered = JSON.stringify({ sections, brief });

    assert.equal(facts.brand_voice, null);
    assert.equal(facts.epistemic_states.brand_voice, 'UNKNOWN');
    assert.equal(facts.hypotheses.brand_voice, undefined);
    assert.equal(facts.evidence_statements.brand_voice, undefined);
    assert.ok(facts.superseded_slots.includes('brand_voice'));
    assert.equal(sections.brandVoice.summary, 'Brand voice: Not yet defined.');
    assert.doesNotMatch(rendered, /premium positioning|did not establish|remove that assumption/i);
  });
});