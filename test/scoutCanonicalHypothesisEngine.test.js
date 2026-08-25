'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  generateCanonicalHypotheses,
  HYPOTHESIS_KIND,
  businessHypothesesForPlanner,
} = require('../packages/scout/hypothesis/CanonicalHypothesisEngine');

describe('CanonicalHypothesisEngine (SPEC-179)', () => {
  const marketDefinition = {
    market: 'Short-term rental operators',
    segmentKey: 'short_term_rental',
    segments: ['short_term_rental'],
    geography: { label: 'Manchester NH', state: 'NH' },
    terminology: ['Vacation Rental', 'Property Manager'],
    adjacentMarkets: ['Corporate Housing'],
  };

  it('produces business, terminology, and search_strategy hypothesis kinds', () => {
    const result = generateCanonicalHypotheses(marketDefinition);

    assert.ok(result.hypotheses.length > 0);
    assert.ok(result.business.length > 0);
    assert.ok(result.terminology.length > 0);

    const kinds = new Set(result.hypotheses.map((hyp) => hyp.kind));
    assert.ok(kinds.has(HYPOTHESIS_KIND.BUSINESS));
    assert.ok(kinds.has(HYPOTHESIS_KIND.TERMINOLOGY));
  });

  it('projects business hypotheses for legacy planner consumption', () => {
    const { business } = generateCanonicalHypotheses(marketDefinition);
    const plannerHyps = businessHypothesesForPlanner(business);

    assert.ok(plannerHyps.length > 0);
    assert.ok(plannerHyps[0].requiredEvidence);
    assert.ok(plannerHyps[0].text);
  });

  it('respects include flags to filter hypothesis kinds', () => {
    const terminologyOnly = generateCanonicalHypotheses(marketDefinition, {}, {
      includeBusiness: false,
      includeSearchStrategies: false,
    });

    assert.equal(terminologyOnly.business.length, 0);
    assert.ok(terminologyOnly.terminology.length > 0);
    assert.ok(
      terminologyOnly.hypotheses.every((hyp) => hyp.kind === HYPOTHESIS_KIND.TERMINOLOGY)
    );
  });
});
