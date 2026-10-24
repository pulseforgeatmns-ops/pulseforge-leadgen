'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const {
  generateCanonicalHypotheses,
  HYPOTHESIS_KIND,
  INVESTIGATION_STATUS,
  businessHypothesesForPlanner,
} = require('../packages/scout/hypothesis/CanonicalHypothesisEngine');
const { resolveMarketHypothesisBySegmentKey } = require('../packages/scout/hypothesis/MarketHypothesisRegistry');
const { generateHypotheses } = require('../packages/scout/investigation/HypothesisGeneration');
const {
  deriveQuestionsFromHypotheses,
  deriveQuestionsForHypothesis,
  INVESTIGATIVE_QUESTIONS,
} = require('../packages/scout/coverage/EvidenceRequirements');

describe('CanonicalHypothesisEngine (SPEC-179 / ADR-094)', () => {
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
    assert.ok(result.searchStrategies.length > 0);

    const kinds = new Set(result.hypotheses.map((hyp) => hyp.kind));
    assert.ok(kinds.has(HYPOTHESIS_KIND.BUSINESS));
    assert.ok(kinds.has(HYPOTHESIS_KIND.TERMINOLOGY));
    assert.ok(kinds.has(HYPOTHESIS_KIND.SEARCH_STRATEGY));
  });

  it('resolves search_strategy by segmentKey (short_term_rental → str_manager)', () => {
    const hypothesis = resolveMarketHypothesisBySegmentKey('short_term_rental');
    assert.ok(hypothesis);
    assert.equal(hypothesis.id, 'str_manager');

    const { searchStrategies } = generateCanonicalHypotheses(marketDefinition);
    assert.ok(searchStrategies.some((hyp) => hyp.id === 'ss-str_manager'));
    assert.ok(searchStrategies[0].searchTerms.length > 0);
  });

  it('returns immutable hypothesis objects with full reasoning fields', () => {
    const { business } = generateCanonicalHypotheses(marketDefinition);
    const hyp = business[0];

    assert.equal(Object.isFrozen(hyp), true);
    assert.equal(Object.isFrozen(hyp.requiredEvidence), true);
    assert.equal(Object.isFrozen(hyp.generatedQuestions), true);
    assert.ok('rationale' in hyp);
    assert.ok('confidence' in hyp);
    assert.ok('uncertainty' in hyp);
    assert.ok('gap' in hyp);
    assert.ok('supportingEvidence' in hyp);
    assert.ok('contradictoryEvidence' in hyp);
    assert.equal(hyp.investigationStatus, INVESTIGATION_STATUS.PENDING);
    assert.throws(() => {
      hyp.text = 'mutated';
    });
  });

  it('propagates gap from segment templates through canonical engine', () => {
    const { business } = generateCanonicalHypotheses(marketDefinition);
    const cleaningHyp = business.find((hyp) => hyp.gap === 'cleaning_responsibility');

    assert.ok(cleaningHyp, 'STR segment should include cleaning_responsibility hypothesis');
    assert.ok(cleaningHyp.generatedQuestions.length > 0);
    assert.ok(
      cleaningHyp.generatedQuestions.some(
        (q) => q.question === INVESTIGATIVE_QUESTIONS.OUTSOURCES_CLEANING
      )
    );
  });

  it('AUDIT-058: gap-specific hypotheses do not fall back to generic questions', () => {
    const hypotheses = generateHypotheses(marketDefinition);
    assert.ok(hypotheses.every((hyp) => hyp.gap));

    const cleaningHyp = hypotheses.find((hyp) => hyp.gap === 'cleaning_responsibility');
    const questions = deriveQuestionsForHypothesis(cleaningHyp, marketDefinition);

    assert.equal(questions.length, 1);
    assert.equal(questions[0].question, INVESTIGATIVE_QUESTIONS.OUTSOURCES_CLEANING);
    assert.equal(questions[0].text, 'Do they outsource cleaning?');
    assert.ok(
      !questions.some((q) => q.question === INVESTIGATIVE_QUESTIONS.BUSINESS_FIT),
      'cleaning_responsibility should not produce generic business_fit question'
    );
  });

  it('every business hypothesis with a gap produces a unique investigation', () => {
    const { business } = generateCanonicalHypotheses(marketDefinition);
    const gapHyps = business.filter((hyp) => hyp.gap);

    const questionSets = gapHyps.map((hyp) =>
      hyp.generatedQuestions.map((q) => q.question).sort().join('|')
    );
    const uniqueSets = new Set(questionSets);

    assert.equal(uniqueSets.size, gapHyps.length, 'each gap hypothesis should have distinct questions');
  });

  it('generic fallback applies only when hypothesis has no gap', () => {
    const noGapHyp = { id: 'hyp-no-gap', text: 'Unknown hypothesis' };
    const questions = deriveQuestionsForHypothesis(noGapHyp, marketDefinition);

    assert.equal(questions.length, 3);
    assert.ok(questions.some((q) => q.question === INVESTIGATIVE_QUESTIONS.BUSINESS_EXISTS));
    assert.ok(questions.some((q) => q.question === INVESTIGATIVE_QUESTIONS.BUSINESS_FIT));
    assert.ok(questions.some((q) => q.question === INVESTIGATIVE_QUESTIONS.BUYING_DECISIONS));
  });

  it('projects business hypotheses for legacy planner consumption', () => {
    const { business } = generateCanonicalHypotheses(marketDefinition);
    const plannerHyps = businessHypothesesForPlanner(business);

    assert.ok(plannerHyps.length > 0);
    assert.ok(plannerHyps[0].requiredEvidence);
    assert.ok(plannerHyps[0].text);
    assert.ok(plannerHyps[0].gap);
    assert.ok(Array.isArray(plannerHyps[0].generatedQuestions));
  });

  it('planner path derives gap-specific questions end-to-end', () => {
    const pmMarket = {
      segmentKey: 'property_management',
      segments: ['property_management'],
      geography: { label: 'Manchester NH' },
    };
    const plannerHyps = businessHypothesesForPlanner(
      generateCanonicalHypotheses(pmMarket).business
    );
    const questions = deriveQuestionsFromHypotheses(plannerHyps, pmMarket);

    assert.ok(
      questions.some((q) => q.question === INVESTIGATIVE_QUESTIONS.OUTSOURCES_CLEANING),
      'property_management cleaning hypothesis should drive outsourcing question'
    );
    assert.ok(
      questions.some((q) => q.question === INVESTIGATIVE_QUESTIONS.MANAGES_STRS),
      'property_management portfolio hypothesis should drive STR question'
    );
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
