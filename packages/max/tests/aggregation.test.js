'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { ScoreAggregator } = require('../aggregation/ScoreAggregator');
const {
  DEFAULT_STRATEGY_WEIGHTS,
  STRATEGY_IDS,
  PRIORITIES,
} = require('../reasoning/ReasoningTypes');
const {
  RecommendationBuilder,
  classifyPriority,
} = require('../recommendations/RecommendationBuilder');

function result(strategy, scoreDelta, confidence, extras = {}) {
  return {
    strategy,
    scoreDelta,
    confidence,
    supportingEvidence: extras.supportingEvidence || [],
    contradictingEvidence: extras.contradictingEvidence || [],
    claims: extras.claims || [],
    summary: extras.summary || `${strategy}:ok`,
  };
}

describe('Aggregation tests', () => {
  it('weights sum to 1', () => {
    const sum = Object.values(DEFAULT_STRATEGY_WEIGHTS).reduce((a, b) => a + b, 0);
    assert.ok(Math.abs(sum - 1) < 1e-9);
  });

  it('normalizes weighted score to 0–100', () => {
    const aggregator = new ScoreAggregator();
    const aggregated = aggregator.aggregate([
      result(STRATEGY_IDS.OPPORTUNITY, 100, 50),
      result(STRATEGY_IDS.RELATIONSHIP, 100, 50),
      result(STRATEGY_IDS.ENGAGEMENT, 100, 50),
      result(STRATEGY_IDS.DECISION_MAKER, 100, 50),
      result(STRATEGY_IDS.TECHNOLOGY, 100, 50),
      result(STRATEGY_IDS.OVERFLOW, 100, 50),
      result(STRATEGY_IDS.RISK, 100, 50),
    ]);
    assert.equal(aggregated.score, 100);
    assert.equal(aggregated.confidence, 50);
  });

  it('maps all-zero deltas to midpoint 50', () => {
    const aggregator = new ScoreAggregator();
    const aggregated = aggregator.aggregate(
      Object.values(STRATEGY_IDS).map((id) => result(id, 0, 40))
    );
    assert.equal(aggregated.score, 50);
  });

  it('keeps confidence independent of score', () => {
    const aggregator = new ScoreAggregator();
    // High opportunity score, weak confidence
    const highScoreWeakConf = aggregator.aggregate([
      result(STRATEGY_IDS.OPPORTUNITY, 100, 20),
      result(STRATEGY_IDS.RELATIONSHIP, 100, 20),
      result(STRATEGY_IDS.ENGAGEMENT, 100, 20),
      result(STRATEGY_IDS.DECISION_MAKER, 100, 20),
      result(STRATEGY_IDS.TECHNOLOGY, 100, 20),
      result(STRATEGY_IDS.OVERFLOW, 100, 20),
      result(STRATEGY_IDS.RISK, 100, 20),
    ]);
    assert.equal(highScoreWeakConf.score, 100);
    assert.equal(highScoreWeakConf.confidence, 20);

    // Low score, high confidence
    const lowScoreHighConf = aggregator.aggregate([
      result(STRATEGY_IDS.OPPORTUNITY, -100, 97),
      result(STRATEGY_IDS.RELATIONSHIP, -100, 97),
      result(STRATEGY_IDS.ENGAGEMENT, -100, 97),
      result(STRATEGY_IDS.DECISION_MAKER, -100, 97),
      result(STRATEGY_IDS.TECHNOLOGY, -100, 97),
      result(STRATEGY_IDS.OVERFLOW, -100, 97),
      result(STRATEGY_IDS.RISK, -100, 97),
    ]);
    assert.equal(lowScoreHighConf.score, 0);
    assert.equal(lowScoreHighConf.confidence, 97);
  });

  it('priority ordering follows score bands (not confidence)', () => {
    assert.equal(classifyPriority(96, 31), PRIORITIES.CRITICAL);
    assert.equal(classifyPriority(42, 97), PRIORITIES.MEDIUM);
    assert.equal(classifyPriority(20, 99), PRIORITIES.LOW);
  });

  it('rejects invalid weight sums', () => {
    assert.throws(
      () => new ScoreAggregator({ weights: { opportunity: 1, risk: 1 } }),
      /sum to 1/
    );
  });

  it('RecommendationBuilder does not invent prose fields', () => {
    const builder = new RecommendationBuilder();
    const context = {
      tenantId: '10',
      company: { id: 'c1', name: 'Acme' },
      people: [],
      metrics: {},
    };
    const strategyResults = [
      result(STRATEGY_IDS.OPPORTUNITY, 40, 60, {
        supportingEvidence: [
          { id: 'e1', kind: 'evidence', summary: 'Hiring signal', sourceId: null, sourceType: null, confidence: 0.8 },
        ],
        contradictingEvidence: [
          { id: 'e2', kind: 'evidence', summary: 'Declined', sourceId: null, sourceType: null, confidence: 0.7 },
        ],
        claims: ['claim1'],
      }),
      result(STRATEGY_IDS.RELATIONSHIP, 0, 40),
      result(STRATEGY_IDS.ENGAGEMENT, 0, 40),
      result(STRATEGY_IDS.DECISION_MAKER, 0, 40),
      result(STRATEGY_IDS.TECHNOLOGY, 0, 40),
      result(STRATEGY_IDS.OVERFLOW, 0, 40),
      result(STRATEGY_IDS.RISK, -20, 40),
    ];
    const aggregated = new ScoreAggregator().aggregate(strategyResults);
    const rec = builder.build({ context, strategyResults, aggregated });
    assert.equal(rec.id, 'rec:10:c1');
    assert.ok(Array.isArray(rec.reasoningSummary.whyThis));
    assert.ok(Array.isArray(rec.reasoningSummary.whyNot));
    assert.ok(rec.opposingSignals.length >= 1);
    assert.ok(rec.supportingSignals.length >= 1);
    assert.equal(typeof rec.recommendedAction, 'string');
  });
});
