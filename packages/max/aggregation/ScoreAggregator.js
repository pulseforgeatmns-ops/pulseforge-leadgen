'use strict';

const {
  DEFAULT_STRATEGY_WEIGHTS,
  clamp,
  round,
} = require('../reasoning/ReasoningTypes');

/**
 * Score Aggregator — weighted normalization of strategy scoreDeltas.
 * Confidence is aggregated independently and never mixed into score.
 */
class ScoreAggregator {
  /**
   * @param {object} [options]
   * @param {Record<string, number>} [options.weights]
   */
  constructor(options = {}) {
    this.weights = Object.freeze({
      ...DEFAULT_STRATEGY_WEIGHTS,
      ...(options.weights || {}),
    });
    validateWeights(this.weights);
  }

  /**
   * @param {import('../reasoning/ReasoningTypes').StrategyResult[]} strategyResults
   * @returns {{
   *   score: number,
   *   confidence: number,
   *   normalizedScores: Record<string, number>,
   *   weightedContributions: Record<string, number>,
   *   contradictions: Array<{ strategy: string, supporting: number, contradicting: number }>,
   *   byStrategy: Record<string, import('../reasoning/ReasoningTypes').StrategyResult>
   * }}
   */
  aggregate(strategyResults) {
    const results = strategyResults || [];
    /** @type {Record<string, import('../reasoning/ReasoningTypes').StrategyResult>} */
    const byStrategy = {};
    for (const r of results) {
      byStrategy[r.strategy] = r;
    }

    /** @type {Record<string, number>} */
    const normalizedScores = {};
    /** @type {Record<string, number>} */
    const weightedContributions = {};
    let weightedSum = 0;
    let weightTotal = 0;
    let confidenceWeightedSum = 0;
    let confidenceWeightTotal = 0;

    for (const [strategyId, weight] of Object.entries(this.weights)) {
      const result = byStrategy[strategyId];
      const delta = result ? clamp(result.scoreDelta, -100, 100) : 0;
      // Map [-100, 100] → [0, 100]
      const normalized = round((delta + 100) / 2);
      normalizedScores[strategyId] = normalized;
      const contribution = weight * normalized;
      weightedContributions[strategyId] = round(contribution);
      weightedSum += contribution;
      weightTotal += weight;

      const conf = result ? clamp(result.confidence, 0, 100) : 0;
      confidenceWeightedSum += weight * conf;
      confidenceWeightTotal += weight;
    }

    const score = round(weightTotal === 0 ? 0 : weightedSum / weightTotal);
    const confidence = round(
      confidenceWeightTotal === 0 ? 0 : confidenceWeightedSum / confidenceWeightTotal
    );

    const contradictions = results
      .map((r) => ({
        strategy: r.strategy,
        supporting: (r.supportingEvidence || []).length,
        contradicting: (r.contradictingEvidence || []).length,
        opposingSignals: r.contradictingEvidence || [],
      }))
      .filter((c) => c.contradicting > 0)
      .sort((a, b) => String(a.strategy).localeCompare(String(b.strategy)));

    return {
      score: clamp(score, 0, 100),
      confidence: clamp(confidence, 0, 100),
      normalizedScores,
      weightedContributions,
      contradictions,
      byStrategy,
      weights: { ...this.weights },
    };
  }
}

/**
 * @param {Record<string, number>} weights
 */
function validateWeights(weights) {
  let sum = 0;
  for (const [k, v] of Object.entries(weights)) {
    const n = Number(v);
    if (!Number.isFinite(n) || n < 0) {
      throw new Error(`Invalid weight for ${k}: ${v}`);
    }
    sum += n;
  }
  if (Math.abs(sum - 1) > 0.001) {
    throw new Error(`Strategy weights must sum to 1 (got ${sum})`);
  }
}

module.exports = {
  ScoreAggregator,
  validateWeights,
};
