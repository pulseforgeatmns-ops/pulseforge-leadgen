'use strict';

const { round } = require('../reasoning/ReasoningTypes');

/**
 * Build a complete ReasoningReport for debugging, regression, and operator inspection.
 *
 * @param {object} input
 * @param {import('../reasoning/ReasoningTypes').ReasoningContext} input.context
 * @param {import('../reasoning/ReasoningTypes').StrategyResult[]} input.strategyResults
 * @param {object} input.aggregated
 * @param {import('../reasoning/ReasoningTypes').Recommendation} input.recommendation
 * @param {object} [input.explanation]
 * @param {Record<string, number>} [input.strategyTimings]
 * @param {number} input.executionTimeMs
 * @returns {object}
 */
function buildReasoningReport(input) {
  const {
    context,
    strategyResults,
    aggregated,
    recommendation,
    explanation,
    strategyTimings,
    executionTimeMs,
  } = input;

  return {
    context: {
      tenantId: context.tenantId,
      companyId: context.company.id,
      companyName: context.company.name || null,
      peopleCount: (context.people || []).length,
      interactionCount: (context.interactions || []).length,
      claimCount: (context.claims || []).length,
      evidenceCount: (context.evidence || []).length,
      relatedCompanyCount: (context.relatedCompanies || []).length,
      builtAt: context.builtAt,
      repositoryType: context.repositoryType,
      metrics: context.metrics,
    },
    strategyResults: strategyResults || [],
    normalizedScores: aggregated.normalizedScores,
    weightedContributions: aggregated.weightedContributions,
    contradictions: aggregated.contradictions,
    recommendation,
    explanation: explanation || null,
    strategyTimings: strategyTimings || {},
    executionTime: round(executionTimeMs),
    performance: {
      executionTimeMs: round(executionTimeMs),
      strategyTimings: strategyTimings || {},
      graphQueries: context.metrics.graphQueries,
      nodesTraversed: context.metrics.nodesTraversed,
      repositoryType: context.repositoryType,
    },
  };
}

module.exports = { buildReasoningReport };
