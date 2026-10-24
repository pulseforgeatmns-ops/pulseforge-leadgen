'use strict';

const {
  LIFECYCLE,
  OUTCOME_RESULTS,
  STRATEGY_IDS,
} = require('./OutcomeTypes');

/**
 * Strategy-level performance metrics (SPEC-013).
 * Internal only — never shown to customers; never reweights strategies.
 */

/**
 * @param {object} input
 * @param {object[]} input.records
 * @returns {object}
 */
function buildStrategyPerformance(input = {}) {
  const records = input.records || [];
  const byStrategy = {};

  for (const id of STRATEGY_IDS) {
    byStrategy[id] = emptyStrategyMetrics(id);
  }

  for (const row of records) {
    const sid = row.strategyId || 'unknown';
    if (!byStrategy[sid]) {
      byStrategy[sid] = emptyStrategyMetrics(sid);
    }
    const m = byStrategy[sid];
    m.generated += 1;
    if (
      row.lifecycle === LIFECYCLE.REVIEWED ||
      row.lifecycle === LIFECYCLE.APPROVED ||
      row.lifecycle === LIFECYCLE.EXECUTED ||
      row.lifecycle === LIFECYCLE.OBSERVED ||
      row.outcome != null
    ) {
      m.reviewed += 1;
    }
    if (
      row.lifecycle === LIFECYCLE.APPROVED ||
      row.lifecycle === LIFECYCLE.EXECUTED ||
      row.lifecycle === LIFECYCLE.OBSERVED ||
      row.outcome != null
    ) {
      m.approved += 1;
    }
    if (row.executed || row.outcome != null) m.executed += 1;
    if (row.outcome === OUTCOME_RESULTS.SUCCESSFUL) m.successful += 1;
    if (row.outcome === OUTCOME_RESULTS.UNSUCCESSFUL) m.unsuccessful += 1;
    if (row.outcome === OUTCOME_RESULTS.INCONCLUSIVE) m.inconclusive += 1;
    if (row.promotedFromWatch) m.promoted += 1;
    if (row.watchAlertEarly) m.watchAlertEarly += 1;

    if (row.executedAt && row.generatedAt) {
      const lead =
        new Date(row.executedAt).getTime() -
        new Date(row.generatedAt).getTime();
      if (Number.isFinite(lead) && lead >= 0) {
        m._leadTimes.push(lead);
      }
    }
  }

  const strategies = Object.keys(byStrategy)
    .sort()
    .map((id) => finalizeStrategyMetrics(byStrategy[id]));

  return {
    generatedAt: new Date().toISOString(),
    strategies,
    highlights: {
      overflow: strategies.find((s) => s.strategyId === 'overflow') || null,
      relationship:
        strategies.find((s) => s.strategyId === 'relationship') || null,
      decisionMaker:
        strategies.find((s) => s.strategyId === 'decision_maker') || null,
    },
    customerFacing: false,
    mutatesReasoning: false,
  };
}

function emptyStrategyMetrics(strategyId) {
  return {
    strategyId,
    generated: 0,
    reviewed: 0,
    approved: 0,
    executed: 0,
    successful: 0,
    unsuccessful: 0,
    inconclusive: 0,
    promoted: 0,
    watchAlertEarly: 0,
    _leadTimes: [],
  };
}

function finalizeStrategyMetrics(m) {
  const decisive = m.successful + m.unsuccessful;
  const observed = decisive + m.inconclusive;
  const precision =
    m.executed === 0 ? null : round3(m.successful / Math.max(m.executed, 1));
  // Recall proxy: successful / generated (of those that reached a terminal success)
  const recall =
    m.generated === 0 ? null : round3(m.successful / m.generated);
  const promotionRate =
    m.generated === 0 ? null : round3(m.promoted / m.generated);
  const recommendationSuccessRate =
    decisive === 0 ? null : round3(m.successful / decisive);
  const avgLeadTimeMs =
    m._leadTimes.length === 0
      ? null
      : Math.round(
          m._leadTimes.reduce((a, b) => a + b, 0) / m._leadTimes.length
        );

  return {
    strategyId: m.strategyId,
    generated: m.generated,
    reviewed: m.reviewed,
    approved: m.approved,
    executed: m.executed,
    successful: m.successful,
    unsuccessful: m.unsuccessful,
    inconclusive: m.inconclusive,
    observed,
    /** Overflow-style: successes among executed. */
    precision,
    /** Overflow-style: successes among all generated. */
    recall,
    /** Watch → priority promotion share. */
    promotionRate,
    /** Relationship-style: success among decisive outcomes. */
    recommendationSuccessRate,
    /** Decision-maker / hiring-style: avg ms from generate → execute. */
    averageLeadTimeMs: avgLeadTimeMs,
    watchAlertEarlyCount: m.watchAlertEarly,
  };
}

function round3(n) {
  return Math.round(n * 1000) / 1000;
}

module.exports = { buildStrategyPerformance };
