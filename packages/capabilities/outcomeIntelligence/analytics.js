'use strict';

/**
 * Campaign analytics from outcome records (SPEC-036).
 */

const {
  OUTCOME_TYPES,
  RESPONSE_OUTCOMES,
  buildCampaignAnalytics,
} = require('./types');

/**
 * @param {object[]} outcomes
 * @param {object} [opts]
 * @returns {object}
 */
function computeCampaignAnalytics(outcomes, opts = {}) {
  const list = Array.isArray(outcomes) ? outcomes : [];
  const mailedCount =
    opts.mailed != null
      ? Number(opts.mailed)
      : Math.max(
          list.length,
          Number(opts.executionMetrics && opts.executionMetrics.mailed) || 0
        );

  const responses = list.filter((o) => RESPONSE_OUTCOMES.has(o.outcomeType))
    .length;
  const walkthroughs = list.filter(
    (o) => o.outcomeType === OUTCOME_TYPES.WALKTHROUGH_SCHEDULED
  ).length;
  const proposals = list.filter(
    (o) =>
      o.outcomeType === OUTCOME_TYPES.PROPOSAL_SENT ||
      o.outcomeType === OUTCOME_TYPES.PROPOSAL_REQUESTED ||
      o.outcomeType === OUTCOME_TYPES.CLOSED_WON
  ).length;
  const wins = list.filter(
    (o) => o.outcomeType === OUTCOME_TYPES.CLOSED_WON
  ).length;

  return buildCampaignAnalytics({
    mailed: mailedCount,
    responses,
    walkthroughs,
    proposals,
    wins,
    cost: opts.cost != null ? Number(opts.cost) : null,
    revenue: opts.revenue != null ? Number(opts.revenue) : null,
  });
}

module.exports = {
  computeCampaignAnalytics,
};
