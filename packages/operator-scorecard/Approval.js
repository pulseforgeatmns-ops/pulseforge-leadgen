'use strict';

/**
 * SPEC-116 — operator approval.
 * Only after operator approval does the scorecard become runtime truth.
 */

const {
  SCORECARD_STATUS,
  METRIC_STATUS,
  asText,
  nowIso,
  osiError,
} = require('./types');
const { assertReviewable } = require('./Review');

function remainingRecommended(scorecard) {
  return (scorecard.metrics || []).filter(
    (m) => m.status === METRIC_STATUS.RECOMMENDED || m.status === METRIC_STATUS.UNDER_REVIEW
  );
}

function approvableMetrics(scorecard) {
  return (scorecard.metrics || []).filter((m) =>
    [METRIC_STATUS.ACCEPTED, METRIC_STATUS.MODIFIED, METRIC_STATUS.ADDED].includes(m.status)
  );
}

function approveScorecard(scorecard, opts = {}) {
  assertReviewable(scorecard);
  const acceptRemaining = opts.acceptRemaining !== false;
  if (acceptRemaining) {
    for (const metric of remainingRecommended(scorecard)) {
      metric.status = METRIC_STATUS.ACCEPTED;
      metric.operatorDecision = {
        action: 'accept',
        operator: asText(opts.operator) || 'operator',
        at: nowIso(),
        via: 'approve_remaining',
      };
    }
  }
  const leftover = remainingRecommended(scorecard);
  if (leftover.length) {
    throw osiError(
      'osi_unreviewed_metrics',
      'Approve requires every recommended metric to be accepted, modified, or removed.',
      { metricIds: leftover.map((m) => m.id) }
    );
  }
  if (!approvableMetrics(scorecard).length) {
    throw osiError('osi_nothing_to_approve', 'Approve requires at least one accepted, modified, or added metric.');
  }
  scorecard.status = SCORECARD_STATUS.APPROVED;
  scorecard.isRuntime = true;
  scorecard.approvedAt = nowIso();
  scorecard.approvedBy = asText(opts.operator) || 'operator';
  scorecard.updatedAt = nowIso();
  return scorecard;
}

function supersedeScorecard(scorecard) {
  if (!scorecard) return scorecard;
  scorecard.status = SCORECARD_STATUS.SUPERSEDED;
  scorecard.isRuntime = false;
  scorecard.updatedAt = nowIso();
  return scorecard;
}

module.exports = {
  remainingRecommended,
  approvableMetrics,
  approveScorecard,
  supersedeScorecard,
};
