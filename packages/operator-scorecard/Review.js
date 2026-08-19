'use strict';

/**
 * SPEC-116 — operator review.
 * Accept · Modify · Add · Remove · Reorder.
 * Recommendations are never automatically adopted.
 */

const {
  SCORECARD_STATUS,
  METRIC_STATUS,
  REVIEW_ACTIONS,
  METRIC_SOURCE,
  asText,
  nowIso,
  newId,
  slugify,
  clampConfidence,
  osiError,
} = require('./types');
const { toRecommendation } = require('./Reasoning');
const { getCatalogEntry } = require('./Catalog');

function assertReviewable(scorecard) {
  if (!scorecard) throw osiError('osi_not_found', 'Scorecard not found.');
  if (scorecard.status === SCORECARD_STATUS.APPROVED) {
    throw osiError(
      'osi_approved_immutable',
      'The approved scorecard is authoritative. Generate a new draft to propose changes.'
    );
  }
  if (scorecard.status === SCORECARD_STATUS.SUPERSEDED) {
    throw osiError('osi_superseded', 'This scorecard has been superseded.');
  }
}

function findMetric(scorecard, metricId) {
  const metric = (scorecard.metrics || []).find(
    (row) => row.id === metricId || row.key === metricId
  );
  if (!metric) throw osiError('osi_metric_not_found', `Metric not found: ${metricId}`);
  return metric;
}

function recordReview(scorecard, metric, action, payload, operator) {
  scorecard.reviews = scorecard.reviews || [];
  scorecard.reviews.push({
    id: newId('rev'),
    metricId: metric ? metric.id : null,
    metricKey: metric ? metric.key : null,
    metricName: metric ? metric.name : null,
    action,
    operator: asText(operator) || 'operator',
    payload: payload || {},
    createdAt: nowIso(),
  });
}

function removalPrompt(metric) {
  return `I noticed you removed "${metric.name}." Would you like to tell me why?`;
}

function acceptMetric(scorecard, metricId, opts = {}) {
  assertReviewable(scorecard);
  const metric = findMetric(scorecard, metricId);
  metric.status = METRIC_STATUS.ACCEPTED;
  metric.operatorDecision = {
    action: REVIEW_ACTIONS.ACCEPT,
    operator: asText(opts.operator) || 'operator',
    at: nowIso(),
  };
  recordReview(scorecard, metric, REVIEW_ACTIONS.ACCEPT, {}, opts.operator);
  scorecard.status = SCORECARD_STATUS.IN_REVIEW;
  scorecard.updatedAt = nowIso();
  return { metric, prompt: null };
}

function modifyMetric(scorecard, metricId, patch = {}, opts = {}) {
  assertReviewable(scorecard);
  const metric = findMetric(scorecard, metricId);
  metric.original = metric.original || {
    name: metric.name,
    reason: metric.reason,
    businessOutcome: metric.businessOutcome,
    category: metric.category,
    indicator: metric.indicator,
  };
  if (patch.name != null) metric.name = asText(patch.name);
  if (patch.reason != null) metric.reason = asText(patch.reason);
  if (patch.businessOutcome != null) metric.businessOutcome = asText(patch.businessOutcome);
  if (patch.category != null) metric.category = asText(patch.category);
  if (patch.indicator != null) metric.indicator = asText(patch.indicator);
  if (patch.confidence != null) metric.confidence = clampConfidence(patch.confidence, metric.confidence);
  metric.status = METRIC_STATUS.MODIFIED;
  metric.operatorDecision = {
    action: REVIEW_ACTIONS.MODIFY,
    operator: asText(opts.operator) || 'operator',
    at: nowIso(),
  };
  recordReview(scorecard, metric, REVIEW_ACTIONS.MODIFY, patch, opts.operator);
  scorecard.status = SCORECARD_STATUS.IN_REVIEW;
  scorecard.updatedAt = nowIso();
  return { metric, prompt: null };
}

function removeMetric(scorecard, metricId, opts = {}) {
  assertReviewable(scorecard);
  const metric = findMetric(scorecard, metricId);
  metric.status = METRIC_STATUS.REMOVED;
  metric.removedReason = asText(opts.reason) || null;
  metric.operatorDecision = {
    action: REVIEW_ACTIONS.REMOVE,
    operator: asText(opts.operator) || 'operator',
    at: nowIso(),
    reason: metric.removedReason,
  };
  recordReview(
    scorecard,
    metric,
    REVIEW_ACTIONS.REMOVE,
    { reason: metric.removedReason },
    opts.operator
  );
  scorecard.status = SCORECARD_STATUS.IN_REVIEW;
  scorecard.updatedAt = nowIso();
  return {
    metric,
    prompt: metric.removedReason ? null : removalPrompt(metric),
    learning: {
      metricKey: metric.key,
      metricName: metric.name,
      action: REVIEW_ACTIONS.REMOVE,
      reason: metric.removedReason,
      suppress: true,
    },
  };
}

function addMetric(scorecard, input = {}, opts = {}) {
  assertReviewable(scorecard);
  const name = asText(input.name);
  if (!name) throw osiError('osi_metric_name_required', 'Add requires a metric name.');
  const catalog = getCatalogEntry(input.key) || getCatalogEntry(slugify(name));
  const understanding = (scorecard.reasoning && scorecard.reasoning.understanding) || {
    stage: scorecard.businessStage || 'repeatable_acquisition',
    profile: scorecard.profile || 'default',
  };
  const metric = toRecommendation(
    catalog || {
      key: slugify(input.key || name),
      name,
      category: input.category || 'acquisition',
      indicator: input.indicator || 'leading',
      defaultConfidence: 1,
      reason: input.reason || 'The operator added this metric to the scorecard.',
      businessOutcome: input.businessOutcome || scorecard.businessGoal || 'Operator-defined success',
    },
    understanding,
    {
      name,
      reason: input.reason || (catalog && catalog.reason) || 'The operator added this metric to the scorecard.',
      businessOutcome: input.businessOutcome || (catalog && catalog.businessOutcome) || scorecard.businessGoal,
      category: input.category || (catalog && catalog.category),
      indicator: input.indicator || (catalog && catalog.indicator),
      confidence: input.confidence != null ? input.confidence : 1,
      status: METRIC_STATUS.ADDED,
      source: METRIC_SOURCE.OPERATOR,
      sortOrder: (scorecard.metrics || []).length,
      whyItBelongs: 'The operator added this metric. It is part of the operator-defined scorecard.',
    }
  );
  scorecard.metrics = scorecard.metrics || [];
  scorecard.metrics.push(metric);
  recordReview(scorecard, metric, REVIEW_ACTIONS.ADD, { name }, opts.operator);
  scorecard.status = SCORECARD_STATUS.IN_REVIEW;
  scorecard.updatedAt = nowIso();
  return {
    metric,
    prompt: null,
    learning: {
      metricKey: metric.key,
      metricName: metric.name,
      action: REVIEW_ACTIONS.ADD,
      prioritize: true,
    },
  };
}

function reorderMetrics(scorecard, orderedIds = [], opts = {}) {
  assertReviewable(scorecard);
  const ids = (orderedIds || []).map(asText).filter(Boolean);
  if (!ids.length) throw osiError('osi_reorder_required', 'Reorder requires an ordered list of metric ids.');
  const byId = new Map((scorecard.metrics || []).map((m) => [m.id, m]));
  const byKey = new Map((scorecard.metrics || []).map((m) => [m.key, m]));
  const next = [];
  const used = new Set();
  ids.forEach((id, index) => {
    const metric = byId.get(id) || byKey.get(id);
    if (!metric || used.has(metric.id)) return;
    metric.sortOrder = index;
    next.push(metric);
    used.add(metric.id);
  });
  for (const metric of scorecard.metrics || []) {
    if (used.has(metric.id)) continue;
    metric.sortOrder = next.length;
    next.push(metric);
  }
  scorecard.metrics = next;
  recordReview(scorecard, null, REVIEW_ACTIONS.REORDER, { orderedIds: ids }, opts.operator);
  scorecard.status = SCORECARD_STATUS.IN_REVIEW;
  scorecard.updatedAt = nowIso();
  return { metrics: scorecard.metrics, prompt: null };
}

function reviewMetric(scorecard, metricId, input = {}, opts = {}) {
  const action = asText(input.action).toLowerCase();
  if (action === REVIEW_ACTIONS.ACCEPT) return acceptMetric(scorecard, metricId, opts);
  if (action === REVIEW_ACTIONS.MODIFY) return modifyMetric(scorecard, metricId, input, opts);
  if (action === REVIEW_ACTIONS.REMOVE) {
    return removeMetric(scorecard, metricId, { ...opts, reason: input.reason });
  }
  if (action === REVIEW_ACTIONS.ADD) return addMetric(scorecard, { ...input, name: input.name }, opts);
  if (action === REVIEW_ACTIONS.REORDER) {
    return reorderMetrics(scorecard, input.orderedIds || input.order, opts);
  }
  throw osiError('osi_unknown_review_action', `Unknown review action: ${action || '(empty)'}`);
}

module.exports = {
  assertReviewable,
  findMetric,
  acceptMetric,
  modifyMetric,
  removeMetric,
  addMetric,
  reorderMetrics,
  reviewMetric,
  removalPrompt,
};
