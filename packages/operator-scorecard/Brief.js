'use strict';

/**
 * SPEC-116 — Executive Business Brief + Daily Briefing scorecard sections.
 * Distinguishes Max recommendations from operator-approved metrics.
 * Daily briefings never treat a draft as the definition of success.
 */

const { SCORECARD_STATUS, METRIC_STATUS, confidencePercent } = require('./types');
const { recommendedMetrics, approvedMetrics, getRuntimeScorecard, activeMetrics } = require('./Reasoning');

function metricItem(metric) {
  const support =
    typeof metric.businessOutcome === 'string' &&
    metric.businessOutcome.length <= 80 &&
    !/[.!?]/.test(metric.businessOutcome)
      ? ` Supports ${metric.businessOutcome}.`
      : '';
  return {
    id: metric.id,
    key: metric.key,
    name: metric.name,
    reason: metric.reason,
    businessOutcome: metric.businessOutcome,
    confidence: metric.confidence,
    confidencePercent: confidencePercent(metric.confidence),
    indicator: metric.indicator,
    category: metric.category,
    status: metric.status,
    source: metric.source,
    whyItBelongs: metric.whyItBelongs,
    label: `${metric.name} — ${metric.reason}${support} Confidence ${confidencePercent(
      metric.confidence
    )}%. ${metric.indicator === 'lagging' ? 'Lagging' : 'Leading'} indicator.`,
  };
}

function emptyApprovedBody() {
  return 'The operator has not yet approved a scorecard. Until that happens, Max\'s recommendations remain advisory and are not the definition of business success.';
}

function buildBriefScorecardSections(scorecard) {
  const recommended = scorecard ? recommendedMetrics(scorecard) : [];
  const approved =
    scorecard && scorecard.status === SCORECARD_STATUS.APPROVED ? approvedMetrics(scorecard) : [];
  const underReview = recommended.filter(
    (m) => m.status === METRIC_STATUS.UNDER_REVIEW
  );
  const extraExplore =
    (scorecard && scorecard.reasoning && scorecard.reasoning.extraExplore) || [];

  const recommendedBody = extraExplore.length
    ? `Max recommends these metrics because they best support the operator's stated business objectives. Max may also want to explore: ${extraExplore.join(', ')}.`
    : "Max recommends these metrics because they best support the operator's stated business objectives.";

  return [
    {
      id: 'recommendedScorecard',
      title: 'Recommended Operator Scorecard',
      kind: 'scorecard',
      body: recommendedBody,
      items: (scorecard ? activeMetrics(scorecard) : []).map(metricItem),
    },
    {
      id: 'approvedScorecard',
      title: 'Operator Approved Scorecard',
      kind: 'scorecard',
      body:
        approved.length > 0
          ? 'The following metrics have been explicitly approved by the operator and define business success.'
          : emptyApprovedBody(),
      items: approved.map(metricItem),
    },
    {
      id: 'metricsUnderReview',
      title: 'Metrics Under Review',
      kind: 'scorecard',
      body: 'Metrics Max recommends but the operator has not yet accepted.',
      items: underReview.map(metricItem),
    },
  ];
}

function buildDailyBriefingScorecardSection(scorecard) {
  const runtime = getRuntimeScorecard(scorecard);
  return {
    status: runtime.status,
    source: runtime.source,
    definitionOfSuccess: runtime.definitionOfSuccess,
    metrics: runtime.metrics,
    businessGoal: runtime.businessGoal || (scorecard && scorecard.businessGoal) || null,
    note: runtime.note,
  };
}

function formatRuntimeForDigest(runtime) {
  if (!runtime || runtime.status !== 'approved' || !(runtime.metrics || []).length) {
    return 'No operator-approved scorecard. Do not treat Max draft recommendations as the definition of success.';
  }
  const lines = runtime.metrics.map(
    (m) => `- ${m.name} (${m.category}; ${m.indicator}) — ${m.businessOutcome}`
  );
  return [
    'OPERATOR APPROVED SCORECARD (definition of business success):',
    ...lines,
    'Measure and recommend against these metrics only. Do not substitute unapproved draft metrics.',
  ].join('\n');
}

module.exports = {
  metricItem,
  buildBriefScorecardSections,
  buildDailyBriefingScorecardSection,
  formatRuntimeForDigest,
};
