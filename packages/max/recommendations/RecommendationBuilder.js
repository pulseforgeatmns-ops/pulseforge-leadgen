'use strict';

const {
  RECOMMENDATION_TYPES,
  PRIORITIES,
  RECOMMENDED_ACTIONS,
  recommendationId,
  round,
  clamp,
} = require('../reasoning/ReasoningTypes');

/**
 * Recommendation Builder — structured recommendation from strategy observations.
 * No LLM. No generated prose. Pure structured data.
 */
class RecommendationBuilder {
  /**
   * @param {object} input
   * @param {import('../reasoning/ReasoningTypes').ReasoningContext} input.context
   * @param {import('../reasoning/ReasoningTypes').StrategyResult[]} input.strategyResults
   * @param {ReturnType<import('../aggregation/ScoreAggregator').ScoreAggregator['aggregate']>} input.aggregated
   * @returns {import('../reasoning/ReasoningTypes').Recommendation}
   */
  build(input) {
    const { context, strategyResults, aggregated } = input;
    if (!context || !context.company) {
      throw new Error('RecommendationBuilder requires context.company');
    }

    const supportingSignals = dedupeRefs(
      (strategyResults || []).flatMap((r) => r.supportingEvidence || [])
    );
    const opposingSignals = dedupeRefs(
      (strategyResults || []).flatMap((r) => r.contradictingEvidence || [])
    );
    const claims = [
      ...new Set((strategyResults || []).flatMap((r) => r.claims || [])),
    ].sort();
    const evidence = [
      ...new Set(
        [...supportingSignals, ...opposingSignals]
          .filter((s) => s.kind === 'evidence')
          .map((s) => s.id)
      ),
    ].sort();

    const score = round(clamp(aggregated.score, 0, 100));
    const confidence = round(clamp(aggregated.confidence, 0, 100));
    const type = classifyType(score, opposingSignals.length, supportingSignals.length);
    const priority = classifyPriority(score, confidence);
    const recommendedAction = classifyAction(type, context, strategyResults);

    const whyThis = supportingSignals.slice(0, 12).map((s) => s.summary);
    const whyNot = opposingSignals.slice(0, 12).map((s) => s.summary);
    const whyNow = buildWhyNow(context, strategyResults);
    const confidenceBasis = (strategyResults || []).map(
      (r) => `${r.strategy}:confidence=${r.confidence}:evidence=${
        (r.supportingEvidence || []).length + (r.contradictingEvidence || []).length
      }`
    );

    return {
      id: recommendationId(context.tenantId, context.company.id),
      subject: {
        id: context.company.id,
        name: context.company.name != null ? context.company.name : null,
        type: 'company',
      },
      type,
      priority,
      score,
      confidence,
      recommendedAction,
      supportingSignals,
      opposingSignals,
      claims,
      evidence,
      reasoningSummary: {
        whyThis,
        whyNow,
        whyNot,
        confidenceBasis,
        scoreComponents: aggregated.normalizedScores,
        weights: aggregated.weights,
      },
    };
  }
}

/**
 * @param {number} score
 * @param {number} opposeCount
 * @param {number} supportCount
 */
function classifyType(score, opposeCount, supportCount) {
  if (score >= 70 && opposeCount <= supportCount) return RECOMMENDATION_TYPES.PURSUE;
  if (score >= 55) return RECOMMENDATION_TYPES.FOLLOW_UP;
  if (score >= 40) return RECOMMENDATION_TYPES.NURTURE;
  return RECOMMENDATION_TYPES.DEPRIORITIZE;
}

/**
 * @param {number} score
 * @param {number} confidence
 */
function classifyPriority(score, confidence) {
  // Priority from score bands; confidence does not inflate priority
  if (score >= 80) return PRIORITIES.CRITICAL;
  if (score >= 60) return PRIORITIES.HIGH;
  if (score >= 40) return PRIORITIES.MEDIUM;
  return PRIORITIES.LOW;
}

/**
 * @param {string} type
 * @param {import('../reasoning/ReasoningTypes').ReasoningContext} context
 * @param {import('../reasoning/ReasoningTypes').StrategyResult[]} strategyResults
 */
function classifyAction(type, context, strategyResults) {
  const byId = Object.fromEntries((strategyResults || []).map((r) => [r.strategy, r]));
  const dm = byId.decision_maker;
  const engagement = byId.engagement;
  const hasDm =
    dm && (dm.supportingEvidence || []).some((e) => String(e.summary).includes('Decision-maker'));
  const peopleCount = (context.people || []).length;

  if (type === RECOMMENDATION_TYPES.DEPRIORITIZE) {
    return RECOMMENDED_ACTIONS.DEPRIORITIZE;
  }
  if (peopleCount === 0 || (dm && dm.scoreDelta < 0 && !hasDm)) {
    return RECOMMENDED_ACTIONS.ENRICH_CONTACTS;
  }
  if (type === RECOMMENDATION_TYPES.PURSUE && hasDm) {
    return RECOMMENDED_ACTIONS.REQUEST_INTRO;
  }
  if (type === RECOMMENDATION_TYPES.FOLLOW_UP) {
    return RECOMMENDED_ACTIONS.FOLLOW_UP_OUTREACH;
  }
  if (type === RECOMMENDATION_TYPES.NURTURE) {
    return RECOMMENDED_ACTIONS.NURTURE_SEQUENCE;
  }
  if (engagement && engagement.scoreDelta < -20) {
    return RECOMMENDED_ACTIONS.HOLD;
  }
  return RECOMMENDED_ACTIONS.FOLLOW_UP_OUTREACH;
}

/**
 * @param {import('../reasoning/ReasoningTypes').ReasoningContext} context
 * @param {import('../reasoning/ReasoningTypes').StrategyResult[]} strategyResults
 */
function buildWhyNow(context, strategyResults) {
  const reasons = [];
  for (const r of strategyResults || []) {
    if (r.strategy === 'engagement' && r.summary.includes('days_since=')) {
      const m = /days_since=(\d+|null)/.exec(r.summary);
      if (m) reasons.push(`engagement_recency:days_since=${m[1]}`);
    }
    if (r.strategy === 'opportunity' && (r.supportingEvidence || []).length > 0) {
      reasons.push(`opportunity_signals=${r.supportingEvidence.length}`);
    }
    if (r.strategy === 'overflow' && (r.supportingEvidence || []).length > 0) {
      reasons.push(`overflow_signals=${r.supportingEvidence.length}`);
    }
  }
  if (reasons.length === 0) {
    reasons.push('no_time_sensitive_signal');
  }
  return reasons.sort();
}

/**
 * @param {import('../reasoning/ReasoningTypes').EvidenceRef[]} refs
 */
function dedupeRefs(refs) {
  const map = new Map();
  for (const ref of refs || []) {
    const key = `${ref.kind}:${ref.id}:${ref.summary}`;
    if (!map.has(key)) map.set(key, ref);
  }
  return [...map.values()].sort((a, b) => {
    const c = String(a.id).localeCompare(String(b.id));
    if (c !== 0) return c;
    return String(a.summary).localeCompare(String(b.summary));
  });
}

module.exports = {
  RecommendationBuilder,
  classifyType,
  classifyPriority,
  classifyAction,
};
