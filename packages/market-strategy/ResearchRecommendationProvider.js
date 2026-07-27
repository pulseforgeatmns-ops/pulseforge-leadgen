'use strict';

const { assertRecommendationProvider } = require('@pulseforge/reasoning-runtime');
const {
  RESEARCH_ACTIONS,
  FORBIDDEN_ACTIONS,
  MARKET_CLAIM_TYPES,
  clamp,
  round,
} = require('./types');

/**
 * ResearchRecommendationProvider — research-only recommendations (SPEC-016).
 * Explicitly prohibits execution vocabulary.
 */
class ResearchRecommendationProvider {
  /**
   * @param {object} [deps]
   * @param {string} [deps.id]
   */
  constructor(deps = {}) {
    this.id = deps.id || 'research-recommendation';
    assertRecommendationProvider(this);
  }

  /**
   * @param {object} input
   * @param {import('./types').MarketContext} input.context
   * @param {import('./types').MarketStrategyResult[]} input.strategyResults
   * @param {object} input.aggregated
   * @param {object} [input.claims]
   * @param {import('./types').MarketEvidenceRef[]} [input.evidence]
   * @param {import('./types').HistoricalAnalog[]} [input.analogs]
   * @returns {import('./types').ResearchRecommendation}
   */
  generate(input) {
    const { context, strategyResults, aggregated, analogs } = input;
    if (!context || !context.subjectId) {
      throw new Error('ResearchRecommendationProvider requires context.subjectId');
    }

    const supportingSignals = dedupeRefs(
      (strategyResults || []).flatMap((r) => r.supportingEvidence || [])
    );
    const opposingSignals = dedupeRefs(
      (strategyResults || []).flatMap((r) => r.contradictingEvidence || [])
    );
    const claimIds = [
      ...new Set((strategyResults || []).flatMap((r) => r.claims || [])),
    ].sort();
    const evidenceIds = [
      ...new Set(
        [...supportingSignals, ...opposingSignals]
          .filter((s) => s.kind === 'evidence')
          .map((s) => s.id)
      ),
    ].sort();

    const score = round(clamp(aggregated.score, 0, 100));
    const confidence = round(clamp(aggregated.confidence, 0, 100));
    const { type, recommendedAction } = classifyResearchAction(
      strategyResults || [],
      analogs || [],
      aggregated
    );

    assertResearchOnly(recommendedAction);

    const priority = classifyPriority(score, confidence);
    const whyThis = supportingSignals.slice(0, 10).map((s) => s.summary);
    const whyNot = opposingSignals.slice(0, 10).map((s) => s.summary);
    const asOf = context.builtAt || context.asOf || 'asof';
    const deterministicId = [
      'research',
      context.subjectId,
      asOf,
      recommendedAction,
      score,
      confidence,
    ].join(':');

    return {
      id: deterministicId,
      subject: {
        id: context.subjectId,
        name: context.asset?.symbol || context.subjectId,
        type: 'asset',
      },
      type,
      priority,
      score,
      confidence,
      recommendedAction,
      supportingSignals,
      opposingSignals,
      claims: claimIds,
      evidence: evidenceIds,
      reasoningSummary: {
        whyThis,
        whyNot,
        confidenceBasis: (strategyResults || []).map(
          (r) => `${r.strategy}:confidence=${r.confidence}`
        ),
        scoreComponents: aggregated.normalizedScores || {},
        analogCount: (analogs || []).length,
      },
    };
  }
}

/**
 * @param {import('./types').MarketStrategyResult[]} strategyResults
 * @param {import('./types').HistoricalAnalog[]} analogs
 * @param {object} aggregated
 */
function classifyResearchAction(strategyResults, analogs, aggregated) {
  const byId = Object.fromEntries(strategyResults.map((r) => [r.strategy, r]));
  const regime = byId[MARKET_CLAIM_TYPES.REGIME_TRANSITION];
  const exhaustion = byId[MARKET_CLAIM_TYPES.MOMENTUM_EXHAUSTION];
  const continuation = byId[MARKET_CLAIM_TYPES.MOMENTUM_CONTINUATION];
  const analogTop = analogs[0];

  if (regime && (regime.claims || []).length > 0) {
    return {
      type: RESEARCH_ACTIONS.REGIME_TRANSITION,
      recommendedAction: RESEARCH_ACTIONS.REGIME_TRANSITION,
    };
  }
  if (analogTop && analogTop.similarityScore >= 70) {
    return {
      type: RESEARCH_ACTIONS.HISTORICAL_ANALOG_FOUND,
      recommendedAction: RESEARCH_ACTIONS.HISTORICAL_ANALOG_FOUND,
    };
  }
  if (exhaustion && (exhaustion.claims || []).length > 0) {
    return {
      type: RESEARCH_ACTIONS.HYPOTHESIS_WEAKENING,
      recommendedAction: RESEARCH_ACTIONS.HYPOTHESIS_WEAKENING,
    };
  }
  if (continuation && (continuation.claims || []).length > 0 && aggregated.score >= 55) {
    return {
      type: RESEARCH_ACTIONS.HYPOTHESIS_STRENGTHENING,
      recommendedAction: RESEARCH_ACTIONS.HYPOTHESIS_STRENGTHENING,
    };
  }
  if (aggregated.confidence < 45) {
    return {
      type: RESEARCH_ACTIONS.GATHER_MORE_EVIDENCE,
      recommendedAction: RESEARCH_ACTIONS.GATHER_MORE_EVIDENCE,
    };
  }
  if ((strategyResults || []).some((r) => (r.contradictingEvidence || []).length > 0)) {
    return {
      type: RESEARCH_ACTIONS.EVIDENCE_SHIFT,
      recommendedAction: RESEARCH_ACTIONS.EVIDENCE_SHIFT,
    };
  }
  if (analogTop) {
    return {
      type: RESEARCH_ACTIONS.REPLAY_SUGGESTED,
      recommendedAction: RESEARCH_ACTIONS.REPLAY_SUGGESTED,
    };
  }
  return {
    type: RESEARCH_ACTIONS.OBSERVE,
    recommendedAction: RESEARCH_ACTIONS.OBSERVE,
  };
}

/**
 * @param {number} score
 * @param {number} confidence
 */
function classifyPriority(score, confidence) {
  if (score >= 75 && confidence >= 60) return 'high';
  if (score >= 50) return 'medium';
  return 'low';
}

/**
 * @param {import('./types').MarketEvidenceRef[]} refs
 */
function dedupeRefs(refs) {
  const seen = new Set();
  return refs.filter((r) => {
    if (seen.has(r.id)) return false;
    seen.add(r.id);
    return true;
  });
}

/**
 * @param {string} action
 */
function assertResearchOnly(action) {
  const lower = String(action).toLowerCase();
  for (const forbidden of FORBIDDEN_ACTIONS) {
    if (lower.includes(forbidden)) {
      throw new Error(
        `ResearchRecommendationProvider prohibits execution action: ${action}`
      );
    }
  }
  const allowed = new Set(Object.values(RESEARCH_ACTIONS));
  if (!allowed.has(action)) {
    throw new Error(`Unknown research action: ${action}`);
  }
}

/**
 * @param {object} [deps]
 * @returns {ResearchRecommendationProvider}
 */
function createResearchRecommendationProvider(deps) {
  return new ResearchRecommendationProvider(deps);
}

module.exports = {
  ResearchRecommendationProvider,
  createResearchRecommendationProvider,
  classifyResearchAction,
  assertResearchOnly,
};
