'use strict';

/**
 * Helpers to build synthetic ReasoningSnapshots for memory unit tests.
 */

function snapshot(overrides = {}) {
  const tenantId = overrides.tenantId || '10';
  const companyId = overrides.companyId || 'co-1';
  const timestamp = overrides.timestamp || '2026-07-20T12:00:00.000Z';
  const score = overrides.score == null ? 70 : overrides.score;
  const confidence = overrides.confidence == null ? 50 : overrides.confidence;
  const claims = [...(overrides.claims || [])].sort();
  const evidence = [...(overrides.evidence || [])].sort();
  const strategyResults = (overrides.strategyResults || defaultStrategies()).map((r) => ({
    ...r,
    claims: [...(r.claims || [])].sort(),
    supportingEvidence: [...(r.supportingEvidence || [])],
    contradictingEvidence: [...(r.contradictingEvidence || [])],
  }));

  return {
    id: overrides.id || `snap:${tenantId}:${companyId}:${timestamp}:000001`,
    tenantId,
    companyId,
    timestamp,
    score,
    confidence,
    claims,
    evidence,
    strategyResults,
    recommendation: {
      id: `rec:${tenantId}:${companyId}`,
      subject: { id: companyId, name: overrides.name || 'Acme', type: 'company' },
      type: overrides.type || 'follow_up',
      priority: overrides.priority || 'high',
      score,
      confidence,
      recommendedAction: overrides.recommendedAction || 'follow_up_outreach',
      supportingSignals: overrides.supportingSignals || [],
      opposingSignals: overrides.opposingSignals || [],
      claims,
      evidence,
      reasoningSummary: { whyThis: [], whyNow: [], whyNot: [], confidenceBasis: [] },
    },
    meta: {
      recommendationId: `rec:${tenantId}:${companyId}`,
      type: overrides.type || 'follow_up',
      priority: overrides.priority || 'high',
      recommendedAction: overrides.recommendedAction || 'follow_up_outreach',
      subjectName: overrides.name || 'Acme',
      claimConfidences: overrides.claimConfidences || undefined,
    },
  };
}

function defaultStrategies() {
  return [
    strat('opportunity', 20, 40),
    strat('relationship', 10, 40),
    strat('engagement', 10, 40),
    strat('decision_maker', 10, 40),
    strat('technology', 5, 40),
    strat('overflow', 5, 40),
    strat('risk', -5, 40),
  ];
}

function strat(strategy, scoreDelta, confidence, extras = {}) {
  return {
    strategy,
    scoreDelta,
    confidence,
    summary: `${strategy}:ok`,
    claims: extras.claims || [],
    supportingEvidence: extras.supportingEvidence || [],
    contradictingEvidence: extras.contradictingEvidence || [],
  };
}

module.exports = { snapshot, strat, defaultStrategies };
