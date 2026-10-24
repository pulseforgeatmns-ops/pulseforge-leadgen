'use strict';

/**
 * Explanation Engine — evidence chains for recommendations.
 *
 * Recommendation → Supporting Claims → Evidence → Original Sources → Confidence → Contradictions
 */
class ExplanationEngine {
  /**
   * @param {object} input
   * @param {import('../reasoning/ReasoningTypes').Recommendation} input.recommendation
   * @param {import('../reasoning/ReasoningTypes').ReasoningContext} input.context
   * @param {import('../reasoning/ReasoningTypes').StrategyResult[]} [input.strategyResults]
   */
  explain(input) {
    const { recommendation, context } = input;
    if (!recommendation) throw new Error('ExplanationEngine requires recommendation');
    if (!context) throw new Error('ExplanationEngine requires context');

    const claimById = new Map((context.claims || []).map((c) => [c.id, c]));
    const evidenceById = new Map((context.evidence || []).map((e) => [e.id, e]));

    const supportingClaims = (recommendation.claims || [])
      .map((id) => claimById.get(id))
      .filter(Boolean)
      .map((c) => ({
        id: c.id,
        statement: c.statement,
        confidence: c.confidence,
        status: c.status,
        reason: c.reason || null,
      }))
      .sort((a, b) => String(a.id).localeCompare(String(b.id)));

    const evidenceNodes = [
      ...new Set([
        ...(recommendation.evidence || []),
        ...recommendation.supportingSignals
          .filter((s) => s.kind === 'evidence')
          .map((s) => s.id),
        ...recommendation.opposingSignals
          .filter((s) => s.kind === 'evidence')
          .map((s) => s.id),
      ]),
    ]
      .map((id) => evidenceById.get(id))
      .filter(Boolean)
      .map((e) => ({
        id: e.id,
        summary: e.summary,
        sourceType: e.sourceType || null,
        sourceId: e.sourceId || null,
        confidence: e.confidence == null ? null : Number(e.confidence),
      }))
      .sort((a, b) => String(a.id).localeCompare(String(b.id)));

    const originalSources = evidenceNodes
      .filter((e) => e.sourceId || e.sourceType)
      .map((e) => ({
        evidenceId: e.id,
        sourceType: e.sourceType,
        sourceId: e.sourceId,
      }))
      .sort((a, b) => String(a.evidenceId).localeCompare(String(b.evidenceId)));

    return {
      recommendationId: recommendation.id,
      subjectId: recommendation.subject.id,
      score: recommendation.score,
      confidence: recommendation.confidence,
      supportingClaims,
      evidence: evidenceNodes,
      originalSources,
      supportingSignals: recommendation.supportingSignals,
      contradictions: recommendation.opposingSignals,
      chain: {
        recommendation: recommendation.id,
        supportingClaims: supportingClaims.map((c) => c.id),
        evidence: evidenceNodes.map((e) => e.id),
        originalSources: originalSources.map((s) => s.sourceId),
        confidence: recommendation.confidence,
        contradictions: recommendation.opposingSignals.map((s) => s.id),
      },
    };
  }
}

module.exports = { ExplanationEngine };
