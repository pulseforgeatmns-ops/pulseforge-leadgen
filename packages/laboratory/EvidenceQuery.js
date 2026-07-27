'use strict';

/**
 * EvidenceQuery — ask questions of replay / experiment results (SPEC-019).
 *
 * The Laboratory asks; the Evidence Platform (via replay results) answers.
 * Pure functions over already-regenerated reasoning — no production I/O.
 */
class EvidenceQuery {
  /**
   * Show every observation that supported a claim during replay.
   *
   * @param {object} result - ScenarioRunner / ReplayEngine result
   * @param {string} claimId
   * @returns {object[]}
   */
  observationsSupportingClaim(result, claimId) {
    const target = String(claimId);
    const matches = [];

    for (const step of result.steps || []) {
      if (!step.observation) continue;
      const claimIds = extractClaimIds(step.affectedClaims);
      const appeared = (step.affectedClaims && step.affectedClaims.appeared) || [];
      if (claimIds.includes(target) || appeared.includes(target)) {
        matches.push({
          observationId: step.observation.id,
          observedAt: step.observation.observedAt,
          observationType: step.observation.observationType,
          claimId: target,
          confidence: step.confidence,
          recommendation: step.recommendation
            ? step.recommendation.recommendedAction
            : null,
          role: appeared.includes(target) ? 'appeared' : 'present',
        });
      }
    }

    // Also scan supporting evidence refs on explanations / recommendations.
    const evidenceRefs = collectEvidenceRefs(result, 'supporting');
    for (const ref of evidenceRefs) {
      if (ref.claimId === target || (ref.claims || []).includes(target)) {
        if (!matches.some((m) => m.observationId === ref.observationId)) {
          matches.push({
            observationId: ref.observationId || ref.id,
            observedAt: ref.observedAt || null,
            observationType: ref.observationType || ref.kind || null,
            claimId: target,
            confidence: ref.confidence ?? null,
            recommendation: null,
            role: 'supporting_ref',
          });
        }
      }
    }

    return matches;
  }

  /**
   * Observations linked to contradicting evidence for a claim.
   *
   * @param {object} result
   * @param {string} claimId
   * @returns {object}
   */
  observationsContradictingClaim(result, claimId) {
    const target = String(claimId);
    const last = (result.steps || [])[(result.steps || []).length - 1];
    const explanation = (last && last.explanation) || result.explanation || {};
    const contradicting =
      explanation.contradictingEvidence ||
      explanation.opposingSignals ||
      [];

    return {
      claimId: target,
      contradictingEvidence: Array.isArray(contradicting) ? contradicting : [],
      temporal:
        result.queries && typeof result.queries.whatEvidenceContradictedClaim === 'function'
          ? result.queries.whatEvidenceContradictedClaim(target)
          : null,
    };
  }

  /**
   * Full claim evidence surface: supporting + contradicting + first appearance.
   *
   * @param {object} result
   * @param {string} claimId
   * @returns {object}
   */
  claimEvidence(result, claimId) {
    const target = String(claimId);
    return {
      claimId: target,
      supporting: this.observationsSupportingClaim(result, target),
      contradicting: this.observationsContradictingClaim(result, target),
      firstAppeared:
        result.queries && typeof result.queries.whenClaimFirstAppeared === 'function'
          ? result.queries.whenClaimFirstAppeared(target)
          : null,
      becameDominant:
        result.queries && typeof result.queries.whenClaimBecameDominant === 'function'
          ? result.queries.whenClaimBecameDominant(target)
          : null,
    };
  }

  /**
   * Belief / recommendation / confidence at timestamp T.
   *
   * @param {object} result
   * @param {string} timestamp
   * @returns {object|null}
   */
  beliefAt(result, timestamp) {
    if (result.queries && typeof result.queries.beliefAt === 'function') {
      return result.queries.beliefAt(timestamp);
    }
    return null;
  }

  /**
   * List all recommendations generated across the experiment.
   *
   * @param {object} result
   * @returns {object[]}
   */
  recommendations(result) {
    if (result.queries && typeof result.queries.showEveryRecommendation === 'function') {
      return result.queries.showEveryRecommendation();
    }
    return (result.recommendations || []).map((rec, index) => ({
      index,
      id: rec.id,
      recommendedAction: rec.recommendedAction,
      score: rec.score,
      confidence: rec.confidence,
    }));
  }

  /**
   * Diff observation sets between two experiments (id-level).
   *
   * @param {import('./Experiment').Experiment} left
   * @param {import('./Experiment').Experiment} right
   * @returns {object}
   */
  observationDiff(left, right) {
    const leftIds = new Set(left.getObservations().map((o) => o.id));
    const rightIds = new Set(right.getObservations().map((o) => o.id));
    return {
      leftOnly: [...leftIds].filter((id) => !rightIds.has(id)).sort(),
      rightOnly: [...rightIds].filter((id) => !leftIds.has(id)).sort(),
      shared: [...leftIds].filter((id) => rightIds.has(id)).sort(),
    };
  }
}

/**
 * @param {unknown} claims
 * @returns {string[]}
 */
function extractClaimIds(claims) {
  return flattenClaims(claims)
    .map((c) => c.id || c.claimType || c.strategy)
    .filter(Boolean)
    .map(String);
}

/**
 * @param {unknown} claims
 * @returns {object[]}
 */
function flattenClaims(claims) {
  if (!claims) return [];
  if (Array.isArray(claims)) return claims.filter(Boolean);
  if (typeof claims === 'object') {
    const lists = [
      claims.derived,
      claims.results,
      claims.observations,
      claims.graph,
      claims.items,
    ];
    return lists.flatMap((list) => (Array.isArray(list) ? list : []));
  }
  return [];
}

/**
 * @param {object} result
 * @param {'supporting'|'contradicting'} kind
 * @returns {object[]}
 */
function collectEvidenceRefs(result, kind) {
  const explanation = result.explanation || {};
  const key =
    kind === 'supporting'
      ? explanation.supportingEvidence || explanation.supportingSignals || []
      : explanation.contradictingEvidence || explanation.opposingSignals || [];
  return Array.isArray(key) ? key : [];
}

/**
 * @returns {EvidenceQuery}
 */
function createEvidenceQuery() {
  return new EvidenceQuery();
}

module.exports = {
  EvidenceQuery,
  createEvidenceQuery,
  extractClaimIds,
  flattenClaims,
};
