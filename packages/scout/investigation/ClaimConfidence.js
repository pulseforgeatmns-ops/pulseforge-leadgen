'use strict';

/**
 * SPEC-142 — Claim Confidence.
 * Confidence belongs to claims, not providers. Provider trust is one input.
 */

const { buildClaim } = require('./types');
const { fuseCandidateEvidence } = require('../intelligence/EvidenceFusion');
const { HYPOTHESIS_STATUS } = require('./types');
const { evidenceSatisfiesGap } = require('./MissingEvidence');

function computeClaimConfidence(supportedEvidence = [], contradictions = []) {
  if (supportedEvidence.length === 0) return 0;

  const weights = supportedEvidence.map((e) => {
    if (typeof e === 'object' && e.weight != null) return e.weight;
    if (typeof e === 'object' && e.confidence != null) return e.confidence;
    return 0.65;
  });

  const sourceCount = new Set(
    supportedEvidence.map((e) => (typeof e === 'string' ? e : e.source || e.evidenceType))
  ).size;

  let confidence =
    weights.reduce((sum, w) => sum + w, 0) / weights.length + Math.min(0.12, sourceCount * 0.03);

  for (const conflict of contradictions) {
    const penalty = conflict.resolved ? 0.05 : 0.15;
    confidence -= penalty;
  }

  return Number(Math.max(0, Math.min(0.98, confidence)).toFixed(2));
}

/**
 * Derive claims from fused evidence for a candidate.
 * @param {object} candidate
 * @param {object} fusedEvidence
 * @param {object[]} hypotheses
 * @returns {object[]}
 */
function deriveClaimsFromEvidence(candidate, fusedEvidence, hypotheses = []) {
  const claims = [];
  const evidence = fusedEvidence.evidence || [];

  if (evidence.length > 0) {
    claims.push(
      buildClaim({
        id: `claim-evidence:${candidate.id}`,
        entityId: candidate.id,
        text: `${candidate.name} has verifiable business evidence from ${fusedEvidence.sources.length} source(s).`,
        confidence: fusedEvidence.confidence || 0.2,
        supportedBy: evidence.map((e) => ({
          source: e.source,
          evidenceId: e.id,
          label: e.label,
          weight: e.weight,
        })),
      })
    );
  }

  for (const hyp of hypotheses.filter((h) => !h.entityId || h.entityId === candidate.id)) {
    const collected = evidence.filter((e) =>
      (hyp.requiredEvidence || []).some((req) =>
        String(e.source || e.kind || '')
          .toLowerCase()
          .includes(String(req).toLowerCase().replace(/_/g, ''))
      )
    );

    const supportedBy =
      collected.length > 0
        ? collected.map((e) => ({ source: e.source, evidenceId: e.id, label: e.label, weight: e.weight }))
        : evidence.slice(0, 2).map((e) => ({ source: e.source, evidenceId: e.id, label: e.label, weight: e.weight }));

    const missing = (hyp.requiredEvidence || []).filter(
      (req) =>
        !evidence.some((e) =>
          String(e.source || e.kind || '')
            .toLowerCase()
            .includes(String(req).toLowerCase().replace(/_/g, ''))
        )
    );

    const confidence = collected.length > 0 ? computeClaimConfidence(supportedBy) : supportedBy.length > 0 ? computeClaimConfidence(supportedBy) * 0.6 : 0.2;

    claims.push(
      buildClaim({
        id: `claim:${hyp.id}`,
        entityId: candidate.id,
        hypothesisId: hyp.id,
        text: hyp.text.replace(`${candidate.name}: `, ''),
        confidence,
        supportedBy,
        missingEvidence: missing,
      })
    );
  }

  return claims;
}

/**
 * Update hypothesis status from claim confidence.
 * @param {object} hypothesis
 * @param {object} claim
 * @returns {object}
 */
function updateHypothesisFromClaim(hypothesis, claim) {
  const updated = { ...hypothesis, confidence: claim.confidence, collectedEvidence: claim.supportedBy };

  if (claim.confidence >= hypothesis.minConfidence && (claim.missingEvidence || []).length === 0) {
    updated.status = HYPOTHESIS_STATUS.CONFIRMED;
    updated.missingEvidence = [];
  } else if (claim.confidence >= hypothesis.minConfidence * 0.5) {
    updated.status = HYPOTHESIS_STATUS.OPEN;
    updated.missingEvidence = claim.missingEvidence || [];
  } else if (claim.confidence < 0.3 && (claim.supportedBy || []).length === 0) {
    updated.status = HYPOTHESIS_STATUS.INCONCLUSIVE;
    updated.missingEvidence = hypothesis.requiredEvidence || [];
  } else {
    updated.status = HYPOTHESIS_STATUS.OPEN;
    updated.missingEvidence = claim.missingEvidence || hypothesis.requiredEvidence || [];
  }

  updated.claimIds = [...(hypothesis.claimIds || []), claim.id].filter(Boolean);
  return updated;
}

/**
 * Fuse evidence and update all claims for a candidate.
 * @param {object} candidate
 * @param {object[]} hypotheses
 * @param {object[]} existingClaims
 * @param {object[]} contradictions
 * @returns {object}
 */
function fuseAndUpdateClaims(candidate, hypotheses, existingClaims = [], contradictions = []) {
  const fused = fuseCandidateEvidence(candidate, candidate.evidence || []);
  let claims = deriveClaimsFromEvidence(candidate, fused, hypotheses);

  for (const claim of claims) {
    const relevantConflicts = contradictions.filter(
      (c) => c.entityId === candidate.id || c.claimId === claim.id
    );
    claim.contradictions = relevantConflicts;
    claim.confidence = computeClaimConfidence(claim.supportedBy, relevantConflicts);
  }

  const updatedHypotheses = hypotheses.map((hyp) => {
    const claim = claims.find((c) => c.hypothesisId === hyp.id);
    return claim ? updateHypothesisFromClaim(hyp, claim) : hyp;
  });

  return { fused, claims, hypotheses: updatedHypotheses };
}

module.exports = {
  computeClaimConfidence,
  deriveClaimsFromEvidence,
  updateHypothesisFromClaim,
  fuseAndUpdateClaims,
};
