'use strict';

/**
 * SPEC-141 Stage 5 — Evidence Collection.
 * Each candidate receives evidence from assigned providers.
 */

const { fuseCandidateEvidence } = require('./EvidenceFusion');

/**
 * Collect and fuse evidence for each candidate in the universe.
 *
 * @param {object} input
 * @returns {object}
 */
function collectEvidence(input = {}) {
  const candidates = input.candidateUniverse.candidates || input.candidateUniverse.resolved || [];
  const providerStrategy = input.providerStrategy || { assignments: [] };

  const evidenceByCandidate = candidates.map((candidate) => {
    const fused = fuseCandidateEvidence(candidate, candidate.evidence || []);
    const assignedProviders = providerStrategy.providers || [];
    return {
      ...fused,
      assignedProviders,
      sufficient: fused.confidence >= 0.5 || fused.evidence.length >= 2,
    };
  });

  const withEvidence = evidenceByCandidate.filter((e) => e.evidence.length > 0).length;
  const avgConfidence =
    evidenceByCandidate.length > 0
      ? Number(
          (
            evidenceByCandidate.reduce((sum, e) => sum + e.confidence, 0) /
            evidenceByCandidate.length
          ).toFixed(2)
        )
      : 0;

  return {
    evidenceByCandidate,
    withEvidence,
    avgConfidence,
    sourcesUsed: [
      ...new Set(
        evidenceByCandidate.flatMap((e) => e.sources || [])
      ),
    ],
  };
}

module.exports = {
  collectEvidence,
};
