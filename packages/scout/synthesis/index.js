'use strict';

/**
 * SPEC-160 — Evidence Synthesis Engine public exports.
 * ADR-080 — Understanding emerges from evidence.
 */

const {
  UNDERSTANDING_KINDS,
  SYNTHESIS_REVISION_KINDS,
  buildEvidence,
  buildUnderstanding,
  computeUnderstandingConfidence,
} = require('./types');

const {
  normalizeCandidateEvidence,
  resolveEntityGroups,
  detectAssertionsFromEvidence,
  detectContradictions,
  synthesizeEntityUnderstanding,
  reviseUnderstandingWithContradiction,
  explainUnderstanding,
  synthesizeFromCandidates,
  applyEvidenceToUnderstandings,
  buildBusinessUnderstandingReport,
} = require('./EvidenceSynthesisEngine');

module.exports = {
  UNDERSTANDING_KINDS,
  SYNTHESIS_REVISION_KINDS,
  buildEvidence,
  buildUnderstanding,
  computeUnderstandingConfidence,
  normalizeCandidateEvidence,
  resolveEntityGroups,
  detectAssertionsFromEvidence,
  detectContradictions,
  synthesizeEntityUnderstanding,
  reviseUnderstandingWithContradiction,
  explainUnderstanding,
  synthesizeFromCandidates,
  applyEvidenceToUnderstandings,
  buildBusinessUnderstandingReport,
};
