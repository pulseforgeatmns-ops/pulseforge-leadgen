'use strict';

const { clampConfidence } = require('../nodes/Evidence');

/**
 * Confidence helpers for evidence and claims.
 *
 * Strategy (v0.7.1):
 * - Single evidence: use its confidence
 * - Multiple independent evidence: 1 - Π(1 - c_i) (noisy-OR style combine)
 * - Soft floor/ceiling via clamp
 */

/**
 * @param {number[]} confidences
 * @returns {number}
 */
function combineConfidences(confidences) {
  const values = (confidences || [])
    .map((c) => clampConfidence(c))
    .filter((c) => Number.isFinite(c));
  if (values.length === 0) return 0;
  if (values.length === 1) return values[0];
  const combined = 1 - values.reduce((acc, c) => acc * (1 - c), 1);
  return clampConfidence(combined);
}

/**
 * @param {Array<{ confidence?: number }>} evidenceNodes
 * @returns {{ confidence: number, reason: string, components: Array<{ id?: string, confidence: number }> }}
 */
function calculateConfidenceFromEvidence(evidenceNodes) {
  const components = (evidenceNodes || []).map((e) => ({
    id: e.id,
    confidence: clampConfidence(e.confidence),
    sourceType: e.sourceType || null,
  }));
  const confidence = combineConfidences(components.map((c) => c.confidence));
  const reason =
    components.length === 0
      ? 'No supporting evidence'
      : components.length === 1
        ? `Single evidence source (${components[0].sourceType || 'unknown'}) at ${components[0].confidence.toFixed(2)}`
        : `Combined ${components.length} evidence sources via noisy-OR to ${confidence.toFixed(2)}`;
  return { confidence, reason, components };
}

module.exports = {
  combineConfidences,
  calculateConfidenceFromEvidence,
  clampConfidence,
};
