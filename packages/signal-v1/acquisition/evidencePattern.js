'use strict';

const { createHash } = require('crypto');

const EVIDENCE_PATTERNS = Object.freeze(['dual_cluster_run', 'single_cluster_fade']);

/**
 * Evidence generation pattern — MUST NOT depend on selectionCategory or market outcome labels.
 *
 * @param {string} tokenAddress
 * @param {number} [catalogIndex]
 */
function deriveEvidencePattern(tokenAddress, catalogIndex = 0) {
  const digest = createHash('sha256')
    .update(`signal-v1-evidence-pattern:v1:${tokenAddress}:${catalogIndex}`)
    .digest('hex');
  const slot = parseInt(digest.slice(0, 8), 16);
  return EVIDENCE_PATTERNS[slot % EVIDENCE_PATTERNS.length];
}

module.exports = {
  EVIDENCE_PATTERNS,
  deriveEvidencePattern,
};
