'use strict';

/**
 * Confidence helpers (SPEC-031).
 */

const {
  SIGNAL_CONFIDENCE,
  CONFIDENCE_SCORE,
  normalizeConfidence,
  clamp01,
} = require('./types');

/**
 * Derive confidence from corroboration count / source quality.
 * High = official/primary · Medium = multiple sources · Low = single indirect · Unknown = insufficient.
 *
 * @param {object} opts
 * @param {number} [opts.sourceCount]
 * @param {boolean} [opts.official]
 * @param {boolean} [opts.primary]
 * @param {boolean} [opts.indirect]
 * @param {number} [opts.evidenceCount]
 * @returns {'high'|'medium'|'low'|'unknown'}
 */
function deriveConfidence(opts = {}) {
  const evidenceCount = Number(opts.evidenceCount) || 0;
  const sourceCount = Number(opts.sourceCount) || evidenceCount;
  if (evidenceCount < 1 && sourceCount < 1) return SIGNAL_CONFIDENCE.UNKNOWN;
  if (opts.official === true || opts.primary === true) {
    return SIGNAL_CONFIDENCE.HIGH;
  }
  if (sourceCount >= 2 || evidenceCount >= 2) {
    return SIGNAL_CONFIDENCE.MEDIUM;
  }
  if (opts.indirect === true || evidenceCount === 1) {
    return SIGNAL_CONFIDENCE.LOW;
  }
  return SIGNAL_CONFIDENCE.UNKNOWN;
}

/**
 * @param {string} confidence
 * @returns {number}
 */
function scoreForConfidence(confidence) {
  const c = normalizeConfidence(confidence);
  return CONFIDENCE_SCORE[c] != null ? CONFIDENCE_SCORE[c] : 0;
}

/**
 * Unknown never becomes Active for ranking influence.
 * @param {string} confidence
 * @returns {boolean}
 */
function canBecomeActive(confidence) {
  const c = normalizeConfidence(confidence);
  return c !== SIGNAL_CONFIDENCE.UNKNOWN && scoreForConfidence(c) > 0;
}

/**
 * Low becomes Active only when Playbook marks the type as preferred.
 * @param {string} confidence
 * @param {object} [opts]
 * @param {boolean} [opts.playbookPreferred]
 * @returns {boolean}
 */
function passesVerificationGate(confidence, opts = {}) {
  const c = normalizeConfidence(confidence);
  if (c === SIGNAL_CONFIDENCE.UNKNOWN) return false;
  if (c === SIGNAL_CONFIDENCE.HIGH || c === SIGNAL_CONFIDENCE.MEDIUM) return true;
  if (c === SIGNAL_CONFIDENCE.LOW) return opts.playbookPreferred === true;
  return false;
}

module.exports = {
  deriveConfidence,
  scoreForConfidence,
  canBecomeActive,
  passesVerificationGate,
  clamp01,
};
