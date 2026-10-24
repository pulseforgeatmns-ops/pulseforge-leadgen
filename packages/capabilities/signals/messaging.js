'use strict';

/**
 * Campaign / Brief messaging from Active Business Signals (SPEC-031).
 */

const {
  postureForSignalType,
  postureDescription,
} = require('./categories');
const { SIGNAL_LIFECYCLE, SIGNAL_CONFIDENCE } = require('./types');

/**
 * @param {object[]} signals
 * @returns {object[]}
 */
function selectActive(signals) {
  return (Array.isArray(signals) ? signals : []).filter(
    (s) =>
      s &&
      (s.lifecycle === SIGNAL_LIFECYCLE.ACTIVE ||
        s.lifecycle === SIGNAL_LIFECYCLE.DECAYING) &&
      Number(s.influenceWeight) > 0 &&
      s.confidence !== SIGNAL_CONFIDENCE.UNKNOWN
  );
}

/**
 * Pick primary messaging posture from Active signals (highest influence).
 * @param {object[]} signals
 * @returns {{ posture: string|null, description: string, drivingSignal: object|null }}
 */
function messagingPostureFromSignals(signals) {
  const active = selectActive(signals).sort(
    (a, b) => Number(b.influenceWeight) - Number(a.influenceWeight)
  );
  for (const signal of active) {
    const posture = postureForSignalType(signal.type);
    if (posture) {
      return {
        posture,
        description: postureDescription(posture),
        drivingSignal: signal,
      };
    }
  }
  return { posture: null, description: '', drivingSignal: null };
}

/**
 * Operator-facing Active Business Signals list.
 * @param {object[]} signals
 * @returns {object[]}
 */
function activeSignalsForOperator(signals) {
  return selectActive(signals)
    .sort((a, b) => Number(b.influenceWeight) - Number(a.influenceWeight))
    .map((s) => ({
      id: s.id,
      type: s.type,
      category: s.category,
      title: s.title,
      description: s.description,
      confidence: s.confidence,
      confidenceScore: s.confidenceScore,
      influenceWeight: s.influenceWeight,
      lifecycle: s.lifecycle,
      observedAt: s.observedAt,
      evidenceRefs: s.evidenceRefs,
    }));
}

/**
 * Buying + timing-relevant Active signals for Ranking (SPEC-026 additive field).
 * @param {object[]} signals
 * @returns {object[]}
 */
function buyingSignalsForRanking(signals) {
  const active = selectActive(signals);
  return active.filter((s) => {
    if (s.category === 'buying' || s.category === 'growth') return true;
    const t = String(s.type || '');
    return /hir|expans|location|lease|renovat|acquisit/i.test(t);
  });
}

module.exports = {
  selectActive,
  messagingPostureFromSignals,
  activeSignalsForOperator,
  buyingSignalsForRanking,
};
