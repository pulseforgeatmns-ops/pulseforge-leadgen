'use strict';

/**
 * Knowledge handoff for Business Signals (SPEC-031).
 * Verified/Active → evidence · uncertain Detected/Unknown → inference (or omit).
 */

const { SIGNAL_LIFECYCLE, SIGNAL_CONFIDENCE } = require('./types');

/**
 * @param {object[]} signals
 * @returns {object[]} KnowledgeWrite[]
 */
function buildKnowledgeWrites(signals) {
  const writes = [];
  for (const signal of Array.isArray(signals) ? signals : []) {
    if (!signal || !signal.type) continue;
    const subject = signal.companyId || signal.prospectId || 'company';
    const claim = `${signal.title}: ${signal.description}`.trim();
    const refs = Array.isArray(signal.evidenceRefs) ? signal.evidenceRefs : [];

    if (
      signal.lifecycle === SIGNAL_LIFECYCLE.ACTIVE ||
      signal.lifecycle === SIGNAL_LIFECYCLE.DECAYING ||
      signal.lifecycle === SIGNAL_LIFECYCLE.VERIFIED ||
      signal.lifecycle === SIGNAL_LIFECYCLE.ARCHIVED
    ) {
      if (signal.confidence === SIGNAL_CONFIDENCE.UNKNOWN) continue;
      writes.push({
        kind: 'evidence',
        subject: String(subject),
        claim,
        confidence: Number(signal.confidenceScore) || 0,
        evidenceRefs: refs,
        signalId: signal.id,
        signalType: signal.type,
        lifecycle: signal.lifecycle,
      });
      continue;
    }

    if (signal.lifecycle === SIGNAL_LIFECYCLE.DETECTED) {
      writes.push({
        kind: 'inference',
        subject: String(subject),
        claim,
        confidence: Number(signal.confidenceScore) || 0,
        evidenceRefs: refs,
        signalId: signal.id,
        signalType: signal.type,
        lifecycle: signal.lifecycle,
      });
    }
  }
  return writes;
}

module.exports = {
  buildKnowledgeWrites,
};
