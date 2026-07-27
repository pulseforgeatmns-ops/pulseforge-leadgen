'use strict';

/**
 * Signal collection orchestration (SPEC-031).
 * Collectors only transform evidenced inputs — never invent observations.
 */

const {
  collectMultiLocation,
  collectCommercialFootprint,
} = require('./providers/placesSignals');
const { collectHiring, collectExpansion } = require('./providers/hiringSignals');
const { collectWebsiteSignals } = require('./providers/websiteSignals');

const COLLECTORS = [
  collectMultiLocation,
  collectCommercialFootprint,
  collectHiring,
  collectExpansion,
  collectWebsiteSignals,
];

/**
 * Collect Detected candidates from a prospect (+ optional knowledge).
 * Also accepts pre-structured businessSignals on the prospect (passthrough).
 *
 * @param {object} prospect
 * @param {object} [ctx]
 * @returns {object[]}
 */
function collectSignals(prospect, ctx = {}) {
  if (!prospect || typeof prospect !== 'object') return [];

  const collected = [];
  for (const collector of COLLECTORS) {
    const batch = collector(prospect, ctx) || [];
    for (const signal of batch) {
      if (signal && Array.isArray(signal.evidence) && signal.evidence.length > 0) {
        collected.push(signal);
      }
    }
  }

  // Passthrough already-structured signals (e.g. from Company Intelligence)
  const existing = [
    ...(Array.isArray(prospect.businessSignals) ? prospect.businessSignals : []),
    ...(Array.isArray(ctx.businessSignals) ? ctx.businessSignals : []),
  ];
  for (const raw of existing) {
    if (!raw || typeof raw !== 'object') continue;
    if (!Array.isArray(raw.evidence) || raw.evidence.length === 0) continue;
    if (!raw.type || !raw.title) continue;
    collected.push(raw);
  }

  return dedupeByType(collected);
}

function dedupeByType(signals) {
  const byKey = new Map();
  for (const s of signals) {
    const key = `${s.prospectId || ''}:${s.type}`;
    const prev = byKey.get(key);
    if (!prev) {
      byKey.set(key, s);
      continue;
    }
    // Prefer higher confidence / more evidence
    const prevN = (prev.evidence && prev.evidence.length) || 0;
    const nextN = (s.evidence && s.evidence.length) || 0;
    if (nextN > prevN || (s.confidenceScore || 0) > (prev.confidenceScore || 0)) {
      byKey.set(key, s);
    }
  }
  return [...byKey.values()];
}

module.exports = {
  collectSignals,
  COLLECTORS,
};
