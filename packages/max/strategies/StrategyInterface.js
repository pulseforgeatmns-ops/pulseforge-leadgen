'use strict';

const { clamp, round, evidenceRef } = require('../reasoning/ReasoningTypes');

/**
 * @typedef {object} ReasoningStrategy
 * @property {string} id
 * @property {string} name
 * @property {(context: import('../reasoning/ReasoningTypes').ReasoningContext) => import('../reasoning/ReasoningTypes').StrategyResult} evaluate
 */

/**
 * Build a StrategyResult with clamped fields and sorted evidence.
 * @param {object} input
 * @returns {import('../reasoning/ReasoningTypes').StrategyResult}
 */
function strategyResult(input) {
  const supporting = sortRefs(input.supportingEvidence || []);
  const contradicting = sortRefs(input.contradictingEvidence || []);
  const claims = [...new Set(input.claims || [])].map(String).sort();
  return {
    strategy: String(input.strategy),
    scoreDelta: round(clamp(input.scoreDelta == null ? 0 : input.scoreDelta, -100, 100)),
    confidence: round(clamp(input.confidence == null ? 0 : input.confidence, 0, 100)),
    supportingEvidence: supporting,
    contradictingEvidence: contradicting,
    claims,
    summary: String(input.summary || ''),
  };
}

/**
 * Confidence from evidence count + average graph confidence (0–1 → 0–100).
 * Independent of scoreDelta.
 * @param {import('../reasoning/ReasoningTypes').EvidenceRef[]} refs
 * @param {{ base?: number, perItem?: number, max?: number }} [opts]
 */
function confidenceFromEvidence(refs, opts = {}) {
  const list = refs || [];
  if (list.length === 0) return opts.base == null ? 15 : opts.base;
  const perItem = opts.perItem == null ? 12 : opts.perItem;
  const max = opts.max == null ? 97 : opts.max;
  const fromCount = Math.min(list.length * perItem, max * 0.7);
  const confidences = list
    .map((r) => r.confidence)
    .filter((c) => c != null && Number.isFinite(Number(c)))
    .map((c) => {
      const n = Number(c);
      return n <= 1 ? n * 100 : n;
    });
  const avg =
    confidences.length === 0
      ? 40
      : confidences.reduce((a, b) => a + b, 0) / confidences.length;
  return round(clamp(fromCount * 0.5 + avg * 0.5, 0, max));
}

/**
 * @param {import('../reasoning/ReasoningTypes').EvidenceRef[]} refs
 */
function sortRefs(refs) {
  return [...refs].sort((a, b) => {
    const c = String(a.id).localeCompare(String(b.id));
    if (c !== 0) return c;
    return String(a.summary).localeCompare(String(b.summary));
  });
}

/**
 * Assert a strategy implements the required interface.
 * @param {ReasoningStrategy} strategy
 */
function assertStrategy(strategy) {
  if (!strategy || typeof strategy !== 'object') {
    throw new Error('Strategy must be an object');
  }
  if (!strategy.id) throw new Error('Strategy requires id');
  if (!strategy.name) throw new Error('Strategy requires name');
  if (typeof strategy.evaluate !== 'function') {
    throw new Error(`Strategy ${strategy.id} requires evaluate(context)`);
  }
}

module.exports = {
  strategyResult,
  confidenceFromEvidence,
  evidenceRef,
  assertStrategy,
  sortRefs,
};
