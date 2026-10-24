'use strict';

const { createReplayComparator } = require('@pulseforge/replay');

/**
 * ComparisonWorkspace — side-by-side laboratory comparison (SPEC-019).
 *
 * Holds paired experiment results and surfaces structured diffs.
 * Ephemeral. Never persisted. Never mutates production.
 */
class ComparisonWorkspace {
  /**
   * @param {object} [deps]
   * @param {{ compare: (left: object, right: object) => object }} [deps.comparator]
   */
  constructor(deps = {}) {
    this._comparator =
      deps.comparator || createReplayComparator();
    /** @type {Map<string, object>} */
    this._pairs = new Map();
  }

  /**
   * Register a named comparison of two laboratory / replay results.
   *
   * @param {string} name
   * @param {object} left
   * @param {object} right
   * @param {object} [meta]
   * @returns {object}
   */
  add(name, left, right, meta = {}) {
    if (!name) throw new Error('ComparisonWorkspace.add requires a name');
    if (!left || !right) {
      throw new Error('ComparisonWorkspace.add requires left and right results');
    }

    const comparison = this._comparator.compare(left, right);
    const entry = Object.freeze({
      name: String(name),
      meta: Object.freeze({ ...meta }),
      left: summarizeResult(left),
      right: summarizeResult(right),
      comparison,
      sideBySide: buildSideBySide(left, right, comparison),
      identical: Boolean(comparison.identical),
      createdAt: new Date().toISOString(),
    });

    this._pairs.set(String(name), entry);
    return entry;
  }

  /**
   * @param {string} name
   * @returns {object|null}
   */
  get(name) {
    return this._pairs.get(String(name)) || null;
  }

  /**
   * @returns {object[]}
   */
  list() {
    return [...this._pairs.values()];
  }

  /**
   * Clear all comparisons (dispose workspace).
   */
  clear() {
    this._pairs.clear();
  }

  /**
   * Compare two results without registering.
   *
   * @param {object} left
   * @param {object} right
   * @returns {object}
   */
  compare(left, right) {
    return this._comparator.compare(left, right);
  }
}

/**
 * @param {object} result
 */
function summarizeResult(result) {
  const finalRec =
    (result.recommendations &&
      result.recommendations[result.recommendations.length - 1]) ||
    null;
  return {
    experimentId: result.experimentId || null,
    subjectId: result.subjectId,
    confidence: result.confidence,
    observationCount: (result.observations || []).length,
    recommendationAction: finalRec ? finalRec.recommendedAction : null,
    recommendationScore: finalRec ? finalRec.score : null,
    versions: result.versions || null,
    fingerprint: result.fingerprint || result.replayFingerprint || null,
  };
}

/**
 * Operator-friendly side-by-side board.
 *
 * @param {object} left
 * @param {object} right
 * @param {object} comparison
 */
function buildSideBySide(left, right, comparison) {
  const leftRec =
    (left.recommendations &&
      left.recommendations[left.recommendations.length - 1]) ||
    null;
  const rightRec =
    (right.recommendations &&
      right.recommendations[right.recommendations.length - 1]) ||
    null;

  return {
    confidence: {
      left: left.confidence,
      right: right.confidence,
      delta: comparison.confidenceDifferences
        ? comparison.confidenceDifferences.delta
        : null,
    },
    recommendation: {
      left: leftRec
        ? {
            action: leftRec.recommendedAction,
            score: leftRec.score,
            confidence: leftRec.confidence,
          }
        : null,
      right: rightRec
        ? {
            action: rightRec.recommendedAction,
            score: rightRec.score,
            confidence: rightRec.confidence,
          }
        : null,
      actionChanged: comparison.recommendationDifferences
        ? comparison.recommendationDifferences.actionChanged
        : null,
    },
    claims: comparison.claimDifferences || null,
    reasoning: comparison.reasoningDifferences || null,
    versions: comparison.versions || {
      left: left.versions || null,
      right: right.versions || null,
    },
  };
}

/**
 * @param {object} [deps]
 * @returns {ComparisonWorkspace}
 */
function createComparisonWorkspace(deps) {
  return new ComparisonWorkspace(deps);
}

module.exports = {
  ComparisonWorkspace,
  createComparisonWorkspace,
  summarizeResult,
  buildSideBySide,
};
