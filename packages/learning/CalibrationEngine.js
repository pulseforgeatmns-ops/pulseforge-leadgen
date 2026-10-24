'use strict';

const { DEFAULT_BLEND_WEIGHT } = require('./types');
const { normalizeConfidence } = require('./OutcomeEvaluator');

/**
 * CalibrationEngine — keep calibration separate from confidence (SPEC-021).
 *
 * Confidence = runtime belief.
 * Calibration = historical performance.
 * Adjusted confidence = deterministic blend (no ML).
 *
 * Never mutates runtime confidence, history, or replay.
 */
class CalibrationEngine {
  /**
   * @param {object} [opts]
   * @param {number} [opts.blendWeight=0.5] - weight on runtime confidence (rest on calibration)
   * @param {number} [opts.minSamples=1] - minimum resolved outcomes before adjusting
   */
  constructor(opts = {}) {
    this._blendWeight =
      opts.blendWeight != null ? clamp01(Number(opts.blendWeight)) : DEFAULT_BLEND_WEIGHT;
    this._minSamples =
      opts.minSamples != null ? Math.max(0, Number(opts.minSamples)) : 1;
  }

  /**
   * Calibrate a claim's confidence against historical belief stats.
   *
   * @param {object} args
   * @param {string} args.claimId
   * @param {number|null} [args.confidence] - runtime confidence (0–1 or 0–100)
   * @param {import('./types').BeliefStats|null} args.stats
   * @param {string|null} [args.claimType]
   * @param {unknown[]} [args.observationsConsidered]
   * @param {object|null} [args.outcome]
   * @param {string|null} [args.evaluationExplanation]
   * @returns {import('./types').CalibrationResult}
   */
  calibrate(args = {}) {
    if (!args.claimId) {
      throw new Error('CalibrationEngine.calibrate requires claimId');
    }

    const stats = args.stats || emptyStats(args.claimId, args.claimType);
    const confidence = normalizeConfidence(
      args.confidence != null ? args.confidence : null
    );
    const historicalCalibration =
      stats.historicalCalibration != null
        ? stats.historicalCalibration
        : stats.accuracy;

    const sampleOk =
      (stats.correct || 0) +
        (stats.incorrect || 0) +
        (stats.partiallyCorrect || 0) >=
      this._minSamples;

    let adjustedConfidence = confidence;
    if (sampleOk && confidence != null && historicalCalibration != null) {
      adjustedConfidence = blend(
        confidence,
        historicalCalibration,
        this._blendWeight
      );
    } else if (sampleOk && confidence == null && historicalCalibration != null) {
      adjustedConfidence = historicalCalibration;
    }

    const observationsConsidered = Object.freeze(
      (args.observationsConsidered || []).slice()
    );

    const explanation = Object.freeze({
      observationsConsidered,
      outcome: args.outcome
        ? Object.freeze({ ...(args.outcome || {}) })
        : null,
      historicalStatistics: Object.freeze({
        occurrences: stats.occurrences,
        correct: stats.correct,
        incorrect: stats.incorrect,
        partiallyCorrect: stats.partiallyCorrect,
        unresolved: stats.unresolved,
        accuracy: stats.accuracy,
        precision: stats.precision,
        recall: stats.recall,
        historicalCalibration,
      }),
      confidenceBefore: confidence,
      confidenceAfter: adjustedConfidence,
      blendWeight: this._blendWeight,
      evaluationExplanation: args.evaluationExplanation || null,
      narrative: buildNarrative({
        claimId: args.claimId,
        confidence,
        historicalCalibration,
        adjustedConfidence,
        stats,
        sampleOk,
      }),
    });

    return Object.freeze({
      claimId: String(args.claimId),
      claimType: args.claimType || stats.claimType || null,
      confidence,
      historicalCalibration:
        historicalCalibration != null ? round4(historicalCalibration) : null,
      adjustedConfidence:
        adjustedConfidence != null ? round4(adjustedConfidence) : null,
      blendWeight: this._blendWeight,
      stats: Object.freeze({ ...stats }),
      explanation,
      mutatesHistory: false,
      mutatesReplay: false,
      mutatesRuntime: false,
    });
  }

  /**
   * Report accuracy for a belief scope (claim or strategy pack).
   *
   * @param {object} args
   * @param {'claim'|'strategy_pack'} args.scope
   * @param {string} args.scopeId
   * @param {import('./types').BeliefStats|null} args.stats
   * @param {import('./types').BeliefStats[]} [args.claims]
   * @returns {import('./types').AccuracyReport}
   */
  accuracyReport(args = {}) {
    if (!args.scope || !args.scopeId) {
      throw new Error('CalibrationEngine.accuracyReport requires scope and scopeId');
    }
    const stats = args.stats || emptyStats(args.scopeId, null);
    return Object.freeze({
      scope: args.scope,
      scopeId: String(args.scopeId),
      occurrences: stats.occurrences,
      correct: stats.correct,
      incorrect: stats.incorrect,
      partiallyCorrect: stats.partiallyCorrect,
      unresolved: stats.unresolved,
      accuracy: stats.accuracy,
      precision: stats.precision,
      recall: stats.recall,
      historicalCalibration: stats.historicalCalibration,
      claims: Object.freeze((args.claims || []).map((c) => Object.freeze({ ...c }))),
      explanation: Object.freeze({
        narrative: `${args.scope} "${args.scopeId}": accuracy=${formatPct(
          stats.accuracy
        )}, precision=${formatPct(stats.precision)}, recall=${
          stats.recall == null ? 'n/a' : formatPct(stats.recall)
        } (${stats.correct} correct / ${stats.incorrect} incorrect over ${stats.occurrences} occurrences).`,
        historicalStatistics: Object.freeze({ ...stats }),
      }),
      mutatesHistory: false,
      mutatesReplay: false,
      mutatesRuntime: false,
    });
  }
}

/**
 * adjusted = w * confidence + (1-w) * calibration
 * Example: 0.5*0.82 + 0.5*0.67 = 0.745 → ~74%
 */
function blend(confidence, calibration, weight) {
  const w = clamp01(weight);
  return round4(w * confidence + (1 - w) * calibration);
}

function emptyStats(claimId, claimType) {
  return Object.freeze({
    claimId: String(claimId),
    claimType: claimType || null,
    label: claimType || String(claimId),
    occurrences: 0,
    correct: 0,
    incorrect: 0,
    partiallyCorrect: 0,
    unresolved: 0,
    accuracy: null,
    precision: null,
    recall: null,
    historicalCalibration: null,
  });
}

function buildNarrative({
  claimId,
  confidence,
  historicalCalibration,
  adjustedConfidence,
  stats,
  sampleOk,
}) {
  if (!sampleOk) {
    return `Claim "${claimId}" has insufficient resolved outcomes for calibration (${stats.occurrences} occurrences).`;
  }
  return (
    `Claim "${claimId}": confidence ${formatPct(confidence)} ` +
    `× historical calibration ${formatPct(historicalCalibration)} ` +
    `(${stats.correct} correct / ${stats.incorrect} incorrect, accuracy ${formatPct(
      stats.accuracy
    )}) → adjusted ${formatPct(adjustedConfidence)}. ` +
    `Runtime confidence is unchanged.`
  );
}

function formatPct(value) {
  if (value == null || !Number.isFinite(Number(value))) return 'n/a';
  const n = Number(value);
  const pct = n <= 1 ? n * 100 : n;
  return `${(Math.round(pct * 100) / 100).toFixed(2).replace(/\.?0+$/, '')}%`;
}

function clamp01(n) {
  if (!Number.isFinite(n)) return DEFAULT_BLEND_WEIGHT;
  if (n < 0) return 0;
  if (n > 1) return 1;
  return n;
}

function round4(n) {
  return Math.round(n * 10000) / 10000;
}

/**
 * @param {object} [opts]
 * @returns {CalibrationEngine}
 */
function createCalibrationEngine(opts) {
  return new CalibrationEngine(opts);
}

module.exports = {
  CalibrationEngine,
  createCalibrationEngine,
  blend,
  formatPct,
};
