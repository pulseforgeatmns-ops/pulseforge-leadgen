'use strict';

const { OUTCOME_VERDICTS } = require('./types');

/**
 * BeliefTracker — track every Claim over time (SPEC-021).
 *
 * Aggregates occurrences / correct / incorrect / accuracy.
 * Never mutates source claims or outcomes.
 */
class BeliefTracker {
  constructor() {
    /** @type {Map<string, BeliefBucket>} */
    this._byClaim = new Map();
    /** @type {Map<string, BeliefBucket>} */
    this._byStrategy = new Map();
    /** @type {import('./types').EvaluationRecord[]} */
    this._history = [];
  }

  /**
   * Record an evaluation against a claim belief.
   * @param {import('./types').EvaluationRecord} evaluation
   * @returns {import('./types').BeliefStats}
   */
  record(evaluation) {
    if (!evaluation || !evaluation.claimId) {
      throw new Error('BeliefTracker.record requires evaluation.claimId');
    }

    // Clone into immutable history — never mutate the caller's object.
    const frozen = Object.freeze({ ...evaluation });
    this._history.push(frozen);

    const claimKey = String(evaluation.claimId);
    const claimBucket = this._ensure(this._byClaim, claimKey, {
      claimId: claimKey,
      claimType: evaluation.claimType || null,
      label: evaluation.claimType || claimKey,
    });
    applyEvaluation(claimBucket, evaluation);

    if (evaluation.strategyPack) {
      const packKey = String(evaluation.strategyPack);
      const packBucket = this._ensure(this._byStrategy, packKey, {
        claimId: packKey,
        claimType: null,
        label: packKey,
        strategyPack: packKey,
      });
      applyEvaluation(packBucket, evaluation);
    }

    return this.statsFor(claimKey);
  }

  /**
   * @param {import('./types').EvaluationRecord[]} evaluations
   */
  recordMany(evaluations) {
    return (evaluations || []).map((e) => this.record(e));
  }

  /**
   * @param {string} claimId
   * @returns {import('./types').BeliefStats|null}
   */
  statsFor(claimId) {
    const bucket = this._byClaim.get(String(claimId));
    return bucket ? freezeStats(bucket) : null;
  }

  /**
   * @param {string} strategyPack
   * @returns {import('./types').BeliefStats|null}
   */
  statsForStrategy(strategyPack) {
    const bucket = this._byStrategy.get(String(strategyPack));
    return bucket ? freezeStats(bucket) : null;
  }

  /**
   * @returns {import('./types').BeliefStats[]}
   */
  listClaims() {
    return [...this._byClaim.values()].map(freezeStats);
  }

  /**
   * @returns {import('./types').BeliefStats[]}
   */
  listStrategies() {
    return [...this._byStrategy.values()].map(freezeStats);
  }

  /**
   * Immutable copy of evaluation history.
   * @returns {import('./types').EvaluationRecord[]}
   */
  history() {
    return this._history.slice();
  }

  /**
   * Snapshot of all belief stats (does not expose mutable buckets).
   */
  snapshot() {
    return Object.freeze({
      claims: Object.freeze(this.listClaims()),
      strategies: Object.freeze(this.listStrategies()),
      evaluations: Object.freeze(this.history()),
      mutatesHistory: false,
    });
  }

  /**
   * @param {Map<string, BeliefBucket>} map
   * @param {string} key
   * @param {object} seed
   */
  _ensure(map, key, seed) {
    let bucket = map.get(key);
    if (!bucket) {
      bucket = {
        claimId: seed.claimId,
        claimType: seed.claimType || null,
        label: seed.label || seed.claimId,
        strategyPack: seed.strategyPack || null,
        occurrences: 0,
        correct: 0,
        incorrect: 0,
        partiallyCorrect: 0,
        unresolved: 0,
        truePositives: 0,
        falsePositives: 0,
        falseNegatives: 0,
        creditSum: 0,
        resolvedCount: 0,
      };
      map.set(key, bucket);
    }
    return bucket;
  }
}

/**
 * @typedef {object} BeliefBucket
 * @property {string} claimId
 * @property {string|null} claimType
 * @property {string} label
 * @property {string|null} [strategyPack]
 * @property {number} occurrences
 * @property {number} correct
 * @property {number} incorrect
 * @property {number} partiallyCorrect
 * @property {number} unresolved
 * @property {number} truePositives
 * @property {number} falsePositives
 * @property {number} falseNegatives
 * @property {number} creditSum
 * @property {number} resolvedCount
 */

/**
 * @param {BeliefBucket} bucket
 * @param {import('./types').EvaluationRecord} evaluation
 */
function applyEvaluation(bucket, evaluation) {
  bucket.occurrences += 1;

  switch (evaluation.verdict) {
    case OUTCOME_VERDICTS.CORRECT:
      bucket.correct += 1;
      bucket.truePositives += 1;
      bucket.creditSum += 1;
      bucket.resolvedCount += 1;
      break;
    case OUTCOME_VERDICTS.INCORRECT:
      bucket.incorrect += 1;
      bucket.falsePositives += 1;
      bucket.resolvedCount += 1;
      break;
    case OUTCOME_VERDICTS.PARTIALLY_CORRECT: {
      bucket.partiallyCorrect += 1;
      const credit =
        evaluation.credit != null && Number.isFinite(Number(evaluation.credit))
          ? Number(evaluation.credit)
          : 0.5;
      bucket.creditSum += credit;
      bucket.resolvedCount += 1;
      // Partial counts toward precision numerator proportionally via creditSum;
      // for discrete TP/FP we treat ≥0.5 as TP-ish contribution.
      if (credit >= 0.5) bucket.truePositives += 1;
      else bucket.falsePositives += 1;
      break;
    }
    case OUTCOME_VERDICTS.UNRESOLVED:
    default:
      bucket.unresolved += 1;
      break;
  }

  // Optional explicit false-negative signal on the evaluation.
  if (evaluation.falseNegative === true) {
    bucket.falseNegatives += 1;
  }
}

/**
 * @param {BeliefBucket} bucket
 * @returns {import('./types').BeliefStats}
 */
function freezeStats(bucket) {
  const accuracy =
    bucket.resolvedCount === 0
      ? null
      : round4(bucket.creditSum / bucket.resolvedCount);

  const precisionDenom = bucket.truePositives + bucket.falsePositives;
  const precision =
    precisionDenom === 0 ? null : round4(bucket.truePositives / precisionDenom);

  // Recall only when false-negatives were explicitly tracked (otherwise n/a).
  const recall =
    bucket.falseNegatives > 0
      ? round4(
          bucket.truePositives /
            (bucket.truePositives + bucket.falseNegatives)
        )
      : null;

  return Object.freeze({
    claimId: bucket.claimId,
    claimType: bucket.claimType,
    label: bucket.label,
    strategyPack: bucket.strategyPack || null,
    occurrences: bucket.occurrences,
    correct: bucket.correct,
    incorrect: bucket.incorrect,
    partiallyCorrect: bucket.partiallyCorrect,
    unresolved: bucket.unresolved,
    accuracy,
    precision,
    recall,
    historicalCalibration: accuracy,
    truePositives: bucket.truePositives,
    falsePositives: bucket.falsePositives,
    falseNegatives: bucket.falseNegatives,
  });
}

function round4(n) {
  return Math.round(n * 10000) / 10000;
}

/**
 * @returns {BeliefTracker}
 */
function createBeliefTracker() {
  return new BeliefTracker();
}

module.exports = {
  BeliefTracker,
  createBeliefTracker,
  freezeStats,
};
