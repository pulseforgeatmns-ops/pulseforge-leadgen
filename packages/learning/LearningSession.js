'use strict';

const { randomUUID } = require('crypto');
const { LEARNING_RULES } = require('./types');
const { createBeliefTracker } = require('./BeliefTracker');
const { createCalibrationEngine } = require('./CalibrationEngine');
const { createOutcomeEvaluator } = require('./OutcomeEvaluator');

/**
 * LearningSession — isolated learning run over a frozen claim/outcome set (SPEC-021).
 *
 * Copy-on-write inputs. Produces calibration overlays only.
 * Never mutates history, replay, or runtime.
 */
class LearningSession {
  /**
   * @param {object} [seed]
   * @param {string} [seed.id]
   * @param {string} [seed.name]
   * @param {object[]} [seed.claims]
   * @param {object[]} [seed.evidence]
   * @param {object[]} [seed.outcomes]
   * @param {object[]} [seed.observations]
   * @param {string} [seed.strategyPack]
   * @param {object} [seed.metadata]
   * @param {object} [deps]
   * @param {import('./BeliefTracker').BeliefTracker} [deps.beliefTracker]
   * @param {import('./CalibrationEngine').CalibrationEngine} [deps.calibrationEngine]
   * @param {import('./OutcomeEvaluator').OutcomeEvaluator} [deps.outcomeEvaluator]
   */
  constructor(seed = {}, deps = {}) {
    this.id = seed.id || randomUUID();
    this.name = seed.name || `learning:${this.id.slice(0, 8)}`;
    this.strategyPack =
      seed.strategyPack != null ? String(seed.strategyPack) : null;
    this.metadata = Object.freeze({ ...(seed.metadata || {}) });

    this._claims = freezeList(seed.claims);
    this._evidence = freezeList(seed.evidence);
    this._outcomes = freezeList(seed.outcomes);
    this._observations = freezeList(seed.observations);

    this.beliefs = deps.beliefTracker || createBeliefTracker();
    this.calibration = deps.calibrationEngine || createCalibrationEngine();
    this.evaluator = deps.outcomeEvaluator || createOutcomeEvaluator();

    /** @type {import('./types').EvaluationRecord[]} */
    this._evaluations = [];
    /** @type {import('./types').CalibrationResult[]} */
    this._calibrations = [];

    this.mutatesHistory = false;
    this.mutatesReplay = false;
    this.mutatesRuntime = false;
    this.isIsolated = true;
  }

  /** @returns {object[]} */
  getClaims() {
    return this._claims.slice();
  }

  /** @returns {object[]} */
  getOutcomes() {
    return this._outcomes.slice();
  }

  /** @returns {object[]} */
  getEvidence() {
    return this._evidence.slice();
  }

  /** @returns {object[]} */
  getObservations() {
    return this._observations.slice();
  }

  /**
   * Derive a child session with additional outcomes (copy-on-write).
   * @param {object|object[]} outcomes
   * @param {object} [opts]
   */
  withOutcomes(outcomes, opts = {}) {
    const extra = Array.isArray(outcomes) ? outcomes : [outcomes];
    return new LearningSession(
      {
        name: opts.name || `${this.name}:outcomes`,
        claims: this._claims,
        evidence: this._evidence,
        outcomes: this._outcomes.concat(extra),
        observations: this._observations,
        strategyPack: opts.strategyPack != null ? opts.strategyPack : this.strategyPack,
        metadata: { ...this.metadata, ...(opts.metadata || {}), parentId: this.id },
      },
      {
        beliefTracker: createBeliefTracker(),
        calibrationEngine: this.calibration,
        outcomeEvaluator: this.evaluator,
      }
    );
  }

  /**
   * Run evaluation + belief tracking + calibration for all claim/outcome pairs.
   * @param {object} [opts]
   * @param {number} [opts.blendWeight]
   * @returns {object}
   */
  run(opts = {}) {
    if (opts.blendWeight != null) {
      this.calibration = createCalibrationEngine({
        blendWeight: opts.blendWeight,
      });
    }

    const pairs = pairClaimsAndOutcomes(
      this._claims,
      this._outcomes,
      this.strategyPack
    );

    this._evaluations = [];
    this._calibrations = [];

    for (const pair of pairs) {
      const evaluation = this.evaluator.evaluate(pair);
      this.beliefs.record(evaluation);
      this._evaluations.push(evaluation);

      if (evaluation.verdict === 'unresolved') {
        continue;
      }

      const stats = this.beliefs.statsFor(evaluation.claimId);
      const claim = pair.claim || {};
      const calibration = this.calibration.calibrate({
        claimId: evaluation.claimId,
        claimType: evaluation.claimType,
        confidence: evaluation.confidenceBefore ?? claim.confidence,
        stats,
        observationsConsidered: evaluation.observationsConsidered,
        outcome: pair.outcome,
        evaluationExplanation: evaluation.explanation,
      });
      this._calibrations.push(calibration);
    }

    return this.result();
  }

  /**
   * @returns {object}
   */
  result() {
    const claimStats = this.beliefs.listClaims();
    const strategyStats = this.beliefs.listStrategies();
    const accuracy = this.strategyPack
      ? this.calibration.accuracyReport({
          scope: 'strategy_pack',
          scopeId: this.strategyPack,
          stats: this.beliefs.statsForStrategy(this.strategyPack),
          claims: claimStats,
        })
      : null;

    return Object.freeze({
      sessionId: this.id,
      name: this.name,
      strategyPack: this.strategyPack,
      evaluations: Object.freeze(this._evaluations.slice()),
      calibrations: Object.freeze(this._calibrations.slice()),
      beliefs: Object.freeze(claimStats),
      strategyBeliefs: Object.freeze(strategyStats),
      accuracy,
      claims: this.getClaims(),
      outcomes: this.getOutcomes(),
      evidence: this.getEvidence(),
      observations: this.getObservations(),
      rules: LEARNING_RULES,
      mutatesHistory: false,
      mutatesReplay: false,
      mutatesRuntime: false,
      isolated: true,
    });
  }

  /**
   * Immutable snapshot of session inputs (not results).
   */
  snapshot() {
    return Object.freeze({
      id: this.id,
      name: this.name,
      strategyPack: this.strategyPack,
      claims: this.getClaims(),
      outcomes: this.getOutcomes(),
      evidence: this.getEvidence(),
      observations: this.getObservations(),
      metadata: this.metadata,
      mutatesHistory: false,
      mutatesReplay: false,
      mutatesRuntime: false,
    });
  }
}

/**
 * Pair claims with outcomes by claimId / claimType.
 * Unmatched claims yield unresolved evaluations when an empty outcome is paired.
 *
 * @param {object[]} claims
 * @param {object[]} outcomes
 * @param {string|null} defaultStrategyPack
 */
function pairClaimsAndOutcomes(claims, outcomes, defaultStrategyPack) {
  const claimList = Array.isArray(claims) ? claims : [];
  const outcomeList = Array.isArray(outcomes) ? outcomes : [];

  /** @type {Array<{ claim: object, outcome: object }>} */
  const pairs = [];
  const usedOutcomes = new Set();

  for (const claim of claimList) {
    const claimKey = String(
      claim.id || claim.claimId || claim.claimType || claim.type || ''
    );
    const matches = outcomeList.filter((outcome, idx) => {
      if (usedOutcomes.has(idx)) return false;
      const outKey = String(
        outcome.claimId || outcome.claimType || outcome.claim_id || ''
      );
      return (
        outKey &&
        (outKey === claimKey ||
          outKey === String(claim.claimType || '') ||
          outKey === String(claim.id || ''))
      );
    });

    if (matches.length === 0) {
      // No outcome yet — still track as unresolved if we want full claim coverage.
      // Only emit when outcomes array is non-empty overall? Spec says update after reality.
      // Skip unpaired claims (no reality known).
      continue;
    }

    for (const outcome of matches) {
      const idx = outcomeList.indexOf(outcome);
      if (idx >= 0) usedOutcomes.add(idx);
      pairs.push({
        claim: {
          ...claim,
          strategyPack:
            claim.strategyPack || defaultStrategyPack || outcome.strategyPack || null,
        },
        outcome: {
          ...outcome,
          strategyPack:
            outcome.strategyPack || claim.strategyPack || defaultStrategyPack || null,
        },
      });
    }
  }

  // Outcomes that reference claims not in the claim list still evaluate.
  outcomeList.forEach((outcome, idx) => {
    if (usedOutcomes.has(idx)) return;
    const claimId = outcome.claimId || outcome.claimType;
    if (!claimId) return;
    pairs.push({
      claim: {
        id: String(claimId),
        claimType: outcome.claimType || String(claimId),
        confidence: outcome.confidenceAtClaim ?? outcome.confidence ?? null,
        strategyPack: outcome.strategyPack || defaultStrategyPack || null,
        subjectId: outcome.subjectId || outcome.subject || null,
      },
      outcome: {
        ...outcome,
        strategyPack: outcome.strategyPack || defaultStrategyPack || null,
      },
    });
  });

  return pairs;
}

function freezeList(value) {
  return Object.freeze((Array.isArray(value) ? value : []).map((item) =>
    item && typeof item === 'object' ? Object.freeze({ ...item }) : item
  ));
}

/**
 * @param {object} [seed]
 * @param {object} [deps]
 * @returns {LearningSession}
 */
function createLearningSession(seed, deps) {
  return new LearningSession(seed, deps);
}

module.exports = {
  LearningSession,
  createLearningSession,
  pairClaimsAndOutcomes,
};
