'use strict';

const { LEARNING_RULES, DEFAULT_BLEND_WEIGHT } = require('./types');
const { BeliefTracker, createBeliefTracker } = require('./BeliefTracker');
const {
  CalibrationEngine,
  createCalibrationEngine,
} = require('./CalibrationEngine');
const {
  OutcomeEvaluator,
  createOutcomeEvaluator,
} = require('./OutcomeEvaluator');
const {
  LearningSession,
  createLearningSession,
} = require('./LearningSession');

/**
 * LearningEngine — Belief Evolution façade (SPEC-021).
 *
 * Consumes Claims · Evidence · Outcomes.
 * Produces calibration updates · historical accuracy · confidence adjustments.
 *
 * Never changes history. Never mutates replay. Never mutates runtime confidence.
 *
 * @example
 *   const engine = createLearningEngine();
 *   const result = engine.learn({
 *     claims: [{ id: 'momentum_continuation', confidence: 0.82 }],
 *     outcomes: [{ claimId: 'momentum_continuation', verdict: 'correct' }],
 *   });
 */
class LearningEngine {
  /**
   * @param {object} [deps]
   * @param {BeliefTracker} [deps.beliefTracker]
   * @param {CalibrationEngine} [deps.calibrationEngine]
   * @param {OutcomeEvaluator} [deps.outcomeEvaluator]
   * @param {number} [deps.blendWeight]
   */
  constructor(deps = {}) {
    this.beliefs = deps.beliefTracker || createBeliefTracker();
    this.calibration =
      deps.calibrationEngine ||
      createCalibrationEngine({
        blendWeight:
          deps.blendWeight != null ? deps.blendWeight : DEFAULT_BLEND_WEIGHT,
      });
    this.evaluator = deps.outcomeEvaluator || createOutcomeEvaluator();

    /** @type {Map<string, LearningSession>} */
    this._sessions = new Map();
  }

  /**
   * Create an isolated learning session (does not auto-run).
   * @param {object} seed
   * @returns {LearningSession}
   */
  createSession(seed = {}) {
    const session = createLearningSession(seed, {
      beliefTracker: createBeliefTracker(),
      calibrationEngine: this.calibration,
      outcomeEvaluator: this.evaluator,
    });
    this._sessions.set(session.id, session);
    return session;
  }

  /**
   * @param {string} id
   * @returns {LearningSession|null}
   */
  getSession(id) {
    return this._sessions.get(String(id)) || null;
  }

  /**
   * Learn from claims + outcomes. Returns accuracy, precision, recall,
   * calibration, and confidence adjustments — without mutating inputs.
   *
   * @param {object} input
   * @param {object[]} [input.claims]
   * @param {object[]} [input.evidence]
   * @param {object[]} [input.outcomes]
   * @param {object[]} [input.observations]
   * @param {string} [input.strategyPack]
   * @param {string} [input.name]
   * @param {number} [input.blendWeight]
   * @param {boolean} [input.persistBeliefs=true] - merge into engine-level BeliefTracker
   * @returns {object}
   */
  learn(input = {}) {
    const session = this.createSession({
      name: input.name || 'learn',
      claims: cloneList(input.claims),
      evidence: cloneList(input.evidence),
      outcomes: cloneList(input.outcomes),
      observations: cloneList(input.observations),
      strategyPack: input.strategyPack || null,
      metadata: { source: 'LearningEngine.learn' },
    });

    const result = session.run({ blendWeight: input.blendWeight });

    if (input.persistBeliefs !== false) {
      for (const evaluation of result.evaluations) {
        this.beliefs.record(evaluation);
      }
    }

    return Object.freeze({
      ...result,
      engineBeliefs: Object.freeze(this.beliefs.listClaims()),
      engineStrategyBeliefs: Object.freeze(this.beliefs.listStrategies()),
      produced: Object.freeze({
        calibrationUpdates: result.calibrations,
        historicalAccuracy: summarizeAccuracy(result),
        confidenceAdjustments: result.calibrations.map((c) =>
          Object.freeze({
            claimId: c.claimId,
            confidenceBefore: c.confidence,
            confidenceAfter: c.adjustedConfidence,
            historicalCalibration: c.historicalCalibration,
          })
        ),
      }),
      rules: LEARNING_RULES,
    });
  }

  /**
   * Record a single claim/outcome evaluation into the engine tracker.
   * @param {{ claim: object, outcome: object }} pair
   * @returns {{ evaluation: object, calibration: object|null, stats: object }}
   */
  recordOutcome(pair = {}) {
    const evaluation = this.evaluator.evaluate(pair);
    const stats = this.beliefs.record(evaluation);

    let calibration = null;
    if (evaluation.verdict !== 'unresolved') {
      calibration = this.calibration.calibrate({
        claimId: evaluation.claimId,
        claimType: evaluation.claimType,
        confidence: evaluation.confidenceBefore,
        stats,
        observationsConsidered: evaluation.observationsConsidered,
        outcome: pair.outcome,
        evaluationExplanation: evaluation.explanation,
      });
    }

    return Object.freeze({
      evaluation,
      calibration,
      stats,
      mutatesHistory: false,
      mutatesReplay: false,
      mutatesRuntime: false,
    });
  }

  /**
   * SHOW Calibration FOR Claim("…")
   * @param {string} claimId
   * @param {object} [opts]
   * @param {number|null} [opts.confidence]
   * @returns {import('./types').CalibrationResult}
   */
  showCalibration(claimId, opts = {}) {
    if (!claimId) throw new Error('showCalibration requires claimId');
    const stats =
      this.beliefs.statsFor(claimId) ||
      emptyClaimStats(claimId, opts.claimType || null);
    return this.calibration.calibrate({
      claimId: String(claimId),
      claimType: opts.claimType || stats.claimType,
      confidence: opts.confidence != null ? opts.confidence : guessConfidence(opts),
      stats,
      observationsConsidered: opts.observationsConsidered || [],
      outcome: opts.outcome || null,
    });
  }

  /**
   * SHOW Accuracy FOR StrategyPack("…") or Claim("…")
   * @param {object} args
   * @param {'claim'|'strategy_pack'} [args.scope]
   * @param {string} args.id
   * @returns {import('./types').AccuracyReport}
   */
  showAccuracy(args = {}) {
    const id = args.id || args.scopeId;
    if (!id) throw new Error('showAccuracy requires id');

    const scope =
      args.scope ||
      (args.strategyPack != null || args.kind === 'strategy_pack'
        ? 'strategy_pack'
        : 'claim');

    if (scope === 'strategy_pack') {
      const stats =
        this.beliefs.statsForStrategy(id) || emptyClaimStats(id, null);
      const claims = this.beliefs
        .listClaims()
        .filter((c) => !args.filterClaims || c.strategyPack === id);
      return this.calibration.accuracyReport({
        scope: 'strategy_pack',
        scopeId: String(id),
        stats,
        claims,
      });
    }

    const stats = this.beliefs.statsFor(id) || emptyClaimStats(id, null);
    return this.calibration.accuracyReport({
      scope: 'claim',
      scopeId: String(id),
      stats,
      claims: [stats],
    });
  }

  /**
   * Build an EQL-compatible catalog projection of calibrations / accuracies.
   * @returns {object}
   */
  toCatalogSeed() {
    const calibrations = this.beliefs.listClaims().map((stats) =>
      this.calibration.calibrate({
        claimId: stats.claimId,
        claimType: stats.claimType,
        confidence: null,
        stats,
      })
    );
    const accuracies = [
      ...this.beliefs.listClaims().map((stats) =>
        this.calibration.accuracyReport({
          scope: 'claim',
          scopeId: stats.claimId,
          stats,
          claims: [stats],
        })
      ),
      ...this.beliefs.listStrategies().map((stats) =>
        this.calibration.accuracyReport({
          scope: 'strategy_pack',
          scopeId: stats.claimId,
          stats,
          claims: this.beliefs.listClaims(),
        })
      ),
    ];

    return {
      calibrations,
      accuracies,
      claims: this.beliefs.listClaims(),
      strategy_packs: this.beliefs.listStrategies().map((s) =>
        Object.freeze({
          id: s.claimId,
          strategyPack: s.claimId,
          accuracy: s.accuracy,
          precision: s.precision,
          recall: s.recall,
          occurrences: s.occurrences,
        })
      ),
    };
  }

  /**
   * Clear engine-level belief aggregates (sessions retained).
   */
  resetBeliefs() {
    this.beliefs = createBeliefTracker();
  }
}

function summarizeAccuracy(result) {
  const beliefs = result.beliefs || [];
  const totalCorrect = beliefs.reduce((n, b) => n + (b.correct || 0), 0);
  const totalIncorrect = beliefs.reduce((n, b) => n + (b.incorrect || 0), 0);
  const totalPartial = beliefs.reduce(
    (n, b) => n + (b.partiallyCorrect || 0),
    0
  );
  const totalUnresolved = beliefs.reduce((n, b) => n + (b.unresolved || 0), 0);
  const totalOcc = beliefs.reduce((n, b) => n + (b.occurrences || 0), 0);
  const resolved = totalCorrect + totalIncorrect + totalPartial;
  return Object.freeze({
    occurrences: totalOcc,
    correct: totalCorrect,
    incorrect: totalIncorrect,
    partiallyCorrect: totalPartial,
    unresolved: totalUnresolved,
    accuracy: resolved === 0 ? null : round4(
      beliefs.reduce((n, b) => {
        if (b.accuracy == null || !b.occurrences) return n;
        // weight by resolved count approximation
        const r = (b.correct || 0) + (b.incorrect || 0) + (b.partiallyCorrect || 0);
        return n + b.accuracy * r;
      }, 0) / resolved
    ),
    precision: averageMetric(beliefs, 'precision'),
    recall: averageMetric(beliefs, 'recall'),
  });
}

function averageMetric(beliefs, key) {
  const values = beliefs
    .map((b) => b[key])
    .filter((v) => v != null && Number.isFinite(Number(v)));
  if (values.length === 0) return null;
  return round4(values.reduce((a, b) => a + Number(b), 0) / values.length);
}

function emptyClaimStats(claimId, claimType) {
  return Object.freeze({
    claimId: String(claimId),
    claimType: claimType,
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

function guessConfidence(opts) {
  if (opts.claim && opts.claim.confidence != null) return opts.claim.confidence;
  return null;
}

function cloneList(value) {
  return Array.isArray(value)
    ? value.map((item) =>
        item && typeof item === 'object' ? { ...item } : item
      )
    : [];
}

function round4(n) {
  return Math.round(n * 10000) / 10000;
}

/**
 * @param {object} [deps]
 * @returns {LearningEngine}
 */
function createLearningEngine(deps) {
  return new LearningEngine(deps);
}

module.exports = {
  LearningEngine,
  createLearningEngine,
  BeliefTracker,
  CalibrationEngine,
  OutcomeEvaluator,
  LearningSession,
};
