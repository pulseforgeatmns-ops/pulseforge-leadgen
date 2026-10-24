'use strict';

/**
 * ScenarioRunner — execute an isolated Experiment via ReplayEngine (SPEC-019).
 *
 * Reads only. Never persists session state. Never mutates production observations.
 */
class ScenarioRunner {
  /**
   * @param {object} deps
   * @param {{ run: (input: object) => Promise<object> }} deps.replayEngine
   */
  constructor(deps = {}) {
    if (!deps.replayEngine || typeof deps.replayEngine.run !== 'function') {
      throw new Error('ScenarioRunner requires a replayEngine with run()');
    }
    this._replayEngine = deps.replayEngine;
  }

  /**
   * Run a single experiment and return a frozen laboratory result.
   *
   * @param {import('./Experiment').Experiment} experiment
   * @param {object} [overrides] - optional ReplayRunInput overrides (still isolated)
   * @returns {Promise<object>}
   */
  async run(experiment, overrides = {}) {
    assertExperiment(experiment);

    const input = {
      ...experiment.toReplayInput(),
      ...overrides,
      // Always use experiment observations unless explicitly overridden with a clone.
      observations:
        overrides.observations != null
          ? overrides.observations
          : experiment.getObservations(),
      subjectId: overrides.subjectId || experiment.subjectId,
    };

    // Defensive: never accept a production write hook.
    if (input.persist || input.write || input.commit) {
      throw new Error(
        'ScenarioRunner refuses persist/write/commit options — laboratory is read-only'
      );
    }

    const replay = await this._replayEngine.run(input);

    return Object.freeze({
      experimentId: experiment.id,
      experiment: experiment.snapshot(),
      fingerprint: experiment.fingerprint(),
      subjectId: replay.subjectId,
      startTime: replay.startTime,
      endTime: replay.endTime,
      observations: replay.observations,
      evidence: replay.evidence,
      claims: replay.claims,
      confidence: replay.confidence,
      recommendations: replay.recommendations,
      explanation: replay.explanation,
      reasoningTrace: replay.reasoningTrace,
      steps: replay.steps,
      versions: replay.versions,
      ranked: replay.ranked || null,
      replayFingerprint: replay.fingerprint || null,
      queries: replay.queries || null,
      isolated: true,
      mutatesProduction: false,
    });
  }

  /**
   * Run two experiments and return both results (no comparison yet).
   *
   * @param {import('./Experiment').Experiment} left
   * @param {import('./Experiment').Experiment} right
   * @returns {Promise<{ left: object, right: object }>}
   */
  async runPair(left, right) {
    const [leftResult, rightResult] = await Promise.all([
      this.run(left),
      this.run(right),
    ]);
    return { left: leftResult, right: rightResult };
  }
}

/**
 * @param {unknown} experiment
 */
function assertExperiment(experiment) {
  if (!experiment || typeof experiment.getObservations !== 'function') {
    throw new Error('ScenarioRunner.run requires an Experiment');
  }
  if (experiment.mutatesProduction === true) {
    throw new Error('ScenarioRunner refuses experiments that mutate production');
  }
}

/**
 * @param {object} deps
 * @returns {ScenarioRunner}
 */
function createScenarioRunner(deps) {
  return new ScenarioRunner(deps);
}

module.exports = {
  ScenarioRunner,
  createScenarioRunner,
};
