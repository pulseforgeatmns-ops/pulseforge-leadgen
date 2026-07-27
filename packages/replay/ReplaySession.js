'use strict';

/**
 * ReplaySession — disposable replay execution state (SPEC-018).
 *
 * Not persisted. Never written to a database.
 */
class ReplaySession {
  /**
   * @param {object} [seed]
   * @param {string} [seed.subjectId]
   * @param {import('./types').ReplayVersions} [seed.versions]
   */
  constructor(seed = {}) {
    this.subjectId = seed.subjectId || null;
    this.versions = seed.versions
      ? Object.freeze({ ...seed.versions })
      : Object.freeze({
          ontology: 'unknown',
          strategyPack: 'unknown',
          runtime: 'unknown',
        });

    /** @type {import('./types').ImmutableObservation|null} */
    this.currentObservation = null;
    /** @type {unknown} */
    this.currentClaims = null;
    /** @type {number|null} */
    this.currentConfidence = null;
    /** @type {unknown} */
    this.currentEvidence = null;
    /** @type {object|null} */
    this.currentRecommendation = null;
    /** @type {object|null} */
    this.currentExplanation = null;
    /** @type {object|null} */
    this.currentReasoningTrace = null;
    /** @type {import('./types').ReplayStep[]} */
    this.steps = [];
    /** @type {object[]} */
    this.recommendations = [];
    this.closed = false;
  }

  /**
   * @param {import('./types').ReplayStep} step
   */
  applyStep(step) {
    assertOpen(this);
    if (!step || typeof step !== 'object') {
      throw new Error('ReplaySession.applyStep requires a step object');
    }

    this.currentObservation = step.observation || null;
    this.currentEvidence = step.generatedEvidence;
    this.currentClaims = step.affectedClaims;
    this.currentConfidence =
      step.confidence != null ? Number(step.confidence) : null;
    this.currentRecommendation = step.recommendation || null;
    this.currentReasoningTrace = step.reasoningTrace || null;
    this.currentExplanation = step.explanation || null;

    this.steps.push(Object.freeze({ ...step }));
    if (step.recommendation) {
      this.recommendations.push(step.recommendation);
    }
  }

  /**
   * Snapshot of current session state (deep-enough clone for consumers).
   * @returns {object}
   */
  getState() {
    return {
      subjectId: this.subjectId,
      versions: { ...this.versions },
      currentObservation: this.currentObservation,
      currentClaims: this.currentClaims,
      currentConfidence: this.currentConfidence,
      currentEvidence: this.currentEvidence,
      currentRecommendation: this.currentRecommendation,
      currentExplanation: this.currentExplanation,
      currentReasoningTrace: this.currentReasoningTrace,
      stepCount: this.steps.length,
      recommendationCount: this.recommendations.length,
      closed: this.closed,
    };
  }

  /**
   * Mark session disposed. Further applyStep calls throw.
   */
  close() {
    this.closed = true;
  }
}

/**
 * @param {ReplaySession} session
 */
function assertOpen(session) {
  if (session.closed) {
    throw new Error('ReplaySession is closed and cannot accept further steps');
  }
}

/**
 * @param {object} [seed]
 * @returns {ReplaySession}
 */
function createReplaySession(seed) {
  return new ReplaySession(seed);
}

module.exports = {
  ReplaySession,
  createReplaySession,
};
