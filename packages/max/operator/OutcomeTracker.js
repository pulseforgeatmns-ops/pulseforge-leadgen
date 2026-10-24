'use strict';

const {
  OUTCOMES,
  canTransitionOutcome,
} = require('./OperatorTypes');

/**
 * Explicit recommendation outcome lifecycle (SPEC-012).
 *
 * Recommended → Reviewed → Approved → Executed → Successful
 *                          ↘ Dismissed | Expired | Contradicted
 */
class OutcomeTracker {
  /**
   * @param {object} deps
   * @param {import('./LearningStore').LearningStore} deps.learning
   */
  constructor(deps) {
    if (!deps || !deps.learning) {
      throw new Error('OutcomeTracker requires learning');
    }
    this._learning = deps.learning;
    /** @type {Map<string, object[]>} */
    this._history = new Map();
  }

  /**
   * @param {object} input
   * @param {string} input.tenantId
   * @param {string} input.recommendationId
   * @param {string} input.outcome
   * @param {string} [input.timestamp]
   * @param {string} [input.reason]
   * @param {boolean} [input.force=false] - allow same-state refresh
   */
  transition(input) {
    if (!input || !input.tenantId || !input.recommendationId) {
      throw new Error('transition requires tenantId and recommendationId');
    }
    const outcome = String(input.outcome || '').toLowerCase();
    if (!Object.values(OUTCOMES).includes(outcome)) {
      throw new Error(`Unknown outcome: ${input.outcome}`);
    }

    const current = this._learning.get(
      input.tenantId,
      input.recommendationId
    );
    const from = current.outcome || OUTCOMES.RECOMMENDED;
    if (!canTransitionOutcome(from, outcome) && input.force !== true) {
      throw new Error(
        `Invalid outcome transition: ${from} → ${outcome}`
      );
    }

    const ts = input.timestamp || new Date().toISOString();
    const learning = this._learning.setOutcome(
      input.tenantId,
      input.recommendationId,
      outcome,
      ts
    );

    const key = `${input.tenantId}::${input.recommendationId}`;
    const hist = this._history.get(key) || [];
    hist.push({
      from,
      to: outcome,
      timestamp: ts,
      reason: input.reason != null ? String(input.reason) : null,
    });
    this._history.set(key, hist);

    return {
      recommendationId: String(input.recommendationId),
      tenantId: String(input.tenantId),
      from,
      to: outcome,
      learning,
      history: hist.slice(),
    };
  }

  /**
   * @param {string} tenantId
   * @param {string} recommendationId
   */
  history(tenantId, recommendationId) {
    const key = `${tenantId}::${recommendationId}`;
    return (this._history.get(key) || []).slice();
  }

  clear() {
    this._history.clear();
  }
}

module.exports = { OutcomeTracker };
