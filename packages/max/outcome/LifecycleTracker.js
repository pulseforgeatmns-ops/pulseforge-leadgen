'use strict';

const {
  LIFECYCLE,
  canTransitionLifecycle,
  isTerminalOutcome,
  outcomeFromLifecycle,
} = require('./OutcomeTypes');

/**
 * Explicit recommendation outcome lifecycle (SPEC-013).
 *
 * Generated → Reviewed → Approved → Executed → Observed
 *                                              ↓
 *                                    Successful | Unsuccessful | Inconclusive
 */
class LifecycleTracker {
  /**
   * @param {object} deps
   * @param {import('./OutcomeStore').OutcomeStore} deps.store
   */
  constructor(deps) {
    if (!deps || !deps.store) {
      throw new Error('LifecycleTracker requires store');
    }
    this._store = deps.store;
    /** @type {Map<string, object[]>} */
    this._transitions = new Map();
  }

  /**
   * @param {object} input
   * @param {string} input.tenantId
   * @param {string} input.recommendationId
   * @param {string} input.lifecycle
   * @param {boolean} [input.force]
   * @param {string} [input.notes]
   * @param {number} [input.confidenceAtOutcome]
   */
  transition(input) {
    if (!input || !input.tenantId || !input.recommendationId) {
      throw new Error('transition requires tenantId and recommendationId');
    }
    const lifecycle = String(input.lifecycle || '').toLowerCase();
    if (!Object.values(LIFECYCLE).includes(lifecycle)) {
      throw new Error(`Unknown lifecycle: ${input.lifecycle}`);
    }

    const existing = this._store.get(
      input.tenantId,
      input.recommendationId
    );
    const from = existing ? existing.lifecycle : LIFECYCLE.GENERATED;
    if (!existing && lifecycle !== LIFECYCLE.GENERATED && input.force !== true) {
      throw new Error(
        `Recommendation not registered: ${input.recommendationId}`
      );
    }
    if (
      existing &&
      !canTransitionLifecycle(from, lifecycle) &&
      input.force !== true
    ) {
      throw new Error(
        `Invalid lifecycle transition: ${from} → ${lifecycle}`
      );
    }

    const ts = input.timestamp || new Date().toISOString();
    const patch = {
      tenantId: String(input.tenantId),
      recommendationId: String(input.recommendationId),
      lifecycle,
      outcome: outcomeFromLifecycle(lifecycle),
      executed:
        lifecycle === LIFECYCLE.EXECUTED ||
        lifecycle === LIFECYCLE.OBSERVED ||
        isTerminalOutcome(lifecycle),
      notes: input.notes,
      confidenceAtOutcome: input.confidenceAtOutcome,
      confidenceAtRecommendation:
        input.confidenceAtRecommendation != null
          ? input.confidenceAtRecommendation
          : existing
            ? existing.confidenceAtRecommendation
            : 0,
      strategyId: input.strategyId || (existing && existing.strategyId),
      force: input.force === true,
    };

    if (lifecycle === LIFECYCLE.REVIEWED) patch.reviewedAt = ts;
    if (lifecycle === LIFECYCLE.APPROVED) {
      patch.reviewedAt = (existing && existing.reviewedAt) || ts;
      patch.approvedAt = ts;
    }
    if (lifecycle === LIFECYCLE.EXECUTED) {
      patch.approvedAt = (existing && existing.approvedAt) || ts;
      patch.executedAt = ts;
    }
    if (lifecycle === LIFECYCLE.OBSERVED || isTerminalOutcome(lifecycle)) {
      patch.executedAt = (existing && existing.executedAt) || ts;
      patch.observedAt = ts;
    }

    const record = this._store.record(patch);
    const key = `${input.tenantId}::${input.recommendationId}`;
    const hist = this._transitions.get(key) || [];
    hist.push({ from, to: lifecycle, timestamp: ts, notes: input.notes || null });
    this._transitions.set(key, hist);

    return {
      recommendationId: String(input.recommendationId),
      tenantId: String(input.tenantId),
      from,
      to: lifecycle,
      record,
      history: hist.slice(),
    };
  }

  /**
   * @param {string} tenantId
   * @param {string} recommendationId
   */
  history(tenantId, recommendationId) {
    const key = `${tenantId}::${recommendationId}`;
    return (this._transitions.get(key) || []).slice();
  }

  clear() {
    this._transitions.clear();
  }
}

module.exports = { LifecycleTracker };
