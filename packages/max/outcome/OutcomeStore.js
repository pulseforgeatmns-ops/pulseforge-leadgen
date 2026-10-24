'use strict';

const {
  LIFECYCLE,
  buildRecommendationOutcome,
  canTransitionLifecycle,
  isTerminalOutcome,
  outcomeFromLifecycle,
} = require('./OutcomeTypes');

/**
 * Process-scoped store for RecommendationOutcome records (SPEC-013).
 * Latest state keyed by tenantId::recommendationId; history is append-only.
 */
class OutcomeStore {
  constructor() {
    /** @type {Map<string, object>} */
    this._latest = new Map();
    /** @type {object[]} */
    this._history = [];
  }

  /**
   * Register or update a recommendation outcome. Prior history entries are kept.
   * @param {object} input
   */
  record(input) {
    if (!input || input.recommendationId == null || input.tenantId == null) {
      throw new Error('record requires tenantId and recommendationId');
    }

    const key = this._key(input.tenantId, input.recommendationId);
    const existing = this._latest.get(key) || null;
    const nextLifecycle = String(
      input.lifecycle || (existing && existing.lifecycle) || LIFECYCLE.GENERATED
    ).toLowerCase();

    if (existing && existing.lifecycle !== nextLifecycle) {
      if (
        !canTransitionLifecycle(existing.lifecycle, nextLifecycle) &&
        input.force !== true
      ) {
        throw new Error(
          `Invalid lifecycle transition: ${existing.lifecycle} → ${nextLifecycle}`
        );
      }
    }

    const now = new Date().toISOString();
    const mergedInput = {
      id: existing ? existing.id : input.id,
      recommendationId: input.recommendationId,
      tenantId: input.tenantId,
      strategyId:
        input.strategyId != null
          ? input.strategyId
          : existing
            ? existing.strategyId
            : null,
      lifecycle: nextLifecycle,
      outcome:
        input.outcome != null
          ? input.outcome
          : outcomeFromLifecycle(nextLifecycle),
      executed:
        input.executed === true ||
        (existing && existing.executed) ||
        nextLifecycle === LIFECYCLE.EXECUTED ||
        nextLifecycle === LIFECYCLE.OBSERVED ||
        isTerminalOutcome(nextLifecycle),
      confidenceAtRecommendation:
        input.confidenceAtRecommendation != null
          ? input.confidenceAtRecommendation
          : existing
            ? existing.confidenceAtRecommendation
            : 0,
      confidenceAtOutcome:
        input.confidenceAtOutcome != null
          ? input.confidenceAtOutcome
          : existing
            ? existing.confidenceAtOutcome
            : null,
      notes:
        input.notes != null ? input.notes : existing ? existing.notes : null,
      generatedAt:
        (existing && existing.generatedAt) ||
        input.generatedAt ||
        now,
      reviewedAt: pickTimestamp(
        nextLifecycle,
        LIFECYCLE.REVIEWED,
        input.reviewedAt,
        existing && existing.reviewedAt,
        now
      ),
      approvedAt: pickTimestamp(
        nextLifecycle,
        LIFECYCLE.APPROVED,
        input.approvedAt,
        existing && existing.approvedAt,
        now
      ),
      executedAt: pickTimestamp(
        nextLifecycle,
        LIFECYCLE.EXECUTED,
        input.executedAt,
        existing && existing.executedAt,
        now,
        isTerminalOutcome(nextLifecycle) || nextLifecycle === LIFECYCLE.OBSERVED
      ),
      observedAt:
        input.observedAt ||
        (existing && existing.observedAt) ||
        (nextLifecycle === LIFECYCLE.OBSERVED || isTerminalOutcome(nextLifecycle)
          ? now
          : null),
      evidenceSourceIds:
        Array.isArray(input.evidenceSourceIds) && input.evidenceSourceIds.length
          ? input.evidenceSourceIds
          : (existing && existing.evidenceSourceIds) || [],
      promotedFromWatch:
        input.promotedFromWatch === true ||
        (existing && existing.promotedFromWatch === true),
      watchAlertEarly:
        input.watchAlertEarly === true ||
        (existing && existing.watchAlertEarly === true),
      meta: input.meta != null ? input.meta : existing ? existing.meta : null,
    };

    const built = buildRecommendationOutcome(mergedInput);
    this._latest.set(key, built);
    this._history.push({
      ...built,
      recordedAt: now,
      priorLifecycle: existing ? existing.lifecycle : null,
    });
    return built;
  }

  /**
   * @param {string} tenantId
   * @param {string} recommendationId
   */
  get(tenantId, recommendationId) {
    return this._latest.get(this._key(tenantId, recommendationId)) || null;
  }

  /**
   * @param {string} tenantId
   * @param {object} [opts]
   */
  listForTenant(tenantId, opts = {}) {
    const tid = String(tenantId);
    const rows = [];
    for (const row of this._latest.values()) {
      if (row.tenantId === tid) rows.push(row);
    }
    rows.sort((a, b) =>
      String(a.generatedAt).localeCompare(String(b.generatedAt))
    );
    const limit = opts.limit != null ? Number(opts.limit) : rows.length;
    return rows.slice(0, Math.max(0, limit));
  }

  /**
   * @param {string} [tenantId]
   */
  history(tenantId) {
    if (tenantId == null) return this._history.slice();
    const tid = String(tenantId);
    return this._history.filter((h) => h.tenantId === tid);
  }

  clear() {
    this._latest.clear();
    this._history = [];
  }

  get size() {
    return this._latest.size;
  }

  _key(tenantId, recommendationId) {
    return `${tenantId}::${recommendationId}`;
  }
}

/**
 * Preserve existing timestamp; set when lifecycle first reaches the stage.
 */
function pickTimestamp(lifecycle, stage, inputTs, existingTs, now, force) {
  if (inputTs) return inputTs;
  if (existingTs) return existingTs;
  const reached =
    force === true ||
    lifecycle === stage ||
    stageOrder(lifecycle) > stageOrder(stage);
  return reached ? now : null;
}

function stageOrder(lifecycle) {
  const order = [
    LIFECYCLE.GENERATED,
    LIFECYCLE.REVIEWED,
    LIFECYCLE.APPROVED,
    LIFECYCLE.EXECUTED,
    LIFECYCLE.OBSERVED,
    LIFECYCLE.SUCCESSFUL,
  ];
  const idx = order.indexOf(lifecycle);
  if (idx >= 0) return idx;
  if (isTerminalOutcome(lifecycle)) return order.length;
  return 0;
}

module.exports = { OutcomeStore };
