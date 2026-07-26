'use strict';

const { LIFECYCLE } = require('./OutcomeTypes');
const { OutcomeStore } = require('./OutcomeStore');
const { LifecycleTracker } = require('./LifecycleTracker');
const { buildCalibrationReport } = require('./CalibrationReport');
const { buildStrategyPerformance } = require('./StrategyPerformance');
const { detectDrift } = require('./DriftDetector');
const { buildReviewDashboard } = require('./ReviewDashboard');

/**
 * OutcomeEngine — SPEC-013 / ADR-008.
 * Evaluates whether intelligence was right. Never changes reasoning.
 */
class OutcomeEngine {
  /**
   * @param {object} [options]
   * @param {OutcomeStore} [options.store]
   * @param {LifecycleTracker} [options.lifecycle]
   * @param {() => object|null} [options.getOperatorQuality] - optional bridge to SPEC-012
   */
  constructor(options = {}) {
    this._store = options.store || new OutcomeStore();
    this._lifecycle =
      options.lifecycle || new LifecycleTracker({ store: this._store });
    this._getOperatorQuality =
      typeof options.getOperatorQuality === 'function'
        ? options.getOperatorQuality
        : null;
  }

  get store() {
    return this._store;
  }

  get lifecycle() {
    return this._lifecycle;
  }

  /**
   * Register or update a RecommendationOutcome (direct write).
   * @param {object} input
   */
  record(input) {
    return this._store.record(input);
  }

  /**
   * Explicit lifecycle transition.
   * @param {object} input
   */
  transition(input) {
    return this._lifecycle.transition(input);
  }

  /**
   * Observe Generated recommendations from a CommandDeckModel.
   * Additive registration only — does not mutate the model.
   * @param {object} model
   * @param {string} tenantId
   * @returns {object[]} newly or already registered rows
   */
  observeGenerated(model, tenantId) {
    const tid = String(tenantId || '');
    if (!tid || !model || typeof model !== 'object') return [];

    const cards = collectRecommendationCards(model);
    const registered = [];
    for (const card of cards) {
      const recommendationId = String(
        card.recommendationId || card.id || ''
      ).trim();
      if (!recommendationId) continue;

      const existing = this._store.get(tid, recommendationId);
      if (existing) {
        registered.push(existing);
        continue;
      }

      const row = this._store.record({
        tenantId: tid,
        recommendationId,
        lifecycle: LIFECYCLE.GENERATED,
        confidenceAtRecommendation:
          card.confidence != null
            ? card.confidence
            : card.meta && card.meta.confidence != null
              ? card.meta.confidence
              : 0,
        strategyId:
          card.strategyId ||
          (card.meta && card.meta.primaryStrategy) ||
          (card.reasoningSummary && card.reasoningSummary.primaryStrategy) ||
          null,
        evidenceSourceIds: extractEvidenceIds(card),
        promotedFromWatch: card.promotedFromWatch === true,
        watchAlertEarly: card.watchAlertEarly === true,
        generatedAt: card.generatedAt || new Date().toISOString(),
      });
      registered.push(row);
    }
    return registered;
  }

  /**
   * @param {string} tenantId
   * @param {string} recommendationId
   */
  get(tenantId, recommendationId) {
    return this._store.get(tenantId, recommendationId);
  }

  /**
   * @param {string} tenantId
   */
  calibration(tenantId) {
    return buildCalibrationReport({
      records: this._store.listForTenant(tenantId),
    });
  }

  /**
   * @param {string} tenantId
   */
  strategies(tenantId) {
    return buildStrategyPerformance({
      records: this._store.listForTenant(tenantId),
    });
  }

  /**
   * @param {string} tenantId
   * @param {object} [options]
   */
  drift(tenantId, options = {}) {
    const operatorQuality = this._resolveOperatorQuality(tenantId);
    return detectDrift({
      records: this._store.listForTenant(tenantId),
      operatorQuality,
      ...options,
    });
  }

  /**
   * Internal Intelligence Review dashboard.
   * @param {string} tenantId
   * @param {object} [options]
   */
  review(tenantId) {
    const operatorQuality = this._resolveOperatorQuality(tenantId);
    return buildReviewDashboard({
      records: this._store.listForTenant(tenantId),
      operatorQuality,
    });
  }

  _resolveOperatorQuality(tenantId) {
    if (!this._getOperatorQuality) return null;
    try {
      return this._getOperatorQuality(String(tenantId));
    } catch {
      return null;
    }
  }
}

/**
 * Collect recommendation-like cards from a deck model without mutating it.
 * @param {object} model
 */
function collectRecommendationCards(model) {
  const out = [];
  const push = (item) => {
    if (!item || typeof item !== 'object') return;
    if (item.recommendationId || item.id) out.push(item);
  };

  if (model.highestLeverageAction) push(model.highestLeverageAction);
  if (Array.isArray(model.priorityQueue)) {
    for (const item of model.priorityQueue) push(item);
  }
  if (Array.isArray(model.watchAlerts)) {
    for (const item of model.watchAlerts) {
      push({ ...item, watchAlertEarly: item.watchAlertEarly === true });
    }
  }
  if (Array.isArray(model.cards)) {
    for (const item of model.cards) push(item);
  }
  if (
    model.morningBrief &&
    Array.isArray(model.morningBrief.recommendations)
  ) {
    for (const item of model.morningBrief.recommendations) push(item);
  }
  return out;
}

function extractEvidenceIds(card) {
  const ids = [];
  const signals = card.supportingSignals || card.evidence || [];
  if (Array.isArray(signals)) {
    for (const s of signals) {
      if (s == null) continue;
      if (typeof s === 'string') ids.push(s);
      else if (s.id) ids.push(String(s.id));
      else if (s.sourceId) ids.push(String(s.sourceId));
    }
  }
  return ids;
}

/**
 * @param {object} [options]
 */
function createOutcomeEngine(options = {}) {
  return new OutcomeEngine(options);
}

module.exports = {
  OutcomeEngine,
  createOutcomeEngine,
  collectRecommendationCards,
};
