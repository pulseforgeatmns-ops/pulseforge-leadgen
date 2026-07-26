'use strict';

const { InteractionStore } = require('./InteractionStore');
const { LearningStore } = require('./LearningStore');
const { OutcomeTracker } = require('./OutcomeTracker');
const { PreferenceLearner } = require('./PreferenceLearner');
const {
  buildAdaptivePresentation,
  decorateDeck,
} = require('./AdaptivePresentation');
const { buildQualityDashboard } = require('./QualityDashboard');
/**
 * OperatorEngine — SPEC-012 / ADR-007.
 * Learns operator behavior. Changes presentation only.
 */
class OperatorEngine {
  /**
   * @param {object} [options]
   * @param {InteractionStore} [options.store]
   * @param {LearningStore} [options.learning]
   * @param {OutcomeTracker} [options.outcomes]
   * @param {PreferenceLearner} [options.preferences]
   */
  constructor(options = {}) {
    this._store = options.store || new InteractionStore();
    this._learning = options.learning || new LearningStore();
    this._preferences = options.preferences || new PreferenceLearner();
    this._outcomes =
      options.outcomes ||
      new OutcomeTracker({ learning: this._learning });
  }

  get store() {
    return this._store;
  }

  get learning() {
    return this._learning;
  }

  get outcomes() {
    return this._outcomes;
  }

  get preferences() {
    return this._preferences;
  }

  /**
   * Record one or more interaction events.
   * @param {object|object[]} input
   * @returns {{ events: object[], learnings: object[] }}
   */
  track(input) {
    const items = Array.isArray(input) ? input : [input];
    const events = [];
    const learnings = [];
    for (const partial of items) {
      const event = this._store.append(partial);
      events.push(event);
      const learning = this._learning.applyEvent(event);
      if (learning) learnings.push(learning);
      this._preferences.observeEvent(event);
    }
    return { events, learnings };
  }

  /**
   * Explicit outcome transition.
   * @param {object} input
   */
  setOutcome(input) {
    return this._outcomes.transition(input);
  }

  /**
   * @param {string} tenantId
   * @param {string} recommendationId
   */
  getLearning(tenantId, recommendationId) {
    return this._learning.get(tenantId, recommendationId);
  }

  /**
   * Decorate a CommandDeckModel with adaptive presentation.
   * @param {object} model
   * @param {string} tenantId
   */
  decorate(model, tenantId) {
    const events = this._store.query(String(tenantId), { limit: 2000 });
    const adaptive = buildAdaptivePresentation({ events });
    const prefs = this._preferences.snapshot(tenantId);
    return decorateDeck(model, {
      ...adaptive,
      preferences: { topIntents: prefs.topIntents },
    });
  }

  /**
   * Personalized Max suggestion chips.
   * @param {object} context
   * @param {string} tenantId
   */
  suggestions(context, tenantId) {
    return this._preferences.personalizedSuggestions(context, tenantId);
  }

  /**
   * Observe a Max question for preference learning (without full event).
   * @param {string} tenantId
   * @param {string} question
   */
  observeQuestion(tenantId, question) {
    return this._preferences.observeText(tenantId, question);
  }

  /**
   * Internal quality dashboard for a tenant.
   * @param {string} tenantId
   */
  quality(tenantId) {
    const tid = String(tenantId || '');
    return buildQualityDashboard({
      events: this._store.query(tid, { limit: 5000 }),
      learnings: this._learning.listForTenant(tid),
      preferences: this._preferences.snapshot(tid),
    });
  }
}

/**
 * @param {object} [options]
 */
function createOperatorEngine(options = {}) {
  return new OperatorEngine(options);
}

module.exports = {
  OperatorEngine,
  createOperatorEngine,
};
