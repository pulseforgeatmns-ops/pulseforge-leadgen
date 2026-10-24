'use strict';

const { assertStrategy } = require('./StrategyInterface');

/**
 * Strategy Registry — discover and execute strategies without engine changes.
 * New strategies register here; the Reasoning Engine executes all.
 */
class StrategyRegistry {
  constructor() {
    /** @type {Map<string, import('./StrategyInterface').ReasoningStrategy>} */
    this._strategies = new Map();
  }

  /**
   * @param {import('./StrategyInterface').ReasoningStrategy} strategy
   */
  register(strategy) {
    assertStrategy(strategy);
    if (this._strategies.has(strategy.id)) {
      throw new Error(`Strategy already registered: ${strategy.id}`);
    }
    this._strategies.set(strategy.id, strategy);
    return this;
  }

  /**
   * @param {string} id
   */
  get(id) {
    return this._strategies.get(id) || null;
  }

  /**
   * @returns {import('./StrategyInterface').ReasoningStrategy[]}
   */
  list() {
    return [...this._strategies.values()].sort((a, b) =>
      String(a.id).localeCompare(String(b.id))
    );
  }

  /**
   * @returns {string[]}
   */
  ids() {
    return this.list().map((s) => s.id);
  }

  /**
   * Evaluate all strategies against an immutable context.
   * Strategies must not mutate context or produce recommendations.
   *
   * @param {import('../reasoning/ReasoningTypes').ReasoningContext} context
   * @returns {{ results: import('../reasoning/ReasoningTypes').StrategyResult[], timings: Record<string, number> }}
   */
  evaluateAll(context) {
    if (!context || typeof context !== 'object') {
      throw new Error('evaluateAll requires a ReasoningContext');
    }
    const results = [];
    /** @type {Record<string, number>} */
    const timings = {};
    for (const strategy of this.list()) {
      const start = process.hrtime.bigint();
      const result = strategy.evaluate(context);
      const elapsedNs = process.hrtime.bigint() - start;
      timings[strategy.id] = Number(elapsedNs) / 1e6;
      if (!result || result.strategy !== strategy.id) {
        throw new Error(
          `Strategy ${strategy.id} returned invalid StrategyResult (strategy field mismatch)`
        );
      }
      if (Object.prototype.hasOwnProperty.call(result, 'recommendedAction')) {
        throw new Error(`Strategy ${strategy.id} must not produce recommendations`);
      }
      results.push(result);
    }
    return { results, timings };
  }
}

/**
 * Create a registry with the seven initial CRM strategies.
 * @returns {StrategyRegistry}
 */
function createDefaultStrategyRegistry() {
  return createCRMStrategyRegistry();
}

/**
 * Named CRM strategy pack registry (SPEC-015A / audit R3).
 * Alias of the historical default registry — identical strategies and behavior.
 * @returns {StrategyRegistry}
 */
function createCRMStrategyRegistry() {
  const { OpportunityStrategy } = require('./OpportunityStrategy');
  const { EngagementStrategy } = require('./EngagementStrategy');
  const { RelationshipStrategy } = require('./RelationshipStrategy');
  const { DecisionMakerStrategy } = require('./DecisionMakerStrategy');
  const { OverflowStrategy } = require('./OverflowStrategy');
  const { TechnologyStrategy } = require('./TechnologyStrategy');
  const { RiskStrategy } = require('./RiskStrategy');

  const registry = new StrategyRegistry();
  registry
    .register(OpportunityStrategy)
    .register(EngagementStrategy)
    .register(RelationshipStrategy)
    .register(DecisionMakerStrategy)
    .register(OverflowStrategy)
    .register(TechnologyStrategy)
    .register(RiskStrategy);
  return registry;
}

module.exports = {
  StrategyRegistry,
  createDefaultStrategyRegistry,
  createCRMStrategyRegistry,
};
