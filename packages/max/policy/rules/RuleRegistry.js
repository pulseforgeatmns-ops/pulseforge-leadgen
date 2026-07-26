'use strict';

const { assertRule } = require('./RuleInterface');

/**
 * Rule Registry — modular policies; no hardcoded branching in the engine.
 *
 * PolicyEngine → RuleRegistry → Execute Rules (by priority) → Decision
 */
class RuleRegistry {
  constructor() {
    /** @type {Map<string, import('./RuleInterface').PolicyRule>} */
    this._rules = new Map();
  }

  /**
   * @param {import('./RuleInterface').PolicyRule} rule
   */
  register(rule) {
    assertRule(rule);
    if (this._rules.has(rule.id)) {
      throw new Error(`Policy rule already registered: ${rule.id}`);
    }
    this._rules.set(rule.id, rule);
    return this;
  }

  /**
   * @param {string} id
   */
  get(id) {
    return this._rules.get(id) || null;
  }

  /**
   * @param {string} id
   */
  unregister(id) {
    return this._rules.delete(id);
  }

  /**
   * Rules sorted by priority asc, then id asc (deterministic).
   * @returns {import('./RuleInterface').PolicyRule[]}
   */
  list() {
    return [...this._rules.values()].sort((a, b) => {
      if (a.priority !== b.priority) return a.priority - b.priority;
      return String(a.id).localeCompare(String(b.id));
    });
  }

  ids() {
    return this.list().map((r) => r.id);
  }

  clear() {
    this._rules.clear();
  }

  /**
   * Evaluate all rules against a frozen policy context.
   * @param {object} context
   * @returns {{ results: object[], timings: Record<string, number> }}
   */
  evaluateAll(context) {
    if (!context || typeof context !== 'object') {
      throw new Error('RuleRegistry.evaluateAll requires context');
    }
    const results = [];
    /** @type {Record<string, number>} */
    const timings = {};

    for (const rule of this.list()) {
      const start = process.hrtime.bigint();
      const result = rule.evaluate(context);
      timings[rule.id] = Number(process.hrtime.bigint() - start) / 1e6;

      if (!result || typeof result.passed !== 'boolean' || !result.action) {
        throw new Error(`Policy rule ${rule.id} returned invalid RuleResult`);
      }

      results.push({
        ruleId: rule.id,
        ruleName: rule.name,
        priority: rule.priority,
        ...result,
      });
    }

    return { results, timings };
  }
}

module.exports = {
  RuleRegistry,
};
