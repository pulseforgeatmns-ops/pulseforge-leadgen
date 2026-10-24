'use strict';

/**
 * @pulseforge/eql — Evidence Query Language (SPEC-020)
 *
 * Domain-neutral declarative queries over the Evidence Graph,
 * replay history, and reasoning outputs. No mutation statements.
 *
 * @example
 *   const { createEqlEngine, createEvidenceCatalog } = require('@pulseforge/eql');
 *   const catalog = createEvidenceCatalog({
 *     claims: [
 *       { id: 'c1', subject: 'BTC', confidence: 0.9 },
 *       { id: 'c2', subject: 'Company123', confidence: 0.8 },
 *     ],
 *   });
 *   const eql = createEqlEngine({ catalog });
 *   await eql.query(`FIND Claims WHERE subject = "BTC" AND confidence > 0.75`);
 */

const {
  EQL_RULES,
  STATEMENT_KINDS,
  MUTATION_KEYWORDS,
  TARGET_ALIASES,
  SUBJECT_FIELD_ALIASES,
  CONFIDENCE_FIELD_ALIASES,
  ID_FIELD_ALIASES,
} = require('./types');
const {
  parseEql,
  tokenize,
  resolveTarget,
  EqlParseError,
} = require('./Parser');
const { planEql, QueryPlanner } = require('./QueryPlanner');
const {
  Executor,
  createExecutor,
  createEvidenceCatalog,
  catalogFromResult,
  matchesCondition,
  resolveField,
} = require('./Executor');

/**
 * High-level EQL engine: parse → plan → execute.
 */
class EqlEngine {
  /**
   * @param {object} [deps]
   * @param {import('./Executor').EvidenceCatalog} [deps.catalog]
   * @param {Executor} [deps.executor]
   */
  constructor(deps = {}) {
    this.catalog = deps.catalog || createEvidenceCatalog();
    this.executor =
      deps.executor ||
      createExecutor({ catalog: this.catalog });
  }

  /**
   * @param {string} source
   * @returns {import('./types').EqlAst}
   */
  parse(source) {
    return parseEql(source);
  }

  /**
   * @param {string|import('./types').EqlAst} sourceOrAst
   * @returns {import('./types').EqlPlan}
   */
  plan(sourceOrAst) {
    const ast =
      typeof sourceOrAst === 'string' ? parseEql(sourceOrAst) : sourceOrAst;
    return planEql(ast);
  }

  /**
   * @param {string} source
   * @returns {Promise<import('./types').EqlResult>}
   */
  async query(source) {
    return this.executor.query(source);
  }

  /**
   * @param {import('./types').EqlPlan} plan
   * @returns {Promise<import('./types').EqlResult>}
   */
  async execute(plan) {
    return this.executor.execute(plan);
  }
}

/**
 * @param {object} [deps]
 * @returns {EqlEngine}
 */
function createEqlEngine(deps) {
  return new EqlEngine(deps);
}

module.exports = {
  EqlEngine,
  createEqlEngine,
  parseEql,
  tokenize,
  resolveTarget,
  EqlParseError,
  planEql,
  QueryPlanner,
  Executor,
  createExecutor,
  createEvidenceCatalog,
  catalogFromResult,
  matchesCondition,
  resolveField,
  EQL_RULES,
  STATEMENT_KINDS,
  MUTATION_KEYWORDS,
  TARGET_ALIASES,
  SUBJECT_FIELD_ALIASES,
  CONFIDENCE_FIELD_ALIASES,
  ID_FIELD_ALIASES,
};
