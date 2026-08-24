'use strict';

/**
 * @pulseforge/cog — Cognitive Evaluation Framework
 *
 * Permanent benchmark for Max reasoning maturity.
 * Knowledge and cognition are different. COG evaluates cognition.
 *
 * Layer order (scoring is last):
 *   Domains → Conversations → Behaviors → Failures → Results → Reports → Scoring
 */

const types = require('./types');
const failures = require('./failures');
const domains = require('./domains');
const suites = require('./suites');
const conversations = require('./conversations');
const behaviors = require('./behaviors/BehaviorChecker');
const results = require('./results/ResultStore');
const reports = require('./reports');
const scoring = require('./scoring');
const { CogEngine, createCogEngine, createStubAskFn, createMaxAskFn } = require('./engine/CogEngine');

module.exports = {
  ...types,
  ...failures,
  ...domains,
  ...suites,
  ...conversations,
  ...behaviors,
  ...results,
  ...reports,
  ...scoring,
  CogEngine,
  createCogEngine,
  createStubAskFn,
  createMaxAskFn,
};
