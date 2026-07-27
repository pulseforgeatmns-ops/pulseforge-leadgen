'use strict';

/**
 * Shared Max runtime for HTTP surfaces (Command Deck + Max Workspace).
 * SPEC-014: boots persistent Knowledge + dual-write when enabled.
 */

const { getKnowledgeBoot } = require('./knowledgeRuntime');

let runtimePromise = null;

/**
 * @param {object} [options]
 * @param {boolean} [options.reset=false] - force a fresh runtime (tests)
 * @param {boolean} [options.disableLlm] - force deterministic presentation
 * @param {boolean} [options.inMemory] - force in-memory knowledge (tests)
 */
function getMaxRuntime(options = {}) {
  if (options.reset) {
    runtimePromise = null;
  }
  if (!runtimePromise) {
    runtimePromise = getKnowledgeBoot({
      reset: options.reset,
      disableLlm: options.disableLlm,
      inMemory: options.inMemory,
      tenantPolicies: options.tenantPolicies,
      pool: options.pool,
    }).then((boot) => boot.max);
  }
  return runtimePromise;
}

module.exports = {
  getMaxRuntime,
};
