'use strict';

/**
 * Shared Max runtime for HTTP surfaces (Command Deck API).
 * Library-only wiring — starts without knowledge dual-write.
 * Singleton keeps in-memory snapshot/policy state stable across requests
 * within a process (empty until Scout/CRM sync is live).
 */

const { createMaxReasoningRuntime } = require('../packages/max');

let runtimePromise = null;

/**
 * @param {object} [options]
 * @param {boolean} [options.reset=false] - force a fresh runtime (tests)
 */
function getMaxRuntime(options = {}) {
  if (options.reset) {
    runtimePromise = null;
  }
  if (!runtimePromise) {
    runtimePromise = Promise.resolve(
      createMaxReasoningRuntime({
        withSync: false,
        startIngestor: false,
        tenantPolicies: options.tenantPolicies,
      })
    );
  }
  return runtimePromise;
}

module.exports = {
  getMaxRuntime,
};
