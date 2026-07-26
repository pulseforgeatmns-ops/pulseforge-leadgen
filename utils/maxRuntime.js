'use strict';

/**
 * Shared Max runtime for HTTP surfaces (Command Deck + Max Workspace).
 * Library-only wiring — starts without knowledge dual-write.
 * Singleton keeps in-memory snapshot/policy/workspace session state stable
 * across requests within a process (empty until Scout/CRM sync is live).
 */

const { createMaxReasoningRuntime } = require('../packages/max');

let runtimePromise = null;

/**
 * @param {object} [options]
 * @param {boolean} [options.reset=false] - force a fresh runtime (tests)
 * @param {boolean} [options.disableLlm] - force deterministic presentation
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
        // Workspace PresentationEngine attaches Anthropic at request time when keyed;
        // disableLlm forces deterministic prose for tests / locked environments.
        disableLlm: options.disableLlm === true,
      })
    );
  }
  return runtimePromise;
}

module.exports = {
  getMaxRuntime,
};
