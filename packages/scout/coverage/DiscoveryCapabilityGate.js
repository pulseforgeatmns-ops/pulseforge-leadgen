'use strict';

/**
 * SPEC-175 — Discovery Capability Gate (ADR-076 extension).
 * Coverage Plan → Capability Evaluation → Enough sensors? → Execute or Block.
 */

const { DISCOVERY_OUTCOMES } = require('../types');
const {
  buildProviderRegistry,
  resolveOperationalProvidersFromAdapters,
  EXTERNAL_DISCOVERY_CAPABILITY_UNAVAILABLE,
  hasOperationalEvidenceProvider,
} = require('./ExternalDiscoveryProviderRegistry');
const { getDefaultUnifiedRegistry } = require('./ProviderCapabilityRegistry');

const CAPABILITY_BLOCKER_CODE = 'external_discovery_capability_unavailable';

/**
 * Evaluate whether discovery can execute external workloads.
 * @param {object} input
 * @param {object[]} [input.adapters]
 * @param {object} [input.coveragePlan]
 * @param {boolean} [input.requireExternalDiscovery]
 * @returns {object}
 */
function evaluateDiscoveryCapability(input = {}) {
  const adapters = Array.isArray(input.adapters) ? input.adapters : [];
  const unifiedRegistry = input.registry || getDefaultUnifiedRegistry();
  const registry = buildProviderRegistry({ ...input, adapters, registry: unifiedRegistry });
  const operationalProviders = resolveOperationalProvidersFromAdapters(adapters);
  const enoughSensors =
    operationalProviders.length > 0 ||
    hasOperationalEvidenceProvider({ adapters, registry: unifiedRegistry, ...input });
  const requireExternal =
    input.requireExternalDiscovery !== false &&
    Boolean(
      input.coveragePlan &&
        (input.coveragePlan.totals?.searches > 0 ||
          (input.coveragePlan.sources || []).length > 0)
    );

  if (requireExternal && !enoughSensors) {
    return {
      canExecute: false,
      enoughSensors: false,
      registry,
      operationalProviders,
      blockReason: EXTERNAL_DISCOVERY_CAPABILITY_UNAVAILABLE,
      blockerCode: CAPABILITY_BLOCKER_CODE,
      explanation:
        'I cannot investigate this market because no external discovery provider is available.',
      outcome: DISCOVERY_OUTCOMES.BLOCKED,
    };
  }

  return {
    canExecute: true,
    enoughSensors,
    registry,
    operationalProviders,
    blockReason: null,
    blockerCode: null,
    explanation: enoughSensors
      ? `${operationalProviders.length || registry.filter((r) => r.evidenceProducing).length} external discovery provider(s) operational.`
      : 'External discovery not required for this investigation.',
    outcome: null,
  };
}

/**
 * Build a blocked discovery result when capability gate fails.
 * @param {object} evaluation
 * @param {object} [partial]
 * @returns {object}
 */
function buildCapabilityBlockedResult(evaluation, partial = {}) {
  return {
    blocked: true,
    outcome: DISCOVERY_OUTCOMES.BLOCKED,
    blockReason: evaluation.blockReason || EXTERNAL_DISCOVERY_CAPABILITY_UNAVAILABLE,
    blockerCode: evaluation.blockerCode || CAPABILITY_BLOCKER_CODE,
    explanation: evaluation.explanation,
    capabilityEvaluation: {
      registry: evaluation.registry,
      operationalProviders: (evaluation.operationalProviders || []).map((row) => ({
        provider: row.provider,
        capability: row.capability,
        sourceType: row.sourceType,
      })),
      enoughSensors: evaluation.enoughSensors === true,
      explanation: evaluation.explanation,
    },
    ...partial,
  };
}

module.exports = {
  CAPABILITY_BLOCKER_CODE,
  evaluateDiscoveryCapability,
  buildCapabilityBlockedResult,
};
