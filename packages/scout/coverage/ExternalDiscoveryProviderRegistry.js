'use strict';

/**
 * SPEC-175 / SPEC-182 — External Discovery Provider Registry (delegates to unified registry).
 * Every discovery provider declares capability state before workloads execute.
 */

const { SOURCE_TYPES } = require('../../max/scoutAcquisition/Types');
const {
  PROVIDER_CAPABILITY,
  EVIDENCE_PRODUCING_SOURCE_TYPES,
  EXTERNAL_DISCOVERY_CAPABILITY_UNAVAILABLE,
  getDefaultUnifiedRegistry,
  isOperationalCapability,
  isEvidenceProducingCapability,
  availabilityToLegacyCapability,
  AVAILABILITY,
} = require('./ProviderCapabilityRegistry');

/** @deprecated Use DEFAULT_PROVIDER_DEFINITIONS from coverage/ProviderCapabilityRegistry */
const STATIC_PROVIDER_DEFINITIONS = getDefaultUnifiedRegistry()
  .buildDiscoveryRegistry()
  .map((row) => ({
    id: row.id === 'google_places' ? 'google_maps' : row.id,
    label: row.provider,
    sourceType: row.sourceType,
    producesCandidates: row.producesCandidates,
  }));

function evaluateAdapterCapability(adapter) {
  if (!adapter) return PROVIDER_CAPABILITY.UNAVAILABLE;
  const registry = getDefaultUnifiedRegistry();
  const match = registry.providers.find(
    (p) =>
      p.sourceType === adapter.sourceType ||
      (p.adapterIds || []).includes(adapter.id)
  );
  const sourceType = adapter.sourceType || SOURCE_TYPES.PUBLIC_BUSINESS_DATA;
  const isSocial = [
    SOURCE_TYPES.LINKEDIN,
    SOURCE_TYPES.FACEBOOK,
    SOURCE_TYPES.INSTAGRAM,
  ].includes(sourceType);

  if (typeof adapter.available === 'function' && adapter.available()) {
    return isSocial ? PROVIDER_CAPABILITY.DEGRADED : PROVIDER_CAPABILITY.AVAILABLE;
  }
  if (isSocial) return PROVIDER_CAPABILITY.STUB;
  if (adapter.id === 'injected_discover') return PROVIDER_CAPABILITY.UNAVAILABLE;
  if (match) {
    const availability = registry.resolveAvailability(match);
    return availabilityToLegacyCapability(availability);
  }
  return PROVIDER_CAPABILITY.UNAVAILABLE;
}

/**
 * Build the provider capability registry for the current runtime.
 * @param {object} [opts]
 * @returns {object[]}
 */
function buildProviderRegistry(opts = {}) {
  return getDefaultUnifiedRegistry().buildDiscoveryRegistry(opts);
}

/**
 * Resolve operational external discovery providers from runtime adapters.
 * @param {object[]} adapters
 * @returns {object[]}
 */
function resolveOperationalProvidersFromAdapters(adapters = []) {
  return getDefaultUnifiedRegistry().resolveOperationalProvidersFromAdapters(adapters);
}

/**
 * Whether at least one external provider can produce candidate evidence.
 * @param {object} input
 * @returns {boolean}
 */
function hasOperationalEvidenceProvider(input = {}) {
  return getDefaultUnifiedRegistry().hasOperationalEvidenceProvider(input);
}

module.exports = {
  PROVIDER_CAPABILITY,
  AVAILABILITY,
  EVIDENCE_PRODUCING_SOURCE_TYPES,
  EXTERNAL_DISCOVERY_CAPABILITY_UNAVAILABLE,
  STATIC_PROVIDER_DEFINITIONS,
  buildProviderRegistry,
  evaluateAdapterCapability,
  resolveOperationalProvidersFromAdapters,
  hasOperationalEvidenceProvider,
  isOperationalCapability,
  isEvidenceProducingCapability,
};
