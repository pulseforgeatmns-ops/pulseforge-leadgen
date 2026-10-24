'use strict';

/**
 * SPEC-141 / SPEC-182 — Provider Capability Registry (delegates to unified registry).
 * Backward-compatible facade over coverage/ProviderCapabilityRegistry.js.
 */

const {
  EVIDENCE_CAPABILITIES,
  COST_TIER_ORDER,
  createDefaultUnifiedRegistry,
  createUnifiedProviderRegistry,
} = require('../coverage/ProviderCapabilityRegistry');
const { COST_TIERS } = require('./types');

/** @deprecated Use DEFAULT_PROVIDER_DEFINITIONS from coverage/ProviderCapabilityRegistry */
const DEFAULT_PROVIDERS = createDefaultUnifiedRegistry().list();

class ProviderCapabilityRegistry {
  constructor(providers) {
    this._unified = providers
      ? createUnifiedProviderRegistry(providers)
      : createDefaultUnifiedRegistry();
  }

  list() {
    return this._unified.list();
  }

  get(id) {
    return this._unified.get(id);
  }

  findByCapability(capability) {
    return this._unified.findByCapability(capability);
  }

  selectForCapabilities(requiredCapabilities, opts = {}) {
    return this._unified.selectForCapabilities(requiredCapabilities, opts);
  }

  selectForEvidenceType(evidenceType, opts = {}) {
    return this._unified.selectForEvidenceType(evidenceType, opts);
  }

  isAvailable(providerId, opts = {}) {
    return this._unified.isAvailable(providerId, opts);
  }
}

function createProviderCapabilityRegistry(providers) {
  return new ProviderCapabilityRegistry(providers);
}

function createDefaultProviderRegistry() {
  return new ProviderCapabilityRegistry();
}

module.exports = {
  EVIDENCE_CAPABILITIES,
  DEFAULT_PROVIDERS,
  COST_TIER_ORDER,
  COST_TIERS,
  ProviderCapabilityRegistry,
  createProviderCapabilityRegistry,
  createDefaultProviderRegistry,
};
