'use strict';

/**
 * SPEC-175 — External Discovery Provider Registry.
 * Every discovery provider declares capability state before workloads execute.
 * Mission planning can explain capability gaps instead of attempting doomed discovery.
 */

const { SOURCE_TYPES } = require('../../max/scoutAcquisition/Types');
const { createPlacesProvider } = require('../../capabilities/discovery/providers/PlacesProvider');

const PROVIDER_CAPABILITY = Object.freeze({
  AVAILABLE: 'AVAILABLE',
  DEGRADED: 'DEGRADED',
  UNAVAILABLE: 'UNAVAILABLE',
  STUB: 'STUB',
  NOT_IMPLEMENTED: 'NOT_IMPLEMENTED',
});

/** Providers that can produce external market candidates (not CRM-only). */
const EVIDENCE_PRODUCING_SOURCE_TYPES = Object.freeze([
  SOURCE_TYPES.PUBLIC_BUSINESS_DATA,
  SOURCE_TYPES.COMPANY_WEBSITES,
  SOURCE_TYPES.ENRICHMENT_PROVIDER,
]);

const EXTERNAL_DISCOVERY_CAPABILITY_UNAVAILABLE =
  'External Discovery Capability Unavailable';

const STATIC_PROVIDER_DEFINITIONS = Object.freeze([
  {
    id: 'google_places',
    label: 'Google Places',
    sourceType: SOURCE_TYPES.PUBLIC_BUSINESS_DATA,
    adapterIds: ['public_business_places', 'injected_discover'],
    producesCandidates: true,
    defaultCapability: PROVIDER_CAPABILITY.UNAVAILABLE,
    evaluate(opts = {}) {
      if (typeof opts.discover === 'function') {
        return PROVIDER_CAPABILITY.AVAILABLE;
      }
      const provider =
        opts.placesProvider ||
        createPlacesProvider({
          apiKey: opts.apiKey || process.env.GOOGLE_PLACES_KEY || '',
          fetchImpl: opts.fetchImpl,
        });
      if (provider && typeof provider.available === 'function' && provider.available()) {
        return PROVIDER_CAPABILITY.AVAILABLE;
      }
      if (opts.enablePlaces === false) {
        return PROVIDER_CAPABILITY.UNAVAILABLE;
      }
      return PROVIDER_CAPABILITY.UNAVAILABLE;
    },
  },
  {
    id: 'linkedin',
    label: 'LinkedIn',
    sourceType: SOURCE_TYPES.LINKEDIN,
    adapterIds: ['linkedin'],
    producesCandidates: false,
    defaultCapability: PROVIDER_CAPABILITY.STUB,
    evaluate() {
      return Boolean(process.env.LINKEDIN_SESSION)
        ? PROVIDER_CAPABILITY.DEGRADED
        : PROVIDER_CAPABILITY.STUB;
    },
  },
  {
    id: 'facebook',
    label: 'Facebook',
    sourceType: SOURCE_TYPES.FACEBOOK,
    adapterIds: ['facebook'],
    producesCandidates: false,
    defaultCapability: PROVIDER_CAPABILITY.STUB,
    evaluate() {
      return Boolean(process.env.FACEBOOK_SESSION)
        ? PROVIDER_CAPABILITY.DEGRADED
        : PROVIDER_CAPABILITY.STUB;
    },
  },
  {
    id: 'instagram',
    label: 'Instagram',
    sourceType: SOURCE_TYPES.INSTAGRAM,
    adapterIds: ['instagram'],
    producesCandidates: false,
    defaultCapability: PROVIDER_CAPABILITY.STUB,
    evaluate() {
      return PROVIDER_CAPABILITY.STUB;
    },
  },
  {
    id: 'airbnb',
    label: 'Airbnb',
    sourceType: 'airbnb_listings',
    adapterIds: [],
    producesCandidates: true,
    defaultCapability: PROVIDER_CAPABILITY.NOT_IMPLEMENTED,
    evaluate() {
      return PROVIDER_CAPABILITY.NOT_IMPLEMENTED;
    },
  },
  {
    id: 'vrbo',
    label: 'VRBO',
    sourceType: 'vrbo_listings',
    adapterIds: [],
    producesCandidates: true,
    defaultCapability: PROVIDER_CAPABILITY.NOT_IMPLEMENTED,
    evaluate() {
      return PROVIDER_CAPABILITY.NOT_IMPLEMENTED;
    },
  },
]);

function isOperationalCapability(capability) {
  return (
    capability === PROVIDER_CAPABILITY.AVAILABLE ||
    capability === PROVIDER_CAPABILITY.DEGRADED
  );
}

function isEvidenceProducingCapability(capability) {
  return isOperationalCapability(capability);
}

function evaluateAdapterCapability(adapter) {
  if (!adapter) return PROVIDER_CAPABILITY.UNAVAILABLE;
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
  return PROVIDER_CAPABILITY.UNAVAILABLE;
}

/**
 * Build the provider capability registry for the current runtime.
 * @param {object} [opts]
 * @returns {object[]}
 */
function buildProviderRegistry(opts = {}) {
  const adapters = Array.isArray(opts.adapters) ? opts.adapters : [];
  const adapterById = new Map(
    adapters.filter(Boolean).map((adapter) => [adapter.id, adapter])
  );

  return STATIC_PROVIDER_DEFINITIONS.map((def) => {
    const matchedAdapter = (def.adapterIds || [])
      .map((id) => adapterById.get(id))
      .find(Boolean);
    let capability = def.evaluate(opts);
    if (matchedAdapter) {
      const adapterCapability = evaluateAdapterCapability(matchedAdapter);
      if (isOperationalCapability(adapterCapability)) {
        capability = adapterCapability;
      } else if (
        capability === PROVIDER_CAPABILITY.UNAVAILABLE &&
        adapterCapability === PROVIDER_CAPABILITY.STUB
      ) {
        capability = adapterCapability;
      }
    }
    return {
      provider: def.label,
      id: def.id,
      sourceType: def.sourceType,
      capability,
      producesCandidates: def.producesCandidates === true,
      operational: isOperationalCapability(capability),
      evidenceProducing:
        def.producesCandidates === true && isEvidenceProducingCapability(capability),
    };
  });
}

/**
 * Resolve operational external discovery providers from runtime adapters.
 * @param {object[]} adapters
 * @returns {object[]}
 */
function resolveOperationalProvidersFromAdapters(adapters = []) {
  const marketAdapters = (adapters || []).filter(
    (row) => row && row.id !== 'existing_pf' && row.sourceType !== SOURCE_TYPES.EXISTING_PF
  );
  const operational = [];
  for (const adapter of marketAdapters) {
    const capability = evaluateAdapterCapability(adapter);
    const producesCandidates = EVIDENCE_PRODUCING_SOURCE_TYPES.includes(adapter.sourceType);
    if (producesCandidates && isOperationalCapability(capability)) {
      operational.push({
        provider: adapter.id || adapter.sourceType,
        id: adapter.id || adapter.sourceType,
        sourceType: adapter.sourceType,
        capability,
        adapter,
      });
    }
  }
  return operational;
}

/**
 * Whether at least one external provider can produce candidate evidence.
 * @param {object} input
 * @returns {boolean}
 */
function hasOperationalEvidenceProvider(input = {}) {
  const adapters = input.adapters || [];
  if (resolveOperationalProvidersFromAdapters(adapters).length > 0) {
    return true;
  }
  const registry = buildProviderRegistry(input);
  return registry.some((row) => row.evidenceProducing === true);
}

module.exports = {
  PROVIDER_CAPABILITY,
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
