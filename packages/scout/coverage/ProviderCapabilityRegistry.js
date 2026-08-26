'use strict';

/**
 * SPEC-182 — Unified Provider Capability Registry (ADR-097).
 * Providers are capability engines: each advertises evidence types it can collect.
 * Planners ask "who can answer this question?" via dynamic capability matching —
 * never hardcoded provider lists.
 */

const { SOURCE_TYPES } = require('../../max/scoutAcquisition/Types');
const { COST_TIERS } = require('../intelligence/types');
const { INVESTIGATIVE_EVIDENCE } = require('./EvidenceRequirements');
const { createPlacesProvider } = require('../../capabilities/discovery/providers/PlacesProvider');

/** Low-level evidence capabilities (SPEC-141). */
const EVIDENCE_CAPABILITIES = Object.freeze({
  BUSINESSES: 'businesses',
  REVIEWS: 'reviews',
  PHONE: 'phone',
  HOURS: 'hours',
  WEBSITE: 'website',
  PEOPLE: 'people',
  OWNERSHIP: 'ownership',
  GROWTH: 'growth',
  HIRING: 'hiring',
  EMAILS: 'emails',
  VERIFICATION: 'verification',
  CONTACTS: 'contacts',
  TITLES: 'titles',
  ENRICHMENT: 'enrichment',
  PROPERTY_COUNT: 'property_count',
  BUYING_SIGNALS: 'buying_signals',
  NEWS: 'news',
  COUNTY_RECORDS: 'county_records',
  SOCIAL: 'social',
  LICENSING: 'licensing',
  FINANCIAL: 'financial_signals',
});

const AVAILABILITY = Object.freeze({
  AVAILABLE: 'available',
  UNAVAILABLE: 'unavailable',
  STUB: 'stub',
});

/** Legacy SPEC-175 capability states (for ExternalDiscoveryProviderRegistry compat). */
const PROVIDER_CAPABILITY = Object.freeze({
  AVAILABLE: 'AVAILABLE',
  DEGRADED: 'DEGRADED',
  UNAVAILABLE: 'UNAVAILABLE',
  STUB: 'STUB',
  NOT_IMPLEMENTED: 'NOT_IMPLEMENTED',
});

const COST_TIER_ORDER = Object.freeze([
  COST_TIERS.FREE,
  COST_TIERS.CACHED,
  COST_TIERS.LOCAL,
  COST_TIERS.PAID,
]);

/** Investigative evidence type → low-level capability. */
const EVIDENCE_TO_CAPABILITY = Object.freeze({
  [INVESTIGATIVE_EVIDENCE.IDENTITY]: EVIDENCE_CAPABILITIES.BUSINESSES,
  [INVESTIGATIVE_EVIDENCE.DECISION_MAKERS]: EVIDENCE_CAPABILITIES.PEOPLE,
  [INVESTIGATIVE_EVIDENCE.REVIEWS]: EVIDENCE_CAPABILITIES.REVIEWS,
  [INVESTIGATIVE_EVIDENCE.PORTFOLIO]: EVIDENCE_CAPABILITIES.PROPERTY_COUNT,
  [INVESTIGATIVE_EVIDENCE.GROWTH]: EVIDENCE_CAPABILITIES.GROWTH,
  [INVESTIGATIVE_EVIDENCE.CLEANING]: EVIDENCE_CAPABILITIES.WEBSITE,
  [INVESTIGATIVE_EVIDENCE.LICENSING]: EVIDENCE_CAPABILITIES.COUNTY_RECORDS,
  [INVESTIGATIVE_EVIDENCE.SOCIAL]: EVIDENCE_CAPABILITIES.SOCIAL,
  [INVESTIGATIVE_EVIDENCE.CONTACT]: EVIDENCE_CAPABILITIES.CONTACTS,
  [INVESTIGATIVE_EVIDENCE.BUYING]: EVIDENCE_CAPABILITIES.BUYING_SIGNALS,
});

const EVIDENCE_PRODUCING_SOURCE_TYPES = Object.freeze([
  SOURCE_TYPES.PUBLIC_BUSINESS_DATA,
  SOURCE_TYPES.COMPANY_WEBSITES,
  SOURCE_TYPES.ENRICHMENT_PROVIDER,
]);

const EXTERNAL_DISCOVERY_CAPABILITY_UNAVAILABLE =
  'External Discovery Capability Unavailable';

function deriveEvidenceTypesFromCapabilities(capabilities = []) {
  const types = new Set();
  for (const cap of capabilities) {
    for (const [evidenceType, mappedCap] of Object.entries(EVIDENCE_TO_CAPABILITY)) {
      if (mappedCap === cap) types.add(evidenceType);
    }
    if (cap === EVIDENCE_CAPABILITIES.OWNERSHIP) {
      types.add(INVESTIGATIVE_EVIDENCE.DECISION_MAKERS);
    }
    if (cap === EVIDENCE_CAPABILITIES.HIRING) {
      types.add(INVESTIGATIVE_EVIDENCE.GROWTH);
    }
    if (cap === EVIDENCE_CAPABILITIES.NEWS) {
      types.add(INVESTIGATIVE_EVIDENCE.GROWTH);
      types.add(INVESTIGATIVE_EVIDENCE.BUYING);
    }
    if (cap === EVIDENCE_CAPABILITIES.EMAILS || cap === EVIDENCE_CAPABILITIES.VERIFICATION) {
      types.add(INVESTIGATIVE_EVIDENCE.CONTACT);
    }
    if (cap === EVIDENCE_CAPABILITIES.COUNTY_RECORDS) {
      types.add(INVESTIGATIVE_EVIDENCE.IDENTITY);
      types.add(INVESTIGATIVE_EVIDENCE.LICENSING);
    }
    if (cap === EVIDENCE_CAPABILITIES.SOCIAL) {
      types.add(INVESTIGATIVE_EVIDENCE.SOCIAL);
    }
    if (cap === EVIDENCE_CAPABILITIES.FINANCIAL) {
      types.add(INVESTIGATIVE_EVIDENCE.BUYING);
    }
  }
  return [...types];
}

function buildProviderEntry(partial = {}) {
  const capabilities = Array.isArray(partial.capabilities) ? partial.capabilities : [];
  const evidenceTypes =
    Array.isArray(partial.evidenceTypes) && partial.evidenceTypes.length > 0
      ? partial.evidenceTypes
      : deriveEvidenceTypesFromCapabilities(capabilities);

  const reliability = partial.reliability != null ? Number(partial.reliability) : 0.7;
  const coverage = partial.coverage != null ? Number(partial.coverage) : 0.5;

  return Object.freeze({
    id: partial.id,
    providerId: partial.id,
    label: partial.label || partial.id,
    capabilities,
    evidenceTypes,
    sourceType: partial.sourceType || null,
    adapterIds: Array.isArray(partial.adapterIds) ? partial.adapterIds : [],
    producesCandidates: partial.producesCandidates === true,
    costTier: partial.costTier || COST_TIERS.PAID,
    confidenceGain: partial.confidenceGain != null ? Number(partial.confidenceGain) : reliability * coverage,
    latencyMs: partial.latencyMs != null ? Number(partial.latencyMs) : 1000,
    freshnessHours: partial.freshnessHours != null ? Number(partial.freshnessHours) : 168,
    coverage,
    reliability,
    limitations: Array.isArray(partial.limitations) ? partial.limitations : [],
    unavailableReason: partial.unavailableReason || null,
    externalDiscovery: partial.externalDiscovery === true,
    available: partial.available || (() => true),
    evaluateAvailability: partial.evaluateAvailability || null,
  });
}

const DEFAULT_PROVIDER_DEFINITIONS = Object.freeze([
  buildProviderEntry({
    id: 'existing_pf',
    label: 'PulseForge Repository',
    capabilities: [
      EVIDENCE_CAPABILITIES.BUSINESSES,
      EVIDENCE_CAPABILITIES.PEOPLE,
      EVIDENCE_CAPABILITIES.EMAILS,
      EVIDENCE_CAPABILITIES.CONTACTS,
    ],
    evidenceTypes: [INVESTIGATIVE_EVIDENCE.IDENTITY, INVESTIGATIVE_EVIDENCE.DECISION_MAKERS, INVESTIGATIVE_EVIDENCE.CONTACT],
    sourceType: SOURCE_TYPES.EXISTING_PF,
    adapterIds: ['existing_pf'],
    producesCandidates: false,
    costTier: COST_TIERS.CACHED,
    latencyMs: 50,
    freshnessHours: 168,
    coverage: 0.6,
    reliability: 0.95,
    available: () => true,
  }),
  buildProviderEntry({
    id: 'google_maps',
    label: 'Google Maps',
    externalDiscovery: true,
    capabilities: [
      EVIDENCE_CAPABILITIES.BUSINESSES,
      EVIDENCE_CAPABILITIES.REVIEWS,
      EVIDENCE_CAPABILITIES.PHONE,
      EVIDENCE_CAPABILITIES.HOURS,
      EVIDENCE_CAPABILITIES.WEBSITE,
    ],
    evidenceTypes: [
      INVESTIGATIVE_EVIDENCE.IDENTITY,
      INVESTIGATIVE_EVIDENCE.REVIEWS,
      INVESTIGATIVE_EVIDENCE.CONTACT,
      INVESTIGATIVE_EVIDENCE.CLEANING,
    ],
    sourceType: SOURCE_TYPES.PUBLIC_BUSINESS_DATA,
    adapterIds: ['public_business_places', 'injected_discover'],
    producesCandidates: true,
    costTier: COST_TIERS.PAID,
    latencyMs: 800,
    freshnessHours: 24,
    coverage: 0.85,
    reliability: 0.9,
    available: () => Boolean(process.env.GOOGLE_PLACES_KEY),
    evaluateAvailability(opts = {}) {
      if (typeof opts.discover === 'function') return AVAILABILITY.AVAILABLE;
      const provider =
        opts.placesProvider ||
        createPlacesProvider({
          apiKey: opts.apiKey || process.env.GOOGLE_PLACES_KEY || '',
          fetchImpl: opts.fetchImpl,
        });
      if (provider && typeof provider.available === 'function' && provider.available()) {
        return AVAILABILITY.AVAILABLE;
      }
      return AVAILABILITY.UNAVAILABLE;
    },
  }),
  buildProviderEntry({
    id: 'linkedin',
    label: 'LinkedIn',
    externalDiscovery: true,
    capabilities: [
      EVIDENCE_CAPABILITIES.PEOPLE,
      EVIDENCE_CAPABILITIES.OWNERSHIP,
      EVIDENCE_CAPABILITIES.GROWTH,
      EVIDENCE_CAPABILITIES.HIRING,
    ],
    evidenceTypes: [
      INVESTIGATIVE_EVIDENCE.DECISION_MAKERS,
      INVESTIGATIVE_EVIDENCE.GROWTH,
      INVESTIGATIVE_EVIDENCE.BUYING,
    ],
    sourceType: SOURCE_TYPES.LINKEDIN,
    adapterIds: ['linkedin'],
    producesCandidates: false,
    costTier: COST_TIERS.PAID,
    latencyMs: 1200,
    freshnessHours: 72,
    coverage: 0.5,
    reliability: 0.75,
    available: () => Boolean(process.env.LINKEDIN_SESSION),
    limitations: ['Requires authenticated session'],
  }),
  buildProviderEntry({
    id: 'hunter',
    label: 'Hunter',
    capabilities: [EVIDENCE_CAPABILITIES.EMAILS, EVIDENCE_CAPABILITIES.VERIFICATION],
    evidenceTypes: [INVESTIGATIVE_EVIDENCE.CONTACT],
    sourceType: SOURCE_TYPES.ENRICHMENT_PROVIDER,
    adapterIds: ['hunter'],
    costTier: COST_TIERS.PAID,
    latencyMs: 600,
    coverage: 0.7,
    reliability: 0.85,
    available: () => Boolean(process.env.HUNTER_API_KEY),
  }),
  buildProviderEntry({
    id: 'prospeo',
    label: 'Prospeo',
    capabilities: [
      EVIDENCE_CAPABILITIES.CONTACTS,
      EVIDENCE_CAPABILITIES.TITLES,
      EVIDENCE_CAPABILITIES.ENRICHMENT,
      EVIDENCE_CAPABILITIES.PHONE,
    ],
    evidenceTypes: [INVESTIGATIVE_EVIDENCE.DECISION_MAKERS, INVESTIGATIVE_EVIDENCE.CONTACT],
    sourceType: SOURCE_TYPES.ENRICHMENT_PROVIDER,
    adapterIds: ['prospeo'],
    costTier: COST_TIERS.PAID,
    latencyMs: 700,
    coverage: 0.65,
    reliability: 0.8,
    available: () => Boolean(process.env.PROSPEO_API_KEY),
  }),
  buildProviderEntry({
    id: 'apollo',
    label: 'Apollo',
    capabilities: [
      EVIDENCE_CAPABILITIES.CONTACTS,
      EVIDENCE_CAPABILITIES.TITLES,
      EVIDENCE_CAPABILITIES.ENRICHMENT,
    ],
    evidenceTypes: [INVESTIGATIVE_EVIDENCE.DECISION_MAKERS, INVESTIGATIVE_EVIDENCE.CONTACT],
    sourceType: SOURCE_TYPES.ENRICHMENT_PROVIDER,
    costTier: COST_TIERS.PAID,
    latencyMs: 900,
    coverage: 0.6,
    reliability: 0.78,
    available: () => false,
    unavailableReason: 'Apollo integration not configured',
  }),
  buildProviderEntry({
    id: 'website',
    label: 'Business Websites',
    capabilities: [
      EVIDENCE_CAPABILITIES.WEBSITE,
      EVIDENCE_CAPABILITIES.BUSINESSES,
      EVIDENCE_CAPABILITIES.PROPERTY_COUNT,
    ],
    evidenceTypes: [
      INVESTIGATIVE_EVIDENCE.IDENTITY,
      INVESTIGATIVE_EVIDENCE.PORTFOLIO,
      INVESTIGATIVE_EVIDENCE.CLEANING,
      INVESTIGATIVE_EVIDENCE.DECISION_MAKERS,
      INVESTIGATIVE_EVIDENCE.REVIEWS,
    ],
    sourceType: SOURCE_TYPES.COMPANY_WEBSITES,
    adapterIds: ['company_websites'],
    producesCandidates: true,
    costTier: COST_TIERS.FREE,
    latencyMs: 1500,
    coverage: 0.55,
    reliability: 0.7,
    available: () => true,
  }),
  buildProviderEntry({
    id: 'news',
    label: 'News',
    capabilities: [EVIDENCE_CAPABILITIES.BUYING_SIGNALS, EVIDENCE_CAPABILITIES.NEWS],
    evidenceTypes: [INVESTIGATIVE_EVIDENCE.GROWTH, INVESTIGATIVE_EVIDENCE.BUYING],
    sourceType: SOURCE_TYPES.ENRICHMENT_PROVIDER,
    costTier: COST_TIERS.PAID,
    latencyMs: 2000,
    coverage: 0.4,
    reliability: 0.65,
    available: () => Boolean(process.env.SERPAPI_KEY),
  }),
  buildProviderEntry({
    id: 'county_records',
    label: 'County Records',
    capabilities: [EVIDENCE_CAPABILITIES.COUNTY_RECORDS, EVIDENCE_CAPABILITIES.PROPERTY_COUNT],
    evidenceTypes: [
      INVESTIGATIVE_EVIDENCE.IDENTITY,
      INVESTIGATIVE_EVIDENCE.PORTFOLIO,
      INVESTIGATIVE_EVIDENCE.LICENSING,
    ],
    sourceType: SOURCE_TYPES.PUBLIC_BUSINESS_DATA,
    costTier: COST_TIERS.LOCAL,
    latencyMs: 3000,
    coverage: 0.35,
    reliability: 0.6,
    available: () => false,
    unavailableReason: 'County records integration not configured',
  }),
  buildProviderEntry({
    id: 'facebook',
    label: 'Facebook',
    externalDiscovery: true,
    capabilities: [EVIDENCE_CAPABILITIES.SOCIAL, EVIDENCE_CAPABILITIES.BUSINESSES],
    evidenceTypes: [INVESTIGATIVE_EVIDENCE.SOCIAL],
    sourceType: SOURCE_TYPES.FACEBOOK,
    adapterIds: ['facebook'],
    producesCandidates: false,
    costTier: COST_TIERS.PAID,
    latencyMs: 1500,
    coverage: 0.4,
    reliability: 0.6,
    available: () => Boolean(process.env.FACEBOOK_SESSION),
    limitations: ['Social scraping — degraded reliability'],
  }),
  buildProviderEntry({
    id: 'instagram',
    label: 'Instagram',
    externalDiscovery: true,
    capabilities: [EVIDENCE_CAPABILITIES.SOCIAL],
    evidenceTypes: [INVESTIGATIVE_EVIDENCE.SOCIAL],
    sourceType: SOURCE_TYPES.INSTAGRAM,
    adapterIds: ['instagram'],
    producesCandidates: false,
    costTier: COST_TIERS.PAID,
    latencyMs: 1500,
    coverage: 0.35,
    reliability: 0.55,
    available: () => false,
    unavailableReason: 'Instagram integration stub',
    limitations: ['Stub — not yet operational'],
  }),
  buildProviderEntry({
    id: 'airbnb',
    label: 'Airbnb',
    externalDiscovery: true,
    capabilities: [EVIDENCE_CAPABILITIES.PROPERTY_COUNT],
    evidenceTypes: [INVESTIGATIVE_EVIDENCE.PORTFOLIO],
    sourceType: 'airbnb_listings',
    producesCandidates: true,
    costTier: COST_TIERS.PAID,
    coverage: 0.3,
    reliability: 0.5,
    available: () => false,
    unavailableReason: 'Airbnb integration not implemented',
  }),
  buildProviderEntry({
    id: 'vrbo',
    label: 'VRBO',
    externalDiscovery: true,
    capabilities: [EVIDENCE_CAPABILITIES.PROPERTY_COUNT],
    evidenceTypes: [INVESTIGATIVE_EVIDENCE.PORTFOLIO],
    sourceType: 'vrbo_listings',
    producesCandidates: true,
    costTier: COST_TIERS.PAID,
    coverage: 0.3,
    reliability: 0.5,
    available: () => false,
    unavailableReason: 'VRBO integration not implemented',
  }),
]);

function availabilityToLegacyCapability(availability, provider) {
  if (provider?.unavailableReason?.includes('not implemented')) {
    return PROVIDER_CAPABILITY.NOT_IMPLEMENTED;
  }
  if (availability === AVAILABILITY.AVAILABLE) {
    if (provider?.limitations?.some((l) => l.toLowerCase().includes('degraded') || l.toLowerCase().includes('social scraping'))) {
      return PROVIDER_CAPABILITY.DEGRADED;
    }
    return PROVIDER_CAPABILITY.AVAILABLE;
  }
  if (availability === AVAILABILITY.STUB) return PROVIDER_CAPABILITY.STUB;
  if (provider?.id === 'instagram') return PROVIDER_CAPABILITY.STUB;
  if (provider?.id === 'linkedin' && !provider.available()) return PROVIDER_CAPABILITY.STUB;
  if (['airbnb', 'vrbo'].includes(provider?.id)) return PROVIDER_CAPABILITY.NOT_IMPLEMENTED;
  return PROVIDER_CAPABILITY.UNAVAILABLE;
}

function isOperationalAvailability(availability) {
  return availability === AVAILABILITY.AVAILABLE;
}

function isOperationalCapability(capability) {
  return (
    capability === PROVIDER_CAPABILITY.AVAILABLE ||
    capability === PROVIDER_CAPABILITY.DEGRADED
  );
}

function isEvidenceProducingCapability(capability) {
  return isOperationalCapability(capability);
}

class UnifiedProviderCapabilityRegistry {
  constructor(providers = DEFAULT_PROVIDER_DEFINITIONS) {
    this.providers = providers.map((p) => ({ ...p }));
    this._aliasMap = new Map();
    this._aliasMap.set('google_places', 'google_maps');
  }

  /**
   * Register a new provider with advertised capabilities.
   * New providers are automatically considered by planners — no planner changes required.
   * @param {object} definition
   * @returns {object}
   */
  register(definition) {
    const entry = buildProviderEntry(definition);
    const existing = this.providers.findIndex((p) => p.id === entry.id);
    if (existing >= 0) {
      this.providers[existing] = entry;
    } else {
      this.providers.push(entry);
    }
    return entry;
  }

  list() {
    return this.providers.slice();
  }

  get(id) {
    const resolved = this._aliasMap.get(id) || id;
    return this.providers.find((p) => p.id === resolved) || null;
  }

  getSourceType(providerId) {
    const meta = this.get(providerId);
    return meta?.sourceType || null;
  }

  resolveAvailability(provider, opts = {}) {
    if (provider.id === 'instagram') return AVAILABILITY.STUB;
    if (provider.id === 'linkedin' && typeof provider.available === 'function' && !provider.available()) {
      return AVAILABILITY.STUB;
    }
    if (['airbnb', 'vrbo'].includes(provider.id)) return AVAILABILITY.UNAVAILABLE;
    if (typeof provider.evaluateAvailability === 'function') {
      return provider.evaluateAvailability(opts);
    }
    if (typeof provider.available === 'function') {
      const ok = provider.available(opts);
      if (!ok) {
        if (provider.unavailableReason && provider.limitations?.includes('Stub — not yet operational')) {
          return AVAILABILITY.STUB;
        }
        return provider.unavailableReason ? AVAILABILITY.UNAVAILABLE : AVAILABILITY.UNAVAILABLE;
      }
      if (provider.limitations?.some((l) => l.includes('degraded') || l.includes('Stub'))) {
        return AVAILABILITY.AVAILABLE;
      }
      return AVAILABILITY.AVAILABLE;
    }
    return AVAILABILITY.AVAILABLE;
  }

  isAvailable(providerId, opts = {}) {
    const meta = this.get(providerId);
    if (!meta) return false;
    return this.resolveAvailability(meta, opts) === AVAILABILITY.AVAILABLE;
  }

  /** Find providers that advertise a low-level capability (SPEC-141 compat). */
  findByCapability(capability) {
    return this.providers.filter((p) => (p.capabilities || []).includes(capability));
  }

  /** Find providers that can collect an investigative evidence type (SPEC-182). */
  findByEvidenceType(evidenceType) {
    return this.providers.filter((p) => (p.evidenceTypes || []).includes(evidenceType));
  }

  /**
   * Who can answer this evidence question? Dynamic capability matching.
   * @param {string} evidenceType — INVESTIGATIVE_EVIDENCE value
   * @param {object} [opts]
   * @returns {object[]}
   */
  selectForEvidenceType(evidenceType, opts = {}) {
    const capability = EVIDENCE_TO_CAPABILITY[evidenceType];
    const byEvidence = this.findByEvidenceType(evidenceType);
    const byCapability = capability ? this.findByCapability(capability) : [];
    const merged = new Map();
    for (const p of [...byEvidence, ...byCapability]) {
      merged.set(p.id, p);
    }

    return [...merged.values()]
      .map((provider) => ({
        provider,
        availability: this.resolveAvailability(provider, opts),
      }))
      .filter((row) => opts.includeUnavailable || row.availability === AVAILABILITY.AVAILABLE)
      .sort((a, b) => {
        const availA = a.availability === AVAILABILITY.AVAILABLE ? 0 : 1;
        const availB = b.availability === AVAILABILITY.AVAILABLE ? 0 : 1;
        if (availA !== availB) return availA - availB;
        const tierA = COST_TIER_ORDER.indexOf(a.provider.costTier);
        const tierB = COST_TIER_ORDER.indexOf(b.provider.costTier);
        if (tierA !== tierB) return tierA - tierB;
        const confA = a.provider.confidenceGain || 0;
        const confB = b.provider.confidenceGain || 0;
        if (confB !== confA) return confB - confA;
        return (a.provider.latencyMs || 9999) - (b.provider.latencyMs || 9999);
      })
      .map((row) => row.provider);
  }

  /**
   * Select providers for low-level capabilities (SPEC-141 compat).
   * @param {string[]} requiredCapabilities
   * @param {object} [opts]
   * @returns {object[]}
   */
  selectForCapabilities(requiredCapabilities, opts = {}) {
    const selected = [];
    const seen = new Set();

    for (const cap of requiredCapabilities || []) {
      const candidates = this.findByCapability(cap)
        .filter((p) => opts.includeUnavailable || this.isAvailable(p.id, opts))
        .sort((a, b) => {
          const tierA = COST_TIER_ORDER.indexOf(a.costTier);
          const tierB = COST_TIER_ORDER.indexOf(b.costTier);
          if (tierA !== tierB) return tierA - tierB;
          return (a.latencyMs || 9999) - (b.latencyMs || 9999);
        });

      for (const provider of candidates) {
        if (seen.has(provider.id)) continue;
        seen.add(provider.id);
        selected.push({
          providerId: provider.id,
          label: provider.label,
          capability: cap,
          costTier: provider.costTier,
          latencyMs: provider.latencyMs,
          reliability: provider.reliability,
        });
        if (!opts.allowMultiplePerCapability) break;
      }
    }

    return selected.sort(
      (a, b) => COST_TIER_ORDER.indexOf(a.costTier) - COST_TIER_ORDER.indexOf(b.costTier)
    );
  }

  /**
   * Build legacy external-discovery registry rows (SPEC-175 compat).
   * @param {object} [opts]
   * @returns {object[]}
   */
  buildDiscoveryRegistry(opts = {}) {
    const adapters = Array.isArray(opts.adapters) ? opts.adapters : [];
    const adapterById = new Map(
      adapters.filter(Boolean).map((adapter) => [adapter.id, adapter])
    );

    return this.providers
      .filter((p) => p.externalDiscovery === true)
      .map((def) => {
        const matchedAdapter = (def.adapterIds || [])
          .map((id) => adapterById.get(id))
          .find(Boolean);
        let availability = this.resolveAvailability(def, opts);
        if (matchedAdapter && typeof matchedAdapter.available === 'function') {
          if (matchedAdapter.available()) {
            availability = AVAILABILITY.AVAILABLE;
          }
        }
        const legacyCapability = availabilityToLegacyCapability(availability, def);

        return {
          provider: def.label,
          id: def.id === 'google_maps' ? 'google_places' : def.id,
          sourceType: def.sourceType,
          capability: legacyCapability,
          producesCandidates: def.producesCandidates === true,
          operational: isOperationalCapability(legacyCapability),
          evidenceProducing:
            def.producesCandidates === true && isOperationalCapability(legacyCapability),
        };
      });
  }

  /**
   * Resolve operational providers from runtime adapters.
   * @param {object[]} adapters
   * @returns {object[]}
   */
  resolveOperationalProvidersFromAdapters(adapters = []) {
    const marketAdapters = (adapters || []).filter(
      (row) => row && row.id !== 'existing_pf' && row.sourceType !== SOURCE_TYPES.EXISTING_PF
    );
    const operational = [];
    for (const adapter of marketAdapters) {
      const registryMatch = this.providers.find(
        (p) =>
          p.sourceType === adapter.sourceType ||
          (p.adapterIds || []).includes(adapter.id)
      );
      const producesCandidates =
        registryMatch?.producesCandidates ||
        EVIDENCE_PRODUCING_SOURCE_TYPES.includes(adapter.sourceType);
      const available =
        typeof adapter.available === 'function' ? adapter.available() : true;
      if (producesCandidates && available) {
        operational.push({
          provider: adapter.id || adapter.sourceType,
          id: adapter.id || adapter.sourceType,
          sourceType: adapter.sourceType,
          capability: PROVIDER_CAPABILITY.AVAILABLE,
          adapter,
        });
      }
    }
    return operational;
  }

  hasOperationalEvidenceProvider(opts = {}) {
    const adapters = opts.adapters || [];
    if (this.resolveOperationalProvidersFromAdapters(adapters).length > 0) {
      return true;
    }
    return this.buildDiscoveryRegistry(opts).some((row) => row.evidenceProducing === true);
  }
}

let defaultRegistry = null;

function createUnifiedProviderRegistry(providers) {
  return new UnifiedProviderCapabilityRegistry(providers);
}

function createDefaultUnifiedRegistry() {
  return new UnifiedProviderCapabilityRegistry();
}

function getDefaultUnifiedRegistry() {
  if (!defaultRegistry) {
    defaultRegistry = createDefaultUnifiedRegistry();
  }
  return defaultRegistry;
}

function resetDefaultUnifiedRegistry() {
  defaultRegistry = null;
}

module.exports = {
  EVIDENCE_CAPABILITIES,
  AVAILABILITY,
  PROVIDER_CAPABILITY,
  COST_TIER_ORDER,
  EVIDENCE_TO_CAPABILITY,
  EVIDENCE_PRODUCING_SOURCE_TYPES,
  EXTERNAL_DISCOVERY_CAPABILITY_UNAVAILABLE,
  DEFAULT_PROVIDER_DEFINITIONS,
  UnifiedProviderCapabilityRegistry,
  createUnifiedProviderRegistry,
  createDefaultUnifiedRegistry,
  getDefaultUnifiedRegistry,
  resetDefaultUnifiedRegistry,
  buildProviderEntry,
  deriveEvidenceTypesFromCapabilities,
  availabilityToLegacyCapability,
  isOperationalAvailability,
  isOperationalCapability,
  isEvidenceProducingCapability,
};
