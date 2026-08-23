'use strict';

/**
 * SPEC-141 — Provider Capability Registry.
 * Scout owns capabilities; providers advertise what they can supply.
 * Cost metadata drives automatic optimization (free → cached → local → paid).
 */

const { COST_TIERS } = require('./types');

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
});

const DEFAULT_PROVIDERS = Object.freeze([
  {
    id: 'existing_pf',
    label: 'PulseForge Repository',
    capabilities: [
      EVIDENCE_CAPABILITIES.BUSINESSES,
      EVIDENCE_CAPABILITIES.PEOPLE,
      EVIDENCE_CAPABILITIES.EMAILS,
      EVIDENCE_CAPABILITIES.CONTACTS,
    ],
    costTier: COST_TIERS.CACHED,
    latencyMs: 50,
    freshnessHours: 168,
    coverage: 0.6,
    reliability: 0.95,
    available: () => true,
  },
  {
    id: 'google_maps',
    label: 'Google Maps',
    capabilities: [
      EVIDENCE_CAPABILITIES.BUSINESSES,
      EVIDENCE_CAPABILITIES.REVIEWS,
      EVIDENCE_CAPABILITIES.PHONE,
      EVIDENCE_CAPABILITIES.HOURS,
      EVIDENCE_CAPABILITIES.WEBSITE,
    ],
    costTier: COST_TIERS.PAID,
    latencyMs: 800,
    freshnessHours: 24,
    coverage: 0.85,
    reliability: 0.9,
    available: () => Boolean(process.env.GOOGLE_PLACES_KEY),
  },
  {
    id: 'linkedin',
    label: 'LinkedIn',
    capabilities: [
      EVIDENCE_CAPABILITIES.PEOPLE,
      EVIDENCE_CAPABILITIES.OWNERSHIP,
      EVIDENCE_CAPABILITIES.GROWTH,
      EVIDENCE_CAPABILITIES.HIRING,
    ],
    costTier: COST_TIERS.PAID,
    latencyMs: 1200,
    freshnessHours: 72,
    coverage: 0.5,
    reliability: 0.75,
    available: () => Boolean(process.env.LINKEDIN_SESSION),
  },
  {
    id: 'hunter',
    label: 'Hunter',
    capabilities: [EVIDENCE_CAPABILITIES.EMAILS, EVIDENCE_CAPABILITIES.VERIFICATION],
    costTier: COST_TIERS.PAID,
    latencyMs: 600,
    freshnessHours: 168,
    coverage: 0.7,
    reliability: 0.85,
    available: () => Boolean(process.env.HUNTER_API_KEY),
  },
  {
    id: 'prospeo',
    label: 'Prospeo',
    capabilities: [
      EVIDENCE_CAPABILITIES.CONTACTS,
      EVIDENCE_CAPABILITIES.TITLES,
      EVIDENCE_CAPABILITIES.ENRICHMENT,
      EVIDENCE_CAPABILITIES.PHONE,
    ],
    costTier: COST_TIERS.PAID,
    latencyMs: 700,
    freshnessHours: 168,
    coverage: 0.65,
    reliability: 0.8,
    available: () => Boolean(process.env.PROSPEO_API_KEY),
  },
  {
    id: 'apollo',
    label: 'Apollo',
    capabilities: [
      EVIDENCE_CAPABILITIES.CONTACTS,
      EVIDENCE_CAPABILITIES.TITLES,
      EVIDENCE_CAPABILITIES.ENRICHMENT,
    ],
    costTier: COST_TIERS.PAID,
    latencyMs: 900,
    freshnessHours: 168,
    coverage: 0.6,
    reliability: 0.78,
    available: () => false,
  },
  {
    id: 'website',
    label: 'Business Websites',
    capabilities: [
      EVIDENCE_CAPABILITIES.WEBSITE,
      EVIDENCE_CAPABILITIES.BUSINESSES,
      EVIDENCE_CAPABILITIES.PROPERTY_COUNT,
    ],
    costTier: COST_TIERS.FREE,
    latencyMs: 1500,
    freshnessHours: 48,
    coverage: 0.55,
    reliability: 0.7,
    available: () => true,
  },
  {
    id: 'news',
    label: 'News',
    capabilities: [EVIDENCE_CAPABILITIES.BUYING_SIGNALS, EVIDENCE_CAPABILITIES.NEWS],
    costTier: COST_TIERS.PAID,
    latencyMs: 2000,
    freshnessHours: 12,
    coverage: 0.4,
    reliability: 0.65,
    available: () => Boolean(process.env.SERPAPI_KEY),
  },
  {
    id: 'county_records',
    label: 'County Records',
    capabilities: [EVIDENCE_CAPABILITIES.COUNTY_RECORDS, EVIDENCE_CAPABILITIES.PROPERTY_COUNT],
    costTier: COST_TIERS.LOCAL,
    latencyMs: 3000,
    freshnessHours: 720,
    coverage: 0.35,
    reliability: 0.6,
    available: () => false,
  },
]);

const COST_TIER_ORDER = Object.freeze([
  COST_TIERS.FREE,
  COST_TIERS.CACHED,
  COST_TIERS.LOCAL,
  COST_TIERS.PAID,
]);

class ProviderCapabilityRegistry {
  constructor(providers = DEFAULT_PROVIDERS) {
    this.providers = providers.map((p) => ({ ...p }));
  }

  list() {
    return this.providers.slice();
  }

  get(id) {
    return this.providers.find((p) => p.id === id) || null;
  }

  findByCapability(capability) {
    return this.providers.filter((p) => (p.capabilities || []).includes(capability));
  }

  /**
   * Select providers for evidence requirements, preferring lower cost tiers.
   * @param {string[]} requiredCapabilities
   * @param {object} [opts]
   * @returns {object[]}
   */
  selectForCapabilities(requiredCapabilities, opts = {}) {
    const selected = [];
    const seen = new Set();

    for (const cap of requiredCapabilities || []) {
      const candidates = this.findByCapability(cap)
        .filter((p) => (typeof p.available !== 'function' ? true : p.available()))
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
      (a, b) =>
        COST_TIER_ORDER.indexOf(a.costTier) - COST_TIER_ORDER.indexOf(b.costTier)
    );
  }
}

function createProviderCapabilityRegistry(providers) {
  return new ProviderCapabilityRegistry(providers);
}

function createDefaultProviderRegistry() {
  return new ProviderCapabilityRegistry(DEFAULT_PROVIDERS);
}

module.exports = {
  EVIDENCE_CAPABILITIES,
  DEFAULT_PROVIDERS,
  COST_TIER_ORDER,
  ProviderCapabilityRegistry,
  createProviderCapabilityRegistry,
  createDefaultProviderRegistry,
};
