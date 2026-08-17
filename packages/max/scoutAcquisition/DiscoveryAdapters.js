'use strict';

/**
 * SPEC-100A — candidate discovery adapters.
 * Business reasoning is not hard-coded to one vendor. Social specialists
 * are optional enrichment and must not be required to perceive businesses.
 */

const { asText, nowIso, SOURCE_TYPES } = require('./Types');
const { createPlacesProvider } = require('../../capabilities/discovery/providers/PlacesProvider');

function adapterResult({
  source,
  sourceType,
  candidates = [],
  coverage = null,
  errors = [],
  available = true,
}) {
  return {
    source,
    sourceType,
    candidates,
    coverage,
    errors,
    available,
  };
}

function toDiscoveredCompany(raw, searchDefinition, source) {
  const name = asText(raw.name || raw.companyName || raw.company);
  if (!name) return null;
  return {
    id: asText(raw.id || raw.companyId || raw.placeId) || null,
    tenantId: asText(raw.tenantId || raw.client_id || raw.clientId) || searchDefinition.tenantId,
    name,
    industry: asText(raw.industry || raw.vertical || raw.segment) ||
      ((searchDefinition.segments || [])[0] || null),
    location: asText(raw.location || raw.address || raw.geography) ||
      (searchDefinition.geography && searchDefinition.geography.label) ||
      null,
    address: asText(raw.address),
    website: asText(raw.website || raw.url),
    phone: asText(raw.phone),
    people: Array.isArray(raw.people) ? raw.people : [],
    signals: Array.isArray(raw.signals) ? raw.signals : [],
    evidence: Array.isArray(raw.evidence) ? raw.evidence : [],
    icpScore: raw.icpScore != null ? Number(raw.icpScore) : raw.icp_score,
    source,
    discoverySource: source,
    discoveredAt: raw.discoveredAt || nowIso(),
    lastEvaluatedAt: raw.lastEvaluatedAt || null,
    placeId: asText(raw.placeId || raw.place_id),
    snippet: asText(raw.snippet || raw.description),
  };
}

function createExistingIntelligenceAdapter(existingCompanies) {
  const list = Array.isArray(existingCompanies) ? existingCompanies : [];
  return {
    id: 'existing_pf',
    sourceType: SOURCE_TYPES.EXISTING_PF,
    required: false,
    available() {
      return true;
    },
    async discover() {
      return adapterResult({
        source: 'existing_pf',
        sourceType: SOURCE_TYPES.EXISTING_PF,
        candidates: list.map((c) => ({ ...c, source: c.source || 'existing_pf' })),
        coverage: { reused: list.length },
      });
    },
  };
}

function createInjectedDiscoverAdapter(discoverFn) {
  return {
    id: 'injected_discover',
    sourceType: SOURCE_TYPES.PUBLIC_BUSINESS_DATA,
    required: false,
    available() {
      return typeof discoverFn === 'function';
    },
    async discover(searchDefinition) {
      if (typeof discoverFn !== 'function') {
        return adapterResult({
          source: 'injected_discover',
          sourceType: SOURCE_TYPES.PUBLIC_BUSINESS_DATA,
          available: false,
          errors: [{ code: 'adapter_unavailable', message: 'No injected discover function.' }],
        });
      }
      const raw = await discoverFn({
        tenantId: searchDefinition.tenantId,
        searchDefinition,
        targetContext: {
          geography: searchDefinition.geography && searchDefinition.geography.label,
          segments: searchDefinition.segments,
          businessType: searchDefinition.businessNeed,
          desiredSignals: searchDefinition.desiredSignals,
        },
        businessContext: {
          serviceGeography: searchDefinition.geography && searchDefinition.geography.label,
          commercialCapability: searchDefinition.businessNeed,
          preferredSegments: searchDefinition.segments,
          exclusions: searchDefinition.exclusions,
        },
        criteria: {
          geography: searchDefinition.geography && searchDefinition.geography.label,
          segments: searchDefinition.segments,
          exclusions: searchDefinition.exclusions,
        },
      });
      const rows = Array.isArray(raw) ? raw : (raw && raw.companies) || [];
      return adapterResult({
        source: 'injected_discover',
        sourceType: SOURCE_TYPES.PUBLIC_BUSINESS_DATA,
        candidates: rows
          .map((row) => toDiscoveredCompany(row, searchDefinition, 'injected_discover'))
          .filter(Boolean),
        coverage: { queries: 1 },
      });
    },
  };
}

function createPlacesDiscoveryAdapter(opts = {}) {
  const provider =
    opts.placesProvider ||
    createPlacesProvider({
      apiKey: opts.apiKey || process.env.GOOGLE_PLACES_KEY || '',
      fetchImpl: opts.fetchImpl,
    });
  return {
    id: 'public_business_places',
    sourceType: SOURCE_TYPES.PUBLIC_BUSINESS_DATA,
    required: false,
    available() {
      return Boolean(provider && typeof provider.search === 'function' && provider.available());
    },
    async discover(searchDefinition) {
      if (!this.available()) {
        return adapterResult({
          source: 'public_business_places',
          sourceType: SOURCE_TYPES.PUBLIC_BUSINESS_DATA,
          available: false,
          errors: [
            {
              code: 'adapter_unavailable',
              message: 'Candidate discovery provider unavailable.',
            },
          ],
        });
      }
      const geo = searchDefinition.geography && searchDefinition.geography.label;
      const segments = searchDefinition.segments || [];
      const queries = (segments.length ? segments : [searchDefinition.businessNeed || 'commercial'])
        .map((segment) => ({
          industry: String(segment).replace(/_/g, ' '),
          location: geo,
          limit: 20,
        }));
      const candidates = [];
      const errors = [];
      for (const query of queries) {
        try {
          const hits = await provider.search(query);
          for (const hit of hits || []) {
            const mapped = toDiscoveredCompany(hit, searchDefinition, 'public_business_places');
            if (mapped) candidates.push(mapped);
          }
        } catch (err) {
          errors.push({
            code: 'provider_error',
            message: err.message || String(err),
            query,
          });
        }
      }
      return adapterResult({
        source: 'public_business_places',
        sourceType: SOURCE_TYPES.PUBLIC_BUSINESS_DATA,
        candidates,
        coverage: { queries: queries.length },
        errors,
      });
    },
  };
}

function createSocialStubAdapter(channel, sourceType) {
  return {
    id: channel,
    sourceType,
    required: false,
    available() {
      return false;
    },
    async discover() {
      return adapterResult({
        source: channel,
        sourceType,
        available: false,
        errors: [
          {
            code: 'social_unavailable',
            message: `${channel} social intelligence is not required for candidate discovery.`,
          },
        ],
      });
    },
  };
}

function defaultDiscoveryAdapters(opts = {}) {
  const adapters = [];
  if (Array.isArray(opts.discoveryAdapters)) {
    adapters.push(...opts.discoveryAdapters);
  }
  if (typeof opts.discover === 'function') {
    adapters.push(createInjectedDiscoverAdapter(opts.discover));
  }
  const allowPlaces =
    opts.enablePlaces === true ||
    (opts.companies === undefined && typeof opts.discover !== 'function' && !opts.discoveryAdapters);
  if (allowPlaces) {
    adapters.push(createPlacesDiscoveryAdapter(opts));
  }
  adapters.push(createSocialStubAdapter('linkedin', SOURCE_TYPES.LINKEDIN));
  adapters.push(createSocialStubAdapter('facebook', SOURCE_TYPES.FACEBOOK));
  adapters.push(createSocialStubAdapter('instagram', SOURCE_TYPES.INSTAGRAM));
  return adapters;
}

/**
 * Run discovery adapters independently. One source failure does not collapse
 * the investigation. Social adapters are recorded as unavailable, never required.
 *
 * @returns {Promise<object>} CandidateDiscoveryResult
 */
async function discoverCandidates(searchDefinition, adapters = []) {
  const candidates = [];
  const errors = [];
  const sourceReports = [];
  const sourceTypesChecked = [];
  const sourceTypesUnavailable = [];

  for (const adapter of adapters) {
    if (!adapter) continue;
    const sourceType = adapter.sourceType || SOURCE_TYPES.PUBLIC_BUSINESS_DATA;
    const isSocial = [
      SOURCE_TYPES.LINKEDIN,
      SOURCE_TYPES.FACEBOOK,
      SOURCE_TYPES.INSTAGRAM,
    ].includes(sourceType);
    if (typeof adapter.available === 'function' && !adapter.available()) {
      sourceTypesUnavailable.push(sourceType);
      sourceReports.push(
        adapterResult({
          source: adapter.id || sourceType,
          sourceType,
          available: false,
          errors: isSocial
            ? []
            : [{ code: 'adapter_unavailable', message: `${adapter.id || sourceType} unavailable.` }],
        })
      );
      continue;
    }
    try {
      const report = await adapter.discover(searchDefinition);
      sourceReports.push(report);
      if (report.available === false) {
        sourceTypesUnavailable.push(sourceType);
        errors.push(...(report.errors || []));
        continue;
      }
      sourceTypesChecked.push(sourceType);
      for (const row of report.candidates || []) {
        candidates.push(row);
      }
      if (report.errors && report.errors.length) {
        errors.push(...report.errors);
      }
    } catch (err) {
      sourceTypesUnavailable.push(sourceType);
      const failure = {
        code: 'provider_error',
        message: err.message || String(err),
        source: adapter.id || sourceType,
      };
      errors.push(failure);
      sourceReports.push(
        adapterResult({
          source: adapter.id || sourceType,
          sourceType,
          available: true,
          errors: [failure],
        })
      );
    }
  }

  return {
    candidates,
    source: sourceReports.map((r) => r.source),
    coverage: {
      adapterCount: adapters.length,
      succeeded: sourceTypesChecked.length,
      failed: sourceTypesUnavailable.length,
    },
    errors,
    sourceReports,
    sourceTypesChecked: [...new Set(sourceTypesChecked)],
    sourceTypesUnavailable: [...new Set(sourceTypesUnavailable)],
  };
}

module.exports = {
  adapterResult,
  toDiscoveredCompany,
  createExistingIntelligenceAdapter,
  createInjectedDiscoverAdapter,
  createPlacesDiscoveryAdapter,
  createSocialStubAdapter,
  defaultDiscoveryAdapters,
  discoverCandidates,
};
