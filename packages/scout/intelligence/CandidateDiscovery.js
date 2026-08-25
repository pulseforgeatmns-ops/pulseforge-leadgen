'use strict';

/**
 * SPEC-141 Stage 4 — Candidate Universe Discovery.
 * Build candidate universe — not qualified prospects.
 */

const { loadRepository } = require('../../max/scoutAcquisition/ExistingIntelligence');
const {
  constructCandidateUniverse,
} = require('../../max/scoutAcquisition/CandidateUniverse');
const { defaultDiscoveryAdapters } = require('../../max/scoutAcquisition/DiscoveryAdapters');
const { buildDelegationFromMission } = require('./MarketUnderstanding');
const {
  computeCoverageFromEstimate,
  normalizeCandidateUniverseEstimate,
  extractExpectedValue,
} = require('../universe/CandidateUniverseEstimate');

/**
 * Discover candidate universe for a market investigation.
 *
 * @param {object} input
 * @returns {Promise<object>}
 */
async function discoverCandidateUniverse(input = {}) {
  const marketDefinition = input.marketDefinition;
  const searchDefinition = marketDefinition.searchDefinition;
  const delegation =
    input.delegation ||
    buildDelegationFromMission(input.mission || {}, input.scoutPayload || {});
  const tenantId = marketDefinition.tenantId;

  let existing = { companies: [], people: [] };
  try {
    existing = await loadRepository({
      authorizedTenantId: tenantId,
      tenantId,
      targetContext: delegation.targetContext,
      businessContext: delegation.businessContext,
      companies: input.opts && input.opts.companies,
      people: input.opts && input.opts.people,
      loadCompanies: input.opts && input.opts.loadCompanies,
    });
  } catch (err) {
    return {
      estimatedMarket: null,
      discovered: 0,
      coverage: 0,
      candidates: [],
      error: err.message || String(err),
      sourceTypesChecked: [],
      sourceTypesUnavailable: ['existing_pf'],
    };
  }

  const adapters =
    (input.opts && input.opts.discoveryAdapters) ||
    defaultDiscoveryAdapters({
      discover: input.opts && input.opts.discover,
      enablePlaces: input.opts && input.opts.enablePlaces,
      placesProvider: input.opts && input.opts.placesProvider,
      companies: input.opts && input.opts.companies,
      discoveryAdapters: input.opts && input.opts.discoveryAdapters,
    });

  const universe = await constructCandidateUniverse({
    searchDefinition,
    existing,
    companies: input.opts && input.opts.companies,
    people: input.opts && input.opts.people,
    adapters,
    adapterOpts: input.opts || {},
    discoveryStore: input.opts && input.opts.discoveryStore,
    persistCompanies: input.opts && input.opts.persistCompanies,
    now: input.opts && input.opts.now,
    freshnessMs: input.opts && input.opts.freshnessMs,
    forceDiscover: input.opts && input.opts.forceDiscover,
  });

  const candidates = universe.resolved || universe.companies || [];
  const discovered = candidates.length;
  const retrieved = universe.retrievedCount || 0;
  const newlyDiscovered = universe.discoveredCount || 0;

  const universeEstimate =
    normalizeCandidateUniverseEstimate(input.opts && input.opts.universeEstimate) ||
    normalizeCandidateUniverseEstimate(input.opts && input.opts.estimatedMarket);

  const estimatedMarket = extractExpectedValue(universeEstimate);
  const coverage = computeCoverageFromEstimate(discovered, universeEstimate);

  return {
    estimatedMarket,
    universeEstimate,
    discovered,
    retrieved,
    newlyDiscovered,
    coverage,
    candidates,
    resolved: universe.resolved || candidates,
    rejected: universe.rejected || [],
    sourceTypesChecked: universe.sourceTypesChecked || [],
    sourceTypesUnavailable: universe.sourceTypesUnavailable || [],
    discoveryRan: universe.discoveryRan === true,
    actionsTaken: universe.actionsTaken || [],
  };
}

module.exports = {
  discoverCandidateUniverse,
};
