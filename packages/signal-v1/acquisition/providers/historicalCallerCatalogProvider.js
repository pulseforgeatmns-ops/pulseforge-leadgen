'use strict';

const fs = require('fs');
const path = require('path');
const { isValidSolanaAddress } = require('../solanaAddress');

const DEFAULT_CATALOG_PATH = path.join(
  __dirname,
  '../../fixtures/historicalCallerCatalog.json'
);

const PROVIDER_ID = 'pulseforge-historical-caller-catalog-v1';

/**
 * @param {object} [options]
 * @returns {import('./interfaces').ResearchCandidateProvider}
 */
function createHistoricalCallerCatalogProvider(options = {}) {
  const catalogPath = options.catalogPath || DEFAULT_CATALOG_PATH;
  const catalog = options.catalog || loadCatalog(catalogPath);

  return {
    providerId: catalog.providerId || PROVIDER_ID,
    providerVersion: catalog.providerVersion || '1.0.0',
    catalog,
    discoverCandidates() {
      return discoverCandidatesFromCatalog(catalog);
    },
    getCoverageBounds() {
      return {
        earliestMarketEvaluableAt: catalog.coverage?.historicalStart
          ? new Date(catalog.coverage.historicalStart)
          : null,
        latestMarketEvaluableAt: catalog.coverage?.historicalEnd
          ? new Date(catalog.coverage.historicalEnd)
          : null,
      };
    },
    investigatedProviders() {
      return catalog.investigatedProviders || [];
    },
  };
}

function loadCatalog(catalogPath) {
  const raw = fs.readFileSync(catalogPath, 'utf8');
  return JSON.parse(raw);
}

/**
 * @param {object} catalog
 * @returns {import('./interfaces').RawResearchCandidate[]}
 */
function discoverCandidatesFromCatalog(catalog) {
  const out = [];
  for (const token of catalog.tokens || []) {
    if (!token.tokenAddress || !isValidSolanaAddress(token.tokenAddress)) continue;
    const calls = token.calls || [];
    if (!calls.length) continue;
    const earliest =
      token.earliestCallAt ||
      calls.map(c => c.occurredAt).sort()[0];
    const sourceIds = [...new Set(calls.map(c => c.sourceId).filter(Boolean))];
    const sourceClusterIds = [...new Set(calls.map(c => c.sourceClusterId).filter(Boolean))];
    out.push({
      tokenAddress: token.tokenAddress,
      chain: 'solana',
      discoveredFrom: catalog.providerId || PROVIDER_ID,
      earliestKnownCallAt: earliest,
      sourceIds,
      sourceClusterIds,
      selectionCategory: 'natural_chronological',
      selectionReason: 'Empirical catalog candidate (outcome-agnostic)',
      provenance: {
        evidenceClass: 'REAL_PROVIDER',
        provider: catalog.providerId || PROVIDER_ID,
        providerVersion: catalog.providerVersion,
        catalogToken: token.ticker || token.tokenAddress,
        callCount: calls.length,
      },
      acquisitionPayload: {
        catalogCalls: calls,
        clusterRelationships: token.clusterRelationships || [],
        ticker: token.ticker,
      },
    });
  }
  return out;
}

const historicalCallerCatalogProvider = createHistoricalCallerCatalogProvider();

module.exports = {
  PROVIDER_ID,
  DEFAULT_CATALOG_PATH,
  loadCatalog,
  createHistoricalCallerCatalogProvider,
  discoverCandidatesFromCatalog,
  historicalCallerCatalogProvider,
};
