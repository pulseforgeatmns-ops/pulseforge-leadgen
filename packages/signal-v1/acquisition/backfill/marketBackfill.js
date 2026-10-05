'use strict';

const { resolveResearchWindowFromAnchor } = require('../../fixtures/researchWindows');
const { callStore } = require('../../storage/storeUtils');
const { ingestHistoricalMarketData } = require('../../ingestion/ingestHistoricalMarketData');
const { FixtureMarketDataProvider } = require('../../providers/FixtureMarketDataProvider');
const { deriveEvidencePattern } = require('../evidencePattern');

/**
 * @param {object} store
 * @param {object} candidate
 * @param {import('../providers/interfaces').RawResearchCandidate} raw
 * @param {{ marketProvider?: object, pricePath?: object[] }} [options]
 */
async function backfillMarketHistory(store, candidate, raw, options = {}) {
  const window = resolveResearchWindowFromAnchor(
    candidate.tokenAddress,
    candidate.earliestKnownCallAt || raw.earliestKnownCallAt
  );

  const pricePath =
    options.pricePath ||
    buildFixturePricePath(candidate, raw, window.anchor) ||
    [];

  if (pricePath.length) {
    const paths = {
      [candidate.tokenAddress]: pricePath.map(p => ({
        occurredAt: p.occurredAt,
        price: p.priceUsd ?? p.price,
      })),
    };
    const provider =
      options.marketProvider || new FixtureMarketDataProvider({ pricePaths: paths });
    const result = await ingestHistoricalMarketData(store, provider, {
      tokenAddress: candidate.tokenAddress,
      startTime: window.startTime,
      endTime: window.endTime,
      resolutionSeconds: window.resolutionSeconds,
      decisionAnchor: window.anchor,
    });
    return {
      window,
      ...result,
      source: 'fixture_price_path',
    };
  }

  if (options.marketProvider && !(options.marketProvider instanceof FixtureMarketDataProvider)) {
    const result = await ingestHistoricalMarketData(store, options.marketProvider, {
      tokenAddress: candidate.tokenAddress,
      startTime: window.startTime,
      endTime: window.endTime,
      resolutionSeconds: window.resolutionSeconds,
      decisionAnchor: window.anchor,
    });
    return { window, ...result, source: 'live_provider' };
  }

  return {
    window,
    historicalDataStatus: 'UNAVAILABLE',
    source: 'none',
    note: 'No fixture path and no live provider configured',
  };
}

function buildFixturePricePath(candidate, raw, anchorIso) {
  const anchor = new Date(anchorIso).getTime();
  const catalogIndex = raw?.provenance?.catalogIndex ?? raw?.acquisitionPayload?.catalogIndex ?? 0;
  const pattern =
    raw?.acquisitionPayload?.pattern ||
    deriveEvidencePattern(candidate.tokenAddress, catalogIndex);
  const points = [];

  const push = (offsetMin, price) => {
    points.push({
      occurredAt: new Date(anchor + offsetMin * 60000).toISOString(),
      priceUsd: price,
    });
  };

  if (pattern === 'dual_cluster_run') {
    push(-30, 0.00008);
    push(0, 0.0001);
    push(10, 0.00014);
    push(30, 0.00022);
    push(60, 0.00035);
    push(120, 0.00028);
    push(360, 0.0004);
  } else {
    push(-30, 0.00012);
    push(0, 0.00011);
    push(15, 0.0001);
    push(45, 0.00007);
    push(90, 0.00005);
    push(180, 0.00004);
  }

  return points;
}

async function persistPricePathAsObservations(store, tokenAddress, pricePath) {
  let inserted = 0;
  for (const p of pricePath) {
    const res = await callStore(store, 'insertMarketObservation', {
      tokenAddress,
      occurredAt: p.occurredAt,
      priceUsd: p.priceUsd ?? p.price,
      intervalSeconds: 60,
      provider: 'fixture-acquisition',
      provenance: { backfill: 'marketBackfill' },
    });
    if (!res.duplicate) inserted += 1;
  }
  return inserted;
}

module.exports = {
  backfillMarketHistory,
  buildFixturePricePath,
  persistPricePathAsObservations,
};
