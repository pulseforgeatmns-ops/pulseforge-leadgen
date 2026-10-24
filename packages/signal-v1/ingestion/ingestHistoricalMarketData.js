'use strict';

const { randomUUID } = require('crypto');
const { createHash } = require('crypto');
const {
  validateHistoricalCoverage,
  buildHistoricalUnavailablePayload,
  HISTORICAL_DATA_UNAVAILABLE,
} = require('../market/historicalCoverage');
const { PROVIDER_ID } = require('../providers/GeckoTerminalMarketDataProvider');

/**
 * @param {object} store — Signal store with market observation methods
 * @param {import('../providers/interfaces').MarketDataProvider} provider
 * @param {object} args
 * @param {string} args.tokenAddress
 * @param {Date|string} args.startTime
 * @param {Date|string} args.endTime
 * @param {number} [args.resolutionSeconds]
 * @param {Date|string} [args.decisionAnchor]
 */
async function ingestHistoricalMarketData(store, provider, args) {
  const tokenAddress = args.tokenAddress;
  const startTime = new Date(args.startTime);
  const endTime = new Date(args.endTime);
  const resolutionSeconds = args.resolutionSeconds ?? 60;
  const decisionAnchor = args.decisionAnchor || null;

  if (endTime <= startTime) {
    const coverage = validateHistoricalCoverage({
      requestedStart: startTime,
      requestedEnd: endTime,
      observations: [],
      decisionAnchor,
      intervalSeconds: resolutionSeconds,
    });
    return unavailableStats(tokenAddress, provider, coverage, {
      inserted: 0,
      duplicates: 0,
      rejected: 0,
      receivedObservations: 0,
    });
  }

  const received = await provider.getHistoricalPrices(tokenAddress, startTime, endTime, {
    resolutionSeconds,
  });

  const providerId =
    received[0]?.provider ||
    (provider.constructor?.name === 'GeckoTerminalMarketDataProvider'
      ? PROVIDER_ID
      : provider.providerId || 'unknown');

  const coverage = validateHistoricalCoverage({
    requestedStart: startTime,
    requestedEnd: endTime,
    observations: received,
    decisionAnchor,
    intervalSeconds: resolutionSeconds,
  });

  let inserted = 0;
  let duplicates = 0;
  let rejected = 0;

  const persistable =
    coverage.status === 'AVAILABLE' || coverage.status === 'PARTIAL'
      ? received.filter(o => {
          const t = new Date(o.occurredAt).getTime();
          return t >= startTime.getTime() && t <= endTime.getTime();
        })
      : [];

  for (const obs of persistable) {
    if (!validateObservation(obs)) {
      rejected += 1;
      continue;
    }
    const result = await store.insertMarketObservation(obs);
    if (result.duplicate) duplicates += 1;
    else inserted += 1;
  }

  const persisted = await store.getMarketObservationsForToken(tokenAddress, {
    startTime,
    endTime,
  });

  const effectiveResolution = inferEffectiveResolution(persisted);
  const missingIntervals = countMissingIntervals(persisted, startTime, endTime, effectiveResolution);

  const stats = {
    tokenAddress,
    requestedRange: {
      start: startTime.toISOString(),
      end: endTime.toISOString(),
    },
    provider: providerId,
    historicalDataStatus: coverage.status,
    coverage,
    receivedObservations: received.length,
    inserted,
    duplicates,
    rejected,
    missingIntervals,
    effectiveResolutionSeconds: effectiveResolution,
    observationCount: persisted.length,
    observationStart: persisted[0]?.occurredAt?.toISOString?.() || null,
    observationEnd: persisted[persisted.length - 1]?.occurredAt?.toISOString?.() || null,
    unavailable: coverage.status === 'UNAVAILABLE' || coverage.status === 'INVALID_RANGE',
  };

  if (stats.unavailable) {
    stats.error = HISTORICAL_DATA_UNAVAILABLE;
    stats.unavailablePayload = buildHistoricalUnavailablePayload({
      tokenAddress,
      provider: providerId,
      coverage,
    });
  }

  if (store.insertMarketIngestionStats) {
    await store.insertMarketIngestionStats({
      id: randomUUID(),
      tokenAddress,
      provider: stats.provider,
      requestedStart: startTime,
      requestedEnd: endTime,
      effectiveResolutionSeconds: effectiveResolution,
      receivedCount: received.length,
      insertedCount: inserted,
      duplicateCount: duplicates,
      rejectedCount: rejected,
      missingIntervalCount: missingIntervals,
      metadata: stats,
    });
  }

  return stats;
}

function unavailableStats(tokenAddress, provider, coverage, counts) {
  const providerId =
    provider?.providerId ||
    (provider?.constructor?.name === 'GeckoTerminalMarketDataProvider' ? PROVIDER_ID : 'unknown');
  return {
    tokenAddress,
    requestedRange: {
      start: coverage.requestedStart,
      end: coverage.requestedEnd,
    },
    provider: providerId,
    historicalDataStatus: coverage.status,
    coverage,
    receivedObservations: counts.receivedObservations,
    inserted: counts.inserted,
    duplicates: counts.duplicates,
    rejected: counts.rejected,
    missingIntervals: 0,
    effectiveResolutionSeconds: null,
    observationCount: 0,
    observationStart: null,
    observationEnd: null,
    unavailable: true,
    error: HISTORICAL_DATA_UNAVAILABLE,
    unavailablePayload: buildHistoricalUnavailablePayload({
      tokenAddress,
      provider: providerId,
      coverage,
    }),
  };
}

function validateObservation(obs) {
  if (!obs?.tokenAddress || !obs.occurredAt) return false;
  const price = Number(obs.priceUsd);
  if (!Number.isFinite(price) || price <= 0) return false;
  const t = new Date(obs.occurredAt).getTime();
  if (!Number.isFinite(t)) return false;
  return true;
}

function inferEffectiveResolution(observations) {
  if (!observations.length) return null;
  const counts = new Map();
  for (const o of observations) {
    const sec = o.intervalSeconds || 60;
    counts.set(sec, (counts.get(sec) || 0) + 1);
  }
  let best = 60;
  let bestCount =  0;
  for (const [sec, count] of counts) {
    if (count > bestCount) {
      best = sec;
      bestCount = count;
    }
  }
  return best;
}

function countMissingIntervals(observations, startTime, endTime, intervalSeconds) {
  if (!observations.length || !intervalSeconds) return 0;
  const set = new Set(observations.map(o => bucketKey(o.occurredAt, intervalSeconds)));
  const startMs = new Date(startTime).getTime();
  const endMs = new Date(endTime).getTime();
  let missing = 0;
  for (let t = startMs; t <= endMs; t += intervalSeconds * 1000) {
    const key = bucketKey(new Date(t), intervalSeconds);
    if (!set.has(key)) missing += 1;
  }
  return missing;
}

function bucketKey(date, intervalSeconds) {
  const ms = new Date(date).getTime();
  const bucket = Math.floor(ms / (intervalSeconds * 1000));
  return String(bucket);
}

function observationDedupeId(obs) {
  const payload = `${obs.tokenAddress}|${obs.provider}|${obs.occurredAt.toISOString()}|${obs.intervalSeconds}`;
  return createHash('sha256').update(payload).digest('hex').slice(0, 32);
}

module.exports = {
  ingestHistoricalMarketData,
  observationDedupeId,
  HISTORICAL_DATA_UNAVAILABLE,
};
