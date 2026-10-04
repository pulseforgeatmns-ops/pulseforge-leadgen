'use strict';

const { randomUUID } = require('crypto');
const { createHash } = require('crypto');

/**
 * @param {object} store — Signal store with market observation methods
 * @param {import('../providers/interfaces').MarketDataProvider} provider
 * @param {object} args
 * @param {string} args.tokenAddress
 * @param {Date|string} args.startTime
 * @param {Date|string} args.endTime
 * @param {number} [args.resolutionSeconds]
 */
async function ingestHistoricalMarketData(store, provider, args) {
  const tokenAddress = args.tokenAddress;
  const startTime = new Date(args.startTime);
  const endTime = new Date(args.endTime);
  const resolutionSeconds = args.resolutionSeconds ?? 60;

  if (endTime <= startTime) {
    throw new Error('endTime must be after startTime');
  }

  const received = await provider.getHistoricalPrices(tokenAddress, startTime, endTime, {
    resolutionSeconds,
  });

  let inserted = 0;
  let duplicates = 0;
  let rejected = 0;

  for (const obs of received) {
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
    provider: received[0]?.provider || provider.constructor.name,
    receivedObservations: received.length,
    inserted,
    duplicates,
    rejected,
    missingIntervals,
    effectiveResolutionSeconds: effectiveResolution,
    observationCount: persisted.length,
    observationStart: persisted[0]?.occurredAt?.toISOString?.() || null,
    observationEnd: persisted[persisted.length - 1]?.occurredAt?.toISOString?.() || null,
  };

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
  let bestCount = 0;
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
};
