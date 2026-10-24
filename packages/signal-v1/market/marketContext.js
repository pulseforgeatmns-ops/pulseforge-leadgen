'use strict';

const { filterObservationsAtOrBefore } = require('../temporal/temporalFirewall');

/**
 * Merge persisted observations with optional MARKET_SNAPSHOT event payloads (fixtures).
 */
function latestMarketContext(store, tokenAddress, at, options = {}) {
  const evaluatedMs = new Date(at).getTime();
  let observations = [];
  if (options.observations) {
    observations = filterObservationsAtOrBefore(options.observations, at);
  } else if (store.getMarketObservationsForToken) {
    const result = store.getMarketObservationsForToken(tokenAddress, { maxOccurredAt: at });
    observations = Array.isArray(result) ? result : [];
  }

  const events = store.getEventsForToken(tokenAddress, { maxOccurredAt: at });
  const marketEvents = events.filter(e => e.eventType === 'MARKET_SNAPSHOT');
  const lastEvent = marketEvents.length ? marketEvents[marketEvents.length - 1] : null;
  const lastObs =
    observations.length > 0 ? observations[observations.length - 1] : null;

  const eventPayload = lastEvent?.payload || {};
  const priceUsd =
    lastObs?.priceUsd ??
    numOrNull(eventPayload.priceUsd ?? eventPayload.price);

  return {
    priceUsd,
    marketCapUsd: numOrNull(
      lastObs?.marketCapUsd ?? eventPayload.marketCapUsd ?? eventPayload.marketCap
    ),
    liquidityUsd: numOrNull(lastObs?.liquidityUsd ?? eventPayload.liquidityUsd),
    tokenAgeSeconds: numOrNull(eventPayload.tokenAgeSeconds),
    uniqueBuyers5m: numOrNull(eventPayload.uniqueBuyers5m),
    uniqueSellers5m: numOrNull(eventPayload.uniqueSellers5m),
    volume5mUsd: numOrNull(eventPayload.volume5mUsd),
    priceAcceleration: numOrNull(eventPayload.priceAcceleration),
    lastObservationAt: lastObs?.occurredAt || null,
    lastMarketEventAt: lastEvent?.occurredAt || null,
  };
}

function buildPricePathFromObservations(observations) {
  return observations.map(o => ({
    occurredAt: o.occurredAt,
    priceUsd: o.priceUsd,
    price: o.priceUsd,
    intervalSeconds: o.intervalSeconds,
    marketCapUsd: o.marketCapUsd,
    liquidityUsd: o.liquidityUsd,
  }));
}

function numOrNull(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

module.exports = {
  latestMarketContext,
  buildPricePathFromObservations,
  filterObservationsAtOrBefore,
};
