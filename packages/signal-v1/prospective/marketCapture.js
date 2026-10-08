'use strict';

const { DATA_CLASS } = require('./constants');

function validDate(value) {
  if (value == null) return null;
  const date = new Date(value);
  return Number.isFinite(date.getTime()) ? date : null;
}

// Never turn a requested time into evidence of an actual market observation.
async function captureMarketSnapshot(marketProvider, tokenAddress, targetAt, options = {}) {
  const now = options.now || (() => new Date());
  try {
    const snap = marketProvider.getLiveTokenSnapshot
      ? await marketProvider.getLiveTokenSnapshot(tokenAddress)
      : await marketProvider.getTokenSnapshot(tokenAddress, targetAt);
    const receivedAt = now();
    const observedAt = validDate(snap.occurredAt);
    const sampledAt = validDate(snap.sampledAt || snap.observedTimestamp);
    const providerAt = validDate(snap.providerTimestamp);
    if (!observedAt || !sampledAt || observedAt > sampledAt || observedAt > receivedAt || sampledAt > receivedAt
      || (providerAt && providerAt > sampledAt)) {
      throw new Error('market_observation_timestamp_missing_or_future');
    }
    if (snap.priceUsd == null || !Number.isFinite(Number(snap.priceUsd)) || Number(snap.priceUsd) <= 0) {
      throw new Error('market_price_unavailable');
    }
    if (snap.tokenAddress !== tokenAddress || snap.provenance?.dataClass !== 'EMPIRICAL'
      || snap.provenance?.synthetic || snap.provenance?.testOnly) {
      throw new Error('non_empirical_market_snapshot');
    }
    return { ok: true, snapshot: {
      tokenAddress,
      occurredAt: observedAt,
      priceUsd: Number(snap.priceUsd),
      marketCapUsd: snap.marketCapUsd ?? null,
      liquidityUsd: snap.liquidityUsd ?? null,
      volumeIntervalUsd: snap.volumeIntervalUsd ?? null,
      intervalSeconds: snap.intervalSeconds ?? 0,
      provider: snap.provider || marketProvider.providerId,
      providerTimestamp: providerAt,
      observedTimestamp: sampledAt,
      ingestedAt: receivedAt,
      provenance: {
        ...(snap.provenance || {}), dataClass: DATA_CLASS.EMPIRICAL,
        captureKind: options.captureKind || 'trigger_snapshot',
        targetAt: new Date(targetAt).toISOString(),
        sampledAt: sampledAt.toISOString(),
        sourceTimestamp: providerAt?.toISOString() || null,
        receivedAt: receivedAt.toISOString(),
        freshness: snap.provenance?.freshness || 'UNKNOWN',
      },
    } };
  } catch (err) {
    return { ok: false, error: String(err.message || err), snapshot: null };
  }
}
module.exports = { captureMarketSnapshot };
