'use strict';

const { DATA_CLASS } = require('./constants');

/**
 * Capture real market snapshot at trigger time — no fabrication on failure.
 *
 * @param {object} marketProvider
 * @param {string} tokenAddress
 * @param {Date|string} asOf
 */
async function captureMarketSnapshot(marketProvider, tokenAddress, asOf) {
  try {
    const snap = await marketProvider.getTokenSnapshot(tokenAddress, asOf);
    return {
      ok: true,
      snapshot: {
        tokenAddress,
        occurredAt: new Date(asOf),
        priceUsd: snap.priceUsd,
        marketCapUsd: snap.marketCapUsd ?? null,
        liquidityUsd: snap.liquidityUsd ?? null,
        volumeIntervalUsd: snap.volumeIntervalUsd ?? null,
        intervalSeconds: 60,
        provider: snap.provider || marketProvider.providerId,
        providerTimestamp: snap.asOf || new Date(asOf),
        provenance: {
          dataClass: DATA_CLASS.EMPIRICAL,
          captureKind: 'trigger_snapshot',
        },
      },
    };
  } catch (err) {
    return {
      ok: false,
      error: String(err.message || err),
      snapshot: null,
    };
  }
}

module.exports = {
  captureMarketSnapshot,
};
