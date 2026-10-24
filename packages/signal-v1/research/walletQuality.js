'use strict';

const { DEFAULT_RESEARCH_CONFIG } = require('../config/defaultConfig');
const { SOURCE_PERFORMANCE_VERSION } = require('../types');

function unavailableWalletQuality() {
  return {
    quality: 'unavailable',
    sampleSize: 0,
    classification: 'unavailable',
  };
}

/**
 * Wallet quality as-of T from observed wallet performance only.
 *
 * @returns {{ quality: 'unavailable' | number, sampleSize: number, classification: string }}
 */
function getWalletQualityAsOf(store, walletAddress, asOf, config = {}) {
  const cfg = { ...DEFAULT_RESEARCH_CONFIG, ...config };
  if (!walletAddress) return unavailableWalletQuality();

  const perf = store.getWalletPerformance?.(walletAddress, asOf);
  if (!perf || perf.version !== SOURCE_PERFORMANCE_VERSION) {
    return unavailableWalletQuality();
  }
  if ((perf.sampleSize || 0) < cfg.minWalletQualitySampleSize) {
    return { ...unavailableWalletQuality(), sampleSize: perf.sampleSize || 0 };
  }
  if (typeof perf.score !== 'number') {
    return { ...unavailableWalletQuality(), sampleSize: perf.sampleSize || 0 };
  }
  const profitable = perf.score >= cfg.walletProfitableScoreMin;
  return {
    quality: perf.score,
    sampleSize: perf.sampleSize,
    classification: profitable ? 'historically_profitable' : 'not_profitable',
  };
}

module.exports = {
  getWalletQualityAsOf,
  unavailableWalletQuality,
};
