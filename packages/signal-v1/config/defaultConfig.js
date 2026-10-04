'use strict';

/** @type {import('../scoring/signalScoring').SignalWeights} */
const DEFAULT_WEIGHTS = Object.freeze({
  independentConvergence: 18,
  sourceQuality: 14,
  convergenceVelocity: 10,
  walletAccumulation: 12,
  buyerAcceleration: 10,
  liquidity: 8,
  attentionAcceleration: 8,
  amplifierPotential: 6,

  bundleRisk: 12,
  holderRisk: 10,
  devRisk: 10,
  walletDistribution: 10,
  whaleDistribution: 8,
  priceOverextension: 6,
  liquidityDeterioration: 10,
  correlationPenalty: 8,
});

const DEFAULT_STATE_THRESHOLDS = Object.freeze({
  entryScoreMin: 72,
  watchScoreMin: 48,
  rejectScoreMax: 25,
  deRiskGainPct: 100,
  exitDistributionScoreMin: 65,
  maxBundleSupplyPct: 35,
  maxTop10HolderPct: 55,
  maxDevHoldingPct: 12,
  minLiquidityUsd: 15000,
  minIndependentClustersForEntry: 2,
});

const DEFAULT_PAPER_CONFIG = Object.freeze({
  notionalUsd: 100,
  slippageBps: 80,
  feeBps: 30,
  executionDelaySeconds: 30,
});

const DEFAULT_OUTCOME_CONFIG = Object.freeze({
  horizonHours: 24,
  passMultiple: 2.0,
  failMultiple: 0.7,
});

const CONVERGENCE_WINDOWS_MINUTES = Object.freeze([5, 15, 30, 60]);

module.exports = {
  DEFAULT_WEIGHTS,
  DEFAULT_STATE_THRESHOLDS,
  DEFAULT_PAPER_CONFIG,
  DEFAULT_OUTCOME_CONFIG,
  CONVERGENCE_WINDOWS_MINUTES,
};
