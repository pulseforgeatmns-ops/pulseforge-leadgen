'use strict';

const { DEFAULT_WEIGHTS } = require('../config/defaultConfig');

/**
 * @typedef {typeof DEFAULT_WEIGHTS} SignalWeights
 */

/**
 * @param {object} features — SignalFeatureSnapshot.features
 * @param {Partial<SignalWeights>} [weights]
 */
function scoreSignal(features, weights = {}) {
  const w = { ...DEFAULT_WEIGHTS, ...weights };

  const independentConvergence = normalizeCluster(features.independentClusterCount, 4);
  const sourceQuality = normalizeUnit(features.qualityWeightedConvergence, 3);
  const convergenceVelocity = normalizeUnit(features.convergenceVelocity, 2);
  const walletAccumulation = features.walletAccumulationScore ?? 0;
  const buyerAcceleration = normalizeUnit(features.volumeAcceleration, 4);
  const liquidity = liquidityScore(features.liquidityUsd);
  const attentionAcceleration = normalizeUnit(features.socialMentionVelocity, 5);
  const amplifierPotential = normalizeUnit(features.amplifierStage, 3);

  const bundleRisk = pctRisk(features.bundledSupplyPct, 40);
  const holderRisk = pctRisk(features.top10HolderPct, 60);
  const devRisk = pctRisk(features.devHoldingPct, 15);
  const walletDistribution = features.walletDistributionScore ?? 0;
  const whaleDistribution = features.whaleDistributionDetected ? 1 : 0;
  const priceOverextension = normalizeUnit(features.priceAcceleration, 6) > 0.85 ? 1 : 0;
  const liquidityDeterioration = features.liquidityDeteriorationDetected ? 1 : 0;
  const correlationPenalty =
    features.rawSourceCount > 0 && features.independentClusterCount > 0
      ? Math.max(0, 1 - features.independentClusterCount / features.rawSourceCount)
      : 0;

  const components = {
    independentConvergence: independentConvergence * w.independentConvergence,
    sourceQuality: sourceQuality * w.sourceQuality,
    convergenceVelocity: convergenceVelocity * w.convergenceVelocity,
    walletAccumulation: walletAccumulation * w.walletAccumulation,
    buyerAcceleration: buyerAcceleration * w.buyerAcceleration,
    liquidity: liquidity * w.liquidity,
    attentionAcceleration: attentionAcceleration * w.attentionAcceleration,
    amplifierPotential: amplifierPotential * w.amplifierPotential,

    bundleRisk: -bundleRisk * w.bundleRisk,
    holderRisk: -holderRisk * w.holderRisk,
    devRisk: -devRisk * w.devRisk,
    walletDistribution: -walletDistribution * w.walletDistribution,
    whaleDistribution: -whaleDistribution * w.whaleDistribution,
    priceOverextension: -priceOverextension * w.priceOverextension,
    liquidityDeterioration: -liquidityDeterioration * w.liquidityDeterioration,
    correlationPenalty: -correlationPenalty * w.correlationPenalty,
  };

  const rawTotal =
    components.independentConvergence +
    components.sourceQuality +
    components.convergenceVelocity +
    components.walletAccumulation +
    components.buyerAcceleration +
    components.liquidity +
    components.attentionAcceleration +
    components.amplifierPotential +
    components.bundleRisk +
    components.holderRisk +
    components.devRisk +
    components.walletDistribution +
    components.whaleDistribution +
    components.priceOverextension +
    components.liquidityDeterioration +
    components.correlationPenalty;

  const maxPositive =
    w.independentConvergence +
    w.sourceQuality +
    w.convergenceVelocity +
    w.walletAccumulation +
    w.buyerAcceleration +
    w.liquidity +
    w.attentionAcceleration +
    w.amplifierPotential;
  const maxNegative =
    w.bundleRisk +
    w.holderRisk +
    w.devRisk +
    w.walletDistribution +
    w.whaleDistribution +
    w.priceOverextension +
    w.liquidityDeterioration +
    w.correlationPenalty;

  const normalized = scaleTo100(rawTotal, -maxNegative, maxPositive);

  return {
    score: Math.round(normalized * 10) / 10,
    components,
  };
}

function normalizeCluster(count, cap) {
  return Math.max(0, Math.min(1, (count || 0) / cap));
}

function normalizeUnit(value, cap) {
  if (value == null || !Number.isFinite(Number(value))) return 0;
  return Math.max(0, Math.min(1, Number(value) / cap));
}

function liquidityScore(liquidityUsd) {
  if (liquidityUsd == null) return 0.3;
  if (liquidityUsd >= 50000) return 1;
  if (liquidityUsd >= 20000) return 0.75;
  if (liquidityUsd >= 10000) return 0.5;
  return 0.2;
}

function pctRisk(pct, threshold) {
  if (pct == null) return 0;
  return Math.max(0, Math.min(1, pct / threshold));
}

function scaleTo100(value, min, max) {
  if (max <= min) return 50;
  return ((value - min) / (max - min)) * 100;
}

module.exports = {
  scoreSignal,
};
