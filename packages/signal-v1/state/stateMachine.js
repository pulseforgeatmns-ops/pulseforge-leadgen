'use strict';

const { DEFAULT_STATE_THRESHOLDS } = require('../config/defaultConfig');

/**
 * Deterministic state transition from features + score + prior state.
 *
 * @param {object} input
 * @param {import('../types').SignalState|null} [input.previousState]
 * @param {number} input.score
 * @param {object} input.features
 * @param {object} [input.position] — open paper position context
 * @param {Partial<typeof DEFAULT_STATE_THRESHOLDS>} [input.thresholds]
 */
function decideSignalState(input) {
  const thresholds = { ...DEFAULT_STATE_THRESHOLDS, ...(input.thresholds || {}) };
  const { features, score, previousState = null, position = null } = input;

  const risks = [];
  const reasons = [];

  if (features.bundledSupplyPct != null && features.bundledSupplyPct > thresholds.maxBundleSupplyPct) {
    risks.push(`bundled supply estimated at ${Math.round(features.bundledSupplyPct)}%`);
  }
  if (features.top10HolderPct != null && features.top10HolderPct > thresholds.maxTop10HolderPct) {
    risks.push(`top-10 holder concentration at ${Math.round(features.top10HolderPct)}%`);
  }
  if (features.devHoldingPct != null && features.devHoldingPct > thresholds.maxDevHoldingPct) {
    risks.push(`dev holding ${Math.round(features.devHoldingPct)}% exceeds threshold`);
  }
  if (features.liquidityUsd != null && features.liquidityUsd < thresholds.minLiquidityUsd) {
    risks.push('liquidity below preferred threshold');
  }
  if (features.whaleDistributionDetected) risks.push('whale distribution detected');
  if (features.devDistributionDetected) risks.push('dev distribution detected');
  if (features.smartWalletDistributionDetected) risks.push('smart-wallet distribution detected');
  if (features.liquidityDeteriorationDetected) risks.push('liquidity deterioration detected');

  if (features.independentClusterCount >= 2) {
    reasons.push(
      `${features.independentClusterCount} independent source clusters mentioned token recently`
    );
  } else if (features.rawSourceCount > 0) {
    reasons.push(`${features.rawSourceCount} raw source mentions (cluster-adjusted)`);
  }
  if (features.profitableWalletBuyCount > 0) {
    reasons.push(`${features.profitableWalletBuyCount} historically profitable tracked wallet buy(s)`);
  }
  if (features.volumeAcceleration != null && features.volumeAcceleration >= 2) {
    reasons.push(`unique buyer / volume velocity increased ${features.volumeAcceleration.toFixed(1)}x`);
  }

  const hardReject =
    features.liquidityDeteriorationDetected ||
    features.devDistributionDetected ||
    (features.bundledSupplyPct != null && features.bundledSupplyPct > thresholds.maxBundleSupplyPct + 10) ||
    score <= thresholds.rejectScoreMax;

  const distributionExit =
    features.smartWalletDistributionDetected ||
    features.whaleDistributionDetected ||
    (features.walletDistributionScore != null && features.walletDistributionScore >= 0.65);

  let state = previousState || 'WATCH';

  if (hardReject && !position) {
    state = 'REJECT';
  } else if (position && (distributionExit || score < thresholds.watchScoreMin)) {
    state = 'EXIT';
  } else if (position && position.unrealizedGainPct != null && position.unrealizedGainPct >= thresholds.deRiskGainPct) {
    state = 'DE_RISK';
  } else if (
    !position &&
    score >= thresholds.entryScoreMin &&
    features.independentClusterCount >= thresholds.minIndependentClustersForEntry &&
    risks.filter(r => r.includes('liquidity') || r.includes('bundled')).length === 0
  ) {
    state = 'ENTRY';
  } else if (!position && score >= thresholds.watchScoreMin) {
    state = 'WATCH';
  } else if (!position && hardReject) {
    state = 'REJECT';
  } else if (!position) {
    state = score < thresholds.watchScoreMin ? 'REJECT' : 'WATCH';
  }

  if (state === 'ENTRY' && features.profitableWalletBuyCount === 0 && features.independentClusterCount < 2) {
    state = 'WATCH';
    reasons.push('entry gated: awaiting wallet confirmation or stronger independent convergence');
  }

  return {
    state,
    score,
    reasons,
    risks,
  };
}

module.exports = {
  decideSignalState,
};
