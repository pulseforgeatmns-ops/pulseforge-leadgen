'use strict';

const { DEFAULT_RESEARCH_CONFIG } = require('../config/defaultConfig');

/**
 * @typedef {'PASS' | 'FAIL' | 'UNKNOWN'} StructureGateResult
 */

/**
 * Evaluate structural evidence at decision time. Missing data → UNKNOWN (not safe).
 *
 * @param {object} features — feature snapshot fields
 * @param {Partial<typeof DEFAULT_RESEARCH_CONFIG.structureGate>} [gateConfig]
 * @returns {{ result: StructureGateResult, components: Record<string, unknown> }}
 */
function evaluateStructureGate(features, gateConfig = {}) {
  const cfg = { ...DEFAULT_RESEARCH_CONFIG.structureGate, ...gateConfig };
  const components = {
    liquidityUsd: features.liquidityUsd,
    bundledSupplyPct: features.bundledSupplyPct,
    top10HolderPct: features.top10HolderPct,
    devHoldingPct: features.devHoldingPct,
    tokenAgeSeconds: features.tokenAgeSeconds,
    thresholds: cfg,
  };

  const checks = [];

  if (features.liquidityUsd == null) {
    checks.push('unknown');
  } else if (features.liquidityUsd < cfg.minLiquidityUsd) {
    checks.push('fail');
  } else {
    checks.push('pass');
  }

  if (features.bundledSupplyPct == null) {
    checks.push('unknown');
  } else if (features.bundledSupplyPct > cfg.maxBundleSupplyPct) {
    checks.push('fail');
  } else {
    checks.push('pass');
  }

  if (features.top10HolderPct == null) {
    checks.push('unknown');
  } else if (features.top10HolderPct > cfg.maxTop10HolderPct) {
    checks.push('fail');
  } else {
    checks.push('pass');
  }

  if (features.devHoldingPct == null) {
    checks.push('unknown');
  } else if (features.devHoldingPct > cfg.maxDevHoldingPct) {
    checks.push('fail');
  } else {
    checks.push('pass');
  }

  if (features.tokenAgeSeconds == null) {
    checks.push('unknown');
  } else if (features.tokenAgeSeconds < cfg.minTokenAgeSeconds) {
    checks.push('fail');
  } else {
    checks.push('pass');
  }

  if (checks.includes('unknown')) {
    return { result: 'UNKNOWN', components: { ...components, checks } };
  }
  if (checks.includes('fail')) {
    return { result: 'FAIL', components: { ...components, checks } };
  }
  return { result: 'PASS', components: { ...components, checks } };
}

module.exports = {
  evaluateStructureGate,
};
