'use strict';

/**
 * SPEC-145 — Provider learning feedback.
 * Second Brain learns which providers answer which questions well.
 */

const { GAP_TO_CAPABILITY } = require('./GapCapabilities');
const { EVIDENCE_CAPABILITIES } = require('../intelligence/ProviderCapabilityRegistry');

/** Baseline effectiveness: provider × gap (0–1). */
const DEFAULT_PROVIDER_EFFECTIVENESS = Object.freeze({
  linkedin: {
    decision_maker: 0.9,
    ownership: 0.75,
    company_size: 0.7,
    buying_signals: 0.55,
    portfolio_size: 0.35,
    geographic_fit: 0.2,
    contact_path: 0.45,
  },
  website: {
    business_fit: 0.7,
    cleaning_responsibility: 0.55,
    portfolio_size: 0.4,
    geographic_fit: 0.45,
    buying_signals: 0.35,
    decision_maker: 0.3,
    contact_path: 0.25,
    ownership: 0.35,
  },
  google_maps: {
    geographic_fit: 0.85,
    business_fit: 0.6,
    contact_path: 0.5,
    portfolio_size: 0.15,
    ownership: 0.2,
    decision_maker: 0.1,
  },
  prospeo: {
    contact_path: 0.85,
    decision_maker: 0.55,
    company_size: 0.4,
  },
  hunter: {
    contact_path: 0.75,
    decision_maker: 0.4,
  },
  county_records: {
    portfolio_size: 0.85,
    ownership: 0.7,
    property_count: 0.9,
    decision_maker: 0.15,
    geographic_fit: 0.3,
  },
  news: {
    buying_signals: 0.75,
    expansion_plans: 0.7,
    vendor_relationship: 0.45,
    portfolio_size: 0.3,
  },
  existing_pf: {
    decision_maker: 0.5,
    contact_path: 0.55,
    business_fit: 0.45,
    portfolio_size: 0.4,
  },
});

const LEARNING_DECAY = 0.85;
const LEARNING_WEIGHT = 0.15;

function normalizeKey(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[\s-]+/g, '_');
}

function createProviderLearningStore(seed = DEFAULT_PROVIDER_EFFECTIVENESS) {
  const effectiveness = JSON.parse(JSON.stringify(seed));
  const observations = {};

  function obsKey(providerId, gap) {
    return `${normalizeKey(providerId)}:${normalizeKey(gap)}`;
  }

  function getEffectiveness(providerId, gap) {
    const pid = normalizeKey(providerId);
    const g = normalizeKey(gap);
    const providerGaps = effectiveness[pid];
    if (providerGaps && providerGaps[g] != null) {
      return providerGaps[g];
    }
    const capMap = GAP_TO_CAPABILITY || {};
    const cap = capMap[g];
    if (cap && providerGaps) {
      const capEntries = Object.entries(providerGaps).filter(([k]) => capMap[k] === cap);
      if (capEntries.length) {
        return capEntries.reduce((s, [, v]) => s + v, 0) / capEntries.length;
      }
    }
    return 0.35;
  }

  function recordOutcome(providerId, gap, outcome = {}) {
    const key = obsKey(providerId, gap);
    if (!observations[key]) {
      observations[key] = { attempts: 0, successes: 0, partial: 0, failures: 0 };
    }
    const obs = observations[key];
    obs.attempts += 1;
    if (outcome.resolved) obs.successes += 1;
    else if (outcome.partial) obs.partial += 1;
    else obs.failures += 1;

    const pid = normalizeKey(providerId);
    const g = normalizeKey(gap);
    if (!effectiveness[pid]) effectiveness[pid] = {};

    const prior = getEffectiveness(pid, g);
    let signal = 0;
    if (outcome.resolved) signal = 1;
    else if (outcome.partial) signal = 0.5;

    const updated = prior * LEARNING_DECAY + signal * LEARNING_WEIGHT + (1 - LEARNING_DECAY - LEARNING_WEIGHT) * prior;
    effectiveness[pid][g] = Number(Math.min(0.98, Math.max(0.05, updated)).toFixed(3));

    return {
      providerId: pid,
      gap: g,
      prior,
      updated: effectiveness[pid][g],
      observations: { ...obs },
    };
  }

  function getBestProvidersForGap(gap, limit = 3) {
    const g = normalizeKey(gap);
    const ranked = Object.entries(effectiveness)
      .map(([providerId, gaps]) => ({
        providerId,
        gap: g,
        effectiveness: gaps && gaps[g] != null ? gaps[g] : getEffectiveness(providerId, g),
      }))
      .sort((a, b) => b.effectiveness - a.effectiveness)
      .slice(0, limit);
    return ranked;
  }

  function summarize() {
    const patterns = [];
    for (const [providerId, gaps] of Object.entries(effectiveness)) {
      const top = Object.entries(gaps).sort((a, b) => b[1] - a[1]).slice(0, 2);
      for (const [gap, score] of top) {
        if (score >= 0.7) {
          patterns.push({
            provider: providerId,
            gap,
            effectiveness: score,
            rating: score >= 0.8 ? 'excellent' : 'good',
          });
        }
      }
      const weak = Object.entries(gaps).sort((a, b) => a[1] - b[1]).slice(0, 1);
      for (const [gap, score] of weak) {
        if (score <= 0.25) {
          patterns.push({
            provider: providerId,
            gap,
            effectiveness: score,
            rating: 'poor',
          });
        }
      }
    }
    return { effectiveness, observations, patterns };
  }

  return {
    effectiveness,
    observations,
    getEffectiveness,
    recordOutcome,
    getBestProvidersForGap,
    summarize,
  };
}

/**
 * Estimate expected information gain for a provider answering a gap.
 * @param {object} input
 * @returns {number}
 */
function estimateInformationGain(input = {}) {
  const gapImpact = input.gapImpact != null ? input.gapImpact : 0.5;
  const providerEffectiveness =
    input.providerEffectiveness != null ? input.providerEffectiveness : 0.35;
  const providerCoverage = input.providerCoverage != null ? input.providerCoverage : 0.5;
  const providerReliability = input.providerReliability != null ? input.providerReliability : 0.7;
  const diminishingFactor = input.diminishingFactor != null ? input.diminishingFactor : 1;

  const raw =
    gapImpact * providerEffectiveness * providerCoverage * providerReliability * diminishingFactor;
  return Number(Math.min(0.99, Math.max(0, raw)).toFixed(3));
}

function loadLearningFromMemory(memory = {}) {
  const investigation = memory.investigation || {};
  const seed = investigation.providerLearning || DEFAULT_PROVIDER_EFFECTIVENESS;
  return createProviderLearningStore(seed);
}

function exportLearningForMemory(learningStore) {
  return learningStore.effectiveness;
}

module.exports = {
  DEFAULT_PROVIDER_EFFECTIVENESS,
  EVIDENCE_CAPABILITIES,
  createProviderLearningStore,
  estimateInformationGain,
  loadLearningFromMemory,
  exportLearningForMemory,
};
