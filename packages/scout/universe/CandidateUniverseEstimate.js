'use strict';

/**
 * SPEC-155 — Candidate Universe Estimation (ADR-078: Estimate Before Investigation).
 *
 * Every investigation begins with an estimated candidate universe.
 * Coverage percentages may only be computed relative to an explicit estimate.
 */

const INDUSTRY_DENSITY_RATIOS = Object.freeze({
  law_firm: 0.00018,
  accounting: 0.00014,
  restaurant: 0.0012,
  salon: 0.0009,
  fitness: 0.0006,
  cleaning: 0.0008,
  landscaping: 0.0007,
  hvac: 0.0005,
  dental: 0.0004,
  auto_repair: 0.0006,
  property: 0.0003,
  med_spa: 0.00025,
  default: 0.0005,
});

function clamp01(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Math.max(0, Math.min(1, n));
}

function asPositiveInt(value, fallback = 0) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return fallback;
  return Math.round(n);
}

function normalizeSegment(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_|_$/g, '');
}

function getIndustryDensityRatio(segment) {
  const key = normalizeSegment(segment);
  return INDUSTRY_DENSITY_RATIOS[key] || INDUSTRY_DENSITY_RATIOS.default;
}

/**
 * @typedef {object} CandidateUniverseEstimate
 * @property {number} minimum
 * @property {number} expected
 * @property {number} maximum
 * @property {number} confidence
 * @property {string[]} reasoning
 * @property {object[]} [signals]
 * @property {object[]} [revisionHistory]
 * @property {string} [estimatedAt]
 */

/**
 * Normalize a partial estimate into the canonical CandidateUniverseEstimate shape.
 * @param {object} partial
 * @returns {CandidateUniverseEstimate|null}
 */
function normalizeCandidateUniverseEstimate(partial) {
  if (partial == null) return null;

  if (typeof partial === 'number' && Number.isFinite(partial) && partial > 0) {
    const expected = asPositiveInt(partial, 1);
    return {
      minimum: Math.max(1, Math.round(expected * 0.6)),
      expected,
      maximum: Math.round(expected * 1.5),
      confidence: 0.45,
      reasoning: ['Converted from legacy scalar universe estimate.'],
      revisionHistory: [],
      estimatedAt: new Date().toISOString(),
    };
  }

  if (typeof partial !== 'object') return null;
  const expected = asPositiveInt(partial.expected, 0);
  if (expected <= 0) return null;

  return {
    minimum: asPositiveInt(partial.minimum, Math.max(1, Math.round(expected * 0.6))),
    expected,
    maximum: asPositiveInt(partial.maximum, Math.round(expected * 1.5)),
    confidence: clamp01(partial.confidence != null ? partial.confidence : 0.5),
    reasoning: Array.isArray(partial.reasoning)
      ? partial.reasoning.filter(Boolean).map(String)
      : [],
    signals: Array.isArray(partial.signals) ? partial.signals : [],
    revisionHistory: Array.isArray(partial.revisionHistory) ? partial.revisionHistory : [],
    estimatedAt: partial.estimatedAt || new Date().toISOString(),
  };
}

function extractExpectedValue(estimate) {
  const normalized = normalizeCandidateUniverseEstimate(estimate);
  return normalized ? normalized.expected : null;
}

function buildReasoning(signals, expected, confidence, presentSources) {
  const reasoning = [];
  for (const signal of signals) {
    if (signal.value <= 0) continue;
    switch (signal.source) {
      case 'coverage_plan':
        reasoning.push(
          `Coverage plan geometry suggests ~${signal.value} candidates across planned searches.`
        );
        break;
      case 'crm_records':
        reasoning.push(
          `Existing CRM intelligence (${Math.round(signal.value / 1.5)} relevant records) anchors the lower bound.`
        );
        break;
      case 'geographic_size':
        reasoning.push(
          `Geographic footprint (${signal.meta?.cities || '?'} cities × ${signal.meta?.concepts || '?'} concepts) implies ~${signal.value} operators.`
        );
        break;
      case 'historical_missions':
        reasoning.push(
          `Historical mission memory reports prior market size of ~${signal.value}.`
        );
        break;
      case 'industry_ratio':
        reasoning.push(
          `Industry density ratio for ${signal.meta?.segment || 'target segment'} projects ~${signal.value} operators.`
        );
        break;
      default:
        reasoning.push(`${signal.source} contributed ~${signal.value} to the estimate.`);
    }
  }

  const missing = ['coverage_plan', 'crm_records', 'geographic_size', 'historical_missions', 'industry_ratio'].filter(
    (source) => !presentSources.has(source)
  );
  if (missing.length) {
    reasoning.push(
      `Missing signals (${missing.join(', ')}) reduced estimate confidence to ${confidence.toFixed(2)}.`
    );
  } else {
    reasoning.push(`All estimation signals present; confidence ${confidence.toFixed(2)}.`);
  }

  reasoning.push(`Weighted expected universe: ${expected} operators.`);
  return reasoning;
}

/**
 * Estimate candidate universe before external discovery.
 *
 * Combines coverage plan geometry, CRM records, geography, historical missions,
 * and industry density ratios. Missing signals reduce confidence, not estimation.
 *
 * @param {object} input
 * @returns {CandidateUniverseEstimate}
 */
function estimateCandidateUniverse(input = {}) {
  const discoveryPlan = input.discoveryPlan || input.coveragePlan || {};
  const totals = discoveryPlan.totals || {};
  const existingIntelligence = input.existingIntelligence || {};
  const gapAnalysis = input.gapAnalysis || {};
  const memory = input.memory || input.investigationMemory || {};
  const marketDefinition = input.marketDefinition || {};

  const plannedSearches = asPositiveInt(totals.searches, 0);
  const plannedCities = asPositiveInt(totals.cities, 0);
  const plannedConcepts = Math.max(1, asPositiveInt(totals.concepts, 1));

  const segment =
    marketDefinition.segment ||
    (marketDefinition.searchDefinition &&
      marketDefinition.searchDefinition.segments &&
      marketDefinition.searchDefinition.segments[0]) ||
    (marketDefinition.searchDefinition &&
      marketDefinition.searchDefinition.targetContext &&
      marketDefinition.searchDefinition.targetContext.segments &&
      marketDefinition.searchDefinition.targetContext.segments[0]) ||
    null;

  const signals = [];

  const planEstimate = Math.max(plannedSearches * 3, plannedCities * plannedConcepts * 2, 1);
  signals.push({ source: 'coverage_plan', value: planEstimate, weight: 0.25 });

  const existingCount =
    asPositiveInt(existingIntelligence.companyCount, 0) +
    asPositiveInt(gapAnalysis.relevantCount, 0) +
    asPositiveInt(gapAnalysis.freshCount, 0);
  if (existingCount > 0) {
    signals.push({ source: 'crm_records', value: Math.round(existingCount * 1.5), weight: 0.2 });
  }

  const geoEstimate = plannedCities * plannedConcepts * 4;
  signals.push({
    source: 'geographic_size',
    value: geoEstimate,
    weight: 0.15,
    meta: { cities: plannedCities, concepts: plannedConcepts },
  });

  const marketSize =
    memory.marketSize ||
    (memory.marketProfile && memory.marketProfile.marketSize) ||
    (input.historicalMissions &&
      input.historicalMissions.reduce((max, row) => Math.max(max, Number(row.marketSize) || 0), 0)) ||
    null;
  if (marketSize && Number(marketSize) > 0) {
    signals.push({
      source: 'historical_missions',
      value: asPositiveInt(marketSize, 0),
      weight: 0.25,
    });
  }

  const industryRatio = getIndustryDensityRatio(segment);
  const popProxy = Math.max(plannedCities, 1) * 15000;
  const ratioEstimate = Math.max(1, Math.round(popProxy * industryRatio));
  signals.push({
    source: 'industry_ratio',
    value: ratioEstimate,
    weight: 0.15,
    meta: { segment: segment || 'default', ratio: industryRatio },
  });

  const activeSignals = signals.filter((signal) => signal.value > 0);
  const totalWeight = activeSignals.reduce((sum, signal) => sum + signal.weight, 0);
  const expected =
    totalWeight > 0
      ? Math.max(
          1,
          Math.round(
            activeSignals.reduce((sum, signal) => sum + signal.value * signal.weight, 0) / totalWeight
          )
        )
      : Math.max(planEstimate, 1);

  const presentSources = new Set(activeSignals.map((signal) => signal.source));
  const signalCoverage = presentSources.size / 5;
  const confidence = Number(
    clamp01(
      signalCoverage * 0.55 +
        (existingCount > 0 ? 0.15 : 0) +
        (marketSize ? 0.15 : 0) +
        (plannedSearches > 0 ? 0.1 : 0) +
        (plannedCities > 1 ? 0.05 : 0)
    ).toFixed(2)
  );

  const spread = 0.25 + (1 - confidence) * 0.2;
  const minimum = Math.max(1, Math.round(expected * (1 - spread)));
  const maximum = Math.max(expected + 1, Math.round(expected * (1 + spread + 0.15)));

  const reasoning = buildReasoning(activeSignals, expected, confidence, presentSources);

  return {
    minimum,
    expected,
    maximum,
    confidence,
    reasoning,
    signals: activeSignals,
    revisionHistory: [],
    estimatedAt: new Date().toISOString(),
  };
}

/**
 * Revise universe estimate when new evidence materially changes market understanding.
 *
 * @param {CandidateUniverseEstimate} current
 * @param {object} evidence
 * @returns {CandidateUniverseEstimate}
 */
function reviseCandidateUniverseEstimate(current, evidence = {}) {
  const estimate = normalizeCandidateUniverseEstimate(current);
  if (!estimate) return estimate;

  const investigated = asPositiveInt(evidence.investigated ?? evidence.discovered, 0);
  const coverageComplete = Boolean(
    evidence.coverageComplete ??
      (evidence.coverageMetrics && evidence.coverageMetrics.complete)
  );

  const exceedsMaximum = investigated > estimate.maximum * 1.15;
  const belowMinimum = coverageComplete && investigated > 0 && investigated < estimate.minimum * 0.5;
  const emptyAfterInvestigation =
    coverageComplete && investigated === 0 && estimate.expected >= 5;
  const highDiscoveryMidInvestigation =
    !coverageComplete && investigated > estimate.maximum && investigated >= 3;

  if (!exceedsMaximum && !belowMinimum && !emptyAfterInvestigation && !highDiscoveryMidInvestigation) {
    return estimate;
  }

  let reason;
  let newExpected = estimate.expected;

  if (exceedsMaximum || highDiscoveryMidInvestigation) {
    newExpected = Math.max(investigated, Math.round((estimate.expected + investigated) / 2));
    reason = `Discovered ${investigated} candidates exceeds prior maximum (${estimate.maximum}); estimate revised upward.`;
  } else if (belowMinimum) {
    newExpected = Math.max(investigated, Math.round((estimate.expected + investigated) / 2));
    reason = `Investigation found ${investigated} candidates, below prior minimum (${estimate.minimum}); estimate revised downward.`;
  } else if (emptyAfterInvestigation) {
    newExpected = Math.max(1, Math.round(estimate.expected * 0.35));
    reason =
      'Complete investigation returned zero candidates; estimate revised to reflect likely sparse market.';
  }

  const spread = 0.25 + (1 - estimate.confidence) * 0.2;
  const revised = {
    ...estimate,
    expected: newExpected,
    minimum: Math.max(1, Math.round(newExpected * (1 - spread))),
    maximum: Math.max(newExpected + 1, Math.round(newExpected * (1 + spread + 0.15))),
    revisionHistory: [
      ...(estimate.revisionHistory || []),
      {
        previous: {
          minimum: estimate.minimum,
          expected: estimate.expected,
          maximum: estimate.maximum,
        },
        revised: {
          minimum: Math.max(1, Math.round(newExpected * (1 - spread))),
          expected: newExpected,
          maximum: Math.max(newExpected + 1, Math.round(newExpected * (1 + spread + 0.15))),
        },
        reason,
        revisedAt: new Date().toISOString(),
        trigger: {
          investigated,
          coverageComplete,
        },
      },
    ],
  };

  revised.reasoning = [
    ...(estimate.reasoning || []),
    `Revision: ${reason}`,
  ];

  return revised;
}

/**
 * Compute investigation coverage relative to an explicit universe estimate.
 * Returns null when no valid estimate exists (invariant: no coverage without estimate).
 *
 * @param {number} investigated
 * @param {CandidateUniverseEstimate|number|null} estimate
 * @returns {number|null}
 */
function computeCoverageFromEstimate(investigated, estimate) {
  const expected = extractExpectedValue(estimate);
  if (expected == null || expected <= 0) return null;
  const count = asPositiveInt(investigated, 0);
  return Number((count / expected).toFixed(2));
}

/**
 * Backward-compatible scalar helper used by legacy callers.
 * @deprecated Prefer estimateCandidateUniverse() returning full estimate object.
 */
function estimateUniverseFromPlan(discoveryPlan, fallback = 0) {
  const estimate = estimateCandidateUniverse({
    discoveryPlan,
    existingIntelligence: { companyCount: Number(fallback) || 0 },
  });
  return estimate.expected;
}

module.exports = {
  INDUSTRY_DENSITY_RATIOS,
  estimateCandidateUniverse,
  reviseCandidateUniverseEstimate,
  computeCoverageFromEstimate,
  normalizeCandidateUniverseEstimate,
  extractExpectedValue,
  estimateUniverseFromPlan,
};
