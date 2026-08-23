'use strict';

/**
 * SPEC-142 — Investigation Planner.
 * Select the lowest-cost investigation to resolve uncertainty.
 */

const { buildInvestigationStep } = require('./types');
const { GAP_TO_EVIDENCE_TYPES } = require('./MissingEvidence');
const {
  createDefaultProviderRegistry,
  EVIDENCE_CAPABILITIES,
  COST_TIER_ORDER,
} = require('../intelligence/ProviderCapabilityRegistry');

const GAP_TO_CAPABILITY = Object.freeze({
  decision_maker: EVIDENCE_CAPABILITIES.PEOPLE,
  portfolio_size: EVIDENCE_CAPABILITIES.PROPERTY_COUNT,
  cleaning_responsibility: EVIDENCE_CAPABILITIES.WEBSITE,
  contact_path: EVIDENCE_CAPABILITIES.EMAILS,
  buying_signals: EVIDENCE_CAPABILITIES.BUYING_SIGNALS,
  business_fit: EVIDENCE_CAPABILITIES.WEBSITE,
  geographic_fit: EVIDENCE_CAPABILITIES.BUSINESSES,
  vendor_relationship: EVIDENCE_CAPABILITIES.BUYING_SIGNALS,
  company_size: EVIDENCE_CAPABILITIES.GROWTH,
  ownership: EVIDENCE_CAPABILITIES.OWNERSHIP,
});

const EVIDENCE_TYPE_TO_CAPABILITY = Object.freeze({
  website: EVIDENCE_CAPABILITIES.WEBSITE,
  linkedin: EVIDENCE_CAPABILITIES.PEOPLE,
  contacts: EVIDENCE_CAPABILITIES.CONTACTS,
  people: EVIDENCE_CAPABILITIES.PEOPLE,
  emails: EVIDENCE_CAPABILITIES.EMAILS,
  phone: EVIDENCE_CAPABILITIES.PHONE,
  reviews: EVIDENCE_CAPABILITIES.REVIEWS,
  news: EVIDENCE_CAPABILITIES.NEWS,
  hiring_activity: EVIDENCE_CAPABILITIES.HIRING,
  property_portfolio: EVIDENCE_CAPABILITIES.PROPERTY_COUNT,
  county_records: EVIDENCE_CAPABILITIES.COUNTY_RECORDS,
  vendor_references: EVIDENCE_CAPABILITIES.BUYING_SIGNALS,
});

const COST_TIER_SCORE = Object.freeze({
  free: 1,
  cached: 2,
  local: 4,
  paid: 8,
});

function costScoreForTier(tier) {
  return COST_TIER_SCORE[tier] != null ? COST_TIER_SCORE[tier] : 10;
}

function resolveCapability(gapOrEvidenceType) {
  const key = String(gapOrEvidenceType || '').toLowerCase();
  if (GAP_TO_CAPABILITY[key]) return GAP_TO_CAPABILITY[key];
  if (EVIDENCE_TYPE_TO_CAPABILITY[key]) return EVIDENCE_TYPE_TO_CAPABILITY[key];
  return key;
}

/**
 * Build investigation chain for a gap (cheapest providers first).
 * @param {string} gap
 * @param {object} [opts]
 * @returns {object[]}
 */
function planInvestigationChain(gap, opts = {}) {
  const registry = opts.registry || createDefaultProviderRegistry();
  const evidenceTypes = GAP_TO_EVIDENCE_TYPES[gap] || [gap];
  const steps = [];
  const seen = new Set();

  for (const evidenceType of evidenceTypes) {
    const capability = resolveCapability(evidenceType);
    const providers = registry
      .selectForCapabilities([capability], { allowMultiplePerCapability: true })
      .filter((p) => !seen.has(`${p.providerId}:${capability}`));

    for (const provider of providers) {
      seen.add(`${provider.providerId}:${capability}`);
      steps.push(
        buildInvestigationStep({
          gap,
          capability,
          providerId: provider.providerId,
          providerLabel: provider.label,
          costTier: provider.costTier,
          costScore: costScoreForTier(provider.costTier),
          entityId: opts.entityId || null,
          rationale: `Resolve ${gap} via ${provider.label} (${evidenceType})`,
        })
      );
    }
  }

  return steps.sort((a, b) => a.costScore - b.costScore);
}

/**
 * Select the next best investigation step.
 * @param {object} input
 * @returns {object|null}
 */
function selectNextInvestigation(input = {}) {
  const missing = input.missing || [];
  const attempted = new Set(input.attempted || []);
  const resolvedGaps = new Set(input.resolvedGaps || []);
  const registry = input.registry || createDefaultProviderRegistry();

  const priorityGaps = missing.filter((g) => !resolvedGaps.has(g));
  if (priorityGaps.length === 0) return null;

  const chains = [];
  for (const gap of priorityGaps) {
    chains.push(...planInvestigationChain(gap, { registry, entityId: input.entityId }));
  }

  const available = chains.filter((step) => {
    const key = `${step.entityId || 'global'}:${step.gap}:${step.providerId}:${step.capability}`;
    return !attempted.has(key);
  });

  if (available.length === 0) return null;

  available.sort((a, b) => {
    if (a.costScore !== b.costScore) return a.costScore - b.costScore;
    return COST_TIER_ORDER.indexOf(a.costTier) - COST_TIER_ORDER.indexOf(b.costTier);
  });

  return available[0];
}

/**
 * Mark steps skipped when earlier evidence resolved the gap.
 * @param {object[]} chain
 * @param {Set<string>} resolvedGaps
 * @returns {object[]}
 */
function applyDynamicReplanning(chain, resolvedGaps) {
  const resolved = new Set(resolvedGaps);
  let skipRemainingForGap = null;

  return chain.map((step) => {
    if (resolved.has(step.gap)) {
      return { ...step, skipped: true, skipReason: `${step.gap} already resolved` };
    }
    if (skipRemainingForGap === step.gap) {
      return { ...step, skipped: true, skipReason: `Earlier step resolved ${step.gap}` };
    }
    if (!step.skipped && resolved.has(step.gap)) skipRemainingForGap = step.gap;
    return step;
  });
}

module.exports = {
  GAP_TO_CAPABILITY,
  EVIDENCE_TYPE_TO_CAPABILITY,
  COST_TIER_SCORE,
  planInvestigationChain,
  selectNextInvestigation,
  applyDynamicReplanning,
  costScoreForTier,
};
