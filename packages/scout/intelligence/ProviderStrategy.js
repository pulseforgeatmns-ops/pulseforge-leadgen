'use strict';

/**
 * SPEC-141 Stage 3 — Provider Strategy.
 * Evidence-driven provider selection. Scout decides where to look.
 */

const { createDefaultProviderRegistry, COST_TIER_ORDER } = require('./ProviderCapabilityRegistry');

/**
 * Map evidence plan to provider assignments with cost optimization.
 * Rules: prefer free → cached → local → paid.
 *
 * @param {object} evidencePlan
 * @param {object} [opts]
 * @returns {object}
 */
function buildProviderStrategy(evidencePlan, opts = {}) {
  const registry = opts.registry || createDefaultProviderRegistry();
  const capabilities = evidencePlan.capabilities || [];

  const assignments = registry.selectForCapabilities(capabilities, {
    allowMultiplePerCapability: false,
  });

  const byRequirement = (evidencePlan.requirementCapabilities || []).map((row) => {
    const match = assignments.find((a) => a.capability === row.capability);
    return {
      requirement: row.requirement,
      capability: row.capability,
      providerId: match ? match.providerId : null,
      providerLabel: match ? match.label : null,
      costTier: match ? match.costTier : null,
    };
  });

  const unavailable = (evidencePlan.requirementCapabilities || [])
    .filter((row) => !assignments.some((a) => a.capability === row.capability))
    .map((row) => row.requirement);

  const costSummary = {
    free: assignments.filter((a) => a.costTier === 'free').length,
    cached: assignments.filter((a) => a.costTier === 'cached').length,
    local: assignments.filter((a) => a.costTier === 'local').length,
    paid: assignments.filter((a) => a.costTier === 'paid').length,
  };

  return {
    assignments,
    byRequirement,
    providers: [...new Set(assignments.map((a) => a.providerId))],
    unavailableRequirements: unavailable,
    optimizationOrder: COST_TIER_ORDER.slice(),
    costSummary,
    rationale:
      'Provider selection is evidence-driven. Lower-cost sources preferred when capable.',
  };
}

module.exports = {
  buildProviderStrategy,
};
