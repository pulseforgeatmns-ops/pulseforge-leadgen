'use strict';

/**
 * SPEC-123 — discovery strategy selection (internal optimization).
 */

const { DISCOVERY_STRATEGIES } = require('./types');

/**
 * Select discovery strategy from gap analysis — internal optimization only.
 * @param {object} gapAnalysis
 * @param {object} existing
 * @returns {string}
 */
function selectDiscoveryStrategy(gapAnalysis, existing) {
  const existingCount = ((existing && existing.companies) || []).length;
  const freshCount = gapAnalysis.freshCount || 0;

  if (existingCount === 0) {
    return DISCOVERY_STRATEGIES.EXTERNAL_HEAVY;
  }
  if (freshCount > 0 && gapAnalysis.shouldDiscoverGap) {
    return DISCOVERY_STRATEGIES.HYBRID;
  }
  if (freshCount > 0 && !gapAnalysis.shouldDiscoverGap) {
    return DISCOVERY_STRATEGIES.RETRIEVE_ONLY;
  }
  if (existingCount > 0 && freshCount === 0) {
    return DISCOVERY_STRATEGIES.VERIFICATION_ONLY;
  }
  return DISCOVERY_STRATEGIES.HYBRID;
}

function buildDelegationFromMission(mission, scoutPayload = {}) {
  const constraints = mission.constraints || {};
  const plan = (mission.plan && mission.plan.missionPlan) || mission.missionPlan || {};
  const profileGeo =
    constraints.discoveryProfile &&
    constraints.discoveryProfile.geography &&
    (constraints.discoveryProfile.geography.label ||
      (Array.isArray(constraints.discoveryProfile.geography.cities)
        ? constraints.discoveryProfile.geography.cities.join(', ')
        : null));
  const geography =
    scoutPayload.geography ||
    constraints.locationHint ||
    profileGeo ||
    (plan.geography && plan.geography.label) ||
    null;
  return {
    tenantId: String(mission.tenantId || mission.clientId || scoutPayload.tenantId || ''),
    targetContext: {
      geography,
      segments: constraints.vertical
        ? [constraints.vertical]
        : constraints.discoveryProfile &&
            Array.isArray(constraints.discoveryProfile.industryTargets)
          ? constraints.discoveryProfile.industryTargets.slice(0, 1)
          : [],
      businessType: constraints.vertical || constraints.industry || null,
    },
    businessContext: {
      serviceGeography: geography,
      preferredSegments: constraints.vertical
        ? [constraints.vertical]
        : constraints.discoveryProfile &&
            Array.isArray(constraints.discoveryProfile.industryTargets)
          ? constraints.discoveryProfile.industryTargets.slice(0, 1)
          : [],
      operatorDirection: scoutPayload.operatorMessage || null,
      commercialCapability:
        constraints.discoveryProfile &&
        /cleaning/i.test(String(constraints.discoveryProfile.name || ''))
          ? 'commercial_cleaning'
          : null,
    },
  };
}

module.exports = {
  selectDiscoveryStrategy,
  buildDelegationFromMission,
};
