'use strict';

/**
 * SPEC-123 — discovery strategy selection (internal optimization).
 */

const { DISCOVERY_STRATEGIES } = require('./types');
const { resolveCanonicalSegmentKey } = require('./intelligence/MarketDefinition');

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
  const structured = mission.structuredMission || null;
  const marketPlan = (structured && structured.market) || (plan && plan.market) || {};
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
    (structured && structured.geography && structured.geography.region) ||
    null;

  const legacySegments =
    constraints.discoveryProfile &&
    Array.isArray(constraints.discoveryProfile.industryTargets)
      ? constraints.discoveryProfile.industryTargets.slice(0, 1)
      : [];

  const canonical = resolveCanonicalSegmentKey({
    mission,
    segments: legacySegments,
    operatorObjective: mission.objectiveText || mission.objective,
    operatorDirection: scoutPayload.operatorMessage || null,
    missionSegment: marketPlan.segment,
    constraintVertical: constraints.vertical,
  });
  const segmentKey = canonical.segmentKey;

  return {
    tenantId: String(mission.tenantId || mission.clientId || scoutPayload.tenantId || ''),
    targetContext: {
      geography,
      segments: segmentKey && segmentKey !== 'general' ? [segmentKey] : legacySegments,
      businessType: segmentKey && segmentKey !== 'general' ? segmentKey : constraints.industry || null,
      missionBound: Boolean(structured && mission.structuredMissionApproved),
      structuredMission: Boolean(structured),
    },
    businessContext: {
      serviceGeography: geography,
      preferredSegments: segmentKey && segmentKey !== 'general' ? [segmentKey] : legacySegments,
      operatorDirection: scoutPayload.operatorMessage || null,
      commercialCapability:
        constraints.discoveryProfile &&
        /cleaning/i.test(String(constraints.discoveryProfile.name || ''))
          ? 'commercial_cleaning'
          : null,
      missionObjectiveImmutable: Boolean(structured && mission.structuredMissionApproved),
    },
  };
}

module.exports = {
  selectDiscoveryStrategy,
  buildDelegationFromMission,
};
