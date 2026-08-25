'use strict';

/**
 * SPEC-141 Stage 1 — Market Understanding.
 * Answer: What market am I actually investigating?
 */

const { buildAcquisitionSearchDefinition } = require('../../max/scoutAcquisition/SearchDefinition');
const { buildDelegationFromMission } = require('../Discovery.helpers');

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

/**
 * Derive a structured market definition from mission/delegation input.
 *
 * @param {object} input
 * @param {object} [input.mission]
 * @param {object} [input.delegation]
 * @param {object} [input.scoutPayload]
 * @returns {object}
 */
function buildMarketDefinition(input = {}) {
  const mission = input.mission || {};
  const delegation = input.delegation || buildDelegationFromMission(mission, input.scoutPayload);
  const constraints = mission.constraints || {};
  const plan = (mission.plan && mission.plan.missionPlan) || mission.missionPlan || {};

  const searchDefinition = buildAcquisitionSearchDefinition({
    delegation,
    tenantId: asText(delegation.tenantId || mission.tenantId || mission.clientId),
    authorizedTenantId: asText(delegation.tenantId || mission.tenantId || mission.clientId),
    targetContext: delegation.targetContext,
    businessContext: delegation.businessContext,
  });

  const segments = Array.isArray(delegation.targetContext && delegation.targetContext.segments)
    ? delegation.targetContext.segments
    : searchDefinition.segments || [];

  const geography =
    asText(delegation.targetContext && delegation.targetContext.geography) ||
    asText(delegation.businessContext && delegation.businessContext.serviceGeography) ||
    (searchDefinition.geography && searchDefinition.geography.label) ||
    asText(constraints.locationHint) ||
    (plan.geography && plan.geography.label) ||
    null;

  return {
    segment: segments[0] ? String(segments[0]).replace(/_/g, ' ') : null,
    segments,
    geography,
    buyer:
      asText(delegation.targetContext && delegation.targetContext.businessType) ||
      asText(delegation.businessContext && delegation.businessContext.commercialCapability) ||
      searchDefinition.businessNeed ||
      null,
    industry:
      asText(constraints.industry) ||
      asText(constraints.vertical) ||
      segments[0] ||
      null,
    missionGoal:
      asText(mission.objectiveText || mission.objective || mission.title) ||
      asText(input.scoutPayload && input.scoutPayload.objective) ||
      searchDefinition.populationStatement ||
      null,
    tenantId: asText(delegation.tenantId || mission.tenantId || mission.clientId),
    searchDefinition,
    valid: searchDefinition.valid === true,
    invalidReason: searchDefinition.invalidReason || null,
  };
}

module.exports = {
  buildMarketDefinition,
  buildDelegationFromMission,
};
