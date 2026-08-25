'use strict';

/**
 * SPEC-141 Stage 1 — Market Understanding.
 * SPEC-158 — Semantic Market Definition (Scout brain).
 * Answer: What market am I actually investigating?
 *
 * Invariant: every investigation begins from a Market Definition, not operator wording.
 */

const { buildAcquisitionSearchDefinition } = require('../../max/scoutAcquisition/SearchDefinition');
const { buildDelegationFromMission } = require('../Discovery.helpers');
const {
  buildSemanticMarketDefinition,
  conceptsFromMarketDefinition,
} = require('./MarketDefinition');
const { applyTerminologyLearning } = require('../memory/TerminologyLearning');

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

  const operatorSegment = segments[0] ? String(segments[0]).replace(/_/g, ' ') : null;

  let semantic = buildSemanticMarketDefinition({
    mission,
    segments,
    geography,
    operatorSegment,
    searchDefinition,
  });

  if (input.terminologyLearning || input.opts?.terminologyLearning) {
    semantic = applyTerminologyLearning(semantic, input.terminologyLearning || input.opts.terminologyLearning);
  }

  return {
    segment: operatorSegment,
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
    // SPEC-158 semantic market model
    market: semantic.market,
    customerTypes: semantic.customerTypes,
    decisionMakers: semantic.decisionMakers,
    businessModels: semantic.businessModels,
    terminology: semantic.terminology,
    adjacentMarkets: semantic.adjacentMarkets,
    exclusions: semantic.exclusions,
    buyingSignals: semantic.buyingSignals,
    expectedEvidence: semantic.expectedEvidence,
    operatorSegment: semantic.operatorSegment,
    segmentKey: semantic.segmentKey,
    semanticSource: semantic.source,
    searchConcepts: conceptsFromMarketDefinition(semantic),
  };
}

module.exports = {
  buildMarketDefinition,
  buildDelegationFromMission,
};
