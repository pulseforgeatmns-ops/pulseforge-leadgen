'use strict';

/**
 * SPEC-141 Stage 1 — Market Understanding.
 * SPEC-158 — Semantic Market Definition (Scout brain).
 * SPEC-178 / ADR-093 — Canonical Market Definition facade.
 *
 * Invariant: every investigation begins from a Market Definition, not operator wording.
 * SearchDefinition is always a projection of MarketDefinition — never a semantic source.
 */

const {
  buildAcquisitionSearchDefinition,
  buildSearchDefinitionFromMarketDefinition,
} = require('../../max/scoutAcquisition/SearchDefinition');
const { buildDelegationFromMission } = require('../Discovery.helpers');
const {
  buildSemanticMarketDefinition,
  conceptsFromMarketDefinition,
  resolveCanonicalSegmentKey,
} = require('./MarketDefinition');
const { applyTerminologyLearning } = require('../memory/TerminologyLearning');

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

function suppliedSegments(delegation = {}) {
  const target = delegation.targetContext || {};
  const business = delegation.businessContext || {};
  if (Array.isArray(target.segments) && target.segments.length) return target.segments.slice();
  if (Array.isArray(business.preferredSegments) && business.preferredSegments.length) {
    return business.preferredSegments.slice();
  }
  return [];
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
  const structured = mission.structuredMission || null;
  const marketPlan = (structured && structured.market) || (plan && plan.market) || {};

  const geography =
    asText(delegation.targetContext && delegation.targetContext.geography) ||
    asText(delegation.businessContext && delegation.businessContext.serviceGeography) ||
    asText(constraints.locationHint) ||
    (plan.geography && plan.geography.label) ||
    (structured && structured.geography && structured.geography.region) ||
    null;

  const segments = suppliedSegments(delegation);
  const canonical = resolveCanonicalSegmentKey({
    mission,
    segments,
    operatorObjective: mission.objectiveText || mission.objective,
    operatorDirection:
      asText(delegation.businessContext && delegation.businessContext.operatorDirection) ||
      asText(input.scoutPayload && input.scoutPayload.operatorMessage) ||
      null,
    missionSegment: marketPlan.segment,
    constraintVertical: constraints.vertical,
  });

  const operatorSegment =
    asText(marketPlan.label) ||
    (segments[0] ? String(segments[0]).replace(/_/g, ' ') : null) ||
    String(canonical.segmentKey).replace(/_/g, ' ');

  let semantic = buildSemanticMarketDefinition({
    mission,
    segments: [canonical.segmentKey],
    segmentKey: canonical.segmentKey,
    segmentSource: canonical.source,
    geography,
    operatorSegment,
    operatorObjective: mission.objectiveText || mission.objective,
    missionSegment: marketPlan.segment,
    constraintVertical: constraints.vertical,
  });

  if (input.terminologyLearning || input.opts?.terminologyLearning) {
    semantic = applyTerminologyLearning(semantic, input.terminologyLearning || input.opts.terminologyLearning);
  }

  const searchDefinition = buildSearchDefinitionFromMarketDefinition(
    {
      ...semantic,
      geography,
      tenantId: asText(delegation.tenantId || mission.tenantId || mission.clientId),
      missionGoal:
        asText(mission.objectiveText || mission.objective || mission.title) ||
        asText(input.scoutPayload && input.scoutPayload.objective) ||
        null,
    },
    {
      delegation,
      tenantId: asText(delegation.tenantId || mission.tenantId || mission.clientId),
      authorizedTenantId: asText(delegation.tenantId || mission.tenantId || mission.clientId),
      targetContext: delegation.targetContext,
      businessContext: delegation.businessContext,
    }
  );

  return {
    segment: operatorSegment,
    segments: [canonical.segmentKey],
    geography,
    buyer:
      asText(marketPlan.buyer) ||
      asText(delegation.targetContext && delegation.targetContext.businessType) ||
      asText(delegation.businessContext && delegation.businessContext.commercialCapability) ||
      searchDefinition.businessNeed ||
      null,
    industry:
      asText(marketPlan.industry) ||
      asText(constraints.industry) ||
      canonical.segmentKey ||
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
    segmentResolutionSource: semantic.segmentResolutionSource,
    semanticSource: semantic.source,
    searchConcepts: conceptsFromMarketDefinition(semantic),
  };
}

module.exports = {
  buildMarketDefinition,
  buildDelegationFromMission,
};
