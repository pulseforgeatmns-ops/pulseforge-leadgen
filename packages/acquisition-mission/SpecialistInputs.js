'use strict';

/**
 * SPEC-130 — Specialist input contracts.
 * Each specialist receives structured mission fields — never free-form operator text.
 */

const { asText } = require('./types');
const { isStructuredMissionApproved } = require('./StructuredMission');

function requireStructuredMission(mission) {
  const plan = mission && (mission.structuredMission || mission.missionPlanDraft);
  if (!plan) {
    throw new Error('Structured mission contract is required before specialist execution.');
  }
  return plan;
}

/**
 * Scout receives segment, industry, buyer, geography, constraints, success metric.
 * Scout is forbidden from inferring these from English.
 */
function scoutInput(mission) {
  const plan = requireStructuredMission(mission);
  return {
    segment: plan.market.segment,
    industry: plan.market.industry,
    buyer: plan.market.buyer,
    geography: plan.geography,
    constraints: plan.constraints.slice(),
    successMetric: { ...plan.successMetric },
    missionBound: true,
    structuredOnly: true,
  };
}

/**
 * Paige receives market, buyer, objective, campaign goal, constraints.
 */
function paigeInput(mission) {
  const plan = requireStructuredMission(mission);
  return {
    market: { ...plan.market },
    buyer: plan.market.buyer,
    objective: plan.objective,
    campaignGoal: plan.successMetric,
    constraints: plan.constraints.slice(),
    structuredOnly: true,
  };
}

/**
 * Vera receives market, buyer, region.
 */
function veraInput(mission, companies = []) {
  const plan = requireStructuredMission(mission);
  return {
    market: { ...plan.market },
    buyer: plan.market.buyer,
    region: plan.geography.region,
    companies: Array.isArray(companies) ? companies.slice() : [],
    structuredOnly: true,
  };
}

/**
 * Rex receives mission summary, objective, success metric, progress.
 */
function rexInput(mission, progress = {}) {
  const plan = requireStructuredMission(mission);
  return {
    mission: {
      id: mission.id,
      title: mission.title,
      stage: mission.stage,
      status: mission.status,
    },
    objective: plan.objective,
    successMetric: { ...plan.successMetric },
    progress: {
      percent: progress.percent != null ? progress.percent : mission.progressPercent,
      stage: progress.stage || mission.stage,
      ...progress,
    },
    structuredOnly: true,
  };
}

/**
 * Build Scout delegation from structured mission — no English parsing.
 */
function scoutDelegationFromMission(mission) {
  const plan = requireStructuredMission(mission);
  const input = scoutInput(mission);
  const geographyLabel =
    plan.geography.region ||
    (plan.geography.cities.length ? plan.geography.cities.join(', ') : null);

  return {
    tenantId: String(mission.tenantId || mission.clientId || ''),
    missionId: mission.id,
    targetContext: {
      geography: geographyLabel,
      cities: plan.geography.cities.slice(),
      segments: [plan.market.segment],
      industry: plan.market.industry,
      buyer: plan.market.buyer,
      businessType: plan.market.segment,
      missionBound: true,
      structuredMission: true,
    },
    businessContext: {
      serviceGeography: geographyLabel,
      preferredSegments: [plan.market.segment],
      operatorDirection: plan.objective,
      missionObjectiveImmutable: isStructuredMissionApproved(mission),
      commercialCapability: plan.constraints.includes('commercial_only')
        ? 'commercial_cleaning'
        : null,
      exclusions: plan.constraints.slice(),
      successMetric: { ...plan.successMetric },
      structuredOnly: true,
    },
    specialistInput: input,
  };
}

module.exports = {
  scoutInput,
  paigeInput,
  veraInput,
  rexInput,
  scoutDelegationFromMission,
  requireStructuredMission,
};
