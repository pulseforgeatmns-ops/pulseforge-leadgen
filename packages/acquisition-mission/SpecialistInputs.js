'use strict';

/**
 * SPEC-130 — Specialist input contracts.
 * Each specialist receives structured mission fields — never free-form operator text.
 */

const { asText, SPECIALISTS, CONTRIBUTION_KINDS, clone } = require('./types');
const { isStructuredMissionApproved } = require('./StructuredMission');

function findLatestScoutDiscovery(contributions = []) {
  return [...contributions]
    .reverse()
    .find(
      (row) => row.specialist === SPECIALISTS.SCOUT && row.kind === CONTRIBUTION_KINDS.DISCOVERY
    );
}

function findLatestOperatorPrioritizationApproval(contributions = []) {
  return [...contributions]
    .reverse()
    .find(
      (row) =>
        row.specialist === SPECIALISTS.OPERATOR
        && row.kind === CONTRIBUTION_KINDS.APPROVAL
        && (row.payload.action === 'prioritization_approved'
          || row.payload.kind === 'prioritization_approval')
    );
}

function requireStructuredMission(mission) {
  const plan = mission && (mission.structuredMission || mission.missionPlanDraft);
  if (!plan) {
    throw new Error('Structured mission contract is required before specialist execution.');
  }
  return plan;
}

function requireLockedMissionPlan(mission) {
  if (!isStructuredMissionApproved(mission) || !mission.structuredMission) {
    const err = new Error('Specialists cannot execute until the Mission Plan is approved and locked.');
    err.code = 'amo_plan_not_locked';
    throw err;
  }
  return mission.structuredMission;
}

/**
 * Scout receives segment, industry, buyer, geography, constraints, evidence policy, success metric.
 * Scout is forbidden from inferring these from English.
 */
function scoutInput(mission) {
  const plan = requireStructuredMission(mission);
  return {
    segment: plan.market.segment,
    industry: plan.market.industry,
    buyer: plan.market.buyer,
    geography: plan.geography,
    constraints: (plan.constraints || []).slice(),
    evidencePolicy: { ...(plan.evidence || plan.evidencePolicy || {}) },
    successMetric: { ...(plan.successMetric || plan.success || {}) },
    missionBound: true,
    structuredOnly: true,
  };
}

/**
 * Paige receives audience, campaign goal, market, constraints, tone, objective.
 */
function paigeInput(mission) {
  const plan = requireStructuredMission(mission);
  return {
    audience: plan.market.label || plan.market.segment,
    campaignGoal: plan.successMetric || plan.success,
    market: { ...plan.market },
    buyer: plan.market.buyer,
    objective: plan.objective,
    constraints: (plan.constraints || []).slice(),
    tone: asText(plan.tone) || 'operator_voice',
    structuredOnly: true,
  };
}

/**
 * Vera receives market, companies, buyer, review policy.
 */
function veraInput(mission, companies = []) {
  const plan = requireStructuredMission(mission);
  return {
    market: { ...plan.market },
    buyer: plan.market.buyer,
    region: plan.geography.region,
    companies: Array.isArray(companies) ? companies.slice() : [],
    reviewPolicy: plan.reviewPolicy || {
      respondToAll: true,
      minRating: 1,
    },
    structuredOnly: true,
  };
}

/**
 * Emmett receives audience, geography, constraints, and deliverability policy.
 */
function emmettInput(mission, extras = {}) {
  const plan = requireStructuredMission(mission);
  return {
    audience: plan.market.label || plan.market.segment,
    market: { ...plan.market },
    geography: { ...plan.geography },
    constraints: (plan.constraints || []).slice(),
    deliverabilityPolicy: extras.deliverabilityPolicy || {
      safeSendGovernor: true,
      requireHealthyInbox: true,
    },
    queueContext: extras.queueContext || null,
    structuredOnly: true,
  };
}

/**
 * Rex receives mission, progress, objective, KPIs.
 */
function rexInput(mission, progress = {}) {
  const plan = requireStructuredMission(mission);
  const success = plan.successMetric || plan.success || {};
  return {
    mission: {
      id: mission.id,
      title: mission.title,
      stage: mission.stage,
      status: mission.status,
    },
    objective: plan.objective,
    successMetric: { ...success },
    kpis: [
      { metric: success.metric || success.type, target: success.target },
    ],
    progress: {
      percent: progress.percent != null ? progress.percent : mission.progressPercent,
      stage: progress.stage || mission.stage,
      ...progress,
    },
    structuredOnly: true,
  };
}

/**
 * Max receives locked structured mission, Scout discovery, MIR, ranked opportunities,
 * evidence, operator prioritization approval, and mission constraints.
 */
function maxInput(mission, extras = {}) {
  const plan = requireLockedMissionPlan(mission);
  const contributions = Array.isArray(extras.contributions) ? extras.contributions : [];
  const scoutDiscovery = extras.discovery
    ? { payload: extras.discovery }
    : findLatestScoutDiscovery(contributions);
  const discoveryPayload = scoutDiscovery ? clone(scoutDiscovery.payload || {}) : null;
  const operatorApproval = extras.operatorApproval || findLatestOperatorPrioritizationApproval(contributions);

  return {
    structuredMission: clone(plan),
    discovery: discoveryPayload,
    missionIntelligenceReport: discoveryPayload && discoveryPayload.missionIntelligenceReport
      ? clone(discoveryPayload.missionIntelligenceReport)
      : null,
    rankedOpportunities: discoveryPayload
      ? clone(
        discoveryPayload.rankedProspects
          || discoveryPayload.opportunities
          || discoveryPayload.companies
          || []
      )
      : [],
    evidence: discoveryPayload ? clone(discoveryPayload.evidence || []) : [],
    buyingSignals: discoveryPayload ? clone(discoveryPayload.buyingSignals || []) : [],
    operatorPrioritizationApproval: operatorApproval ? clone(operatorApproval.payload || {}) : null,
    constraints: (plan.constraints || []).slice(),
    observations: clone(extras.observations || []),
    missionBound: true,
    structuredOnly: true,
  };
}

/**
 * Build Scout delegation from a locked Mission Plan — no English parsing.
 */
function scoutDelegationFromMission(mission) {
  const plan = requireLockedMissionPlan(mission);
  const input = scoutInput(mission);
  const geographyLabel =
    plan.geography.region ||
    (plan.geography.cities && plan.geography.cities.length ? plan.geography.cities.join(', ') : null);

  return {
    tenantId: String(mission.tenantId || mission.clientId || ''),
    missionId: mission.id,
    targetContext: {
      geography: geographyLabel,
      cities: (plan.geography.cities || []).slice(),
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
      missionObjectiveImmutable: true,
      commercialCapability: (plan.constraints || []).includes('commercial_only')
        ? 'commercial_cleaning'
        : null,
      exclusions: (plan.constraints || []).slice(),
      successMetric: { ...(plan.successMetric || {}) },
      evidencePolicy: { ...(plan.evidence || plan.evidencePolicy || {}) },
      structuredOnly: true,
    },
    specialistInput: input,
  };
}

module.exports = {
  scoutInput,
  maxInput,
  paigeInput,
  veraInput,
  rexInput,
  emmettInput,
  scoutDelegationFromMission,
  findLatestScoutDiscovery,
  requireStructuredMission,
  requireLockedMissionPlan,
};
