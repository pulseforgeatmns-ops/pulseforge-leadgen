'use strict';

/**
 * SPEC-130 — Specialist input contracts.
 * Each specialist receives structured mission fields — never free-form operator text.
 */

const { asText } = require('./types');
const { isStructuredMissionApproved } = require('./StructuredMission');
const { buildSharedContext } = require('./Context');
const { SPECIALISTS, CONTRIBUTION_KINDS } = require('./types');

function latestContribution(contributions = [], specialist, kind) {
  return [...contributions]
    .reverse()
    .find((row) => row.specialist === specialist && (!kind || row.kind === kind));
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
 * Paige receives audience, campaign goal, market, constraints, tone, objective,
 * plus mission-bound Scout discovery and Max prioritization intelligence.
 * Paige must not rebuild Scout or Max reasoning — only consume upstream context.
 */
function paigeInput(mission, extras = {}) {
  const plan = requireStructuredMission(mission);
  const contributions = Array.isArray(extras.contributions) ? extras.contributions : [];
  const sharedContext = extras.sharedContext
    || (contributions.length ? buildSharedContext(mission, contributions) : null);
  const scoutRow = latestContribution(contributions, SPECIALISTS.SCOUT, CONTRIBUTION_KINDS.DISCOVERY);
  const maxRow = latestContribution(contributions, SPECIALISTS.MAX, CONTRIBUTION_KINDS.PRIORITIZATION);
  const scoutPayload = scoutRow?.payload || sharedContext?.scout || {};
  const maxPayload = maxRow?.payload || sharedContext?.max || {};
  const prioritizationApproval = latestContribution(contributions, SPECIALISTS.OPERATOR, CONTRIBUTION_KINDS.APPROVAL);

  return {
    audience: plan.market.label || plan.market.segment,
    campaignGoal: plan.successMetric || plan.success,
    market: { ...plan.market },
    buyer: plan.market.buyer,
    objective: plan.objective,
    constraints: [
      ...(plan.constraints || []).slice(),
      ...((maxPayload.constraints || []).filter(Boolean)),
    ],
    tone: asText(plan.tone) || 'operator_voice',
    structuredMission: plan,
    scoutDiscovery: scoutPayload,
    maxPrioritization: maxPayload,
    priorities: maxPayload.priorities || [],
    objectives: maxPayload.objectives || [],
    objectiveReason: maxPayload.objectiveReason || null,
    timing: maxPayload.timing || null,
    recommendations: maxPayload.recommendations || sharedContext?.priorityReasoning || [],
    delegation: maxPayload.delegation || null,
    rankedTargets: maxPayload.rankedTargets || maxPayload.priorities || [],
    buyingSignals: scoutPayload.buyingSignals || scoutPayload.signals || sharedContext?.buyingSignals || [],
    evidence: scoutPayload.evidence || sharedContext?.evidence || [],
    operatorApproval: prioritizationApproval
      ? { consumed: prioritizationApproval.payload?.consumed === true }
      : null,
    workspaceContext: sharedContext ? {
      objective: sharedContext.objective,
      missionUnderstanding: sharedContext.mission?.missionUnderstanding || null,
    } : null,
    structuredOnly: true,
    missionBound: true,
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
 * Emmett receives mission-bound upstream intelligence plus infrastructure evidence policy.
 * Emmett must not recreate Scout / Max / Paige cognition.
 */
function emmettInput(mission, extras = {}) {
  const plan = requireStructuredMission(mission);
  const contributions = Array.isArray(extras.contributions) ? extras.contributions : [];
  const sharedContext = extras.sharedContext
    || (contributions.length ? buildSharedContext(mission, contributions) : null);
  const scoutRow = latestContribution(contributions, SPECIALISTS.SCOUT, CONTRIBUTION_KINDS.DISCOVERY);
  const maxRow = latestContribution(contributions, SPECIALISTS.MAX, CONTRIBUTION_KINDS.PRIORITIZATION);
  const paigeRow = latestContribution(contributions, SPECIALISTS.PAIGE, CONTRIBUTION_KINDS.VARIANTS);
  const prioritizationApproval = latestContribution(contributions, SPECIALISTS.OPERATOR, CONTRIBUTION_KINDS.APPROVAL);
  const scoutPayload = scoutRow?.payload || sharedContext?.scout || {};
  const maxPayload = maxRow?.payload || sharedContext?.max || {};
  const paigePayload = paigeRow?.payload || {};

  let missionCandidates = extras.missionCandidates || null;
  let paigeReadiness = {
    ready: Boolean(paigePayload.variants?.length),
    variantCount: paigePayload.variants?.length || 0,
  };
  if (!missionCandidates && contributions.length) {
    try {
      const { buildMissionBoundCandidates, buildPaigeReadinessMetadata } = require('../max/workspace/EmmettMissionCandidates');
      missionCandidates = buildMissionBoundCandidates(mission, contributions);
      paigeReadiness = buildPaigeReadinessMetadata(paigePayload);
    } catch (_) {
      missionCandidates = [];
    }
  }

  return {
    tenantId: String(mission.tenantId || mission.clientId || ''),
    audience: plan.market.label || plan.market.segment,
    market: { ...plan.market },
    geography: { ...plan.geography },
    buyer: plan.market.buyer,
    objective: plan.objective || mission.objective,
    structuredMission: plan,
    scoutDiscovery: scoutPayload,
    maxPrioritization: maxPayload,
    paigeReadiness,
    rankedTargets: maxPayload.rankedTargets || maxPayload.priorities || [],
    priorities: maxPayload.priorities || [],
    objectives: maxPayload.objectives || [],
    objectiveReason: maxPayload.objectiveReason || null,
    timing: maxPayload.timing || null,
    recommendations: maxPayload.recommendations || sharedContext?.priorityReasoning || [],
    buyingSignals: scoutPayload.buyingSignals || scoutPayload.signals || sharedContext?.buyingSignals || [],
    evidence: scoutPayload.evidence || sharedContext?.evidence || [],
    constraints: [
      ...(plan.constraints || []).slice(),
      ...((maxPayload.constraints || []).filter(Boolean)),
    ],
    deliverabilityPolicy: extras.deliverabilityPolicy || {
      safeSendGovernor: true,
      requireHealthyInbox: true,
    },
    infrastructureSnapshot: extras.infrastructureSnapshot || null,
    missionCandidates: missionCandidates || [],
    observations: extras.observations || [],
    operatorApproval: prioritizationApproval
      ? { consumed: prioritizationApproval.payload?.consumed === true }
      : null,
    workspaceContext: sharedContext ? {
      objective: sharedContext.objective,
      missionUnderstanding: sharedContext.mission?.missionUnderstanding || null,
    } : null,
    structuredOnly: true,
    missionBound: true,
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
  paigeInput,
  veraInput,
  rexInput,
  emmettInput,
  scoutDelegationFromMission,
  requireStructuredMission,
  requireLockedMissionPlan,
};
