'use strict';

/**
 * SPEC-118 — Acquisition Mission object.
 */

const {
  STAGES,
  STAGE_LABELS,
  PRIORITIES,
  SPECIALISTS,
  OPERATOR_DECISION_KINDS,
  clone,
  asText,
  nowIso,
  newId,
  amoError,
  clamp,
  round2,
} = require('./types');
const { deriveMissionTitle } = require('./MissionNaming');
const { planFromObjective } = require('./MissionPlanner');
const { formatMissionUnderstandingProse } = require('./StructuredMission');

function normalizePriority(value) {
  const text = asText(value).toLowerCase();
  if (text === PRIORITIES.HIGH || text === 'urgent') return PRIORITIES.HIGH;
  if (text === PRIORITIES.LOW) return PRIORITIES.LOW;
  return PRIORITIES.NORMAL;
}

function createMission(input = {}) {
  const tenantId = asText(input.tenantId || input.tenant_id || input.clientId);
  if (!tenantId) throw amoError('amo_tenant_required', 'tenantId is required.');

  const objective = asText(input.objective || input.objectiveText);
  if (!objective) throw amoError('amo_objective_required', 'Objective is required.');

  const now = nowIso(input.now);
  const targetSegment = asText(input.targetSegment || input.segment) || null;
  const campaign = asText(input.campaign) || null;
  const stage = asText(input.stage).toLowerCase() || STAGES.DISCOVER;
  const confidence = clamp(input.confidence == null ? 0.5 : input.confidence, 0, 1);

  let missionPlanDraft = input.missionPlanDraft || input.structuredMissionDraft || null;
  let structuredMission = input.structuredMission || null;
  if (input.planApproved === true && !structuredMission) {
    const planned = planFromObjective(objective, {
      targetSegment,
      constraints: input.constraints,
      priority: input.priority,
    });
    structuredMission = require('./StructuredMission').freezeStructuredMission(planned.draft, {
      approvedBy: 'test',
    });
    missionPlanDraft = null;
  } else if (!missionPlanDraft && !structuredMission && input.skipMissionPlanning !== true) {
    const planned = planFromObjective(objective, {
      targetSegment,
      constraints: input.constraints,
      priority: input.priority,
    });
    missionPlanDraft = planned.draft;
  }
  if (structuredMission && structuredMission.immutable) {
    missionPlanDraft = null;
  }

  const pendingOperatorDecision = buildPendingOperatorDecision({
    stage,
    input,
    missionPlanDraft,
    structuredMission,
  });

  return {
    id: asText(input.id) || newId('mission'),
    kind: 'acquisition_mission',
    spec: 'SPEC-118',
    tenantId,
    clientId: input.clientId != null ? Number(input.clientId) : Number(tenantId) || null,
    objective,
    targetSegment,
    campaign,
    title: asText(input.title) || deriveMissionTitle(objective, targetSegment),
    priority: normalizePriority(input.priority),
    stage,
    status: STAGE_LABELS[stage] || 'Discovering',
    confidence: round2(confidence),
    owner: asText(input.owner) || 'Operator',
    createdBy: asText(input.createdBy || input.created_by) || SPECIALISTS.MAX,
    orchestrationMissionId: asText(input.orchestrationMissionId) || null,
    constraints: Array.isArray(input.constraints) ? input.constraints.map(asText).filter(Boolean) : [],
    blockers: [],
    progressPercent: 8,
    missionPlanDraft,
    structuredMission: structuredMission || null,
    structuredMissionApproved: Boolean(structuredMission && structuredMission.immutable),
    pendingOperatorDecision,
    createdAt: now,
    updatedAt: now,
  };
}

function buildPendingOperatorDecision({ stage, input, missionPlanDraft, structuredMission }) {
  if (input.pendingOperatorDecision !== undefined) return input.pendingOperatorDecision;
  if (stage !== STAGES.DISCOVER) return null;
  if (structuredMission && structuredMission.immutable) {
    return {
      stage: STAGES.DISCOVER,
      kind: OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL,
      prompt: 'Approve discovery?',
    };
  }
  if (missionPlanDraft) {
    return {
      stage: STAGES.DISCOVER,
      kind: OPERATOR_DECISION_KINDS.PLAN_APPROVAL,
      prompt: 'Approve mission plan?',
      missionUnderstanding: formatMissionUnderstandingProse(missionPlanDraft),
    };
  }
  return {
    stage: STAGES.DISCOVER,
    kind: OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL,
    prompt: 'Approve discovery?',
  };
}

function snapshotMission(mission) {
  return clone(mission);
}

module.exports = {
  createMission,
  snapshotMission,
  normalizePriority,
};
