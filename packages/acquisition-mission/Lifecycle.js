'use strict';

/**
 * SPEC-118 — mission lifecycle. Max advances stages from evidence.
 */

const {
  STAGES,
  STAGE_ORDER,
  STAGE_LABELS,
  STAGE_PROGRESS_BASE,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  SPECIALIST_STATES,
  amoError,
  clamp,
} = require('./types');

const PREREQUISITES = Object.freeze({
  [STAGES.UNDERSTAND]: (ctx) =>
    ctx.scoutComplete ? null : 'Scout discovery is required before Understand.',
  [STAGES.PLAN]: (ctx) =>
    ctx.maxHasObjectives || ctx.maxComplete ? null : 'Max objectives or prioritization are required before Plan.',
  [STAGES.PREPARE]: (ctx) =>
    ctx.maxComplete ? null : 'Max prioritization is required before Prepare.',
  [STAGES.READY]: (ctx) => {
    if (!ctx.paigeComplete) return 'Paige variants are required before Ready.';
    if (!ctx.emmettComplete) return 'Emmett capacity is required before Ready.';
    if (ctx.deliverabilityPaused) return 'Deliverability risk blocks Ready.';
    return null;
  },
  [STAGES.EXECUTE]: (ctx) =>
    ctx.operatorApproved ? null : 'Operator approval is required before Execute.',
  [STAGES.OBSERVE]: (ctx) =>
    ctx.queuedOrLaunched ? null : 'Queued or launched sends are required before Observe.',
  [STAGES.LEARN]: (ctx) =>
    ctx.hasOutcomes ? null : 'Outcomes are required before Learn.',
  [STAGES.IMPROVE]: (ctx) =>
    ctx.hasLearning ? null : 'Learning records are required before Improve.',
});

function stageIndex(stage) {
  const idx = STAGE_ORDER.indexOf(stage);
  return idx < 0 ? 0 : idx;
}

function nextStage(stage) {
  const idx = stageIndex(stage);
  return STAGE_ORDER[Math.min(idx + 1, STAGE_ORDER.length - 1)];
}

function specialistContext(contributions = [], extras = {}) {
  const rows = contributions || [];
  const by = (specialist, kinds) =>
    rows.some((row) => row.specialist === specialist && (!kinds || kinds.includes(row.kind)));
  const scout = rows.filter((row) => row.specialist === SPECIALISTS.SCOUT);
  const prospectCount = scout.reduce((sum, row) => {
    const payload = row.payload || {};
    if (payload.qualifiedCount != null && Number.isFinite(Number(payload.qualifiedCount))) {
      return sum + Number(payload.qualifiedCount);
    }
    const companies = Array.isArray(payload.companies) ? payload.companies.length : 0;
    const prospects = Array.isArray(payload.prospects) ? payload.prospects.length : 0;
    return sum + Math.max(companies, prospects);
  }, extras.prospectCount || 0);

  return {
    scoutComplete: by(SPECIALISTS.SCOUT, [CONTRIBUTION_KINDS.DISCOVERY]) || extras.scoutComplete,
    maxComplete: by(SPECIALISTS.MAX, [CONTRIBUTION_KINDS.PRIORITIZATION]) || extras.maxComplete,
    maxHasObjectives: by(SPECIALISTS.MAX, [CONTRIBUTION_KINDS.OBJECTIVE, CONTRIBUTION_KINDS.CONSTRAINTS]),
    paigeComplete: by(SPECIALISTS.PAIGE, [CONTRIBUTION_KINDS.VARIANTS]) || extras.paigeComplete,
    paigeGenerating: extras.paigeGenerating === true,
    emmettComplete: by(SPECIALISTS.EMMETT, [CONTRIBUTION_KINDS.CAPACITY]) || extras.emmettComplete,
    operatorApproved: by(SPECIALISTS.OPERATOR, [CONTRIBUTION_KINDS.APPROVAL]) || extras.operatorApproved,
    deliverabilityPaused: extras.deliverabilityPaused === true,
    queuedOrLaunched: extras.queuedOrLaunched === true,
    hasOutcomes: extras.hasOutcomes === true,
    hasLearning: extras.hasLearning === true,
    prospectCount,
  };
}

function canEnter(stage, ctx) {
  if (stage === STAGES.DISCOVER) return { ok: true };
  const check = PREREQUISITES[stage];
  if (!check) return { ok: true };
  const reason = check(ctx);
  return reason ? { ok: false, reason } : { ok: true };
}

function assertActorCanProgress(actor) {
  const role = String(actor && (actor.role || actor) || '').toLowerCase();
  if (role !== SPECIALISTS.MAX && role !== SPECIALISTS.OPERATOR) {
    throw amoError(
      'amo_max_orchestrates',
      'Only Max (or the operator) may advance the mission lifecycle.'
    );
  }
}

function progressPercent(stage, ctx = {}) {
  const base = STAGE_PROGRESS_BASE[stage] != null ? STAGE_PROGRESS_BASE[stage] : 8;
  if (stage === STAGES.IMPROVE) return 100;
  let bonus = 0;
  if (ctx.scoutComplete) bonus += 12;
  if (ctx.maxComplete) bonus += 8;
  if (ctx.paigeComplete) bonus += 8;
  else if (ctx.paigeGenerating) bonus += 0;
  if (ctx.emmettComplete) bonus += 6;
  if (ctx.operatorApproved) bonus += 6;
  const next = STAGE_ORDER[Math.min(stageIndex(stage) + 1, STAGE_ORDER.length - 1)];
  const cap = next === stage ? 100 : (STAGE_PROGRESS_BASE[next] || 100) - 1;
  return clamp(base + bonus, 0, cap);
}

function specialistState(specialist, ctx, mission) {
  if (specialist === SPECIALISTS.SCOUT) {
    return ctx.scoutComplete
      ? { state: SPECIALIST_STATES.COMPLETE, label: 'Discovery Complete' }
      : { state: SPECIALIST_STATES.WAITING, label: 'Waiting' };
  }
  if (specialist === SPECIALISTS.MAX) {
    return ctx.maxComplete
      ? { state: SPECIALIST_STATES.COMPLETE, label: 'Prioritization Complete' }
      : { state: SPECIALIST_STATES.WAITING, label: 'Waiting' };
  }
  if (specialist === SPECIALISTS.PAIGE) {
    if (ctx.paigeComplete) return { state: SPECIALIST_STATES.COMPLETE, label: 'Variants Ready' };
    if (ctx.paigeGenerating || mission.stage === STAGES.PREPARE) {
      return { state: SPECIALIST_STATES.GENERATING, label: 'Generating Variants' };
    }
    return { state: SPECIALIST_STATES.WAITING, label: 'Waiting' };
  }
  if (specialist === SPECIALISTS.EMMETT) {
    return ctx.emmettComplete
      ? { state: SPECIALIST_STATES.COMPLETE, label: 'Capacity Approved' }
      : { state: SPECIALIST_STATES.WAITING, label: 'Waiting' };
  }
  if (ctx.operatorApproved) {
    return { state: SPECIALIST_STATES.APPROVED, label: 'Approved' };
  }
  if (mission.stage === STAGES.PREPARE || mission.stage === STAGES.READY) {
    return { state: SPECIALIST_STATES.APPROVAL_REQUIRED, label: 'Approval Required' };
  }
  return { state: SPECIALIST_STATES.WAITING, label: 'Waiting' };
}

function applyStage(mission, stage) {
  mission.stage = stage;
  mission.status = STAGE_LABELS[stage] || stage;
  mission.updatedAt = mission.updatedAt;
  return mission;
}

module.exports = {
  PREREQUISITES,
  stageIndex,
  nextStage,
  specialistContext,
  canEnter,
  assertActorCanProgress,
  progressPercent,
  specialistState,
  applyStage,
};
