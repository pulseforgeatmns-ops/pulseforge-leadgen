'use strict';

/**
 * SPEC-248 — Max-owned acquisition approach decision at PLAN.
 * Decides how the mission should advance; does not execute channel work.
 */

const amo = require('../../acquisition-mission');
const {
  ACQUISITION_APPROACHES,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  EXECUTION_STATUSES,
  createExecutionResult,
  executeSpecialist,
  buildExecutionInput,
} = amo;
const {
  createAcquisitionApproachPayload,
  normalizeApproach,
  findLatestAcquisitionApproach,
} = require('../../acquisition-mission/AcquisitionApproach');

function asText(value) {
  return value == null ? '' : String(value).trim();
}

function findLatestMaxPrioritization(contributions = []) {
  return [...contributions]
    .reverse()
    .find(
      (row) => row.specialist === SPECIALISTS.MAX && row.kind === CONTRIBUTION_KINDS.PRIORITIZATION
    ) || null;
}

function inferRequestedApproach(input = {}, plan = {}) {
  const direct = normalizeApproach(
    input.selectedApproach ||
    input.approach ||
    input.requestedApproach ||
    input.executionContext?.approach ||
    input.executionContext?.requestedApproach ||
    input.payload?.approach
  );
  if (direct) return direct;

  const text = [
    plan.objective,
    plan.campaign,
    input.mission?.objective,
    input.mission?.campaign,
  ].map(asText).join(' ').toLowerCase();

  if (/\b(google ads|paid search|paid media|paid acquisition|ads?|advertising)\b/.test(text)) {
    return ACQUISITION_APPROACHES.PAID;
  }
  return ACQUISITION_APPROACHES.OUTBOUND;
}

function buildAcquisitionApproachPayload(executionInput = {}) {
  const specialistInput = executionInput.specialistInput || {};
  const plan = specialistInput.structuredMission || executionInput.missionPlan || {};
  const prioritization = findLatestMaxPrioritization(executionInput.contributions || [])
    || { payload: specialistInput.maxPrioritization || {} };
  const priorityPayload = prioritization.payload || {};
  const priorities = Array.isArray(priorityPayload.priorities) ? priorityPayload.priorities : [];
  const evidence = Array.isArray(priorityPayload.evidence)
    ? priorityPayload.evidence
    : Array.isArray(specialistInput.evidence)
      ? specialistInput.evidence
      : [];
  const unknowns = [];
  const blockers = [];
  let selected = inferRequestedApproach(executionInput, plan);

  if (!priorities.length) {
    selected = ACQUISITION_APPROACHES.BLOCKED;
    blockers.push({
      kind: 'insufficient_prioritization',
      reason: 'No prioritized targets are available for acquisition approach selection.',
    });
    unknowns.push({
      unknown: 'Which target should receive channel-specific preparation',
      reason: 'Max target prioritization did not produce ranked targets.',
    });
  }

  const targetName = priorities[0]?.name || priorities[0]?.segment || 'the top ranked opportunity';
  const rationale = selected === ACQUISITION_APPROACHES.OUTBOUND
    ? `Prioritized target evidence supports outbound preparation for ${targetName}.`
    : selected === ACQUISITION_APPROACHES.BOTH
      ? `Prioritized target evidence supports outbound preparation now while preserving paid acquisition as an unsupported future path.`
      : selected === ACQUISITION_APPROACHES.PAID
        ? 'Max selected paid acquisition as the next approach; paid preparation is not implemented yet.'
        : selected === ACQUISITION_APPROACHES.DEFER
          ? 'Max deferred channel-specific preparation until stronger evidence or operator direction exists.'
          : 'Max cannot select an acquisition approach from the available prioritization evidence.';

  return createAcquisitionApproachPayload({
    selectedApproach: selected,
    rationale,
    confidence: priorityPayload.confidence != null ? priorityPayload.confidence : 0.7,
    evidence: evidence.slice(0, 12),
    constraints: priorityPayload.constraints || specialistInput.constraints || [],
    blockers,
    unknowns,
    requiresSpecialistEvidence: selected === ACQUISITION_APPROACHES.PAID,
  });
}

async function runMaxAcquisitionApproach(executionInput = {}) {
  const specialistInput = executionInput.specialistInput || {};
  const transactionId = executionInput.transactionId;

  if (!specialistInput.structuredMission) {
    return createExecutionResult({
      specialist: SPECIALISTS.MAX,
      transactionId,
      status: EXECUTION_STATUSES.BLOCKED,
      reason: 'Structured mission plan is required for acquisition approach selection.',
      requiredPrecondition: 'structured_mission',
    });
  }

  const prioritization = findLatestMaxPrioritization(executionInput.contributions || []);
  if (!prioritization && !specialistInput.maxPrioritization) {
    return createExecutionResult({
      specialist: SPECIALISTS.MAX,
      transactionId,
      status: EXECUTION_STATUSES.BLOCKED,
      reason: 'Max target prioritization is required before acquisition approach selection.',
      requiredPrecondition: 'max_prioritization',
    });
  }

  const contributions = buildAcquisitionApproachPayload(executionInput);
  const decision = contributions.acquisitionApproach;
  const status = decision.selectedApproach === ACQUISITION_APPROACHES.BLOCKED
    ? EXECUTION_STATUSES.BLOCKED
    : EXECUTION_STATUSES.SUCCESS;

  return createExecutionResult({
    specialist: SPECIALISTS.MAX,
    transactionId,
    status,
    confidence: decision.confidence,
    evidence: decision.evidence,
    contributions,
    recommendations: decision.nextStep ? [{ tier: 'required', text: decision.nextStep }] : [],
    unknowns: decision.unknowns,
    nextActions: [{ kind: 'advance_stage', label: decision.nextStep }],
    reason: status === EXECUTION_STATUSES.BLOCKED ? decision.rationale : null,
    requiredPrecondition: status === EXECUTION_STATUSES.BLOCKED ? 'acquisition_approach_evidence' : null,
  });
}

async function runMaxApproachForAmoMission(mission, opts = {}) {
  if (typeof opts.runMaxApproach === 'function') {
    return opts.runMaxApproach(mission, opts);
  }

  const tenantId = opts.tenantId != null ? String(opts.tenantId) : String(mission.tenantId || '');
  const contributions = opts.contributions
    || (opts.engine && typeof opts.engine.inspect === 'function'
      ? (opts.engine.inspect(mission.id, { tenantId }).contributions || [])
      : []);
  const input = buildExecutionInput({
    mission,
    specialist: SPECIALISTS.MAX,
    contributions,
    transactionId: opts.transactionId,
    executionContext: {
      stage: mission.stage,
      missionId: mission.id,
      tenantId,
      executionRequestId: opts.executionRequest?.id || null,
      requestedApproach: opts.approach || opts.selectedApproach || null,
    },
    store: opts.engine?.store,
  });

  return executeSpecialist({
    specialist: SPECIALISTS.MAX,
    mission,
    contributions,
    transactionId: opts.transactionId,
    store: opts.engine?.store,
    run: () => runMaxAcquisitionApproach({
      ...input,
      mission,
      contributions,
      requestedApproach: opts.approach || opts.selectedApproach || null,
    }),
    treatErrorsAsBlocked: opts.treatErrorsAsBlocked !== false,
  });
}

function acquisitionApproachPayloadFromMaxResult(maxResult = {}) {
  if (maxResult.contributions && Object.keys(maxResult.contributions).length) {
    return maxResult.contributions;
  }
  if (maxResult.payload && Object.keys(maxResult.payload).length) {
    return maxResult.payload;
  }
  throw new Error('Max acquisition approach result is missing contributions.');
}

module.exports = {
  buildAcquisitionApproachPayload,
  runMaxAcquisitionApproach,
  runMaxApproachForAmoMission,
  acquisitionApproachPayloadFromMaxResult,
  findLatestAcquisitionApproach,
};
