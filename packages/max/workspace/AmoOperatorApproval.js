'use strict';

/**
 * SPEC-128 — Operator approval consumes pending decisions and executes the stage once.
 */

const amo = require('../../acquisition-mission');
const { specialistContext } = require('../../acquisition-mission/Lifecycle');
const { runScoutAcquisitionIntelligence } = require('../scoutAcquisition/ScoutAdapter');

const {
  STAGES,
  STAGE_LABELS,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
} = amo;
const {
  createMissionApprovalAudit,
  logMissionApprovalReceived,
  logMissionApprovalConsumed,
  logMissionStageExecutionStarted,
  logMissionStageExecutionCompleted,
} = require('./audit/MissionApprovalAudit');
const askPathTrace = require('./audit/AskPathTrace');
const {
  buildMissionCommunication,
  formatMissionProse,
} = require('./MissionCommunication');
const {
  presentationFromDiscoveryPayload,
} = require('../../acquisition-mission/DiscoveryPresentation');
const {
  normalizeScoutDiscoveryPayload,
  hasSufficientEvidenceForPrioritization,
} = require('../../acquisition-mission/DiscoveryPayload');
const {
  inferTargetSegmentFromObjective,
  extractGeography,
  segmentToSearchKey,
} = require('../../acquisition-mission/MissionNaming');
const {
  freezeStructuredMission,
  isStructuredMissionApproved,
} = require('../../acquisition-mission/StructuredMission');
const {
  scoutDelegationFromMission,
} = require('../../acquisition-mission/SpecialistInputs');
const { OPERATOR_DECISION_KINDS } = amo;

const DISCOVERY_APPROVAL_ACTION = 'discovery_approved';
const PLAN_APPROVAL_ACTION = 'plan_approved';

/** SPEC-128 — operator approval lifecycle phases (audit + response). */
const APPROVAL_PHASES = Object.freeze({
  WAITING_FOR_OPERATOR: 'waiting_for_operator',
  APPROVAL_RECEIVED: 'approval_received',
  EXECUTING_STAGE: 'executing_stage',
  STAGE_COMPLETED: 'stage_completed',
  WAITING_FOR_NEXT_DECISION: 'waiting_for_next_decision',
});

function buildDelegationFromAmoMission(mission) {
  if (isStructuredMissionApproved(mission) || mission.structuredMission) {
    return scoutDelegationFromMission(mission);
  }

  const objective = String(mission.objective || '');
  const geography = extractGeography(objective);
  const segment =
    inferTargetSegmentFromObjective(objective) || mission.targetSegment || null;
  const searchSegment = segment ? segmentToSearchKey(segment) : null;

  return {
    tenantId: String(mission.tenantId || mission.clientId || ''),
    missionId: mission.id,
    targetContext: {
      geography,
      segments: searchSegment ? [searchSegment] : [],
      businessType: 'commercial_cleaning',
      missionBound: true,
    },
    businessContext: {
      serviceGeography: geography,
      preferredSegments: searchSegment ? [searchSegment] : [],
      operatorDirection: objective,
      missionObjectiveImmutable: true,
      commercialCapability: 'commercial_cleaning',
      exclusions: Array.isArray(mission.constraints) ? mission.constraints.slice() : [],
    },
  };
}

function hasPendingPlanApproval(snapshot) {
  const mission = snapshot.mission || {};
  if (isStructuredMissionApproved(mission)) return false;
  if (mission.pendingOperatorDecision) {
    return mission.pendingOperatorDecision.kind === OPERATOR_DECISION_KINDS.PLAN_APPROVAL;
  }
  return Boolean(mission.missionPlanDraft && !mission.structuredMission);
}

function findPlanApproval(contributions = []) {
  return [...contributions]
    .reverse()
    .find(
      (row) =>
        row.specialist === SPECIALISTS.OPERATOR &&
        row.kind === CONTRIBUTION_KINDS.APPROVAL &&
        (row.payload.action === PLAN_APPROVAL_ACTION ||
          row.payload.kind === OPERATOR_DECISION_KINDS.PLAN_APPROVAL)
    );
}

/**
 * Consume plan approval, freeze structured mission, and advance to discovery approval.
 * @param {object} input
 * @returns {Promise<object>}
 */
async function advancePlanAfterApproval(input = {}) {
  const { engine, mission, tenantId, question, operatorId } = input;
  if (!engine || !mission) throw new Error('engine and mission are required');

  let snapshot = engine.inspect(mission.id, { tenantId });
  const existing = findPlanApproval(snapshot.contributions || []);
  if (existing && isStructuredMissionApproved(snapshot.mission)) {
    return {
      alreadyExecuted: true,
      approval: existing,
      snapshot,
      structuredMission: snapshot.mission.structuredMission,
    };
  }

  const draft = snapshot.mission.missionPlanDraft;
  if (!draft) throw new Error('No mission plan draft to approve.');

  const frozen = freezeStructuredMission(draft, {
    approvedBy: operatorId || 'operator',
  });

  const approvalResult = engine.contribute(
    mission.id,
    {
      specialist: SPECIALISTS.OPERATOR,
      kind: CONTRIBUTION_KINDS.APPROVAL,
      payload: {
        approved: true,
        consumed: true,
        command: question,
        action: PLAN_APPROVAL_ACTION,
        kind: OPERATOR_DECISION_KINDS.PLAN_APPROVAL,
        contractHash: frozen.contractHash,
      },
    },
    { tenantId }
  );

  const updated = approvalResult.mission;
  updated.missionPlanDraft = null;
  updated.structuredMission = frozen;
  updated.structuredMissionApproved = true;
  updated.pendingOperatorDecision = {
    stage: STAGES.DISCOVER,
    kind: OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL,
    prompt: 'Approve discovery?',
  };
  engine.store.putMission(updated);

  engine.contribute(
    mission.id,
    {
      specialist: SPECIALISTS.MAX,
      kind: CONTRIBUTION_KINDS.MISSION_PLAN,
      payload: { structuredMission: frozen, contractHash: frozen.contractHash },
    },
    { tenantId }
  );

  snapshot = engine.inspect(mission.id, { tenantId });
  return {
    alreadyExecuted: false,
    approval: approvalResult.contribution,
    snapshot,
    structuredMission: frozen,
  };
}

function findDiscoveryApproval(contributions = []) {
  return [...contributions]
    .reverse()
    .find(
      (row) =>
        row.specialist === SPECIALISTS.OPERATOR &&
        row.kind === CONTRIBUTION_KINDS.APPROVAL &&
        (row.payload.action === DISCOVERY_APPROVAL_ACTION ||
          row.payload.stage === STAGES.DISCOVER)
    );
}

function findScoutDiscoveryAfterApproval(contributions = [], approval) {
  if (!approval) return null;
  const approvalAt = approval.at || '';
  return [...contributions]
    .reverse()
    .find(
      (row) =>
        row.specialist === SPECIALISTS.SCOUT &&
        row.kind === CONTRIBUTION_KINDS.DISCOVERY &&
        (!approvalAt || !row.at || row.at >= approvalAt)
    );
}

function hasPendingDiscoveryApproval(snapshot) {
  const mission = snapshot.mission || {};
  if (hasPendingPlanApproval(snapshot)) return false;
  if (!isStructuredMissionApproved(mission) && mission.missionPlanDraft) return false;
  if (mission.pendingOperatorDecision) {
    return mission.pendingOperatorDecision.kind === OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL ||
      (!mission.pendingOperatorDecision.kind &&
        mission.pendingOperatorDecision.stage === STAGES.DISCOVER);
  }
  const contributions = snapshot.contributions || [];
  const ctx = specialistContext(contributions, {});
  if (mission.stage !== STAGES.DISCOVER || ctx.scoutComplete) return false;
  return !findDiscoveryApproval(contributions);
}

function mapScoutIntelligenceToDiscoveryPayload(result = {}, opts = {}) {
  return normalizeScoutDiscoveryPayload(result, {
    missionObjective: opts.missionObjective,
    approvalConsumed: true,
  });
}

function fixtureScoutDiscoveryResult() {
  const opportunities = [
    {
      companyId: 'co-harbor',
      name: 'Harbor Law Group',
      fit: 0.78,
      timing: 0.65,
      confidence: 0.74,
      signals: [
        {
          type: 'hiring',
          label: 'Hiring operations manager',
          source: 'job_board',
          observedAt: '2026-08-01T00:00:00.000Z',
        },
        {
          type: 'decision_maker',
          label: 'Alex Morgan, Office Manager',
          source: 'existing_repository',
        },
      ],
      evidenceRefs: [
        {
          id: 'ev-harbor-hire',
          label: 'Operations manager job posting on company careers page',
          snapshot: { source: 'job_board', companyName: 'Harbor Law Group' },
        },
      ],
      unknowns: [{ text: 'Current cleaning vendor unknown.' }],
    },
    {
      companyId: 'co-granite',
      name: 'Granite Legal Partners',
      fit: 0.71,
      timing: 0.58,
      confidence: 0.68,
      signals: [
        {
          type: 'hiring',
          label: 'Hiring office coordinator',
          source: 'linkedin',
          observedAt: '2026-07-28T00:00:00.000Z',
        },
      ],
      evidenceRefs: [
        {
          id: 'ev-granite-li',
          label: 'LinkedIn hiring post for office coordinator',
          snapshot: { source: 'linkedin', companyName: 'Granite Legal Partners' },
        },
      ],
      unknowns: [],
    },
  ];
  return {
    status: 'completed',
    confidence: 0.72,
    summary:
      '2 prospects ranked against the mission objective. Top: Harbor Law Group, Granite Legal Partners. 1 have identifiable decision-makers.',
    payload: {
      companies: opportunities.map((o) => ({ id: o.companyId, name: o.name })),
      prospects: [{ id: 'p-1', name: 'Alex Morgan', title: 'Office Manager' }],
      opportunities,
      qualifiedCount: 2,
      confidence: 0.72,
    },
  };
}

async function runScoutForAmoMission(mission, opts = {}) {
  if (typeof opts.runScout === 'function') {
    return opts.runScout(mission, opts);
  }

  const delegation = buildDelegationFromAmoMission(mission);
  try {
    const result = await runScoutAcquisitionIntelligence(delegation, {
      mode: opts.scoutMode || 'completed',
      missionId: mission.id,
      tenantId: delegation.tenantId,
      companies: opts.scoutCompanies,
      people: opts.scoutPeople,
      discover: opts.discover,
      enablePlaces: opts.enablePlaces,
    });
    const mapped = mapScoutIntelligenceToDiscoveryPayload(result, {
      missionObjective: mission.objective,
    });
    if (
      opts.allowFixtureFallback === true &&
      (mapped.blocked || mapped.qualifiedCount <= 0)
    ) {
      return fixtureScoutDiscoveryResult();
    }
    return result;
  } catch (err) {
    if (opts.allowFixtureFallback === true) return fixtureScoutDiscoveryResult();
    throw err;
  }
}

function discoveryPayloadFromScoutResult(scoutResult, mission) {
  if (scoutResult && scoutResult.discoveryReport) {
    const report = scoutResult.discoveryReport;
    return normalizeScoutDiscoveryPayload(
      {
        status: /blocked/i.test(String(report.outcome || '')) ? 'blocked' : 'completed',
        summary: report.blockReason || null,
        payload: {
          qualifiedCount: report.prospectCount || 0,
          evidence: report.evidenceSources
            ? report.evidenceSources
                .filter((row) => row.succeeded)
                .map((row) => ({ label: row.source, source: row.source }))
            : [{ label: 'Scout discovery', source: 'scout.discover' }],
          outcome: report.outcome,
        },
      },
      { missionObjective: mission && mission.objective, approvalConsumed: true }
    );
  }
  return mapScoutIntelligenceToDiscoveryPayload(scoutResult, {
    missionObjective: mission && mission.objective,
  });
}

function clearPendingOperatorDecision(engine, mission) {
  if (!mission || !mission.pendingOperatorDecision) return mission;
  mission.pendingOperatorDecision = null;
  return engine.store.putMission(mission);
}

function setDiscoveryExecutingStatus(engine, mission) {
  mission.status = 'Discovery Executing';
  mission.updatedAt = new Date().toISOString();
  return engine.store.putMission(mission);
}

/**
 * Consume discovery approval, execute Scout once, attach results, and advance when ready.
 * @param {object} input
 * @returns {Promise<object>}
 */
async function advanceDiscoveryAfterApproval(input = {}) {
  askPathTrace.traceEnter('advanceDiscoveryAfterApproval');
  const {
    engine,
    mission,
    tenantId,
    question,
    operatorId,
    missionEngine,
    persist,
    runScout,
    scoutCompanies,
    scoutPeople,
    allowFixtureFallback,
    audit: inputAudit,
  } = input;

  if (!engine || !mission) {
    throw new Error('engine and mission are required');
  }

  const audit = inputAudit || createMissionApprovalAudit();
  const useGlobalAudit = !inputAudit;
  const emitReceived = useGlobalAudit
    ? logMissionApprovalReceived
    : audit.logApprovalReceived.bind(audit);
  const emitConsumed = useGlobalAudit
    ? logMissionApprovalConsumed
    : audit.logApprovalConsumed.bind(audit);
  const emitStarted = useGlobalAudit
    ? logMissionStageExecutionStarted
    : audit.logStageExecutionStarted.bind(audit);
  const emitCompleted = useGlobalAudit
    ? logMissionStageExecutionCompleted
    : audit.logStageExecutionCompleted.bind(audit);

  emitReceived({
    missionId: mission.id,
    tenantId,
    stage: STAGES.DISCOVER,
    action: DISCOVERY_APPROVAL_ACTION,
    phase: APPROVAL_PHASES.APPROVAL_RECEIVED,
    command: question,
    operatorId,
  });

  let snapshot = engine.inspect(mission.id, { tenantId });
  const contributions = snapshot.contributions || [];
  const existingApproval = findDiscoveryApproval(contributions);
  const existingDiscovery = findScoutDiscoveryAfterApproval(contributions, existingApproval);

  if (existingApproval && existingDiscovery) {
    askPathTrace.traceEarlyReturn('advanceDiscoveryAfterApproval', 'already_executed');
    return {
      alreadyExecuted: true,
      approval: existingApproval,
      discovery: existingDiscovery,
      snapshot: engine.inspect(mission.id, { tenantId }),
      executionOutcome: existingDiscovery.payload.outcome || 'completed',
      approvalPhase: APPROVAL_PHASES.WAITING_FOR_NEXT_DECISION,
      audit,
    };
  }

  let approval = existingApproval;
  if (!approval) {
    setDiscoveryExecutingStatus(engine, snapshot.mission);
    const approvalResult = engine.contribute(
      mission.id,
      {
        specialist: SPECIALISTS.OPERATOR,
        kind: CONTRIBUTION_KINDS.APPROVAL,
        payload: {
          approved: true,
          consumed: true,
          command: question,
          action: DISCOVERY_APPROVAL_ACTION,
          stage: STAGES.DISCOVER,
        },
      },
      { tenantId }
    );
    approval = approvalResult.contribution;
    clearPendingOperatorDecision(engine, approvalResult.mission);
    emitConsumed({
      missionId: mission.id,
      tenantId,
      stage: STAGES.DISCOVER,
      action: DISCOVERY_APPROVAL_ACTION,
      phase: APPROVAL_PHASES.EXECUTING_STAGE,
      approvalId: approval.id,
      operatorId,
    });
  } else {
    clearPendingOperatorDecision(engine, snapshot.mission);
    emitConsumed({
      missionId: mission.id,
      tenantId,
      stage: STAGES.DISCOVER,
      action: DISCOVERY_APPROVAL_ACTION,
      phase: APPROVAL_PHASES.EXECUTING_STAGE,
      approvalId: approval.id,
      operatorId,
      reusedApproval: true,
    });
  }

  emitStarted({
    missionId: mission.id,
    tenantId,
    stage: STAGES.DISCOVER,
    executor: SPECIALISTS.SCOUT,
    phase: APPROVAL_PHASES.EXECUTING_STAGE,
    approvalId: approval.id,
  });

  const scoutResult = await runScoutForAmoMission(snapshot.mission, {
    question,
    operatorId,
    missionEngine,
    persist,
    runScout,
    scoutCompanies,
    scoutPeople,
    allowFixtureFallback,
  });

  const discoveryPayload = discoveryPayloadFromScoutResult(scoutResult, snapshot.mission);
  discoveryPayload.approvalId = approval.id;

  const discoveryContribution = engine.contribute(
    mission.id,
    {
      specialist: SPECIALISTS.SCOUT,
      kind: CONTRIBUTION_KINDS.DISCOVERY,
      payload: discoveryPayload,
    },
    { tenantId }
  );

  snapshot = engine.inspect(mission.id, { tenantId });
  const ctx = specialistContext(snapshot.contributions || [], {});

  if (ctx.scoutComplete && snapshot.mission.stage === STAGES.DISCOVER) {
    try {
      engine.progress(mission.id, SPECIALISTS.OPERATOR, {
        tenantId,
        stage: STAGES.UNDERSTAND,
      });
    } catch (_) {
      /* stage may already have advanced */
    }
    snapshot = engine.inspect(mission.id, { tenantId });
  } else if (snapshot.mission.status === 'Discovery Executing') {
    snapshot.mission.status = STAGE_LABELS[STAGES.DISCOVER] || 'Discovering';
    engine.store.putMission(snapshot.mission);
    snapshot = engine.inspect(mission.id, { tenantId });
  }

  const executionOutcome = discoveryPayload.blocked ? 'blocked' : 'completed';

  emitCompleted({
    missionId: mission.id,
    tenantId,
    stage: STAGES.DISCOVER,
    executor: SPECIALISTS.SCOUT,
    outcome: executionOutcome,
    phase:
      executionOutcome === 'blocked'
        ? APPROVAL_PHASES.WAITING_FOR_OPERATOR
        : APPROVAL_PHASES.WAITING_FOR_NEXT_DECISION,
    nextStage: snapshot.mission.stage,
    approvalId: approval.id,
    discoveryId: discoveryContribution.contribution.id,
    qualifiedCount: discoveryPayload.qualifiedCount || 0,
  });

  askPathTrace.traceEarlyReturn('advanceDiscoveryAfterApproval', executionOutcome);
  return {
    alreadyExecuted: false,
    approval,
    discovery: discoveryContribution.contribution,
    scoutResult,
    snapshot,
    executionOutcome,
    approvalPhase:
      executionOutcome === 'blocked'
        ? APPROVAL_PHASES.WAITING_FOR_OPERATOR
        : APPROVAL_PHASES.WAITING_FOR_NEXT_DECISION,
    audit,
  };
}

function buildDiscoveryApprovalProse(result) {
  askPathTrace.traceEnter('buildDiscoveryApprovalProse');
  const scoutPayload = (result.discovery && result.discovery.payload) || {};
  const discoveryResults = presentationFromDiscoveryPayload(scoutPayload);
  const blocked = result.executionOutcome === 'blocked';
  const sufficientEvidence = hasSufficientEvidenceForPrioritization(discoveryResults);
  const waitingOn = blocked
    ? 'Discovery blocker'
    : result.approvalPhase === APPROVAL_PHASES.WAITING_FOR_NEXT_DECISION && sufficientEvidence
      ? 'Prioritization approval'
      : result.approvalPhase === APPROVAL_PHASES.WAITING_FOR_NEXT_DECISION
        ? 'Evidence review'
        : null;
  const comm = buildMissionCommunication({
    headline: 'Mission Updated',
    mission: result.alreadyExecuted ? 'Discovery already executed.' : 'Approval Consumed',
    stage: 'Discovery',
    status: blocked ? 'Discovery Blocked' : 'Discovery Complete',
    waitingOn,
    confidence: discoveryResults.confidence,
    confidenceBreakdown: discoveryResults.confidenceBreakdown,
    nextStep: blocked
      ? 'Resolve the discovery blocker, then retry Discovery.'
      : sufficientEvidence
        ? 'Review discovered prospects and approve prioritization to continue.'
        : 'Review discovery evidence. Scout must surface attributable signals before prioritization.',
    operatorDecision: blocked
      ? 'Retry discovery?'
      : !blocked && sufficientEvidence
        ? 'Approve prioritization?'
        : !blocked
          ? 'Request more discovery evidence?'
          : null,
    discoveryResults: result.discovery ? discoveryResults : null,
    evidenceStatus: blocked && scoutPayload.summary ? scoutPayload.summary : null,
    sources: ['acquisition_mission', 'scout'],
    includeReasoningMarker: false,
  });
  return formatMissionProse(comm);
}

module.exports = {
  APPROVAL_PHASES,
  DISCOVERY_APPROVAL_ACTION,
  PLAN_APPROVAL_ACTION,
  buildDelegationFromAmoMission,
  findDiscoveryApproval,
  findPlanApproval,
  findScoutDiscoveryAfterApproval,
  hasPendingPlanApproval,
  hasPendingDiscoveryApproval,
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  buildDiscoveryApprovalProse,
  mapScoutIntelligenceToDiscoveryPayload,
  fixtureScoutDiscoveryResult,
};
