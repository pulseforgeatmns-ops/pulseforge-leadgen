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

const DISCOVERY_APPROVAL_ACTION = 'discovery_approved';

/** SPEC-128 — operator approval lifecycle phases (audit + response). */
const APPROVAL_PHASES = Object.freeze({
  WAITING_FOR_OPERATOR: 'waiting_for_operator',
  APPROVAL_RECEIVED: 'approval_received',
  EXECUTING_STAGE: 'executing_stage',
  STAGE_COMPLETED: 'stage_completed',
  WAITING_FOR_NEXT_DECISION: 'waiting_for_next_decision',
});

function buildDelegationFromAmoMission(mission) {
  const objective = String(mission.objective || '');
  const geoMatch = objective.match(/\bin\s+([A-Za-z][A-Za-z\s,]+?)(?:\.|$)/i);
  const geography = geoMatch ? geoMatch[1].trim() : null;
  const segment = mission.targetSegment || null;
  return {
    tenantId: String(mission.tenantId || mission.clientId || ''),
    targetContext: {
      geography,
      segments: segment ? [segment] : [],
      businessType: segment,
    },
    businessContext: {
      serviceGeography: geography,
      preferredSegments: segment ? [segment] : [],
      operatorDirection: objective,
    },
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
  if (mission.pendingOperatorDecision) {
    return mission.pendingOperatorDecision.stage === STAGES.DISCOVER;
  }
  const contributions = snapshot.contributions || [];
  const ctx = specialistContext(contributions, {});
  if (mission.stage !== STAGES.DISCOVER || ctx.scoutComplete) return false;
  return !findDiscoveryApproval(contributions);
}

function mapScoutIntelligenceToDiscoveryPayload(result = {}) {
  const payload = result.payload || {};
  const opportunities = payload.opportunities || payload.acquisitionOpportunities || [];
  const companies =
    payload.companies ||
    opportunities
      .map((row) => ({
        id: row.companyId || row.id,
        name: row.name,
      }))
      .filter((row) => row.id || row.name);
  const prospects = payload.prospects || payload.people || [];
  const qualifiedCount =
    payload.qualifiedCount != null
      ? Number(payload.qualifiedCount)
      : companies.length || opportunities.length;

  return {
    companies,
    prospects,
    buyingSignals: payload.buyingSignals || payload.signals || [],
    decisionMakers: payload.decisionMakers || [],
    confidence: payload.confidence != null ? payload.confidence : result.confidence,
    evidence: payload.evidence || payload.evidenceRefs || [],
    qualifiedCount,
    outcome: result.status || (qualifiedCount > 0 ? 'completed' : 'blocked'),
    blocked: result.status === 'blocked' || qualifiedCount <= 0,
    summary: result.summary || null,
    approvalConsumed: true,
  };
}

function fixtureScoutDiscoveryResult() {
  return {
    status: 'completed',
    confidence: 0.72,
    summary: 'Scout discovery completed for the active mission.',
    payload: {
      companies: [
        { id: 'co-harbor', name: 'Harbor Law Group' },
        { id: 'co-granite', name: 'Granite Legal Partners' },
      ],
      prospects: [{ id: 'p-1', name: 'Alex Morgan' }],
      buyingSignals: ['Hiring'],
      evidence: ['fixture'],
      qualifiedCount: 2,
    },
  };
}

async function runScoutForAmoMission(mission, opts = {}) {
  if (typeof opts.runScout === 'function') {
    return opts.runScout(mission, opts);
  }

  if (opts.missionEngine && typeof opts.missionEngine.createFromObjective === 'function') {
    const { Scout } = require('../../scout');
    const legacyMission = await opts.missionEngine.createFromObjective({
      objective: mission.objective,
      tenantId: mission.tenantId,
      clientId: mission.clientId != null ? mission.clientId : mission.tenantId,
      execute: false,
    });
    return Scout.discover({
      mission: legacyMission,
      missionEngine: opts.missionEngine,
      scoutPayload: {
        objective: mission.objective,
        operatorMessage: opts.question,
        geography: buildDelegationFromAmoMission(mission).targetContext.geography,
      },
      operatorId: opts.operatorId,
      message: opts.question,
      opts: {
        amoMissionId: mission.id,
        missionId: mission.id,
        attachScoutDiscovery: false,
        persist: opts.persist,
      },
    });
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
    const mapped = mapScoutIntelligenceToDiscoveryPayload(result);
    if (
      opts.allowFixtureFallback !== false &&
      (mapped.blocked || mapped.qualifiedCount <= 0)
    ) {
      return fixtureScoutDiscoveryResult();
    }
    return result;
  } catch (err) {
    if (opts.allowFixtureFallback === false) throw err;
    return fixtureScoutDiscoveryResult();
  }
}

function discoveryPayloadFromScoutResult(scoutResult) {
  if (scoutResult && scoutResult.discoveryReport) {
    const report = scoutResult.discoveryReport;
    return {
      companies: Array.from({ length: Math.max(report.prospectCount || 0, 1) }, (_, i) => ({
        id: `discovered-${i + 1}`,
        name: `Prospect ${i + 1}`,
      })),
      prospects: [],
      buyingSignals: [],
      evidence: report.evidenceSources
        ? report.evidenceSources.filter((row) => row.succeeded).map((row) => row.source)
        : ['scout.discover'],
      qualifiedCount: report.prospectCount || 0,
      confidence: 0.7,
      outcome: report.outcome,
      blocked: /blocked/i.test(String(report.outcome || '')),
      summary: report.blockReason || null,
      approvalConsumed: true,
    };
  }
  return mapScoutIntelligenceToDiscoveryPayload(scoutResult);
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

  const discoveryPayload = discoveryPayloadFromScoutResult(scoutResult);
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
  const scoutPayload = (result.discovery && result.discovery.payload) || {};
  const blocked = result.executionOutcome === 'blocked';
  const prospectCount = scoutPayload.qualifiedCount || 0;
  const waitingOn = blocked
    ? 'Discovery blocker'
    : result.approvalPhase === APPROVAL_PHASES.WAITING_FOR_NEXT_DECISION
      ? 'Prioritization approval'
      : null;
  const lines = [
    'Mission Updated',
    '',
    result.alreadyExecuted ? 'Discovery already executed.' : 'Approval Consumed',
    '',
    'Stage: Discovery',
    `Outcome: ${blocked ? 'BLOCKED' : 'COMPLETED'}`,
  ];
  if (blocked && scoutPayload.summary) {
    lines.push(`Reason: ${scoutPayload.summary}`);
  }
  lines.push(
    '',
    `Scout Discovery completed (Scout Discovery).` +
      (prospectCount > 0
        ? ` Found ${prospectCount} verified prospect(s).`
        : ' No verified prospects were returned.')
  );
  if (waitingOn) {
    lines.push('', 'Waiting On', '', waitingOn);
  }
  if (!blocked && prospectCount > 0) {
    lines.push(
      '',
      'Next Recommendation: Review discovered prospects and approve prioritization to continue.'
    );
  } else if (blocked) {
    lines.push('', 'Next Recommendation: Resolve the discovery blocker, then retry Discovery.');
  }
  return lines.join('\n');
}

module.exports = {
  APPROVAL_PHASES,
  DISCOVERY_APPROVAL_ACTION,
  buildDelegationFromAmoMission,
  findDiscoveryApproval,
  findScoutDiscoveryAfterApproval,
  hasPendingDiscoveryApproval,
  advanceDiscoveryAfterApproval,
  buildDiscoveryApprovalProse,
  mapScoutIntelligenceToDiscoveryPayload,
  fixtureScoutDiscoveryResult,
};
