'use strict';

/**
 * SPEC-127 / SPEC-128 — Acquisition Mission execution commands from the workspace ask path.
 * Operator approval consumes pending decisions and executes the current stage once.
 */

const amo = require('../../acquisition-mission');
const { buildStructuredResponse } = require('./WorkspaceTypes');
const {
  buildAcquisitionMissionCommunication,
  buildMissionCommunication,
  formatMissionProse,
  applyMissionCommunication,
  buildReasoningEvidence,
} = require('./MissionCommunication');
const {
  resolveTenantId,
  resolveAcquisitionEngine,
  resolveMissionId,
} = require('./WorkspaceMissionInspection');
const { isMissionExecutionCommand } = require('./ExecutionLanguageDetection');
const {
  isExplicitMissionExit,
  resolveAcquisitionActiveMission,
} = require('./ActiveMissionGuard');
const {
  advanceDiscoveryAfterApproval,
  hasPendingDiscoveryApproval,
  findDiscoveryApproval,
  findScoutDiscoveryAfterApproval,
  APPROVAL_PHASES,
} = require('./AmoOperatorApproval');
const {
  presentationFromDiscoveryPayload,
} = require('../../acquisition-mission/DiscoveryPresentation');
const {
  createMissionApprovalAudit,
  logMissionApprovalMatched,
} = require('./audit/MissionApprovalAudit');
const askPathTrace = require('./audit/AskPathTrace');

const { STAGES, STAGE_LABELS, SPECIALISTS, CONTRIBUTION_KINDS } = amo;

function stageLabel(stage) {
  return STAGE_LABELS[stage] || stage || 'Active';
}

function buildExecutionMissionResponse({
  mission,
  snapshot,
  action,
  question,
  executionResult,
}) {
  askPathTrace.traceEnter('buildExecutionMissionResponse', { action });
  const workspace = snapshot.workspace || {};
  const scout = workspace.scout || null;
  const stage = mission.stage || STAGES.DISCOVER;
  const progress = mission.progressPercent != null ? mission.progressPercent : null;

  if (executionResult && action === 'discovery_approved') {
    const scoutPayload = (executionResult.discovery && executionResult.discovery.payload) || {};
    const discoveryResults = presentationFromDiscoveryPayload(scoutPayload);
    const blocked = executionResult.executionOutcome === 'blocked';
    const scoutComplete =
      !blocked &&
      snapshot.workspace &&
      snapshot.workspace.scout &&
      snapshot.workspace.scout.state === 'complete';
    const comm = buildMissionCommunication({
      headline: 'Mission Updated',
      mission: mission.title || mission.id,
      objective: mission.objective,
      status: blocked ? 'Discovery Blocked' : stageLabel(mission.stage || stage),
      stage: 'Discovery',
      progress,
      health: snapshot.health && snapshot.health.label ? snapshot.health.label : 'Healthy',
      waitingOn: blocked
        ? 'Discovery blocker'
        : scoutComplete
          ? 'Prioritization approval'
          : null,
      confidence:
        discoveryResults.confidence != null
          ? discoveryResults.confidence
          : mission.confidence,
      nextStep: blocked
        ? 'Resolve the discovery blocker, then retry Discovery.'
        : scoutComplete
          ? 'Review discovered prospects and approve prioritization to continue.'
          : 'Review mission workspace for Scout contributions.',
      operatorDecision: blocked
        ? 'Retry discovery?'
        : scoutComplete
          ? 'Approve prioritization?'
          : null,
      discoveryResults: executionResult.discovery ? discoveryResults : null,
      evidenceStatus: 'Mission state',
      sources: ['acquisition_mission', 'scout'],
      reasoningEvidence: buildReasoningEvidence({
        known: [`Mission ${mission.id} executed Discovery after operator approval.`],
        inference: executionResult.alreadyExecuted
          ? ['Discovery was already executed for this approval — not re-run.']
          : ['Operator approval consumed; Scout discovery ran once.'],
        unknown: [],
        evidenceNeeded: [],
        confidence:
          discoveryResults.confidence != null
            ? discoveryResults.confidence
            : mission.confidence,
      }),
      includeReasoningMarker: true,
    });
    const prose = formatMissionProse(comm);

    const structured = applyMissionCommunication(
      buildStructuredResponse({
        answer: prose,
        reasoning: [],
        supportingEvidence: [],
        contradictingEvidence: [],
        confidence: mission.confidence != null ? mission.confidence : 0.84,
        nextInvestigations: [],
        recommendedActions: [
          {
            id: 'open_mission',
            type: 'open_mission',
            label: 'Open mission workspace',
            payload: { missionId: mission.id },
          },
        ],
        confidenceContributors: ['spec_128', 'acquisition_mission'],
        timelineReferences: [],
        relatedEntities: [
          { id: mission.id, type: 'acquisition_mission', name: mission.title || mission.id },
        ],
        metadata: buildExecutionMetadata(mission, action, executionResult),
      }),
      comm
    );

    return { structured, prose, comm, action };
  }

  let status = `Active mission — ${stageLabel(stage)}.`;
  let nextStep = 'Review mission workspace for the latest specialist contributions.';
  let operatorDecision = null;

  const pendingDiscovery = hasPendingDiscoveryApproval(snapshot);
  if (pendingDiscovery) {
    operatorDecision = (mission.pendingOperatorDecision && mission.pendingOperatorDecision.prompt) ||
      'Approve discovery?';
  } else if (action === 'operator_approved') {
    status = `Operator approved — ${stageLabel(stage)}.`;
    nextStep = 'Mission advanced under operator approval.';
    operatorDecision = 'Continue in mission workspace?';
  }

  const comm = buildAcquisitionMissionCommunication({
    mission: mission.title || mission.id,
    objective: mission.objective,
    status,
    stage: stageLabel(stage),
    progress,
    health: snapshot.health && snapshot.health.label
      ? snapshot.health.label
      : null,
    waitingOn: scout && scout.label ? scout.label : null,
    confidence: mission.confidence,
    currentUnderstanding: [
      mission.targetSegment
        ? `Target segment: ${mission.targetSegment}.`
        : null,
      `Execution command received: "${String(question || '').trim()}".`,
    ].filter(Boolean),
    nextStep,
    operatorDecision,
    evidenceStatus: 'Mission state',
    sources: ['acquisition_mission'],
    reasoningEvidence: buildReasoningEvidence({
      known: [`Mission ${mission.id} is active at stage ${stageLabel(stage)}.`],
      inference: ['Execution commands bind to the active mission workspace.'],
      unknown: [],
      evidenceNeeded: [],
      confidence: mission.confidence,
    }),
  });

  const prose = formatMissionProse(comm);
  const structured = applyMissionCommunication(
    buildStructuredResponse({
      answer: prose,
      reasoning: [],
      supportingEvidence: [],
      contradictingEvidence: [],
      confidence: mission.confidence != null ? mission.confidence : 0.84,
      nextInvestigations: [],
      recommendedActions: [
        {
          id: 'open_mission',
          type: 'open_mission',
          label: 'Open mission workspace',
          payload: { missionId: mission.id },
        },
      ],
      confidenceContributors: ['spec_127', 'acquisition_mission'],
      timelineReferences: [],
      relatedEntities: [
        { id: mission.id, type: 'acquisition_mission', name: mission.title || mission.id },
      ],
      metadata: buildExecutionMetadata(mission, action, executionResult),
    }),
    comm
  );

  return { structured, prose, comm, action };
}

function buildExecutionMetadata(mission, action, executionResult) {
  return {
    sourcesUsed: {
      briefing: false,
      reasoning: false,
      memory: false,
      policy: false,
      knowledge: false,
      missionState: true,
      clientIntelligence: false,
    },
    evidenceCount: 0,
    asOf: new Date().toISOString(),
    unavailable: [],
    acquisitionMission: true,
    acquisitionMissionExecution: true,
    missionExecutionAction: action,
    approvalConsumed: Boolean(executionResult && executionResult.approval),
    stageExecuted: Boolean(executionResult && executionResult.discovery),
    executionOutcome: executionResult ? executionResult.executionOutcome : null,
    approvalPhase: executionResult ? executionResult.approvalPhase || null : null,
    strictOutputShape: true,
    missionCommunication: true,
  };
}

function detectExecutionAction(question, snapshot) {
  askPathTrace.traceEnter('detectExecutionAction');
  const q = String(question || '').trim();
  const lower = q.toLowerCase();

  const discoveryApprovalPattern =
    /\bapprov(e|al|ed)\b.*\bbegin\b/i.test(q) ||
    /\bbegin\b.*\bdiscover/i.test(lower) ||
    /\b(?:begin|start|run|execute)\b.*\bdiscover/i.test(lower);

  if (discoveryApprovalPattern) {
    askPathTrace.traceEarlyReturn('detectExecutionAction', 'discovery_approved');
    return 'discovery_approved';
  }

  if (
    snapshot &&
    snapshot.mission &&
    snapshot.mission.stage === STAGES.DISCOVER &&
    hasPendingDiscoveryApproval(snapshot) &&
    /\bapprov(e|al|ed)\b/i.test(q)
  ) {
    askPathTrace.traceEarlyReturn('detectExecutionAction', 'discovery_approved_pending');
    return 'discovery_approved';
  }

  if (/\bapprov(e|al|ed)\b/i.test(q)) {
    askPathTrace.traceEarlyReturn('detectExecutionAction', 'operator_approved');
    return 'operator_approved';
  }
  if (/\b(?:continue|proceed|resume|next)\b/i.test(q)) {
    askPathTrace.traceEarlyReturn('detectExecutionAction', 'operator_approved_continue');
    return 'operator_approved';
  }
  if (/\b(?:prioritization|outreach|send)\b/i.test(q)) {
    askPathTrace.traceEarlyReturn('detectExecutionAction', 'operator_approved_prioritization');
    return 'operator_approved';
  }
  askPathTrace.traceEarlyReturn('detectExecutionAction', 'operator_approved_default');
  return 'operator_approved';
}

function shouldExecuteDiscovery(action, snapshot) {
  askPathTrace.traceEnter('shouldExecuteDiscovery', { action });
  if (action !== 'discovery_approved') {
    askPathTrace.traceEarlyReturn('shouldExecuteDiscovery', 'action_not_discovery_approved');
    return false;
  }
  const mission = snapshot.mission || {};
  if (mission.stage !== STAGES.DISCOVER) {
    askPathTrace.traceEarlyReturn('shouldExecuteDiscovery', 'wrong_stage');
    return false;
  }
  const contributions = snapshot.contributions || [];
  const approval = findDiscoveryApproval(contributions);
  if (approval && findScoutDiscoveryAfterApproval(contributions, approval)) {
    askPathTrace.traceEarlyReturn('shouldExecuteDiscovery', 'already_executed');
    return false;
  }
  askPathTrace.traceEarlyReturn('shouldExecuteDiscovery', 'should_execute', { result: true });
  return true;
}

/**
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function maybeHandleAcquisitionMissionExecution(input = {}) {
  askPathTrace.traceEnter('maybeHandleAcquisitionMissionExecution');
  const question = String(input.question || '').trim();
  if (!question || !isMissionExecutionCommand(question)) {
    askPathTrace.traceEarlyReturn('maybeHandleAcquisitionMissionExecution', 'not_execution_command');
    return null;
  }
  if (isExplicitMissionExit(question).explicit) {
    askPathTrace.traceEarlyReturn('maybeHandleAcquisitionMissionExecution', 'explicit_mission_exit');
    return null;
  }

  const tenantId = resolveTenantId(input);
  const engine = resolveAcquisitionEngine(input);
  if (!engine || !tenantId) {
    askPathTrace.traceEarlyReturn('maybeHandleAcquisitionMissionExecution', 'no_engine_or_tenant');
    return null;
  }

  const mission =
    (await resolveAcquisitionActiveMission(input)) ||
    (() => {
      const missions = engine.list(tenantId);
      const missionId = resolveMissionId(input, missions);
      return missionId ? missions.find((row) => row && row.id === missionId) : null;
    })();

  if (!mission || mission.stage === STAGES.IMPROVE) {
    askPathTrace.traceEarlyReturn('maybeHandleAcquisitionMissionExecution', 'no_mission_or_improve');
    return null;
  }

  let snapshot = engine.inspect(mission.id, { tenantId });
  const action = detectExecutionAction(question, snapshot);
  const audit = input.audit || createMissionApprovalAudit();
  const useGlobalAudit = !input.audit;
  const emitMatched = useGlobalAudit
    ? logMissionApprovalMatched
    : audit.logApprovalMatched.bind(audit);

  if (action === 'discovery_approved' && shouldExecuteDiscovery(action, snapshot)) {
    emitMatched({
      missionId: mission.id,
      tenantId,
      stage: STAGES.DISCOVER,
      action,
      phase: APPROVAL_PHASES.WAITING_FOR_OPERATOR,
      pendingPrompt:
        (mission.pendingOperatorDecision && mission.pendingOperatorDecision.prompt) ||
        'Approve discovery?',
      command: question,
    });
  }

  let executionResult = null;

  if (shouldExecuteDiscovery(action, snapshot)) {
    executionResult = await advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId,
      question,
      operatorId: input.operatorId || (input.session && input.session.operator) || null,
      missionEngine: input.missionEngine,
      persist: input.persist,
      runScout: input.runScout,
      scoutCompanies: input.scoutCompanies,
      scoutPeople: input.scoutPeople,
      allowFixtureFallback: input.allowFixtureFallback,
      audit,
    });
    snapshot = executionResult.snapshot || engine.inspect(mission.id, { tenantId });
  } else if (action === 'discovery_approved') {
    const approval = findDiscoveryApproval(snapshot.contributions || []);
    const discovery = findScoutDiscoveryAfterApproval(snapshot.contributions || [], approval);
    if (approval && discovery) {
      const blocked = discovery.payload.outcome === 'blocked';
      executionResult = {
        alreadyExecuted: true,
        approval,
        discovery,
        snapshot,
        executionOutcome: blocked ? 'blocked' : 'completed',
        approvalPhase: blocked
          ? APPROVAL_PHASES.WAITING_FOR_OPERATOR
          : APPROVAL_PHASES.WAITING_FOR_NEXT_DECISION,
      };
    }
  } else if (action === 'operator_approved') {
    try {
      engine.contribute(
        mission.id,
        {
          specialist: SPECIALISTS.OPERATOR,
          kind: CONTRIBUTION_KINDS.APPROVAL,
          payload: { approved: true, command: question, action },
        },
        { tenantId }
      );
    } catch (_) {
      /* approval may already exist — mission still owns the turn */
    }
    snapshot = engine.inspect(mission.id, { tenantId });
  }

  const response = buildExecutionMissionResponse({
    mission: snapshot.mission || mission,
    snapshot,
    action,
    question,
    executionResult,
  });

  if (input.session && input.session.context && typeof input.session.context === 'object') {
    input.session.context.missionId = mission.id;
    input.session.context.acquisitionMissionId = mission.id;
    input.session.context.acquisitionOwner = 'MissionEngine';
  }

  askPathTrace.traceEarlyReturn('maybeHandleAcquisitionMissionExecution', `acquisition_mission_${action}`, {
    action,
    missionId: mission.id,
  });
  return {
    reason: `acquisition_mission_${action}`,
    structured: response.structured,
    prose: response.prose,
    mission: snapshot.mission || mission,
    action,
    executionResult,
    audit,
  };
}

module.exports = {
  detectExecutionAction,
  buildExecutionMissionResponse,
  maybeHandleAcquisitionMissionExecution,
  shouldExecuteDiscovery,
};
