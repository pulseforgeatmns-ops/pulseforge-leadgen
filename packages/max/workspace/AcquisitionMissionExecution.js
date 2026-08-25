'use strict';

/**
 * SPEC-127 / SPEC-128 — Acquisition Mission execution commands from the workspace ask path.
 * Operator approval consumes pending decisions and executes the current stage once.
 */

const amo = require('../../acquisition-mission');
const { isRolledBackExecution, formatRollbackProse } = amo;
const { rollbackStageLabel } = require('../../acquisition-mission/DecisionReadiness');
const { buildStructuredResponse } = require('./WorkspaceTypes');
const { buildOpenMissionAction, MISSION_RUNTIMES } = require('./MissionActions');
const {
  buildAcquisitionMissionCommunication,
  buildMissionCommunication,
  formatMissionProse,
  applyMissionCommunication,
  buildReasoningEvidence,
} = require('./MissionCommunication');
const {
  resolveTenantId,
  resolveAcquisitionMissionRuntime,
  resolveAcquisitionEngine,
  resolveMissionId,
  assertRuntimeEngine,
} = require('./WorkspaceMissionInspection');
const { isMissionExecutionCommand } = require('./ExecutionLanguageDetection');
const {
  isExplicitMissionExit,
  resolveAcquisitionActiveMission,
} = require('./ActiveMissionGuard');
const {
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
  advancePlanAfterApproval,
  advancePlanClarification,
  cancelMissionPlan,
  beginPlanEdit,
  applyPlanEdits,
  hasPendingDiscoveryApproval,
  hasPendingPrioritizationApproval,
  hasPendingPlanApproval,
  hasPendingPlanClarification,
  hasConsumablePendingDecision,
  presentableOperatorDecision,
  findDiscoveryApproval,
  findScoutDiscoveryAfterApproval,
  findPrioritizationApproval,
  APPROVAL_PHASES,
} = require('./AmoOperatorApproval');
const {
  assertMissionStateConsistent,
  MISSION_STATE_INCONSISTENT,
} = require('../../acquisition-mission/PendingOperatorDecision');
const {
  runAutonomousProgression,
  isAutonomousProgressionCommand,
  formatMissionProgressPresentation,
  AUTONOMOUS_OPERATOR_ID,
} = require('../../acquisition-mission/MissionProgression');
const { shouldAutoConsumeDiscoveryApproval } = require('../../acquisition-mission/OperatorDecisionPolicy');
const { getSessionState } = require('./SessionState');
const { isStructuredMissionApproved } = require('../../acquisition-mission/StructuredMission');
const {
  presentationFromDiscoveryPayload,
} = require('../../acquisition-mission/DiscoveryPresentation');
const {
  createMissionApprovalAudit,
  logMissionApprovalMatched,
} = require('./audit/MissionApprovalAudit');
const { formatMissionUnderstandingProse } = require('../../acquisition-mission/StructuredMission');
const { isMissionPlanningTurn } = require('./MissionPlanningTurn');
const askPathTrace = require('./audit/AskPathTrace');

const { STAGES, STAGE_LABELS, SPECIALISTS, CONTRIBUTION_KINDS, OPERATOR_DECISION_KINDS } = amo;

function resolveExecutionPolicy(input = {}) {
  if (input.executionPolicy) return input.executionPolicy;
  const sessionState = getSessionState(input.session);
  return sessionState && sessionState.executionPolicy ? sessionState.executionPolicy : null;
}

async function maybeAutoAdvanceDiscoveryAfterPlan(input = {}, planResult = {}) {
  const { engine, mission, tenantId } = input;
  if (!engine || !mission || planResult.rolledBack) {
    return { planResult, snapshot: planResult.snapshot, discoveryResult: null, action: 'plan_approved' };
  }

  let snapshot = planResult.snapshot || engine.inspect(mission.id, { tenantId });
  const executionPolicy = resolveExecutionPolicy(input);
  if (!shouldAutoConsumeDiscoveryApproval(snapshot, executionPolicy)) {
    return { planResult, snapshot, discoveryResult: null, action: 'plan_approved' };
  }

  try {
    const discoveryResult = await advanceDiscoveryAfterApproval({
      engine,
      mission: (snapshot && snapshot.mission) || mission,
      tenantId,
      question: 'Autonomous execution policy. Discovery approval auto-consumed.',
      operatorId: input.operatorId || (input.session && input.session.operator) || AUTONOMOUS_OPERATOR_ID,
      missionEngine: input.missionEngine,
      runScout: input.runScout,
      scoutCompanies: input.scoutCompanies,
      scoutPeople: input.scoutPeople,
      allowFixtureFallback: input.allowFixtureFallback,
      audit: input.audit,
      ...resolveStagePersistOpts(input),
    });
    snapshot = discoveryResult.snapshot || engine.inspect(mission.id, { tenantId });
    return {
      planResult,
      snapshot,
      discoveryResult,
      action: 'discovery_approved',
      autoConsumedDiscoveryApproval: true,
    };
  } catch (err) {
    if (!isRolledBackExecution(err)) throw err;
    return {
      planResult,
      snapshot: engine.inspect(mission.id, { tenantId }),
      discoveryResult: {
        rolledBack: true,
        error: err,
        transactionId: err.transactionId,
      },
      action: 'plan_approved',
    };
  }
}

function resolveStagePersistOpts(input = {}) {
  if (input.persist === false) {
    return { persist: false, pool: input.pool, persistStage: input.persistStage };
  }
  if (typeof input.persistStage === 'function') {
    return { persistStage: input.persistStage, pool: input.pool };
  }
  if (input.pool) {
    return { persist: true, pool: input.pool };
  }
  return {};
}

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

  if (executionResult && executionResult.rolledBack) {
    const err = executionResult.error || {};
    const stageName = rollbackStageLabel(action);
    const presented = presentableOperatorDecision(snapshot);
    const comm = buildMissionCommunication({
      headline: `${stageName} could not execute`,
      mission: mission.title || mission.id,
      objective: mission.objective,
      status: 'Unchanged',
      stage: stageName,
      progress,
      waitingOn: 'Resolve the blocker',
      nextStep: err.details && err.details.recommendedAction
        ? err.details.recommendedAction
        : 'Resolve the blocker and retry.',
      operatorDecision: presented ? presented.prompt : (
        action === 'plan_approved'
          ? 'Approve mission plan?'
          : action === 'prioritization_approved'
            ? 'Approve findings?'
            : 'Approve discovery?'
      ),
      evidenceStatus: err.rollbackReason || err.message || formatRollbackProse(stageName, err),
      sources: ['acquisition_mission', 'tme'],
      includeReasoningMarker: false,
    });
    const prose = formatRollbackProse(stageName, err);
    const structured = applyMissionCommunication(
      buildStructuredResponse({
        answer: prose,
        reasoning: [],
        supportingEvidence: [],
        contradictingEvidence: [],
        confidence: mission.confidence != null ? mission.confidence : 0.5,
        nextInvestigations: [],
        recommendedActions: [],
        confidenceContributors: ['spec_131', 'tme'],
        timelineReferences: [],
        relatedEntities: [
          { id: mission.id, type: 'acquisition_mission', name: mission.title || mission.id },
        ],
        metadata: {
          ...buildExecutionMetadata(mission, action, executionResult),
          rolledBack: true,
          transactionId: executionResult.transactionId || err.transactionId,
          errorClass: err.tmeClass || null,
        },
      }),
      comm
    );
    return { structured, prose, comm, action };
  }

  if (executionResult && action === 'plan_clarified') {
    const pending = mission.pendingOperatorDecision || {};
    const comm = buildMissionCommunication({
      headline: pending.kind === 'plan_approval' ? 'Mission Understanding' : 'Need a decision',
      mission: mission.title || mission.id,
      objective: mission.objective,
      status: pending.kind === 'plan_approval' ? 'Plan ready for approval' : 'Clarification required',
      stage: 'Planning',
      progress,
      waitingOn: pending.prompt || 'Operator clarification',
      nextStep: pending.clarificationPrompt || pending.missionUnderstanding || pending.prompt,
      operatorDecision: pending.kind === 'plan_approval'
        ? 'Approve\nEdit\nCancel'
        : pending.prompt,
      evidenceStatus: pending.missionUnderstanding || pending.clarificationPrompt || 'Mission Planning Engine',
      sources: ['acquisition_mission', 'mission_planner'],
      includeReasoningMarker: false,
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
        recommendedActions: [],
        confidenceContributors: ['spec_130', 'mission_planner'],
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

  if (executionResult && action === 'plan_cancelled') {
    const comm = buildMissionCommunication({
      headline: 'Mission Plan Cancelled',
      mission: mission.title || mission.id,
      objective: mission.objective,
      status: 'Cancelled',
      stage: 'Planning',
      progress,
      nextStep: 'No specialist will execute. Create a new mission when ready.',
      operatorDecision: null,
      evidenceStatus: 'Operator cancelled before lock.',
      sources: ['acquisition_mission', 'mission_planner'],
      includeReasoningMarker: false,
    });
    const prose = formatMissionProse(comm);
    const structured = applyMissionCommunication(
      buildStructuredResponse({
        answer: prose,
        reasoning: [],
        supportingEvidence: [],
        contradictingEvidence: [],
        confidence: 1,
        nextInvestigations: [],
        recommendedActions: [],
        confidenceContributors: ['spec_130', 'mission_planner'],
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

  if (executionResult && action === 'plan_approved') {
    const understanding =
      (mission.structuredMission &&
        formatMissionUnderstandingProse(mission.structuredMission)) ||
      (executionResult.structuredMission &&
        formatMissionUnderstandingProse(executionResult.structuredMission)) ||
      '';
    const comm = buildMissionCommunication({
      headline: 'Mission Plan Approved',
      mission: mission.title || mission.id,
      objective: mission.objective,
      status: 'Plan Approved',
      stage: 'Discover',
      progress,
      health: snapshot.health && snapshot.health.label ? snapshot.health.label : 'Healthy',
      waitingOn: 'Discovery approval',
      confidence: mission.confidence,
      nextStep: 'Mission plan is now immutable. Approve discovery to begin Scout.',
      operatorDecision: 'Approve discovery?',
      evidenceStatus: understanding || 'Structured mission contract frozen',
      sources: ['acquisition_mission', 'mission_planner'],
      includeReasoningMarker: false,
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
          buildOpenMissionAction({
            missionId: mission.id,
            runtime: mission.runtime || MISSION_RUNTIMES.AMO,
            label: 'Open mission workspace',
          }),
        ],
        confidenceContributors: ['spec_130', 'acquisition_mission'],
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

  if (executionResult && action === 'discovery_approved') {
    const scoutPayload = (executionResult.discovery && executionResult.discovery.payload) || {};
    const discoveryResults = presentationFromDiscoveryPayload(scoutPayload);
    const blocked = executionResult.executionOutcome === 'blocked';
    const presented = presentableOperatorDecision(snapshot);
    const comm = buildMissionCommunication({
      headline: presented && presented.headline
        ? presented.headline
        : presented && presented.kind === OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL
          ? 'Mission Intelligence Report Ready'
          : 'Mission Updated',
      mission: mission.title || mission.id,
      objective: mission.objective,
      status: blocked ? 'Discovery Blocked' : 'Discovery Complete',
      stage: 'Discovery',
      progress,
      health: snapshot.health && snapshot.health.label ? snapshot.health.label : 'Healthy',
      waitingOn: blocked
        ? 'Discovery blocker'
        : presented && presented.kind === OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL
          ? 'Prioritization approval'
          : presented && presented.kind === OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION
            ? 'Discovery coverage review'
            : 'Evidence review',
      confidence:
        discoveryResults.confidence != null
          ? discoveryResults.confidence
          : mission.confidence,
      confidenceBreakdown: discoveryResults.confidenceBreakdown,
      nextStep: blocked
        ? 'Resolve the discovery blocker, then retry Discovery.'
        : presented && presented.readiness && presented.readiness.recommendedAction
          ? presented.readiness.recommendedAction
          : presented && presented.kind === OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL
            ? 'Review discovered prospects and approve findings to continue.'
            : 'Review discovery coverage and continue investigation.',
      operatorDecision: presented ? presented.prompt : null,
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
          buildOpenMissionAction({
            missionId: mission.id,
            runtime: mission.runtime || MISSION_RUNTIMES.AMO,
            label: 'Open mission workspace',
          }),
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

  if (executionResult && action === 'prioritization_approved') {
    const scoutPayload = ((snapshot.contributions || []).find(
      (row) => row.specialist === SPECIALISTS.SCOUT && row.kind === CONTRIBUTION_KINDS.DISCOVERY
    ) || {}).payload || {};
    const discoveryResults = presentationFromDiscoveryPayload(scoutPayload);
    const comm = buildMissionCommunication({
      headline: 'Mission Updated',
      mission: mission.title || mission.id,
      objective: mission.objective,
      status: stageLabel(mission.stage || stage),
      stage: stageLabel(mission.stage || stage),
      progress,
      health: snapshot.health && snapshot.health.label ? snapshot.health.label : 'Healthy',
      waitingOn: null,
      confidence:
        discoveryResults.confidence != null
          ? discoveryResults.confidence
          : mission.confidence,
      confidenceBreakdown: discoveryResults.confidenceBreakdown,
      nextStep: 'Review mission workspace for Max prioritization and planning.',
      operatorDecision: null,
      discoveryResults,
      evidenceStatus: 'Prioritization approved',
      sources: ['acquisition_mission', 'scout'],
      includeReasoningMarker: false,
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
          buildOpenMissionAction({
            missionId: mission.id,
            runtime: mission.runtime || MISSION_RUNTIMES.AMO,
            label: 'Open mission workspace',
          }),
        ],
        confidenceContributors: ['spec_141', 'acquisition_mission'],
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

  const presented = presentableOperatorDecision(snapshot);
  if (presented) {
    operatorDecision = presented.prompt;
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
        buildOpenMissionAction({
          missionId: mission.id,
          runtime: mission.runtime || MISSION_RUNTIMES.AMO,
          label: 'Open mission workspace',
        }),
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

function detectExecutionAction(question, snapshot, operatorIntent = null) {
  askPathTrace.traceEnter('detectExecutionAction');
  const q = String(question || '').trim();
  const lower = q.toLowerCase();
  const planningTurn = operatorIntent
    ? operatorIntent.planningRequested
    : isMissionPlanningTurn(snapshot.mission || snapshot, q);

  if (
    hasPendingPlanClarification(snapshot) &&
    !/\b(cancel)\b/i.test(q) &&
    planningTurn
  ) {
    askPathTrace.traceEarlyReturn('detectExecutionAction', 'plan_clarified');
    return 'plan_clarified';
  }

  if (
    (hasPendingPlanApproval(snapshot) || hasPendingPlanClarification(snapshot))
    && /\bcancel\b/i.test(q)
  ) {
    askPathTrace.traceEarlyReturn('detectExecutionAction', 'plan_cancelled');
    return 'plan_cancelled';
  }

  if (hasPendingPlanApproval(snapshot) && /\bedit\b/i.test(q) && !/\bapprov/i.test(q)) {
    const pending = snapshot.mission && snapshot.mission.pendingOperatorDecision;
    if (pending && pending.kind === 'plan_edit' && !/^edit$/i.test(q)) {
      askPathTrace.traceEarlyReturn('detectExecutionAction', 'plan_edited');
      return 'plan_edited';
    }
    askPathTrace.traceEarlyReturn('detectExecutionAction', 'plan_edit');
    return 'plan_edit';
  }

  if (hasPendingPlanApproval(snapshot) && /\bapprov(e|al|ed)|proceed\b/i.test(q)) {
    askPathTrace.traceEarlyReturn('detectExecutionAction', 'plan_approved');
    return 'plan_approved';
  }

  const discoveryApprovalPattern =
    /\bapprov(e|al|ed)\b.*\bbegin\b/i.test(q) ||
    /\bbegin\b.*\bdiscover/i.test(lower) ||
    /\b(?:begin|start|run|execute)\b.*\bdiscover/i.test(lower);

  if (discoveryApprovalPattern && hasPendingDiscoveryApproval(snapshot)) {
    askPathTrace.traceEarlyReturn('detectExecutionAction', 'discovery_approved');
    return 'discovery_approved';
  }

  const discoveryAlreadyExecuted = findScoutDiscoveryAfterApproval(
    snapshot.contributions || [],
    findDiscoveryApproval(snapshot.contributions || [])
  );
  if (
    discoveryAlreadyExecuted &&
    (discoveryApprovalPattern || /\bbegin\b.*\bdiscover/i.test(lower))
  ) {
    askPathTrace.traceEarlyReturn('detectExecutionAction', 'discovery_already_executed');
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

  const prioritizationApprovalPattern =
    /\bapprov(e|al|ed)\b.*\b(findings|priorit)/i.test(q) ||
    (/\b(findings|priorit)/i.test(q) && /\bapprov(e|al|ed)\b/i.test(q));

  if (prioritizationApprovalPattern && hasPendingPrioritizationApproval(snapshot)) {
    askPathTrace.traceEarlyReturn('detectExecutionAction', 'prioritization_approved');
    return 'prioritization_approved';
  }

  if (
    hasPendingPrioritizationApproval(snapshot) &&
    /\bapprov(e|al|ed)\b/i.test(q) &&
    !/\bdiscover/i.test(q)
  ) {
    askPathTrace.traceEarlyReturn('detectExecutionAction', 'prioritization_approved_pending');
    return 'prioritization_approved';
  }

  if (hasConsumablePendingDecision(snapshot)) {
    askPathTrace.traceEarlyReturn('detectExecutionAction', 'pending_operator_decision');
    return 'pending_operator_decision';
  }

  const pending = snapshot && snapshot.mission && snapshot.mission.pendingOperatorDecision;
  if (pending) {
    const err = new Error('Pending operator decision does not match executable mission state.');
    err.code = MISSION_STATE_INCONSISTENT;
    err.spec = 'SPEC-136';
    throw err;
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

function shouldExecutePlan(action, snapshot) {
  if (action !== 'plan_approved') return false;
  return hasPendingPlanApproval(snapshot);
}

function shouldClarifyPlan(action, snapshot) {
  return action === 'plan_clarified' && hasPendingPlanClarification(snapshot);
}

function shouldExecuteDiscovery(action, snapshot) {
  askPathTrace.traceEnter('shouldExecuteDiscovery', { action });
  if (action !== 'discovery_approved') {
    askPathTrace.traceEarlyReturn('shouldExecuteDiscovery', 'action_not_discovery_approved');
    return false;
  }
  if (hasPendingPlanApproval(snapshot)) {
    askPathTrace.traceEarlyReturn('shouldExecuteDiscovery', 'plan_not_approved');
    return false;
  }
  if (!hasPendingDiscoveryApproval(snapshot)) {
    askPathTrace.traceEarlyReturn('shouldExecuteDiscovery', 'no_pending_discovery_approval');
    return false;
  }
  const mission = snapshot.mission || {};
  if (!isStructuredMissionApproved(mission)) {
    askPathTrace.traceEarlyReturn('shouldExecuteDiscovery', 'plan_not_locked');
    return false;
  }
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

function shouldExecutePrioritization(action, snapshot) {
  askPathTrace.traceEnter('shouldExecutePrioritization', { action });
  if (action !== 'prioritization_approved') {
    askPathTrace.traceEarlyReturn('shouldExecutePrioritization', 'action_not_prioritization_approved');
    return false;
  }
  if (!hasPendingPrioritizationApproval(snapshot)) {
    askPathTrace.traceEarlyReturn('shouldExecutePrioritization', 'no_pending_prioritization_approval');
    return false;
  }
  const mission = snapshot.mission || {};
  if (mission.stage !== STAGES.DISCOVER) {
    askPathTrace.traceEarlyReturn('shouldExecutePrioritization', 'wrong_stage');
    return false;
  }
  const approval = findPrioritizationApproval(snapshot.contributions || []);
  if (approval && mission.stage === STAGES.UNDERSTAND) {
    askPathTrace.traceEarlyReturn('shouldExecutePrioritization', 'already_executed');
    return false;
  }
  askPathTrace.traceEarlyReturn('shouldExecutePrioritization', 'should_execute', { result: true });
  return true;
}

/**
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function maybeHandleAcquisitionMissionExecution(input = {}) {
  askPathTrace.traceEnter('maybeHandleAcquisitionMissionExecution');
  const question = String(input.question || '').trim();
  const conversationIntent = input.conversationIntent || null;
  const { mayMutateMission } = require('../operatorCognition');
  if (conversationIntent && !mayMutateMission(conversationIntent)) {
    askPathTrace.traceEarlyReturn('maybeHandleAcquisitionMissionExecution', 'cognition_read_only');
    return null;
  }
  const tenantId = resolveTenantId(input);
  const runtime = resolveAcquisitionMissionRuntime(input);
  const engine = runtime.engine();
  assertRuntimeEngine(engine, runtime);
  if (!tenantId) {
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

  const operatorIntent = input.operatorIntent || null;
  const planningTurn = operatorIntent
    ? operatorIntent.planningRequested
    : isMissionPlanningTurn(mission, question);
  const executionRequested = operatorIntent
    ? operatorIntent.executionRequested
    : isMissionExecutionCommand(question);
  if (!question || (!executionRequested && !planningTurn)) {
    askPathTrace.traceEarlyReturn('maybeHandleAcquisitionMissionExecution', 'not_execution_command');
    return null;
  }
  if (isExplicitMissionExit(question).explicit && !planningTurn) {
    askPathTrace.traceEarlyReturn('maybeHandleAcquisitionMissionExecution', 'explicit_mission_exit');
    return null;
  }

  let snapshot = engine.inspect(mission.id, { tenantId });
  assertMissionStateConsistent(snapshot.mission, {
    contributions: snapshot.contributions,
  });
  const audit = input.audit || createMissionApprovalAudit();
  const useGlobalAudit = !input.audit;

  if (isAutonomousProgressionCommand(question)) {
    const progression = await runAutonomousProgression({
      engine,
      missionId: mission.id,
      tenantId,
      operatorId: input.operatorId || (input.session && input.session.operator) || null,
      allowFixtureFallback: input.allowFixtureFallback,
      ...resolveStagePersistOpts(input),
    });
    snapshot = progression.snapshot || engine.inspect(mission.id, { tenantId });
    const prose = progression.presentation || formatMissionProgressPresentation(snapshot, progression);
    const structured = buildStructuredResponse({
      answer: prose,
      reasoning: [],
      supportingEvidence: [],
      contradictingEvidence: [],
      confidence: snapshot.mission.confidence != null ? snapshot.mission.confidence : 0.84,
      nextInvestigations: [],
      recommendedActions: progression.pause && progression.pause.availableOptions
        ? progression.pause.availableOptions
        : [],
      confidenceContributors: ['spec_147', 'autonomous_progression'],
      timelineReferences: [],
      relatedEntities: [
        { id: mission.id, type: 'acquisition_mission', name: snapshot.mission.title || mission.id },
      ],
      metadata: {
        spec: 'SPEC-147',
        outcome: progression.outcome,
        progressionStage: progression.progressionStage,
        pause: progression.pause,
        block: progression.block,
        transitions: progression.transitions,
      },
    });
    askPathTrace.traceEarlyReturn('maybeHandleAcquisitionMissionExecution', 'autonomous_progression');
    return {
      reason: 'acquisition_mission_autonomous_progression',
      structured,
      prose,
      mission: snapshot.mission || mission,
      action: 'autonomous_progression',
      executionResult: progression,
      audit,
    };
  }

  let action = detectExecutionAction(question, snapshot, operatorIntent);
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
  const persistOpts = resolveStagePersistOpts(input);

  if (shouldClarifyPlan(action, snapshot)) {
    executionResult = advancePlanClarification({
      engine,
      mission,
      tenantId,
      question,
      context: input.planningContext || input.context,
    });
    snapshot = executionResult.snapshot || engine.inspect(mission.id, { tenantId });
  } else if (action === 'plan_cancelled') {
    executionResult = cancelMissionPlan({ engine, mission, tenantId, question });
    snapshot = executionResult.snapshot || engine.inspect(mission.id, { tenantId });
  } else if (action === 'plan_edit') {
    executionResult = beginPlanEdit({ engine, mission, tenantId, question });
    snapshot = executionResult.snapshot || engine.inspect(mission.id, { tenantId });
  } else if (action === 'plan_edited') {
    executionResult = applyPlanEdits({ engine, mission, tenantId, question });
    snapshot = executionResult.snapshot || engine.inspect(mission.id, { tenantId });
  } else if (shouldExecutePlan(action, snapshot)) {
    try {
      executionResult = await advancePlanAfterApproval({
        engine,
        mission,
        tenantId,
        question,
        operatorId: input.operatorId || (input.session && input.session.operator) || null,
        ...persistOpts,
      });
    } catch (err) {
      if (!isRolledBackExecution(err)) throw err;
      executionResult = {
        rolledBack: true,
        error: err,
        snapshot: engine.inspect(mission.id, { tenantId }),
        transactionId: err.transactionId,
      };
    }
    if (executionResult && !executionResult.rolledBack && !executionResult.alreadyExecuted) {
      const chained = await maybeAutoAdvanceDiscoveryAfterPlan(
        { ...input, engine, mission, tenantId },
        executionResult
      );
      snapshot = chained.snapshot || engine.inspect(mission.id, { tenantId });
      if (chained.discoveryResult) {
        executionResult = chained.discoveryResult;
        action = chained.action;
      } else {
        executionResult = chained.planResult;
      }
    } else {
      snapshot = executionResult.snapshot || engine.inspect(mission.id, { tenantId });
    }
  } else if (shouldExecuteDiscovery(action, snapshot)) {
    try {
      executionResult = await advanceDiscoveryAfterApproval({
        engine,
        mission,
        tenantId,
        question,
        operatorId: input.operatorId || (input.session && input.session.operator) || null,
        missionEngine: input.missionEngine,
        runScout: input.runScout,
        scoutCompanies: input.scoutCompanies,
        scoutPeople: input.scoutPeople,
        allowFixtureFallback: input.allowFixtureFallback,
        audit,
        ...persistOpts,
      });
    } catch (err) {
      if (!isRolledBackExecution(err)) throw err;
      executionResult = {
        rolledBack: true,
        error: err,
        snapshot: engine.inspect(mission.id, { tenantId }),
        transactionId: err.transactionId,
      };
    }
    snapshot = executionResult.snapshot || engine.inspect(mission.id, { tenantId });
  } else if (shouldExecutePrioritization(action, snapshot)) {
    try {
      executionResult = await advancePrioritizationAfterApproval({
        engine,
        mission,
        tenantId,
        question,
        operatorId: input.operatorId || (input.session && input.session.operator) || null,
        ...persistOpts,
      });
    } catch (err) {
      if (!isRolledBackExecution(err)) throw err;
      executionResult = {
        rolledBack: true,
        error: err,
        snapshot: engine.inspect(mission.id, { tenantId }),
        transactionId: err.transactionId,
      };
    }
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
  } else if (action === 'prioritization_approved') {
    const approval = findPrioritizationApproval(snapshot.contributions || []);
    if (approval && snapshot.mission.stage === STAGES.UNDERSTAND) {
      executionResult = {
        alreadyExecuted: true,
        approval,
        snapshot,
        approvalPhase: APPROVAL_PHASES.STAGE_COMPLETED,
      };
    }
  } else if (action === 'operator_approved' && !hasConsumablePendingDecision(snapshot)) {
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
  maybeAutoAdvanceDiscoveryAfterPlan,
  shouldExecuteDiscovery,
  shouldExecutePrioritization,
  shouldExecutePlan,
  shouldClarifyPlan,
  resolveExecutionPolicy,
};
