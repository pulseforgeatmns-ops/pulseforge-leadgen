'use strict';

/**
 * SPEC-127 / SPEC-128 — Acquisition Mission execution commands from the workspace ask path.
 * Operator approval consumes pending decisions and executes the current stage once.
 */

const amo = require('../../acquisition-mission');
const { formatRollbackProse } = amo;
const {
  EXECUTION_INTENTS,
  createExecutionRequestFromChat,
  intentFromAction,
  routeExecutionRequest,
  resolveMissionRuntimeOwner,
} = amo;
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
  buildUnresolvedBoundMissionResponse,
} = require('./ActiveMissionGuard');
const {
  hasPendingDiscoveryApproval,
  hasPendingDiscoveryInvestigation,
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
  hasPendingExecutionApproval,
} = require('../../acquisition-mission/PendingOperatorDecision');
const {
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
  resolvePrioritizationPayload,
  presentationFromPrioritizationPayload,
  resolvePrioritizationApprovedNextStep,
} = require('../../acquisition-mission/PrioritizationPresentation');
const {
  resolveInvestigationContinuationPayloads,
  presentationFromInvestigationContinuation,
} = require('../../acquisition-mission/InvestigationContinuationPresentation');
const { formatProviderExecutionProse } = require('../../scout/coverage/ProviderExecution');
const {
  hasSufficientEvidenceForPrioritization,
} = require('../../acquisition-mission/DiscoveryPayload');
const {
  createMissionApprovalAudit,
  logMissionApprovalMatched,
} = require('./audit/MissionApprovalAudit');
const { formatMissionUnderstandingProse } = require('../../acquisition-mission/StructuredMission');
const { isMissionPlanningTurn } = require('./MissionPlanningTurn');
const askPathTrace = require('./audit/AskPathTrace');

const { STAGES, STAGE_LABELS, SPECIALISTS, CONTRIBUTION_KINDS } = amo;

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

  const current = (snapshot && snapshot.mission) || mission;
  const routed = await routeExecutionRequest(
    createExecutionRequestFromChat({
      intent: EXECUTION_INTENTS.APPROVE_DISCOVERY,
      missionId: current.id,
      mission: current,
      operatorId: input.operatorId || (input.session && input.session.operator) || AUTONOMOUS_OPERATOR_ID,
      stage: current.stage,
      executionMode: executionPolicy,
      objective: current.objective,
      runtimeOwner: resolveMissionRuntimeOwner(current),
      question: 'Autonomous execution policy. Discovery approval auto-consumed.',
      metadata: { autoConsumed: true },
    }),
    {
      engine,
      tenantId,
      runScout: input.runScout,
      scoutCompanies: input.scoutCompanies,
      scoutPeople: input.scoutPeople,
      allowFixtureFallback: input.allowFixtureFallback,
      audit: input.audit,
      executionPolicy,
      ...resolveStagePersistOpts(input),
    }
  );
  snapshot = routed.snapshot || engine.inspect(mission.id, { tenantId });
  return {
    planResult,
    snapshot,
    discoveryResult: routed.executionResult,
    action: routed.action || 'discovery_approved',
    autoConsumedDiscoveryApproval: true,
    executionRequest: routed.request,
  };
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

function resolveChatOperatorId(input = {}) {
  return input.operatorId
    || (input.session && (input.session.operator || (input.session.user && input.session.user.id)))
    || 'operator';
}

async function submitChatExecutionRequest(input, {
  intent,
  mission,
  tenantId,
  engine,
  question,
  audit,
}) {
  const executionPolicy = resolveExecutionPolicy(input);
  const request = createExecutionRequestFromChat({
    intent,
    missionId: mission.id,
    mission,
    operatorId: resolveChatOperatorId(input),
    stage: mission.stage,
    executionMode: executionPolicy,
    objective: mission.objective,
    runtimeOwner: resolveMissionRuntimeOwner(mission),
    question,
    permissions: { canExecute: true },
  });
  return routeExecutionRequest(request, {
    engine,
    tenantId,
    question,
    operatorId: request.operatorId,
    runScout: input.runScout,
    scoutCompanies: input.scoutCompanies,
    scoutPeople: input.scoutPeople,
    allowFixtureFallback: input.allowFixtureFallback,
    audit,
    executionPolicy,
    planningContext: input.planningContext || input.context,
    context: input.planningContext || input.context,
    ...resolveStagePersistOpts(input),
  });
}

function stageLabel(stage) {
  return STAGE_LABELS[stage] || stage || 'Active';
}

function ensureReadyExecutionReview(snapshot, mission) {
  const review = snapshot && snapshot.executionReview
    ? snapshot.executionReview
    : (mission && mission.executionReview) || null;
  return review || null;
}

function summarizeReadyExecutionTargets(review) {
  const targets = Array.isArray(review && review.targets) ? review.targets : [];
  if (!targets.length) {
    return '• No targets prepared.';
  }
  return targets
    .slice(0, 5)
    .map((target, index) => {
      const label = target.company || target.name || `Target ${index + 1}`;
      const reason = target.priorityReason || target.reason || target.fit || null;
      return `• ${label}${reason ? ` — ${reason}` : ''}`;
    })
    .join('\n');
}

function summarizeReadyExecutionMessage(review) {
  const communication = review && review.communication ? review.communication : {};
  const lines = [];
  if (communication.subject) lines.push(`Subject: ${communication.subject}`);
  if (communication.body) lines.push(`Body: ${communication.body}`);
  if (communication.cta) lines.push(`CTA: ${communication.cta}`);
  if (!lines.length) lines.push('No prepared message found.');
  return lines.join('\n');
}

function summarizeReadyExecutionQueue(review) {
  const infrastructure = review && review.infrastructure ? review.infrastructure : {};
  const queue = Array.isArray(infrastructure.queue) ? infrastructure.queue : [];
  const sendCount = Number(review && review.decision && review.decision.plannedSendCount != null
    ? review.decision.plannedSendCount
    : queue.length);
  const safeCapacity = infrastructure.safeCapacity != null
    ? infrastructure.safeCapacity
    : Math.max(0, queue.length);
  const senderIdentity = review && review.artifactBinding && review.artifactBinding.senderEmail
    ? review.artifactBinding.senderEmail
    : ((review && review.infrastructure) ? (review.infrastructure.senderIdentity || null) : null);
  return [
    `Planned sends: ${sendCount}`,
    `Safe capacity: ${safeCapacity}`,
    senderIdentity ? `Sender: ${senderIdentity}` : 'Sender: unresolved',
  ].join('\n');
}

function summarizeReadyExecutionSafety(review) {
  const infrastructure = review && review.infrastructure ? review.infrastructure : {};
  const blockers = Array.isArray(review && review.decision && review.decision.blockers)
    ? review.decision.blockers
    : [];
  const lineParts = [
    `Delivery: ${infrastructure.deliverabilityStatus || 'unknown'}`,
    `Governor: ${infrastructure.governorOutcome || 'unknown'}`,
  ];
  if (blockers.length) {
    lineParts.push(`Blockers: ${blockers.join('; ')}`);
  }
  return lineParts.join('\n');
}

function readyExecutionApprovalPrompt(review) {
  const blockers = Array.isArray(review && review.decision && review.decision.blockers)
    ? review.decision.blockers
    : [];
  if (blockers.length) {
    return `Resolve execution blocker to continue: ${blockers.join('; ')}`;
  }
  return 'Authorize external execution of this prepared outreach?';
}

function buildReadyExecutionPresentation({ mission, snapshot }) {
  const review = ensureReadyExecutionReview(snapshot, mission);
  if (!review) {
    return null;
  }

  const pending = mission && mission.pendingOperatorDecision ? mission.pendingOperatorDecision : {};
  const blockers = Array.isArray(review && review.decision && review.decision.blockers)
    ? review.decision.blockers
    : [];
  const artifactBinding = review.artifactBinding || {};
  const channel = review.communication && review.communication.channel
    ? review.communication.channel
    : (review.infrastructure && review.infrastructure.channel)
      ? review.infrastructure.channel
      : 'Email';

  const currentUnderstanding = [
    { label: `Prepared targets\n${summarizeReadyExecutionTargets(review)}`, done: !blockers.length },
    { label: `Prepared message\n${summarizeReadyExecutionMessage(review)}`, done: !blockers.length },
    { label: `Channel\n• ${channel}`, done: !blockers.length },
    { label: `Send/capacity summary\n${summarizeReadyExecutionQueue(review)}`, done: !blockers.length },
    { label: `Delivery/governor state\n${summarizeReadyExecutionSafety(review)}`, done: !blockers.length },
  ];

  const status = blockers.length ? 'Prepared but blocked' : 'Prepared — not sent';
  const headline = blockers.length ? 'Execution Blocked' : 'Execution Ready';
  const waitingOn = blockers.length ? 'Execution blocker resolution' : 'Execution approval';
  const nextStep = [
    'Prepared artifacts bound to the canonical revision:',
    `• Max: ${artifactBinding.maxContributionId || 'unknown'}`,
    `• Paige: ${artifactBinding.paigeContributionId || 'unknown'}`,
    `• Emmett: ${artifactBinding.emmettContributionId || 'unknown'}`,
    blockers.length
      ? `Resolve the blocker and return to the mission workspace.`
      : 'Authorize execution to produce external sends from this exact prepared bundle.',
  ].join('\n');

  const comm = buildMissionCommunication({
    headline,
    mission: mission && mission.title ? mission.title : (mission && mission.id) || 'Acquisition Mission',
    objective: mission && mission.objective ? mission.objective : null,
    status,
    stage: 'Ready',
    progress: mission && mission.progressPercent != null ? mission.progressPercent : null,
    health: snapshot && snapshot.health && snapshot.health.label ? snapshot.health.label : 'Healthy',
    waitingOn,
    confidence: mission && mission.confidence != null ? mission.confidence : null,
    currentUnderstanding,
    nextStep,
    operatorDecision: readyExecutionApprovalPrompt(review),
    evidenceStatus: blockers.length ? 'Canonical execution review indicates a blocker.' : 'Canonical execution review is prepared for approval.',
    sources: ['execution_review', 'acquisition_mission'],
    includeReasoningMarker: false,
  });

  return { comm, prose: formatMissionProse(comm) };
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
    const stageName = action === 'plan_approved'
      ? 'Mission plan'
      : action === 'prioritization_approved'
        ? 'Prioritization'
        : 'Discovery';
    const providerDiagnostics =
      (err.details && Array.isArray(err.details.providerExecution) && err.details.providerExecution.length)
        ? formatProviderExecutionProse(err.details.providerExecution)
        : null;
    const evidenceStatus = providerDiagnostics
      ? `${err.message || err.rollbackReason || formatRollbackProse(stageName)}\n\n${providerDiagnostics}`
      : err.rollbackReason || err.message || formatRollbackProse(stageName);
    const comm = buildMissionCommunication({
      headline: `${stageName} could not execute`,
      mission: mission.title || mission.id,
      objective: mission.objective,
      status: 'Unchanged',
      stage: stageName,
      progress,
      waitingOn: 'Resolve the blocker',
      nextStep: 'Resolve the blocker and retry.',
      operatorDecision: action === 'plan_approved'
        ? 'Approve mission plan?'
        : action === 'prioritization_approved'
          ? 'Approve prioritization?'
          : 'Approve discovery?',
      evidenceStatus,
      sources: ['acquisition_mission', 'tme'],
      includeReasoningMarker: false,
    });
    const prose = providerDiagnostics
      ? `${formatRollbackProse(stageName)}\n\n${providerDiagnostics}`
      : formatRollbackProse(stageName);
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
    const sufficientEvidence = hasSufficientEvidenceForPrioritization(discoveryResults);
    const comm = buildMissionCommunication({
      headline: 'Mission Updated',
      mission: mission.title || mission.id,
      objective: mission.objective,
      status: blocked ? 'Discovery Blocked' : 'Discovery Complete',
      stage: 'Discovery',
      progress,
      health: snapshot.health && snapshot.health.label ? snapshot.health.label : 'Healthy',
      waitingOn: blocked
        ? 'Discovery blocker'
        : sufficientEvidence
          ? 'Prioritization approval'
          : 'Evidence review',
      confidence:
        discoveryResults.confidence != null
          ? discoveryResults.confidence
          : mission.confidence,
      confidenceBreakdown: discoveryResults.confidenceBreakdown,
      nextStep: blocked
        ? 'Resolve the discovery blocker, then retry Discovery.'
        : sufficientEvidence
          ? 'Review discovered prospects and approve prioritization to continue.'
          : 'Review discovery evidence. Scout must surface attributable signals before prioritization.',
      operatorDecision: blocked
        ? 'Retry discovery?'
        : sufficientEvidence
          ? 'Approve prioritization?'
          : 'Request more discovery evidence?',
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
    const prioritizationPayload = resolvePrioritizationPayload({ executionResult, snapshot });
    const prioritizationResults = presentationFromPrioritizationPayload(prioritizationPayload);
    const nextStep = resolvePrioritizationApprovedNextStep(snapshot, mission);
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
        prioritizationResults.confidence != null
          ? prioritizationResults.confidence
          : mission.confidence,
      nextStep,
      operatorDecision: null,
      prioritizationResults,
      evidenceStatus: 'Prioritization committed',
      sources: ['acquisition_mission', 'max'],
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

  if (executionResult && action === 'discovery_investigation_continued') {
    const { priorPayload, currentPayload } = resolveInvestigationContinuationPayloads({
      snapshot,
      executionResult,
    });
    const investigationResults = presentationFromInvestigationContinuation({
      priorPayload,
      currentPayload,
      mission,
      executionResult,
    });
    const blocked = executionResult.executionOutcome === 'blocked';
    const pending = mission.pendingOperatorDecision || {};
    const comm = buildMissionCommunication({
      headline: 'Investigation Continued',
      mission: mission.title || mission.id,
      objective: mission.objective,
      status: blocked ? 'Investigation Blocked' : 'Investigation Updated',
      stage: 'Discovery',
      progress,
      health: snapshot.health && snapshot.health.label ? snapshot.health.label : 'Healthy',
      waitingOn: pending.waitingOn || pending.blocker || null,
      nextStep: null,
      operatorDecision: investigationResults.operatorDecision,
      investigationContinuationResults: investigationResults,
      evidenceStatus: 'Committed Scout investigation delta',
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
        confidence:
          investigationResults.confidenceAfter != null
            ? investigationResults.confidenceAfter
            : mission.confidence != null
              ? mission.confidence
              : 0.84,
        nextInvestigations: [],
        recommendedActions: [
          buildOpenMissionAction({
            missionId: mission.id,
            runtime: mission.runtime || MISSION_RUNTIMES.AMO,
            label: 'Open mission workspace',
          }),
        ],
        confidenceContributors: ['spec_203', 'acquisition_mission'],
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

  const readyReview = ensureReadyExecutionReview(snapshot, mission);
  const executionApprovalPending =
    (mission && mission.pendingOperatorDecision && mission.pendingOperatorDecision.kind === 'execution_approval')
    || hasPendingExecutionApproval(snapshot);
  if ((stage === STAGES.READY || executionApprovalPending) && readyReview) {
    const readyPresentation = buildReadyExecutionPresentation({ mission, snapshot });
    if (readyPresentation) {
      const structured = applyMissionCommunication(
        buildStructuredResponse({
          answer: readyPresentation.prose,
          reasoning: [],
          supportingEvidence: [],
          contradictingEvidence: [],
          confidence: mission.confidence != null ? mission.confidence : 0.84,
          nextInvestigations: [],
          recommendedActions: [],
          confidenceContributors: ['spec_210', 'execution_review'],
          timelineReferences: [],
          relatedEntities: [{ id: mission.id, type: 'acquisition_mission', name: mission.title || mission.id }],
          metadata: buildExecutionMetadata(mission, action, executionResult),
        }),
        readyPresentation.comm
      );
      return { structured, prose: readyPresentation.prose, comm: readyPresentation.comm, action };
    }
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
  const resolution = operatorIntent && operatorIntent.pendingDecisionResolution;
  if (resolution && resolution.resolvedFromPendingDecision && resolution.executionAction) {
    askPathTrace.traceEarlyReturn('detectExecutionAction', 'pending_decision_resolution');
    return resolution.executionAction;
  }

  const missionContinuation =
    operatorIntent &&
    operatorIntent.conversationIntent &&
    operatorIntent.conversationIntent.missionContinuation;
  if (
    operatorIntent &&
    operatorIntent.conversationIntent &&
    operatorIntent.conversationIntent.via === 'mission_continuation' &&
    missionContinuation &&
    missionContinuation.action
  ) {
    askPathTrace.traceEarlyReturn('detectExecutionAction', 'mission_continuation');
    return missionContinuation.action;
  }

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
    /\bapprov(e|al|ed)\b.*\bpriorit/i.test(q) ||
    (/\bpriorit/i.test(q) && /\bapprov(e|al|ed)\b/i.test(q));

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

  const investigationContinuationPattern =
    /\b(?:continue|retry|resume|proceed)\b.*\binvestig/i.test(q) ||
    (/\binvestig/i.test(q) && /\b(?:continue|retry|resume|proceed)\b/i.test(q));

  if (investigationContinuationPattern && hasPendingDiscoveryInvestigation(snapshot)) {
    askPathTrace.traceEarlyReturn('detectExecutionAction', 'discovery_investigation_continued');
    return 'discovery_investigation_continued';
  }

  if (
    hasPendingDiscoveryInvestigation(snapshot) &&
    (/\b(?:continue|retry|resume|proceed)\b/i.test(q) || /\binvestig/i.test(q))
  ) {
    askPathTrace.traceEarlyReturn('detectExecutionAction', 'discovery_investigation_continued_pending');
    return 'discovery_investigation_continued';
  }

  if (hasConsumablePendingDecision(snapshot)) {
    if (
      hasPendingExecutionApproval(snapshot) &&
      (/\bapprov(e|al|ed)\b/i.test(q) || /\bauthoriz(e|ed|ation)\b/i.test(q) || /\bexecute\b/i.test(q))
    ) {
      askPathTrace.traceEarlyReturn('detectExecutionAction', 'execution_approved');
      return 'execution_approved';
    }
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
    if (hasPendingDiscoveryInvestigation(snapshot)) {
      askPathTrace.traceEarlyReturn('detectExecutionAction', 'discovery_investigation_continued_continue');
      return 'discovery_investigation_continued';
    }
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
  const operatorIntent = input.operatorIntent || null;

  const tenantId = resolveTenantId(input);
  let amoResolution = { mission: null, unresolvedBoundMissionId: null };
  if (tenantId) {
    amoResolution = await resolveAcquisitionActiveMission(input);
    if (amoResolution.unresolvedBoundMissionId) {
      askPathTrace.traceEarlyReturn(
        'maybeHandleAcquisitionMissionExecution',
        'unresolved_bound_mission_context'
      );
      return buildUnresolvedBoundMissionResponse(amoResolution.unresolvedBoundMissionId, input);
    }
  }

  const pendingDecisionResolution =
    operatorIntent && operatorIntent.pendingDecisionResolution;
  const pendingDecisionExecutable = Boolean(
    pendingDecisionResolution &&
      pendingDecisionResolution.resolvedFromPendingDecision &&
      pendingDecisionResolution.executionIntent
  );
  const { mayMutateMission } = require('../operatorCognition');
  if (
    conversationIntent &&
    !mayMutateMission(conversationIntent) &&
    !pendingDecisionExecutable
  ) {
    askPathTrace.traceEarlyReturn('maybeHandleAcquisitionMissionExecution', 'cognition_read_only');
    return null;
  }
  const runtime = resolveAcquisitionMissionRuntime(input);
  const engine = runtime.engine();
  assertRuntimeEngine(engine, runtime);
  if (!tenantId) {
    askPathTrace.traceEarlyReturn('maybeHandleAcquisitionMissionExecution', 'no_engine_or_tenant');
    return null;
  }

  const amoResolutionAfterGate = tenantId
    ? amoResolution
    : await resolveAcquisitionActiveMission(input);
  const mission =
    amoResolutionAfterGate.mission ||
    (() => {
      const missions = engine.list(tenantId);
      const missionId = resolveMissionId(input, missions);
      return missionId ? missions.find((row) => row && row.id === missionId) : null;
    })();

  if (!mission || mission.stage === STAGES.IMPROVE) {
    askPathTrace.traceEarlyReturn('maybeHandleAcquisitionMissionExecution', 'no_mission_or_improve');
    return null;
  }

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
    const routed = await submitChatExecutionRequest(input, {
      intent: EXECUTION_INTENTS.AUTONOMOUS_PROGRESSION,
      mission,
      tenantId,
      engine,
      question,
      audit,
    });
    const progression = routed.executionResult;
    snapshot = routed.snapshot || engine.inspect(mission.id, { tenantId });
    const prose = (progression && progression.presentation) || formatMissionProgressPresentation(snapshot, progression);
    const structured = buildStructuredResponse({
      answer: prose,
      reasoning: [],
      supportingEvidence: [],
      contradictingEvidence: [],
      confidence: snapshot.mission.confidence != null ? snapshot.mission.confidence : 0.84,
      nextInvestigations: [],
      recommendedActions: progression && progression.pause && progression.pause.availableOptions
        ? progression.pause.availableOptions
        : [],
      confidenceContributors: ['spec_147', 'autonomous_progression', 'spec_171'],
      timelineReferences: [],
      relatedEntities: [
        { id: mission.id, type: 'acquisition_mission', name: snapshot.mission.title || mission.id },
      ],
      metadata: {
        spec: 'SPEC-171',
        executionRequestId: routed.request && routed.request.id,
        outcome: progression && progression.outcome,
        progressionStage: progression && progression.progressionStage,
        pause: progression && progression.pause,
        block: progression && progression.block,
        transitions: progression && progression.transitions,
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
      executionRequest: routed.request,
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
  let executionRequest = null;
  const intent = intentFromAction(action);

  if (intent) {
    const routed = await submitChatExecutionRequest(input, {
      intent,
      mission,
      tenantId,
      engine,
      question,
      audit,
    });
    executionRequest = routed.request;
    executionResult = routed.executionResult;
    action = routed.action || action;
    snapshot = routed.snapshot || engine.inspect(mission.id, { tenantId });
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
    input.session.context.acquisitionOwner = 'AMO';
  }

  askPathTrace.traceEarlyReturn('maybeHandleAcquisitionMissionExecution', `acquisition_mission_${action}`, {
    action,
    missionId: mission.id,
    executionRequestId: executionRequest && executionRequest.id,
  });
  return {
    reason: `acquisition_mission_${action}`,
    structured: response.structured,
    prose: response.prose,
    mission: snapshot.mission || mission,
    action,
    executionResult,
    executionRequest,
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
  // SPEC-211 — Export execution review formatting helpers for pending decision clarification
  ensureReadyExecutionReview,
  summarizeReadyExecutionTargets,
  summarizeReadyExecutionMessage,
  summarizeReadyExecutionQueue,
  summarizeReadyExecutionSafety,
  readyExecutionApprovalPrompt,
};
