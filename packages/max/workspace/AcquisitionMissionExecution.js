'use strict';

/**
 * SPEC-127 / SPEC-128 — Acquisition Mission execution commands from the workspace ask path.
 * Operator approval consumes pending decisions and executes the current stage once.
 */

const amo = require('../../acquisition-mission');
const { formatRollbackProse } = amo;
const { resolveExecutionBlocker } = require('./ExecutionBlockerPresentation');
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
const { resolvePendingOperatorDecision, authorizationPolarityVerdict, mismatchedActionTargets } = require('./PendingDecisionResolver');
const { splitClauses, isVerbNegatedInClause } = require('./MissionLifecycleIntent');
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
const {
  hasExplicitMissionApprovalLanguage,
  isOperationalStatusQuery,
} = require('./OperatorMissionTurnIntent');

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

function operatorMessageIncludesApproval(question) {
  const clauses = splitClauses(String(question || '').replace(/’/g, "'"));
  if (clauses.some((clause) => /\b(?:if|unless|provided|once|after|when|until|assuming|subject to|maybe|perhaps|possibly)\b/i.test(clause))) return false;
  return clauses.some((clause) =>
    /\bapprov(e|al|ed)\b/i.test(clause) &&
    !clause.includes('?') && !isVerbNegatedInClause(clause, 'approve')
  );
}

function operatorMessageRequestsDiscovery(question) {
  const clauses = splitClauses(String(question || '').replace(/’/g, "'")).flatMap((clause) => clause.split(/;\s*/));
  if (operatorMessageProhibitsDiscovery(question)) return false;
  return clauses.some((clause) =>
    /\b(?:scout|discovery|discover)\b/i.test(clause) &&
    /\b(?:begin|start|run|execute|proceed|continue|approve|approved)\b/i.test(clause)
  );
}

function operatorMessageProhibitsDiscovery(question) {
  return splitClauses(String(question || '').replace(/’/g, "'")).some((clause) =>
    /\b(?:scout|discovery|discover)\b/i.test(clause) &&
    (isVerbNegatedInClause(clause, ['begin', 'start', 'run', 'execute', 'proceed', 'continue', 'approve', 'launch']) ||
      /\b(?:hold off|not yet|wait|pause)\b/i.test(clause))
  );
}

/**
 * Compound planning turns: region clarification + plan approval (+ optional discovery) in one message.
 */
async function maybeChainPlanningAfterClarification(input, state = {}) {
  let {
    action,
    executionResult,
    snapshot,
    mission,
    tenantId,
    engine,
    audit,
    question,
    executionRequest,
  } = state;

  if (action !== 'plan_clarified' || !executionResult || executionResult.matched === false) {
    return state;
  }
  if (!operatorMessageIncludesApproval(question)) {
    return state;
  }

  snapshot = executionResult.snapshot || engine.inspect(mission.id, { tenantId });
  if (!hasPendingPlanApproval(snapshot)) {
    return state;
  }

  const planRouted = await submitChatExecutionRequest(input, {
    intent: EXECUTION_INTENTS.APPROVE_PLAN,
    mission: snapshot.mission || mission,
    tenantId,
    engine,
    question,
    audit,
  });
  action = planRouted.action || 'plan_approved';
  executionResult = planRouted.executionResult;
  executionRequest = planRouted.request;
  snapshot = planRouted.snapshot || engine.inspect(mission.id, { tenantId });

  const autoAdvanced = operatorMessageProhibitsDiscovery(question) ? {} : await maybeAutoAdvanceDiscoveryAfterPlan(input, {
    snapshot,
    structuredMission: executionResult && executionResult.structuredMission,
    rolledBack: executionResult && executionResult.rolledBack,
  });
  if (autoAdvanced.discoveryResult) {
    return {
      action: autoAdvanced.action || 'discovery_approved',
      executionResult: autoAdvanced.discoveryResult,
      snapshot: autoAdvanced.snapshot || snapshot,
      executionRequest: autoAdvanced.executionRequest || executionRequest,
    };
  }

  if (
    operatorMessageRequestsDiscovery(question) &&
    shouldExecuteDiscovery('discovery_approved', snapshot)
  ) {
    const discoveryRouted = await submitChatExecutionRequest(input, {
      intent: EXECUTION_INTENTS.APPROVE_DISCOVERY,
      mission: snapshot.mission || mission,
      tenantId,
      engine,
      question,
      audit,
    });
    return {
      action: discoveryRouted.action || 'discovery_approved',
      executionResult: discoveryRouted.executionResult,
      snapshot: discoveryRouted.snapshot || engine.inspect(mission.id, { tenantId }),
      executionRequest: discoveryRouted.request,
    };
  }

  return { action, executionResult, snapshot, executionRequest };
}

/**
 * P0 — the prepared-package revision last shown to this session for a mission.
 * Recorded by WorkspaceEngine after each turn (recordPresentedExecutionDecision).
 */
function presentedExecutionRevision(session, missionId) {
  const presented = session && session.context && session.context.presentedExecutionDecision;
  if (!presented || presented.missionId !== missionId) return null;
  return presented.revision || null;
}

function presentedExecutionContentDigest(session, missionId) {
  const presented = session && session.context && session.context.presentedExecutionDecision;
  if (!presented || presented.missionId !== missionId) return null;
  return presented.contentDigest || null;
}

/**
 * Record which execution-approval package (if any) a turn displayed for the
 * session's bound mission. Only a mission turn that rendered the pending
 * execution decision counts as "shown"; anything else clears the record.
 */
function recordPresentedExecutionDecision(session, result) {
  if (!session || !session.context || typeof session.context !== 'object') return;
  const boundMissionId = session.context.acquisitionMissionId || session.context.missionId || null;
  const mission = result && result.mission;
  const metadata = (result && result.structured && result.structured.metadata) || {};
  const missionTurn = metadata.acquisitionMission === true || metadata.pendingDecisionTurnOwnership === true;
  const pending = mission && mission.pendingOperatorDecision;
  // "Shown" means the turn rendered the package itself, not just the prompt.
  const renderedPackage = /(?:^|\n)\s*(?:## )?Execution Ready\b/.test(String((result && result.prose) || ''));
  if (
    missionTurn && renderedPackage &&
    mission && boundMissionId && mission.id === boundMissionId &&
    pending && pending.kind === 'execution_approval' && pending.preparedArtifactRevision
  ) {
    session.context.presentedExecutionDecision = {
      missionId: mission.id,
      revision: pending.preparedArtifactRevision,
      contentDigest: pending.preparedContentDigest || null,
    };
    return;
  }
  if (missionTurn || !boundMissionId) session.context.presentedExecutionDecision = null;
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
  // P0 — an execution approval binds to the package revision this session displayed.
  const presented = intent === EXECUTION_INTENTS.APPROVE_EXECUTION
    ? presentedExecutionRevision(input.session, mission.id)
    : null;
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
    ...(intent === EXECUTION_INTENTS.APPROVE_EXECUTION
      ? { payload: {
        expectedPreparedArtifactRevision: presented,
        expectedPreparedContentDigest: presentedExecutionContentDigest(input.session, mission.id),
      } }
      : {}),
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

const EXECUTION_REVIEW_REFUSALS = new Set([
  'tme_execution_approval_unreviewed',
  'tme_execution_approval_stale',
]);

/** Shared "## Execution Ready" package block (same shape as the SPEC-211 clarify). */
function formatExecutionPackageBlock(review) {
  return [
    '## Execution Ready',
    '',
    '**Targets**',
    summarizeReadyExecutionTargets(review),
    '',
    '**Channel**',
    '• Email',
    '',
    '**Message**',
    summarizeReadyExecutionMessage(review),
    '',
    '**Outbound Plan**',
    summarizeReadyExecutionQueue(review),
    '',
    '**Safety / Delivery**',
    summarizeReadyExecutionSafety(review),
  ].join('\n');
}

function buildExecutionReviewRefusal({ err, mission, snapshot }) {
  const current = (snapshot && snapshot.mission) || mission || {};
  const review = ensureReadyExecutionReview(snapshot, current);
  const prose = [
    `Not executed: ${err.message} Nothing was approved or sent.`,
    review ? formatExecutionPackageBlock(review) : null,
    readyExecutionApprovalPrompt(review),
  ].filter(Boolean).join('\n\n');
  const structured = buildStructuredResponse({
    answer: prose,
    reasoning: [],
    supportingEvidence: [],
    contradictingEvidence: [],
    confidence: 1,
    nextInvestigations: [],
    recommendedActions: [],
    confidenceContributors: ['p0_execution_approval_binding'],
    timelineReferences: [],
    relatedEntities: [{ id: current.id, type: 'acquisition_mission', name: current.objective || current.id }],
    metadata: { acquisitionMission: true, strictOutputShape: true },
  });
  Object.assign(structured.metadata, {
    executionState: 'blocked',
    executionVerified: false,
    blocker: err.code,
  });
  return { structured, prose, executionReviewRefusal: err.code };
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
    // P0 — an execution approval refused for review/currency re-shows the
    // current package and says plainly that nothing was approved or sent.
    if (action === 'execution_approved' && EXECUTION_REVIEW_REFUSALS.has(err.code)) {
      return buildExecutionReviewRefusal({ err, mission, snapshot });
    }
    const stageName = action === 'plan_approved'
      ? 'Mission plan'
      : action === 'prioritization_approved'
        ? 'Prioritization'
        : action === 'execution_approved'
          ? 'Execution approval'
          : 'Discovery';
    const providerDiagnostics =
      (err.details && Array.isArray(err.details.providerExecution) && err.details.providerExecution.length)
        ? formatProviderExecutionProse(err.details.providerExecution)
        : null;
    const blocker = resolveExecutionBlocker({
      error: err,
      rollbackReason: executionResult.rollbackReason,
      executionResult,
      stageName,
    });
    const evidenceStatus = providerDiagnostics
      ? `${blocker.message}\n\n${providerDiagnostics}`
      : blocker.message;
    const comm = buildMissionCommunication({
      headline: `${stageName} could not execute`,
      mission: mission.title || mission.id,
      objective: mission.objective,
      status: 'Unchanged',
      stage: stageName,
      progress,
      waitingOn: blocker.waitingOn,
      nextStep: blocker.nextStep,
      operatorDecision: action === 'plan_approved'
        ? 'Approve mission plan?'
        : action === 'prioritization_approved'
          ? 'Approve prioritization?'
          : action === 'execution_approved'
            ? 'Authorize external execution of prepared outreach?'
            : 'Approve discovery?',
      evidenceStatus,
      sources: ['acquisition_mission', 'tme'],
      includeReasoningMarker: false,
    });
    const recoveryProse = formatRollbackProse(stageName);
    const prose = [
      recoveryProse,
      blocker.message !== recoveryProse ? blocker.message : null,
      providerDiagnostics,
    ].filter(Boolean).join('\n\n');
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
    const discoveryBlocker = blocked
      ? resolveExecutionBlocker({ scoutPayload, stageName: 'Discovery' })
      : null;
    const comm = buildMissionCommunication({
      headline: 'Mission Updated',
      mission: mission.title || mission.id,
      objective: mission.objective,
      status: blocked ? 'Discovery Blocked' : 'Discovery Complete',
      stage: 'Discovery',
      progress,
      health: snapshot.health && snapshot.health.label ? snapshot.health.label : 'Healthy',
      waitingOn: blocked
        ? (discoveryBlocker && discoveryBlocker.waitingOn) || 'Discovery blocker'
        : sufficientEvidence
          ? 'Prioritization approval'
          : 'Evidence review',
      confidence:
        discoveryResults.confidence != null
          ? discoveryResults.confidence
          : mission.confidence,
      confidenceBreakdown: discoveryResults.confidenceBreakdown,
      nextStep: blocked
        ? (discoveryBlocker && discoveryBlocker.nextStep) || 'Resolve the discovery blocker, then retry Discovery.'
        : sufficientEvidence
          ? 'Review discovered prospects and approve prioritization to continue.'
          : 'Review discovery evidence. Scout must surface attributable signals before prioritization.',
      operatorDecision: blocked
        ? 'Retry discovery?'
        : sufficientEvidence
          ? 'Approve prioritization?'
          : 'Request more discovery evidence?',
      discoveryResults: executionResult.discovery ? discoveryResults : null,
      evidenceStatus: blocked
        ? (discoveryBlocker && discoveryBlocker.message) || scoutPayload.summary || 'Discovery blocked.'
        : scoutPayload.summary || 'Discovery complete.',
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
  const qGuard = String(question || '').trim();
  if (isOperationalStatusQuery(qGuard) && !hasExplicitMissionApprovalLanguage(qGuard)) {
    askPathTrace.traceEarlyReturn('detectExecutionAction', 'status_query');
    return null;
  }
  const resolution = operatorIntent && operatorIntent.pendingDecisionResolution;
  // P0 authorization polarity — a prohibition ("Don't proceed.", "Do not send
  // it", "Don't cancel the mission") never dispatches an approval, continuation,
  // send, or cancellation, whatever raw verbs the fallbacks below would match.
  // Same verdict as PendingDecisionResolver: only `held`/`mixed` are blocked; an
  // explicit cancellation, an explicit revision, or an approval whose
  // prohibitions name a different (later) action keep their normal path.
  const pendingKind = snapshot && snapshot.mission && snapshot.mission.pendingOperatorDecision
    ? snapshot.mission.pendingOperatorDecision.kind
    : null;
  const polarityVerdict = authorizationPolarityVerdict(qGuard, pendingKind).verdict;
  if (polarityVerdict === 'held' || polarityVerdict === 'mixed') {
    askPathTrace.traceEarlyReturn('detectExecutionAction', `negated_authorization_${polarityVerdict}`);
    return null;
  }
  const wrongTargets = mismatchedActionTargets(pendingKind, qGuard);
  const historicalDiscovery = wrongTargets?.every(target => target === 'discovery') &&
    findScoutDiscoveryAfterApproval(snapshot.contributions || [], findDiscoveryApproval(snapshot.contributions || []));
  if (polarityVerdict !== 'cancel' && polarityVerdict !== 'revision' && wrongTargets && !historicalDiscovery) return null;
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

  if (
    hasPendingPlanApproval(snapshot) &&
    (hasExplicitMissionApprovalLanguage(q) || /\bproceed\b/i.test(q))
  ) {
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
    hasExplicitMissionApprovalLanguage(q)
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
    hasExplicitMissionApprovalLanguage(q) &&
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
      (hasExplicitMissionApprovalLanguage(q) ||
        /\bauthoriz(e|ed|ation)\b/i.test(q) ||
        /\bexecute\b/i.test(q))
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

  if (hasExplicitMissionApprovalLanguage(q)) {
    askPathTrace.traceEarlyReturn('detectExecutionAction', 'operator_approved');
    return 'operator_approved';
  }
  if (
    /^(?:continue|proceed|resume|next)\.?$/i.test(q) ||
    /\bcontinue\s+with\s+this\s+mission\b/i.test(q)
  ) {
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
  askPathTrace.traceEarlyReturn('detectExecutionAction', 'no_explicit_execution_action');
  return null;
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

  if (isOperationalStatusQuery(question) && !hasExplicitMissionApprovalLanguage(question)) {
    askPathTrace.traceEarlyReturn('maybeHandleAcquisitionMissionExecution', 'status_query_read_only');
    return null;
  }

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
  const governedDecision = resolvePendingOperatorDecision(question, mission, { tenantId });
  if (isExplicitMissionExit(question).explicit && !planningTurn &&
      !(governedDecision.resolvedFromPendingDecision && governedDecision.action === 'cancel')) {
    askPathTrace.traceEarlyReturn('maybeHandleAcquisitionMissionExecution', 'explicit_mission_exit');
    return null;
  }

  let snapshot = engine.inspect(mission.id, { tenantId });
  assertMissionStateConsistent(snapshot.mission, {
    contributions: snapshot.contributions,
  });
  const clarificationResolution = operatorIntent
    ? pendingDecisionResolution
    : resolvePendingOperatorDecision(question, snapshot.mission, { tenantId });
  if (
    hasPendingPlanClarification(snapshot) &&
    clarificationResolution && clarificationResolution.captureGuarded
  ) {
    askPathTrace.traceEarlyReturn('maybeHandleAcquisitionMissionExecution', 'clarification_answer_guarded');
    return null;
  }
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
  if (!action) {
    askPathTrace.traceEarlyReturn('maybeHandleAcquisitionMissionExecution', 'no_execution_action_detected');
    return null;
  }
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
  let execution = null;
  const intent = intentFromAction(action);

  if (intent) {
    let routed;
    try {
      routed = await submitChatExecutionRequest(input, {
        intent,
        mission,
        tenantId,
        engine,
        question,
        audit,
      });
    } catch (err) {
      if (amo.isSyntheticEvidenceError(err)) return buildSyntheticBlockedTurn({ mission, action, intent });
      if (!amo.isAuthorizationConstraintError(err)) throw err;
      return buildConstraintBlockedTurn({ err, mission, action, intent });
    }
    executionRequest = routed.request;
    executionResult = routed.executionResult;
    execution = routed.execution || null;
    action = routed.action || action;
    snapshot = routed.snapshot || engine.inspect(mission.id, { tenantId });
  }

  if (action === 'plan_clarified') {
    const chained = await maybeChainPlanningAfterClarification(input, {
      action,
      executionResult,
      snapshot,
      mission,
      tenantId,
      engine,
      audit,
      question,
      executionRequest,
    });
    action = chained.action;
    const chainedResult = chained.executionResult;
    if (chainedResult !== executionResult) {
      execution = chained.execution || (chained.executionRequest
        ? amo.classifyExecutionEvidence({
          request: chained.executionRequest,
          executionResult: chainedResult,
          specialist: amo.specialistForIntent(chained.executionRequest.intent),
          tenantId,
        })
        : null);
    }
    executionResult = chainedResult;
    snapshot = chained.snapshot || snapshot;
    executionRequest = chained.executionRequest || executionRequest;
  }

  const response = applySyntheticNotice(applyExecutionTruth(buildExecutionMissionResponse({
    mission: snapshot.mission || mission,
    snapshot,
    action,
    question,
    executionResult,
  }), execution), snapshot.mission || mission, execution);

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
    execution,
    audit,
  };
}

/**
 * P0 — a durable mission restriction refused the request before dispatch.
 */
function buildConstraintBlockedTurn({ err, mission, action, intent }) {
  const constraint = err.code === amo.AUTHORIZATION_CONSTRAINT_BLOCKED ? err : err.cause;
  const actionClass = (constraint && constraint.actionClass) || amo.intentActionClass(intent);
  const constraints = (constraint && constraint.authorizationConstraints) || mission.authorizationConstraints || null;
  const answer = [
    `Not executed: this mission's authorization forbids ${String(actionClass).replace(/_/g, ' ')}` +
      (constraints && constraints.reviewOnly ? ' (review-only mission).' : '.'),
    'No specialist was dispatched and no external action was taken.',
    'The restriction was set by the operator and can only be lifted by an explicit operator decision.',
  ].join(' ');
  const structured = buildStructuredResponse({
    answer,
    reasoning: [`Mission ${mission.id} prohibits: ${((constraints && constraints.prohibited) || []).join(', ') || actionClass}.`],
    relatedEntities: [{ id: mission.id, type: 'acquisition_mission', name: mission.title || mission.id }],
  });
  // Assigned after construction: buildStructuredResponse whitelists metadata keys.
  Object.assign(structured.metadata, {
    acquisitionMission: true,
    acquisitionMissionExecution: true,
    missionExecutionAction: action,
    executionState: 'blocked',
    executionVerified: false,
    blocker: amo.AUTHORIZATION_CONSTRAINT_BLOCKED,
    actionClass,
    authorizationConstraints: constraints,
  });
  return {
    reason: 'acquisition_mission_authorization_constraint_blocked',
    structured,
    prose: answer,
    mission,
    action,
    executionResult: null,
    executionRequest: null,
    execution: {
      state: 'blocked',
      verified: false,
      reason: 'authorization_constraint',
      evidence: { actionClass, prohibited: (constraints && constraints.prohibited) || [] },
      correlation: { missionId: mission.id, intent, specialist: amo.specialistForIntent(intent) },
    },
  };
}

/**
 * P0 — every later turn on a synthetic-evidence mission says so, so reports
 * built from it can never pass for live results.
 */
function applySyntheticNotice(response, mission, execution) {
  if (!response || !amo.missionHasSyntheticEvidence(mission)) return response;
  const structured = response.structured || {};
  structured.metadata = { ...(structured.metadata || {}), syntheticEvidence: mission.syntheticEvidence };
  response.structured = structured;
  if (execution && execution.state === 'synthetic_result') return response; // already stated
  const notice = 'Note: this mission\'s discovery data is synthetic (fixture), not live results; it cannot be used for outreach.';
  response.prose = [notice, response.prose].filter(Boolean).join('\n\n');
  structured.answer = [notice, structured.answer].filter(Boolean).join('\n\n');
  return response;
}

/**
 * P0 — synthetic (fixture) evidence refused an outreach request before dispatch.
 */
function buildSyntheticBlockedTurn({ mission, action, intent }) {
  const answer = [
    'Not executed: this mission\'s discovery data is synthetic (fixture), not live provider results.',
    'Synthetic data cannot authorize or perform outreach, enter the CRM, or feed learning.',
    'No specialist was dispatched and no external action was taken. Start a new mission with live discovery.',
  ].join(' ');
  const structured = buildStructuredResponse({
    answer,
    reasoning: [`Mission ${mission.id} carries synthetic evidence.`],
    relatedEntities: [{ id: mission.id, type: 'acquisition_mission', name: mission.title || mission.id }],
  });
  Object.assign(structured.metadata, {
    acquisitionMission: true,
    acquisitionMissionExecution: true,
    missionExecutionAction: action,
    executionState: 'blocked',
    executionVerified: false,
    blocker: amo.SYNTHETIC_EVIDENCE_BLOCKED,
    syntheticEvidence: mission.syntheticEvidence || null,
  });
  return {
    reason: 'acquisition_mission_synthetic_evidence_blocked',
    structured,
    prose: answer,
    mission,
    action,
    executionResult: null,
    executionRequest: null,
    execution: {
      state: 'blocked',
      verified: false,
      reason: 'synthetic_evidence',
      evidence: { provenance: 'synthetic_fixture' },
      correlation: { missionId: mission.id, intent, specialist: amo.specialistForIntent(intent) },
    },
  };
}

/**
 * P0 — bind the operator-facing response to router execution evidence.
 * Records the classified state on the structured metadata and, when a work
 * specialist was dispatched without verified completion, says so plainly
 * instead of letting downstream copy imply the work finished.
 * @param {{ structured: object, prose: string }} response
 * @param {object|null} execution
 */
function applyExecutionTruth(response, execution) {
  if (!response || !execution) return response;
  // A review-gate refusal is blocked (nothing ran), not a specialist failure.
  if (response.executionReviewRefusal) return response;
  const structured = response.structured || {};
  structured.metadata = {
    ...(structured.metadata || {}),
    executionState: execution.state,
    executionVerified: execution.verified,
    executionEvidence: execution.evidence || null,
    executionCorrelation: execution.correlation || null,
  };
  if (!amo.specialistCompletionUnverified(execution)) {
    response.structured = structured;
    return response;
  }
  if (execution.state === amo.EXECUTION_EVIDENCE_STATES.FAILED && execution.reason === 'transaction_rolled_back') {
    // Rollback presentation already states the stage could not execute.
    response.structured = structured;
    return response;
  }
  const specialist = execution.correlation && execution.correlation.specialist;
  const name = specialist ? specialist.charAt(0).toUpperCase() + specialist.slice(1) : 'The specialist';
  const STATES = amo.EXECUTION_EVIDENCE_STATES;
  const counts = execution.evidence && execution.evidence.counts;
  let notice;
  if (execution.state === STATES.BLOCKED && execution.reason === 'authorization_constraint') {
    notice = `${name} did not run: this mission's authorization forbids ${String(execution.evidence && execution.evidence.actionClass || 'this action').replace(/_/g, ' ')}. No external action was taken.`;
  } else if (execution.state === STATES.BLOCKED) {
    notice = `${name} did not complete this stage: it is blocked. No external action was taken.`;
  } else if (execution.state === STATES.SYNTHETIC_RESULT) {
    notice = `${name} returned synthetic fixture data, not live provider results. Nothing here is verified and none of it may be used for outreach.`;
  } else if (execution.state === STATES.PARTIALLY_COMPLETED && counts) {
    notice = `${name} partially completed: the provider accepted ${counts.accepted} of ${counts.total - counts.blocked}; ${counts.failed} failed and ${counts.pending + counts.unconfirmed} are unconfirmed.`;
  } else if (execution.state === STATES.FAILED) {
    notice = `${name} did not complete this stage: the provider rejected every attempt.`;
  } else {
    notice = `${name} was dispatched, but completion is not verified — the provider evidence this action requires was not returned. Treat this as outcome unknown.`;
  }
  const prose = [notice, response.prose].filter(Boolean).join('\n\n');
  structured.answer = [notice, structured.answer].filter(Boolean).join('\n\n');
  response.structured = structured;
  response.prose = prose;
  return response;
}

module.exports = {
  presentedExecutionRevision,
  recordPresentedExecutionDecision,
  applyExecutionTruth,
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
