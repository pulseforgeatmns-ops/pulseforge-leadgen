'use strict';

/**
 * SPEC-128 / SPEC-131 / SPEC-136 — Operator approval consumes pending decisions
 * only when the stage commits. pendingOperatorDecision must match execution predicates.
 */

const amo = require('../../acquisition-mission');
const { Scout } = require('../../scout');
const { defaultDiscoveryAdapters } = require('../scoutAcquisition/DiscoveryAdapters');
const { evaluateDiscoveryCapability } = require('../../scout/coverage/DiscoveryCapabilityGate');
const { EXTERNAL_DISCOVERY_CAPABILITY_UNAVAILABLE } = require('../../scout/coverage/ExternalDiscoveryProviderRegistry');
const {
  buildInvestigationContinuationContext,
  extractPayloadFromDiscoveryContribution,
} = require('../../scout/investigation/EntityInvestigationContinuation');

const {
  STAGES,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  EVENT_KINDS,
} = amo;
const {
  executeMissionStage,
  assertConfidenceValid,
  assertEvidenceAttached,
  assertContributionContract,
  assertExecutionResult,
  executionResultFromStageOutput,
  bumpMissionVersion,
  planningError,
  validationError,
  buildMissionExecutionContext,
  executeSpecialist,
  buildExecutionInput,
  EXECUTION_STATUSES,
} = amo;
const {
  runMaxForAmoMission,
  prioritizationPayloadFromMaxResult,
  buildPrioritizationPayload,
} = require('./MaxPrioritizationExecutor');
const { createEvent } = require('../../acquisition-mission/Timeline');
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
  buildScoutDiscoveryArtifact,
  assertScoutEvidenceHandoff,
} = require('../../scout/adapters/ScoutDiscoveryArtifact');
const {
  freezeStructuredMission,
  isStructuredMissionApproved,
  isReadyForLock,
  formatOperatorConfirmation,
  formatAmbiguityPrompt,
} = require('../../acquisition-mission/StructuredMission');
const {
  scoutDelegationFromMission,
} = require('../../acquisition-mission/SpecialistInputs');
const { applyClarification, applyEdits } = require('../../acquisition-mission/MissionPlanner');
const { bindStagePersistDurable } = require('../../../services/acquisitionMissionPersistence');
const { specialistContext, canEnter } = require('../../acquisition-mission/Lifecycle');
const {
  findEmmettCapacity,
  runEmmettForAmoMission,
  validateEmmettPreconditions,
  validateEmmettCapacityOutput,
  commitEmmettCapacityStage,
  buildEmmettExecutionResult,
} = require('./EmmettCapacityExecution');
const { isMissionPlanningTurn } = require('./MissionPlanningTurn');
const { OPERATOR_DECISION_KINDS } = amo;
const {
  hasPendingPlanClarification,
  hasPendingPlanApproval,
  hasPendingDiscoveryApproval,
  hasPendingDiscoveryInvestigation,
  hasPendingPrioritizationApproval,
  hasPendingExecutionApproval,
  hasConsumablePendingDecision,
  presentableOperatorDecision,
  assertMissionStateConsistent,
} = require('../../acquisition-mission/PendingOperatorDecision');
const {
  buildPostDiscoveryPendingDecision,
  discoveryNeedsInvestigation,
} = require('../../acquisition-mission/DecisionReadiness');
const {
  buildExecutionApprovalPayload,
  findValidExecutionApproval,
  EXECUTION_APPROVAL_ACTION,
} = require('../../acquisition-mission/ExecutionApproval');

const DISCOVERY_APPROVAL_ACTION = 'discovery_approved';
const DISCOVERY_INVESTIGATION_ACTION = 'discovery_investigation_continued';
const PRIORITIZATION_APPROVAL_ACTION = 'prioritization_approved';
const PLAN_APPROVAL_ACTION = 'plan_approved';
const PLAN_CLARIFICATION_ACTION = 'plan_clarified';
const PLAN_CANCEL_ACTION = 'plan_cancelled';
const PLAN_EDIT_ACTION = 'plan_edited';

/** SPEC-128 — operator approval lifecycle phases (audit + response). */
const APPROVAL_PHASES = Object.freeze({
  WAITING_FOR_OPERATOR: 'waiting_for_operator',
  APPROVAL_RECEIVED: 'approval_received',
  EXECUTING_STAGE: 'executing_stage',
  STAGE_COMPLETED: 'stage_completed',
  WAITING_FOR_NEXT_DECISION: 'waiting_for_next_decision',
});

function buildDelegationFromAmoMission(mission) {
  return scoutDelegationFromMission(mission);
}

function bindPersistDurable(input, engine, tenantId) {
  return bindStagePersistDurable(input, engine, tenantId);
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
 * SPEC-131 — plan lock commits atomically; failure leaves the mission unchanged.
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

  const staged = await executeMissionStage({
    engine,
    missionId: mission.id,
    tenantId,
    pool: input.pool,
    specialist: SPECIALISTS.MAX,
    stage: 'plan_lock',
    operatorId,
    requireLockedPlan: false,
    validatePreconditions: ({ mission: current }) => {
      if (!current) throw planningError('tme_mission_missing', 'Mission does not exist.');
      if (!current.missionPlanDraft) {
        throw planningError('tme_plan_missing', 'No mission plan draft to approve.');
      }
      if (!isReadyForLock(current.missionPlanDraft) || hasPendingPlanClarification({ mission: current })) {
        throw planningError('tme_plan_ambiguous', 'Mission plan still has unresolved ambiguities.');
      }
      return {
        missionExists: true,
        missionActive: current.planCancelled !== true,
        missionLocked: false,
        structuredPlanApproved: false,
        specialistAvailable: true,
        requiredEvidencePresent: true,
      };
    },
    execute: async ({ mission: current }) => {
      const frozen = freezeStructuredMission(current.missionPlanDraft, {
        approvedBy: operatorId || 'operator',
      });
      return { frozen, question };
    },
    validateOutput: (output) => {
      if (!output || !output.frozen || output.frozen.immutable !== true) {
        throw planningError('tme_plan_invalid', 'Structured mission was not frozen.');
      }
    },
    commit: ({ engine: amoEngine, tenantId: tid, output, transactionId, missionVersion }) => {
      const approvalResult = amoEngine.contribute(
        mission.id,
        {
          specialist: SPECIALISTS.OPERATOR,
          kind: CONTRIBUTION_KINDS.APPROVAL,
          payload: {
            approved: true,
            consumed: true,
            command: output.question,
            action: PLAN_APPROVAL_ACTION,
            kind: OPERATOR_DECISION_KINDS.PLAN_APPROVAL,
            contractHash: output.frozen.contractHash,
            transactionId,
          },
        },
        { tenantId: tid }
      );

      const updated = approvalResult.mission;
      updated.missionPlanDraft = null;
      updated.structuredMission = output.frozen;
      updated.structuredMissionApproved = true;
      updated.planAmbiguities = [];
      updated.pendingOperatorDecision = {
        stage: STAGES.DISCOVER,
        kind: OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL,
        prompt: 'Approve discovery?',
      };
      bumpMissionVersion(updated, transactionId);
      amoEngine.store.putMission(updated);

      amoEngine.contribute(
        mission.id,
        {
          specialist: SPECIALISTS.MAX,
          kind: CONTRIBUTION_KINDS.MISSION_PLAN,
          payload: {
            structuredMission: output.frozen,
            contractHash: output.frozen.contractHash,
            transactionId,
          },
        },
        { tenantId: tid }
      );

      amoEngine.store.addEvent(createEvent({
        missionId: mission.id,
        kind: EVENT_KINDS.EXECUTION_COMMITTED,
        specialist: SPECIALISTS.MAX,
        label: 'Mission plan committed',
        payload: { transactionId, missionVersion: missionVersion + 1, priorVersion: missionVersion },
      }));

      const nextSnapshot = amoEngine.inspect(mission.id, { tenantId: tid });
      assertMissionStateConsistent(nextSnapshot.mission, {
        contributions: nextSnapshot.contributions,
      });
      return {
        approval: approvalResult.contribution,
        snapshot: nextSnapshot,
        structuredMission: output.frozen,
      };
    },
    persistDurable: bindPersistDurable(input, engine, tenantId),
  });

  return {
    alreadyExecuted: false,
    approval: staged.commitResult.approval,
    snapshot: staged.commitResult.snapshot,
    structuredMission: staged.output.frozen,
    transactionId: staged.transactionId,
    audit: staged.audit,
  };
}

function putPlannedMission(engine, mission, planned, extra = {}) {
  mission.missionPlanDraft = planned.draft;
  mission.planAmbiguities = planned.ambiguities || [];
  mission.planResolutions = planned.resolutions || mission.planResolutions || null;
  mission.structuredMission = null;
  mission.structuredMissionApproved = false;
  Object.assign(mission, extra);
  if (planned.ambiguities && planned.ambiguities.length) {
    const first = planned.ambiguities[0];
    mission.pendingOperatorDecision = {
      stage: STAGES.DISCOVER,
      kind: OPERATOR_DECISION_KINDS.PLAN_CLARIFICATION,
      prompt: first.question,
      question: first.question,
      choices: first.choices || [],
      field: first.field,
      clarificationPrompt: formatAmbiguityPrompt(first),
    };
  } else {
    mission.pendingOperatorDecision = {
      stage: STAGES.DISCOVER,
      kind: OPERATOR_DECISION_KINDS.PLAN_APPROVAL,
      prompt: 'Approve mission plan?',
      missionUnderstanding: planned.confirmation || formatOperatorConfirmation(planned.draft),
      actions: ['Approve', 'Edit', 'Cancel'],
    };
  }
  return engine.store.putMission(mission);
}

/**
 * Apply an operator clarification choice and replan. Does not execute specialists.
 */
function advancePlanClarification(input = {}) {
  const { engine, mission, tenantId, question } = input;
  if (!engine || !mission) throw new Error('engine and mission are required');
  const snapshot = engine.inspect(mission.id, { tenantId });
  const current = snapshot.mission;
  const planned = applyClarification(current.objective, question, {
    targetSegment: current.targetSegment,
    constraints: current.constraints,
    prior: { ambiguities: current.planAmbiguities, draft: current.missionPlanDraft, resolutions: current.planResolutions },
    resolutions: current.planResolutions,
    context: input.context,
  });
  if (planned.unmatchedClarification) {
    return {
      matched: false,
      snapshot: engine.inspect(mission.id, { tenantId }),
      clarificationPrompt: planned.clarificationPrompt,
      planned,
    };
  }
  engine.contribute(
    mission.id,
    {
      specialist: SPECIALISTS.OPERATOR,
      kind: CONTRIBUTION_KINDS.EDIT,
      payload: {
        action: PLAN_CLARIFICATION_ACTION,
        command: question,
        field: current.pendingOperatorDecision && current.pendingOperatorDecision.field,
        resolutions: planned.resolutions,
      },
    },
    { tenantId }
  );
  putPlannedMission(engine, engine.get(mission.id, tenantId), planned);
  return {
    matched: true,
    snapshot: engine.inspect(mission.id, { tenantId }),
    planned,
    readyForConfirmation: planned.readyForConfirmation,
  };
}

function cancelMissionPlan(input = {}) {
  const { engine, mission, tenantId, question } = input;
  if (!engine || !mission) throw new Error('engine and mission are required');
  const current = engine.get(mission.id, tenantId);
  current.planCancelled = true;
  current.missionPlanDraft = current.missionPlanDraft
    ? { ...current.missionPlanDraft, execution: { state: 'cancelled' } }
    : null;
  current.pendingOperatorDecision = null;
  current.status = 'Cancelled';
  engine.store.putMission(current);
  engine.contribute(
    mission.id,
    {
      specialist: SPECIALISTS.OPERATOR,
      kind: CONTRIBUTION_KINDS.EDIT,
      payload: { action: PLAN_CANCEL_ACTION, command: question, cancelled: true },
    },
    { tenantId }
  );
  return { snapshot: engine.inspect(mission.id, { tenantId }), cancelled: true };
}

function beginPlanEdit(input = {}) {
  const { engine, mission, tenantId } = input;
  const current = engine.get(mission.id, tenantId);
  current.pendingOperatorDecision = {
    stage: STAGES.DISCOVER,
    kind: OPERATOR_DECISION_KINDS.PLAN_EDIT,
    prompt: 'What should we change?',
    actions: ['Approve', 'Edit', 'Cancel'],
  };
  engine.store.putMission(current);
  return { snapshot: engine.inspect(mission.id, tenantId) };
}

function applyPlanEdits(input = {}) {
  const { engine, mission, tenantId, question, edits } = input;
  const current = engine.get(mission.id, tenantId);
  const planned = applyEdits(current.objective, edits || { geographyText: question, region: question }, {
    targetSegment: current.targetSegment,
    constraints: current.constraints,
    resolutions: current.planResolutions,
    context: input.context,
  });
  engine.contribute(
    mission.id,
    {
      specialist: SPECIALISTS.OPERATOR,
      kind: CONTRIBUTION_KINDS.EDIT,
      payload: { action: PLAN_EDIT_ACTION, command: question, edits: edits || { region: question } },
    },
    { tenantId }
  );
  putPlannedMission(engine, engine.get(mission.id, tenantId), planned);
  return { snapshot: engine.inspect(mission.id, tenantId), planned };
}

function findPrioritizationApproval(contributions = []) {
  return [...contributions]
    .reverse()
    .find(
      (row) =>
        row.specialist === SPECIALISTS.OPERATOR &&
        row.kind === CONTRIBUTION_KINDS.APPROVAL &&
        (row.payload.action === PRIORITIZATION_APPROVAL_ACTION ||
          row.payload.kind === OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL)
    );
}

function findLatestScoutDiscovery(contributions = []) {
  return [...contributions]
    .reverse()
    .find(
      (row) => row.specialist === SPECIALISTS.SCOUT && row.kind === CONTRIBUTION_KINDS.DISCOVERY
    );
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

function findLatestMaxPrioritization(contributions = []) {
  return [...contributions]
    .reverse()
    .find(
      (row) => row.specialist === SPECIALISTS.MAX && row.kind === CONTRIBUTION_KINDS.PRIORITIZATION
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


function mapScoutIntelligenceToDiscoveryPayload(result = {}, opts = {}) {
  const artifact = buildScoutDiscoveryArtifact(result, {
    missionObjective: opts.missionObjective,
    approvalConsumed: true,
  });
  const payload = normalizeScoutDiscoveryPayload(result, {
    ...opts,
    discoveryArtifact: artifact,
  });
  assertScoutEvidenceHandoff(artifact, payload);
  return payload;
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
  const executionContext = buildMissionExecutionContext({
    engine: opts.engine,
    mission,
    tenantId: delegation.tenantId,
    transactionId: opts.transactionId,
    pool: opts.pool,
  });
  if (opts.executionRequest) {
    executionContext.executionRequest = opts.executionRequest;
  }

  let scoutOpts = {
    ...opts,
    delegation,
    executionContext,
    mode: opts.scoutMode || 'completed',
    missionId: mission.id,
    amoMissionId: mission.id,
    runtimeOwner: 'amo',
    attachScoutDiscovery: false,
    tenantId: delegation.tenantId,
    companies: opts.scoutCompanies,
    people: opts.scoutPeople,
    discover: opts.discover,
    enablePlaces: opts.enablePlaces,
    allowFixtureFallback: opts.allowFixtureFallback,
  };

  if (opts.investigationContinuation === true && opts.engine) {
    const snapshot = opts.engine.inspect(mission.id, { tenantId: delegation.tenantId });
    const priorDiscovery = findLatestScoutDiscovery(snapshot.contributions || []);
    const priorPayload = extractPayloadFromDiscoveryContribution(priorDiscovery || {});
    const continuation = buildInvestigationContinuationContext({
      priorPayload,
      opts: {
        ...opts,
        investigationContinuation: true,
        question: opts.question,
      },
    });
    scoutOpts = {
      ...scoutOpts,
      investigationContinuation: true,
      investigationMode: continuation.investigationMode,
      priorDiscoveryPayload: continuation.priorDiscoveryPayload,
      preservedCandidates: continuation.preservedCandidates,
      entityInvestigationContinuation:
        continuation.investigationMode === 'entity_continuation',
    };
  }

  try {
    const result = await Scout.discover({
      mission,
      // ADR-089 — AMO-owned missions never sync through Mission Engine.
      missionEngine: null,
      scoutPayload: {},
      operatorId: opts.operatorId,
      opts: scoutOpts,
    });
    const intelligenceResult = result.intelligenceResult || result;
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
  let payload;
  if (scoutResult && scoutResult.discoveryReport && !scoutResult.intelligenceResult && !scoutResult.payload) {
    const report = scoutResult.discoveryReport;
    payload = normalizeScoutDiscoveryPayload(
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
  } else {
    payload = mapScoutIntelligenceToDiscoveryPayload(scoutResult, {
      missionObjective: mission && mission.objective,
    });
  }
  return payload;
}

function clearPendingOperatorDecision(engine, mission) {
  if (!mission || !mission.pendingOperatorDecision) return mission;
  mission.pendingOperatorDecision = null;
  return engine.store.putMission(mission);
}

function validateDiscoveryPreconditions({
  mission,
  engine,
  tenantId,
  discover,
  enablePlaces,
  placesProvider,
  runScout,
}) {
  if (!mission) throw planningError('tme_mission_missing', 'Mission does not exist.');
  if (mission.planCancelled === true || /cancelled/i.test(String(mission.status || ''))) {
    throw planningError('tme_mission_inactive', 'Mission is not active.');
  }
  if (!isStructuredMissionApproved(mission)) {
    throw planningError('tme_plan_missing', 'Mission Plan missing.');
  }
  const snapshot = engine.inspect(mission.id, { tenantId });
  if (hasPendingPlanClarification(snapshot) || hasPendingPlanApproval(snapshot)) {
    throw planningError('tme_plan_missing', 'Mission Plan missing.');
  }
  if (mission.stage && mission.stage !== STAGES.DISCOVER) {
    throw planningError('tme_wrong_stage', `Discovery cannot execute while the mission is at ${mission.stage}.`);
  }

  if (typeof runScout === 'function') {
    return {
      missionExists: true,
      missionActive: true,
      missionLocked: true,
      structuredPlanApproved: true,
      specialistAvailable: true,
      requiredEvidencePresent: true,
      externalDiscoveryCapability: { skipped: true, reason: 'runScout override' },
    };
  }

  const adapters = defaultDiscoveryAdapters({
    discover,
    enablePlaces,
    placesProvider,
  });
  const capability = evaluateDiscoveryCapability({
    adapters,
    requireExternalDiscovery: true,
    coveragePlan: { totals: { searches: 1 }, sources: ['public_business_data'] },
    discover,
    enablePlaces,
    placesProvider,
  });
  if (!capability.canExecute) {
    throw planningError(
      'external_discovery_capability_unavailable',
      capability.blockReason || EXTERNAL_DISCOVERY_CAPABILITY_UNAVAILABLE
    );
  }

  return {
    missionExists: true,
    missionActive: true,
    missionLocked: true,
    structuredPlanApproved: true,
    specialistAvailable: true,
    requiredEvidencePresent: true,
    externalDiscoveryCapability: capability,
  };
}

function validateDiscoveryOutput(output, ctx = {}) {
  if (!output || !output.discoveryPayload) {
    throw validationError('tme_contribution_missing', 'Discovery contribution is missing.');
  }
  const payload = output.discoveryPayload;
  const blocked = payload.blocked === true || payload.outcome === 'blocked';
  assertContributionContract(SPECIALISTS.SCOUT, payload);
  assertConfidenceValid(payload.confidence, { required: true });
  assertEvidenceAttached(payload, { required: !blocked });

  const executionResult = output.executionResult || executionResultFromStageOutput(output, {
    specialist: SPECIALISTS.SCOUT,
    transactionId: ctx.transactionId,
  });
  assertExecutionResult(executionResult, {
    specialist: SPECIALISTS.SCOUT,
    requireContributions: !blocked,
    requireEvidence: !blocked,
  });
  output.executionResult = executionResult;
}

function commitDiscoveryStage({
  engine,
  mission,
  tenantId,
  output,
  transactionId,
  missionVersion,
  existingApproval,
  investigationContinuation = false,
  investigationCommand = null,
}) {
  const missionId = (mission && mission.id) || output.missionId;
  const { question, discoveryPayload } = output;
  let approval = existingApproval;
  if (investigationContinuation) {
    const investigationResult = engine.contribute(
      missionId,
      {
        specialist: SPECIALISTS.OPERATOR,
        kind: CONTRIBUTION_KINDS.APPROVAL,
        payload: {
          approved: true,
          consumed: true,
          command: investigationCommand || question,
          action: DISCOVERY_INVESTIGATION_ACTION,
          kind: OPERATOR_DECISION_KINDS.DISCOVERY_INVESTIGATION,
          stage: STAGES.DISCOVER,
          transactionId,
        },
      },
      { tenantId }
    );
    approval = approval || investigationResult.contribution;
  } else if (!approval) {
    const approvalResult = engine.contribute(
      missionId,
      {
        specialist: SPECIALISTS.OPERATOR,
        kind: CONTRIBUTION_KINDS.APPROVAL,
        payload: {
          approved: true,
          consumed: true,
          command: question,
          action: DISCOVERY_APPROVAL_ACTION,
          stage: STAGES.DISCOVER,
          transactionId,
        },
      },
      { tenantId }
    );
    approval = approvalResult.contribution;
    clearPendingOperatorDecision(engine, approvalResult.mission);
  }

  const payload = { ...discoveryPayload, approvalId: approval.id, transactionId };
  const discoveryContribution = engine.contribute(
    missionId,
    {
      specialist: SPECIALISTS.SCOUT,
      kind: CONTRIBUTION_KINDS.DISCOVERY,
      payload,
    },
    { tenantId }
  );

  const updated = engine.get(missionId, tenantId);
  updated.pendingOperatorDecision = buildPostDiscoveryPendingDecision(payload);
  bumpMissionVersion(updated, transactionId);
  engine.store.putMission(updated);

  engine.store.addEvent(createEvent({
    missionId,
    kind: EVENT_KINDS.EXECUTION_COMMITTED,
    specialist: SPECIALISTS.SCOUT,
    label: 'Discovery stage committed',
    payload: {
      transactionId,
      missionVersion: updated.version,
      priorVersion: missionVersion,
      approvalId: approval.id,
      discoveryId: discoveryContribution.contribution.id,
    },
  }));

  const snapshot = engine.inspect(missionId, { tenantId });
  assertMissionStateConsistent(snapshot.mission, {
    contributions: snapshot.contributions,
  });
  return {
    approval,
    discovery: discoveryContribution.contribution,
    scoutResult: output.scoutResult,
    snapshot,
  };
}

/**
 * Consume discovery approval, execute Scout once, attach results, and advance when ready.
 * SPEC-131 — approval is consumed only in the same commit as successful Scout execution.
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
    persist,
    runScout,
    scoutCompanies,
    scoutPeople,
    allowFixtureFallback,
    audit: inputAudit,
    discover: inputDiscover,
    enablePlaces: inputEnablePlaces,
    placesProvider: inputPlacesProvider,
  } = input;

  const effectiveDiscover =
    inputDiscover ||
    (typeof runScout === 'function'
      ? undefined
      : allowFixtureFallback === true
        ? async () => []
        : undefined);

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
    const needsInvestigation = discoveryNeedsInvestigation(existingDiscovery.payload || {});
    const investigationPending = hasPendingDiscoveryInvestigation(snapshot);
    if (!needsInvestigation || !investigationPending) {
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
  }

  emitStarted({
    missionId: mission.id,
    tenantId,
    stage: STAGES.DISCOVER,
    executor: SPECIALISTS.SCOUT,
    phase: APPROVAL_PHASES.EXECUTING_STAGE,
    approvalId: existingApproval && existingApproval.id,
  });

  let staged;
  try {
    staged = await executeMissionStage({
      engine,
      missionId: mission.id,
      tenantId,
      pool: input.pool,
      specialist: SPECIALISTS.SCOUT,
      stage: STAGES.DISCOVER,
      operatorId,
      validatePreconditions: (ctx) =>
        validateDiscoveryPreconditions({
          ...ctx,
          discover: effectiveDiscover,
          enablePlaces: inputEnablePlaces,
          placesProvider: inputPlacesProvider,
          runScout,
        }),
      execute: async ({ mission: current, transactionId }) => {
        const scoutResult = await runScoutForAmoMission(current, {
          question,
          operatorId,
          engine,
          transactionId,
          pool: input.pool,
          persist,
          runScout,
          scoutCompanies,
          scoutPeople,
          allowFixtureFallback,
          executionRequest: input.executionRequest || null,
          discover: effectiveDiscover,
          enablePlaces: inputEnablePlaces,
          placesProvider: inputPlacesProvider,
        });
        const discoveryPayload = discoveryPayloadFromScoutResult(scoutResult, current);
        const executionResult = executionResultFromStageOutput(
          { scoutResult, discoveryPayload },
          { specialist: SPECIALISTS.SCOUT, transactionId }
        );
        return {
          scoutResult,
          discoveryPayload,
          executionResult,
          question,
          missionId: current.id,
        };
      },
      validateOutput: validateDiscoveryOutput,
      commit: (ctx) => commitDiscoveryStage({
        ...ctx,
        existingApproval,
      }),
      persistDurable: bindPersistDurable(input, engine, tenantId),
    });
  } catch (err) {
    askPathTrace.traceEarlyReturn('advanceDiscoveryAfterApproval', 'rolled_back');
    throw err;
  }

  const approval = staged.commitResult.approval;
  const discovery = staged.commitResult.discovery;
  snapshot = staged.commitResult.snapshot;
  const discoveryPayload = staged.output.discoveryPayload;
  const executionOutcome = discoveryPayload.blocked ? 'blocked' : 'completed';

  emitConsumed({
    missionId: mission.id,
    tenantId,
    stage: STAGES.DISCOVER,
    action: DISCOVERY_APPROVAL_ACTION,
    phase: APPROVAL_PHASES.EXECUTING_STAGE,
    approvalId: approval.id,
    operatorId,
    reusedApproval: Boolean(existingApproval),
    transactionId: staged.transactionId,
  });

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
    discoveryId: discovery.id,
    qualifiedCount: discoveryPayload.qualifiedCount || 0,
    transactionId: staged.transactionId,
  });

  askPathTrace.traceEarlyReturn('advanceDiscoveryAfterApproval', executionOutcome);
  return {
    alreadyExecuted: false,
    approval,
    discovery,
    scoutResult: staged.output.scoutResult,
    snapshot,
    executionOutcome,
    approvalPhase:
      executionOutcome === 'blocked'
        ? APPROVAL_PHASES.WAITING_FOR_OPERATOR
        : APPROVAL_PHASES.WAITING_FOR_NEXT_DECISION,
    audit,
    transactionId: staged.transactionId,
    missionVersion: staged.missionVersion,
  };
}

/**
 * Continue Scout investigation when post-discovery readiness is insufficient.
 * SPEC-193 — investigation continuation must not be short-circuited by prior Discovery commits.
 * @param {object} input
 * @returns {Promise<object>}
 */
async function advanceDiscoveryInvestigationAfterApproval(input = {}) {
  askPathTrace.traceEnter('advanceDiscoveryInvestigationAfterApproval');
  const {
    engine,
    mission,
    tenantId,
    question,
    operatorId,
    persist,
    runScout,
    scoutCompanies,
    scoutPeople,
    allowFixtureFallback,
    audit: inputAudit,
    discover: inputDiscover,
    enablePlaces: inputEnablePlaces,
    placesProvider: inputPlacesProvider,
  } = input;

  const effectiveDiscover =
    inputDiscover ||
    (typeof runScout === 'function'
      ? undefined
      : allowFixtureFallback === true
        ? async () => []
        : undefined);

  if (!engine || !mission) {
    throw new Error('engine and mission are required');
  }

  let snapshot = engine.inspect(mission.id, { tenantId });
  if (!hasPendingDiscoveryInvestigation(snapshot)) {
    throw planningError(
      'tme_no_pending_investigation',
      'No discovery investigation decision is pending.'
    );
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
    action: DISCOVERY_INVESTIGATION_ACTION,
    phase: APPROVAL_PHASES.APPROVAL_RECEIVED,
    command: question,
    operatorId,
  });

  const contributions = snapshot.contributions || [];
  const existingApproval = findDiscoveryApproval(contributions);

  emitStarted({
    missionId: mission.id,
    tenantId,
    stage: STAGES.DISCOVER,
    executor: SPECIALISTS.SCOUT,
    phase: APPROVAL_PHASES.EXECUTING_STAGE,
    approvalId: existingApproval && existingApproval.id,
    investigationContinuation: true,
  });

  let staged;
  try {
    staged = await executeMissionStage({
      engine,
      missionId: mission.id,
      tenantId,
      pool: input.pool,
      specialist: SPECIALISTS.SCOUT,
      stage: STAGES.DISCOVER,
      operatorId,
      validatePreconditions: (ctx) =>
        validateDiscoveryPreconditions({
          ...ctx,
          discover: effectiveDiscover,
          enablePlaces: inputEnablePlaces,
          placesProvider: inputPlacesProvider,
          runScout,
        }),
      execute: async ({ mission: current, transactionId }) => {
        const scoutResult = await runScoutForAmoMission(current, {
          question,
          operatorId,
          engine,
          transactionId,
          pool: input.pool,
          persist,
          runScout,
          scoutCompanies,
          scoutPeople,
          allowFixtureFallback,
          executionRequest: input.executionRequest || null,
          discover: effectiveDiscover,
          enablePlaces: inputEnablePlaces,
          placesProvider: inputPlacesProvider,
          investigationContinuation: true,
        });
        const discoveryPayload = discoveryPayloadFromScoutResult(scoutResult, current);
        const executionResult = executionResultFromStageOutput(
          { scoutResult, discoveryPayload },
          { specialist: SPECIALISTS.SCOUT, transactionId }
        );
        return {
          scoutResult,
          discoveryPayload,
          executionResult,
          question,
          missionId: current.id,
        };
      },
      validateOutput: validateDiscoveryOutput,
      commit: (ctx) => commitDiscoveryStage({
        ...ctx,
        existingApproval,
        investigationContinuation: true,
        investigationCommand: question,
      }),
      persistDurable: bindPersistDurable(input, engine, tenantId),
    });
  } catch (err) {
    askPathTrace.traceEarlyReturn('advanceDiscoveryInvestigationAfterApproval', 'rolled_back');
    throw err;
  }

  const approval = staged.commitResult.approval;
  const discovery = staged.commitResult.discovery;
  snapshot = staged.commitResult.snapshot;
  const discoveryPayload = staged.output.discoveryPayload;
  const executionOutcome = discoveryPayload.blocked ? 'blocked' : 'completed';

  emitConsumed({
    missionId: mission.id,
    tenantId,
    stage: STAGES.DISCOVER,
    action: DISCOVERY_INVESTIGATION_ACTION,
    phase: APPROVAL_PHASES.EXECUTING_STAGE,
    approvalId: approval.id,
    operatorId,
    reusedApproval: Boolean(existingApproval),
    transactionId: staged.transactionId,
    investigationContinuation: true,
  });

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
    discoveryId: discovery.id,
    qualifiedCount: discoveryPayload.qualifiedCount || 0,
    transactionId: staged.transactionId,
    investigationContinuation: true,
  });

  askPathTrace.traceEarlyReturn('advanceDiscoveryInvestigationAfterApproval', executionOutcome);
  return {
    alreadyExecuted: false,
    approval,
    discovery,
    scoutResult: staged.output.scoutResult,
    snapshot,
    executionOutcome,
    approvalPhase:
      executionOutcome === 'blocked'
        ? APPROVAL_PHASES.WAITING_FOR_OPERATOR
        : APPROVAL_PHASES.WAITING_FOR_NEXT_DECISION,
    audit,
    transactionId: staged.transactionId,
    missionVersion: staged.missionVersion,
    investigationContinuation: true,
  };
}

function validatePrioritizationPreconditions({ mission, engine, tenantId }) {
  if (!mission) throw planningError('tme_mission_missing', 'Mission does not exist.');
  if (mission.planCancelled === true || /cancelled/i.test(String(mission.status || ''))) {
    throw planningError('tme_mission_inactive', 'Mission is not active.');
  }
  if (!isStructuredMissionApproved(mission)) {
    throw planningError('tme_plan_missing', 'Mission Plan missing.');
  }
  const snapshot = engine.inspect(mission.id, { tenantId });
  if (!hasPendingPrioritizationApproval(snapshot)) {
    throw planningError('tme_no_pending_prioritization', 'No prioritization approval is pending.');
  }
  if (mission.stage && mission.stage !== STAGES.DISCOVER) {
    throw planningError('tme_wrong_stage', `Prioritization cannot execute while the mission is at ${mission.stage}.`);
  }
  const discovery = findLatestScoutDiscovery(snapshot.contributions || []);
  if (!discovery) {
    throw planningError('tme_discovery_missing', 'Discovery artifact is required before prioritization approval.');
  }
  const presentation = presentationFromDiscoveryPayload(discovery.payload || {});
  if (!hasSufficientEvidenceForPrioritization(presentation)) {
    throw validationError('tme_evidence_insufficient', 'Insufficient evidence for prioritization approval.');
  }
  return {
    missionExists: true,
    missionActive: true,
    missionLocked: true,
    structuredPlanApproved: true,
    specialistAvailable: true,
    requiredEvidencePresent: true,
  };
}

function validatePrioritizationOutput(output, ctx = {}) {
  const executionResult = output.executionResult || executionResultFromStageOutput(output, {
    specialist: SPECIALISTS.MAX,
    transactionId: ctx.transactionId,
  });

  if (
    executionResult.status === EXECUTION_STATUSES.BLOCKED
    || executionResult.status === EXECUTION_STATUSES.FAILED
  ) {
    const reason =
      (executionResult.blocked && executionResult.blocked.reason)
      || executionResult.reason
      || 'Max prioritization did not complete.';
    throw validationError('tme_max_blocked', reason);
  }

  if (!output || !output.prioritizationPayload) {
    throw validationError('tme_contribution_missing', 'Prioritization contribution is missing.');
  }
  const payload = output.prioritizationPayload;

  assertContributionContract(SPECIALISTS.MAX, payload);
  assertExecutionResult(executionResult, {
    specialist: SPECIALISTS.MAX,
    requireContributions: true,
    requireEvidence: true,
  });
  output.executionResult = executionResult;
}

function commitPrioritizationStage({
  engine,
  mission,
  tenantId,
  output,
  transactionId,
  missionVersion,
  existingApproval,
}) {
  const missionId = (mission && mission.id) || output.missionId;
  const { question, prioritizationPayload } = output;
  let approval = existingApproval;
  if (!approval) {
    const approvalResult = engine.contribute(
      missionId,
      {
        specialist: SPECIALISTS.OPERATOR,
        kind: CONTRIBUTION_KINDS.APPROVAL,
        payload: {
          approved: true,
          consumed: true,
          command: question,
          action: PRIORITIZATION_APPROVAL_ACTION,
          kind: OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL,
          stage: STAGES.DISCOVER,
          transactionId,
        },
      },
      { tenantId }
    );
    approval = approvalResult.contribution;
  }

  const current = engine.get(missionId, tenantId);
  current.pendingOperatorDecision = null;
  engine.store.putMission(current);

  const payload = { ...prioritizationPayload, approvalId: approval.id, transactionId };
  const prioritizationContribution = engine.contribute(
    missionId,
    {
      specialist: SPECIALISTS.MAX,
      kind: CONTRIBUTION_KINDS.PRIORITIZATION,
      payload,
    },
    { tenantId }
  );

  engine.progress(missionId, SPECIALISTS.MAX, {
    tenantId,
    stage: STAGES.UNDERSTAND,
  });

  const updated = engine.get(missionId, tenantId);
  bumpMissionVersion(updated, transactionId);
  engine.store.putMission(updated);
  engine.store.addEvent(createEvent({
    missionId,
    kind: EVENT_KINDS.EXECUTION_COMMITTED,
    specialist: SPECIALISTS.MAX,
    label: 'Max prioritization committed',
    payload: {
      transactionId,
      missionVersion: updated.version,
      priorVersion: missionVersion,
      approvalId: approval.id,
      prioritizationId: prioritizationContribution.contribution.id,
    },
  }));

  const snapshot = engine.inspect(missionId, { tenantId });
  assertMissionStateConsistent(snapshot.mission, {
    contributions: snapshot.contributions,
  });
  return {
    approval,
    prioritization: prioritizationContribution.contribution,
    snapshot,
  };
}

/**
 * Consume prioritization approval, execute Max at UNDERSTAND, attach prioritization, and advance.
 * SPEC-141 — the only path that transitions to Understanding after Discovery.
 * @param {object} input
 * @returns {Promise<object>}
 */
async function advancePrioritizationAfterApproval(input = {}) {
  askPathTrace.traceEnter('advancePrioritizationAfterApproval');
  const {
    engine,
    mission,
    tenantId,
    question,
    operatorId,
    runMax,
  } = input;

  if (!engine || !mission) {
    throw new Error('engine and mission are required');
  }

  let snapshot = engine.inspect(mission.id, { tenantId });
  const contributions = snapshot.contributions || [];
  const existingApproval = findPrioritizationApproval(contributions);
  const existingPrioritization = findLatestMaxPrioritization(contributions);

  if (existingApproval && existingPrioritization && snapshot.mission.stage === STAGES.UNDERSTAND) {
    askPathTrace.traceEarlyReturn('advancePrioritizationAfterApproval', 'already_executed');
    return {
      alreadyExecuted: true,
      approval: existingApproval,
      prioritization: existingPrioritization,
      snapshot: engine.inspect(mission.id, { tenantId }),
      approvalPhase: APPROVAL_PHASES.STAGE_COMPLETED,
    };
  }

  let staged;
  try {
    staged = await executeMissionStage({
      engine,
      missionId: mission.id,
      tenantId,
      pool: input.pool,
      specialist: SPECIALISTS.MAX,
      stage: STAGES.UNDERSTAND,
      operatorId,
      validatePreconditions: (ctx) => validatePrioritizationPreconditions(ctx),
      execute: async ({ mission: current, transactionId }) => {
        const maxResult = await runMaxForAmoMission(current, {
          question,
          operatorId,
          engine,
          transactionId,
          pool: input.pool,
          runMax,
          executionRequest: input.executionRequest || null,
        });
        const executionResult = maxResult && maxResult.spec === 'SPEC-132'
          ? maxResult
          : executionResultFromStageOutput(
            { maxResult, prioritizationPayload: maxResult && maxResult.contributions },
            { specialist: SPECIALISTS.MAX, transactionId }
          );
        const prioritizationPayload =
          maxResult
          && maxResult.status === EXECUTION_STATUSES.SUCCESS
          && maxResult.contributions
          && Object.keys(maxResult.contributions).length
            ? prioritizationPayloadFromMaxResult(maxResult)
            : null;
        return {
          maxResult,
          prioritizationPayload,
          executionResult,
          question,
          missionId: current.id,
        };
      },
      validateOutput: validatePrioritizationOutput,
      commit: (ctx) => commitPrioritizationStage({
        ...ctx,
        existingApproval,
      }),
      persistDurable: bindPersistDurable(input, engine, tenantId),
    });
  } catch (err) {
    askPathTrace.traceEarlyReturn('advancePrioritizationAfterApproval', 'rolled_back');
    throw err;
  }

  snapshot = staged.commitResult.snapshot;
  askPathTrace.traceEarlyReturn('advancePrioritizationAfterApproval', 'completed');
  return {
    alreadyExecuted: false,
    approval: staged.commitResult.approval,
    prioritization: staged.commitResult.prioritization,
    maxResult: staged.output.maxResult,
    snapshot,
    approvalPhase: APPROVAL_PHASES.STAGE_COMPLETED,
    transactionId: staged.transactionId,
    missionVersion: staged.missionVersion,
  };
}

function findMaxPrioritization(contributions = []) {
  return findLatestMaxPrioritization(contributions);
}

function findPaigeVariants(contributions = []) {
  return [...contributions]
    .reverse()
    .find(
      (row) => row.specialist === SPECIALISTS.PAIGE && row.kind === CONTRIBUTION_KINDS.VARIANTS
    );
}

function buildMaxPrioritizationPayload(mission, contributions = []) {
  const scout = findLatestScoutDiscovery(contributions);
  const scoutPayload = scout?.payload || {};
  const plan = mission.structuredMission || {};
  return buildPrioritizationPayload(mission, scoutPayload, plan);
}

function fixtureMaxPrioritizationResult(mission, contributions = []) {
  return buildMaxPrioritizationPayload(mission, contributions);
}

function buildPaigeVariantsPayload(executionInput = {}) {
  const max = executionInput.workspaceContext?.max
    || executionInput.specialistInput?.maxPrioritization
    || {};
  const scout = executionInput.workspaceContext?.scout
    || executionInput.specialistInput?.scoutDiscovery
    || {};
  const plan = executionInput.missionPlan || executionInput.specialistInput?.structuredMission || {};
  const topTarget = max.rankedTargets?.[0]?.name
    || max.priorities?.[0]?.name
    || scout.companies?.[0]?.name
    || plan.market?.label
    || 'your office';
  const objective = max.objectives?.[0]?.text || plan.objective || executionInput.specialistInput?.objective;
  const subject = `Commercial cleaning walkthrough for ${topTarget}`;
  const body = [
    `Hi — we help ${plan.market?.label || 'local offices'} maintain spotless workspaces.`,
    objective ? `Mission focus: ${objective}` : null,
    max.recommendations?.[0] ? `Why now: ${max.recommendations[0]}` : null,
  ].filter(Boolean).join('\n\n');

  return {
    variants: [{
      label: 'Primary',
      subject,
      body,
      cta: 'Reply to schedule a walkthrough',
    }],
    subjects: [subject],
    cta: 'Reply to schedule a walkthrough',
    hypotheses: [
      max.objectiveReason || 'Prioritized targets respond to timing-specific outreach.',
      scout.buyingSignals?.[0]
        ? `Signal: ${typeof scout.buyingSignals[0] === 'string' ? scout.buyingSignals[0] : scout.buyingSignals[0].label}`
        : 'Ops hiring signals indicate receptivity window.',
    ].filter(Boolean),
    experiments: [{
      name: 'subject_personalization',
      variant: 'company_name_in_subject',
      hypothesis: 'Company-specific subject lines increase open rates.',
    }],
    messaging: body,
  };
}

function fixturePaigeVariantsResult(mission, contributions = []) {
  const input = buildExecutionInput({
    mission,
    contributions,
    specialist: SPECIALISTS.PAIGE,
    transactionId: 'fixture_paige',
  });
  return buildPaigeVariantsPayload(input);
}

async function runMaxPrioritizationForAmoMission(mission, opts = {}) {
  if (typeof opts.runMax === 'function') {
    const custom = await opts.runMax(mission, opts);
    if (custom && custom.contributions) return custom.contributions;
    return custom;
  }
  const maxResult = await runMaxForAmoMission(mission, opts);
  if (maxResult.status === EXECUTION_STATUSES.BLOCKED || maxResult.status === EXECUTION_STATUSES.FAILED) {
    const reason =
      (maxResult.blocked && maxResult.blocked.reason)
      || maxResult.reason
      || 'Max prioritization did not complete.';
    throw validationError('tme_max_blocked', reason);
  }
  return prioritizationPayloadFromMaxResult(maxResult);
}

async function runPaigeForAmoMission(mission, opts = {}) {
  if (typeof opts.runPaige === 'function') {
    return opts.runPaige(mission, opts);
  }
  const contributions = opts.contributions
    || (opts.engine && opts.engine.inspect(mission.id, { tenantId: opts.tenantId }).contributions)
    || [];
  const executionInput = buildExecutionInput({
    mission,
    contributions,
    specialist: SPECIALISTS.PAIGE,
    transactionId: opts.transactionId,
    executionContext: opts.executionContext,
  });
  if (opts.allowFixtureFallback === false && !findMaxPrioritization(contributions)) {
    throw validationError('tme_max_prioritization_missing', 'Max prioritization is required before Paige execution.');
  }
  return buildPaigeVariantsPayload(executionInput);
}

function validateMaxPrioritizationOutput(output, ctx = {}) {
  if (!output || !output.prioritizationPayload) {
    throw validationError('tme_contribution_missing', 'Max prioritization contribution is missing.');
  }
  const payload = output.prioritizationPayload;
  assertContributionContract(SPECIALISTS.MAX, payload);
  assertConfidenceValid(payload.confidence, { required: false });
  const executionResult = output.executionResult || executionResultFromStageOutput(output, {
    specialist: SPECIALISTS.MAX,
    transactionId: ctx.transactionId,
  });
  assertExecutionResult(executionResult, {
    specialist: SPECIALISTS.MAX,
    requireContributions: true,
    requireEvidence: false,
  });
  output.executionResult = executionResult;
}

function validatePaigeOutput(output, ctx = {}) {
  if (!output || !output.variantsPayload) {
    throw validationError('tme_contribution_missing', 'Paige variants contribution is missing.');
  }
  const payload = output.variantsPayload;
  assertContributionContract(SPECIALISTS.PAIGE, payload);
  const executionResult = output.executionResult || executionResultFromStageOutput(output, {
    specialist: SPECIALISTS.PAIGE,
    transactionId: ctx.transactionId,
  });
  assertExecutionResult(executionResult, {
    specialist: SPECIALISTS.PAIGE,
    requireContributions: true,
    requireEvidence: false,
  });
  output.executionResult = executionResult;
}

function commitMaxPrioritizationStage({
  engine,
  mission,
  tenantId,
  output,
  transactionId,
  missionVersion,
}) {
  const missionId = (mission && mission.id) || output.missionId;
  const { prioritizationPayload } = output;
  const payload = { ...prioritizationPayload, transactionId };

  const contribution = engine.contribute(
    missionId,
    {
      specialist: SPECIALISTS.MAX,
      kind: CONTRIBUTION_KINDS.PRIORITIZATION,
      payload,
    },
    { tenantId }
  );

  const updated = engine.get(missionId, tenantId);
  bumpMissionVersion(updated, transactionId);
  engine.store.putMission(updated);
  engine.store.addEvent(createEvent({
    missionId,
    kind: EVENT_KINDS.EXECUTION_COMMITTED,
    specialist: SPECIALISTS.MAX,
    label: 'Max prioritization committed',
    payload: {
      transactionId,
      missionVersion: updated.version,
      priorVersion: missionVersion,
      contributionId: contribution.contribution.id,
    },
  }));

  const snapshot = engine.inspect(missionId, { tenantId });
  assertMissionStateConsistent(snapshot.mission, {
    contributions: snapshot.contributions,
  });
  return {
    prioritization: contribution.contribution,
    snapshot,
  };
}

function commitPaigeVariantsStage({
  engine,
  mission,
  tenantId,
  output,
  transactionId,
  missionVersion,
}) {
  const missionId = (mission && mission.id) || output.missionId;
  const { variantsPayload } = output;
  const payload = { ...variantsPayload, transactionId };

  const contribution = engine.contribute(
    missionId,
    {
      specialist: SPECIALISTS.PAIGE,
      kind: CONTRIBUTION_KINDS.VARIANTS,
      payload,
    },
    { tenantId }
  );

  const updated = engine.get(missionId, tenantId);
  bumpMissionVersion(updated, transactionId);
  engine.store.putMission(updated);
  engine.store.addEvent(createEvent({
    missionId,
    kind: EVENT_KINDS.EXECUTION_COMMITTED,
    specialist: SPECIALISTS.PAIGE,
    label: 'Paige variants committed',
    payload: {
      transactionId,
      missionVersion: updated.version,
      priorVersion: missionVersion,
      contributionId: contribution.contribution.id,
    },
  }));

  const snapshot = engine.inspect(missionId, { tenantId });
  assertMissionStateConsistent(snapshot.mission, {
    contributions: snapshot.contributions,
  });
  return {
    variants: contribution.contribution,
    snapshot,
  };
}

function validateMaxPrioritizationPreconditions({ mission, engine, tenantId }) {
  if (!mission) throw planningError('tme_mission_missing', 'Mission does not exist.');
  if (mission.planCancelled === true || /cancelled/i.test(String(mission.status || ''))) {
    throw planningError('tme_mission_inactive', 'Mission is not active.');
  }
  if (!isStructuredMissionApproved(mission)) {
    throw planningError('tme_plan_missing', 'Mission Plan missing.');
  }
  const snapshot = engine.inspect(mission.id, { tenantId });
  if (mission.stage !== STAGES.UNDERSTAND) {
    throw planningError('tme_wrong_stage', `Max prioritization requires stage ${STAGES.UNDERSTAND}.`);
  }
  if (!findPrioritizationApproval(snapshot.contributions || [])) {
    throw planningError('tme_prioritization_not_approved', 'Operator prioritization approval is required.');
  }
  if (!findLatestScoutDiscovery(snapshot.contributions || [])) {
    throw planningError('tme_discovery_missing', 'Scout discovery is required before Max prioritization.');
  }
  if (findMaxPrioritization(snapshot.contributions || [])) {
    throw planningError('tme_already_executed', 'Max prioritization already committed.');
  }
  return {
    missionExists: true,
    missionActive: true,
    missionLocked: true,
    structuredPlanApproved: true,
    specialistAvailable: true,
    requiredEvidencePresent: true,
  };
}

function validatePaigePreconditions({ mission, engine, tenantId }) {
  if (!mission) throw planningError('tme_mission_missing', 'Mission does not exist.');
  if (mission.planCancelled === true || /cancelled/i.test(String(mission.status || ''))) {
    throw planningError('tme_mission_inactive', 'Mission is not active.');
  }
  const snapshot = engine.inspect(mission.id, { tenantId });
  const ctx = specialistContext(snapshot.contributions || []);
  if (!ctx.maxComplete) {
    throw planningError('tme_max_incomplete', 'Max prioritization is required before Paige execution.');
  }
  if (ctx.paigeComplete) {
    throw planningError('tme_already_executed', 'Paige variants already committed.');
  }
  if (mission.stage !== STAGES.PREPARE) {
    throw planningError('tme_wrong_stage', `Paige execution requires stage ${STAGES.PREPARE}.`);
  }
  return {
    missionExists: true,
    missionActive: true,
    missionLocked: true,
    structuredPlanApproved: true,
    specialistAvailable: true,
    requiredEvidencePresent: true,
  };
}

function ensureStagesForPaige(engine, missionId, tenantId) {
  let mission = engine.get(missionId, tenantId);
  const snapshot = engine.inspect(missionId, { tenantId });
  const ctx = specialistContext(snapshot.contributions || []);

  if (mission.stage === STAGES.UNDERSTAND && ctx.maxComplete) {
    const planGate = canEnter(STAGES.PLAN, ctx);
    if (!planGate.ok) throw planningError('tme_stage_blocked', planGate.reason);
    engine.progress(missionId, { role: 'max' }, { tenantId, stage: STAGES.PLAN });
    mission = engine.get(missionId, tenantId);
  }

  const afterPlan = engine.inspect(missionId, { tenantId });
  const ctxAfterPlan = specialistContext(afterPlan.contributions || []);
  if (mission.stage === STAGES.PLAN && ctxAfterPlan.maxComplete) {
    const prepareGate = canEnter(STAGES.PREPARE, ctxAfterPlan);
    if (!prepareGate.ok) throw planningError('tme_stage_blocked', prepareGate.reason);
    engine.progress(missionId, { role: 'max' }, { tenantId, stage: STAGES.PREPARE });
    mission = engine.get(missionId, tenantId);
  }

  return mission;
}

/**
 * Execute Max prioritization at UNDERSTAND and commit PRIORITIZATION contribution.
 * @param {object} input
 * @returns {Promise<object>}
 */
async function advanceMaxPrioritization(input = {}) {
  const { engine, mission, tenantId, operatorId } = input;
  if (!engine || !mission) throw new Error('engine and mission are required');

  const snapshot = engine.inspect(mission.id, { tenantId });
  const existing = findMaxPrioritization(snapshot.contributions || []);
  if (existing) {
    return {
      alreadyExecuted: true,
      prioritization: existing,
      snapshot: engine.inspect(mission.id, { tenantId }),
    };
  }

  const staged = await executeMissionStage({
    engine,
    missionId: mission.id,
    tenantId,
    pool: input.pool,
    specialist: SPECIALISTS.MAX,
    stage: STAGES.UNDERSTAND,
    operatorId,
    validatePreconditions: (ctx) => validateMaxPrioritizationPreconditions(ctx),
    execute: async ({ mission: current, transactionId }) => {
      const maxResult = await runMaxForAmoMission(current, {
        ...input,
        transactionId,
      });
      const executionResult = maxResult && maxResult.spec === 'SPEC-132'
        ? maxResult
        : executionResultFromStageOutput(
          { maxResult, prioritizationPayload: maxResult && maxResult.contributions },
          { specialist: SPECIALISTS.MAX, transactionId }
        );
      const prioritizationPayload =
        maxResult
        && maxResult.status === EXECUTION_STATUSES.SUCCESS
        && maxResult.contributions
        && Object.keys(maxResult.contributions).length
          ? prioritizationPayloadFromMaxResult(maxResult)
          : null;
      return {
        maxResult,
        prioritizationPayload,
        executionResult,
        missionId: current.id,
      };
    },
    validateOutput: validatePrioritizationOutput,
    commit: (ctx) => commitMaxPrioritizationStage(ctx),
    persistDurable: bindPersistDurable(input, engine, tenantId),
  });

  return {
    alreadyExecuted: false,
    prioritization: staged.commitResult.prioritization,
    snapshot: staged.commitResult.snapshot,
    transactionId: staged.transactionId,
    missionVersion: staged.missionVersion,
  };
}

/**
 * Execute Paige at PREPARE via SEC and commit VARIANTS contribution.
 * Canonical path: CER → router → executeMissionStage → executeSpecialist('paige') → VARIANTS.
 * @param {object} input
 * @returns {Promise<object>}
 */
async function advancePaigeVariants(input = {}) {
  const { engine, mission, tenantId, operatorId, question } = input;
  if (!engine || !mission) throw new Error('engine and mission are required');

  let currentMission = ensureStagesForPaige(engine, mission.id, tenantId);
  const snapshot = engine.inspect(currentMission.id, { tenantId });
  const existing = findPaigeVariants(snapshot.contributions || []);
  if (existing) {
    return {
      alreadyExecuted: true,
      variants: existing,
      snapshot: engine.inspect(currentMission.id, { tenantId }),
      executionOutcome: 'completed',
    };
  }

  const staged = await executeMissionStage({
    engine,
    missionId: currentMission.id,
    tenantId,
    pool: input.pool,
    specialist: SPECIALISTS.PAIGE,
    stage: STAGES.PREPARE,
    operatorId,
    validatePreconditions: (ctx) => validatePaigePreconditions(ctx),
    execute: async ({ mission: current, transactionId }) => {
      const contributions = engine.inspect(current.id, { tenantId }).contributions || [];
      const variantsPayload = await runPaigeForAmoMission(current, {
        ...input,
        contributions,
        transactionId,
        executionContext: {
          stage: STAGES.PREPARE,
          missionId: current.id,
          tenantId,
          executionRequestId: input.executionRequest?.id || null,
        },
      });
      const executionResult = await executeSpecialist({
        mission: current,
        contributions,
        specialist: SPECIALISTS.PAIGE,
        transactionId,
        run: async () => ({
          spec: 'SPEC-132',
          status: EXECUTION_STATUSES.SUCCESS,
          confidence: { overall: 0.75, evidence: 0.7, fit: 0.8, completeness: 0.75 },
          evidence: [{
            id: 'ev_paige_0',
            label: 'Max prioritization consumed for messaging',
            source: 'max_prioritization',
            timestamp: new Date().toISOString(),
            provenance: { kind: 'upstream_intelligence', source: 'max' },
          }],
          contributions: variantsPayload,
          recommendations: [{ tier: 'suggested', text: 'Review variants before operator approval.' }],
          unknowns: [],
          nextActions: [{ kind: 'operator_review', label: 'Operator review variants' }],
        }),
      });

      if (executionResult.status === EXECUTION_STATUSES.BLOCKED
        || executionResult.status === EXECUTION_STATUSES.FAILED) {
        return {
          variantsPayload: null,
          executionResult,
          missionId: current.id,
          blocked: true,
        };
      }

      return {
        variantsPayload,
        executionResult,
        missionId: current.id,
        question,
      };
    },
    validateOutput: (output, ctx) => {
      if (output.blocked) {
        throw validationError(
          'tme_paige_blocked',
          output.executionResult?.blocked?.reason || 'Paige execution blocked.'
        );
      }
      validatePaigeOutput(output, ctx);
    },
    commit: (ctx) => commitPaigeVariantsStage(ctx),
    persistDurable: bindPersistDurable(input, engine, tenantId),
  });

  const finalSnapshot = staged.commitResult.snapshot;
  const executionOutcome = staged.output.blocked ? 'blocked' : 'completed';
  return {
    alreadyExecuted: false,
    variants: staged.commitResult.variants,
    snapshot: finalSnapshot,
    executionOutcome,
    transactionId: staged.transactionId,
    missionVersion: staged.missionVersion,
    executionResult: staged.output.executionResult,
  };
}

/**
 * Execute Emmett at PREPARE via SEC and commit CAPACITY contribution.
 * Canonical path: CER → router → executeMissionStage → executeSpecialist('emmett') → CAPACITY.
 * @param {object} input
 * @returns {Promise<object>}
 */
async function advanceEmmettCapacity(input = {}) {
  const { engine, mission, tenantId, operatorId, question } = input;
  if (!engine || !mission) throw new Error('engine and mission are required');

  const snapshot = engine.inspect(mission.id, { tenantId });
  const existing = findEmmettCapacity(snapshot.contributions || []);
  if (existing) {
    return {
      alreadyExecuted: true,
      capacity: existing,
      snapshot: engine.inspect(mission.id, { tenantId }),
      executionOutcome: 'completed',
    };
  }

  const staged = await executeMissionStage({
    engine,
    missionId: mission.id,
    tenantId,
    pool: input.pool,
    specialist: SPECIALISTS.EMMETT,
    stage: STAGES.PREPARE,
    operatorId,
    validatePreconditions: (ctx) => validateEmmettPreconditions(ctx),
    execute: async ({ mission: current, transactionId }) => {
      const contributions = engine.inspect(current.id, { tenantId }).contributions || [];
      const emmettRun = await runEmmettForAmoMission(current, {
        ...input,
        contributions,
        transactionId,
        executionContext: {
          stage: STAGES.PREPARE,
          missionId: current.id,
          tenantId,
          executionRequestId: input.executionRequest?.id || null,
        },
      });
      const { capacityPayload, assessed, executionInput } = emmettRun;
      const executionResult = await executeSpecialist({
        mission: current,
        contributions,
        specialist: SPECIALISTS.EMMETT,
        transactionId,
        run: async () => buildEmmettExecutionResult(capacityPayload, executionInput, { assessed }),
      });

      if (executionResult.status === EXECUTION_STATUSES.BLOCKED
        || executionResult.status === EXECUTION_STATUSES.FAILED) {
        return {
          capacityPayload: null,
          executionResult,
          executionInput,
          missionId: current.id,
          blocked: true,
        };
      }

      return {
        capacityPayload,
        executionResult,
        executionInput,
        assessed,
        missionId: current.id,
        question,
      };
    },
    validateOutput: (output, ctx) => {
      if (output.blocked) {
        throw validationError(
          'tme_emmett_blocked',
          output.executionResult?.blocked?.reason || 'Emmett execution blocked.'
        );
      }
      validateEmmettCapacityOutput(output, ctx);
    },
    commit: (ctx) => commitEmmettCapacityStage(ctx),
    persistDurable: bindPersistDurable(input, engine, tenantId),
  });

  const finalSnapshot = staged.commitResult.snapshot;
  const executionOutcome = staged.output.blocked ? 'blocked' : 'completed';
  return {
    alreadyExecuted: false,
    capacity: staged.commitResult.capacity,
    snapshot: finalSnapshot,
    executionOutcome,
    transactionId: staged.transactionId,
    missionVersion: staged.missionVersion,
    executionResult: staged.output.executionResult,
  };
}

function validateExecutionPreconditions({ mission, engine, tenantId }) {
  if (!mission) throw planningError('tme_mission_missing', 'Mission does not exist.');
  if (mission.planCancelled === true || /cancelled/i.test(String(mission.status || ''))) {
    throw planningError('tme_mission_inactive', 'Mission is not active.');
  }
  const snapshot = engine.inspect(mission.id, { tenantId });
  if (!hasPendingExecutionApproval(snapshot)) {
    throw planningError('tme_no_pending_execution', 'No execution approval is pending.');
  }
  if (mission.stage && mission.stage !== STAGES.READY) {
    throw planningError('tme_wrong_stage', `Execution approval cannot execute while the mission is at ${mission.stage}.`);
  }
  const ctx = specialistContext(snapshot.contributions || [], { missionId: mission.id, ...snapshot });
  if (ctx.deliverabilityPaused) {
    throw planningError('tme_deliverability_paused', 'Deliverability risk blocks execution approval.');
  }
  const readyGate = canEnter(STAGES.READY, ctx);
  if (!readyGate.ok) {
    throw planningError('tme_execution_not_ready', readyGate.reason);
  }
  return {
    missionExists: true,
    missionActive: true,
    specialistAvailable: true,
  };
}

function commitExecutionApprovalStage({
  engine,
  mission,
  tenantId,
  output,
  transactionId,
  existingApproval,
  executionRequest,
}) {
  const missionId = (mission && mission.id) || output.missionId;
  const contributions = engine.inspect(missionId, { tenantId }).contributions || [];
  let approval = existingApproval;
  if (!approval) {
    const approvalResult = engine.contribute(
      missionId,
      {
        specialist: SPECIALISTS.OPERATOR,
        kind: CONTRIBUTION_KINDS.APPROVAL,
        payload: buildExecutionApprovalPayload(mission, contributions, {
          command: output.question,
          operatorId: output.operatorId,
          executionRequestId: executionRequest?.id || null,
          transactionId,
        }),
      },
      { tenantId }
    );
    approval = approvalResult.contribution;
  }

  const current = engine.get(missionId, tenantId);
  current.pendingOperatorDecision = null;
  engine.store.putMission(current);

  const snapshot = engine.inspect(missionId, { tenantId });
  assertMissionStateConsistent(snapshot.mission, { contributions: snapshot.contributions });
  return { approval, snapshot };
}

/**
 * Consume execution approval at READY. Does not dispatch provider sending.
 * @param {object} input
 * @returns {Promise<object>}
 */
async function advanceExecutionAfterApproval(input = {}) {
  askPathTrace.traceEnter('advanceExecutionAfterApproval');
  const {
    engine,
    mission,
    tenantId,
    question,
    operatorId,
    executionRequest,
  } = input;

  if (!engine || !mission) {
    throw new Error('engine and mission are required');
  }

  let snapshot = engine.inspect(mission.id, { tenantId });
  const contributions = snapshot.contributions || [];
  const existingApproval = findValidExecutionApproval(contributions, mission.id);

  if (existingApproval && snapshot.mission.stage === STAGES.READY) {
    askPathTrace.traceEarlyReturn('advanceExecutionAfterApproval', 'already_executed');
    return {
      alreadyExecuted: true,
      approval: existingApproval,
      snapshot: engine.inspect(mission.id, { tenantId }),
      approvalPhase: APPROVAL_PHASES.STAGE_COMPLETED,
    };
  }

  const staged = await executeMissionStage({
    engine,
    missionId: mission.id,
    tenantId,
    pool: input.pool,
    specialist: SPECIALISTS.OPERATOR,
    stage: 'execution_approval',
    operatorId,
    validatePreconditions: (ctx) => validateExecutionPreconditions(ctx),
    execute: async ({ mission: current }) => ({
      question,
      missionId: current.id,
      operatorId,
    }),
    commit: (ctx) => commitExecutionApprovalStage({
      ...ctx,
      existingApproval,
      executionRequest,
    }),
    persistDurable: bindPersistDurable(input, engine, tenantId),
  });

  snapshot = staged.commitResult.snapshot;
  askPathTrace.traceEarlyReturn('advanceExecutionAfterApproval', 'completed');
  return {
    alreadyExecuted: false,
    approval: staged.commitResult.approval,
    snapshot,
    approvalPhase: APPROVAL_PHASES.STAGE_COMPLETED,
    transactionId: staged.transactionId,
    missionVersion: staged.missionVersion,
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
  DISCOVERY_INVESTIGATION_ACTION,
  PRIORITIZATION_APPROVAL_ACTION,
  EXECUTION_APPROVAL_ACTION,
  PLAN_APPROVAL_ACTION,
  PLAN_CLARIFICATION_ACTION,
  PLAN_CANCEL_ACTION,
  PLAN_EDIT_ACTION,
  buildDelegationFromAmoMission,
  findDiscoveryApproval,
  findPrioritizationApproval,
  findLatestMaxPrioritization,
  findPlanApproval,
  findScoutDiscoveryAfterApproval,
  hasPendingPlanApproval,
  hasPendingPlanClarification,
  hasConsumablePendingDecision,
  presentableOperatorDecision,
  isMissionPlanningTurn,
  hasPendingDiscoveryApproval,
  hasPendingDiscoveryInvestigation,
  hasPendingPrioritizationApproval,
  advancePlanAfterApproval,
  advancePlanClarification,
  cancelMissionPlan,
  beginPlanEdit,
  applyPlanEdits,
  advanceDiscoveryAfterApproval,
  advanceDiscoveryInvestigationAfterApproval,
  advancePrioritizationAfterApproval,
  advanceMaxPrioritization,
  advancePaigeVariants,
  advanceEmmettCapacity,
  advanceExecutionAfterApproval,
  advanceExecuteOutbound: require('./EmmettOutboundExecution').advanceExecuteOutbound,
  validateDiscoveryPreconditions,
  buildDiscoveryApprovalProse,
  mapScoutIntelligenceToDiscoveryPayload,
  fixtureScoutDiscoveryResult,
  fixtureMaxPrioritizationResult,
  fixturePaigeVariantsResult,
  findMaxPrioritization,
  findPaigeVariants,
  runMaxPrioritizationForAmoMission,
  runPaigeForAmoMission,
  buildMaxPrioritizationPayload,
  buildPaigeVariantsPayload,
  ensureStagesForPaige,
};
