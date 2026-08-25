'use strict';

/**
 * SPEC-128 / SPEC-131 / SPEC-136 — Operator approval consumes pending decisions
 * only when the stage commits. pendingOperatorDecision must match execution predicates.
 */

const amo = require('../../acquisition-mission');
const { Scout } = require('../../scout');

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
} = amo;
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
const { isMissionPlanningTurn } = require('./MissionPlanningTurn');
const { OPERATOR_DECISION_KINDS } = amo;
const {
  hasPendingPlanClarification,
  hasPendingPlanApproval,
  hasPendingDiscoveryApproval,
  hasPendingPrioritizationApproval,
  hasConsumablePendingDecision,
  presentableOperatorDecision,
  assertMissionStateConsistent,
} = require('../../acquisition-mission/PendingOperatorDecision');

const DISCOVERY_APPROVAL_ACTION = 'discovery_approved';
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
    const result = await Scout.discover({
      mission,
      missionEngine: opts.missionEngine,
      scoutPayload: {},
      operatorId: opts.operatorId,
      opts: {
        ...opts,
        delegation,
        mode: opts.scoutMode || 'completed',
        missionId: mission.id,
        amoMissionId: mission.id,
        tenantId: delegation.tenantId,
        companies: opts.scoutCompanies,
        people: opts.scoutPeople,
        discover: opts.discover,
        enablePlaces: opts.enablePlaces,
        allowFixtureFallback: opts.allowFixtureFallback,
      },
    });
    const intelligenceResult = result.intelligenceResult || result;
    const mapped = mapScoutIntelligenceToDiscoveryPayload(intelligenceResult, {
      missionObjective: mission.objective,
    });
    if (
      opts.allowFixtureFallback === true &&
      (mapped.blocked || mapped.qualifiedCount <= 0)
    ) {
      return fixtureScoutDiscoveryResult();
    }
    return intelligenceResult;
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

function validateDiscoveryPreconditions({ mission, engine, tenantId }) {
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
  return {
    missionExists: true,
    missionActive: true,
    missionLocked: true,
    structuredPlanApproved: true,
    specialistAvailable: true,
    requiredEvidencePresent: true,
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
}) {
  const missionId = (mission && mission.id) || output.missionId;
  const { question, discoveryPayload } = output;
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
          action: DISCOVERY_APPROVAL_ACTION,
          stage: STAGES.DISCOVER,
          transactionId,
        },
      },
      { tenantId }
    );
    approval = approvalResult.contribution;
    clearPendingOperatorDecision(engine, approvalResult.mission);
  } else {
    const current = engine.get(missionId, tenantId);
    clearPendingOperatorDecision(engine, current);
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
  updated.pendingOperatorDecision = {
    stage: STAGES.DISCOVER,
    kind: OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL,
    prompt: 'Approve prioritization?',
  };
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
      specialist: SPECIALISTS.SCOUT,
      stage: STAGES.DISCOVER,
      operatorId,
      validatePreconditions: (ctx) => validateDiscoveryPreconditions(ctx),
      execute: async ({ mission: current, transactionId }) => {
        const scoutResult = await runScoutForAmoMission(current, {
          question,
          operatorId,
          missionEngine,
          persist,
          runScout,
          scoutCompanies,
          scoutPeople,
          allowFixtureFallback,
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
  const { question } = output;
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

  engine.progress(missionId, SPECIALISTS.OPERATOR, {
    tenantId,
    stage: STAGES.UNDERSTAND,
  });

  const updated = engine.get(missionId, tenantId);
  bumpMissionVersion(updated, transactionId);
  engine.store.putMission(updated);
  engine.store.addEvent(createEvent({
    missionId,
    kind: EVENT_KINDS.EXECUTION_COMMITTED,
    specialist: SPECIALISTS.OPERATOR,
    label: 'Prioritization approved — advancing to Understanding',
    payload: {
      transactionId,
      missionVersion: updated.version,
      priorVersion: missionVersion,
      approvalId: approval.id,
    },
  }));

  const snapshot = engine.inspect(missionId, { tenantId });
  assertMissionStateConsistent(snapshot.mission, {
    contributions: snapshot.contributions,
  });
  return {
    approval,
    snapshot,
  };
}

/**
 * Consume prioritization approval and advance discover → understand.
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
  } = input;

  if (!engine || !mission) {
    throw new Error('engine and mission are required');
  }

  let snapshot = engine.inspect(mission.id, { tenantId });
  const contributions = snapshot.contributions || [];
  const existingApproval = findPrioritizationApproval(contributions);

  if (existingApproval && snapshot.mission.stage === STAGES.UNDERSTAND) {
    askPathTrace.traceEarlyReturn('advancePrioritizationAfterApproval', 'already_executed');
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
    specialist: SPECIALISTS.OPERATOR,
    stage: 'prioritization_approval',
    operatorId,
    validatePreconditions: (ctx) => validatePrioritizationPreconditions(ctx),
    execute: async ({ mission: current }) => ({
      question,
      missionId: current.id,
    }),
    commit: (ctx) => commitPrioritizationStage({
      ...ctx,
      existingApproval,
    }),
    persistDurable: bindPersistDurable(input, engine, tenantId),
  });

  snapshot = staged.commitResult.snapshot;
  askPathTrace.traceEarlyReturn('advancePrioritizationAfterApproval', 'completed');
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
  PRIORITIZATION_APPROVAL_ACTION,
  PLAN_APPROVAL_ACTION,
  PLAN_CLARIFICATION_ACTION,
  PLAN_CANCEL_ACTION,
  PLAN_EDIT_ACTION,
  buildDelegationFromAmoMission,
  findDiscoveryApproval,
  findPrioritizationApproval,
  findPlanApproval,
  findScoutDiscoveryAfterApproval,
  hasPendingPlanApproval,
  hasPendingPlanClarification,
  hasConsumablePendingDecision,
  presentableOperatorDecision,
  isMissionPlanningTurn,
  hasPendingDiscoveryApproval,
  hasPendingPrioritizationApproval,
  advancePlanAfterApproval,
  advancePlanClarification,
  cancelMissionPlan,
  beginPlanEdit,
  applyPlanEdits,
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
  buildDiscoveryApprovalProse,
  mapScoutIntelligenceToDiscoveryPayload,
  fixtureScoutDiscoveryResult,
};
