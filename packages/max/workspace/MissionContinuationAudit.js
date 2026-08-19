'use strict';

/**
 * AUDIT-002 — Mission Continuation & Delegation instrumentation.
 *
 * Observes Workspace ask() routing. Does not advance stages, invoke Scout,
 * or change which pipeline produces the operator-facing response.
 *
 * Required order (observational):
 *   WorkspaceEngine.ask() → Load Active Mission → Continuation Evaluation
 *   → log recommended pipeline → existing ask() handlers continue unchanged.
 */

const CHECKPOINTS = Object.freeze({
  MISSION_CREATED: 'MISSION_CREATED',
  MISSION_LOADED: 'MISSION_LOADED',
  MISSION_STAGE: 'MISSION_STAGE',
  MISSION_TRANSITION: 'MISSION_TRANSITION',
  MISSION_APPROVAL: 'MISSION_APPROVAL',
  MISSION_DELEGATE: 'MISSION_DELEGATE',
  MISSION_RESULT: 'MISSION_RESULT',
  MISSION_RESPONSE: 'MISSION_RESPONSE',
  ACTIVE_MISSION_FOUND: 'ACTIVE_MISSION_FOUND',
});

const PIPELINES = Object.freeze({
  MISSION_ENGINE: 'mission_engine',
  GENERAL_REASONING: 'general_reasoning',
});

const INTENTS = Object.freeze({
  OPERATOR_APPROVAL: 'operator_approval',
  MISSION_STATUS: 'mission_status',
  CURRENT_OBJECTIVE: 'current_objective',
  MODIFY_MISSION: 'modify_mission',
  CANCEL_OR_PAUSE: 'cancel_or_pause',
  NEW_MISSION: 'new_mission',
  GENERAL_QUESTION: 'general_question',
});

const APPROVAL_RE =
  /\b(approved|approve(?:d)?(?:\s+(?:this|that|the|it))?(?:\s+mission)?|looks good|lgtm|go ahead|proceed)\b/i;
const BEGIN_MISSION_RE =
  /\b(begin|start|run|continue|execute)\b.{0,48}\bmission\b/i;
const STATUS_RE =
  /\b(mission status|where are we|show(?:\s+me)?(?:\s+the)?\s+progress|how is (?:the )?mission|what(?:'s| is) blocking)\b/i;
const OBJECTIVE_RE =
  /\b(current objective|what are we (?:doing|working on)|why is this mission|why (?:does|do) this mission exist)\b/i;
const MODIFY_RE =
  /\b(change|update|modify|instead of|increase (?:the )?target|remove |switch to)\b/i;
const CANCEL_RE =
  /\b(cancel|pause|stop|abandon|hold)\b.{0,24}\bmission\b/i;
const NEW_MISSION_RE =
  /\b(new mission|start over|create another campaign|different objective|start a new campaign)\b/i;

function nowIso(value) {
  if (value) return new Date(value).toISOString();
  return new Date().toISOString();
}

function asText(value) {
  return value == null ? '' : String(value).trim();
}

function resolveTenantId(input = {}) {
  const session = input.session || null;
  const sessionCtx =
    session && session.context && typeof session.context === 'object'
      ? session.context
      : {};
  const envelope =
    input.context && typeof input.context === 'object' ? input.context : {};
  return asText(
    input.tenantId ||
      input.authorizedTenantId ||
      envelope.tenantId ||
      sessionCtx.tenantId ||
      envelope.clientId ||
      sessionCtx.clientId
  );
}

function resolveWorkspace(input = {}) {
  const session = input.session || null;
  const envelope =
    input.context && typeof input.context === 'object' ? input.context : {};
  const sessionCtx =
    session && session.context && typeof session.context === 'object'
      ? session.context
      : {};
  return (
    asText(envelope.workspaceId || sessionCtx.workspaceId) ||
    asText(session && session.id) ||
    resolveTenantId(input) ||
    null
  );
}

function resolveOperator(input = {}) {
  const session = input.session || null;
  const envelope =
    input.context && typeof input.context === 'object' ? input.context : {};
  const sessionCtx =
    session && session.context && typeof session.context === 'object'
      ? session.context
      : {};
  return (
    asText(input.operator || input.operatorId) ||
    asText(session && session.operator) ||
    asText(envelope.operatorId || sessionCtx.operatorId) ||
    null
  );
}

function explicitMissionId(input = {}) {
  const session = input.session || null;
  const sessionCtx =
    session && session.context && typeof session.context === 'object'
      ? session.context
      : {};
  const envelope =
    input.context && typeof input.context === 'object' ? input.context : {};
  return asText(
    input.missionId ||
      envelope.missionId ||
      envelope.acquisitionMissionId ||
      sessionCtx.missionId ||
      sessionCtx.acquisitionMissionId
  );
}

function emit(kind, payload = {}, log = null) {
  const row = {
    audit: 'AUDIT-002',
    kind,
    timestamp: payload.timestamp || nowIso(),
    missionId: payload.missionId || null,
    workspace: payload.workspace || null,
    stage: payload.stage != null ? payload.stage : null,
    selectedCapability: payload.selectedCapability || payload.capability || null,
    outcome: payload.outcome != null ? payload.outcome : null,
  };
  for (const [key, value] of Object.entries(payload)) {
    if (row[key] === undefined) row[key] = value;
  }
  if (log && Array.isArray(log.records)) log.records.push(row);
  if (!log || log.console !== false) {
    console.info(`[AUDIT-002] ${kind} ${JSON.stringify(row)}`);
  }
  return row;
}

function createAuditLog(opts = {}) {
  const records = opts.records || [];
  return {
    records,
    console: opts.console === true,
    emit(kind, payload) {
      return emit(kind, payload, { records, console: opts.console === true });
    },
  };
}

/**
 * Classify the operator message against an optional active Mission.
 * Approval is detected from language, not from cognitive-mode execution verbs.
 */
function classifyOperatorIntent(question, mission = null) {
  const q = asText(question);
  if (!q) {
    return {
      intent: INTENTS.GENERAL_QUESTION,
      confidence: 0,
      reasoning: 'empty_message',
    };
  }

  if (NEW_MISSION_RE.test(q)) {
    return {
      intent: INTENTS.NEW_MISSION,
      confidence: 0.95,
      reasoning: 'explicit_new_mission_language',
    };
  }

  if (CANCEL_RE.test(q)) {
    return {
      intent: INTENTS.CANCEL_OR_PAUSE,
      confidence: 0.92,
      reasoning: 'cancel_or_pause_language',
    };
  }

  const approved = APPROVAL_RE.test(q);
  const beginMission = BEGIN_MISSION_RE.test(q);
  if (approved && beginMission) {
    return {
      intent: INTENTS.OPERATOR_APPROVAL,
      confidence: 0.96,
      reasoning: 'operator_approved_and_requested_mission_start',
    };
  }
  if (approved && mission) {
    return {
      intent: INTENTS.OPERATOR_APPROVAL,
      confidence: 0.9,
      reasoning: 'operator_approval_with_active_mission',
    };
  }
  if (beginMission && mission) {
    return {
      intent: INTENTS.OPERATOR_APPROVAL,
      confidence: 0.88,
      reasoning: 'begin_mission_with_active_mission',
    };
  }

  if (STATUS_RE.test(q)) {
    return {
      intent: INTENTS.MISSION_STATUS,
      confidence: 0.9,
      reasoning: 'mission_status_request',
    };
  }

  if (OBJECTIVE_RE.test(q)) {
    return {
      intent: INTENTS.CURRENT_OBJECTIVE,
      confidence: 0.88,
      reasoning: 'current_objective_question',
    };
  }

  if (mission && MODIFY_RE.test(q)) {
    return {
      intent: INTENTS.MODIFY_MISSION,
      confidence: 0.8,
      reasoning: 'modify_active_mission',
    };
  }

  if (approved && !mission) {
    return {
      intent: INTENTS.OPERATOR_APPROVAL,
      confidence: 0.7,
      reasoning: 'approval_language_without_loaded_mission',
    };
  }

  return {
    intent: INTENTS.GENERAL_QUESTION,
    confidence: 0.4,
    reasoning: 'no_mission_continuation_cues',
  };
}

function selectCapability(mission) {
  if (!mission) return null;
  const stage = asText(mission.stage || (mission.progress && mission.progress.currentStage)).toLowerCase();
  if (!stage || stage === 'discover' || stage === 'requested' || stage === 'planning') {
    return 'scout';
  }
  if (stage === 'understand') return 'max';
  if (stage === 'plan' || stage === 'prepare') return 'paige';
  if (stage === 'ready') return 'emmett';
  if (stage === 'execute' || stage === 'observe') return 'emmett';
  return 'scout';
}

function expectedNextStage(mission) {
  if (!mission) return null;
  const stage = asText(mission.stage).toLowerCase();
  if (stage === 'discover') return 'understand';
  return stage || null;
}

/**
 * When an active Mission exists, continue it unless the operator explicitly
 * starts a new objective.
 */
function evaluateContinuation(question, mission) {
  const classified = classifyOperatorIntent(question, mission);
  if (!mission) {
    return {
      continueMission: false,
      pipelineSelected: PIPELINES.GENERAL_REASONING,
      reason: classified.intent === INTENTS.OPERATOR_APPROVAL
        ? 'approval_detected_but_no_active_mission'
        : 'no_active_mission',
      classified,
      capability: null,
    };
  }
  if (classified.intent === INTENTS.NEW_MISSION) {
    return {
      continueMission: false,
      pipelineSelected: PIPELINES.GENERAL_REASONING,
      reason: 'explicit_new_objective',
      classified,
      capability: null,
    };
  }
  return {
    continueMission: true,
    pipelineSelected: PIPELINES.MISSION_ENGINE,
    reason: classified.reasoning,
    classified,
    capability: selectCapability(mission),
  };
}

function tryAcquisitionService(input = {}) {
  if (input.acquisitionMissionService) return input.acquisitionMissionService;
  if (input.acquisitionMissionEngine) return null;
  try {
    return require('../../../services/acquisitionMission');
  } catch (_) {
    return null;
  }
}

/**
 * Load the active SPEC-118 Acquisition Mission for this workspace/session.
 */
async function loadActiveAcquisitionMission(input = {}) {
  const tenantId = resolveTenantId(input);
  const missionId = explicitMissionId(input);
  const service = tryAcquisitionService(input);
  const engine =
    input.acquisitionMissionEngine ||
    (service && typeof service.getEngine === 'function' ? service.getEngine() : null);

  if (tenantId && service && typeof service.hydrateTenant === 'function' && input.persist !== false) {
    try {
      await service.hydrateTenant(tenantId, {
        persist: input.persist,
        engine,
        pool: input.pool,
      });
    } catch (_) {
      /* hydrate is best-effort during audit */
    }
  }

  if (missionId && engine && typeof engine.get === 'function') {
    try {
      const mission = engine.get(missionId, tenantId || undefined);
      if (mission) {
        return {
          mission,
          source: 'explicit_id',
          tenantId,
          reason: null,
        };
      }
    } catch (err) {
      return {
        mission: null,
        source: 'explicit_id',
        tenantId,
        reason: err && err.message ? err.message : 'explicit_id_failed',
      };
    }
  }

  if (tenantId && service && typeof service.activeMissionFor === 'function') {
    try {
      const mission = service.activeMissionFor(tenantId, { engine, persist: false });
      if (mission) {
        return { mission, source: 'active_for_tenant', tenantId, reason: null };
      }
    } catch (_) {
      /* fall through */
    }
  }

  if (tenantId && engine && typeof engine.list === 'function') {
    const list = engine.list(tenantId) || [];
    const mission =
      list.find((row) => row && row.stage && row.stage !== 'improve') || list[0] || null;
    if (mission) {
      return { mission, source: 'engine_list', tenantId, reason: null };
    }
  }

  return {
    mission: null,
    source: 'none',
    tenantId,
    reason: tenantId ? 'no_mission_in_store' : 'no_tenant',
  };
}

async function loadOrchestrationMission(input = {}) {
  const resolver = input.resolver;
  const session = input.session;
  if (!resolver || !session || !session.id) return null;
  if (typeof resolver.resolveActiveMission !== 'function') return null;
  try {
    return await resolver.resolveActiveMission(session.id);
  } catch (_) {
    return null;
  }
}

function inferActualPipeline(result) {
  if (!result || typeof result !== 'object') return 'unknown';
  if (result.scoutLoop) return 'scout_loop';
  if (result.route === 'mission' && result.mission) return PIPELINES.MISSION_ENGINE;
  if (result.retrieval) return PIPELINES.GENERAL_REASONING;
  const reason = result.domainDecision && result.domainDecision.reason;
  if (reason && /client_intelligence|retrieval|reasoning|general_conversation/i.test(String(reason))) {
    return PIPELINES.GENERAL_REASONING;
  }
  if (result.route === 'intelligence') return PIPELINES.GENERAL_REASONING;
  return result.route || 'unknown';
}

function buildDelegationPayload(mission, capability) {
  if (!mission || !capability) return null;
  return {
    capability,
    missionId: mission.id,
    objective: mission.objective || mission.objectiveText || null,
    targetSegment: mission.targetSegment || null,
    campaign: mission.campaign || null,
    stage: mission.stage || null,
    constraints: mission.constraints || [],
  };
}

/**
 * Observe one Workspace ask() before the general reasoning pipeline.
 * Logs continuation evidence. Does not divert ask() routing.
 */
async function evaluateWorkspaceMissionContinuation(input = {}) {
  const log = input.log || createAuditLog({ console: input.console === true, records: input.records });
  const question = asText(input.question);
  const workspace = resolveWorkspace(input);
  const operator = resolveOperator(input);
  const timestamp = nowIso(input.now);

  const loaded = await loadActiveAcquisitionMission(input);
  const orchestration = await loadOrchestrationMission(input);
  const mission = loaded.mission || null;

  emit(
    CHECKPOINTS.ACTIVE_MISSION_FOUND,
    {
      timestamp,
      missionId: mission ? mission.id : null,
      workspace,
      operator,
      tenantId: loaded.tenantId || null,
      stage: mission ? mission.stage : null,
      spec: mission ? 'SPEC-118' : orchestration ? 'SPEC-022' : null,
      found: Boolean(mission),
      orchestrationMissionId: orchestration && orchestration.id ? orchestration.id : null,
      loadSource: loaded.source,
      loadReason: loaded.reason,
      sessionBound: Boolean(explicitMissionId(input)),
      outcome: mission ? 'found' : 'not_found',
    },
    log
  );

  if (mission) {
    emit(
      CHECKPOINTS.MISSION_LOADED,
      {
        timestamp,
        missionId: mission.id,
        workspace,
        operator,
        tenantId: loaded.tenantId || mission.tenantId || null,
        stage: mission.stage,
        status: mission.status,
        source: loaded.source,
        persisted: Boolean(mission.id && mission.createdAt),
        outcome: 'loaded',
      },
      log
    );
    emit(
      CHECKPOINTS.MISSION_STAGE,
      {
        timestamp,
        missionId: mission.id,
        workspace,
        stage: mission.stage,
        status: mission.status,
        expectedNextStage: expectedNextStage(mission),
        outcome: 'observed',
      },
      log
    );
  }

  const continuation = evaluateContinuation(question, mission);
  const classified = continuation.classified;

  emit(
    CHECKPOINTS.MISSION_APPROVAL,
    {
      timestamp,
      missionId: mission ? mission.id : null,
      workspace,
      stage: mission ? mission.stage : null,
      intent: classified.intent,
      confidence: classified.confidence,
      reasoning: classified.reasoning,
      outcome:
        classified.intent === INTENTS.OPERATOR_APPROVAL ? 'operator_approval' : classified.intent,
    },
    log
  );

  const snapshot = {
    audit: 'AUDIT-002',
    timestamp,
    missionId: mission ? mission.id : null,
    workspace,
    operator,
    tenantId: loaded.tenantId || (mission && mission.tenantId) || null,
    stage: mission ? mission.stage : null,
    status: mission ? mission.status : null,
    activeMissionFound: Boolean(mission),
    loadSource: loaded.source,
    loadReason: loaded.reason,
    orchestrationMissionId: orchestration && orchestration.id ? orchestration.id : null,
    continuationEvaluated: true,
    continueMission: continuation.continueMission,
    continuationDecision: continuation.continueMission ? 'continue' : 'bypass',
    reason: continuation.reason,
    pipelineSelected: continuation.pipelineSelected,
    recommendedPipeline: continuation.pipelineSelected,
    intent: classified.intent,
    confidence: classified.confidence,
    intentReasoning: classified.reasoning,
    selectedCapability: continuation.capability,
    delegationPayload: buildDelegationPayload(mission, continuation.capability),
    previousStage: mission ? mission.stage : null,
    nextStage: continuation.continueMission && classified.intent === INTENTS.OPERATOR_APPROVAL
      ? expectedNextStage(mission)
      : null,
    transitionReason:
      continuation.continueMission && classified.intent === INTENTS.OPERATOR_APPROVAL
        ? 'operator_approval'
        : null,
    scoutInvoked: false,
    stageAdvanced: false,
    resultsPersisted: false,
    responseFromMission: false,
    bypassJustification:
      !continuation.continueMission && mission
        ? continuation.reason
        : continuation.continueMission
          ? 'continuation_recommended_ask_does_not_divert'
          : continuation.reason,
  };

  return {
    snapshot,
    mission,
    orchestration,
    continuation,
    classified,
    log,
  };
}

function observeDelegation(input = {}) {
  const log = input.log;
  return emit(
    CHECKPOINTS.MISSION_DELEGATE,
    {
      timestamp: nowIso(input.now),
      missionId: input.missionId || null,
      workspace: input.workspace || null,
      stage: input.stage || null,
      capability: input.capability || 'scout',
      selectedCapability: input.capability || 'scout',
      reason: input.reason || null,
      missionContext: input.missionContext || null,
      delegationPayload: input.delegationPayload || null,
      outcome: input.outcome || 'not_attempted',
      invoked: input.invoked === true,
      succeeded: input.succeeded === true,
      failed: input.failed === true,
      timeout: input.timeout === true,
      exception: input.exception || null,
    },
    log
  );
}

function observeResult(input = {}) {
  const log = input.log;
  return emit(
    CHECKPOINTS.MISSION_RESULT,
    {
      timestamp: nowIso(input.now),
      missionId: input.missionId || null,
      workspace: input.workspace || null,
      stage: input.stage || null,
      capability: input.capability || 'scout',
      prospectCount: input.prospectCount != null ? input.prospectCount : null,
      confidence: input.confidence != null ? input.confidence : null,
      evidence: input.evidence || null,
      attached: input.attached === true,
      outcome: input.attached === true ? 'attached' : 'discarded_or_absent',
    },
    log
  );
}

function observeResponse(input = {}) {
  const log = input.log;
  return emit(
    CHECKPOINTS.MISSION_RESPONSE,
    {
      timestamp: nowIso(input.now),
      missionId: input.missionId || null,
      workspace: input.workspace || null,
      stage: input.stage || null,
      selectedCapability: input.capability || null,
      composedFrom: input.composedFrom || null,
      actualPipeline: input.actualPipeline || null,
      outcome: input.outcome || null,
    },
    log
  );
}

function observeCreated(mission, input = {}) {
  return emit(
    CHECKPOINTS.MISSION_CREATED,
    {
      timestamp: (mission && mission.createdAt) || nowIso(input.now),
      missionId: mission && mission.id,
      workspace: input.workspace || (mission && mission.tenantId) || null,
      operator: input.operator || (mission && mission.owner) || null,
      tenantId: mission && mission.tenantId,
      stage: mission && mission.stage,
      status: mission && mission.status,
      persisted: input.persisted !== false,
      outcome: 'created',
    },
    input.log
  );
}

function observeTransition(input = {}) {
  return emit(
    CHECKPOINTS.MISSION_TRANSITION,
    {
      timestamp: nowIso(input.now),
      missionId: input.missionId || null,
      workspace: input.workspace || null,
      stage: input.nextStage || input.stage || null,
      previousStage: input.previousStage || null,
      nextStage: input.nextStage || null,
      transitionReason: input.reason || null,
      outcome: input.outcome || 'transitioned',
    },
    input.log
  );
}

function attachAskObservation(evaluated, result) {
  if (!evaluated || !evaluated.snapshot) return result;
  const snapshot = evaluated.snapshot;
  const actualPipeline = inferActualPipeline(result);
  snapshot.actualPipeline = actualPipeline;
  snapshot.scoutInvoked = Boolean(result && result.scoutLoop);
  snapshot.responseFromMission = Boolean(
    result &&
      result.mission &&
      result.route === 'mission' &&
      result.mission.kind === 'acquisition_mission'
  );
  snapshot.stageAdvanced = false;
  if (result && result.context && typeof result.context === 'object') {
    result.context.missionContinuation = { ...snapshot };
  }
  observeResponse(
    {
      log: evaluated.log,
      missionId: snapshot.missionId,
      workspace: snapshot.workspace,
      stage: snapshot.stage,
      capability: snapshot.selectedCapability,
      composedFrom: snapshot.responseFromMission
        ? 'mission'
        : actualPipeline === PIPELINES.GENERAL_REASONING
          ? 'fresh_reasoning'
          : actualPipeline,
      actualPipeline,
      outcome: snapshot.responseFromMission ? 'mission_composed' : 'not_mission_composed',
    }
  );
  if (
    snapshot.continueMission &&
    snapshot.intent === INTENTS.OPERATOR_APPROVAL &&
    snapshot.selectedCapability === 'scout' &&
    !snapshot.scoutInvoked
  ) {
    observeDelegation({
      log: evaluated.log,
      missionId: snapshot.missionId,
      workspace: snapshot.workspace,
      stage: snapshot.stage,
      capability: 'scout',
      reason: 'continuation_selected_scout_ask_did_not_invoke',
      missionContext: snapshot.delegationPayload,
      delegationPayload: snapshot.delegationPayload,
      outcome: 'not_attempted',
      invoked: false,
    });
  }
  return result;
}

module.exports = {
  CHECKPOINTS,
  PIPELINES,
  INTENTS,
  APPROVAL_RE,
  BEGIN_MISSION_RE,
  emit,
  createAuditLog,
  classifyOperatorIntent,
  selectCapability,
  expectedNextStage,
  evaluateContinuation,
  loadActiveAcquisitionMission,
  loadOrchestrationMission,
  inferActualPipeline,
  buildDelegationPayload,
  evaluateWorkspaceMissionContinuation,
  observeDelegation,
  observeResult,
  observeResponse,
  observeCreated,
  observeTransition,
  attachAskObservation,
  resolveTenantId,
  resolveWorkspace,
  resolveOperator,
};
