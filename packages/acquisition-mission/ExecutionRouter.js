'use strict';

/**
 * ADR-090 / SPEC-171 — Canonical Execution Router.
 *
 * Only this module may dispatch specialists or TME for execution-capable surfaces.
 * Surfaces produce a frozen ExecutionRequest and render the result.
 */

const { asText, nowIso, amoError, clone } = require('./types');
const { SPECIALISTS, CONTRIBUTION_KINDS } = require('./types');
const {
  RUNTIME_OWNERS,
  MISSION_RUNTIME_BOUNDARY_VIOLATION,
  resolveMissionRuntimeOwner,
} = require('./MissionRuntimeOwnership');
const { isRolledBackExecution } = require('./ExecutionErrors');
const { shouldAutoConsumeDiscoveryApproval } = require('./OperatorDecisionPolicy');
const { AUTONOMOUS_OPERATOR_ID } = require('./MissionProgression');
const {
  EXECUTION_INTENTS,
  EXECUTION_SOURCES,
  createExecutionRequest,
  isCanonicalExecutionRequest,
  isDiscoveryIntent,
  actionFromIntent,
  specialistForIntent,
} = require('./ExecutionRequest');

const READ_ONLY_MODES = new Set(['read_only', 'execution_disabled']);

/** @type {object[]} */
const _routerAudit = [];

function cerError(code, message, extras = {}) {
  const err = amoError(code, message);
  err.spec = 'SPEC-171';
  Object.assign(err, extras);
  return err;
}

function recordRouterAudit(entry = {}) {
  const row = {
    spec: 'SPEC-171',
    id: entry.id || entry.requestId || null,
    requestId: entry.requestId || null,
    source: entry.source || null,
    intent: entry.intent || null,
    missionId: entry.missionId || null,
    operatorId: entry.operatorId || null,
    runtimeOwner: entry.runtimeOwner || null,
    specialist: entry.specialist || null,
    action: entry.action || null,
    outcome: entry.outcome || null,
    replay: entry.replay === true,
    parentRequestId: entry.parentRequestId || null,
    transactionId: entry.transactionId || null,
    errorCode: entry.errorCode || null,
    at: entry.at || nowIso(),
    request: entry.request && typeof entry.request === 'object' ? clone(entry.request) : null,
  };
  _routerAudit.push(row);
  if (typeof process !== 'undefined' && process.env.NODE_ENV !== 'test' && !process.env.NODE_TEST_CONTEXT) {
    console.info('[CER_EXECUTION_AUDIT]', JSON.stringify({
      requestId: row.requestId,
      intent: row.intent,
      source: row.source,
      outcome: row.outcome,
      missionId: row.missionId,
    }));
  }
  return clone(row);
}

function listExecutionRouterAudit(filter = {}) {
  return _routerAudit
    .filter((row) => {
      if (filter.requestId && row.requestId !== filter.requestId) return false;
      if (filter.missionId && row.missionId !== filter.missionId) return false;
      if (filter.intent && row.intent !== filter.intent) return false;
      return true;
    })
    .map(clone);
}

function clearExecutionRouterAudit() {
  _routerAudit.length = 0;
}

function getExecutionRequestAudit(requestId) {
  return listExecutionRouterAudit({ requestId });
}

function defaultHandlers() {
  const approval = require('../max/workspace/AmoOperatorApproval');
  const { runAutonomousProgression } = require('./MissionProgression');
  return {
    [EXECUTION_INTENTS.APPROVE_PLAN]: (ctx) => approval.advancePlanAfterApproval(ctx),
    [EXECUTION_INTENTS.APPROVE_DISCOVERY]: (ctx) => approval.advanceDiscoveryAfterApproval(ctx),
    [EXECUTION_INTENTS.START_DISCOVERY]: (ctx) => approval.advanceDiscoveryAfterApproval(ctx),
    [EXECUTION_INTENTS.APPROVE_PRIORITIZATION]: (ctx) => approval.advancePrioritizationAfterApproval(ctx),
    [EXECUTION_INTENTS.CLARIFY_PLAN]: (ctx) => approval.advancePlanClarification(ctx),
    [EXECUTION_INTENTS.CANCEL_PLAN]: (ctx) => approval.cancelMissionPlan(ctx),
    [EXECUTION_INTENTS.EDIT_PLAN]: (ctx) => approval.beginPlanEdit(ctx),
    [EXECUTION_INTENTS.APPLY_PLAN_EDITS]: (ctx) => approval.applyPlanEdits(ctx),
    [EXECUTION_INTENTS.AUTONOMOUS_PROGRESSION]: (ctx) => runAutonomousProgression({
      engine: ctx.engine,
      missionId: ctx.mission.id,
      tenantId: ctx.tenantId,
      operatorId: ctx.operatorId,
      allowFixtureFallback: ctx.allowFixtureFallback,
      persist: ctx.persist,
      pool: ctx.pool,
      persistStage: ctx.persistStage,
      runPaige: ctx.runPaige,
      runMax: ctx.runMax,
      runEmmett: ctx.runEmmett,
      infrastructureSnapshot: ctx.infrastructureSnapshot,
    }),
    [EXECUTION_INTENTS.GENERATE_VARIANTS]: (ctx) => approval.advancePaigeVariants(ctx),
    [EXECUTION_INTENTS.GENERATE_CAPACITY]: (ctx) => approval.advanceEmmettCapacity(ctx),
    [EXECUTION_INTENTS.APPROVE_EXECUTION]: (ctx) => approval.advanceExecutionAfterApproval(ctx),
    [EXECUTION_INTENTS.EXECUTE_OUTBOUND]: (ctx) => {
      const { executeOutboundMission } = require('./OutboundExecution');
      return executeOutboundMission({
        engine: ctx.engine,
        mission: ctx.mission,
        tenantId: ctx.tenantId,
        operatorId: ctx.operatorId,
        executionRequest: ctx.executionRequest,
        provider: ctx.provider,
        resolveEmail: ctx.resolveEmail,
        senderIdentity: ctx.senderIdentity,
        pool: ctx.pool,
        persistDurable: ctx.persistDurable,
      });
    },
    [EXECUTION_INTENTS.OPERATOR_APPROVED]: async (ctx) => {
      const question = (ctx.executionRequest.payload && ctx.executionRequest.payload.question) || '';
      try {
        ctx.engine.contribute(
          ctx.mission.id,
          {
            specialist: SPECIALISTS.OPERATOR,
            kind: CONTRIBUTION_KINDS.APPROVAL,
            payload: {
              approved: true,
              command: question,
              action: 'operator_approved',
              executionRequestId: ctx.executionRequest.id,
            },
          },
          { tenantId: ctx.tenantId }
        );
      } catch (_) {
        /* approval may already exist — mission still owns the turn */
      }
      return {
        snapshot: ctx.engine.inspect(ctx.mission.id, { tenantId: ctx.tenantId }),
        executionRequestId: ctx.executionRequest.id,
      };
    },
  };
}

function assertRequest(request) {
  if (!isCanonicalExecutionRequest(request)) {
    throw cerError('cer_invalid', 'Execution Router requires a frozen Canonical Execution Request.');
  }
}

function assertPermissions(request) {
  const permissions = request.permissions || {};
  if (permissions.canExecute === false) {
    throw cerError(
      'cer_permission_denied',
      'Execution is not permitted for this request.',
      { requestId: request.id }
    );
  }
  const mutating = request.intent !== EXECUTION_INTENTS.OPERATOR_APPROVED;
  const autonomous = request.executionMode === 'autonomous'
    || request.operatorId === AUTONOMOUS_OPERATOR_ID;
  if (mutating && !request.operatorId && !autonomous) {
    throw cerError(
      'cer_permission_denied',
      'Mutating execution requires an operatorId.',
      { requestId: request.id, intent: request.intent }
    );
  }
}

function assertExecutionPolicy(request) {
  const mode = asText(request.executionMode);
  if (READ_ONLY_MODES.has(mode)) {
    throw cerError(
      'cer_policy_blocked',
      `Execution policy '${mode}' forbids dispatch.`,
      { requestId: request.id, executionMode: mode }
    );
  }
}

function assertRuntimeOwnership(request, mission) {
  const owner = resolveMissionRuntimeOwner(mission || request.missionId);
  if (!owner) {
    throw cerError(
      'cer_runtime_owner_required',
      'Execution Router could not resolve mission runtime ownership.',
      { requestId: request.id, missionId: request.missionId }
    );
  }
  if (request.runtimeOwner && owner !== request.runtimeOwner) {
    throw cerError(
      MISSION_RUNTIME_BOUNDARY_VIOLATION,
      `Execution Request runtimeOwner '${request.runtimeOwner}' does not match mission owner '${owner}'.`,
      {
        requestId: request.id,
        missionId: request.missionId,
        owner,
        attemptedRuntime: request.runtimeOwner,
      }
    );
  }
  return owner;
}

function handlerContext(request, context, mission, runtimeOwner) {
  const question = (request.payload && request.payload.question)
    || context.question
    || request.intent;
  const owner = runtimeOwner || request.runtimeOwner || resolveMissionRuntimeOwner(mission);
  return {
    engine: context.engine,
    mission,
    tenantId: context.tenantId,
    question,
    operatorId: request.operatorId || context.operatorId || null,
    runScout: context.runScout,
    runPaige: context.runPaige,
    runMax: context.runMax,
    runEmmett: context.runEmmett,
    infrastructureSnapshot: context.infrastructureSnapshot,
    scoutCompanies: context.scoutCompanies,
    scoutPeople: context.scoutPeople,
    allowFixtureFallback: context.allowFixtureFallback,
    audit: context.audit,
    persist: context.persist,
    pool: context.pool,
    persistStage: context.persistStage,
    context: context.planningContext || context.context,
    executionRequest: request,
    provider: context.provider,
    resolveEmail: context.resolveEmail,
    senderIdentity: context.senderIdentity,
    // ADR-089 / SPEC-170 — AMO-owned missions never receive Mission Engine.
    missionEngine: owner === RUNTIME_OWNERS.AMO ? null : (context.missionEngine || null),
    executionContext: {
      spec: 'SPEC-171',
      request,
      runtimeOwner: owner,
    },
  };
}

function resolveMission(request, context) {
  const engine = context.engine;
  if (!engine) {
    throw cerError('cer_invalid', 'Execution Router requires a mission engine.');
  }
  if (!request.missionId) {
    throw cerError('cer_invalid', 'Execution Request is missing missionId.');
  }
  const mission = typeof engine.get === 'function'
    ? engine.get(request.missionId, context.tenantId)
    : null;
  if (!mission) {
    throw amoError('amo_mission_not_found', `Unknown mission: ${request.missionId}`);
  }
  return mission;
}

function persistOpts(context = {}) {
  if (context.persist === false) {
    return { persist: false, pool: context.pool, persistStage: context.persistStage };
  }
  if (typeof context.persistStage === 'function') {
    return { persistStage: context.persistStage, pool: context.pool };
  }
  if (context.pool) {
    return { persist: true, pool: context.pool };
  }
  return {};
}

async function dispatch(request, context, mission, handlers, runtimeOwner) {
  const intent = isDiscoveryIntent(request.intent)
    ? EXECUTION_INTENTS.APPROVE_DISCOVERY
    : request.intent;
  const handler = handlers[request.intent] || handlers[intent];
  if (typeof handler !== 'function') {
    throw cerError('cer_unknown_intent', `No router handler for intent ${request.intent}.`);
  }
  const ctx = {
    ...handlerContext(request, context, mission, runtimeOwner),
    ...persistOpts(context),
  };
  try {
    const executionResult = await handler(ctx);
    return { executionResult, rolledBack: false };
  } catch (err) {
    if (isRolledBackExecution(err)) {
      const snapshot = context.engine.inspect(mission.id, { tenantId: context.tenantId });
      return {
        executionResult: {
          rolledBack: true,
          error: err,
          snapshot,
          transactionId: err.transactionId,
        },
        rolledBack: true,
      };
    }
    throw err;
  }
}

async function maybeChainAutonomousDiscovery(request, context, dispatched, handlers) {
  if (request.intent !== EXECUTION_INTENTS.APPROVE_PLAN) return dispatched;
  const executionResult = dispatched.executionResult;
  if (!executionResult || executionResult.rolledBack || executionResult.alreadyExecuted) {
    return dispatched;
  }
  const snapshot = executionResult.snapshot
    || (context.engine && context.engine.inspect(request.missionId, { tenantId: context.tenantId }));
  const executionPolicy = request.executionMode || context.executionPolicy || null;
  if (!shouldAutoConsumeDiscoveryApproval(snapshot, executionPolicy)) {
    return dispatched;
  }

  const child = createExecutionRequest({
    source: request.source,
    missionId: request.missionId,
    operatorId: request.operatorId || AUTONOMOUS_OPERATOR_ID,
    intent: EXECUTION_INTENTS.APPROVE_DISCOVERY,
    stage: 'discover',
    executionMode: request.executionMode,
    permissions: request.permissions,
    runtimeOwner: request.runtimeOwner,
    objective: request.objective,
    payload: {
      question: 'Autonomous execution policy. Discovery approval auto-consumed.',
    },
    metadata: {
      autoConsumed: true,
      parentRequestId: request.id,
    },
  });

  const childRouted = await routeExecutionRequest(child, {
    ...context,
    handlers,
    question: child.payload.question,
  });
  return {
    ...childRouted,
    parentRequest: request,
    autoConsumedDiscoveryApproval: true,
    planResult: executionResult,
  };
}

/**
 * Dispatch a frozen Canonical Execution Request.
 * @param {object} request
 * @param {object} [context]
 * @returns {Promise<object>}
 */
async function routeExecutionRequest(request, context = {}) {
  assertRequest(request);
  assertPermissions(request);
  assertExecutionPolicy(request);

  const mission = resolveMission(request, context);
  const runtimeOwner = assertRuntimeOwnership(request, mission);
  const handlers = context.handlers || defaultHandlers();
  const specialist = specialistForIntent(request.intent);
  const action = actionFromIntent(request.intent);
  const replay = context.replay === true;

  const dispatched = await dispatch(request, context, mission, handlers, runtimeOwner);
  const chained = await maybeChainAutonomousDiscovery(request, context, dispatched, handlers);

  const snapshot = (chained.executionResult && chained.executionResult.snapshot)
    || (context.engine && context.engine.inspect(mission.id, { tenantId: context.tenantId }));

  const outcome = chained.executionResult && chained.executionResult.rolledBack
    ? 'rolled_back'
    : chained.executionResult && chained.executionResult.alreadyExecuted
      ? 'already_executed'
      : chained.autoConsumedDiscoveryApproval
        ? 'auto_consumed_discovery'
        : 'dispatched';

  const audit = recordRouterAudit({
    requestId: request.id,
    source: request.source,
    intent: request.intent,
    missionId: request.missionId,
    operatorId: request.operatorId,
    runtimeOwner,
    specialist,
    action: chained.action || action,
    outcome,
    replay,
    parentRequestId: request.metadata && request.metadata.parentRequestId,
    transactionId: chained.executionResult && chained.executionResult.transactionId,
    request,
  });

  return {
    spec: 'SPEC-171',
    request,
    action: chained.autoConsumedDiscoveryApproval
      ? 'discovery_approved'
      : (chained.action || action),
    specialist,
    runtimeOwner,
    executionResult: chained.executionResult,
    snapshot,
    audit,
    replay,
    autoConsumedDiscoveryApproval: chained.autoConsumedDiscoveryApproval === true,
    planResult: chained.planResult || null,
    routed: true,
  };
}

/**
 * Replay a previously created CER. Same id, same contract, new audit row.
 * @param {object} request
 * @param {object} [context]
 */
async function replayExecutionRequest(request, context = {}) {
  return routeExecutionRequest(request, { ...context, replay: true });
}

module.exports = {
  EXECUTION_INTENTS,
  EXECUTION_SOURCES,
  routeExecutionRequest,
  replayExecutionRequest,
  recordRouterAudit,
  listExecutionRouterAudit,
  clearExecutionRouterAudit,
  getExecutionRequestAudit,
  defaultHandlers,
};
