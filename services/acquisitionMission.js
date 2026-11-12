'use strict';

/**
 * SPEC-118 — Acquisition Mission Orchestration service facade.
 * SPEC-140 — All operations resolve through AcquisitionMissionRuntime.current().
 * Max manages missions. Capabilities attach evidence to the mission.
 */

const amo = require('../packages/acquisition-mission');
const { assertActionAuthorized } = require('../packages/acquisition-mission/AuthorizationConstraints');
const { recordRouterAudit } = require('../packages/acquisition-mission/ExecutionRouter');
const { STAGE_ORDER } = amo;
const { normalizeScoutDiscoveryPayload } = require('../packages/acquisition-mission/DiscoveryPayload');
const {
  getAcquisitionMissionRuntime,
  resetAcquisitionMissionRuntime,
  resolveAcquisitionMissionRuntime,
  assertRuntimeEngine,
  resetAmoRuntime,
} = require('./acquisitionMissionRuntime');

/** Legacy SPEC-022 types that now persist only through AMO (SPEC-131). */
const ACQUISITION_LEGACY_MISSION_TYPES = new Set([
  'acquisition_search',
  'prospect_discovery',
  'campaign_creation',
]);

function runtimeFromOpts(opts = {}) {
  if (opts.runtime) return opts.runtime;
  if (opts.acquisitionMissionRuntime) return opts.acquisitionMissionRuntime;
  return resolveAcquisitionMissionRuntime(opts);
}

function getEngine(opts = {}) {
  const runtime = runtimeFromOpts(opts);
  if (opts.engine) {
    runtime.assertEngine(opts.engine);
    return opts.engine;
  }
  return runtime.engine();
}

function resetEngine() {
  resetAcquisitionMissionRuntime();
  const runtime = getAcquisitionMissionRuntime({ production: false, persist: false });
  return runtime.reset();
}

async function hydrateTenant(tenantId, opts = {}) {
  const runtime = runtimeFromOpts(opts);
  return runtime.hydrate(tenantId, opts);
}

async function rememberMission(mission, opts = {}) {
  const runtime = runtimeFromOpts(opts);
  return runtime.rememberMission(mission, opts);
}

async function persistSideEffects(missionId, opts = {}) {
  const runtime = runtimeFromOpts(opts);
  return runtime.persistSideEffects(missionId, opts);
}

async function createMission(input = {}, opts = {}) {
  const runtime = runtimeFromOpts(opts);
  return runtime.create(input, opts);
}

async function inspectMission(missionId, opts = {}) {
  const runtime = runtimeFromOpts(opts);
  return runtime.inspectMission(missionId, opts);
}

async function listMissions(tenantId, opts = {}) {
  const runtime = runtimeFromOpts(opts);
  return runtime.list(tenantId, opts);
}

async function contribute(missionId, input, opts = {}) {
  const runtime = runtimeFromOpts(opts);
  return runtime.contribute(missionId, input, opts);
}

async function progressMission(missionId, actor, progressOpts = {}, opts = {}) {
  const runtime = runtimeFromOpts(opts);
  return runtime.progress(missionId, actor, progressOpts, opts);
}

async function runAutonomousProgressionForMission(missionId, opts = {}) {
  const runtime = runtimeFromOpts(opts);
  return runtime.runAutonomousProgression(missionId, opts);
}

async function answerOperator(question, input = {}, opts = {}) {
  const runtime = runtimeFromOpts(opts);
  return runtime.answerOperator(question, input, opts);
}

/** Intents that send, or approve a prepared package for sending. */
const SENDER_REQUIRED_INTENTS = new Set([
  amo.EXECUTION_INTENTS.EXECUTE_OUTBOUND,
  amo.EXECUTION_INTENTS.APPROVE_EXECUTION,
]);

async function executeCanonical(input = {}, opts = {}) {
  const runtime = runtimeFromOpts(opts);
  const tenantId = String(input.tenantId || opts.tenantId || '');
  if (!tenantId) {
    const err = new Error('No active client selected.');
    err.code = 'no_tenant';
    throw err;
  }
  await runtime.hydrate(tenantId, opts);
  const engine = getEngine(opts);
  const missionId = String(input.missionId || '').trim();
  if (!missionId) {
    throw amo.amoError('amo_mission_not_found', 'missionId is required.');
  }
  const mission = engine.get(missionId, tenantId);
  if (!mission) {
    throw amo.amoError('amo_mission_not_found', `Unknown mission: ${missionId}`);
  }

  const intent = input.intent
    || amo.intentFromPendingDecision(mission.pendingOperatorDecision);
  if (!intent) {
    throw amo.amoError(
      'cer_unknown_intent',
      'No executable intent. Pass intent or wait for a pending operator decision.'
    );
  }
  const source = input.source || amo.EXECUTION_SOURCES.API;
  const factory = source === amo.EXECUTION_SOURCES.APPROVAL_BUTTON
    ? amo.createExecutionRequestFromApprovalButton
    : source === amo.EXECUTION_SOURCES.VOICE
      ? amo.createExecutionRequestFromVoice
      : source === amo.EXECUTION_SOURCES.COMMAND_DECK
        ? amo.createExecutionRequestFromCommandDeck
        : source === amo.EXECUTION_SOURCES.CHAT
          ? amo.createExecutionRequestFromChat
          : amo.createExecutionRequestFromApi;

  const request = factory({
    intent,
    missionId: mission.id,
    mission,
    operatorId: input.operatorId || 'operator',
    stage: mission.stage,
    executionMode: input.executionMode || null,
    objective: mission.objective,
    runtimeOwner: amo.resolveMissionRuntimeOwner(mission),
    permissions: input.permissions || { canExecute: true, role: input.role || null },
    pendingOperatorDecision: mission.pendingOperatorDecision,
    payload: {
      question: input.question || input.intent || null,
      ...(input.payload || {}),
    },
    metadata: input.metadata || {},
    source,
  });

  // P0 — mission authorization is decided before any sender configuration is
  // read: a prohibited action is refused (authorization_constraint_blocked,
  // audited like the router does) even when the tenant's sender is incomplete.
  const actionClass = amo.intentActionClass(intent);
  if (!amo.evaluateAction(mission, actionClass).allowed) {
    recordRouterAudit({
      requestId: request.id,
      source: request.source,
      intent: request.intent,
      missionId: request.missionId,
      operatorId: request.operatorId,
      runtimeOwner: request.runtimeOwner,
      specialist: amo.specialistForIntent(request.intent),
      action: null,
      outcome: 'blocked_by_constraint',
      executionState: 'blocked',
      errorCode: amo.AUTHORIZATION_CONSTRAINT_BLOCKED,
      request,
    });
    assertActionAuthorized(mission, actionClass, { requestId: request.id, intent: request.intent });
  }

  // The canonical sender is needed only to send, or to approve a package for
  // sending. Other intents never read it, so its configuration cannot fail them.
  // Resolution still fails closed for the intents that need it.
  let canonicalSender = input.canonicalSender || opts.canonicalSender || null;
  if (
    !canonicalSender &&
    SENDER_REQUIRED_INTENTS.has(intent) &&
    (opts.pool || opts.client || input.client || typeof opts.loadClient === 'function')
  ) {
    const { resolveCanonicalSenderIdentity } = require('../utils/canonicalSenderIdentity');
    const resolved = await resolveCanonicalSenderIdentity({
      tenantId,
      clientId: input.clientId || tenantId,
      client: opts.client || input.client || null,
      pool: opts.pool,
      loadClient: opts.loadClient,
    });
    if (!resolved.ok) {
      const err = amo.amoError(resolved.code || 'canonical_sender_incomplete', resolved.blockReason);
      err.blockReason = resolved.blockReason;
      throw err;
    }
    canonicalSender = resolved.identity;
  }

  const routed = await amo.routeExecutionRequest(request, {
    engine,
    tenantId,
    question: input.question,
    operatorId: request.operatorId,
    runScout: input.runScout,
    allowFixtureFallback: input.allowFixtureFallback,
    persist: opts.persist,
    pool: opts.pool,
    persistStage: opts.persistStage,
    missionEngine: opts.missionEngine,
    sendEmail: input.sendEmail || opts.sendEmail,
    resolveProspectAttributes: input.resolveProspectAttributes || opts.resolveProspectAttributes,
    canonicalSender,
    senderIdentity: canonicalSender,
    client: opts.client || input.client,
    loadClient: opts.loadClient,
    brevoState: input.brevoState || opts.brevoState,
    senderReadiness: input.senderReadiness || opts.senderReadiness,
    requireProviderReadiness: input.requireProviderReadiness || opts.requireProviderReadiness,
  });
  // Cancellation is an operator edit, not a specialist TME stage. Persist it
  // through the runtime's canonical commit path before a subsequent hydration.
  if (intent === amo.EXECUTION_INTENTS.CANCEL_PLAN && !routed.executionResult?.rolledBack) {
    await runtime.persistMissionState(mission.id, opts);
  }
  return routed;
}

function activeMissionFor(tenantId, opts = {}) {
  const runtime = runtimeFromOpts(opts);
  const missions = runtime.engine().list(tenantId).filter(mission => !mission.planCancelled);
  return missions.find((row) => row.stage !== 'improve') || missions[0] || null;
}

async function attachScoutDiscovery(input = {}, result = {}, opts = {}) {
  const tenantId = String(input.tenantId || input.authorizedTenantId || input.clientId || '');
  if (tenantId) await hydrateTenant(tenantId, opts);
  const missionId = input.missionId || opts.missionId
    || (opts.attachToActive ? (activeMissionFor(tenantId, opts) || {}).id : null);
  if (!missionId) return null;
  const normalized = normalizeScoutDiscoveryPayload(result, {
    missionObjective: input.missionObjective || opts.missionObjective,
  });
  try {
    return await contribute(missionId, {
      specialist: 'scout',
      kind: 'discovery',
      payload: normalized,
    }, { ...opts, tenantId });
  } catch (err) {
    console.error('[amo] attach scout:', err.message);
    return null;
  }
}

async function attachPaigeVariants(input = {}, recommendation = {}, opts = {}) {
  const tenantId = String(input.tenantId || input.clientId || recommendation.tenantId || '');
  if (tenantId) await hydrateTenant(tenantId, opts);
  const missionId = input.missionId || opts.missionId || input.campaignContext?.missionId
    || (opts.attachToActive ? (activeMissionFor(tenantId, opts) || {}).id : null);
  if (!missionId) return null;
  try {
    return await contribute(missionId, {
      specialist: 'paige',
      payload: {
        variants: recommendation.variants || [{
          label: recommendation.recommendedDirection || 'Variant',
          subject: recommendation.subject,
        }],
        subjects: recommendation.subjects || (recommendation.subject ? [recommendation.subject] : []),
        cta: recommendation.cta || recommendation.callToAction || null,
        hypotheses: recommendation.hypotheses
          || [recommendation.learningObjective || recommendation.reason].filter(Boolean),
        experiments: recommendation.experiments || [],
        generating: recommendation.generating === true,
      },
    }, { ...opts, tenantId });
  } catch (err) {
    console.error('[amo] attach paige:', err.message);
    return null;
  }
}

async function attachEmmettCapacity(input = {}, day = {}, opts = {}) {
  const tenantId = String(input.tenantId || input.clientId || day.plan?.tenantId || '');
  if (tenantId) await hydrateTenant(tenantId, opts);
  const missionId = input.missionId || opts.missionId || day.plan?.missionId
    || (opts.attachToActive ? (activeMissionFor(tenantId, opts) || {}).id : null);
  if (!missionId) return null;
  const queue = (day.queue && (day.queue.items || day.queue.prospects)) || [];
  try {
    return await contribute(missionId, {
      specialist: 'emmett',
      payload: {
        capacity: day.capacity || {},
        queue,
        sendRecommendations: day.recommendations || [],
        deliverability: day.health || day.governor || {},
        reputation: day.health || {},
        governor: day.governor || null,
        queuedCount: Array.isArray(queue) ? queue.length : null,
      },
    }, { ...opts, tenantId });
  } catch (err) {
    console.error('[amo] attach emmett:', err.message);
    return null;
  }
}

function isAcquisitionLegacyMissionType(mission) {
  if (!mission) return false;
  const type = String(mission.type || '').toLowerCase();
  return ACQUISITION_LEGACY_MISSION_TYPES.has(type);
}

/**
 * Command Deck Operations card for an acquisition mission (SPEC-131).
 * @param {object} mission
 */
function toCommandDeckCard(mission) {
  if (!mission) return null;
  const stageIdx = STAGE_ORDER.indexOf(mission.stage);
  const totalSteps = STAGE_ORDER.length;
  const completedSteps = stageIdx >= 0 ? stageIdx + 1 : 0;
  return {
    id: mission.id,
    title: mission.title || mission.objective,
    type: 'acquisition',
    runtime: 'AMO',
    status: mission.stage === 'improve' ? 'completed' : 'executing',
    statusLabel: mission.status || mission.stage,
    progress: {
      completedSteps,
      totalSteps,
      percent: mission.progressPercent || 0,
      currentStage: mission.stage || null,
      label: stageIdx >= 0 ? `${completedSteps} / ${totalSteps}` : null,
    },
    startedAt: mission.createdAt,
    createdAt: mission.createdAt,
    objectiveText: mission.objective,
    pendingOperatorDecision: mission.pendingOperatorDecision || null,
  };
}

/**
 * P0 — read the current mission authorization and the hash an amendment must bind to.
 */
async function previewMissionAuthorization(input = {}, opts = {}) {
  const runtime = runtimeFromOpts(opts);
  const tenantId = String(input.tenantId || '');
  if (!tenantId) throw Object.assign(new Error('No active client selected.'), { code: 'no_tenant' });
  await runtime.hydrate(tenantId, opts);
  return amo.previewAuthorization({ engine: getEngine(opts), missionId: String(input.missionId || ''), tenantId });
}

/**
 * P0 — apply an operator-approved authorization amendment as one durable TME commit.
 * Transport-level authorization (role, fresh confirmation) is the caller's duty.
 */
async function amendMissionAuthorization(input = {}, opts = {}) {
  const runtime = runtimeFromOpts(opts);
  const tenantId = String(input.tenantId || '');
  if (!tenantId) throw Object.assign(new Error('No active client selected.'), { code: 'no_tenant' });
  await runtime.hydrate(tenantId, opts);
  const engine = getEngine(opts);
  const { bindStagePersistDurable } = require('./acquisitionMissionPersistence');
  return amo.applyAuthorizationAmendment({
    engine,
    missionId: String(input.missionId || ''),
    tenantId,
    change: input.change,
    expectedConstraintsHash: input.expectedConstraintsHash,
    amendmentId: input.amendmentId,
    approver: input.approver,
    pool: opts.pool,
    persistDurable: bindStagePersistDurable(
      { pool: opts.pool, persist: opts.persist, persistStage: opts.persistStage },
      engine,
      tenantId
    ),
  });
}

module.exports = {
  previewMissionAuthorization,
  amendMissionAuthorization,
  getEngine,
  resetEngine,
  resetAmoRuntime,
  createMission,
  inspectMission,
  listMissions,
  contribute,
  progressMission,
  runAutonomousProgressionForMission,
  answerOperator,
  executeCanonical,
  attachScoutDiscovery,
  attachPaigeVariants,
  attachEmmettCapacity,
  activeMissionFor,
  hydrateTenant,
  toCommandDeckCard,
  isAcquisitionLegacyMissionType,
  ACQUISITION_LEGACY_MISSION_TYPES,
  assertRuntimeEngine,
  getAcquisitionMissionRuntime,
};
