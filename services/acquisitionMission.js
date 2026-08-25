'use strict';

/**
 * SPEC-118 — Acquisition Mission Orchestration service facade.
 * SPEC-140 — All operations resolve through AcquisitionMissionRuntime.current().
 * Max manages missions. Capabilities attach evidence to the mission.
 */

const amo = require('../packages/acquisition-mission');
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

  return amo.routeExecutionRequest(request, {
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
  });
}

function activeMissionFor(tenantId, opts = {}) {
  const runtime = runtimeFromOpts(opts);
  const missions = runtime.engine().list(tenantId);
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

module.exports = {
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
