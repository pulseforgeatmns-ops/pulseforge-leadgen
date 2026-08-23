'use strict';

/**
 * SPEC-118 — Acquisition Mission Orchestration service facade.
 * Max manages missions. Capabilities attach evidence to the mission.
 */

const amo = require('../packages/acquisition-mission');
const { STAGE_ORDER } = amo;
const { normalizeScoutDiscoveryPayload } = require('../packages/acquisition-mission/DiscoveryPayload');

/** Legacy SPEC-022 types that now persist only through AMO (SPEC-131). */
const ACQUISITION_LEGACY_MISSION_TYPES = new Set([
  'acquisition_search',
  'prospect_discovery',
  'campaign_creation',
]);
const {
  persistMission,
  persistEvent,
  persistContribution,
  persistObservation,
  persistOutcome,
  persistLearning,
  loadTenantMissions,
  countAmoRows,
  deleteAllAmoData,
  clearAmoSessionBindings,
} = require('./acquisitionMissionPersistence');

let engine = null;

function getEngine(opts = {}) {
  if (opts.engine) return opts.engine;
  if (!engine) engine = amo.createAcquisitionMissionEngine({ store: opts.store });
  return engine;
}

function resetEngine() {
  engine = amo.createAcquisitionMissionEngine();
  return engine;
}

/**
 * SPEC-138 — one-time production reset: purge legacy AMO workflow state and clear runtime caches.
 */
async function resetAmoRuntime(opts = {}) {
  const { tenantId = null, pool, clearSessions = true } = opts;
  const deleted = await deleteAllAmoData(tenantId, pool);
  resetEngine();
  try {
    const { clearAmoHydrationCache } = require('../packages/max/workspace/AmoWorkspaceHydration');
    clearAmoHydrationCache();
  } catch (_) {
    /* hydration module optional in isolated scripts */
  }
  let sessionsCleared = 0;
  if (clearSessions && opts.persist !== false) {
    try {
      const sessionResult = await clearAmoSessionBindings(pool);
      sessionsCleared = sessionResult.sessionsCleared;
    } catch (err) {
      if (!/relation .* does not exist/i.test(String(err.message))) {
        console.error('[amo] clear session bindings:', err.message);
      }
    }
  }
  const verifyTenant = tenantId != null ? String(tenantId) : null;
  const missionsRemaining = verifyTenant
    ? getEngine(opts).list(verifyTenant).length
    : 0;
  return {
    spec: 'SPEC-138',
    runtimeVersion: amo.RUNTIME_VERSION,
    deleted,
    sessionsCleared,
    missionsRemaining,
  };
}

async function hydrateTenant(tenantId, opts = {}) {
  const instance = getEngine(opts);
  if (tenantId == null || tenantId === '' || opts.persist === false) return instance;
  try {
    const loaded = await loadTenantMissions(tenantId, opts.pool);
    for (const mission of loaded.missions) {
      try {
        instance.store.putMission(mission);
      } catch (putErr) {
        if (putErr && putErr.code === amo.MISSION_STATE_INCONSISTENT) {
          console.error('[amo] hydrate skip inconsistent mission', mission && mission.id, putErr.message);
          continue;
        }
        throw putErr;
      }
    }
    for (const event of loaded.events) {
      const payload = event.payload && typeof event.payload === 'object' ? event.payload : event;
      instance.store.addEvent({
        id: payload.id || event.id,
        missionId: payload.missionId || event.mission_id,
        kind: payload.kind || event.kind,
        specialist: payload.specialist || event.specialist,
        label: payload.label || event.label,
        at: payload.at || event.at,
        payload: payload.payload || {},
      });
    }
    for (const row of loaded.contributions) if (row) instance.store.addContribution(row);
    for (const row of loaded.observations) if (row) instance.store.addObservation(row);
    for (const row of loaded.outcomes) if (row) instance.store.addOutcome(row);
    for (const row of loaded.learning) if (row) instance.store.addLearning(row);
  } catch (err) {
    if (!/relation .* does not exist/i.test(String(err.message))) {
      console.error('[amo] hydrate:', err.message);
    }
  }
  return instance;
}

async function rememberMission(mission, opts = {}) {
  if (!mission) return mission;
  getEngine(opts).store.putMission(mission);
  if (opts.persist !== false) {
    try {
      await persistMission(mission, opts.pool, { caller: 'rememberMission()' });
    } catch (err) {
      console.error('[amo] persist mission:', err.message);
    }
  }
  return mission;
}

async function persistSideEffects(missionId, opts = {}) {
  if (opts.persist === false) return;
  const instance = getEngine(opts);
  const mission = instance.store.getMission(missionId);
  if (!mission) return;
  try {
    await persistMission(mission, opts.pool, { caller: 'persistSideEffects()' });
    for (const event of instance.store.listEvents(missionId)) {
      await persistEvent(event, mission.tenantId, opts.pool);
    }
    for (const row of instance.store.listContributions(missionId)) {
      await persistContribution(row, mission.tenantId, opts.pool);
    }
    for (const row of instance.store.listObservations(missionId)) {
      await persistObservation(row, mission.tenantId, opts.pool);
    }
    for (const row of instance.store.listOutcomes(missionId)) {
      await persistOutcome(row, opts.pool);
    }
    for (const row of instance.store.listLearning(mission.tenantId)) {
      await persistLearning(row, opts.pool);
    }
  } catch (err) {
    console.error('[amo] persist side effects:', err.message);
  }
}

async function createMission(input = {}, opts = {}) {
  const instance = await hydrateTenant(input.tenantId || input.clientId, opts);
  const mission = instance.create(input);
  await rememberMission(mission, opts);
  await persistSideEffects(mission.id, opts);
  return mission;
}

async function inspectMission(missionId, opts = {}) {
  if (opts.tenantId) await hydrateTenant(opts.tenantId, opts);
  return getEngine(opts).inspect(missionId, opts);
}

async function listMissions(tenantId, opts = {}) {
  const instance = await hydrateTenant(tenantId, opts);
  return instance.list(tenantId);
}

async function contribute(missionId, input, opts = {}) {
  if (opts.tenantId) await hydrateTenant(opts.tenantId, opts);
  const result = getEngine(opts).contribute(missionId, input, opts);
  await persistSideEffects(missionId, opts);
  return result;
}

async function progressMission(missionId, actor, progressOpts = {}, opts = {}) {
  if (opts.tenantId) await hydrateTenant(opts.tenantId, opts);
  const mission = getEngine(opts).progress(missionId, actor, { ...progressOpts, tenantId: opts.tenantId });
  await persistSideEffects(missionId, opts);
  return mission;
}

async function answerOperator(question, input = {}, opts = {}) {
  const instance = await hydrateTenant(input.tenantId, opts);
  return instance.answerOperator(question, input);
}

function activeMissionFor(tenantId, opts = {}) {
  const missions = getEngine(opts).list(tenantId);
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
  answerOperator,
  attachScoutDiscovery,
  attachPaigeVariants,
  attachEmmettCapacity,
  activeMissionFor,
  hydrateTenant,
  toCommandDeckCard,
  isAcquisitionLegacyMissionType,
  ACQUISITION_LEGACY_MISSION_TYPES,
};
