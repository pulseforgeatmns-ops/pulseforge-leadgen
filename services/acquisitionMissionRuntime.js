'use strict';

/**
 * SPEC-140 — Unified Acquisition Mission Runtime.
 * Exactly one AMO runtime per Node process in production.
 * All AMO operations resolve through Runtime.current().
 */

const crypto = require('crypto');
const amo = require('../packages/acquisition-mission');
const { MISSION_STATE_INCONSISTENT } = amo;
const {
  persistLearning,
  persistPrediction,
  persistOutcomeEvaluation,
  persistOutcomeLearning,
  persistStageCommit,
  loadTenantMissions,
  deleteAllAmoData,
  clearAmoSessionBindings,
} = require('./acquisitionMissionPersistence');
const {
  shouldSuppressLegacyDurableWrite,
  acquireMissionDurableLock,
  releaseMissionDurableLock,
} = require('../packages/acquisition-mission/TransactionalPersistence');

const MULTIPLE_ACQUISITION_RUNTIMES = 'MULTIPLE_ACQUISITION_RUNTIMES';

/** @type {object|null} */
let currentRuntime = null;
/** @type {boolean} */
let productionRuntimeLocked = false;
/** @type {Set<string>} */
const registeredProductionRuntimeIds = new Set();

function runtimeError(code, message, extras = {}) {
  const err = new Error(message);
  err.code = code;
  err.spec = 'SPEC-140';
  Object.assign(err, extras);
  return err;
}

function newRuntimeId(prefix) {
  return `${prefix}_${crypto.randomBytes(8).toString('hex')}`;
}

function resolveDefaultPool(explicitPool) {
  if (explicitPool === null) return null;
  if (explicitPool) return explicitPool;
  try {
    return require('../db');
  } catch (_) {
    return null;
  }
}

/**
 * @param {object} [opts]
 * @param {object} [opts.engine] - Pre-built engine (tests only)
 * @param {object} [opts.store] - Pre-built store (tests only)
 * @param {{ query: Function }} [opts.pool]
 * @param {boolean} [opts.persist]
 * @param {boolean} [opts.production] - Marks a production singleton boot
 * @param {string} [opts.runtimeId]
 */
function createAcquisitionMissionRuntime(opts = {}) {
  if (opts.production === true) {
    if (productionRuntimeLocked) {
      throw runtimeError(
        MULTIPLE_ACQUISITION_RUNTIMES,
        'A production Acquisition Mission Runtime is already active in this process.'
      );
    }
  }

  const runtimeId = opts.runtimeId || newRuntimeId('amo_rt');
  const storeId = opts.storeId || newRuntimeId('amo_store');
  const engineId = opts.engineId || newRuntimeId('amo_eng');

  const initialStore = opts.store || (opts.engine && opts.engine.store) || amo.createMemoryAmoStore();
  initialStore._amoStoreId = storeId;

  const initialEngine = opts.engine || amo.createAcquisitionMissionEngine({ store: initialStore });
  initialEngine._amoEngineId = engineId;

  const persist = opts.persist !== false;
  const pool = persist ? resolveDefaultPool(Object.prototype.hasOwnProperty.call(opts, 'pool') ? opts.pool : undefined) : null;

  /** @type {{ engine: object, engineId: string, storeId: string }} */
  const state = {
    engine: initialEngine,
    engineId,
    storeId,
  };

  const runtime = {
    runtimeId,
    get engineId() {
      return state.engineId;
    },
    get storeId() {
      return state.storeId;
    },
    spec: 'SPEC-140',

    engine() {
      return state.engine;
    },

    store() {
      return state.engine.store;
    },

    persistOpts(overrides = {}) {
      const mergedPool = overrides.pool || pool;
      let mergedPersist;
      if (overrides.persist != null) {
        mergedPersist = overrides.persist !== false;
      } else if (mergedPool) {
        mergedPersist = true;
      } else {
        mergedPersist = persist;
      }
      if (!mergedPersist) return { persist: false };
      if (mergedPool) return { persist: true, pool: mergedPool };
      return { persist: false };
    },

    inspect() {
      return {
        runtimeId,
        engineId: state.engineId,
        storeId: state.storeId,
        persist,
        hasPool: Boolean(pool),
      };
    },

    assertEngine(expectedEngine) {
      if (expectedEngine && expectedEngine !== state.engine) {
        throw runtimeError(
          MULTIPLE_ACQUISITION_RUNTIMES,
          'Acquisition Mission execution referenced a different engine than Runtime.current().',
          {
            expectedEngineId: state.engineId,
            actualEngineId: expectedEngine._amoEngineId || null,
          }
        );
      }
      return state.engine;
    },

    async hydrate(tenantId, hydrateOpts = {}) {
      if (tenantId == null || tenantId === '') return state.engine;
      const { persist: effectivePersist, pool: effectivePool } = this.persistOpts(hydrateOpts);
      if (effectivePersist === false || !effectivePool) return state.engine;

      try {
        const loaded = await loadTenantMissions(tenantId, effectivePool);
        for (const mission of loaded.missions) {
          try {
            state.engine.store.putMission(mission);
          } catch (putErr) {
            if (putErr && putErr.code === MISSION_STATE_INCONSISTENT) {
              console.error('[amo] hydrate skip inconsistent mission', mission && mission.id, putErr.message);
              continue;
            }
            throw putErr;
          }
        }
        for (const event of loaded.events) {
          const payload = event.payload && typeof event.payload === 'object' ? event.payload : event;
          state.engine.store.addEvent({
            id: payload.id || event.id,
            missionId: payload.missionId || event.mission_id,
            kind: payload.kind || event.kind,
            specialist: payload.specialist || event.specialist,
            label: payload.label || event.label,
            at: payload.at || event.at,
            payload: payload.payload || {},
          });
        }
        for (const row of loaded.contributions) if (row) state.engine.store.addContribution(row);
        for (const row of loaded.observations) if (row) state.engine.store.addObservation(row);
        for (const row of loaded.outcomes) if (row) state.engine.store.addOutcome(row);
        for (const row of loaded.learning) if (row) state.engine.store.addLearning(row);
        for (const row of loaded.predictions || []) if (row) state.engine.store.addPrediction(row);
        for (const row of loaded.evaluations || []) if (row) state.engine.store.addEvaluation(row);
        for (const row of loaded.outcomeLearnings || []) if (row) state.engine.store.addOutcomeLearning(row);
      } catch (err) {
        if (!/relation .* does not exist/i.test(String(err.message))) {
          console.error('[amo] hydrate:', err.message);
        }
      }
      return state.engine;
    },

    /**
     * ADR-075 — in-memory only. Durable writes route through persistMissionState → persistStageCommit.
     */
    async rememberMission(mission, rememberOpts = {}) {
      if (!mission) return mission;
      state.engine.store.putMission(mission);
      if (shouldSuppressLegacyDurableWrite(mission.id)) {
        return mission;
      }
      return mission;
    },

    /**
     * ADR-075 — deprecated legacy durable writer. Queues no durable writes during TME.
     * Non-TME callers should prefer persistMissionState() which routes through persistStageCommit.
     */
    async persistSideEffects(missionId, sideOpts = {}) {
      if (shouldSuppressLegacyDurableWrite(missionId)) {
        return { suppressed: true, reason: 'tme_transaction_active' };
      }
      return this.persistMissionState(missionId, sideOpts);
    },

    /**
     * ADR-075 — single durable writer for non-TME mission state mutations.
     * Routes exclusively through persistStageCommit().
     */
    async persistMissionState(missionId, sideOpts = {}) {
      if (shouldSuppressLegacyDurableWrite(missionId)) {
        return { suppressed: true, reason: 'tme_transaction_active' };
      }
      const { persist: effectivePersist, pool: effectivePool } = this.persistOpts(sideOpts);
      if (effectivePersist === false || !effectivePool) return;

      const mission = state.engine.store.getMission(missionId);
      if (!mission) return;

      let lockHeld = false;
      try {
        const lockResult = await acquireMissionDurableLock(missionId, effectivePool, { tryOnly: true });
        lockHeld = lockResult.acquired === true && lockResult.reentrant !== true;
        await persistStageCommit({
          mission,
          events: state.engine.store.listEvents(missionId),
          contributions: state.engine.store.listContributions(missionId),
          observations: state.engine.store.listObservations(missionId),
          outcomes: state.engine.store.listOutcomes(missionId),
        }, effectivePool, { skipGlobalLock: true });
        for (const row of state.engine.store.listLearning(mission.tenantId)) {
          await persistLearning(row, effectivePool);
        }
        for (const row of state.engine.store.listPredictions(missionId)) {
          await persistPrediction(row, effectivePool);
        }
        for (const row of state.engine.store.listEvaluations(missionId)) {
          await persistOutcomeEvaluation(row, effectivePool);
        }
        for (const row of state.engine.store.listOutcomeLearnings(mission.tenantId, missionId)) {
          await persistOutcomeLearning(row, effectivePool);
        }
      } catch (err) {
        console.error('[amo] persist mission state:', err.message);
        throw err;
      } finally {
        if (lockHeld) {
          await releaseMissionDurableLock(missionId, effectivePool);
        }
      }
    },

    async create(input = {}, createOpts = {}) {
      await this.hydrate(input.tenantId || input.clientId, createOpts);
      const mission = state.engine.create(input);
      const autonomous = input.autonomous === true || createOpts.autonomous === true;
      // ADR-075 — defer durable writes until persistStageCommit (autonomous TME or explicit persist).
      if (!autonomous) {
        await this.persistMissionState(mission.id, createOpts);
      }
      if (autonomous) {
        return this.runAutonomousProgression(mission.id, {
          tenantId: mission.tenantId,
          ...createOpts,
          ...input,
        });
      }
      return mission;
    },

    async runAutonomousProgression(missionId, opts = {}) {
      const { runAutonomousProgression } = require('../packages/acquisition-mission/MissionProgression');
      if (opts.tenantId) await this.hydrate(opts.tenantId, opts);
      const result = await runAutonomousProgression({
        engine: state.engine,
        missionId,
        tenantId: opts.tenantId,
        operatorId: opts.operatorId,
        allowFixtureFallback: opts.allowFixtureFallback !== false,
        maxSteps: opts.maxSteps,
        deps: opts.deps,
        ...opts,
      });
      // ADR-075 — persist post-TME side effects (e.g. stage transitions) via persistStageCommit only.
      await this.persistMissionState(missionId, opts);
      return result;
    },

    async inspectMission(missionId, inspectOpts = {}) {
      if (inspectOpts.tenantId) await this.hydrate(inspectOpts.tenantId, inspectOpts);
      return state.engine.inspect(missionId, inspectOpts);
    },

    async list(tenantId, listOpts = {}) {
      await this.hydrate(tenantId, listOpts);
      return state.engine.list(tenantId);
    },

    async contribute(missionId, input, contributeOpts = {}) {
      if (contributeOpts.tenantId) await this.hydrate(contributeOpts.tenantId, contributeOpts);
      const result = state.engine.contribute(missionId, input, contributeOpts);
      await this.persistMissionState(missionId, contributeOpts);
      return result;
    },

    async progress(missionId, actor, progressOpts = {}, opts = {}) {
      if (opts.tenantId) await this.hydrate(opts.tenantId, opts);
      const mission = state.engine.progress(missionId, actor, { ...progressOpts, tenantId: opts.tenantId });
      await this.persistMissionState(missionId, opts);
      return mission;
    },

    async answerOperator(question, input = {}, opts = {}) {
      await this.hydrate(input.tenantId, opts);
      return state.engine.answerOperator(question, input);
    },

    reset() {
      const freshStore = amo.createMemoryAmoStore();
      freshStore._amoStoreId = newRuntimeId('amo_store');
      const freshEngine = amo.createAcquisitionMissionEngine({ store: freshStore });
      freshEngine._amoEngineId = newRuntimeId('amo_eng');
      state.engine = freshEngine;
      state.engineId = freshEngine._amoEngineId;
      state.storeId = freshStore._amoStoreId;
      return freshEngine;
    },
  };

  if (opts.production === true) {
    productionRuntimeLocked = true;
    registeredProductionRuntimeIds.add(runtimeId);
  }

  return runtime;
}

/**
 * Boot the production runtime exactly once.
 * @param {object} [opts]
 */
function bootAcquisitionMissionRuntime(opts = {}) {
  if (currentRuntime && productionRuntimeLocked) {
    return currentRuntime;
  }
  currentRuntime = createAcquisitionMissionRuntime({ ...opts, production: true });
  return currentRuntime;
}

/**
 * Process singleton accessor.
 * @param {object} [opts]
 * @param {object} [opts.runtime] - Test override (non-production only)
 */
function getAcquisitionMissionRuntime(opts = {}) {
  if (opts.runtime) {
    if (productionRuntimeLocked && opts.allowTestOverride !== true) {
      throw runtimeError(
        MULTIPLE_ACQUISITION_RUNTIMES,
        'Cannot replace the production Acquisition Mission Runtime without allowTestOverride.'
      );
    }
    currentRuntime = opts.runtime;
    return currentRuntime;
  }
  if (!currentRuntime) {
    currentRuntime = createAcquisitionMissionRuntime({
      pool: opts.pool,
      persist: opts.persist,
      production: opts.production !== false && process.env.NODE_ENV !== 'test',
    });
    if (opts.production !== false && process.env.NODE_ENV !== 'test') {
      productionRuntimeLocked = true;
      registeredProductionRuntimeIds.add(currentRuntime.runtimeId);
    }
  }
  return currentRuntime;
}

/** Alias matching SPEC-140 Runtime.current() */
function current(opts = {}) {
  return getAcquisitionMissionRuntime(opts);
}

function setAcquisitionMissionRuntimeForTests(runtime) {
  if (productionRuntimeLocked) {
    productionRuntimeLocked = false;
    registeredProductionRuntimeIds.clear();
  }
  currentRuntime = runtime;
}

function resetAcquisitionMissionRuntime() {
  currentRuntime = null;
  productionRuntimeLocked = false;
  registeredProductionRuntimeIds.clear();
}

function assertSingleRuntime() {
  if (registeredProductionRuntimeIds.size > 1) {
    throw runtimeError(
      MULTIPLE_ACQUISITION_RUNTIMES,
      `Expected one production runtime; found ${registeredProductionRuntimeIds.size}.`
    );
  }
  if (!currentRuntime) {
    throw runtimeError(
      MULTIPLE_ACQUISITION_RUNTIMES,
      'Acquisition Mission Runtime has not been booted.'
    );
  }
  return currentRuntime;
}

function assertRuntimeEngine(engine, runtime) {
  const resolved = runtime || getAcquisitionMissionRuntime();
  return resolved.assertEngine(engine);
}

/**
 * Unified resolver — every production caller obtains runtime through this.
 * @param {object} [input]
 */
function resolveAcquisitionMissionRuntime(input = {}) {
  if (input.acquisitionMissionRuntime) {
    return input.acquisitionMissionRuntime;
  }
  if (input.runtimeProvider && typeof input.runtimeProvider === 'function') {
    return input.runtimeProvider();
  }
  return getAcquisitionMissionRuntime();
}

function resolveAcquisitionEngine(input = {}) {
  return resolveAcquisitionMissionRuntime(input).engine();
}

/**
 * SPEC-138 — purge DB + reset in-memory runtime.
 */
async function resetAmoRuntime(opts = {}) {
  const { tenantId = null, pool, clearSessions = true } = opts;
  const runtime = resolveAcquisitionMissionRuntime(opts);
  const deleted = await deleteAllAmoData(tenantId, pool || runtime.persistOpts(opts).pool);
  runtime.reset();
  try {
    const { clearAmoHydrationCache } = require('../packages/max/workspace/AmoWorkspaceHydration');
    clearAmoHydrationCache();
  } catch (_) {
    /* hydration module optional in isolated scripts */
  }
  let sessionsCleared = 0;
  const persistOpts = runtime.persistOpts(opts);
  if (clearSessions && persistOpts.persist !== false) {
    try {
      const sessionResult = await clearAmoSessionBindings(persistOpts.pool);
      sessionsCleared = sessionResult.sessionsCleared;
    } catch (err) {
      if (!/relation .* does not exist/i.test(String(err.message))) {
        console.error('[amo] clear session bindings:', err.message);
      }
    }
  }
  const verifyTenant = tenantId != null ? String(tenantId) : null;
  const missionsRemaining = verifyTenant ? runtime.engine().list(verifyTenant).length : 0;
  return {
    spec: 'SPEC-138',
    runtimeVersion: amo.RUNTIME_VERSION,
    runtimeId: runtime.runtimeId,
    engineId: runtime.engineId,
    storeId: runtime.storeId,
    deleted,
    sessionsCleared,
    missionsRemaining,
  };
}

module.exports = {
  MULTIPLE_ACQUISITION_RUNTIMES,
  createAcquisitionMissionRuntime,
  bootAcquisitionMissionRuntime,
  getAcquisitionMissionRuntime,
  current,
  setAcquisitionMissionRuntimeForTests,
  resetAcquisitionMissionRuntime,
  assertSingleRuntime,
  assertRuntimeEngine,
  resolveAcquisitionMissionRuntime,
  resolveAcquisitionEngine,
  resetAmoRuntime,
};
