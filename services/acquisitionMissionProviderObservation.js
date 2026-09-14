'use strict';

/**
 * Mission-bound provider event → canonical communication observation → OBSERVE.
 * Idempotent: same provider event yields one observation.
 * Cross-process safe: durable observation write precedes mission hydration / stage locks.
 */

const { STAGES, EVENT_KINDS } = require('../packages/acquisition-mission/types');
const { canEnter } = require('../packages/acquisition-mission/Lifecycle');
const { isCommunicationEvidenceEventType, buildCommunicationObservationId } = require('../packages/acquisition-mission/CommunicationObservation');
const { getAcquisitionMissionRuntime } = require('./acquisitionMissionRuntime');
const { persistProviderCommunicationObservation } = require('./acquisitionMissionPersistence');
const { providerEventFromRow } = require('./acquisitionMissionOutboundPersistence');
const { isGlobalLockHeld } = require('../packages/acquisition-mission/TransactionalPersistence');
const { tryProgressToLearn, shouldProgressToLearn } = require('../packages/acquisition-mission/LearnProgression');

function defaultPool() {
  return require('../db');
}

function providerEventResultFromInput(input = {}) {
  if (!input) return null;
  if (input.event) return input;
  if (input.missionId && input.eventType) {
    return { event: input, inserted: true, duplicate: false };
  }
  return null;
}

function extrasFromStore(store, mission) {
  const outcomes = store.listOutcomes(mission.id);
  const events = store.listEvents(mission.id);
  const queued = events.some((row) => row.kind === EVENT_KINDS.QUEUED || row.kind === EVENT_KINDS.LAUNCHED)
    || outcomes.some((row) => row.type === 'queued' || row.type === 'sent');
  return { queuedOrLaunched: queued };
}

function executionAllowsObserveProgress(mission = {}) {
  const summary = mission.executionSummary;
  if (!summary || typeof summary !== 'object') return true;
  return summary.complete !== false;
}

function shouldProgressToObserve(mission, store) {
  if (!mission || mission.stage !== STAGES.EXECUTE) return false;
  if (!executionAllowsObserveProgress(mission)) return false;
  const extra = extrasFromStore(store, mission);
  const gate = canEnter(STAGES.OBSERVE, extra);
  return gate.ok === true;
}

async function syncObservationToEngine(engine, missionId, observation) {
  if (!engine || !observation?.id) return false;
  const existing = engine.store.listObservations(missionId).some((row) => row.id === observation.id);
  if (existing) return false;
  engine.store.addObservation(observation);
  return true;
}

async function tryMissionStageSideEffects({
  providerEvent,
  observation,
  tenantId,
  missionId,
  pool,
  opts,
}) {
  let progressed = false;
  let observeBlocked = null;
  let learnProgressed = false;
  let learnBlocked = null;
  let outcomesCreated = 0;
  let interpretationCount = 0;
  let stage = null;

  if (opts.persist === false) {
    return {
      progressed,
      observeBlocked: 'persist_disabled',
      learnProgressed,
      learnBlocked,
      outcomesCreated,
      interpretationCount,
      stage,
    };
  }

  try {
    const runtime = opts.runtime || getAcquisitionMissionRuntime({
      pool: opts.pool || pool,
      persist: opts.persist !== false,
      production: false,
    });

    await runtime.hydrate(tenantId, { pool: opts.pool || pool, persist: opts.persist });
    const engine = runtime.engine();
    let mission = engine.get(missionId, tenantId);

    if (!mission) {
      return {
        progressed,
        observeBlocked: 'mission_not_hydrated',
        learnProgressed,
        learnBlocked,
        outcomesCreated,
        interpretationCount,
        stage,
      };
    }

    await syncObservationToEngine(engine, missionId, observation);

    const priorOutcomes = engine.store.listOutcomes(missionId).length;
    const structured = engine.recordCommunicationObservation(missionId, providerEvent, {
      tenantId,
      skipInterpretation: opts.skipInterpretation === true,
    });
    if (structured && structured.id !== observation.id) {
      await syncObservationToEngine(engine, missionId, structured);
    }

    mission = engine.get(missionId, tenantId);
    if (shouldProgressToObserve(mission, engine.store)) {
      if (isGlobalLockHeld(missionId)) {
        observeBlocked = 'mission_lock_active';
      } else {
        try {
          await runtime.progress(
            missionId,
            { role: 'max' },
            { stage: STAGES.OBSERVE },
            { tenantId, pool: opts.pool || pool, persist: opts.persist }
          );
          progressed = true;
        } catch (err) {
          observeBlocked = err.code || err.message;
        }
      }
    } else if (mission.stage === STAGES.EXECUTE && !executionAllowsObserveProgress(mission)) {
      observeBlocked = 'execution_incomplete';
    }

    mission = engine.get(missionId, tenantId);
    if (shouldProgressToLearn(mission, engine.store)) {
      if (isGlobalLockHeld(missionId)) {
        learnBlocked = 'mission_lock_active';
      } else {
        try {
          const learnResult = tryProgressToLearn(engine, missionId, { tenantId });
          learnProgressed = learnResult.progressed === true;
        } catch (err) {
          learnBlocked = err.code || err.message;
        }
      }
    }

    try {
      await runtime.persistMissionState(missionId, { pool: opts.pool || pool, persist: opts.persist });
    } catch (err) {
      const code = err.code || err.message;
      observeBlocked = observeBlocked || code;
    }

    const afterMission = engine.get(missionId, tenantId);
    outcomesCreated = engine.store.listOutcomes(missionId).length - priorOutcomes;
    interpretationCount = engine.store.listInterpretations
      ? engine.store.listInterpretations(missionId).length
      : 0;
    stage = afterMission && afterMission.stage;
  } catch (err) {
    observeBlocked = err.code || err.message;
  }

  return {
    progressed,
    observeBlocked,
    learnProgressed,
    learnBlocked,
    outcomesCreated,
    interpretationCount,
    stage,
  };
}

/**
 * @param {object} providerEventResult — { event, inserted, duplicate } from persistMissionProviderEvent
 * @param {object} [pool]
 * @param {object} [opts]
 */
async function consumeMissionProviderEvent(providerEventResult, pool = defaultPool(), opts = {}) {
  const result = providerEventResultFromInput(providerEventResult);
  const providerEvent = result && result.event;
  if (!providerEvent || !providerEvent.missionId) {
    return { skipped: true, reason: 'missing_provider_event' };
  }

  if (!isCommunicationEvidenceEventType(providerEvent.eventType)) {
    return { skipped: true, reason: 'unsupported_event_type', eventType: providerEvent.eventType };
  }

  const tenantId = providerEvent.tenantId;
  const missionId = providerEvent.missionId;
  const observationId = buildCommunicationObservationId(providerEvent);

  if (opts.persist === false) {
    const runtime = opts.runtime || getAcquisitionMissionRuntime({
      pool: null,
      persist: false,
      production: false,
    });
    await runtime.hydrate(tenantId, { persist: false });
    const engine = runtime.engine();
    const mission = engine.get(missionId, tenantId);
    if (!mission) {
      return { skipped: true, reason: 'mission_not_found', missionId };
    }
    const existedBefore = observationId
      ? engine.store.listObservations(missionId).some((row) => row.id === observationId)
      : false;
    const priorOutcomes = engine.store.listOutcomes(missionId).length;
    const observation = engine.recordCommunicationObservation(missionId, providerEvent, { tenantId });
    if (!observation) {
      return { skipped: true, reason: 'observation_not_created', missionId };
    }

    let progressed = false;
    let observeBlocked = null;
    let learnProgressed = false;
    let learnBlocked = null;

    let activeMission = engine.get(missionId, tenantId);
    if (shouldProgressToObserve(activeMission, engine.store)) {
      if (isGlobalLockHeld(missionId)) {
        observeBlocked = 'mission_lock_active';
      } else {
        try {
          await runtime.progress(
            missionId,
            { role: 'max' },
            { stage: STAGES.OBSERVE },
            { tenantId, persist: false }
          );
          progressed = true;
        } catch (err) {
          observeBlocked = err.code || err.message;
        }
      }
    } else if (activeMission.stage === STAGES.EXECUTE && !executionAllowsObserveProgress(activeMission)) {
      observeBlocked = 'execution_incomplete';
    }

    activeMission = engine.get(missionId, tenantId);
    if (shouldProgressToLearn(activeMission, engine.store)) {
      if (isGlobalLockHeld(missionId)) {
        learnBlocked = 'mission_lock_active';
      } else {
        try {
          const learnResult = tryProgressToLearn(engine, missionId, { tenantId });
          learnProgressed = learnResult.progressed === true;
        } catch (err) {
          learnBlocked = err.code || err.message;
        }
      }
    }

    const afterMission = engine.get(missionId, tenantId);
    const outcomesCreated = engine.store.listOutcomes(missionId).length - priorOutcomes;
    const interpretationCount = engine.store.listInterpretations
      ? engine.store.listInterpretations(missionId).length
      : 0;

    return {
      observation,
      observationId,
      duplicate: existedBefore || result.duplicate === true,
      persisted: false,
      providerEventInserted: result.inserted === true,
      progressed,
      observeBlocked,
      learnProgressed,
      learnBlocked,
      stage: afterMission && afterMission.stage,
      outcomesCreated,
      interpretationCount,
    };
  }

  const persistResult = await persistProviderCommunicationObservation(
    providerEvent,
    opts.pool || pool,
    opts
  );
  if (persistResult.skipped) {
    return persistResult;
  }

  const sideEffects = await tryMissionStageSideEffects({
    providerEvent,
    observation: persistResult.observation,
    tenantId,
    missionId,
    pool,
    opts,
  });

  return {
    observation: persistResult.observation,
    observationId,
    duplicate: persistResult.duplicate === true,
    inserted: persistResult.inserted === true,
    persisted: persistResult.persisted === true,
    providerEventInserted: result.inserted === true,
    providerEventDuplicate: result.duplicate === true,
    ...sideEffects,
  };
}

/**
 * Backfill mission observations from durable provider events (no resend required).
 */
async function backfillMissionObservationsFromProviderEvents(input = {}, pool = defaultPool(), opts = {}) {
  const missionId = input.missionId || null;
  const executionRecordId = input.executionRecordId || null;
  const tenantId = input.tenantId != null ? String(input.tenantId) : null;
  if (!missionId && !executionRecordId) {
    return { skipped: true, reason: 'missing_scope' };
  }

  const params = [];
  const clauses = [];
  if (missionId) {
    params.push(missionId);
    clauses.push(`mission_id = $${params.length}`);
  }
  if (executionRecordId) {
    params.push(executionRecordId);
    clauses.push(`execution_record_id = $${params.length}`);
  }
  if (tenantId) {
    params.push(tenantId);
    clauses.push(`tenant_id = $${params.length}`);
  }

  const providerRows = (await pool.query(`
    SELECT *
    FROM acquisition_mission_provider_events
    WHERE ${clauses.join(' AND ')}
    ORDER BY occurred_at ASC NULLS LAST, created_at ASC
  `, params)).rows;

  const results = [];
  for (const row of providerRows) {
    const event = providerEventFromRow(row);
    const consumed = await consumeMissionProviderEvent(
      { event, inserted: false, duplicate: true },
      pool,
      { ...opts, skipInterpretation: opts.skipInterpretation !== false }
    );
    results.push({
      providerEventId: row.id,
      eventType: row.event_type,
      observationId: consumed.observation?.id || consumed.observationId || null,
      inserted: consumed.inserted === true,
      duplicate: consumed.duplicate === true,
      skipped: consumed.skipped === true,
      reason: consumed.reason || null,
    });
  }

  const created = results.filter((row) => row.inserted === true);
  return {
    missionId,
    executionRecordId,
    tenantId,
    providerEventCount: providerRows.length,
    observationsCreated: created.length,
    results,
  };
}

module.exports = {
  consumeMissionProviderEvent,
  backfillMissionObservationsFromProviderEvents,
  shouldProgressToObserve,
  shouldProgressToLearn,
  executionAllowsObserveProgress,
  extrasFromStore,
  providerEventResultFromInput,
  syncObservationToEngine,
};
