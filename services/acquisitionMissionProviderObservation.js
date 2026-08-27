'use strict';

/**
 * Mission-bound provider event → canonical communication observation → OBSERVE.
 * Idempotent: same provider event yields one observation.
 * Cross-process safe: hydrates mission from durable store before recording.
 */

const { STAGES, EVENT_KINDS } = require('../packages/acquisition-mission/types');
const { canEnter } = require('../packages/acquisition-mission/Lifecycle');
const { isCommunicationEvidenceEventType, buildCommunicationObservationId } = require('../packages/acquisition-mission/CommunicationObservation');
const { getAcquisitionMissionRuntime } = require('./acquisitionMissionRuntime');
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

  const runtime = opts.runtime || getAcquisitionMissionRuntime({
    pool: opts.pool || pool,
    persist: opts.persist !== false,
    production: false,
  });

  await runtime.hydrate(tenantId, { pool: opts.pool || pool, persist: opts.persist });

  const engine = runtime.engine();
  let mission = engine.get(missionId, tenantId);
  if (!mission) {
    return { skipped: true, reason: 'mission_not_found', missionId };
  }

  const observationId = buildCommunicationObservationId(providerEvent);
  const existedBefore = observationId
    ? engine.store.listObservations(missionId).some((row) => row.id === observationId)
    : false;
  const priorOutcomes = engine.store.listOutcomes(missionId).length;

  const observation = engine.recordCommunicationObservation(missionId, providerEvent, { tenantId });
  if (!observation) {
    return { skipped: true, reason: 'observation_not_created', missionId };
  }

  const duplicateObservation = existedBefore;

  let progressed = false;
  let observeBlocked = null;
  let learnProgressed = false;
  let learnBlocked = null;

  mission = engine.get(missionId, tenantId);
  if (shouldProgressToObserve(mission, engine.store)) {
    if (isGlobalLockHeld(missionId)) {
      observeBlocked = 'mission_lock_active';
    } else {
      try {
        runtime.progress(
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

  await runtime.persistMissionState(missionId, { pool: opts.pool || pool, persist: opts.persist });

  const afterMission = engine.get(missionId, tenantId);
  const outcomesCreated = engine.store.listOutcomes(missionId).length - priorOutcomes;
  const interpretations = engine.store.listInterpretations
    ? engine.store.listInterpretations(missionId)
    : [];

  return {
    observation,
    duplicate: duplicateObservation || result.duplicate === true,
    providerEventInserted: result.inserted === true,
    progressed,
    observeBlocked,
    learnProgressed,
    learnBlocked,
    stage: afterMission && afterMission.stage,
    outcomesCreated,
    interpretationCount: interpretations.length,
  };
}

module.exports = {
  consumeMissionProviderEvent,
  shouldProgressToObserve,
  shouldProgressToLearn,
  executionAllowsObserveProgress,
  extrasFromStore,
  providerEventResultFromInput,
};
