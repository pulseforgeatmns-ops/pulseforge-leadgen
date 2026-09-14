'use strict';

/**
 * SPEC-251 / SPEC-252 — Derived follow-up cadence from prepared sequence artifacts only.
 */

const { asText } = require('./types');
const { SPECIALISTS, CONTRIBUTION_KINDS } = require('./types');
const { findEmmettCapacity, findPaigeVariants } = require('./ExecutionApproval');
const { unwrapSpecialistPayload } = require('./ContributionSupersession');
const {
  extractOutreachSequenceSteps,
  CADENCE_PROVENANCE,
} = require('./PreparedOutreachSequence');

const CADENCE_SOURCES = Object.freeze({
  PREPARED_SEQUENCE: 'prepared_sequence',
  UNRESOLVED: 'unresolved',
});

function normalizeSteps(raw) {
  return extractOutreachSequenceSteps({ steps: raw }).map((row, index) => ({
    day: row.day,
    index,
    step: row.step != null ? row.step : index,
  }));
}

function extractStepsFromObject(obj = {}) {
  const steps = extractOutreachSequenceSteps(obj);
  return steps.map((row, index) => ({
    day: row.day,
    index,
    step: row.step != null ? row.step : index,
  }));
}

function extractPreparedSequenceSteps(input = {}) {
  const {
    mission = {},
    store = {},
    executionRecord = null,
    preparedArtifactRevision = null,
    preparedCadence = null,
  } = input;

  if (preparedCadence?.steps?.length) {
    return {
      steps: preparedCadence.steps.map((row, index) => ({
        day: row.day,
        index,
        step: row.step != null ? row.step : index,
      })),
      source: CADENCE_SOURCES.PREPARED_SEQUENCE,
      cadenceProvenance: preparedCadence.cadenceProvenance || null,
      reconstructed: preparedCadence.reconstructed === true,
    };
  }

  const fromExecution = extractStepsFromObject(executionRecord?.payload || {});
  if (fromExecution.length) {
    return {
      steps: fromExecution,
      source: CADENCE_SOURCES.PREPARED_SEQUENCE,
      cadenceProvenance: CADENCE_PROVENANCE.IN_MEMORY_STORE,
      reconstructed: false,
    };
  }

  const contributions = store.listContributions
    ? store.listContributions(mission.id)
    : (store.contributions || []).filter((row) => row.missionId === mission.id);

  const emmett = findEmmettCapacity(contributions);
  const paige = findPaigeVariants(contributions);
  const emmettPayload = emmett ? unwrapSpecialistPayload(emmett) : {};
  const paigePayload = paige ? unwrapSpecialistPayload(paige) : {};

  const emmettSteps = extractStepsFromObject(emmettPayload);
  if (emmettSteps.length) {
    return {
      steps: emmettSteps,
      source: CADENCE_SOURCES.PREPARED_SEQUENCE,
      cadenceProvenance: CADENCE_PROVENANCE.IN_MEMORY_STORE,
      reconstructed: false,
    };
  }

  const paigeSteps = extractStepsFromObject(paigePayload);
  if (paigeSteps.length) {
    return {
      steps: paigeSteps,
      source: CADENCE_SOURCES.PREPARED_SEQUENCE,
      cadenceProvenance: CADENCE_PROVENANCE.PAIGE_CONTRIBUTION,
      reconstructed: false,
    };
  }

  if (preparedArtifactRevision && store.listExecutionRecords) {
    const records = store.listExecutionRecords(mission.id, {});
    const match = records.find(
      (row) => asText(row.preparedArtifactRevision) === asText(preparedArtifactRevision)
    );
    const recordSteps = extractStepsFromObject(match?.payload || {});
    if (recordSteps.length) {
      return {
        steps: recordSteps,
        source: CADENCE_SOURCES.PREPARED_SEQUENCE,
        cadenceProvenance: CADENCE_PROVENANCE.IN_MEMORY_STORE,
        reconstructed: false,
      };
    }
  }

  return {
    steps: [],
    source: CADENCE_SOURCES.UNRESOLVED,
    cadenceProvenance: null,
    reconstructed: false,
  };
}

/**
 * Resolve wait delta from current sent step to next unsent step.
 */
function resolveObserveCadence(input = {}) {
  const {
    mission = {},
    store = {},
    executionRecord = null,
    preparedArtifactRevision = null,
    preparedCadence = null,
    sequenceStepSent = 0,
    clockStart = null,
    now = new Date(),
  } = input;

  const extracted = extractPreparedSequenceSteps({
    mission,
    store,
    executionRecord,
    preparedArtifactRevision,
    preparedCadence,
  });
  const { steps, source, cadenceProvenance, reconstructed } = extracted;

  if (!steps.length) {
    return {
      cadenceSource: CADENCE_SOURCES.UNRESOLVED,
      waitDays: null,
      currentStepDay: null,
      nextStepDay: null,
      dueAt: null,
      kind: 'unresolved',
      clockStart: clockStart || null,
      businessDays: false,
      cadenceProvenance: null,
      reconstructed: false,
    };
  }

  const sentIndex = Math.max(0, Number(sequenceStepSent) || 0);
  const current = steps[sentIndex] || steps[0];
  const next = steps[sentIndex + 1] || null;

  if (!next) {
    return {
      cadenceSource: source,
      waitDays: null,
      currentStepDay: current?.day ?? null,
      nextStepDay: null,
      dueAt: null,
      kind: 'none',
      clockStart: clockStart || null,
      businessDays: false,
      sequenceExhausted: true,
      cadenceProvenance,
      reconstructed: reconstructed === true,
    };
  }

  const waitDays = Math.max(0, next.day - current.day);
  let dueAt = null;
  if (clockStart && waitDays != null) {
    const start = new Date(clockStart);
    if (!Number.isNaN(start.getTime())) {
      dueAt = new Date(start.getTime() + waitDays * 86400000).toISOString();
    }
  }

  const nowMs = now instanceof Date ? now.getTime() : new Date(now).getTime();
  const dueMs = dueAt ? new Date(dueAt).getTime() : null;
  const cadenceElapsed = dueMs != null && !Number.isNaN(nowMs) && nowMs >= dueMs;

  return {
    cadenceSource: source,
    waitDays,
    currentStepDay: current?.day ?? null,
    nextStepDay: next?.day ?? null,
    dueAt,
    kind: cadenceElapsed ? 'due' : 'wait_until',
    clockStart: clockStart || null,
    businessDays: false,
    cadenceElapsed,
    sequenceExhausted: false,
    cadenceProvenance,
    reconstructed: reconstructed === true,
  };
}

module.exports = {
  CADENCE_SOURCES,
  normalizeSteps,
  extractStepsFromObject,
  extractPreparedSequenceSteps,
  resolveObserveCadence,
};
