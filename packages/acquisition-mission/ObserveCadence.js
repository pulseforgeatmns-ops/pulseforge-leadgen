'use strict';

/**
 * SPEC-251 — Derived follow-up cadence from prepared sequence artifacts only.
 */

const { asText } = require('./types');
const { SPECIALISTS, CONTRIBUTION_KINDS } = require('./types');
const { findEmmettCapacity, findPaigeVariants } = require('./ExecutionApproval');
const { unwrapSpecialistPayload } = require('./ContributionSupersession');

const CADENCE_SOURCES = Object.freeze({
  PREPARED_SEQUENCE: 'prepared_sequence',
  UNRESOLVED: 'unresolved',
});

function normalizeSteps(raw) {
  if (!Array.isArray(raw)) return [];
  return raw
    .map((step, index) => ({
      day: Number(step?.day),
      index,
      step: step?.step != null ? Number(step.step) : index,
    }))
    .filter((row) => Number.isFinite(row.day))
    .sort((a, b) => a.day - b.day || a.index - b.index);
}

function extractStepsFromObject(obj = {}) {
  if (!obj || typeof obj !== 'object') return [];
  const candidates = [
    obj.steps,
    obj.sequence,
    obj.preparedSequence,
    obj.outreachSequence,
    obj.preparedOutreach?.steps,
    obj.prepared?.steps,
    obj.sequenceSteps,
  ];
  for (const raw of candidates) {
    const steps = normalizeSteps(raw);
    if (steps.length) return steps;
  }
  return [];
}

function extractPreparedSequenceSteps(input = {}) {
  const {
    mission = {},
    store = {},
    executionRecord = null,
    preparedArtifactRevision = null,
  } = input;

  const fromExecution = extractStepsFromObject(executionRecord?.payload || {});
  if (fromExecution.length) {
    return { steps: fromExecution, source: CADENCE_SOURCES.PREPARED_SEQUENCE };
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
    return { steps: emmettSteps, source: CADENCE_SOURCES.PREPARED_SEQUENCE };
  }

  const paigeSteps = extractStepsFromObject(paigePayload);
  if (paigeSteps.length) {
    return { steps: paigeSteps, source: CADENCE_SOURCES.PREPARED_SEQUENCE };
  }

  if (preparedArtifactRevision && store.listExecutionRecords) {
    const records = store.listExecutionRecords(mission.id, {});
    const match = records.find(
      (row) => asText(row.preparedArtifactRevision) === asText(preparedArtifactRevision)
    );
    const recordSteps = extractStepsFromObject(match?.payload || {});
    if (recordSteps.length) {
      return { steps: recordSteps, source: CADENCE_SOURCES.PREPARED_SEQUENCE };
    }
  }

  return { steps: [], source: CADENCE_SOURCES.UNRESOLVED };
}

/**
 * Resolve wait delta from current sent step to next unsent step.
 * @returns {{ cadenceSource: string, waitDays: number|null, currentStepDay: number|null, nextStepDay: number|null }}
 */
function resolveObserveCadence(input = {}) {
  const {
    mission = {},
    store = {},
    executionRecord = null,
    preparedArtifactRevision = null,
    sequenceStepSent = 0,
    clockStart = null,
    now = new Date(),
  } = input;

  const { steps, source } = extractPreparedSequenceSteps({
    mission,
    store,
    executionRecord,
    preparedArtifactRevision,
  });

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
  };
}

module.exports = {
  CADENCE_SOURCES,
  normalizeSteps,
  extractStepsFromObject,
  extractPreparedSequenceSteps,
  resolveObserveCadence,
};
