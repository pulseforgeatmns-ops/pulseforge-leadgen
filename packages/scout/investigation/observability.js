'use strict';

/**
 * SPEC-142 — Investigation engine observability.
 */

const { INVESTIGATION_EVENTS } = require('./types');

const _log = [];

function emitInvestigationStarted(meta = {}) {
  _log.push({
    event: INVESTIGATION_EVENTS.STARTED,
    at: new Date().toISOString(),
    ...meta,
  });
}

function emitInvestigationIteration(meta = {}) {
  _log.push({
    event: INVESTIGATION_EVENTS.ITERATION,
    at: new Date().toISOString(),
    ...meta,
  });
}

function emitInvestigationStep(meta = {}) {
  _log.push({
    event: INVESTIGATION_EVENTS.STEP,
    at: new Date().toISOString(),
    ...meta,
  });
}

function emitInvestigationConflict(meta = {}) {
  _log.push({
    event: INVESTIGATION_EVENTS.CONFLICT,
    at: new Date().toISOString(),
    ...meta,
  });
}

function emitInvestigationCompleted(meta = {}) {
  _log.push({
    event: INVESTIGATION_EVENTS.COMPLETED,
    at: new Date().toISOString(),
    ...meta,
  });
}

function listInvestigationLog() {
  return _log.slice();
}

function clearInvestigationLog() {
  _log.length = 0;
}

module.exports = {
  INVESTIGATION_EVENTS,
  emitInvestigationStarted,
  emitInvestigationIteration,
  emitInvestigationStep,
  emitInvestigationConflict,
  emitInvestigationCompleted,
  listInvestigationLog,
  clearInvestigationLog,
};
