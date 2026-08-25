'use strict';

/**
 * SPEC-131 — Transactional Mission Execution error classes.
 * Planning / specialist / persistence failures roll back.
 * Presentation failures do not.
 */

const TME_CLASSES = Object.freeze({
  PLANNING: 'planning',
  PRECONDITION: 'precondition',
  SPECIALIST: 'specialist',
  VALIDATION: 'validation',
  PERSISTENCE: 'persistence',
  PRESENTATION: 'presentation',
});

const ROLLBACK_CLASSES = new Set([
  TME_CLASSES.PLANNING,
  TME_CLASSES.PRECONDITION,
  TME_CLASSES.SPECIALIST,
  TME_CLASSES.VALIDATION,
  TME_CLASSES.PERSISTENCE,
]);

function tmeError(tmeClass, code, message, extras = {}) {
  const err = new Error(message);
  err.name = classToName(tmeClass);
  err.code = code;
  err.tmeClass = tmeClass;
  err.spec = 'SPEC-131';
  err.rollback = ROLLBACK_CLASSES.has(tmeClass);
  err.commitStatus = extras.commitStatus || (err.rollback ? 'rolled_back' : 'committed');
  err.rollbackReason = extras.rollbackReason || (err.rollback ? message : null);
  if (extras.cause) err.cause = extras.cause;
  if (extras.transactionId) err.transactionId = extras.transactionId;
  if (extras.missionId) err.missionId = extras.missionId;
  if (extras.missionVersion != null) err.missionVersion = extras.missionVersion;
  if (extras.details) err.details = extras.details;
  return err;
}

function classToName(tmeClass) {
  if (tmeClass === TME_CLASSES.PLANNING) return 'PlanningError';
  if (tmeClass === TME_CLASSES.PRECONDITION) return 'PreconditionError';
  if (tmeClass === TME_CLASSES.SPECIALIST) return 'SpecialistError';
  if (tmeClass === TME_CLASSES.VALIDATION) return 'ValidationError';
  if (tmeClass === TME_CLASSES.PERSISTENCE) return 'PersistenceError';
  if (tmeClass === TME_CLASSES.PRESENTATION) return 'PresentationError';
  return 'TmeError';
}

function planningError(code, message, extras) {
  return tmeError(TME_CLASSES.PLANNING, code || 'tme_planning', message, extras);
}

function preconditionError(code, message, extras) {
  return tmeError(TME_CLASSES.PRECONDITION, code || 'tme_precondition', message, extras);
}

function specialistError(code, message, extras) {
  return tmeError(TME_CLASSES.SPECIALIST, code || 'tme_specialist', message, extras);
}

function validationError(code, message, extras) {
  return tmeError(TME_CLASSES.VALIDATION, code || 'tme_validation', message, extras);
}

function persistenceError(code, message, extras) {
  return tmeError(TME_CLASSES.PERSISTENCE, code || 'tme_persistence', message, extras);
}

function presentationError(code, message, extras) {
  return tmeError(TME_CLASSES.PRESENTATION, code || 'tme_presentation', message, extras);
}

function isTmeError(err) {
  return Boolean(err && err.spec === 'SPEC-131' && err.tmeClass);
}

function isRolledBackExecution(err) {
  return Boolean(isTmeError(err) && err.rollback && err.commitStatus === 'rolled_back');
}

function wrapAs(tmeClass, err, fallbackCode, fallbackMessage) {
  if (isTmeError(err) && err.tmeClass === tmeClass) return err;
  const wrapped = tmeError(
    tmeClass,
    (err && err.code) || fallbackCode,
    (err && err.message) || fallbackMessage,
    { cause: err }
  );
  return wrapped;
}

function classifyError(err) {
  if (isTmeError(err)) return err.tmeClass;
  return TME_CLASSES.SPECIALIST;
}

function formatRollbackProse(stageLabel = 'Discovery', err = null) {
  const label = String(stageLabel || 'Discovery').replace(/^\w/, (ch) => ch.toUpperCase());
  const reason = err && (err.rollbackReason || err.message);
  const recommendation = (err && err.details && err.details.recommendedAction)
    || (reason && /insufficient evidence|coverage is incomplete|discovery evidence is insufficient/i.test(reason)
      ? 'Continue investigation.'
      : null)
    || 'Resolve the blocker and retry.';
  const lines = [`${label} could not execute.`];
  if (reason) {
    lines.push(`Reason: ${reason}`);
  } else {
    lines.push('Mission remains unchanged.');
  }
  lines.push(`Recommendation: ${recommendation}`);
  return lines.join('\n');
}

module.exports = {
  TME_CLASSES,
  tmeError,
  planningError,
  preconditionError,
  specialistError,
  validationError,
  persistenceError,
  presentationError,
  isTmeError,
  isRolledBackExecution,
  wrapAs,
  classifyError,
  formatRollbackProse,
};
