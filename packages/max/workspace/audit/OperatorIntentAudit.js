'use strict';

/**
 * SPEC-153 — Audit guard: language parsers must not run after OperatorIntent
 * is sealed for a turn. Tests enable strict mode via OPERATOR_INTENT_AUDIT=strict.
 */

/** @type {Set<string>} */
const PRE_INTENT_PARSERS = new Set([
  'isMissionExecutionCommand',
  'isMissionPlanningTurn',
  'hasExecutionLanguage',
  'detectMissionExecutionLanguage',
]);

/** @type {string[]} */
let postIntentViolations = [];

/** @type {object|null} */
let sealedIntent = null;

function resetOperatorIntentAudit() {
  postIntentViolations = [];
  sealedIntent = null;
}

function sealOperatorIntent(intent) {
  sealedIntent = intent || null;
}

function recordPreIntentParsing(parserName) {
  void parserName;
}

function guardPostIntentParsing(parserName) {
  if (!sealedIntent) return;
  if (PRE_INTENT_PARSERS.has(parserName)) {
    postIntentViolations.push(parserName);
    if (process.env.OPERATOR_INTENT_AUDIT === 'strict') {
      throw new Error(
        `SPEC-153 violation: ${parserName} invoked after OperatorIntent was sealed`
      );
    }
  }
}

function getOperatorIntentAuditViolations() {
  return [...postIntentViolations];
}

function getSealedOperatorIntent() {
  return sealedIntent;
}

module.exports = {
  PRE_INTENT_PARSERS,
  resetOperatorIntentAudit,
  sealOperatorIntent,
  recordPreIntentParsing,
  guardPostIntentParsing,
  getOperatorIntentAuditViolations,
  getSealedOperatorIntent,
};
