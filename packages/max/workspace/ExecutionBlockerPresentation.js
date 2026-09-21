'use strict';

/**
 * Operator-facing blocker messages for TME / SEC / discovery failures.
 * Preserves actionable business reasons instead of generic "Execution blocked."
 */

const { asText } = require('../../acquisition-mission/types');
const { formatRollbackProse } = require('../../acquisition-mission/ExecutionErrors');

const GENERIC_BLOCKER_PATTERNS = [
  /^execution blocked\.?$/i,
  /^scout discovery blocked\.?$/i,
  /^discovery blocked\.?$/i,
  /^execution blocked by missing precondition\.?$/i,
];

function isGenericBlockerMessage(message) {
  const text = asText(message);
  if (!text) return true;
  return GENERIC_BLOCKER_PATTERNS.some((re) => re.test(text.trim()));
}

function codeToBlockerMessage(code) {
  const map = Object.freeze({
    tme_evidence_missing: 'Scout contribution did not attach canonical evidence.',
    sec_evidence_missing: 'Scout contribution did not attach canonical evidence.',
    tme_contribution_missing: 'Specialist contribution did not commit.',
    tme_contract: 'Specialist contribution failed contract validation.',
    sec_validation: 'Specialist output failed validation.',
    tme_specialist: 'Specialist execution failed.',
    scout_blocked: 'Scout discovery could not complete.',
    external_discovery_capability_unavailable: 'External discovery capability is unavailable.',
    tme_plan_missing: 'Mission plan must be approved before discovery.',
    tme_wrong_stage: 'Mission is not in the discovery stage.',
    tme_mission_inactive: 'Mission is inactive.',
    discovery_blocked: 'Discovery is blocked until prerequisites are resolved.',
    coverage_incomplete: 'Discovery coverage is incomplete for prioritization.',
    no_ranked_prospects: 'Discovery did not surface ranked prospects.',
    missing_buying_signals: 'Discovery evidence lacks required buying signals.',
    missing_provenance: 'Discovery evidence lacks required provenance.',
  });
  return map[asText(code)] || null;
}

function fromScoutPayload(scoutPayload = {}) {
  if (scoutPayload.summary && !isGenericBlockerMessage(scoutPayload.summary)) {
    return scoutPayload.summary;
  }
  if (scoutPayload.blocked && scoutPayload.blocked.reason && !isGenericBlockerMessage(scoutPayload.blocked.reason)) {
    return scoutPayload.blocked.reason;
  }
  if (scoutPayload.blockReason && !isGenericBlockerMessage(scoutPayload.blockReason)) {
    return scoutPayload.blockReason;
  }
  return null;
}

function fromError(err = {}) {
  const codeMessage = codeToBlockerMessage(err.code);
  if (codeMessage) return codeMessage;
  if (err.message && !isGenericBlockerMessage(err.message)) return err.message;
  if (err.rollbackReason && !isGenericBlockerMessage(err.rollbackReason)) return err.rollbackReason;
  if (err.blocked && err.blocked.reason && !isGenericBlockerMessage(err.blocked.reason)) {
    return err.blocked.reason;
  }
  return null;
}

/**
 * Resolve the best operator-facing blocker message.
 *
 * @param {object} input
 * @param {object} [input.error]
 * @param {string} [input.rollbackReason]
 * @param {object} [input.scoutPayload]
 * @param {object} [input.executionResult]
 * @param {string} [input.stageName]
 * @returns {{ message: string, waitingOn: string, nextStep: string, code: string|null }}
 */
function resolveExecutionBlocker(input = {}) {
  const err = input.error || {};
  const scoutPayload = input.scoutPayload || {};
  const executionResult = input.executionResult || {};
  const stageName = input.stageName || 'Discovery';

  const specific =
    fromScoutPayload(scoutPayload) ||
    fromError({ ...err, rollbackReason: input.rollbackReason || executionResult.rollbackReason }) ||
    fromError(executionResult.error || {}) ||
    codeToBlockerMessage(err.code || executionResult.errorClass);

  const code = asText(err.code || executionResult.errorClass) || null;
  const message = specific || formatRollbackProse(stageName);

  const waitingOn = specific
    ? (code ? `${code}: ${specific}` : specific)
    : 'Resolve the blocker';

  const recommended =
    err.recommendedAction ||
    (scoutPayload.blocked && scoutPayload.blocked.recommendedAction) ||
    (err.blocked && err.blocked.recommendedAction) ||
    null;

  const nextStep = recommended || (specific ? `Resolve: ${specific}` : 'Resolve the blocker and retry.');

  return {
    message,
    waitingOn,
    nextStep,
    code,
  };
}

module.exports = {
  resolveExecutionBlocker,
  isGenericBlockerMessage,
  codeToBlockerMessage,
};
