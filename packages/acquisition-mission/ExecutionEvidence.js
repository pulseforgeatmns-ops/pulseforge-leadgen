'use strict';

/**
 * P0 Max reliability — execution evidence at the Canonical Execution Router
 * boundary (SPEC-171).
 *
 * A dispatched request is not proof of execution. Every router result is
 * classified into exactly one terminal execution state, and only
 * `completed_verified` and `already_executed` may be presented as done.
 *
 * Lifecycle (adapted to the existing CER + TME contracts, not replacing them):
 *
 *   accepted              CER minted and validated (assertRequest/permissions)
 *   persisted             mission state durable (persistStageCommit)
 *   dispatched            handler invoked by routeExecutionRequest
 *   running               TME stage executing (never durable — SPEC-131)
 *   awaiting_authorization  pending operator decision blocks the stage
 *   completed_verified    TME committed: transactionId + executionOutcome=completed
 *   already_executed      idempotent replay of a previously committed stage
 *   blocked               stage committed a blocked outcome (no external effect)
 *   failed                TME rolled back; mission restored to prior version
 *   outcome_unknown       handler returned without commit or rollback evidence
 *
 * Classification is structural only (no wording, no tenant data).
 */

const EXECUTION_STATES = Object.freeze({
  ACCEPTED: 'accepted',
  PERSISTED: 'persisted',
  DISPATCHED: 'dispatched',
  RUNNING: 'running',
  AWAITING_AUTHORIZATION: 'awaiting_authorization',
  COMPLETED_VERIFIED: 'completed_verified',
  ALREADY_EXECUTED: 'already_executed',
  BLOCKED: 'blocked',
  FAILED: 'failed',
  OUTCOME_UNKNOWN: 'outcome_unknown',
});

/** States that may be reported to the operator as finished work. */
const VERIFIED_STATES = new Set([
  EXECUTION_STATES.COMPLETED_VERIFIED,
  EXECUTION_STATES.ALREADY_EXECUTED,
]);

/** Specialists whose work produces external or mission-changing effects. */
const WORK_SPECIALISTS = new Set(['scout', 'paige', 'emmett', 'penny', 'rex', 'cal', 'sam', 'riley', 'vera']);

function text(value) {
  return value == null ? null : String(value);
}

/**
 * @param {object} input
 * @param {object} input.request - frozen CER
 * @param {object|null} input.executionResult - handler result
 * @param {string|null} [input.specialist]
 * @param {string|null} [input.runtimeOwner]
 * @param {string|null} [input.tenantId]
 * @returns {{
 *   state: string, verified: boolean, reason: string,
 *   evidence: object|null, correlation: object
 * }}
 */
function classifyExecutionEvidence(input = {}) {
  const request = input.request || {};
  const result = input.executionResult;
  const specialist = input.specialist || null;
  const transactionId = result && typeof result === 'object'
    ? text(result.transactionId || (result.error && result.error.transactionId))
    : null;

  const correlation = {
    requestId: text(request.id),
    missionId: text(request.missionId),
    tenantId: text(input.tenantId),
    operatorId: text(request.operatorId),
    intent: text(request.intent),
    specialist,
    runtimeOwner: text(input.runtimeOwner || request.runtimeOwner),
    executionMode: text(request.executionMode),
    authorizationScope: request.permissions && typeof request.permissions === 'object'
      ? { ...request.permissions }
      : null,
    parentRequestId: text(request.metadata && request.metadata.parentRequestId),
    transactionId,
  };

  const out = (state, reason, evidence = null) => ({
    state,
    verified: VERIFIED_STATES.has(state),
    reason,
    evidence,
    correlation,
  });

  if (!result || typeof result !== 'object') {
    return out(EXECUTION_STATES.OUTCOME_UNKNOWN, 'no_handler_result');
  }
  if (result.rolledBack === true) {
    return out(EXECUTION_STATES.FAILED, 'transaction_rolled_back', {
      transactionId,
      rollbackReason: text(result.rollbackReason),
    });
  }
  if (result.alreadyExecuted === true) {
    return out(EXECUTION_STATES.ALREADY_EXECUTED, 'idempotent_replay', {
      transactionId,
      missionVersion: result.missionVersion != null ? Number(result.missionVersion) : null,
    });
  }
  if (result.blocked === true || result.executionOutcome === 'blocked') {
    return out(EXECUTION_STATES.BLOCKED, 'stage_blocked', { transactionId });
  }
  const committed = result.committed === true || Boolean(transactionId);
  if (committed && (result.executionOutcome == null || result.executionOutcome === 'completed')) {
    return out(EXECUTION_STATES.COMPLETED_VERIFIED, 'transaction_committed', {
      transactionId,
      missionVersion: result.missionVersion != null ? Number(result.missionVersion) : null,
      stage: text(result.stage),
    });
  }
  if (committed) {
    return out(EXECUTION_STATES.OUTCOME_UNKNOWN, `unrecognized_outcome:${text(result.executionOutcome)}`, {
      transactionId,
    });
  }
  return out(EXECUTION_STATES.OUTCOME_UNKNOWN, 'no_commit_evidence');
}

/**
 * True when Max must not present a specialist as having completed work.
 * @param {{ state: string, correlation?: { specialist?: string } }} execution
 */
function specialistCompletionUnverified(execution) {
  if (!execution) return false;
  const specialist = execution.correlation && execution.correlation.specialist;
  return WORK_SPECIALISTS.has(String(specialist || '')) && !VERIFIED_STATES.has(execution.state);
}

module.exports = {
  EXECUTION_STATES,
  VERIFIED_STATES,
  WORK_SPECIALISTS,
  classifyExecutionEvidence,
  specialistCompletionUnverified,
};
