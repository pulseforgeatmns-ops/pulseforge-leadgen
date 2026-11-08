'use strict';

/**
 * P0 Max reliability — execution evidence at the Canonical Execution Router
 * boundary (SPEC-171).
 *
 * A dispatched request is not proof of execution, and an internal transaction
 * commit is not proof of an external outcome. Every router result is classified
 * into exactly one state against the evidence requirement of its intent.
 *
 * Lifecycle (adapted to the existing CER + TME contracts, not replacing them):
 *
 *   accepted               CER minted and validated (assertRequest/permissions)
 *   persisted              mission state durable (persistStageCommit)
 *   dispatched             handler invoked by routeExecutionRequest
 *   running                TME stage executing (never durable — SPEC-131)
 *   awaiting_authorization pending operator decision blocks the stage
 *   committed_internal     internal work (plan, prioritization, drafts, capacity)
 *                          committed by TME — the artifact IS the outcome
 *   completed_verified     external work with its provider evidence present
 *   partially_completed    some external effects confirmed, some failed/unknown
 *   already_executed       idempotent replay of a previously committed stage
 *   blocked                stage blocked (precondition, governor, constraint)
 *   failed                 TME rolled back, or provider rejected every attempt
 *   synthetic_result       committed from fixture data, not a live provider
 *   outcome_unknown        no commit/rollback evidence, or external effect
 *                          lacks the provider evidence its intent requires
 *
 * Reportable as done: committed_internal, completed_verified, already_executed.
 * Classification is structural (no wording, no tenant data).
 */

const EXECUTION_STATES = Object.freeze({
  ACCEPTED: 'accepted',
  PERSISTED: 'persisted',
  DISPATCHED: 'dispatched',
  RUNNING: 'running',
  AWAITING_AUTHORIZATION: 'awaiting_authorization',
  COMMITTED_INTERNAL: 'committed_internal',
  COMPLETED_VERIFIED: 'completed_verified',
  PARTIALLY_COMPLETED: 'partially_completed',
  ALREADY_EXECUTED: 'already_executed',
  BLOCKED: 'blocked',
  FAILED: 'failed',
  SYNTHETIC_RESULT: 'synthetic_result',
  OUTCOME_UNKNOWN: 'outcome_unknown',
});

/** States that may be reported to the operator as finished work. */
const VERIFIED_STATES = new Set([
  EXECUTION_STATES.COMMITTED_INTERNAL,
  EXECUTION_STATES.COMPLETED_VERIFIED,
  EXECUTION_STATES.ALREADY_EXECUTED,
]);

/** Specialists whose work produces external or mission-changing effects. */
const WORK_SPECIALISTS = new Set(['scout', 'paige', 'emmett', 'penny', 'rex', 'cal', 'sam', 'riley', 'vera']);

const SYNTHETIC_PROVENANCE = 'synthetic_fixture';

/**
 * Evidence each intent must produce before it may be reported as done.
 * Internal intents are complete when TME commits; external intents need
 * evidence from the provider they acted through.
 */
const EVIDENCE_REQUIREMENTS = Object.freeze({
  execute_outbound: Object.freeze({
    kind: 'provider_acceptance',
    level: 'provider_accepted',
    external: true,
    requirement:
      'Every attempted send record has status "sent" and a provider message id. ' +
      'Delivery, opens and replies arrive later by webhook and are not implied.',
  }),
  approve_discovery: Object.freeze({
    kind: 'provider_results',
    level: 'provider_results_committed',
    external: true,
    requirement: 'A live (non-synthetic) Scout result committed with a discovery record.',
  }),
  start_discovery: null,
  continue_investigation: null,
  internal: Object.freeze({
    kind: 'internal_commit',
    level: 'internal_commit',
    external: false,
    requirement: 'A committed TME transaction for the stage.',
  }),
});

function requirementFor(intent) {
  // CER intents are upper-case constants (EXECUTE_OUTBOUND).
  const key = String(intent || '').toLowerCase();
  if (key === 'start_discovery' || key === 'continue_investigation') {
    return EVIDENCE_REQUIREMENTS.approve_discovery;
  }
  return EVIDENCE_REQUIREMENTS[key] || EVIDENCE_REQUIREMENTS.internal;
}

function text(value) {
  return value == null ? null : String(value);
}

function isConstraintError(err) {
  if (!err) return false;
  if (err.code === 'authorization_constraint_blocked') return true;
  return Boolean(err.cause && err.cause.code === 'authorization_constraint_blocked');
}

/**
 * Summarize outbound execution records against provider acceptance.
 * @param {object[]} records
 */
function summarizeProviderRecords(records) {
  const counts = { total: 0, accepted: 0, unconfirmed: 0, failed: 0, pending: 0, blocked: 0 };
  const providerMessageIds = [];
  for (const row of records || []) {
    if (!row || typeof row !== 'object') continue;
    counts.total += 1;
    const status = String(row.status || '').toLowerCase();
    if (status === 'sent' && row.providerMessageId) {
      counts.accepted += 1;
      if (providerMessageIds.length < 20) providerMessageIds.push(String(row.providerMessageId));
    } else if (status === 'sent') counts.unconfirmed += 1;
    else if (status === 'failed') counts.failed += 1;
    else if (status === 'blocked') counts.blocked += 1;
    else counts.pending += 1; // queued / attempted / unknown
  }
  return { counts, providerMessageIds };
}

function synthetic(result) {
  const scout = result && result.scoutResult;
  return Boolean(
    (scout && scout.provenance === SYNTHETIC_PROVENANCE) ||
      (result && result.provenance === SYNTHETIC_PROVENANCE)
  );
}

/**
 * @param {object} input
 * @param {object} input.request - frozen CER
 * @param {object|null} input.executionResult - handler result
 * @param {string|null} [input.specialist]
 * @param {string|null} [input.runtimeOwner]
 * @param {string|null} [input.tenantId]
 */
function classifyExecutionEvidence(input = {}) {
  const request = input.request || {};
  const result = input.executionResult;
  const specialist = input.specialist || null;
  const requirement = requirementFor(request.intent);
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

  const out = (state, reason, evidence = null, level = null) => ({
    state,
    verified: VERIFIED_STATES.has(state),
    reason,
    evidence,
    verification: {
      kind: requirement.kind,
      requirement: requirement.requirement,
      external: requirement.external,
      levelRequired: requirement.level,
      levelAchieved: level,
    },
    correlation,
  });

  if (!result || typeof result !== 'object') {
    return out(EXECUTION_STATES.OUTCOME_UNKNOWN, 'no_handler_result');
  }
  if (result.rolledBack === true) {
    const syntheticErr = result.error && (result.error.code === 'synthetic_evidence_blocked'
      ? result.error
      : (result.error.cause && result.error.cause.code === 'synthetic_evidence_blocked' ? result.error.cause : null));
    if (syntheticErr) {
      return out(EXECUTION_STATES.BLOCKED, 'synthetic_evidence', {
        transactionId,
        actionClass: text(syntheticErr.actionClass),
        provenance: SYNTHETIC_PROVENANCE,
      });
    }
    if (isConstraintError(result.error)) {
      const err = result.error.code === 'authorization_constraint_blocked' ? result.error : result.error.cause;
      return out(EXECUTION_STATES.BLOCKED, 'authorization_constraint', {
        transactionId,
        actionClass: text(err && err.actionClass),
        prohibited: (err && err.authorizationConstraints && err.authorizationConstraints.prohibited) || [],
      });
    }
    return out(EXECUTION_STATES.FAILED, 'transaction_rolled_back', {
      transactionId,
      rollbackReason: text(result.rollbackReason),
    });
  }
  if (result.alreadyExecuted === true) {
    return out(EXECUTION_STATES.ALREADY_EXECUTED, 'idempotent_replay', {
      transactionId,
      missionVersion: result.missionVersion != null ? Number(result.missionVersion) : null,
    }, 'prior_commit');
  }
  if (result.blocked === true || result.executionOutcome === 'blocked') {
    return out(EXECUTION_STATES.BLOCKED, 'stage_blocked', { transactionId });
  }

  const committed = result.committed === true || Boolean(transactionId);
  if (!committed) return out(EXECUTION_STATES.OUTCOME_UNKNOWN, 'no_commit_evidence');
  if (result.executionOutcome != null && result.executionOutcome !== 'completed') {
    return out(EXECUTION_STATES.OUTCOME_UNKNOWN, `unrecognized_outcome:${text(result.executionOutcome)}`, {
      transactionId,
    });
  }

  if (synthetic(result)) {
    return out(EXECUTION_STATES.SYNTHETIC_RESULT, 'synthetic_fixture_data', { transactionId, provenance: SYNTHETIC_PROVENANCE });
  }

  if (requirement.kind === 'provider_acceptance') {
    const { counts, providerMessageIds } = summarizeProviderRecords(result.records);
    const evidence = { transactionId, counts, providerMessageIds, provider: 'brevo' };
    if (!counts.total) return out(EXECUTION_STATES.OUTCOME_UNKNOWN, 'no_provider_records', evidence);
    if (counts.blocked === counts.total) return out(EXECUTION_STATES.BLOCKED, 'all_sends_blocked', evidence);
    const effective = counts.total - counts.blocked;
    if (counts.accepted === effective) {
      return out(EXECUTION_STATES.COMPLETED_VERIFIED, 'provider_accepted_all', evidence, requirement.level);
    }
    if (counts.accepted > 0) {
      return out(EXECUTION_STATES.PARTIALLY_COMPLETED, 'provider_accepted_some', evidence, requirement.level);
    }
    if (counts.failed === effective) return out(EXECUTION_STATES.FAILED, 'provider_rejected_all', evidence);
    return out(
      EXECUTION_STATES.OUTCOME_UNKNOWN,
      counts.unconfirmed > 0 ? 'provider_acceptance_unconfirmed' : 'provider_outcome_pending',
      evidence
    );
  }

  if (requirement.kind === 'provider_results') {
    const discoveryId = result.discovery && (result.discovery.id || result.discovery.contributionId);
    if (!discoveryId) return out(EXECUTION_STATES.OUTCOME_UNKNOWN, 'no_discovery_record', { transactionId });
    return out(EXECUTION_STATES.COMPLETED_VERIFIED, 'live_provider_results_committed', {
      transactionId,
      discoveryId: text(discoveryId),
      qualifiedCount: result.discovery.payload && result.discovery.payload.qualifiedCount != null
        ? Number(result.discovery.payload.qualifiedCount)
        : null,
    }, requirement.level);
  }

  return out(EXECUTION_STATES.COMMITTED_INTERNAL, 'transaction_committed', {
    transactionId,
    missionVersion: result.missionVersion != null ? Number(result.missionVersion) : null,
    stage: text(result.stage),
  }, requirement.level);
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
  EVIDENCE_REQUIREMENTS,
  SYNTHETIC_PROVENANCE,
  requirementFor,
  summarizeProviderRecords,
  classifyExecutionEvidence,
  specialistCompletionUnverified,
};
