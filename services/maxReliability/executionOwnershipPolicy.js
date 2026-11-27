'use strict';

/**
 * MAX-P0-004 criterion 1 — no execution or mission mutation bypasses durable
 * task ownership once durable tasks are enabled (MAX_DURABLE_TASKS=true).
 *
 * With the flag unset nothing here changes behaviour. With it set, four choke
 * points enforce ownership:
 *   - routeExecutionRequest  requires a task store (or the offline exemption);
 *   - executeMissionStage    requires a live task fence (or the offline exemption);
 *   - OutboundExecutionAdapter requires a live task fence (never exempt);
 *   - persistStageCommit     requires a live task fence or a WRITE exemption.
 *
 * Exemptions are a closed allowlist. None of them permits a specialist stage
 * or an external send. Write exemptions permit only a leased mission write
 * (inside a mission lease transaction) and every exempted write is recorded in
 * max_reliability_audit IN THAT SAME TRANSACTION, so nothing is untracked.
 */
const { AsyncLocalStorage } = require('async_hooks');

const storage = new AsyncLocalStorage();

/** Non-execution mission writes that are not requests to run a specialist. */
const WRITE_EXEMPTIONS = Object.freeze({
  mission_creation: 'Creating a new mission record (no specialist runs, no external effect).',
  operator_restriction_tightening: 'Adding operator restrictions to an existing mission (monotonic, narrowing only).',
  inbound_observation: 'Recording an inbound provider event or reply interpretation (no outbound effect).',
});

/**
 * Non-durable evaluation (no pool, nothing persisted). Permits in-memory
 * routing and TME only; never a durable write, never a send, and discovery only
 * with an injected Scout runner (no live provider).
 */
const OFFLINE_EVALUATION = 'offline_evaluation';

const ALL_EXEMPTIONS = Object.freeze([...Object.keys(WRITE_EXEMPTIONS), OFFLINE_EVALUATION]);

/**
 * Mission fields a write exemption may never change (mission_creation aside;
 * operator_restriction_tightening may change only authorizationConstraints).
 */
const EXEMPT_GUARDED_FIELDS = Object.freeze(['stage', 'status', 'planCancelled', 'pendingOperatorDecision',
  'structuredMission', 'objective', 'tenantId', 'clientId', 'authorizationConstraints']);

/** Canonical fingerprint of a mission's guarded fields (key order irrelevant). */
function guardedSnapshot(mission) {
  const { requestFingerprint } = require('./externalActionLedger');
  const picked = {};
  for (const f of EXEMPT_GUARDED_FIELDS) picked[f] = mission && mission[f] !== undefined ? mission[f] : null;
  return requestFingerprint(picked);
}

function ownershipError(code, message, extra = {}) {
  return Object.assign(new Error(message), { code, spec: 'MAX-P0-004', statusCode: 409, ...extra });
}

/** Ownership is enforced only when durable tasks are explicitly enabled. */
function ownershipEnforced(env = process.env) {
  return String(env.MAX_DURABLE_TASKS || '').toLowerCase() === 'true';
}

/** Run fn under one named exemption from the closed allowlist. */
function runWithOwnershipExemption(reason, fn) {
  if (!ALL_EXEMPTIONS.includes(reason)) {
    throw ownershipError('durable_task_exemption_invalid', `Unknown ownership exemption: ${reason}.`);
  }
  return storage.run({ reason }, fn);
}

function currentOwnershipExemption() {
  const store = storage.getStore();
  return store ? store.reason : null;
}

function isWriteExempt() {
  const reason = currentOwnershipExemption();
  return Boolean(reason && Object.prototype.hasOwnProperty.call(WRITE_EXEMPTIONS, reason));
}

function isOfflineEvaluation() {
  return currentOwnershipExemption() === OFFLINE_EVALUATION;
}

/** Record an exempted mission write in the lease transaction (atomic with the write). */
async function auditExemptWrite(client, { missionId, tenantId, reason }) {
  const { shortHash } = require('./redaction');
  await client.query(
    `INSERT INTO max_reliability_audit (tenant_id, subject_kind, subject_key_hash, event, detail, at)
     VALUES ($1, 'mission_write_exemption', $2, 'exempt_mission_write', $3::jsonb, clock_timestamp())`,
    [String(tenantId), shortHash(String(missionId)), JSON.stringify({ reason, missionId: String(missionId) })]
  );
}

module.exports = {
  EXEMPT_GUARDED_FIELDS,
  guardedSnapshot,
  WRITE_EXEMPTIONS,
  OFFLINE_EVALUATION,
  ALL_EXEMPTIONS,
  ownershipEnforced,
  ownershipError,
  runWithOwnershipExemption,
  currentOwnershipExemption,
  isWriteExempt,
  isOfflineEvaluation,
  auditExemptWrite,
};
