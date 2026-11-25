'use strict';

/**
 * MAX-P0-003 — durable external-action ledger.
 *
 * Exactly-once delivery to an external provider is not universally achievable:
 * a process can crash after the provider accepted a request and before the
 * acknowledgement is persisted. This ledger therefore makes the uncertainty
 * explicit instead of hiding it, and never retries an uncertain action.
 *
 * One row per (tenant_id, idempotency_key). States:
 *
 *   claimed                   claim committed; provider NOT invoked
 *   dispatch_attempted        committed BEFORE the provider call; outcome pending
 *   provider_acknowledged     provider accepted (provider_ref when supplied)
 *   verified                  independently verified (webhook, provider lookup, operator evidence)
 *   failed_definitive         provably not delivered (rejected before/at the boundary)
 *   outcome_unknown           dispatched, outcome not known — escalated, never auto-retried
 *   abandoned_not_dispatched  a claim whose holder died before the dispatch marker
 *
 * Invariants:
 *   - The provider is invoked only after markDispatchAttempted() has COMMITTED,
 *     on its own connection (never inside a longer business transaction that
 *     could roll the marker back).
 *   - A key reused with a different request fingerprint is rejected.
 *   - provider_acknowledged / verified replay the original receipt; nothing is
 *     dispatched again.
 *   - dispatch_attempted / outcome_unknown block every new claim until an
 *     evidence-backed reconcile() resolves them.
 *   - Only failed_definitive and abandoned_not_dispatched may be claimed again.
 *   - Rows never expire; there is no TTL. Retention must never delete a row
 *     whose action was, or may have been, dispatched.
 *   - Every read and write is tenant-scoped.
 */

const crypto = require('crypto');
const os = require('os');
const { redact, shortHash } = require('./redaction');
const { recordReliabilityAudit } = require('./reliabilityAudit');
const { assertReliabilitySchema } = require('./schema');

const STATES = Object.freeze({
  CLAIMED: 'claimed',
  DISPATCH_ATTEMPTED: 'dispatch_attempted',
  PROVIDER_ACKNOWLEDGED: 'provider_acknowledged',
  VERIFIED: 'verified',
  FAILED_DEFINITIVE: 'failed_definitive',
  OUTCOME_UNKNOWN: 'outcome_unknown',
  ABANDONED_NOT_DISPATCHED: 'abandoned_not_dispatched',
});

const CLAIM_OUTCOMES = Object.freeze({
  CLAIMED: 'claimed',
  REPLAY: 'replay',
  BLOCKED: 'blocked',
});

const BLOCK_CODES = Object.freeze({
  KEY_REUSED: 'idempotency_key_reused',
  IN_PROGRESS: 'external_action_in_progress',
  OUTCOME_UNKNOWN: 'external_action_outcome_unknown',
  SUBJECT_ALREADY_DISPATCHED: 'outreach_intent_already_dispatched',
});

const REPLAYABLE = new Set([STATES.PROVIDER_ACKNOWLEDGED, STATES.VERIFIED]);
const RECLAIMABLE = new Set([STATES.FAILED_DEFINITIVE, STATES.ABANDONED_NOT_DISPATCHED]);
const UNRESOLVED = new Set([STATES.DISPATCH_ATTEMPTED, STATES.OUTCOME_UNKNOWN]);

function ledgerError(code, message, extra = {}) {
  return Object.assign(new Error(message), { code, spec: 'MAX-P0-003' }, extra);
}

function requireText(value, name) {
  const text = value == null ? '' : String(value).trim();
  if (!text) throw ledgerError('external_action_invalid', `${name} is required.`);
  return text;
}

/** Canonical, order-independent SHA-256 of the logical request. */
function requestFingerprint(value) {
  const canonical = (v) => {
    if (v == null || typeof v !== 'object') return v === undefined ? null : v;
    if (Array.isArray(v)) return v.map(canonical);
    return Object.keys(v).sort().reduce((acc, k) => {
      if (v[k] !== undefined) acc[k] = canonical(v[k]);
      return acc;
    }, {});
  };
  return crypto.createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
}

function newOwnerToken() {
  return `${os.hostname().slice(0, 32)}:${process.pid}:${crypto.randomBytes(8).toString('hex')}`;
}

function receiptFromRow(row) {
  if (!row) return null;
  return {
    tenantId: String(row.tenant_id),
    idempotencyKey: row.idempotency_key,
    actionKind: row.action_kind,
    state: row.state,
    attempt: Number(row.attempt),
    missionId: row.mission_id || null,
    taskId: row.task_id || null,
    subjectRef: row.subject_ref || null,
    provider: row.provider || null,
    providerRef: row.provider_ref || null,
    correlationId: row.correlation_id || null,
    errorClass: row.error_class || null,
    claimedAt: row.claimed_at ? new Date(row.claimed_at).toISOString() : null,
    dispatchAttemptedAt: row.dispatch_attempted_at ? new Date(row.dispatch_attempted_at).toISOString() : null,
    acknowledgedAt: row.acknowledged_at ? new Date(row.acknowledged_at).toISOString() : null,
    verifiedAt: row.verified_at ? new Date(row.verified_at).toISOString() : null,
    resolvedAt: row.resolved_at ? new Date(row.resolved_at).toISOString() : null,
  };
}

/**
 * Classify a provider result. Unknown is the default: only explicit evidence
 * that nothing was (or could have been) accepted counts as definitive.
 */
function classifyProviderResult(result) {
  if (result && result.success === true) {
    return { state: STATES.PROVIDER_ACKNOWLEDGED, providerRef: result.providerMessageId || result.messageId || null };
  }
  if (!result || typeof result !== 'object') return { state: STATES.OUTCOME_UNKNOWN, errorClass: 'no_provider_result' };
  if (result.definitive === true) return { state: STATES.FAILED_DEFINITIVE, errorClass: String(result.providerErrorCode || 'provider_rejected') };
  if (result.providerBoundaryCrossed === false) {
    return { state: STATES.FAILED_DEFINITIVE, errorClass: String(result.providerErrorCode || 'rejected_before_provider') };
  }
  const http = /^brevo_http_(\d{3})$/.exec(String(result.providerErrorCode || ''));
  if (http) {
    const status = Number(http[1]);
    // 4xx is a provider rejection — except statuses where the request may still
    // have been (or be) processed: timeout, conflict, too-early, rate limit.
    if (status >= 400 && status < 500 && ![408, 409, 425, 429].includes(status)) {
      return { state: STATES.FAILED_DEFINITIVE, errorClass: `provider_http_${status}` };
    }
  }
  return { state: STATES.OUTCOME_UNKNOWN, errorClass: String(result.providerErrorCode || 'provider_outcome_unknown') };
}

function createExternalActionLedger(pool, options = {}) {
  if (!pool || typeof pool.query !== 'function') throw ledgerError('external_action_ledger_unavailable', 'A database pool is required.');
  const now = typeof options.now === 'function' ? options.now : () => new Date();

  async function audit(row, event, fromState, toState, detail) {
    await recordReliabilityAudit(pool, {
      tenantId: row.tenant_id,
      correlationId: row.correlation_id,
      subjectKind: 'external_action',
      subjectKey: row.idempotency_key,
      event,
      fromState,
      toState,
      detail: { actionKind: row.action_kind, missionId: row.mission_id, attempt: row.attempt, ...(detail || {}) },
    });
  }

  async function load(tenantId, idempotencyKey) {
    const r = await pool.query(
      'SELECT * FROM max_external_action_claims WHERE tenant_id = $1 AND idempotency_key = $2',
      [tenantId, idempotencyKey]
    );
    return r.rows[0] || null;
  }

  /**
   * Claim an action before any provider call.
   * @returns {Promise<{ outcome: 'claimed'|'replay'|'blocked', code?: string, ownerToken?: string, receipt: object|null }>}
   */
  async function claim(input = {}) {
    await assertReliabilitySchema(pool);
    const tenantId = requireText(input.tenantId, 'tenantId');
    const idempotencyKey = requireText(input.idempotencyKey, 'idempotencyKey');
    const actionKind = requireText(input.actionKind, 'actionKind');
    const fingerprint = requireText(input.requestFingerprint, 'requestFingerprint');
    const ownerToken = newOwnerToken();
    const correlationId = input.correlationId || null;

    const r7Block = (err) => {
      if (err && err.code === '23505' && err.constraint === 'max_external_action_claims_one_outbound_per_subject') {
        return { outcome: CLAIM_OUTCOMES.BLOCKED, code: BLOCK_CODES.SUBJECT_ALREADY_DISPATCHED, receipt: null };
      }
      throw err;
    };
    // task_id is introduced by the additive P0-004 migration. Keep the
    // P0-003-only path usable until that migration is explicitly applied.
    const taskColumn = input.taskId ? ', task_id' : '';
    const taskValue = input.taskId ? ',$6' : '';
    const offset = input.taskId ? 1 : 0;
    const inserted = await pool.query(
      `INSERT INTO max_external_action_claims (
         tenant_id, idempotency_key, action_kind, request_fingerprint, attempt, state,
         mission_id${taskColumn}, subject_ref, provider, provider_idempotency_key, correlation_id, owner_token,
         claimed_at, updated_at)
       VALUES ($1,$2,$3,$4,1,'claimed',$5${taskValue},$${6 + offset},$${7 + offset},$${8 + offset},$${9 + offset},$${10 + offset},$${11 + offset},$${11 + offset})
       ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
       RETURNING *`,
      input.taskId
        ? [tenantId, idempotencyKey, actionKind, fingerprint, input.missionId || null, input.taskId, input.subjectRef || null,
          input.provider || null, input.providerIdempotencyKey || null, correlationId, ownerToken, now()]
        : [tenantId, idempotencyKey, actionKind, fingerprint, input.missionId || null, input.subjectRef || null,
          input.provider || null, input.providerIdempotencyKey || null, correlationId, ownerToken, now()]
    ).catch((err) => ({ r7: r7Block(err) }));
    if (inserted.r7) return inserted.r7;
    if (inserted.rows[0]) {
      await audit(inserted.rows[0], 'claimed', null, STATES.CLAIMED);
      return { outcome: CLAIM_OUTCOMES.CLAIMED, ownerToken, receipt: receiptFromRow(inserted.rows[0]) };
    }

    const existing = await load(tenantId, idempotencyKey);
    if (!existing) {
      // Deleted between the conflict and the read — refuse rather than guess.
      return { outcome: CLAIM_OUTCOMES.BLOCKED, code: BLOCK_CODES.IN_PROGRESS, receipt: null };
    }
    if (existing.request_fingerprint !== fingerprint || existing.action_kind !== actionKind) {
      await audit(existing, 'claim_rejected_fingerprint_mismatch', existing.state, existing.state,
        { offeredFingerprint: shortHash(fingerprint) });
      return { outcome: CLAIM_OUTCOMES.BLOCKED, code: BLOCK_CODES.KEY_REUSED, receipt: receiptFromRow(existing) };
    }
    if (REPLAYABLE.has(existing.state)) {
      return { outcome: CLAIM_OUTCOMES.REPLAY, receipt: receiptFromRow(existing) };
    }
    if (UNRESOLVED.has(existing.state)) {
      await audit(existing, 'claim_rejected_outcome_unknown', existing.state, existing.state);
      return { outcome: CLAIM_OUTCOMES.BLOCKED, code: BLOCK_CODES.OUTCOME_UNKNOWN, receipt: receiptFromRow(existing) };
    }
    if (existing.state === STATES.CLAIMED) {
      return { outcome: CLAIM_OUTCOMES.BLOCKED, code: BLOCK_CODES.IN_PROGRESS, receipt: receiptFromRow(existing) };
    }
    if (RECLAIMABLE.has(existing.state)) {
      // Compare-and-set on the observed state + attempt: two concurrent re-claims
      // cannot both win.
      const reclaimed = await pool.query(
        `UPDATE max_external_action_claims
            SET state = 'claimed', attempt = attempt + 1, owner_token = $3, correlation_id = $4,
                provider_ref = NULL, error_class = NULL, claimed_at = $5, dispatch_attempted_at = NULL,
                acknowledged_at = NULL, verified_at = NULL, resolved_at = NULL, updated_at = $5
          WHERE tenant_id = $1 AND idempotency_key = $2 AND state = $6 AND attempt = $7
        RETURNING *`,
        [tenantId, idempotencyKey, ownerToken, correlationId, now(), existing.state, existing.attempt]
      ).catch((err) => ({ r7: r7Block(err) }));
      if (reclaimed.r7) return reclaimed.r7;
      if (reclaimed.rows[0]) {
        await audit(reclaimed.rows[0], 'reclaimed', existing.state, STATES.CLAIMED);
        return { outcome: CLAIM_OUTCOMES.CLAIMED, ownerToken, receipt: receiptFromRow(reclaimed.rows[0]) };
      }
      return { outcome: CLAIM_OUTCOMES.BLOCKED, code: BLOCK_CODES.IN_PROGRESS, receipt: receiptFromRow(await load(tenantId, idempotencyKey)) };
    }
    return { outcome: CLAIM_OUTCOMES.BLOCKED, code: BLOCK_CODES.IN_PROGRESS, receipt: receiptFromRow(existing) };
  }

  async function transition(input, fromStates, toState, set, event) {
    await assertReliabilitySchema(pool);
    const tenantId = requireText(input.tenantId, 'tenantId');
    const idempotencyKey = requireText(input.idempotencyKey, 'idempotencyKey');
    const ownerClause = input.ownerToken ? 'AND owner_token = $4' : '';
    const params = [tenantId, idempotencyKey, fromStates];
    if (input.ownerToken) params.push(input.ownerToken);
    const base = params.length;
    const assignments = [`state = '${toState}'`, `updated_at = $${base + 1}`];
    params.push(now());
    for (const [column, value] of Object.entries(set || {})) {
      params.push(value);
      assignments.push(`${column} = $${params.length}`);
    }
    const before = await load(tenantId, idempotencyKey);
    const r = await pool.query(
      `UPDATE max_external_action_claims SET ${assignments.join(', ')}
        WHERE tenant_id = $1 AND idempotency_key = $2 AND state = ANY($3::text[]) ${ownerClause}
      RETURNING *`,
      params
    );
    if (!r.rows[0]) {
      throw ledgerError('external_action_state_conflict',
        `External action cannot move to ${toState} from ${before ? before.state : 'missing'}.`,
        { receipt: receiptFromRow(before) });
    }
    await audit(r.rows[0], event, before && before.state, toState, input.auditDetail);
    return receiptFromRow(r.rows[0]);
  }

  /** Must COMMIT before the provider is invoked. */
  function markDispatchAttempted(input) {
    return transition(input, [STATES.CLAIMED], STATES.DISPATCH_ATTEMPTED, { dispatch_attempted_at: now() }, 'dispatch_attempted');
  }

  /** Record a provider result classified by classifyProviderResult(). */
  function recordProviderResult(input, providerResult) {
    const c = classifyProviderResult(providerResult);
    const set = { error_class: c.errorClass || null };
    if (c.state === STATES.PROVIDER_ACKNOWLEDGED) {
      set.provider_ref = c.providerRef || null;
      set.acknowledged_at = now();
    } else if (c.state === STATES.FAILED_DEFINITIVE) {
      set.resolved_at = now();
    }
    return transition(input, [STATES.DISPATCH_ATTEMPTED], c.state, set, `provider_${c.state}`);
  }

  /** The provider call threw: the request may or may not have been accepted. */
  function markOutcomeUnknown(input, errorClass = 'provider_exception') {
    return transition(input, [STATES.DISPATCH_ATTEMPTED], STATES.OUTCOME_UNKNOWN,
      { error_class: String(errorClass).slice(0, 120) }, 'outcome_unknown');
  }

  /** Release a claim that never reached the provider (pre-dispatch guard refused). */
  function releaseUndispatched(input, reason = 'not_dispatched') {
    return transition(input, [STATES.CLAIMED], STATES.ABANDONED_NOT_DISPATCHED,
      { error_class: String(reason).slice(0, 120), resolved_at: now() }, 'released_not_dispatched');
  }

  /**
   * Resolve an uncertain or acknowledged action from independent evidence.
   * evidence.source is required (e.g. 'brevo_webhook_delivered',
   * 'brevo_message_lookup', 'operator_attestation'); providerRef when known.
   */
  async function reconcile(input = {}) {
    const evidence = input.evidence || {};
    const source = requireText(evidence.source, 'evidence.source');
    const resolution = input.resolution;
    const allowed = {
      [STATES.VERIFIED]: [STATES.DISPATCH_ATTEMPTED, STATES.OUTCOME_UNKNOWN, STATES.PROVIDER_ACKNOWLEDGED],
      [STATES.PROVIDER_ACKNOWLEDGED]: [STATES.DISPATCH_ATTEMPTED, STATES.OUTCOME_UNKNOWN],
      [STATES.FAILED_DEFINITIVE]: [STATES.DISPATCH_ATTEMPTED, STATES.OUTCOME_UNKNOWN],
    }[resolution];
    if (!allowed) throw ledgerError('external_action_invalid', 'resolution must be verified, provider_acknowledged or failed_definitive.');
    const set = { resolved_at: now() };
    if (resolution === STATES.VERIFIED) set.verified_at = now();
    if (resolution === STATES.PROVIDER_ACKNOWLEDGED) set.acknowledged_at = now();
    if (evidence.providerRef) set.provider_ref = String(evidence.providerRef);
    return transition({ ...input, ownerToken: null, auditDetail: { evidence: redact({ source, ...evidence }), actor: input.actor || null } },
      allowed, resolution, set, `reconciled_${resolution}`);
  }

  async function get(tenantId, idempotencyKey) {
    await assertReliabilitySchema(pool);
    return receiptFromRow(await load(requireText(tenantId, 'tenantId'), requireText(idempotencyKey, 'idempotencyKey')));
  }

  /**
   * R7 — claims for the SAME logical outreach target under a DIFFERENT key that
   * may have reached (or are reaching) the recipient. Changing message content
   * creates a new key; it must not create a second send while any of these exist.
   * failed_definitive and abandoned_not_dispatched never delivered and do not block.
   */
  async function findBlockingForSubject(input = {}) {
    await assertReliabilitySchema(pool);
    const r = await pool.query(
      `SELECT * FROM max_external_action_claims
        WHERE tenant_id = $1 AND action_kind = $2 AND mission_id = $3 AND subject_ref = $4
          AND idempotency_key <> $5
          AND state IN ('claimed','dispatch_attempted','provider_acknowledged','verified','outcome_unknown')
        ORDER BY updated_at DESC LIMIT 1`,
      [requireText(input.tenantId, 'tenantId'), requireText(input.actionKind, 'actionKind'),
        requireText(input.missionId, 'missionId'), requireText(input.subjectRef, 'subjectRef'),
        requireText(input.excludeKey, 'excludeKey')]
    );
    return receiptFromRow(r.rows[0]);
  }

  async function listUnresolved(tenantId, { limit = 100 } = {}) {
    await assertReliabilitySchema(pool);
    const r = await pool.query(
      `SELECT * FROM max_external_action_claims
        WHERE tenant_id = $1 AND state IN ('dispatch_attempted','outcome_unknown','claimed')
        ORDER BY updated_at ASC LIMIT $2`,
      [requireText(tenantId, 'tenantId'), Math.min(Math.max(1, Number(limit) || 100), 1000)]
    );
    return r.rows.map(receiptFromRow);
  }

  return {
    claim,
    markDispatchAttempted,
    recordProviderResult,
    markOutcomeUnknown,
    releaseUndispatched,
    reconcile,
    get,
    findBlockingForSubject,
    listUnresolved,
  };
}

module.exports = {
  STATES,
  CLAIM_OUTCOMES,
  BLOCK_CODES,
  createExternalActionLedger,
  classifyProviderResult,
  requestFingerprint,
  receiptFromRow,
};
