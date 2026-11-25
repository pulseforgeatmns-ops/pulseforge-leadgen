'use strict';

/**
 * MAX-P0-003 — interrupted execution recovery (tenant-scoped, operator-run).
 *
 * Never scheduled: background workers stay disabled. Plan mode reads only.
 *
 *   claimed            older than staleAfter → abandoned_not_dispatched
 *                      (safe: the provider is only called after the dispatch
 *                      marker commits, so a bare claim was never dispatched)
 *   dispatch_attempted older than staleAfter → outcome_unknown (escalated;
 *                      never retried, resolved only by evidence-backed reconcile)
 *   receipt in_progress older than staleAfter → interrupted (replays answer
 *                      "outcome unknown", never re-run)
 *
 * Mission leases need no recovery: they are transaction-scoped and end with the
 * holder's connection. TME never persists an `executing` state (SPEC-131).
 */

const { recordReliabilityAudit } = require('./reliabilityAudit');
const { assertReliabilitySchema } = require('./schema');

const DEFAULT_STALE_AFTER_MS = 15 * 60 * 1000;
const MIN_STALE_AFTER_MS = 60 * 1000;

async function recoverInterruptedExecution(pool, opts = {}) {
  const tenantId = String(opts.tenantId == null ? '' : opts.tenantId).trim();
  if (!tenantId) throw Object.assign(new Error('tenantId is required.'), { code: 'recovery_tenant_required' });
  const staleAfterMs = Math.max(MIN_STALE_AFTER_MS, Number(opts.staleAfterMs) || DEFAULT_STALE_AFTER_MS);
  const apply = opts.apply === true;
  // Applying changes durable state: it must name the operator who authorised
  // it. Every audit row carries that actor; plan mode writes nothing.
  const actor = opts.actor == null ? '' : String(opts.actor).trim();
  if (apply && !/^[A-Za-z0-9][A-Za-z0-9._:@-]{0,79}$/.test(actor)) {
    throw Object.assign(new Error('Applying recovery requires an operator identity (actor).'), { code: 'recovery_actor_required' });
  }
  await assertReliabilitySchema(pool);
  const cutoff = new Date((opts.now ? opts.now() : new Date()).getTime() - staleAfterMs);

  const select = async (sql) => (await pool.query(sql, [tenantId, cutoff])).rows;
  const plan = {
    tenantId,
    cutoff: cutoff.toISOString(),
    staleClaims: await select(`SELECT idempotency_key, action_kind, mission_id, attempt, claimed_at, correlation_id
      FROM max_external_action_claims WHERE tenant_id = $1 AND state = 'claimed' AND claimed_at < $2 ORDER BY claimed_at`),
    staleDispatches: await select(`SELECT idempotency_key, action_kind, mission_id, attempt, dispatch_attempted_at, correlation_id
      FROM max_external_action_claims WHERE tenant_id = $1 AND state = 'dispatch_attempted' AND dispatch_attempted_at < $2 ORDER BY dispatch_attempted_at`),
    staleReceipts: await select(`SELECT scope, idempotency_key, created_at, correlation_id
      FROM max_request_receipts WHERE tenant_id = $1 AND state = 'in_progress' AND updated_at < $2 ORDER BY created_at`),
    unresolvedUnknown: (await pool.query(
      `SELECT count(*)::int AS n FROM max_external_action_claims WHERE tenant_id = $1 AND state = 'outcome_unknown'`, [tenantId]
    )).rows[0].n,
  };
  if (!apply) return { applied: false, ...summarize(plan), plan };

  const changed = { abandoned: [], unknown: [], interrupted: [] };
  for (const row of plan.staleClaims) {
    const r = await pool.query(
      `UPDATE max_external_action_claims SET state = 'abandoned_not_dispatched', error_class = 'holder_lost_before_dispatch',
         resolved_at = now(), updated_at = now()
       WHERE tenant_id = $1 AND idempotency_key = $2 AND state = 'claimed' AND claimed_at < $3 RETURNING idempotency_key`,
      [tenantId, row.idempotency_key, cutoff]);
    if (r.rows[0]) {
      changed.abandoned.push(row.idempotency_key);
      await recordReliabilityAudit(pool, { tenantId, correlationId: row.correlation_id, subjectKind: 'external_action',
        subjectKey: row.idempotency_key, event: 'recovery_abandoned_not_dispatched', fromState: 'claimed',
        toState: 'abandoned_not_dispatched', detail: { missionId: row.mission_id, actionKind: row.action_kind, actor } });
    }
  }
  for (const row of plan.staleDispatches) {
    const r = await pool.query(
      `UPDATE max_external_action_claims SET state = 'outcome_unknown', error_class = 'holder_lost_after_dispatch', updated_at = now()
       WHERE tenant_id = $1 AND idempotency_key = $2 AND state = 'dispatch_attempted' AND dispatch_attempted_at < $3 RETURNING idempotency_key`,
      [tenantId, row.idempotency_key, cutoff]);
    if (r.rows[0]) {
      changed.unknown.push(row.idempotency_key);
      await recordReliabilityAudit(pool, { tenantId, correlationId: row.correlation_id, subjectKind: 'external_action',
        subjectKey: row.idempotency_key, event: 'recovery_outcome_unknown_escalated', fromState: 'dispatch_attempted',
        toState: 'outcome_unknown', detail: { missionId: row.mission_id, actionKind: row.action_kind, escalation: 'operator_reconcile_required', actor } });
    }
  }
  for (const row of plan.staleReceipts) {
    const r = await pool.query(
      `UPDATE max_request_receipts SET state = 'interrupted', error_code = 'request_interrupted', updated_at = now()
       WHERE tenant_id = $1 AND scope = $2 AND idempotency_key = $3 AND state = 'in_progress' AND updated_at < $4 RETURNING idempotency_key`,
      [tenantId, row.scope, row.idempotency_key, cutoff]);
    if (r.rows[0]) {
      changed.interrupted.push(`${row.scope}:${row.idempotency_key}`);
      await recordReliabilityAudit(pool, { tenantId, correlationId: row.correlation_id, subjectKind: 'request_receipt',
        subjectKey: `${row.scope}:${row.idempotency_key}`, event: 'recovery_request_interrupted', fromState: 'in_progress',
        toState: 'interrupted', detail: { scope: row.scope, actor } });
    }
  }
  await recordReliabilityAudit(pool, { tenantId, correlationId: opts.correlationId || null, subjectKind: 'recovery_run',
    subjectKey: `${tenantId}:${(opts.now ? opts.now() : new Date()).toISOString()}`, event: 'recovery_applied', fromState: null, toState: null,
    detail: { actor, cutoff: plan.cutoff, staleAfterMs, abandoned: changed.abandoned.length, unknown: changed.unknown.length,
      interrupted: changed.interrupted.length, unresolvedUnknownBefore: plan.unresolvedUnknown } });
  return { applied: true, actor, ...summarize(plan), changed, escalationRequired: changed.unknown.length + plan.unresolvedUnknown > 0 };
}

function summarize(plan) {
  return {
    tenantId: plan.tenantId,
    cutoff: plan.cutoff,
    counts: {
      staleClaims: plan.staleClaims.length,
      staleDispatches: plan.staleDispatches.length,
      staleReceipts: plan.staleReceipts.length,
      unresolvedUnknown: plan.unresolvedUnknown,
    },
  };
}

module.exports = { recoverInterruptedExecution, DEFAULT_STALE_AFTER_MS };
