'use strict';

/**
 * MAX-P0-004 durable work acceptance and fenced ownership. This is a storage
 * boundary, not a worker: callers persist acceptance before acknowledging a
 * handoff; separately authorised workers may claim it later.
 *
 * Invariants (enforced here and, where possible, by the operator migration):
 *  - Every ownership-bound write is one compare-and-set on
 *    (tenant, task, ownership token, fence epoch, active state, unexpired
 *    database-clock lease, unexpired deadline). A superseded owner cannot
 *    start, heartbeat, checkpoint, complete, or pass the dispatch fence.
 *  - Every state change and its event row commit in one transaction.
 *  - Recovery, cancellation and verification lock the task row FOR UPDATE.
 *    The P0-003 dispatch marker for a task-linked claim locks the same row
 *    FOR SHARE (externalActionLedger.markDispatchAttempted with taskFence),
 *    so a dispatch either commits before them (and they see it) or is fenced.
 *  - An external action that is claimed but unresolved, or already
 *    dispatched, is never replayed: recovery escalates, cancellation after
 *    dispatch escalates, verification refuses.
 */
const { randomUUID } = require('crypto');
const { requestFingerprint } = require('./externalActionLedger');

const STATES = Object.freeze({
  REQUESTED: 'requested', QUEUED: 'queued', BLOCKED_DEPENDENCY: 'blocked_dependency',
  CLAIMED: 'claimed', RUNNING: 'running', AWAITING_VERIFICATION: 'awaiting_verification',
  RECOVERY_REQUIRED: 'recovery_required', COMPLETED: 'completed', FAILED: 'failed',
  CANCELLED: 'cancelled', ESCALATED: 'escalated',
});
const ACTIVE = [STATES.CLAIMED, STATES.RUNNING];
// recovery_required is deliberately NOT claimable: re-admission is explicit,
// through revalidateDependencies (drift → blocked_dependency, match → queued).
const CLAIMABLE = [STATES.QUEUED];
const WAITING = [STATES.REQUESTED, STATES.QUEUED, STATES.RECOVERY_REQUIRED, STATES.BLOCKED_DEPENDENCY];
const TERMINAL = [STATES.COMPLETED, STATES.FAILED, STATES.CANCELLED, STATES.ESCALATED];
/** Claim states whose external effect is undecided (never replay; reconcile). */
const UNRESOLVED_ACTION_STATES = ['claimed', 'dispatch_attempted', 'outcome_unknown'];
/** Claim states that may already have reached the provider. */
const DISPATCHED_ACTION_STATES = ['dispatch_attempted', 'outcome_unknown', 'provider_acknowledged', 'verified'];
const REQUIRED_TABLES = Object.freeze(['max_durable_execution_tasks', 'max_durable_execution_task_events']);
const MAX_RETRY_BUDGET = 100;
const verifiedPools = new WeakSet();

/** The single ownership predicate. $1 id, $2 tenant, $3 token, $4 epoch. */
const OWNER_PREDICATE = `id=$1 AND tenant_id=$2 AND ownership_token=$3 AND fence_epoch=$4
  AND state IN ('claimed','running') AND lease_expires_at > clock_timestamp()
  AND (deadline_at IS NULL OR deadline_at > clock_timestamp())`;

function error(code, message, extra = {}) { return Object.assign(new Error(message), { code, spec: 'MAX-P0-004', ...extra }); }
function text(value, name) { const out = value == null ? '' : String(value).trim(); if (!out) throw error('durable_task_invalid', `${name} is required.`); return out; }
function json(value, fallback) { return value == null ? fallback : value; }
function iso(value) { return value ? new Date(value).toISOString() : null; }
function row(task) {
  if (!task) return null;
  const { prior_state: _prior, deadline_passed: _deadline, ...rest } = task;
  return { ...rest, id: String(task.id), tenantId: String(task.tenant_id), objectiveId: task.objective_id,
    missionId: task.mission_id, parentTaskId: task.parent_task_id && String(task.parent_task_id), logicalKey: task.logical_key,
    specialist: task.specialist, state: task.state, ownershipToken: task.ownership_token, fenceEpoch: Number(task.fence_epoch),
    leaseExpiresAt: iso(task.lease_expires_at), heartbeatAt: iso(task.heartbeat_at), deadlineAt: iso(task.deadline_at),
    retryBudget: Number(task.retry_budget), recoveryAttempts: Number(task.recovery_attempts),
    recoveryReason: task.recovery_reason || null, cancellationReason: task.cancellation_reason || null,
    escalationReason: task.escalation_reason || null };
}
async function assertSchema(pool) {
  if (verifiedPools.has(pool)) return;
  let r;
  try { r = await pool.query('SELECT t AS name, to_regclass(t) IS NOT NULL AS installed FROM unnest($1::text[]) AS t', [REQUIRED_TABLES]); }
  catch (cause) { throw error('durable_task_schema_unavailable', 'Durable task schema could not be verified.', { cause }); }
  const missing = r.rows.filter(x => x.installed !== true).map(x => x.name);
  if (missing.length) throw error('durable_task_schema_missing', `Durable task schema missing: ${missing.join(', ')}.`, { missing });
  verifiedPools.add(pool);
}
function validOwner(input = {}) {
  const o = { tenantId: text(input.tenantId, 'tenantId'), taskId: text(input.taskId, 'taskId'),
    ownershipToken: text(input.ownershipToken, 'ownershipToken'), fenceEpoch: Number(input.fenceEpoch) };
  if (!Number.isSafeInteger(o.fenceEpoch) || o.fenceEpoch < 1) throw error('durable_task_invalid', 'fenceEpoch must be a positive integer.');
  return o;
}
function leaseOf(input) {
  const leaseMs = input.leaseMs == null ? 30000 : Number(input.leaseMs);
  if (!Number.isFinite(leaseMs) || leaseMs < 1000 || leaseMs > 300000) throw error('durable_task_invalid', 'leaseMs must be 1,000–300,000.');
  return leaseMs;
}
function deadlineOf(value) {
  if (value == null) return null;
  const at = new Date(value);
  if (Number.isNaN(at.getTime())) throw error('durable_task_invalid', 'deadlineAt must be a valid timestamp.');
  return at.toISOString();
}
function retryBudgetOf(value) {
  if (value == null) return 0;
  if (!Number.isSafeInteger(value) || value < 0 || value > MAX_RETRY_BUDGET) throw error('durable_task_invalid', `retryBudget must be an integer 0–${MAX_RETRY_BUDGET}.`);
  return value;
}

function createDurableExecutionOwnership(pool, options = {}) {
  if (!pool || typeof pool.query !== 'function' || typeof pool.connect !== 'function') throw error('durable_task_unavailable', 'A PostgreSQL pool is required.');
  const uuid = options.uuid || randomUUID;

  async function tx(work) {
    await assertSchema(pool);
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const out = await work(client);
      await client.query('COMMIT');
      return out;
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      throw err;
    } finally {
      client.release();
    }
  }
  async function event(db, task, eventName, from, to, detail = {}) {
    await db.query('INSERT INTO max_durable_execution_task_events(task_id,tenant_id,fence_epoch,event,from_state,to_state,detail) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb)',
      [task.id, task.tenant_id, task.fence_epoch, eventName, from, to, JSON.stringify(detail)]);
  }
  async function lockTask(db, tenantId, taskId) {
    return (await db.query('SELECT * FROM max_durable_execution_tasks WHERE id=$1 AND tenant_id=$2 FOR UPDATE', [taskId, tenantId])).rows[0] || null;
  }
  async function actionStates(db, tenantId, taskId) {
    const r = await db.query('SELECT state, count(*)::int AS n FROM max_external_action_claims WHERE tenant_id=$1 AND task_id=$2 GROUP BY state ORDER BY state', [tenantId, taskId]);
    return Object.fromEntries(r.rows.map(x => [x.state, x.n]));
  }
  const anyIn = (states, set) => set.some(s => states[s] > 0);

  async function accept(input = {}) {
    const tenantId = text(input.tenantId, 'tenantId'); const logicalKey = text(input.logicalKey, 'logicalKey'); const specialist = text(input.specialist, 'specialist');
    const executionScope = json(input.executionScope, {}); const immutableInput = json(input.immutableInput, {}); const dependencyRefs = json(input.dependencyRefs, []);
    if (!Array.isArray(dependencyRefs)) throw error('durable_task_invalid', 'dependencyRefs must be an array.');
    const deadlineAt = deadlineOf(input.deadlineAt); const retryBudget = retryBudgetOf(input.retryBudget);
    const parentTaskId = input.parentTaskId || null;
    // Deadline and retry budget are part of the accepted contract: a replay
    // that changes them is a different request.
    const fingerprint = requestFingerprint({ tenantId, objectiveId: input.objectiveId || null, missionId: input.missionId || null, parentTaskId,
      specialist, executionScope, immutableInput, dependencyRefs, deadlineAt, retryBudget });
    const taskId = input.taskId || uuid();
    if (parentTaskId && String(parentTaskId) === String(taskId)) throw error('durable_task_invalid', 'A task cannot be its own parent.');
    return tx(async (db) => {
      const inserted = await db.query(`INSERT INTO max_durable_execution_tasks(id,tenant_id,objective_id,mission_id,parent_task_id,logical_key,input_fingerprint,specialist,execution_scope,immutable_input,dependency_refs,state,deadline_at,retry_budget)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10::jsonb,$11::jsonb,'queued',$12,$13)
        ON CONFLICT DO NOTHING RETURNING *`, [taskId, tenantId, input.objectiveId || null, input.missionId || null, parentTaskId, logicalKey, fingerprint, specialist,
        JSON.stringify(executionScope), JSON.stringify(immutableInput), JSON.stringify(dependencyRefs), deadlineAt, retryBudget]);
      if (inserted.rows[0]) { await event(db, inserted.rows[0], 'accepted', null, STATES.QUEUED); return { accepted: true, replay: false, task: row(inserted.rows[0]) }; }
      const existing = (await db.query('SELECT * FROM max_durable_execution_tasks WHERE tenant_id=$1 AND logical_key=$2', [tenantId, logicalKey])).rows[0];
      // No row for this key: either a concurrent writer, or a caller-supplied
      // taskId that already exists (possibly in another tenant). Never say which.
      if (!existing) throw error('durable_task_conflict', 'Task could not be accepted with this identity; retry with the same key or without a taskId.');
      if (existing.input_fingerprint !== fingerprint) throw error('durable_task_key_reused', 'logicalKey was already accepted with different immutable input.');
      return { accepted: true, replay: true, task: row(existing) };
    });
  }

  async function claim(input = {}) {
    const tenantId = text(input.tenantId, 'tenantId'); const ownerToken = text(input.ownerToken, 'ownerToken'); const leaseMs = leaseOf(input);
    return tx(async (db) => {
      // A concurrent claimer may activate another task of the same mission
      // between our check and update; the unique index decides, and the loser
      // simply claims nothing this round.
      const claimed = await db.query(`WITH candidate AS (
          SELECT id, state AS prior_state FROM max_durable_execution_tasks
           WHERE tenant_id=$1 AND state=ANY($2::text[]) AND (deadline_at IS NULL OR deadline_at>clock_timestamp())
             AND (mission_id IS NULL OR NOT EXISTS (SELECT 1 FROM max_durable_execution_tasks active
                   WHERE active.tenant_id=max_durable_execution_tasks.tenant_id AND active.mission_id=max_durable_execution_tasks.mission_id
                     AND active.state IN ('claimed','running')))
           ORDER BY accepted_at, id FOR UPDATE SKIP LOCKED LIMIT 1)
        UPDATE max_durable_execution_tasks t SET state='claimed', ownership_token=$3, fence_epoch=t.fence_epoch+1,
               lease_expires_at=clock_timestamp()+($4 * interval '1 millisecond'), heartbeat_at=clock_timestamp(), updated_at=clock_timestamp()
          FROM candidate WHERE t.id=candidate.id RETURNING t.*, candidate.prior_state`, [tenantId, CLAIMABLE, ownerToken, leaseMs])
        .catch((err) => { if (err && err.code === '23505' && err.constraint === 'max_durable_execution_tasks_one_active_per_mission') return { rows: [], missionBusy: true }; throw err; });
      const task = claimed.rows[0]; if (!task) return null;
      await event(db, task, 'claimed', task.prior_state, STATES.CLAIMED, { ownerToken });
      return row(task);
    });
  }

  /**
   * Claim one specific queued task (the inline owner of a request it just
   * accepted). Same CAS as claim(): new token, fence epoch + 1, DB-clock lease,
   * unexpired deadline, at most one active task per mission. Returns null when
   * the task is not claimable or its mission already has an active task.
   */
  async function claimTask(input = {}) {
    const tenantId = text(input.tenantId, 'tenantId'); const taskId = text(input.taskId, 'taskId');
    const ownerToken = text(input.ownerToken, 'ownerToken'); const leaseMs = leaseOf(input);
    return tx(async (db) => {
      const claimed = await db.query(`WITH candidate AS (
          SELECT id, state AS prior_state FROM max_durable_execution_tasks
           WHERE id=$1 AND tenant_id=$2 AND state=ANY($3::text[]) AND (deadline_at IS NULL OR deadline_at>clock_timestamp())
             AND (mission_id IS NULL OR NOT EXISTS (SELECT 1 FROM max_durable_execution_tasks active
                   WHERE active.tenant_id=max_durable_execution_tasks.tenant_id AND active.mission_id=max_durable_execution_tasks.mission_id
                     AND active.state IN ('claimed','running')))
           FOR UPDATE)
        UPDATE max_durable_execution_tasks t SET state='claimed', ownership_token=$4, fence_epoch=t.fence_epoch+1,
               lease_expires_at=clock_timestamp()+($5 * interval '1 millisecond'), heartbeat_at=clock_timestamp(), updated_at=clock_timestamp()
          FROM candidate WHERE t.id=candidate.id RETURNING t.*, candidate.prior_state`, [taskId, tenantId, CLAIMABLE, ownerToken, leaseMs])
        .catch((err) => { if (err && err.code === '23505' && err.constraint === 'max_durable_execution_tasks_one_active_per_mission') return { rows: [] }; throw err; });
      const task = claimed.rows[0]; if (!task) return null;
      await event(db, task, 'claimed', task.prior_state, STATES.CLAIMED, { ownerToken });
      return row(task);
    });
  }

  /**
   * The current owner reports a definitive failure (nothing in doubt). If a
   * task-linked external action is unresolved, the task is escalated instead:
   * a failure must never hide an effect that may have happened.
   */
  async function fail(input = {}) {
    const o = validOwner(input); const reason = String(input.reason || 'failed').slice(0, 200);
    return tx(async (db) => {
      const task = (await db.query(`SELECT * FROM max_durable_execution_tasks WHERE ${OWNER_PREDICATE} FOR UPDATE`,
        [o.taskId, o.tenantId, o.ownershipToken, o.fenceEpoch])).rows[0];
      if (!task) throw error('durable_task_fenced', 'Failure rejected for stale owner.');
      const actions = await actionStates(db, o.tenantId, o.taskId);
      const unresolved = anyIn(actions, UNRESOLVED_ACTION_STATES);
      const r = await db.query(`UPDATE max_durable_execution_tasks SET state=$3, recovery_reason=$4, escalation_reason=$5,
           result_ref=$6::jsonb, ownership_token=NULL, lease_expires_at=NULL, updated_at=clock_timestamp()
         WHERE id=$1 AND tenant_id=$2 RETURNING *`,
      [o.taskId, o.tenantId, unresolved ? STATES.ESCALATED : STATES.FAILED, reason,
        unresolved ? 'external_action_unresolved' : null, JSON.stringify(json(input.resultRef, {}))]);
      await event(db, r.rows[0], unresolved ? 'failure_escalated' : 'failed', task.state, r.rows[0].state, { reason, actionStates: actions });
      return row(r.rows[0]);
    });
  }

  /**
   * Read-only recovery plan for one tenant: what recoverExpired would decide
   * now, without changing anything (used by the operator CLI's plan mode).
   */
  async function planRecovery(input = {}) {
    await assertSchema(pool);
    const tenantId = text(input.tenantId, 'tenantId'); const limit = Math.min(500, Math.max(1, Number(input.limit) || 100));
    const r = await pool.query(`SELECT t.*, (t.deadline_at IS NOT NULL AND t.deadline_at<=clock_timestamp()) AS deadline_passed,
         EXISTS (SELECT 1 FROM max_external_action_claims c WHERE c.tenant_id=t.tenant_id AND c.task_id=t.id
                  AND c.state = ANY($4::text[])) AS action_unresolved
       FROM max_durable_execution_tasks t
      WHERE t.tenant_id=$1 AND ((t.state=ANY($2::text[]) AND t.lease_expires_at<=clock_timestamp())
         OR (t.state=ANY($3::text[]) AND t.deadline_at IS NOT NULL AND t.deadline_at<=clock_timestamp()))
      ORDER BY t.updated_at, t.id LIMIT $5`, [tenantId, ACTIVE, WAITING, UNRESOLVED_ACTION_STATES, limit]);
    return r.rows.map((task) => {
      let decision = 'recovery_required'; let reason = 'lease_expired';
      if (task.action_unresolved) { decision = 'escalated'; reason = 'external_action_unresolved'; }
      else if (task.deadline_passed) { decision = 'escalated'; reason = 'deadline_exceeded'; }
      else if (Number(task.recovery_attempts) >= Number(task.retry_budget)) { decision = 'escalated'; reason = 'retry_budget_exhausted'; }
      const { action_unresolved: _a, ...rest } = task;
      return { task: row(rest), decision, reason };
    });
  }

  async function requireOwner(input) {
    await assertSchema(pool); const o = validOwner(input);
    const r = await pool.query(`SELECT * FROM max_durable_execution_tasks WHERE ${OWNER_PREDICATE}`, [o.taskId, o.tenantId, o.ownershipToken, o.fenceEpoch]);
    if (!r.rows[0]) throw error('durable_task_fenced', 'Task ownership is stale, expired, cancelled, past its deadline, or unavailable.');
    return row(r.rows[0]);
  }

  /** One fenced compare-and-set plus its event, in one transaction. */
  async function ownedUpdate(input, { set, params = [], fromStates = ACTIVE, eventName, toState, detail, message }) {
    const o = validOwner(input);
    return tx(async (db) => {
      const r = await db.query(`WITH prior AS (SELECT id, state AS prior_state FROM max_durable_execution_tasks WHERE ${OWNER_PREDICATE} AND state=ANY($5::text[]) FOR UPDATE)
        UPDATE max_durable_execution_tasks t SET ${set}, updated_at=clock_timestamp() FROM prior WHERE t.id=prior.id RETURNING t.*, prior.prior_state`,
      [o.taskId, o.tenantId, o.ownershipToken, o.fenceEpoch, fromStates, ...params]);
      const task = r.rows[0];
      if (!task) throw error('durable_task_fenced', message);
      if (eventName) await event(db, task, eventName, task.prior_state, toState || task.state, detail);
      return row(task);
    });
  }
  function start(input) {
    return ownedUpdate(input, { set: "state='running'", fromStates: [STATES.CLAIMED], eventName: 'started', toState: STATES.RUNNING, message: 'Task cannot be started by this owner.' });
  }
  function heartbeat(input = {}) {
    const leaseMs = leaseOf(input);
    return ownedUpdate(input, { set: "heartbeat_at=clock_timestamp(), lease_expires_at=clock_timestamp()+($6 * interval '1 millisecond')", params: [leaseMs], message: 'Task lease cannot be renewed by this owner.' });
  }
  function checkpoint(input = {}) {
    return ownedUpdate(input, { set: 'progress_checkpoint=$6::jsonb', params: [JSON.stringify(json(input.checkpoint, {}))], eventName: 'checkpointed', message: 'Checkpoint rejected for stale owner.' });
  }
  /**
   * Specialist completion moves to awaiting_verification. If the task still
   * has an unresolved external action, it cannot be verified and nobody owns
   * it any more, so it is escalated in the same transaction instead of
   * waiting indefinitely.
   */
  async function reportCompletion(input = {}) {
    const o = validOwner(input);
    return tx(async (db) => {
      const r = await db.query(`WITH prior AS (SELECT id FROM max_durable_execution_tasks WHERE ${OWNER_PREDICATE} AND state='running' FOR UPDATE)
        UPDATE max_durable_execution_tasks t SET state='awaiting_verification', result_ref=$5::jsonb, evidence_refs=$6::jsonb,
               ownership_token=NULL, lease_expires_at=NULL, updated_at=clock_timestamp() FROM prior WHERE t.id=prior.id RETURNING t.*`,
      [o.taskId, o.tenantId, o.ownershipToken, o.fenceEpoch, JSON.stringify(json(input.resultRef, {})), JSON.stringify(json(input.evidenceRefs, []))]);
      let task = r.rows[0];
      if (!task) throw error('durable_task_fenced', 'Completion rejected for stale owner or a task that was never started.');
      await event(db, task, 'specialist_completed', STATES.RUNNING, STATES.AWAITING_VERIFICATION);
      const actions = await actionStates(db, o.tenantId, o.taskId);
      if (anyIn(actions, UNRESOLVED_ACTION_STATES)) {
        task = (await db.query(`UPDATE max_durable_execution_tasks SET state='escalated', escalation_reason='external_action_unresolved', updated_at=clock_timestamp()
          WHERE id=$1 AND tenant_id=$2 RETURNING *`, [o.taskId, o.tenantId])).rows[0];
        await event(db, task, 'completion_escalated', STATES.AWAITING_VERIFICATION, STATES.ESCALATED, { reason: 'external_action_unresolved', actionStates: actions });
      }
      return row(task);
    });
  }

  async function verify(input = {}) {
    const tenantId = text(input.tenantId, 'tenantId'); const taskId = text(input.taskId, 'taskId');
    if (!Array.isArray(input.evidenceRefs) || !input.evidenceRefs.length) throw error('durable_task_invalid', 'Verification requires evidenceRefs.');
    return tx(async (db) => {
      const task = await lockTask(db, tenantId, taskId);
      if (!task || task.state !== STATES.AWAITING_VERIFICATION) throw error('durable_task_state_conflict', 'Task is not awaiting verification.');
      const actions = await actionStates(db, tenantId, taskId);
      if (anyIn(actions, UNRESOLVED_ACTION_STATES)) throw error('durable_task_external_action_unresolved', 'Task has an external action requiring reconciliation.', { actionStates: actions });
      const r = await db.query(`UPDATE max_durable_execution_tasks SET state='completed', evidence_refs=$3::jsonb, completed_at=clock_timestamp(), updated_at=clock_timestamp()
        WHERE id=$1 AND tenant_id=$2 RETURNING *`, [taskId, tenantId, JSON.stringify(input.evidenceRefs)]);
      await event(db, r.rows[0], 'verified', STATES.AWAITING_VERIFICATION, STATES.COMPLETED, { evidenceCount: input.evidenceRefs.length });
      return row(r.rows[0]);
    });
  }

  /**
   * Bounded recovery sweep for one tenant. Active tasks whose lease expired,
   * and waiting tasks whose deadline passed, are decided under a row lock:
   *   external action unresolved → escalated (never replayed)
   *   deadline passed            → escalated
   *   retry budget exhausted     → escalated
   *   otherwise                  → recovery_required (checkpoint preserved)
   * recovery_attempts counts lease-expiry recoveries; it is incremented here,
   * so revalidation or requeueing can never reset the budget.
   */
  async function recoverExpired(input = {}) {
    const tenantId = text(input.tenantId, 'tenantId'); const limit = Math.min(100, Math.max(1, Number(input.limit) || 100));
    return tx(async (db) => {
      const due = await db.query(`SELECT *, (deadline_at IS NOT NULL AND deadline_at<=clock_timestamp()) AS deadline_passed FROM max_durable_execution_tasks
         WHERE tenant_id=$1 AND ((state=ANY($2::text[]) AND lease_expires_at<=clock_timestamp())
            OR (state=ANY($3::text[]) AND deadline_at IS NOT NULL AND deadline_at<=clock_timestamp()))
         ORDER BY updated_at, id FOR UPDATE SKIP LOCKED LIMIT $4`, [tenantId, ACTIVE, WAITING, limit]);
      const out = [];
      for (const task of due.rows) {
        const actions = await actionStates(db, tenantId, task.id);
        const wasActive = ACTIVE.includes(task.state);
        let reason; let escalate = true;
        if (anyIn(actions, UNRESOLVED_ACTION_STATES)) reason = 'external_action_unresolved';
        else if (task.deadline_passed) reason = 'deadline_exceeded';
        else if (Number(task.recovery_attempts) >= Number(task.retry_budget)) reason = 'retry_budget_exhausted';
        else { reason = 'lease_expired'; escalate = false; }
        const r = await db.query(`UPDATE max_durable_execution_tasks SET state=$3, recovery_reason=$4, escalation_reason=$5,
             recovery_attempts=recovery_attempts+$6, ownership_token=NULL, lease_expires_at=NULL, updated_at=clock_timestamp()
           WHERE id=$1 AND tenant_id=$2 RETURNING *`,
        [task.id, tenantId, escalate ? STATES.ESCALATED : STATES.RECOVERY_REQUIRED, reason, escalate ? reason : null, escalate ? 0 : 1]);
        await event(db, r.rows[0], wasActive ? 'lease_expired' : 'deadline_expired', task.state, r.rows[0].state,
          { reason, actionStates: actions, ...(input.actor ? { actor: String(input.actor).slice(0, 120) } : {}) });
        out.push(row(r.rows[0]));
      }
      return out;
    });
  }

  async function revalidateDependencies(input = {}) {
    const tenantId = text(input.tenantId, 'tenantId'); const taskId = text(input.taskId, 'taskId');
    const current = json(input.dependencyRefs, []);
    if (!Array.isArray(current)) throw error('durable_task_invalid', 'dependencyRefs must be an array.');
    return tx(async (db) => {
      const task = await lockTask(db, tenantId, taskId);
      if (!task || ![STATES.RECOVERY_REQUIRED, STATES.BLOCKED_DEPENDENCY].includes(task.state)) throw error('durable_task_state_conflict', 'Task cannot be revalidated in its current state.');
      const r = await db.query(`UPDATE max_durable_execution_tasks SET state=CASE WHEN dependency_refs=$3::jsonb THEN 'queued' ELSE 'blocked_dependency' END,
          recovery_reason=CASE WHEN dependency_refs=$3::jsonb THEN NULL ELSE 'dependency_drift' END, updated_at=clock_timestamp()
        WHERE id=$1 AND tenant_id=$2 RETURNING *`, [taskId, tenantId, JSON.stringify(current)]);
      await event(db, r.rows[0], 'dependencies_revalidated', task.state, r.rows[0].state,
        { drift: r.rows[0].state === STATES.BLOCKED_DEPENDENCY, ...(input.actor ? { actor: String(input.actor).slice(0, 120) } : {}) });
      return row(r.rows[0]);
    });
  }

  /**
   * Cancellation clears ownership and the lease. If an external action of
   * this task may already have reached a provider, the effect cannot be
   * recalled: the task ends `escalated` (cancelled_after_dispatch) with the
   * claim states attached, never `cancelled`.
   */
  async function cancel(input = {}) {
    const tenantId = text(input.tenantId, 'tenantId'); const taskId = text(input.taskId, 'taskId');
    const reason = String(input.reason || 'operator_cancelled').slice(0, 200);
    return tx(async (db) => {
      const task = await lockTask(db, tenantId, taskId);
      if (!task || TERMINAL.includes(task.state)) throw error('durable_task_state_conflict', 'Task is terminal or unavailable.');
      const actions = await actionStates(db, tenantId, taskId);
      const afterDispatch = anyIn(actions, DISPATCHED_ACTION_STATES);
      const r = await db.query(`UPDATE max_durable_execution_tasks SET state=$3, cancellation_reason=$4, escalation_reason=$5,
           ownership_token=NULL, lease_expires_at=NULL, updated_at=clock_timestamp() WHERE id=$1 AND tenant_id=$2 RETURNING *`,
      [taskId, tenantId, afterDispatch ? STATES.ESCALATED : STATES.CANCELLED, reason, afterDispatch ? 'cancelled_after_dispatch' : null]);
      await event(db, r.rows[0], afterDispatch ? 'cancel_escalated' : 'cancelled', task.state, r.rows[0].state,
        { reason, actionStates: actions, ...(input.actor ? { actor: String(input.actor).slice(0, 120) } : {}) });
      return row(r.rows[0]);
    });
  }

  async function get(input = {}) { await assertSchema(pool); const r = await pool.query('SELECT * FROM max_durable_execution_tasks WHERE id=$1 AND tenant_id=$2', [text(input.taskId, 'taskId'), text(input.tenantId, 'tenantId')]); return row(r.rows[0]); }
  async function getByLogicalKey(input = {}) { await assertSchema(pool); const r = await pool.query('SELECT * FROM max_durable_execution_tasks WHERE tenant_id=$1 AND logical_key=$2', [text(input.tenantId, 'tenantId'), text(input.logicalKey, 'logicalKey')]); return row(r.rows[0]); }
  async function list(input = {}) {
    await assertSchema(pool); const tenantId = text(input.tenantId, 'tenantId'); const limit = Math.min(100, Math.max(1, Number(input.limit) || 50));
    const missionId = input.missionId == null ? null : String(input.missionId);
    const r = missionId
      ? await pool.query('SELECT * FROM max_durable_execution_tasks WHERE tenant_id=$1 AND mission_id=$2 ORDER BY updated_at DESC, id LIMIT $3', [tenantId, missionId, limit])
      : await pool.query('SELECT * FROM max_durable_execution_tasks WHERE tenant_id=$1 ORDER BY updated_at DESC, id LIMIT $2', [tenantId, limit]);
    return r.rows.map(row);
  }
  async function listEvents(input = {}) { await assertSchema(pool); const tenantId = text(input.tenantId, 'tenantId'); const taskId = text(input.taskId, 'taskId'); const r = await pool.query('SELECT id,fence_epoch,event,from_state,to_state,detail,at FROM max_durable_execution_task_events WHERE tenant_id=$1 AND task_id=$2 ORDER BY id', [tenantId, taskId]); return r.rows; }
  return { accept, claim, claimTask, fail, planRecovery, requireOwner, start, heartbeat, checkpoint, reportCompletion, verify, recoverExpired, revalidateDependencies, cancel, get, getByLogicalKey, list, listEvents };
}
module.exports = {
  STATES, REQUIRED_TABLES, UNRESOLVED_ACTION_STATES, DISPATCHED_ACTION_STATES,
  assertDurableExecutionSchema: assertSchema, createDurableExecutionOwnership,
};
