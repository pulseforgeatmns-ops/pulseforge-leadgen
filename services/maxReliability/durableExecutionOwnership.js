'use strict';

/**
 * MAX-P0-004 durable work acceptance and fenced ownership. This is a storage
 * boundary, not a worker: callers persist acceptance before acknowledging a
 * handoff; separately authorised workers may claim it later.
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
const CLAIMABLE = [STATES.QUEUED, STATES.RECOVERY_REQUIRED];
const TERMINAL = [STATES.COMPLETED, STATES.FAILED, STATES.CANCELLED, STATES.ESCALATED];
const REQUIRED_TABLES = Object.freeze(['max_durable_execution_tasks', 'max_durable_execution_task_events']);
const verifiedPools = new WeakSet();

function error(code, message, extra = {}) { return Object.assign(new Error(message), { code, spec: 'MAX-P0-004', ...extra }); }
function text(value, name) { const out = value == null ? '' : String(value).trim(); if (!out) throw error('durable_task_invalid', `${name} is required.`); return out; }
function json(value, fallback) { return value == null ? fallback : value; }
function row(task) {
  if (!task) return null;
  return { ...task, id: String(task.id), tenantId: String(task.tenant_id), objectiveId: task.objective_id,
    missionId: task.mission_id, parentTaskId: task.parent_task_id && String(task.parent_task_id), logicalKey: task.logical_key,
    specialist: task.specialist, ownershipToken: task.ownership_token, fenceEpoch: Number(task.fence_epoch),
    leaseExpiresAt: task.lease_expires_at && new Date(task.lease_expires_at).toISOString(), heartbeatAt: task.heartbeat_at && new Date(task.heartbeat_at).toISOString(),
    deadlineAt: task.deadline_at && new Date(task.deadline_at).toISOString(), retryBudget: Number(task.retry_budget), recoveryAttempts: Number(task.recovery_attempts) };
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
function ownership(input) { return { tenantId: text(input.tenantId, 'tenantId'), taskId: text(input.taskId, 'taskId'), ownershipToken: text(input.ownershipToken, 'ownershipToken'), fenceEpoch: Number(input.fenceEpoch) }; }
function validOwner(input) { const o = ownership(input); if (!Number.isSafeInteger(o.fenceEpoch) || o.fenceEpoch < 1) throw error('durable_task_invalid', 'fenceEpoch must be a positive integer.'); return o; }

function createDurableExecutionOwnership(pool, options = {}) {
  if (!pool || typeof pool.query !== 'function') throw error('durable_task_unavailable', 'A PostgreSQL pool is required.');
  const uuid = options.uuid || randomUUID;
  async function event(db, task, eventName, from, to, detail = {}) {
    await db.query('INSERT INTO max_durable_execution_task_events(task_id,tenant_id,fence_epoch,event,from_state,to_state,detail) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb)',
      [task.id, task.tenant_id, task.fence_epoch, eventName, from, to, JSON.stringify(detail)]);
  }
  async function accept(input = {}) {
    await assertSchema(pool);
    const tenantId = text(input.tenantId, 'tenantId'); const logicalKey = text(input.logicalKey, 'logicalKey'); const specialist = text(input.specialist, 'specialist');
    const immutableInput = json(input.immutableInput, {}); const fingerprint = requestFingerprint({ tenantId, objectiveId: input.objectiveId || null, missionId: input.missionId || null, parentTaskId: input.parentTaskId || null, specialist, executionScope: json(input.executionScope, {}), immutableInput, dependencyRefs: json(input.dependencyRefs, []) });
    const taskId = input.taskId || uuid();
    const inserted = await pool.query(`INSERT INTO max_durable_execution_tasks(id,tenant_id,objective_id,mission_id,parent_task_id,logical_key,input_fingerprint,specialist,execution_scope,immutable_input,dependency_refs,state,deadline_at,retry_budget)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10::jsonb,$11::jsonb,'queued',$12,$13)
      ON CONFLICT(tenant_id,logical_key) DO NOTHING RETURNING *`, [taskId, tenantId, input.objectiveId || null, input.missionId || null, input.parentTaskId || null, logicalKey, fingerprint, specialist, JSON.stringify(json(input.executionScope, {})), JSON.stringify(immutableInput), JSON.stringify(json(input.dependencyRefs, [])), input.deadlineAt || null, Number.isInteger(input.retryBudget) ? input.retryBudget : 0]);
    if (inserted.rows[0]) { await event(pool, inserted.rows[0], 'accepted', null, STATES.QUEUED); return { accepted: true, replay: false, task: row(inserted.rows[0]) }; }
    const existing = (await pool.query('SELECT * FROM max_durable_execution_tasks WHERE tenant_id=$1 AND logical_key=$2', [tenantId, logicalKey])).rows[0];
    if (!existing) throw error('durable_task_conflict', 'Task acceptance raced; retry with the same key.');
    if (existing.input_fingerprint !== fingerprint) throw error('durable_task_key_reused', 'logicalKey was already accepted with different immutable input.');
    return { accepted: true, replay: true, task: row(existing) };
  }
  async function claim(input = {}) {
    await assertSchema(pool); const tenantId = text(input.tenantId, 'tenantId'); const ownerToken = text(input.ownerToken, 'ownerToken'); const leaseMs = Number(input.leaseMs || 30000);
    if (!Number.isFinite(leaseMs) || leaseMs < 1000 || leaseMs > 300000) throw error('durable_task_invalid', 'leaseMs must be 1,000–300,000.');
    const claimed = await pool.query(`WITH candidate AS (SELECT id FROM max_durable_execution_tasks WHERE tenant_id=$1 AND state=ANY($2::text[]) AND (deadline_at IS NULL OR deadline_at>clock_timestamp()) ORDER BY accepted_at FOR UPDATE SKIP LOCKED LIMIT 1)
      UPDATE max_durable_execution_tasks t SET state='claimed', ownership_token=$3, fence_epoch=t.fence_epoch+1, lease_expires_at=clock_timestamp()+($4 * interval '1 millisecond'), heartbeat_at=clock_timestamp(), recovery_attempts=CASE WHEN t.state='recovery_required' THEN t.recovery_attempts+1 ELSE t.recovery_attempts END, updated_at=clock_timestamp() FROM candidate WHERE t.id=candidate.id RETURNING t.*`, [tenantId, CLAIMABLE, ownerToken, leaseMs]);
    const task = claimed.rows[0]; if (!task) return null;
    await event(pool, task, 'claimed', task.recovery_attempts ? STATES.RECOVERY_REQUIRED : STATES.QUEUED, STATES.CLAIMED, { ownerToken });
    return row(task);
  }
  async function requireOwner(input, { requireLease = true } = {}) {
    await assertSchema(pool); const o = validOwner(input);
    const r = await pool.query(`SELECT * FROM max_durable_execution_tasks WHERE id=$1 AND tenant_id=$2 AND ownership_token=$3 AND fence_epoch=$4 AND state=ANY($5::text[]) ${requireLease ? 'AND lease_expires_at > clock_timestamp()' : ''}`, [o.taskId, o.tenantId, o.ownershipToken, o.fenceEpoch, ACTIVE]);
    if (!r.rows[0]) throw error('durable_task_fenced', 'Task ownership is stale, expired, cancelled, or unavailable.');
    return row(r.rows[0]);
  }
  async function start(input) { const o = validOwner(input); await requireOwner(o); const r = await pool.query(`UPDATE max_durable_execution_tasks SET state='running',updated_at=clock_timestamp() WHERE id=$1 AND tenant_id=$2 AND ownership_token=$3 AND fence_epoch=$4 AND state='claimed' AND lease_expires_at>clock_timestamp() RETURNING *`, [o.taskId,o.tenantId,o.ownershipToken,o.fenceEpoch]); if (!r.rows[0]) throw error('durable_task_fenced', 'Task cannot be started by this owner.'); await event(pool,r.rows[0],'started',STATES.CLAIMED,STATES.RUNNING); return row(r.rows[0]); }
  async function heartbeat(input) { const o=validOwner(input); const leaseMs=Number(input.leaseMs||30000); const r=await pool.query(`UPDATE max_durable_execution_tasks SET heartbeat_at=clock_timestamp(),lease_expires_at=clock_timestamp()+($5 * interval '1 millisecond'),updated_at=clock_timestamp() WHERE id=$1 AND tenant_id=$2 AND ownership_token=$3 AND fence_epoch=$4 AND state=ANY($6::text[]) AND lease_expires_at>clock_timestamp() RETURNING *`,[o.taskId,o.tenantId,o.ownershipToken,o.fenceEpoch,leaseMs,ACTIVE]); if(!r.rows[0]) throw error('durable_task_fenced','Task lease cannot be renewed by this owner.'); return row(r.rows[0]); }
  async function checkpoint(input) { const o=validOwner(input); await requireOwner(o); const r=await pool.query(`UPDATE max_durable_execution_tasks SET progress_checkpoint=$5::jsonb,updated_at=clock_timestamp() WHERE id=$1 AND tenant_id=$2 AND ownership_token=$3 AND fence_epoch=$4 AND state=ANY($6::text[]) AND lease_expires_at>clock_timestamp() RETURNING *`,[o.taskId,o.tenantId,o.ownershipToken,o.fenceEpoch,JSON.stringify(json(input.checkpoint,{})),ACTIVE]); if(!r.rows[0]) throw error('durable_task_fenced','Checkpoint rejected for stale owner.'); await event(pool,r.rows[0],'checkpointed',r.rows[0].state,r.rows[0].state); return row(r.rows[0]); }
  async function reportCompletion(input) { const o=validOwner(input); await requireOwner(o); const r=await pool.query(`UPDATE max_durable_execution_tasks SET state='awaiting_verification',result_ref=$5::jsonb,evidence_refs=$6::jsonb,ownership_token=NULL,lease_expires_at=NULL,updated_at=clock_timestamp() WHERE id=$1 AND tenant_id=$2 AND ownership_token=$3 AND fence_epoch=$4 AND state=ANY($7::text[]) AND lease_expires_at>clock_timestamp() RETURNING *`,[o.taskId,o.tenantId,o.ownershipToken,o.fenceEpoch,JSON.stringify(json(input.resultRef,{})),JSON.stringify(json(input.evidenceRefs,[])),ACTIVE]); if(!r.rows[0]) throw error('durable_task_fenced','Completion rejected for stale owner.'); await event(pool,r.rows[0],'specialist_completed',STATES.RUNNING,STATES.AWAITING_VERIFICATION); return row(r.rows[0]); }
  async function verify(input={}) { await assertSchema(pool); const tenantId=text(input.tenantId,'tenantId'); const taskId=text(input.taskId,'taskId'); if (!Array.isArray(input.evidenceRefs) || !input.evidenceRefs.length) throw error('durable_task_invalid','Verification requires evidenceRefs.'); const unresolved=await pool.query("SELECT 1 FROM max_external_action_claims WHERE tenant_id=$1 AND task_id=$2 AND state IN ('claimed','dispatch_attempted','outcome_unknown') LIMIT 1",[tenantId,taskId]); if(unresolved.rows[0]) throw error('durable_task_external_action_unresolved','Task has an external action requiring reconciliation.'); const r=await pool.query(`UPDATE max_durable_execution_tasks SET state='completed',evidence_refs=$3::jsonb,completed_at=clock_timestamp(),updated_at=clock_timestamp() WHERE id=$1 AND tenant_id=$2 AND state='awaiting_verification' RETURNING *`,[taskId,tenantId,JSON.stringify(input.evidenceRefs)]); if(!r.rows[0]) throw error('durable_task_state_conflict','Task is not awaiting verification.'); await event(pool,r.rows[0],'verified',STATES.AWAITING_VERIFICATION,STATES.COMPLETED); return row(r.rows[0]); }
  async function recoverExpired(input={}) { await assertSchema(pool); const tenantId=text(input.tenantId,'tenantId'); const r=await pool.query(`UPDATE max_durable_execution_tasks SET state=CASE WHEN deadline_at IS NOT NULL AND deadline_at<=clock_timestamp() OR recovery_attempts>=retry_budget THEN 'escalated' ELSE 'recovery_required' END, recovery_reason=CASE WHEN deadline_at IS NOT NULL AND deadline_at<=clock_timestamp() THEN 'deadline_exceeded' WHEN recovery_attempts>=retry_budget THEN 'retry_budget_exhausted' ELSE 'lease_expired' END, escalation_reason=CASE WHEN deadline_at IS NOT NULL AND deadline_at<=clock_timestamp() THEN 'deadline_exceeded' WHEN recovery_attempts>=retry_budget THEN 'retry_budget_exhausted' ELSE NULL END, ownership_token=NULL,lease_expires_at=NULL,updated_at=clock_timestamp() WHERE tenant_id=$1 AND state=ANY($2::text[]) AND lease_expires_at<=clock_timestamp() RETURNING *`,[tenantId,ACTIVE]); for(const task of r.rows) await event(pool,task,'lease_expired',task.state,task.state,{reason:task.recovery_reason}); return r.rows.map(row); }
  async function revalidateDependencies(input={}) { await assertSchema(pool); const tenantId=text(input.tenantId,'tenantId'); const taskId=text(input.taskId,'taskId'); const r=await pool.query(`UPDATE max_durable_execution_tasks SET state=CASE WHEN dependency_refs=$3::jsonb THEN 'queued' ELSE 'blocked_dependency' END, recovery_reason=CASE WHEN dependency_refs=$3::jsonb THEN NULL ELSE 'dependency_drift' END,updated_at=clock_timestamp() WHERE id=$1 AND tenant_id=$2 AND state IN ('recovery_required','blocked_dependency') RETURNING *`,[taskId,tenantId,JSON.stringify(json(input.dependencyRefs,[]))]); if(!r.rows[0]) throw error('durable_task_state_conflict','Task cannot be revalidated in its current state.'); await event(pool,r.rows[0],'dependencies_revalidated',STATES.RECOVERY_REQUIRED,r.rows[0].state); return row(r.rows[0]); }
  async function cancel(input={}) { await assertSchema(pool); const tenantId=text(input.tenantId,'tenantId'); const taskId=text(input.taskId,'taskId'); const r=await pool.query(`UPDATE max_durable_execution_tasks SET state='cancelled',cancellation_reason=$3,ownership_token=NULL,lease_expires_at=NULL,updated_at=clock_timestamp() WHERE id=$1 AND tenant_id=$2 AND state<>ALL($4::text[]) RETURNING *`,[taskId,tenantId,String(input.reason||'operator_cancelled'),TERMINAL]); if(!r.rows[0]) throw error('durable_task_state_conflict','Task is terminal or unavailable.'); await event(pool,r.rows[0],'cancelled',null,STATES.CANCELLED,{reason:input.reason||'operator_cancelled'}); return row(r.rows[0]); }
  async function get(input={}) { await assertSchema(pool); const r=await pool.query('SELECT * FROM max_durable_execution_tasks WHERE id=$1 AND tenant_id=$2',[text(input.taskId,'taskId'),text(input.tenantId,'tenantId')]); return row(r.rows[0]); }
  async function list(input={}) { await assertSchema(pool); const tenantId=text(input.tenantId,'tenantId'); const limit=Math.min(100,Math.max(1,Number(input.limit)||50)); const r=await pool.query('SELECT * FROM max_durable_execution_tasks WHERE tenant_id=$1 ORDER BY updated_at DESC LIMIT $2',[tenantId,limit]); return r.rows.map(row); }
  async function listEvents(input={}) { await assertSchema(pool); const tenantId=text(input.tenantId,'tenantId'); const taskId=text(input.taskId,'taskId'); const r=await pool.query('SELECT id,fence_epoch,event,from_state,to_state,detail,at FROM max_durable_execution_task_events WHERE tenant_id=$1 AND task_id=$2 ORDER BY id',[tenantId,taskId]); return r.rows; }
  return { accept, claim, requireOwner, start, heartbeat, checkpoint, reportCompletion, verify, recoverExpired, revalidateDependencies, cancel, get, list, listEvents };
}
module.exports = { STATES, REQUIRED_TABLES, assertDurableExecutionSchema: assertSchema, createDurableExecutionOwnership };
