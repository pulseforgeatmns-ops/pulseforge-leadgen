'use strict';

/**
 * MAX-P0-004 P4 — operator-run durable task recovery for ONE tenant.
 *
 * Same rules as P0-003 recovery (scripts/max-reliability/recover.cjs): never
 * scheduled, plan by default (reads only), applying needs the tenant confirmed
 * twice and a named operator, and every change is an evented, attributed
 * transition in the task store. Nothing is dispatched, retried or replayed:
 *
 *   plan      what recoverExpired would decide now (no writes)
 *   apply     expired leases / passed deadlines → recovery_required or escalated
 *             (unresolved external action, deadline, exhausted budget → escalated)
 *   revalidate re-queue ONE recovery_required/blocked task only if its persisted
 *             dependency references equal the supplied current ones; otherwise
 *             blocked_dependency (explicit, bounded re-admission)
 *   cancel    cancel ONE task (escalated instead if an effect may have happened)
 *
 * Output is redacted: ids, states, reasons and counts only, never task input.
 */

const { createDurableExecutionOwnership, assertDurableExecutionSchema } = require('./durableExecutionOwnership');

const OPERATOR_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:@-]{0,79}$/;

function recoveryError(code, message) {
  return Object.assign(new Error(message), { code, spec: 'MAX-P0-004' });
}

function redactTask(task) {
  if (!task) return null;
  return {
    id: task.id,
    state: task.state,
    missionId: task.missionId || null,
    specialist: task.specialist || null,
    fenceEpoch: task.fenceEpoch,
    recoveryAttempts: task.recoveryAttempts,
    retryBudget: task.retryBudget,
    deadlineAt: task.deadlineAt || null,
    recoveryReason: task.recoveryReason || null,
    escalationReason: task.escalationReason || null,
    cancellationReason: task.cancellationReason || null,
  };
}

/**
 * @param {object} pool
 * @param {object} opts { tenantId, mode: 'plan'|'apply'|'revalidate'|'cancel',
 *   confirmTenant, operator, taskId, dependencyRefs, reason, limit }
 */
async function runTaskRecovery(pool, opts = {}) {
  const tenantId = String(opts.tenantId == null ? '' : opts.tenantId).trim();
  if (!tenantId) throw recoveryError('recovery_tenant_required', 'tenantId is required.');
  const mode = opts.mode || 'plan';
  if (!['plan', 'apply', 'revalidate', 'cancel'].includes(mode)) {
    throw recoveryError('recovery_mode_invalid', 'mode must be plan, apply, revalidate or cancel.');
  }
  await assertDurableExecutionSchema(pool);
  const store = createDurableExecutionOwnership(pool);

  if (mode === 'plan') {
    const plan = await store.planRecovery({ tenantId, limit: opts.limit });
    return {
      kind: 'max_durable_task_recovery', mode, tenantId, applied: false,
      plan: plan.map((p) => ({ task: redactTask(p.task), decision: p.decision, reason: p.reason })),
      escalationRequired: plan.some((p) => p.decision === 'escalated'),
    };
  }

  // Every mutating mode: tenant confirmed twice and a named operator.
  if (String(opts.confirmTenant == null ? '' : opts.confirmTenant).trim() !== tenantId) {
    throw recoveryError('recovery_tenant_unconfirmed', 'Applying requires --confirm-tenant equal to --tenant.');
  }
  const operator = opts.operator == null ? '' : String(opts.operator).trim();
  if (!OPERATOR_PATTERN.test(operator)) {
    throw recoveryError('recovery_actor_required', 'Applying requires an operator identity (--operator).');
  }
  const actor = `operator:${operator}`;

  if (mode === 'apply') {
    const changed = await store.recoverExpired({ tenantId, limit: opts.limit, actor });
    return {
      kind: 'max_durable_task_recovery', mode, tenantId, applied: true, operator,
      changed: changed.map(redactTask),
      escalationRequired: changed.some((t) => t.state === 'escalated'),
    };
  }

  const taskId = String(opts.taskId == null ? '' : opts.taskId).trim();
  if (!taskId) throw recoveryError('recovery_task_required', 'A task id is required.');

  if (mode === 'revalidate') {
    if (!Array.isArray(opts.dependencyRefs)) {
      throw recoveryError('recovery_dependencies_required', 'Revalidation requires the current dependency references (a JSON array).');
    }
    const task = await store.revalidateDependencies({ tenantId, taskId, dependencyRefs: opts.dependencyRefs, actor });
    return { kind: 'max_durable_task_recovery', mode, tenantId, applied: true, operator, task: redactTask(task) };
  }

  const task = await store.cancel({ tenantId, taskId, reason: String(opts.reason || 'operator_cancelled').slice(0, 200), actor });
  return {
    kind: 'max_durable_task_recovery', mode, tenantId, applied: true, operator, task: redactTask(task),
    escalationRequired: task.state === 'escalated',
  };
}

module.exports = { runTaskRecovery, redactTask };
