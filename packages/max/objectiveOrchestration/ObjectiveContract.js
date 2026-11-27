'use strict';

/**
 * MAX-P0-005 Package A -- durable objective contract and root-task admission.
 *
 * This is deliberately not an execution driver. It validates and persists the
 * bounded governance/wake contract that a later, separately enabled driver
 * may consume. No timer, provider, model, specialist, or outbound path lives
 * here. The only durable work record it creates is a P0-004 root task.
 */

const MAX_STEPS = 64;
const MAX_CHILDREN = 32;
const MAX_REPLANS = 8;
const MAX_RETRIES = 20;
const MAX_POLL_ATTEMPTS = 32;
const MIN_POLL_MS = 5_000;
const MAX_POLL_MS = 24 * 60 * 60 * 1000;
const WAKE_KINDS = Object.freeze([
  'objective_accepted', 'dependency_terminal', 'lease_or_deadline_due',
  'correlated_inbound', 'operator_reverify', 'due_verification',
]);
// Package A has no provider integration.  This closed registry deliberately
// admits only the hermetic fixture used by the contract tests.  Later packages
// must make a reviewed additive registry change; unknown strings are never a
// provider policy.
const APPROVED_PROVIDERS = Object.freeze(['deterministic_fixture']);
const APPROVED_SPECIALISTS = Object.freeze(['scout']);
const APPROVED_ACTION_CLASSES = Object.freeze([]);
const TERMINAL_VERIFICATION = new Set(['verified', 'contradicted', 'unverifiable', 'error']);

class ObjectiveContractError extends Error {
  constructor(code, message, status = 400) {
    super(message);
    this.name = 'ObjectiveContractError';
    this.code = code;
    this.status = status;
  }
}

function clone(value) { return JSON.parse(JSON.stringify(value)); }
function text(value, name) {
  const out = value == null ? '' : String(value).trim();
  if (!out) throw new ObjectiveContractError('objective_contract_invalid', `${name} is required`);
  return out;
}
function int(value, name, min, max) {
  const n = Number(value);
  if (!Number.isSafeInteger(n) || n < min || n > max) {
    throw new ObjectiveContractError('objective_contract_invalid', `${name} must be an integer from ${min} to ${max}`);
  }
  return n;
}
function nonNegative(value, name) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) throw new ObjectiveContractError('objective_contract_invalid', `${name} must be a non-negative number`);
  return n;
}
function asList(value, name, { min = 0 } = {}) {
  if (!Array.isArray(value)) throw new ObjectiveContractError('objective_contract_invalid', `${name} must be an array`);
  const out = [...new Set(value.map(v => String(v || '').trim()).filter(Boolean))].sort();
  if (out.length < min) throw new ObjectiveContractError('objective_contract_invalid', `${name} requires at least ${min} value(s)`);
  return out;
}
function hasAnthropic(value) { return /anthropic|claude/i.test(JSON.stringify(value)); }
function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value && typeof value === 'object') return `{${Object.keys(value).sort().map(k => `${JSON.stringify(k)}:${canonical(value[k])}`).join(',')}}`;
  return JSON.stringify(value);
}
function fingerprint(value) { return require('crypto').createHash('sha256').update(canonical(value)).digest('hex'); }
function enabled(env = process.env) {
  return String(env.MAX_AUTONOMOUS_OBJECTIVES || '').toLowerCase() === 'true'
    && String(env.MAX_DURABLE_TASKS || '').toLowerCase() === 'true';
}

/** Validate an immutable, non-executing objective contract. */
function validateObjectiveContract(input = {}) {
  if (!input || typeof input !== 'object' || Array.isArray(input)) {
    throw new ObjectiveContractError('objective_contract_invalid', 'contract must be an object');
  }
  const successPredicates = asList(input.successPredicates, 'successPredicates', { min: 1 });
  const allowedActionClasses = asList(input.allowedActionClasses || [], 'allowedActionClasses');
  const allowedSpecialists = asList(input.allowedSpecialists || [], 'allowedSpecialists');
  const allowedProviders = asList(input.allowedProviders || [], 'allowedProviders');
  if (hasAnthropic(allowedProviders) || hasAnthropic(input.providerPolicy)) {
    throw new ObjectiveContractError('objective_contract_provider_retired', 'Anthropic is retired and cannot appear in an objective contract');
  }
  for (const provider of allowedProviders) if (!APPROVED_PROVIDERS.includes(provider)) throw new ObjectiveContractError('objective_contract_provider_unapproved', 'provider is not approved');
  for (const specialist of allowedSpecialists) if (!APPROVED_SPECIALISTS.includes(specialist)) throw new ObjectiveContractError('objective_contract_specialist_unapproved', 'specialist is not approved');
  for (const action of allowedActionClasses) if (!APPROVED_ACTION_CLASSES.includes(action)) throw new ObjectiveContractError('objective_contract_action_unapproved', 'action class is not approved');
  const budget = input.budget;
  if (!budget || typeof budget !== 'object' || Array.isArray(budget)) {
    throw new ObjectiveContractError('objective_contract_invalid', 'budget is required');
  }
  const budgetCaps = {};
  for (const [dimension, cap] of Object.entries(budget.caps || {})) {
    const key = text(dimension, 'budget dimension');
    budgetCaps[key] = nonNegative(cap, `budget.caps.${key}`);
  }
  if (!Object.keys(budgetCaps).length) {
    throw new ObjectiveContractError('objective_contract_invalid', 'budget.caps requires at least one dimension');
  }
  const verification = input.verification;
  if (!verification || typeof verification !== 'object' || Array.isArray(verification)) {
    throw new ObjectiveContractError('objective_contract_invalid', 'verification is required');
  }
  const initialPollMs = int(verification.initialPollMs, 'verification.initialPollMs', MIN_POLL_MS, MAX_POLL_MS);
  const maxPollMs = int(verification.maxPollMs, 'verification.maxPollMs', initialPollMs, MAX_POLL_MS);
  const maxAttempts = int(verification.maxAttempts, 'verification.maxAttempts', 1, MAX_POLL_ATTEMPTS);
  const backoffMultiplier = Number(verification.backoffMultiplier);
  if (!Number.isFinite(backoffMultiplier) || backoffMultiplier < 1 || backoffMultiplier > 4) {
    throw new ObjectiveContractError('objective_contract_invalid', 'verification.backoffMultiplier must be from 1 to 4');
  }
  const deadlineAt = new Date(verification.deadlineAt);
  if (Number.isNaN(deadlineAt.getTime())) {
    throw new ObjectiveContractError('objective_contract_invalid', 'verification.deadlineAt must be a timestamp');
  }
  const objectiveDeadline = new Date(input.deadlineAt);
  if (Number.isNaN(objectiveDeadline.getTime()) || objectiveDeadline.getTime() <= Date.now() || deadlineAt.getTime() > objectiveDeadline.getTime()) {
    throw new ObjectiveContractError('objective_contract_invalid', 'verification.deadlineAt must not exceed deadlineAt');
  }
  const wakeConditions = asList(input.wakeConditions, 'wakeConditions', { min: 1 });
  for (const wake of wakeConditions) {
    if (!WAKE_KINDS.includes(wake)) throw new ObjectiveContractError('objective_contract_invalid', `unknown wake condition: ${wake}`);
  }
  const plan = input.plan;
  if (!plan || typeof plan !== 'object' || Array.isArray(plan)) {
    throw new ObjectiveContractError('objective_contract_invalid', 'plan is required');
  }
  return Object.freeze({
    version: int(input.version == null ? 1 : input.version, 'version', 1, Number.MAX_SAFE_INTEGER),
    successPredicates,
    allowedActionClasses,
    allowedSpecialists,
    allowedProviders,
    evidenceFreshnessMs: int(input.evidenceFreshnessMs, 'evidenceFreshnessMs', 0, MAX_POLL_MS),
    deadlineAt: objectiveDeadline.toISOString(),
    plan: Object.freeze({
      maxSteps: int(plan.maxSteps, 'plan.maxSteps', 1, MAX_STEPS),
      maxChildren: int(plan.maxChildren, 'plan.maxChildren', 0, MAX_CHILDREN),
      maxReplans: int(plan.maxReplans == null ? 0 : plan.maxReplans, 'plan.maxReplans', 0, MAX_REPLANS),
      maxRetries: int(plan.maxRetries == null ? 0 : plan.maxRetries, 'plan.maxRetries', 0, MAX_RETRIES),
    }),
    budget: Object.freeze({ caps: Object.freeze(budgetCaps) }),
    wakeConditions,
    idlePolicy: Object.freeze({ noModelCalls: true, noProviderCalls: true, noSpecialistCalls: true }),
    verification: Object.freeze({ initialPollMs, maxPollMs, maxAttempts, backoffMultiplier, jitterRatio: 0.1, deadlineAt: deadlineAt.toISOString() }),
    escalationPolicy: text(input.escalationPolicy, 'escalationPolicy'),
  });
}

/** Returns a bounded next verification delay; no timer is created. */
function nextVerificationWake(policy, attempts, outcome = 'pending', random = Math.random) {
  if (TERMINAL_VERIFICATION.has(outcome)) return null;
  const verification = validateObjectiveContract(policy).verification;
  const count = int(attempts, 'attempts', 0, MAX_POLL_ATTEMPTS);
  if (count >= verification.maxAttempts) return null;
  const base = Math.min(verification.maxPollMs, Math.round(verification.initialPollMs * (verification.backoffMultiplier ** count)));
  return Math.round(base * (1 + ((Number(random()) * 2 - 1) * verification.jitterRatio)));
}

function rootLogicalKey(objectiveId) {
  return `objective:${text(objectiveId, 'objectiveId')}:root`;
}

/** Root tasks have no mission ID by design; children own mission slots in Package C. */
function rootTaskInput({ tenantId, objective, contract }) {
  return {
    tenantId: text(tenantId, 'tenantId'),
    objectiveId: text(objective.id, 'objective.id'),
    missionId: null,
    logicalKey: rootLogicalKey(objective.id),
    specialist: 'max_objective',
    executionScope: { kind: 'objective_orchestration', role: 'root', missionOwnership: 'none' },
    immutableInput: {
      objectiveId: String(objective.id), objectiveRevision: contract.version,
      objectiveText: String(objective.objectiveText), contract: clone(contract),
    },
    dependencyRefs: [],
    dependencyTaskIds: [],
    deadlineAt: contract.deadlineAt,
    retryBudget: 0,
  };
}

/** Read-only projection for operators; it cannot advance, claim, or wake work. */
function projectObjectiveStatus(task) {
  if (!task) return null;
  const contract = task.immutableInput && task.immutableInput.contract;
  return Object.freeze({ objectiveId: task.objectiveId || null, rootTaskId: task.id, state: task.state,
    nextWakeConditions: contract ? contract.wakeConditions.slice() : [],
    verification: contract ? { maxAttempts: contract.verification.maxAttempts, deadlineAt: contract.verification.deadlineAt } : null,
    externalActionsEnabled: false });
}

/**
 * Explicit opt-in admission. It does not claim, start, poll, or execute a task.
 * A caller must inject the existing objective service and P0-004 task store.
 */
async function admitObjectiveRoot(input = {}, opts = {}) {
  const env = opts.env || process.env;
  if (!enabled(env)) return { enabled: false, admitted: false, reason: 'max_p0_005_disabled' };
  const tenantId = text(input.tenantId, 'tenantId');
  if (!input.actor || !input.actor.id || !input.actor.authorizedTenantId || String(input.actor.authorizedTenantId) !== tenantId) {
    throw new ObjectiveContractError('objective_contract_actor_unauthorized', 'authenticated tenant-bound actor is required', 403);
  }
  if (typeof opts.authorizeActor !== 'function' || !(await opts.authorizeActor(input.actor, { tenantId, objectiveId: input.objectiveId }))) {
    throw new ObjectiveContractError('objective_contract_actor_unauthorized', 'actor is not authorized for objective admission', 403);
  }
  const objectiveService = opts.objectiveService || require('../../../services/operatorObjectives');
  const taskStore = opts.taskStore;
  if (!taskStore || typeof taskStore.accept !== 'function') {
    throw new ObjectiveContractError('objective_contract_task_store_required', 'A P0-004 durable task store is required', 503);
  }
  let objective = await objectiveService.getObjectiveById(input.objectiveId, { tenantId, ...(opts.objectiveOpts || {}) });
  if (!objective) throw new ObjectiveContractError('objective_contract_not_found', 'objective not found', 404);
  if (objective.status !== 'active') throw new ObjectiveContractError('objective_contract_inactive', 'only active objectives may be admitted', 409);
  const contract = validateObjectiveContract(input.contract);
  const accepted = await taskStore.accept(rootTaskInput({ tenantId, objective, contract }));
  const acceptedContract = accepted.task && accepted.task.immutableInput && accepted.task.immutableInput.contract;
  if (accepted.replay && (!acceptedContract || fingerprint(acceptedContract) !== fingerprint(contract))) {
    throw new ObjectiveContractError('objective_contract_revision_conflict', 'the admitted root contract is immutable', 409);
  }
  return { enabled: true, admitted: accepted.accepted === true, replay: accepted.replay === true, task: accepted.task, contract: acceptedContract || contract };
}

module.exports = {
  MAX_STEPS, MAX_CHILDREN, MAX_REPLANS, MAX_RETRIES, MAX_POLL_ATTEMPTS,
  MIN_POLL_MS, MAX_POLL_MS, WAKE_KINDS, APPROVED_PROVIDERS, APPROVED_SPECIALISTS, APPROVED_ACTION_CLASSES, ObjectiveContractError,
  enabled, validateObjectiveContract, nextVerificationWake, rootLogicalKey,
  rootTaskInput, projectObjectiveStatus, admitObjectiveRoot, canonical, fingerprint,
};
