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
  const out = [...new Set(value.map(v => String(v || '').trim()).filter(Boolean))];
  if (out.length < min) throw new ObjectiveContractError('objective_contract_invalid', `${name} requires at least ${min} value(s)`);
  return out;
}
function hasAnthropic(value) { return /anthropic|claude/i.test(JSON.stringify(value)); }
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
  if (Number.isNaN(objectiveDeadline.getTime()) || deadlineAt.getTime() > objectiveDeadline.getTime()) {
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
    verification: Object.freeze({ initialPollMs, maxPollMs, maxAttempts, backoffMultiplier, deadlineAt: deadlineAt.toISOString() }),
    escalationPolicy: text(input.escalationPolicy, 'escalationPolicy'),
  });
}

/** Returns a bounded next verification delay; no timer is created. */
function nextVerificationWake(policy, attempts, outcome = 'pending') {
  if (TERMINAL_VERIFICATION.has(outcome)) return null;
  const verification = validateObjectiveContract(policy).verification;
  const count = int(attempts, 'attempts', 0, MAX_POLL_ATTEMPTS);
  if (count >= verification.maxAttempts) return null;
  return Math.min(verification.maxPollMs, Math.round(verification.initialPollMs * (verification.backoffMultiplier ** count)));
}

function rootLogicalKey(objectiveId, version) {
  return `objective:${text(objectiveId, 'objectiveId')}:v:${int(version, 'version', 1, Number.MAX_SAFE_INTEGER)}`;
}

/** Root tasks have no mission ID by design; children own mission slots in Package C. */
function rootTaskInput({ tenantId, objective, contract }) {
  return {
    tenantId: text(tenantId, 'tenantId'),
    objectiveId: text(objective.id, 'objective.id'),
    missionId: null,
    logicalKey: rootLogicalKey(objective.id, contract.version),
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

/**
 * Explicit opt-in admission. It does not claim, start, poll, or execute a task.
 * A caller must inject the existing objective service and P0-004 task store.
 */
async function admitObjectiveRoot(input = {}, opts = {}) {
  const env = opts.env || process.env;
  if (!enabled(env)) return { enabled: false, admitted: false, reason: 'max_p0_005_disabled' };
  const tenantId = text(input.tenantId, 'tenantId');
  const objectiveService = opts.objectiveService || require('../../../services/operatorObjectives');
  const taskStore = opts.taskStore;
  if (!taskStore || typeof taskStore.accept !== 'function') {
    throw new ObjectiveContractError('objective_contract_task_store_required', 'A P0-004 durable task store is required', 503);
  }
  let objective = await objectiveService.getObjectiveById(input.objectiveId, { tenantId, ...(opts.objectiveOpts || {}) });
  if (!objective) throw new ObjectiveContractError('objective_contract_not_found', 'objective not found', 404);
  if (objective.status !== 'active') throw new ObjectiveContractError('objective_contract_inactive', 'only active objectives may be admitted', 409);
  const contract = validateObjectiveContract(input.contract || objective.context?.maxP005Contract);
  const existingContract = objective.context?.maxP005Contract || null;
  if (existingContract && JSON.stringify(existingContract) !== JSON.stringify(contract)) {
    throw new ObjectiveContractError('objective_contract_revision_conflict', 'the admitted objective contract is immutable; create a reviewed revision before admission', 409);
  }
  // Persist the canonical contract before accepting the root. A crash in this
  // harmless gap leaves no execution or external effect; a later admission
  // resumes by the same stable root key. The root's immutable input remains
  // the authority used by later packages.
  if (!existingContract) {
    objective = await objectiveService.updateOperatorObjective(objective.id, {
      tenantId, context: { maxP005Contract: clone(contract) },
    }, opts.objectiveOpts || {});
  }
  const accepted = await taskStore.accept(rootTaskInput({ tenantId, objective, contract }));
  // This is a continuity projection only; P0-004's immutable logical key and
  // root task remain authoritative if a process dies before this update.
  await objectiveService.updateOperatorObjective(objective.id, {
    tenantId, context: { maxP005RootTaskId: accepted.task.id },
  }, opts.objectiveOpts || {});
  return { enabled: true, admitted: accepted.accepted === true, replay: accepted.replay === true, task: accepted.task, contract };
}

module.exports = {
  MAX_STEPS, MAX_CHILDREN, MAX_REPLANS, MAX_RETRIES, MAX_POLL_ATTEMPTS,
  MIN_POLL_MS, MAX_POLL_MS, WAKE_KINDS, ObjectiveContractError,
  enabled, validateObjectiveContract, nextVerificationWake, rootLogicalKey,
  rootTaskInput, admitObjectiveRoot,
};
