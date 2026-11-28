'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const objectives = require('../../../services/operatorObjectives');
const {
  ObjectiveContractError, validateObjectiveContract, nextVerificationWake,
  rootTaskInput, admitObjectiveRoot, wakeIdentity,
} = require('../objectiveOrchestration/ObjectiveContract');

function contract(overrides = {}) {
  return {
    version: 1,
    successPredicates: ['persisted_non_synthetic_research'],
    allowedActionClasses: [], allowedSpecialists: ['scout'], allowedProviders: ['deterministic_fixture'],
    evidenceFreshnessMs: 60000, deadlineAt: '2030-01-01T00:00:00.000Z',
    plan: { maxSteps: 2, maxChildren: 1, maxReplans: 0, maxRetries: 0 },
    budget: { caps: { model_usd: 0, provider_usd: 0 } },
    wakeConditions: ['objective_accepted', 'dependency_terminal', 'due_verification'],
    verification: { initialPollMs: 5000, maxPollMs: 20000, maxAttempts: 3, backoffMultiplier: 2, deadlineAt: '2030-01-01T00:00:00.000Z' },
    escalationPolicy: 'operator_review', ...overrides,
  };
}

describe('MAX-P0-005 Package A objective contract', () => {
  it('validates bounded governance, wakes, and polling without creating a timer', () => {
    const validated = validateObjectiveContract(contract());
    assert.equal(validated.idlePolicy.noModelCalls, true);
    assert.equal(nextVerificationWake(validated, 0, 'pending', () => 0.5), 5000);
    assert.equal(nextVerificationWake(validated, 1, 'pending', () => 0.5), 10000);
    assert.equal(nextVerificationWake(validated, 2, 'pending', () => 0.5), 20000);
    assert.equal(nextVerificationWake(validated, 3), null);
    assert.equal(nextVerificationWake(validated, 0, 'verified'), null);
  });

  it('fails closed for retired providers and unbounded polling/budget contracts', () => {
    assert.throws(() => validateObjectiveContract(contract({ allowedProviders: ['anthropic'] })), (err) => err instanceof ObjectiveContractError && err.code === 'objective_contract_provider_retired');
    assert.throws(() => validateObjectiveContract(contract({ verification: { ...contract().verification, initialPollMs: 1 } })), { code: 'objective_contract_invalid' });
    assert.throws(() => validateObjectiveContract(contract({ budget: { caps: {} } })), { code: 'objective_contract_invalid' });
    assert.throws(() => validateObjectiveContract(contract({ allowedProviders: ['openai'] })), { code: 'objective_contract_provider_unapproved' });
  });

  it('contains no Anthropic credential, client, fallback, or spend path', () => {
    const source = fs.readFileSync(path.join(__dirname, '../objectiveOrchestration/ObjectiveContract.js'), 'utf8');
    assert.doesNotMatch(source, /ANTHROPIC_API_KEY|meteredAnthropic|@anthropic-ai|createAnthropic|anthropic.*fallback/i);
  });

  it('keeps root ownership outside the P0-004 mission slot', () => {
    const task = rootTaskInput({ tenantId: 't1', objective: { id: 'o1', objectiveText: 'Research safely.' }, contract: validateObjectiveContract(contract()) });
    assert.equal(task.missionId, null);
    assert.equal(task.executionScope.missionOwnership, 'none');
    assert.equal(task.retryBudget, 0);
    assert.match(task.logicalKey, /^objective:o1:root$/);
  });

  it('defines a stable durable wake identity without scheduling work', () => {
    const a = wakeIdentity({ objectiveId: 'o1', rootTaskId: 'r1', reason: 'due_verification', version: 1 });
    assert.equal(a, wakeIdentity({ reason: 'due_verification', version: 1, rootTaskId: 'r1', objectiveId: 'o1' }));
    assert.match(a, /^objective-wake:o1:r1:due_verification:v1$/);
    assert.equal(validateObjectiveContract(contract()).wakeDeduplication.persistence, 'max_durable_execution_task_events');
  });

  it('is default-off and cannot accept work or call a task store', async () => {
    let calls = 0;
    const result = await admitObjectiveRoot({ tenantId: 't1', objectiveId: 'missing', contract: contract() }, {
      env: {}, taskStore: { accept: async () => { calls += 1; } },
    });
    assert.deepEqual(result, { enabled: false, admitted: false, reason: 'max_p0_005_disabled' });
    assert.equal(calls, 0);
  });

  it('cannot use a caller-supplied authorizer to bypass the trusted default-deny policy', async () => {
    const store = objectives.createMemoryStore();
    const objective = await objectives.createOperatorObjective({ tenantId: 't1', scope: 'operator', title: 'Research', objectiveText: 'Find evidence.' }, { store });
    const calls = [];
    const taskStore = { accept: async input => { calls.push(input); return { accepted: true, replay: false, task: { id: 'root-1', ...input, state: 'queued' } }; } };
    await assert.rejects(() => admitObjectiveRoot({ tenantId: 't1', objectiveId: objective.id, contract: contract(), actor: { id: 'operator-1', authorizedTenantId: 't1' } }, {
      env: { MAX_AUTONOMOUS_OBJECTIVES: 'true', MAX_DURABLE_TASKS: 'true' }, taskStore,
      objectiveService: objectives, objectiveOpts: { store }, authorizeActor: async () => ({ authorized: true, policy: 'caller_controlled' }),
    }), { code: 'objective_contract_actor_unauthorized' });
    assert.equal(calls.length, 0);
    const persisted = await objectives.getObjectiveById(objective.id, { tenantId: 't1', store });
    assert.equal(persisted.context.maxP005Contract, undefined);
  });

  it('requires an authenticated tenant-bound actor before enabled admission', async () => {
    await assert.rejects(() => admitObjectiveRoot({ tenantId: 't1', objectiveId: 'missing', contract: contract() }, {
      env: { MAX_AUTONOMOUS_OBJECTIVES: 'true', MAX_DURABLE_TASKS: 'true' }, taskStore: { accept: async () => null },
    }), { code: 'objective_contract_actor_unauthorized' });
  });
});
