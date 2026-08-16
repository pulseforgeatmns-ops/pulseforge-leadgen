'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const {
  createMemoryStore,
  createSpecialistDelegationService,
  createDelegation,
  executeDelegation,
  delegateAndExecute,
  evaluateResult,
  applyEvaluationPriority,
  getDelegation,
  getResult,
  getResultForDelegation,
  listDelegations,
  traceProvenance,
  CONTRACT_OBJECTIVE,
  SpecialistDelegationError,
} = require('../services/specialistDelegation');

const CONTRACT_REASON =
  'The operator asked whether Acquisition currently has meaningful opportunity.';

function contractInput(overrides = {}) {
  return {
    authorizedTenantId: '1',
    tenantId: '1',
    specialist: 'test_intelligence',
    capability: 'acquisition_assessment',
    objective: CONTRACT_OBJECTIVE,
    reason: CONTRACT_REASON,
    authority: 'observe',
    expectedReturn: {
      type: 'acquisition_intelligence',
      requireEvidence: true,
      requireConfidence: true,
      requireRecommendation: true,
    },
    evidenceRefs: [
      {
        id: 'ev-max-direction-1',
        kind: 'operator_correction',
        sourceKind: 'operator_instruction',
        label: 'Operator asked Max to assess Acquisition opportunity.',
      },
    ],
    businessContext: {
      targetMarket: { segments: ['property_manager'], geography: 'Manchester NH' },
    },
    ...overrides,
  };
}

describe('SPEC-098 specialist delegation contract', () => {
  /** @type {ReturnType<typeof createMemoryStore>} */
  let store;
  /** @type {ReturnType<typeof createSpecialistDelegationService>} */
  let service;

  beforeEach(() => {
    store = createMemoryStore();
    service = createSpecialistDelegationService({ store });
  });

  it('runs the contract fixture: create → persist → execute → result → evaluate → trace', async () => {
    const { delegation, result, spawned } = await service.delegateAndExecute(
      contractInput()
    );

    assert.equal(delegation.status, 'authorized');
    assert.equal(delegation.authority, 'observe');
    assert.equal(delegation.objective, CONTRACT_OBJECTIVE);
    assert.equal(delegation.reason, CONTRACT_REASON);
    assert.equal(result.status, 'completed');
    assert.equal(result.summary, 'Three relevant opportunities detected.');
    assert.equal(result.confidence, 0.84);
    assert.ok(result.uncertainties.some((u) => /timing evidence/i.test(u)));
    assert.ok(result.evidenceRefs.length >= 3);
    assert.ok(result.observations.length);
    assert.ok(result.actionsTaken.length);
    assert.notEqual(result.observations[0].text, result.actionsTaken[0].text);
    assert.deepEqual(spawned, []);
    assert.equal(result.actionsTaken.some((a) => /publish|send|call/i.test(a.text)), false);

    const persistedDelegation = await service.getDelegation(delegation.id, {
      authorizedTenantId: '1',
    });
    const persistedResult = await service.getResult(result.id, {
      authorizedTenantId: '1',
    });
    assert.equal(persistedDelegation.objective, CONTRACT_OBJECTIVE);
    assert.equal(persistedResult.summary, result.summary);

    const evaluation = await service.evaluateResult({
      authorizedTenantId: '1',
      resultId: result.id,
    });
    assert.equal(evaluation.acceptedAsGroundTruth, false);
    assert.equal(evaluation.priorityApplied, false);
    assert.equal(evaluation.objectiveSatisfied, true);
    assert.ok(evaluation.explanation);

    const trail = await service.traceProvenance({
      authorizedTenantId: '1',
      evaluationId: evaluation.id,
    });
    assert.equal(trail.chain.delegation.id, delegation.id);
    assert.equal(trail.chain.result.id, result.id);
    assert.ok(trail.chain.evidence.length >= 3);
    assert.match(trail.narrative, /Three relevant opportunities/i);
    assert.match(trail.narrative, /Assess whether Acquisition/i);
  });

  it('isolates tenants — tenant A cannot retrieve tenant B context or results', async () => {
    const a = await service.delegateAndExecute(contractInput({ tenantId: '1' }));
    const b = await service.delegateAndExecute(
      contractInput({ authorizedTenantId: '2', tenantId: '2' })
    );

    const leakedDelegation = await store.getDelegation(b.delegation.id, '1');
    const leakedResult = await store.getResult(b.result.id, '1');
    assert.equal(leakedDelegation, null);
    assert.equal(leakedResult, null);

    await assert.rejects(
      () =>
        createDelegation(
          contractInput({
            authorizedTenantId: '1',
            tenantId: '2',
          }),
          { store }
        ),
      (err) =>
        err instanceof SpecialistDelegationError && err.code === 'tenant_mismatch'
    );

    const listed = await service.listDelegations({ authorizedTenantId: '1' });
    assert.ok(listed.every((d) => d.tenantId === '1'));
    assert.ok(listed.some((d) => d.id === a.delegation.id));
    assert.ok(!listed.some((d) => d.id === b.delegation.id));
  });

  it('fails closed when authority is missing', async () => {
    await assert.rejects(
      () => service.createDelegation(contractInput({ authority: null })),
      (err) =>
        err instanceof SpecialistDelegationError &&
        err.code === 'missing_authority'
    );
    const rows = await store.listDelegations({ tenantId: '1' });
    assert.equal(rows.length, 1);
    assert.equal(rows[0].status, 'declined_policy');
  });

  it('rejects unsupported authority (execute on observe-only capability)', async () => {
    await assert.rejects(
      () => service.createDelegation(contractInput({ authority: 'execute' })),
      (err) =>
        err instanceof SpecialistDelegationError &&
        err.code === 'unsupported_authority'
    );
  });

  it('fails closed on tenant policy conflict and does not silently downgrade', async () => {
    const restricted = createSpecialistDelegationService({
      store,
      tenantPolicy: { maxDelegationAuthority: 'observe' },
    });

    await assert.rejects(
      () =>
        restricted.createDelegation({
          authorizedTenantId: '1',
          specialist: 'paige',
          capability: 'content_strategy',
          objective: 'Draft a LinkedIn post.',
          reason: 'Operator asked for a draft.',
          authority: 'draft',
        }),
      (err) =>
        err instanceof SpecialistDelegationError &&
        err.code === 'tenant_policy_conflict'
    );

    const saved = (await store.listDelegations({ tenantId: '1' }))[0];
    assert.equal(saved.status, 'declined_policy');
    assert.equal(saved.authority, 'draft');
    assert.ok(
      saved.policyEvents.some((e) => e.kind === 'tenant_policy_conflict')
    );
  });

  it('persists delegation and result across a process-restart snapshot', async () => {
    const { delegation, result } = await service.delegateAndExecute(contractInput());
    const evaluation = await service.evaluateResult({
      authorizedTenantId: '1',
      resultId: result.id,
    });

    const snapshot = store.serialize();
    const restarted = createMemoryStore(snapshot);
    const revived = createSpecialistDelegationService({ store: restarted });

    const d = await revived.getDelegation(delegation.id, { authorizedTenantId: '1' });
    const r = await revived.getResult(result.id, { authorizedTenantId: '1' });
    const e = await revived.getEvaluation(evaluation.id, { authorizedTenantId: '1' });
    assert.equal(d.objective, CONTRACT_OBJECTIVE);
    assert.equal(r.summary, 'Three relevant opportunities detected.');
    assert.equal(e.acceptedAsGroundTruth, false);
    assert.ok(r.evidenceRefs.length >= 3);
  });

  it('preserves partial evidence when execution only partially succeeds', async () => {
    const { result } = await service.delegateAndExecute(
      contractInput({ fixtureMode: 'partial' })
    );
    assert.equal(result.status, 'partial');
    assert.ok(result.evidenceRefs.length >= 1);
    assert.ok(result.errors.some((e) => e.code === 'enrichment_unavailable'));

    const again = await service.getResultForDelegation(result.delegationId, {
      authorizedTenantId: '1',
    });
    assert.equal(again.evidenceRefs.length, result.evidenceRefs.length);
    assert.equal(again.status, 'partial');

    const evaluation = await service.evaluateResult({
      authorizedTenantId: '1',
      resultId: result.id,
    });
    assert.equal(evaluation.objectiveSatisfied, false);
    assert.equal(evaluation.warrantsAnotherDelegation, true);
    assert.match(evaluation.explanation, /partial/i);
  });

  it('traces a material conclusion back to evidence', async () => {
    const { result } = await service.delegateAndExecute(contractInput());
    const evaluation = await service.evaluateResult({
      authorizedTenantId: '1',
      resultId: result.id,
    });
    const trail = await service.traceProvenance({
      authorizedTenantId: '1',
      evaluationId: evaluation.id,
    });
    assert.ok(trail.chain.evaluation);
    assert.ok(trail.chain.result);
    assert.ok(trail.chain.delegation);
    assert.ok(trail.chain.evidence.some((e) => e.id === 'ev-test-opp-1'));
    assert.match(trail.narrative, /Evidence:/);
  });

  it('does not let a specialist mutate Command Deck priority', async () => {
    let applierCalls = 0;
    const { result } = await service.delegateAndExecute(contractInput());
    assert.equal(result.priorityChange, undefined);
    assert.equal(result.commandDeckPriority, undefined);

    const evaluation = await service.evaluateResult({
      authorizedTenantId: '1',
      resultId: result.id,
    });
    assert.equal(evaluation.priorityApplied, false);
    assert.ok(evaluation.suggestedPriorityChange);
    assert.equal(evaluation.suggestedPriorityChange.domain, 'acquisition');
    assert.equal(evaluation.suggestedPriorityChange.to, 'elevated');

    await assert.rejects(
      () =>
        applyEvaluationPriority(
          { authorizedTenantId: '1', evaluationId: evaluation.id },
          { store }
        ),
      (err) =>
        err instanceof SpecialistDelegationError &&
        err.code === 'priority_applier_required'
    );
    assert.equal(applierCalls, 0);

    const applied = await applyEvaluationPriority(
      { authorizedTenantId: '1', evaluationId: evaluation.id },
      {
        store,
        priorityApplier: async (payload) => {
          applierCalls += 1;
          return { applied: true, domainId: payload.domainId };
        },
      }
    );
    assert.equal(applierCalls, 1);
    assert.equal(applied.priorityApplied, true);
  });

  it('keeps operator direction authoritative over a prior specialist result', async () => {
    const { result } = await service.delegateAndExecute(
      contractInput({
        businessContext: {
          operatorDirection: {
            text: "Don't pursue law firms anymore. Focus on property managers.",
            excludedSegments: ['law_firm', 'law firms'],
            focusSegments: ['property_manager'],
            authoritative: true,
          },
        },
      })
    );

    const rogue = {
      ...result,
      summary: 'Law firms remain the strongest historical segment. Restore law-firm outreach.',
      observations: [
        { text: 'Historical performance suggests law firms should be restored.' },
      ],
      recommendedNextAction: {
        type: 'pursue',
        text: 'Resume law firm acquisition.',
      },
    };

    const evaluation = await evaluateResult(
      {
        authorizedTenantId: '1',
        result: rogue,
        delegation: await store.getDelegation(result.delegationId, '1'),
        operatorDirection: {
          text: "Don't pursue law firms anymore. Focus on property managers.",
          excludedSegments: ['law firms'],
          focusSegments: ['property managers'],
        },
      },
      { store }
    );

    assert.equal(evaluation.operatorDirectionHonored, false);
    assert.equal(evaluation.acceptedAsGroundTruth, false);
    assert.equal(evaluation.suggestedPriorityChange, null);
    assert.match(evaluation.explanation, /operator direction remains authoritative/i);
    assert.match(evaluation.operatorChallenge || evaluation.explanation, /law firm/i);
  });

  it('does not automatically spawn another specialist from a recommendation', async () => {
    const { result, spawned } = await service.delegateAndExecute(
      contractInput({ fixtureMode: 'partial' })
    );
    assert.equal(result.recommendedNextAction.type, 'retry');
    assert.deepEqual(spawned, []);
    const all = await store.listDelegations({ tenantId: '1' });
    assert.equal(all.length, 1);
  });

  it('blocks an unwired specialist instead of inventing execution', async () => {
    const delegation = await service.createDelegation({
      authorizedTenantId: '1',
      specialist: 'scout',
      capability: 'prospect_intelligence',
      objective: 'Identify Manchester property managers.',
      reason: 'Pipeline volume in that segment is below target.',
      authority: 'observe',
    });
    const result = await service.executeDelegation({
      authorizedTenantId: '1',
      delegationId: delegation.id,
    });
    assert.equal(result.status, 'blocked');
    assert.match(result.summary, /adapter/i);
  });

  it('fails closed when a required constraint cannot be determined', async () => {
    await assert.rejects(
      () =>
        service.createDelegation(
          contractInput({
            constraints: {
              requiredDeterminate: ['geography'],
            },
          })
        ),
      (err) =>
        err instanceof SpecialistDelegationError &&
        err.code === 'constraint_indeterminate'
    );
  });

  it('distinguishes observed fact, Max inference, and operator instruction', async () => {
    const delegation = await service.createDelegation(
      contractInput({
        evidenceRefs: [
          {
            id: 'ev-obs',
            kind: 'company',
            sourceKind: 'observed_fact',
            label: 'Company expanded its portfolio',
          },
          {
            id: 'ev-inf',
            kind: 'recommendation',
            sourceKind: 'max_inference',
            label: 'Max infers near-term cleaning demand',
          },
          {
            id: 'ev-op',
            kind: 'operator_correction',
            sourceKind: 'operator_instruction',
            label: 'Focus on property managers',
          },
        ],
      })
    );
    const kinds = new Set(delegation.evidenceRefs.map((e) => e.sourceKind));
    assert.deepEqual(
      [...kinds].sort(),
      ['max_inference', 'observed_fact', 'operator_instruction']
    );
  });

  it('does not register envisioned specialists that are not callable', async () => {
    const listed = service.registry.list();
    const names = listed.map((e) => e.specialist);
    assert.ok(names.includes('test_intelligence'));
    assert.ok(names.includes('scout'));
    assert.ok(names.includes('paige'));
    assert.ok(!names.includes('penny'));
    assert.ok(!names.includes('emmett'));
    assert.ok(!names.includes('sam'));
    assert.ok(!names.includes('cal'));
    assert.equal(
      listed.find((e) => e.specialist === 'test_intelligence').callable,
      true
    );
    assert.equal(listed.find((e) => e.specialist === 'scout').callable, false);
  });
});
