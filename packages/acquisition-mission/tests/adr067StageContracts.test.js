'use strict';

/**
 * ADR-067 — Stage Contracts Are Authoritative.
 * Execution pauses when the contract requires human judgment, even if MissionPause cannot be built.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../index');
const {
  STAGES,
  PROGRESSION_STAGES,
  MISSION_STAGE_CONTRACTS,
  resolveHumanDecisionGate,
  resolveProgressionState,
  runAutonomousProgression,
  deriveMissionPause,
} = amo;

describe('ADR-067 — Stage Contracts Are Authoritative', () => {
  let engine;
  let mission;

  beforeEach(() => {
    engine = amo.createAcquisitionMissionEngine();
    mission = engine.create({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester NH for law firms.',
      targetSegment: 'Law Firms',
    });
  });

  it('contract requires human decision even when MissionPause is unavailable', () => {
    const snapshot = {
      mission: {
        ...mission,
        stage: STAGES.EXECUTE,
        pendingOperatorDecision: null,
        blockers: [],
      },
      contributions: [],
    };

    assert.equal(deriveMissionPause(snapshot), null);
    assert.equal(MISSION_STAGE_CONTRACTS[PROGRESSION_STAGES.EXECUTION].requiresHumanDecision, true);

    const gate = resolveHumanDecisionGate(snapshot);
    assert.equal(gate.shouldPause, true);
    assert.equal(gate.pause, null);
    assert.ok(gate.block);
    assert.equal(gate.block.pauseFallback, true);
    assert.equal(gate.block.blockingComponent, 'Stage Contract');
  });

  it('resolveProgressionState returns paused with ExecutionBlock fallback', () => {
    const snapshot = {
      mission: {
        ...mission,
        stage: STAGES.EXECUTE,
        pendingOperatorDecision: null,
        blockers: [],
      },
      contributions: [],
    };

    const state = resolveProgressionState(snapshot);
    assert.equal(state.outcome, 'paused');
    assert.equal(state.pause, null);
    assert.ok(state.block);
    assert.equal(state.block.pauseFallback, true);
  });

  it('runAutonomousProgression never continues past contract human-decision boundary', async () => {
    const execEngine = amo.createAcquisitionMissionEngine();
    const execMission = execEngine.create({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester NH for law firms.',
      targetSegment: 'Law Firms',
    });

    const updated = execEngine.get(execMission.id, '10');
    execEngine.store.putMission({
      ...updated,
      stage: STAGES.EXECUTE,
      pendingOperatorDecision: null,
      structuredMissionApproved: true,
      blockers: [],
    });

    const result = await runAutonomousProgression({
      engine: execEngine,
      missionId: execMission.id,
      tenantId: '10',
    });

    assert.equal(result.outcome, 'paused');
    assert.equal(result.progressionStage, PROGRESSION_STAGES.EXECUTION);
    assert.equal(result.pause, null);
    assert.ok(result.block);
    assert.equal(result.block.pauseFallback, true);
    assert.match(result.presentation, /Mission Paused/i);
  });

  it('inspect snapshot uses contract-authoritative progression state', () => {
    const updated = engine.get(mission.id, '10');
    engine.store.putMission({
      ...updated,
      stage: STAGES.EXECUTE,
      pendingOperatorDecision: null,
      structuredMissionApproved: true,
      blockers: [],
    });

    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(snapshot.progression.outcome, 'paused');
    assert.ok(snapshot.progression.contract);
    assert.equal(snapshot.progression.contract.requiresHumanDecision, true);
    assert.ok(snapshot.progression.block);
    assert.equal(snapshot.progression.block.pauseFallback, true);
  });

  it('existing Discovery Review pause still uses MissionPause when available', async () => {
    const result = await runAutonomousProgression({
      engine,
      missionId: mission.id,
      tenantId: '10',
      allowFixtureFallback: true,
    });

    assert.equal(result.outcome, 'paused');
    assert.ok(result.pause);
    assert.equal(result.pause.stage, PROGRESSION_STAGES.DISCOVERY_REVIEW);
    assert.equal(result.block, null);
  });
});

// Current-graph reconciliation regressions, independent of the original PR tests.
describe('ADR-067 — current graph safety reconciliation', () => {
  function executionFixture(stage = STAGES.EXECUTE, extra = {}) {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester NH for law firms.',
      targetSegment: 'Law Firms',
    });
    engine.store.putMission({
      ...engine.get(mission.id, '10'),
      stage,
      pendingOperatorDecision: null,
      structuredMissionApproved: true,
      blockers: [],
      ...extra,
    });
    return { engine, mission };
  }

  for (const stage of [STAGES.EXECUTE, STAGES.READY]) {
    it(`${stage} without presentation pauses without dispatch or writes`, async () => {
      const { engine, mission } = executionFixture(stage);
      const before = JSON.stringify(engine.inspect(mission.id, { tenantId: '10' }));
      let dispatches = 0;
      const unexpectedDispatch = async () => { dispatches += 1; throw new Error('Unexpected dispatch.'); };
      const deps = Object.fromEntries([
        'advancePlanAfterApproval', 'advanceDiscoveryAfterApproval', 'advanceMaxPrioritization',
        'advanceAcquisitionApproach', 'advancePennyPaidAcquisition', 'advancePaigeVariants',
        'advanceEmmettCapacity',
      ].map((key) => [key, unexpectedDispatch]));
      const result = await runAutonomousProgression({
        engine, missionId: mission.id, tenantId: '10', allowFixtureFallback: false, deps,
      });
      assert.equal(result.outcome, 'paused');
      assert.equal(result.pause, null);
      assert.equal(result.block.pauseFallback, true);
      assert.deepEqual(result.transitions, []);
      assert.equal(dispatches, 0);
      assert.equal(JSON.stringify(engine.inspect(mission.id, { tenantId: '10' })), before);
      assert.equal(result.snapshot.progression.outcome, 'paused');
      assert.match(result.snapshot.progression.presentation, /^Mission Paused/);
    });
  }

  it('the final result cannot bypass a human-decision contract when maxSteps is zero', async () => {
    const { engine, mission } = executionFixture();
    const result = await runAutonomousProgression({ engine, missionId: mission.id, tenantId: '10', maxSteps: 0 });
    assert.equal(result.outcome, 'paused');
    assert.equal(result.block.pauseFallback, true);
    assert.deepEqual(result.transitions, []);
  });

  it('cancellation is not reopened as a fallback operator decision', async () => {
    const { engine, mission } = executionFixture(STAGES.EXECUTE, { planCancelled: true });
    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(resolveHumanDecisionGate(snapshot).shouldPause, false);
    assert.equal(snapshot.progression.block, null);
    assert.equal(snapshot.workspaceContext.workspaceMode, 'complete');
    const result = await runAutonomousProgression({ engine, missionId: mission.id, tenantId: '10' });
    assert.equal(result.outcome, 'complete');
    assert.equal(result.block, null);
    assert.deepEqual(result.transitions, []);
  });

  it('the pure contract gate covers discovery review even without pause metadata', () => {
    const snapshot = {
      mission: { stage: STAGES.DISCOVER, structuredMissionApproved: true, pendingOperatorDecision: null, blockers: [] },
      contributions: [{ specialist: 'scout', kind: 'discovery', payload: {} }],
    };
    assert.equal(deriveMissionPause(snapshot), null);
    const gate = resolveHumanDecisionGate(snapshot);
    assert.equal(gate.progressionStage, PROGRESSION_STAGES.DISCOVERY_REVIEW);
    assert.equal(gate.shouldPause, true);
    assert.equal(gate.block.pauseFallback, true);
  });

  it('the store still rejects discovery artifacts without a post-discovery decision', () => {
    const { engine, mission } = executionFixture(STAGES.DISCOVER);
    engine.contribute(mission.id, {
      specialist: 'scout', kind: 'discovery',
      payload: { qualifiedCount: 0, confidence: 0.4, companies: [], prospects: [] },
    }, { tenantId: '10' });
    assert.throws(() => engine.store.putMission({
      ...engine.get(mission.id, '10'), pendingOperatorDecision: null, blockers: [],
    }), { code: 'MISSION_STATE_INCONSISTENT' });
  });

  it('inspect preserves an actionable provider block instead of replacing it with a fallback', () => {
    const { engine, mission } = executionFixture(STAGES.DISCOVER);
    engine.contribute(mission.id, {
      specialist: 'scout', kind: 'discovery',
      payload: {
        blocked: true, blockerCode: 'external_discovery_capability_unavailable',
        summary: 'External discovery capability unavailable.', qualifiedCount: 0,
        confidence: 0.4, companies: [], prospects: [],
      },
    }, { tenantId: '10' });
    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(snapshot.progression.outcome, 'blocked');
    assert.equal(snapshot.progression.block.blockingComponent, 'External Discovery Provider Registry');
    assert.notEqual(snapshot.progression.block.pauseFallback, true);
    assert.match(snapshot.progression.presentation, /^Mission Blocked/);
  });

  it('an execution error remains blocked even if the final snapshot has a human-decision contract', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10', objective: 'Acquire commercial cleaning customers in Manchester NH for law firms.',
      targetSegment: 'Law Firms',
    });
    const failure = new Error('Synthetic commit failure.');
    const result = await runAutonomousProgression({
      engine, missionId: mission.id, tenantId: '10',
      deps: {
        advancePlanAfterApproval: async () => {
          engine.store.putMission({
            ...engine.get(mission.id, '10'), stage: STAGES.EXECUTE,
            structuredMissionApproved: true, pendingOperatorDecision: null, blockers: [],
          });
          throw failure;
        },
      },
    });
    assert.equal(result.outcome, 'blocked');
    assert.equal(result.error, failure);
    assert.equal(result.block.unmetPrecondition, failure.message);
    assert.notEqual(result.block.pauseFallback, true);
    assert.match(result.presentation, /^Mission Blocked/);
  });

  it('existing pending-decision consistency guards still reject invalid state', () => {
    assert.throws(() => executionFixture(STAGES.EXECUTE, {
      pendingOperatorDecision: { kind: amo.OPERATOR_DECISION_KINDS.PLAN_APPROVAL, stage: STAGES.DISCOVER },
    }), { code: 'MISSION_STATE_INCONSISTENT' });
  });

  it('unknown and inherited stage names do not resolve to contracts', () => {
    assert.equal(amo.getStageContract('unknown_stage'), null);
    assert.equal(amo.getStageContract('__proto__'), null);
    assert.equal(amo.getStageContract('constructor'), null);
  });
});

describe('ADR-067 — preserve artifact-bound execution authority', () => {
  async function approvedReadyFixture() {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester NH for law firms.',
      targetSegment: 'Law Firms',
    });
    const input = { engine, missionId: mission.id, tenantId: '10', allowFixtureFallback: true };
    const discovered = await runAutonomousProgression(input);
    assert.equal(discovered.progressionStage, PROGRESSION_STAGES.DISCOVERY_REVIEW);
    await require('../../max/workspace/AmoOperatorApproval').advancePrioritizationAfterApproval({
      ...input, mission: engine.get(mission.id, '10'), question: 'Approved prioritization.',
    });
    const prepared = await runAutonomousProgression(input);
    assert.equal(prepared.snapshot.mission.stage, STAGES.READY);
    assert.equal(prepared.pause.requiredDecision, 'Authorize external execution of prepared outreach?');
    const request = amo.createExecutionRequest({
      source: amo.EXECUTION_SOURCES.APPROVAL_BUTTON,
      intent: amo.EXECUTION_INTENTS.APPROVE_EXECUTION,
      missionId: mission.id, operatorId: 'operator-1', stage: STAGES.READY,
    });
    await amo.routeExecutionRequest(request, { engine, tenantId: '10' });
    return { engine, mission, snapshot: engine.inspect(mission.id, { tenantId: '10' }) };
  }

  for (const stage of [STAGES.READY, STAGES.EXECUTE]) {
    it(`current artifact-bound approval satisfies the ${stage} gate without advancing or sending`, async () => {
      const { engine, mission } = await approvedReadyFixture();
      if (stage === STAGES.EXECUTE) {
        engine.progress(mission.id, { role: 'max' }, { tenantId: '10', stage });
      }
      const snapshot = engine.inspect(mission.id, { tenantId: '10' });
      assert.equal(snapshot.mission.stage, stage);
      assert.equal(snapshot.mission.pendingOperatorDecision, null);
      assert.equal(amo.isExecutionApproved(snapshot.contributions, mission.id), true);
      assert.equal(amo.canEnter(STAGES.EXECUTE,
        amo.specialistContext(snapshot.contributions, { missionId: mission.id })).ok, true);
      assert.equal(resolveHumanDecisionGate(snapshot).shouldPause, false);
      assert.equal(snapshot.progression.outcome, 'active');
      assert.equal(snapshot.progression.pause, null);
      assert.equal(snapshot.progression.block, null);
      assert.doesNotMatch(snapshot.progression.presentation, /Mission Paused|Operator approval before send/);
      const before = JSON.stringify(snapshot);
      let dispatches = 0;
      const unexpectedDispatch = async () => { dispatches += 1; throw new Error('Unexpected dispatch.'); };
      const deps = Object.fromEntries([
        'advancePlanAfterApproval', 'advanceDiscoveryAfterApproval', 'advanceMaxPrioritization',
        'advanceAcquisitionApproach', 'advancePennyPaidAcquisition', 'advancePaigeVariants',
        'advanceEmmettCapacity',
      ].map((key) => [key, unexpectedDispatch]));
      for (const maxSteps of [8, 0]) {
        const result = await runAutonomousProgression({
          engine, missionId: mission.id, tenantId: '10', allowFixtureFallback: false, deps, maxSteps,
        });
        assert.equal(result.outcome, 'complete');
        assert.equal(result.pause, null);
        assert.equal(result.block, null);
        assert.deepEqual(result.transitions, []);
        assert.equal(dispatches, 0);
        assert.equal(JSON.stringify(engine.inspect(mission.id, { tenantId: '10' })), before);
      }
    });
  }

  for (const invalidity of ['invalidated', 'superseded', 'stale_revision']) {
    it(`${invalidity} approval does not satisfy a missing-presentation gate`, async () => {
      const { engine, snapshot, mission } = await approvedReadyFixture();
      const changed = structuredClone(snapshot);
      const approval = amo.findValidExecutionApproval(changed.contributions, mission.id);
      assert.ok(approval);
      if (invalidity === 'stale_revision') approval.payload.preparedArtifactRevision = 'old-revision';
      else approval.payload[invalidity] = true;
      assert.equal(amo.isExecutionApproved(changed.contributions, mission.id), false);
      assert.equal(deriveMissionPause(changed), null);
      const gate = resolveHumanDecisionGate(changed);
      assert.equal(gate.shouldPause, true);
      assert.equal(gate.block.pauseFallback, true);
      assert.equal(resolveProgressionState(changed).outcome, 'paused');

      // The real store must advertise the restored decision when its valid approval
      // is revoked. Synthetic missing-presentation behavior above stays fail-closed.
      engine.store.updateContribution(approval.id, approval);
      engine.store.putMission({
        ...engine.get(mission.id, '10'),
        pendingOperatorDecision: amo.buildPendingExecutionDecision(changed.mission, changed.contributions),
      });
      const before = JSON.stringify(engine.inspect(mission.id, { tenantId: '10' }));
      let dispatches = 0;
      const unexpectedDispatch = async () => { dispatches += 1; throw new Error('Unexpected dispatch.'); };
      const deps = Object.fromEntries([
        'advancePlanAfterApproval', 'advanceDiscoveryAfterApproval', 'advanceMaxPrioritization',
        'advanceAcquisitionApproach', 'advancePennyPaidAcquisition', 'advancePaigeVariants',
        'advanceEmmettCapacity',
      ].map((key) => [key, unexpectedDispatch]));
      for (const maxSteps of [8, 0]) {
        const result = await runAutonomousProgression({
          engine, missionId: mission.id, tenantId: '10', allowFixtureFallback: false, deps, maxSteps,
        });
        assert.equal(result.outcome, 'paused');
        assert.ok(result.pause);
        assert.deepEqual(result.transitions, []);
        assert.equal(dispatches, 0);
        assert.equal(JSON.stringify(engine.inspect(mission.id, { tenantId: '10' })), before);
      }
    });
  }

  it('changed Paige artifacts invalidate approval and restore a decision boundary', async () => {
    const { engine, mission } = await approvedReadyFixture();
    engine.contribute(mission.id, {
      specialist: amo.SPECIALISTS.PAIGE, kind: amo.CONTRIBUTION_KINDS.VARIANTS,
      payload: { variants: [{ label: 'Revised', subject: 'Updated subject', body: 'Updated body', cta: 'Reply now' }] },
    }, { tenantId: '10' });
    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(amo.isExecutionApproved(snapshot.contributions, mission.id), false);
    assert.equal(resolveHumanDecisionGate(snapshot).shouldPause, true);
    const result = await runAutonomousProgression({
      engine, missionId: mission.id, tenantId: '10', allowFixtureFallback: false,
    });
    assert.equal(result.outcome, 'paused');
    assert.deepEqual(result.transitions, []);
  });

  it('changed prepared queue evidence invalidates the exact bound approval', async () => {
    const { snapshot, mission } = await approvedReadyFixture();
    const changed = structuredClone(snapshot);
    const emmett = amo.findEmmettCapacity(changed.contributions);
    assert.ok(emmett);
    const payload = require('../ContributionSupersession').unwrapSpecialistPayload(emmett);
    payload.queue = { ...payload.queue, items: [...(payload.queue?.items || []), { prospectId: 'new-target' }] };
    assert.equal(amo.isExecutionApproved(changed.contributions, mission.id), false);
    assert.equal(resolveHumanDecisionGate(changed).shouldPause, true);
    assert.equal(resolveProgressionState(changed).outcome, 'paused');
  });

  it('a valid approval cannot bypass the tenant boundary during inspect or progression', async () => {
    const { engine, mission, snapshot } = await approvedReadyFixture();
    assert.equal(amo.isExecutionApproved(snapshot.contributions, mission.id), true);
    const before = JSON.stringify(engine.inspect(mission.id, { tenantId: '10' }));
    let dispatches = 0;
    const unexpectedDispatch = async () => { dispatches += 1; throw new Error('Unexpected dispatch.'); };
    const deps = Object.fromEntries([
      'advancePlanAfterApproval', 'advanceDiscoveryAfterApproval', 'advanceMaxPrioritization',
      'advanceAcquisitionApproach', 'advancePennyPaidAcquisition', 'advancePaigeVariants',
      'advanceEmmettCapacity',
    ].map((key) => [key, unexpectedDispatch]));
    assert.throws(() => engine.inspect(mission.id, { tenantId: 'other-tenant' }), {
      code: 'amo_tenant_mismatch',
    });
    for (const maxSteps of [8, 0]) {
      await assert.rejects(() => runAutonomousProgression({
        engine, missionId: mission.id, tenantId: 'other-tenant',
        allowFixtureFallback: false, deps, maxSteps,
      }), { code: 'amo_tenant_mismatch' });
    }
    assert.equal(dispatches, 0);
    assert.equal(JSON.stringify(engine.inspect(mission.id, { tenantId: '10' })), before);
  });

  it('raw executionApproved flags cannot replace artifact-bound approval evidence', () => {
    const snapshot = {
      mission: { id: 'no-approval', stage: STAGES.READY, executionApproved: true, pendingOperatorDecision: null, blockers: [] },
      executionApproved: true,
      contributions: [],
    };
    const gate = resolveHumanDecisionGate(snapshot);
    assert.equal(gate.shouldPause, true);
    assert.equal(gate.block.pauseFallback, true);
  });
});
