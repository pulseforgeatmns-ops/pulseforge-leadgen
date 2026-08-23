'use strict';
const { createTestAmoRuntime } = require('../../max/workspace/tests/amoTestRuntime');

/**
 * SPEC-136 — Pending Operator Decision Consistency.
 * pendingOperatorDecision must satisfy execution predicates after every mutation.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../index');
const { createMission } = require('../Mission');
const {
  OPERATOR_DECISION_KINDS,
  STAGES,
  MISSION_STATE_INCONSISTENT,
  hasPendingPlanApproval,
  hasPendingDiscoveryApproval,
  hasConsumablePendingDecision,
  presentableOperatorDecision,
  assertMissionStateConsistent,
} = amo;
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
} = require('../../max/workspace/AmoOperatorApproval');
const { maybeHandleAcquisitionMissionExecution } = require('../../max/workspace/AcquisitionMissionExecution');

const OBJECTIVE =
  'Acquire one recurring commercial cleaning client from a short-term rental operator in Greater Manchester.';

function inconsistentPlanAfterApprove() {
  const mission = createMission({
    tenantId: '10',
    objective: OBJECTIVE,
  });
  return {
    ...mission,
    structuredMissionApproved: true,
    structuredMission: { immutable: true, objective: OBJECTIVE },
    missionPlanDraft: null,
    pendingOperatorDecision: {
      stage: STAGES.DISCOVER,
      kind: OPERATOR_DECISION_KINDS.PLAN_APPROVAL,
      prompt: 'Approve mission plan?',
    },
  };
}

function inconsistentDiscoveryBeforePlan() {
  const mission = createMission({
    tenantId: '10',
    objective: OBJECTIVE,
  });
  return {
    ...mission,
    structuredMissionApproved: false,
    structuredMission: null,
    pendingOperatorDecision: {
      stage: STAGES.DISCOVER,
      kind: OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL,
      prompt: 'Approve discovery?',
    },
  };
}

describe('SPEC-136 — Pending Operator Decision Consistency', () => {
  it('rejects structuredMissionApproved + stale plan_approval', () => {
    const broken = inconsistentPlanAfterApprove();
    assert.equal(hasPendingPlanApproval({ mission: broken }), false);
    assert.throws(
      () => assertMissionStateConsistent(broken),
      (err) => err.code === MISSION_STATE_INCONSISTENT
    );
    assert.throws(
      () => amo.createMemoryAmoStore().putMission(broken),
      (err) => err.code === MISSION_STATE_INCONSISTENT
    );
  });

  it('rejects discovery_approval when hasPendingDiscoveryApproval is false', () => {
    const broken = inconsistentDiscoveryBeforePlan();
    assert.equal(hasPendingDiscoveryApproval({ mission: broken }), false);
    assert.throws(
      () => assertMissionStateConsistent(broken),
      (err) => err.code === MISSION_STATE_INCONSISTENT
    );
    assert.throws(
      () => createMission({
        tenantId: '10',
        objective: OBJECTIVE,
        pendingOperatorDecision: {
          kind: OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL,
          prompt: 'Approve discovery?',
        },
      }),
      (err) => err.code === MISSION_STATE_INCONSISTENT
    );
  });

  it('inspect refuses to render an inconsistent mission', () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
    });
    const stored = engine.store.getMission(mission.id);
    stored.structuredMissionApproved = true;
    stored.structuredMission = { immutable: true, objective: OBJECTIVE };
    stored.pendingOperatorDecision = {
      kind: OPERATOR_DECISION_KINDS.PLAN_APPROVAL,
      prompt: 'Approve mission plan?',
    };
    engine.store.restore({
      missions: [[stored.id, stored]],
      events: engine.store.snapshot().events,
      contributions: [],
      observations: [],
      outcomes: [],
      learning: [],
    });
    assert.throws(
      () => engine.inspect(mission.id, { tenantId: '10' }),
      (err) => err.code === MISSION_STATE_INCONSISTENT
    );
  });

  it('presentable decision matches execution predicates', () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
    });
    const before = engine.inspect(mission.id, { tenantId: '10' });
    const presented = presentableOperatorDecision(before);
    assert.equal(hasPendingPlanApproval(before), true);
    assert.equal(hasPendingDiscoveryApproval(before), false);
    assert.equal(presented.kind, OPERATOR_DECISION_KINDS.PLAN_APPROVAL);
    assert.equal(presented.prompt, 'Approve mission plan?');
    assert.equal(presented.consumable, true);
  });

  it('plan approval atomically replaces plan_approval with discovery_approval', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
    });
    assert.equal(mission.structuredMissionApproved, false);
    assert.equal(mission.pendingOperatorDecision.kind, OPERATOR_DECISION_KINDS.PLAN_APPROVAL);

    await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved.',
    });

    const after = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(after.mission.structuredMissionApproved, true);
    assert.equal(after.mission.pendingOperatorDecision.kind, OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL);
    assert.equal(after.mission.pendingOperatorDecision.prompt, 'Approve discovery?');
    assert.equal(hasPendingPlanApproval(after), false);
    assert.equal(hasPendingDiscoveryApproval(after), true);
    assert.equal(presentableOperatorDecision(after).prompt, 'Approve discovery?');
    assert.equal(after.executableDecision.prompt, 'Approve discovery?');
  });

  it('full lifecycle: plan → approve → discovery → consume → Scout → cleared', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
    });

    const planTurn = await maybeHandleAcquisitionMissionExecution({
      question: 'Approved.',
      context: { tenantId: '10', missionId: mission.id },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: engine }),
      allowFixtureFallback: true,
    });
    assert.equal(planTurn.action, 'plan_approved');
    const afterPlan = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(hasPendingDiscoveryApproval(afterPlan), true);
    assert.equal(afterPlan.mission.pendingOperatorDecision.kind, OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL);
    assert.notEqual(afterPlan.mission.pendingOperatorDecision.kind, OPERATOR_DECISION_KINDS.PLAN_APPROVAL);

    const discoveryTurn = await maybeHandleAcquisitionMissionExecution({
      question: 'Approved. Begin Discovery.',
      context: { tenantId: '10', missionId: mission.id },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: engine }),
      allowFixtureFallback: true,
    });
    assert.equal(discoveryTurn.action, 'discovery_approved');
    assert.equal(discoveryTurn.executionResult.executionOutcome, 'completed');
    assert.ok(discoveryTurn.executionResult.discovery);

    const afterDiscovery = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(afterDiscovery.mission.pendingOperatorDecision, null);
    assert.equal(hasPendingDiscoveryApproval(afterDiscovery), false);
    assert.equal(hasPendingPlanApproval(afterDiscovery), false);
    assert.equal(hasConsumablePendingDecision(afterDiscovery), false);
    assert.equal(presentableOperatorDecision(afterDiscovery), null);
    assert.equal(
      afterDiscovery.contributions.some((row) => row.specialist === amo.SPECIALISTS.SCOUT),
      true
    );
  });

  it('never executes generic operator_approved while a consumable decision exists', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
    });

    const turn = await maybeHandleAcquisitionMissionExecution({
      question: 'Continue.',
      context: { tenantId: '10', missionId: mission.id },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: engine }),
      allowFixtureFallback: true,
    });

    assert.ok(turn);
    assert.notEqual(turn.action, 'operator_approved');
    assert.equal(hasConsumablePendingDecision(engine.inspect(mission.id, { tenantId: '10' })), true);
    assert.equal(
      (engine.inspect(mission.id, { tenantId: '10' }).contributions || [])
        .some((row) => row.payload && row.payload.action === 'operator_approved'),
      false
    );
  });

  it('progressing off Discover clears unconsumable pending decisions', () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
    });
    engine.contribute(mission.id, {
      specialist: amo.SPECIALISTS.SCOUT,
      kind: amo.CONTRIBUTION_KINDS.DISCOVERY,
      payload: {
        companies: [{ id: 'c1', name: 'Harbor Law' }],
        prospects: [{ id: 'p1', name: 'Alex' }],
        buyingSignals: ['Hiring office manager'],
        evidence: [{ label: 'Job post', source: 'job_board' }],
        confidence: 0.8,
      },
    }, { tenantId: '10' });
    const progressed = engine.progress(mission.id, { role: 'max' }, {
      tenantId: '10',
      stage: STAGES.UNDERSTAND,
    });
    assert.equal(progressed.stage, STAGES.UNDERSTAND);
    assert.equal(progressed.pendingOperatorDecision, null);
    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(hasPendingPlanApproval(snapshot), false);
    assert.equal(hasPendingDiscoveryApproval(snapshot), false);
  });

  it('advanceDiscoveryAfterApproval consumes pending before Scout result is durable', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
    });
    await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved.',
    });
    assert.equal(hasPendingDiscoveryApproval(engine.inspect(mission.id, { tenantId: '10' })), true);

    const result = await advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: true,
    });
    assert.equal(result.executionOutcome, 'completed');
    assert.equal(result.snapshot.mission.pendingOperatorDecision, null);
    assert.ok(result.discovery);
  });
});
