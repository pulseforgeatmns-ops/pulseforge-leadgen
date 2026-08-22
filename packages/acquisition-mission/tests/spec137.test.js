'use strict';

/**
 * SPEC-137 — Atomic Mission Lifecycle Transitions.
 * Stage transitions must update stage, status, and pendingOperatorDecision together.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../index');
const { createMission } = require('../Mission');
const {
  OPERATOR_DECISION_KINDS,
  STAGES,
  MISSION_STATE_INCONSISTENT,
  applyStageTransition,
  deriveStageLifecycle,
  assertMissionStateConsistent,
} = amo;

const OBJECTIVE =
  'Acquire one recurring commercial cleaning client from a short-term rental operator in Greater Manchester.';

describe('SPEC-137 — Atomic Mission Lifecycle Transitions', () => {
  it('deriveStageLifecycle clears pending when leaving Discover', () => {
    const mission = createMission({
      tenantId: '10',
      objective: OBJECTIVE,
    });
    assert.equal(mission.stage, STAGES.DISCOVER);
    assert.equal(mission.pendingOperatorDecision.kind, OPERATOR_DECISION_KINDS.PLAN_APPROVAL);

    const lifecycle = deriveStageLifecycle(mission, STAGES.UNDERSTAND);
    assert.equal(lifecycle.stage, STAGES.UNDERSTAND);
    assert.equal(lifecycle.pendingOperatorDecision, null);
    assert.notEqual(lifecycle.status, mission.status);
  });

  it('applyStageTransition mutates stage, status, and pending together', () => {
    const mission = createMission({
      tenantId: '10',
      objective: OBJECTIVE,
    });
    const result = applyStageTransition(mission, STAGES.UNDERSTAND, { contributions: [] });
    assert.equal(result.from, STAGES.DISCOVER);
    assert.equal(result.to, STAGES.UNDERSTAND);
    assert.equal(mission.stage, STAGES.UNDERSTAND);
    assert.equal(mission.pendingOperatorDecision, null);
    assert.doesNotThrow(() => assertMissionStateConsistent(mission));
  });

  it('discover → understand via Engine.progress leaves no stale pending decision', () => {
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

    const stored = engine.store.getMission(mission.id);
    stored.pendingOperatorDecision = {
      stage: STAGES.DISCOVER,
      kind: OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL,
      prompt: 'Approve discovery?',
    };
    engine.store.restore({
      missions: [[stored.id, stored]],
      events: engine.store.snapshot().events,
      contributions: engine.store.snapshot().contributions,
      observations: [],
      outcomes: [],
      learning: [],
    });

    const progressed = engine.progress(mission.id, { role: 'max' }, {
      tenantId: '10',
      stage: STAGES.UNDERSTAND,
    });
    assert.equal(progressed.stage, STAGES.UNDERSTAND);
    assert.equal(progressed.pendingOperatorDecision, null);
    assert.doesNotThrow(() => engine.inspect(mission.id, { tenantId: '10' }));
  });

  it('rejects pendingOperatorDecision.stage mismatch with mission.stage', () => {
    const mission = createMission({
      tenantId: '10',
      objective: OBJECTIVE,
      planApproved: true,
    });
    mission.stage = STAGES.UNDERSTAND;
    mission.pendingOperatorDecision = {
      stage: STAGES.DISCOVER,
      kind: OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL,
      prompt: 'Approve discovery?',
    };
    assert.throws(
      () => assertMissionStateConsistent(mission),
      (err) => err.code === MISSION_STATE_INCONSISTENT
        && /pendingOperatorDecision\.stage does not match mission\.stage/.test(err.message)
    );
    assert.throws(
      () => amo.createMemoryAmoStore().putMission(mission),
      (err) => err.code === MISSION_STATE_INCONSISTENT
    );
  });

  it('never persists partial transition: stage advanced without clearing pending', () => {
    const mission = createMission({
      tenantId: '10',
      objective: OBJECTIVE,
    });
    mission.stage = STAGES.UNDERSTAND;
    mission.status = 'Understanding';
    mission.pendingOperatorDecision = {
      stage: STAGES.DISCOVER,
      kind: OPERATOR_DECISION_KINDS.PLAN_APPROVAL,
      prompt: 'Approve mission plan?',
    };
    assert.throws(
      () => assertMissionStateConsistent(mission),
      (err) => err.code === MISSION_STATE_INCONSISTENT
    );
  });

  it('applyStageTransition to Discover rebuilds valid pending from mission state', () => {
    const mission = createMission({
      tenantId: '10',
      objective: OBJECTIVE,
    });
    mission.structuredMissionApproved = false;
    mission.structuredMission = null;
    mission.pendingOperatorDecision = {
      stage: STAGES.DISCOVER,
      kind: OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL,
      prompt: 'Approve discovery?',
    };
    applyStageTransition(mission, STAGES.DISCOVER, { contributions: [] });
    assert.equal(mission.stage, STAGES.DISCOVER);
    assert.equal(mission.pendingOperatorDecision.kind, OPERATOR_DECISION_KINDS.PLAN_APPROVAL);
    assert.doesNotThrow(() => assertMissionStateConsistent(mission));
  });
});
