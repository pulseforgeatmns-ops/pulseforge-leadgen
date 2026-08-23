'use strict';
const { createTestAmoRuntime } = require('../../max/workspace/tests/amoTestRuntime');

/**
 * SPEC-135 — Mission Planning Gate Before Discovery.
 * Discovery approval must never be presented unless the mission has an approved, immutable Mission Plan.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../index');
const { createMission, buildPendingOperatorDecision } = require('../Mission');
const { OPERATOR_DECISION_KINDS, STAGES } = require('../types');
const { freezeStructuredMission } = require('../StructuredMission');
const { planFromObjective } = require('../MissionPlanner');
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  hasPendingDiscoveryApproval,
  hasPendingPlanApproval,
} = require('../../max/workspace/AmoOperatorApproval');
const { maybeHandleAcquisitionMissionExecution } = require('../../max/workspace/AcquisitionMissionExecution');
const { inferBlockers } = require('../Blockers');

const STR_OBJECTIVE =
  'Acquire one recurring commercial cleaning client from a short-term rental operator in Greater Manchester.';

describe('SPEC-135 — Mission Planning Gate Before Discovery', () => {
  it('new missions seed plan approval — never discovery approval', () => {
    const mission = createMission({
      tenantId: '10',
      objective: STR_OBJECTIVE,
    });
    assert.ok(mission.missionPlanDraft);
    assert.equal(mission.structuredMissionApproved, false);
    assert.ok(mission.pendingOperatorDecision);
    assert.equal(mission.pendingOperatorDecision.kind, OPERATOR_DECISION_KINDS.PLAN_APPROVAL);
    assert.equal(mission.pendingOperatorDecision.prompt, 'Approve mission plan?');
    assert.notEqual(mission.pendingOperatorDecision.kind, OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL);
  });

  it('buildPendingOperatorDecision returns null without draft or frozen plan', () => {
    const decision = buildPendingOperatorDecision({
      stage: STAGES.DISCOVER,
      input: {},
      missionPlanDraft: null,
      structuredMission: null,
      planAmbiguities: [],
      planned: null,
    });
    assert.equal(decision, null);
  });

  it('buildPendingOperatorDecision returns discovery only after plan is frozen', () => {
    const frozen = freezeStructuredMission(planFromObjective(STR_OBJECTIVE).draft, {
      approvedBy: 'operator',
    });
    const decision = buildPendingOperatorDecision({
      stage: STAGES.DISCOVER,
      input: {},
      missionPlanDraft: null,
      structuredMission: frozen,
      planAmbiguities: [],
      planned: null,
    });
    assert.equal(decision.kind, OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL);
    assert.equal(decision.prompt, 'Approve discovery?');
  });

  it('skipMissionPlanning missions do not get discovery approval pending', () => {
    const mission = createMission({
      tenantId: '10',
      objective: STR_OBJECTIVE,
      skipMissionPlanning: true,
    });
    assert.equal(mission.missionPlanDraft, null);
    assert.equal(mission.pendingOperatorDecision, null);
  });

  it('hasPendingDiscoveryApproval is false until plan is locked', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: STR_OBJECTIVE,
    });
    const before = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(hasPendingPlanApproval(before), true);
    assert.equal(hasPendingDiscoveryApproval(before), false);

    await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved.',
    });

    const after = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(hasPendingPlanApproval(after), false);
    assert.equal(hasPendingDiscoveryApproval(after), true);
    assert.equal(after.mission.pendingOperatorDecision.prompt, 'Approve discovery?');
  });

  it('full lifecycle: create → plan approve → discovery approve → Scout executes', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: STR_OBJECTIVE,
    });

    const planResult = await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved.',
    });
    assert.ok(planResult.structuredMission.immutable);

    const discoveryResult = await advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: true,
    });
    assert.equal(discoveryResult.executionOutcome, 'completed');
    assert.ok(discoveryResult.discovery);
  });

  it('discovery does not execute on ask path before plan is locked', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: STR_OBJECTIVE,
    });

    const turn = await maybeHandleAcquisitionMissionExecution({
      question: 'Begin discovery.',
      context: { tenantId: '10', missionId: mission.id },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: engine }),
      allowFixtureFallback: true,
    });

    assert.ok(turn);
    assert.notEqual(turn.action, 'discovery_approved');
    const after = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(hasPendingDiscoveryApproval(after), false);
    assert.equal(hasPendingPlanApproval(after), true);
    assert.equal(
      after.contributions.some((row) => row.specialist === amo.SPECIALISTS.SCOUT),
      false
    );
  });

  it('plan approval in same turn does not auto-execute Scout', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: STR_OBJECTIVE,
    });

    const turn = await maybeHandleAcquisitionMissionExecution({
      question: 'Approved. Begin Discovery.',
      context: { tenantId: '10', missionId: mission.id },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: engine }),
      allowFixtureFallback: true,
    });

    assert.ok(turn);
    assert.equal(turn.action, 'plan_approved');
    const after = engine.inspect(mission.id, { tenantId: '10' });
    assert.ok(after.mission.structuredMissionApproved);
    assert.equal(hasPendingDiscoveryApproval(after), true);
    assert.equal(
      after.contributions.some((row) => row.specialist === amo.SPECIALISTS.SCOUT),
      false
    );
  });

  it('inferBlockers waits for operator plan approval before Scout at discover stage', () => {
    const mission = createMission({
      tenantId: '10',
      objective: STR_OBJECTIVE,
    });
    const blockers = inferBlockers(mission, { scoutComplete: false });
    assert.ok(blockers.some((row) => row.kind === 'waiting_for_operator'));
    assert.equal(blockers.some((row) => row.kind === 'waiting_for_scout'), false);
  });

  it('inferBlockers waits for Scout after plan is locked', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: STR_OBJECTIVE,
    });
    await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved.',
    });
    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    const blockers = inferBlockers(snapshot.mission, { scoutComplete: false });
    assert.equal(blockers.some((row) => row.kind === 'waiting_for_operator'), false);
    assert.ok(blockers.some((row) => row.kind === 'waiting_for_scout'));
  });
});
