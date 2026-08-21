'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../../../acquisition-mission');
const {
  maybeHandleAcquisitionMissionExecution,
  shouldExecuteDiscovery,
} = require('../AcquisitionMissionExecution');
const {
  hasPendingDiscoveryApproval,
  hasPendingPlanApproval,
  advanceDiscoveryAfterApproval,
  advancePlanAfterApproval,
  findDiscoveryApproval,
  findScoutDiscoveryAfterApproval,
  APPROVAL_PHASES,
} = require('../AmoOperatorApproval');
const {
  createMissionApprovalAudit,
  clearMissionApprovalAuditLog,
  listMissionApprovalAuditLog,
} = require('../audit/MissionApprovalAudit');

const ANCHOR_OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

describe('SPEC-128 — Operator Approval Must Advance Stage', () => {
  let engine;
  let mission;

  beforeEach(() => {
    clearMissionApprovalAuditLog();
    engine = amo.createAcquisitionMissionEngine();
    mission = engine.create({
      tenantId: '10',
      objective: ANCHOR_OBJECTIVE,
      targetSegment: 'Law Firms',
    });
  });

  it('creates discover missions with a pending plan approval decision', () => {
    assert.equal(mission.stage, 'discover');
    assert.ok(mission.pendingOperatorDecision);
    assert.equal(mission.pendingOperatorDecision.prompt, 'Approve mission plan?');
  });

  async function approvePlan() {
    await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved.',
    });
  }

  it('consumes approval and executes discovery exactly once', async () => {
    await approvePlan();
    const snapshotBefore = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(hasPendingDiscoveryApproval(snapshotBefore), true);
    assert.equal(shouldExecuteDiscovery('discovery_approved', snapshotBefore), true);

    const first = await advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: true,
    });

    assert.equal(first.alreadyExecuted, false);
    assert.ok(first.approval);
    assert.equal(first.approval.payload.consumed, true);
    assert.ok(first.discovery);
    assert.equal(first.executionOutcome, 'completed');

    const snapshotAfter = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(hasPendingDiscoveryApproval(snapshotAfter), false);
    assert.equal(snapshotAfter.mission.pendingOperatorDecision, null);
    assert.ok(findDiscoveryApproval(snapshotAfter.contributions));
    assert.ok(findScoutDiscoveryAfterApproval(snapshotAfter.contributions, first.approval));

    const second = await advanceDiscoveryAfterApproval({
      engine,
      mission: snapshotAfter.mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: true,
    });

    assert.equal(second.alreadyExecuted, true);
    assert.equal(
      findScoutDiscoveryAfterApproval(snapshotAfter.contributions, first.approval).id,
      second.discovery.id
    );
  });

  it('maybeHandleAcquisitionMissionExecution returns Mission Updated without repeating Approve discovery', async () => {
    await approvePlan();
    const turn = await maybeHandleAcquisitionMissionExecution({
      question: 'Approved. Begin Discovery.',
      context: { tenantId: '10', missionId: mission.id },
      acquisitionMissionEngine: engine,
      allowFixtureFallback: true,
    });

    assert.ok(turn);
    assert.match(turn.prose, /Mission Updated/i);
    assert.match(turn.prose, /Scout Discovery/i);
    assert.doesNotMatch(turn.prose, /Approve discovery\?/i);
    assert.equal(turn.executionResult.executionOutcome, 'completed');

    const repeat = await maybeHandleAcquisitionMissionExecution({
      question: 'Approved. Begin Discovery.',
      context: { tenantId: '10', missionId: mission.id },
      acquisitionMissionEngine: engine,
      allowFixtureFallback: true,
    });

    assert.ok(repeat);
    assert.doesNotMatch(repeat.prose, /Approve discovery\?/i);
    if (repeat.executionResult) {
      assert.equal(repeat.executionResult.alreadyExecuted, true);
    }
  });

  it('advances mission after successful discovery', async () => {
    await approvePlan();
    await advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: true,
    });

    const updated = engine.get(mission.id, '10');
    assert.equal(updated.stage, 'understand');
    assert.equal(updated.pendingOperatorDecision, null);
  });

  it('emits SPEC-128 audit events through approval consumption lifecycle', async () => {
    const audit = createMissionApprovalAudit();
    await approvePlan();

    await advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: true,
      audit,
    });

    const events = audit.list().map((row) => row.event);
    assert.ok(events.includes('MISSION_APPROVAL_RECEIVED'));
    assert.ok(events.includes('MISSION_APPROVAL_CONSUMED'));
    assert.ok(events.includes('MISSION_STAGE_EXECUTION_STARTED'));
    assert.ok(events.includes('MISSION_STAGE_EXECUTION_COMPLETED'));

    const completed = audit.list().find((row) => row.event === 'MISSION_STAGE_EXECUTION_COMPLETED');
    assert.equal(completed.phase, APPROVAL_PHASES.WAITING_FOR_NEXT_DECISION);
    assert.equal(completed.outcome, 'completed');
  });

  it('maybeHandleAcquisitionMissionExecution emits MISSION_APPROVAL_MATCHED and clears waiting state', async () => {
    const audit = createMissionApprovalAudit();
    await approvePlan();

    const turn = await maybeHandleAcquisitionMissionExecution({
      question: 'Approved. Begin Discovery.',
      context: { tenantId: '10', missionId: mission.id },
      acquisitionMissionEngine: engine,
      allowFixtureFallback: true,
      audit,
    });

    assert.ok(turn);
    assert.match(turn.prose, /Mission Updated/i);
    assert.match(turn.prose, /Scout Discovery/i);
    assert.match(turn.prose, /Harbor Law Group/);
    assert.doesNotMatch(turn.prose, /Waiting On[\s\S]*Operator direction/i);
    assert.match(turn.prose, /Waiting On[\s\S]*Prioritization approval/i);
    assert.match(turn.prose, /Approve prioritization\?/);
    assert.equal(turn.executionResult.approvalPhase, APPROVAL_PHASES.WAITING_FOR_NEXT_DECISION);

    const events = audit.list().map((row) => row.event);
    assert.ok(events.includes('MISSION_APPROVAL_MATCHED'));
    assert.ok(events.includes('MISSION_APPROVAL_CONSUMED'));
    assert.ok(events.includes('MISSION_STAGE_EXECUTION_COMPLETED'));

    const globalEvents = listMissionApprovalAuditLog().map((row) => row.event);
    assert.ok(globalEvents.includes('MISSION_APPROVAL_RECEIVED'));
  });
});
