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
  advanceDiscoveryAfterApproval,
  findDiscoveryApproval,
  findScoutDiscoveryAfterApproval,
} = require('../AmoOperatorApproval');

const ANCHOR_OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester for law firms.';

describe('SPEC-128 — Operator Approval Must Advance Stage', () => {
  let engine;
  let mission;

  beforeEach(() => {
    engine = amo.createAcquisitionMissionEngine();
    mission = engine.create({
      tenantId: '10',
      objective: ANCHOR_OBJECTIVE,
      targetSegment: 'Law Firms',
    });
  });

  it('creates discover missions with a pending operator decision', () => {
    assert.equal(mission.stage, 'discover');
    assert.ok(mission.pendingOperatorDecision);
    assert.equal(mission.pendingOperatorDecision.prompt, 'Approve discovery?');
  });

  it('consumes approval and executes discovery exactly once', async () => {
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
    const turn = await maybeHandleAcquisitionMissionExecution({
      question: 'Approved. Begin Discovery.',
      context: { tenantId: '10', missionId: mission.id },
      acquisitionMissionEngine: engine,
      allowFixtureFallback: true,
    });

    assert.ok(turn);
    assert.match(turn.prose, /Mission Updated/i);
    assert.match(turn.prose, /Scout Discovery completed/i);
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
});
