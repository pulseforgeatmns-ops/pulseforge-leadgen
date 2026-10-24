'use strict';
const { createTestAmoRuntime, runtimeProviderFromEngine, createHydratingTestRuntime } = require('./amoTestRuntime');

/**
 * SPEC-131 — Operator approval is consumed only when the stage commits.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../../../acquisition-mission');
const {
  advanceDiscoveryAfterApproval,
  advancePlanAfterApproval,
  findDiscoveryApproval,
  hasPendingDiscoveryApproval,
} = require('../AmoOperatorApproval');
const { maybeHandleAcquisitionMissionExecution } = require('../AcquisitionMissionExecution');
const {
  clearExecutionAudit,
  listExecutionAudit,
  isRolledBackExecution,
  TME_CLASSES,
} = amo;

const ANCHOR_OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

describe('SPEC-131 — approval consumption is part of the commit', () => {
  let engine;
  let mission;

  beforeEach(() => {
    clearExecutionAudit();
    engine = amo.createAcquisitionMissionEngine();
    mission = engine.create({
      tenantId: '10',
      objective: ANCHOR_OBJECTIVE,
      targetSegment: 'Law Firms',
    });
  });

  async function approvePlan() {
    await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved.',
    });
  }

  it('never leaves approval consumed when Scout throws', async () => {
    await approvePlan();
    const before = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(hasPendingDiscoveryApproval(before), true);
    assert.equal(findDiscoveryApproval(before.contributions), undefined);

    await assert.rejects(
      () => advanceDiscoveryAfterApproval({
        engine,
        mission,
        tenantId: '10',
        question: 'Approved. Begin Discovery.',
        allowFixtureFallback: false,
        runScout: async () => {
          throw new Error('scout crashed');
        },
      }),
      (err) => {
        assert.equal(isRolledBackExecution(err), true);
        assert.equal(err.tmeClass, TME_CLASSES.SPECIALIST);
        return true;
      }
    );

    const after = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(hasPendingDiscoveryApproval(after), true);
    assert.equal(findDiscoveryApproval(after.contributions), undefined);
    assert.equal(
      after.contributions.some((row) => row.specialist === amo.SPECIALISTS.SCOUT),
      false
    );
    assert.equal(after.mission.status, before.mission.status);
    assert.equal(after.mission.stage, before.mission.stage);
    assert.equal(after.mission.version, before.mission.version);
    assert.deepEqual(after.mission.pendingOperatorDecision, before.mission.pendingOperatorDecision);
    assert.notEqual(after.mission.status, 'Discovery Executing');

    const audit = listExecutionAudit({ missionId: mission.id, commitStatus: 'rolled_back' });
    assert.equal(audit.length, 1);
    assert.equal(audit[0].errorClass, TME_CLASSES.SPECIALIST);
  });

  it('consumes approval only after Scout succeeds', async () => {
    await approvePlan();
    const result = await advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: true,
    });

    assert.equal(result.alreadyExecuted, false);
    assert.equal(result.approval.payload.consumed, true);
    assert.ok(result.transactionId);
    assert.equal(result.approval.payload.transactionId, result.transactionId);
    const after = engine.inspect(mission.id, { tenantId: '10' });
    assert.ok(findDiscoveryApproval(after.contributions));
    assert.ok(after.contributions.some((row) => row.specialist === amo.SPECIALISTS.SCOUT));
    assert.ok(after.mission.version >= 1);
    assert.ok(
      after.timeline.some((row) => row.kind === amo.EVENT_KINDS.EXECUTION_COMMITTED)
    );
  });

  it('discovery without an approved plan does not mutate the mission', async () => {
    const before = engine.inspect(mission.id, { tenantId: '10' });
    await assert.rejects(
      () => advanceDiscoveryAfterApproval({
        engine,
        mission,
        tenantId: '10',
        question: 'Approved. Begin Discovery.',
        allowFixtureFallback: true,
      }),
      (err) => {
        assert.equal(err.tmeClass, TME_CLASSES.PLANNING);
        assert.match(err.message, /Mission Plan missing/);
        return true;
      }
    );
    const after = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(after.contributions.length, before.contributions.length);
    assert.deepEqual(after.mission.pendingOperatorDecision, before.mission.pendingOperatorDecision);
  });

  it('workspace recovery prose keeps the pending discovery decision', async () => {
    await approvePlan();
    const turn = await maybeHandleAcquisitionMissionExecution({
      question: 'Approved. Begin Discovery.',
      context: { tenantId: '10', missionId: mission.id },
      acquisitionMissionRuntime: createTestAmoRuntime({ engine: engine }),
      allowFixtureFallback: false,
      runScout: async () => {
        throw new Error('scout crashed');
      },
    });

    assert.ok(turn);
    assert.equal(turn.executionResult.rolledBack, true);
    assert.match(turn.prose, /Discovery could not execute/);
    assert.match(turn.prose, /Mission remains unchanged/);
    const after = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(hasPendingDiscoveryApproval(after), true);
    assert.equal(findDiscoveryApproval(after.contributions), undefined);
  });
});
