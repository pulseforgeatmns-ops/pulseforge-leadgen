'use strict';

/**
 * SPEC-206 — Acceptance: prioritization approval must not hit circular JSON serialization.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../../../acquisition-mission');
const { STAGES, OPERATOR_DECISION_KINDS } = amo;
const {
  maybeHandleAcquisitionMissionExecution,
} = require('../AcquisitionMissionExecution');
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
} = require('../AmoOperatorApproval');
const { createTestAmoRuntime } = require('./amoTestRuntime');
const { resetMissionDurableLocksForTests } = require('../../../acquisition-mission/TransactionalPersistence');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

function createProductionLikePool() {
  const pool = {
    async query() {
      return { rows: [] };
    },
    async connect() {
      return this;
    },
    release() {},
  };
  const timer = setTimeout(() => {}, 100_000);
  pool._timer = timer;
  timer._pool = pool;
  return { pool, timer };
}

describe('SPEC-206 — prioritization approval acceptance', () => {
  let engine;
  let mission;

  beforeEach(() => {
    resetMissionDurableLocksForTests();
    engine = amo.createAcquisitionMissionEngine();
    mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });
  });

  it('approved prioritization does not throw circular JSON and advances Max execution', async () => {
    await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved.',
    });
    await advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      allowFixtureFallback: true,
    });

    const before = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(before.mission.stage, STAGES.DISCOVER);
    assert.equal(before.mission.pendingOperatorDecision.kind, OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL);

    const { pool, timer } = createProductionLikePool();

    try {
      const turn = await maybeHandleAcquisitionMissionExecution({
        question: 'approved',
        context: { tenantId: '10', missionId: mission.id },
        acquisitionMissionRuntime: createTestAmoRuntime({ engine, pool }),
        pool,
        persist: false,
        allowFixtureFallback: true,
      });

      assert.ok(turn);
      assert.notEqual(turn.prose, null);
      assert.doesNotMatch(String(turn.prose), /Converting circular structure to JSON/i);
      assert.equal(turn.action, 'prioritization_approved');

      const after = engine.inspect(mission.id, { tenantId: '10' });
      assert.equal(after.mission.stage, STAGES.UNDERSTAND);
      assert.equal(after.mission.pendingOperatorDecision, null);
    } finally {
      clearTimeout(timer);
    }
  });

  it('prioritization rollback copy uses Prioritization label', () => {
    const { buildExecutionMissionResponse } = require('../AcquisitionMissionExecution');
    const response = buildExecutionMissionResponse({
      mission: {
        id: 'mission_test',
        title: 'Test Mission',
        objective: OBJECTIVE,
        stage: STAGES.DISCOVER,
        pendingOperatorDecision: {
          kind: OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL,
          prompt: 'Approve prioritization?',
        },
      },
      snapshot: { contributions: [] },
      action: 'prioritization_approved',
      question: 'approved',
      executionResult: {
        rolledBack: true,
        error: {
          message: 'Max blocked.',
          tmeClass: 'specialist',
        },
      },
    });

    assert.match(response.prose, /Prioritization could not execute/);
    assert.doesNotMatch(response.prose, /Discovery could not execute/);
    assert.equal(response.comm.operatorDecision, 'Approve prioritization?');
  });
});
