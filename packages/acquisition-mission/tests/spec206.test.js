'use strict';

/**
 * SPEC-206 — Serializable Mission Execution Context.
 * Runtime dependencies must not enter canonical specialist execution payloads.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../index');
const {
  buildMissionExecutionContext,
  buildRuntimeDependencies,
} = require('../MissionExecutionContext');
const {
  buildExecutionInput,
  SPECIALISTS,
  STAGES,
  EXECUTION_STATUSES,
  CONTRIBUTION_KINDS,
} = amo;
const { resetMissionDurableLocksForTests } = require('../TransactionalPersistence');
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
} = require('../../max/workspace/AmoOperatorApproval');
const { runMaxForAmoMission } = require('../../max/workspace/MaxPrioritizationExecutor');

const OBJECTIVE =
  'Acquire commercial cleaning customers in Manchester NH for law firms.';

const RUNTIME_CONSTRUCTOR_NAMES = new Set([
  'Timeout',
  'TimersList',
  'Socket',
  'Pool',
  'Client',
  'AbortController',
]);

function containsRuntimeReference(value, seen = new WeakSet()) {
  if (value == null) return false;
  if (typeof value === 'function') return true;
  if (typeof value !== 'object') return false;
  if (seen.has(value)) return false;
  seen.add(value);

  if (typeof value.then === 'function' && typeof value.catch === 'function') {
    return true;
  }

  const ctorName = value.constructor && value.constructor.name;
  if (ctorName && RUNTIME_CONSTRUCTOR_NAMES.has(ctorName)) {
    return true;
  }

  if (Array.isArray(value)) {
    return value.some((item) => containsRuntimeReference(item, seen));
  }

  return Object.values(value).some((item) => containsRuntimeReference(item, seen));
}

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

describe('SPEC-206 — Serializable Mission Execution Context', () => {
  beforeEach(() => {
    resetMissionDurableLocksForTests();
  });

  async function throughDiscovery(engine, mission) {
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
  }

  it('buildMissionExecutionContext is JSON-serializable and excludes runtime handles', () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });
    const { pool, timer } = createProductionLikePool();

    try {
      const context = buildMissionExecutionContext({
        engine,
        mission,
        tenantId: '10',
        transactionId: 'txn-spec206',
        pool,
      });

      assert.doesNotThrow(() => JSON.stringify(context));
      assert.equal(context.persistence.runtime, 'amo');
      assert.equal(context.persistence.suppressSideEffects, true);
      assert.equal(context.persistence.pool, undefined);
      assert.equal(context.persistence.engine, undefined);
      assert.equal(containsRuntimeReference(context), false);
    } finally {
      clearTimeout(timer);
    }
  });

  it('buildRuntimeDependencies carries pool and engine separately from executionContext', () => {
    const engine = amo.createAcquisitionMissionEngine();
    const { pool, timer } = createProductionLikePool();

    try {
      const runtimeDependencies = buildRuntimeDependencies({ pool, engine });
      assert.equal(runtimeDependencies.pool, pool);
      assert.equal(runtimeDependencies.engine, engine);
    } finally {
      clearTimeout(timer);
    }
  });

  it('buildExecutionInput succeeds when pool is outside executionContext', () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
      planApproved: true,
    });
    const { pool, timer } = createProductionLikePool();

    try {
      const executionContext = buildMissionExecutionContext({
        engine,
        mission,
        tenantId: '10',
        pool,
      });

      const input = buildExecutionInput({
        mission,
        specialist: SPECIALISTS.MAX,
        contributions: [],
        executionContext,
      });

      assert.doesNotThrow(() => JSON.stringify(input.executionContext));
      assert.equal(containsRuntimeReference(input.executionContext), false);
      assert.equal(input.executionContext.persistence.pool, undefined);
      assert.equal(input.executionContext.persistence.engine, undefined);
    } finally {
      clearTimeout(timer);
    }
  });

  it('production-like pool regression: runMaxForAmoMission succeeds with pool outside executionContext', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });
    await throughDiscovery(engine, mission);
    const current = engine.get(mission.id, '10');

    const { pool, timer } = createProductionLikePool();

    try {
      const maxResult = await runMaxForAmoMission(current, {
        engine,
        tenantId: '10',
        pool,
        transactionId: 'txn-spec206-pool',
      });

      assert.equal(maxResult.status, EXECUTION_STATUSES.SUCCESS);
      assert.ok(maxResult.contributions && maxResult.contributions.priorities);
    } finally {
      clearTimeout(timer);
    }
  });

  it('executor receives pool via runtimeDependencies while executionContext stays serializable', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });
    await throughDiscovery(engine, mission);
    const current = engine.get(mission.id, '10');

    const { pool, timer } = createProductionLikePool();
    let observedPool = null;
    let observedExecutionContext = null;

    try {
      await runMaxForAmoMission(current, {
        engine,
        tenantId: '10',
        pool,
        runMax: async (_mission, opts) => {
          observedPool = opts.pool;
          observedExecutionContext = buildMissionExecutionContext({
            engine: opts.engine,
            mission: _mission,
            tenantId: opts.tenantId,
            transactionId: opts.transactionId,
            pool: opts.pool,
          });
          const input = buildExecutionInput({
            mission: _mission,
            specialist: SPECIALISTS.MAX,
            contributions: opts.contributions || [],
            executionContext: observedExecutionContext,
          });
          assert.doesNotThrow(() => JSON.stringify(input.executionContext));
          assert.equal(input.executionContext.persistence.pool, undefined);
          return runMaxForAmoMission(_mission, { ...opts, runMax: undefined });
        },
      });

      assert.equal(observedPool, pool);
      assert.equal(observedExecutionContext.persistence.pool, undefined);
    } finally {
      clearTimeout(timer);
    }
  });

  it('TME prioritization approval path succeeds with production-like pool', async () => {
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });
    const { pool, timer } = createProductionLikePool();

    try {
      await throughDiscovery(engine, mission);

      const snapshotBefore = engine.inspect(mission.id, { tenantId: '10' });
      assert.equal(snapshotBefore.mission.stage, STAGES.DISCOVER);
      assert.equal(snapshotBefore.mission.pendingOperatorDecision.kind, 'prioritization_approval');

      const result = await advancePrioritizationAfterApproval({
        engine,
        mission,
        tenantId: '10',
        pool,
        persist: false,
        question: 'Approved prioritization.',
      });

      assert.equal(result.alreadyExecuted, false);
      assert.equal(result.prioritization.specialist, SPECIALISTS.MAX);
      assert.equal(result.prioritization.kind, CONTRIBUTION_KINDS.PRIORITIZATION);
      assert.equal(result.maxResult.status, EXECUTION_STATUSES.SUCCESS);

      const snapshotAfter = engine.inspect(mission.id, { tenantId: '10' });
      assert.equal(snapshotAfter.mission.stage, STAGES.UNDERSTAND);
      assert.equal(snapshotAfter.mission.pendingOperatorDecision, null);
    } finally {
      clearTimeout(timer);
    }
  });
});
