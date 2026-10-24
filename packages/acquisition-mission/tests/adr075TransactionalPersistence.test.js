'use strict';

/**
 * ADR-075 — Transactional Persistence Exclusivity.
 * Legacy writers must not mutate durable mission state during TME.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../index');
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  fixtureScoutDiscoveryResult,
} = require('../../max/workspace/AmoOperatorApproval');
const {
  assertPersistedMatchesEngine,
} = require('../../../services/acquisitionMissionPersistence');
const { createAcquisitionMissionRuntime, resetAcquisitionMissionRuntime } = require('../../../services/acquisitionMissionRuntime');
const {
  beginTmeTransaction,
  endTmeTransaction,
  shouldSuppressLegacyDurableWrite,
  resetMissionDurableLocksForTests,
} = require('../TransactionalPersistence');
const { persistMission } = require('../../../services/acquisitionMissionPersistence');

const OBJECTIVE = 'Acquire commercial cleaning customers in Manchester NH for law firms.';

function createAmoMemoryPool() {
  const tables = {
    acquisition_missions: new Map(),
    acquisition_mission_events: new Map(),
    acquisition_mission_contributions: new Map(),
    acquisition_mission_observations: new Map(),
    acquisition_mission_outcomes: new Map(),
    acquisition_mission_execution_audit: new Map(),
  };
  let txnBackup = null;

  function cloneTables() {
    return Object.fromEntries(
      Object.entries(tables).map(([name, map]) => [name, new Map(map)])
    );
  }

  function restoreTables(backup) {
    for (const [name, map] of Object.entries(backup)) {
      tables[name] = map;
    }
  }

  /** @type {Map<string, { key1: number, key2: number }>} */
  const advisoryLocks = new Map();

  async function query(sql, params = []) {
    const trimmed = sql.trim();
    if (/^CREATE TABLE|^CREATE INDEX/i.test(trimmed)) return { rows: [] };
    if (/pg_try_advisory_lock/i.test(trimmed)) {
      const key = `${params[0]}:${params[1]}`;
      if (advisoryLocks.has(key)) return { rows: [{ locked: false }] };
      advisoryLocks.set(key, { key1: params[0], key2: params[1] });
      return { rows: [{ locked: true }] };
    }
    if (/pg_advisory_lock/i.test(trimmed) && !/pg_try_advisory_lock/i.test(trimmed)) {
      const key = `${params[0]}:${params[1]}`;
      if (advisoryLocks.has(key)) {
        throw new Error('pg_advisory_lock: already held');
      }
      advisoryLocks.set(key, { key1: params[0], key2: params[1] });
      return { rows: [{ locked: true }] };
    }
    if (/pg_advisory_unlock/i.test(trimmed)) {
      const key = `${params[0]}:${params[1]}`;
      advisoryLocks.delete(key);
      return { rows: [{ pg_advisory_unlock: true }] };
    }
    if (trimmed === 'BEGIN') {
      txnBackup = cloneTables();
      return { rows: [] };
    }
    if (trimmed === 'COMMIT') {
      txnBackup = null;
      return { rows: [] };
    }
    if (trimmed === 'ROLLBACK') {
      if (txnBackup) restoreTables(txnBackup);
      txnBackup = null;
      return { rows: [] };
    }

    if (/INSERT INTO acquisition_missions/i.test(sql)) {
      const mission = params[14];
      tables.acquisition_missions.set(params[0], {
        id: params[0],
        tenant_id: String(params[1]),
        payload: mission,
        stage: params[3],
        status: params[4],
        updated_at: params[16],
      });
      return { rows: [] };
    }

    if (/INSERT INTO acquisition_mission_events/i.test(sql)) {
      tables.acquisition_mission_events.set(params[0], {
        id: params[0],
        mission_id: params[1],
        tenant_id: String(params[2]),
        payload: params[6],
        kind: params[3],
        specialist: params[4],
        label: params[5],
        at: params[7],
      });
      return { rows: [] };
    }

    if (/INSERT INTO acquisition_mission_contributions/i.test(sql)) {
      const row = params[5];
      tables.acquisition_mission_contributions.set(params[0], {
        id: params[0],
        mission_id: params[1],
        tenant_id: String(params[2]),
        payload: row,
      });
      return { rows: [] };
    }

    if (/INSERT INTO acquisition_mission_observations/i.test(sql)) {
      tables.acquisition_mission_observations.set(params[0], {
        id: params[0],
        mission_id: params[1],
        tenant_id: String(params[2]),
      });
      return { rows: [] };
    }

    if (/INSERT INTO acquisition_mission_outcomes/i.test(sql)) {
      tables.acquisition_mission_outcomes.set(params[0], {
        id: params[0],
        mission_id: params[1],
        tenant_id: String(params[2]),
        payload: params[7],
      });
      return { rows: [] };
    }

    if (/INSERT INTO acquisition_mission_execution_audit/i.test(sql)) {
      tables.acquisition_mission_execution_audit.set(params[0], {
        id: params[0],
        mission_id: params[2],
        tenant_id: params[3] != null ? String(params[3]) : null,
      });
      return { rows: [] };
    }

    if (/SELECT \* FROM acquisition_missions WHERE tenant_id/i.test(sql)) {
      const tenantId = String(params[0]);
      return {
        rows: [...tables.acquisition_missions.values()].filter(
          (row) => String(row.tenant_id) === tenantId
        ),
      };
    }

    if (/SELECT payload FROM acquisition_mission_contributions WHERE tenant_id/i.test(sql)) {
      const tenantId = String(params[0]);
      return {
        rows: [...tables.acquisition_mission_contributions.values()]
          .filter((row) => String(row.tenant_id) === tenantId)
          .map((row) => ({ payload: row.payload })),
      };
    }

    if (/SELECT payload, id, mission_id, kind, specialist, label, at FROM acquisition_mission_events/i.test(sql)) {
      const tenantId = String(params[0]);
      return {
        rows: [...tables.acquisition_mission_events.values()]
          .filter((row) => String(row.tenant_id) === tenantId)
          .map((row) => ({
            id: row.id,
            mission_id: row.mission_id,
            kind: row.kind,
            specialist: row.specialist,
            label: row.label,
            at: row.at,
            payload: row.payload,
          })),
      };
    }

    if (/SELECT id, mission_id, specialist, observation, at FROM acquisition_mission_observations/i.test(sql)) {
      return { rows: [] };
    }

    if (/SELECT payload FROM acquisition_mission_outcomes WHERE tenant_id/i.test(sql)) {
      return { rows: [] };
    }

    if (/SELECT payload FROM acquisition_mission_learning/i.test(sql)) {
      return { rows: [] };
    }

    if (/SELECT payload FROM acquisition_mission_predictions/i.test(sql)) {
      return { rows: [] };
    }

    if (/SELECT payload FROM acquisition_mission_outcome_evaluations/i.test(sql)) {
      return { rows: [] };
    }

    if (/SELECT payload FROM acquisition_mission_outcome_learnings/i.test(sql)) {
      return { rows: [] };
    }

    throw new Error(`Unhandled SQL: ${trimmed.split('\n')[0]}`);
  }

  return {
    query,
    connect: async () => ({ query, release() {} }),
    tables,
    advisoryLocks,
  };
}

describe('ADR-075 — Transactional Persistence Exclusivity', () => {
  beforeEach(() => {
    amo.clearExecutionAudit();
    resetAcquisitionMissionRuntime();
    resetMissionDurableLocksForTests();
  });

  it('shouldSuppressLegacyDurableWrite is true while TME transaction is active', () => {
    assert.equal(shouldSuppressLegacyDurableWrite('m1'), false);
    beginTmeTransaction('m1', 'tme_1');
    assert.equal(shouldSuppressLegacyDurableWrite('m1'), true);
    assert.equal(shouldSuppressLegacyDurableWrite('m2'), false);
    endTmeTransaction('m1', 'tme_1');
    assert.equal(shouldSuppressLegacyDurableWrite('m1'), false);
  });

  it('runtime.persistSideEffects returns suppressed during active TME guard', async () => {
    const runtime = createAcquisitionMissionRuntime({ persist: false });
    const mission = runtime.engine().create({
      tenantId: '10',
      objective: OBJECTIVE,
    });

    beginTmeTransaction(mission.id, 'tme_test');
    const result = await runtime.persistSideEffects(mission.id, { persist: true, pool: {} });
    endTmeTransaction(mission.id, 'tme_test');

    assert.deepEqual(result, { suppressed: true, reason: 'tme_transaction_active' });
  });

  it('plan and discovery TME commits keep contributionIds aligned with durable store', async () => {
    const pool = createAmoMemoryPool();
    const runtime = createAcquisitionMissionRuntime({ pool, persist: true });
    const engine = runtime.engine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });

    await advancePlanAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved.',
      persist: true,
      pool,
    });

    await advanceDiscoveryAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved. Begin Discovery.',
      persist: true,
      pool,
      allowFixtureFallback: true,
      runScout: async () => fixtureScoutDiscoveryResult(),
    });

    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    await assertPersistedMatchesEngine(engine, mission.id, '10', pool);

    const persistedIds = [...pool.tables.acquisition_mission_contributions.values()]
      .filter((row) => row.mission_id === mission.id)
      .map((row) => row.id)
      .sort();
    const memoryIds = snapshot.contributions.map((row) => row.id).sort();
    assert.deepEqual(persistedIds, memoryIds);
  });

  it('runtime.persistMissionState is suppressed when TME guard is active', async () => {
    const pool = createAmoMemoryPool();
    const runtime = createAcquisitionMissionRuntime({ pool, persist: true });
    const mission = runtime.engine().create({
      tenantId: '10',
      objective: OBJECTIVE,
    });

    beginTmeTransaction(mission.id, 'tme_guard_test');
    const before = pool.tables.acquisition_missions.size;
    const result = await runtime.persistMissionState(mission.id, { pool, persist: true });
    endTmeTransaction(mission.id, 'tme_guard_test');

    assert.deepEqual(result, { suppressed: true, reason: 'tme_transaction_active' });
    assert.equal(pool.tables.acquisition_missions.size, before);
  });

  it('rememberMission is in-memory only and does not write durable mission rows', async () => {
    const pool = createAmoMemoryPool();
    const runtime = createAcquisitionMissionRuntime({ pool, persist: true });
    const mission = runtime.engine().create({
      tenantId: '10',
      objective: OBJECTIVE,
    });

    await runtime.rememberMission({ ...mission, title: 'Updated in memory' }, { pool, persist: true });

    assert.equal(pool.tables.acquisition_missions.size, 0);
    assert.equal(runtime.engine().get(mission.id, '10').title, 'Updated in memory');
  });

  it('legacy persistMission rejects direct durable writes outside persistStageCommit', async () => {
    const pool = createAmoMemoryPool();
    const mission = {
      id: 'm_legacy',
      tenantId: '10',
      stage: 'discover',
      status: 'active',
      objective: OBJECTIVE,
      priority: 'normal',
    };

    await assert.rejects(
      () => persistMission(mission, pool),
      (err) => err.code === 'tme_persistence_exclusivity'
    );
  });

  it('concurrent TME attempts reject when global advisory lock is held', async () => {
    const pool = createAmoMemoryPool();
    const runtime = createAcquisitionMissionRuntime({ pool, persist: true });
    const engine = runtime.engine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });

    const { acquireMissionDurableLock, releaseMissionDurableLock } = require('../TransactionalPersistence');
    await acquireMissionDurableLock(mission.id, pool, { tryOnly: true });

    try {
      await assert.rejects(
        () => advancePlanAfterApproval({
          engine,
          mission,
          tenantId: '10',
          question: 'Approved.',
          persist: true,
          pool,
        }),
        (err) => err.code === 'tme_transaction_overlap' || err.cause?.code === 'tme_transaction_overlap'
      );
    } finally {
      await releaseMissionDurableLock(mission.id, pool);
    }
  });

  it('runtime.create persists exclusively through persistStageCommit bundle', async () => {
    const pool = createAmoMemoryPool();
    const runtime = createAcquisitionMissionRuntime({ pool, persist: true });
    const mission = await runtime.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    }, { pool, persist: true });

    assert.equal(pool.tables.acquisition_missions.size, 1);
    const stored = [...pool.tables.acquisition_missions.values()][0];
    assert.equal(stored.id, mission.id);
    assert.equal(stored.payload.objective, OBJECTIVE);
  });
});
