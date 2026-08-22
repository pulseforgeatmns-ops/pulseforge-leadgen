'use strict';

/**
 * SPEC-138 — AMO Runtime Production Reset.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../index');
const { RUNTIME_VERSION, createMission, createAcquisitionMissionEngine } = amo;
const {
  deleteAllAmoData,
  countAmoRows,
} = require('../../../services/acquisitionMissionPersistence');
const {
  resetEngine,
  resetAmoRuntime,
  createMission: createServiceMission,
  listMissions,
} = require('../../../services/acquisitionMission');
const { clearAmoHydrationCache } = require('../../max/workspace/AmoWorkspaceHydration');

function mockPool() {
  const counts = {
    acquisition_mission_execution_audit: 2,
    acquisition_mission_learning: 1,
    acquisition_mission_outcomes: 3,
    acquisition_mission_observations: 4,
    acquisition_mission_contributions: 5,
    acquisition_mission_events: 6,
    acquisition_missions: 1,
  };
  const queries = [];
  return {
    queries,
    async query(sql, params) {
      queries.push({ sql, params });
      if (/SELECT COUNT\(\*\)/i.test(sql)) {
        const table = sql.match(/FROM (\w+)/i)[1];
        const tenantClause = /tenant_id = \$1/i.test(sql);
        if (tenantClause && params && params[0] === '99') {
          return { rows: [{ count: 0 }] };
        }
        return { rows: [{ count: counts[table] || 0 }] };
      }
      if (/^DELETE FROM/i.test(sql.trim())) {
        const table = sql.match(/DELETE FROM (\w+)/i)[1];
        if (/tenant_id = \$1/i.test(sql)) {
          if (params && params[0] === '99') counts[table] = 0;
        } else {
          counts[table] = 0;
        }
        return { rowCount: 1 };
      }
      if (/^BEGIN|^COMMIT|^ROLLBACK/i.test(sql.trim())) {
        return { rows: [] };
      }
      if (/CREATE TABLE/i.test(sql)) {
        return { rows: [] };
      }
      return { rows: [], rowCount: 0 };
    },
    connect() {
      return Promise.resolve(this);
    },
    release() {},
  };
}

describe('SPEC-138 — runtime version', () => {
  it('stamps runtimeVersion on newly created missions', () => {
    const mission = createMission({
      tenantId: '10',
      objective: 'Acquire commercial cleaning customers in Manchester NH.',
      skipMissionPlanning: true,
    });
    assert.equal(mission.runtimeVersion, RUNTIME_VERSION);
    assert.equal(RUNTIME_VERSION, 1);
  });

  it('allows explicit runtimeVersion override for migration tests', () => {
    const mission = createMission({
      tenantId: '10',
      objective: 'Test objective.',
      runtimeVersion: 2,
      skipMissionPlanning: true,
    });
    assert.equal(mission.runtimeVersion, 2);
  });
});

describe('SPEC-138 — production reset', () => {
  beforeEach(() => {
    resetEngine();
    clearAmoHydrationCache();
  });

  it('deleteAllAmoData clears all AMO tables in FK-safe order', async () => {
    const pool = mockPool();
    const result = await deleteAllAmoData(null, pool);
    assert.ok(result.before.acquisition_missions >= 1);
    assert.equal(result.after.acquisition_missions, 0);
    assert.equal(result.after.acquisition_mission_events, 0);
    const deleteQueries = pool.queries.filter((q) => /^DELETE FROM/i.test(q.sql.trim()));
    assert.equal(deleteQueries.length, 7);
    const tables = deleteQueries.map((q) => q.sql.match(/DELETE FROM (\w+)/i)[1]);
    assert.deepEqual(tables, [
      'acquisition_mission_execution_audit',
      'acquisition_mission_learning',
      'acquisition_mission_outcomes',
      'acquisition_mission_observations',
      'acquisition_mission_contributions',
      'acquisition_mission_events',
      'acquisition_missions',
    ]);
  });

  it('deleteAllAmoData scopes by tenant when tenantId is provided', async () => {
    const pool = mockPool();
    await deleteAllAmoData('99', pool);
    const deleteQueries = pool.queries.filter((q) => /DELETE FROM acquisition_missions/i.test(q.sql));
    assert.ok(deleteQueries.every((q) => /tenant_id = \$1/i.test(q.sql)));
    assert.deepEqual(deleteQueries[0].params, ['99']);
  });

  it('resetAmoRuntime clears in-memory engine after DB purge', async () => {
    const pool = mockPool();
    await createServiceMission({
      tenantId: '10',
      objective: 'Legacy mission to purge.',
      skipMissionPlanning: true,
    }, { persist: false });
    assert.equal(getEngineMissionCount('10'), 1);

    const result = await resetAmoRuntime({ pool, clearSessions: false });
    assert.equal(result.spec, 'SPEC-138');
    assert.equal(result.runtimeVersion, RUNTIME_VERSION);
    assert.equal(result.missionsRemaining, 0);
    assert.equal(getEngineMissionCount('10'), 0);
  });

  it('hydration after reset returns empty mission list', async () => {
    const pool = mockPool();
    await resetAmoRuntime({ pool, clearSessions: false });
    const missions = await listMissions('10', { pool, persist: false });
    assert.deepEqual(missions, []);
  });

  it('new missions after reset carry runtimeVersion 1', async () => {
    const pool = mockPool();
    await resetAmoRuntime({ pool, clearSessions: false });
    const mission = await createServiceMission({
      tenantId: '10',
      objective: 'First production mission after reset.',
      skipMissionPlanning: true,
    }, { pool, persist: false });
    assert.equal(mission.runtimeVersion, 1);
    assert.equal(mission.spec, 'SPEC-118');
    assert.ok(mission.pendingOperatorDecision == null || typeof mission.pendingOperatorDecision === 'object');
  });
});

function getEngineMissionCount(tenantId) {
  const { getEngine } = require('../../../services/acquisitionMission');
  return getEngine().list(tenantId).length;
}
