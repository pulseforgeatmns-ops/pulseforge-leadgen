'use strict';

/**
 * SPEC-139 — Transactional Durable Mission Persistence.
 * Memory commit must equal durable commit must equal future hydration.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../index');
const { advancePlanAfterApproval, advanceDiscoveryAfterApproval, advancePrioritizationAfterApproval } = require('../../max/workspace/AmoOperatorApproval');
const {
  bindStagePersistDurable,
  assertPersistedMatchesEngine,
} = require('../../../services/acquisitionMissionPersistence');
const { hydrateTenant, resetEngine } = require('../../../services/acquisitionMission');
const { TME_CLASSES, isRolledBackExecution } = amo;

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

  async function query(sql, params = []) {
    const trimmed = sql.trim();
    if (/^CREATE TABLE|^CREATE INDEX/i.test(trimmed)) {
      return { rows: [] };
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
      const id = params[0];
      tables.acquisition_missions.set(id, {
        id,
        tenant_id: String(params[1]),
        client_id: params[2],
        stage: params[3],
        status: params[4],
        objective: params[5],
        target_segment: params[6],
        campaign: params[7],
        title: params[8],
        priority: params[9],
        confidence: params[10],
        owner: params[11],
        created_by: params[12],
        orchestration_mission_id: params[13],
        payload: mission,
        created_at: params[15],
        updated_at: params[16],
      });
      return { rows: [] };
    }

    if (/INSERT INTO acquisition_mission_events/i.test(sql)) {
      const event = params[6];
      tables.acquisition_mission_events.set(params[0], {
        id: params[0],
        mission_id: params[1],
        tenant_id: String(params[2]),
        kind: params[3],
        specialist: params[4],
        label: params[5],
        payload: event,
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
        specialist: params[3],
        kind: params[4],
        payload: row,
        at: params[6],
      });
      return { rows: [] };
    }

    if (/INSERT INTO acquisition_mission_observations/i.test(sql)) {
      tables.acquisition_mission_observations.set(params[0], {
        id: params[0],
        mission_id: params[1],
        tenant_id: String(params[2]),
        specialist: params[3],
        observation: params[4],
        at: params[5],
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
        transaction_id: params[1],
        mission_id: params[2],
        tenant_id: params[3] != null ? String(params[3]) : null,
        payload: params[13],
      });
      return { rows: [] };
    }

    if (/SELECT \* FROM acquisition_missions WHERE tenant_id/i.test(sql)) {
      const tenantId = String(params[0]);
      const rows = [...tables.acquisition_missions.values()].filter(
        (row) => String(row.tenant_id) === tenantId
      );
      return { rows };
    }

    if (/SELECT payload FROM acquisition_mission_contributions WHERE tenant_id/i.test(sql)) {
      const tenantId = String(params[0]);
      const rows = [...tables.acquisition_mission_contributions.values()]
        .filter((row) => String(row.tenant_id) === tenantId)
        .map((row) => ({ payload: row.payload }));
      return { rows };
    }

    if (/SELECT payload, id, mission_id, kind, specialist, label, at FROM acquisition_mission_events/i.test(sql)) {
      const tenantId = String(params[0]);
      const rows = [...tables.acquisition_mission_events.values()]
        .filter((row) => String(row.tenant_id) === tenantId)
        .map((row) => ({
          id: row.id,
          mission_id: row.mission_id,
          kind: row.kind,
          specialist: row.specialist,
          label: row.label,
          at: row.at,
          payload: row.payload,
        }));
      return { rows };
    }

    if (/SELECT id, mission_id, specialist, observation, at FROM acquisition_mission_observations/i.test(sql)) {
      const tenantId = String(params[0]);
      const rows = [...tables.acquisition_mission_observations.values()].filter(
        (row) => String(row.tenant_id) === tenantId
      );
      return { rows };
    }

    if (/SELECT payload FROM acquisition_mission_outcomes WHERE tenant_id/i.test(sql)) {
      const tenantId = String(params[0]);
      const rows = [...tables.acquisition_mission_outcomes.values()]
        .filter((row) => String(row.tenant_id) === tenantId)
        .map((row) => ({ payload: row.payload }));
      return { rows };
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

    throw new Error(`Unhandled SQL in amo memory pool: ${trimmed.split('\n')[0]}`);
  }

  const client = {
    query,
    release() {},
  };

  return {
    query,
    connect: async () => client,
    tables,
  };
}

describe('SPEC-139 — Transactional Durable Mission Persistence', () => {
  beforeEach(() => {
    amo.clearExecutionAudit();
    resetEngine();
  });

  it('bindStagePersistDurable returns null when persist is false', () => {
    const engine = amo.createAcquisitionMissionEngine();
    const bound = bindStagePersistDurable({ persist: false, pool: {} }, engine, '10');
    assert.equal(bound, null);
  });

  it('bindStagePersistDurable returns null when pool is missing', () => {
    const engine = amo.createAcquisitionMissionEngine();
    const bound = bindStagePersistDurable({ persist: true }, engine, '10');
    assert.equal(bound, null);
  });

  it('plan approval persists and survives hydration', async () => {
    const pool = createAmoMemoryPool();
    const engine = amo.createAcquisitionMissionEngine();
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

    const inMemory = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(inMemory.mission.structuredMissionApproved, true);
    assert.ok(inMemory.mission.structuredMission);
    assert.equal(inMemory.mission.missionPlanDraft, null);
    assert.equal(inMemory.mission.pendingOperatorDecision.kind, amo.OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL);

    resetEngine();
    await hydrateTenant('10', { pool });
    const { getEngine } = require('../../../services/acquisitionMission');
    const hydrated = getEngine().inspect(mission.id, { tenantId: '10' });
    assert.equal(hydrated.mission.structuredMissionApproved, true);
    assert.ok(hydrated.mission.structuredMission);
    assert.equal(hydrated.mission.missionPlanDraft, null);
    assert.equal(hydrated.mission.pendingOperatorDecision.kind, amo.OPERATOR_DECISION_KINDS.DISCOVERY_APPROVAL);
    assert.equal(hydrated.mission.version, inMemory.mission.version);
  });

  it('discovery approval persists and survives hydration', async () => {
    const pool = createAmoMemoryPool();
    const engine = amo.createAcquisitionMissionEngine();
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
    });

    const inMemory = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(inMemory.mission.stage, amo.STAGES.DISCOVER);
    assert.equal(inMemory.mission.pendingOperatorDecision.kind, amo.OPERATOR_DECISION_KINDS.PRIORITIZATION_APPROVAL);
    assert.ok(inMemory.contributions.some((row) => row.specialist === amo.SPECIALISTS.SCOUT));

    await advancePrioritizationAfterApproval({
      engine,
      mission,
      tenantId: '10',
      question: 'Approved prioritization.',
      persist: true,
      pool,
    });

    const inMemoryAfterPrioritization = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(inMemoryAfterPrioritization.mission.stage, amo.STAGES.UNDERSTAND);

    resetEngine();
    await hydrateTenant('10', { pool });
    const { getEngine } = require('../../../services/acquisitionMission');
    const hydrated = getEngine().inspect(mission.id, { tenantId: '10' });
    assert.equal(hydrated.mission.stage, amo.STAGES.UNDERSTAND);
    assert.ok(hydrated.contributions.some((row) => row.specialist === amo.SPECIALISTS.SCOUT));
    assert.equal(hydrated.mission.version, inMemoryAfterPrioritization.mission.version);
  });

  it('persistence failure rolls back the in-memory commit', async () => {
    const pool = createAmoMemoryPool();
    const engine = amo.createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });
    const before = engine.inspect(mission.id, { tenantId: '10' });

    await assert.rejects(
      () => advancePlanAfterApproval({
        engine,
        mission,
        tenantId: '10',
        question: 'Approved.',
        persist: true,
        pool,
        persistStage: async () => {
          throw new Error('durable write failed');
        },
      }),
      (err) => {
        assert.equal(isRolledBackExecution(err), true);
        assert.equal(err.tmeClass, TME_CLASSES.PERSISTENCE);
        return true;
      }
    );

    const after = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(after.mission.structuredMissionApproved, before.mission.structuredMissionApproved);
    assert.equal(after.mission.version, before.mission.version);
    assert.deepEqual(after.mission.pendingOperatorDecision, before.mission.pendingOperatorDecision);
  });

  it('assertPersistedMatchesEngine rejects mismatched durable snapshots', async () => {
    const pool = createAmoMemoryPool();
    const engine = amo.createAcquisitionMissionEngine();
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

    const stored = engine.store.getMission(mission.id);
    stored.structuredMissionApproved = false;
    engine.store.putMission(stored);

    await assert.rejects(
      () => assertPersistedMatchesEngine(engine, mission.id, '10', pool),
      (err) => err.code === 'tme_persistence_verify'
    );
  });
});
