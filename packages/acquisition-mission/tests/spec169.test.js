'use strict';

/**
 * SPEC-169 — Canonical Mission Verification acceptance tests.
 * Persist what you verify. Verify what you persist.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../index');
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
} = require('../../max/workspace/AmoOperatorApproval');
const {
  assertPersistedMatchesEngine,
  loadMissionSnapshot,
} = require('../../../services/acquisitionMissionPersistence');
const { resetEngine } = require('../../../services/acquisitionMission');
const {
  CANONICAL_PROJECTION_KEYS,
  buildCanonicalMissionProjection,
  snapshotFromEngine,
  diffCanonicalMissionProjections,
} = amo;

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
    if (/^CREATE TABLE|^CREATE INDEX/i.test(trimmed)) return { rows: [] };
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

    if (/SELECT payload FROM acquisition_mission_learning/i.test(sql)) return { rows: [] };
    if (/SELECT payload FROM acquisition_mission_predictions/i.test(sql)) return { rows: [] };
    if (/SELECT payload FROM acquisition_mission_outcome_evaluations/i.test(sql)) return { rows: [] };
    if (/SELECT payload FROM acquisition_mission_outcome_learnings/i.test(sql)) return { rows: [] };

    throw new Error(`Unhandled SQL in amo memory pool: ${trimmed.split('\n')[0]}`);
  }

  const client = { query, release() {} };
  return { query, connect: async () => client, tables };
}

async function assertCanonicalMatch(engine, missionId, tenantId, pool) {
  const memorySnap = snapshotFromEngine(engine, missionId, tenantId);
  const persisted = await loadMissionSnapshot(missionId, tenantId, pool);
  const memoryProjection = buildCanonicalMissionProjection(memorySnap);
  const persistedProjection = buildCanonicalMissionProjection(persisted);
  const diff = diffCanonicalMissionProjections(memoryProjection, persistedProjection);
  assert.equal(diff.equal, true, diff.firstDivergence && JSON.stringify(diff.firstDivergence));
  for (const key of CANONICAL_PROJECTION_KEYS) {
    assert.ok(Object.prototype.hasOwnProperty.call(memoryProjection, key), `missing ${key}`);
  }
  return { memoryProjection, persistedProjection };
}

describe('SPEC-169 — Canonical Mission Verification', () => {
  beforeEach(() => {
    amo.clearExecutionAudit();
    resetEngine();
  });

  describe('Scenario 1 — Mission Plan approval', () => {
    it('canonical projections are identical after plan approval', async () => {
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

      const { memoryProjection } = await assertCanonicalMatch(engine, mission.id, '10', pool);
      assert.ok(memoryProjection.structuredMission);
      assert.ok(memoryProjection.pendingOperatorDecision);
      assert.ok(Array.isArray(memoryProjection.events));
      assert.ok(memoryProjection.events.length > 0);
      assert.equal(
        Object.prototype.hasOwnProperty.call(memoryProjection, 'resolvedObjective'),
        true
      );
      assert.equal(
        Object.prototype.hasOwnProperty.call(memoryProjection, 'executionPolicy'),
        true
      );
    });
  });

  describe('Scenario 2 — Discovery approval', () => {
    it('canonical projections are identical after discovery approval', async () => {
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

      const { memoryProjection } = await assertCanonicalMatch(engine, mission.id, '10', pool);
      assert.ok(memoryProjection.contributions.some((row) => row.specialist === amo.SPECIALISTS.SCOUT));
      assert.ok(memoryProjection.events.length > 0);
    });
  });

  describe('Scenario 3 — Prioritization approval', () => {
    it('canonical projections are identical after prioritization approval', async () => {
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
      await advancePrioritizationAfterApproval({
        engine,
        mission,
        tenantId: '10',
        question: 'Approved prioritization.',
        persist: true,
        pool,
      });

      const { memoryProjection } = await assertCanonicalMatch(engine, mission.id, '10', pool);
      assert.equal(memoryProjection.mission.stage, amo.STAGES.UNDERSTAND);
    });
  });

  describe('Scenario 4 — Future mission fields participate automatically', () => {
    it('organizationalPlan is verified through the canonical projection without comparator changes', () => {
      const memory = buildCanonicalMissionProjection({
        mission: {
          id: 'mission_future',
          stage: 'discover',
          objective: OBJECTIVE,
          organizationalPlan: { owner: 'max', cadence: 'weekly' },
        },
        contributions: [],
        events: [],
        observations: [],
        outcomes: [],
      });
      const persisted = buildCanonicalMissionProjection({
        mission: {
          id: 'mission_future',
          stage: 'discover',
          objective: OBJECTIVE,
          organizationalPlan: { owner: 'max', cadence: 'weekly' },
        },
        contributions: [],
        events: [],
        observations: [],
        outcomes: [],
      });

      assert.deepEqual(memory.mission.organizationalPlan, { cadence: 'weekly', owner: 'max' });
      assert.equal(diffCanonicalMissionProjections(memory, persisted).equal, true);

      const drifted = buildCanonicalMissionProjection({
        mission: {
          id: 'mission_future',
          stage: 'discover',
          objective: OBJECTIVE,
          organizationalPlan: { owner: 'operator', cadence: 'weekly' },
        },
        contributions: [],
        events: [],
        observations: [],
        outcomes: [],
      });
      const mismatch = diffCanonicalMissionProjections(memory, drifted);
      assert.equal(mismatch.equal, false);
      assert.match(mismatch.firstDivergence.field, /organizationalPlan/);
    });
  });

  describe('Scenario 5 — Structured projection diff on mismatch', () => {
    it('returns first divergent field, memory value, persisted value, and reason', async () => {
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
      stored.resolvedObjective = {
        ...(stored.resolvedObjective || {}),
        objective: 'Drifted objective that was never persisted',
      };
      engine.store.putMission(stored);

      await assert.rejects(
        () => assertPersistedMatchesEngine(engine, mission.id, '10', pool),
        (err) => {
          assert.equal(err.code, 'tme_persistence_verify');
          assert.ok(err.details);
          assert.ok(err.details.firstDivergence);
          const first = err.details.firstDivergence;
          assert.equal(typeof first.field, 'string');
          assert.ok(first.field.length > 0);
          assert.ok(Object.prototype.hasOwnProperty.call(first, 'memory'));
          assert.ok(Object.prototype.hasOwnProperty.call(first, 'persisted'));
          assert.equal(typeof first.reason, 'string');
          assert.match(first.field, /resolvedObjective|objective/);
          assert.equal(err.details.field, first.field);
          assert.equal(err.details.reason, first.reason);
          return true;
        }
      );
    });

    it('detects event drift that the 10-field subset comparator excluded', () => {
      const sharedMission = {
        id: 'mission_events',
        version: 1,
        stage: 'discover',
        status: 'Discovering',
        structuredMissionApproved: true,
        structuredMission: { immutable: true, objective: OBJECTIVE },
        missionPlanDraft: null,
        pendingOperatorDecision: { kind: 'discovery_approval' },
        lastTransactionId: 'tme_1',
        resolvedObjective: { objective: OBJECTIVE },
        executionPolicy: { autonomy: 'autonomous' },
        communicationPolicy: { style: 'executive' },
        evaluationPolicy: { executiveBehavior: true },
      };
      const memory = buildCanonicalMissionProjection({
        mission: sharedMission,
        contributions: [{ id: 'c1', missionId: 'mission_events' }],
        events: [{
          id: 'evt_1',
          missionId: 'mission_events',
          kind: 'mission_created',
          specialist: 'max',
          label: 'Mission Created',
          at: '2026-08-25T00:00:00.000Z',
          payload: {},
        }],
        observations: [],
        outcomes: [],
      });
      const persisted = buildCanonicalMissionProjection({
        mission: sharedMission,
        contributions: [{ id: 'c1', missionId: 'mission_events' }],
        events: [{
          id: 'evt_1',
          missionId: 'mission_events',
          kind: 'mission_created',
          specialist: 'max',
          label: 'Mission Created (altered)',
          at: '2026-08-25T00:00:00.000Z',
          payload: {},
        }],
        observations: [],
        outcomes: [],
      });

      const diff = diffCanonicalMissionProjections(memory, persisted);
      assert.equal(diff.equal, false);
      assert.equal(diff.firstDivergence.field, 'events[0].label');
      assert.equal(diff.firstDivergence.memory, 'Mission Created');
      assert.equal(diff.firstDivergence.persisted, 'Mission Created (altered)');
      assert.equal(diff.firstDivergence.reason, 'value mismatch');
    });
  });

  it('excludes inspect presentation fields from the projection', () => {
    const projection = buildCanonicalMissionProjection({
      spec: 'SPEC-118',
      mission: { id: 'm1', stage: 'discover', objective: OBJECTIVE },
      workspace: { scout: { state: 'waiting' } },
      health: { label: 'Healthy' },
      timeline: [{
        id: 'evt_1',
        missionId: 'm1',
        kind: 'mission_created',
        specialist: 'max',
        label: 'Mission Created',
        at: '2026-08-25T00:00:00.000Z',
        payload: {},
        clock: '12:00',
        line: '12:00   Mission Created',
      }],
      observations: [{
        id: 'obs_1',
        missionId: 'm1',
        specialist: 'max',
        observation: 'noted',
        at: '2026-08-25T00:00:00.000Z',
        line: 'Max   noted',
      }],
      contributions: [],
      outcomes: [],
    });

    assert.equal(projection.workspace, undefined);
    assert.equal(projection.health, undefined);
    assert.equal(projection.timeline, undefined);
    assert.equal(projection.events[0].clock, undefined);
    assert.equal(projection.events[0].line, undefined);
    assert.equal(projection.observations[0].line, undefined);
    assert.equal(projection.events[0].label, 'Mission Created');
  });
});
