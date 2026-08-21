'use strict';

/**
 * SPEC-131 — Transactional Mission Execution.
 * A stage is fully committed or fully rolled back. Never partial.
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const amo = require('../index');
const {
  createAcquisitionMissionEngine,
  executeMissionStage,
  clearExecutionAudit,
  listExecutionAudit,
  TME_CLASSES,
  isRolledBackExecution,
  formatRollbackProse,
  SPECIALISTS,
  STAGES,
  EVENT_KINDS,
} = amo;
const { persistStageCommit } = require('../../../services/acquisitionMissionPersistence');

const OBJECTIVE = 'Acquire commercial cleaning customers in Manchester NH for law firms.';

describe('SPEC-131 — Transactional Mission Execution', () => {
  beforeEach(() => {
    clearExecutionAudit();
  });

  function engineWithApprovedPlan() {
    const engine = createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
      planApproved: true,
    });
    return { engine, mission };
  }

  it('commits contribution, mission version, and audit together on success', async () => {
    const { engine, mission } = engineWithApprovedPlan();
    const prior = engine.get(mission.id, '10');
    assert.equal(prior.version, 0);

    const result = await executeMissionStage({
      engine,
      missionId: mission.id,
      tenantId: '10',
      specialist: SPECIALISTS.SCOUT,
      stage: STAGES.DISCOVER,
      execute: async () => ({
        payload: {
          companies: [{ id: 'c1', name: 'Harbor Law' }],
          prospects: [{ id: 'p1', name: 'Alex' }],
          buyingSignals: ['Hiring office manager'],
          evidence: [{ label: 'Job post', source: 'job_board' }],
          confidence: 0.8,
        },
      }),
      validateOutput: (output) => {
        assert.ok(output.payload);
      },
      commit: ({ engine: amoEngine, mission: current, output, transactionId }) => {
        amo.bumpMissionVersion(current, transactionId);
        amoEngine.store.putMission(current);
        const contribution = amoEngine.contribute(current.id, {
          specialist: SPECIALISTS.SCOUT,
          kind: amo.CONTRIBUTION_KINDS.DISCOVERY,
          payload: output.payload,
        }, { tenantId: '10' });
        return { contribution: contribution.contribution };
      },
    });

    assert.equal(result.committed, true);
    assert.ok(result.transactionId);
    const after = engine.get(mission.id, '10');
    assert.equal(after.version, 1);
    assert.equal(after.lastTransactionId, result.transactionId);
    assert.equal(engine.store.listContributions(mission.id).length, 1);
    const committed = listExecutionAudit({ missionId: mission.id, commitStatus: 'committed' });
    assert.equal(committed.length, 1);
    assert.equal(committed[0].transactionId, result.transactionId);
    assert.equal(committed[0].specialist, SPECIALISTS.SCOUT);
    assert.ok(Number.isFinite(committed[0].durationMs));
  });

  it('specialist exception rolls back with mission unchanged', async () => {
    const { engine, mission } = engineWithApprovedPlan();
    const before = engine.inspect(mission.id, { tenantId: '10' });

    await assert.rejects(
      () => executeMissionStage({
        engine,
        missionId: mission.id,
        tenantId: '10',
        specialist: SPECIALISTS.SCOUT,
        stage: STAGES.DISCOVER,
        execute: async () => {
          throw new Error('scout crashed');
        },
        commit: () => {
          throw new Error('commit must not run');
        },
      }),
      (err) => {
        assert.equal(isRolledBackExecution(err), true);
        assert.equal(err.tmeClass, TME_CLASSES.SPECIALIST);
        assert.match(err.message, /scout crashed/);
        assert.ok(err.transactionId);
        return true;
      }
    );

    const after = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(after.mission.version, before.mission.version);
    assert.equal(after.mission.status, before.mission.status);
    assert.equal(after.mission.stage, before.mission.stage);
    assert.deepEqual(after.mission.pendingOperatorDecision, before.mission.pendingOperatorDecision);
    assert.equal(after.contributions.length, before.contributions.length);
    const rolled = listExecutionAudit({ missionId: mission.id, commitStatus: 'rolled_back' });
    assert.equal(rolled.length, 1);
    assert.equal(rolled[0].errorClass, TME_CLASSES.SPECIALIST);
    assert.match(rolled[0].rollbackReason, /scout crashed/);
    assert.equal(rolled[0].exception, 'scout crashed');
  });

  it('planning error does not execute the specialist', async () => {
    const engine = createAcquisitionMissionEngine();
    const mission = engine.create({
      tenantId: '10',
      objective: OBJECTIVE,
      targetSegment: 'Law Firms',
    });
    let executed = 0;

    await assert.rejects(
      () => executeMissionStage({
        engine,
        missionId: mission.id,
        tenantId: '10',
        specialist: SPECIALISTS.SCOUT,
        stage: STAGES.DISCOVER,
        execute: async () => {
          executed += 1;
          return {};
        },
        commit: () => {
          throw new Error('commit must not run');
        },
      }),
      (err) => {
        assert.equal(err.tmeClass, TME_CLASSES.PLANNING);
        assert.match(err.message, /Mission Plan missing/);
        return true;
      }
    );

    assert.equal(executed, 0);
    assert.equal(engine.get(mission.id, '10').structuredMissionApproved, false);
  });

  it('validation failure after specialist work leaves the mission unchanged', async () => {
    const { engine, mission } = engineWithApprovedPlan();
    const before = engine.inspect(mission.id, { tenantId: '10' });

    await assert.rejects(
      () => executeMissionStage({
        engine,
        missionId: mission.id,
        tenantId: '10',
        specialist: SPECIALISTS.SCOUT,
        stage: STAGES.DISCOVER,
        execute: async () => ({ payload: { confidence: 9 } }),
        validateOutput: (output) => amo.assertConfidenceValid(output.payload.confidence),
        commit: () => {
          throw new Error('commit must not run');
        },
      }),
      (err) => {
        assert.equal(err.tmeClass, TME_CLASSES.VALIDATION);
        return true;
      }
    );

    const after = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(after.contributions.length, before.contributions.length);
    assert.equal(after.mission.version, before.mission.version);
  });

  it('persistence failure after in-memory writes restores prior mission state', async () => {
    const { engine, mission } = engineWithApprovedPlan();
    const before = engine.inspect(mission.id, { tenantId: '10' });

    await assert.rejects(
      () => executeMissionStage({
        engine,
        missionId: mission.id,
        tenantId: '10',
        specialist: SPECIALISTS.SCOUT,
        stage: STAGES.DISCOVER,
        execute: async () => ({ ok: true }),
        commit: ({ engine: amoEngine, mission: current, transactionId }) => {
          current.status = 'Discovery Executing';
          amo.bumpMissionVersion(current, transactionId);
          amoEngine.store.putMission(current);
          amoEngine.contribute(current.id, {
            specialist: SPECIALISTS.OPERATOR,
            kind: amo.CONTRIBUTION_KINDS.APPROVAL,
            payload: { approved: true, consumed: true, companies: ['x'], confidence: 1 },
          }, { tenantId: '10' });
          return {};
        },
        persistDurable: async () => {
          throw new Error('database unavailable');
        },
      }),
      (err) => {
        assert.equal(err.tmeClass, TME_CLASSES.PERSISTENCE);
        return true;
      }
    );

    const after = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(after.mission.status, before.mission.status);
    assert.equal(after.mission.version, before.mission.version);
    assert.equal(after.contributions.length, before.contributions.length);
    assert.equal(
      after.contributions.some((row) => row.kind === amo.CONTRIBUTION_KINDS.APPROVAL),
      false
    );
  });

  it('presentation failure does not invalidate a committed stage', async () => {
    const { engine, mission } = engineWithApprovedPlan();

    const result = await executeMissionStage({
      engine,
      missionId: mission.id,
      tenantId: '10',
      specialist: SPECIALISTS.SCOUT,
      stage: STAGES.DISCOVER,
      execute: async () => ({ payload: { companies: [{ id: 1 }], confidence: 0.7 } }),
      commit: ({ engine: amoEngine, mission: current, output, transactionId }) => {
        amo.bumpMissionVersion(current, transactionId);
        amoEngine.store.putMission(current);
        return amoEngine.contribute(current.id, {
          specialist: SPECIALISTS.SCOUT,
          kind: amo.CONTRIBUTION_KINDS.DISCOVERY,
          payload: output.payload,
        }, { tenantId: '10' });
      },
      present: async () => {
        throw new Error('renderer exploded');
      },
    });

    assert.equal(result.committed, true);
    assert.equal(result.presentation.retryable, true);
    assert.match(result.presentation.error.message, /renderer exploded/);
    assert.equal(engine.get(mission.id, '10').version, 1);
    assert.equal(engine.store.listContributions(mission.id).length, 1);
  });

  it('executing is never left as durable mission state', async () => {
    const { engine, mission } = engineWithApprovedPlan();
    await assert.rejects(() => executeMissionStage({
      engine,
      missionId: mission.id,
      tenantId: '10',
      specialist: SPECIALISTS.SCOUT,
      stage: STAGES.DISCOVER,
      execute: async () => {
        throw new Error('boom');
      },
      commit: () => {},
    }));
    const after = engine.get(mission.id, '10');
    assert.notEqual(String(after.status).toLowerCase(), 'executing');
    assert.notEqual(after.status, 'Discovery Executing');
  });

  it('recovery copy tells the operator the mission is unchanged', () => {
    assert.match(formatRollbackProse('Discovery'), /Discovery could not execute/);
    assert.match(formatRollbackProse('Discovery'), /Mission remains unchanged/);
    assert.match(formatRollbackProse('Discovery'), /Resolve the blocker and retry/);
  });

  it('SQL persistStageCommit rolls back when a later write fails', async () => {
    const calls = [];
    const client = {
      async query(sql, params) {
        calls.push(sql.split('\n')[0].trim());
        if (/acquisition_mission_contributions/i.test(sql)) {
          throw new Error('contribution write failed');
        }
        return { rows: [] };
      },
      release() {},
    };
    const pool = {
      async query(sql, params) {
        return client.query(sql, params);
      },
      async connect() {
        return client;
      },
    };

    await assert.rejects(
      () => persistStageCommit({
        mission: {
          id: 'mission_1',
          tenantId: '10',
          stage: 'discover',
          status: 'Discovering',
          objective: OBJECTIVE,
          priority: 'normal',
        },
        events: [{
          id: 'evt_1',
          missionId: 'mission_1',
          kind: EVENT_KINDS.EXECUTION_COMMITTED,
          specialist: SPECIALISTS.SCOUT,
          label: 'committed',
          at: new Date().toISOString(),
        }],
        contributions: [{
          id: 'contrib_1',
          missionId: 'mission_1',
          specialist: SPECIALISTS.SCOUT,
          kind: 'discovery',
          at: new Date().toISOString(),
        }],
      }, pool, { skipEnsure: true }),
      /contribution write failed|Persistence failure/
    );

    assert.ok(calls.some((sql) => sql === 'BEGIN'));
    assert.ok(calls.some((sql) => sql === 'ROLLBACK'));
    assert.equal(calls.some((sql) => sql === 'COMMIT'), false);
  });
});
