'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const amo = require('../packages/acquisition-mission');
const {
  STAGES,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  EXECUTION_INTENTS,
  validateProspectMessageBindings,
} = amo;
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
  advanceMaxPrioritization,
  advanceAcquisitionApproach,
  advancePaigeVariants,
  advanceEmmettCapacity,
  advanceExecutionAfterApproval,
} = require('../packages/max/workspace/AmoOperatorApproval');
const { sanitizeQueueItem } = require('../packages/max/workspace/EmmettCapacityExecution');
const {
  inspectCapacitySpec212,
  activePaigePayload,
} = require('../scripts/regenerateAnchorCapacityRevision');
const { isSupersededContribution } = amo;
const {
  persistStageCommit,
  loadMissionSnapshot,
} = require('../services/acquisitionMissionPersistence');
const {
  createAcquisitionMissionRuntime,
  resetAcquisitionMissionRuntime,
} = require('../services/acquisitionMissionRuntime');
const {
  selectActiveCapacityContribution,
} = require('../scripts/probeAnchorEmmettOutboundReadiness');

const OBJECTIVE = 'Acquire commercial cleaning customers in Manchester NH for law firms.';

async function preparedReadyMission() {
  const engine = amo.createAcquisitionMissionEngine();
  const mission = engine.create({ tenantId: '10', objective: OBJECTIVE, targetSegment: 'Law Firms' });
  await advancePlanAfterApproval({ engine, mission, tenantId: '10', question: 'Approved.' });
  await advanceDiscoveryAfterApproval({
    engine, mission, tenantId: '10', question: 'Approved.', allowFixtureFallback: true,
  });
  await advancePrioritizationAfterApproval({
    engine, mission: engine.get(mission.id, '10'), tenantId: '10', question: 'Approved.',
  });
  await advanceMaxPrioritization({
    engine, mission: engine.get(mission.id, '10'), tenantId: '10', allowFixtureFallback: true,
  });
  await advanceAcquisitionApproach({
    engine, mission: engine.get(mission.id, '10'), tenantId: '10', allowFixtureFallback: true,
  });
  await advancePaigeVariants({
    engine, mission: engine.get(mission.id, '10'), tenantId: '10', allowFixtureFallback: true,
  });
  await advanceEmmettCapacity({
    engine, mission: engine.get(mission.id, '10'), tenantId: '10', allowFixtureFallback: true,
  });
  await advanceExecutionAfterApproval({
    engine,
    mission: engine.get(mission.id, '10'),
    tenantId: '10',
    operatorId: 'operator-1',
    question: 'Authorize prepared bundle.',
  });
  return { engine, mission: engine.get(mission.id, '10') };
}

function findEmmettCapacityContribution(engine, missionId) {
  return engine.store.listContributions(missionId).find(
    (row) => row.specialist === SPECIALISTS.EMMETT && row.kind === CONTRIBUTION_KINDS.CAPACITY
  ) || null;
}

function simulateProductionNestedCapacityPersist(engine, missionId) {
  const emmett = findEmmettCapacityContribution(engine, missionId);
  assert.ok(emmett, 'expected emmett capacity');
  engine.store.updateContribution(emmett.id, (row) => ({
    ...row,
    payload: {
      id: row.id,
      specialist: row.specialist,
      kind: row.kind,
      payload: { ...(row.payload || {}) },
    },
  }));
}

function simulatePreSpec212Persist(engine, missionId) {
  const emmett = findEmmettCapacityContribution(engine, missionId);
  assert.ok(emmett, 'expected emmett capacity');
  const body = { ...(emmett.payload || {}) };
  const items = Array.isArray(body.queue?.items) ? body.queue.items : [];
  body.queue = {
    ...(body.queue || {}),
    items: items.map((item) => sanitizeQueueItem({
      ...item,
      paige: item.paige
        ? {
          ...item.paige,
          candidateId: item.paige.candidateId,
          bindingScope: item.paige.bindingScope,
          attributableIntelligence: item.paige.attributableIntelligence,
          variantId: item.paige.variantId,
        }
        : item.paige,
    })),
  };
  // Pre-fix bug dropped bindings — simulate by stripping after sanitize with old behavior
  body.queue.items = body.queue.items.map((item) => {
    if (!item.paige) return item;
    return {
      ...item,
      paige: {
        author: item.paige.author,
        source: item.paige.source,
        ready: item.paige.ready,
        variantLabel: item.paige.variantLabel,
        sendable: item.paige.sendable,
      },
    };
  });
  engine.store.updateContribution(emmett.id, (row) => ({
    ...row,
    payload: body,
  }));
}

/**
 * Durable pool that honors contribution ON CONFLICT:
 * DO NOTHING leaves existing rows unchanged; DO UPDATE writes payload.
 * This is the production Postgres contract the revision path must satisfy.
 */
function createRevisionMemoryPool() {
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
    if (
      /^CREATE |^ALTER |^UPDATE acquisition_knowledge/i.test(trimmed)
      || /pg_try_advisory_lock|pg_advisory_lock|pg_advisory_unlock/i.test(trimmed)
    ) {
      if (/pg_try_advisory_lock/i.test(trimmed)) return { rows: [{ locked: true }] };
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
      tables.acquisition_missions.set(params[0], {
        id: params[0],
        tenant_id: String(params[1]),
        client_id: params[2],
        stage: params[3],
        status: params[4],
        objective: params[5],
        payload: mission,
        created_at: params[15],
        updated_at: params[16],
      });
      return { rows: [] };
    }

    if (/INSERT INTO acquisition_mission_events/i.test(sql)) {
      if (tables.acquisition_mission_events.has(params[0]) && /DO NOTHING/i.test(sql)) {
        return { rows: [] };
      }
      tables.acquisition_mission_events.set(params[0], {
        id: params[0],
        mission_id: params[1],
        tenant_id: String(params[2]),
        kind: params[3],
        specialist: params[4],
        label: params[5],
        payload: params[6],
        at: params[7],
      });
      return { rows: [] };
    }

    if (/INSERT INTO acquisition_mission_contributions/i.test(sql)) {
      const id = params[0];
      const exists = tables.acquisition_mission_contributions.has(id);
      const canUpdate = /ON CONFLICT \(id\) DO UPDATE/i.test(sql);
      if (exists && !canUpdate) return { rows: [] };
      tables.acquisition_mission_contributions.set(id, {
        id,
        mission_id: params[1],
        tenant_id: String(params[2]),
        specialist: params[3],
        kind: params[4],
        payload: params[5],
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
        payload: params[5],
        at: params[6],
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

    if (/SELECT id, mission_id, specialist, observation, payload, at FROM acquisition_mission_observations/i.test(sql)) {
      return { rows: [] };
    }

    if (/SELECT payload FROM acquisition_mission_outcomes WHERE tenant_id/i.test(sql)) {
      return { rows: [] };
    }

    if (/SELECT payload FROM acquisition_mission_learning/i.test(sql)) return { rows: [] };
    if (/SELECT payload FROM acquisition_mission_predictions/i.test(sql)) return { rows: [] };
    if (/SELECT payload FROM acquisition_mission_outcome_evaluations/i.test(sql)) return { rows: [] };
    if (/SELECT payload FROM acquisition_mission_outcome_learnings/i.test(sql)) return { rows: [] };

    throw new Error(`Unhandled SQL in revision memory pool: ${trimmed.split('\n')[0]}`);
  }

  return {
    query,
    connect: async () => ({ query, release() {} }),
    tables,
  };
}

async function persistEngineMission(engine, missionId, tenantId, pool) {
  await persistStageCommit({
    mission: engine.get(missionId, tenantId),
    events: engine.store.listEvents(missionId),
    contributions: engine.store.listContributions(missionId),
    observations: engine.store.listObservations(missionId),
    outcomes: engine.store.listOutcomes(missionId),
  }, pool, { skipGlobalLock: true });
}

describe('regenerateAnchorCapacityRevision — canonical path', () => {
  it('canonical contribution writer upserts payload so supersession is durable', () => {
    const src = fs.readFileSync(
      path.join(__dirname, '../services/acquisitionMissionPersistence.js'),
      'utf8'
    );
    const start = src.indexOf('async function persistContribution');
    const end = src.indexOf('async function persistObservation');
    const fn = src.slice(start, end);
    assert.match(fn, /ON CONFLICT \(id\) DO UPDATE SET/);
    assert.match(fn, /payload = EXCLUDED\.payload/);
    assert.doesNotMatch(fn, /ON CONFLICT \(id\) DO NOTHING/);
  });

  it('revision runner binds persist through runtime.persistOpts, not a missing runtime.pool', () => {
    const src = fs.readFileSync(
      path.join(__dirname, '../scripts/regenerateAnchorCapacityRevision.js'),
      'utf8'
    );
    assert.match(src, /runtime\.persistOpts\(\{\s*persist:\s*true\s*\}\)/);
    assert.match(src, /loadMissionSnapshot/);
    assert.doesNotMatch(src, /pool:\s*runtime\.pool/);
  });

  it('REVISE_PREPARED_OUTREACH clears pre-SPEC-212 contamination when Paige variants are reused', async () => {
    const { engine, mission } = await preparedReadyMission();
    simulatePreSpec212Persist(engine, mission.id);

    const before = engine.inspect(mission.id, { tenantId: '10' });
    const oldCapacity = before.contributions.find(
      (row) => row.specialist === SPECIALISTS.EMMETT && row.kind === CONTRIBUTION_KINDS.CAPACITY
    );
    const beforeSpec212 = inspectCapacitySpec212(oldCapacity.payload);
    assert.equal(beforeSpec212.valid, false);
    assert.ok(beforeSpec212.violationCount > 0);

    const paigePayload = activePaigePayload(before.contributions);
    assert.ok(paigePayload?.variants?.length);

    const request = amo.createExecutionRequest({
      source: amo.EXECUTION_SOURCES.API,
      intent: EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH,
      missionId: mission.id,
      mission: engine.get(mission.id, '10'),
      operatorId: 'operator-1',
      stage: STAGES.READY,
      question: 'Regenerate capacity only.',
    });

    const routed = await amo.routeExecutionRequest(request, {
      engine,
      tenantId: '10',
      allowFixtureFallback: true,
      runPaige: async () => paigePayload,
    });

    assert.equal(routed.action, 'revise_prepared_outreach');
    const after = routed.snapshot;
    assert.equal(after.mission.stage, STAGES.READY);

    const capacities = after.contributions.filter(
      (row) => row.specialist === SPECIALISTS.EMMETT && row.kind === CONTRIBUTION_KINDS.CAPACITY
    );
    const newCapacity = capacities.at(-1);
    const supersededOld = after.contributions.find((row) => row.id === oldCapacity.id);
    assert.notEqual(newCapacity.id, oldCapacity.id);
    assert.equal(supersededOld.payload.superseded, true);

    const afterSpec212 = inspectCapacitySpec212(newCapacity.payload);
    assert.equal(afterSpec212.valid, true, afterSpec212.blocker);
    assert.equal(afterSpec212.violationCount, 0);

    const validation = validateProspectMessageBindings(
      (newCapacity.payload.payload && newCapacity.payload.specialist)
        ? newCapacity.payload.payload
        : newCapacity.payload
    );
    assert.equal(validation.valid, true);
  });

  it('fails closed when persist=true but persistStageCommit cannot bind', async () => {
    const { engine, mission } = await preparedReadyMission();
    const paigePayload = activePaigePayload(engine.inspect(mission.id, { tenantId: '10' }).contributions);
    const request = amo.createExecutionRequest({
      source: amo.EXECUTION_SOURCES.API,
      intent: EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH,
      missionId: mission.id,
      mission: engine.get(mission.id, '10'),
      operatorId: 'operator-1',
      stage: STAGES.READY,
      question: 'Regenerate capacity only.',
    });

    const routed = await amo.routeExecutionRequest(request, {
      engine,
      tenantId: '10',
      persist: true,
      allowFixtureFallback: true,
      runPaige: async () => paigePayload,
    });
    assert.equal(routed.executionResult?.rolledBack, true);
    assert.equal(routed.executionResult?.error?.code, 'tme_persistence');
    assert.match(String(routed.executionResult?.error?.message || ''), /persistStageCommit/);

    const after = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(after.mission.stage, STAGES.READY);
    const capacities = after.contributions.filter(
      (row) => row.specialist === SPECIALISTS.EMMETT && row.kind === CONTRIBUTION_KINDS.CAPACITY
    );
    assert.equal(capacities.length, 1);
  });

  it('revision inserts a new durable CAPACITY and marks the old row superseded after reload', async () => {
    const pool = createRevisionMemoryPool();
    const { engine, mission } = await preparedReadyMission();
    simulatePreSpec212Persist(engine, mission.id);
    await persistEngineMission(engine, mission.id, '10', pool);

    const before = engine.inspect(mission.id, { tenantId: '10' });
    const oldCapacity = before.contributions.find(
      (row) => row.specialist === SPECIALISTS.EMMETT && row.kind === CONTRIBUTION_KINDS.CAPACITY
    );
    const paigePayload = activePaigePayload(before.contributions);
    assert.ok(oldCapacity);
    assert.equal(inspectCapacitySpec212(oldCapacity.payload).valid, false);

    const durableBefore = await loadMissionSnapshot(mission.id, '10', pool);
    assert.equal(
      durableBefore.contributions.filter(
        (row) => row.specialist === SPECIALISTS.EMMETT && row.kind === CONTRIBUTION_KINDS.CAPACITY
      ).length,
      1
    );

    const request = amo.createExecutionRequest({
      source: amo.EXECUTION_SOURCES.API,
      intent: EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH,
      missionId: mission.id,
      mission: engine.get(mission.id, '10'),
      operatorId: 'operator-1',
      stage: STAGES.READY,
      question: 'Regenerate capacity only.',
    });

    const routed = await amo.routeExecutionRequest(request, {
      engine,
      tenantId: '10',
      persist: true,
      pool,
      allowFixtureFallback: true,
      runPaige: async () => paigePayload,
    });
    assert.equal(routed.action, 'revise_prepared_outreach');
    assert.notEqual(routed.executionResult?.rolledBack, true, routed.executionResult?.error?.message);

    resetAcquisitionMissionRuntime();
    const reloadedRuntime = createAcquisitionMissionRuntime({
      persist: true,
      pool,
      production: false,
    });
    await reloadedRuntime.hydrate('10', { persist: true, pool });
    const reloaded = reloadedRuntime.engine().inspect(mission.id, { tenantId: '10' });
    const durable = await loadMissionSnapshot(mission.id, '10', pool);

    const capacities = (durable.contributions || []).filter(
      (row) => row.specialist === SPECIALISTS.EMMETT && row.kind === CONTRIBUTION_KINDS.CAPACITY
    );
    assert.equal(capacities.length, 2, 'old and new CAPACITY must both be durable');

    const persistedOld = capacities.find((row) => row.id === oldCapacity.id);
    const persistedNew = capacities.find((row) => row.id !== oldCapacity.id);
    assert.ok(persistedOld, 'old CAPACITY row remains present');
    assert.ok(persistedNew, 'new CAPACITY row must be inserted');
    assert.notEqual(persistedNew.id, oldCapacity.id);
    assert.equal(persistedOld.payload.superseded, true);
    assert.equal(persistedOld.payload.supersededBy, persistedNew.id);
    assert.equal(persistedNew.payload.superseded, undefined);

    const afterSpec212 = inspectCapacitySpec212(persistedNew.payload);
    assert.equal(afterSpec212.valid, true, afterSpec212.blocker);
    assert.equal(afterSpec212.violationCount, 0);

    const probeRows = capacities.map((row) => ({
      capacity_id: row.id,
      at: row.at,
      payload: row,
    }));
    const selected = selectActiveCapacityContribution(durable.mission, probeRows);
    assert.equal(selected.capacity_id, persistedNew.id);

    const hydratedCapacities = reloaded.contributions.filter(
      (row) => row.specialist === SPECIALISTS.EMMETT && row.kind === CONTRIBUTION_KINDS.CAPACITY
    );
    const hydratedOld = hydratedCapacities.find((row) => row.id === oldCapacity.id);
    const hydratedNew = hydratedCapacities.find((row) => row.id === persistedNew.id);
    assert.equal(hydratedOld.payload.superseded, true);
    assert.equal(inspectCapacitySpec212(hydratedNew.payload).valid, true);
    assert.equal(reloaded.mission.stage, STAGES.READY);
    assert.equal(reloaded.mission.revisionState?.emmettContributionId, persistedNew.id);
  });

  it('durable supersede verification passes for production nested CAPACITY JSONB', async () => {
    const pool = createRevisionMemoryPool();
    const { engine, mission } = await preparedReadyMission();
    simulateProductionNestedCapacityPersist(engine, mission.id);
    simulatePreSpec212Persist(engine, mission.id);
    await persistEngineMission(engine, mission.id, '10', pool);

    const contributionsBefore = engine.store.listContributions(mission.id);
    const oldCapacity = findEmmettCapacityContribution(engine, mission.id);
    const paigePayload = activePaigePayload(contributionsBefore);
    assert.ok(oldCapacity);
    assert.equal(isSupersededContribution(oldCapacity), false);

    const request = amo.createExecutionRequest({
      source: amo.EXECUTION_SOURCES.API,
      intent: EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH,
      missionId: mission.id,
      mission: engine.get(mission.id, '10'),
      operatorId: 'operator-1',
      stage: STAGES.READY,
      question: 'Regenerate capacity only.',
    });

    const routed = await amo.routeExecutionRequest(request, {
      engine,
      tenantId: '10',
      persist: true,
      pool,
      allowFixtureFallback: true,
      runPaige: async () => paigePayload,
    });
    assert.equal(routed.action, 'revise_prepared_outreach');
    assert.notEqual(routed.executionResult?.rolledBack, true, routed.executionResult?.error?.message);

    const durable = await loadMissionSnapshot(mission.id, '10', pool);
    const capacities = (durable.contributions || []).filter(
      (row) => row.specialist === SPECIALISTS.EMMETT && row.kind === CONTRIBUTION_KINDS.CAPACITY
    );
    const persistedOld = capacities.find((row) => row.id === oldCapacity.id);
    const persistedNew = capacities.find((row) => row.id !== oldCapacity.id);
    assert.ok(persistedOld);
    assert.ok(persistedNew);
    assert.equal(isSupersededContribution(persistedOld), true);
    assert.equal(persistedOld.payload.superseded, true);
    assert.equal(persistedOld.payload.payload.superseded, true);
    assert.equal(isSupersededContribution(persistedNew), false);
    assert.equal(
      capacities.filter((row) => !isSupersededContribution(row)).length,
      1
    );
  });
});
