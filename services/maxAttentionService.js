'use strict';

const pool = require('../db');
const {
  runAttentionCycle,
  wakeAttentionForIngestion,
  syncAttentionFromDecision,
  operatorVisibleItems,
  MemoryAttentionStore,
  PostgresAttentionStore,
} = require('../packages/max/attention');
const { PostgresStateStore } = require('../packages/max/stateIngestion');
const { PostgresDecisionStore } = require('../packages/max/decisionExecution');

async function createStores(clientId, db = pool) {
  const stateStore = new PostgresStateStore(db, { clientId });
  await stateStore.init();
  const decisionStore = new PostgresDecisionStore(db, { clientId });
  await decisionStore.init();
  const attentionStore = new PostgresAttentionStore(db, { clientId });
  await attentionStore.init();
  return { stateStore, decisionStore, attentionStore };
}

async function runAttentionScheduler(clientId, { db = pool, now = new Date(), limit = 20 } = {}) {
  const { stateStore, decisionStore, attentionStore } = await createStores(clientId, db);
  return runAttentionCycle({
    clientId,
    stateStore,
    decisionStore,
    attentionStore,
    now,
    limit,
  });
}

async function runAttentionSchedulerAllClients({ db = pool, now = new Date(), limit = 20 } = {}) {
  const { rows } = await db.query(`SELECT id FROM clients WHERE active = true ORDER BY id`);
  const results = [];
  for (const row of rows) {
    try {
      results.push({
        client_id: row.id,
        ...(await runAttentionScheduler(row.id, { db, now, limit })),
      });
    } catch (err) {
      results.push({ client_id: row.id, error: err.message });
    }
  }
  return results;
}

async function afterIngestionAttention(clientId, ingestionResult, { db = pool, now = new Date() } = {}) {
  const attentionStore = new PostgresAttentionStore(db, { clientId });
  await attentionStore.init();
  const stateStore = new PostgresStateStore(db, { clientId });
  await stateStore.init();
  return wakeAttentionForIngestion({
    attentionStore,
    clientId,
    ingestionResult,
    stateStore,
    now,
  });
}

async function afterDecisionAttention(clientId, decisionResult, { db = pool, now = new Date() } = {}) {
  if (!decisionResult?.decision) return null;
  const attentionStore = new PostgresAttentionStore(db, { clientId });
  await attentionStore.init();
  return syncAttentionFromDecision({
    attentionStore,
    decision: decisionResult.decision,
    snapshot: decisionResult.snapshot,
    now,
  });
}

async function listOperatorAttention(clientId, { db = pool, limit = 10 } = {}) {
  const attentionStore = new PostgresAttentionStore(db, { clientId });
  await attentionStore.init();
  const items = await attentionStore.listUnresolved({ clientId });
  return operatorVisibleItems(items, { limit });
}

async function getAttentionHealth({ db = pool } = {}) {
  const attentionStore = new PostgresAttentionStore(db, {});
  await attentionStore.init();
  return attentionStore.getHeartbeat();
}

module.exports = {
  runAttentionScheduler,
  runAttentionSchedulerAllClients,
  afterIngestionAttention,
  afterDecisionAttention,
  listOperatorAttention,
  getAttentionHealth,
  MemoryAttentionStore,
};
