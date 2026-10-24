'use strict';

const pool = require('../db');
const {
  evaluateOperationalDecision,
  scanExpectationTriggers,
  reevaluateOnIngestion,
  PostgresDecisionStore,
  MemoryDecisionStore,
} = require('../packages/max/decisionExecution');
const { PostgresStateStore } = require('../packages/max/stateIngestion');
const { PostgresAttentionStore } = require('../packages/max/attention/store/postgresStore');

async function createStores(clientId, db = pool, { withAttention = true } = {}) {
  const stateStore = new PostgresStateStore(db, { clientId });
  await stateStore.init();
  const decisionStore = new PostgresDecisionStore(db, { clientId });
  await decisionStore.init();
  let attentionStore = null;
  if (withAttention) {
    attentionStore = new PostgresAttentionStore(db, { clientId });
    await attentionStore.init();
  }
  return { stateStore, decisionStore, attentionStore };
}

async function evaluateDecision(clientId, body = {}, { db = pool } = {}) {
  const { stateStore, decisionStore, attentionStore } = await createStores(clientId, db);
  const trigger = body.trigger || {
    type: body.trigger_type || body.triggerType,
    payload: body.payload || body.trigger_payload || {},
  };
  return evaluateOperationalDecision({
    clientId,
    trigger,
    stateStore,
    decisionStore,
    attentionStore,
    now: body.now ? new Date(body.now) : new Date(),
    policy: body.policy || {},
  });
}

async function runExpectationDecisionScan(clientId, { db = pool, now = new Date() } = {}) {
  const { stateStore, decisionStore } = await createStores(clientId, db);
  return scanExpectationTriggers({ clientId, stateStore, decisionStore, now });
}

async function afterIngestionDecisions(clientId, ingestionResult, { db = pool, now = new Date() } = {}) {
  const { stateStore, decisionStore, attentionStore } = await createStores(clientId, db);
  return reevaluateOnIngestion({
    clientId,
    stateStore,
    decisionStore,
    attentionStore,
    ingestionResult,
    now,
  });
}

module.exports = {
  evaluateDecision,
  runExpectationDecisionScan,
  afterIngestionDecisions,
  MemoryDecisionStore,
};
