'use strict';

const { MemoryAttentionStore } = require('./store/memoryStore');
const { MemoryStateStore } = require('../stateIngestion/store/memoryStore');
const { MemoryDecisionStore } = require('../decisionExecution/store/memoryStore');
const { PostgresAttentionStore } = require('./store/postgresStore');
const { PostgresStateStore } = require('../stateIngestion/store/postgresStore');
const { PostgresDecisionStore } = require('../decisionExecution/store/postgresStore');

function exactStore(store, Type, db, clientId) {
  return Object.getPrototypeOf(store || {}) === Type.prototype
    && store.db === db && store.clientId === clientId
    && !Object.values(store).some(value => typeof value === 'function');
}

async function withClaimedStores({ item, claimToken, clientId, attentionStore, stateStore, decisionStore }, evaluate) {
  if (attentionStore instanceof MemoryAttentionStore && stateStore instanceof MemoryStateStore
    && decisionStore instanceof MemoryDecisionStore) {
    return attentionStore.withClaim(item.id, { claimToken, clientId }, scope =>
      evaluate({ ...scope, stateStore, decisionStore }));
  }

  // Current built-in PostgreSQL execution is deterministic and DB-only. Do not
  // silently wrap arbitrary executors: transactions cannot fence provider calls.
  // If a provider/outbox executor is introduced, review this boundary first.
  const db = attentionStore?.db;
  if (!exactStore(attentionStore, PostgresAttentionStore, db, clientId)
    || !exactStore(stateStore, PostgresStateStore, db, clientId)
    || !exactStore(decisionStore, PostgresDecisionStore, db, clientId)) {
    throw Object.assign(new Error('Attention recovery requires matching database-only stores'), {
      code: 'ATTENTION_UNSAFE_STORE',
    });
  }
  return attentionStore.withClaim(item.id, { claimToken, clientId }, scope => evaluate({
    ...scope,
    stateStore: new PostgresStateStore(scope.db, { clientId }),
    decisionStore: new PostgresDecisionStore(scope.db, { clientId }),
  }));
}

module.exports = { withClaimedStores };
