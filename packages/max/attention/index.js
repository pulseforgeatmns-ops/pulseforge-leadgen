'use strict';

const types = require('./types');
const { syncAttentionFromDecision } = require('./syncFromDecision');
const { wakeAttentionForSubject, wakeAttentionForIngestion } = require('./wake');
const { runAttentionCycle } = require('./scheduler');
const { operatorVisibleItems } = require('./budget');
const { MemoryAttentionStore } = require('./store/memoryStore');

module.exports = {
  ...types,
  syncAttentionFromDecision,
  wakeAttentionForSubject,
  wakeAttentionForIngestion,
  runAttentionCycle,
  operatorVisibleItems,
  MemoryAttentionStore,
  get PostgresAttentionStore() {
    return require('./store/postgresStore').PostgresAttentionStore;
  },
};
