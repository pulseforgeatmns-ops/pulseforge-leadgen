'use strict';

const { GraphSyncEngine } = require('./GraphSyncEngine');
const { InMemorySyncLedger } = require('./InMemorySyncLedger');
const { PostgresSyncLedger } = require('./PostgresSyncLedger');
const { MemoryRelationalSource } = require('./RelationalSource');
const { PostgresRelationalSource } = require('./adapters/PostgresRelationalSource');
const { SYNC_EVENTS, SYNC_ENTITY_KINDS } = require('./syncEvents');
const {
  mapCompanyRow,
  mapProspectRow,
  mapTouchpointRow,
  mapImportBatchItem,
  mapEntityMutation,
} = require('./mappers');
const {
  companyNodeId,
  personNodeId,
  interactionNodeId,
  stableEvidenceId,
  syncIdempotencyKey,
} = require('./stableIds');

/**
 * @param {object} runtime - from createKnowledgeRuntime()
 * @param {object} [options]
 * @param {object} [options.ledger]
 */
function createGraphSyncEngine(runtime, options = {}) {
  if (!runtime || !runtime.knowledge || !runtime.bus) {
    throw new Error('createGraphSyncEngine requires createKnowledgeRuntime() result');
  }
  const ledger = options.ledger || new InMemorySyncLedger();
  const sync = new GraphSyncEngine({
    knowledge: runtime.knowledge,
    bus: runtime.bus,
    ledger,
  });
  return { sync, ledger };
}

module.exports = {
  GraphSyncEngine,
  InMemorySyncLedger,
  PostgresSyncLedger,
  MemoryRelationalSource,
  PostgresRelationalSource,
  createGraphSyncEngine,
  SYNC_EVENTS,
  SYNC_ENTITY_KINDS,
  mapCompanyRow,
  mapProspectRow,
  mapTouchpointRow,
  mapImportBatchItem,
  mapEntityMutation,
  companyNodeId,
  personNodeId,
  interactionNodeId,
  stableEvidenceId,
  syncIdempotencyKey,
};
