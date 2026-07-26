'use strict';

const { KnowledgeService } = require('./services/KnowledgeService');
const { InMemoryGraphRepository } = require('./repositories/InMemoryGraphRepository');
const { PersistentGraphRepository } = require('./repositories/PersistentGraphRepository');
const { ensureKnowledgeSchema } = require('./repositories/ensureKnowledgeSchema');
const {
  assertGraphRepository,
  isGraphRepository,
  GRAPH_REPOSITORY_METHODS,
} = require('./repositories/GraphRepository');
const { EvidenceEngine } = require('./evidence/EvidenceEngine');
const { ClaimEngine } = require('./claims/ClaimEngine');
const { KnowledgeEventBus, KNOWLEDGE_EVENTS } = require('./events/KnowledgeEventBus');
const { KnowledgeIngestor } = require('./events/KnowledgeIngestor');
const { NODE_TYPES } = require('./types/nodeTypes');
const { EDGE_TYPES } = require('./edges/edgeTypes');
const {
  combineConfidences,
  calculateConfidenceFromEvidence,
} = require('./confidence/calculateConfidence');
const {
  GraphSyncEngine,
  InMemorySyncLedger,
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
} = require('./sync');

/**
 * Create a ready-to-use knowledge runtime (in-memory by default).
 *
 * @param {object} [options]
 * @param {import('./repositories/GraphRepository').GraphRepository} [options.repository]
 * @param {boolean} [options.startIngestor=true]
 * @param {boolean} [options.withSync=true] - attach GraphSyncEngine + ledger
 */
function createKnowledgeRuntime(options = {}) {
  const repository = options.repository || new InMemoryGraphRepository();
  assertGraphRepository(repository);
  const knowledge = new KnowledgeService({ repository });
  const bus = new KnowledgeEventBus();
  const ingestor = new KnowledgeIngestor({ knowledge, bus });
  if (options.startIngestor !== false) {
    ingestor.start();
  }

  const runtime = { knowledge, repository, bus, ingestor };

  if (options.withSync !== false) {
    const { sync, ledger } = createGraphSyncEngine(runtime);
    runtime.sync = sync;
    runtime.ledger = ledger;
  }

  return runtime;
}

module.exports = {
  createKnowledgeRuntime,
  KnowledgeService,
  InMemoryGraphRepository,
  PersistentGraphRepository,
  ensureKnowledgeSchema,
  EvidenceEngine,
  ClaimEngine,
  KnowledgeEventBus,
  KnowledgeIngestor,
  KNOWLEDGE_EVENTS,
  NODE_TYPES,
  EDGE_TYPES,
  GRAPH_REPOSITORY_METHODS,
  assertGraphRepository,
  isGraphRepository,
  combineConfidences,
  calculateConfidenceFromEvidence,
  // SPEC-001B sync
  GraphSyncEngine,
  InMemorySyncLedger,
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
