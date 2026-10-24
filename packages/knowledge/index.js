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
} = require('./sync');

/**
 * Create a ready-to-use knowledge runtime (in-memory by default).
 *
 * @param {object} [options]
 * @param {import('./repositories/GraphRepository').GraphRepository} [options.repository]
 * @param {boolean} [options.startIngestor=true]
 * @param {boolean} [options.withSync=true] - attach GraphSyncEngine + ledger
 * @param {object} [options.ledger] - sync ledger (default InMemorySyncLedger)
 * @param {(m: object) => void} [options.onQueryMetrics] - SPEC-001C metrics hook
 */
function createKnowledgeRuntime(options = {}) {
  const repository = options.repository || new InMemoryGraphRepository();
  assertGraphRepository(repository);
  const knowledge = new KnowledgeService({
    repository,
    onQueryMetrics: options.onQueryMetrics,
  });
  const bus = new KnowledgeEventBus();
  const ingestor = new KnowledgeIngestor({ knowledge, bus });
  if (options.startIngestor !== false) {
    ingestor.start();
  }

  const runtime = { knowledge, repository, bus, ingestor };

  if (options.withSync !== false) {
    const { sync, ledger } = createGraphSyncEngine(runtime, {
      ledger: options.ledger,
    });
    runtime.sync = sync;
    runtime.ledger = ledger;
  }

  return runtime;
}

const {
  QueryEngine,
  detectRepositoryType,
  MetricsCollector,
  MetricsSink,
  DEFAULT_RELATED_DEPTH,
  MAX_RELATED_DEPTH,
  DEFAULT_PATH_DEPTH,
  MAX_PATH_DEPTH,
} = require('./query');

const dualWrite = require('./dualWrite');
const ontology = require('./ontology');

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
  // SPEC-001C query engine
  QueryEngine,
  detectRepositoryType,
  MetricsCollector,
  MetricsSink,
  DEFAULT_RELATED_DEPTH,
  MAX_RELATED_DEPTH,
  DEFAULT_PATH_DEPTH,
  MAX_PATH_DEPTH,
  // SPEC-014
  dualWrite,
  KnowledgeWriter: dualWrite.KnowledgeWriter,
  OPERATIONAL_EVENTS: dualWrite.OPERATIONAL_EVENTS,
  FLIGHT_STAGES: dualWrite.FLIGHT_STAGES,
  ensureDualWriteSchema: dualWrite.ensureDualWriteSchema,
  // SPEC-017
  ontology,
  getOntologyRegistry: ontology.getOntologyRegistry,
  registerDomainOntology: ontology.registerDomainOntology,
  createDomainOntology: ontology.createDomainOntology,
  CORE_NODE_CATEGORIES: ontology.CORE_NODE_CATEGORIES,
  CORE_EDGE_TYPES: ontology.CORE_EDGE_TYPES,
  buildProvenance: ontology.buildProvenance,
  deterministicId: ontology.deterministicId,
};
