'use strict';

/**
 * Shared Knowledge + Max runtime singleton (SPEC-014).
 *
 * Boots PersistentGraphRepository + GraphSyncEngine + KnowledgeWriter when
 * dual-write is enabled. Command Deck / Max / dual-write share one graph.
 */

const {
  createKnowledgeRuntime,
  PersistentGraphRepository,
  ensureKnowledgeSchema,
  InMemorySyncLedger,
} = require('../packages/knowledge');
const { PostgresSyncLedger } = require('../packages/knowledge/sync/PostgresSyncLedger');
const {
  KnowledgeWriter,
  ensureDualWriteSchema,
  recordFlightStage,
  FLIGHT_STAGES,
} = require('../packages/knowledge/dualWrite');
const { createMaxReasoningRuntime } = require('../packages/max');

let bootPromise = null;

function dualWriteEnabled() {
  const flag = process.env.KNOWLEDGE_DUAL_WRITE;
  if (flag === '0' || flag === 'false' || flag === 'off') return false;
  return true;
}

/**
 * @param {object} [options]
 * @param {boolean} [options.reset]
 * @param {{ query: Function }} [options.pool]
 * @param {boolean} [options.inMemory] - force in-memory (tests)
 * @param {boolean} [options.disableLlm]
 * @param {object} [options.tenantPolicies]
 */
function getKnowledgeBoot(options = {}) {
  if (options.reset) {
    bootPromise = null;
  }
  if (!bootPromise) {
    bootPromise = boot(options);
  }
  return bootPromise;
}

async function boot(options = {}) {
  const useMemory = options.inMemory === true || !dualWriteEnabled();
  let pool = options.pool || null;
  let knowledgeRuntime;
  let writer = null;
  let schemaReady = false;

  if (!useMemory) {
    pool = pool || require('../db');
    try {
      await ensureKnowledgeSchema(pool);
      await ensureDualWriteSchema(pool);
      schemaReady = true;
    } catch (err) {
      console.error('[knowledgeRuntime] schema ensure failed — falling back to memory:', err.message);
      return bootMemory(options, err);
    }

    const repository = new PersistentGraphRepository(pool);
    const ledger = new PostgresSyncLedger(pool);
    knowledgeRuntime = createKnowledgeRuntime({
      repository,
      withSync: true,
      startIngestor: true,
      ledger,
    });

    writer = new KnowledgeWriter({
      pool,
      sync: knowledgeRuntime.sync,
      onLog: (m) => {
        if (m.level === 'error') {
          console.error('[knowledgeDualWrite]', m.msg, m.error || '');
        }
      },
    });
  } else {
    knowledgeRuntime = createKnowledgeRuntime({
      withSync: true,
      startIngestor: true,
    });
  }

  const max = createMaxReasoningRuntime({
    knowledge: knowledgeRuntime.knowledge,
    withSync: false,
    startIngestor: false,
    tenantPolicies: options.tenantPolicies,
    disableLlm: options.disableLlm === true,
  });
  // Attach shared sync/writer onto the max handle for compose-side flight stages
  max.runtime = knowledgeRuntime;
  max.writer = writer;
  max.pool = pool;
  max.dualWriteEnabled = !useMemory && schemaReady;

  const originalCompose = max.compose.bind(max);
  max.compose = async function composeWithFlight(input) {
    const model = await originalCompose(input);
    if (writer && pool && input && input.flightId) {
      const tenantId = String(input.tenantId);
      const entityId = input.entityId || null;
      const entityType = input.entityType || 'prospect';
      const flightId = input.flightId;
      try {
        await recordFlightStage(pool, {
          flightId,
          tenantId,
          entityId,
          entityType,
          stage: FLIGHT_STAGES.REASONING_GENERATED,
          metadata: { via: 'compose' },
        });
        await recordFlightStage(pool, {
          flightId,
          tenantId,
          entityId,
          entityType,
          stage: FLIGHT_STAGES.MEMORY_UPDATED,
          metadata: { via: 'compose' },
        });
        await recordFlightStage(pool, {
          flightId,
          tenantId,
          entityId,
          entityType,
          stage: FLIGHT_STAGES.BRIEFING_UPDATED,
          metadata: { via: 'compose' },
        });
        await recordFlightStage(pool, {
          flightId,
          tenantId,
          entityId,
          entityType,
          stage: FLIGHT_STAGES.COMMAND_DECK_REFRESHED,
          metadata: {
            via: 'compose',
            cardCount: Array.isArray(model?.cards) ? model.cards.length : null,
          },
        });
      } catch (err) {
        console.warn('[knowledgeRuntime] flight compose stages failed:', err.message);
      }
    }
    return model;
  };

  return {
    knowledge: knowledgeRuntime.knowledge,
    sync: knowledgeRuntime.sync,
    ledger: knowledgeRuntime.ledger,
    bus: knowledgeRuntime.bus,
    writer,
    max,
    pool,
    dualWriteEnabled: !useMemory && schemaReady,
    inMemory: useMemory,
  };
}

function bootMemory(options, schemaError) {
  const knowledgeRuntime = createKnowledgeRuntime({
    withSync: true,
    startIngestor: true,
  });
  const max = createMaxReasoningRuntime({
    knowledge: knowledgeRuntime.knowledge,
    withSync: false,
    startIngestor: false,
    tenantPolicies: options.tenantPolicies,
    disableLlm: options.disableLlm === true,
  });
  max.runtime = knowledgeRuntime;
  max.writer = null;
  max.dualWriteEnabled = false;
  return {
    knowledge: knowledgeRuntime.knowledge,
    sync: knowledgeRuntime.sync,
    ledger: knowledgeRuntime.ledger || new InMemorySyncLedger(),
    bus: knowledgeRuntime.bus,
    writer: null,
    max,
    pool: null,
    dualWriteEnabled: false,
    inMemory: true,
    schemaError: schemaError ? String(schemaError.message || schemaError) : null,
  };
}

module.exports = {
  getKnowledgeBoot,
  dualWriteEnabled,
};
