'use strict';

/**
 * SPEC-143 — Scout Acquisition Intelligence Memory public API.
 * Scout owns durable acquisition intelligence that compounds across investigations.
 */

const types = require('./types');
const confidence = require('./MemoryConfidence');
const extraction = require('./KnowledgeExtraction');
const store = require('./MemoryStore');
const retrieval = require('./MemoryRetrieval');
const startingPoint = require('./InvestigationStartingPoint');
const contradictions = require('./ContradictionMemory');
const graph = require('./MemoryGraph');

const MEMORY_EVENTS = Object.freeze({
  LOADED: 'SCOUT_MEMORY_LOADED',
  EXTRACTED: 'SCOUT_MEMORY_EXTRACTED',
  PERSISTED: 'SCOUT_MEMORY_PERSISTED',
  CONFLICT: 'SCOUT_MEMORY_CONFLICT',
});

/** @type {object[]} */
const eventLog = [];

function emitMemoryEvent(event, payload = {}) {
  eventLog.push({
    event,
    at: new Date().toISOString(),
    ...payload,
  });
}

function listMemoryLog() {
  return eventLog.slice();
}

function clearMemoryLog() {
  eventLog.length = 0;
}

/**
 * Persist knowledge extracted from a completed investigation.
 * @param {object} investigationResult
 * @param {object} context — { tenantId, missionId, mission, store }
 * @returns {Promise<object>}
 */
async function persistInvestigationKnowledge(investigationResult, context = {}) {
  const knowledge = extraction.extractKnowledgeFromInvestigation(investigationResult, context);
  emitMemoryEvent(MEMORY_EVENTS.EXTRACTED, {
    tenantId: context.tenantId,
    missionId: context.missionId,
    counts: knowledge.counts,
  });

  const tenantId = context.tenantId || context.mission?.tenantId;
  if (!tenantId) {
    return { persisted: false, knowledge, reason: 'missing_tenant_id' };
  }

  const memoryStore =
    context.store || retrieval.getDefaultStore(context.opts || {});
  const result = await memoryStore.persistKnowledge(tenantId, knowledge, context.opts);

  emitMemoryEvent(MEMORY_EVENTS.PERSISTED, {
    tenantId,
    missionId: context.missionId,
    counts: result.counts,
    conflicts: result.conflicts?.length || 0,
  });

  for (const conflict of result.conflicts || []) {
    emitMemoryEvent(MEMORY_EVENTS.CONFLICT, { tenantId, conflict });
  }

  return { ...result, knowledge };
}

module.exports = {
  ...types,
  ...confidence,
  ...extraction,
  ...store,
  ...retrieval,
  ...startingPoint,
  ...contradictions,
  ...graph,
  MEMORY_EVENTS,
  emitMemoryEvent,
  listMemoryLog,
  clearMemoryLog,
  persistInvestigationKnowledge,
  ...require('./TerminologyLearning'),
};
