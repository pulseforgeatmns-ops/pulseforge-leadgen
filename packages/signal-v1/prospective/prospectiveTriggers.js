'use strict';

const { PROSPECTIVE_FROZEN_HYPOTHESIS } = require('./constants');
const { knowledgeAt } = require('./knowledgeClock');
const { isProvenIndependent, resolveClusterRelationship } = require('./sourceIndependence');

function isValidEmpiricalCall(event) {
  if (event.eventType !== 'CALL') return false;
  if (event.provenance?.dataClass === 'PROCEDURAL' || event.payload?.procedural === true) return false;
  if (!event.sourceId) return false;
  if (!event.tokenAddress) return false;
  return true;
}

function clusterIdForEvent(store, event) {
  return event.sourceClusterId || store.getClusterIdForSource(event.sourceId) || event.sourceId;
}

/**
 * FIRST_CALLER at knowledge time for previously unseen token (within episode cooldown handled upstream).
 */
function evaluateProspectiveFirstCaller(store, tokenAddress, asOfKnowledge) {
  const asOfMs = new Date(asOfKnowledge).getTime();
  const events = store
    .getEventsForToken(tokenAddress)
    .filter(isValidEmpiricalCall)
    .map(e => ({
      ...e,
      knowledgeAt: knowledgeAt(e.occurredAt, e.ingestedAt),
    }))
    .filter(e => e.knowledgeAt.getTime() <= asOfMs)
    .sort((a, b) => a.knowledgeAt - b.knowledgeAt || a.id.localeCompare(b.id));

  const first = events[0];
  if (!first) return null;

  return {
    occurredAt: first.knowledgeAt,
    originalOccurredAt: first.occurredAt,
    ingestedAt: first.ingestedAt,
    triggerEventIds: [first.id],
    evidenceEventIds: [first.id],
    metadata: {
      sourceId: first.sourceId,
      sourceClusterId: clusterIdForEvent(store, first),
      ingestionLatencyMs: first.ingestedAt.getTime() - first.occurredAt.getTime(),
      knowledgeAt: first.knowledgeAt.toISOString(),
    },
  };
}

/**
 * Strict INDEPENDENT_CONVERGENCE — PROVEN independent clusters only; knowledge clock enforced.
 */
function evaluateProspectiveIndependentConvergence(store, tokenAddress, asOfKnowledge, registryBySourceId = new Map()) {
  const cfg = PROSPECTIVE_FROZEN_HYPOTHESIS;
  const asOfMs = new Date(asOfKnowledge).getTime();

  const events = store
    .getEventsForToken(tokenAddress)
    .filter(isValidEmpiricalCall)
    .map(e => ({
      ...e,
      knowledgeAt: knowledgeAt(e.occurredAt, e.ingestedAt),
    }))
    .filter(e => e.knowledgeAt.getTime() <= asOfMs)
    .sort((a, b) => a.knowledgeAt - b.knowledgeAt || a.id.localeCompare(b.id));

  if (events.length < cfg.minProvenIndependentClusters) return null;

  let triggerKnowledgeAt = null;
  let qualifyingClusters = [];
  let evidenceEventIds = [];

  for (let i = 0; i < events.length; i += 1) {
    const anchor = events[i];
    const windowStartMs = anchor.occurredAt.getTime();
    const windowEndMs = windowStartMs + cfg.convergenceWindowMinutes * 60 * 1000;

    const inWindow = events.filter(
      e =>
        e.occurredAt.getTime() >= windowStartMs &&
        e.occurredAt.getTime() <= windowEndMs
    );

    const clusterMap = new Map();
    for (const e of inWindow) {
      const clusterId = clusterIdForEvent(store, e);
      const registry = registryBySourceId.get(e.sourceId) || null;
      const relationship = resolveClusterRelationship(store, registry, e.provenance || e.payload || {});
      if (!isProvenIndependent(relationship)) continue;
      if (!clusterMap.has(clusterId)) {
        clusterMap.set(clusterId, { clusterId, firstKnowledgeAt: e.knowledgeAt, events: [e] });
      } else {
        clusterMap.get(clusterId).events.push(e);
      }
    }

    if (clusterMap.size < cfg.minProvenIndependentClusters) continue;

    const clusters = [...clusterMap.values()];
    const knowledgeTimes = clusters.map(c => c.firstKnowledgeAt.getTime());
    const secondKnowledgeMs = Math.max(...knowledgeTimes);
    if (secondKnowledgeMs > asOfMs) continue;

    triggerKnowledgeAt = new Date(secondKnowledgeMs);
    qualifyingClusters = clusters.map(c => c.clusterId);
    evidenceEventIds = inWindow.map(e => e.id);
    break;
  }

  if (!triggerKnowledgeAt) return null;

  return {
    occurredAt: triggerKnowledgeAt,
    triggerEventIds: evidenceEventIds,
    evidenceEventIds,
    metadata: {
      independentClusterCount: qualifyingClusters.length,
      clusterIds: qualifyingClusters,
      convergenceWindowMinutes: cfg.convergenceWindowMinutes,
      knowledgeAt: triggerKnowledgeAt.toISOString(),
      strictProvenIndependent: true,
    },
  };
}

module.exports = {
  evaluateProspectiveFirstCaller,
  evaluateProspectiveIndependentConvergence,
  isValidEmpiricalCall,
};
