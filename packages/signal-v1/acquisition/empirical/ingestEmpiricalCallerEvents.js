'use strict';

const { callStore } = require('../../storage/storeUtils');
const { persistClusterRelationship } = require('../../research/clusterRelationships');
const { FRONT_RUNNERS_CLUSTER_ID } = require('../../fixtures/frontRunnersCases');

/**
 * Persist normalized CALL events from empirical catalog — no synthetic timestamp adjustment.
 *
 * @param {object} store
 * @param {object} raw — candidate raw row with acquisitionPayload.catalogCalls
 */
async function ingestEmpiricalCallerEvents(store, raw) {
  const tokenAddress = raw.tokenAddress;
  const calls = raw.acquisitionPayload?.catalogCalls || [];
  const relationships = raw.acquisitionPayload?.clusterRelationships || [];

  await ensureCatalogSources(store, calls);

  for (const rel of relationships) {
    persistClusterRelationship(store, rel);
  }

  let inserted = 0;
  for (const call of calls) {
    const eventType = call.eventType || 'CALL';
    const occurredAt = new Date(call.occurredAt);
    const row = {
      tokenAddress,
      chain: 'solana',
      eventType,
      sourceType: call.sourceType || 'telegram',
      sourceId: call.sourceId,
      sourceClusterId: call.sourceClusterId,
      occurredAt,
      observedAt: call.observedAt ? new Date(call.observedAt) : occurredAt,
      ingestedAt: new Date(),
      confidence: call.confidence ?? 0.85,
      payload: call.payload || { message: 'Historical caller observation' },
      provenance: {
        ...(call.provenance || {}),
        provider: call.provenance?.provider || raw.provenance?.provider,
        externalId: call.provenance?.externalId || null,
        referenceUrl: call.provenance?.referenceUrl || null,
      },
    };

    const existing = (await callStore(store, 'getEventsForToken', tokenAddress)).find(
      e =>
        e.eventType === row.eventType &&
        e.sourceId === row.sourceId &&
        e.occurredAt.getTime() === row.occurredAt.getTime()
    );
    if (!existing) {
      await callStore(store, 'insertEvent', row);
      inserted += 1;
    }
  }

  return { inserted, callCount: calls.length };
}

async function ensureCatalogSources(store, calls) {
  const clusterIds = new Set(calls.map(c => c.sourceClusterId).filter(Boolean));
  for (const clusterId of clusterIds) {
    if (clusterId === FRONT_RUNNERS_CLUSTER_ID) {
      store.upsertCluster({
        id: clusterId,
        name: 'Front Runners research network',
        clusterType: 'promotional_network',
        confidence: 0.55,
        metadata: { relationshipBasis: 'historical_catalog' },
      });
    } else {
      store.upsertCluster({
        id: clusterId,
        name: clusterId,
        clusterType: 'unknown',
        confidence: 0.4,
        metadata: { relationshipBasis: 'historical_catalog' },
      });
    }
  }

  for (const call of calls) {
    if (!call.sourceId) continue;
    store.upsertSource({
      id: call.sourceId,
      name: call.sourceId,
      sourceType: call.sourceType || 'telegram',
      clusterId: call.sourceClusterId,
      active: true,
      metadata: { historicalCatalog: true },
    });
    if (call.sourceClusterId) {
      store.addClusterMember(call.sourceId, call.sourceClusterId);
    }
  }
}

module.exports = {
  ingestEmpiricalCallerEvents,
};
