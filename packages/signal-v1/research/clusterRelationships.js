'use strict';

const CLUSTER_RELATIONSHIP = Object.freeze({
  INDEPENDENT: 'INDEPENDENT',
  CORRELATED: 'CORRELATED',
  UNKNOWN: 'UNKNOWN',
});

/**
 * @param {object} store
 * @param {string} clusterA
 * @param {string} clusterB
 */
function getClusterPairRelationship(store, clusterA, clusterB) {
  if (!clusterA || !clusterB) return CLUSTER_RELATIONSHIP.UNKNOWN;
  if (clusterA === clusterB) return CLUSTER_RELATIONSHIP.CORRELATED;

  const key = pairKey(clusterA, clusterB);
  const explicit = store.clusterRelationships?.get?.(key);
  if (explicit?.relationship) return explicit.relationship;

  const a = store.clusters?.get?.(clusterA);
  const b = store.clusters?.get?.(clusterB);
  const relatedA = a?.metadata?.relatedClusterIds || [];
  const relatedB = b?.metadata?.relatedClusterIds || [];
  if (relatedA.includes(clusterB) || relatedB.includes(clusterA)) {
    return CLUSTER_RELATIONSHIP.CORRELATED;
  }

  const relMeta = a?.metadata?.relationshipEvidence?.[clusterB] || b?.metadata?.relationshipEvidence?.[clusterA];
  if (relMeta?.relationship) return relMeta.relationship;

  if (a?.clusterType === 'unknown' && b?.clusterType === 'unknown') {
    return CLUSTER_RELATIONSHIP.UNKNOWN;
  }

  if (a?.metadata?.relationshipBasis === 'acquisition_seed' && b?.metadata?.relationshipBasis === 'acquisition_seed') {
    return CLUSTER_RELATIONSHIP.INDEPENDENT;
  }

  return CLUSTER_RELATIONSHIP.UNKNOWN;
}

function pairKey(a, b) {
  return a < b ? `${a}|${b}` : `${b}|${a}`;
}

/**
 * @param {object} store
 * @param {{ clusterA: string, clusterB: string, relationship: string, confidence?: number, basis?: string, provenance?: object }} row
 */
function persistClusterRelationship(store, row) {
  const key = pairKey(row.clusterA, row.clusterB);
  const record = {
    clusterA: row.clusterA,
    clusterB: row.clusterB,
    relationship: row.relationship,
    confidence: row.confidence ?? null,
    basis: row.basis || null,
    provenance: row.provenance || {},
  };
  if (!store.clusterRelationships) {
    store.clusterRelationships = new Map();
  }
  store.clusterRelationships.set(key, record);
  return record;
}

module.exports = {
  CLUSTER_RELATIONSHIP,
  getClusterPairRelationship,
  persistClusterRelationship,
  pairKey,
};
