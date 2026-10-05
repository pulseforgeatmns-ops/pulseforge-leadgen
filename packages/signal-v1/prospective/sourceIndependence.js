'use strict';

const { CLUSTER_RELATIONSHIP } = require('./constants');

/**
 * Resolve cluster relationship for strict prospective convergence.
 *
 * @param {object} store
 * @param {object} sourceRegistryEntry — from source registry
 * @param {object} [observationProvenance]
 */
function resolveClusterRelationship(store, sourceRegistryEntry, observationProvenance = {}) {
  if (observationProvenance.forwardedFromSourceId) {
    return CLUSTER_RELATIONSHIP.CORRELATED;
  }
  if (observationProvenance.forwardedFromClusterId) {
    return CLUSTER_RELATIONSHIP.CORRELATED;
  }
  if (sourceRegistryEntry?.clusterRelationshipStatus) {
    return sourceRegistryEntry.clusterRelationshipStatus;
  }
  const clusterId = sourceRegistryEntry?.clusterId;
  if (!clusterId) return CLUSTER_RELATIONSHIP.UNKNOWN;
  const cluster = store.clusters?.get?.(clusterId);
  if (cluster?.metadata?.relationshipStatus) {
    return cluster.metadata.relationshipStatus;
  }
  if (cluster?.clusterType === 'promotional_network' || cluster?.clusterType === 'copy_network') {
    return CLUSTER_RELATIONSHIP.CORRELATED;
  }
  if (cluster?.clusterType === 'unknown') {
    return CLUSTER_RELATIONSHIP.UNKNOWN;
  }
  return CLUSTER_RELATIONSHIP.INDEPENDENT;
}

function isProvenIndependent(relationship) {
  return relationship === CLUSTER_RELATIONSHIP.INDEPENDENT;
}

module.exports = {
  resolveClusterRelationship,
  isProvenIndependent,
};
