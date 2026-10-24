'use strict';

const { createBaseNode } = require('../types/baseNode');
const { CORE_NODE_CATEGORIES } = require('../ontology/coreGraphInvariants');
const { buildProvenance } = require('../ontology/provenance');

/**
 * Immutable observation node (SPEC-017 Rule 1).
 *
 * @param {object} input
 * @param {string} input.tenantId
 * @param {string} input.observationType - domain observation vocabulary id
 * @param {string} input.subjectId - node the observation is OBSERVED_ON
 * @param {string} input.observedAt
 * @param {Record<string, unknown>} [input.payload]
 * @param {Partial<import('../ontology/provenance').Provenance>} [input.provenance]
 * @param {Record<string, unknown>} [input.metadata]
 * @param {string} [input.id]
 */
function createObservationNode(input) {
  if (!input.observationType) {
    throw new Error('Observation requires observationType');
  }
  if (!input.subjectId) {
    throw new Error('Observation requires subjectId');
  }
  if (!input.observedAt) {
    throw new Error('Observation requires observedAt');
  }

  const provenance = buildProvenance({
    tenant: input.tenantId,
    observedAt: input.observedAt,
    ...input.provenance,
  });

  const base = createBaseNode({
    tenantId: input.tenantId,
    type: CORE_NODE_CATEGORIES.OBSERVATION,
    metadata: input.metadata,
    id: input.id,
    createdAt: provenance.recordedAt,
    updatedAt: provenance.recordedAt,
  });

  return {
    ...base,
    observationType: String(input.observationType),
    subjectId: String(input.subjectId),
    observedAt: String(input.observedAt),
    provenance,
    payload: input.payload && typeof input.payload === 'object' ? { ...input.payload } : {},
  };
}

/**
 * Observations are immutable — updates are forbidden (SPEC-017 Rule 1).
 *
 * @param {object} node
 * @param {object} _patch
 */
function updateObservationNode(node, _patch) {
  throw new Error(
    `Observations are immutable (Rule 1): cannot update observation ${node.id}`
  );
}

module.exports = {
  createObservationNode,
  updateObservationNode,
};
