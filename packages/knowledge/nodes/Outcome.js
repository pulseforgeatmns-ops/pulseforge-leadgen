'use strict';

const { createBaseNode, applyNodeUpdate } = require('../types/baseNode');
const { CORE_NODE_CATEGORIES } = require('../ontology/coreGraphInvariants');
const { buildProvenance } = require('../ontology/provenance');

/**
 * Outcome node — reality that validates claims (SPEC-017).
 *
 * @param {object} input
 * @param {string} input.tenantId
 * @param {string} input.outcomeType - domain outcome vocabulary id
 * @param {string} input.statement
 * @param {string} [input.subjectId]
 * @param {string} input.observedAt
 * @param {Record<string, unknown>} [input.metadata]
 * @param {Partial<import('../ontology/provenance').Provenance>} [input.provenance]
 * @param {string} [input.id]
 */
function createOutcomeNode(input) {
  if (!input.outcomeType) {
    throw new Error('Outcome requires outcomeType');
  }
  if (!input.statement) {
    throw new Error('Outcome requires statement');
  }
  if (!input.observedAt) {
    throw new Error('Outcome requires observedAt');
  }

  const provenance = buildProvenance({
    tenant: input.tenantId,
    observedAt: input.observedAt,
    ...input.provenance,
  });

  const base = createBaseNode({
    tenantId: input.tenantId,
    type: CORE_NODE_CATEGORIES.OUTCOME,
    metadata: input.metadata,
    id: input.id,
  });

  return {
    ...base,
    outcomeType: String(input.outcomeType),
    statement: String(input.statement),
    subjectId: input.subjectId != null ? String(input.subjectId) : null,
    observedAt: String(input.observedAt),
    provenance,
  };
}

/**
 * @param {object} node
 * @param {object} patch
 */
function updateOutcomeNode(node, patch) {
  const next = applyNodeUpdate(node, patch);
  for (const key of ['outcomeType', 'statement', 'subjectId', 'observedAt']) {
    if (Object.prototype.hasOwnProperty.call(patch, key)) {
      next[key] = patch[key] != null ? String(patch[key]) : null;
    }
  }
  return next;
}

module.exports = {
  createOutcomeNode,
  updateOutcomeNode,
};
