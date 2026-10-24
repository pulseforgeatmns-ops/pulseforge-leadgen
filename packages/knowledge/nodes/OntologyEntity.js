'use strict';

const { createBaseNode, applyNodeUpdate } = require('../types/baseNode');
const { getOntologyRegistry } = require('../ontology/OntologyRegistry');

/**
 * Generic domain entity node factory.
 * Used for ontology-registered entity types without bespoke node modules.
 *
 * @param {object} input
 * @param {string} input.tenantId
 * @param {string} input.type - registered entity type
 * @param {string} [input.name]
 * @param {string} [input.naturalKey]
 * @param {Record<string, unknown>} [input.metadata]
 * @param {string} [input.id]
 */
function createOntologyEntity(input) {
  const registry = getOntologyRegistry();
  if (!registry.entityTypes.has(input.type)) {
    throw new Error(`Unknown ontology entity type: ${input.type}`);
  }
  const base = createBaseNode({
    tenantId: input.tenantId,
    type: input.type,
    metadata: input.metadata,
    id: input.id,
  });
  return {
    ...base,
    name: input.name != null ? String(input.name) : null,
    naturalKey: input.naturalKey != null ? String(input.naturalKey) : null,
  };
}

/**
 * @param {object} node
 * @param {object} patch
 */
function updateOntologyEntity(node, patch) {
  const next = applyNodeUpdate(node, patch);
  for (const key of ['name', 'naturalKey']) {
    if (Object.prototype.hasOwnProperty.call(patch, key)) {
      next[key] = patch[key] != null ? String(patch[key]) : null;
    }
  }
  return next;
}

module.exports = {
  createOntologyEntity,
  updateOntologyEntity,
};
