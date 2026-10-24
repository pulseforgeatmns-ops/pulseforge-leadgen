'use strict';

const { createBaseNode, applyNodeUpdate } = require('../types/baseNode');
const { NODE_TYPES } = require('../types/nodeTypes');

/**
 * @param {object} input
 * @param {string} input.tenantId
 * @param {string} [input.name]
 * @param {string} [input.email]
 * @param {string} [input.title]
 * @param {Record<string, unknown>} [input.metadata]
 * @param {string} [input.id]
 */
function createPerson(input) {
  const base = createBaseNode({
    tenantId: input.tenantId,
    type: NODE_TYPES.PERSON,
    metadata: input.metadata,
    id: input.id,
  });
  return {
    ...base,
    name: input.name != null ? String(input.name) : null,
    email: input.email != null ? String(input.email) : null,
    title: input.title != null ? String(input.title) : null,
  };
}

function updatePerson(node, patch) {
  const next = applyNodeUpdate(node, patch);
  for (const key of ['name', 'email', 'title']) {
    if (Object.prototype.hasOwnProperty.call(patch, key)) {
      next[key] = patch[key] != null ? String(patch[key]) : null;
    }
  }
  return next;
}

module.exports = { createPerson, updatePerson };
