'use strict';

const { createBaseNode, applyNodeUpdate } = require('../types/baseNode');
const { NODE_TYPES } = require('../types/nodeTypes');

/**
 * @param {object} input
 * @param {string} input.tenantId
 * @param {string} [input.name]
 * @param {Record<string, unknown>} [input.metadata]
 * @param {string} [input.id]
 * @returns {import('../types/baseNode').BaseNode & { name: string|null }}
 */
function createCompany(input) {
  const base = createBaseNode({
    tenantId: input.tenantId,
    type: NODE_TYPES.COMPANY,
    metadata: input.metadata,
    id: input.id,
  });
  return { ...base, name: input.name != null ? String(input.name) : null };
}

/**
 * @param {ReturnType<typeof createCompany>} node
 * @param {object} patch
 */
function updateCompany(node, patch) {
  const next = applyNodeUpdate(node, patch);
  if (Object.prototype.hasOwnProperty.call(patch, 'name')) {
    next.name = patch.name != null ? String(patch.name) : null;
  }
  return next;
}

module.exports = { createCompany, updateCompany };
