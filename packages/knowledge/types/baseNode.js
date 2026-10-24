'use strict';

const { randomUUID } = require('crypto');

/**
 * @typedef {object} BaseNode
 * @property {string} id
 * @property {string} tenantId
 * @property {string} type
 * @property {string} createdAt
 * @property {string} updatedAt
 * @property {Record<string, unknown>} metadata
 */

/**
 * @param {object} input
 * @param {string} input.tenantId
 * @param {string} input.type
 * @param {Record<string, unknown>} [input.metadata]
 * @param {string} [input.id]
 * @param {string} [input.createdAt]
 * @param {string} [input.updatedAt]
 * @returns {BaseNode}
 */
function createBaseNode({
  tenantId,
  type,
  metadata = {},
  id = randomUUID(),
  createdAt = new Date().toISOString(),
  updatedAt = createdAt,
}) {
  if (!tenantId || typeof tenantId !== 'string') {
    throw new Error('tenantId is required');
  }
  if (!type || typeof type !== 'string') {
    throw new Error('type is required');
  }
  return {
    id,
    tenantId: String(tenantId),
    type,
    createdAt,
    updatedAt,
    metadata: metadata && typeof metadata === 'object' ? { ...metadata } : {},
  };
}

/**
 * @param {BaseNode} node
 * @param {Partial<BaseNode> & Record<string, unknown>} patch
 * @returns {BaseNode}
 */
function applyNodeUpdate(node, patch = {}) {
  const { id: _id, tenantId: _tenantId, type: _type, createdAt: _createdAt, ...rest } = patch;
  const next = {
    ...node,
    ...rest,
    id: node.id,
    tenantId: node.tenantId,
    type: node.type,
    createdAt: node.createdAt,
    updatedAt: new Date().toISOString(),
  };
  if (rest.metadata && typeof rest.metadata === 'object') {
    next.metadata = { ...(node.metadata || {}), ...rest.metadata };
  }
  return next;
}

module.exports = {
  createBaseNode,
  applyNodeUpdate,
};
