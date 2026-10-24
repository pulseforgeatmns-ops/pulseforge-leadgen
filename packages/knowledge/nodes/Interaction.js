'use strict';

const { createBaseNode, applyNodeUpdate } = require('../types/baseNode');
const { NODE_TYPES } = require('../types/nodeTypes');

/**
 * @param {object} input
 * @param {string} input.tenantId
 * @param {string} [input.channel]
 * @param {string} [input.actionType]
 * @param {string} [input.summary]
 * @param {string} [input.occurredAt]
 * @param {Record<string, unknown>} [input.metadata]
 * @param {string} [input.id]
 */
function createInteraction(input) {
  const base = createBaseNode({
    tenantId: input.tenantId,
    type: NODE_TYPES.INTERACTION,
    metadata: input.metadata,
    id: input.id,
  });
  return {
    ...base,
    channel: input.channel != null ? String(input.channel) : null,
    actionType: input.actionType != null ? String(input.actionType) : null,
    summary: input.summary != null ? String(input.summary) : null,
    occurredAt: input.occurredAt || base.createdAt,
  };
}

function updateInteraction(node, patch) {
  const next = applyNodeUpdate(node, patch);
  for (const key of ['channel', 'actionType', 'summary', 'occurredAt']) {
    if (Object.prototype.hasOwnProperty.call(patch, key)) {
      next[key] = patch[key] != null ? String(patch[key]) : null;
    }
  }
  return next;
}

module.exports = { createInteraction, updateInteraction };
