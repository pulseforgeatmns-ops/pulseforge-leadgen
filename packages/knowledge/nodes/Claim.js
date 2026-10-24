'use strict';

const { createBaseNode, applyNodeUpdate } = require('../types/baseNode');
const { NODE_TYPES } = require('../types/nodeTypes');

/**
 * A Claim is a derived assertion supported by evidence.
 *
 * @param {object} input
 * @param {string} input.tenantId
 * @param {string} input.statement - human-readable claim text
 * @param {string} [input.status] - active | invalidated | merged
 * @param {number} [input.confidence]
 * @param {string} [input.reason]
 * @param {Record<string, unknown>} [input.metadata]
 * @param {string} [input.id]
 */
function createClaimNode(input) {
  if (!input.statement) {
    throw new Error('Claim requires statement');
  }
  const base = createBaseNode({
    tenantId: input.tenantId,
    type: NODE_TYPES.CLAIM,
    metadata: input.metadata,
    id: input.id,
  });
  return {
    ...base,
    statement: String(input.statement),
    status: input.status || 'active',
    confidence: clamp01(input.confidence == null ? 0 : input.confidence),
    reason: input.reason != null ? String(input.reason) : null,
  };
}

function updateClaimNode(node, patch) {
  const next = applyNodeUpdate(node, patch);
  if (Object.prototype.hasOwnProperty.call(patch, 'statement')) {
    next.statement = patch.statement != null ? String(patch.statement) : node.statement;
  }
  if (Object.prototype.hasOwnProperty.call(patch, 'status')) {
    next.status = String(patch.status);
  }
  if (Object.prototype.hasOwnProperty.call(patch, 'confidence')) {
    next.confidence = clamp01(patch.confidence);
  }
  if (Object.prototype.hasOwnProperty.call(patch, 'reason')) {
    next.reason = patch.reason != null ? String(patch.reason) : null;
  }
  return next;
}

function clamp01(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  if (n < 0) return 0;
  if (n > 1) return 1;
  return n;
}

module.exports = {
  createClaimNode,
  updateClaimNode,
};
