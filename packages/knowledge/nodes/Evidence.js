'use strict';

const { createBaseNode, applyNodeUpdate } = require('../types/baseNode');
const { NODE_TYPES } = require('../types/nodeTypes');

/**
 * Evidence is a first-class node: an observed fact from a source system.
 *
 * @param {object} input
 * @param {string} input.tenantId
 * @param {string} input.sourceType - e.g. scout_insert, brevo_webhook, crm_field
 * @param {string} [input.sourceId] - durable id in the source system
 * @param {string} [input.summary]
 * @param {number} [input.confidence] - 0..1 prior/base confidence
 * @param {Record<string, unknown>} [input.payload]
 * @param {Record<string, unknown>} [input.metadata]
 * @param {string} [input.id]
 */
function createEvidenceNode(input) {
  if (!input.sourceType) {
    throw new Error('Evidence requires sourceType');
  }
  const confidence = clampConfidence(input.confidence == null ? 0.7 : input.confidence);
  const base = createBaseNode({
    tenantId: input.tenantId,
    type: NODE_TYPES.EVIDENCE,
    metadata: input.metadata,
    id: input.id,
  });
  return {
    ...base,
    sourceType: String(input.sourceType),
    sourceId: input.sourceId != null ? String(input.sourceId) : null,
    summary: input.summary != null ? String(input.summary) : null,
    confidence,
    payload: input.payload && typeof input.payload === 'object' ? { ...input.payload } : {},
  };
}

function updateEvidenceNode(node, patch) {
  const next = applyNodeUpdate(node, patch);
  for (const key of ['sourceType', 'sourceId', 'summary']) {
    if (Object.prototype.hasOwnProperty.call(patch, key)) {
      next[key] = patch[key] != null ? String(patch[key]) : null;
    }
  }
  if (Object.prototype.hasOwnProperty.call(patch, 'confidence')) {
    next.confidence = clampConfidence(patch.confidence);
  }
  if (patch.payload && typeof patch.payload === 'object') {
    next.payload = { ...(node.payload || {}), ...patch.payload };
  }
  return next;
}

function clampConfidence(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  if (n < 0) return 0;
  if (n > 1) return 1;
  return n;
}

module.exports = {
  createEvidenceNode,
  updateEvidenceNode,
  clampConfidence,
};
