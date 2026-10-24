'use strict';

/**
 * Deterministic knowledge node IDs derived from CRM / source identities.
 * Same tenant + source entity always maps to the same graph node.
 */

function normalizeTenant(tenantId) {
  if (tenantId == null || tenantId === '') {
    throw new Error('tenantId is required for stable ids');
  }
  return String(tenantId);
}

function normalizeSourceId(sourceId) {
  if (sourceId == null || sourceId === '') {
    throw new Error('sourceId is required for stable ids');
  }
  return String(sourceId);
}

function companyNodeId(tenantId, companyId) {
  return `company:${normalizeTenant(tenantId)}:${normalizeSourceId(companyId)}`;
}

function personNodeId(tenantId, prospectId) {
  return `person:${normalizeTenant(tenantId)}:${normalizeSourceId(prospectId)}`;
}

function interactionNodeId(tenantId, touchpointId) {
  return `interaction:${normalizeTenant(tenantId)}:${normalizeSourceId(touchpointId)}`;
}

function stableEvidenceId(tenantId, sourceType, sourceId) {
  if (!sourceType) throw new Error('sourceType is required for evidence ids');
  return `evidence:${normalizeTenant(tenantId)}:${sourceType}:${normalizeSourceId(sourceId)}`;
}

/**
 * Sync ledger key for a source mutation / rebuild row.
 * @param {string} tenantId
 * @param {string} entityKind
 * @param {string|number} entityId
 * @param {string} [revision] - updated_at or content hash; omit for identity-only
 */
function syncIdempotencyKey(tenantId, entityKind, entityId, revision) {
  const base = `sync:${normalizeTenant(tenantId)}:${entityKind}:${normalizeSourceId(entityId)}`;
  return revision != null && revision !== '' ? `${base}:${revision}` : base;
}

module.exports = {
  companyNodeId,
  personNodeId,
  interactionNodeId,
  stableEvidenceId,
  syncIdempotencyKey,
};
