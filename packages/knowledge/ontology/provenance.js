'use strict';

/**
 * Provenance metadata contract (SPEC-017 Rule 4).
 *
 * @typedef {object} Provenance
 * @property {string} origin - source system or adapter name
 * @property {string} adapter - adapter that produced the node
 * @property {string|null} [rawReference] - durable pointer to raw payload
 * @property {string} observedAt - ISO timestamp when reality was observed
 * @property {string} recordedAt - ISO timestamp when node was ingested
 * @property {string} [version] - schema or adapter version
 * @property {string} tenant - tenant scope
 * @property {number|null} [confidence] - 0..1 prior confidence
 */

const PROVENANCE_FIELDS = Object.freeze([
  'origin',
  'adapter',
  'rawReference',
  'observedAt',
  'recordedAt',
  'version',
  'tenant',
  'confidence',
]);

/**
 * @param {Partial<Provenance> & { tenant: string, observedAt: string }} input
 * @returns {Provenance}
 */
function buildProvenance(input) {
  if (!input.tenant) {
    throw new Error('Provenance requires tenant');
  }
  if (!input.observedAt) {
    throw new Error('Provenance requires observedAt');
  }
  const recordedAt = input.recordedAt || new Date().toISOString();
  return {
    origin: input.origin != null ? String(input.origin) : 'unknown',
    adapter: input.adapter != null ? String(input.adapter) : 'unknown',
    rawReference: input.rawReference != null ? String(input.rawReference) : null,
    observedAt: String(input.observedAt),
    recordedAt: String(recordedAt),
    version: input.version != null ? String(input.version) : '1',
    tenant: String(input.tenant),
    confidence:
      input.confidence == null || !Number.isFinite(Number(input.confidence))
        ? null
        : clamp01(input.confidence),
  };
}

/**
 * @param {Provenance} provenance
 * @returns {string[]}
 */
function validateProvenance(provenance) {
  const errors = [];
  for (const field of ['origin', 'adapter', 'observedAt', 'recordedAt', 'tenant']) {
    if (!provenance[field]) {
      errors.push(`provenance.${field} is required`);
    }
  }
  return errors;
}

function clamp01(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  if (n < 0) return 0;
  if (n > 1) return 1;
  return n;
}

module.exports = {
  PROVENANCE_FIELDS,
  buildProvenance,
  validateProvenance,
};
