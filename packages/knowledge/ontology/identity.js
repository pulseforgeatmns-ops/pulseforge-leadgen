'use strict';

const { createHash } = require('crypto');

/**
 * Deterministic node identity (SPEC-017).
 * Example: BTC + PriceTick + 2026-07-26T18:05:00Z + Coinbase → stable hash.
 *
 * @param {string[]} parts - ordered identity components
 * @param {object} [options]
 * @param {number} [options.length=32] - hex digest length
 * @returns {string}
 */
function deterministicId(parts, options = {}) {
  const length = options.length == null ? 32 : Number(options.length);
  if (!Array.isArray(parts) || parts.length === 0) {
    throw new Error('deterministicId requires at least one part');
  }
  const normalized = parts
    .map((part) => String(part).trim().toLowerCase())
    .join('|');
  return createHash('sha256').update(normalized).digest('hex').slice(0, length);
}

/**
 * @param {object} input
 * @param {string} input.domain
 * @param {string} input.entityType
 * @param {string} input.naturalKey
 * @returns {string}
 */
function entityId({ domain, entityType, naturalKey }) {
  return deterministicId([domain, entityType, naturalKey]);
}

/**
 * @param {object} input
 * @param {string} input.domain
 * @param {string} input.observationType
 * @param {string} input.subjectKey
 * @param {string} input.observedAt
 * @param {string} [input.venue]
 * @returns {string}
 */
function observationId({ domain, observationType, subjectKey, observedAt, venue }) {
  const parts = [domain, observationType, subjectKey, observedAt];
  if (venue) parts.push(venue);
  return deterministicId(parts);
}

module.exports = {
  deterministicId,
  entityId,
  observationId,
};
