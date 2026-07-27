'use strict';

const { entityId, observationId } = require('@pulseforge/knowledge/ontology');

const DOMAIN = 'market';

/**
 * @param {string} symbol
 * @returns {string}
 */
function assetId(symbol) {
  return entityId({
    domain: DOMAIN,
    entityType: 'asset',
    naturalKey: symbol,
  });
}

/**
 * @param {string} venue
 * @returns {string}
 */
function exchangeId(venue) {
  return entityId({
    domain: DOMAIN,
    entityType: 'exchange',
    naturalKey: venue,
  });
}

/**
 * @param {string} symbol
 * @param {string} venue
 * @returns {string}
 */
function contractId(symbol, venue) {
  return entityId({
    domain: DOMAIN,
    entityType: 'contract',
    naturalKey: `${symbol}@${venue}`,
  });
}

/**
 * @param {object} input
 * @param {string} input.observationType
 * @param {string} input.subjectKey
 * @param {string} input.observedAt
 * @param {string} [input.venue]
 * @returns {string}
 */
function marketObservationId(input) {
  return observationId({
    domain: DOMAIN,
    observationType: input.observationType,
    subjectKey: input.subjectKey,
    observedAt: input.observedAt,
    venue: input.venue,
  });
}

/**
 * Deterministic identity example from SPEC-017:
 * BTC + PriceTick + 2026-07-26T18:05:00Z + Coinbase
 *
 * @param {object} input
 * @param {string} input.asset
 * @param {string} input.observationType
 * @param {string} input.observedAt
 * @param {string} [input.venue]
 */
function priceTickId({ asset, observationType, observedAt, venue }) {
  return marketObservationId({
    observationType,
    subjectKey: asset,
    observedAt,
    venue,
  });
}

module.exports = {
  DOMAIN,
  assetId,
  exchangeId,
  contractId,
  marketObservationId,
  priceTickId,
};
