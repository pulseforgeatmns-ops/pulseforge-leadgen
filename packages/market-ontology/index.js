'use strict';

const {
  MARKET_ENTITY_TYPES,
  MARKET_SUBJECT_TYPES,
  MARKET_OBSERVATION_TYPES,
  MARKET_CLAIM_VOCABULARY,
  MARKET_OUTCOME_VOCABULARY,
  MARKET_RELATIONSHIP_TYPES,
  createMarketOntology,
} = require('./MarketOntology');
const {
  DOMAIN,
  assetId,
  exchangeId,
  contractId,
  marketObservationId,
  priceTickId,
} = require('./identities');
const { registerMarketOntology } = require('./register');

module.exports = {
  MARKET_ENTITY_TYPES,
  MARKET_SUBJECT_TYPES,
  MARKET_OBSERVATION_TYPES,
  MARKET_CLAIM_VOCABULARY,
  MARKET_OUTCOME_VOCABULARY,
  MARKET_RELATIONSHIP_TYPES,
  createMarketOntology,
  DOMAIN,
  assetId,
  exchangeId,
  contractId,
  marketObservationId,
  priceTickId,
  registerMarketOntology,
};
