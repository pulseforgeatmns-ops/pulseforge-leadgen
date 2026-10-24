'use strict';

const MarketHypothesisRegistry = require('./MarketHypothesisRegistry');
const CanonicalHypothesisEngine = require('./CanonicalHypothesisEngine');

module.exports = {
  ...MarketHypothesisRegistry,
  ...CanonicalHypothesisEngine,
};
