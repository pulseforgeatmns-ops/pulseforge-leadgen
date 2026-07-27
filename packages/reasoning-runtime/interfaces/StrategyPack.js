'use strict';

/**
 * StrategyPack — domain-specific reasoning meaning.
 *
 * The runtime invokes these methods in order and never branches on domain type.
 *
 * @typedef {object} StrategyPack
 * @property {string} id
 * @property {string} domain
 * @property {(input: object) => object|void|Promise<object|void>} initialize
 * @property {() => object|object[]|Promise<object|object[]>} buildEvidence
 * @property {() => object|object[]|Promise<object|object[]>} buildClaims
 * @property {() => object[]|Promise<object[]>} findHistoricalAnalogs
 * @property {() => object|Promise<object>} rankClaims
 * @property {() => object|Promise<object>} generateRecommendations
 * @property {() => object|Promise<object>} explain
 */

const REQUIRED_METHODS = [
  'initialize',
  'buildEvidence',
  'buildClaims',
  'findHistoricalAnalogs',
  'rankClaims',
  'generateRecommendations',
  'explain',
];

/**
 * @param {StrategyPack} pack
 */
function assertStrategyPack(pack) {
  if (!pack || typeof pack !== 'object') {
    throw new Error('StrategyPack must be an object');
  }
  if (!pack.id) throw new Error('StrategyPack requires id');
  if (!pack.domain) throw new Error('StrategyPack requires domain');
  for (const method of REQUIRED_METHODS) {
    if (typeof pack[method] !== 'function') {
      throw new Error(`StrategyPack ${pack.id} requires ${method}()`);
    }
  }
}

module.exports = {
  assertStrategyPack,
  REQUIRED_METHODS,
};
