'use strict';

/**
 * RecommendationProvider — maps ranked claims / observations to domain actions.
 * Exactly one provider is active for a given domain.
 *
 * @typedef {object} RecommendationProvider
 * @property {string} [id]
 * @property {(input: object) => object|Promise<object>} generate
 */

/**
 * @param {RecommendationProvider} provider
 */
function assertRecommendationProvider(provider) {
  if (!provider || typeof provider !== 'object') {
    throw new Error('RecommendationProvider must be an object');
  }
  if (typeof provider.generate !== 'function') {
    throw new Error('RecommendationProvider requires generate(input)');
  }
}

module.exports = {
  assertRecommendationProvider,
};
