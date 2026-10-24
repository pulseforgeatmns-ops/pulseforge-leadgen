'use strict';

const {
  assertRecommendationProvider,
} = require('../interfaces/RecommendationProvider');

/**
 * NextBestActionProvider — CRM recommendation surface.
 * Wraps an injected RecommendationBuilder (or compatible generate/build fn).
 */
class NextBestActionProvider {
  /**
   * @param {object} deps
   * @param {{ build: Function }|{ generate: Function }|Function} deps.builder
   * @param {string} [deps.id]
   */
  constructor(deps) {
    if (!deps || deps.builder == null) {
      throw new Error('NextBestActionProvider requires builder');
    }
    this.id = deps.id || 'next-best-action';
    this._builder = deps.builder;
    assertRecommendationProvider(this);
  }

  /**
   * @param {object} input
   */
  generate(input) {
    if (typeof this._builder === 'function') {
      return this._builder(input);
    }
    if (typeof this._builder.generate === 'function') {
      return this._builder.generate(input);
    }
    if (typeof this._builder.build === 'function') {
      return this._builder.build(input);
    }
    throw new Error('NextBestActionProvider builder must implement build or generate');
  }
}

/**
 * @param {object} deps
 * @returns {NextBestActionProvider}
 */
function createNextBestActionProvider(deps) {
  return new NextBestActionProvider(deps);
}

module.exports = {
  NextBestActionProvider,
  createNextBestActionProvider,
};
