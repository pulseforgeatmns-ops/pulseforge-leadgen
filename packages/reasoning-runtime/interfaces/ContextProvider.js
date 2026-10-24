'use strict';

/**
 * ContextProvider — builds immutable reasoning context for a domain.
 * The runtime receives context; it does not construct domain context.
 *
 * @typedef {object} ContextProvider
 * @property {string} [id]
 * @property {(input: object) => object|Promise<object>} build
 */

/**
 * @param {ContextProvider} provider
 */
function assertContextProvider(provider) {
  if (!provider || typeof provider !== 'object') {
    throw new Error('ContextProvider must be an object');
  }
  if (typeof provider.build !== 'function') {
    throw new Error('ContextProvider requires build(input)');
  }
}

module.exports = {
  assertContextProvider,
};
