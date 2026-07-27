'use strict';

const { assertContextProvider } = require('../interfaces/ContextProvider');

/**
 * CRM Context Provider — wraps an injected context builder (ReasoningContextBuilder).
 *
 * @typedef {object} ContextBuilder
 * @property {(input: object) => object|Promise<object>} build
 */
class CRMContextProvider {
  /**
   * @param {object} deps
   * @param {ContextBuilder} deps.builder
   * @param {string} [deps.id]
   */
  constructor(deps) {
    if (!deps || !deps.builder || typeof deps.builder.build !== 'function') {
      throw new Error('CRMContextProvider requires builder.build');
    }
    this.id = deps.id || 'crm-context';
    this._builder = deps.builder;
    assertContextProvider(this);
  }

  /**
   * @param {object} input
   */
  build(input) {
    return this._builder.build(input);
  }
}

/**
 * @param {object} deps
 * @returns {CRMContextProvider}
 */
function createCRMContextProvider(deps) {
  return new CRMContextProvider(deps);
}

module.exports = {
  CRMContextProvider,
  createCRMContextProvider,
};
