'use strict';

/**
 * SPEC-259A — Normalized platform publication contract for Paige social content.
 */

const ADAPTER_VERSION = '1.0.0';

/**
 * @param {object} partial
 * @returns {object}
 */
function buildPublishSuccess(partial = {}) {
  return {
    success: true,
    externalPlatform: partial.externalPlatform || partial.platform,
    externalAccountId: partial.externalAccountId ?? null,
    externalPostId: partial.externalPostId ?? null,
    externalUrl: partial.externalUrl ?? null,
    raw: partial.raw && typeof partial.raw === 'object' ? partial.raw : {},
    retryable: false,
    errorCode: null,
    errorMessage: null,
  };
}

/**
 * @param {object} partial
 * @returns {object}
 */
function buildPublishFailure(partial = {}) {
  return {
    success: false,
    externalPlatform: partial.externalPlatform || partial.platform || null,
    externalAccountId: null,
    externalPostId: null,
    externalUrl: null,
    raw: partial.raw && typeof partial.raw === 'object' ? partial.raw : {},
    retryable: partial.retryable !== false,
    errorCode: partial.errorCode || 'publish_failed',
    errorMessage: partial.errorMessage || partial.message || 'publish_failed',
  };
}

module.exports = {
  ADAPTER_VERSION,
  buildPublishSuccess,
  buildPublishFailure,
};
