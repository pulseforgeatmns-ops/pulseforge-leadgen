'use strict';

/**
 * @typedef {object} RawCallerObservation
 * @property {string} sourceId
 * @property {string} [communityId]
 * @property {string} externalMessageId
 * @property {Date|string} messageTimestamp
 * @property {string} [rawText]
 * @property {string} [rawReferenceUrl]
 * @property {string} [tokenCa]
 * @property {Date|string} [providerTimestamp]
 * @property {object} [provenance]
 * @property {object} [forwarding]
 */

/**
 * @typedef {object} LiveCallerCollector
 * @property {string} id
 * @property {() => Promise<RawCallerObservation[]>} poll
 * @property {() => Promise<{ available: boolean, reason?: string }>} [health]
 */

module.exports = {};
