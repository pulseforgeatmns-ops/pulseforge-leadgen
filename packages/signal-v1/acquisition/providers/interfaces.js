'use strict';

/**
 * @typedef {object} RawResearchCandidate
 * @property {string} tokenAddress
 * @property {string} [chain]
 * @property {string} discoveredFrom
 * @property {string|Date} earliestKnownCallAt
 * @property {string[]} [sourceIds]
 * @property {string[]} [sourceClusterIds]
 * @property {string} [selectionCategory]
 * @property {string} [selectionReason]
 * @property {object} [provenance]
 * @property {object} [acquisitionPayload] — provider-specific caller/market/structure hints (not Signal features)
 */

/**
 * @typedef {object} ResearchCandidateProvider
 * @property {string} providerId
 * @property {() => Promise<RawResearchCandidate[]>|RawResearchCandidate[]} discoverCandidates
 */

module.exports = {};
